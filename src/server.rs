//! The server side: a tower layer that rebuilds DPD1 request bodies before the inner service sees them.
//!
//! The inner service (an LLM API handler, or a forwarder to one) receives every request exactly as
//! the client built it: the rebuilt body, `content-length` set, and no `dpd1-*` or `dpd1` encoding.
//! A request without DPD1 headers passes through untouched and unbuffered.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH};
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode};
use http_body::Body;
use http_body_util::{BodyExt, Either, Full, LengthLimitError, Limited};
use sha2::{Digest, Sha256};

use crate::codec::{self, Error};
use crate::store::{Scope, Store};
use crate::{ENCODING, ERROR, KEEP, KEPT, hex};

/// What the inner service is given: the original body, or a rebuilt one.
pub type InnerBody<B> = Either<B, Full<Bytes>>;

#[derive(Debug, Clone)]
pub struct Config {
    /// The largest body kept or rebuilt, and the largest delta read.
    pub max_body: usize,
    /// The headers whose values make a tenant's scope: a base is found only under the scope that
    /// kept it. Requests that carry none of them share one scope.
    pub scope_headers: Vec<HeaderName>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_body: 64 << 20,
            scope_headers: vec![http::header::AUTHORIZATION, HeaderName::from_static("x-api-key")],
        }
    }
}

/// Counters for metrics: bytes as they arrived, and as the inner service received them.
#[derive(Debug, Default)]
pub struct Stats {
    pub deltas: AtomicU64,
    pub delta_bytes: AtomicU64,
    pub rebuilt_bytes: AtomicU64,
    pub kept: AtomicU64,
    pub refused: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct Dpd1Layer {
    store: Arc<Store>,
    config: Arc<Config>,
    stats: Arc<Stats>,
}

impl Dpd1Layer {
    pub fn new(store: Arc<Store>, config: Config) -> Self {
        Dpd1Layer { store, config: Arc::new(config), stats: Arc::default() }
    }

    pub fn stats(&self) -> &Arc<Stats> {
        &self.stats
    }
}

impl<S> tower_layer::Layer<S> for Dpd1Layer {
    type Service = Dpd1<S>;

    fn layer(&self, inner: S) -> Dpd1<S> {
        Dpd1 { inner, layer: self.clone() }
    }
}

#[derive(Debug, Clone)]
pub struct Dpd1<S> {
    inner: S,
    layer: Dpd1Layer,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

impl<S, B, R> tower_service::Service<Request<B>> for Dpd1<S>
where
    S: tower_service::Service<Request<InnerBody<B>>, Response = Response<R>> + Clone + Send + 'static,
    S::Future: Send,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    R: Default,
{
    type Response = Response<R>;
    type Error = S::Error;
    type Future = BoxFuture<Result<Response<R>, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        // the clone that was not polled ready stays behind (tower's usual swap)
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let layer = self.layer.clone();
        let headers = request.headers();
        let delta =
            headers.get(CONTENT_ENCODING).is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(ENCODING.as_bytes()));
        let keep = headers.get(KEEP).is_some_and(|v| v == "?1");
        let declared = headers.get(CONTENT_LENGTH).and_then(|v| v.to_str().ok()?.parse::<u64>().ok());
        if !delta && (!keep || declared.is_some_and(|n| n > layer.config.max_body as u64)) {
            return Box::pin(inner.call(request.map(Either::Left)));
        }
        Box::pin(async move {
            let (mut parts, body) = request.into_parts();
            let scope = scope(&layer.config, &parts.headers);
            let (body, hash) = match layer.rebuild(&scope, body, delta).await {
                Ok(rebuilt) => rebuilt,
                Err(e) => {
                    layer.stats.refused.fetch_add(1, Ordering::Relaxed);
                    return Ok(refusal(e));
                }
            };
            parts.headers.remove(KEEP);
            if delta {
                parts.headers.remove(CONTENT_ENCODING);
            }
            parts.headers.insert(CONTENT_LENGTH, HeaderValue::from(body.len()));
            if keep {
                layer.store.put(&scope, hash, body.clone());
                layer.stats.kept.fetch_add(1, Ordering::Relaxed);
            }
            let mut response = inner.call(Request::from_parts(parts, Either::Right(Full::new(body)))).await?;
            if keep {
                response.headers_mut().insert(KEPT, HeaderValue::from_str(&hex(&hash)).expect("hex is a valid header"));
            }
            Ok(response)
        })
    }
}

impl Dpd1Layer {
    /// The body the client built, and its hash: read in full (at most `max_body`), and rebuilt if a delta.
    async fn rebuild<B>(&self, scope: &Scope, body: B, delta: bool) -> Result<(Bytes, codec::Hash), Error>
    where
        B: Body<Data = Bytes>,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let max = self.config.max_body;
        let wire = match Limited::new(body, max).collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) if e.downcast_ref::<LengthLimitError>().is_some() => return Err(Error::TooLarge),
            Err(_) => return Err(Error::Malformed("unreadable body")),
        };
        if !delta {
            let hash = codec::hash(&wire);
            return Ok((wire, hash));
        }
        let (header, frame) = codec::parse(&wire, max)?;
        let base = self.store.get(scope, &header.base).ok_or(Error::UnknownBase)?;
        let target = codec::apply(&base, &header, frame)?;
        self.stats.deltas.fetch_add(1, Ordering::Relaxed);
        self.stats.delta_bytes.fetch_add(wire.len() as u64, Ordering::Relaxed);
        self.stats.rebuilt_bytes.fetch_add(target.len() as u64, Ordering::Relaxed);
        Ok((Bytes::from(target), header.target))
    }
}

fn refusal<R: Default>(e: Error) -> Response<R> {
    let status = match e {
        Error::UnknownBase => StatusCode::CONFLICT,
        Error::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        Error::Malformed(_) | Error::UnsupportedVersion(_) | Error::Corrupt => StatusCode::BAD_REQUEST,
    };
    let mut response = Response::new(R::default());
    *response.status_mut() = status;
    response.headers_mut().insert(ERROR, HeaderValue::from_static(e.code()));
    response
}

/// SHA-256 over each scope header's name and values: header values cannot hold NUL or 0xff bytes,
/// so the separators keep `a: xy` and `a: x, y` (or another header with the same value) apart.
fn scope(config: &Config, headers: &HeaderMap) -> Scope {
    let mut h = Sha256::new();
    for name in &config.scope_headers {
        h.update(name.as_str());
        for value in headers.get_all(name) {
            h.update([0]);
            h.update(value.as_bytes());
        }
        h.update([0xff]);
    }
    h.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_is_the_configured_headers_and_nothing_else() {
        let config = Config::default();
        let mut a = HeaderMap::new();
        a.insert("authorization", HeaderValue::from_static("Bearer k1"));
        let mut b = a.clone();
        b.insert("x-other", HeaderValue::from_static("1"));
        assert_eq!(scope(&config, &a), scope(&config, &b));
        b.insert("x-api-key", HeaderValue::from_static("k"));
        assert_ne!(scope(&config, &a), scope(&config, &b));
        let mut c = HeaderMap::new();
        c.insert("x-api-key", HeaderValue::from_static("Bearer k1"));
        assert_ne!(scope(&config, &a), scope(&config, &c), "a value under another header is another scope");
    }
}
