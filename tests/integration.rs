//! The client and the server layer together, in process: what the inner service receives, and
//! what the server refuses.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH};
use http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use logpak::client::Client;
use logpak::server::{Config, InnerBody, Lpd1, Lpd1Layer, Stats};
use logpak::store::Store;
use logpak::{ENCODING, ERROR, KEEP, KEPT, codec, hex};
use tower_layer::Layer;
use tower_service::Service;

type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

/// The inner service: records what it was given, answers 200.
#[derive(Clone, Default)]
struct Upstream(Arc<Mutex<Vec<(HeaderMap, Bytes)>>>);

impl<B> Service<Request<InnerBody<B>>> for Upstream
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = Response<Full<Bytes>>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<InnerBody<B>>) -> Self::Future {
        let seen = self.0.clone();
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let body = body.collect().await.unwrap().to_bytes();
            seen.lock().unwrap().push((parts.headers, body));
            Ok(Response::new(Full::new(Bytes::from_static(b"ok"))))
        })
    }
}

fn server(max_body: usize) -> (Lpd1<Upstream>, Upstream, Arc<Store>, Arc<Stats>) {
    let upstream = Upstream::default();
    let store = Arc::new(Store::new(1 << 24, Duration::from_secs(60)));
    let layer = Lpd1Layer::new(store.clone(), Config { max_body, ..Config::default() });
    let stats = layer.stats().clone();
    (layer.layer(upstream.clone()), upstream, store, stats)
}

fn request(body: &[u8], headers: &[(&str, &str)]) -> Request<Full<Bytes>> {
    let mut r = Request::post("/v1/messages").header("authorization", "Bearer k1");
    for (k, v) in headers {
        r = r.header(*k, *v);
    }
    r.body(Full::new(Bytes::copy_from_slice(body))).unwrap()
}

async fn send(s: &mut Lpd1<Upstream>, r: Request<Full<Bytes>>) -> Response<Full<Bytes>> {
    s.call(r).await.unwrap()
}

fn header<'a>(r: &'a Response<Full<Bytes>>, name: &str) -> Option<&'a str> {
    r.headers().get(name).map(|v| v.to_str().unwrap())
}

fn turn(n: usize) -> Vec<u8> {
    let msgs: String = (0..n).map(|i| format!(r#"{{"role":"user","content":"turn {i}"}},"#)).collect();
    format!(r#"{{"system":"{}","messages":[{msgs}{{}}]}}"#, "be helpful ".repeat(1000)).into_bytes()
}

#[tokio::test]
async fn a_plain_request_passes_through_untouched_and_unkept() {
    let (mut s, up, store, _) = server(1 << 20);
    let r = send(&mut s, request(b"hello", &[("x-other", "1")])).await;
    assert_eq!(r.status(), 200);
    assert_eq!(header(&r, KEPT), None);
    let seen = up.0.lock().unwrap();
    assert_eq!(seen[0].1, "hello");
    assert_eq!(seen[0].0["x-other"], "1");
    assert_eq!(store.bytes(), 0);
}

#[tokio::test]
async fn a_kept_request_is_answered_with_its_hash_and_forwarded_without_lpd1_headers() {
    let (mut s, up, store, _) = server(1 << 20);
    let r = send(&mut s, request(b"hello", &[(KEEP, "?1")])).await;
    assert_eq!(header(&r, KEPT), Some(hex(&codec::hash(b"hello")).as_str()));
    let seen = up.0.lock().unwrap();
    assert_eq!(seen[0].1, "hello");
    assert!(!seen[0].0.contains_key(KEEP));
    assert_eq!(store.bytes(), 5);
}

#[tokio::test]
async fn a_delta_reaches_the_inner_service_as_the_body_the_client_built() {
    let (mut s, up, _, stats) = server(1 << 20);
    let client = Client::new(1);
    for n in 1..6 {
        let p = client.prepare(Bytes::from(turn(n)));
        assert_eq!(p.is_delta(), n > 1);
        let r = send(&mut s, request(p.wire_body(), &p.headers())).await;
        assert_eq!(r.status(), 200, "{:?}", header(&r, ERROR));
        client.confirm(&p, header(&r, KEPT));
    }
    let seen = up.0.lock().unwrap();
    for (n, (headers, body)) in seen.iter().enumerate() {
        assert_eq!(body.as_ref(), turn(n + 1));
        assert_eq!(headers[CONTENT_LENGTH], body.len().to_string());
        assert!(!headers.contains_key(CONTENT_ENCODING) && !headers.contains_key(KEEP));
        assert_eq!(headers["authorization"], "Bearer k1");
    }
    assert_eq!(stats.deltas.load(Ordering::Relaxed), 4);
    assert!(stats.delta_bytes.load(Ordering::Relaxed) * 50 < stats.rebuilt_bytes.load(Ordering::Relaxed));
}

#[tokio::test]
async fn a_delta_against_an_unknown_base_is_refused_and_not_forwarded() {
    let (mut s, up, _, _) = server(1 << 20);
    let d = codec::encode(b"base", &codec::hash(b"base"), b"base plus", &codec::hash(b"base plus"));
    let r = send(&mut s, request(&d, &[("content-encoding", ENCODING)])).await;
    assert_eq!(r.status(), StatusCode::CONFLICT);
    assert_eq!(header(&r, ERROR), Some("unknown-base"));
    assert!(up.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn another_tenants_base_is_unknown() {
    let (mut s, _, _, _) = server(1 << 20);
    send(&mut s, request(b"base", &[(KEEP, "?1")])).await;
    let d = codec::encode(b"base", &codec::hash(b"base"), b"base plus", &codec::hash(b"base plus"));
    let mut r = request(&d, &[("content-encoding", ENCODING)]);
    r.headers_mut().insert("authorization", HeaderValue::from_static("Bearer k2"));
    let r = send(&mut s, r).await;
    assert_eq!(header(&r, ERROR), Some("unknown-base"));
}

#[tokio::test]
async fn a_corrupt_or_oversized_delta_is_refused_with_its_code() {
    let (mut s, up, _, stats) = server(200);
    send(&mut s, request(b"base", &[(KEEP, "?1")])).await;
    let mut d = codec::encode(b"base", &codec::hash(b"base"), b"base plus", &codec::hash(b"base plus"));
    *d.last_mut().unwrap() ^= 0xff;
    let r = send(&mut s, request(&d, &[("content-encoding", ENCODING)])).await;
    assert_eq!((r.status(), header(&r, ERROR)), (StatusCode::BAD_REQUEST, Some("corrupt")));
    let r = send(&mut s, request(b"LPD1\x07", &[("content-encoding", ENCODING)])).await;
    assert_eq!((r.status(), header(&r, ERROR)), (StatusCode::BAD_REQUEST, Some("unsupported-version")));
    let r = send(&mut s, request(&[0; 201], &[("content-encoding", ENCODING)])).await;
    assert_eq!((r.status(), header(&r, ERROR)), (StatusCode::PAYLOAD_TOO_LARGE, Some("too-large")));
    assert_eq!(up.0.lock().unwrap().len(), 1, "only the plain request was forwarded");
    assert_eq!(stats.refused.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn a_kept_body_over_the_limit_is_refused_unless_its_length_says_so_up_front() {
    let (mut s, up, store, _) = server(8);
    // content-length over the limit: forwarded as it is, not kept, not buffered
    let mut r = request(b"123456789", &[(KEEP, "?1")]);
    r.headers_mut().insert(CONTENT_LENGTH, HeaderValue::from_static("9"));
    let r = send(&mut s, r).await;
    assert_eq!((r.status(), header(&r, KEPT)), (StatusCode::OK, None));
    assert_eq!(up.0.lock().unwrap()[0].1, "123456789");
    // no content-length, and the body runs over: refused before anything is forwarded
    let r = send(&mut s, request(b"123456789", &[(KEEP, "?1")])).await;
    assert_eq!((r.status(), header(&r, ERROR)), (StatusCode::PAYLOAD_TOO_LARGE, Some("too-large")));
    assert_eq!(up.0.lock().unwrap().len(), 1);
    assert_eq!(store.bytes(), 0);
    // exactly at the limit, declared: kept
    let mut r = request(b"12345678", &[(KEEP, "?1")]);
    r.headers_mut().insert(CONTENT_LENGTH, HeaderValue::from_static("8"));
    let r = send(&mut s, r).await;
    assert_eq!(header(&r, KEPT), Some(hex(&codec::hash(b"12345678")).as_str()));
}

/// An inner service that is never ready: the layer must not claim readiness it does not have.
#[derive(Clone)]
struct Busy;

impl Service<Request<InnerBody<Full<Bytes>>>> for Busy {
    type Response = Response<Full<Bytes>>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }

    fn call(&mut self, _: Request<InnerBody<Full<Bytes>>>) -> Self::Future {
        unreachable!("never ready")
    }
}

#[test]
fn the_layer_is_ready_only_when_the_inner_service_is() {
    let store = Arc::new(Store::new(1 << 20, Duration::from_secs(60)));
    let mut s = Lpd1Layer::new(store, Config::default()).layer(Busy);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let ready = <Lpd1<Busy> as Service<Request<Full<Bytes>>>>::poll_ready(&mut s, &mut cx);
    assert!(ready.is_pending());
}

#[tokio::test]
async fn a_body_that_fails_to_arrive_is_refused_as_malformed_not_too_large() {
    let upstream = Upstream::default();
    let store = Arc::new(Store::new(1 << 20, Duration::from_secs(60)));
    let mut s = Lpd1Layer::new(store, Config::default()).layer(upstream.clone());
    let broken = futures_util::stream::iter([
        Ok(http_body::Frame::data(Bytes::from_static(b"half a bo"))),
        Err(std::io::Error::other("connection reset")),
    ]);
    let r = Request::post("/").header(KEEP, "?1").body(http_body_util::StreamBody::new(broken)).unwrap();
    let r = s.call(r).await.unwrap();
    assert_eq!((r.status(), header(&r, ERROR)), (StatusCode::BAD_REQUEST, Some("malformed")));
    assert!(upstream.0.lock().unwrap().is_empty());
}
