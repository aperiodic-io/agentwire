//! `agentwire server`: the AWD1 layer as a reverse proxy in front of an HTTP API.
//! `agentwire client`: a local proxy that sends what a tool posts to it as AWD1 deltas.
//! `agentwire collect`: a log sink (OTLP/HTTP, a gateway's HTTP log plugin) that stores every
//! request body in AWL1, and forwards it unchanged when given a backend.
//! `agentwire log`: pack, list and read AWL1 files (what `--record-dir` and `collect` write).

use std::convert::Infallible;
use std::fs::File;
use std::future::Future;
use std::io::{self, BufRead, BufWriter, Read, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use agentwire::client::Client;
use agentwire::log;
use agentwire::server::{Awd1Layer, Config, InnerBody, Stats};
use agentwire::store::Store;
use agentwire::{ERROR, KEPT};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use http::header::{self, HeaderMap, HeaderName};
use http::{Request, Response, StatusCode};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tower_layer::Layer;

mod grpc;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type ResBody = UnsyncBoxBody<Bytes, BoxError>;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand)]
enum Mode {
    /// Rebuild AWD1 requests and forward them, byte-exact, to the upstream API.
    Server {
        #[arg(long, env = "AGENTWIRE_LISTEN", default_value = "0.0.0.0:8080")]
        listen: SocketAddr,
        /// The API requests are forwarded to, e.g. https://api.anthropic.com
        #[arg(long, env = "AGENTWIRE_UPSTREAM")]
        upstream: reqwest::Url,
        /// Bytes of kept bases held in memory, across all tenants.
        #[arg(long, env = "AGENTWIRE_STORE_BYTES", default_value_t = 1 << 30)]
        store_bytes: usize,
        /// Seconds a base unused is kept.
        #[arg(long, env = "AGENTWIRE_TTL_SECONDS", default_value_t = 1800)]
        ttl_seconds: u64,
        /// The largest body kept or rebuilt.
        #[arg(long, env = "AGENTWIRE_MAX_BODY", default_value_t = 64 << 20)]
        max_body: usize,
        /// The headers that identify a tenant (repeat the flag); bases are never shared across them.
        #[arg(long = "scope-header", env = "AGENTWIRE_SCOPE_HEADERS", value_delimiter = ',', default_values_t = [String::from("authorization"), String::from("x-api-key")])]
        scope_headers: Vec<String>,
        /// Serve Prometheus metrics at http://<addr>/metrics.
        #[arg(long, env = "AGENTWIRE_METRICS")]
        metrics: Option<SocketAddr>,
        /// Store every request body forwarded, with its time, method and path (never its headers),
        /// in a new AWL1 file in this directory; read it with `agentwire log`.
        #[arg(long, env = "AGENTWIRE_RECORD_DIR")]
        record_dir: Option<PathBuf>,
        /// Records a new one may delta against: about one per conversation in flight.
        #[arg(long, env = "AGENTWIRE_RECORD_WINDOW", default_value_t = 16)]
        record_window: usize,
        /// Start a new AWL1 file once one reaches this size; every file reads on its own.
        #[arg(long, env = "AGENTWIRE_ROTATE_BYTES", default_value_t = 1 << 30)]
        rotate_bytes: u64,
    },
    /// Store every request body posted (OTLP/HTTP traces and logs, a gateway's HTTP log plugin) in
    /// AWL1, gzip decoded; forward each request as received when given a backend. Also accepts
    /// AWD1 deltas from `agentwire client`.
    Collect {
        /// Where to listen (repeat, or comma-separate); each address takes OTLP/HTTP and OTLP/gRPC.
        /// Loopback by default: anyone who can reach it can fill the disk. In a container, give
        /// 0.0.0.0:4318,0.0.0.0:4317 and put it on a private network.
        #[arg(long, env = "AGENTWIRE_LISTEN", value_delimiter = ',', default_value = "127.0.0.1:4318,127.0.0.1:4317")]
        listen: Vec<SocketAddr>,
        /// The new AWL1 file goes in this directory; read it with `agentwire log`.
        #[arg(long, env = "AGENTWIRE_RECORD_DIR")]
        record_dir: PathBuf,
        /// The backend each request is forwarded to, as received, e.g.
        /// https://cloud.langfuse.com/api/public/otel. Without one, each request is answered 200.
        #[arg(long, env = "AGENTWIRE_FORWARD")]
        forward: Option<reqwest::Url>,
        /// Records a new one may delta against: about one per trace or exporter interleaved.
        #[arg(long, env = "AGENTWIRE_RECORD_WINDOW", default_value_t = 16)]
        record_window: usize,
        /// Start a new AWL1 file once one reaches this size; every file reads on its own.
        #[arg(long, env = "AGENTWIRE_ROTATE_BYTES", default_value_t = 1 << 30)]
        rotate_bytes: u64,
        /// The largest body stored, after gzip decoding.
        #[arg(long, env = "AGENTWIRE_MAX_BODY", default_value_t = 64 << 20)]
        max_body: usize,
        #[arg(long, env = "AGENTWIRE_METRICS")]
        metrics: Option<SocketAddr>,
    },
    /// Accept plain requests from a local tool and send them to an `agentwire server` as deltas.
    Client {
        #[arg(long, env = "AGENTWIRE_LISTEN", default_value = "127.0.0.1:8787")]
        listen: SocketAddr,
        /// The `agentwire server` (or an AWD1-aware API) to send to.
        #[arg(long, env = "AGENTWIRE_UPSTREAM")]
        upstream: reqwest::Url,
        /// Bodies remembered as bases: about one per conversation in flight.
        #[arg(long, env = "AGENTWIRE_BASES", default_value_t = 8)]
        bases: usize,
        /// The largest request body accepted from the tool.
        #[arg(long, env = "AGENTWIRE_MAX_BODY", default_value_t = 64 << 20)]
        max_body: usize,
        #[arg(long, env = "AGENTWIRE_METRICS")]
        metrics: Option<SocketAddr>,
    },
    /// Read and write AWL1 files: bodies stored as deltas against earlier ones.
    #[command(subcommand)]
    Log(LogCmd),
}

#[derive(Subcommand)]
enum LogCmd {
    /// Store each line of stdin (JSON lines: spans, requests) as a record of a new AWL1 file.
    Pack {
        file: PathBuf,
        /// Records a new one may delta against: about one per trace or conversation interleaved.
        #[arg(long, default_value_t = 16)]
        window: usize,
    },
    /// Write every record's body to stdout, one per line: `pack`'s input back.
    Unpack {
        file: PathBuf,
        #[arg(long, default_value_t = 64 << 20)]
        max_body: usize,
    },
    /// Write one record's body to stdout, byte for byte.
    Get {
        file: PathBuf,
        index: usize,
        #[arg(long, default_value_t = 64 << 20)]
        max_body: usize,
    },
    /// List the records: index, records back to its base (0: stored whole), bytes stored, meta.
    Ls { file: PathBuf },
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    if let Mode::Log(cmd) = cli.mode {
        return run_log(cmd);
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "agentwire=info".into()),
        )
        .init();
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    match cli.mode {
        Mode::Log(_) => unreachable!("handled above"),
        Mode::Server {
            listen,
            upstream,
            store_bytes,
            ttl_seconds,
            max_body,
            scope_headers,
            metrics,
            record_dir,
            record_window,
            rotate_bytes,
        } => {
            let record = match record_dir {
                Some(dir) => Some(Arc::new(Mutex::new(Recording::open(dir, record_window, rotate_bytes)?))),
                None => None,
            };
            let store = Store::new(store_bytes, Duration::from_secs(ttl_seconds));
            let scope_headers =
                scope_headers.iter().map(|h| HeaderName::try_from(h.as_str())).collect::<Result<_, _>>()?;
            let forward = Forward { http, upstream: Some(Arc::new(upstream)), record, max_body, grpc: false };
            run_server(vec![listen], store, Config { max_body, scope_headers }, metrics, forward).await
        }
        Mode::Collect { listen, record_dir, forward, record_window, rotate_bytes, max_body, metrics } => {
            let record = Some(Arc::new(Mutex::new(Recording::open(record_dir, record_window, rotate_bytes)?)));
            let forward = Forward { http, upstream: forward.map(Arc::new), record, max_body, grpc: true };
            let config = Config { max_body, ..Config::default() };
            run_server(listen, Store::new(1 << 30, Duration::from_secs(1800)), config, metrics, forward).await
        }
        Mode::Client { listen, upstream, bases, max_body, metrics } => {
            let proxy = Arc::new(ClientProxy {
                http,
                upstream,
                client: Client::new(bases),
                max_body,
                stats: Default::default(),
            });
            if let Some(addr) = metrics {
                let proxy = proxy.clone();
                serve_metrics(addr, move || proxy.stats.render()).await?;
            }
            serve(listen, move || {
                let proxy = proxy.clone();
                hyper::service::service_fn(move |r| {
                    let proxy = proxy.clone();
                    async move { Ok::<_, Infallible>(proxy.handle(r).await) }
                })
            })
            .await
        }
    }
}

/// The AWD1 layer in front of `forward`, until shutdown.
async fn run_server(
    listen: Vec<SocketAddr>,
    store: Store,
    config: Config,
    metrics: Option<SocketAddr>,
    forward: Forward,
) -> Result<(), BoxError> {
    let store = Arc::new(store);
    let layer = Awd1Layer::new(store.clone(), config);
    if let Some(addr) = metrics {
        let (stats, store, record) = (layer.stats().clone(), store.clone(), forward.record.clone());
        serve_metrics(addr, move || {
            server_metrics(&stats, &store) + &record.as_ref().map_or_else(String::new, |r| lock(r).metrics())
        })
        .await?;
    }
    let service = layer.layer(forward.clone());
    let make = move || TowerToHyperService::new(service.clone());
    futures_util::future::try_join_all(listen.into_iter().map(|addr| serve(addr, make.clone()))).await?;
    Ok(())
}

/// Accepts connections until SIGTERM or Ctrl-C, then lets requests in flight finish.
async fn serve<S, F>(addr: SocketAddr, make: F) -> Result<(), BoxError>
where
    F: Fn() -> S,
    S: hyper::service::Service<Request<Incoming>, Response = Response<ResBody>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder.http1().timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(30));
    let graceful = GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown_signal());
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(a) => a,
                    Err(e) => { tracing::warn!(error = %e, "accept failed"); continue; }
                };
                let conn = builder.serve_connection_with_upgrades(TokioIo::new(stream), make());
                let conn = graceful.watch(conn.into_owned());
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::debug!(error = %e, "connection closed with an error");
                    }
                });
            }
            () = &mut shutdown => break,
        }
    }
    tracing::info!("shutting down; waiting for requests in flight");
    graceful.shutdown().await;
    Ok(())
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

type Recorder = Arc<Mutex<Recording>>;

fn lock(r: &Recorder) -> std::sync::MutexGuard<'_, Recording> {
    // every append completes, or fails and moves to a new file, under the lock
    r.lock().unwrap_or_else(|p| p.into_inner())
}

/// AWL1 files in `dir`, named by when each was opened, so a restart never appends to or replaces
/// one, and in order by name. Each is self-contained: a new one starts without bases.
struct Recording {
    dir: PathBuf,
    window: usize,
    rotate_bytes: u64,
    path: PathBuf,
    writer: log::Writer<BufWriter<File>>,
    seq: u64,
    /// bytes of the files already closed
    closed: u64,
    recorded: u64,
    body_bytes: u64,
    failures: u64,
}

impl Recording {
    fn open(dir: PathBuf, window: usize, rotate_bytes: u64) -> io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let (path, writer) = Self::create(&dir, window, 0)?;
        Ok(Recording {
            dir,
            window,
            rotate_bytes,
            path,
            writer,
            seq: 0,
            closed: 0,
            recorded: 0,
            body_bytes: 0,
            failures: 0,
        })
    }

    fn create(dir: &std::path::Path, window: usize, seq: u64) -> io::Result<(PathBuf, log::Writer<BufWriter<File>>)> {
        let path = dir.join(format!("{}-{}-{seq:06}.awl", unix_ms(), std::process::id()));
        let file = File::options().write(true).create_new(true).open(&path)?;
        tracing::info!(path = %path.display(), "recording");
        let mut w = log::Writer::new(BufWriter::new(file), window)?;
        w.flush()?; // a valid, empty AWL1 file before the first record
        Ok((path, w))
    }

    fn next_file(&mut self) -> io::Result<()> {
        let len = std::fs::metadata(&self.path).map_or(0, |m| m.len());
        let (path, writer) = Self::create(&self.dir, self.window, self.seq + 1)?;
        (self.closed, self.seq, self.path, self.writer) = (self.closed + len, self.seq + 1, path, writer);
        Ok(())
    }

    /// Appends and flushes one record, then starts a new file if this one is full. A failed write
    /// may leave a part of a record at the end of the file: readers keep the records before it, and
    /// the next record goes to a new file.
    fn append(&mut self, meta: &[u8], body: Bytes) -> io::Result<()> {
        let n = body.len() as u64;
        let appended = self.writer.append(meta, body).and_then(|_| self.writer.flush());
        if let Err(e) = appended {
            tracing::error!(error = %e, path = %self.path.display(), "recording failed; forwarding the request unrecorded");
            self.failures += 1;
            let _ = self.next_file();
            return Err(e);
        }
        self.recorded += 1;
        self.body_bytes += n;
        if std::fs::metadata(&self.path)?.len() >= self.rotate_bytes {
            self.next_file()?;
        }
        Ok(())
    }

    fn metrics(&self) -> String {
        let on_disk = self.closed + std::fs::metadata(&self.path).map_or(0, |m| m.len());
        format!(
            "# TYPE agentwire_recorded_total counter\nagentwire_recorded_total {}\n\
             # TYPE agentwire_recorded_body_bytes_total counter\nagentwire_recorded_body_bytes_total {}\n\
             # TYPE agentwire_recorded_file_bytes_total counter\nagentwire_recorded_file_bytes_total {on_disk}\n\
             # TYPE agentwire_record_failures_total counter\nagentwire_record_failures_total {}\n",
            self.recorded, self.body_bytes, self.failures,
        )
    }
}

fn unix_ms() -> u128 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis())
}

/// The inner service of `agentwire server` and `collect`: records the request (if recording), then
/// sends it on to the upstream, or answers it (a collector without a backend).
#[derive(Clone)]
struct Forward {
    http: reqwest::Client,
    upstream: Option<Arc<reqwest::Url>>,
    record: Option<Recorder>,
    max_body: usize,
    /// `collect`: take OTLP/gRPC Export calls too
    grpc: bool,
}

impl tower_service::Service<Request<InnerBody<Incoming>>> for Forward {
    type Response = Response<ResBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response<ResBody>, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<InnerBody<Incoming>>) -> Self::Future {
        let (http, upstream, record, max_body) =
            (self.http.clone(), self.upstream.clone(), self.record.clone(), self.max_body);
        let grpc = self.grpc && grpc::is_grpc(request.headers());
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            if let (true, Some(record)) = (grpc, &record) {
                return Ok(export(&http, upstream.as_deref(), record, parts, body, max_body).await);
            }
            let (Some(record), upstream) = (record, &upstream) else {
                let upstream = upstream.as_ref().expect("a server without a recorder has an upstream");
                return Ok(forward(&http, upstream, &parts, reqwest::Body::wrap(body)).await);
            };
            let body = match Limited::new(body, max_body).collect().await {
                Ok(b) => b.to_bytes(),
                Err(_) => return Ok(status(StatusCode::PAYLOAD_TOO_LARGE)),
            };
            let (stored, encoding) = match decoded(&parts.headers, body.clone(), max_body) {
                Ok(d) => d,
                Err(code) => return Ok(status(code)),
            };
            store(&record, &parts, &encoding, stored).await;
            Ok(match upstream {
                Some(upstream) => forward(&http, upstream, &parts, body).await,
                None => ack(&parts.headers),
            })
        })
    }
}

/// Stores one record, flushed, before the request goes on: what the upstream saw is on disk. Its
/// meta is the time, method, path, content type and the encoding `body` is still in.
// ponytail: one writer behind a lock; a writer per shard if recording limits throughput
async fn store(record: &Recorder, parts: &http::request::Parts, encoding: &str, body: Bytes) {
    let path = parts.uri.path_and_query().map_or("/", |p| p.as_str());
    let ct = parts.headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).map(|v| v.replace(' ', ""));
    let meta = format!("{} {} {path} {} {encoding}", unix_ms(), parts.method, ct.as_deref().unwrap_or("-"));
    let record = record.clone();
    // a failed write is logged and counted where it happens; it never stops the request
    if let Err(e) = tokio::task::spawn_blocking(move || lock(&record).append(meta.as_bytes(), body)).await {
        tracing::error!(error = %e, "recording panicked; forwarding the request unrecorded");
    }
}

/// One OTLP/gRPC Export call: its message is stored (decompressed), then sent on to the backend as
/// OTLP/HTTP protobuf, whose answer comes back as the gRPC status; or acknowledged, without one.
async fn export(
    http: &reqwest::Client,
    upstream: Option<&reqwest::Url>,
    record: &Recorder,
    mut parts: http::request::Parts,
    body: InnerBody<Incoming>,
    max_body: usize,
) -> Response<ResBody> {
    let Some(otlp_path) = grpc::otlp_http_path(parts.uri.path()) else {
        return grpc::error(grpc::Code::Unimplemented, "not an OTLP Export method");
    };
    let Ok(body) = Limited::new(body, max_body + 5).collect().await.map(|b| b.to_bytes()) else {
        return grpc::error(grpc::Code::ResourceExhausted, "the export exceeds the size limit");
    };
    let (message, compressed) = match grpc::message(&body) {
        Ok(m) => m,
        Err(code) => return grpc::error(code, "not one length-prefixed message"),
    };
    let message = if compressed {
        let gzip = parts.headers.get("grpc-encoding").is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"gzip"));
        if !gzip {
            return grpc::error(grpc::Code::Unimplemented, "only gzip compression is supported");
        }
        match gunzip(message, max_body) {
            Ok(m) => m,
            Err(StatusCode::PAYLOAD_TOO_LARGE) => {
                return grpc::error(grpc::Code::ResourceExhausted, "the export exceeds the size limit");
            }
            Err(_) => return grpc::error(grpc::Code::InvalidArgument, "the message is not gzip"),
        }
    } else {
        body.slice_ref(message)
    };
    store(record, &parts, "-", message.clone()).await;
    let Some(upstream) = upstream else { return grpc::ok(b"") };
    // OTLP/HTTP: the same message, as protobuf, at the signal's path; the request's other headers
    // (authorization, user-agent) go along
    parts.uri = http::Uri::from_static(otlp_path);
    parts.headers.insert(header::CONTENT_TYPE, http::HeaderValue::from_static("application/x-protobuf"));
    for name in ["grpc-encoding", "grpc-accept-encoding", "grpc-timeout", "content-length"] {
        parts.headers.remove(name);
    }
    let response = forward(http, upstream, &parts, message).await;
    let code = grpc::code_for(response.status());
    if code != grpc::Code::Ok {
        return grpc::error(code, "the backend refused the export");
    }
    // an Export response from the backend the operator configured, as an HTTP answer is streamed
    // back unbounded: small (a partial-success count at most)
    match response.into_body().collect().await {
        Ok(answer) => grpc::ok(&answer.to_bytes()),
        Err(_) => grpc::error(grpc::Code::Unknown, "the backend's answer could not be read"),
    }
}

fn run_log(cmd: LogCmd) -> Result<(), BoxError> {
    let mut out = BufWriter::new(io::stdout().lock());
    match cmd {
        LogCmd::Pack { file, window } => {
            let file = File::options().write(true).create_new(true).open(file)?;
            let mut w = log::Writer::new(BufWriter::new(file), window)?;
            for line in io::stdin().lock().split(b'\n') {
                w.append(b"", Bytes::from(line?))?;
            }
            w.flush()?;
        }
        LogCmd::Unpack { file, max_body } => {
            let mut r = open_log(&file, max_body)?;
            r.scan(|_, body| {
                out.write_all(body)?;
                out.write_all(b"\n")
            })?;
        }
        LogCmd::Get { file, index, max_body } => out.write_all(&open_log(&file, max_body)?.get(index)?)?,
        LogCmd::Ls { file } => {
            for (i, r) in open_log(&file, 0)?.records().iter().enumerate() {
                writeln!(out, "{i} {} {} {}", r.back, r.stored, String::from_utf8_lossy(&r.meta))?;
            }
        }
    }
    out.flush()?;
    Ok(())
}

fn open_log(path: &std::path::Path, max_body: usize) -> io::Result<log::Reader<File>> {
    let r = log::Reader::open(File::open(path)?, max_body)?;
    if r.truncated {
        eprintln!("{}: the file ends inside a record; reading the records before it", path.display());
    }
    Ok(r)
}

/// One request to the upstream, its response streamed back; a failure to reach it is a 502.
async fn forward(
    http: &reqwest::Client,
    upstream: &reqwest::Url,
    parts: &http::request::Parts,
    body: impl Into<reqwest::Body>,
) -> Response<ResBody> {
    let url = match upstream_url(upstream, parts.uri.path_and_query().map_or("/", |p| p.as_str())) {
        Ok(url) => url,
        Err(_) => return status(StatusCode::BAD_REQUEST),
    };
    let mut headers = parts.headers.clone();
    strip_hop_by_hop(&mut headers);
    let sent = http.request(parts.method.clone(), url).headers(headers).body(body).send().await;
    match sent {
        Ok(response) => {
            let mut response = http::Response::from(response);
            strip_hop_by_hop(response.headers_mut());
            response.map(|b| b.map_err(BoxError::from).boxed_unsync())
        }
        Err(e) => {
            tracing::warn!(error = %e, "upstream request failed");
            status(StatusCode::BAD_GATEWAY)
        }
    }
}

/// The upstream's own path, then the request's (`Url::join` would replace the upstream's last segment).
fn upstream_url(upstream: &reqwest::Url, path_and_query: &str) -> Result<reqwest::Url, impl std::error::Error> {
    let base = upstream.as_str().trim_end_matches('/');
    reqwest::Url::parse(&format!("{base}/{}", path_and_query.trim_start_matches('/')))
}

/// RFC 9110 §7.6.1: connection-specific headers are not forwarded; `host` is the upstream's.
fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let named: Vec<HeaderName> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|name| HeaderName::try_from(name.trim()).ok())
        .collect();
    for name in named {
        headers.remove(name);
    }
    for name in [
        header::CONNECTION,
        header::HOST,
        header::PROXY_AUTHORIZATION,
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
        HeaderName::from_static("keep-alive"),
        HeaderName::from_static("proxy-connection"),
    ] {
        headers.remove(name);
    }
}

/// The body as stored and delta'd: gzip decoded (at most `max` bytes), with "-" for its encoding;
/// any other encoding kept as sent, and named.
fn decoded(headers: &HeaderMap, body: Bytes, max: usize) -> Result<(Bytes, String), StatusCode> {
    let encoding = headers.get(header::CONTENT_ENCODING).and_then(|v| v.to_str().ok()).unwrap_or("").trim();
    if encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") {
        return Ok((body, "-".to_owned()));
    }
    if !encoding.eq_ignore_ascii_case("gzip") && !encoding.eq_ignore_ascii_case("x-gzip") {
        return Ok((body, encoding.replace(' ', "")));
    }
    Ok((gunzip(&body, max)?, "-".to_owned()))
}

/// Gzip (one member or several) decoded to at most `max` bytes: 413 past it, 400 for what is not gzip.
fn gunzip(body: &[u8], max: usize) -> Result<Bytes, StatusCode> {
    let mut out = Vec::new();
    flate2::read::MultiGzDecoder::new(body)
        .take(max as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    if out.len() > max {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    Ok(Bytes::from(out))
}

/// A collector's answer: OTLP/HTTP's empty success, in the request's content type.
fn ack(headers: &HeaderMap) -> Response<ResBody> {
    let content_type = headers.get(header::CONTENT_TYPE).cloned();
    let json = content_type.as_ref().is_some_and(|v| v.as_bytes().starts_with(b"application/json"));
    let body = Full::new(Bytes::from_static(if json { b"{}" } else { b"" }));
    let mut r = Response::new(body.map_err(|never| match never {}).boxed_unsync());
    if let Some(v) = content_type {
        r.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    r
}

fn status(code: StatusCode) -> Response<ResBody> {
    let mut r = Response::new(ResBody::default());
    *r.status_mut() = code;
    r
}

struct ClientProxy {
    http: reqwest::Client,
    upstream: reqwest::Url,
    client: Client,
    max_body: usize,
    stats: ClientStats,
}

#[derive(Default)]
struct ClientStats {
    requests: AtomicU64,
    deltas: AtomicU64,
    body_bytes: AtomicU64,
    wire_bytes: AtomicU64,
    resent: AtomicU64,
}

impl ClientProxy {
    async fn handle(&self, request: Request<Incoming>) -> Response<ResBody> {
        let (mut parts, body) = request.into_parts();
        let body = match Limited::new(body, self.max_body).collect().await {
            Ok(b) => b.to_bytes(),
            Err(_) => return status(StatusCode::PAYLOAD_TOO_LARGE),
        };
        // a delta of gzip bytes finds nothing: decode first. A body in another encoding goes as is
        // (`awd1` must be its only content coding)
        let body = match decoded(&parts.headers, body, self.max_body) {
            Ok((body, encoding)) if encoding == "-" => {
                parts.headers.remove(header::CONTENT_ENCODING);
                body
            }
            Ok((body, _)) => return forward(&self.http, &self.upstream, &parts, body).await,
            Err(code) => return status(code),
        };
        if body.is_empty() {
            return forward(&self.http, &self.upstream, &parts, body).await;
        }
        let prepared = self.client.prepare(body);
        // the tool's content-length is the full body's; reqwest sets the one for what is sent
        parts.headers.remove(header::CONTENT_LENGTH);
        set(&mut parts.headers, &prepared.headers());
        let wire = Bytes::copy_from_slice(prepared.wire_body());
        let mut response = forward(&self.http, &self.upstream, &parts, wire.clone()).await;
        let mut wire_bytes = wire.len();
        if prepared.is_delta() && response.headers().contains_key(ERROR) {
            // refused before it was forwarded: the base was evicted or the server restarted
            tracing::debug!(error = ?response.headers()[ERROR], "delta refused; resending in full");
            self.stats.resent.fetch_add(1, Ordering::Relaxed);
            parts.headers.remove(header::CONTENT_ENCODING);
            let plain = Bytes::copy_from_slice(prepared.body());
            wire_bytes += plain.len();
            response = forward(&self.http, &self.upstream, &parts, plain).await;
        } else if prepared.is_delta() {
            self.stats.deltas.fetch_add(1, Ordering::Relaxed);
        }
        self.client.confirm(&prepared, response.headers().get(KEPT).and_then(|v| v.to_str().ok()));
        response.headers_mut().remove(KEPT);
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        self.stats.body_bytes.fetch_add(prepared.body().len() as u64, Ordering::Relaxed);
        self.stats.wire_bytes.fetch_add(wire_bytes as u64, Ordering::Relaxed);
        response
    }
}

fn set(headers: &mut HeaderMap, pairs: &[(&'static str, &'static str)]) {
    for (name, value) in pairs {
        headers.insert(*name, http::HeaderValue::from_static(value));
    }
}

impl ClientStats {
    fn render(&self) -> String {
        let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
        format!(
            "# TYPE agentwire_client_requests_total counter\nagentwire_client_requests_total {}\n\
             # TYPE agentwire_client_deltas_total counter\nagentwire_client_deltas_total {}\n\
             # TYPE agentwire_client_resent_total counter\nagentwire_client_resent_total {}\n\
             # TYPE agentwire_client_body_bytes_total counter\nagentwire_client_body_bytes_total {}\n\
             # TYPE agentwire_client_wire_bytes_total counter\nagentwire_client_wire_bytes_total {}\n",
            g(&self.requests),
            g(&self.deltas),
            g(&self.resent),
            g(&self.body_bytes),
            g(&self.wire_bytes),
        )
    }
}

fn server_metrics(stats: &Stats, store: &Store) -> String {
    let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
    format!(
        "# TYPE agentwire_server_deltas_total counter\nagentwire_server_deltas_total {}\n\
         # TYPE agentwire_server_delta_bytes_total counter\nagentwire_server_delta_bytes_total {}\n\
         # TYPE agentwire_server_rebuilt_bytes_total counter\nagentwire_server_rebuilt_bytes_total {}\n\
         # TYPE agentwire_server_kept_total counter\nagentwire_server_kept_total {}\n\
         # TYPE agentwire_server_refused_total counter\nagentwire_server_refused_total {}\n\
         # TYPE agentwire_server_store_bytes gauge\nagentwire_server_store_bytes {}\n",
        g(&stats.deltas),
        g(&stats.delta_bytes),
        g(&stats.rebuilt_bytes),
        g(&stats.kept),
        g(&stats.refused),
        store.bytes(),
    )
}

/// A plain-text Prometheus endpoint on its own address, so it never shadows an upstream path.
async fn serve_metrics<F>(addr: SocketAddr, render: F) -> Result<(), BoxError>
where
    F: Fn() -> String + Clone + Send + Sync + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr = %listener.local_addr()?, "metrics");
    tokio::spawn(async move {
        let builder = auto::Builder::new(TokioExecutor::new());
        while let Ok((stream, _)) = listener.accept().await {
            let render = render.clone();
            let service = hyper::service::service_fn(move |r: Request<Incoming>| {
                let response = if r.uri().path() == "/metrics" {
                    Response::new(Full::new(Bytes::from(render())))
                } else {
                    let mut r = Response::new(Full::default());
                    *r.status_mut() = StatusCode::NOT_FOUND;
                    r
                };
                async move { Ok::<_, Infallible>(response) }
            });
            tokio::spawn(builder.serve_connection(TokioIo::new(stream), service).into_owned());
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_request_path_is_appended_to_the_upstream_including_its_own_path() {
        let at = |up: &str, pq: &str| upstream_url(&up.parse().unwrap(), pq).unwrap().to_string();
        assert_eq!(at("https://api.example.com", "/v1/messages?beta=1"), "https://api.example.com/v1/messages?beta=1");
        assert_eq!(
            at("https://gw.example.com/anthropic", "/v1/messages"),
            "https://gw.example.com/anthropic/v1/messages"
        );
        assert_eq!(
            at("https://gw.example.com/anthropic/", "/v1/messages"),
            "https://gw.example.com/anthropic/v1/messages"
        );
        assert_eq!(at("http://127.0.0.1:8080", "/"), "http://127.0.0.1:8080/");
    }

    #[test]
    fn gzip_is_decoded_within_the_limit_and_other_encodings_are_kept() {
        let gz = |b: &[u8]| {
            let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            e.write_all(b).unwrap();
            Bytes::from(e.finish().unwrap())
        };
        let with = |enc: &str| {
            let mut h = HeaderMap::new();
            h.insert(header::CONTENT_ENCODING, enc.parse().unwrap());
            h
        };
        let plain = Bytes::from_static(b"hello");
        assert_eq!(decoded(&HeaderMap::new(), plain.clone(), 5), Ok((plain.clone(), "-".into())));
        assert_eq!(decoded(&with("identity"), plain.clone(), 5), Ok((plain.clone(), "-".into())));
        for enc in ["gzip", "GZIP", "x-gzip"] {
            assert_eq!(decoded(&with(enc), gz(b"hello"), 5), Ok((plain.clone(), "-".into())), "{enc}");
        }
        let two_members = [gz(b"hel"), gz(b"lo")].concat();
        assert_eq!(decoded(&with("gzip"), two_members.into(), 5), Ok((plain.clone(), "-".into())));
        assert_eq!(decoded(&with("gzip"), gz(b"hello!"), 5), Err(StatusCode::PAYLOAD_TOO_LARGE));
        assert_eq!(decoded(&with("gzip"), plain.clone(), 5), Err(StatusCode::BAD_REQUEST), "not gzip");
        assert_eq!(decoded(&with("br"), plain.clone(), 5), Ok((plain, "br".into())));
    }

    #[test]
    fn a_collector_acknowledges_in_the_requests_content_type() {
        let with = |ct: &str| {
            let mut h = HeaderMap::new();
            h.insert(header::CONTENT_TYPE, ct.parse().unwrap());
            h
        };
        let r = ack(&with("application/json"));
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.headers()[header::CONTENT_TYPE], "application/json");
        let r = ack(&with("application/x-protobuf"));
        assert_eq!(r.headers()[header::CONTENT_TYPE], "application/x-protobuf");
        assert!(ack(&HeaderMap::new()).headers().get(header::CONTENT_TYPE).is_none());
    }

    #[test]
    fn a_failed_write_moves_recording_to_a_new_file_and_loses_nothing_after_it() {
        let dir = std::env::temp_dir().join(format!("agentwire-recording-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut r = Recording::open(dir.clone(), 4, 1 << 30).unwrap();
        r.append(b"m", Bytes::from_static(b"before")).unwrap();
        // the disk fills up: /dev/full fails every write with ENOSPC
        let full = File::options().write(true).open("/dev/full").unwrap();
        r.writer = log::Writer::new(BufWriter::new(full), 4).unwrap(); // the header waits in the buffer
        assert!(r.append(b"m", Bytes::from_static(b"lost")).is_err());
        r.append(b"m", Bytes::from_static(b"after")).unwrap();
        assert_eq!((r.recorded, r.failures), (2, 1));
        let mut files: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().path()).collect();
        files.sort();
        let bodies: Vec<Vec<u8>> = files
            .iter()
            .flat_map(|f| {
                let mut reader = log::Reader::open(File::open(f).unwrap(), 1 << 20).unwrap();
                (0..reader.records().len()).map(|i| reader.get(i).unwrap()).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(bodies, [&b"before"[..], b"after"]);
        assert!(r.metrics().contains("agentwire_record_failures_total 1\n"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn files_rotated_within_a_millisecond_get_their_own_names_in_order() {
        let dir = std::env::temp_dir().join(format!("agentwire-rotation-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut r = Recording::open(dir.clone(), 1, 1).unwrap(); // every record fills its file
        for body in ["a", "b", "c"] {
            r.append(b"", Bytes::from(body)).unwrap();
        }
        let mut names: Vec<String> =
            std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
        names.sort();
        let seqs: Vec<&str> = names.iter().map(|n| &n[n.len() - 10..n.len() - 4]).collect();
        assert_eq!(seqs, ["000000", "000001", "000002", "000003"], "{names:?}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn hop_by_hop_headers_are_not_forwarded() {
        let mut h = HeaderMap::new();
        for (k, v) in [
            ("connection", "keep-alive, x-private"),
            ("x-private", "1"),
            ("keep-alive", "timeout=5"),
            ("host", "client.local"),
            ("transfer-encoding", "chunked"),
            ("authorization", "Bearer k"),
            ("content-type", "application/json"),
        ] {
            h.insert(k, v.parse().unwrap());
        }
        strip_hop_by_hop(&mut h);
        let mut left: Vec<&str> = h.keys().map(|k| k.as_str()).collect();
        left.sort();
        assert_eq!(left, ["authorization", "content-type"]);
    }
}
