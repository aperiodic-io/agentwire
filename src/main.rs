//! `deltapack server`: the DPD1 layer as a reverse proxy in front of an HTTP API.
//! `deltapack client`: a local proxy that sends what a tool posts to it as DPD1 deltas.
//! `deltapack collect`: a log sink (OTLP/HTTP, a gateway's HTTP log plugin) that stores every
//! request body in DPK1, and forwards it unchanged when given a backend.
//! `deltapack log`: pack, list and read DPK1 files (what `--record-dir` and `collect` write).

use std::collections::HashSet;
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

use bytes::Bytes;
use clap::{Parser, Subcommand};
use deltapack::client::Client;
use deltapack::log;
use deltapack::server::{Config, Dpd1Layer, InnerBody, Stats};
use deltapack::store::Store;
use deltapack::{ERROR, KEPT};
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
mod show;
mod split;

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
    /// Rebuild DPD1 requests and forward them, byte-exact, to the upstream API.
    Server {
        #[arg(long, env = "DELTAPACK_LISTEN", default_value = "0.0.0.0:8080")]
        listen: SocketAddr,
        /// The API requests are forwarded to, e.g. https://api.anthropic.com
        #[arg(long, env = "DELTAPACK_UPSTREAM")]
        upstream: reqwest::Url,
        /// Bytes of kept bases held in memory, across all tenants.
        #[arg(long, env = "DELTAPACK_STORE_BYTES", default_value_t = 1 << 30)]
        store_bytes: usize,
        /// Seconds a base unused is kept.
        #[arg(long, env = "DELTAPACK_TTL_SECONDS", default_value_t = 1800)]
        ttl_seconds: u64,
        /// The largest body kept or rebuilt.
        #[arg(long, env = "DELTAPACK_MAX_BODY", default_value_t = 64 << 20)]
        max_body: usize,
        /// The headers that identify a tenant (repeat the flag); bases are never shared across them.
        #[arg(long = "scope-header", env = "DELTAPACK_SCOPE_HEADERS", value_delimiter = ',', default_values_t = [String::from("authorization"), String::from("x-api-key")])]
        scope_headers: Vec<String>,
        /// Serve Prometheus metrics at http://<addr>/metrics.
        #[arg(long, env = "DELTAPACK_METRICS")]
        metrics: Option<SocketAddr>,
        /// Store every request body forwarded, with its time, method and path (never its headers),
        /// in a new DPK1 file in this directory; read it with `deltapack log`.
        #[arg(long, env = "DELTAPACK_RECORD_DIR")]
        record_dir: Option<PathBuf>,
        /// Records a new one may delta against: about one per conversation in flight.
        #[arg(long, env = "DELTAPACK_RECORD_WINDOW", default_value_t = 16)]
        record_window: usize,
        /// Start a new DPK1 file once one reaches this size; every file reads on its own.
        #[arg(long, env = "DELTAPACK_ROTATE_BYTES", default_value_t = 1 << 30)]
        rotate_bytes: u64,
        #[command(flatten)]
        retention: Retention,
        #[command(flatten)]
        tenants: Tenants,
        #[command(flatten)]
        keys: Keys,
        #[command(flatten)]
        reads: Reads,
    },
    /// Store every request body posted (OTLP/HTTP traces and logs, a gateway's HTTP log plugin) in
    /// DPK1, decoded (gzip, deflate, br, zstd, snappy); forward each request as received when given a backend. Also accepts
    /// DPD1 deltas from `deltapack client`.
    Collect {
        /// Where to listen (repeat, or comma-separate); each address takes OTLP/HTTP and OTLP/gRPC.
        /// Loopback by default: anyone who can reach it can fill the disk. In a container, give
        /// 0.0.0.0:4318,0.0.0.0:4317 and put it on a private network.
        #[arg(long, env = "DELTAPACK_LISTEN", value_delimiter = ',', default_value = "127.0.0.1:4318,127.0.0.1:4317")]
        listen: Vec<SocketAddr>,
        /// The new DPK1 file goes in this directory; read it with `deltapack log`.
        #[arg(long, env = "DELTAPACK_RECORD_DIR")]
        record_dir: PathBuf,
        /// The backend each request is forwarded to, as received, e.g.
        /// https://cloud.langfuse.com/api/public/otel. Without one, each request is answered 200.
        #[arg(long, env = "DELTAPACK_FORWARD")]
        forward: Option<reqwest::Url>,
        /// Records a new one may delta against: about one per trace or exporter interleaved.
        #[arg(long, env = "DELTAPACK_RECORD_WINDOW", default_value_t = 16)]
        record_window: usize,
        /// Start a new DPK1 file once one reaches this size; every file reads on its own.
        #[arg(long, env = "DELTAPACK_ROTATE_BYTES", default_value_t = 1 << 30)]
        rotate_bytes: u64,
        /// The largest body stored, after decoding.
        #[arg(long, env = "DELTAPACK_MAX_BODY", default_value_t = 64 << 20)]
        max_body: usize,
        /// Split an OTLP export larger than this (decoded) into exports that fit before forwarding
        /// it, for backends with a body limit (New Relic 1 MB, Datadog 15 MiB): by resource, then
        /// scope, then span. The record holds the whole export.
        #[arg(long, env = "DELTAPACK_FORWARD_MAX_BODY", requires = "forward")]
        forward_max_body: Option<usize>,
        #[arg(long, env = "DELTAPACK_METRICS")]
        metrics: Option<SocketAddr>,
        #[command(flatten)]
        retention: Retention,
        #[command(flatten)]
        tenants: Tenants,
        #[command(flatten)]
        keys: Keys,
        #[command(flatten)]
        reads: Reads,
    },
    /// Accept plain requests from a local tool and send them to a `deltapack server` as deltas.
    Client {
        #[arg(long, env = "DELTAPACK_LISTEN", default_value = "127.0.0.1:8787")]
        listen: SocketAddr,
        /// The `deltapack server` (or a DPD1-aware API) to send to.
        #[arg(long, env = "DELTAPACK_UPSTREAM")]
        upstream: reqwest::Url,
        /// Bodies remembered as bases: about one per conversation in flight.
        #[arg(long, env = "DELTAPACK_BASES", default_value_t = 8)]
        bases: usize,
        /// The largest request body accepted from the tool.
        #[arg(long, env = "DELTAPACK_MAX_BODY", default_value_t = 64 << 20)]
        max_body: usize,
        #[arg(long, env = "DELTAPACK_METRICS")]
        metrics: Option<SocketAddr>,
    },
    /// Read and write DPK1 files: bodies stored as deltas against earlier ones.
    Log {
        /// The key sealed files were sealed with (a file of 64 hex digits); `pack` seals with it.
        #[arg(long, env = "DELTAPACK_KEY_FILE", global = true)]
        key_file: Option<PathBuf>,
        #[command(subcommand)]
        cmd: LogCmd,
    },
}

/// What happens to a record directory's closed files. Files are deleted oldest first; the open file
/// is never deleted, nor a file whose `--on-rotate` command is still running. Give each collector
/// its own directory: retention counts every `.dpk` file in it.
#[derive(clap::Args, Clone, Default)]
struct Retention {
    /// Delete the oldest closed files while the directory holds more than this many bytes (checked
    /// at start and at each new file).
    #[arg(long, env = "DELTAPACK_KEEP_BYTES")]
    keep_bytes: Option<u64>,
    /// Delete closed files last written more than this many days ago (checked at start, at each new
    /// file, and hourly).
    #[arg(long, env = "DELTAPACK_KEEP_DAYS")]
    keep_days: Option<u64>,
    /// Run this with `sh -c` once a file is closed, the file's path as $1, e.g.
    /// 'aws s3 mv "$1" s3://bucket/deltapack/'. It should move or delete the file. Commands run one at
    /// a time, in order; wrap a slow one in `timeout`. Files left from an earlier run, and those
    /// whose command failed, get it again at start; retention keeps a file until its command succeeds.
    #[arg(long, env = "DELTAPACK_ON_ROTATE")]
    on_rotate: Option<String>,
}

/// Records kept apart by tenant: a directory each, in the record directory.
#[derive(clap::Args, Clone, Default)]
struct Tenants {
    /// Record each request in the directory of the tenant this header names (letters, digits, `.`,
    /// `_` and `-`, up to 64; `_none` when absent, `_invalid` otherwise), each with its own files,
    /// lock, retention and --on-rotate. Set it at a gateway that authenticates the tenant.
    #[arg(long, env = "DELTAPACK_TENANT_HEADER")]
    tenant_header: Option<HeaderName>,
    /// Tenants given a directory of their own; requests of any more share `_other`.
    #[arg(long, env = "DELTAPACK_MAX_TENANTS", default_value_t = 256)]
    max_tenants: usize,
}

/// Records sealed at rest: a key for all, or one per tenant.
#[derive(clap::Args, Clone, Default)]
struct Keys {
    /// Seal every record with this key: a file of 64 hex digits (`openssl rand -hex 32`). Bodies
    /// are unreadable without it; each record's meta (time, method, path, trace ids) is not sealed.
    #[arg(long, env = "DELTAPACK_KEY_FILE", conflicts_with = "key_dir")]
    key_file: Option<PathBuf>,
    /// With --tenant-header: seal each tenant's records with its own key, DIR/<tenant>.key. Deleting
    /// a tenant's key makes its records unreadable everywhere, copies included. A tenant without a
    /// key is not recorded.
    #[arg(long, env = "DELTAPACK_KEY_DIR", requires = "tenant_header")]
    key_dir: Option<PathBuf>,
}

/// The read API: a trace's spans over HTTP, for a backend to link a span to its full prompt.
#[derive(clap::Args, Clone, Default)]
struct Reads {
    /// Serve GET /v1/traces/{trace id} here: the trace's spans and log records as JSON lines, as
    /// `log show --trace` writes them (`?since=2h`; `?tenant=` with --tenant-header). Off a loopback
    /// address it needs --read-token-file: it serves prompts.
    #[arg(long, env = "DELTAPACK_READ_LISTEN")]
    read_listen: Option<SocketAddr>,
    /// A file holding the bearer token a reader must send (`Authorization: Bearer …`).
    #[arg(long, env = "DELTAPACK_READ_TOKEN_FILE", requires = "read_listen")]
    read_token_file: Option<PathBuf>,
}

/// Whether a key file's mode lets anyone but its owner read it.
fn key_too_open(mode: u32) -> bool {
    mode & 0o077 != 0
}

/// A key file: 64 hex digits. One others can read is used, and warned of.
fn read_key(path: &std::path::Path) -> io::Result<log::Key> {
    use std::os::unix::fs::PermissionsExt;
    if std::fs::metadata(path).is_ok_and(|m| key_too_open(m.permissions().mode())) {
        eprintln!("warning: {} can be read by others than its owner: chmod 600 it", path.display());
    }
    let text =
        std::fs::read_to_string(path).map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))?;
    log::Key::from_hex(&text).map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))
}

#[derive(Subcommand)]
enum LogCmd {
    /// Store each line of stdin (JSON lines: spans, requests) as a record of a new DPK1 file.
    Pack {
        file: PathBuf,
        /// Records a new one may delta against: about one per trace or conversation interleaved.
        #[arg(long, default_value_t = 16)]
        window: usize,
    },
    /// Write every record's body to stdout, one per line: `pack`'s input back.
    Unpack {
        #[arg(required = true)]
        files: Vec<PathBuf>,
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
    /// List the records: index, records back to its base (0: stored whole), bytes stored, meta. Given
    /// more than one file, each line starts with its file and a tab.
    Ls {
        #[arg(required = true)]
        files: Vec<PathBuf>,
    },
    /// Read JSON lines (or any lines) from stdin and say what `pack` would store, without writing a
    /// file: records, their bytes, the bytes packed, the ratio; with --usd-per-gb, the cost of each.
    Estimate {
        /// Records a new one may delta against: about one per trace or conversation interleaved.
        #[arg(long, default_value_t = 16)]
        window: usize,
        /// A storage price, to say what each would cost.
        #[arg(long)]
        usd_per_gb: Option<f64>,
    },
    /// Per file: its records, the bytes they stand for, its bytes on disk and the ratio, then a tab
    /// and its name; then `(total)`, when given more than one file. A file that cannot be read is
    /// named on stderr, and the command fails once the others are counted.
    Stats {
        #[arg(required = true)]
        files: Vec<PathBuf>,
        #[arg(long, default_value_t = 64 << 20)]
        max_body: usize,
    },
    /// Write records as JSON lines: one per span or log record of an OTLP export (JSON or
    /// protobuf), any other record whole; each with its file, index and the time it was received.
    Show {
        #[arg(required = true)]
        files: Vec<PathBuf>,
        /// Only the spans and log records of this trace (hex).
        #[arg(long)]
        trace: Option<String>,
        /// Only records received at or after this time: Unix ms, or a duration ago (90s, 15m, 2h, 7d).
        #[arg(long)]
        since: Option<String>,
        /// Only records received at or before this time, in the same forms.
        #[arg(long)]
        until: Option<String>,
        /// Only spans and log records whose attribute, or resource attribute, KEY has VALUE
        /// (repeat: all of them), e.g. --attr gen_ai.request.model=gpt-4o --attr session.id=s1.
        #[arg(long = "attr", value_name = "KEY=VALUE", value_parser = show::attr)]
        attrs: Vec<(String, String)>,
        #[arg(long, default_value_t = 64 << 20)]
        max_body: usize,
    },
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    if let Mode::Log { key_file, cmd } = cli.mode {
        let key = key_file.as_deref().map(read_key).transpose()?;
        return match run_log(cmd, key.as_ref()) {
            // a reader that stopped reading (`| head`): the rest has nowhere to go, which is no error
            Err(e) if e.downcast_ref::<io::Error>().is_some_and(|e| e.kind() == io::ErrorKind::BrokenPipe) => Ok(()),
            done => done,
        };
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "deltapack=info".into()),
        )
        .init();
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    match cli.mode {
        Mode::Log { .. } => unreachable!("handled above"),
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
            retention,
            tenants,
            keys,
            reads,
        } => {
            let record = match record_dir {
                Some(dir) => Some(Recorders::start(dir, record_window, rotate_bytes, retention, tenants, keys)?),
                None => None,
            };
            start_reads(&reads, record.as_ref(), max_body).await?;
            let store = Store::new(store_bytes, Duration::from_secs(ttl_seconds));
            let scope_headers =
                scope_headers.iter().map(|h| HeaderName::try_from(h.as_str())).collect::<Result<_, _>>()?;
            let forward =
                Forward { http, upstream: Some(Arc::new(upstream)), record, max_body, grpc: false, split: None };
            run_server(vec![listen], store, Config { max_body, scope_headers }, metrics, forward).await
        }
        Mode::Collect {
            listen,
            record_dir,
            forward,
            record_window,
            rotate_bytes,
            max_body,
            forward_max_body,
            metrics,
            retention,
            tenants,
            keys,
            reads,
        } => {
            let record = Some(Recorders::start(record_dir, record_window, rotate_bytes, retention, tenants, keys)?);
            start_reads(&reads, record.as_ref(), max_body).await?;
            let forward = Forward {
                http,
                upstream: forward.map(Arc::new),
                record,
                max_body,
                grpc: true,
                split: forward_max_body,
            };
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

/// The DPD1 layer in front of `forward`, until shutdown.
async fn run_server(
    listen: Vec<SocketAddr>,
    store: Store,
    config: Config,
    metrics: Option<SocketAddr>,
    forward: Forward,
) -> Result<(), BoxError> {
    let store = Arc::new(store);
    let layer = Dpd1Layer::new(store.clone(), config);
    if let Some(addr) = metrics {
        let (stats, store, record) = (layer.stats().clone(), store.clone(), forward.record.clone());
        serve_metrics(addr, move || {
            server_metrics(&stats, &store) + &record.as_ref().map_or_else(String::new, Recorders::metrics)
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

/// Where requests are recorded: the record directory, or with --tenant-header a directory per
/// tenant in it, each recording started at its tenant's first request.
#[derive(Clone)]
struct Recorders {
    root: PathBuf,
    window: usize,
    rotate_bytes: u64,
    retention: Retention,
    tenants: Tenants,
    /// --key-dir, or the key of --key-file
    key_dir: Option<PathBuf>,
    key: Option<log::Key>,
    /// by tenant ("" without --tenant-header)
    open: Arc<Mutex<std::collections::HashMap<String, Slot>>>,
    /// requests that named a tenant whose recording could not be started, or one past the cap
    /// with --key-dir
    unrecorded: Arc<AtomicU64>,
    /// tenants whose start failed, and when: tried again after `retry_after` ([`RETRY_START`]); they
    /// hold no place under --max-tenants
    failed: Arc<Mutex<std::collections::HashMap<String, std::time::Instant>>>,
    retry_after: Duration,
}

/// A tenant's recording, started once by its first request (`None`: its start failed).
type Slot = Arc<std::sync::OnceLock<Option<Recorder>>>;

/// Failed starts remembered at once, at most: past it, a failed tenant is tried again sooner.
const MAX_FAILED: usize = 4096;

/// A tenant whose recording could not be started is tried again after this long.
const RETRY_START: Duration = Duration::from_secs(30);

impl Recorders {
    fn start(
        root: PathBuf,
        window: usize,
        rotate_bytes: u64,
        retention: Retention,
        tenants: Tenants,
        keys: Keys,
    ) -> io::Result<Self> {
        let key = keys.key_file.as_deref().map(read_key).transpose()?;
        let r = Recorders {
            root,
            window,
            rotate_bytes,
            retention,
            tenants,
            key_dir: keys.key_dir,
            key,
            open: Default::default(),
            unrecorded: Default::default(),
            failed: Default::default(),
            retry_after: RETRY_START,
        };
        if r.tenants.tenant_header.is_none() {
            // one recording: started now, so that a directory it cannot use stops the process
            let one = Recording::start(r.root.clone(), window, rotate_bytes, r.retention.clone(), r.key.clone())?;
            lock_open(&r.open).insert(String::new(), Arc::new(Some(one).into()));
        }
        Ok(r)
    }

    /// The recording for a request; none when its tenant's cannot be started (logged once, counted,
    /// tried again after [`RETRY_START`]), or the tenant is past the cap with --key-dir. A tenant's
    /// start holds no lock but its own: other tenants' requests go on meanwhile.
    fn of(&self, headers: &HeaderMap) -> Option<Recorder> {
        let tenant = match &self.tenants.tenant_header {
            None => String::new(),
            Some(name) => tenant_dir(headers.get_all(name)),
        };
        let unrecorded = || {
            self.unrecorded.fetch_add(1, Ordering::Relaxed);
            None
        };
        let (tenant, slot) = {
            let mut open = lock_open(&self.open);
            let tenant = match open.contains_key(&tenant) {
                true => tenant,
                // past the cap: a tenant with a key of its own is not sealed with another's
                false if open.len() >= self.tenants.max_tenants && self.key_dir.is_some() => return unrecorded(),
                false if open.len() >= self.tenants.max_tenants => "_other".to_owned(),
                false => tenant,
            };
            if lock_failed(&self.failed).get(&tenant).is_some_and(|at| at.elapsed() < self.retry_after) {
                return unrecorded();
            }
            let slot = open.entry(tenant.clone()).or_default().clone();
            (tenant, slot)
        };
        // the start holds no lock but its slot's: other tenants' requests go on meanwhile
        let mut tried = false;
        let started = tokio::task::block_in_place(|| {
            slot.get_or_init(|| {
                tried = true;
                self.start_tenant(&tenant)
            })
            .clone()
        });
        match started {
            Some(r) => {
                if tried {
                    lock_failed(&self.failed).remove(&tenant);
                }
                Some(r)
            }
            None => {
                // only the request that tried the start forgets its slot and remembers when, both
                // under the map's lock: no request sees the tenant in neither (and starts it again
                // at once), and none that waited on the slot marks a tenant started since as failed
                if tried {
                    let mut open = lock_open(&self.open);
                    let mut failed = lock_failed(&self.failed);
                    failed.retain(|_, at| at.elapsed() < self.retry_after);
                    if failed.len() < MAX_FAILED {
                        failed.insert(tenant.clone(), std::time::Instant::now());
                    }
                    // still this slot: only a failed start's own request removes one
                    open.remove(&tenant);
                }
                unrecorded()
            }
        }
    }

    /// A tenant's recording, started: with its own key (--key-dir), or the one for all.
    fn start_tenant(&self, tenant: &str) -> Option<Recorder> {
        let key = match &self.key_dir {
            Some(dir) => read_key(&dir.join(format!("{tenant}.key"))).map(Some),
            None => Ok(self.key.clone()),
        };
        let dir = self.root.join(tenant);
        match key.and_then(|key| Recording::start(dir, self.window, self.rotate_bytes, self.retention.clone(), key)) {
            Ok(r) => Some(r),
            Err(e) => {
                tracing::error!(tenant, error = %e, "cannot record for this tenant; trying again in 30 s");
                None
            }
        }
    }

    /// Every recording's counters, summed.
    fn metrics(&self) -> String {
        let mut sum = [0; 5];
        // the recordings, taken out of the map so that routing waits for none of them
        let started: Vec<Recorder> =
            lock_open(&self.open).values().filter_map(|s| s.get().cloned().flatten()).collect();
        for r in &started {
            for (s, n) in sum.iter_mut().zip(lock(r).counters()) {
                *s += n;
            }
        }
        let names =
            ["recorded", "recorded_body_bytes", "recorded_file_bytes", "record_failures", "rotate_command_failures"];
        names
            .iter()
            .zip(sum)
            .chain(std::iter::once((&"unrecorded", self.unrecorded.load(Ordering::Relaxed))))
            .map(|(name, n)| format!("# TYPE deltapack_{name}_total counter\ndeltapack_{name}_total {n}\n"))
            .collect()
    }
}

fn lock_failed(
    m: &Mutex<std::collections::HashMap<String, std::time::Instant>>,
) -> std::sync::MutexGuard<'_, std::collections::HashMap<String, std::time::Instant>> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

fn lock_open(
    m: &Mutex<std::collections::HashMap<String, Slot>>,
) -> std::sync::MutexGuard<'_, std::collections::HashMap<String, Slot>> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// A tenant's directory name, from its header, kept only if it cannot name another place, a
/// reserved name, or (on a case-insensitive file system) another tenant's: lowercase letters,
/// digits, `.`, `_`, `-`; up to 64; not starting with `.` or `_`. `_none` without the header, `_invalid`
/// otherwise, and when it is given more than once (a gateway that appends instead of replacing it).
fn tenant_dir(values: http::header::GetAll<'_, http::HeaderValue>) -> String {
    let mut it = values.iter();
    let v = match (it.next(), it.next()) {
        (None, _) => return "_none".into(),
        (Some(v), None) => v,
        (Some(_), Some(_)) => return "_invalid".into(),
    };
    let ok = |b: &u8| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(b);
    match v.as_bytes() {
        t if !t.is_empty() && t.len() <= 64 && !b"._".contains(&t[0]) && t.iter().all(ok) => {
            String::from_utf8_lossy(t).into_owned()
        }
        _ => "_invalid".into(),
    }
}

fn lock(r: &Recorder) -> std::sync::MutexGuard<'_, Recording> {
    // every append completes, or fails and moves to a new file, under the lock
    r.lock().unwrap_or_else(|p| p.into_inner())
}

/// DPK1 files in `dir`, named by when each was opened, so a restart never appends to or replaces
/// one, and in order by name. Each is self-contained: a new one starts without bases.
struct Recording {
    dir: PathBuf,
    /// what each file's records are sealed with, if anything
    key: Option<log::Key>,
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
    retention: Retention,
    /// closed files retention leaves alone: their `--on-rotate` command is waiting, running, or
    /// failed (it runs again at the next start)
    held: Arc<Mutex<HashSet<PathBuf>>>,
    /// files for the `--on-rotate` command, run one at a time in order
    commands: Option<std::sync::mpsc::Sender<PathBuf>>,
    command_failures: Arc<AtomicU64>,
    /// held while recording: one process per directory
    _lock: File,
}

impl Recording {
    /// Opens a new file in `dir`; hands the files already there to `--on-rotate`, and applies
    /// retention now, at each new file, and hourly when it keeps by age.
    fn start(
        dir: PathBuf,
        window: usize,
        rotate_bytes: u64,
        retention: Retention,
        key: Option<log::Key>,
    ) -> io::Result<Recorder> {
        std::fs::create_dir_all(&dir)?;
        // the directory itself is locked (flock), so it holds nothing but its files
        let dir_lock = File::open(&dir)?;
        match dir_lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                let e = format!("{} is recorded to by another process: give each its own directory", dir.display());
                return Err(io::Error::other(e));
            }
            // a file system that cannot lock a directory (some network ones): record unguarded
            Err(std::fs::TryLockError::Error(e)) => {
                tracing::warn!(dir = %dir.display(), error = %e, "cannot lock the record directory; let one process record to it")
            }
        }
        let earlier = dpk_files(&dir)?;
        let (path, writer) = Self::create(&dir, window, 0, key.as_ref())?;
        let by_age = retention.keep_days.is_some();
        let (held, command_failures) = (Arc::<Mutex<HashSet<PathBuf>>>::default(), Arc::<AtomicU64>::default());
        let commands = retention.on_rotate.clone().map(|command| {
            let (tx, rx) = std::sync::mpsc::channel::<PathBuf>();
            let (held, failures) = (held.clone(), command_failures.clone());
            std::thread::spawn(move || {
                for file in rx {
                    let status = std::process::Command::new("sh")
                        .arg("-c")
                        .arg(&command)
                        .arg("deltapack-on-rotate")
                        .arg(&file)
                        .stdin(std::process::Stdio::null())
                        .status();
                    if status.as_ref().is_ok_and(|s| s.success()) {
                        lock_set(&held).remove(&file);
                    } else {
                        tracing::warn!(path = %file.display(), ?status, "the --on-rotate command failed; the file is kept");
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            tx
        });
        let r = Recording {
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
            retention,
            held,
            commands,
            command_failures,
            _lock: dir_lock,
            key,
        };
        for (file, ..) in earlier {
            r.hand_over(file);
        }
        r.prune();
        let r = Arc::new(Mutex::new(r));
        if by_age {
            let r = r.clone();
            std::thread::spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_secs(3600));
                    lock(&r).prune();
                }
            });
        }
        Ok(r)
    }

    /// Queues a closed file for `--on-rotate`; retention holds it until its command succeeds.
    fn hand_over(&self, file: PathBuf) {
        let Some(commands) = &self.commands else { return };
        lock_set(&self.held).insert(file.clone());
        // the worker only stops with the process
        let _ = commands.send(file);
    }

    /// Deletes what `--keep-bytes` and `--keep-days` say is past keeping.
    fn prune(&self) {
        let Retention { keep_bytes, keep_days, .. } = self.retention;
        if keep_bytes.is_none() && keep_days.is_none() {
            return;
        }
        let files = match dpk_files(&self.dir) {
            Ok(files) => files,
            Err(e) => return tracing::warn!(error = %e, "retention could not list the record directory"),
        };
        let total = files.iter().map(|f| f.1).sum();
        let held = lock_set(&self.held);
        let closed: Vec<_> = files.into_iter().filter(|f| f.0 != self.path && !held.contains(&f.0)).collect();
        drop(held);
        let cutoff = cutoff(keep_days, std::time::SystemTime::now());
        for file in delete_expired(&closed, total, keep_bytes, cutoff, |f| std::fs::remove_file(f)) {
            tracing::info!(path = %file.display(), "deleted by retention");
        }
    }

    fn create(
        dir: &std::path::Path,
        window: usize,
        seq: u64,
        key: Option<&log::Key>,
    ) -> io::Result<(PathBuf, log::Writer<BufWriter<File>>)> {
        let path = dir.join(format!("{}-{}-{seq:06}.dpk", unix_ms(), std::process::id()));
        let file = File::options().write(true).create_new(true).open(&path)?;
        tracing::info!(path = %path.display(), "recording");
        let mut w = match key {
            Some(key) => log::Writer::new_sealed(BufWriter::new(file), window, key)?,
            None => log::Writer::new(BufWriter::new(file), window)?,
        };
        w.flush()?; // a valid, empty DPK1 file before the first record
        Ok((path, w))
    }

    fn next_file(&mut self) -> io::Result<()> {
        // first, so that a full disk is freed before the new file needs room (the open file is kept)
        self.prune();
        let len = std::fs::metadata(&self.path).map_or(0, |m| m.len());
        let (path, writer) = Self::create(&self.dir, self.window, self.seq + 1, self.key.as_ref())?;
        let old = std::mem::replace(&mut self.path, path);
        (self.closed, self.seq, self.writer) = (self.closed + len, self.seq + 1, writer);
        self.hand_over(old);
        self.prune();
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

    /// Records, their bytes, bytes of the files written, failed writes, failed --on-rotate commands.
    fn counters(&self) -> [u64; 5] {
        let on_disk = self.closed + std::fs::metadata(&self.path).map_or(0, |m| m.len());
        [self.recorded, self.body_bytes, on_disk, self.failures, self.command_failures.load(Ordering::Relaxed)]
    }
}

fn lock_set(s: &Mutex<HashSet<PathBuf>>) -> std::sync::MutexGuard<'_, HashSet<PathBuf>> {
    s.lock().unwrap_or_else(|p| p.into_inner())
}

/// The `.dpk` files in `dir`, oldest first (their names start with the time each was opened), with
/// their bytes and when each was last written.
fn dpk_files(dir: &std::path::Path) -> io::Result<Vec<(PathBuf, u64, std::time::SystemTime)>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "dpk") {
            let m = match std::fs::metadata(&path) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue, // moved or deleted meanwhile
                m => m?,
            };
            files.push((path, m.len(), m.modified()?));
        }
    }
    files.sort();
    Ok(files)
}

/// Deletes from `closed` (oldest first) the oldest while the directory's `total` bytes exceed
/// `keep_bytes`, and any last written before `cutoff`; returns those deleted. A file `delete` fails
/// on frees nothing, and is logged.
fn delete_expired(
    closed: &[(PathBuf, u64, std::time::SystemTime)],
    mut total: u64,
    keep_bytes: Option<u64>,
    cutoff: Option<std::time::SystemTime>,
    mut delete: impl FnMut(&std::path::Path) -> io::Result<()>,
) -> Vec<PathBuf> {
    let mut deleted = Vec::new();
    for (path, bytes, written) in closed {
        if keep_bytes.is_some_and(|k| total > k) || cutoff.is_some_and(|c| *written < c) {
            match delete(path) {
                Ok(()) => (total, _) = (total - bytes, deleted.push(path.clone())),
                Err(e) => tracing::warn!(path = %path.display(), error = %e, "retention could not delete"),
            }
        }
    }
    deleted
}

/// The time before which `--keep-days` deletes; none when it reaches past the epoch.
fn cutoff(keep_days: Option<u64>, now: std::time::SystemTime) -> Option<std::time::SystemTime> {
    now.checked_sub(Duration::from_secs(keep_days?.checked_mul(86_400)?))
}

fn unix_ms() -> u128 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis())
}

/// The inner service of `deltapack server` and `collect`: records the request (if recording), then
/// sends it on to the upstream, or answers it (a collector without a backend).
#[derive(Clone)]
struct Forward {
    http: reqwest::Client,
    upstream: Option<Arc<reqwest::Url>>,
    record: Option<Recorders>,
    max_body: usize,
    /// `collect`: take OTLP/gRPC Export calls too
    grpc: bool,
    /// --forward-max-body
    split: Option<usize>,
}

impl tower_service::Service<Request<InnerBody<Incoming>>> for Forward {
    type Response = Response<ResBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response<ResBody>, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut request: Request<InnerBody<Incoming>>) -> Self::Future {
        let (http, upstream, max_body, split) = (self.http.clone(), self.upstream.clone(), self.max_body, self.split);
        // which recording, by the request's tenant; none when its tenant's cannot be started
        let record = self.record.as_ref().map(|r| r.of(request.headers()));
        let grpc = self.grpc && grpc::is_grpc(request.headers());
        // `server` only: a collector's backend takes OTLP, not sessions
        if let (true, false, Some(upstream)) = (websocket::is_upgrade(&request), self.grpc, &upstream) {
            let upgrade = hyper::upgrade::on(&mut request);
            let (parts, _) = request.into_parts();
            let upstream = upstream.clone();
            return Box::pin(async move {
                Ok(websocket::proxy(&upstream, record.flatten(), parts, upgrade, max_body).await)
            });
        }
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            // a tenant that cannot be recorded, and nothing to forward to: refused, so the exporter
            // retries, rather than acknowledged and lost
            if let (Some(None), None) = (&record, &upstream) {
                return Ok(match grpc {
                    true => grpc::error(grpc::Code::Unavailable, "cannot record for this tenant"),
                    false => status(StatusCode::SERVICE_UNAVAILABLE),
                });
            }
            if let (true, Some(record)) = (grpc, &record) {
                return Ok(export(&http, upstream.as_deref(), record.as_ref(), parts, body, max_body, split).await);
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
            let pieces = match (split, show::signal(parts.uri.path())) {
                // a body still in a coding deltapack cannot decode is forwarded as it came
                (Some(max), Some(signal)) if encoding == "-" => split::split(&stored, signal, max),
                _ => None,
            };
            if let Some(record) = &record {
                store(record, &parts, &encoding, stored).await;
            }
            Ok(match (upstream, pieces) {
                (Some(upstream), Some(pieces)) => forward_pieces(&http, upstream, parts, pieces).await,
                (Some(upstream), None) => forward(&http, upstream, &parts, body).await,
                (None, _) => ack(&parts.headers),
            })
        })
    }
}

/// `server`'s WebSocket sessions (realtime and voice APIs): the upgrade is proxied to the upstream,
/// and each text or binary message, either way, is stored as a record of its own as it passes. Its
/// meta is the time, `WS>` (to the upstream) or `WS<` (from it), the session's path, `text` or
/// `binary`, and `-` (stored as it came). Pings, pongs and the close are passed on, not stored.
mod websocket {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::{self, Message, handshake::derive_accept_key, protocol::WebSocketConfig};

    /// The longest the upstream may take to answer a handshake.
    const HANDSHAKE: Duration = Duration::from_secs(30);
    /// How often the tool is pinged; a tool silent for [`IDLE`] (no message, no pong) has gone, and
    /// its session (and the upstream's, perhaps billed) is ended.
    const PING: Duration = Duration::from_secs(30);
    const IDLE: Duration = Duration::from_secs(75);
    /// Longer than any session: a timer that does not fire.
    const NEVER: Duration = Duration::from_secs(1 << 30);

    /// A WebSocket handshake (RFC 6455 §4.1): a GET asking to upgrade to `websocket`.
    pub(super) fn is_upgrade<B>(request: &Request<B>) -> bool {
        let has = |name, token: &str| {
            request
                .headers()
                .get_all(name)
                .iter()
                .filter_map(|v| v.to_str().ok())
                .flat_map(|v| v.split(','))
                .any(|t| t.trim().eq_ignore_ascii_case(token))
        };
        request.method() == http::Method::GET && has(header::UPGRADE, "websocket") && has(header::CONNECTION, "upgrade")
    }

    /// The upstream's answer to the handshake, given to the tool: 101 and the session pumped both
    /// ways, or the upstream's refusal (its status), or 502 when it cannot be reached.
    pub(super) async fn proxy(
        upstream: &reqwest::Url,
        record: Option<Recorder>,
        parts: http::request::Parts,
        upgrade: hyper::upgrade::OnUpgrade,
        max_body: usize,
    ) -> Response<ResBody> {
        let path = parts.uri.path_and_query().map_or("/", |p| p.as_str()).to_owned();
        let Some(key) = parts.headers.get(header::SEC_WEBSOCKET_KEY).cloned() else {
            return status(StatusCode::BAD_REQUEST);
        };
        let Ok(mut url) = upstream_url(upstream, &path) else { return status(StatusCode::BAD_REQUEST) };
        if url.set_scheme(if url.scheme() == "https" { "wss" } else { "ws" }).is_err() {
            return status(StatusCode::BAD_GATEWAY);
        }
        let Ok(mut to_upstream) = tungstenite::client::IntoClientRequest::into_client_request(url.as_str()) else {
            return status(StatusCode::BAD_GATEWAY);
        };
        // the tool's own headers (authorization, subprotocols, ...); the handshake's are the
        // upstream connection's own, and no extension is offered (none is spoken here)
        let mut headers = parts.headers.clone();
        strip_hop_by_hop(&mut headers);
        for name in [
            header::SEC_WEBSOCKET_KEY,
            header::SEC_WEBSOCKET_VERSION,
            header::SEC_WEBSOCKET_EXTENSIONS,
            header::CONTENT_LENGTH,
        ] {
            headers.remove(name);
        }
        for (name, value) in &headers {
            to_upstream.headers_mut().append(name, value.clone());
        }
        let config = WebSocketConfig::default().max_message_size(Some(max_body)).max_frame_size(Some(max_body));
        let connect = tokio_tungstenite::connect_async_with_config(to_upstream, Some(config), false);
        let Ok(connected) = tokio::time::timeout(HANDSHAKE, connect).await else {
            tracing::warn!("upstream WebSocket handshake timed out");
            return status(StatusCode::GATEWAY_TIMEOUT);
        };
        let (upstream_ws, answer) = match connected {
            Ok(c) => c,
            Err(tungstenite::Error::Http(refused)) => {
                let code = refused.status();
                return status(if code.is_client_error() || code.is_server_error() {
                    code
                } else {
                    StatusCode::BAD_GATEWAY
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "upstream WebSocket handshake failed");
                return status(StatusCode::BAD_GATEWAY);
            }
        };
        tokio::spawn(async move {
            let tool = match upgrade.await {
                Ok(io) => {
                    let io = hyper_util::rt::TokioIo::new(io);
                    tokio_tungstenite::WebSocketStream::from_raw_socket(
                        io,
                        tungstenite::protocol::Role::Server,
                        Some(config),
                    )
                    .await
                }
                Err(e) => return tracing::warn!(error = %e, "the tool's WebSocket upgrade failed"),
            };
            let (to_tool, from_tool) = tool.split();
            let (to_up, from_up) = upstream_ws.split();
            let mut sent =
                tokio::spawn(pump(from_tool, to_up, record.clone(), "WS>", path.clone(), Keepalive::Idle(IDLE)));
            let mut answered = tokio::spawn(pump(from_up, to_tool, record, "WS<", path, Keepalive::Ping(PING)));
            // a side that ends lets the other finish its close a moment later, then ends it too
            let grace = Duration::from_secs(5);
            tokio::select! {
                _ = &mut sent => { let _ = tokio::time::timeout(grace, &mut answered).await; answered.abort(); }
                _ = &mut answered => { let _ = tokio::time::timeout(grace, &mut sent).await; sent.abort(); }
            }
        });
        let mut response = Response::new(ResBody::default());
        *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        let h = response.headers_mut();
        h.insert(header::UPGRADE, http::HeaderValue::from_static("websocket"));
        h.insert(header::CONNECTION, http::HeaderValue::from_static("upgrade"));
        if let Ok(accept) = http::HeaderValue::from_str(&derive_accept_key(key.as_bytes())) {
            h.insert(header::SEC_WEBSOCKET_ACCEPT, accept);
        }
        if let Some(protocol) = answer.headers().get(header::SEC_WEBSOCKET_PROTOCOL) {
            h.insert(header::SEC_WEBSOCKET_PROTOCOL, protocol.clone());
        }
        response
    }

    /// How a pump knows its tool is still there: it ends when the tool has said nothing for so
    /// long, or it pings the tool so often (the other direction's pump hears the pong).
    #[derive(Clone, Copy)]
    pub(super) enum Keepalive {
        Idle(Duration),
        Ping(Duration),
    }

    /// One direction of a session: each message stored (text and binary), then passed on.
    pub(super) async fn pump<S, T>(
        mut from: S,
        mut to: T,
        record: Option<Recorder>,
        direction: &'static str,
        path: String,
        keepalive: Keepalive,
    ) where
        S: futures_util::Stream<Item = Result<Message, tungstenite::Error>> + Unpin,
        T: futures_util::Sink<Message> + Unpin,
    {
        let (idle, every) = match keepalive {
            Keepalive::Idle(d) => (d, NEVER),
            Keepalive::Ping(d) => (NEVER, d),
        };
        let mut ping = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        loop {
            let next = tokio::select! {
                next = tokio::time::timeout(idle, from.next()) => next,
                _ = ping.tick() => {
                    if to.send(Message::Ping(Bytes::new())).await.is_err() {
                        break;
                    }
                    continue;
                }
            };
            // silent too long, ended, or broken
            let Ok(Some(Ok(m))) = next else { break };
            let (kind, body) = match &m {
                Message::Text(t) => ("text", Bytes::copy_from_slice(t.as_bytes())),
                Message::Binary(b) => ("binary", b.clone()),
                // answered by the side that received it
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
                Message::Close(_) => {
                    let _ = to.send(m).await;
                    break;
                }
            };
            if let Some(record) = &record {
                let meta = format!("{} {direction} {path} {kind} -", unix_ms());
                let record = record.clone();
                if let Err(e) = tokio::task::spawn_blocking(move || lock(&record).append(meta.as_bytes(), body)).await {
                    tracing::error!(error = %e, "recording panicked; passing the message on unrecorded");
                }
            }
            if to.send(m).await.is_err() {
                break;
            }
        }
        let _ = to.close().await;
    }
}

/// Stores one record, flushed, before the request goes on: what the upstream saw is on disk. Its
/// meta is the time, method, path, content type and the encoding `body` is still in; for an OTLP
/// trace or log export, then its trace ids (`show::trace_ids`).
// ponytail: one writer behind a lock; a writer per shard if recording limits throughput
async fn store(record: &Recorder, parts: &http::request::Parts, encoding: &str, body: Bytes) {
    let path = parts.uri.path_and_query().map_or("/", |p| p.as_str());
    let ct = parts.headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).map(|v| v.replace(' ', ""));
    let mut meta = format!("{} {} {path} {} {encoding}", unix_ms(), parts.method, ct.as_deref().unwrap_or("-"));
    let signal = show::signal(path);
    let encoding = encoding.to_owned();
    let record = record.clone();
    // a failed write is logged and counted where it happens; it never stops the request
    let append = move || {
        // an OTLP batch's trace ids, so `log show --trace` skips the batches of other traces
        if let Some(signal) = signal {
            // a body still in a coding deltapack cannot decode is not read: its ids are unknown
            let ids = if encoding == "-" { show::trace_ids(signal, &body) } else { "?".into() };
            meta = format!("{meta} {ids}");
        }
        lock(&record).append(meta.as_bytes(), body)
    };
    if let Err(e) = tokio::task::spawn_blocking(append).await {
        tracing::error!(error = %e, "recording panicked; forwarding the request unrecorded");
    }
}

/// One OTLP/gRPC Export call: its message is stored (decompressed), then sent on to the backend as
/// OTLP/HTTP protobuf, whose answer comes back as the gRPC status; or acknowledged, without one.
async fn export(
    http: &reqwest::Client,
    upstream: Option<&reqwest::Url>,
    record: Option<&Recorder>,
    mut parts: http::request::Parts,
    body: InnerBody<Incoming>,
    max_body: usize,
    split: Option<usize>,
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
        match grpc_decoded(&parts.headers, message, max_body) {
            Ok(m) => m,
            Err((code, why)) => return grpc::error(code, why),
        }
    } else {
        body.slice_ref(message)
    };
    if let Some(record) = record {
        store(record, &parts, "-", message.clone()).await;
    }
    let Some(upstream) = upstream else { return grpc::ok(b"") };
    // OTLP/HTTP: the same message, as protobuf, at the signal's path; the request's other headers
    // (authorization, user-agent) go along
    parts.uri = http::Uri::from_static(otlp_path);
    parts.headers.insert(header::CONTENT_TYPE, http::HeaderValue::from_static("application/x-protobuf"));
    for name in ["grpc-encoding", "grpc-accept-encoding", "grpc-timeout", "content-length"] {
        parts.headers.remove(name);
    }
    let pieces = match (split, show::signal(otlp_path)) {
        (Some(max), Some(signal)) => split::split(&message, signal, max),
        _ => None,
    };
    let response = match pieces {
        Some(pieces) => forward_pieces(http, upstream, parts, pieces).await,
        None => forward(http, upstream, &parts, message).await,
    };
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

fn run_log(cmd: LogCmd, key: Option<&log::Key>) -> Result<(), BoxError> {
    let mut out = BufWriter::new(io::stdout().lock());
    match cmd {
        LogCmd::Pack { file, window } => {
            let file = File::options().write(true).create_new(true).open(file)?;
            let mut w = match key {
                Some(key) => log::Writer::new_sealed(BufWriter::new(file), window, key)?,
                None => log::Writer::new(BufWriter::new(file), window)?,
            };
            for line in io::stdin().lock().split(b'\n') {
                w.append(b"", Bytes::from(line?))?;
            }
            w.flush()?;
        }
        LogCmd::Unpack { files, max_body } => {
            for file in &files {
                open_log(file, max_body, key)?.scan(|_, body| {
                    out.write_all(body)?;
                    out.write_all(b"\n")
                })?;
            }
        }
        LogCmd::Estimate { window, usd_per_gb } => {
            let mut w = log::Writer::new(Counted(0), window)?;
            let (mut records, mut body) = (0u64, 0u64);
            for line in io::stdin().lock().split(b'\n') {
                let line = line?;
                (records, body) = (records + 1, body + line.len() as u64);
                w.append(b"", Bytes::from(line))?;
            }
            w.flush()?;
            let packed = w.into_inner().0;
            write!(out, "{records} records, {body} bytes, packed in {packed} ({:.1}x)", body as f64 / packed as f64)?;
            if let Some(usd) = usd_per_gb {
                let cost = |b: u64| b as f64 / 1e9 * usd;
                write!(out, "; at ${usd} per GB: ${:.6} as is, ${:.6} packed", cost(body), cost(packed))?;
            }
            writeln!(out)?;
        }
        LogCmd::Get { file, index, max_body } => out.write_all(&open_log(&file, max_body, key)?.get(index)?)?,
        LogCmd::Show { files, trace, since, until, attrs, max_body } => {
            let now = unix_ms() as u64;
            let filter = show::Filter {
                trace: trace.map(|t| t.to_ascii_lowercase()),
                since: since.map(|s| show::when(&s, now)).transpose()?,
                until: until.map(|s| show::when(&s, now)).transpose()?,
                attrs,
            };
            show_files(&files, &filter, max_body, key, &mut out)?;
        }
        LogCmd::Stats { files, max_body } => {
            let (mut total, mut unread) = ((0, 0, 0), 0);
            for file in &files {
                // a file being written, deleted by retention or not DPK1 is named and skipped
                let counted = std::fs::metadata(file).and_then(|m| {
                    let (mut records, mut body) = (0, 0);
                    open_log(file, max_body, key)?.scan(|_, b| {
                        (records, body) = (records + 1, body + b.len() as u64);
                        Ok(())
                    })?;
                    Ok((records, body, m.len()))
                });
                match counted {
                    Ok((records, body, disk)) => {
                        writeln!(out, "{}", stats_line(&file.display().to_string(), records, body, disk))?;
                        total = (total.0 + records, total.1 + body, total.2 + disk);
                    }
                    Err(e) => (unread, _) = (unread + 1, eprintln!("{}: {e}", file.display())),
                }
            }
            if files.len() > 1 {
                writeln!(out, "{}", stats_line("(total)", total.0, total.1, total.2))?;
            }
            if unread > 0 {
                out.flush()?;
                return Err(format!("{unread} of {} files could not be read", files.len()).into());
            }
        }
        LogCmd::Ls { files } => {
            for file in &files {
                let lead = if files.len() > 1 { format!("{}\t", file.display()) } else { String::new() };
                for (i, r) in open_log(file, 0, key)?.records().iter().enumerate() {
                    writeln!(out, "{lead}{i} {} {} {}", r.back, r.stored, String::from_utf8_lossy(&r.meta))?;
                }
            }
        }
    }
    out.flush()?;
    Ok(())
}

fn stats_line(name: &str, records: u64, body: u64, disk: u64) -> String {
    // the name last, after a tab: it may hold spaces
    format!("{records} records, {body} bytes in {disk} on disk ({:.1}x)\t{name}", body as f64 / disk.max(1) as f64)
}

/// The rows of `files` that `filter` keeps, one JSON line each, with their file, record and time
/// received; how many.
fn show_files(
    files: &[PathBuf],
    filter: &show::Filter,
    max_body: usize,
    key: Option<&log::Key>,
    out: &mut impl Write,
) -> io::Result<u64> {
    let mut shown = 0;
    for file in files {
        let name = serde_json::Value::from(file.display().to_string());
        let mut index = 0;
        // ponytail: every record is rebuilt to reach the ones wanted; `get` per indexed match
        // when files outgrow a scan
        open_log(file, max_body, key)?.scan(|record, body| {
            let meta = show::Meta::parse(&record.meta);
            if filter.wants(&meta) {
                let otlp = meta.decoded().then(|| show::rows(body, meta.path.and_then(show::signal)));
                let rows = otlp.flatten().unwrap_or_else(|| vec![show::whole(body)]);
                for mut row in rows.into_iter().filter(|r| filter.keeps(r)) {
                    row["file"] = name.clone();
                    row["record"] = index.into();
                    row["received"] = meta.time.into();
                    serde_json::to_writer(&mut *out, &row)?;
                    out.write_all(b"\n")?;
                    shown += 1;
                }
            }
            index += 1;
            Ok(())
        })?;
    }
    Ok(shown)
}

/// Counts what is written to it, and keeps none of it.
struct Counted(u64);

impl Write for Counted {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn open_log(path: &std::path::Path, max_body: usize, key: Option<&log::Key>) -> io::Result<log::Reader<File>> {
    let mut r = log::Reader::open(File::open(path)?, max_body)?;
    // given a key, a file that is not sealed is refused: it could have replaced a sealed one
    if let Some(key) = key {
        r = r.with_key(key).map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))?;
    }
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

/// An export split by --forward-max-body, sent one piece after another (in order), decoded: the
/// first answer that is not a success, or the last. A piece refused after others were taken fails
/// the export, and the exporter's retry sends those again: a backend may then hold them twice.
async fn forward_pieces(
    http: &reqwest::Client,
    upstream: &reqwest::Url,
    mut parts: http::request::Parts,
    pieces: Vec<Vec<u8>>,
) -> Response<ResBody> {
    parts.headers.remove(header::CONTENT_ENCODING);
    parts.headers.remove(header::CONTENT_LENGTH);
    let mut last = status(StatusCode::OK);
    for piece in pieces {
        last = forward(http, upstream, &parts, piece).await;
        if !last.status().is_success() {
            break;
        }
    }
    last
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

/// The body as stored and delta'd: decoded from every content coding it names (gzip, deflate, br,
/// zstd, snappy), at most `max` bytes, with "-" for its encoding. A coding none of these is kept as
/// sent, and named.
fn decoded(headers: &HeaderMap, body: Bytes, max: usize) -> Result<(Bytes, String), StatusCode> {
    let encoding = headers.get(header::CONTENT_ENCODING).and_then(|v| v.to_str().ok()).unwrap_or("").trim();
    let mut out = body.clone();
    // codings are listed in the order they were applied
    for coding in encoding.rsplit(',').map(str::trim) {
        if coding.is_empty() || coding.eq_ignore_ascii_case("identity") {
            continue;
        }
        match decompress(coding, &out, max) {
            Some(decoded) => out = decoded?,
            None => return Ok((body, encoding.replace(' ', ""))),
        }
    }
    Ok((out, "-".to_owned()))
}

/// A compressed gRPC message, decoded by the request's `grpc-encoding`.
fn grpc_decoded(headers: &HeaderMap, message: &[u8], max: usize) -> Result<Bytes, (grpc::Code, &'static str)> {
    let coding = headers.get("grpc-encoding").and_then(|v| v.to_str().ok()).unwrap_or("");
    match decompress(coding, message, max) {
        None => Err((grpc::Code::Unimplemented, "unsupported grpc-encoding")),
        Some(Ok(m)) => Ok(m),
        Some(Err(StatusCode::PAYLOAD_TOO_LARGE)) => {
            Err((grpc::Code::ResourceExhausted, "the export exceeds the size limit"))
        }
        Some(Err(_)) => Err((grpc::Code::InvalidArgument, "the message does not decode")),
    }
}

/// `data` decoded from one coding, to at most `max` bytes: 413 past it, 400 for what does not decode;
/// `None` for a coding not known here.
fn decompress(coding: &str, data: &[u8], max: usize) -> Option<Result<Bytes, StatusCode>> {
    Some(match coding.to_ascii_lowercase().as_str() {
        "gzip" | "x-gzip" => bounded(flate2::read::MultiGzDecoder::new(data), max), // one member or several
        // RFC 9110's deflate is zlib; some senders send the bare deflate stream
        "deflate" => bounded(flate2::read::ZlibDecoder::new(data), max).or_else(|code| match code {
            StatusCode::BAD_REQUEST => bounded(flate2::read::DeflateDecoder::new(data), max),
            code => Err(code),
        }),
        "br" => bounded(brotli_decompressor::Decompressor::new(data, 4096), max),
        "zstd" => unzstd(data, max),
        // the framing format opens with its stream identifier; without it, `snappy` is one raw block
        "x-snappy-framed" => bounded(snap::read::FrameDecoder::new(data), max),
        "snappy" if data.starts_with(b"\xff\x06\x00\x00sNaPpY") => bounded(snap::read::FrameDecoder::new(data), max),
        "snappy" => match snap::raw::decompress_len(data) {
            Ok(n) if n > max => Err(StatusCode::PAYLOAD_TOO_LARGE),
            Ok(_) => {
                snap::raw::Decoder::new().decompress_vec(data).map(Bytes::from).map_err(|_| StatusCode::BAD_REQUEST)
            }
            Err(_) => Err(StatusCode::BAD_REQUEST),
        },
        _ => return None,
    })
}

/// Reads a decoder to its end, but never past `max` bytes.
fn bounded(decoder: impl Read, max: usize) -> Result<Bytes, StatusCode> {
    let mut out = Vec::new();
    decoder.take(max as u64 + 1).read_to_end(&mut out).map_err(|_| StatusCode::BAD_REQUEST)?;
    if out.len() > max {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    Ok(Bytes::from(out))
}

/// Zstandard frames, one or several, whether or not they state their size, in a window no larger
/// than DPD1's own (2^27).
fn unzstd(data: &[u8], max: usize) -> Result<Bytes, StatusCode> {
    let mut dctx = zstd_safe::DCtx::create();
    dctx.set_parameter(zstd_safe::DParameter::WindowLogMax(27)).map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut input = zstd_safe::InBuffer::around(data);
    let mut out = Vec::new();
    loop {
        // one block at a time: at most a block past `max` before it is refused
        out.reserve(zstd_safe::DCtx::out_size());
        let pos = out.len();
        let (hint, full) = {
            let mut buf = zstd_safe::OutBuffer::around_pos(&mut out, pos);
            let hint = dctx.decompress_stream(&mut buf, &mut input).map_err(|_| StatusCode::BAD_REQUEST)?;
            (hint, buf.pos() == buf.capacity())
        };
        if out.len() > max {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
        if input.pos() == data.len() && !full {
            // all read and all flushed: done at a frame's end, cut short inside one
            return if hint == 0 { Ok(Bytes::from(out)) } else { Err(StatusCode::BAD_REQUEST) };
        }
    }
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
        // a delta of compressed bytes finds nothing: decode first. A body in a coding not known here goes as is
        // (`dpd1` must be its only content coding)
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
            "# TYPE deltapack_client_requests_total counter\ndeltapack_client_requests_total {}\n\
             # TYPE deltapack_client_deltas_total counter\ndeltapack_client_deltas_total {}\n\
             # TYPE deltapack_client_resent_total counter\ndeltapack_client_resent_total {}\n\
             # TYPE deltapack_client_body_bytes_total counter\ndeltapack_client_body_bytes_total {}\n\
             # TYPE deltapack_client_wire_bytes_total counter\ndeltapack_client_wire_bytes_total {}\n",
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
        "# TYPE deltapack_server_deltas_total counter\ndeltapack_server_deltas_total {}\n\
         # TYPE deltapack_server_delta_bytes_total counter\ndeltapack_server_delta_bytes_total {}\n\
         # TYPE deltapack_server_rebuilt_bytes_total counter\ndeltapack_server_rebuilt_bytes_total {}\n\
         # TYPE deltapack_server_kept_total counter\ndeltapack_server_kept_total {}\n\
         # TYPE deltapack_server_refused_total counter\ndeltapack_server_refused_total {}\n\
         # TYPE deltapack_server_store_bytes gauge\ndeltapack_server_store_bytes {}\n",
        g(&stats.deltas),
        g(&stats.delta_bytes),
        g(&stats.rebuilt_bytes),
        g(&stats.kept),
        g(&stats.refused),
        store.bytes(),
    )
}

/// A plain-text Prometheus endpoint on its own address, so it never shadows an upstream path.
/// The read API's address is refused off a loopback address without a token: it serves prompts.
fn reads_allowed(addr: SocketAddr, token: Option<&str>) -> Result<(), String> {
    match (addr.ip().is_loopback(), token) {
        (false, None) => Err(format!("--read-listen {addr} serves prompts beyond this host: give --read-token-file")),
        _ => Ok(()),
    }
}

/// Whether `authorization` is `Bearer <token>` (the scheme in any case, RFC 9110 §11.1), compared
/// by digest: what a comparison's time could tell is where two digests differ, which says nothing
/// of the token, nor of its length.
fn authorized(authorization: Option<&http::HeaderValue>, token: Option<&str>) -> bool {
    use sha2::{Digest, Sha256};
    let Some(token) = token else { return true };
    let given = authorization.map_or(&b""[..], |v| v.as_bytes());
    let (scheme, given) = given.split_at(given.len().min(7));
    let (given, want) = (Sha256::digest(given), Sha256::digest(token.as_bytes()));
    scheme.eq_ignore_ascii_case(b"bearer ") & (given == want)
}

/// Reads at once, at most: one more is answered 503 rather than scanning files beside the others.
const MAX_READS: usize = 4;

/// The JSON lines of a read, in memory: at most `left` more bytes.
struct Capped {
    out: Vec<u8>,
    left: usize,
}

impl Write for Capped {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.left = self.left.checked_sub(buf.len()).ok_or(io::ErrorKind::FileTooLarge)?;
        self.out.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// One read: the status and the JSON lines of a trace's rows.
fn read_trace(record: &Recorders, path: &str, query: &str, max_body: usize) -> (StatusCode, Vec<u8>) {
    let Some(id) =
        path.strip_prefix("/v1/traces/").filter(|id| id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()))
    else {
        return (StatusCode::NOT_FOUND, b"GET /v1/traces/{32 hex digits}\n".to_vec());
    };
    let param = |name: &str| query.split('&').find_map(|kv| kv.strip_prefix(name)?.strip_prefix('='));
    let since = match param("since").map(|s| show::when(s, unix_ms() as u64)).transpose() {
        Ok(since) => since,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("{e}\n").into_bytes()),
    };
    let (dir, key) = match (&record.tenants.tenant_header, param("tenant")) {
        (None, _) => (record.root.clone(), record.key.clone()),
        (Some(_), None) => return (StatusCode::BAD_REQUEST, b"give ?tenant=\n".to_vec()),
        (Some(_), Some(t)) => {
            let mut h = HeaderMap::new();
            if let Ok(v) = http::HeaderValue::from_str(t) {
                h.insert(header::HOST, v); // any name: tenant_dir reads the values given
            }
            // the directories of requests without a tenant, and past --max-tenants, by their names
            let tenant = match t {
                "_none" | "_other" => t.to_owned(),
                _ => tenant_dir(h.get_all(header::HOST)),
            };
            let key = match &record.key_dir {
                Some(d) => match read_key(&d.join(format!("{tenant}.key"))) {
                    Ok(k) => Some(k),
                    Err(_) => return (StatusCode::NOT_FOUND, Vec::new()),
                },
                None => record.key.clone(),
            };
            (record.root.join(tenant), key)
        }
    };
    let files: Vec<PathBuf> = dpk_files(&dir).map(|f| f.into_iter().map(|f| f.0).collect()).unwrap_or_default();
    let filter = show::Filter { trace: Some(id.to_ascii_lowercase()), since, ..show::Filter::default() };
    let mut out = Capped { out: Vec::new(), left: max_body };
    match show_files(&files, &filter, max_body, key.as_ref(), &mut out) {
        Ok(0) => (StatusCode::NOT_FOUND, out.out),
        Ok(_) => (StatusCode::OK, out.out),
        Err(e) if e.kind() == io::ErrorKind::FileTooLarge => {
            let why =
                format!("the trace's rows exceed {max_body} bytes (--max-body): read them with `deltapack log show`\n");
            (StatusCode::PAYLOAD_TOO_LARGE, why.into_bytes())
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}\n").into_bytes()),
    }
}

/// The read API, when --read-listen asks for it: it reads what is recorded, so it needs a recording.
async fn start_reads(reads: &Reads, record: Option<&Recorders>, max_body: usize) -> Result<(), BoxError> {
    match (record, reads.read_listen) {
        (Some(r), Some(_)) => serve_reads(reads, r.clone(), max_body).await,
        (None, Some(_)) => Err("--read-listen needs --record-dir".into()),
        (_, None) => Ok(()),
    }
}

/// The read API (`--read-listen`): runs beside the collector until the process ends.
async fn serve_reads(reads: &Reads, record: Recorders, max_body: usize) -> Result<(), BoxError> {
    let addr = reads.read_listen.expect("called with --read-listen");
    let token = reads.read_token_file.as_deref().map(std::fs::read_to_string).transpose()?;
    let token: Option<Arc<str>> = token.map(|t| t.trim().into()).filter(|t: &Arc<str>| !t.is_empty());
    reads_allowed(addr, token.as_deref())?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr = %listener.local_addr()?, "reads");
    let reading = Arc::new(tokio::sync::Semaphore::new(MAX_READS));
    tokio::spawn(async move {
        let mut builder = auto::Builder::new(TokioExecutor::new());
        builder.http1().timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(30));
        while let Ok((stream, _)) = listener.accept().await {
            let (record, token, reading) = (record.clone(), token.clone(), reading.clone());
            let service = hyper::service::service_fn(move |r: Request<Incoming>| {
                let (record, token, reading) = (record.clone(), token.clone(), reading.clone());
                async move {
                    let (status, body) = if !authorized(r.headers().get(header::AUTHORIZATION), token.as_deref()) {
                        (StatusCode::UNAUTHORIZED, Vec::new())
                    } else if r.method() != http::Method::GET {
                        (StatusCode::METHOD_NOT_ALLOWED, Vec::new())
                    } else if let Ok(permit) = reading.try_acquire_owned() {
                        let (path, query) = (r.uri().path().to_owned(), r.uri().query().unwrap_or("").to_owned());
                        let read = move || (read_trace(&record, &path, &query, max_body), drop(permit)).0;
                        tokio::task::spawn_blocking(read)
                            .await
                            .unwrap_or((StatusCode::INTERNAL_SERVER_ERROR, Vec::new()))
                    } else {
                        (StatusCode::SERVICE_UNAVAILABLE, b"too many reads at once; try again\n".to_vec())
                    };
                    let mut response = Response::new(Full::new(Bytes::from(body)));
                    *response.status_mut() = status;
                    response
                        .headers_mut()
                        .insert(header::CONTENT_TYPE, http::HeaderValue::from_static("application/x-ndjson"));
                    Ok::<_, Infallible>(response)
                }
            });
            tokio::spawn(builder.serve_connection(TokioIo::new(stream), service).into_owned());
        }
    });
    Ok(())
}

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
        assert_eq!(decoded(&with("lz4"), plain.clone(), 5), Ok((plain, "lz4".into())));
    }

    /// Every coding an exporter or log shipper may pick, so no batch is stored still compressed
    /// (which leaves a delta nothing to find).
    fn encoded(coding: &str, b: &[u8]) -> Bytes {
        let mut out = Vec::new();
        match coding {
            "deflate" => {
                let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                e.write_all(b).unwrap();
                out = e.finish().unwrap();
            }
            "raw-deflate" => {
                let mut e = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
                e.write_all(b).unwrap();
                out = e.finish().unwrap();
            }
            "br" => {
                brotli::BrotliCompress(&mut &b[..], &mut out, &Default::default()).unwrap();
            }
            "zstd" => {
                out = vec![0; zstd_safe::compress_bound(b.len())];
                let n = zstd_safe::compress(&mut out[..], b, 3).unwrap();
                out.truncate(n);
            }
            "snappy" => out = snap::raw::Encoder::new().compress_vec(b).unwrap(),
            "snappy-framed" => {
                let mut w = snap::write::FrameEncoder::new(Vec::new());
                w.write_all(b).unwrap();
                out = w.into_inner().unwrap();
            }
            _ => unreachable!(),
        }
        Bytes::from(out)
    }

    #[test]
    fn deflate_brotli_zstd_and_snappy_are_decoded_within_the_limit() {
        let with = |enc: &str| {
            let mut h = HeaderMap::new();
            h.insert(header::CONTENT_ENCODING, enc.parse().unwrap());
            h
        };
        let plain = Bytes::from_static(b"hello");
        for (fixture, name) in [
            ("deflate", "deflate"),
            ("raw-deflate", "deflate"), // RFC 9110 says zlib; some senders send it bare
            ("br", "br"),
            ("zstd", "zstd"),
            ("zstd", "ZSTD"),
            ("snappy", "snappy"),
            ("snappy-framed", "snappy"),
            ("snappy-framed", "x-snappy-framed"),
        ] {
            assert_eq!(
                decoded(&with(name), encoded(fixture, b"hello"), 5),
                Ok((plain.clone(), "-".into())),
                "{fixture}"
            );
            assert_eq!(
                decoded(&with(name), encoded(fixture, b"hello!"), 5),
                Err(StatusCode::PAYLOAD_TOO_LARGE),
                "{fixture} past the limit"
            );
            // raw snappy opens with the decoded length: these bytes claim more than the limit
            let refused = if name == "snappy" { StatusCode::PAYLOAD_TOO_LARGE } else { StatusCode::BAD_REQUEST };
            assert_eq!(decoded(&with(name), Bytes::from_static(b"\xff\xfe not it"), 5), Err(refused), "{fixture}");
        }
        // zstd: frames back to back, and one cut short
        let two_frames = [encoded("zstd", b"hel"), encoded("zstd", b"lo")].concat();
        assert_eq!(decoded(&with("zstd"), two_frames.into(), 5), Ok((plain.clone(), "-".into())));
        // a body many output blocks long, within the limit and past it
        let big: Vec<u8> = (0..1_000_000u32).flat_map(|i| (i % 251).to_le_bytes()).collect();
        let packed = encoded("zstd", &big);
        assert_eq!(decoded(&with("zstd"), packed.clone(), big.len()), Ok((Bytes::from(big.clone()), "-".into())));
        assert_eq!(decoded(&with("zstd"), packed, big.len() - 1), Err(StatusCode::PAYLOAD_TOO_LARGE));
        let cut = encoded("zstd", b"hello");
        assert_eq!(decoded(&with("zstd"), cut.slice(..cut.len() - 2), 5), Err(StatusCode::BAD_REQUEST));
        // codings applied in order are undone in reverse; one unknown keeps the body as sent
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&encoded("br", b"hello")).unwrap();
        let br_then_gzip = Bytes::from(gz.finish().unwrap());
        // (every layer is held to the limit: the brotli layer is longer than "hello")
        assert_eq!(decoded(&with("br, gzip"), br_then_gzip.clone(), 64), Ok((plain, "-".into())));
        assert_eq!(decoded(&with("lz4, gzip"), br_then_gzip.clone(), 64), Ok((br_then_gzip, "lz4,gzip".into())));
    }

    #[test]
    fn a_grpc_message_is_decoded_by_its_grpc_encoding() {
        let with = |enc: &str| {
            let mut h = HeaderMap::new();
            h.insert("grpc-encoding", enc.parse().unwrap());
            h
        };
        for coding in ["deflate", "zstd", "snappy"] {
            assert_eq!(
                grpc_decoded(&with(coding), &encoded(coding, b"hello"), 5),
                Ok(Bytes::from_static(b"hello")),
                "{coding}"
            );
        }
        assert_eq!(
            grpc_decoded(&with("zstd"), &encoded("zstd", b"hello!"), 5).unwrap_err().0,
            grpc::Code::ResourceExhausted
        );
        assert_eq!(grpc_decoded(&with("zstd"), b"not it", 5).unwrap_err().0, grpc::Code::InvalidArgument);
        assert_eq!(grpc_decoded(&with("lz4"), b"hello", 5).unwrap_err().0, grpc::Code::Unimplemented);
        assert_eq!(
            grpc_decoded(&HeaderMap::new(), b"hello", 5).unwrap_err().0,
            grpc::Code::Unimplemented,
            "compressed, no encoding"
        );
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
        let dir = std::env::temp_dir().join(format!("deltapack-recording-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let r = Recording::start(dir.clone(), 4, 1 << 30, Retention::default(), None).unwrap();
        let mut r = lock(&r);
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
        assert_eq!(r.counters()[3], 1, "one failed write");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn files_rotated_within_a_millisecond_get_their_own_names_in_order() {
        let dir = std::env::temp_dir().join(format!("deltapack-rotation-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let r = Recording::start(dir.clone(), 1, 1, Retention::default(), None).unwrap(); // every record fills its file
        let mut r = lock(&r);
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
    fn retention_deletes_the_oldest_past_the_size_and_any_past_the_age() {
        let t = |s| std::time::UNIX_EPOCH + Duration::from_secs(s);
        let files = [("a", 10, 100), ("b", 20, 200), ("c", 30, 300)].map(|(p, n, w)| (PathBuf::from(p), n, t(w)));
        let run = |keep, cutoff, fails: &str| {
            let deleted = delete_expired(&files, 75, keep, cutoff, |p| match p.to_str() == Some(fails) {
                true => Err(io::Error::other("busy")),
                false => Ok(()),
            });
            deleted.into_iter().map(|p| p.display().to_string()).collect::<Vec<_>>()
        };
        // 70 bytes closed and 5 open
        assert_eq!(run(None, None, ""), Vec::<String>::new());
        assert_eq!(run(Some(75), None, ""), Vec::<String>::new(), "at the limit is kept");
        assert_eq!(run(Some(74), None, ""), ["a"]);
        assert_eq!(run(Some(45), None, ""), ["a", "b"]);
        assert_eq!(run(Some(0), None, ""), ["a", "b", "c"]);
        assert_eq!(run(None, Some(t(200)), ""), ["a"], "written at the cutoff is kept");
        assert_eq!(run(None, Some(t(201)), ""), ["a", "b"]);
        assert_eq!(run(Some(60), Some(t(150)), ""), ["a", "b"], "either reason deletes");
        assert_eq!(run(Some(45), None, "a"), ["b", "c"], "a file not deleted frees nothing");
    }

    #[test]
    fn days_kept_past_the_epoch_keep_everything() {
        let now = std::time::UNIX_EPOCH + Duration::from_secs(86_400 * 10);
        assert_eq!(cutoff(Some(3), now), Some(std::time::UNIX_EPOCH + Duration::from_secs(86_400 * 7)));
        assert_eq!(cutoff(Some(10), now), Some(std::time::UNIX_EPOCH));
        assert_eq!(cutoff(Some(u64::MAX), now), None);
        assert_eq!(cutoff(None, now), None);
    }

    #[test]
    fn a_directory_is_recorded_to_by_one_process_at_a_time() {
        let dir = std::env::temp_dir().join(format!("deltapack-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let first = Recording::start(dir.clone(), 1, 1 << 30, Retention::default(), None).unwrap();
        let second = Recording::start(dir.clone(), 1, 1 << 30, Retention::default(), None).map(|_| ());
        assert!(second.unwrap_err().to_string().contains("another process"), "the second is refused");
        drop(first);
        std::thread::sleep(Duration::from_millis(2)); // a file name of its own: the same pid, a new ms
        Recording::start(dir.clone(), 1, 1 << 30, Retention::default(), None).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn only_dpk_files_are_listed_oldest_first() {
        let dir = std::env::temp_dir().join(format!("deltapack-listing-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for name in ["2-x.dpk", "1-x.dpk", "notes.txt", "3-x.dpk.part"] {
            std::fs::write(dir.join(name), name).unwrap();
        }
        // gone between the listing and its size (a command moved it): skipped
        std::os::unix::fs::symlink(dir.join("missing"), dir.join("0-x.dpk")).unwrap();
        // any other failure to read one is an error
        std::os::unix::fs::symlink(dir.join("loop.dpk"), dir.join("loop.dpk")).unwrap();
        assert!(dpk_files(&dir).is_err(), "a link to itself cannot be read");
        std::fs::remove_file(dir.join("loop.dpk")).unwrap();
        let listed: Vec<_> =
            dpk_files(&dir).unwrap().into_iter().map(|f| (f.0.file_name().unwrap().to_owned(), f.1)).collect();
        assert_eq!(listed, [("1-x.dpk".into(), 7), ("2-x.dpk".into(), 7)]);
        assert!(dpk_files(&dir.join("missing")).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_tenant_names_a_directory_inside_the_record_directory_only() {
        let dir = |vs: &[&str]| {
            let mut h = HeaderMap::new();
            for v in vs {
                h.append("x-tenant", http::HeaderValue::from_bytes(v.as_bytes()).unwrap());
            }
            tenant_dir(h.get_all("x-tenant"))
        };
        assert_eq!(dir(&[]), "_none");
        for ok in ["acme", "a.b_c", "x2-y", "x".repeat(64).as_str()] {
            assert_eq!(dir(&[ok]), ok);
        }
        assert_eq!(dir(&["Acme"]), "_invalid", "upper case: two tenants, one directory, on some file systems");
        for bad in
            ["", ".", "..", "../x", "a/b", ".hidden", "_none", "_other", "_x", "a b", "a\\b", "x".repeat(65).as_str()]
        {
            assert_eq!(dir(&[bad]), "_invalid", "{bad:?}");
        }
        assert_eq!(dir(&["acme", "globex"]), "_invalid", "given twice");
        let mut h = HeaderMap::new();
        h.insert("x-tenant", http::HeaderValue::from_bytes(b"\xff").unwrap());
        assert_eq!(tenant_dir(h.get_all("x-tenant")), "_invalid");
    }

    fn tenant_recorders(dir: &std::path::Path, max: usize, keys: &[&str]) -> Recorders {
        let key_dir = dir.join("keys");
        std::fs::create_dir_all(&key_dir).unwrap();
        for k in keys {
            std::fs::write(key_dir.join(format!("{k}.key")), "11".repeat(32)).unwrap();
        }
        let tenants = Tenants { tenant_header: Some(HeaderName::from_static("x-tenant")), max_tenants: max };
        let keys = Keys { key_file: None, key_dir: Some(key_dir) };
        Recorders::start(dir.join("rec"), 4, 1 << 30, Retention::default(), tenants, keys).unwrap()
    }

    fn as_tenant(t: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("x-tenant", t.parse().unwrap());
        h
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_tenant_that_could_not_start_is_tried_again_only_after_the_delay() {
        let dir = std::env::temp_dir().join(format!("deltapack-retry-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut r = tenant_recorders(&dir, 8, &["acme"]);
        let acme = r.of(&as_tenant("acme")).expect("acme has a key");
        assert!(Arc::ptr_eq(&acme, &r.of(&as_tenant("acme")).unwrap()), "started once");
        assert!(r.of(&as_tenant("globex")).is_none(), "no key");
        std::fs::write(dir.join("keys/globex.key"), "22".repeat(32)).unwrap();
        assert!(r.of(&as_tenant("globex")).is_none(), "within the delay: not tried again");
        assert_eq!(r.unrecorded.load(Ordering::Relaxed), 2);
        r.retry_after = Duration::ZERO;
        assert!(r.of(&as_tenant("globex")).is_some(), "past it: tried again, and started");
        assert!(lock_failed(&r.failed).is_empty(), "a started tenant is not remembered as failed");
        assert!(Arc::ptr_eq(&acme, &r.of(&as_tenant("acme")).unwrap()), "a started tenant is never restarted");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_starts_are_remembered_for_the_delay_and_no_more_than_so_many() {
        let dir = std::env::temp_dir().join(format!("deltapack-failed-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut r = tenant_recorders(&dir, 8, &[]);
        let failed = |r: &Recorders| {
            let mut v: Vec<String> = lock_failed(&r.failed).keys().cloned().collect();
            v.sort();
            v
        };
        r.of(&as_tenant("a"));
        r.of(&as_tenant("b"));
        assert_eq!(failed(&r), ["a", "b"], "within the delay: both remembered");
        r.retry_after = Duration::ZERO;
        r.of(&as_tenant("c"));
        assert_eq!(failed(&r), ["c"], "past it: forgotten when another fails");
        r.retry_after = RETRY_START;
        let fill = |r: &Recorders, n: usize| {
            let mut f = lock_failed(&r.failed);
            f.clear();
            f.extend((0..n).map(|i| (format!("t{i}"), std::time::Instant::now())));
        };
        fill(&r, MAX_FAILED - 1);
        r.of(&as_tenant("d"));
        assert!(lock_failed(&r.failed).contains_key("d"), "under the most: remembered");
        fill(&r, MAX_FAILED);
        r.of(&as_tenant("e"));
        assert!(!lock_failed(&r.failed).contains_key("e"), "at the most: not remembered");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn past_the_cap_a_tenant_with_keys_of_its_own_is_not_sealed_with_others() {
        let dir = std::env::temp_dir().join(format!("deltapack-cap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let r = tenant_recorders(&dir, 1, &["acme", "globex", "_other"]);
        assert!(r.of(&as_tenant("nokey")).is_none(), "no key: failed");
        assert!(r.of(&as_tenant("acme")).is_some(), "a failed tenant holds no place under the cap");
        assert!(r.of(&as_tenant("globex")).is_none(), "past the cap, even with `_other.key` there");
        assert!(!dir.join("rec/_other").exists());
        assert_eq!(r.unrecorded.load(Ordering::Relaxed), 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_key_file_others_can_read_is_warned_of() {
        assert!(!key_too_open(0o100600));
        assert!(!key_too_open(0o100400));
        assert!(key_too_open(0o100640));
        assert!(key_too_open(0o100604));
        assert!(key_too_open(0o100644));
    }

    #[test]
    fn reads_off_loopback_need_a_token() {
        let at = |a: &str| a.parse::<SocketAddr>().unwrap();
        assert!(reads_allowed(at("127.0.0.1:4319"), None).is_ok());
        assert!(reads_allowed(at("[::1]:4319"), None).is_ok());
        assert!(reads_allowed(at("0.0.0.0:4319"), None).is_err());
        assert!(reads_allowed(at("10.0.0.5:4319"), Some("t")).is_ok());
    }

    fn plain_recorders(dir: &std::path::Path) -> Recorders {
        let tenants = Tenants { tenant_header: None, max_tenants: 1 };
        Recorders::start(
            dir.to_owned(),
            4,
            1 << 30,
            Retention::default(),
            tenants,
            Keys { key_file: None, key_dir: None },
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_read_answers_by_trace_time_size_and_damage() {
        let dir = std::env::temp_dir().join(format!("deltapack-read-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let r = plain_recorders(&dir);
        let id = format!("{:032x}", 7);
        let body =
            format!(r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":[{{"traceId":"{id}","name":"chat"}}]}}]}}]}}"#);
        let body_len = body.len();
        // received in 1970: before any --since
        lock(&r.of(&HeaderMap::new()).unwrap())
            .append(format!("1000 POST /v1/traces application/json - {id}").as_bytes(), body.into())
            .unwrap();
        let read = |path: &str, query: &str, max: usize| read_trace(&r, path, query, max);
        let (status, rows) = read(&format!("/v1/traces/{id}"), "", 1 << 20);
        assert_eq!((status, rows.iter().filter(|&&b| b == b'\n').count()), (StatusCode::OK, 1));
        assert_eq!(
            read(&format!("/v1/traces/{id}"), "since=1h", 1 << 20).0,
            StatusCode::NOT_FOUND,
            "before the time asked"
        );
        assert_eq!(read(&format!("/v1/traces/{id}"), "since=soon", 1 << 20).0, StatusCode::BAD_REQUEST);
        // a record within the limit whose row (its file's name, its times) is past it
        let (status, why) = read(&format!("/v1/traces/{id}"), "", body_len + 1);
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "past --max-body: {}", String::from_utf8_lossy(&why));
        for bad in ["/v1/traces/zz", &format!("/v1/traces/{}", "z".repeat(32)), &format!("/v1/traces/{id}0"), "/other"]
        {
            let (status, why) = read(bad, "", 1 << 20);
            assert_eq!(status, StatusCode::NOT_FOUND, "{bad}");
            assert!(why.starts_with(b"GET /v1/traces/"), "{bad}: says what it takes");
        }
        // a file that does not read: an error, not too large
        let file = dpk_files(&dir).unwrap()[0].0.clone();
        drop(r);
        let mut bytes = std::fs::read(&file).unwrap();
        let n = bytes.len();
        bytes[n - 3] ^= 0xff;
        std::fs::write(&file, bytes).unwrap();
        let r = plain_recorders(&dir.join("again"));
        let (status, _) = read_trace(&Recorders { root: dir.clone(), ..r }, &format!("/v1/traces/{id}"), "", 1 << 20);
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn reads_need_a_recording() {
        let reads = Reads { read_listen: Some("127.0.0.1:0".parse().unwrap()), read_token_file: None };
        let refused = start_reads(&reads, None, 1).await.unwrap_err();
        assert!(refused.to_string().contains("--record-dir"), "{refused}");
        let none = Reads { read_listen: None, read_token_file: None };
        start_reads(&none, None, 1).await.unwrap();
    }

    #[test]
    fn a_read_holds_at_most_so_many_bytes() {
        let mut out = Capped { out: Vec::new(), left: 5 };
        out.write_all(b"abc").unwrap();
        out.write_all(b"de").unwrap();
        assert_eq!(out.write_all(b"f").unwrap_err().kind(), io::ErrorKind::FileTooLarge);
        assert_eq!(out.out, b"abcde");
        out.flush().unwrap();
    }

    fn ws_sink() -> (
        impl futures_util::Sink<tokio_tungstenite::tungstenite::Message, Error = ()> + Unpin,
        Arc<Mutex<Vec<tokio_tungstenite::tungstenite::Message>>>,
    ) {
        let got = Arc::new(Mutex::new(Vec::new()));
        let sink = futures_util::sink::unfold(got.clone(), |got, m| async move {
            got.lock().unwrap().push(m);
            Ok::<_, ()>(got)
        });
        (Box::pin(sink), got)
    }

    #[tokio::test(start_paused = true)]
    async fn a_tool_silent_too_long_ends_its_session() {
        use tokio_tungstenite::tungstenite::Message;
        let (sink, got) = ws_sink();
        let silent = futures_util::stream::pending::<Result<Message, tokio_tungstenite::tungstenite::Error>>();
        let idle = Duration::from_secs(75);
        let ended =
            tokio::spawn(websocket::pump(silent, sink, None, "WS>", "/".into(), websocket::Keepalive::Idle(idle)));
        tokio::time::sleep(idle - Duration::from_secs(1)).await;
        assert!(!ended.is_finished(), "not yet");
        tokio::time::sleep(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        assert!(ended.is_finished(), "past it: ended");
        assert!(got.lock().unwrap().is_empty(), "no ping from this side");
    }

    #[tokio::test(start_paused = true)]
    async fn the_tool_is_pinged_while_the_upstream_says_nothing() {
        use tokio_tungstenite::tungstenite::Message;
        let (sink, got) = ws_sink();
        let silent = futures_util::stream::pending::<Result<Message, tokio_tungstenite::tungstenite::Error>>();
        let every = Duration::from_secs(30);
        let pump =
            tokio::spawn(websocket::pump(silent, sink, None, "WS<", "/".into(), websocket::Keepalive::Ping(every)));
        tokio::time::sleep(every * 3 + Duration::from_secs(1)).await;
        assert_eq!(*got.lock().unwrap(), vec![Message::Ping(Bytes::new()); 3]);
        assert!(!pump.is_finished(), "an upstream may be quiet for as long as it likes");
        pump.abort();
    }

    #[tokio::test]
    async fn messages_are_passed_on_pings_are_not_and_a_close_ends_the_pump() {
        use tokio_tungstenite::tungstenite::Message;
        let (sink, got) = ws_sink();
        let from = futures_util::stream::iter(
            [
                Message::text("a"),
                Message::Ping(Bytes::new()),
                Message::binary(vec![1]),
                Message::Close(None),
                Message::text("late"),
            ]
            .map(Ok),
        );
        websocket::pump(from, sink, None, "WS>", "/".into(), websocket::Keepalive::Idle(Duration::from_secs(5))).await;
        assert_eq!(*got.lock().unwrap(), [Message::text("a"), Message::binary(vec![1]), Message::Close(None)]);
    }

    #[test]
    fn a_read_carries_the_token_or_is_refused() {
        let v = |s: &str| http::HeaderValue::from_str(s).unwrap();
        assert!(authorized(None, None), "no token set: anyone on the address");
        assert!(authorized(Some(&v("Bearer s3cret")), Some("s3cret")));
        assert!(authorized(Some(&v("bearer s3cret")), Some("s3cret")), "the scheme in any case");
        for bad in ["Bearer s3cre", "Bearer s3cret2", "Basic  s3cret", "Bearers3cret", "s3cret", "Bearer S3cret", ""] {
            assert!(!authorized(Some(&v(bad)), Some("s3cret")), "{bad:?}");
        }
        assert!(!authorized(None, Some("s3cret")));
    }

    #[test]
    fn a_websocket_handshake_is_a_get_asking_to_upgrade_to_websocket() {
        let req = |method: &str, headers: &[(&str, &str)]| {
            let mut r = Request::builder().method(method).uri("/v1/realtime");
            for (k, v) in headers {
                r = r.header(*k, *v);
            }
            r.body(()).unwrap()
        };
        let ws = [("upgrade", "websocket"), ("connection", "Upgrade")];
        assert!(websocket::is_upgrade(&req("GET", &ws)));
        assert!(websocket::is_upgrade(&req("GET", &[("upgrade", "WebSocket"), ("connection", "keep-alive, Upgrade")])));
        assert!(!websocket::is_upgrade(&req("POST", &ws)), "only a GET");
        assert!(!websocket::is_upgrade(&req("GET", &[("connection", "upgrade")])), "no upgrade");
        assert!(!websocket::is_upgrade(&req("GET", &[("upgrade", "websocket")])), "no connection: upgrade");
        assert!(!websocket::is_upgrade(&req("GET", &[("upgrade", "h2c"), ("connection", "upgrade")])), "not websocket");
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
