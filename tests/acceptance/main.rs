//! Acceptance: `logpak client` and `logpak server` as real processes, between a tool (this
//! test) and a recording upstream API, with a byte-counting TCP relay on the network between them.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use cucumber::{World, given, then, when};
use http::{HeaderMap, Request, Response};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper_util::rt::{TokioExecutor, TokioIo};
use logpak::{KEEP, KEPT, codec, hex};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};

type Recorded = Arc<Mutex<Vec<(HeaderMap, Bytes)>>>;

#[derive(Debug, Default, World)]
struct Proxies {
    /// (address, what it received, its accept loop: aborted, the API is down)
    upstream: Option<(SocketAddr, Recorded, tokio::task::JoinHandle<()>)>,
    /// the status of the tool's last request through the client
    status: u16,
    server: Option<Proc>,
    /// (listening address, where it relays to, bytes relayed towards the server)
    relay: Option<(SocketAddr, Arc<Mutex<SocketAddr>>, Arc<AtomicU64>)>,
    client: Option<Proc>,
    http: reqwest::Client,
    /// what the tools sent, in order
    sent: Vec<Sent>,
    turns: HashMap<String, usize>,
    last: Option<(u16, HeaderMap)>,
    /// where the server records requests (`--record-dir`), when it does
    record_dir: Option<std::path::PathBuf>,
    /// JSON lines to pack, and the packed file
    lines: Vec<Bytes>,
    packed: Option<std::path::PathBuf>,
    /// what an exporter posted: the gzip bytes, and the status of the last post
    gzipped: Vec<Bytes>,
    posted: Option<(u16, HeaderMap, Bytes)>,
    /// gRPC exports: the message sent, and (grpc-status, response message) for each
    grpc: Vec<(Bytes, Option<u32>, Bytes)>,
}

#[derive(Debug)]
struct Sent {
    body: Bytes,
    response: Bytes,
    first_of_conversation: bool,
    relayed: u64,
}

#[derive(Debug)]
struct Proc {
    _child: Child,
    addr: SocketAddr,
    metrics: Option<SocketAddr>,
}

/// A conversation's request after `turns` turns: a 57 KB system prompt of its own that does not
/// compress by itself (hex of hashes), then the turns.
fn conversation(name: &str, turns: usize) -> Bytes {
    let system: String = (0..900).map(|i| hex(&codec::hash(format!("{name}{i}").as_bytes()))).collect();
    let msgs: String = (0..turns)
        .map(|i| format!(r#"{{"role":"user","content":"{name} turn {i}: {}"}},"#, "tell me more ".repeat(i)))
        .collect();
    Bytes::from(format!(r#"{{"model":"m","system":"{system}","messages":[{msgs}{{}}]}}"#))
}

/// What the upstream answers: three streamed frames naming the body it received.
fn answer(body: &[u8]) -> Vec<Bytes> {
    let hash = hex(&codec::hash(body));
    vec![
        Bytes::from("data: start\n\n"),
        Bytes::from(format!("data: {} {hash}\n\n", body.len())),
        Bytes::from("data: end\n\n"),
    ]
}

async fn upstream() -> (SocketAddr, Recorded, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let recorded = Recorded::default();
    let seen = recorded.clone();
    let accepting = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = seen.clone();
            let service = hyper::service::service_fn(move |r: Request<Incoming>| {
                let seen = seen.clone();
                async move {
                    let (parts, body) = r.into_parts();
                    let body = body.collect().await.unwrap().to_bytes();
                    let frames = answer(&body);
                    let mut headers = parts.headers;
                    headers.insert("x-test-path", parts.uri.path().parse().unwrap()); // for the steps
                    seen.lock().unwrap().push((headers, body));
                    let stream =
                        futures_util::stream::iter(frames.into_iter().map(|f| Ok::<_, Infallible>(Frame::data(f))));
                    Ok::<_, Infallible>(Response::new(StreamBody::new(stream)))
                }
            });
            tokio::spawn(
                hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service)
                    .into_owned(),
            );
        }
    });
    (addr, recorded, accepting)
}

/// Relays TCP to whatever `target` is when a connection opens, counting the bytes sent towards it.
async fn relay(target: SocketAddr) -> (SocketAddr, Arc<Mutex<SocketAddr>>, Arc<AtomicU64>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (to, up) = (Arc::new(Mutex::new(target)), Arc::new(AtomicU64::new(0)));
    let (target, counted) = (to.clone(), up.clone());
    tokio::spawn(async move {
        loop {
            let (inbound, _) = listener.accept().await.unwrap();
            let target = *target.lock().unwrap();
            let counted = counted.clone();
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect(target).await else { return };
                let (mut in_read, mut in_write) = inbound.into_split();
                let (mut out_read, mut out_write) = outbound.into_split();
                let upload = async {
                    let mut buf = vec![0; 64 << 10];
                    while let Ok(n @ 1..) = in_read.read(&mut buf).await {
                        counted.fetch_add(n as u64, Ordering::SeqCst);
                        if out_write.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                    let _ = out_write.shutdown().await;
                };
                let download = async {
                    let _ = tokio::io::copy(&mut out_read, &mut in_write).await;
                    let _ = in_write.shutdown().await;
                };
                tokio::join!(upload, download);
            });
        }
    });
    (addr, to, up)
}

/// Starts `logpak <args>` on a free port and reads the addresses it logs.
async fn spawn(args: &[&str]) -> Proc {
    let mut child = Command::new(env!("CARGO_BIN_EXE_logpak"))
        .args(args)
        .args(["--listen", "127.0.0.1:0", "--metrics", "127.0.0.1:0"])
        .env("RUST_LOG", "logpak=debug")
        .env("NO_COLOR", "1")
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let (mut addr, mut metrics) = (None, None);
    while addr.is_none() {
        let line = tokio::time::timeout(Duration::from_secs(30), lines.next_line()).await.unwrap().unwrap().unwrap();
        let logged = || line.split("addr=").nth(1).unwrap().split_whitespace().next().unwrap().parse().unwrap();
        if line.contains(" metrics ") {
            metrics = Some(logged());
        } else if line.contains(" listening ") {
            addr = Some(logged());
        }
    }
    // keep draining its log, or it blocks once the pipe is full
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    Proc { _child: child, addr: addr.unwrap(), metrics }
}

impl Proxies {
    fn upstream_paths(&self) -> Vec<String> {
        self.recorded().iter().map(|(h, _)| h["x-test-path"].to_str().unwrap().to_owned()).collect()
    }

    fn recorded(&self) -> Vec<(HeaderMap, Bytes)> {
        self.upstream.as_ref().unwrap().1.lock().unwrap().clone()
    }

    async fn start_server(&mut self) {
        let upstream = format!("http://{}", self.upstream.as_ref().unwrap().0);
        let mut args = vec!["server", "--upstream", &upstream];
        let dir = self.record_dir.as_ref().map(|d| d.to_str().unwrap().to_owned());
        if let Some(dir) = &dir {
            args.extend(["--record-dir", dir]);
        }
        let server = spawn(&args).await;
        if let Some((_, target, _)) = &self.relay {
            *target.lock().unwrap() = server.addr;
        }
        self.server = Some(server);
    }

    async fn send(&mut self, name: &str, key: &str) {
        let turn = self.turns.entry(name.to_owned()).or_default();
        *turn += 1;
        let (body, first_of_conversation) = (conversation(name, *turn), *turn == 1);
        let (_, _, counted) = self.relay.as_ref().unwrap();
        let before = counted.load(Ordering::SeqCst);
        let url = format!("http://{}/v1/messages", self.client.as_ref().unwrap().addr);
        let response = self.http.post(url).bearer_auth(key).body(body.clone()).send().await.unwrap();
        self.status = response.status().as_u16();
        if self.status != 200 {
            return;
        }
        let response = response.bytes().await.unwrap();
        let relayed = counted.load(Ordering::SeqCst) - before;
        self.sent.push(Sent { body, response, first_of_conversation, relayed });
    }

    async fn metric(&self, of: &Option<Proc>, name: &str) -> u64 {
        let url = format!("http://{}/metrics", of.as_ref().unwrap().metrics.unwrap());
        let text = self.http.get(url).send().await.unwrap().text().await.unwrap();
        text.lines().find_map(|l| l.strip_prefix(name)?.trim().parse().ok()).unwrap()
    }

    async fn post_to_server(&mut self, body: Vec<u8>, headers: &[(&str, &str)]) -> (u16, HeaderMap) {
        let mut r = self.http.post(format!("http://{}/v1/messages", self.server.as_ref().unwrap().addr)).body(body);
        for (k, v) in headers {
            r = r.header(*k, *v);
        }
        let r = r.send().await.unwrap();
        (r.status().as_u16(), r.headers().clone())
    }
}

#[given("an upstream API")]
async fn an_upstream(w: &mut Proxies) {
    w.upstream = Some(upstream().await);
}

#[given("a logpak server in front of it")]
async fn a_server(w: &mut Proxies) {
    w.start_server().await;
}

#[given("a logpak client in front of that server")]
async fn a_client(w: &mut Proxies) {
    let relayed = relay(w.server.as_ref().unwrap().addr).await;
    let upstream = format!("http://{}", relayed.0);
    w.relay = Some(relayed);
    w.client = Some(spawn(&["client", "--upstream", &upstream]).await);
}

#[when(expr = "a tool sends a conversation of {int} turns through the client")]
async fn a_conversation(w: &mut Proxies, turns: usize) {
    for _ in 0..turns {
        w.send("a", "k1").await;
        assert_eq!(w.status, 200);
    }
}

#[when(expr = "a tool sends a conversation of {int} turns through the client with the key {string}")]
async fn a_conversation_with_key(w: &mut Proxies, turns: usize, key: String) {
    for _ in 0..turns {
        w.send("a", &key).await;
        assert_eq!(w.status, 200);
    }
}

#[when(expr = "the tool sends {int} more turn(s) of that conversation")]
async fn more_turns(w: &mut Proxies, turns: usize) {
    a_conversation(w, turns).await;
}

#[when(expr = "the tool sends {int} more turn(s) of that conversation with the key {string}")]
async fn more_turns_with_key(w: &mut Proxies, turns: usize, key: String) {
    a_conversation_with_key(w, turns, key).await;
}

#[when(expr = "two tools send conversations of {int} turns through the client, turn by turn")]
async fn two_conversations(w: &mut Proxies, turns: usize) {
    for _ in 0..turns {
        for name in ["a", "b"] {
            w.send(name, "k1").await;
            assert_eq!(w.status, 200);
        }
    }
}

#[when("the logpak server restarts")]
async fn restart(w: &mut Proxies) {
    w.server = None; // killed on drop
    w.start_server().await;
    // the client's pooled connection through the relay closes with the old server
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[when("the upstream API is down")]
async fn upstream_down(w: &mut Proxies) {
    w.upstream.as_ref().unwrap().2.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[when("a tool posts a request through the client")]
async fn one_request(w: &mut Proxies) {
    w.send("a", "k1").await;
}

#[then(expr = "the tool is answered {int}")]
async fn answered(w: &mut Proxies, status: u16) {
    assert_eq!(w.status, status);
}

#[then(expr = "the server reports {int} deltas and no refusals")]
async fn server_reports(w: &mut Proxies, deltas: u64) {
    assert_eq!(w.metric(&w.server, "logpak_server_deltas_total").await, deltas);
    assert_eq!(w.metric(&w.server, "logpak_server_refused_total").await, 0);
    let (wire, rebuilt) = (
        w.metric(&w.server, "logpak_server_delta_bytes_total").await,
        w.metric(&w.server, "logpak_server_rebuilt_bytes_total").await,
    );
    let later: u64 = w.sent.iter().filter(|s| !s.first_of_conversation).map(|s| s.body.len() as u64).sum();
    assert_eq!(rebuilt, later, "the deltas rebuilt every request after the first");
    assert!(wire > 0 && wire * 50 < rebuilt, "{wire} delta bytes for {rebuilt}");
    assert!(w.metric(&w.server, "logpak_server_store_bytes").await > 0);
}

#[then(expr = "the client reports sending under {int}% of its requests' bytes")]
async fn client_reports(w: &mut Proxies, percent: u64) {
    let body = w.metric(&w.client, "logpak_client_body_bytes_total").await;
    let wire = w.metric(&w.client, "logpak_client_wire_bytes_total").await;
    assert_eq!(body, w.sent.iter().map(|s| s.body.len() as u64).sum::<u64>());
    assert!(wire * 100 < body * percent, "{wire} bytes sent for {body}");
}

#[when(expr = "a plain client posts {string} straight to the server")]
async fn plain_client(w: &mut Proxies, body: String) {
    w.last = Some(w.post_to_server(body.clone().into_bytes(), &[]).await);
    w.sent.push(Sent { body: body.into(), response: Bytes::new(), first_of_conversation: true, relayed: 0 });
}

#[when("a client keeps a base on the server, then posts a corrupt delta against it")]
async fn corrupt_delta(w: &mut Proxies) {
    let (base, target) = (b"base".to_vec(), b"base and more".to_vec());
    let (status, headers) = w.post_to_server(base.clone(), &[(KEEP, "?1")]).await;
    assert_eq!(
        (status, headers.get(KEPT).map(|v| v.to_str().unwrap().to_owned())),
        (200, Some(hex(&codec::hash(&base))))
    );
    let mut delta = codec::encode(&base, &codec::hash(&base), &target, &codec::hash(&target));
    *delta.last_mut().unwrap() ^= 0xff;
    w.last = Some(w.post_to_server(delta, &[("content-encoding", "lpd1")]).await);
}

#[then("the upstream received every request byte for byte")]
async fn byte_for_byte(w: &mut Proxies) {
    let recorded = w.recorded();
    assert_eq!(recorded.len(), w.sent.len());
    for (i, ((headers, body), sent)) in recorded.iter().zip(&w.sent).enumerate() {
        assert_eq!(body, &sent.body, "request {i}");
        assert_eq!(headers["content-length"], body.len().to_string());
        assert!(headers.keys().all(|k| !k.as_str().starts_with("lpd1")), "request {i}: {headers:?}");
        assert!(headers.get("content-encoding").is_none(), "request {i}");
    }
}

#[then("every response reached the tool unchanged")]
async fn responses(w: &mut Proxies) {
    for sent in &w.sent {
        assert_eq!(sent.response, answer(&sent.body).concat());
    }
}

#[then(
    regex = r"^the requests after the first (?:of each conversation )?crossed the network in under (\d+)% of their size$"
)]
async fn ratio(w: &mut Proxies, percent: u64) {
    let later: Vec<&Sent> = w.sent.iter().filter(|s| !s.first_of_conversation).collect();
    assert!(!later.is_empty());
    let relayed: u64 = later.iter().map(|s| s.relayed).sum();
    let body: u64 = later.iter().map(|s| s.body.len() as u64).sum();
    assert!(relayed * 100 < body * percent, "{relayed} bytes relayed for {body} bytes of requests");
}

#[then(expr = "the client resent {int} request(s) in full")]
async fn resent(w: &mut Proxies, n: u64) {
    assert_eq!(w.metric(&w.client, "logpak_client_resent_total").await, n);
}

#[then("the response has no lpd1 headers")]
async fn no_lpd1_headers(w: &mut Proxies) {
    let (status, headers) = w.last.as_ref().unwrap();
    assert_eq!(*status, 200);
    assert!(headers.keys().all(|k| !k.as_str().starts_with("lpd1")), "{headers:?}");
}

#[then(expr = "the server answers {int} with the lpd1-error {string}")]
async fn refused(w: &mut Proxies, status: u16, code: String) {
    let (got, headers) = w.last.as_ref().unwrap();
    assert_eq!((*got, headers["lpd1-error"].to_str().unwrap()), (status, code.as_str()));
}

#[then("the upstream received only the base")]
async fn only_the_base(w: &mut Proxies) {
    let recorded = w.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].1, "base");
}

/// A fresh directory under the system's temp dir, for this scenario only (they run concurrently).
fn scratch(name: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("logpak-acceptance-{}-{name}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// `logpak log <args>`, its stdout; it must succeed.
async fn log(args: &[&str], stdin: &[u8]) -> Vec<u8> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_logpak"))
        .arg("log")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    input.write_all(stdin).await.unwrap();
    drop(input);
    let out = child.wait_with_output().await.unwrap();
    assert!(out.status.success(), "logpak log {args:?}: {}", out.status);
    out.stdout
}

#[given("a logpak server in front of it, recording to a directory")]
async fn a_recording_server(w: &mut Proxies) {
    w.record_dir = Some(scratch("record"));
    w.start_server().await;
}

/// The one file the server records to.
fn recording(w: &Proxies) -> String {
    let files: Vec<_> = std::fs::read_dir(w.record_dir.as_ref().unwrap()).unwrap().map(|e| e.unwrap().path()).collect();
    assert_eq!(files.len(), 1, "{files:?}");
    assert_eq!(files[0].extension().unwrap(), "lpk");
    files[0].to_str().unwrap().to_owned()
}

#[then("the recording holds every request byte for byte, with its method and path")]
async fn recording_holds(w: &mut Proxies) {
    let file = recording(w);
    let listed = String::from_utf8(log(&["ls", &file], b"").await).unwrap();
    assert_eq!(listed.lines().count(), w.sent.len(), "{listed}");
    for (i, line) in listed.lines().enumerate() {
        // index, back, stored bytes, then the meta: a timestamp, the method and the path
        let fields: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(fields[0], i.to_string());
        assert!(fields[3].parse::<u64>().unwrap() > 1_700_000_000_000, "{line}");
        // then the content type and encoding as stored ("-": none)
        assert_eq!(fields[4..], ["POST", "/v1/messages", "-", "-"], "{line}");
        assert_eq!(log(&["get", &file, &i.to_string()], b"").await, w.sent[i].body, "request {i}");
    }
}

#[then(expr = "the recording takes under {int}% of the requests' bytes")]
async fn recording_ratio(w: &mut Proxies, percent: u64) {
    let stored = std::fs::metadata(recording(w)).unwrap().len();
    let body: u64 = w.sent.iter().map(|s| s.body.len() as u64).sum();
    assert!(stored * 100 < body * percent, "{stored} bytes stored for {body}");
}

#[given(expr = "{int} traces of {int} spans, each span carrying the whole prompt, as JSON lines")]
async fn spans_as_json_lines(w: &mut Proxies, traces: usize, spans: usize) {
    // trace by trace, as a collector exports them: the span's ids come first, the prompt after
    for span in 0..spans {
        for trace in 0..traces {
            let prompt = String::from_utf8(conversation(&format!("t{trace}"), span + 1).to_vec()).unwrap();
            let prompt = prompt.replace('"', "\\\"");
            w.lines.push(Bytes::from(format!(
                r#"{{"trace_id":"t{trace}","span_id":"s{span}","name":"chat","attributes":{{"gen_ai.prompt":"{prompt}"}}}}"#
            )));
        }
    }
}

#[when("they are packed with `logpak log pack`")]
async fn pack(w: &mut Proxies) {
    let file = scratch("pack").join("spans.lpk");
    log(&["pack", file.to_str().unwrap()], &jsonl(&w.lines)).await;
    w.packed = Some(file);
}

fn jsonl(lines: &[Bytes]) -> Vec<u8> {
    lines.iter().flat_map(|l| [&l[..], b"\n"].concat()).collect()
}

#[then("`logpak log unpack` gives back the same lines")]
async fn unpack(w: &mut Proxies) {
    let file = w.packed.as_ref().unwrap().to_str().unwrap();
    assert!(log(&["unpack", file], b"").await == jsonl(&w.lines));
}

#[then(expr = "`logpak log get` gives back span {int} alone")]
async fn get_one(w: &mut Proxies, i: usize) {
    let file = w.packed.as_ref().unwrap().to_str().unwrap();
    assert_eq!(log(&["get", file, &i.to_string()], b"").await, w.lines[i]);
}

#[then(expr = "the packed file takes under {int}% of the lines' bytes")]
async fn packed_ratio(w: &mut Proxies, percent: u64) {
    let stored = std::fs::metadata(w.packed.as_ref().unwrap()).unwrap().len();
    let raw = jsonl(&w.lines).len() as u64;
    assert!(stored * 100 < raw * percent, "{stored} bytes stored for {raw}");
}

fn gzip(body: &[u8]) -> Bytes {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut e, body).unwrap();
    Bytes::from(e.finish().unwrap())
}

/// The batch an OTLP exporter posts after turn `turn`: one span per trace, each with the whole prompt.
fn batch(turn: usize) -> Bytes {
    let spans: Vec<String> = (0..3)
        .map(|t| {
            let prompt = String::from_utf8(conversation(&format!("t{t}"), turn).to_vec()).unwrap();
            format!(
                r#"{{"traceId":"{t:032x}","spanId":"{turn:016x}","name":"chat","attributes":[{{"key":"gen_ai.input.messages","value":{{"stringValue":{prompt:?}}}}}]}}"#
            )
        })
        .collect();
    Bytes::from(format!(r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":[{}]}}]}}]}}"#, spans.join(",")))
}

impl Proxies {
    async fn start_collector(&mut self, extra: &[&str]) {
        self.record_dir = Some(scratch("collect"));
        let dir = self.record_dir.as_ref().unwrap().to_str().unwrap().to_owned();
        let mut args = vec!["collect".to_owned(), "--record-dir".to_owned(), dir];
        args.extend(extra.iter().map(|a| a.to_string()));
        if let Some((addr, ..)) = &self.upstream {
            args.extend(["--forward".to_owned(), format!("http://{addr}")]);
        }
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        self.server = Some(spawn(&args).await);
    }

    /// Posts `body` to the collector (or through the client), as an exporter would.
    async fn post_batch(&mut self, to: SocketAddr, path: &str, body: Bytes, headers: &[(&str, &str)]) {
        let mut r = self.http.post(format!("http://{to}{path}")).body(body);
        for (k, v) in headers {
            r = r.header(*k, *v);
        }
        let r = r.send().await.unwrap();
        let (status, headers) = (r.status().as_u16(), r.headers().clone());
        self.posted = Some((status, headers, r.bytes().await.unwrap()));
    }

    async fn export(&mut self, batches: usize, path: &str, through_client: bool) {
        for turn in 1..=batches {
            let body = batch(turn);
            let gz = gzip(&body);
            let to =
                if through_client { self.client.as_ref().unwrap().addr } else { self.server.as_ref().unwrap().addr };
            let before = self.relay.as_ref().map_or(0, |(_, _, c)| c.load(Ordering::SeqCst));
            self.post_batch(
                to,
                path,
                gz.clone(),
                &[("content-type", "application/json"), ("content-encoding", "gzip")],
            )
            .await;
            let (status, _, response) = self.posted.clone().unwrap();
            assert_eq!(status, 200, "batch {turn}");
            let relayed = self.relay.as_ref().map_or(0, |(_, _, c)| c.load(Ordering::SeqCst)) - before;
            self.sent.push(Sent { body, response, first_of_conversation: turn == 1, relayed });
            self.gzipped.push(gz);
        }
    }
}

#[given("a logpak collector recording to a directory")]
async fn a_collector(w: &mut Proxies) {
    w.start_collector(&[]).await;
}

#[given("a logpak collector in front of it, recording to a directory")]
async fn a_forwarding_collector(w: &mut Proxies) {
    w.start_collector(&[]).await;
}

#[given(expr = "a logpak collector recording to a directory, starting a new file every {int} bytes")]
async fn a_rotating_collector(w: &mut Proxies, bytes: u64) {
    w.start_collector(&["--rotate-bytes", &bytes.to_string()]).await;
}

#[then("the recording spans several files, each readable on its own, holding every batch in order")]
async fn recording_spans_files(w: &mut Proxies) {
    let mut files: Vec<_> =
        std::fs::read_dir(w.record_dir.as_ref().unwrap()).unwrap().map(|e| e.unwrap().path()).collect();
    files.sort();
    assert!(files.len() > 1, "{files:?}");
    let mut bodies = Vec::new();
    for (k, f) in files.iter().enumerate() {
        let f = f.to_str().unwrap();
        let n = String::from_utf8(log(&["ls", f], b"").await).unwrap().lines().count();
        // the newest is the one the next record goes to: it may hold none yet
        assert!(n > 0 || k == files.len() - 1, "{f} is empty");
        for i in 0..n {
            bodies.push(Bytes::from(log(&["get", f, &i.to_string()], b"").await));
        }
    }
    assert!(bodies.iter().eq(w.sent.iter().map(|s| &s.body)), "every batch, in order, once");
}

#[then(expr = "the collector reports {int} records stored in under {int}% of their bytes, and no failures")]
async fn collector_reports(w: &mut Proxies, records: u64, percent: u64) {
    assert_eq!(w.metric(&w.server, "logpak_recorded_total").await, records);
    assert_eq!(w.metric(&w.server, "logpak_record_failures_total").await, 0);
    let body = w.metric(&w.server, "logpak_recorded_body_bytes_total").await;
    let stored = w.metric(&w.server, "logpak_recorded_file_bytes_total").await;
    assert_eq!(body, w.sent.iter().map(|s| s.body.len() as u64).sum::<u64>());
    let on_disk: u64 =
        std::fs::read_dir(w.record_dir.as_ref().unwrap()).unwrap().map(|e| e.unwrap().metadata().unwrap().len()).sum();
    assert_eq!(stored, on_disk, "what the metric says is on disk");
    assert!(stored * 100 < body * percent, "{stored} stored for {body}");
}

#[given(expr = "a logpak collector recording to a directory, with a size limit of {int} bytes")]
async fn a_small_collector(w: &mut Proxies, bytes: u64) {
    w.start_collector(&["--max-body", &bytes.to_string()]).await;
}

#[when(expr = "an exporter posts a {int}-byte body to be kept as a base")]
async fn exporter_posts_a_base(w: &mut Proxies, n: usize) {
    let to = w.server.as_ref().unwrap().addr;
    w.post_batch(to, "/v1/traces", Bytes::from(vec![b'x'; n]), &[(KEEP, "?1")]).await;
}

#[then(expr = "it is answered {int}, not kept as a base, and nothing is recorded")]
async fn refused_as_a_base(w: &mut Proxies, status: u16) {
    assert!(w.posted.as_ref().unwrap().1.get(KEPT).is_none(), "a body over the limit is never a base");
    refused_unrecorded(w, status).await;
}

#[given("a logpak client in front of that collector")]
async fn a_client_before_the_collector(w: &mut Proxies) {
    a_client(w).await;
}

#[when(expr = "an exporter posts {int} gzip-compressed JSON batches of spans to {string}")]
async fn exporter_posts(w: &mut Proxies, batches: usize, path: String) {
    w.export(batches, &path, false).await;
}

#[when(expr = "an exporter posts {int} gzip-compressed JSON batches of spans to {string} through the client")]
async fn exporter_posts_through_client(w: &mut Proxies, batches: usize, path: String) {
    w.export(batches, &path, true).await;
}

#[when(expr = "an exporter posts a protobuf batch to {string}")]
async fn exporter_posts_protobuf(w: &mut Proxies, path: String) {
    let to = w.server.as_ref().unwrap().addr;
    w.post_batch(to, &path, Bytes::from_static(b"\x0a\x02\x0a\x00"), &[("content-type", "application/x-protobuf")])
        .await;
}

#[when("an exporter posts a gzip body that inflates past the collector's size limit")]
async fn exporter_posts_a_bomb(w: &mut Proxies) {
    let to = w.server.as_ref().unwrap().addr;
    let bomb = gzip(&vec![0; (64 << 20) + 1]);
    w.post_batch(to, "/v1/traces", bomb, &[("content-type", "application/json"), ("content-encoding", "gzip")]).await;
}

#[then(expr = "every batch is acknowledged with {int} and {string}")]
async fn acknowledged(w: &mut Proxies, status: u16, body: String) {
    assert_eq!(w.posted.as_ref().unwrap().0, status);
    assert!(w.sent.iter().all(|s| s.response == body.as_bytes()));
}

#[then("it is acknowledged with 200, an empty body and the protobuf content type")]
async fn acknowledged_in_kind(w: &mut Proxies) {
    let (status, headers, body) = w.posted.as_ref().unwrap();
    assert_eq!((*status, &body[..]), (200, &b""[..]));
    assert_eq!(headers["content-type"], "application/x-protobuf");
}

#[then("the recording holds every batch decoded, with its path and content type")]
async fn recording_holds_batches(w: &mut Proxies) {
    let file = recording(w);
    let listed = String::from_utf8(log(&["ls", &file], b"").await).unwrap();
    assert_eq!(listed.lines().count(), w.sent.len(), "{listed}");
    for (i, line) in listed.lines().enumerate() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(fields[4..], ["POST", "/v1/traces", "application/json", "-"], "{line}");
        assert!(log(&["get", &file, &i.to_string()], b"").await == w.sent[i].body, "batch {i}");
    }
}

#[then(expr = "the recording takes under {int}% of the batches' decoded bytes")]
async fn recording_ratio_of_batches(w: &mut Proxies, percent: u64) {
    recording_ratio(w, percent).await;
}

#[then("the upstream received every batch byte for byte, still gzip-compressed")]
async fn upstream_received_gzip(w: &mut Proxies) {
    let recorded = w.recorded();
    assert_eq!(recorded.len(), w.gzipped.len());
    for (i, ((headers, body), gz)) in recorded.iter().zip(&w.gzipped).enumerate() {
        assert!(body == gz, "batch {i}");
        assert_eq!(headers["content-encoding"], "gzip", "batch {i}");
    }
}

#[then(expr = "the batches after the first crossed the network in under {int}% of their decoded size")]
async fn batches_ratio(w: &mut Proxies, percent: u64) {
    ratio(w, percent).await;
}

const TRACE_EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

/// One unary gRPC call over HTTP/2 without TLS (h2c, prior knowledge), as OTLP/gRPC exporters make
/// it: `framed` is the length-prefixed body. Returns grpc-status (from the headers of a trailers-only
/// answer, or the trailers) and the response's body.
async fn grpc_call(to: SocketAddr, path: &str, framed: Bytes, gzip: bool) -> (Option<u32>, Bytes) {
    grpc_call_with(to, path, framed, gzip.then_some("gzip")).await
}

async fn grpc_call_with(to: SocketAddr, path: &str, framed: Bytes, encoding: Option<&str>) -> (Option<u32>, Bytes) {
    let io = TokioIo::new(TcpStream::connect(to).await.unwrap());
    let (mut send, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io).await.unwrap();
    tokio::spawn(conn);
    let mut request =
        Request::post(format!("http://{to}{path}")).header("content-type", "application/grpc").header("te", "trailers");
    if let Some(encoding) = encoding {
        request = request.header("grpc-encoding", encoding);
    }
    let response = send.send_request(request.body(http_body_util::Full::new(framed)).unwrap()).await.unwrap();
    assert_eq!(response.status(), 200, "gRPC answers HTTP 200 and puts the outcome in grpc-status");
    let status = |h: &HeaderMap| h.get("grpc-status").map(|v| v.to_str().unwrap().parse::<u32>().unwrap());
    let mut grpc_status = status(response.headers());
    let collected = response.into_body().collect().await.unwrap();
    if let Some(trailers) = collected.trailers() {
        grpc_status = grpc_status.or(status(trailers));
    }
    (grpc_status, collected.to_bytes())
}

/// gRPC's length-prefixed message: a compressed flag, a big-endian length, the bytes.
fn grpc_frame(compressed: bool, message: &[u8]) -> Bytes {
    let mut f = vec![u8::from(compressed)];
    f.extend_from_slice(&(message.len() as u32).to_be_bytes());
    f.extend_from_slice(message);
    Bytes::from(f)
}

#[when(expr = "a gRPC exporter sends {int} gzip-compressed trace exports")]
async fn grpc_exports(w: &mut Proxies, n: usize) {
    let to = w.server.as_ref().unwrap().addr;
    for turn in 1..=n {
        // the collector stores the message as is: any bytes stand in for an ExportTraceServiceRequest
        let message = batch(turn);
        let (status, body) = grpc_call(to, TRACE_EXPORT, grpc_frame(true, &gzip(&message)), true).await;
        w.grpc.push((message, status, body));
    }
}

#[when("a gRPC exporter sends a body that is not one length-prefixed message")]
async fn grpc_garbage(w: &mut Proxies) {
    let to = w.server.as_ref().unwrap().addr;
    let (status, body) = grpc_call(to, TRACE_EXPORT, Bytes::from_static(b"\x00\x00\x00"), false).await;
    w.grpc.push((Bytes::new(), status, body));
}

#[then("every export is answered with gRPC status 0 and an empty response message")]
async fn grpc_ok_empty(w: &mut Proxies) {
    for (i, (_, status, body)) in w.grpc.iter().enumerate() {
        assert_eq!((*status, &body[..]), (Some(0), &b"\x00\x00\x00\x00\x00"[..]), "export {i}");
    }
}

#[then("every export is answered with gRPC status 0")]
async fn grpc_ok(w: &mut Proxies) {
    assert!(
        w.grpc.iter().all(|(_, status, _)| *status == Some(0)),
        "{:?}",
        w.grpc.iter().map(|g| g.1).collect::<Vec<_>>()
    );
}

#[then(expr = "the export is answered with gRPC status {int}")]
async fn grpc_status_is(w: &mut Proxies, code: u32) {
    assert_eq!(w.grpc.last().unwrap().1, Some(code));
}

#[then("the recording holds every export's message, with its gRPC method and content type")]
async fn recording_holds_grpc(w: &mut Proxies) {
    let file = recording(w);
    let listed = String::from_utf8(log(&["ls", &file], b"").await).unwrap();
    assert_eq!(listed.lines().count(), w.grpc.len(), "{listed}");
    for (i, line) in listed.lines().enumerate() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(fields[4..], ["POST", TRACE_EXPORT, "application/grpc", "-"], "{line}");
        assert!(log(&["get", &file, &i.to_string()], b"").await == w.grpc[i].0, "export {i}");
    }
}

#[then(expr = "the upstream received every export's message at {string} as {string}")]
async fn upstream_received_otlp_http(w: &mut Proxies, path: String, content_type: String) {
    let recorded = w.recorded();
    assert_eq!(recorded.len(), w.grpc.len());
    for ((headers, body), (message, ..)) in recorded.iter().zip(&w.grpc) {
        assert!(body == message);
        assert_eq!(headers["content-type"], content_type.as_str());
        assert!(headers.get("grpc-encoding").is_none() && headers.get("content-encoding").is_none());
    }
    assert!(w.upstream_paths().iter().all(|p| *p == path), "{:?}", w.upstream_paths());
}

#[when(regex = r"^a gRPC exporter sends (an uncompressed export of|a gzip export that inflates to) (\d+) bytes$")]
async fn grpc_sized(w: &mut Proxies, kind: String, n: usize) {
    let to = w.server.as_ref().unwrap().addr;
    let message = Bytes::from(vec![b's'; n]);
    let (framed, gz) = if kind.starts_with("a gzip") {
        (grpc_frame(true, &gzip(&message)), true)
    } else {
        (grpc_frame(false, &message), false)
    };
    let (status, body) = grpc_call(to, TRACE_EXPORT, framed, gz).await;
    w.grpc.push((message, status, body));
}

#[when(
    regex = r"^a gRPC exporter sends (an export compressed with lz4|an export flagged as gzip that is not gzip|a call to a method other than Export)$"
)]
async fn grpc_refused(w: &mut Proxies, call: String) {
    let to = w.server.as_ref().unwrap().addr;
    let (path, framed, gz) = match call.as_str() {
        "an export compressed with lz4" => (TRACE_EXPORT, grpc_frame(true, b"lz4 frame"), false),
        "an export flagged as gzip that is not gzip" => (TRACE_EXPORT, grpc_frame(true, b"not gzip"), true),
        _ => ("/grpc.health.v1.Health/Check", grpc_frame(false, b""), false),
    };
    // `false` for gzip with a compressed frame: grpc_call sends no grpc-encoding, so say lz4
    let (status, body) = if call.contains("lz4") {
        grpc_call_with(to, path, framed, Some("lz4")).await
    } else {
        grpc_call(to, path, framed, gz).await
    };
    w.grpc.push((Bytes::new(), status, body));
}

#[then(expr = "the number of records stored is {int}")]
async fn recording_holds_n(w: &mut Proxies, n: usize) {
    let listed = String::from_utf8(log(&["ls", &recording(w)], b"").await).unwrap();
    assert_eq!(listed.lines().count(), n, "{listed}");
}

#[then("nothing is recorded")]
async fn nothing_recorded(w: &mut Proxies) {
    let listed = log(&["ls", &recording(w)], b"").await;
    assert!(listed.is_empty(), "{}", String::from_utf8_lossy(&listed));
}

#[then(expr = "it is answered {int} and nothing is recorded")]
async fn refused_unrecorded(w: &mut Proxies, status: u16) {
    assert_eq!(w.posted.as_ref().unwrap().0, status);
    let listed = log(&["ls", &recording(w)], b"").await;
    assert!(listed.is_empty(), "{}", String::from_utf8_lossy(&listed));
}

#[when("a tool posts an lz4-encoded body through the client")]
async fn lz4_through_client(w: &mut Proxies) {
    // not real lz4 (a coding logpak does not decode): the client must not need to read it, only to leave it alone. Two turns, so
    // the second would have a base to delta against.
    let to = w.client.as_ref().unwrap().addr;
    for turn in [3, 4] {
        let body = conversation("lz4", turn);
        w.post_batch(to, "/v1/messages", body.clone(), &[("content-encoding", "lz4")]).await;
        w.gzipped.push(body);
    }
}

#[then("the upstream received it unchanged, still lz4-encoded")]
async fn upstream_received_lz4(w: &mut Proxies) {
    let recorded = w.recorded();
    assert_eq!(recorded.len(), 2);
    for ((headers, body), sent) in recorded.iter().zip(&w.gzipped) {
        assert!(body == sent);
        assert_eq!(headers["content-encoding"], "lz4");
    }
}

#[tokio::main]
async fn main() {
    Proxies::cucumber().fail_on_skipped().run_and_exit("tests/acceptance/features").await;
}
