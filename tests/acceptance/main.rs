//! Acceptance: `deltapack client` and `deltapack server` as real processes, between a tool (this
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
use deltapack::{KEEP, KEPT, codec, hex};
use http::{HeaderMap, Request, Response};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper_util::rt::{TokioExecutor, TokioIo};
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
    /// where `--on-rotate` moves closed files, and the files a directory held before the collector
    moved_to: Option<std::path::PathBuf>,
    earlier: Vec<std::path::PathBuf>,
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

/// Starts `deltapack <args>` on a free port and reads the addresses it logs.
async fn spawn(args: &[&str]) -> Proc {
    let mut child = Command::new(env!("CARGO_BIN_EXE_deltapack"))
        .args(args)
        .args(["--listen", "127.0.0.1:0", "--metrics", "127.0.0.1:0"])
        .env("RUST_LOG", "deltapack=debug")
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

#[given("a deltapack server in front of it")]
async fn a_server(w: &mut Proxies) {
    w.start_server().await;
}

#[given("a deltapack client in front of that server")]
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

#[when("the deltapack server restarts")]
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
    assert_eq!(w.metric(&w.server, "deltapack_server_deltas_total").await, deltas);
    assert_eq!(w.metric(&w.server, "deltapack_server_refused_total").await, 0);
    let (wire, rebuilt) = (
        w.metric(&w.server, "deltapack_server_delta_bytes_total").await,
        w.metric(&w.server, "deltapack_server_rebuilt_bytes_total").await,
    );
    let later: u64 = w.sent.iter().filter(|s| !s.first_of_conversation).map(|s| s.body.len() as u64).sum();
    assert_eq!(rebuilt, later, "the deltas rebuilt every request after the first");
    assert!(wire > 0 && wire * 50 < rebuilt, "{wire} delta bytes for {rebuilt}");
    assert!(w.metric(&w.server, "deltapack_server_store_bytes").await > 0);
}

#[then(expr = "the client reports sending under {int}% of its requests' bytes")]
async fn client_reports(w: &mut Proxies, percent: u64) {
    let body = w.metric(&w.client, "deltapack_client_body_bytes_total").await;
    let wire = w.metric(&w.client, "deltapack_client_wire_bytes_total").await;
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
    w.last = Some(w.post_to_server(delta, &[("content-encoding", "dpd1")]).await);
}

#[then("the upstream received every request byte for byte")]
async fn byte_for_byte(w: &mut Proxies) {
    let recorded = w.recorded();
    assert_eq!(recorded.len(), w.sent.len());
    for (i, ((headers, body), sent)) in recorded.iter().zip(&w.sent).enumerate() {
        assert_eq!(body, &sent.body, "request {i}");
        assert_eq!(headers["content-length"], body.len().to_string());
        assert!(headers.keys().all(|k| !k.as_str().starts_with("dpd1")), "request {i}: {headers:?}");
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
    assert_eq!(w.metric(&w.client, "deltapack_client_resent_total").await, n);
}

#[then("the response has no dpd1 headers")]
async fn no_dpd1_headers(w: &mut Proxies) {
    let (status, headers) = w.last.as_ref().unwrap();
    assert_eq!(*status, 200);
    assert!(headers.keys().all(|k| !k.as_str().starts_with("dpd1")), "{headers:?}");
}

#[then(expr = "the server answers {int} with the dpd1-error {string}")]
async fn refused(w: &mut Proxies, status: u16, code: String) {
    let (got, headers) = w.last.as_ref().unwrap();
    assert_eq!((*got, headers["dpd1-error"].to_str().unwrap()), (status, code.as_str()));
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
    let dir = std::env::temp_dir().join(format!("deltapack-acceptance-{}-{name}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// `deltapack log <args>`, its stdout; it must succeed.
async fn log(args: &[&str], stdin: &[u8]) -> Vec<u8> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_deltapack"))
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
    assert!(out.status.success(), "deltapack log {args:?}: {}", out.status);
    out.stdout
}

#[given("a deltapack server in front of it, recording to a directory")]
async fn a_recording_server(w: &mut Proxies) {
    w.record_dir = Some(scratch("record"));
    w.start_server().await;
}

/// The one file the server records to.
fn recording(w: &Proxies) -> String {
    let files: Vec<_> = std::fs::read_dir(w.record_dir.as_ref().unwrap()).unwrap().map(|e| e.unwrap().path()).collect();
    assert_eq!(files.len(), 1, "{files:?}");
    assert_eq!(files[0].extension().unwrap(), "dpk");
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

#[when("they are packed with `deltapack log pack`")]
async fn pack(w: &mut Proxies) {
    let file = scratch("pack").join("spans.dpk");
    log(&["pack", file.to_str().unwrap()], &jsonl(&w.lines)).await;
    w.packed = Some(file);
}

fn jsonl(lines: &[Bytes]) -> Vec<u8> {
    lines.iter().flat_map(|l| [&l[..], b"\n"].concat()).collect()
}

#[then("`deltapack log unpack` gives back the same lines")]
async fn unpack(w: &mut Proxies) {
    let file = w.packed.as_ref().unwrap().to_str().unwrap();
    assert!(log(&["unpack", file], b"").await == jsonl(&w.lines));
}

#[then(expr = "`deltapack log get` gives back span {int} alone")]
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
        let dir = self.record_dir.get_or_insert_with(|| scratch("collect")).to_str().unwrap().to_owned();
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

#[given("a deltapack collector recording to a directory")]
async fn a_collector(w: &mut Proxies) {
    w.start_collector(&[]).await;
}

#[given("a deltapack collector in front of it, recording to a directory")]
async fn a_forwarding_collector(w: &mut Proxies) {
    w.start_collector(&[]).await;
}

#[given(expr = "a deltapack collector recording to a directory, starting a new file every {int} bytes")]
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
    assert_eq!(w.metric(&w.server, "deltapack_recorded_total").await, records);
    assert_eq!(w.metric(&w.server, "deltapack_record_failures_total").await, 0);
    let body = w.metric(&w.server, "deltapack_recorded_body_bytes_total").await;
    let stored = w.metric(&w.server, "deltapack_recorded_file_bytes_total").await;
    assert_eq!(body, w.sent.iter().map(|s| s.body.len() as u64).sum::<u64>());
    let on_disk: u64 =
        std::fs::read_dir(w.record_dir.as_ref().unwrap()).unwrap().map(|e| e.unwrap().metadata().unwrap().len()).sum();
    assert_eq!(stored, on_disk, "what the metric says is on disk");
    assert!(stored * 100 < body * percent, "{stored} stored for {body}");
}

#[given(expr = "a deltapack collector recording to a directory, with a size limit of {int} bytes")]
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

#[given("a deltapack client in front of that collector")]
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
        assert_eq!(fields[4..8], ["POST", "/v1/traces", "application/json", "-"], "{line}");
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
        assert_eq!(fields[4..8], ["POST", TRACE_EXPORT, "application/grpc", "-"], "{line}");
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
    // not real lz4 (a coding deltapack does not decode): the client must not need to read it, only to leave it alone. Two turns, so
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

/// `deltapack log show <args> <the recording>`, one JSON value per line.
async fn show(w: &Proxies, args: &[&str]) -> Vec<serde_json::Value> {
    let file = match &w.packed {
        Some(f) => f.to_str().unwrap().to_owned(),
        None => recording(w),
    };
    let mut args = [&["show"], args].concat();
    args.push(&file);
    let out = String::from_utf8(log(&args, b"").await).unwrap();
    out.lines().map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("{e}: {l}"))).collect()
}

#[then("each record's meta lists the trace ids of its batch")]
async fn meta_lists_traces(w: &mut Proxies) {
    let listed = String::from_utf8(log(&["ls", &recording(w)], b"").await).unwrap();
    assert_eq!(listed.lines().count(), w.sent.len());
    for line in listed.lines() {
        let ids = line.split_whitespace().nth(8).unwrap_or_else(|| panic!("no trace ids: {line}"));
        assert_eq!(ids, format!("{:032x},{:032x},{:032x}", 0, 1, 2), "{line}");
    }
}

#[then(expr = "`deltapack log show --trace` of trace {int} gives its {int} spans in order, each with its prompt")]
async fn show_trace(w: &mut Proxies, trace: usize, n: usize) {
    let id = format!("{trace:032x}");
    let rows = show(w, &["--trace", &id]).await;
    assert_eq!(rows.len(), n, "{rows:?}");
    for (turn, row) in (1..).zip(&rows) {
        assert_eq!(row["trace_id"], id.as_str());
        assert_eq!(row["file"], recording(w).as_str());
        assert!(row["received"].as_u64().unwrap() > 1_700_000_000_000, "{row}");
        let prompt = String::from_utf8(conversation(&format!("t{trace}"), turn).to_vec()).unwrap();
        assert_eq!(row["attributes"]["gen_ai.input.messages"], prompt.as_str(), "span {turn}");
        assert_eq!(row["record"], turn - 1);
    }
}

#[then("`deltapack log show` gives each line back as the body of its record")]
async fn show_raw(w: &mut Proxies) {
    let rows = show(w, &[]).await;
    assert_eq!(rows.len(), w.lines.len());
    for (i, (row, line)) in rows.iter().zip(&w.lines).enumerate() {
        assert_eq!(row["record"], i);
        assert_eq!(row["body"], serde_json::from_slice::<serde_json::Value>(line).unwrap(), "line {i}");
    }
}

#[then(expr = "`deltapack log show --since {word}` gives {int} spans")]
async fn show_since(w: &mut Proxies, since: String, n: usize) {
    assert_eq!(show(w, &["--since", &since]).await.len(), n);
}

#[then(expr = "`deltapack log show --until {word}` gives none")]
async fn show_until(w: &mut Proxies, until: String) {
    assert_eq!(show(w, &["--until", &until]).await, Vec::<serde_json::Value>::new());
}

/// Protobuf as OTLP encodes it: a field's key, then its value.
fn pb(field: u64, wire: u64, value: &[u8]) -> Vec<u8> {
    let mut out = varint((field << 3) | wire);
    if wire == 2 {
        out.extend(varint(value.len() as u64));
    }
    out.extend_from_slice(value);
    out
}

fn varint(mut v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
    out
}

/// An ExportTraceServiceRequest: one span per trace, each with the whole prompt and its turn as an
/// int attribute.
fn trace_export(turn: usize, traces: usize) -> Bytes {
    let spans: Vec<u8> = (0..traces)
        .flat_map(|t| {
            let kv = |key: &str, any: Vec<u8>| pb(9, 2, &[pb(1, 2, key.as_bytes()), pb(2, 2, &any)].concat());
            let prompt = conversation(&format!("t{t}"), turn);
            let span = [
                pb(1, 2, &(t as u128).to_be_bytes()),
                pb(2, 2, &(turn as u64).to_be_bytes()),
                pb(5, 2, b"chat"),
                pb(6, 0, &varint(3)),
                pb(7, 1, &(1_700_000_000_000_000_000u64 + turn as u64).to_le_bytes()),
                pb(8, 1, &(1_700_000_001_000_000_000u64 + turn as u64).to_le_bytes()),
                kv("gen_ai.input.messages", pb(1, 2, &prompt)),
                kv("gen_ai.usage.input_tokens", pb(3, 0, &varint(turn as u64))),
            ]
            .concat();
            pb(2, 2, &span)
        })
        .collect();
    let scope_spans = pb(2, 2, &[pb(1, 2, &pb(1, 2, b"agent")), spans].concat());
    let resource = pb(1, 2, &pb(1, 2, &[pb(1, 2, b"service.name"), pb(2, 2, &pb(1, 2, b"swe"))].concat()));
    Bytes::from(pb(1, 2, &[resource, scope_spans].concat()))
}

#[when(expr = "a gRPC exporter sends {int} protobuf trace exports of {int} traces each")]
async fn grpc_protobuf_exports(w: &mut Proxies, n: usize, traces: usize) {
    let to = w.server.as_ref().unwrap().addr;
    for turn in 1..=n {
        let message = trace_export(turn, traces);
        let (status, body) = grpc_call(to, TRACE_EXPORT, grpc_frame(true, &gzip(&message)), true).await;
        assert_eq!(status, Some(0));
        w.grpc.push((message, status, body));
    }
}

#[then(expr = "`deltapack log show` gives {int} spans, each with its trace id, name, times and attributes")]
async fn show_protobuf(w: &mut Proxies, n: usize) {
    let rows = show(w, &[]).await;
    assert_eq!(rows.len(), n, "{rows:?}");
    for (i, row) in rows.iter().enumerate() {
        let (turn, trace) = (i / 2 + 1, i % 2);
        assert_eq!(row["signal"], "span");
        assert_eq!(row["trace_id"], format!("{trace:032x}"));
        assert_eq!(row["span_id"], format!("{turn:016x}"));
        assert_eq!((&row["name"], &row["kind"]), (&"chat".into(), &3.into()));
        assert_eq!(row["start"], 1_700_000_000_000_000_000u64 + turn as u64);
        assert_eq!(row["end"], 1_700_000_001_000_000_000u64 + turn as u64);
        assert_eq!(row["attributes"]["gen_ai.usage.input_tokens"], turn);
        assert_eq!((&row["resource"]["service.name"], &row["scope"]), (&"swe".into(), &"agent".into()));
    }
}

#[then("`deltapack log show --trace` in capitals finds the same spans")]
async fn show_trace_in_capitals(w: &mut Proxies) {
    let lower = show(w, &["--trace", &format!("{:032x}", 1)]).await;
    let upper = show(w, &["--trace", &format!("{:032X}", 1)]).await;
    assert!(!lower.is_empty());
    assert_eq!(upper, lower);
}

#[when(expr = "an exporter posts a batch of spans to {string} in a coding deltapack does not decode")]
async fn exporter_posts_lz4(w: &mut Proxies, path: String) {
    let to = w.server.as_ref().unwrap().addr;
    let body = batch(1);
    w.post_batch(to, &path, body.clone(), &[("content-type", "application/json"), ("content-encoding", "lz4")]).await;
    assert_eq!(w.posted.as_ref().unwrap().0, 200);
    w.gzipped.push(body);
}

#[then("its record's meta lists its trace ids as unknown")]
async fn meta_lists_unknown(w: &mut Proxies) {
    let listed = String::from_utf8(log(&["ls", &recording(w)], b"").await).unwrap();
    let fields: Vec<&str> = listed.lines().next().unwrap().split_whitespace().collect();
    assert_eq!(fields[7..], ["lz4", "?"], "{listed}");
}

#[then("`deltapack log show` gives the record whole")]
async fn show_gives_whole(w: &mut Proxies) {
    let rows = show(w, &[]).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    let body: serde_json::Value = serde_json::from_slice(&w.gzipped[0]).unwrap();
    assert_eq!(rows[0]["body"], body, "the record whole, not its spans");
}

/// The record directory's files, oldest first, and their bytes.
fn files_in(dir: &std::path::Path) -> Vec<(std::path::PathBuf, u64)> {
    // a file an --on-rotate command moves away meanwhile is not listed
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| Some((e.path(), e.metadata().ok()?.len())))
        .collect();
    files.sort();
    files
}

/// Every record of `files`, in order.
async fn bodies_of(files: &[std::path::PathBuf]) -> Vec<Bytes> {
    let mut bodies = Vec::new();
    for f in files {
        let f = f.to_str().unwrap();
        let n = String::from_utf8(log(&["ls", f], b"").await).unwrap().lines().count();
        for i in 0..n {
            bodies.push(Bytes::from(log(&["get", f, &i.to_string()], b"").await));
        }
    }
    bodies
}

/// Waits up to 20 s for `done`: hooks run beside the collector, not before it answers.
async fn eventually(mut done: impl FnMut() -> bool) {
    for _ in 0..200 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("not within 20 s");
}

impl Proxies {
    fn hook_dest(&mut self) -> String {
        let dest = self.moved_to.get_or_insert_with(|| scratch("moved"));
        format!(r#"mv "$1" {}/"#, dest.display())
    }
}

#[given(
    expr = "a deltapack collector recording to a directory, starting a new file every {int} bytes and keeping {int} bytes"
)]
async fn a_collector_keeping_bytes(w: &mut Proxies, rotate: u64, keep: u64) {
    w.start_collector(&["--rotate-bytes", &rotate.to_string(), "--keep-bytes", &keep.to_string()]).await;
}

#[then(expr = "the closed files left take at most {int} bytes, and the oldest are gone")]
async fn closed_files_within(w: &mut Proxies, keep: u64) {
    let files = files_in(w.record_dir.as_ref().unwrap());
    let closed: u64 = files[..files.len() - 1].iter().map(|f| f.1).sum();
    assert!(closed <= keep, "{closed} bytes closed: {files:?}");
    let left = bodies_of(&files.iter().map(|f| f.0.clone()).collect::<Vec<_>>()).await;
    assert!(left.len() < w.sent.len(), "nothing was deleted: {files:?}");
}

#[then("the files left hold the newest batches, in order")]
async fn files_left_hold_newest(w: &mut Proxies) {
    let files: Vec<_> = files_in(w.record_dir.as_ref().unwrap()).into_iter().map(|f| f.0).collect();
    let left = bodies_of(&files).await;
    let newest = &w.sent[w.sent.len() - left.len()..];
    assert!(left.iter().eq(newest.iter().map(|s| &s.body)), "the newest batches, in order");
}

#[given("a record directory holding a file last written 3 days ago and one written now")]
async fn a_directory_with_old_files(w: &mut Proxies) {
    let dir = w.record_dir.insert(scratch("earlier")).clone();
    for (name, age) in [("1000000000000-1-000000.dpk", 3 * 86_400), ("1000000000001-1-000000.dpk", 0)] {
        let path = dir.join(name);
        log(&["pack", path.to_str().unwrap()], b"{\"a\":1}\n").await;
        let when = std::time::SystemTime::now() - Duration::from_secs(age);
        std::fs::File::options().write(true).open(&path).unwrap().set_modified(when).unwrap();
        w.earlier.push(path);
    }
}

#[given(expr = "a deltapack collector recording to it, keeping {int} days")]
async fn a_collector_keeping_days(w: &mut Proxies, days: u64) {
    w.start_collector(&["--keep-days", &days.to_string()]).await;
}

#[then("the file written 3 days ago is gone and the one written now is kept")]
async fn old_file_gone(w: &mut Proxies) {
    assert!(!w.earlier[0].exists(), "{:?}", w.earlier[0]);
    assert!(w.earlier[1].exists(), "{:?}", w.earlier[1]);
}

#[given(
    expr = "a deltapack collector recording to a directory, starting a new file every {int} bytes and moving each closed file elsewhere"
)]
async fn a_collector_moving_files(w: &mut Proxies, rotate: u64) {
    let hook = w.hook_dest();
    w.start_collector(&["--rotate-bytes", &rotate.to_string(), "--on-rotate", &hook]).await;
}

#[given("a deltapack collector recording to it, moving each closed file elsewhere")]
async fn a_collector_moving_earlier_files(w: &mut Proxies) {
    let hook = w.hook_dest();
    w.start_collector(&["--on-rotate", &hook]).await;
}

#[then("only the open file is left, and the files moved away and the open file hold every batch in order")]
async fn files_moved_away(w: &mut Proxies) {
    let dir = w.record_dir.clone().unwrap();
    eventually(|| files_in(&dir).len() == 1).await;
    let mut files: Vec<_> = files_in(w.moved_to.as_ref().unwrap()).into_iter().map(|f| f.0).collect();
    assert!(!files.is_empty(), "nothing was moved");
    files.extend(files_in(&dir).into_iter().map(|f| f.0));
    let all = bodies_of(&files).await;
    assert!(all.iter().eq(w.sent.iter().map(|s| &s.body)), "every batch, in order, once");
}

#[then(expr = "the collector reports {int} failed rotation commands")]
async fn hook_failures_are(w: &mut Proxies, n: u64) {
    assert_eq!(w.metric(&w.server, "deltapack_rotate_command_failures_total").await, n);
}

#[then("both files are moved away")]
async fn both_moved(w: &mut Proxies) {
    let dest = w.moved_to.clone().unwrap();
    eventually(|| files_in(&dest).len() == 2).await;
    assert!(w.earlier.iter().all(|f| !f.exists()));
}

#[given(
    expr = "a deltapack collector recording to a directory, starting a new file every {int} bytes and running a command that fails on each closed file"
)]
async fn a_collector_with_failing_hook(w: &mut Proxies, rotate: u64) {
    w.start_collector(&["--rotate-bytes", &rotate.to_string(), "--on-rotate", "exit 3"]).await;
}

#[then("the collector reports a failed rotation command for each closed file")]
async fn hook_failure_per_file(w: &mut Proxies) {
    let closed = files_in(w.record_dir.as_ref().unwrap()).len() as u64 - 1;
    assert!(closed > 0);
    for _ in 0..200 {
        if w.metric(&w.server, "deltapack_rotate_command_failures_total").await == closed {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{closed} closed files, not as many failures");
}

#[then("only the open file is left")]
async fn only_open_file(w: &mut Proxies) {
    let files = files_in(w.record_dir.as_ref().unwrap());
    assert_eq!(files.len(), 1, "{files:?}");
}

#[given(
    expr = "a deltapack collector recording to a directory, starting a new file every {int} bytes, keeping {int} bytes and moving each closed file elsewhere after a second"
)]
async fn a_collector_keeping_and_slowly_moving(w: &mut Proxies, rotate: u64, keep: u64) {
    let hook = format!("sleep 1; {}", w.hook_dest());
    w.start_collector(&[
        "--rotate-bytes",
        &rotate.to_string(),
        "--keep-bytes",
        &keep.to_string(),
        "--on-rotate",
        &hook,
    ])
    .await;
}

#[given(
    expr = "a deltapack collector recording to a directory, starting a new file every {int} bytes, keeping {int} bytes and running a command that fails on each closed file"
)]
async fn a_collector_keeping_with_failing_hook(w: &mut Proxies, rotate: u64, keep: u64) {
    w.start_collector(&[
        "--rotate-bytes",
        &rotate.to_string(),
        "--keep-bytes",
        &keep.to_string(),
        "--on-rotate",
        "exit 3",
    ])
    .await;
}

#[then("a second collector recording to the same directory refuses to start")]
async fn second_collector_refused(w: &mut Proxies) {
    let dir = w.record_dir.as_ref().unwrap().to_str().unwrap().to_owned();
    let out = Command::new(env!("CARGO_BIN_EXE_deltapack"))
        .args(["collect", "--record-dir", &dir, "--listen", "127.0.0.1:0"])
        .stdin(Stdio::null())
        .output();
    let out = tokio::time::timeout(Duration::from_secs(30), out).await.unwrap().unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("another process"), "{err}");
}

/// `deltapack log stats` names, record counts, body bytes and file bytes, from its lines.
fn stats_of(out: &str) -> Vec<(String, u64, u64, u64, String)> {
    out.lines()
        .map(|l| {
            // "<n> records, <body> bytes in <file> on disk (<ratio>x)\t<name>"
            let (counts, name) = l.split_once('\t').unwrap_or_else(|| panic!("no tab: {l}"));
            let f: Vec<&str> = counts.split_whitespace().collect();
            let n = |i: usize| f[i].trim_end_matches(',').parse::<u64>().unwrap_or_else(|_| panic!("{l}"));
            (name.to_owned(), n(0), n(2), n(5), f[8].to_owned())
        })
        .collect()
}

#[then(expr = "`deltapack log stats` reports its {int} records, the lines' bytes, the file's size and their ratio")]
async fn stats_of_packed(w: &mut Proxies, records: u64) {
    let file = w.packed.as_ref().unwrap();
    let stats = stats_of(&String::from_utf8(log(&["stats", file.to_str().unwrap()], b"").await).unwrap());
    let body: u64 = w.lines.iter().map(|l| l.len() as u64).sum();
    let disk = std::fs::metadata(file).unwrap().len();
    let ratio = format!("({:.1}x)", body as f64 / disk as f64);
    assert_eq!(stats, [(file.to_str().unwrap().to_owned(), records, body, disk, ratio)]);
}

#[then("`deltapack log stats` over every file reports each file, then their total")]
async fn stats_of_files(w: &mut Proxies) {
    let files = files_in(w.record_dir.as_ref().unwrap());
    let mut args = vec!["stats".to_owned()];
    args.extend(files.iter().map(|f| f.0.to_str().unwrap().to_owned()));
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let stats = stats_of(&String::from_utf8(log(&args, b"").await).unwrap());
    assert_eq!(stats.len(), files.len() + 1, "{stats:?}");
    let (records, body, disk) = stats[..files.len()].iter().fold((0, 0, 0), |a, s| (a.0 + s.1, a.1 + s.2, a.2 + s.3));
    assert_eq!(records, w.sent.len() as u64);
    assert_eq!(body, w.sent.iter().map(|s| s.body.len() as u64).sum::<u64>());
    assert_eq!(disk, files.iter().map(|f| f.1).sum::<u64>());
    let total = &stats[files.len()];
    assert_eq!((total.0.as_str(), total.1, total.2, total.3), ("(total)", records, body, disk));
    assert_eq!(total.4, format!("({:.1}x)", body as f64 / disk as f64));
}

#[then(
    "`deltapack log stats` over it, a copy whose name has a space and an empty file reports both copies and their total, names the empty file and fails"
)]
async fn stats_with_a_bad_file(w: &mut Proxies) {
    let packed = w.packed.clone().unwrap();
    let copy = packed.with_file_name("a copy.dpk");
    let empty = packed.with_file_name("empty.dpk");
    std::fs::copy(&packed, &copy).unwrap();
    std::fs::write(&empty, b"").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_deltapack"))
        .args(["log", "stats"])
        .args([&packed, &empty, &copy])
        .output()
        .await
        .unwrap();
    assert!(!out.status.success(), "a file it could not read fails the command");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(empty.to_str().unwrap()), "{stderr}");
    let stats = stats_of(&String::from_utf8(out.stdout).unwrap());
    let names: Vec<&str> = stats.iter().map(|s| s.0.as_str()).collect();
    assert_eq!(names, [packed.to_str().unwrap(), copy.to_str().unwrap(), "(total)"]);
    assert_eq!((stats[2].1, stats[2].2, stats[2].3), (stats[0].1 * 2, stats[0].2 * 2, stats[0].3 * 2));
}

#[then(expr = "`deltapack log show --attr gen_ai.usage.input_tokens={int}` gives the {int} spans of turn {int}")]
async fn show_by_attr(w: &mut Proxies, tokens: u64, n: usize, turn: u64) {
    let rows = show(w, &["--attr", &format!("gen_ai.usage.input_tokens={tokens}")]).await;
    assert_eq!(rows.len(), n, "{rows:?}");
    assert!(rows.iter().all(|r| r["attributes"]["gen_ai.usage.input_tokens"] == turn));
}

#[then(
    expr = "`deltapack log show --attr service.name=swe --attr gen_ai.usage.input_tokens={int}` gives the {int} spans of turn {int}"
)]
async fn show_by_two_attrs(w: &mut Proxies, tokens: u64, n: usize, turn: u64) {
    let rows = show(w, &["--attr", "service.name=swe", "--attr", &format!("gen_ai.usage.input_tokens={tokens}")]).await;
    assert_eq!(rows.len(), n, "{rows:?}");
    assert!(rows.iter().all(|r| r["attributes"]["gen_ai.usage.input_tokens"] == turn));
}

#[then("`deltapack log show --attr service.name=other` gives none")]
async fn show_by_absent_attr(w: &mut Proxies) {
    assert_eq!(show(w, &["--attr", "service.name=other"]).await, Vec::<serde_json::Value>::new());
}

#[then("`deltapack log show` read to its first line only ends with success and nothing on stderr")]
async fn show_head(w: &mut Proxies) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_deltapack"))
        .args(["log", "show", &recording(w)])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    assert!(lines.next_line().await.unwrap().is_some(), "a first line");
    drop(lines); // as `| head -1` does: the rest of the output has nowhere to go
    let out = tokio::time::timeout(Duration::from_secs(30), child.wait_with_output()).await.unwrap().unwrap();
    assert!(out.status.success(), "{}", out.status);
    assert_eq!(String::from_utf8_lossy(&out.stderr), "");
}

#[then(
    expr = "`deltapack log estimate --usd-per-gb {float}` on the same lines reports their {int} records, their bytes, the packed file's size and the cost of each"
)]
async fn estimate(w: &mut Proxies, usd: f64, records: u64) {
    let out = String::from_utf8(log(&["estimate", "--usd-per-gb", &usd.to_string()], &jsonl(&w.lines)).await).unwrap();
    let body: u64 = w.lines.iter().map(|l| l.len() as u64).sum();
    let packed = std::fs::metadata(w.packed.as_ref().unwrap()).unwrap().len();
    let gb = |b: u64| b as f64 / 1e9 * usd;
    let expected = format!(
        "{records} records, {body} bytes, packed in {packed} ({:.1}x); at ${usd} per GB: ${:.6} as is, ${:.6} packed\n",
        body as f64 / packed as f64,
        gb(body),
        gb(packed)
    );
    assert_eq!(out, expected);
}

#[then("`deltapack log unpack` over every file gives every batch in order")]
async fn unpack_files(w: &mut Proxies) {
    let files: Vec<String> =
        files_in(w.record_dir.as_ref().unwrap()).iter().map(|f| f.0.to_str().unwrap().to_owned()).collect();
    let args: Vec<&str> = std::iter::once("unpack").chain(files.iter().map(String::as_str)).collect();
    let expected: Vec<u8> = w.sent.iter().flat_map(|s| [&s.body[..], b"\n"].concat()).collect();
    assert!(log(&args, b"").await == expected, "every batch, in order");
}

#[then("`deltapack log ls` over every file lists every batch, each line led by its file")]
async fn ls_files(w: &mut Proxies) {
    let files: Vec<String> =
        files_in(w.record_dir.as_ref().unwrap()).iter().map(|f| f.0.to_str().unwrap().to_owned()).collect();
    let args: Vec<&str> = std::iter::once("ls").chain(files.iter().map(String::as_str)).collect();
    let out = String::from_utf8(log(&args, b"").await).unwrap();
    assert_eq!(out.lines().count(), w.sent.len(), "{out}");
    let mut last: Option<(String, u64)> = None;
    for line in out.lines() {
        let (file, rest) = line.split_once('\t').unwrap_or_else(|| panic!("no tab: {line}"));
        assert!(files.iter().any(|f| f == file), "{line}");
        let index: u64 = rest.split(' ').next().unwrap().parse().unwrap();
        // indexes count from 0 in each file, files in the order given
        match &last {
            Some((f, i)) if f == file => assert_eq!(index, i + 1),
            _ => assert_eq!(index, 0),
        }
        last = Some((file.to_owned(), index));
    }
}

#[given(expr = "a deltapack collector recording to a directory, a directory per tenant by {string}")]
async fn a_tenant_collector(w: &mut Proxies, header: String) {
    w.start_collector(&["--tenant-header", &header]).await;
}

#[given(
    expr = "a deltapack collector recording to a directory, a directory per tenant by {string}, for at most {int} tenants"
)]
async fn a_capped_tenant_collector(w: &mut Proxies, header: String, max: usize) {
    w.start_collector(&["--tenant-header", &header, "--max-tenants", &max.to_string()]).await;
}

impl Proxies {
    /// Posts batch `turn` as `tenant` (none: no header), and remembers what went where.
    async fn post_as(&mut self, tenant: Option<&str>, turn: usize) {
        let body = batch(turn);
        let to = self.server.as_ref().unwrap().addr;
        let mut headers = vec![("content-type", "application/json")];
        if let Some(t) = tenant {
            headers.push(("x-tenant", t));
        }
        self.post_batch(to, "/v1/traces", body.clone(), &headers).await;
        assert_eq!(self.posted.as_ref().unwrap().0, 200);
        self.sent.push(Sent { body, response: Bytes::new(), first_of_conversation: false, relayed: 0 });
        self.lines.push(Bytes::from(tenant.unwrap_or("").to_owned())); // who sent each, by index
    }

    /// The batches stored in `name` under the record directory, in order.
    async fn stored_in(&self, name: &str) -> Vec<Bytes> {
        let dir = self.record_dir.as_ref().unwrap().join(name);
        bodies_of(&files_in(&dir).into_iter().map(|f| f.0).collect::<Vec<_>>()).await
    }

    fn sent_by(&self, tenant: &str) -> Vec<Bytes> {
        self.sent
            .iter()
            .zip(&self.lines)
            .filter(|(_, t)| &t[..] == tenant.as_bytes())
            .map(|(s, _)| s.body.clone())
            .collect()
    }
}

#[when(expr = "an exporter posts {int} batches as tenant {string}, {int} as tenant {string} and {int} as no tenant")]
async fn post_tenants(w: &mut Proxies, a: usize, ta: String, b: usize, tb: String, none: usize) {
    let mut turn = 0;
    for (n, t) in [(a, Some(ta.as_str())), (b, Some(tb.as_str())), (none, None)] {
        for _ in 0..n {
            turn += 1;
            w.post_as(t, turn).await;
        }
    }
}

#[then(expr = "the directory {string} holds acme's {int} batches in order")]
async fn holds_acme(w: &mut Proxies, dir: String, n: usize) {
    let sent = w.sent_by("acme");
    assert_eq!(sent.len(), n);
    assert_eq!(w.stored_in(&dir).await, sent);
}

#[then(expr = "the directory {string} holds globex's {int} batches in order")]
async fn holds_globex(w: &mut Proxies, dir: String, n: usize) {
    let sent = w.sent_by("globex");
    assert_eq!(sent.len(), n);
    assert_eq!(w.stored_in(&dir).await, sent);
}

#[then(expr = "the directory {string} holds the batch that named no tenant")]
async fn holds_none(w: &mut Proxies, dir: String) {
    assert_eq!(w.stored_in(&dir).await, w.sent_by(""));
}

#[then(expr = "the collector reports {int} records stored")]
async fn reports_records(w: &mut Proxies, n: u64) {
    assert_eq!(w.metric(&w.server, "deltapack_recorded_total").await, n);
}

#[when(
    expr = "an exporter posts a batch as each of the tenants {string}, {string}, {string}, {string} and {int} letters"
)]
async fn post_bad_tenants(w: &mut Proxies, a: String, b: String, c: String, d: String, long: usize) {
    let long = "x".repeat(long);
    for (turn, t) in [a.as_str(), &b, &c, &d, &long].into_iter().enumerate() {
        w.post_as(Some(t), turn + 1).await;
    }
}

#[then(expr = "the directory {string} holds those {int} batches in order")]
async fn holds_all(w: &mut Proxies, dir: String, n: usize) {
    let stored = w.stored_in(&dir).await;
    assert_eq!(stored.len(), n);
    assert!(stored.iter().eq(w.sent.iter().map(|s| &s.body)));
}

#[then(expr = "the record directory holds only the directory {string}")]
async fn holds_only(w: &mut Proxies, dir: String) {
    let names: Vec<String> = std::fs::read_dir(w.record_dir.as_ref().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(names, [dir]);
    let parent = w.record_dir.as_ref().unwrap().parent().unwrap();
    assert!(!parent.join("evil").exists() && !parent.join("_invalid").exists(), "nothing outside it");
}

#[when(expr = "an exporter posts a batch as each of the tenants {string}, {string} and {string}")]
async fn post_three_tenants(w: &mut Proxies, a: String, b: String, c: String) {
    for (turn, t) in [a, b, c].iter().enumerate() {
        w.post_as(Some(t), turn + 1).await;
    }
}

#[then(expr = "the record directory holds the directories {string}, {string} and {string}")]
async fn holds_three(w: &mut Proxies, a: String, b: String, c: String) {
    let mut names: Vec<String> = std::fs::read_dir(w.record_dir.as_ref().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, [a, b, c]);
}

#[then(expr = "the directory {string} holds tenant c's batch")]
async fn holds_c(w: &mut Proxies, dir: String) {
    assert_eq!(w.stored_in(&dir).await, w.sent_by("c"));
}

#[tokio::main]
async fn main() {
    Proxies::cucumber().fail_on_skipped().run_and_exit("tests/acceptance/features").await;
}
