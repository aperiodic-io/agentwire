//! Acceptance: `agentwire client` and `agentwire server` as real processes, between a tool (this
//! test) and a recording upstream API, with a byte-counting TCP relay on the network between them.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agentwire::{KEEP, KEPT, codec, hex};
use bytes::Bytes;
use cucumber::{World, given, then, when};
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
                    seen.lock().unwrap().push((parts.headers, body));
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

/// Starts `agentwire <args>` on a free port and reads the addresses it logs.
async fn spawn(args: &[&str]) -> Proc {
    let mut child = Command::new(env!("CARGO_BIN_EXE_agentwire"))
        .args(args)
        .args(["--listen", "127.0.0.1:0", "--metrics", "127.0.0.1:0"])
        .env("RUST_LOG", "agentwire=debug")
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

#[given("an agentwire server in front of it")]
async fn a_server(w: &mut Proxies) {
    w.start_server().await;
}

#[given("an agentwire client in front of that server")]
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

#[when("the agentwire server restarts")]
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
    assert_eq!(w.metric(&w.server, "agentwire_server_deltas_total").await, deltas);
    assert_eq!(w.metric(&w.server, "agentwire_server_refused_total").await, 0);
    let (wire, rebuilt) = (
        w.metric(&w.server, "agentwire_server_delta_bytes_total").await,
        w.metric(&w.server, "agentwire_server_rebuilt_bytes_total").await,
    );
    let later: u64 = w.sent.iter().filter(|s| !s.first_of_conversation).map(|s| s.body.len() as u64).sum();
    assert_eq!(rebuilt, later, "the deltas rebuilt every request after the first");
    assert!(wire > 0 && wire * 50 < rebuilt, "{wire} delta bytes for {rebuilt}");
    assert!(w.metric(&w.server, "agentwire_server_store_bytes").await > 0);
}

#[then(expr = "the client reports sending under {int}% of its requests' bytes")]
async fn client_reports(w: &mut Proxies, percent: u64) {
    let body = w.metric(&w.client, "agentwire_client_body_bytes_total").await;
    let wire = w.metric(&w.client, "agentwire_client_wire_bytes_total").await;
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
    w.last = Some(w.post_to_server(delta, &[("content-encoding", "awd1")]).await);
}

#[then("the upstream received every request byte for byte")]
async fn byte_for_byte(w: &mut Proxies) {
    let recorded = w.recorded();
    assert_eq!(recorded.len(), w.sent.len());
    for (i, ((headers, body), sent)) in recorded.iter().zip(&w.sent).enumerate() {
        assert_eq!(body, &sent.body, "request {i}");
        assert_eq!(headers["content-length"], body.len().to_string());
        assert!(headers.keys().all(|k| !k.as_str().starts_with("awd1")), "request {i}: {headers:?}");
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
    assert_eq!(w.metric(&w.client, "agentwire_client_resent_total").await, n);
}

#[then("the response has no awd1 headers")]
async fn no_awd1_headers(w: &mut Proxies) {
    let (status, headers) = w.last.as_ref().unwrap();
    assert_eq!(*status, 200);
    assert!(headers.keys().all(|k| !k.as_str().starts_with("awd1")), "{headers:?}");
}

#[then(expr = "the server answers {int} with the awd1-error {string}")]
async fn refused(w: &mut Proxies, status: u16, code: String) {
    let (got, headers) = w.last.as_ref().unwrap();
    assert_eq!((*got, headers["awd1-error"].to_str().unwrap()), (status, code.as_str()));
}

#[then("the upstream received only the base")]
async fn only_the_base(w: &mut Proxies) {
    let recorded = w.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].1, "base");
}

/// A fresh directory under the system's temp dir, for this process only.
fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("agentwire-acceptance-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// `agentwire log <args>`, its stdout; it must succeed.
async fn log(args: &[&str], stdin: &[u8]) -> Vec<u8> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_agentwire"))
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
    assert!(out.status.success(), "agentwire log {args:?}: {}", out.status);
    out.stdout
}

#[given("an agentwire server in front of it, recording to a directory")]
async fn a_recording_server(w: &mut Proxies) {
    w.record_dir = Some(scratch("record"));
    w.start_server().await;
}

/// The one file the server records to.
fn recording(w: &Proxies) -> String {
    let files: Vec<_> = std::fs::read_dir(w.record_dir.as_ref().unwrap()).unwrap().map(|e| e.unwrap().path()).collect();
    assert_eq!(files.len(), 1, "{files:?}");
    assert_eq!(files[0].extension().unwrap(), "awl");
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
        assert_eq!(fields[4..], ["POST", "/v1/messages"], "{line}");
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

#[when("they are packed with `agentwire log pack`")]
async fn pack(w: &mut Proxies) {
    let file = scratch("pack").join("spans.awl");
    log(&["pack", file.to_str().unwrap()], &jsonl(&w.lines)).await;
    w.packed = Some(file);
}

fn jsonl(lines: &[Bytes]) -> Vec<u8> {
    lines.iter().flat_map(|l| [&l[..], b"\n"].concat()).collect()
}

#[then("`agentwire log unpack` gives back the same lines")]
async fn unpack(w: &mut Proxies) {
    let file = w.packed.as_ref().unwrap().to_str().unwrap();
    assert!(log(&["unpack", file], b"").await == jsonl(&w.lines));
}

#[then(expr = "`agentwire log get` gives back span {int} alone")]
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

#[tokio::main]
async fn main() {
    Proxies::cucumber().fail_on_skipped().run_and_exit("tests/acceptance/features").await;
}
