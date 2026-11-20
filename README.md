# agentwire

**Stop uploading the same conversation on every turn.**

An agent calls its LLM with the whole conversation every time: the system prompt, the tool
schemas, every file it read and every command it ran, plus one new turn at the end. Request *k+1*
is request *k* plus a few hundred bytes, and the agent uploads all of it again. Late in a long
session that is megabytes per call, hundreds of calls per task.

agentwire sends only the new bytes. The client sends each request as a delta against one the
server already holds, and the server rebuilds the exact original body before your handler (or the
provider) sees it.

| A 6 MiB request that… | plain | zstd, per request¹ | **agentwire** |
|---|---:|---:|---:|
| adds one turn to the previous request | 6 MiB | ~1.6 MiB | **141 bytes** |
| also edits the system prompt at the top | 6 MiB | ~1.6 MiB | **748 bytes** |

¹ At the 3.8× that one zstd frame per request averaged on public agent trajectories
([research](docs/research/compression-research.md#tldr)).

The protocol is called **AWD1**. It is plain HTTP, a pair of headers and a `content-encoding`, and
this repository is its Rust library and two ready-to-run proxies.

## Why use it

- **Agents on slow or metered links.** Laptops on hotel Wi-Fi, CI runners, edge devices, agents in
  another region from their model. Each turn uploads hundreds of bytes instead of megabytes.
- **Egress you pay for.** On public agent traffic, 1 TB/month of request uploads at $0.09/GB costs
  $90 plain, $23.56 with per-request zstd and $1.43 with AWD1
  ([research, §6](docs/research/compression-research.md#6-re-sent-context-windows-delta-patch-from)).
- **Long contexts are where it gains most.** Ordinary HTTP compression sees one request at a time
  and tops out around 4×; zstd's HTTP window stops at 8 MB. AWD1 deltas against the previous
  request whatever its size, so the bigger the context, the bigger the ratio: about 100× at 1 MiB,
  over 2,000× at 6 MiB.

What it does **not** do: change what the model sees. Token counts, your LLM bill and provider prompt
caching are exactly as before, because the provider receives the same bytes.

## Safe by construction

- **Byte-exact, or refused.** The rebuilt body must match the SHA-256 the client sent. The handler
  gets the original bytes, `content-length` and all, so request signing and prompt caching are
  unaffected. A delta that does not check out is never forwarded.
- **The worst case is one normal request.** A client deltas only against a body the server confirmed
  it kept (`awd1-kept`). A server without AWD1 never confirms, so the client just sends plain
  requests. A server that lost a base (restart, eviction, another replica) answers `409`, and the
  client resends that one request in full.
- **Stateless on the wire.** Bases are named by content hash, not by session or connection. Any
  request can be retried, on any connection.
- **Tenants never share bases.** Bases are scoped by the `authorization` / `x-api-key` header, so a
  client cannot use, or learn of, another tenant's base.
- **Heavily tested.** Property tests, acceptance tests of the real proxies, Kani proofs of the
  codec, fuzzing, and mutation testing on every pull request ([below](#development)).

## Quick start: Claude Code, Codex or any tool with a base URL

You need no code changes. Run `agentwire server` near the provider and `agentwire client` next to
the tool:

```bash
cargo install agentwire --features proxy
```

On a host in the provider's region:

```bash
agentwire server --listen 0.0.0.0:8080 --upstream https://api.anthropic.com
```

Or with Docker:

```bash
docker run -p 8080:8080 ghcr.io/aperiodic-io/agentwire server --upstream https://api.anthropic.com
```

On the machine running the agent:

```bash
agentwire client --upstream https://agentwire.example.com
```

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8787 claude
```

For OpenAI-style clients, point `OPENAI_BASE_URL` at the client and use
`--upstream https://api.openai.com` on the server. API keys pass straight through; neither proxy
stores or logs credentials, and bodies are stored only with `--record-dir` ([below](#storing-llm-calls-awl1)). Add `--metrics 127.0.0.1:9464` to either side to see the
bytes saved.

## How it works

```
client                                    server
  │  POST  (full body)  awd1-keep: ?1       │
  │ ──────────────────────────────────────▶ │  keeps the body
  │ ◀────────────────────────────────────── │  awd1-kept: <sha256>
  │                                         │
  │  POST  content-encoding: awd1           │
  │  (delta: base hash, target hash, edit)  │
  │ ──────────────────────────────────────▶ │  rebuilds, checks SHA-256,
  │                                         │  hands the original body on
```

A delta copies the unchanged start and end of the base, and carries the edited middle as one
Zstandard frame that may reference the old middle, so content that moved is still found. The full
wire format is in [SPEC.md](SPEC.md).

## Where it goes

AWD1 is a codec a client and a server embed, the way they embed gzip. Pick by what you control:

| You control | Use |
|---|---|
| **Both ends**: your agent and your API, agents talking to each other, your inference server | The library: [`Client`](src/client.rs) on the sending side, [`Awd1Layer`](src/server.rs) (a `tower` layer) on the receiving side. |
| **Only the client**: you call Anthropic, OpenAI, … | `agentwire server` as a reverse proxy near the provider. It is the same layer in front of a forwarder. |
| **Neither**: a CLI tool such as Claude Code or Codex | `agentwire client` on the tool's machine, talking to an `agentwire server`. |

## Storing logs and traces: AWL1

Gateways log every request for audit and replay, and tracing pipelines store every span, each with
the whole prompt. That is the same re-sent context, at rest. **AWL1** stores such a log as AWD1
deltas, each against an earlier record ([SPEC.md §7](SPEC.md#7-awl1-a-log-of-bodies-stored-as-deltas)).
A record costs about its new bytes, any record reads back on its own, and every one is checked by
SHA-256.

7,343 OTel-GenAI-style spans built from 300 public SWE-agent runs, 16 traces in flight, as JSON
lines (`agentwire log pack`, 337 MB):

| Stored as | Size | |
|---|---:|---:|
| zstd-1, 1 MiB blocks, arrival order (a column store's blocks) | 34.6 MB | 9.7× |
| messages deduplicated per trace, then zstd-1 blocks | 14.6 MB | 23× |
| **AWL1, one record per span** | **3.2 MB** | **105×** |

Any span reads back in 64 ms. One record per span writes at about 12 MB/s on one core: a span
whose own trace's last span is 16 records back is tried against the bases in between. OTLP batches
(what `collect` stores) usually delta against the batch before them, one encode each.

### Collecting: `agentwire collect`

A log sink for OTLP/HTTP traces and logs, and for any HTTP log plugin. It stores every request body
in AWL1, gzip decoded, with its time, method, path, content type and the encoding it is still in
(`-` for none; never a header). With `--forward` it then sends each request on, as received, to the
real backend. Without one it answers OTLP's empty success. Put `agentwire client` in front of an
exporter and the batches also cross the network as deltas.

```bash
agentwire collect --record-dir /var/log/agentwire                         # a sink on :4318
agentwire collect --record-dir /var/log/agentwire --forward https://cloud.langfuse.com/api/public/otel
```

**Langfuse (or any OTLP backend).** Point the exporter at the collector, keep its headers: they are
forwarded, never stored. OTLP over gRPC is not supported, so use `http/protobuf` or `http/json`.

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Basic $(echo -n "$LANGFUSE_PUBLIC_KEY:$LANGFUSE_SECRET_KEY" | base64 -w0)"
```

**Kong AI Gateway.** Log payloads from `ai-proxy`, and send each log entry to the collector with
`http-log`. Agent prompts are larger than Kong's defaults allow: raise `max_request_body_size`, and
the body buffer with `KONG_NGINX_HTTP_CLIENT_BODY_BUFFER_SIZE=16m` (a body Kong spills to a temp file
is answered 400, "request body doesn't contain valid prompts").

```yaml
plugins:
  - name: ai-proxy
    config:
      max_request_body_size: 16777216
      logging: { log_payloads: true, log_statistics: true }
      # route_type, auth, model: as you have them
  - name: http-log
    config:
      http_endpoint: http://agentwire-collect:4318/kong
      content_type: application/json
```

**Measured against the real thing** (300 public SWE-agent runs, 16 traces in flight, the
OpenTelemetry Python SDK's OTLP/HTTP exporter, protobuf and gzip):

| Pipeline | What arrived | Stored |
|---|---|---:|
| SDK → `collect --forward` → Langfuse 3.227 | all 7,643 spans, in Langfuse and in the recording | **3.26 MB** of AWL1 for 338.6 MB of protobuf (104×). Langfuse itself kept 31.6 MiB in ClickHouse (ZSTD(3)) and 356 MB of raw events in S3 |
| SDK → `collect --forward` → OpenTelemetry Collector 0.115 | all 7,343 chat spans, the same span ids at both ends | 2.63 MB for 338.5 MB (129×) |
| 12 conversations, 4 at a time → Kong 3.9 `ai-proxy` → `http-log` → `collect` | all 329 log entries, each with the request's full message list | 325 KB for 16.8 MB of Kong logs (52×) |

### Recording a proxy, packing files

**A gateway:** let `agentwire server` record what it forwards. Each request is on disk before it
reaches the upstream, with the same meta as `collect`. If a write fails, the error is logged and the
request is still forwarded.

```bash
agentwire server --upstream https://api.anthropic.com --record-dir /var/log/agentwire
agentwire log ls /var/log/agentwire/1763650000000-4242.awl      # index, base, bytes stored, meta
agentwire log get /var/log/agentwire/1763650000000-4242.awl 17 > request-17.json        # replay it
```

**A trace pipeline:** pack exported spans, one JSON object per line, and unpack them byte for byte.
`--window` is how many traces or conversations are interleaved in the export.

```bash
agentwire log pack spans.awl --window 32 < exported-spans.jsonl
agentwire log unpack spans.awl > spans.jsonl
```

**Embedded** (Rust): `agentwire::log::Writer` appends, `agentwire::log::Reader` indexes and reads.

```rust
let mut log = agentwire::log::Writer::new(std::io::BufWriter::new(file), 16)?;
log.append(b"2026-11-20T14:15:00Z trace=ab12", body)?; // meta, body
```

## The library (Rust)

```toml
[dependencies]
agentwire = "0.1"                                           # codec, client, server layer
# agentwire = { version = "0.1", default-features = false } # codec and client only
```

**Server.** Wrap your handler. Here it is axum, but any `tower` stack works:

```rust
use std::{sync::Arc, time::Duration};
use agentwire::{server::{Awd1Layer, Config}, store::Store};

let store = Arc::new(Store::new(1 << 30, Duration::from_secs(1800))); // 1 GiB of bases, 30 min idle TTL
let app = axum::Router::new()
    .route("/v1/messages", axum::routing::post(messages))
    .layer(Awd1Layer::new(store, Config::default()));
```

The handler receives each request as the client built it. Requests without AWD1 headers pass
through untouched and unbuffered.

**Client.** Wrap your HTTP call. One `Client` serves concurrent requests and conversations:

```rust
let client = agentwire::client::Client::new(4); // remembers 4 confirmed bases
let request = client.prepare(body);
let mut response = send(request.wire_body(), &request.headers()).await;
if request.is_delta() && response.headers().contains_key(agentwire::ERROR) {
    // the server no longer has the base; it did not forward the request
    response = send(request.body(), &request.plain_headers()).await;
}
client.confirm(&request, response.headers().get(agentwire::KEPT).and_then(|v| v.to_str().ok()));
```

## Proxy reference

**`agentwire server`**: AWD1 in front of an HTTP API.

| Flag | Env | Default | |
|---|---|---|---|
| `--upstream` | `AGENTWIRE_UPSTREAM` | (required) | The API to forward to. Its path is kept: `https://gw/x` + `/v1/messages` → `https://gw/x/v1/messages`. |
| `--listen` | `AGENTWIRE_LISTEN` | `0.0.0.0:8080` | |
| `--store-bytes` | `AGENTWIRE_STORE_BYTES` | 1 GiB | Memory for kept bases, all tenants together. The least recently used goes first. |
| `--ttl-seconds` | `AGENTWIRE_TTL_SECONDS` | 1800 | A base unused this long is dropped. |
| `--max-body` | `AGENTWIRE_MAX_BODY` | 64 MiB | The largest body kept or rebuilt. |
| `--scope-header` | `AGENTWIRE_SCOPE_HEADERS` | `authorization,x-api-key` | The headers that identify a tenant. Bases never cross them. |
| `--metrics` | `AGENTWIRE_METRICS` | off | Prometheus text at `http://<addr>/metrics`, on its own port. |
| `--record-dir` | `AGENTWIRE_RECORD_DIR` | off | Store every request body forwarded in a new AWL1 file here. Requests are then buffered (up to `--max-body`). |
| `--record-window` | `AGENTWIRE_RECORD_WINDOW` | 16 | Records a new one may delta against: about one per conversation in flight. |
| `--rotate-bytes` | `AGENTWIRE_ROTATE_BYTES` | 1 GiB | Start a new file once one reaches this size. |

**`agentwire collect`**: a log sink that stores what is posted to it in AWL1 ([above](#collecting-agentwire-collect)).

| Flag | Env | Default | |
|---|---|---|---|
| `--record-dir` | `AGENTWIRE_RECORD_DIR` | (required) | The new AWL1 file goes here. |
| `--forward` | `AGENTWIRE_FORWARD` | none | The backend each request is sent on to, as received. Without one, each is answered 200. |
| `--listen` | `AGENTWIRE_LISTEN` | `127.0.0.1:4318` | OTLP/HTTP's port, on loopback: whoever reaches it can fill the disk. In a container, `0.0.0.0:4318` on a private network. |
| `--record-window` | `AGENTWIRE_RECORD_WINDOW` | 16 | Records a new one may delta against: about one per trace or exporter interleaved. |
| `--rotate-bytes` | `AGENTWIRE_ROTATE_BYTES` | 1 GiB | Start a new file once one reaches this size. Every file reads on its own; deleting old ones (or shipping them to object storage) is yours to schedule. |
| `--max-body` | `AGENTWIRE_MAX_BODY` | 64 MiB | The largest body stored, after gzip decoding; larger ones are answered 413. |
| `--metrics` | `AGENTWIRE_METRICS` | off | |

**`agentwire client`**: for tools you cannot change. They post plain requests to it, and it sends
deltas on.

| Flag | Env | Default | |
|---|---|---|---|
| `--upstream` | `AGENTWIRE_UPSTREAM` | (required) | The `agentwire server` to send to. |
| `--listen` | `AGENTWIRE_LISTEN` | `127.0.0.1:8787` | |
| `--bases` | `AGENTWIRE_BASES` | 8 | Confirmed bases remembered: about one per conversation in flight. |
| `--max-body` | `AGENTWIRE_MAX_BODY` | 64 MiB | |
| `--metrics` | `AGENTWIRE_METRICS` | off | |

### Metrics

| Metric | |
|---|---|
| `agentwire_server_deltas_total`, `agentwire_server_delta_bytes_total`, `agentwire_server_rebuilt_bytes_total` | Deltas received, their bytes on the wire, and the bytes they rebuilt to. |
| `agentwire_server_kept_total`, `agentwire_server_store_bytes` | Bodies kept, and memory held now. |
| `agentwire_server_refused_total` | Deltas refused (unknown base, corrupt, too large). The client resent each in full. |
| `agentwire_client_requests_total`, `agentwire_client_deltas_total`, `agentwire_client_resent_total` | Requests, how many went as deltas, and how many were resent in full. |
| `agentwire_client_body_bytes_total`, `agentwire_client_wire_bytes_total` | Request bytes, and what was sent for them. Their ratio is your saving. |
| `agentwire_recorded_total`, `agentwire_recorded_body_bytes_total`, `agentwire_recorded_file_bytes_total` | Records stored (`--record-dir`, `collect`), their bytes, and the bytes of the files holding them. Their ratio is the storage saving. |
| `agentwire_record_failures_total` | Records that could not be written (disk full, I/O error). The request was still forwarded; the next record goes to a new file. |

### Deploying

- **Place `agentwire server` near the provider** (same region). The saving is on the hop from the
  client to it.
- **Replicas:** bases live in each process's memory. Route a tenant's requests to the same replica
  (consistent hashing on `authorization`). Otherwise a request may land where its base is not, and
  costs one full resend.
- **Restarts and deploys** drop the bases. Each conversation's next request is then sent in full.
- **Memory:** `--store-bytes`, plus up to two bodies per request in flight while it is rebuilt.
- **Shutdown:** on SIGTERM it stops accepting connections and lets requests in flight (including
  streams) finish.

## Performance

`codec::encode` and `apply` on one core, measured while another workload was running, so read
these as upper bounds:

| Context | Edit | Delta | Encode | Rebuild + SHA-256 check |
|---:|---|---:|---:|---:|
| 1 MiB | new turn | 139 B | 0.1 ms | 1.1 ms |
| 6 MiB | new turn | 141 B | 1.0 ms | 12 ms |
| 6 MiB | top edit + new turn | 748 B | 7.9 ms | 13 ms |
| 32 MiB | top edit + new turn | 3.7 KB | 47 ms | 59 ms |

Hashing the rebuilt body is most of the rebuild time. How AWD1 compares with other ways to compress
agent traffic (per-request zstd, streaming windows, dictionaries, `--patch-from`), measured on public
agent trajectories, is in [docs/research/](docs/research/compression-research.md).

## Security

AWD1 relies on TLS and on your API's authentication. Bases are per tenant, so a client can never
use, or learn of, another tenant's base. Compressing secrets together with attacker-influenced
content has a length side channel, as with any compression. See [SPEC.md §5](SPEC.md#5-scope-and-security)
and [SECURITY.md](SECURITY.md).

## Status

**0.1.** The wire format is version 1 and stable for 0.x. The Rust library and both proxies are
complete and tested. Not built yet: bindings for Python, TypeScript and Go, and a base store shared
across server replicas (use sticky routing; see [Deploying](#deploying)).

## Development

[AGENTS.md](AGENTS.md) is how changes are made here: test first.

```bash
cargo test --all-features                       # unit, property, integration and acceptance tests
cargo test --all-features --test acceptance     # the proxies as processes (tests/acceptance/features/)
cargo kani --lib                                # the codec's proofs (Kani 0.68)
cd fuzz && cargo +nightly fuzz run delta        # or roundtrip
cargo mutants                                   # every mutant must be caught (.cargo/mutants.toml)
```

| Layer | What it checks |
|---|---|
| Unit and property tests (`src/*.rs`) | Wire layout, every refusal path, any base and target round-tripping, corruption never rebuilding a wrong body, the store's budget and LRU, base choice across interleaved conversations. |
| Integration (`tests/integration.rs`) | Client and server layer together, in process: what the handler receives, tenant isolation, limits. |
| Acceptance (`tests/acceptance/`) | Both proxies as real processes, with a byte-counting relay between them, against a recording upstream: byte-exact forwarding, streamed responses, wire size, server restarts, API keys, plain clients, corrupt deltas. |
| Formal (`cargo kani`) | Over all inputs of bounded size: varints are canonical and round-trip; `parse` never panics and accepts only headers that fit; no header can make `apply`'s slicing go out of bounds; prefix and suffix scans are the longest and never overrun. |
| Fuzzing (`fuzz/`) | Any bytes as a delta against any base, and any base and target round-tripping, under libFuzzer. |
| Mutation (`cargo mutants`) | Every mutant in a pull request's diff must fail a test. |

CI runs all of these on pull requests ([.github/workflows/ci.yml](.github/workflows/ci.yml)),
with every mutant and longer fuzzing on demand ([nightly.yml](.github/workflows/nightly.yml), run from the
Actions tab; its schedule is commented out). A `vX.Y.Z` tag publishes the crate, the image, and
a GitHub release ([release.yml](.github/workflows/release.yml)).

## License

Apache-2.0. See [LICENSE](LICENSE).
