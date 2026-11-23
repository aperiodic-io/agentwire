# agentwire

**A compression codec for LLM logs and traces. About 100× smaller, byte-exact, and any record
reads back on its own.**

Every LLM call is logged with its whole prompt, and every prompt is the previous prompt plus one
turn. A 200-step agent run writes its system prompt and tool schemas into your log 200 times, its
first tool result 199 times, its second 198 times. Most of what a gateway log, a trace store or an
audit archive holds is copies of what it already has.

```
what your log holds                                 what agentwire stores

span 1  [system][tools][turn 1]                     [system][tools][turn 1]
span 2  [system][tools][turn 1][turn 2]             + [turn 2]
span 3  [system][tools][turn 1][turn 2][turn 3]     + [turn 3]
```

agentwire stores each record as a delta against the earlier record it shares the most with, so a
record costs about its new bytes. Here are 7,343 OTel GenAI spans from 300 public SWE-agent runs,
16 traces in flight, 337 MB of JSON lines:

| Stored as | Size | Ratio |
|---|---:|---:|
| zstd-1 in 1 MiB blocks, arrival order (how a column store compresses) | 34.6 MB | 9.7× |
| messages deduplicated per trace, then zstd-1 blocks | 14.6 MB | 23× |
| **agentwire, one record per span** | **3.2 MB** | **105×** |

A block compressor loses here because it only sees one block. With 16 agents running, the previous
copy of a prompt usually sits in a different block, and a late prompt is larger than a block
anyway. agentwire looks for the previous copy by record, whatever its size.

At that ratio a terabyte of spans is under 10 GB. You can afford to keep every prompt in full,
without sampling or truncating, and still pull any single request out in milliseconds.

## Try it on your own logs

One JSON object per line in, an `.awl` file out. No service to run and nothing to change.

```bash
cargo install agentwire --features proxy
```

```bash
agentwire log pack spans.awl --window 32 < exported-spans.jsonl
```

```bash
ls -l exported-spans.jsonl spans.awl
```

```bash
agentwire log unpack spans.awl | cmp - exported-spans.jsonl    # silent: every byte is back
```

`--window` is how many traces or conversations are interleaved in the file (default 16).

## Five ways to use it

| You have | Use | What changes in your stack |
|---|---|---|
| An OpenTelemetry exporter feeding Langfuse, a collector or any OTLP backend | [`agentwire collect --forward`](#1-in-front-of-your-tracing-backend) | one endpoint variable |
| A gateway with an HTTP log plugin (Kong, LiteLLM) | [`agentwire collect`](#2-as-your-gateways-log-sink) | one plugin setting |
| No log export at all, or calls straight to a provider | [`agentwire server --record-dir`](#3-as-a-recording-proxy) | the base URL |
| Files of exported spans or request logs | [`agentwire log pack`](#4-on-files-you-already-have) | nothing |
| Your own ingest path or notebook | [the Rust or Python library](#5-embedded-in-rust-or-python) | a few lines |

All five write the same AWL1 files, and [`agentwire log`](#reading-it-back) and both libraries read
them all.

### 1. In front of your tracing backend

Keep the backend you query. Put `agentwire collect` between the exporter and it: every OTLP batch
is stored in full on your disk, then forwarded as received. The backend can sample, truncate or
expire what it likes, and you still hold every span.

```bash
agentwire collect --record-dir /var/log/agentwire --forward https://cloud.langfuse.com/api/public/otel
```

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf   # or grpc, with http://127.0.0.1:4317
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Basic $(echo -n "$LANGFUSE_PUBLIC_KEY:$LANGFUSE_SECRET_KEY" | base64 -w0)"
```

It takes OTLP traces, logs and metrics over HTTP (`:4318`) and gRPC (`:4317`). Your exporter's
headers are forwarded and never stored. Without `--forward` it is a plain sink that answers OTLP's
empty success.

gRPC arrives as HTTP/2 without TLS, as `insecure` exporters send it; put a proxy in front for TLS.
`--forward` is an OTLP/HTTP endpoint: each gRPC Export goes on to it as OTLP/HTTP protobuf
(`/v1/traces`, `/v1/logs` or `/v1/metrics`) and its answer comes back as the gRPC status (429 and
502–504 as `UNAVAILABLE`, which exporters retry). So a gRPC exporter can feed a backend that takes
only OTLP/HTTP, such as Langfuse.

### 2. As your gateway's log sink

Anything that can POST a log entry can log to `agentwire collect`. The path is yours to choose and
is stored with each record.

```bash
agentwire collect --record-dir /var/log/agentwire
```

**LiteLLM**: the `generic_api` callback, with `GENERIC_LOGGER_ENDPOINT=http://agentwire-collect:4318/litellm`.

```yaml
litellm_settings:
  callbacks: ["generic_api"]
```

**Kong AI Gateway**: log payloads from `ai-proxy` and send each entry with `http-log`. Agent
prompts are larger than Kong's defaults allow: raise `max_request_body_size`, and the body buffer
with `KONG_NGINX_HTTP_CLIENT_BODY_BUFFER_SIZE=16m` (a body Kong spills to a temp file is answered
400, "request body doesn't contain valid prompts").

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

### 3. As a recording proxy

For a gateway that exports no logs (Portkey), or for calls that go straight to a provider, let
`agentwire server` record what it forwards. Each request body is on disk before it reaches the
upstream, so the file is a record of what the model was asked. If a write fails, the error is
logged and the request is still forwarded.

```bash
agentwire server --upstream https://api.anthropic.com --record-dir /var/log/agentwire
```

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8080 claude
```

API keys pass straight through. Headers are never written to the file.

### 4. On files you already have

`agentwire log pack` and `unpack`, as in [Try it](#try-it-on-your-own-logs): exported spans, a
gateway's JSON-lines log, an eval run's requests. Pack before you ship to object storage and the
bucket holds about a hundredth of the bytes.

### 5. Embedded, in Rust or Python

Rust: `agentwire::log::Writer` appends, `agentwire::log::Reader` indexes and reads.

```rust
let mut log = agentwire::log::Writer::new(std::io::BufWriter::new(file), 16)?; // window
log.append(b"2026-11-20T14:15:00Z trace=ab12", body)?;                         // meta, body
```

Python ([python/](python/README.md)): the same files, from an ingest path or a notebook. `append`
and reads release the GIL.

```python
import agentwire

with agentwire.Writer("spans.awl", window=16) as w:
    w.append(span_bytes, meta=b"trace=ab12")

for meta, body in agentwire.Reader("/var/log/agentwire/1763650000000-4242-000000.awl"):
    ...
```

### Reading it back

```bash
agentwire log ls /var/log/agentwire/1763650000000-4242-000000.awl       # index, base, bytes stored, meta
```

```bash
agentwire log get /var/log/agentwire/1763650000000-4242-000000.awl 17 > request-17.json
```

`get` writes one record byte for byte, which for a recorded request is the exact body the provider
received: post it again to reproduce a failure, or feed a day of them to an eval. `unpack` writes
them all. `collect` and `server` stamp each record with its time (Unix ms), method, path, content
type and the encoding the body is still in.

## What you can rely on

- It is lossless. Every record carries the SHA-256 of its body and is checked when read. A file
  that does not rebuild what it names is an error, never wrong bytes.
- Any record reads on its own, in 64 ms on the benchmark above, without unpacking the file. The
  chain of deltas behind a record is bounded.
- A crash loses at most the record being written. A file that ends mid-record still holds every
  record before it, and each record is flushed before its request goes on.
- Files are self-contained and rotate at `--rotate-bytes` (1 GiB). A restart starts a new file and
  never appends to or replaces an old one, so shipping or deleting old files is a `mv` or an `rm`
  on your schedule.
- Gzip bodies are stored decoded, so the delta sees the content. Headers, and so credentials, are
  never stored.
- Recording never takes your traffic down: a failed write is counted in
  `agentwire_record_failures_total`, and the request goes on.
- The format is small and public ([SPEC.md §7](SPEC.md#7-awl1-a-log-of-bodies-stored-as-deltas)):
  a file of Zstandard frames, each behind a few varints.

### Measured end to end

300 public SWE-agent runs, 16 traces in flight, sent by the OpenTelemetry Python SDK's OTLP
exporter (protobuf and gzip) through `collect` into the real backends:

| Pipeline | What arrived | Stored |
|---|---|---:|
| SDK → `collect --forward` → Langfuse 3.227 | all 7,643 spans, in Langfuse and in the recording | **3.26 MB** of AWL1 for 338.6 MB of protobuf (104×). Langfuse itself kept 31.6 MiB in ClickHouse (ZSTD(3)) and 356 MB of raw events in S3 |
| SDK → `collect --forward` → OpenTelemetry Collector 0.115 | all 7,343 chat spans, the same span ids at both ends | 2.63 MB for 338.5 MB (129×) |
| SDK over gRPC (gzip) → `collect --forward` → OpenTelemetry Collector 0.115 (OTLP/HTTP) | all 7,343 chat spans | 3.28 MB for 338.6 MB (103×) |
| 12 conversations, 4 at a time → Kong 3.9 `ai-proxy` → `http-log` → `collect` | all 329 log entries, each with the request's full message list | 325 KB for 16.8 MB of Kong logs (52×) |

CI runs agentwire against the real images of **LiteLLM** 1.110, **Kong** 3.9 and **Portkey** 1.15
on every pull request ([tests/gateways/](tests/gateways/test_gateways.py)).

### What it is not

- A query engine. Records are read by position, and `meta` is yours to index. Keep your tracing
  backend for search and dashboards, and agentwire for the full bodies behind them.
- A general log compressor. The gain comes from records that repeat earlier records, which is what
  LLM requests and spans do. On logs without that shape, expect what zstd gives.
- Fast enough for everything. One record per span writes at about 12 MB/s on one core, because a
  span is tried against several candidate bases. OTLP batches usually delta against the batch
  before them, one encode each.
- Encrypted. A file holds its records in the clear: protect it as you would the requests. Keep one
  file per tenant where a tenant's records must be deletable on their own.

## How it works

Each record is a delta against one of the last `window` records: it copies the unchanged start and
end of that record and carries the edited middle as one Zstandard frame, which may reference the
old middle, so content that moved is still found. A record with nothing to delta against is stored
whole, as a delta against the empty body. The writer picks the base by trying the candidates, and
restarts a chain with a whole record before reading one would mean rebuilding 4,096 records or
1 GiB.

## The same codec on the wire: AWD1

The delta that shrinks a log also shrinks the request. An agent uploads its whole conversation on
every call. With AWD1 the client sends each request as a delta against one the server already
holds, and the server rebuilds the exact body before your handler, or the provider, sees it. It is
plain HTTP: a pair of headers and a `content-encoding` ([SPEC.md](SPEC.md)).

| A 6 MiB request that… | plain | zstd, per request¹ | **AWD1** |
|---|---:|---:|---:|
| adds one turn to the previous request | 6 MiB | ~1.6 MiB | **141 bytes** |
| also edits the system prompt at the top | 6 MiB | ~1.6 MiB | **748 bytes** |

¹ At the 3.8× that one zstd frame per request averaged on public agent trajectories
([research](docs/research/compression-research.md#tldr)).

This is for agents on slow or metered links, and for egress you pay for: on public agent traffic,
1 TB a month of request uploads at $0.09/GB costs $90 plain, $23.56 with per-request zstd and $1.43
with AWD1 ([research, §6](docs/research/compression-research.md#6-re-sent-context-windows-delta-patch-from)).
It does not change what the model sees. Token counts, your LLM bill and provider prompt caching are
as before, because the provider receives the same bytes.

Run `agentwire server` near the provider and `agentwire client` next to the tool. Neither needs
code changes:

```bash
agentwire server --listen 0.0.0.0:8080 --upstream https://api.anthropic.com
```

```bash
docker run -p 8080:8080 ghcr.io/aperiodic-io/agentwire server --upstream https://api.anthropic.com
```

```bash
agentwire client --upstream https://agentwire.example.com
```

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8787 claude
```

For OpenAI-style clients, point `OPENAI_BASE_URL` at the client and use
`--upstream https://api.openai.com` on the server. Put `agentwire client` in front of an OTLP
exporter and its batches cross the network to `collect` as deltas too. Add
`--metrics 127.0.0.1:9464` to either side to see the bytes saved.

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

- The rebuilt body must match the SHA-256 the client sent, or the request is refused and never
  forwarded. The handler gets the original bytes, `content-length` and all, so request signing and
  prompt caching are unaffected.
- The worst case is one normal request. A client deltas only against a body the server confirmed
  it kept (`awd1-kept`). A server without AWD1 never confirms, so the client sends plain requests.
  A server that lost a base (restart, eviction, another replica) answers `409`, and the client
  resends that one request in full.
- Bases are named by content hash, not by session or connection, so any request can be retried on
  any connection.
- Bases are scoped by the `authorization` / `x-api-key` header. A client cannot use, or learn of,
  another tenant's base.

| You control | Use |
|---|---|
| Both ends: your agent and your API, agents talking to each other, your inference server | The library: [`Client`](src/client.rs) on the sending side, [`Awd1Layer`](src/server.rs) (a `tower` layer) on the receiving side. |
| Only the client: you call Anthropic, OpenAI, … | `agentwire server` as a reverse proxy near the provider. It is the same layer in front of a forwarder. |
| Neither: a CLI tool such as Claude Code or Codex | `agentwire client` on the tool's machine, talking to an `agentwire server`. |

### The AWD1 library (Rust)

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

**`agentwire collect`**: a log sink that stores what is posted to it in AWL1 ([above](#1-in-front-of-your-tracing-backend)).

| Flag | Env | Default | |
|---|---|---|---|
| `--record-dir` | `AGENTWIRE_RECORD_DIR` | (required) | The new AWL1 file goes here. |
| `--forward` | `AGENTWIRE_FORWARD` | none | The backend each request is sent on to: as received, or a gRPC Export as OTLP/HTTP protobuf. Without one, each is acknowledged. |
| `--listen` | `AGENTWIRE_LISTEN` | `127.0.0.1:4318,127.0.0.1:4317` | Where to listen; each address takes OTLP/HTTP and OTLP/gRPC. On loopback: whoever reaches it can fill the disk. In a container, `0.0.0.0:4318,0.0.0.0:4317` on a private network. |
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

**0.1.** The wire format is version 1 and stable for 0.x. The Rust library, both proxies, `collect`
and the Python bindings for AWL1 are complete and tested. Not built yet: Python bindings for the
AWD1 client, bindings for TypeScript and Go, and a base store shared across server replicas (use
sticky routing; see [Deploying](#deploying)).

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
