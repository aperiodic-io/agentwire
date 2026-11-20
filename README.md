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
stores or logs credentials or bodies. Add `--metrics 127.0.0.1:9464` to either side to see the
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
