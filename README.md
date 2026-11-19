# agentwire

**Send each LLM request as a delta against the previous one.** Agents re-send the whole
conversation on every turn, so request *k+1* is request *k* plus a few hundred bytes. AWD1 lets the
client send only those bytes, and lets the server rebuild the exact request before its handler
sees it.

| A 6 MiB request that… | goes on the wire as |
|---|---:|
| adds one turn to the previous request | 141 bytes |
| also edits the system prompt at the top | 748 bytes |

It is a plain HTTP mechanism (headers plus a `content-encoding`), not a new transport:

- **The server is the source of truth.** A client deltas only against a body the server confirmed
  it kept (`awd1-kept`). A server that does not support AWD1 never confirms, so the client keeps
  sending normal requests. A server that lost a base answers `409`, and the client resends once, in
  full.
- **Byte-exact, or refused.** The rebuilt body must hash to the SHA-256 the client sent. The handler
  receives the original bytes, `content-length` and all, so request signing and provider prompt
  caching are unaffected.
- **Stateless on the wire.** Bases are named by content hash, not by session or connection. Any
  request can be retried.

The wire format is specified in [SPEC.md](SPEC.md).

> **Status: 0.1.** The wire format is version 1. The Rust library and the two proxies are complete
> and tested (below). Not built yet: bindings for Python, TypeScript and Go, and a base store shared
> across server replicas (use sticky routing; see [Deploying](#deploying)).

## Where it goes

AWD1 is a codec for a client and a server to embed, the way they embed gzip:

| You control | Use |
|---|---|
| **Both ends** (your agent and your API, your agents talking to each other, your inference server) | The library: [`Client`](src/client.rs) on the sending side, [`Awd1Layer`](src/server.rs) (a `tower` layer) on the receiving side. |
| **Only the client** (calling Anthropic, OpenAI, …) | `agentwire server` as a reverse proxy near the provider. It is the same layer in front of a forwarder. |
| **Neither** (a CLI tool such as Claude Code or Codex, calling through `agentwire server`) | `agentwire client` on the tool's machine. Point the tool's base URL at it. |

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

## The proxies

```bash
cargo install agentwire --features proxy
# or: docker run ghcr.io/aperiodic-io/agentwire server --upstream https://api.anthropic.com
```

**`agentwire server`**: AWD1 in front of an HTTP API.

```bash
agentwire server --listen 0.0.0.0:8080 --upstream https://api.anthropic.com --metrics 0.0.0.0:9464
```

| Flag | Env | Default | |
|---|---|---|---|
| `--upstream` | `AGENTWIRE_UPSTREAM` | (required) | The API to forward to. Its path is kept: `https://gw/x` + `/v1/messages` → `https://gw/x/v1/messages`. |
| `--listen` | `AGENTWIRE_LISTEN` | `0.0.0.0:8080` | |
| `--store-bytes` | `AGENTWIRE_STORE_BYTES` | 1 GiB | Memory for kept bases, all tenants together. The least recently used goes first. |
| `--ttl-seconds` | `AGENTWIRE_TTL_SECONDS` | 1800 | A base unused this long is dropped. |
| `--max-body` | `AGENTWIRE_MAX_BODY` | 64 MiB | The largest body kept or rebuilt. |
| `--scope-header` | `AGENTWIRE_SCOPE_HEADERS` | `authorization,x-api-key` | The headers that identify a tenant. Bases never cross them. |
| `--metrics` | `AGENTWIRE_METRICS` | off | Prometheus text at `http://<addr>/metrics`, on its own port. |

Credentials pass through to the upstream. The proxy never stores or logs them, nor any body.

**`agentwire client`**: for tools you cannot change. They post plain requests to it, and it sends
deltas on.

```bash
agentwire client --listen 127.0.0.1:8787 --upstream https://agentwire.example.com
ANTHROPIC_BASE_URL=http://127.0.0.1:8787 claude      # or OPENAI_BASE_URL=... for OpenAI clients
```

Flags: `--upstream`, `--listen` (`127.0.0.1:8787`), `--bases` (8: about one per conversation in
flight), `--max-body` (64 MiB), `--metrics`.

### Metrics

| Metric | |
|---|---|
| `agentwire_server_deltas_total`, `agentwire_server_delta_bytes_total`, `agentwire_server_rebuilt_bytes_total` | Deltas received, their bytes on the wire, and the bytes they rebuilt to. |
| `agentwire_server_kept_total`, `agentwire_server_store_bytes` | Bodies kept, and memory held now. |
| `agentwire_server_refused_total` | Deltas refused (unknown base, corrupt, too large). The client resent each in full. |
| `agentwire_client_requests_total`, `agentwire_client_deltas_total`, `agentwire_client_resent_total` | |
| `agentwire_client_body_bytes_total`, `agentwire_client_wire_bytes_total` | Request bytes, and what was sent for them. |

## Deploying

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

Hashing the rebuilt body is most of the rebuild time. Where AWD1 sits among other ways to compress
agent traffic, and why, is in [docs/research/](docs/research/compression-research.md).

## Security

AWD1 relies on TLS and on your API's authentication. Bases are per tenant, so a client can never
use, or learn of, another tenant's base. Compressing secrets together with attacker-influenced
content has a length side channel, as with any compression. See [SPEC.md §5](SPEC.md#5-scope-and-security)
and [SECURITY.md](SECURITY.md).

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
