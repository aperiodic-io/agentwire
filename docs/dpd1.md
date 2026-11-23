# DPD1: the codec on the wire

The delta that shrinks a log also shrinks the request. An agent uploads its whole conversation on
every call. With DPD1 the client sends each request as a delta against one the server already
holds, and the server rebuilds the exact body before your handler, or the provider, sees it. It is
plain HTTP: a pair of headers and a `content-encoding` ([SPEC.md](../SPEC.md)).

| A 6 MiB request that… | plain | zstd, per request¹ | **DPD1** |
|---|---:|---:|---:|
| adds one turn to the previous request | 6 MiB | ~1.6 MiB | **141 bytes** |
| also edits the system prompt at the top | 6 MiB | ~1.6 MiB | **748 bytes** |

¹ At the 3.8× that one zstd frame per request averaged on public agent trajectories, measured
before the 0.1 release ([the research report](https://github.com/aperiodic-io/deltapack/blob/907d44e/docs/research/compression-research.md), kept in git history).

This is for agents on slow or metered links, and for egress you pay for: on public agent traffic,
1 TB a month of request uploads at $0.09/GB costs $90 plain, $23.56 with per-request zstd and $1.43
with DPD1, by the same measurements.
It does not change what the model sees. Token counts, your LLM bill and provider prompt caching are
as before, because the provider receives the same bytes.

Run `deltapack server` near the provider and `deltapack client` next to the tool. Neither needs
code changes:

```bash
deltapack server --listen 0.0.0.0:8080 --upstream https://api.anthropic.com
```

```bash
docker run -p 8080:8080 ghcr.io/aperiodic-io/deltapack server --upstream https://api.anthropic.com
```

```bash
deltapack client --upstream https://deltapack.example.com
```

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8787 claude
```

For OpenAI-style clients, point `OPENAI_BASE_URL` at the client and use
`--upstream https://api.openai.com` on the server. Put `deltapack client` in front of an OTLP
exporter and its batches cross the network to `collect` as deltas too. Add
`--metrics 127.0.0.1:9464` to either side to see the bytes saved.

```
client                                    server
  │  POST  (full body)  dpd1-keep: ?1       │
  │ ──────────────────────────────────────▶ │  keeps the body
  │ ◀────────────────────────────────────── │  dpd1-kept: <sha256>
  │                                         │
  │  POST  content-encoding: dpd1           │
  │  (delta: base hash, target hash, edit)  │
  │ ──────────────────────────────────────▶ │  rebuilds, checks SHA-256,
  │                                         │  hands the original body on
```

- The rebuilt body must match the SHA-256 the client sent, or the request is refused and never
  forwarded. The handler gets the original bytes, `content-length` and all, so request signing and
  prompt caching are unaffected.
- The worst case is one normal request. A client deltas only against a body the server confirmed
  it kept (`dpd1-kept`). A server without DPD1 never confirms, so the client sends plain requests.
  A server that lost a base (restart, eviction, another replica) answers `409`, and the client
  resends that one request in full.
- Bases are named by content hash, not by session or connection, so any request can be retried on
  any connection.
- Bases are scoped by the `authorization` / `x-api-key` header. A client cannot use, or learn of,
  another tenant's base.

| You control | Use |
|---|---|
| Both ends: your agent and your API, agents talking to each other, your inference server | The library: [`Client`](../src/client.rs) on the sending side, [`Dpd1Layer`](../src/server.rs) (a `tower` layer) on the receiving side. |
| Only the client: you call Anthropic, OpenAI, … | `deltapack server` as a reverse proxy near the provider. It is the same layer in front of a forwarder. |
| Neither: a CLI tool such as Claude Code or Codex | `deltapack client` on the tool's machine, talking to a `deltapack server`. |

## The DPD1 library (Rust)

```toml
[dependencies]
deltapack = "0.1"                                           # codec, client, server layer
# deltapack = { version = "0.1", default-features = false } # codec and client only
```

**Server.** Wrap your handler. Here it is axum, but any `tower` stack works:

```rust
use std::{sync::Arc, time::Duration};
use deltapack::{server::{Dpd1Layer, Config}, store::Store};

let store = Arc::new(Store::new(1 << 30, Duration::from_secs(1800))); // 1 GiB of bases, 30 min idle TTL
let app = axum::Router::new()
    .route("/v1/messages", axum::routing::post(messages))
    .layer(Dpd1Layer::new(store, Config::default()));
```

The handler receives each request as the client built it. Requests without DPD1 headers pass
through untouched and unbuffered.

**Client.** Wrap your HTTP call. One `Client` serves concurrent requests and conversations:

```rust
let client = deltapack::client::Client::new(4); // remembers 4 confirmed bases
let request = client.prepare(body);
let mut response = send(request.wire_body(), &request.headers()).await;
if request.is_delta() && response.headers().contains_key(deltapack::ERROR) {
    // the server no longer has the base; it did not forward the request
    response = send(request.body(), &request.plain_headers()).await;
}
client.confirm(&request, response.headers().get(deltapack::KEPT).and_then(|v| v.to_str().ok()));
```

The flags of both proxies and their metrics are in the [README](../README.md#proxy-reference).

## Deploying

- **Place `deltapack server` near the provider** (same region). The saving is on the hop from the
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

Hashing the rebuilt body is most of the rebuild time.

## Security

DPD1 relies on TLS and on your API's authentication. Bases are per tenant, so a client can never
use, or learn of, another tenant's base. Compressing secrets together with attacker-influenced
content has a length side channel, as with any compression. See [SPEC.md §5](../SPEC.md#5-scope-and-security)
and [SECURITY.md](../SECURITY.md).
