# AWD1: plan to make it production ready and publicly releasable

Status: proposal · Scope: the AWD1 context-delta codec (`agentwire/delta.py`, PROTOCOL.md §AWD1) and what it needs around it to ship. AWF1/AWC1, the context reducer and the archives are touched only where AWD1 depends on them.

---

## 1. Summary

AWD1 is the strongest result in this repo: on re-sent LLM context it gets 63× on public-size requests and 682–2,222× on 3–6 MB contexts, where AWC1 falls to ~10× (reports/compression-research.md §6). Today it is a 205-line Python module with good fail-closed tests. It is not wired into the CLI or the async transports. It has no handshake, no versioning, no reconnect story, and no way to carry it over the HTTP APIs that LLM traffic actually uses.

To ship it publicly we need five things:

1. **A frozen, versioned wire spec** (call it AWD1 rev 1) with a handshake, channels, resync, and an HTTP binding. Test vectors back it.
2. **A native reference implementation**: a Rust core with bundled libzstd, plus Python, TypeScript and Go bindings.
3. **A gateway plus client shims**, because no LLM provider decodes AWD1. Value only shows up when a client talks AWD1 to a gateway that forwards plain HTTPS to the provider. These integrations are the product (§6).
4. **Security hardening**: a threat model, memory budgets, fuzzing, compression-oracle isolation, and an external review.
5. **Release hygiene**: a license, CI, packaging, signed releases, docs, and a data-provenance audit of this repo.

Suggested sequencing is four milestones over about 14–18 weeks for 2 engineers (§9). The first public release is a **0.x beta** after milestone M2. **1.0** comes only once the spec is frozen and the external review is done.

---

## 2. What "production ready" and "publicly releasable" mean here

| Gate | Criterion |
|---|---|
| Correct | Byte-exact round trip. Every implementation passes the shared conformance vectors. Over 1B fuzz executions per parser with no findings. Cross-language interop is tested in CI. |
| Safe | A documented threat model. Bounded memory per connection and per process. No single input can make the receiver allocate more than its configured budget. External security review closed. |
| Operable | Reconnect without full resend in the common case. Metrics and logs. Documented config. Horizontal scaling at the gateway. Graceful fallback to plain HTTP when AWD1 is unavailable. |
| Fast | The encoder adds < 1 ms p99 for a 1 MB context and < 5 ms for 8 MB on one core. Gateway decode throughput is ≥ 1 GB/s of reconstructed bytes per core. (Targets to confirm in M1 benchmarks.) |
| Useful | Measured end-to-end wins on real, consented long sessions. This includes upload time on constrained links, not just ratio. |
| Releasable | OSI license, clean provenance, CI, semver, signed artifacts on PyPI, npm and crates.io, a container image, docs, SECURITY.md, and a support policy. |

---

## 3. Current state (gaps found in the code)

What is good and should be kept: fail-closed decoding with session poisoning, bounds checked before decompression, CRC check on the rebuilt message, multi-reference choice, dictionary fingerprint check, and property and fragmentation tests (`tests/test_delta.py`).

Protocol gaps:

1. **No protocol version field.** The preamble is `"AWD1" flags window_log references [fp]`. Any change needs a new magic.
2. **Parameters must match exactly instead of being negotiated.** The decoder rejects a stream whose `references` differs from its own setting (`delta.py`, `_drain`). There is no HELLO/ACK.
3. **The dictionary is identified only by its 16-byte fingerprint**, with no version. That is inconsistent with AWF1/AWC1 and the registry, so there is no rotation inside a stream.
4. **There is no RESET, resync or resume.** Losing the connection loses all references, and the first request after a reconnect costs the full context. For a 6 MB context on a mobile uplink, that is the worst case at the worst time.
5. **There are no channels and no message id or kind.** AWD1 carries bare bytes. A gateway connection that multiplexes more than `references` (default 4) interleaved conversations thrashes the reference list and loses the delta.
6. **There is one edit region per message** (`prefix + literal + suffix`). An edit at the top (a timestamp or date in the system prompt, a reordered `tools` list, a moved `cache_control` marker) plus an append at the end makes the literal almost the whole message. Recovery then depends on the zstd window, which is exactly the limit AWD1 exists to remove. Context compaction or truncation behaves the same way. The research did not measure this (report §10).
7. **There is no HTTP binding.** LLM APIs are request/response, with requests that can land on different connections or gateway replicas. AWD1 assumes one ordered byte stream.

Implementation gaps:

8. **There is no global memory budget.** The default `max_message_bytes` is 64 MiB × 4 references, plus the window and rebuild copies, which is over 300 MiB of worst-case memory that one peer can pin. A gateway needs per-connection and per-process caps with eviction.
9. **The payload cap is wrong near the limit.** `payload_len > max_message_bytes + 1024` is below `ZSTD_compressBound` for large incompressible literals, so legitimate messages near the cap are rejected. The cap should derive from `compressBound(body_len)`.
10. **The encoder enforces no limits,** so it can emit messages that the peer will reject.
11. **CRC32 is the only integrity check.** That is fine for accidental corruption, but it is weak as a desync detector for multi-MB reconstructions. Use xxh3-64 (or SHA-256 at resync points) and carry a hash of the reference, so a state mismatch is reported as a desync rather than corruption.
12. **Python plus ctypes.** `_best()` is O(k × n) per message, rebuilds copy the full context, and `create_string_buffer` copies inputs. Throughput at gateway rates has not been measured.
13. **There are no structured error codes for the peer.** Every failure becomes a `ProtocolError` string, so a client cannot tell "resend full" from "bug".
14. **Packaging.** libzstd is found with `ctypes.util.find_library` at import time, and there are no wheels. AWD1 is not in the CLI, `asyncio_transport.py` or the README API section.

Repository gaps (for a public release):

15. **No LICENSE file.** The package is named `agentwire-poc`, and the README calls it a PoC.
16. **No CI** (`.github/` is missing). The last verified run is macOS-only (`results/build-manifest.json`), and the README says 69 vs 131 tests in different places.
17. **Data provenance needs an audit.** `results/local/` comes from private Claude Code and Codex sessions (aggregates only, but the dates and counts are personal). `results/agent-dict-v1.zdict` and `results/retrained.zdict` have no manifest saying what they were trained on. A dictionary trained on private transcripts leaks fragments of those transcripts. `.gitignore` already guards `*.local.zdict`, but these two files are committed.
18. **Naming check:** "AgentWire" needs a trademark and package-name availability check on PyPI, npm and crates.io before launch.

---

## 4. Target architecture

```
 agent / CLI / SDK                         near the provider (same region / VPC)
┌──────────────────────┐   AWD1 rev1    ┌──────────────────────────┐  plain HTTPS   ┌──────────────┐
│ client shim          │  (WS or HTTP   │ awd1-gateway             │ (byte-exact    │ LLM provider │
│  - httpx transport   │   binding,     │  - auth, quotas          │  original      │ Anthropic /  │
│  - fetch wrapper     │   over TLS)    │  - session store (refs)  │  request)      │ OpenAI /     │
│  - local sidecar     │ ─────────────▶ │  - decode, verify        │ ─────────────▶ │ Bedrock /    │
│    (BASE_URL proxy)  │ ◀───────────── │  - forward, stream back  │ ◀───────────── │ Vertex / ... │
└──────────────────────┘  SSE: gzip     └──────────────────────────┘     SSE        └──────────────┘
                          per-event flush
```

Because the gateway forwards the **exact original bytes**, provider prompt caching, request signing and billing are unaffected. The response direction stays SSE with gzip and a flush per event, which the research measured as best for token streams (§7). AWD1 is upload-only.

Secondary topology: **agent↔agent hand-offs** (A2A, sub-agent full-context returns), where both ends are ours and AWD1 runs directly over a WebSocket or TCP channel with no provider in the loop.

---

## 5. Workstreams

### 5.1 Protocol: AWD1 rev 1 (spec freeze)

Deliverable: `spec/awd1.md`, written in the style of an RFC, plus `spec/vectors/` (binary test vectors with expected outputs, including must-fail vectors).

1. **Preamble and versioning.** `magic "AWD1"`, `version:u8`, `flags`. Unknown versions and flags are rejected. A reserved flag space covers future features.
2. **HELLO/ACK handshake** (shared with AWC1, per ARCHITECTURE roadmap item 1). It negotiates the version range, mode (AWC1, AWD1 or none), window_log, reference count, max message size, max literal size, dictionary `(version, full SHA-256)`, hash algorithm, and feature bits (ops, channels). The receiver chooses within its limits. If there is no common dictionary, both sides explicitly run without one.
3. **Channels.** Each record carries a `channel:uleb128`, and each channel has its own reference ring. The zstd stream may be shared per connection, but only within one tenant (§5.3). This allows one gateway connection to carry many conversations.
4. **Edit operations.** Generalize `prefix/literal/suffix` to a short op list (`COPY ref,off,len` / `INSERT literal`), capped at N ops (for example 64). The encoder finds anchors with a rolling hash or a suffix match over the reference. This keeps context-size independence under top-of-prompt edits, moved cache markers, and compaction. The current form is the special case `COPY, INSERT, COPY`. Gate this on the §5.5 measurement: if real traffic shows ≤ 2 edit regions in over 99% of turns, ship a 3-op form and reserve the rest.
5. **Control records:** `RESET(channel)`, `DROP_REF`, `CHECKPOINT(channel, ref_hash)`, and `ERROR(code)` with stable codes such as `DESYNC`, `LIMIT`, `DICT_MISMATCH`, `UNSUPPORTED` and `INTERNAL`. A client reacts to `DESYNC` by sending a full message (op list = one INSERT) and does not tear down other channels.
6. **Integrity.** A per-record hash of the rebuilt message (xxh3-64 by default, negotiated) plus a hash of the reference used, so desync is detected precisely. The doc must state that integrity ≠ authenticity: TLS is mandatory.
7. **Resume.** Each channel keeps a monotonically increasing message index and a reference hash. On reconnect the client sends `RESUME(channel, last_index, ref_hash)`. If the gateway still holds that state (in memory or in a shared store, §5.4), the delta continues. Otherwise it answers `DESYNC` and the client sends one full message.
8. **HTTP binding** (`spec/awd1-http.md`). This is the stateless-request form for gateways behind load balancers:
   - Request: `Content-Encoding: awd1`, `AWD1-Session: <opaque id>`, `AWD1-Base: <hash of reference>`, body = one AWD1 record (op list + literal, zstd frame with the reference as raw prefix + LDM, so it needs no stream state).
   - Response `409` + `AWD1-Error: desync` means: retry with the full body. This is one extra RTT, only on a miss.
   - Align with **RFC 9842 Compression Dictionary Transport** (`dcz`, `Available-Dictionary`). In the degenerate case "previous request as dictionary", AWD1-HTTP should be expressible as `dcz`, so a provider that ever adopts `dcz` needs no gateway.
9. **WebSocket binding.** The subprotocol is `awd1.agentwire` (Sec-WebSocket-Protocol), one record per binary message, and the handshake goes in the first message. This is the low-RTT binding for interactive sidecars.
10. **Limits section.** Mandatory receiver limits with defaults, and the rule that the encoder must honour the peer's advertised limits.
11. **Compatibility policy.** Rev 1 is frozen at 1.0. After that, extensions come only through feature bits, and wire-breaking changes require a new version byte.

### 5.2 Reference implementation

| Component | Choice | Notes |
|---|---|---|
| Core | **Rust crate `awd1`** (plus a shared `agentwire-proto` crate for AWC1 and the handshake) | `zstd-safe`/`zstd-sys` with **static, bundled libzstd** (a pinned 1.5.x). `no_std`-friendly parser, zero-copy `Bytes`, `#![forbid(unsafe_code)]` outside the FFI module. |
| Encoder matcher | SIMD memcmp for prefix and suffix; rolling-hash anchors for the op list | Benchmark against the Python version on `research/bench_longctx.py`. |
| Decoder memory | Rope or chunked buffers for references, so a rebuild does not copy 6 MB per turn; a refcounted shared prefix between references | Accounted against the budget in §5.3. |
| Python | **PyO3 + maturin, abi3 wheels** (manylinux x86_64/aarch64, musllinux, macOS universal2, Windows) | Keep the pure-Python `agentwire.delta` as a slow reference and test oracle only. |
| TypeScript | **napi-rs** for Node, plus a **WASM** build for browsers, Cloudflare Workers, Deno and Bun | WASM uses a zstd compiled to wasm32. Check the bundle size (< 300 KB gz). |
| Go | cgo binding to the Rust core, *or* a pure-Go port on `klauspost/compress/zstd` | Pure Go is preferred for gateway users; interop is gated by the vectors. |
| CLI | `agentwire delta encode/decode/bench/inspect` | `inspect` dumps records and ops for debugging. |

The Python stdlib-only ethos of the PoC stays in `agentwire` for research. The production package depends on the native wheel.

### 5.3 Security

1. **Threat model doc** (`SECURITY-MODEL.md`) with these assets: prompt contents, API keys, tool outputs, and other tenants' traffic. The adversaries are a network observer, a malicious client, a malicious upstream, and injected content inside the context (prompt injection that controls bytes in the request).
2. **Compression-oracle risk (CRIME/BREACH class).** Context mixes secrets (keys in tool outputs, private files) with attacker-influenced bytes (fetched web pages, issue text), and AWD1 plus the stream history makes per-turn sizes depend on both. A TLS observer sees record sizes. Mitigations:
   - **Never share zstd stream state or references across tenants or auth principals.** This is a hard rule in the spec, with channels scoped to one principal.
   - An optional length-padding policy (bucket record sizes) for high-sensitivity deployments.
   - Document the residual risk plainly. Compression-plus-secrets is a known, accepted trade-off in HTTP too, but people should opt in knowingly.
3. **Resource exhaustion.** Enforce per-connection caps (references × max message + window), a per-process budget with LRU eviction of idle channels (an evicted channel answers `DESYNC`, never crashes), handshake and idle timeouts, a cap on op count, and a cap on literal size. Reject a window_log above the receiver's limit before allocating.
4. **Fuzzing.** `cargo-fuzz` (libFuzzer) targets for the preamble, handshake, records, op lists and the HTTP binding parser, plus structure-aware fuzzing (an arbitrary op list, then encode, then mutate). Run continuously via **OSS-Fuzz** after public release. Add a differential fuzzer: the Rust and Python decoders must agree on accept or reject and on output.
5. **Gateway hardening.** The gateway passes provider credentials through and never stores them. Authenticate gateway clients (mTLS, or an API key distinct from the provider key). Keep an allowlist of upstream hosts, so the gateway is not an open proxy. Never log request bodies by default.
6. **Supply chain.** Pin and vendor libzstd. Use `cargo-deny` and `cargo-audit`, Dependabot, an SBOM (CycloneDX), Sigstore-signed releases, PyPI and npm trusted publishing with provenance, and SLSA level 3 builds via GitHub Actions.
7. **External review** of the spec and the Rust core before 1.0 (for example a 2–3 week engagement with a firm experienced in parsers and compression).

### 5.4 Reliability and operations (gateway)

- **Session state:** in-memory by default, keyed by `(principal, session id, channel)` with a TTL (for example 30 min idle) and a size cap. Optionally, a **shared store** (Redis or Valkey; DynamoDB or Cloudflare Durable Objects in serverless) holding the last reference per channel, so any replica can resume. Otherwise, use **sticky routing** on `AWD1-Session` (consistent hashing at the LB; Envoy `hash_policy` on the header).
- **Fallback is always safe:** a missing state means one full-size request, never an error to the user. The client shim falls back to plain HTTPS if the gateway is unreachable (circuit breaker).
- **Streaming:** forward responses with backpressure, and support cancellation (the client disconnect propagates upstream).
- **Observability** (OpenTelemetry metrics, plus Prometheus exposition): bytes in vs rebuilt, ratio per channel, literal and op counts, desync and resume counts, eviction counts, decode latency, memory in use, and upstream latency. Use structured logs with no payloads. Trace spans link the client request to the upstream request.
- **Config:** a single YAML or env-var file (limits, upstreams, auth, store, padding). Hot reload limits; reject unsafe values.
- **Deploy:** a distroless container image, a Helm chart, and a docker-compose example. Run multi-arch. Track a Terraform module for one cloud as a stretch goal.
- **SLOs to publish:** gateway-added latency p99 < 2 ms at 1 MB, availability is that of the upstream, and the desync rate after a deploy < 1% of requests.

### 5.5 Validation and benchmarks

1. **Real long sessions, consented and redacted.** The study's main gap (§10) is that it had no multi-MB real contexts and no edits, compaction or truncation. Collect over 200 long sessions from at least three harnesses (Claude Code, Codex, an OpenAI Agents SDK app, a LangGraph app) by capturing request bodies at a local sidecar. Measure:
   - edit-region count per turn (this decides the op-list scope in §5.1.4),
   - ratio vs context size with real edits,
   - desync frequency under compaction.
2. **End-to-end value.** Measure upload time and **time-to-first-token** for 0.5/2/6 MB contexts over a throttled link (10/50 Mbps up, 50–150 ms RTT), against plain HTTPS with gzip, and against `zstd` + `Content-Encoding`. This is the number that sells it, because the ratio alone does not.
3. **Performance suite** in CI (criterion for Rust, plus a regression gate of ±10%): encode and decode throughput, memory per channel, and gateway req/s per core.
4. **Interop matrix in CI:** every language encoder × every decoder over the vector set and random corpora.
5. **Chaos tests:** kill the gateway mid-stream, reorder and drop HTTP retries, evict state, rotate dictionaries, run mixed versions during a rolling deploy.
6. **Publish the benchmark harness and the public-corpus results.** Never publish private-session data (§7).

### 5.6 Dictionaries (optional for AWD1, but needed for first-turn ratio)

- Ship a **public, provenance-clean default dictionary**, trained only on public corpora (the SWE-agent and OpenHands sets in `research/build_corpora.py`) with a manifest that records its sources, so the training data can be published.
- Run `agentwire dict train` for per-deployment dictionaries with a manifest `(version, sha256, sources, date)` and immutable versions. The gateway serves the dictionary manifest in the handshake and never fetches dictionaries named by an untrusted peer.
- Retrain on harness, tool or prompt changes, and alert on ratio drops (research §5).

---

## 6. Integrations

AWD1 has value only on paths we control, so integrations are the release, not an afterthought. They are ordered by leverage.

### Tier 1: ship with the first public beta

| Integration | What it is | Why |
|---|---|---|
| **`awd1-gateway`** (Rust binary + container) | Decodes AWD1 (HTTP and WS bindings) and forwards byte-exact HTTPS to **Anthropic, OpenAI, Azure OpenAI, AWS Bedrock, Google Vertex AI, OpenRouter**, and any OpenAI-compatible upstream (vLLM, Ollama, TGI). Streams SSE back with gzip per event. | It is the decode end of every other integration. Bedrock needs SigV4: either the gateway re-signs with its own role, or the client signs the *reconstructed* body. The gateway reconstructs byte-exact, so client-side signatures stay valid. Document both. |
| **Local sidecar `awd1-local`** | A localhost proxy. Tools point `ANTHROPIC_BASE_URL` / `OPENAI_BASE_URL` at it, and it speaks AWD1 upstream to a remote gateway. | **Zero-code integration** for Claude Code, Codex CLI, Aider, Cline, Continue, Cursor-style tools, and anything else that honours a base-URL override. It is also the capture tool for §5.5. |
| **Python `httpx` transport** | `AWD1Transport(gateway=...)` passed as `http_client=httpx.Client(transport=...)` | The `anthropic` and `openai` Python SDKs both run on httpx, so this covers LangChain, LangGraph, LlamaIndex, Pydantic AI, the OpenAI Agents SDK, the Claude Agent SDK, CrewAI and DSPy with a one-line recipe each. |
| **TypeScript `fetch` wrapper** | `awd1Fetch({gateway})` passed as the `fetch` option | `@anthropic-ai/sdk`, `openai` (Node), the **Vercel AI SDK** (custom `fetch` per provider), Mastra and LangChain.js. The WASM build also runs in edge runtimes. |
| **Go `http.RoundTripper`** | `awd1.Transport{Gateway: ...}` | `anthropic-sdk-go`, `openai-go`, and Go-based gateways and agents. |

### Tier 2: soon after the beta (adoption at existing gateways)

| Integration | Shape |
|---|---|
| **LiteLLM Proxy** | A pre-call hook or custom middleware that accepts `Content-Encoding: awd1`. LiteLLM is the most common self-hosted LLM gateway, so this gives AWD1 ingress with no new hop. |
| **Envoy** | An `ext_proc` service, or an Envoy Rust dynamic module, that decodes AWD1 before routing. It covers Envoy AI Gateway, Istio and Gloo. Use the header hash policy for stickiness. |
| **Cloudflare Workers / AI Gateway in front** | A WASM decoder in a Worker, with **Durable Objects** as the per-session reference store (a natural fit for §5.4). |
| **Kong / Apigee / NGINX** | A Kong plugin (Rust/WASM via proxy-wasm). For NGINX, use njs or a dynamic module, or document running the gateway as an upstream. |
| **Portkey, Helicone, Langfuse (proxy mode)** | Upstream PRs or a documented chain: client → awd1-gateway → their proxy. Observability vendors must see the reconstructed request, so keep the decode before them. |
| **OpenTelemetry semantic conventions** | `agentwire.awd1.*` metrics and span attributes, so Datadog, Grafana and Honeycomb dashboards work out of the box. Ship a Grafana dashboard JSON. |

### Tier 3: agent-to-agent and ecosystem

| Integration | Shape |
|---|---|
| **A2A protocol / sub-agent hand-offs** | An AWD1 channel per peer for full-context returns. Propose it as an optional transport extension. |
| **MCP** (Streamable HTTP / WebSocket transports) | Mostly *not* AWD1. Tool calls and results are not re-sent context, so AWC1 is the right codec. Offer an MCP transport wrapper that negotiates AWC1 or AWD1 in the handshake and uses AWD1 only for sampling requests that carry the conversation. |
| **Agent frameworks with server-side state** (LangGraph Platform, Temporal-based agents) | Checkpoint stores benefit from AWD1-style deltas between checkpoints. This is a storage use, and it points to a later "AWD1 at rest" format. |
| **Provider adoption (standards track)** | Write up the `dcz`-with-previous-request profile (§5.1.8) as an Internet-Draft, and pitch it to providers, who already keep the previous prefix for prompt caching. If a provider adopts it, clients skip the gateway entirely. This is the long-term path to the biggest reach. |
| **Kubernetes** | A Helm chart, and an optional sidecar-injector pattern (awd1-local per pod, gateway per region). |

### Explicitly out of scope for AWD1

Response streams (use gzip per event), log shipping (AWF1), and session archives (seekable zstd). Say this in the docs, so people don't misapply AWD1.

---

## 7. Public-release hygiene (repository)

1. **License:** Apache-2.0 is recommended (patent grant, standard for protocol and infra libraries). Add `LICENSE` and `NOTICE`, and add SPDX headers.
2. **Provenance audit** before the repo goes public:
   - Delete or regenerate `results/agent-dict-v1.zdict` and `results/retrained.zdict` unless their training data is confirmed public and synthetic. Note that `examples/agent-dict-v1.zdict` is documented as synthetic (seed 24).
   - Decide whether `results/local/` (aggregates from private sessions, with dates and counts) stays. Recommendation: move it out, and keep only public-corpus results.
   - Scan the full git history for secrets and private data (gitleaks, trufflehog). If anything is found, rewrite history *before* publishing, because a public repo exposes the full history.
3. **Repository layout:** move to a Cargo workspace plus `python/`, `js/` and `go/` packages, keep `research/` as is, add `spec/`, `docs/` and `deploy/`, and mark the old `agentwire` Python package as `agentwire.research` or the reference implementation.
4. **Naming and packaging:** reserve the names on PyPI, npm (`@agentwire/*`) and crates.io, and run a trademark search. Drop `-poc`. Use semver, publishing `0.x` until spec rev 1 is frozen.
5. **CI (GitHub Actions):** a lint, test and fuzz-smoke matrix (Linux, macOS and Windows; Python 3.11–3.14; Node LTS; Go stable), the interop matrix, the benchmark regression gate, release workflows with trusted publishing, Sigstore, an SBOM, and CodeQL.
6. **Community files:** `README` (repositioned from "PoC"), `CONTRIBUTING`, `CODE_OF_CONDUCT`, `SECURITY.md` (private disclosure address, 90-day policy), `CHANGELOG`, issue and PR templates, `GOVERNANCE` (who can change the spec).
7. **Docs site** (mkdocs-material or Docusaurus): a 5-minute quickstart ("point Claude Code at awd1-local"), integration recipes per SDK and framework, the spec, the security model, gateway ops, benchmarks with a reproducible harness, and an FAQ ("does this break prompt caching?" No: the upstream sees byte-exact requests).
8. **Support policy:** supported versions, deprecation window (≥ 6 months for wire versions), and MSRV and Python-version policy.

---

## 8. Testing matrix (definition of done per component)

| Component | Unit / property | Fuzz | Interop | Perf gate | Chaos |
|---|---|---|---|---|---|
| Spec vectors | ✓ (generator + must-fail set) | n/a | consumed by all | n/a | n/a |
| Rust core | ✓ proptest | ✓ cargo-fuzz, OSS-Fuzz | ✓ | ✓ criterion | n/a |
| Python / TS / Go bindings | ✓ | differential vs Rust | ✓ | ✓ | n/a |
| Gateway | ✓ | HTTP-binding fuzz | ✓ against each client shim | ✓ load test (k6 / vegeta) | ✓ kill/evict/rolling deploy |
| Client shims | ✓ against a mock gateway | n/a | ✓ | overhead < 1 ms | ✓ gateway down → plain-HTTPS fallback |
| Provider passthrough | contract tests against recorded fixtures; a nightly live smoke test per provider (Anthropic, OpenAI, Bedrock, Vertex, Azure) with SSE | n/a | n/a | n/a | n/a |

---

## 9. Milestones

Estimates assume 2 engineers, one strong in Rust and systems, one in SDKs and integrations.

| Milestone | Duration | Contents | Exit criteria |
|---|---|---|---|
| **M0: Hygiene and decisions** | 1–2 wk | License, provenance audit, history scan, name reservation, CI for the existing Python, fix gaps 9–10, AWD1 in the CLI. Start the real-session capture (§5.5.1) using a quick Python awd1-local. | The repo could be made public as "research preview" without regret. Capture running. |
| **M1: Spec rev 1 draft + Rust core** | 4–5 wk | §5.1 items 1–7 and 10, the vector set, the Rust encoder and decoder, memory budgets, cargo-fuzz targets, benchmarks vs Python. Decide the op-list scope from the captured data. | Vectors pass in Rust and Python. Fuzz has run 24 h clean. Performance targets met or re-baselined. |
| **M2: Bindings, gateway, Tier-1 integrations → public 0.x beta** | 5–6 wk | PyO3 / napi / WASM / Go bindings, `awd1-gateway` (WS + HTTP bindings, in-memory store, sticky routing, OTel), awd1-local, the httpx / fetch / RoundTripper shims, provider passthrough tests, docs site, threat model. | All Tier-1 integrations have examples and tests. End-to-end TTFT benchmark published. Public beta tagged, with packages on PyPI, npm and crates.io and a container image. |
| **M3: Hardening + Tier 2** | 3–4 wk | Shared session store, resume, chaos suite, padding option, OSS-Fuzz onboarding, LiteLLM / Envoy / Workers integrations, Grafana dashboard, external security review started. | The review has no open high findings. Desync < 1% in rolling-deploy tests. |
| **M4: 1.0** | 1–2 wk + review time | Spec frozen, compatibility policy published, Internet-Draft for the `dcz` profile submitted, Tier-3 proposals (A2A, MCP wrapper) opened upstream. | The §2 gates are all green. |

The critical path is M1's Rust core, followed by the gateway. The integration shims are thin and can start in M1 against the Python reference.

---

## 10. Risks and open questions

| Risk | Impact | Mitigation |
|---|---|---|
| **The value case is weaker than the ratio suggests.** Upload bytes are cheap relative to tokens, and many users sit on fast links. | Low adoption. | Lead with latency on constrained links and big contexts, gateway egress cost, and mobile/edge agents. Prove it with the §5.5.2 numbers before investing in Tier 2. |
| Real traffic has many edit regions (compaction, timestamps). | Ratio collapses toward AWC1. | Op lists (§5.1.4), decided from data in M1. |
| An extra hop adds latency or a failure point. | Users disable it. | Co-locate the gateway with the provider region. Use a circuit-breaker fallback to direct HTTPS. Publish the added-latency SLO. |
| A provider ships native `dcz` or delta uploads. | The gateway becomes unnecessary. | That is a win: align the HTTP binding with RFC 9842 so the client shim targets the provider directly. |
| Compression-oracle side channel. | A security incident and reputation damage. | Tenant isolation as a hard rule, padding option, clear docs, external review. |
| Gateway sees plaintext prompts. | A trust barrier for enterprises. | Self-hosted only (no hosted service at launch), no body logging, documented data flow. |
| Provider ToS on proxies and request modification. | Legal friction. | The gateway forwards byte-exact requests with the user's own keys. Have counsel review it against each provider's terms. |

**Decisions needed from you:**

1. License: Apache-2.0 (recommended) or MIT, or dual.
2. Is a hosted gateway ever in scope, or self-host only? This changes the security, compliance and on-call scope a lot.
3. Native core language: Rust (recommended) or Go.
4. Which harnesses and users can contribute consented long sessions for §5.5.1.
5. Whether to keep `results/local/` and the two `results/*.zdict` files public.
6. The project and package name, after the trademark check.
