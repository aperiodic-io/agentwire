# Roadmap: from a place to keep prompts to a place to use them

Status: phase 1 done (2026-11-23: #11, #13, #14, #15; the rename in #12); phase 2 in progress. Each step
lands as its own pull request, test first (AGENTS.md), after an independent review.

Deltapack is strong on the way in: one binary, OTLP over HTTP and gRPC, every common content coding,
`--forward` to the backend a team already has, and setup for about 80 tools. It is weak on the way
out. A record's meta is its time, method, path, content type and encoding; `deltapack log` packs,
unpacks, gets a record by position and lists. To find "the prompt of trace `ab12`", a user unpacks
everything and decodes OTLP protobuf themselves. The files are a cheap backup, not yet a tool.

## Phase 1: done

| # | Step | What it adds | Why |
|---|---|---|---|
| 1 | **Trace index and `deltapack log show`** | `collect` appends the trace ids of each OTLP batch to its record's meta as a sixth field, after the five there today, so readers of the current format keep working (SPEC.md and the README's "Reading it back" change with it). `deltapack log show FILE…` writes one JSON line per span or log record, filtered by `--trace`, `--since` and `--until`. A JSON body is told by its first byte; protobuf by the meta's path (`/v1/traces`, `/v1/logs` or their gRPC methods). A record that is not OTLP comes out as one line with its body as JSON, a JSON string, or only its length when it is binary; `--trace` leaves it out | Find and read: the two things every user otherwise writes themselves. JSON lines are also the export: DuckDB, pandas and `jq` read them as they are |
| 2 | **Retention and a rotation hook** | `--keep-bytes` and `--keep-days` on `collect` and `server` delete the oldest closed files; `--on-rotate CMD` runs `CMD <file>` once a file is closed | A disk that never fills, a stated retention, and shipping to any object store (`aws s3 mv`, `gsutil mv`, `rclone move`) without a client for each built in. Retention counts only the files in the directory: a file the hook moved away is gone from the count, and retention never deletes a file whose hook has not exited |
| 3 | **`deltapack log stats`** | Records, bytes they stand for, bytes on disk and the ratio, over any set of files | "What does this save me", answered on the user's own traffic |
| 4 | **Prebuilt binaries** | Linux (x86_64, aarch64) binaries on every GitHub release; macOS (arm64) once the organisation has macOS runners (the job never got one) | Today the choice is `cargo install deltapack --features proxy` or the ghcr.io image: a host without Docker or a Rust toolchain has neither |
| 5 | **README** | What the files are for (replay, eval sets, audit, cost), a demo that ends in a readable conversation, a FAQ (redaction, why not zstd in the backend, tenants), DPD1 moved to its own page | The README goes from "how to store" straight to the format |

## Phase 2: everything phase 1 left

| # | Step | What it adds |
|---|---|---|
| A | **Read-side polish** | `log show --attr KEY=VALUE` (session, model or any attribute, without an index); `log estimate` (the ratio and the cost on a raw export, no file written); `log ls` and `log unpack` over many files; `log show \| head` ends quietly |
| B | **Tenants and encryption at rest** | Records routed to a directory per tenant by a header, so a tenant is kept, shipped and deleted on its own; stored records encrypted with a key the operator holds |
| C | **Forwarding to strict backends, and a read API** | Batches over a backend's body limit split before forwarding (New Relic, Datadog); an HTTP endpoint a backend can link a span to for its full prompt |
| D | **Deployment and CI** | A docker-compose demo, Kubernetes manifests, and more tools run in CI (the OpenTelemetry Collector, Phoenix) |
| E | **WebSocket** | `server` proxies and records WebSocket traffic: realtime and voice APIs |
| F | **Bindings and the Collector** | Spans decoded in Python (and so pandas); Parquet through DuckDB; TypeScript and Go readers; an OpenTelemetry Collector exporter; the CLI in the Python wheel |

Phase 1 left these for later; phase 2 takes them on:

| Item | Why not now | Add when |
|---|---|---|
| An index of session ids, models and other attributes | `show`'s JSON lines can be filtered on any decoded field with `jq`; only trace ids need an index to skip decoding | A user's files are too large to decode on each query |
| Parquet export, a DuckDB extension | JSON lines cover both readers today | Someone measures JSON lines as the bottleneck |
| Built-in S3, GCS and R2 clients | `--on-rotate` with the provider's own CLI does it, with no client and no credentials handling in deltapack | An operator cannot run a CLI next to the collector |
| Encryption at rest, deletion per tenant | Encrypted volumes, and one collector with its own `--record-dir` per tenant, cover both now | A buyer needs keys per tenant in one directory |
| An OpenTelemetry Collector exporter | A Go component in another repository, built on the Rust library through FFI or on a reimplementation of DPK1 | Users ask to keep their Collector as the only hop |
| Splitting forwarded batches for backends with low OTLP body limits (docs/integrations.md, "Watch for these") | Needs OTLP re-encoding per backend | A user forwards to one of them |
| TypeScript and Go bindings | Most read paths are the CLI or Python | An ingest path in either language asks |
| WebSocket (realtime and voice APIs) | The proxies speak HTTP only | A voice-agent user asks |
| More gateways and agents run in CI | Each costs a container and minutes on every pull request | An integration breaks unseen |
