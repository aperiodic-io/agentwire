# Roadmap: from a place to keep prompts to a place to use them

Status: phase 1 done (2026-11-23: #11, #13, #14, #15; the rename in #12); phase 2 done (2026-11-24: #16,
#17, #18 and the bindings). Each step
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
| 4 | **Prebuilt binaries** | Linux (x86_64, aarch64) binaries on every GitHub release; macOS (arm64) binaries and wheels once the organisation allows macOS runners (its jobs are cancelled at start, public repository or not); until then Homebrew builds from source | Today the choice is `cargo install deltapack --features proxy` or the ghcr.io image: a host without Docker or a Rust toolchain has neither |
| 5 | **README** | What the files are for (replay, eval sets, audit, cost), a demo that ends in a readable conversation, a FAQ (redaction, why not zstd in the backend, tenants), DPD1 moved to its own page | The README goes from "how to store" straight to the format |

## Phase 2: done

| # | Step | What it adds |
|---|---|---|
| A | **Read-side polish** | `log show --attr KEY=VALUE` (session, model or any attribute, without an index); `log estimate` (the ratio and the cost on a raw export, no file written); `log ls` and `log unpack` over many files; `log show \| head` ends quietly |
| B | **Tenants and encryption at rest** | Records routed to a directory per tenant by a header, so a tenant is kept, shipped and deleted on its own; stored records encrypted with a key the operator holds |
| C | **Forwarding to strict backends, and a read API** | Batches over a backend's body limit split before forwarding (New Relic, Datadog); an HTTP endpoint a backend can link a span to for its full prompt |
| D | **Deployment and CI** | A docker-compose demo, Kubernetes manifests, and more tools run in CI (the OpenTelemetry Collector, Phoenix) |
| E | **WebSocket** | `server` proxies and records WebSocket traffic: realtime and voice APIs |
| F | **Bindings and the Collector** | Spans decoded in Python (and so pandas); Parquet through DuckDB; TypeScript and Go readers; an OpenTelemetry Collector exporter; the CLI in the Python wheel |

Phase 1 left these for later. Phase 2 took on all but two, which stay for later:

| Item | Why not now | Add when |
|---|---|---|
| An index of session ids, models and other attributes | `show --attr` and `Reader.spans(attrs=)` filter without one; only trace ids need an index to skip decoding | A user's files are too large to decode on each query |
| Built-in S3, GCS and R2 clients | `--on-rotate` with the provider's own CLI does it, with no client and no credentials handling in deltapack | An operator cannot run a CLI next to the collector |
