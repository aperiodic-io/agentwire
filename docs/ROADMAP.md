# Roadmap: from a place to keep prompts to a place to use them

Status: adopted 2026-11-23. Each step lands as its own pull request, test first (AGENTS.md).

Logpak is strong on the way in: one binary, OTLP over HTTP and gRPC, every common content coding,
`--forward` to the backend a team already has, and setup for about 80 tools. It is weak on the way
out. A record's meta is its time, method, path, content type and encoding; `logpak log` packs,
unpacks, gets a record by position and lists. To find "the prompt of trace `ab12`", a user unpacks
everything and decodes OTLP protobuf themselves. The files are a cheap backup, not yet a tool.

## Steps, in order

| # | Step | What it adds | Why |
|---|---|---|---|
| 1 | **Trace index and `logpak log show`** | `collect` adds the trace ids of each OTLP batch to its record's meta. `logpak log show FILE…` writes one JSON line per span or log record, OTLP protobuf and JSON decoded, filtered by `--trace`, `--since` and `--until`; records that are not OTLP come out whole | Find and read: the two things every user otherwise writes themselves. JSON lines are also the export: DuckDB, pandas and `jq` read them as they are |
| 2 | **Retention and a rotation hook** | `--keep-bytes` and `--keep-days` on `collect` and `server` delete the oldest closed files; `--on-rotate CMD` runs `CMD <file>` once a file is closed | A disk that never fills, a stated retention, and shipping to any object store (`aws s3 mv`, `gsutil mv`, `rclone move`) without a client for each built in |
| 3 | **`logpak log stats`** | Records, bytes they stand for, bytes on disk and the ratio, over any set of files | "What does this save me", answered on the user's own traffic |
| 4 | **Prebuilt binaries** | Linux (x86_64, aarch64) and macOS (arm64) binaries on every GitHub release | `cargo install --features proxy` stops anyone who does not write Rust |
| 5 | **README** | What the files are for (replay, eval sets, audit, cost), a demo that ends in a readable conversation, a FAQ (redaction, why not zstd in the backend, tenants), LPD1 moved to its own page | The README goes from "how to store" straight to the format |

## Considered and left for later

| Item | Why not now | Add when |
|---|---|---|
| An index of session ids, models and other attributes | `show` filters on any decoded field already (`jq`); only trace ids need to skip decoding | A user's files are too large to decode on each query |
| Parquet export, a DuckDB extension | JSON lines cover both readers today | Someone measures JSON lines as the bottleneck |
| Built-in S3, GCS and R2 clients | `--on-rotate` with the provider's own CLI does it, with no client and no credentials handling in logpak | An operator cannot run a CLI next to the collector |
| Encryption at rest, deletion per tenant | Encrypted volumes and one collector (or `--record-dir`) per tenant cover both now | A buyer needs keys per tenant in one directory |
| An OpenTelemetry Collector exporter | A Go component in another repository, built on the Rust library through FFI or on a reimplementation of LPK1 | Users ask to keep their Collector as the only hop |
| Splitting forwarded batches for backends with low body limits (New Relic 1 MB, Datadog 15 MiB) | Needs OTLP re-encoding per backend | A user forwards to one of them |
| TypeScript and Go bindings | Most read paths are the CLI or Python | An ingest path in either language asks |
| WebSocket (realtime and voice APIs) | The proxies speak HTTP only | A voice-agent user asks |
| More gateways and agents run in CI | Each costs a container and minutes on every pull request | An integration breaks unseen |
