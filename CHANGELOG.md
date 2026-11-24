# Changelog

This project follows [Semantic Versioning](https://semver.org). The DPD1 wire format has its own
version byte (SPEC.md); a crate release that changes it says so here.

## [Unreleased]

- Go (`go/`): DPK1 read (sealed files with their key) and written, and an OpenTelemetry Collector
  exporter (`go/otelcol/deltapackexporter`) that stores traces and logs as `collect` does, trace ids
  in the meta. TypeScript (`ts/`): DPK1 read in Node 22.15+, no dependencies. Both are tested
  against files the CLI packs and reads, in CI.
- Python: `Reader.spans(trace=, attrs=, since=, until=)` decodes a file's OTLP records into one dict
  per span or log record, as `log show` writes them (so `pandas.DataFrame(r.spans())`). The CLI is a
  wheel of its own, `deltapack-cli` (`python/cli/`). The library's `show` feature (`deltapack::show`,
  `deltapack::split`) is what `log show`, `collect` and the bindings decode OTLP with. Parquet: through
  DuckDB or pandas (README).
- `server` proxies WebSocket sessions (realtime and voice APIs) to `--upstream` as `ws://` or
  `wss://`, with the tool's headers and subprotocols, and with `--record-dir` stores each text or
  binary message as a record, its direction (`WS>`, `WS<`), path and kind in the meta. A handshake
  the upstream refuses keeps its status; an upstream that cannot be reached is a 502.
- `deploy/`: a docker-compose stack (deltapack in front of Arize Phoenix) and a Kubernetes
  StatefulSet; the image creates `/data` for its unprivileged user. CI runs both, and runs deltapack
  between real backends: the OpenTelemetry Collector both ways, and Phoenix (`tests/backends/`).
- `--read-listen ADDR` (`collect`, `server --record-dir`) serves `GET /v1/traces/{trace id}`: the
  trace's spans and log records as JSON lines, as `log show --trace` writes them (`?since=`,
  `?tenant=`; sealed files read with the collector's key), so a backend can link a span to its
  full prompt. `--read-token-file` sets a bearer token; off a loopback address one is required.
- `collect --forward-max-body N` splits an OTLP export (JSON or protobuf, HTTP or gRPC) larger than
  N decoded bytes into exports that fit before forwarding it, by resource, then scope, then span,
  each part with its resource and scope; parts go decoded, and the first answer that is not a
  success is the exporter's. The record holds the whole export.
- Records sealed at rest: `collect` and `server --record-dir` take `--key-file` (one key) or, with
  `--tenant-header`, `--key-dir` (a key per tenant, `DIR/<tenant>.key`; a tenant without one is not
  recorded). Sealed files are DPK1 version 2 (SPEC.md, "Sealed files"): each record's delta sealed
  with ChaCha20-Poly1305 under a per-file HKDF key, its place and meta authenticated; meta stays
  readable. `deltapack log --key-file` reads them and `log pack` writes them; in Python,
  `Writer(…, key=)`, `Reader(…, key=)` and `Reader.sealed`. The library's `encrypt` feature
  (`log::Key`, `Writer::new_sealed`, `Reader::with_key`) is on in the binary and the wheel.
- `collect` and `server --record-dir` take `--tenant-header NAME`: each request is recorded in the
  directory of the tenant the header names (sanitised; `_none`, `_invalid`), each with its own
  files, lock, retention and `--on-rotate`, so a tenant is deleted on its own. `--max-tenants`
  (256) caps the directories; any more share `_other`. Metrics sum every tenant's.
- `log show --attr KEY=VALUE` keeps the spans and log records with that attribute or resource
  attribute (repeat for all of them). `log estimate` reads lines on stdin and says what `pack` would
  store, and with `--usd-per-gb` what each would cost. `log ls` and `log unpack` take many files.
  `log show | head` (or any reader that stops) ends with success, not an error.
- README: what the files are for, a one-minute demo, a FAQ; DPD1 (the codec on the wire, its
  library, deploying, performance and security) moved to `docs/dpd1.md`.
- Each release carries a `deltapack` binary for Linux (x86_64 and aarch64, static musl), each with
  its SHA-256 (`scripts/binary.sh`); CI builds and runs them on every pull request.
- `deltapack log stats FILE…`: each file's records, the bytes they stand for, its bytes on disk and
  the ratio; then the total.
- `collect` and `server --record-dir` take `--keep-bytes` and `--keep-days`, which delete the
  oldest closed files, and `--on-rotate`, which runs a command on each file once it is closed, one at
  a time (and at start on files left from an earlier run or whose command failed; retention keeps a
  file until its command succeeds). New metric: `deltapack_rotate_command_failures_total`. A record
  directory is locked: a second process recording to it refuses to start.
- `deltapack log show` writes records as JSON lines: one per span or log record of an OTLP export
  (JSON or protobuf), attributes as plain JSON, any other record whole; filtered by `--trace`,
  `--since` and `--until`. `collect` and `server --record-dir` add the trace ids of each OTLP trace
  or log batch to its record's meta, as a sixth field (`-`: none; `?`: still encoded, not OTLP, or
  more than 64), so `--trace` skips the batches of other traces. Files written before this change are read, their bodies decoded to
  find a trace.
- Renamed from agentwire to **Deltapack** (briefly Logpak, never released). The crate, binary, Python module and Docker image are
  `deltapack` (`ghcr.io/aperiodic-io/deltapack`); environment variables are `DELTAPACK_*` (were
  `AGENTWIRE_*`) and metrics `deltapack_*` (were `agentwire_*`).
- The wire and file formats take Deltapack's prefixes: AWD1 is now **DPD1** (magic `DPD1`, headers
  `dpd1-keep`, `dpd1-kept`, `dpd1-error`, `content-encoding: dpd1`), and AWL1 is now **DPK1** (magic
  `DPK1`, files `.dpk`). Nothing else in either layout changed. Files and clients from before this
  change are not read: no release had shipped the old prefixes. A client and server on different
  sides of the change fall back to plain requests, since neither confirms the other's headers.
- `docs/research` is removed: the report on the prototype formats (AWC1, AWF1, AWJ1 and the Python
  prototype) that preceded 0.1. It stays in git history.
- `collect`, `server --record-dir` and `client` decode deflate, brotli, zstd and snappy request
  bodies, as well as gzip, and a list of codings in order; `collect` does the same for gRPC
  messages by their `grpc-encoding`. A body in another coding is still stored or sent as is.
- Python bindings for DPK1 (`python/`, built with maturin, one abi3 wheel for CPython 3.9+):
  `deltapack.Writer` and `deltapack.Reader`, reading and writing the same files as the CLI and
  `collect`.
- `deltapack::log::Records`: a `Reader` iterates its records in order (`for (record, body) in
  reader`), each body rebuilt once.
- `deltapack collect` takes OTLP over gRPC (HTTP/2 without TLS) on every address it listens on, by
  default 127.0.0.1:4318 and 127.0.0.1:4317. An Export call's message is stored decompressed; with
  `--forward` it goes on as OTLP/HTTP protobuf, and the backend's answer comes back as the gRPC
  status.
- DPK1 (SPEC.md §7): logs of LLM requests or trace spans stored as DPD1 deltas against earlier
  records, every record checked by SHA-256. `deltapack::log::{Writer, Reader}`.
- `deltapack server --record-dir`: stores each request it forwards (body, time, method, path).
- `deltapack log pack | unpack | get | ls`: JSON lines in and out of DPK1 files.
- `deltapack collect`: an OTLP/HTTP and HTTP-log sink that stores what is posted in DPK1, and
  forwards it unchanged with `--forward`. Gzip bodies are stored decoded. It listens on loopback
  unless told otherwise.
- Recording (`collect`, `server --record-dir`) starts a new, self-contained file at
  `--rotate-bytes` (1 GiB), moves to a new file after a failed write, and reports
  `deltapack_recorded_*` and `deltapack_record_failures_total` metrics.
- `deltapack client` decodes gzip request bodies before it deltas them, and sends a body in any
  other content coding as is (a `dpd1` delta of it used to drop that coding).

## [0.1.0]

First release.

- AWD1 version 1 (SPEC.md): request bodies as deltas against a base the server confirmed it kept,
  rebuilt byte-exact and checked by SHA-256.
- `agentwire::codec`, `agentwire::client::Client`, and `agentwire::server::Awd1Layer` (a tower layer),
  with `agentwire::store::Store` (per-tenant bases, byte budget, idle TTL).
- The `agentwire` binary (feature `proxy`): `agentwire server`, a reverse proxy, and `agentwire client`,
  a local proxy for tools that only take a base URL; Prometheus metrics; graceful shutdown.
