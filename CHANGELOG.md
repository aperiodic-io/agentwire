# Changelog

This project follows [Semantic Versioning](https://semver.org). The AWD1 wire format has its own
version byte (SPEC.md); a crate release that changes it says so here.

## [Unreleased]

- Renamed from agentwire to **Logpak**. The crate, binary, Python module and Docker image are
  `logpak` (`ghcr.io/aperiodic-io/logpak`); environment variables are `LOGPAK_*` (were
  `AGENTWIRE_*`) and metrics `logpak_*` (were `agentwire_*`). The AWD1 wire format, the AWL1 file
  format, the `awd1-*` headers and `.awl` files are unchanged, so existing recordings read as before.
- Python bindings for AWL1 (`python/`, built with maturin, one abi3 wheel for CPython 3.9+):
  `logpak.Writer` and `logpak.Reader`, reading and writing the same files as the CLI and
  `collect`.
- `logpak::log::Records`: a `Reader` iterates its records in order (`for (record, body) in
  reader`), each body rebuilt once.
- `logpak collect` takes OTLP over gRPC (HTTP/2 without TLS) on every address it listens on, by
  default 127.0.0.1:4318 and 127.0.0.1:4317. An Export call's message is stored decompressed; with
  `--forward` it goes on as OTLP/HTTP protobuf, and the backend's answer comes back as the gRPC
  status.
- AWL1 (SPEC.md §7): logs of LLM requests or trace spans stored as AWD1 deltas against earlier
  records, every record checked by SHA-256. `logpak::log::{Writer, Reader}`.
- `logpak server --record-dir`: stores each request it forwards (body, time, method, path).
- `logpak log pack | unpack | get | ls`: JSON lines in and out of AWL1 files.
- `logpak collect`: an OTLP/HTTP and HTTP-log sink that stores what is posted in AWL1, and
  forwards it unchanged with `--forward`. Gzip bodies are stored decoded. It listens on loopback
  unless told otherwise.
- Recording (`collect`, `server --record-dir`) starts a new, self-contained file at
  `--rotate-bytes` (1 GiB), moves to a new file after a failed write, and reports
  `logpak_recorded_*` and `logpak_record_failures_total` metrics.
- `logpak client` decodes gzip request bodies before it deltas them, and sends a body in any
  other content coding as is (an `awd1` delta of it used to drop that coding).

## [0.1.0]

First release.

- AWD1 version 1 (SPEC.md): request bodies as deltas against a base the server confirmed it kept,
  rebuilt byte-exact and checked by SHA-256.
- `agentwire::codec`, `agentwire::client::Client`, and `agentwire::server::Awd1Layer` (a tower layer),
  with `agentwire::store::Store` (per-tenant bases, byte budget, idle TTL).
- The `agentwire` binary (feature `proxy`): `agentwire server`, a reverse proxy, and `agentwire client`,
  a local proxy for tools that only take a base URL; Prometheus metrics; graceful shutdown.
