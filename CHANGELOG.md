# Changelog

This project follows [Semantic Versioning](https://semver.org). The AWD1 wire format has its own
version byte (SPEC.md); a crate release that changes it says so here.

## [0.1.0]

First release.

- AWD1 version 1 (SPEC.md): request bodies as deltas against a base the server confirmed it kept,
  rebuilt byte-exact and checked by SHA-256.
- `agentwire::codec`, `agentwire::client::Client`, and `agentwire::server::Awd1Layer` (a tower layer),
  with `agentwire::store::Store` (per-tenant bases, byte budget, idle TTL).
- The `agentwire` binary (feature `proxy`): `agentwire server`, a reverse proxy, and `agentwire client`,
  a local proxy for tools that only take a base URL; Prometheus metrics; graceful shutdown.
