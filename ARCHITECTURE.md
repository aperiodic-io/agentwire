# AgentWire architecture, invariants, and production roadmap

## Invariants

1. For every valid AWF1 frame, the sequence of `Message(id, kind, payload)` emitted by a compatible decoder equals the original byte-exact sequence emitted by the sender.
2. A decoder must never use a dictionary with a different fingerprint from the one recorded in a frame. Immutable versions are required.
3. No single decompression may allocate beyond the configured `max_raw_bytes` from untrusted input. Invalid frames fail before allocating their advertised size.
4. A sender must flush a batch by count, uncompressed byte bound, or deadline. The timer deadline is driven by the async wrapper (low-level batcher requires polling).
5. Context reduction is explicitly opt-in, deterministic, and never mutates protected events. It may replace only low-priority successful observations with a reference and preview.
6. A reduced context references exactly recoverable archived content, identified by SHA-256. Missing/corrupt blobs throw, never return an invented observation.
7. If the requested byte budget cannot be met under the preservation rules, return an explicit error. Never silently omit an instruction, tool-call argument, or state update.

## Data flow

### Layer A — lossless transport

`Message(bytes,id,kind)` -> bounded queue -> batcher -> record stream -> Zstd(shared dictionary vN) -> AWF1 envelope -> ordered TCP/TLS -> frame parser -> fingerprint check -> bounded Zstd decoder -> digest verification -> `Message(bytes,id,kind)`.

The transport codec does *not* need to know what is inside a message. The wire format is independent of JSON schemas, tool types or model brands. Arbitrary binary content, including partial UTF-8 fragments, is safe.

### Layer A′ — AWC1 stream mode (context takeover)

`Message` -> record -> per-connection `ZSTD_compressStream2(..., ZSTD_e_flush)` -> 8–12 byte DATA header + CRC32 -> ordered TCP/TLS -> incremental parser -> per-connection `ZSTD_decompressStream` (window capped by the receiver) -> CRC check -> `Message`.

AWF1 has to *wait* to collect cross-message history; AWC1 *remembers* it instead. Use AWC1 for interactive, low-rate streams (one agent session, one tool connection) where every message should leave immediately. Use AWF1 batching for high-rate bursts or delay-tolerant traffic (log shipping), or where frames must be decodable independently (storage, fan-out, replay). Invariant 1 holds for both. AWC1 adds one more: a DATA frame is decoded only with the exact Zstd state produced by every earlier frame since the last RESET, and any failure ends the session.

### Layer B — task-specific active context

`AgentEvent` -> priority gate -> preserve critical/instructions/user/tool-call/state/error events -> greedily replace large explicitly low-priority observations with:

```json
{
  "_agentwire": "agentwire.observation_ref.v1",
  "sha256": "...",
  "original_bytes": 2412,
  "preview": "{\"service\":\"...\"...",
  "preview_truncated": true,
  "top_level_keys": ["status", "items", "service"]
}
```

The exact canonical JSON payload is written to a content-addressed Zstd blob before the summary can be emitted. To rehydrate, load by SHA-256, decompress with a bounded output allocation, validate digest and size, parse JSON, and replace the summary with the exact original JSON value. No other event is edited.

**Note:** An agent viewing just the active prompt sees the summary, not the full omitted observation. This could degrade performance if the classification was wrong. In production, context reduction needs task-based end-to-end evaluations and a retrieval tool for references.

## Why batch before Zstd?

The same language snippets, JSON field names, trace prefixes and tool identifiers are repeated **across** agent messages, not just inside each message. Combining them into a bounded body gives Zstd longer contiguous patterns to exploit and amortizes per-frame metadata. With a pre-trained immutable dictionary, this also helps initial small batches compress well.

At low traffic, long deadline windows can cost user-visible latency. Typical policy should differ by channel:

| Stream | Example max age | Batch bytes | Dictionary |
|---|---:|---:|---|
| Interactive tool responses | 5–25 ms | 8–32 KiB | tool schema vN |
| Agent-to-agent observations | 25–100 ms | 32–128 KiB | agent-output vN |
| Audit/log archival | 250–1000 ms | 128–1024 KiB | log-event vN |

These are architecture suggestions, **not measurements**. Adaptive policy should compare latency SLO and savings online.

## Operational safety model

**Per connection:** A sender is bound to a dictionary manifest and monotonic sequence. Receiver validates the version and fingerprint before decoding. No remote dictionary fetch occurs from an untrusted frame. Dictionary is provisioned over an authenticated out-of-band control plane.

**Error handling:** Mismatched version/hash, unsupported frame flags, decompression errors, inflated length, noncanonical varints, invalid message count, and checksum errors fail closed. Gap/replay detection is per-session and is not a durable delivery guarantee.

**Backpressure:** The async writer uses a bounded queue and `StreamWriter.drain()`. It does not promise process-crash durability. On connection failure the caller must catch exceptions and decide what to replay.

**Cold observations:** The sample archive uses local content-addressed blobs and atomic replace; a real design should use an encrypted object store or log (R2/S3/MinIO), TTL/lifecycle policy, and per-tenant access controls. Remember that many tiny object writes can be relatively expensive.

## Practical production roadmap

1. Add a documented **HELLO/ACK handshake** (protocol version range, AWF1 vs AWC1 mode and AWC1 window, dictionary version + full SHA-256, max frame sizes, supported modes). Implement immutable dictionary publishing, registry TTL, and in-band explicit no-dictionary fallback during upgrade.
2. Replace Python orchestration with Rust or Go and cache prepared CDicts/DDicts and compression contexts; pool `bytes::Bytes` buffers to avoid copies.
3. Add durable sequence IDs plus acknowledgment only after downstream commit, journal/spool on sender, idempotent receiver deduplication, replay windows, connection resume and exactly-once application semantics where warranted. TCP alone is not enough.
4. Add TLS, authentication/authorization, per-tenant quotas, timeouts, and audit logs; fuzz parser with libFuzzer/AFL and run a network fault injector.
5. Benchmark 10K+ **real** messages with redacted/consented traces across agent/tool/model types; partition training/evaluation chronologically to avoid leakage. Report throughput, CPU, p50/p95/p99 enqueue-to-receive latency, allocations and trained dictionary rollout churn.
6. Add token-aware context budgeting and a **rehydrate tool**; measure downstream task correctness and tool-call accuracy when summaries replace observations. Preserve signed proof/reference anchors, failures, and any state dependencies.
7. Only after these gates, consider stronger adaptive codecs or neural compressors for cold storage; preserve the fast Zstd path for interactive traffic.

## Known gaps

No authenticated handshake, no network retries/resume, no durable frame write-ahead log, no multi-process distributed dictionary service, no TLS certificate management, no task-correctness evaluation for summaries, no fuzz coverage from a native sanitizer, no independent third-party audit. This is an engineering POC, not a production transport protocol yet.
