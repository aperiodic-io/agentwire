# AWF1 binary transport specification (experimental)

AWF1 is a length-delimited frame format for an **ordered**, **reliable** underlying byte stream. All fixed-width integers are big-endian; lengths are byte lengths. This is an experimental local wire format, not an IETF standard.

## Fixed header: 56 bytes

| Offset | Bytes | Name | Meaning |
|---|---:|---|---|
| 0 | 4 | magic | ASCII `AWF1` |
| 4 | 1 | protocol_version | `1` |
| 5 | 1 | flags | bit 0 = dictionary, bit 1 = raw uncompressed |
| 6 | 2 | message_count | 1..65535, capped by receiver |
| 8 | 4 | frame_sequence | u32, starts at 0 in this session |
| 12 | 4 | dictionary_version | immutable, nonzero when bit 0 set |
| 16 | 4 | original_length | uncompressed record-body length |
| 20 | 4 | payload_length | compressed/raw body length |
| 24 | 16 | dictionary_fingerprint | first 16 bytes of SHA-256 over dictionary bytes; all zeros without a dictionary |
| 40 | 16 | body_fingerprint | first 16 bytes of SHA-256 over **uncompressed** record body |
| 56 | N | payload | raw record body or Zstandard frame with optional shared dictionary |

### Uncompressed record body

Repeat `message_count` times:

```
message_id: unsigned LEB128, canonical, max u64
kind:       u8
length:     unsigned LEB128, canonical, max u64
payload:    length exact bytes
```

After decoding exactly `message_count` records, the decoder must reject trailing body bytes. Binary payloads have no text/JSON restrictions. Record IDs are arbitrary `u64` values; order is the order of records, not numerical sorting of IDs.

## Flags and failure cases

- `flags & 0x01` implies a nonzero dictionary version and nonzero matching fingerprint. Decoder refuses missing or mismatched version/hash.
- `flags & 0x02` means the payload is already uncompressed. It must not have a dictionary flag, must set version 0 and zero fingerprint, and `original_length == payload_length`.
- Unknown flags and future version numbers are rejected, not guessed.
- All frames must have one or more messages. Empty stream is valid for zero messages.
- Maximum default uncompressed record body = **1 MiB**, maximum full frame = **2 MiB**, maximum records = **65535** (the batcher defaults to 32). Bounds are checked *before decompression*.
- Decoder verifies byte count and SHA-256 prefix before handing records to application.
- Each receiver expects contiguous frame sequence numbers in a session; gaps, repeats and out-of-order frames are errors. A fresh connection begins a fresh session at sequence 0 in this PoC.
- Hashes are for accidental corruption/dictionary identity, **not authenticity or confidentiality**. Deploy inside mutually trusted TLS or add AEAD/MAC framing.

## Dictionary lifecycle

Each registry entry is `(version:u32, raw_zstd_dictionary_bytes, sha256)`.

1. Train offline on prior agent-output training data with **no held-out evaluation leakage**.
2. Assign a new immutable positive integer version; publish full SHA-256 and bytes in a trusted registry.
3. Load dictionary into receivers before sender promotion.
4. Call `BoundedBatcher.rotate(version)` to flush the old dictionary batch and begin the new one.
5. Retain old versions until no retained frames require them; archive manifests with compressed data.
6. If receiver cannot find/match a requested dictionary, fail and resolve through the control plane. Do **not** attempt decode using unrelated dictionaries.

For production, add HELLO/ACK manifest negotiation and a documented zero-dictionary fallback; this PoC uses *preprovisioned* registries only.

## AWC1: per-connection Zstd stream (context takeover)

AWF1 compresses each frame independently, so it must wait and batch to give Zstd shared history. AWC1 keeps **one Zstandard compression stream open for the whole connection** and flushes it (`ZSTD_e_flush`) after every frame. Each frame is decodable as soon as it arrives, yet its matches can point at anything sent earlier on the connection (within the window) and at the shared dictionary. This is the same idea as WebSocket `permessage-deflate` with context takeover. Implemented in `agentwire/stream.py`.

```
stream := "AWC1" RESET (RESET | DATA)*

RESET (24 bytes):
  type:u8 = 0x01 | protocol:u8 = 1 | flags:u8 (bit 0 = dictionary)
  | dictionary_version:u32 | dictionary_fingerprint[16] | window_log:u8

DATA:
  type:u8 = 0x00 | message_count:uleb128 | original_length:uleb128
  | payload_length:uleb128 | crc32(uncompressed body):u32 | payload
```

- The DATA body uses exactly the AWF1 record encoding (`id`, `kind`, `length`, `payload`). One DATA frame normally carries one message, but multiple messages are allowed.
- Typical per-frame overhead is **8–12 bytes** (vs. 56 for AWF1); the 4-byte magic plus 24-byte RESET is paid once per connection. Sequence numbers are omitted: DATA frames can only decode in order anyway, and a gap, repeat or reorder makes the Zstd stream (or the CRC) fail.
- **RESET** starts a fresh Zstd stream. It is sent at the start of a connection, on dictionary rotation (`StreamEncoder.rotate(version)`), or to discard history. The dictionary version and fingerprint are checked against the registry exactly as in AWF1; there is no fallback to a different dictionary.
- **window_log** (default 20 = 1 MiB) is how far back matches may reach. The receiver rejects a RESET whose `window_log` exceeds its `max_window_log`, which bounds the memory a peer can make it hold.
- `message_count`, `original_length` and `payload_length` are bounds-checked **before** the payload is buffered or decompressed. Decompression is capped at `original_length + 1` bytes, and the output must be exactly `original_length` bytes.
- **There is no raw fallback.** A message must be compressed onto the stream to stay in the shared history, so incompressible data can expand slightly.
- **Any error poisons the session.** After a failed frame the decoder's Zstd state is unknown, so every later `feed()` raises `SessionPoisoned`. Recovery means reconnecting, which starts a new RESET with empty history.
- CRC32 detects accidental corruption only. Like AWF1, AWC1 must run inside TLS or a MACed channel.

**Trade-offs vs. AWF1:** zero added latency and small headers, but each connection keeps compressor and decompressor state (measured about 2.1 MiB + 1.5 MiB at `window_log=20`), frames cannot be decoded independently or out of order, and losing the connection loses the shared history. Batched AWF1 still compresses better when messages genuinely arrive in fast bursts and a 25 ms wait is acceptable.

## Archive format: AWB1

Context cold storage is separate from AWF1. Files are named by full SHA-256 of canonical **original JSON bytes**.

```
AWB1 (4 bytes) | compression_flag (1 byte) | original_len (u32 big endian)
| body bytes (Zstd if flag=1, raw if flag=0)
```

Archive recovery enforces a configurable raw-byte limit, checks original length and the full SHA-256, and returns canonical JSON bytes. One archive entry per distinct content SHA-256 is idempotent. AWB1 is not a wire protocol and is not currently encrypted.
