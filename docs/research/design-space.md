# Design space: compressing agentic workloads

Companion to [compression-research.md](compression-research.md), which has the measurements and the
recommendation. Numbers in brackets refer to [references.md](references.md). Statements marked
*(measured)* refer to this repository's benchmarks (`results/research/`); everything else is from the
cited sources.

Every approach is rated on the same axes: **ratio**, **speed** (compress/decompress), **latency**
(added wait before a byte can leave), **memory** (per live connection or per open archive),
**random access**, and **operational complexity** (what you have to run, version and debug).

---

## 1. General-purpose codecs

| Codec | Knobs that matter here | Ratio | Speed (native, per core) | Latency | Memory | Random access | Ops complexity |
|---|---|---|---|---|---|---|---|
| **zstd** [1][3][4] | level 1–19 (22 ultra), `windowLog` (HTTP caps it at 8 MB [2]), long-distance matching (`--long`, default window 2^27 [3]), dictionaries, `--patch-from` (a reference "prefix") [5], `ZSTD_e_flush` for streaming [4] | High; zstd-19 `--long` is within 4–9% of xz-9e on packed corpora *(measured)* | ~500 MB/s at -1, GB/s decode [13] | Flush per message: none | Window + tables: ~2 MiB/side at w20, ~9 MiB at w23 (AgentWire) | Per frame; seekable format [7] | Low: one library, RFC 8878 |
| **brotli** [8][9] | quality 0–11, `lgwin` 10–24 (30 non-RFC), raw shared dictionaries (brotli ≥ 1.1; `dcb` in RFC 9842 [10]) | Slightly above zstd at equal speed on small, text-like messages *(measured)* | q5 ~ zstd-6 speed; q11 is ~1 MB/s | Flush per message: none | Window-sized | Per stream | Medium: shared-dictionary API is new and browser-only on the web [11] |
| **LZ4** [12][13] | fast / HC levels, linked vs independent blocks | Lowest (64 KiB window) | Fastest: ~770 MB/s compress, ~5 GB/s decompress [13] | None | Tiny | Per block (independent mode) | Low |
| **gzip / DEFLATE** [16][17][18] | level, 32 KiB window, `Z_SYNC_FLUSH` (adds `00 00 ff ff`; permessage-deflate strips it [33]) | Good on short, repetitive messages; collapses once repetition is > 32 KiB away *(measured)* | ~50–100 MB/s at -6 | None with sync flush | ~256 KiB | Per member | Lowest: everywhere, including every HTTP stack |
| **xz / LZMA2** [14][15] | presets 0–9(e), dictionary up to 64 MiB at -9 | Highest of the classic codecs | Slow: ~1–5 MB/s compress at -9 | Batch only (no flush in Python's binding) | 674 MiB to compress at -9 [15] | Per block | Low, but too slow for anything live |

**Takeaways.** zstd is the only codec that covers every use case with one library: fast live streaming
(flush per message), large windows, long-distance matching, dictionaries, patch-from, and a seekable
archive format. Brotli is a close peer on ratio for small messages, especially with a raw dictionary,
but its large-window and dictionary support is less widely deployed. LZ4 trades about half the ratio for
speed and is useful only when CPU, not bytes, is the bottleneck. gzip is the interoperability
floor. xz belongs only in cold archives, and zstd-19 --long gets close to it.

## 2. Delta and dedup

LLM conversations are **append-mostly**: request *k+1* = request *k* + new turns. Agent harness logs are
append-only JSONL. This is the dominant redundancy in API traffic (92% of request bytes are a copy of
the previous request *(measured)*), and it is invisible to any codec whose window is smaller than the
context.

| Approach | How it removes redundancy | Ratio | Speed | Latency | Memory | Random access | Ops complexity |
|---|---|---|---|---|---|---|---|
| **Windowed stream (AWC1, permessage-deflate)** [33] | The previous request is in the LZ window | Excellent while context < window | Codec speed | None | Window per connection | No | Low |
| **zstd `--patch-from` / prefix** [3][5] | Previous message is a raw-content reference for an independent frame | Excellent; needs LDM *(measured: 1,727× vs 46× at a 6 MB context)* | Fast | None | Reference + window | Each frame needs only its reference | Medium: both sides must hold the same reference |
| **AWD1 context delta** (this repo) | Explicit common-prefix/suffix against one of the last *k* messages, literal through a zstd stream | Excellent at any context size *(measured)* | Prefix compare is memcmp-speed | None | *k* × message size + small window | No | Low–medium |
| **rsync** [21] | Rolling weak + strong block checksums against the receiver's copy | Good for large files with scattered edits | Moderate | Round trip | Block-hash table | n/a | High: interactive protocol |
| **VCDIFF** [22] / **bsdiff** [23] | Generic COPY/ADD against a source | Good | bsdiff is slow and memory-hungry (17n) [23] | Batch | High | n/a | Medium |
| **FastCDC + dedup** [19][20] | Content-defined chunks, store each unique chunk once | In principle catches duplicates beyond any window; *measured* it loses to long-window zstd on every corpus here | ~1 GB/s native [20]; ~9 MB/s pure Python *(measured)* | Batch | Chunk index | Per chunk | High: index, GC, chunk-size tuning |
| **Prompt caching** [29][30] | The provider deduplicates the token prefix server-side | Saves *compute and input-token cost*, not bytes on the wire; the full body is still uploaded | n/a | n/a | Provider side | n/a | Must keep the prefix byte-stable |

**Prefix sharing in storage.** For archives, append-only structure means a session log can be stored as
a parent pointer + appended suffix (a tree of conversations). AWD1 implements the streaming version of
this. A store can do the same with a content-addressed parent hash.

**Interaction with prompt caching.** Anything that rewrites earlier context (lossy summarisation,
observation masking) breaks both the provider's prefix cache and the wire-level delta, so it can
*increase* bytes and cost even while it shrinks the prompt *(measured, §7 of the research report)*.

## 3. Structure-aware encodings

| Approach | Ratio | Speed | Latency | Memory | Random access | Ops complexity |
|---|---|---|---|---|---|---|
| **Columnar JSON (AWJ1, this repo)**: keys/structure separated from per-path value columns, each zstd'd | +2–18% over row-wise zstd at equal level on keyed JSON logs; −1 to −15% on requests, log lines and SSE *(measured)* | Python JSON parse + re-serialize dominates | Batch (needs a block) | Block-sized | Per block | Medium: must prove byte-exact re-serialization; falls back to verbatim |
| **Parquet + zstd** [34][35][36] | Similar to zstd on the payload column; adds typed columns, statistics, page index | Native, fast | Batch | Row-group sized | Row group / page | Medium: pyarrow or equivalent, schema evolution |
| **Log-template extraction (CLP, LogReducer, Logzip)** [24][27][28] | 1.2–5.3× over LZMA on system logs [27], ~2× gzip with search on compressed data [24] | Fast native ingestion [24] | Batch | Template dictionary | Search without full decompress [24] | High: parser rules, template drift |
| **Tokenizer-aware (store token IDs)** | Worse than UTF-8 + zstd *(measured)*: token IDs hide the byte patterns LZ coders exploit; ~4 bytes/token of text become 3-byte IDs | Tokenizing costs more than compressing | n/a | Vocabulary | n/a | Ties storage to one tokenizer version |
| **LLM-based coding (LLMZip, LMIC)** [38][39] | Best possible ratio on text (8.3% enwik9 [39]) | Orders of magnitude too slow; model weights dwarf the data [39] | Very high | GPU | n/a | Impractical for infrastructure |
| **Lossy summarisation / masking** [40][41][42] | 1.3–1.8× fewer context bytes *(measured)*; LLMLingua claims up to 20× [40] | Masking is free; LLM summarisation costs a model call | n/a | n/a | Recoverable via a reference archive (AgentWire's reducer) | Affects task success: must be evaluated end to end [41][42] |

## 4. Streaming (SSE and token deltas)

An SSE response is a long sequence of tiny events (~100–250 B) where each event differs from the previous
one by a few bytes of text [32]. Every event must be flushed immediately, so the per-flush overhead
dominates:

- `Z_SYNC_FLUSH` adds a 4-byte empty stored block per flush [18] (permessage-deflate strips it [33]; HTTP
  gzip cannot).
- `ZSTD_e_flush` closes a block (3-byte block header plus at least one sequence) [4].
- `BROTLI_OPERATION_FLUSH` emits an empty metadata block when needed [9].

Latency budget: at ~60 tokens/s an event arrives every ~16 ms, and every codec here costs microseconds
per event, so compression never threatens the latency budget; only *coalescing* (holding events back
to compress them together) does. The useful knobs are codec choice, window (HTTP zstd ≤ 8 MB [2]), a
shared dictionary for the fixed JSON scaffolding, and delta-against-previous-event.

## 5. Dictionary training and lifecycle

- **Training**: `ZDICT_trainFromBuffer` / `zstd --train` (fastCover d=8, 110 KiB default size); wants
  ~100× the dictionary size in samples [6]. Brotli ≥ 1.1 accepts any raw bytes as a dictionary [10].
- **Versioning**: immutable versions identified by hash (AgentWire's registry; RFC 9842 uses the
  SHA-256 as the dictionary id [10]).
- **Drift**: dictionaries capture keys, tool names, prompt boilerplate. These change when the harness,
  tool set or prompt template changes, rather than with time. *(Measured: a dictionary from another
  harness keeps only ~⅓ of the gain; no decay within one harness.)*
- **Retraining**: cheap (seconds) *(measured)*; the cost is distribution. Keep old versions as long as any
  stored frame references them, and record the dictionary id in every frame and archive manifest.

## 6. Storage and archive formats

| Format | Ratio | Random access | Notes |
|---|---|---|---|
| One zstd frame per session file | Good; `--long` helps sessions that repeat content > 8 MB apart | Whole session per read | Simplest; standard tools |
| **zstd seekable** [7] (`agentwire/seekable.py`) | 77–99% of the packed-corpus ratio at 1–4 MiB frames *(measured)* | One frame per read | Standard frames + seek table; readable by `zstd -d`; verified against an independent reader |
| Parquet + zstd [34][35] | Close to zstd on blob columns | Row group / page | Best when you query metadata (session, kind, time) |
| CDC dedup store [19] | Adds cross-session dedup | Per chunk | Worth it only when exact repeats are common across sessions (tool outputs, file reads, CI logs) |
| Cold tier (Glacier Deep Archive) [47] | n/a | Hours, 180-day minimum | At $0.00099/GB-month, ratio matters 23× less than on S3 Standard |

## 7. What existing systems do

- **LLM APIs**: responses are compressed with standard HTTP content-coding. The only first-party
  evidence that a major API accepts compressed *request* bodies is a CLI changelog that turned on
  gzip request bodies [31]; neither official Python SDK compresses requests [31]. Request upload of the
  re-sent context is uncompressed by default. That is the biggest untapped saving, and it is the client's to take.
- **Prompt caching** [29][30] is server-side dedup of the re-sent prefix. It cuts input-token cost
  (reads 0.1× of base price) but not the bytes uploaded.
- **WebSocket** permessage-deflate [33] is the established "stream with context takeover" design that
  AWC1 generalises to zstd.
- **Log systems**: CLP [24], LogReducer [27] and Logzip [28] get their gains from template/variable
  separation on line-structured system logs (Loghub [25][26]); agent transcripts are dominated by free
  text and code, where template extraction has less to work with.
