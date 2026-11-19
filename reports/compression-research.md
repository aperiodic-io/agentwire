# Compressing agentic workloads: measurements and recommendations

**Goal:** cut storage and bandwidth cost for AI-agent traffic. **Method:** build realistic corpora from
public agent trajectories, measure their structure, implement each candidate technique alongside
AgentWire's AWF1/AWC1 layers, and benchmark everything on the same held-out data. AgentWire's own
published findings were treated as hypotheses and re-tested, not assumed.

- Design-space survey with per-approach tradeoffs: [design-space.md](design-space.md)
- Verified bibliography (48 references, retrieved 2026-11-19): [references.md](references.md)
- Every measured number: [results/research/SUMMARY.md](../results/research/SUMMARY.md)
  (generated from the JSON files next to it); workload structure:
  [characterisation.md](../results/research/characterisation.md)

---

## TL;DR

1. **The dominant redundancy is the re-sent context.** 92% of every LLM request body is a byte-for-byte
   copy of the previous request; SSE events are 95% a copy of the previous event. Per-message
   compression (what HTTP does today) cannot see this. Exploiting it gives **40–60×** on public-size
   contexts and **1,700–2,200×** on multi-MB contexts, versus **3.8×** for one zstd frame per request.
2. **Windows are the cliff.** A per-connection zstd stream (AWC1) is excellent until the context
   outgrows its window, then collapses: **1 MiB window: 26× at a 1.2 MB context, ~10× at 3–6 MB.**
   Explicit context delta (AWD1, new here) or zstd `--patch-from` with long-distance matching remove the
   dependence on window size.
3. **Pack sessions together for storage.** Compressing many sessions in one long-window stream beats
   per-session files by **1.3–1.9×** (17× vs 10.6× on harness logs). Seekable zstd with 4 MiB
   frames keeps 81–99% of that and still fetches any message in **< 4.5 ms**.
4. **AgentWire findings re-tested:** "history beats batching" ✅ confirmed (also at a busy gateway);
   memory figures ✅ confirmed; "dictionaries add only 0–4% once you stream" ❌ **not reproduced**:
   on these sessions a matching 64 KiB dictionary adds **+13–30%** to AWC1 (but almost nothing on long
   ones, see §5); "AWC1 is the best live mode" ⚠️ only while the context fits the window, and not
   for SSE or tiny log lines, where plain deflate with sync flush wins.
5. **What doesn't pay:** CDC dedup (loses to long-window zstd everywhere), token-ID storage (4–16%
   worse than UTF-8), xz/brotli-11 (+5–7% ratio for 10× CPU), and the lossy observation-masking layer as a
   *bandwidth* tool (it shrinks prompt bytes 1.3–1.8× but makes delta-compressed wire bytes 5–6% *larger*).

Recommended stacks are in [§9](#9-recommendation).

---

## 1. Corpora

All inputs are public or synthetic. Raw downloads live in the git-ignored `corpora/`; only the
manifest (URLs at pinned revisions, sizes, SHA-256) is committed:
[corpora-manifest.json](../results/research/corpora-manifest.json). Rebuild with
`python3 research/build_corpora.py`.

| Source | What | Size used | License |
|---|---|---|---|
| nebius/SWE-agent-trajectories [43], shard 0 @ `68195a1` | 300 sampled SWE-agent runs (system prompt, issue, model turns, shell observations), their SWE-bench eval logs and final patches | 89 MB parquet shard (6,670 runs) | CC-BY-4.0 |
| SWE-Gym/OpenHands-Sampled-Trajectories [44], shard 0 @ `baf3a4e` | 300 sampled OpenHands runs with OpenAI-style `tool_calls` and tool schemas | 71 MB parquet shard (2,019 runs) | none declared; not redistributed here |
| logpai/loghub [26] 2k samples | HDFS, Apache, Linux, Spark, OpenSSH, Hadoop, Android, Zookeeper, BGL, Mac | 2.7 MB | research use, cite [25] |

Derived corpora (message = exact UTF-8 bytes; timestamps synthetic and deterministic because the
datasets carry none):

| Corpus | Sessions | Messages | MiB | Built how |
|---|---:|---:|---:|---|
| `transcripts-cc` | 300 | 15,132 | 35.3 | SWE-agent runs re-emitted as Claude-Code-style session JSONL. **The envelope is modelled** (uuid chain, cwd, version, usage, and a `toolUseResult` copy of each tool output), the content is real. |
| `transcripts-oh` | 300 | 10,357 | 17.5 | OpenHands messages as event-log lines; no duplicated fields |
| `api-requests` | 80 | 1,209 | 44.3 | One full request body per model turn (Anthropic-shaped for SWE-agent, OpenAI-shaped with tool schemas for OpenHands) |
| `tool-outputs` | 620 | 13,005 | 29.9 | Every real observation, classified (file reads, listings, test runs, stack traces, search, diffs from the runs' patches) + 1,000 synthetic JSON API responses, HTML pages, stack traces, base64 image blobs, hexdumps |
| `ci-logs` | 286 | 44,822 | 4.0 | SWE-bench eval logs (patch apply + pytest) and Loghub lines |
| `collab` | 40 | 3,601 | 3.1 | Synthetic orchestrator + 3–6 workers: task assignments, progress, hand-backs, state patches/snapshots, GitHub `check_run` and review-comment webhooks; free text drawn from the real runs |
| `sse` | 80 | 9,762 | 1.7 | Real assistant turns tokenized with o200k_base into deltas, wrapped as Anthropic-style and OpenAI-style SSE events, plus streamed tool-call JSON |
| `longctx` (bench only) | 4 | 336 | 774 MB of requests | OpenHands runs chained into conversations whose context grows to 0.25 / 1.2 / 3.0 / 6.0 MiB |

**Held-out protocol:** sessions are split 60/40 by index; dictionaries are trained only on the 60%,
every transport/storage number is measured on the 40%. Every encoded byte stream is decoded and
compared byte-for-byte inside the benchmark.

**Machine:** 4 vCPU Intel Xeon @ 2.10 GHz, 15.7 GiB, Linux 6.18, Python 3.13.18, libzstd 1.5.5,
zlib 1.3, libbrotli 1.1.0, liblz4 1.9.4 (full record in every JSON file).

## 2. Workload structure

From [characterisation.md](../results/research/characterisation.md):

- **Re-sent context.** In `api-requests` 92% of bytes copy the previous request (median per request
  higher). In `sse` 95% of each event copies the previous event. In session logs (append-only JSONL)
  consecutive lines share < 1%. The repetition there is further back.
- **Chunk-level duplication** (1 KiB FastCDC chunks seen before): 20% within a session for
  `transcripts-cc` (inflated by the modelled `toolUseResult` copy), 4% for `transcripts-oh`, 15% for
  tool outputs (files re-read, the same test run repeated); 14–24% across the corpus.
- **JSON redundancy is small but extremely regular.** Keys are 5–16% of bytes and punctuation 2–5%, with
  only 3–13 distinct schemas per corpus. A dictionary or columnar layout can remove the keys, but
  values (free text, code, tool output) dominate.
- **Message sizes:** session-log lines p50 0.6–1.2 KB, p99 11–21 KB; requests p50 30 KB, max 180 KB;
  SSE events p50 144 B; log lines p50 84 B (99% under 256 B).
- **Entropy:** order-0 entropy is ~5 bits/B everywhere and useless as a guide. What matters is
  history. Transcript tool results go from **2.3 bits/B** per message (zstd-3 alone) to **0.45 bits/B**
  with full history (zstd-19 --long). Log lines go from **7–8.6 bits/B** (they *expand* alone) to
  **0.3–1.2 bits/B**. Base64 image blobs stay at ~6 bits/B regardless. Already-compressed data is
  incompressible, and 9% of tool-output bytes are such blobs.

## 3. What was built (all lossless and byte-exact unless stated)

| Module | What | Tests |
|---|---|---|
| `agentwire/codecs.py` | zstd, brotli (incl. raw shared dictionaries), lz4 frame, deflate, xz behind one interface; one-shot and per-message-flushed streaming; ctypes only, zero pip deps | `tests/test_codecs.py`: property round trips, every stream codec with messages up to 1.5 MB, corruption |
| `agentwire/zstd.py` (extended) | `compress_advanced`: window, long-distance matching, raw prefix (`--patch-from`), content checksum; context sizes | same |
| `agentwire/delta.py` (**AWD1**) | Context-delta stream: each message = prefix + literal + suffix of one of the last *k* messages, literal through a zstd stream; CRC32; fails closed | `tests/test_delta.py`: random edits, 1-byte fragmentation, corruption at every position, bounds, poisoning |
| `agentwire/cdc.py` | FastCDC (normalized gear chunking) + deduplicating zstd chunk store | `tests/test_cdc.py` |
| `agentwire/jsoncol.py` (**AWJ1**) | Columnar JSON blocks: structure stream + key table + per-path value columns, verbatim fallback for anything not reproducible | `tests/test_jsoncol.py`: random JSON in 5 serializer styles, BOMs, duplicate keys, `1e5`, surrogates |
| `agentwire/seekable.py` | zstd seekable format [7] + message index; **verified both ways against pyzstd's independent `SeekableZstdFile`** | `tests/test_seekable.py` |
| `research/*.py` | corpus builder, characterisation, 10 benchmarks, summary + cost model | |

The context reducer that already existed is the opt-in lossy-but-recoverable layer. It is benchmarked
here, not changed.

Bugs found by the round-trip checks and fixed with regression tests: brotli streaming silently
truncated messages > 64 KiB (the decoder reports `NEEDS_MORE_INPUT` while output is still pending);
the FastCDC gear table was 256 copies of one value; the seekable message index broke independent
readers until it was listed in the seek table.

## 4. Live transport between agents

One connection per session, every message sent immediately, framing included (generic codecs are
charged the same header as AWC1: type, lengths and CRC32).

| Variant | cc logs | oh logs | API requests | tool outputs | CI/log lines | collab |
|---|---:|---:|---:|---:|---:|---:|
| AWF1 one frame / message (baseline) | 2.64× | 2.34× | 3.82× | 2.46× | 0.68× | 1.58× |
| AWF1 + dict | 4.46× | 3.53× | 4.84× | 2.78× | 0.73× | 3.08× |
| AWF1 25 ms batches + dict | 4.46× | 3.55× | 4.84× | 2.78× | 0.96× | 3.31× |
| AWC1 (zstd-3 stream, 1 MiB) | 8.05× | 4.76× | 47.7× | 3.37× | 3.01× | 3.34× |
| **AWC1 + dict** | **10.49×** | **5.40×** | 58.8× | 3.48× | 3.19× | 4.35× |
| deflate-6 sync-flush stream | 7.83× | 4.93× | 5.17× | 3.42× | **4.17×** | 3.79× |
| brotli-5 stream + same dict | **11.07×** | **6.16×** | **69.0×** | **3.80×** | 3.52× | **4.82×** |
| lz4 linked-block stream | 5.78× | 3.31× | 5.53× | 2.28× | 2.83× | 2.60× |
| **AWD1 + dict** | 10.64× | 5.50× | 62.9× | 3.52× | 3.90× | 4.36× |
| *Offline zstd-19 --long, whole session* | *10.64×* | *6.08×* | *57.7×* | *3.81×* | *10.17×* | *4.67×* |

Per-connection memory (fresh process, 4 MiB fed): zstd-3 stream 1 MiB window **3.6 MiB**
(libzstd: 4.0 MiB), 8 MiB window 9.5 MiB RSS / 18 MiB allocated, brotli-5 ~10.5 MiB, AWD1 8.9 MiB,
lz4 0.5 MiB, deflate 0.3 MiB. AgentWire's "2.1 + 1.5 MiB at 1 MiB" is confirmed.

Findings:

- **History is worth 3–15×; batching is worth ~0.** AWF1's 25 ms batches carry 1.0–1.3 messages at
  agent timing, even at a gateway multiplexing 60 sessions ([multiplex table](../results/research/SUMMARY.md)).
  One shared stream for all sessions at a gateway gains only 0–10% over per-session streams.
- **Live AWC1 + dict reaches the offline whole-session ratio** on every corpus except tiny log lines.
- **Brotli-5 with the same raw dictionary is 5–17% smaller than AWC1 + dict**, at ~3× the memory and
  ~20% lower speed. It is a real option where both ends are under your control.
- **Tiny messages are a different regime.** For 84-byte log lines a zstd flush + AWC1 header costs more
  than deflate's sync flush; all streams stay 2.5–3× below offline. Ship logs in batches or coalesce
  (AgentWire's AWF1 log-shipping advice holds).
- Throughput/latency here are Python + ctypes (p99 0.2–0.8 ms per message, dominated by call overhead).
  Native zstd-3 is ~400 MB/s on these files (storage benchmark), so CPU is never the constraint for
  agent-rate traffic.

## 5. Dictionaries: re-testing AgentWire's "0–4%" claim

| Test corpus | no dict (AWF1 / AWC1) | matching 64 KiB dict | other-harness dict | mixed dict | first 5 msgs: none → matching |
|---|---|---|---|---|---|
| transcripts-cc | 2.63× / 8.03× | 4.43× / **10.48× (+30%)** | 2.95× / 8.82× | 4.42× / 10.26× | 2.88× → 6.26× |
| transcripts-oh | 2.17× / 4.75× | 3.55× / **5.63× (+19%)** | 2.52× / 5.00× | 3.37× / 5.49× | 2.42× → 4.99× |
| collab | 1.58× / 3.34× | 3.08× / **4.35× (+30%)** | 2.02× / 4.09× | 2.78× / 4.17× | 2.09× → 3.52× |
| tool-outputs | 2.68× / 4.36× | 3.29× / **4.69× (+8%)** | 2.93× / 4.55× | 3.28× / 4.70× | 4.42× → 4.97× |

- **The claim is not reproduced on these sessions.** The dictionary adds 8–30% on top of the stream.
  The reconciliation is session length: the gain is concentrated in the first messages (first-5
  ratios double), and these public sessions are short (median 50–80 KB) while AgentWire's private
  ones averaged ~2.3 MB. On long sessions history dominates and the dictionary's share shrinks. Both
  results are consistent with that, but this study could not test multi-MB real sessions directly.
- **A dictionary must match the harness.** One trained on another harness keeps only ~⅓ of the gain.
  A mixed dictionary trained on all harnesses is within 2–5% of a specialised one. Use one mixed
  dictionary per deployment unless one harness dominates.
- **No staleness within a harness:** a dictionary trained on the first 20% of sessions works equally
  well on all later quarters. Drift comes from changing the harness, tools or prompt template.
- **Size:** 4 KiB gets most of the AWF1 gain, 16–64 KiB most of the stream gain, 112 KiB adds < 1%.
  Training takes 0.1–3 s. Retraining is cheap; distributing and retaining versions is the work.
- **Dictionaries matter even more for storage:** per-session zstd-19 + dict is 13.8× vs 10.6×
  (cc) and 7.2× vs 6.1× (oh).

## 6. Re-sent context: windows, delta, patch-from

`api-requests` contexts are small (≤ 180 KB), so every window ≥ 1 MiB sees the previous request.
Long agent sessions re-send 0.5–2 MB+ per turn, so the benchmark chains real runs into longer conversations:

| Final context | AWC1 1 MiB | AWC1 8 MiB | AWC1 32 MiB | zstd-3 stream 8 MiB + LDM | brotli-5 16 MiB | deflate (32 KiB) | **AWD1** | **patch-from + LDM** (stateless) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.25 MiB | 19× | 19× | 19× | 19× | 21× | 4.5× | 19× | n/a |
| 1.15 MiB | **26×** | 91× | 92× | 84× | 105× | 4.8× | 96× | 93× |
| 3.0 MiB | **11×** | 150× | 326× | 302× | 364× | 4.7× | **682×** | n/a |
| 6.0 MiB | **10×** | 342× | 510× | 673× | 961× | 4.6× | **2,222×** | **1,727×** |

- **A window smaller than the context destroys the gain.** AWC1's default 1 MiB falls to ~10× once
  the context passes ~2 MB. HTTP zstd is capped at an 8 MB window (RFC 9659 [2]), so HTTP-level
  compression can never delta a > 8 MB context.
- **AWD1 makes the ratio independent of context size.** It also uses the least memory for the ratio
  it gets: it holds the last message(s) plus a 4 MiB window, instead of a window ≥ context on both sides.
- **Stateless alternative for HTTP APIs:** compress each request as a standard zstd frame with the
  previous request as a raw prefix (`--patch-from`; RFC 9842 `dcz` with the previous request as the
  dictionary [10]). That gets 1,727× at 6 MB with plain zstd decoders. **Long-distance matching is
  required:** without it the same scheme gets 46× at 6 MB, and at small sizes LDM changes little.
- On the public-size corpus, AWD1 + dict (63×) beats patch-from (43–47×) because its stream carries
  history and the dictionary across requests.

Cost meaning: 1 TB/month of request uploads at $0.09/GB is **$90 → $23.56 (per-request zstd) → $1.43
(AWD1)**; at multi-MB contexts the transfer becomes negligible.

## 7. Streaming responses (SSE)

9,762 events across 32 held-out streams; mean event 184 B, one every ~16 ms:

| Variant | Ratio | B/event | Codec p99 | Added wait |
|---|---:|---:|---:|---:|
| identity | 1.0× | 183.7 | – | 0 |
| **deflate-6, flush per event** (permessage-deflate; HTTP gzip +4 B/event) | **15.4×** (HTTP: ~11.6×) | **11.9** (~15.9) | 19 µs | 0 |
| zstd-3, flush per event | 9.6× | 19.2 | 164 µs | 0 |
| zstd-3 + 16 KiB dict, flush per event | 11.2× | 16.5 | 183 µs | 0 |
| brotli-5 + dict, flush per event | 12.1× | 15.2 | 85 µs | 0 |
| AWC1 (+ dict) | 6.3× (7.0×) | 29.0 (26.3) | 218 µs | 0 |
| AWD1 + dict | 10.5× | 17.5 | 306 µs | 0 |
| zstd-3 + dict, coalesce 50 ms | 21.2× | 8.7 | 102 µs | 50 ms |

- **Plain deflate with sync flush is the best per-event codec.** Its per-flush overhead is smaller
  than a zstd block, and it uses 0.3 MiB per stream. Event-level delta (AWD1) and dictionaries help
  zstd but don't overtake deflate. AWC1's own framing makes it the worst choice for SSE.
- **Codec cost is negligible against the 16 ms budget** (µs per event, even in Python). Latency comes
  only from coalescing: 50 ms buys ~1.4–1.8× more.

## 8. Session-log storage and long-term archive

Per-session file ratios and corpus-level ratios (compress speed measured on transcripts-cc):

| Variant | cc | oh | requests | tools | CI logs | collab | c MB/s | d MB/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| gzip-6 | 8.61× | 5.18× | 5.19× | 3.41× | 8.06× | 4.27× | 99 | 500 |
| zstd-3 | 9.59× | 5.26× | 50.0× | 3.46× | 8.14× | 4.12× | 393 | 1,275 |
| zstd-19 (--long changes nothing at these sizes) | 10.64× | 6.08× | 57.6× | 3.82× | 10.23× | 4.67× | 4 | 1,172 |
| **zstd-19 + 64 KiB dict** | **13.77×** | **7.22×** | 72.8× | 4.02× | 11.87× | 6.20× | 5.5 | 1,044 |
| brotli-11 w24 | 11.99× | 6.75× | 63.5× | 4.16× | 11.34× | 5.25× | 1.4 | 488 |
| xz-9e | 10.94× | 6.30× | 60.0× | 3.95× | 10.54× | 4.82× | 2.1 | 190 |
| AWJ1 columnar, level 19 | 11.10× | 6.08× | 54.7× | 3.83× | 9.35× | 4.64× | 7 | 38 |
| **corpus: zstd-19 --long** | **16.97×** | **9.78×** | **83.2×** | **5.14×** | **12.96×** | **7.42×** | 3.6 | 1,061 |
| corpus: AWJ1-19 | 20.08× | 10.18× | 82.6× | 5.25× | 12.02× | 7.93× | 8 | 36 |
| corpus: xz-9e / brotli-11 | 17.9× / 18.3× | 10.3× / 10.4× | 89.5× / 88.8× | 5.4× / 5.4× | 14.1× / 13.9× | 7.7× / 7.9× | 1–3 | 190–620 |
| CDC 4 KiB dedup → zstd-19 --long pack | 14.90× | 8.86× | 66.5× | 4.95× | 4.19× | 6.43× | 9 (Python) | – |

Random access (fetch one random message, cc corpus):

| Archive | Ratio | µs per fetch |
|---|---:|---:|
| per-session zstd-19 file | 10.65× | 109 |
| seekable zstd 64 KiB frames + dict | 10.88× | 91 |
| seekable zstd 256 KiB frames | 12.20× | 156 |
| **seekable zstd 1 MiB frames** | **14.88×** | **748** |
| seekable zstd 4 MiB frames | 16.16× | 2,511 |
| Parquet zstd-19, 1,024-row groups | 15.43× | 1,751 |

- **Cross-session packing is the biggest storage lever** (+27–85%). Sessions from one harness share
  prompts, tool schemas and repository content.
- **Seekable zstd at 4 MiB frames gets 81–99% of the corpus ratio with ms-level random access**
  (1 MiB frames: 77–90%, except re-sent requests at 45% because a frame then holds only a few requests).
  Small frames should carry the dictionary. Parquet is equivalent in ratio and adds typed metadata
  columns, at ~2× the fetch cost.
- **Columnar JSON (AWJ1)** helps only on heavily keyed logs (+18% at corpus level on the
  modelled Claude-Code envelope, +2–7% on other JSON). It loses 1–15% on requests, log lines and SSE and
  decodes slowly in Python. Not worth a format of its own; Parquet already provides columns where queries need them.
- **CDC dedup loses to long-window compression on every corpus.** zstd --long already finds the
  repeats, and chunking adds recipe overhead and splits matches. Dedup only pays for exact
  repeats across far more data than a window can hold (e.g. a fleet-wide store of identical file reads).
- **xz / brotli-11 add 5–7%** for 3–10× more compression CPU and 2–5× slower decode.
- **CPU vs storage:** zstd-19 costs ~$2–3 of CPU per TB compressed once ($0.04/vCPU-hour assumed),
  versus $276 → $16 per month for 12 TB on S3 Standard. On Glacier Deep Archive ($0.00099/GB-month)
  the storage bill is already below $12/month raw, so there CPU, not ratio, is the deciding cost.

## 9. Recommendation

| Use case | Recommended stack | Measured | Why not the alternatives |
|---|---|---|---|
| **Live transport between agents** (agent↔agent, agent↔tool, gateway) | **AWC1** (zstd-3 stream, flush per message) + one **mixed 64 KiB dictionary** per deployment; switch a connection to **AWD1** when it carries re-sent context (LLM proxying, full-context hand-backs) | 4.4–10.5× on logs/collab; 59–63× on requests; window-independent with AWD1 | AWF1 batching ≈ AWF1 single at agent timing; deflate/lz4 lose 20–50%; brotli+dict is 5–17% smaller but 3× memory |
| **LLM API requests** (client → provider) | **Previous request as zstd prefix with LDM** (standard frames; RFC 9842 `dcz` shape) where the provider supports it; otherwise an AWD1 hop to a gateway near the provider | 43–1,700× vs 3.8× per request | Plain HTTP `Content-Encoding` cannot see the previous request; the 8 MB HTTP zstd window cap breaks long contexts |
| **Streaming API (SSE)** | **gzip/deflate, flush per event** (what HTTP already supports); add a shared dictionary via `dcb`/`dcz` only where clients support it; coalesce 50 ms only for machine consumers | 11.6–15.4× at 0 added latency; 21× at 50 ms | zstd per event is 21–61% bigger; AWC1 framing is worst |
| **Session-log storage** (hot, append-only) | Write each session as zstd frames (level 3–9 + dictionary); at rotation, **compact into seekable zstd, level 19, 1–4 MiB frames packed across sessions** with the message index (4 MiB for request traffic); Parquet + zstd-19 if you query metadata | cc logs: 14.9× at 0.75 ms or 16.2× at 2.5 ms per random fetch (vs 9.6× zstd-3 per file) | Per-session files leave 40–80% on the table; CDC and columnar JSON don't pay |
| **Long-term archive** (cold) | **Seekable zstd-19 --long, 4 MiB frames + dictionary**, corpus-packed per harness and month; keep the dictionary and manifest with the archive; Deep Archive tier | 16–17× | xz/brotli-11 +5–7% for 3–10× CPU; on Deep Archive, storage is already cheap |
| **Lossy context reduction** | Use **only to cut tokens** (and only after task-success evaluation [41][42]); never as a bandwidth or storage tool; keep it recoverable via the existing archive | 1.8× fewer prompt bytes, but delta-compressed wire bytes +5–6% | Masking rewrites history, which breaks both wire deltas and provider prefix caching |

### What to build next

1. **AWD1 in native code + a HELLO/ACK handshake** that negotiates AWC1 vs AWD1, window, reference
   count and dictionary id. Cap the reference memory, and fall back to AWC1 for non-append traffic.
2. **Previous-request-as-prefix request compression** for LLM API clients/gateways (zstd frame with
   LDM; `dcz` shape), and propose it to providers, who already keep the previous prefix for caching.
3. **Archive compactor:** JSONL session logs → seekable zstd-19, 1–4 MiB frames across sessions,
   message index, manifest with dictionary ids, and lifecycle to Deep Archive.
4. **Dictionary service:** train a mixed dictionary per deployment from recent traffic and version it
   immutably. Retrain on harness, tool or prompt-template changes, not on a timer. Alert on ratio
   drops (that is the drift signal).
5. **SSE at the edge:** keep gzip with per-event flush; add `dcz`/`dcb` dictionaries for clients that
   advertise them.
6. **Validate on long, consented real sessions** to settle the dictionary-gain-vs-session-length
   question (§5) and to measure AWD1 on real multi-MB contexts.

## 10. Limits of this study, and what could not be run

- **No multi-MB real sessions.** Public trajectories are short (contexts ≤ 180 KB), so long contexts
  were built by chaining real runs. Real long sessions also edit, summarise or truncate context,
  which AWD1's multi-reference design handles but this study did not measure.
- **Modelled envelopes.** The Claude-Code-style corpus has a realistic but modelled envelope,
  including a duplicate copy of each tool output. That flatters dictionaries, dedup and columnar
  coding. `transcripts-oh` (no duplication) is the conservative counterpart; conclusions hold on both.
- **Synthetic parts:** timestamps, collaboration traffic, PR/CI webhooks, SSE wrapping (content is real),
  and the JSON/HTML/base64/hexdump tool outputs.
- **Public CI logs from GitHub Actions** were not collected (log downloads need authentication).
  SWE-bench eval logs and Loghub stand in for them.
- **Speeds are Python + ctypes.** Storage throughput (one call per file) is near native; per-message
  transport throughput and latency are dominated by Python call overhead and should only be compared
  relative to each other. No native (Rust/Go) implementation was built.
- **Memory** for brotli and deflate is RSS-measured; zstd also has libzstd's exact accounting.
- **Not verified against the reference zstd CLI.** The CLI is not installable in this environment
  (this is also why the existing `test_zstd_native_compatibility_with_cli` fails here). Seekable
  archives were instead verified against pyzstd in both directions, and all frames decode with libzstd.
- **Task-quality effects** of lossy reduction were not measured (cited from [41][42] instead).
- **Request compression support** by LLM providers is undocumented beyond one first-party CLI changelog
  ([31]; unverified for other providers).
- **Dataset licenses:** the OpenHands sample declares none, so no data is redistributed; only hashes
  and aggregate statistics are committed.
