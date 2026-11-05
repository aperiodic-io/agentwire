# Measured results — AgentWire PoC

All data below was measured on deterministic **synthetic fixtures**, not on production agent traces.
All byte figures include frame headers (56 B per AWF1 frame; 8–12 B per AWC1 frame plus a 28 B stream preamble) and per-message metadata. Dictionary distribution cost is **excluded** from wire bytes and shown in a separate amortized metric.

## Wire compression

Variants: `*/one` = one AWF1 frame per message; `batch-*` = AWF1 bounded batches (25 ms deadline) at the workload's synthetic arrival gap; `@1s` = the same batcher when messages arrive 1 s apart; `*/stream` = AWC1, one Zstd stream per connection, one frame per message, no added delay.

### Summary: zero-delay stream vs. batching

| Workload | Batch+dict, fast arrivals | Batch+dict, 1 s gap | One frame+dict | **Stream+dict** | Stream, no dict |
|---|---:|---:|---:|---:|---:|
| tool-json | 8.20× (25 ms) | 4.58× (25 ms) | 4.58× (0 ms) | **6.56× (0 ms)** | 5.70× (0 ms) |
| mixed-agent | 7.92× (25 ms) | 4.23× (25 ms) | 4.23× (0 ms) | **7.99× (0 ms)** | 7.07× (0 ms) |
| logs-diffs | 7.57× (25 ms) | 2.47× (25 ms) | 2.47× (0 ms) | **5.94× (0 ms)** | 5.03× (0 ms) |
| high-entropy | 1.28× (25 ms) | 0.93× (25 ms) | 0.93× (0 ms) | **0.98× (0 ms)** | 1.18× (0 ms) |
| short-chat | 0.60× (25 ms) | 0.17× (25 ms) | 0.17× (0 ms) | **0.56× (0 ms)** | 0.56× (0 ms) |

Ratio (max simulated added wait).

### All measurements

| Workload | Variant | Gap | Ratio | Wire bytes | Frames | Max simulated wait | Encode ms | Decode ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| tool-json | raw/one | 2 ms | 0.935× | 221,688 | 240 | 0 ms | 0.50 | 1.10 |
| tool-json | zstd3/one | 2 ms | 2.166× | 95,636 | 240 | 0 ms | 2.61 | 2.19 |
| tool-json | zstd3+dict/one | 2 ms | 4.577× | 45,261 | 240 | 0 ms | 4.79 | 3.52 |
| tool-json | zstd3/batch-8k | 2 ms | 6.048× | 34,256 | 27 | 25 ms | 1.08 | 0.83 |
| tool-json | zstd3/batch-8k@1s | 1000 ms | 2.166× | 95,636 | 240 | 25 ms | 2.59 | 2.12 |
| tool-json | zstd3/batch-64k | 2 ms | 6.610× | 31,344 | 19 | 25 ms | 0.82 | 0.71 |
| tool-json | zstd3/batch-64k@1s | 1000 ms | 2.166× | 95,636 | 240 | 25 ms | 2.53 | 2.02 |
| tool-json | zstd3+dict/batch-64k | 2 ms | 8.202× | 25,260 | 19 | 25 ms | 0.96 | 0.80 |
| tool-json | zstd3+dict/batch-64k@1s | 1000 ms | 4.577× | 45,261 | 240 | 25 ms | 4.96 | 3.60 |
| tool-json | zstd3/stream | 2 ms | 5.703× | 36,326 | 240 | 0 ms | 4.66 | 3.62 |
| tool-json | zstd3+dict/stream | 2 ms | 6.557× | 31,594 | 240 | 0 ms | 4.19 | 2.88 |
| mixed-agent | raw/one | 5 ms | 0.900× | 145,277 | 240 | 0 ms | 0.46 | 1.08 |
| mixed-agent | zstd3/one | 5 ms | 2.348× | 55,706 | 240 | 0 ms | 3.78 | 2.99 |
| mixed-agent | zstd3+dict/one | 5 ms | 4.231× | 30,914 | 240 | 0 ms | 5.79 | 4.08 |
| mixed-agent | zstd3/batch-8k | 5 ms | 3.759× | 34,801 | 48 | 25 ms | 1.01 | 0.83 |
| mixed-agent | zstd3/batch-8k@1s | 1000 ms | 2.348× | 55,706 | 240 | 25 ms | 3.64 | 3.04 |
| mixed-agent | zstd3/batch-64k | 5 ms | 3.759× | 34,801 | 48 | 25 ms | 1.32 | 1.02 |
| mixed-agent | zstd3/batch-64k@1s | 1000 ms | 2.348× | 55,706 | 240 | 25 ms | 3.65 | 2.98 |
| mixed-agent | zstd3+dict/batch-64k | 5 ms | 7.915× | 16,526 | 48 | 25 ms | 1.72 | 1.33 |
| mixed-agent | zstd3+dict/batch-64k@1s | 1000 ms | 4.231× | 30,914 | 240 | 25 ms | 5.75 | 4.28 |
| mixed-agent | zstd3/stream | 5 ms | 7.066× | 18,511 | 240 | 0 ms | 4.87 | 3.82 |
| mixed-agent | zstd3+dict/stream | 5 ms | 7.991× | 16,369 | 240 | 0 ms | 4.42 | 3.93 |
| logs-diffs | raw/one | 1 ms | 0.850× | 97,002 | 240 | 0 ms | 0.41 | 0.96 |
| logs-diffs | zstd3/one | 1 ms | 1.251× | 65,930 | 240 | 0 ms | 2.24 | 2.04 |
| logs-diffs | zstd3+dict/one | 1 ms | 2.469× | 33,406 | 240 | 0 ms | 4.53 | 3.32 |
| logs-diffs | zstd3/batch-8k | 1 ms | 4.948× | 16,670 | 15 | 15 ms | 0.76 | 0.72 |
| logs-diffs | zstd3/batch-8k@1s | 1000 ms | 1.251× | 65,930 | 240 | 25 ms | 2.20 | 1.96 |
| logs-diffs | zstd3/batch-64k | 1 ms | 5.838× | 14,130 | 10 | 25 ms | 0.56 | 0.52 |
| logs-diffs | zstd3/batch-64k@1s | 1000 ms | 1.251× | 65,930 | 240 | 25 ms | 2.17 | 1.94 |
| logs-diffs | zstd3+dict/batch-64k | 1 ms | 7.567× | 10,902 | 10 | 25 ms | 0.65 | 0.60 |
| logs-diffs | zstd3+dict/batch-64k@1s | 1000 ms | 2.469× | 33,406 | 240 | 25 ms | 4.65 | 3.55 |
| logs-diffs | zstd3/stream | 1 ms | 5.028× | 16,406 | 240 | 0 ms | 3.93 | 2.80 |
| logs-diffs | zstd3+dict/stream | 1 ms | 5.940× | 13,888 | 240 | 0 ms | 3.84 | 3.32 |
| high-entropy | raw/one | 3 ms | 0.925× | 192,584 | 240 | 0 ms | 0.50 | 1.14 |
| high-entropy | zstd3/one | 3 ms | 1.092× | 163,001 | 240 | 0 ms | 4.29 | 4.40 |
| high-entropy | zstd3+dict/one | 3 ms | 0.925× | 192,584 | 240 | 0 ms | 5.81 | 1.08 |
| high-entropy | zstd3/batch-8k | 3 ms | 1.280× | 139,101 | 27 | 25 ms | 0.99 | 0.88 |
| high-entropy | zstd3/batch-8k@1s | 1000 ms | 1.092× | 163,001 | 240 | 25 ms | 4.11 | 4.30 |
| high-entropy | zstd3/batch-64k | 3 ms | 1.280× | 139,101 | 27 | 25 ms | 1.13 | 0.93 |
| high-entropy | zstd3/batch-64k@1s | 1000 ms | 1.092× | 163,001 | 240 | 25 ms | 4.09 | 4.25 |
| high-entropy | zstd3+dict/batch-64k | 3 ms | 1.279× | 139,208 | 27 | 25 ms | 1.21 | 1.10 |
| high-entropy | zstd3+dict/batch-64k@1s | 1000 ms | 0.925× | 192,584 | 240 | 25 ms | 6.30 | 1.07 |
| high-entropy | zstd3/stream | 3 ms | 1.184× | 150,336 | 240 | 0 ms | 5.78 | 4.80 |
| high-entropy | zstd3+dict/stream | 3 ms | 0.977× | 182,302 | 240 | 0 ms | 5.47 | 4.39 |
| short-chat | raw/one | 3 ms | 0.168× | 17,150 | 240 | 0 ms | 0.36 | 0.83 |
| short-chat | zstd3/one | 3 ms | 0.168× | 17,150 | 240 | 0 ms | 1.28 | 0.89 |
| short-chat | zstd3+dict/one | 3 ms | 0.168× | 17,150 | 240 | 0 ms | 3.76 | 0.84 |
| short-chat | zstd3/batch-8k | 3 ms | 0.607× | 4,743 | 27 | 25 ms | 0.54 | 0.48 |
| short-chat | zstd3/batch-8k@1s | 1000 ms | 0.168× | 17,150 | 240 | 25 ms | 1.30 | 0.91 |
| short-chat | zstd3/batch-64k | 3 ms | 0.607× | 4,743 | 27 | 25 ms | 0.53 | 0.48 |
| short-chat | zstd3/batch-64k@1s | 1000 ms | 0.168× | 17,150 | 240 | 25 ms | 1.30 | 0.89 |
| short-chat | zstd3+dict/batch-64k | 3 ms | 0.596× | 4,829 | 27 | 25 ms | 0.82 | 0.61 |
| short-chat | zstd3+dict/batch-64k@1s | 1000 ms | 0.168× | 17,150 | 240 | 25 ms | 3.87 | 0.86 |
| short-chat | zstd3/stream | 3 ms | 0.562× | 5,120 | 240 | 0 ms | 5.13 | 1.79 |
| short-chat | zstd3+dict/stream | 3 ms | 0.561× | 5,132 | 240 | 0 ms | 3.88 | 1.83 |

## Optional agent-context reduction (NOT lossless context)

- Context before: **65,061 bytes**
- Context after: **12,162 bytes** (5.35× reduction)
- Condensed: **24 of 29 events**
- Explicitly protected: **5 events**
- Cold archive bytes: **8,980** (not counted in context)
- Every condensed observation can be restored exactly as a JSON value from the archive; context alone is intentionally lossy.

## Interpretation

- This measures payload compression plus wire framing, not HTTP/TLS network performance.
- Batch waits are simulated with a timer that fires at the 25 ms deadline (as AsyncBatchedSender does); AWC1 stream frames add no wait.
- AWC1 has no raw fallback (a message on the stream must be sent compressed), so incompressible data can expand slightly.
- Times are single-process Python ctypes/libzstd microbenchmarks and include Python orchestration overhead.
- Synthetic workload structure favors dictionary reuse; evaluate against real traces before selecting a default.
- A correct production transport requires TLS, delivery acknowledgments/replay, durable dictionary distribution, and telemetry.
