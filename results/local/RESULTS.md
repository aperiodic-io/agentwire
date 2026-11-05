# AgentWire on real local agent sessions

Each session file = one connection; each JSONL line = one message (exact bytes), replayed at its recorded timestamp. Dictionaries are trained only on the oldest sessions of each corpus and measured on newer ones. Dictionary size: 65,536 bytes (distribution cost excluded). Every frame was decoded and compared byte-for-byte: True.

## claude-code

- Train: 69 sessions (up to 2026-10-21); test: 46 sessions (2026-10-21 to 2026-11-04), 25,997 messages, 74.9 MiB
- Message size p50 / p90 / p99: 1,475 / 4,629 / 42,049 bytes

| Variant | Ratio | Wire MiB | Msgs/frame | Added wait p50 / p99 / max | Encode MiB/s |
|---|---:|---:|---:|---:|---:|
| awf1/one | 2.28× | 32.82 | 1.0 | 0 / 0 / 0 ms | 102.9 |
| awf1/one+dict | 3.14× | 23.88 | 1.0 | 0 / 0 / 0 ms | 27.5 |
| awf1/batch | 2.67× | 28.05 | 2.8 | 25 / 25 / 25 ms | 152.7 |
| awf1/batch+dict | 3.53× | 21.22 | 2.8 | 25 / 25 / 25 ms | 61.2 |
| awc1/stream | 6.02× | 12.45 | 1.0 | 0 / 0 / 0 ms | 96.1 |
| awc1/stream+dict | 6.24× | 12.01 | 1.0 | 0 / 0 / 0 ms | 102.8 |
| awc1/stream+dict/w8M | 6.42× | 11.67 | 1.0 | 0 / 0 / 0 ms | 97.8 |
| ref/whole-file | 6.55× | 11.43 | 565.15 | — | 681.8 |

## codex

- Train: 226 sessions (up to 2026-08-21); test: 152 sessions (2026-08-21 to 2026-11-01), 81,299 messages, 383.1 MiB
- Message size p50 / p90 / p99: 888 / 6,208 / 60,302 bytes

| Variant | Ratio | Wire MiB | Msgs/frame | Added wait p50 / p99 / max | Encode MiB/s |
|---|---:|---:|---:|---:|---:|
| awf1/one | 3.49× | 109.82 | 1.0 | 0 / 0 / 0 ms | 167.8 |
| awf1/one+dict | 4.17× | 91.89 | 1.0 | 0 / 0 / 0 ms | 48.4 |
| awf1/batch | 3.94× | 97.14 | 1.8 | 25 / 25 / 25 ms | 207.6 |
| awf1/batch+dict | 4.62× | 82.92 | 1.8 | 25 / 25 / 25 ms | 74.5 |
| awc1/stream | 6.01× | 63.80 | 1.0 | 0 / 0 / 0 ms | 155.2 |
| awc1/stream+dict | 6.03× | 63.57 | 1.0 | 0 / 0 / 0 ms | 159.7 |
| awc1/stream+dict/w8M | 6.75× | 56.73 | 1.0 | 0 / 0 / 0 ms | 161.0 |
| ref/whole-file | 6.94× | 55.17 | 534.86 | — | 789.9 |

## Caveats

- Session logs are a proxy for wire traffic: they are what an agent records, not exactly what it sends to a model or tool API.
- `ref/whole-file` compresses a finished session in one shot. It is an offline reference, not a transport option.
- Encode speed is single-threaded Python + ctypes and includes framing overhead.
