# Deltapack (Python)

DPK1 files from Python: LLM request logs and trace spans stored as deltas against earlier records.
A gateway's or a tracer's log re-sends the whole prompt in every record, so a record costs about
its new bytes. Every body is checked by SHA-256 when it is read back. The format is in
[SPEC.md §7](https://github.com/aperiodic-io/deltapack/blob/main/SPEC.md#7-dpk1-a-log-of-bodies-stored-as-deltas);
files are interchangeable with the `deltapack` CLI (`deltapack log ls | get | unpack`) and with
what `deltapack collect` records.

```python
import deltapack

with deltapack.Writer("spans.dpk", window=16) as w:   # a new file; never replaces one
    for span in exported_spans:                       # bytes: JSON, protobuf, anything
        w.append(span, meta=b"trace=ab12")

r = deltapack.Reader("spans.dpk")
r[17]                        # one record's body, rebuilt and checked
r.meta(17), len(r), r.truncated
for meta, body in r:         # every record in order, each rebuilt once
    ...
```

`Reader.spans()` decodes the file's OTLP records (JSON or protobuf, as `deltapack collect` stores
them) into one dict per span or log record, attributes as plain values, as `deltapack log show`
writes them; `trace=`, `attrs={...}`, `since=` and `until=` (Unix ms) filter them. Records that are
not OTLP are left out.

```python
import pandas as pd

spans = pd.DataFrame(deltapack.Reader("1763650000000-4242-000000.dpk").spans(attrs={"gen_ai.request.model": "gpt-5"}))
spans[["trace_id", "name", "received"]]
spans.to_parquet("spans.parquet")             # or DuckDB: SELECT * FROM spans
```

`window` is how many of the latest records a new one may delta against: about one per trace or
conversation interleaved in the log. A body that does not rebuild what it names raises
`ValueError`, never returns wrong bytes. `append` and reads release the GIL.

Build from source with [maturin](https://www.maturin.rs): `pip install ./python`, or
`maturin build --release -m python/Cargo.toml`. The `deltapack` command (`collect`, `server`, `log`)
is a wheel of its own: `pip install ./python/cli`.
