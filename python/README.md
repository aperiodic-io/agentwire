# Logpak (Python)

AWL1 files from Python: LLM request logs and trace spans stored as deltas against earlier records.
A gateway's or a tracer's log re-sends the whole prompt in every record, so a record costs about
its new bytes. Every body is checked by SHA-256 when it is read back. The format is in
[SPEC.md §7](https://github.com/aperiodic-io/agentwire/blob/main/SPEC.md#7-awl1-a-log-of-bodies-stored-as-deltas);
files are interchangeable with the `logpak` CLI (`logpak log ls | get | unpack`) and with
what `logpak collect` records.

```python
import logpak

with logpak.Writer("spans.awl", window=16) as w:   # a new file; never replaces one
    for span in exported_spans:                       # bytes: JSON, protobuf, anything
        w.append(span, meta=b"trace=ab12")

r = logpak.Reader("spans.awl")
r[17]                        # one record's body, rebuilt and checked
r.meta(17), len(r), r.truncated
for meta, body in r:         # every record in order, each rebuilt once
    ...
```

`window` is how many of the latest records a new one may delta against: about one per trace or
conversation interleaved in the log. A body that does not rebuild what it names raises
`ValueError`, never returns wrong bytes. `append` and reads release the GIL.

Build from source with [maturin](https://www.maturin.rs): `pip install ./python`, or
`maturin build --release -m python/Cargo.toml`.
