# Deltapack (Go)

DPK1 files from Go: read, and written, as the deltapack CLI, `deltapack collect` and the Rust and
Python libraries do ([SPEC.md §7](../SPEC.md#7-dpk1-a-log-of-bodies-stored-as-deltas)). Every body
is checked by SHA-256 when it is read; sealed files (version 2) are read with their key.

```go
import deltapack "github.com/aperiodic-io/deltapack/go"

f, _ := os.Create("spans.dpk")                 // a new file
w, _ := deltapack.NewWriter(f, 16)             // window: records a new one may delta against
w.Append([]byte("trace=ab12"), spanBytes)      // meta, body
w.Flush()

f, _ := os.Open("1763650000000-4242-000000.dpk")
st, _ := f.Stat()
r, _ := deltapack.Open(f, st.Size(), 64<<20)  // maxBody caps any body it will rebuild
body, _ := r.Get(17)                           // one record, rebuilt through its bases
r.Scan(func(i int, rec deltapack.Record, body []byte) error { return nil }) // every record, once
```

A sealed file: `key, _ := deltapack.ParseKey(hex); r.WithKey(key)`. Given a key, a file that is
not sealed is refused (`ErrNotSealed`): it could have replaced a sealed one. The writer writes
version 1 (not sealed).

## The OpenTelemetry Collector exporter

[`otelcol/deltapackexporter`](otelcol/deltapackexporter) stores a Collector's traces and logs as
DPK1 files, as `deltapack collect` does, so the Collector stays the only hop: each export is a
record of OTLP protobuf, its meta the one `collect` writes (trace ids included, for `deltapack log
show --trace`). Add it to a Collector build (ocb):

```yaml
# builder-config.yaml
exporters:
  - gomod: github.com/aperiodic-io/deltapack/go/otelcol/deltapackexporter v0.1.0
```

```yaml
# collector config
exporters:
  deltapack:
    directory: /var/lib/deltapack   # a directory of its own: not one a deltapack collector records to
    window: 16
    rotate_bytes: 1073741824        # a new file once one is this large
service:
  pipelines:
    traces: { receivers: [otlp], exporters: [deltapack, otlphttp] }
    logs: { receivers: [otlp], exporters: [deltapack] }
```

Retention and shipping are the Collector host's: delete or move closed files as you would
`collect`'s (a file is closed once a newer one exists).

Tests: `go test ./...` in `go/` and in the exporter's directory; with `DELTAPACK_BIN` naming the CLI,
files cross between the two both ways.
