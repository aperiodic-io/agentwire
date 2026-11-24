// Package deltapackexporter is an OpenTelemetry Collector exporter that stores traces and logs as
// DPK1 files (deltapack's SPEC.md §7), as `deltapack collect` does: each export is one record,
// OTLP protobuf, a delta against an earlier one, so a prompt re-sent in every span costs about its
// new bytes. Its meta is the one `collect` writes, trace ids included, so `deltapack log show
// --trace` and the deltapack libraries read the files as they read the collector's.
package deltapackexporter

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	deltapack "github.com/aperiodic-io/deltapack/go"
)

// Config is the exporter's configuration:
//
//	exporters:
//	  deltapack:
//	    directory: /var/lib/deltapack   # a directory of its own
//	    window: 16                      # records a new one may delta against
//	    rotate_bytes: 1073741824        # a new file once one is this large
type Config struct {
	Directory   string `mapstructure:"directory"`
	Window      int    `mapstructure:"window"`
	RotateBytes int64  `mapstructure:"rotate_bytes"`
}

// Validate refuses a configuration the exporter cannot record with.
func (c *Config) Validate() error {
	if c.Directory == "" {
		return errors.New("directory is required")
	}
	if c.Window < 1 {
		return errors.New("window is at least 1")
	}
	if c.RotateBytes < 1 {
		return errors.New("rotate_bytes is at least 1")
	}
	return nil
}

var typ = component.MustNewType("deltapack")

// NewFactory is the exporter's factory, to add to a Collector build (ocb).
func NewFactory() exporter.Factory {
	return exporter.NewFactory(typ,
		func() component.Config { return &Config{Window: 16, RotateBytes: 1 << 30} },
		exporter.WithTraces(createTraces, component.StabilityLevelAlpha),
		exporter.WithLogs(createLogs, component.StabilityLevelAlpha),
	)
}

func createTraces(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	r := recorders.get(cfg.(*Config))
	return exporterhelper.NewTraces(ctx, set, cfg, r.traces,
		exporterhelper.WithStart(r.start), exporterhelper.WithShutdown(r.shutdown))
}

func createLogs(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	r := recorders.get(cfg.(*Config))
	return exporterhelper.NewLogs(ctx, set, cfg, r.logs,
		exporterhelper.WithStart(r.start), exporterhelper.WithShutdown(r.shutdown))
}

// one recorder per configuration: a traces and a logs pipeline to one directory share its files
var recorders = shared{byConfig: map[*Config]*recorder{}}

type shared struct {
	mu       sync.Mutex
	byConfig map[*Config]*recorder
}

func (s *shared) get(cfg *Config) *recorder {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.byConfig[cfg]; ok {
		return r
	}
	r := &recorder{cfg: *cfg}
	s.byConfig[cfg] = r
	return r
}

type recorder struct {
	cfg     Config
	mu      sync.Mutex
	started int
	file    *os.File
	writer  *deltapack.Writer
	written int64
	seq     int
}

func (r *recorder) start(context.Context, component.Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started++
	if r.started > 1 {
		return nil
	}
	if err := os.MkdirAll(r.cfg.Directory, 0o700); err != nil {
		return err
	}
	return r.open()
}

// open starts a new file, named as `deltapack collect` names its own: start time, pid, sequence.
func (r *recorder) open() error {
	name := fmt.Sprintf("%d-%d-%06d.dpk", time.Now().UnixMilli(), os.Getpid(), r.seq)
	f, err := os.OpenFile(filepath.Join(r.cfg.Directory, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	w, err := deltapack.NewWriter(f, r.cfg.Window)
	if err != nil {
		f.Close()
		return err
	}
	r.file, r.writer, r.written = f, w, 0
	r.seq++
	return nil
}

func (r *recorder) shutdown(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started--
	if r.started > 0 || r.file == nil {
		return nil
	}
	err := errors.Join(r.writer.Flush(), r.file.Close())
	r.file, r.writer = nil, nil
	return err
}

// append stores one export, flushed before the export is acknowledged.
func (r *recorder) append(path string, ids []string, body []byte) error {
	meta := fmt.Sprintf("%d POST %s application/x-protobuf - %s", time.Now().UnixMilli(), path, traceIDs(ids))
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.writer == nil {
		return errors.New("the deltapack exporter is not started")
	}
	if r.written >= r.cfg.RotateBytes {
		if err := errors.Join(r.writer.Flush(), r.file.Close()); err != nil {
			return err
		}
		if err := r.open(); err != nil {
			return err
		}
	}
	if _, err := r.writer.Append([]byte(meta), body); err != nil {
		return err
	}
	if err := r.writer.Flush(); err != nil {
		return err
	}
	if st, err := r.file.Stat(); err == nil {
		r.written = st.Size()
	}
	return nil
}

func (r *recorder) traces(_ context.Context, td ptrace.Traces) error {
	body, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	if err != nil {
		return err
	}
	var ids []string
	for i := range td.ResourceSpans().Len() {
		ss := td.ResourceSpans().At(i).ScopeSpans()
		for j := range ss.Len() {
			spans := ss.At(j).Spans()
			for k := range spans.Len() {
				id := spans.At(k).TraceID()
				ids = append(ids, hex.EncodeToString(id[:]))
			}
		}
	}
	return r.append("/v1/traces", ids, body)
}

func (r *recorder) logs(_ context.Context, ld plog.Logs) error {
	body, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	if err != nil {
		return err
	}
	var ids []string
	for i := range ld.ResourceLogs().Len() {
		sl := ld.ResourceLogs().At(i).ScopeLogs()
		for j := range sl.Len() {
			records := sl.At(j).LogRecords()
			for k := range records.Len() {
				if id := records.At(k).TraceID(); !id.IsEmpty() {
					ids = append(ids, hex.EncodeToString(id[:]))
				}
			}
		}
	}
	return r.append("/v1/logs", ids, body)
}

// maxIDs: more distinct trace ids than this in one export are listed as `?` (SPEC.md §7).
const maxIDs = 64

// traceIDs is the meta's sixth field: each id once, in order of first appearance; `-` for none.
func traceIDs(ids []string) string {
	seen := map[string]bool{}
	var out []string
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	switch {
	case len(out) == 0:
		return "-"
	case len(out) > maxIDs:
		return "?"
	default:
		return strings.Join(out, ",")
	}
}
