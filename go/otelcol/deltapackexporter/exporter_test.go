package deltapackexporter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	deltapack "github.com/aperiodic-io/deltapack/go"
)

var prompt = strings.Repeat("You are a careful coding agent. ", 600)

// turn is one export of a conversation's next turn: two spans of one trace, each carrying the
// whole prompt so far, as GenAI instrumentations send it.
func turn(trace byte, n int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "agent")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for s := range 2 {
		span := spans.AppendEmpty()
		span.SetTraceID(pcommon.TraceID([16]byte{15: trace}))
		span.SetSpanID(pcommon.SpanID([8]byte{6: byte(n), 7: byte(s + 1)}))
		span.SetName("chat")
		span.Attributes().PutStr("gen_ai.prompt", fmt.Sprintf("%s turn %d", prompt, n))
		span.Attributes().PutInt("turn", int64(n))
	}
	return td
}

func start(t *testing.T, cfg *Config) (func(ptrace.Traces), func(plog.Logs), func()) {
	t.Helper()
	f := NewFactory()
	set := exportertest.NewNopSettings(f.Type())
	tr, err := f.CreateTraces(context.Background(), set, cfg)
	if err != nil {
		t.Fatal(err)
	}
	lg, err := f.CreateLogs(context.Background(), set, cfg)
	if err != nil {
		t.Fatal(err)
	}
	host := componenttest.NewNopHost()
	if err := tr.Start(context.Background(), host); err != nil {
		t.Fatal(err)
	}
	if err := lg.Start(context.Background(), host); err != nil {
		t.Fatal(err)
	}
	return func(td ptrace.Traces) {
			if err := tr.ConsumeTraces(context.Background(), td); err != nil {
				t.Fatal(err)
			}
		}, func(ld plog.Logs) {
			if err := lg.ConsumeLogs(context.Background(), ld); err != nil {
				t.Fatal(err)
			}
		}, func() {
			if err := tr.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := lg.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
}

func files(t *testing.T, dir string) []string {
	t.Helper()
	found, err := filepath.Glob(filepath.Join(dir, "*.dpk"))
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func read(t *testing.T, path string) *deltapack.Reader {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	st, _ := f.Stat()
	r, err := deltapack.Open(f, st.Size(), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestEachExportIsARecordOfOTLPProtobufWithItsTraceIDs(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{Directory: dir, Window: 16, RotateBytes: 1 << 30}
	traces, logs, stop := start(t, cfg)
	var sent []ptrace.Traces
	for n := range 8 {
		td := turn(byte(n%2+1), n)
		sent = append(sent, td)
		traces(td)
	}
	ld := plog.NewLogs()
	rec := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	rec.Body().SetStr("a tool call")
	rec.SetTraceID(pcommon.TraceID([16]byte{15: 1}))
	logs(ld)
	stop()

	found := files(t, dir)
	if len(found) != 1 {
		t.Fatalf("one file for both pipelines: %v", found)
	}
	r := read(t, found[0])
	if r.Len() != 9 {
		t.Fatalf("%d records", r.Len())
	}
	err := r.Scan(func(i int, rec deltapack.Record, body []byte) error {
		meta := strings.Fields(string(rec.Meta))
		if i == 8 {
			if meta[2] != "/v1/logs" || meta[5] != fmt.Sprintf("%032x", 1) {
				t.Fatalf("log record's meta: %s", rec.Meta)
			}
			return nil
		}
		want := fmt.Sprintf("%032x", i%2+1)
		if len(meta) != 6 || meta[1] != "POST" || meta[2] != "/v1/traces" || meta[3] != "application/x-protobuf" || meta[4] != "-" || meta[5] != want {
			t.Fatalf("record %d's meta: %s", i, rec.Meta)
		}
		got, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(body)
		if err != nil {
			t.Fatal(err)
		}
		wantBody, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(sent[i])
		gotBody, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(got)
		if !bytes.Equal(gotBody, wantBody) {
			t.Fatalf("record %d is not the export", i)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(found[0])
	whole := 0
	for _, td := range sent {
		b, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
		whole += len(b)
	}
	if st.Size()*5 > int64(whole) {
		t.Fatalf("%d bytes on disk for %d sent: the re-sent prompt is stored once", st.Size(), whole)
	}
}

func TestAFullFileIsClosedAndANewOneStarted(t *testing.T) {
	dir := t.TempDir()
	traces, _, stop := start(t, &Config{Directory: dir, Window: 16, RotateBytes: 1})
	for n := range 3 {
		traces(turn(1, n))
	}
	stop()
	found := files(t, dir)
	if len(found) != 3 {
		t.Fatalf("a file per export past the limit: %v", found)
	}
	for _, f := range found {
		if read(t, f).Len() != 1 {
			t.Fatalf("%s", f)
		}
	}
}

func TestTheConfigurationIsChecked(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	if err := cfg.Validate(); err == nil {
		t.Fatal("a directory is required")
	}
	cfg.Directory = "/tmp/x"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Config{{Directory: "d", Window: 0, RotateBytes: 1}, {Directory: "d", Window: 1, RotateBytes: 0}} {
		if err := bad.Validate(); err == nil {
			t.Fatalf("%+v accepted", bad)
		}
	}
}

func TestTraceIDsAreListedOnceInOrderAndCapped(t *testing.T) {
	if got := traceIDs([]string{"b", "a", "b"}); got != "b,a" {
		t.Fatal(got)
	}
	if got := traceIDs(nil); got != "-" {
		t.Fatal(got)
	}
	many := make([]string, maxIDs+1)
	for i := range many {
		many[i] = fmt.Sprint(i)
	}
	if got := traceIDs(many); got != "?" {
		t.Fatal(got)
	}
	if got := traceIDs(many[:maxIDs]); strings.Count(got, ",") != maxIDs-1 {
		t.Fatal(got)
	}
}

// with DELTAPACK_BIN: `deltapack log show --trace` reads what the exporter wrote
func TestTheCLIShowsATraceTheExporterStored(t *testing.T) {
	bin := os.Getenv("DELTAPACK_BIN")
	if bin == "" {
		t.Skip("DELTAPACK_BIN is not set")
	}
	dir := t.TempDir()
	traces, _, stop := start(t, &Config{Directory: dir, Window: 16, RotateBytes: 1 << 30})
	for n := range 6 {
		traces(turn(byte(n%2+1), n))
	}
	stop()
	out, err := exec.Command(bin, append([]string{"log", "show", "--trace", fmt.Sprintf("%032x", 2)}, files(t, dir)...)...).Output()
	if err != nil {
		t.Fatal(err)
	}
	var turns []float64
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(nil, 1<<24)
	for sc.Scan() {
		var row map[string]any
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		attrs := row["attributes"].(map[string]any)
		if !strings.HasPrefix(attrs["gen_ai.prompt"].(string), prompt) {
			t.Fatal("the prompt whole")
		}
		turns = append(turns, attrs["turn"].(float64))
	}
	if fmt.Sprint(turns) != "[1 1 3 3 5 5]" {
		t.Fatalf("trace 2's spans: %v", turns)
	}
}

func TestAFailedWriteFailsTheExportAndTheNextGoesToANewFile(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{Directory: dir, Window: 16, RotateBytes: 1 << 30}
	traces, _, stop := start(t, cfg)
	traces(turn(1, 0))
	r := recorders.get(cfg)
	r.mu.Lock()
	r.file.Close() // the disk goes away under the writer
	r.mu.Unlock()
	body, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(turn(1, 1))
	if err := r.append("/v1/traces", nil, body); err == nil {
		t.Fatal("a failed write is an error, so the export is retried")
	}
	traces(turn(1, 2))
	stop()
	found := files(t, dir)
	if len(found) != 2 || read(t, found[1]).Len() != 1 {
		t.Fatalf("a new file after the failure: %v", found)
	}
}

func TestShutdownWithoutStartAndAStartThatFailed(t *testing.T) {
	f := NewFactory()
	blocked := filepath.Join(t.TempDir(), "a-file")
	os.WriteFile(blocked, nil, 0o600)
	cfg := &Config{Directory: blocked, Window: 16, RotateBytes: 1}
	tr, err := f.CreateTraces(context.Background(), exportertest.NewNopSettings(f.Type()), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Shutdown(context.Background()); err != nil {
		t.Fatal("never started: nothing to shut down", err)
	}
	if err := tr.Start(context.Background(), componenttest.NewNopHost()); err == nil {
		t.Fatal("a directory that is a file cannot be recorded to")
	}
	r := recorders.get(cfg)
	if r.started != 0 {
		t.Fatalf("a failed start is not counted: %d", r.started)
	}
	if err := r.append("/v1/traces", nil, nil); err == nil {
		t.Fatal("not started")
	}
}

func TestTwoExportersOnOneDirectoryEachKeepTheirOwnFiles(t *testing.T) {
	dir := t.TempDir()
	a, _, stopA := start(t, &Config{Directory: dir, Window: 16, RotateBytes: 1 << 30})
	b, _, stopB := start(t, &Config{Directory: dir, Window: 16, RotateBytes: 1 << 30})
	a(turn(1, 0))
	b(turn(2, 0))
	stopA()
	stopB()
	if found := files(t, dir); len(found) != 2 {
		t.Fatalf("%v", found)
	}
}

func TestASpanWithoutATraceIDListsNone(t *testing.T) {
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("orphan")
	dir := t.TempDir()
	traces, _, stop := start(t, &Config{Directory: dir, Window: 16, RotateBytes: 1 << 30})
	traces(td)
	stop()
	meta := strings.Fields(string(read(t, files(t, dir)[0]).Records()[0].Meta))
	if meta[5] != "-" {
		t.Fatalf("%v", meta)
	}
}
