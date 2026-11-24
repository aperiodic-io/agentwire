package deltapack

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// request is one that re-sends its conversation: a 20 KB system prompt of its own, then the turns.
func request(conversation string, turns int) []byte {
	var system strings.Builder
	for i := range 300 {
		h := sha256.Sum256(fmt.Appendf(nil, "%s%d", conversation, i))
		system.WriteString(hex.EncodeToString(h[:]))
	}
	messages := make([]map[string]string, turns)
	for i := range messages {
		messages[i] = map[string]string{"role": "user", "content": fmt.Sprintf("%s turn %d", conversation, i)}
	}
	b, _ := json.Marshal(map[string]any{"system": system.String(), "messages": messages})
	return b
}

func requests() [][]byte {
	var out [][]byte
	for t := 1; t <= 8; t++ {
		for _, c := range []string{"a", "b", "c"} {
			out = append(out, request(c, t))
		}
	}
	return out
}

func write(t *testing.T, path string, window int, bodies [][]byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w, err := NewWriter(f, window)
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range bodies {
		n, err := w.Append(fmt.Appendf(nil, "m%d", i), b)
		if err != nil || n != uint64(i) {
			t.Fatalf("append %d: %d %v", i, n, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

func open(t *testing.T, path string) *Reader {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	st, _ := f.Stat()
	r, err := Open(f, st.Size(), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRecordsReadBackByIndexAndInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.dpk")
	bodies := requests()
	write(t, path, 4, bodies)
	r := open(t, path)
	if r.Len() != len(bodies) || r.Truncated || r.IsSealed() {
		t.Fatalf("len %d truncated %v sealed %v", r.Len(), r.Truncated, r.IsSealed())
	}
	for _, i := range []int{0, 7, len(bodies) - 1} {
		got, err := r.Get(i)
		if err != nil || !bytes.Equal(got, bodies[i]) {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	n := 0
	err := r.Scan(func(i int, rec Record, body []byte) error {
		if !bytes.Equal(body, bodies[i]) || string(rec.Meta) != fmt.Sprintf("m%d", i) {
			t.Fatalf("record %d differs", i)
		}
		n++
		return nil
	})
	if err != nil || n != len(bodies) {
		t.Fatal(n, err)
	}
	if _, err := r.Get(len(bodies)); err == nil {
		t.Fatal("past the end")
	}
}

func TestAResentContextCostsAboutItsNewBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.dpk")
	bodies := requests()
	write(t, path, 4, bodies)
	st, _ := os.Stat(path)
	total := 0
	for _, b := range bodies {
		total += len(b)
	}
	// three 20 KB prompts once, then a turn each
	if st.Size()*10 > int64(total) {
		t.Fatalf("%d bytes for %d", st.Size(), total)
	}
	for _, rec := range open(t, path).Records()[3:] {
		if rec.Back == 0 {
			t.Fatal("a resent conversation is stored as a delta")
		}
	}
}

func TestACorruptRecordIsRefusedNeverReturnedWrong(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.dpk")
	write(t, path, 4, requests()[:3])
	data, _ := os.ReadFile(path)
	data[len(data)-5] ^= 1
	r, err := Open(bytes.NewReader(data), int64(len(data)), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get(2); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v", err)
	}
}

func TestAFileCutShortKeepsItsWholeRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.dpk")
	write(t, path, 4, requests()[:3])
	data, _ := os.ReadFile(path)
	cut := data[:len(data)-10]
	r, err := Open(bytes.NewReader(cut), int64(len(cut)), 64<<20)
	if err != nil || r.Len() != 2 || !r.Truncated {
		t.Fatalf("%v %d %v", err, r.Len(), r.Truncated)
	}
	if _, err := r.Get(1); err != nil {
		t.Fatal(err)
	}
}

func TestWhatIsNotADPK1FileIsRefused(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("DPK2"), []byte("DPK1\x09\x04"), []byte("nope, not one")} {
		if _, err := Open(bytes.NewReader(data), int64(len(data)), 1<<20); err == nil {
			t.Fatalf("%q accepted", data)
		}
	}
	// a record naming a base before the file's start
	data := appendVarint([]byte("DPK1\x01"), 4)
	data = append(data, 1, 0, 0)
	if _, err := Open(bytes.NewReader(data), int64(len(data)), 1<<20); !errors.Is(err, ErrMalformed) {
		t.Fatalf("got %v", err)
	}
}

func TestABodyOverTheReadersLimitIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.dpk")
	write(t, path, 4, [][]byte{bytes.Repeat([]byte("x"), 1000)})
	f, _ := os.Open(path)
	defer f.Close()
	st, _ := f.Stat()
	r, err := Open(f, st.Size(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get(0); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v", err)
	}
}

func TestVarintsAreCanonical(t *testing.T) {
	for _, v := range []uint64{0, 1, 127, 128, 1 << 32, 1<<64 - 1} {
		got, n, err := readVarint(appendVarint(nil, v))
		if err != nil || got != v || n != len(appendVarint(nil, v)) {
			t.Fatalf("%d: %d %d %v", v, got, n, err)
		}
	}
	for _, bad := range [][]byte{{0x80, 0}, {0x80}, bytes.Repeat([]byte{0xff}, 10), {}} {
		if _, _, err := readVarint(bad); !errors.Is(err, ErrMalformed) {
			t.Fatalf("%x accepted", bad)
		}
	}
}

// the deltapack CLI, when DELTAPACK_BIN names it: files cross between the two both ways
func cli(t *testing.T) string {
	bin := os.Getenv("DELTAPACK_BIN")
	if bin == "" {
		t.Skip("DELTAPACK_BIN is not set")
	}
	return bin
}

func lines() [][]byte {
	var out [][]byte
	for _, r := range requests() {
		out = append(out, r) // JSON: one line each
	}
	return out
}

func TestAFileWrittenHereIsReadByTheCLI(t *testing.T) {
	bin := cli(t)
	path := filepath.Join(t.TempDir(), "go.dpk")
	write(t, path, 16, lines())
	out, err := exec.Command(bin, "log", "unpack", path).Output()
	if err != nil {
		t.Fatal(err)
	}
	if want := append(bytes.Join(lines(), []byte("\n")), '\n'); !bytes.Equal(out, want) {
		t.Fatalf("the CLI read %d bytes, not the %d written", len(out), len(want))
	}
}

func pack(t *testing.T, bin string, args ...string) string {
	path := filepath.Join(t.TempDir(), "cli.dpk")
	cmd := exec.Command(bin, append(append([]string{"log"}, args...), "pack", path)...)
	cmd.Stdin = bytes.NewReader(append(bytes.Join(lines(), []byte("\n")), '\n'))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	return path
}

func TestAFilePackedByTheCLIIsReadHere(t *testing.T) {
	r := open(t, pack(t, cli(t)))
	want := lines()
	if r.Len() != len(want) {
		t.Fatalf("%d records", r.Len())
	}
	err := r.Scan(func(i int, _ Record, body []byte) error {
		if !bytes.Equal(body, want[i]) {
			t.Fatalf("record %d differs", i)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestASealedFileIsReadWithItsKeyOnly(t *testing.T) {
	bin := cli(t)
	hexKey := strings.Repeat("ab", 32)
	keyFile := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(keyFile, []byte(hexKey+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := open(t, pack(t, bin, "--key-file", keyFile))
	if !r.IsSealed() {
		t.Fatal("sealed")
	}
	if _, err := r.Get(0); err == nil {
		t.Fatal("read without its key")
	}
	other, _ := ParseKey(strings.Repeat("cd", 32))
	if err := r.WithKey(other); err == nil {
		t.Fatal("another key accepted")
	}
	key, err := ParseKey(hexKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WithKey(key); err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, r.Len() - 1} {
		got, err := r.Get(i)
		if err != nil || !bytes.Equal(got, lines()[i]) {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	plainFile := open(t, pack(t, bin))
	if err := plainFile.WithKey(key); !errors.Is(err, ErrNotSealed) {
		t.Fatalf("a file not sealed, given a key: %v", err)
	}
	if _, err := ParseKey("abc"); err == nil {
		t.Fatal("a short key")
	}
}
