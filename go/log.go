package deltapack

import (
	"bufio"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

const (
	fileMagic = "DPK1"
	// version 1: records in the clear; version 2: sealed (SPEC.md §7, "Sealed files")
	plain, sealedVersion = 1, 2
	// a writer starts again with a whole record once a chain would rebuild this many records or bytes
	maxDepth      = 4096
	maxChainBytes = 1 << 30
)

// ErrNotSealed: a reader given a key refuses a file written without one, which could have
// replaced a sealed one.
var ErrNotSealed = errors.New("the file is not sealed")

// Record is one record's place in a file: its meta (stored as is), how many records back its
// base is (0: stored whole), and the bytes its delta takes.
type Record struct {
	Meta   []byte
	Back   uint64
	Stored uint64
	offset int64
}

// Reader reads a DPK1 file: Len, Records, Get (one body), Scan (every body in order).
type Reader struct {
	src     io.ReaderAt
	window  uint64
	maxBody uint64
	records []Record
	// Truncated: the file ends inside a record (its writer stopped mid-write); the records before
	// it are whole.
	Truncated bool
	salt      []byte
	keyID     []byte
	aead      cipher.AEAD
}

// Open indexes the DPK1 file in src (size bytes). maxBody caps any body it will rebuild.
func Open(src io.ReaderAt, size int64, maxBody int) (*Reader, error) {
	in := bufio.NewReaderSize(io.NewSectionReader(src, 0, size), 1<<16)
	pos := int64(0)
	read := func(n int) ([]byte, error) {
		b := make([]byte, n)
		_, err := io.ReadFull(in, b)
		pos += int64(n)
		return b, err
	}
	varint := func() (uint64, error) {
		var buf [10]byte
		for i := range buf {
			b, err := in.ReadByte()
			if err != nil {
				return 0, err
			}
			pos++
			buf[i] = b
			if b&0x80 == 0 {
				v, _, err := readVarint(buf[:i+1])
				return v, err
			}
		}
		_, _, err := readVarint(buf[:])
		return 0, err
	}
	head, err := read(5)
	if err != nil || string(head[:4]) != fileMagic {
		return nil, fmt.Errorf("%w: not a DPK1 file", ErrMalformed)
	}
	r := &Reader{src: src, maxBody: uint64(maxBody)}
	if head[4] != plain && head[4] != sealedVersion {
		return nil, fmt.Errorf("%w: DPK1 version %d", ErrUnsupportedVersion, head[4])
	}
	if r.window, err = varint(); err != nil {
		return nil, fmt.Errorf("%w: truncated header", ErrMalformed)
	}
	if head[4] == sealedVersion {
		if r.salt, err = read(16); err != nil {
			return nil, fmt.Errorf("%w: truncated header", ErrMalformed)
		}
		if r.keyID, err = read(8); err != nil {
			return nil, fmt.Errorf("%w: truncated header", ErrMalformed)
		}
	}
	truncated := func() (*Reader, error) {
		r.Truncated = true
		return r, nil
	}
	for {
		start := pos
		back, err := varint()
		if errors.Is(err, io.EOF) && pos == start {
			return r, nil // the end, between records
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return truncated()
			}
			return nil, err
		}
		i := uint64(len(r.records))
		if back > i || back > r.window {
			return nil, fmt.Errorf("%w: record %d names a base %d back", ErrMalformed, i, back)
		}
		metaLen, err := varint()
		if errors.Is(err, io.EOF) {
			return truncated()
		} else if err != nil {
			return nil, err
		}
		// lengths past the file's end are a record cut short: nothing is allocated for them
		if metaLen > uint64(size-pos) {
			return truncated()
		}
		meta, err := read(int(metaLen))
		if err != nil {
			return truncated()
		}
		deltaLen, err := varint()
		if errors.Is(err, io.EOF) {
			return truncated()
		} else if err != nil {
			return nil, err
		}
		if deltaLen > uint64(size-pos) {
			return truncated()
		}
		if _, err := in.Discard(int(deltaLen)); err != nil {
			return truncated()
		}
		pos += int64(deltaLen)
		r.records = append(r.records, Record{Meta: meta, Back: back, Stored: deltaLen, offset: pos - int64(deltaLen)})
	}
}

// Key is the 32 bytes a sealed file is sealed with.
type Key [32]byte

// ParseKey reads a key written as 64 hex digits.
func ParseKey(s string) (Key, error) {
	var k Key
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 32 {
		return k, errors.New("a key is 64 hex digits")
	}
	copy(k[:], b)
	return k, nil
}

// IsSealed: the file's records are readable only with its key (WithKey).
func (r *Reader) IsSealed() bool { return r.keyID != nil }

// WithKey makes a sealed file readable. It refuses a file that is not sealed, and a key other
// than the file's.
func (r *Reader) WithKey(key Key) error {
	if !r.IsSealed() {
		return ErrNotSealed
	}
	id := sha256.Sum256(key[:])
	if string(id[:8]) != string(r.keyID) {
		return errors.New("the key is not this file's")
	}
	fileKey := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha256.New, key[:], r.salt, []byte("DPK1 sealed records")), fileKey); err != nil {
		return err
	}
	aead, err := chacha20poly1305.New(fileKey)
	if err != nil {
		return err
	}
	r.aead = aead
	return nil
}

// Len is the number of whole records.
func (r *Reader) Len() int { return len(r.records) }

// Records lists every record's meta, base and size, without rebuilding a body.
func (r *Reader) Records() []Record { return r.records }

func (r *Reader) delta(i int) ([]byte, error) {
	rec := r.records[i]
	buf := make([]byte, rec.Stored)
	if _, err := r.src.ReadAt(buf, rec.offset); err != nil {
		return nil, err
	}
	if !r.IsSealed() {
		return buf, nil
	}
	if r.aead == nil {
		return nil, errors.New("the file is sealed: read it with its key")
	}
	var nonce [12]byte
	binary.LittleEndian.PutUint64(nonce[:], uint64(i))
	aad := binary.LittleEndian.AppendUint64(nil, uint64(i))
	aad = binary.LittleEndian.AppendUint64(aad, rec.Back)
	aad = append(aad, rec.Meta...)
	delta, err := r.aead.Open(nil, nonce[:], buf, aad)
	if err != nil {
		return nil, fmt.Errorf("%w: record %d does not open", ErrCorrupt, i)
	}
	return delta, nil
}

// rebuild is record i's body from its base's.
func (r *Reader) rebuild(i int, base []byte) ([]byte, error) {
	delta, err := r.delta(i)
	if err != nil {
		return nil, err
	}
	h, frame, err := parseDelta(delta, r.maxBody)
	if err != nil {
		return nil, fmt.Errorf("record %d: %w", i, err)
	}
	if sha256.Sum256(base) != h.base {
		return nil, fmt.Errorf("record %d: %w: its base is not the body it names", i, ErrCorrupt)
	}
	body, err := apply(base, h, frame)
	if err != nil {
		return nil, fmt.Errorf("record %d: %w", i, err)
	}
	return body, nil
}

// Get rebuilds record i's body, through its chain of bases, each checked by SHA-256.
func (r *Reader) Get(i int) ([]byte, error) {
	if i < 0 || i >= len(r.records) {
		return nil, fmt.Errorf("record %d of %d", i, len(r.records))
	}
	chain := []int{i}
	for j := i; r.records[j].Back > 0; {
		j -= int(r.records[j].Back)
		chain = append(chain, j)
	}
	var body []byte
	for k := len(chain) - 1; k >= 0; k-- {
		var err error
		if body, err = r.rebuild(chain[k], body); err != nil {
			return nil, err
		}
	}
	return body, nil
}

// Scan calls f with every record and its body, in order, each rebuilt once (it holds the last
// window bodies, not the file).
func (r *Reader) Scan(f func(i int, rec Record, body []byte) error) error {
	var recent [][]byte // grown as read: the header's window is not trusted for an allocation
	for i, rec := range r.records {
		var base []byte
		if rec.Back > 0 {
			base = recent[len(recent)-int(rec.Back)]
		}
		body, err := r.rebuild(i, base)
		if err != nil {
			return err
		}
		if err := f(i, rec, body); err != nil {
			return err
		}
		recent = append(recent, body)
		if uint64(len(recent)) > r.window {
			recent = recent[1:]
		}
	}
	return nil
}

type recent struct {
	index        uint64
	hash         [32]byte
	body         []byte
	depth, chain int
}

// Writer appends records to a new DPK1 file (version 1, not sealed), each a delta against the
// one of the last window records it shares most with, as the reference writer chooses.
type Writer struct {
	out    *bufio.Writer
	window int
	next   uint64
	recent []recent
}

// NewWriter writes a DPK1 header to w. window is how many of the latest records a new one may
// delta against: about one per trace or conversation interleaved in the log.
func NewWriter(w io.Writer, window int) (*Writer, error) {
	if window < 1 {
		return nil, errors.New("window is at least 1")
	}
	out := bufio.NewWriter(w)
	head := appendVarint([]byte{'D', 'P', 'K', '1', plain}, uint64(window))
	if _, err := out.Write(head); err != nil {
		return nil, err
	}
	return &Writer{out: out, window: window}, nil
}

type encoded struct {
	back         uint64
	depth, chain int
	delta        []byte
}

// Append writes one record and returns its index. Call Flush to make it readable.
func (w *Writer) Append(meta, body []byte) (uint64, error) {
	hash := sha256.Sum256(body)
	encode := func(b *recent) encoded {
		if b == nil {
			return encoded{delta: encodeHashed(nil, sha256.Sum256(nil), body, hash)}
		}
		return encoded{w.next - b.index, b.depth + 1, b.chain, encodeHashed(b.body, b.hash, body, hash)}
	}
	var bases []*recent
	for k := range w.recent {
		if b := &w.recent[k]; b.depth+1 < maxDepth && b.chain+len(body) <= maxChainBytes {
			bases = append(bases, b)
		}
	}
	var best *recent
	bestShared := -1
	for _, b := range bases {
		if s := shared(b.body, body); s >= bestShared {
			best, bestShared = b, s
		}
	}
	var e encoded
	if best != nil && bestShared*2 >= len(body) {
		// a re-sent context shares most of its bytes at the edges: that base, one encode
		e = encode(best)
	} else {
		// otherwise the bases newest first: the first that makes the record under 1/64 of its
		// size, else the smallest, or the record whole if that is no larger
		var smallest *encoded
		found := false
		for k := len(bases) - 1; k >= 0; k-- {
			t := encode(bases[k])
			if len(t.delta)*64 <= len(body) {
				e, found = t, true
				break
			}
			if smallest == nil || len(t.delta) < len(smallest.delta) {
				smallest = &t
			}
		}
		if !found {
			e = encode(nil)
			if smallest != nil && len(smallest.delta) < len(e.delta) {
				e = *smallest
			}
		}
	}
	rec := appendVarint(nil, e.back)
	rec = appendVarint(rec, uint64(len(meta)))
	rec = append(rec, meta...)
	rec = appendVarint(rec, uint64(len(e.delta)))
	if _, err := w.out.Write(rec); err != nil {
		return 0, err
	}
	if _, err := w.out.Write(e.delta); err != nil {
		return 0, err
	}
	index := w.next
	w.next++
	w.recent = append(w.recent, recent{index, hash, append([]byte(nil), body...), e.depth, e.chain + len(body)})
	if len(w.recent) > w.window {
		w.recent = w.recent[1:]
	}
	return index, nil
}

// Flush writes what Append buffered.
func (w *Writer) Flush() error { return w.out.Flush() }
