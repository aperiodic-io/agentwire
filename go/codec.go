// Package deltapack reads and writes DPK1 files (SPEC.md §7): LLM request logs and trace spans
// stored as DPD1 deltas against earlier records, every body checked by SHA-256 when it is read.
// Files are interchangeable with the deltapack CLI, its collector and the Rust and Python libraries.
package deltapack

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/bits"

	"github.com/klauspost/compress/zstd"
)

const (
	magic     = "DPD1"
	version   = 1
	headerLen = 4 + 1 + 32 + 32
	// the largest zstd window a delta may need (SPEC.md §3)
	maxWindowLog = 27
)

// The errors a delta is refused with (SPEC.md §4); match with errors.Is.
var (
	ErrMalformed          = errors.New("malformed")
	ErrUnsupportedVersion = errors.New("unsupported-version")
	ErrTooLarge           = errors.New("too-large")
	ErrCorrupt            = errors.New("corrupt")
)

type header struct {
	base, target              [32]byte
	prefix, suffix, targetLen uint64
}

// encodeDelta is a DPD1 body that rebuilds target from base.
func encodeDelta(base, target []byte) []byte {
	return encodeHashed(base, sha256.Sum256(base), target, sha256.Sum256(target))
}

func encodeHashed(base []byte, baseHash [32]byte, target []byte, targetHash [32]byte) []byte {
	prefix := commonPrefix(base, target)
	suffix := commonSuffix(base[prefix:], target[prefix:])
	reference := base[prefix : len(base)-suffix]
	literal := target[prefix : len(target)-suffix]
	out := make([]byte, 0, headerLen+30+len(literal)/2)
	out = append(out, magic...)
	out = append(out, version)
	out = append(out, baseHash[:]...)
	out = append(out, targetHash[:]...)
	for _, v := range []int{prefix, suffix, len(target)} {
		out = appendVarint(out, uint64(v))
	}
	return compress(out, reference, literal)
}

func compress(out, reference, literal []byte) []byte {
	span := max(len(reference)+len(literal), 1)
	windowLog := min(max(bits.Len(uint(span-1)), 10), maxWindowLog)
	opts := []zstd.EOption{
		// the level whose literals are entropy-coded, as zstd level 3 is (SpeedDefault stores hex as is)
		zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
		zstd.WithWindowSize(1 << windowLog),
		zstd.WithEncoderCRC(false), // the target's SHA-256 checks the whole rebuilt body
		zstd.WithEncoderConcurrency(1),
		zstd.WithZeroFrames(true),
	}
	if len(reference) > 0 {
		opts = append(opts, zstd.WithEncoderDictRaw(0, reference))
	}
	enc, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		panic(fmt.Sprintf("valid zstd options: %v", err)) // fixed options: never
	}
	defer enc.Close()
	return enc.EncodeAll(literal, out)
}

// parseDelta splits a delta into its header and zstd frame; maxLen caps the target's length.
func parseDelta(delta []byte, maxLen uint64) (header, []byte, error) {
	var h header
	if len(delta) < 5 {
		return h, nil, fmt.Errorf("%w: truncated header", ErrMalformed)
	}
	if string(delta[:4]) != magic {
		return h, nil, fmt.Errorf("%w: not a DPD1 body", ErrMalformed)
	}
	if delta[4] != version {
		return h, nil, fmt.Errorf("%w: version %d", ErrUnsupportedVersion, delta[4])
	}
	if len(delta) < headerLen {
		return h, nil, fmt.Errorf("%w: truncated header", ErrMalformed)
	}
	copy(h.base[:], delta[5:37])
	copy(h.target[:], delta[37:69])
	pos := headerLen
	vs := [3]uint64{}
	for i := range vs {
		v, n, err := readVarint(delta[pos:])
		if err != nil {
			return h, nil, err
		}
		vs[i], pos = v, pos+n
	}
	h.prefix, h.suffix, h.targetLen = vs[0], vs[1], vs[2]
	if h.targetLen > maxLen {
		return h, nil, ErrTooLarge
	}
	if edges := h.prefix + h.suffix; edges < h.prefix || edges > h.targetLen {
		return h, nil, fmt.Errorf("%w: prefix and suffix exceed the target", ErrMalformed)
	}
	return h, delta[pos:], nil
}

// apply rebuilds the target from base, which must be the body h.base names.
func apply(base []byte, h header, frame []byte) ([]byte, error) {
	edges := h.prefix + h.suffix // parseDelta checked it fits the target
	if edges > uint64(len(base)) {
		return nil, fmt.Errorf("%w: prefix and suffix exceed the base", ErrMalformed)
	}
	prefix, suffix := int(h.prefix), int(h.suffix)
	literalLen := int(h.targetLen - edges)
	literal, err := decompress(base[prefix:len(base)-suffix], frame, literalLen)
	if err != nil {
		return nil, err
	}
	target := make([]byte, 0, prefix+literalLen+suffix)
	target = append(target, base[:prefix]...)
	target = append(target, literal...)
	target = append(target, base[len(base)-suffix:]...)
	if sha256.Sum256(target) != h.target {
		return nil, ErrCorrupt
	}
	return target, nil
}

func decompress(reference, frame []byte, literalLen int) ([]byte, error) {
	opts := []zstd.DOption{
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxWindow(1 << maxWindowLog),
	}
	if len(reference) > 0 {
		opts = append(opts, zstd.WithDecoderDictRaw(0, reference))
	}
	dec, err := zstd.NewReader(bytes.NewReader(frame), opts...)
	if err != nil {
		return nil, ErrCorrupt
	}
	defer dec.Close()
	// exactly the literal's room, read as a stream: a frame that expands further is refused
	// before it is decoded past it
	literal := make([]byte, literalLen)
	if _, err := io.ReadFull(dec, literal); err != nil {
		return nil, ErrCorrupt
	}
	if n, err := dec.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		return nil, ErrCorrupt
	}
	return literal, nil
}

// readVarint reads canonical unsigned LEB128, at most 10 bytes.
func readVarint(buf []byte) (uint64, int, error) {
	var v uint64
	for i := 0; i < len(buf) && i < 10; i++ {
		b := buf[i]
		if i == 9 && b > 1 {
			return 0, 0, fmt.Errorf("%w: varint overflow", ErrMalformed)
		}
		v |= uint64(b&0x7f) << (7 * i)
		if b&0x80 == 0 {
			if b == 0 && i > 0 {
				return 0, 0, fmt.Errorf("%w: non-canonical varint", ErrMalformed)
			}
			return v, i + 1, nil
		}
	}
	if len(buf) >= 10 {
		return 0, 0, fmt.Errorf("%w: varint overflow", ErrMalformed)
	}
	return 0, 0, fmt.Errorf("%w: truncated varint", ErrMalformed)
}

func appendVarint(out []byte, v uint64) []byte {
	for v >= 0x80 {
		out = append(out, byte(v)|0x80)
		v >>= 7
	}
	return append(out, byte(v))
}

func commonPrefix(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i+64 <= n && bytes.Equal(a[i:i+64], b[i:i+64]) {
		i += 64
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func commonSuffix(a, b []byte) int {
	n := min(len(a), len(b))
	a, b = a[len(a)-n:], b[len(b)-n:]
	i := 0
	for i+64 <= n && bytes.Equal(a[n-i-64:n-i], b[n-i-64:n-i]) {
		i += 64
	}
	for i < n && a[n-i-1] == b[n-i-1] {
		i++
	}
	return i
}

func shared(base, target []byte) int {
	p := commonPrefix(base, target)
	return p + commonSuffix(base[p:], target[p:])
}
