"""AWD1: context-delta stream for "the whole context is re-sent every turn".

LLM conversations are append-mostly: request k+1 is request k plus a few new
messages (and a closing bracket moved). A general-purpose compressor only
exploits that if its window still holds the previous request, which fails once
the context outgrows the window (1-8 MiB for AWC1). AWD1 removes the
redundancy structurally before compressing:

  message  = R[:prefix] + literal + R[len(R) - suffix:]

where R is one of the last ``references`` messages on the connection (chosen by
the encoder to maximise prefix + suffix). Only ``literal`` and three small
integers are new; they go through one long-lived zstd stream (context takeover,
flushed per message), so literals still benefit from history and an optional
shared dictionary. Memory is O(references x message size) plus the zstd window,
independent of how long the conversation's context grows.

Wire (big-endian CRC; see PROTOCOL.md):

  stream := "AWD1" flags:u8 window_log:u8 references:u8 [dict_fp:16 if flags&1] record*
  record := body_len:uleb128 payload_len:uleb128 crc32(message):u32 payload
  body   := ref:u8 prefix:uleb128 suffix:uleb128 literal          (zstd-flushed)

Lossless and byte-exact; any inconsistency (bad CRC, bounds, unknown
reference, decompression error) raises ProtocolError and poisons the decoder.
"""
from __future__ import annotations

import hashlib
import struct
import zlib

from .stream import _try_varint
from .transport import ProtocolError, _read_varint, _varint
from .zstd import ZstdError, ZstdStreamCompressor, ZstdStreamDecompressor, _check_window_log

MAGIC = b"AWD1"
FLAG_DICTIONARY = 1
_CRC = struct.Struct(">I")


def common_prefix(a: bytes, b: bytes) -> int:
    """Length of the longest common prefix, using C-speed slice compares."""
    n = min(len(a), len(b))
    if a[:n] == b[:n]:
        return n
    lo, hi = 0, n            # invariant: a[:lo] == b[:lo], a[:hi] != b[:hi]
    step = 64
    while lo + step < hi and a[lo:lo + step] == b[lo:lo + step]:
        lo += step
        step *= 2
    hi = min(hi, lo + step)
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if a[lo:mid] == b[lo:mid]:
            lo = mid
        else:
            hi = mid
    return lo + (1 if lo < n and a[lo] == b[lo] else 0)


def common_suffix(a: bytes, b: bytes, limit: int | None = None) -> int:
    n = min(len(a), len(b)) if limit is None else min(len(a), len(b), limit)
    if n == 0:
        return 0
    return common_prefix(a[len(a) - n:][::-1], b[len(b) - n:][::-1])


def _fingerprint(d: bytes) -> bytes:
    return hashlib.sha256(d).digest()[:16]


class DeltaEncoder:
    def __init__(self, references: int = 4, level: int = 3, window_log: int = 22,
                 dictionary: bytes | None = None):
        _check_window_log(window_log)
        if not 1 <= references <= 32:
            raise ValueError("references must be in [1, 32]")
        self.references = references
        self._refs: list[bytes] = []
        self._z = ZstdStreamCompressor(level, dictionary, window_log)
        flags = FLAG_DICTIONARY if dictionary else 0
        self._preamble = MAGIC + bytes((flags, window_log, references)) + (_fingerprint(dictionary) if dictionary else b"")

    def _best(self, m: bytes) -> tuple[int, int, int]:
        best = (0, 0, 0)
        for i, ref in enumerate(self._refs):
            p = common_prefix(ref, m)
            s = common_suffix(ref, m, min(len(ref), len(m)) - p)
            if p + s > best[1] + best[2]:
                best = (i, p, s)
        return best

    def encode(self, message: bytes) -> bytes:
        if not isinstance(message, bytes):
            raise TypeError("message must be bytes")
        ref, p, s = self._best(message)
        literal = message[p:len(message) - s]
        body = bytes((ref,)) + _varint(p) + _varint(s) + literal
        payload = self._z.flush(body)
        out = self._preamble + _varint(len(body)) + _varint(len(payload)) + \
            _CRC.pack(zlib.crc32(message)) + payload
        self._preamble = b""
        self._refs.insert(0, message)
        del self._refs[self.references:]
        return out

    def memory_bytes(self) -> int:
        return self._z.memory_bytes() + sum(len(r) for r in self._refs)

    def close(self):
        self._z.close()


class DeltaDecoder:
    def __init__(self, references: int = 4, dictionary: bytes | None = None,
                 max_message_bytes: int = 64 << 20, max_window_log: int = 23):
        self.references = references
        self.dictionary = dictionary
        self.max_message_bytes = max_message_bytes
        self.max_window_log = max_window_log
        self._refs: list[bytes] = []
        self._buf = bytearray()
        self._z: ZstdStreamDecompressor | None = None
        self._failed: Exception | None = None

    def feed(self, chunk: bytes) -> list[bytes]:
        if self._failed is not None:
            raise ProtocolError("delta session already failed") from self._failed
        self._buf.extend(chunk)
        try:
            return self._drain()
        except (ProtocolError, ZstdError, ValueError) as exc:
            self._failed = exc
            if isinstance(exc, ProtocolError):
                raise
            raise ProtocolError(str(exc)) from exc

    def _drain(self) -> list[bytes]:
        buf, out = self._buf, []
        if self._z is None:
            if len(buf) < 7:
                return out
            if bytes(buf[:4]) != MAGIC:
                raise ProtocolError("not an AWD1 stream")
            flags, wlog, refs = buf[4], buf[5], buf[6]
            if flags & ~FLAG_DICTIONARY or refs != self.references or wlog > self.max_window_log:
                raise ProtocolError("unsupported AWD1 parameters")
            need = 7 + (16 if flags & FLAG_DICTIONARY else 0)
            if len(buf) < need:
                return out
            if flags & FLAG_DICTIONARY:
                if not self.dictionary or _fingerprint(self.dictionary) != bytes(buf[7:23]):
                    raise ProtocolError("AWD1 dictionary missing or mismatched")
            elif self.dictionary:
                raise ProtocolError("peer sent no dictionary but one was configured")
            self._z = ZstdStreamDecompressor(self.dictionary if flags & FLAG_DICTIONARY else None,
                                             max(wlog, 10))
            del buf[:need]
        while buf:
            a = _try_varint(buf, 0)
            if a is None:
                break
            body_len, pos = a
            b = _try_varint(buf, pos)
            if b is None:
                break
            payload_len, pos = b
            if body_len > self.max_message_bytes + 32 or payload_len > self.max_message_bytes + 1024:
                raise ProtocolError("AWD1 record exceeds limits")
            if len(buf) < pos + 4 + payload_len:
                break
            (crc,) = _CRC.unpack_from(buf, pos)
            payload = bytes(buf[pos + 4:pos + 4 + payload_len])
            del buf[:pos + 4 + payload_len]
            body = self._z.decompress(payload, body_len)
            if not body:
                raise ProtocolError("empty AWD1 body")
            ref = body[0]
            p, q = _read_varint(body, 1)
            s, q = _read_varint(body, q)
            literal = body[q:]
            if ref:
                if ref >= len(self._refs):
                    raise ProtocolError("unknown AWD1 reference")
            base = self._refs[ref] if self._refs else b""
            if p + s > len(base):
                raise ProtocolError("AWD1 prefix/suffix beyond reference")
            if p + s + len(literal) > self.max_message_bytes:
                raise ProtocolError("AWD1 message exceeds limit")
            message = base[:p] + literal + (base[len(base) - s:] if s else b"")
            if zlib.crc32(message) != crc:
                raise ProtocolError("AWD1 CRC32 mismatch")
            self._refs.insert(0, message)
            del self._refs[self.references:]
            out.append(message)
        return out

    def finish(self):
        if self._failed is None and self._buf:
            raise ProtocolError("truncated AWD1 record")

    def close(self):
        if self._z is not None:
            self._z.close()
