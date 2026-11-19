"""FastCDC content-defined chunking plus a deduplicating, compressed chunk store.

FastCDC (Xia et al., USENIX ATC 2016) cuts a byte stream where a rolling "gear"
hash matches a mask, so boundaries depend on content, not offsets: an insertion
only changes the chunks around it. Normalized chunking uses a stricter mask
before the target size and a looser one after it, which tightens the size
distribution. Identical chunks across messages, sessions or agents are stored
once (keyed by SHA-256) and each unique chunk is zstd-compressed.

Pure Python: the per-byte gear loop runs at a few MB/s, which is enough to
measure dedup ratios; a production version belongs in native code.

Serialized store (all big-endian):
  "AWK1" count:u32 { sha256[32] raw_len:u32 comp_len:u32 zstd_frame }*
A reference ("recipe") is the concatenation of the chunk digests' first 16 bytes.
"""
from __future__ import annotations

import hashlib
import random
import struct
from dataclasses import dataclass

from .zstd import Zstd, ZstdError

_MASK64 = (1 << 64) - 1
_RNG = random.Random(0x5EED)
_GEAR = [_RNG.getrandbits(64) for _ in range(256)]


def _mask(bits: int) -> int:
    # Spread the one-bits over the high half like FastCDC's padded masks.
    m, step = 0, max(1, 48 // max(1, bits))
    for i in range(bits):
        m |= 1 << (63 - i * step)
    return m


class FastCDC:
    def __init__(self, avg_size: int = 8192, min_size: int | None = None, max_size: int | None = None):
        if avg_size < 64 or avg_size & (avg_size - 1):
            raise ValueError("avg_size must be a power of two >= 64")
        self.avg_size = avg_size
        self.min_size = min_size or avg_size // 4
        self.max_size = max_size or avg_size * 8
        bits = avg_size.bit_length() - 1
        self._mask_s = _mask(bits + 2)    # harder to match before the average
        self._mask_l = _mask(bits - 2)    # easier after it

    def cut(self, data: bytes, start: int = 0) -> int:
        n = len(data) - start
        if n <= self.min_size:
            return len(data)
        end = start + min(n, self.max_size)
        normal = start + min(n, self.avg_size)
        fp, gear, ms, ml = 0, _GEAR, self._mask_s, self._mask_l
        i = start + self.min_size
        while i < normal:
            fp = ((fp << 1) + gear[data[i]]) & _MASK64
            if not fp & ms:
                return i + 1
            i += 1
        while i < end:
            fp = ((fp << 1) + gear[data[i]]) & _MASK64
            if not fp & ml:
                return i + 1
            i += 1
        return end

    def chunks(self, data: bytes) -> list[bytes]:
        out, pos = [], 0
        while pos < len(data):
            nxt = self.cut(data, pos)
            out.append(data[pos:nxt])
            pos = nxt
        return out


@dataclass
class StoreStats:
    logical_bytes: int
    unique_bytes: int
    stored_bytes: int
    chunks: int
    unique_chunks: int
    recipe_bytes: int


_ENTRY = struct.Struct(">32sII")
DIGEST_BYTES = 16


class ChunkStore:
    def __init__(self, cdc: FastCDC, level: int = 3, dictionary: bytes | None = None):
        self.cdc = cdc
        self.zstd = Zstd(level)
        self.dictionary = dictionary
        self._chunks: dict[bytes, tuple[int, bytes]] = {}   # digest -> (raw_len, frame)
        self._by_short: dict[bytes, bytes] = {}
        self.logical = self.total_chunks = self.recipe_bytes = 0

    def put(self, data: bytes) -> bytes:
        recipe = bytearray()
        for c in self.cdc.chunks(data):
            d = hashlib.sha256(c).digest()
            if d not in self._chunks:
                short = d[:DIGEST_BYTES]
                if short in self._by_short:
                    raise ValueError("16-byte digest prefix collision")
                self._chunks[d] = (len(c), self.zstd.compress(c, self.dictionary))
                self._by_short[short] = d
            recipe += d[:DIGEST_BYTES]
            self.total_chunks += 1
        self.logical += len(data)
        self.recipe_bytes += len(recipe) + 4
        return bytes(recipe)

    def get(self, recipe: bytes) -> bytes:
        if len(recipe) % DIGEST_BYTES:
            raise ValueError("malformed recipe")
        parts = []
        for i in range(0, len(recipe), DIGEST_BYTES):
            d = self._by_short.get(recipe[i:i + DIGEST_BYTES])
            if d is None:
                raise ValueError("unknown chunk")
            n, frame = self._chunks[d]
            try:
                raw = self.zstd.decompress(frame, n, self.dictionary)
            except ZstdError as exc:
                raise ValueError(f"corrupt chunk: {exc}") from exc
            if hashlib.sha256(raw).digest() != d:
                raise ValueError("chunk digest mismatch")
            parts.append(raw)
        return b"".join(parts)

    def stats(self) -> StoreStats:
        unique = sum(n for n, _ in self._chunks.values())
        stored = sum(len(f) + _ENTRY.size for _, f in self._chunks.values())
        return StoreStats(self.logical, unique, stored, self.total_chunks, len(self._chunks), self.recipe_bytes)

    def serialize(self) -> bytes:
        out = [b"AWK1", struct.pack(">I", len(self._chunks))]
        for d, (n, frame) in self._chunks.items():
            out.append(_ENTRY.pack(d, n, len(frame)))
            out.append(frame)
        return b"".join(out)

    @classmethod
    def deserialize(cls, blob: bytes, cdc: FastCDC, level: int = 3, dictionary: bytes | None = None):
        if blob[:4] != b"AWK1":
            raise ValueError("not an AWK1 store")
        store = cls(cdc, level, dictionary)
        (count,) = struct.unpack_from(">I", blob, 4)
        pos = 8
        for _ in range(count):
            if pos + _ENTRY.size > len(blob):
                raise ValueError("truncated store")
            d, n, m = _ENTRY.unpack_from(blob, pos)
            pos += _ENTRY.size
            store._chunks[d] = (n, blob[pos:pos + m])
            store._by_short[d[:DIGEST_BYTES]] = d
            pos += m
        if pos != len(blob):
            raise ValueError("trailing bytes in store")
        return store
