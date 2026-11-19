"""Seekable zstd archives for long-term agent logs.

Layout follows the zstd seekable format (contrib/seekable_format in the zstd
repository): independent zstd frames, then a skippable frame holding the seek
table, whose 9-byte footer ends the file:

  frame* [message-index skippable frame] seek-table skippable frame

  seek table  := 0x184D2A5E:u32le size:u32le {csize:u32le dsize:u32le}* footer
  footer      := n_frames:u32le descriptor:u8 0x8F92EAB1:u32le

Standard zstd decoders read the archive as an ordinary concatenation (they skip
skippable frames), and seekable-aware readers jump to any byte offset by
decompressing one frame. Frames are cut on message boundaries once they reach
``frame_bytes`` uncompressed. Every frame carries zstd's content checksum (the
spec's optional per-entry checksum is not used, descriptor bit 7 = 0).

AgentWire adds an optional message index in its own skippable frame
(magic 0x184D2A5B): message lengths as uleb128, zstd-compressed, so a reader
can fetch message i with one frame decompression. Frames may use a shared zstd
dictionary (its id is in each frame header; the reader must be given it).
"""
from __future__ import annotations

import bisect
import struct
from dataclasses import dataclass

from .transport import _read_varint, _varint
from .zstd import ZstdError, compress_advanced, decompress_advanced

SKIPPABLE_SEEK = 0x184D2A5E
SKIPPABLE_INDEX = 0x184D2A5B
SEEKABLE_MAGIC = 0x8F92EAB1
_MAX_FRAME = 0x7FFFFFFF


@dataclass(frozen=True)
class FrameEntry:
    offset: int      # compressed offset in the archive
    csize: int
    dstart: int      # decompressed offset
    dsize: int


class SeekableWriter:
    def __init__(self, level: int = 3, frame_bytes: int = 256 * 1024,
                 dictionary: bytes | None = None, message_index: bool = True):
        self.level = level
        self.frame_bytes = max(1, frame_bytes)
        self.dictionary = dictionary
        self.message_index = message_index
        self._out = bytearray()
        self._entries: list[tuple[int, int]] = []
        self._pending: list[bytes] = []
        self._pending_len = 0
        self._lengths: list[int] = []

    def add(self, message: bytes):
        self._pending.append(message)
        self._pending_len += len(message)
        self._lengths.append(len(message))
        if self._pending_len >= self.frame_bytes:
            self._cut()

    def _cut(self):
        if not self._pending:
            return
        data = b"".join(self._pending)
        if len(data) > _MAX_FRAME:
            raise ValueError("frame too large for a u32 seek-table entry")
        frame = compress_advanced(data, self.level, dictionary=self.dictionary, checksum=True)
        self._out += frame
        self._entries.append((len(frame), len(data)))
        self._pending, self._pending_len = [], 0

    def finish(self) -> bytes:
        self._cut()
        out = self._out
        if self.message_index:
            idx = b"".join(_varint(n) for n in self._lengths)
            body = _varint(len(self._lengths)) + _varint(len(idx)) + compress_advanced(idx, 19)
            out += struct.pack("<II", SKIPPABLE_INDEX, len(body)) + body
        table = b"".join(struct.pack("<II", c, d) for c, d in self._entries)
        footer = struct.pack("<IBI", len(self._entries), 0, SEEKABLE_MAGIC)
        out += struct.pack("<II", SKIPPABLE_SEEK, len(table) + len(footer)) + table + footer
        return bytes(out)


class SeekableReader:
    def __init__(self, blob: bytes, dictionary: bytes | None = None):
        self.blob = blob
        self.dictionary = dictionary
        if len(blob) < 17:
            raise ValueError("archive too short")
        n, desc, magic = struct.unpack("<IBI", blob[-9:])
        if magic != SEEKABLE_MAGIC or desc & 0x7C:
            raise ValueError("missing seekable footer")
        entry = 12 if desc & 0x80 else 8
        table_len = n * entry + 9
        start = len(blob) - table_len - 8
        if start < 0:
            raise ValueError("truncated seek table")
        skip, size = struct.unpack_from("<II", blob, start)
        if skip != SKIPPABLE_SEEK or size != table_len:
            raise ValueError("bad seek table header")
        self.frames: list[FrameEntry] = []
        off = dpos = 0
        for i in range(n):
            c, d = struct.unpack_from("<II", blob, start + 8 + i * entry)
            self.frames.append(FrameEntry(off, c, dpos, d))
            off += c
            dpos += d
        self.size = dpos
        self._data_end = off
        self._starts = [f.dstart for f in self.frames]
        self._msg_offsets: list[int] | None = None
        if off < start:   # optional message index between frames and seek table
            skip, size = struct.unpack_from("<II", blob, off)
            if skip == SKIPPABLE_INDEX and off + 8 + size == start:
                self._load_index(blob[off + 8:off + 8 + size])
            else:
                raise ValueError("unexpected bytes before seek table")
        elif off != start:
            raise ValueError("seek table does not match frame sizes")
        self._cache: tuple[int, bytes] | None = None

    def _load_index(self, body: bytes):
        count, p = _read_varint(body, 0)
        raw_len, p = _read_varint(body, p)
        try:
            idx = decompress_advanced(body[p:], raw_len)
        except ZstdError as exc:
            raise ValueError(f"corrupt message index: {exc}") from exc
        offsets, q, total = [0], 0, 0
        for _ in range(count):
            n, q = _read_varint(idx, q)
            total += n
            offsets.append(total)
        if total != self.size:
            raise ValueError("message index does not match archive size")
        self._msg_offsets = offsets

    @property
    def message_count(self) -> int:
        return 0 if self._msg_offsets is None else len(self._msg_offsets) - 1

    def _frame(self, i: int) -> bytes:
        if self._cache and self._cache[0] == i:
            return self._cache[1]
        f = self.frames[i]
        try:
            data = decompress_advanced(self.blob[f.offset:f.offset + f.csize], f.dsize,
                                       dictionary=self.dictionary)
        except ZstdError as exc:
            raise ValueError(f"corrupt frame {i}: {exc}") from exc
        if len(data) != f.dsize:
            raise ValueError(f"frame {i} size mismatch")
        self._cache = (i, data)
        return data

    def read(self, offset: int, length: int) -> bytes:
        if offset < 0 or length < 0 or offset + length > self.size:
            raise ValueError("range outside archive")
        out, pos, end = [], offset, offset + length
        while pos < end:
            i = bisect.bisect_right(self._starts, pos) - 1
            f = self.frames[i]
            data = self._frame(i)
            take = data[pos - f.dstart:min(end, f.dstart + f.dsize) - f.dstart]
            out.append(take)
            pos += len(take)
        return b"".join(out)

    def message(self, i: int) -> bytes:
        if self._msg_offsets is None:
            raise ValueError("archive has no message index")
        return self.read(self._msg_offsets[i], self._msg_offsets[i + 1] - self._msg_offsets[i])

    def read_all(self) -> bytes:
        return b"".join(self._frame(i) for i in range(len(self.frames)))
