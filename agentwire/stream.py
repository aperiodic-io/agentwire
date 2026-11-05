"""AWC1: per-connection Zstd stream with context takeover.

AWF1 compresses every frame independently, so it has to wait and batch to give
Zstd shared history. AWC1 instead keeps ONE Zstd compression stream open for
the whole connection and flushes after every frame. Each message is sent
immediately, yet can reference everything sent earlier on that connection plus
the shared dictionary (like WebSocket permessage-deflate with context takeover).

Wire layout (big-endian; see PROTOCOL.md):

  stream   := "AWC1" RESET (RESET | DATA)*
  RESET    := 0x01 protocol:u8 flags:u8 dictionary_version:u32
              dictionary_fingerprint[16] window_log:u8
  DATA     := 0x00 count:uleb128 original_len:uleb128 payload_len:uleb128
              crc32(uncompressed body):u32 payload[payload_len]

DATA bodies use the same record encoding as AWF1. A RESET starts a fresh Zstd
stream (new dictionary or recovery); DATA frames can only be decoded in order,
after every earlier frame since the last RESET. Any error poisons the session:
the decoder's Zstd state is no longer trustworthy, so the peer must reconnect.
"""
from __future__ import annotations

import struct
import zlib

from .transport import (FLAG_DICTIONARY, MAX_U32, DictionaryRegistry, Message,
                        ProtocolError, _decode_records, _digest, _read_varint,
                        _record, _varint, ZERO_HASH)
from .zstd import (ZstdError, ZstdStreamCompressor, ZstdStreamDecompressor,
                   _check_window_log)

STREAM_MAGIC = b"AWC1"
STREAM_PROTOCOL = 1
TYPE_DATA = 0x00
TYPE_RESET = 0x01
_RESET = struct.Struct(">BBBI16sB")   # type, protocol, flags, version, fingerprint, window_log
_CRC = struct.Struct(">I")


class SessionPoisoned(ProtocolError):
    pass


class StreamEncoder:
    """Encode messages onto one long-lived Zstd stream, flushing per call.

    encode() returns bytes ready to write; the first call also carries the
    stream preamble. Nothing is buffered between calls, so there is no delay.
    """

    def __init__(self, registry: DictionaryRegistry | None = None,
                 dictionary_version: int = 0, level: int = 3,
                 window_log: int = 20, max_raw_bytes: int = 1024 * 1024,
                 max_messages: int = 65535):
        _check_window_log(window_log)
        if not 1 <= max_raw_bytes <= MAX_U32 or not 1 <= max_messages <= 65535:
            raise ValueError("invalid limits")
        self.registry = registry or DictionaryRegistry()
        self.level = level
        self.window_log = window_log
        self.max_raw_bytes = max_raw_bytes
        self.max_messages = max_messages
        self.frames_encoded = 0
        self._started = False
        self._compressor: ZstdStreamCompressor | None = None
        self.dictionary_version = 0
        self._pending = self._reset(dictionary_version)

    def _reset(self, version: int) -> bytes:
        dictionary = self.registry.get(version) if version else None
        compressor = ZstdStreamCompressor(self.level, dictionary, self.window_log)
        if self._compressor is not None:
            self._compressor.close()
        self._compressor = compressor
        self.dictionary_version = version
        return _RESET.pack(TYPE_RESET, STREAM_PROTOCOL,
                           FLAG_DICTIONARY if dictionary else 0, version,
                           _digest(dictionary) if dictionary else ZERO_HASH,
                           self.window_log)

    def encode(self, records: list[Message]) -> bytes:
        if not 1 <= len(records) <= self.max_messages:
            raise ProtocolError("invalid message count")
        body = b"".join(_record(msg) for msg in records)
        if len(body) > self.max_raw_bytes:
            raise ProtocolError("uncompressed frame exceeds configured limit")
        payload = self._compressor.flush(body)
        frame = (bytes((TYPE_DATA,)) + _varint(len(records)) + _varint(len(body)) +
                 _varint(len(payload)) + _CRC.pack(zlib.crc32(body)) + payload)
        prefix = b""
        if self._pending:
            prefix = (b"" if self._started else STREAM_MAGIC) + self._pending
            self._started = True
            self._pending = b""
        self.frames_encoded += 1
        return prefix + frame

    def rotate(self, new_version: int) -> None:
        """Switch dictionary (or restart history). The RESET is prefixed to the
        next encode() output, so the switch never needs its own write."""
        self._pending = self._reset(new_version)

    def close(self):
        if self._compressor is not None:
            self._compressor.close()
            self._compressor = None


def _try_varint(buffer: bytes | bytearray, position: int) -> tuple[int, int] | None:
    """Like _read_varint, but returns None when more bytes are needed."""
    end = min(len(buffer), position + 10)
    for i in range(position, end):
        if buffer[i] < 0x80:
            return _read_varint(bytes(buffer[position:i + 1]), 0)[0], i + 1
    if end - position >= 10:
        raise ProtocolError("varint longer than 10 bytes")
    return None


class StreamSessionDecoder:
    """Incremental AWC1 parser holding the peer's Zstd stream state."""

    def __init__(self, registry: DictionaryRegistry | None = None,
                 max_raw_bytes: int = 1024 * 1024,
                 max_wire_bytes: int = 2 * 1024 * 1024,
                 max_window_log: int = 20, max_messages: int = 65535):
        _check_window_log(max_window_log)
        self.registry = registry or DictionaryRegistry()
        self.max_raw_bytes = max_raw_bytes
        self.max_wire_bytes = max_wire_bytes
        self.max_window_log = max_window_log
        self.max_messages = max_messages
        self.dictionary_version: int | None = None
        self.frames_decoded = 0
        self._buffer = bytearray()
        self._magic_seen = False
        self._decompressor: ZstdStreamDecompressor | None = None
        self._failed: Exception | None = None

    def feed(self, chunk: bytes) -> list[Message]:
        if not isinstance(chunk, bytes):
            raise TypeError("feed requires bytes")
        if self._failed is not None:
            raise SessionPoisoned("stream session already failed") from self._failed
        self._buffer.extend(chunk)
        try:
            return self._drain()
        except Exception as exc:
            self._failed = exc
            self.close()
            raise

    def _drain(self) -> list[Message]:
        messages: list[Message] = []
        buf = self._buffer
        if not self._magic_seen:
            if len(buf) < len(STREAM_MAGIC):
                return messages
            if bytes(buf[:4]) != STREAM_MAGIC:
                raise ProtocolError("not an AWC1 stream")
            del buf[:4]
            self._magic_seen = True
        while buf:
            kind = buf[0]
            if kind == TYPE_RESET:
                if len(buf) < _RESET.size:
                    break
                self._apply_reset(bytes(buf[:_RESET.size]))
                del buf[:_RESET.size]
                continue
            if kind != TYPE_DATA:
                raise ProtocolError(f"unknown AWC1 frame type {kind}")
            if self._decompressor is None:
                raise ProtocolError("DATA frame before RESET")
            parsed = self._parse_data_header(buf)
            if parsed is None:
                break
            count, original_len, payload_len, crc, start = parsed
            if len(buf) < start + payload_len:
                break
            payload = bytes(buf[start:start + payload_len])
            del buf[:start + payload_len]
            try:
                body = self._decompressor.decompress(payload, original_len)
            except ZstdError as exc:
                raise ProtocolError(f"Zstd stream decompression failed: {exc}") from exc
            if zlib.crc32(body) != crc:
                raise ProtocolError("body CRC32 mismatch")
            messages.extend(_decode_records(body, count))
            self.frames_decoded += 1
        if len(buf) > self.max_wire_bytes + 64:
            raise ProtocolError("buffered incomplete frame exceeds limit")
        return messages

    def _parse_data_header(self, buf: bytearray):
        pos = 1
        fields = []
        for _ in range(3):
            got = _try_varint(buf, pos)
            if got is None:
                return None
            value, pos = got
            fields.append(value)
        count, original_len, payload_len = fields
        # Bounds checked BEFORE waiting for, or decompressing, the payload.
        if not 1 <= count <= self.max_messages:
            raise ProtocolError("message count beyond limit")
        if original_len > self.max_raw_bytes or payload_len > self.max_wire_bytes:
            raise ProtocolError("frame size beyond limit")
        if len(buf) < pos + _CRC.size:
            return None
        (crc,) = _CRC.unpack_from(buf, pos)
        return count, original_len, payload_len, crc, pos + _CRC.size

    def _apply_reset(self, raw: bytes):
        _, protocol, flags, version, fingerprint, window_log = _RESET.unpack(raw)
        if protocol != STREAM_PROTOCOL:
            raise ProtocolError("unsupported AWC1 protocol version")
        if flags & ~FLAG_DICTIONARY:
            raise ProtocolError("unknown AWC1 flags")
        if bool(flags & FLAG_DICTIONARY) != bool(version):
            raise ProtocolError("dictionary flag/version inconsistent")
        if not version and fingerprint != ZERO_HASH:
            raise ProtocolError("unexpected dictionary fingerprint")
        if window_log > self.max_window_log:
            raise ProtocolError(f"peer window_log {window_log} exceeds limit {self.max_window_log}")
        dictionary = self.registry.resolve(version, fingerprint) if version else None
        if self._decompressor is not None:
            self._decompressor.close()
        self._decompressor = ZstdStreamDecompressor(dictionary, self.max_window_log)
        self.dictionary_version = version

    def finish(self):
        if self._failed is None and self._buffer:
            raise ProtocolError("truncated trailing frame")

    def close(self):
        if self._decompressor is not None:
            self._decompressor.close()
            self._decompressor = None
