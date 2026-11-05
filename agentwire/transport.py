"""AWF1 lossless, framed, bounded Zstd transport with versioned dictionaries.

Format (network byte order):
  magic[4], protocol:u8, flags:u8, count:u16, sequence:u32,
  dictionary_version:u32, original_len:u32, payload_len:u32,
  dictionary_sha256_prefix[16], body_sha256_prefix[16], payload[payload_len]

Original body: count repetitions of (message_id:uleb128, kind:u8,
                                 payload_length:uleb128, exact_payload:bytes).

This is NOT an authenticated transport; put it inside TLS or a MACed channel.
"""
from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass

from .zstd import Zstd, ZstdError


MAGIC = b"AWF1"
VERSION = 1
FLAG_DICTIONARY = 1
FLAG_RAW = 2
_HEADER = struct.Struct(">4sBBHIIII16s16s")
HEADER_SIZE = _HEADER.size
ZERO_HASH = bytes(16)
MAX_U32 = 2**32 - 1
MAX_U64 = 2**64 - 1


class ProtocolError(ValueError):
    pass


class MissingDictionary(ProtocolError):
    pass


class DictionaryMismatch(ProtocolError):
    pass


class SequenceError(ProtocolError):
    pass


def _digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()[:16]


def _varint(value: int) -> bytes:
    if not isinstance(value, int) or not 0 <= value <= MAX_U64:
        raise ValueError("varint outside 64-bit unsigned range")
    output = bytearray()
    while value >= 128:
        output.append((value & 0x7F) | 0x80)
        value >>= 7
    output.append(value)
    return bytes(output)


def _read_varint(data: bytes, position: int) -> tuple[int, int]:
    start = position
    result = 0
    for i in range(10):
        if position >= len(data):
            raise ProtocolError("truncated varint")
        byte = data[position]
        position += 1
        if i == 9 and byte > 1:
            raise ProtocolError("varint overflows u64")
        result |= (byte & 0x7F) << (7 * i)
        if byte < 128:
            if data[start:position] != _varint(result):
                raise ProtocolError("noncanonical varint")
            return result, position
    raise ProtocolError("varint longer than 10 bytes")


@dataclass(frozen=True)
class Message:
    id: int
    payload: bytes
    kind: int = 0

    def __post_init__(self):
        if not isinstance(self.payload, bytes):
            raise TypeError("payload must be raw bytes")
        if not isinstance(self.id, int) or not 0 <= self.id <= MAX_U64:
            raise ValueError("message id must be unsigned u64")
        if not isinstance(self.kind, int) or not 0 <= self.kind <= 255:
            raise ValueError("kind must be in [0,255]")


def _record_size(message: Message) -> int:
    return len(_varint(message.id)) + 1 + len(_varint(len(message.payload))) + len(message.payload)


def _record(message: Message) -> bytes:
    return _varint(message.id) + bytes((message.kind,)) + _varint(len(message.payload)) + message.payload


def _decode_records(data: bytes, count: int) -> list[Message]:
    position = 0
    records: list[Message] = []
    for _ in range(count):
        ident, position = _read_varint(data, position)
        if position >= len(data):
            raise ProtocolError("truncated message kind")
        kind = data[position]
        position += 1
        length, position = _read_varint(data, position)
        if length > len(data) - position:
            raise ProtocolError("truncated record payload")
        records.append(Message(ident, data[position:position + length], kind))
        position += length
    if position != len(data):
        raise ProtocolError("trailing undecoded bytes in frame")
    return records


class DictionaryRegistry:
    """Dictionary bytes are provisioned out of band. Never silently use a wrong one."""

    def __init__(self):
        self._dicts: dict[int, bytes] = {}

    def register(self, version: int, dictionary: bytes):
        if not isinstance(version, int) or not 1 <= version <= MAX_U32:
            raise ValueError("dictionary version must be positive u32")
        if not isinstance(dictionary, bytes) or not dictionary:
            raise ValueError("dictionary must be nonempty bytes")
        prior = self._dicts.get(version)
        if prior is not None and prior != dictionary:
            raise DictionaryMismatch("cannot rebind an existing dictionary version")
        self._dicts[version] = dictionary

    def get(self, version: int) -> bytes:
        dic = self._dicts.get(version)
        if dic is None:
            raise MissingDictionary(f"dictionary version {version} not available")
        return dic

    def resolve(self, version: int, digest: bytes) -> bytes:
        dic = self._dicts.get(version)
        if dic is None:
            raise MissingDictionary(f"dictionary version {version} not available")
        if _digest(dic) != digest:
            raise DictionaryMismatch(f"dictionary version {version} SHA-256 mismatch")
        return dic

    def manifest(self) -> list[dict[str, object]]:
        return [{"version": v, "bytes": len(dic), "sha256": hashlib.sha256(dic).hexdigest()}
                for v, dic in sorted(self._dicts.items())]


@dataclass(frozen=True)
class FrameInfo:
    sequence: int
    count: int
    dictionary_version: int
    original_length: int
    wire_length: int
    raw: bool


class FrameCodec:
    def __init__(self, registry: DictionaryRegistry | None = None, level: int = 3,
                 max_raw_bytes: int = 1024 * 1024,
                 max_wire_bytes: int = 2 * 1024 * 1024,
                 max_messages: int = 65535):
        if not 1 <= max_raw_bytes <= MAX_U32 or max_wire_bytes < HEADER_SIZE:
            raise ValueError("invalid frame byte limits")
        if not 1 <= max_messages <= 65535:
            raise ValueError("max_messages outside [1,65535]")
        self.registry = registry or DictionaryRegistry()
        self.zstd = Zstd(level)
        self.max_raw_bytes = max_raw_bytes
        self.max_wire_bytes = max_wire_bytes
        self.max_messages = max_messages

    def encode(self, records: list[Message], sequence: int,
               dictionary_version: int = 0, force_raw: bool = False) -> bytes:
        if not 1 <= len(records) <= self.max_messages:
            raise ProtocolError("invalid message count")
        if not 0 <= sequence <= MAX_U32:
            raise ProtocolError("frame sequence overflow")
        body = b"".join(_record(msg) for msg in records)
        if len(body) > self.max_raw_bytes:
            raise ProtocolError("uncompressed frame exceeds configured limit")
        dic = self.registry.get(dictionary_version) if dictionary_version else None
        zipped = self.zstd.compress(body, dic) if not force_raw else body
        use_raw = force_raw or len(zipped) >= len(body)
        if use_raw:
            payload, flags, version, fingerprint = body, FLAG_RAW, 0, ZERO_HASH
        else:
            payload = zipped
            flags = FLAG_DICTIONARY if dic else 0
            version = dictionary_version if dic else 0
            fingerprint = _digest(dic) if dic else ZERO_HASH
        header = _HEADER.pack(MAGIC, VERSION, flags, len(records), sequence,
                              version, len(body), len(payload), fingerprint,
                              _digest(body))
        frame = header + payload
        if len(frame) > self.max_wire_bytes:
            raise ProtocolError("wire frame exceeds configured limit")
        return frame

    def decode(self, frame: bytes) -> tuple[FrameInfo, list[Message]]:
        if len(frame) < HEADER_SIZE:
            raise ProtocolError("frame too short")
        magic, protocol, flags, count, seq, version, original_len, payload_len, dic_fp, digest = \
            _HEADER.unpack_from(frame)
        if magic != MAGIC or protocol != VERSION:
            raise ProtocolError("unsupported frame magic/version")
        if flags & ~(FLAG_DICTIONARY | FLAG_RAW):
            raise ProtocolError("unknown frame flags")
        if not 1 <= count <= self.max_messages:
            raise ProtocolError("message count beyond limit")
        if original_len > self.max_raw_bytes or payload_len + HEADER_SIZE > self.max_wire_bytes:
            raise ProtocolError("frame size beyond limit")
        if len(frame) != HEADER_SIZE + payload_len:
            raise ProtocolError("truncated or extra frame bytes")
        if bool(flags & FLAG_DICTIONARY) != bool(version):
            raise ProtocolError("dictionary flag/version inconsistent")
        if not flags & FLAG_DICTIONARY and dic_fp != ZERO_HASH:
            raise ProtocolError("unexpected dictionary fingerprint")
        if flags & FLAG_RAW and (flags & FLAG_DICTIONARY or original_len != payload_len):
            raise ProtocolError("invalid raw frame")
        payload = frame[HEADER_SIZE:]
        if flags & FLAG_RAW:
            body = payload
        else:
            dictionary = self.registry.resolve(version, dic_fp) if version else None
            try:
                body = self.zstd.decompress(payload, original_len, dictionary)
            except ZstdError as exc:
                raise ProtocolError(f"Zstd decompression failed: {exc}") from exc
        if _digest(body) != digest:
            raise ProtocolError("body SHA-256 checksum mismatch")
        records = _decode_records(body, count)
        info = FrameInfo(seq, count, version, original_len, len(frame), bool(flags & FLAG_RAW))
        return info, records


class BoundedBatcher:
    """Batches on count, encoded body byte budget, or explicit deadline poll.

    If max_delay_ms is used, the caller MUST call poll(now_ms) at that cadence;
    no background timer is spawned. Clock is monotonic milliseconds supplied by caller.
    """
    def __init__(self, codec: FrameCodec, dictionary_version: int = 0,
                 max_messages: int = 32, max_batch_bytes: int = 65536,
                 max_delay_ms: int = 25, start_sequence: int = 0):
        if not 1 <= max_messages <= codec.max_messages:
            raise ValueError("invalid max_messages")
        if not 1 <= max_batch_bytes <= codec.max_raw_bytes:
            raise ValueError("invalid max_batch_bytes")
        if max_delay_ms < 0 or not 0 <= start_sequence <= MAX_U32:
            raise ValueError("invalid delay or start_sequence")
        self.codec = codec
        self.dictionary_version = dictionary_version
        self.max_messages = max_messages
        self.max_batch_bytes = max_batch_bytes
        self.max_delay_ms = max_delay_ms
        self.next_sequence = start_sequence
        self._messages: list[Message] = []
        self._body_bytes = 0
        self._since_ms: int | None = None
        self._last_now_ms: int | None = None

    def _check_clock(self, now_ms: int):
        if not isinstance(now_ms, int) or now_ms < 0:
            raise ValueError("now_ms must be monotonic nonnegative integer")
        if self._last_now_ms is not None and now_ms < self._last_now_ms:
            raise ValueError("clock moved backwards")
        self._last_now_ms = now_ms

    def poll(self, now_ms: int) -> list[bytes]:
        self._check_clock(now_ms)
        if self._messages and now_ms - self._since_ms >= self.max_delay_ms:
            return [self.flush()]
        return []

    def add(self, message: Message, now_ms: int) -> list[bytes]:
        size = _record_size(message)
        if size > self.max_batch_bytes:
            raise ProtocolError("single message exceeds batch byte budget")
        ready = self.poll(now_ms)
        if self._messages and (len(self._messages) >= self.max_messages or
                               size + self._body_bytes > self.max_batch_bytes):
            ready.append(self.flush())
        if not self._messages:
            self._since_ms = now_ms
        self._messages.append(message)
        self._body_bytes += size
        if len(self._messages) == self.max_messages or self._body_bytes == self.max_batch_bytes:
            ready.append(self.flush())
        return ready

    def flush(self) -> bytes | None:
        if not self._messages:
            return None
        # On overflow error, don't mutate pending data.
        frame = self.codec.encode(self._messages, self.next_sequence, self.dictionary_version)
        self.next_sequence += 1
        self._messages = []
        self._body_bytes = 0
        self._since_ms = None
        return frame

    def rotate(self, new_version: int) -> list[bytes]:
        """Flush old-version data before activating the new dictionary."""
        if new_version:
            self.codec.registry.get(new_version)
        flushed = self.flush()
        self.dictionary_version = new_version
        return [flushed] if flushed is not None else []


class StreamDecoder:
    """Incremental frame parser; detects gaps, repeats, and out-of-order frames."""
    def __init__(self, codec: FrameCodec, start_sequence: int = 0):
        self.codec = codec
        self.next_sequence = start_sequence
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> list[Message]:
        if not isinstance(chunk, bytes):
            raise TypeError("feed requires bytes")
        self._buffer.extend(chunk)
        messages: list[Message] = []
        while len(self._buffer) >= HEADER_SIZE:
            magic, protocol, flags, count, seq, version, original_len, payload_len, dic_fp, digest = \
                _HEADER.unpack_from(self._buffer)
            if magic != MAGIC or protocol != VERSION:
                raise ProtocolError("invalid header")
            if original_len > self.codec.max_raw_bytes or payload_len + HEADER_SIZE > self.codec.max_wire_bytes:
                raise ProtocolError("frame size beyond limit")
            required = HEADER_SIZE + payload_len
            if len(self._buffer) < required:
                break
            frame = bytes(self._buffer[:required])
            if seq != self.next_sequence:
                raise SequenceError(f"expected frame {self.next_sequence}, received {seq}")
            info, batch = self.codec.decode(frame)
            del self._buffer[:required]
            messages.extend(batch)
            self.next_sequence += 1
        if len(self._buffer) > self.codec.max_wire_bytes:
            raise ProtocolError("buffered incomplete frame exceeds limit")
        return messages

    def finish(self):
        if self._buffer:
            raise ProtocolError("truncated trailing frame")
