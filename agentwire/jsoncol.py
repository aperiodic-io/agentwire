"""AWJ1: columnar ("shredded") encoding for blocks of JSON messages.

Agent logs are JSON records with a small, stable set of keys and shapes, but
row-oriented compression interleaves keys, punctuation, numbers and free text.
AWJ1 parses each message, and if a known serializer style re-creates it
byte-for-byte, splits it into:

  * a structure stream: type tags, key ids, container lengths;
  * a key table (each distinct key once);
  * one value column per (JSON path, type): every "message.usage.input_tokens"
    together, every "message.content[].text" together, and so on.

Each section is zstd-compressed separately. Messages that are not JSON, or not
reproducible exactly by one of the styles (odd whitespace, duplicate keys, 1e5
style numbers, BOMs), are stored verbatim, so the block is lossless for any
input. Decoding replays the structure and re-serializes with the recorded style.

Block layout (all integers uleb128 unless noted):
  "AWJ1" level:u8 n_messages crc32(all messages concatenated):u32be
  section x4 (meta, structure, keys, columns): raw_len comp_len zstd_frame
meta   : per message: mode (0 raw, 1+ style id) trailer_id; raw messages' bytes
         live in the columns section under column 0.
"""
from __future__ import annotations

import json
import struct
import zlib

from .transport import _read_varint, _varint
from .zstd import Zstd, ZstdError

MAGIC = b"AWJ1"
STYLES = [
    None,   # 0 = raw
    dict(separators=(",", ":"), ensure_ascii=False),
    dict(separators=(",", ":"), ensure_ascii=True),
    dict(ensure_ascii=True),
    dict(ensure_ascii=False),
    dict(indent=2, ensure_ascii=True),
    dict(indent=2, ensure_ascii=False),
]
TRAILERS = [b"", b"\n", b"\r\n"]
T_NULL, T_TRUE, T_FALSE, T_INT, T_FLOAT, T_STR, T_LIST, T_DICT = range(8)
_ENC = "surrogatepass"


def _dumps(obj, style: int) -> bytes:
    return json.dumps(obj, allow_nan=True, **STYLES[style]).encode("utf-8", _ENC)


def _split_trailer(m: bytes) -> tuple[bytes, int]:
    for i in (2, 1):
        if m.endswith(TRAILERS[i]):
            return m[:-len(TRAILERS[i])], i
    return m, 0


def _try_parse(m: bytes):
    body, trailer = _split_trailer(m)
    if not body or body[:1] not in b'{["-0123456789tfn':
        return None
    try:
        obj = json.loads(body.decode("utf-8", _ENC))
    except (ValueError, UnicodeDecodeError, RecursionError):
        return None
    # Cheapest style checks first: compact separators are by far the most common.
    for style in range(1, len(STYLES)):
        try:
            if _dumps(obj, style) == body:
                return obj, style, trailer
        except (ValueError, UnicodeEncodeError):
            return None
    return None


class _Writer:
    def __init__(self):
        self.structure = bytearray()
        self.keys: dict[str, int] = {}
        self.key_table = bytearray()
        self.paths: dict[tuple, int] = {}
        self.columns: list[bytearray] = [bytearray()]   # column 0 = raw messages

    def col(self, path: tuple) -> bytearray:
        i = self.paths.get(path)
        if i is None:
            i = self.paths[path] = len(self.columns)
            self.columns.append(bytearray())
        return self.columns[i]

    def value(self, v, path: tuple):
        s = self.structure
        if v is None:
            s.append(T_NULL)
        elif v is True:
            s.append(T_TRUE)
        elif v is False:
            s.append(T_FALSE)
        elif isinstance(v, int):
            s.append(T_INT)
            t = str(v).encode()
            c = self.col(path + (T_INT,))
            c += _varint(len(t)) + t
        elif isinstance(v, float):
            s.append(T_FLOAT)
            t = json.dumps(v).encode()
            c = self.col(path + (T_FLOAT,))
            c += _varint(len(t)) + t
        elif isinstance(v, str):
            s.append(T_STR)
            t = v.encode("utf-8", _ENC)
            c = self.col(path + (T_STR,))
            c += _varint(len(t)) + t
        elif isinstance(v, list):
            s.append(T_LIST)
            s += _varint(len(v))
            p = path + (-1,)
            for x in v:
                self.value(x, p)
        else:
            s.append(T_DICT)
            s += _varint(len(v))
            for k, x in v.items():
                kid = self.keys.get(k)
                if kid is None:
                    kid = self.keys[k] = len(self.keys)
                    kb = k.encode("utf-8", _ENC)
                    self.key_table += _varint(len(kb)) + kb
                s += _varint(kid)
                self.value(x, path + (kid,))


def shred_stats(messages: list[bytes]) -> dict:
    n = sum(1 for m in messages if _try_parse(m) is not None)
    return {"messages": len(messages), "shredded": n}


def encode_block(messages: list[bytes], level: int = 3) -> bytes:
    w = _Writer()
    meta = bytearray()
    for m in messages:
        parsed = _try_parse(m)
        if parsed is None:
            meta += b"\x00"
            w.columns[0] += _varint(len(m)) + m
        else:
            obj, style, trailer = parsed
            meta += bytes((style, trailer))
            w.value(obj, ())
    cols = bytearray()
    for c in w.columns:
        cols += _varint(len(c))
    for c in w.columns:
        cols += c
    z = Zstd(level)
    out = bytearray(MAGIC + bytes((level,)) + _varint(len(messages)) +
                    struct.pack(">I", zlib.crc32(b"".join(messages))))
    out += _varint(len(w.columns))
    for section in (meta, w.structure, w.key_table, cols):
        frame = z.compress(bytes(section)) if section else b""
        out += _varint(len(section)) + _varint(len(frame)) + frame
    return bytes(out)


class _Reader:
    def __init__(self, structure: bytes, keys: list[str], columns: list[bytes]):
        self.s, self.sp = structure, 0
        self.keys = keys
        self.columns = columns
        self.cpos = [0] * len(columns)
        self.paths: dict[tuple, int] = {}

    def _next_col(self, path: tuple) -> bytes:
        i = self.paths.get(path)
        if i is None:
            i = self.paths[path] = len(self.paths) + 1
        if i >= len(self.columns):
            raise ValueError("AWJ1 column index out of range")
        c, p = self.columns[i], self.cpos[i]
        n, p = _read_varint(c, p)
        if p + n > len(c):
            raise ValueError("AWJ1 column truncated")
        self.cpos[i] = p + n
        return c[p:p + n]

    def value(self, path: tuple):
        if self.sp >= len(self.s):
            raise ValueError("AWJ1 structure truncated")
        t = self.s[self.sp]
        self.sp += 1
        if t == T_NULL:
            return None
        if t == T_TRUE:
            return True
        if t == T_FALSE:
            return False
        if t == T_INT:
            return int(self._next_col(path + (T_INT,)))
        if t == T_FLOAT:
            return float(self._next_col(path + (T_FLOAT,)))
        if t == T_STR:
            return self._next_col(path + (T_STR,)).decode("utf-8", _ENC)
        if t == T_LIST:
            n, self.sp = _read_varint(self.s, self.sp)
            p = path + (-1,)
            return [self.value(p) for _ in range(n)]
        if t == T_DICT:
            n, self.sp = _read_varint(self.s, self.sp)
            d = {}
            for _ in range(n):
                kid, self.sp = _read_varint(self.s, self.sp)
                if kid >= len(self.keys):
                    raise ValueError("AWJ1 key id out of range")
                d[self.keys[kid]] = self.value(path + (kid,))
            return d
        raise ValueError("AWJ1 unknown type tag")


def decode_block(blob: bytes, max_bytes: int = 1 << 30) -> list[bytes]:
    try:
        return _decode(blob, max_bytes)
    except (ZstdError, IndexError, struct.error, UnicodeDecodeError, OverflowError) as exc:
        raise ValueError(f"corrupt AWJ1 block: {exc}") from exc


def _decode(blob: bytes, max_bytes: int) -> list[bytes]:
    if blob[:4] != MAGIC:
        raise ValueError("not an AWJ1 block")
    pos = 5
    count, pos = _read_varint(blob, pos)
    (crc,) = struct.unpack_from(">I", blob, pos)
    pos += 4
    ncols, pos = _read_varint(blob, pos)
    z = Zstd()
    sections = []
    for _ in range(4):
        raw_len, pos = _read_varint(blob, pos)
        comp_len, pos = _read_varint(blob, pos)
        if raw_len > max_bytes:
            raise ValueError("AWJ1 section too large")
        frame = blob[pos:pos + comp_len]
        pos += comp_len
        sections.append(z.decompress(frame, raw_len) if comp_len else b"")
    if pos != len(blob):
        raise ValueError("trailing bytes after AWJ1 block")
    meta, structure, key_bytes, cols = sections
    keys, p = [], 0
    while p < len(key_bytes):
        n, p = _read_varint(key_bytes, p)
        keys.append(key_bytes[p:p + n].decode("utf-8", _ENC))
        p += n
    lens, p = [], 0
    for _ in range(ncols):
        n, p = _read_varint(cols, p)
        lens.append(n)
    columns = []
    for n in lens:
        columns.append(cols[p:p + n])
        p += n
    if p != len(cols):
        raise ValueError("AWJ1 column directory mismatch")
    reader = _Reader(structure, keys, columns)
    out, mp = [], 0
    for _ in range(count):
        mode = meta[mp]
        mp += 1
        if mode == 0:
            out.append(_take_raw(reader))
            continue
        if mode >= len(STYLES):
            raise ValueError("AWJ1 unknown style")
        trailer = meta[mp]
        mp += 1
        out.append(_dumps(reader.value(()), mode) + TRAILERS[trailer])
    if mp != len(meta) or reader.sp != len(structure):
        raise ValueError("AWJ1 trailing structure")
    if zlib.crc32(b"".join(out)) != crc:
        raise ValueError("AWJ1 CRC mismatch")
    return out


def _take_raw(reader: _Reader) -> bytes:
    c, p = reader.columns[0], reader.cpos[0]
    n, p = _read_varint(c, p)
    if p + n > len(c):
        raise ValueError("AWJ1 raw column truncated")
    reader.cpos[0] = p + n
    return c[p:p + n]
