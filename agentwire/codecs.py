"""General-purpose codecs behind one interface, for like-for-like comparison.

Zero pip dependencies: zstd, brotli and lz4 are bound from the system shared
libraries with ctypes; gzip/deflate and xz come from the standard library
(zlib, lzma). A codec whose library is missing raises ``CodecUnavailable`` when
constructed, so callers can skip it.

Two shapes:

* ``Codec``: one-shot ``compress(bytes) -> bytes`` / ``decompress(bytes) -> bytes``.
  Every output is a self-describing frame of that codec's standard format
  (zstd frame, brotli stream, LZ4 frame, gzip member, .xz container).
* ``StreamCodec``: one long-lived context per connection. ``encoder().push(msg)``
  returns the bytes to send now (the codec is flushed, so the peer can decode
  ``msg`` immediately) and ``decoder().push(chunk)`` returns exactly ``msg``.
  History is shared across messages (context takeover). The returned chunks
  carry no length framing; transports add their own (see ``stream_wire_bytes``).
"""
from __future__ import annotations

import ctypes as ct
import ctypes.util
import lzma
import zlib
from dataclasses import dataclass, field

from . import zstd as _z


class CodecUnavailable(RuntimeError):
    pass


class CodecError(ValueError):
    pass


def _load(*names: str):
    for n in names:
        path = ctypes.util.find_library(n) or n
        try:
            return ct.CDLL(path)
        except OSError:
            continue
    return None


_size = ct.c_size_t
_u8p = ct.POINTER(ct.c_uint8)


def _varint_len(n: int) -> int:
    return max(1, (n.bit_length() + 6) // 7)


# =========================================================================== zstd

@dataclass
class ZstdCodec:
    level: int = 3
    window_log: int | None = None
    long_distance: bool = False
    dictionary: bytes | None = None
    max_size: int = 1 << 31

    @property
    def name(self) -> str:
        s = f"zstd-{self.level}"
        if self.long_distance:
            s += "-long"
        if self.window_log:
            s += f"-w{self.window_log}"
        if self.dictionary:
            s += f"+dict{len(self.dictionary) // 1024}k"
        return s

    def compress(self, data: bytes) -> bytes:
        return _z.compress_advanced(data, self.level, self.window_log, self.long_distance,
                                    dictionary=self.dictionary)

    def decompress(self, blob: bytes) -> bytes:
        return _z.decompress_advanced(blob, self.max_size, dictionary=self.dictionary)


@dataclass
class ZstdStreamCodec:
    level: int = 3
    window_log: int = 20
    long_distance: bool = False
    dictionary: bytes | None = None

    @property
    def name(self) -> str:
        s = f"zstd-{self.level}-stream-w{self.window_log}"
        if self.long_distance:
            s += "-long"
        if self.dictionary:
            s += f"+dict{len(self.dictionary) // 1024}k"
        return s

    def encoder(self):
        c = _z.ZstdStreamCompressor(self.level, self.dictionary, self.window_log, self.long_distance)
        return _Pusher(c.flush, c.close, c.memory_bytes)

    def decoder(self):
        d = _z.ZstdStreamDecompressor(self.dictionary, max(self.window_log, 20))

        def push(chunk: bytes, size: int) -> bytes:
            return d.decompress(chunk, size)
        return _Pusher(push, d.close, d.memory_bytes, needs_size=True)


@dataclass
class _Pusher:
    fn: object
    closer: object = None
    mem: object = None
    needs_size: bool = False

    def push(self, data: bytes, size: int | None = None) -> bytes:
        return self.fn(data, size) if self.needs_size else self.fn(data)

    def memory_bytes(self) -> int | None:
        return self.mem() if self.mem else None

    def close(self):
        if self.closer:
            self.closer()


# =========================================================================== gzip / deflate

@dataclass
class GzipCodec:
    level: int = 6

    @property
    def name(self) -> str:
        return f"gzip-{self.level}"

    def compress(self, data: bytes) -> bytes:
        c = zlib.compressobj(self.level, zlib.DEFLATED, 31)
        return c.compress(data) + c.flush()

    def decompress(self, blob: bytes) -> bytes:
        d = zlib.decompressobj(31)
        out = d.decompress(blob) + d.flush()
        if not d.eof or d.unused_data:
            raise CodecError("truncated or trailing gzip data")
        return out


_SYNC_TAIL = b"\x00\x00\xff\xff"


@dataclass
class DeflateStreamCodec:
    """Raw deflate with Z_SYNC_FLUSH per message, like HTTP gzip streaming and
    WebSocket permessage-deflate (RFC 7692), including its trick of dropping the
    constant 4-byte empty-block tail from every message."""
    level: int = 6
    wbits: int = 15

    @property
    def name(self) -> str:
        return f"deflate-{self.level}-stream-w{self.wbits}"

    def encoder(self):
        c = zlib.compressobj(self.level, zlib.DEFLATED, -self.wbits)

        def push(data: bytes) -> bytes:
            out = c.compress(data) + c.flush(zlib.Z_SYNC_FLUSH)
            if not out.endswith(_SYNC_TAIL):
                raise CodecError("unexpected sync-flush tail")
            return out[:-4]
        return _Pusher(push)

    def decoder(self):
        d = zlib.decompressobj(-self.wbits)
        return _Pusher(lambda chunk: d.decompress(chunk + _SYNC_TAIL))


# =========================================================================== xz

@dataclass
class XzCodec:
    preset: int = 6
    extreme: bool = False

    @property
    def name(self) -> str:
        return f"xz-{self.preset}{'e' if self.extreme else ''}"

    def compress(self, data: bytes) -> bytes:
        return lzma.compress(data, preset=self.preset | (lzma.PRESET_EXTREME if self.extreme else 0))

    def decompress(self, blob: bytes) -> bytes:
        return lzma.decompress(blob)


# =========================================================================== brotli

_benc = _load("brotlienc", "libbrotlienc.so.1")
_bdec = _load("brotlidec", "libbrotlidec.so.1")
_BROTLI_OK = _benc is not None and _bdec is not None
if _BROTLI_OK:
    def _bb(lib, name, res, *args):
        f = getattr(lib, name)
        f.restype, f.argtypes = res, list(args)
        return f
    _be_create = _bb(_benc, "BrotliEncoderCreateInstance", ct.c_void_p, ct.c_void_p, ct.c_void_p, ct.c_void_p)
    _be_destroy = _bb(_benc, "BrotliEncoderDestroyInstance", None, ct.c_void_p)
    _be_set = _bb(_benc, "BrotliEncoderSetParameter", ct.c_int, ct.c_void_p, ct.c_int, ct.c_uint32)
    _be_stream = _bb(_benc, "BrotliEncoderCompressStream", ct.c_int, ct.c_void_p, ct.c_int,
                     ct.POINTER(_size), ct.POINTER(_u8p), ct.POINTER(_size), ct.POINTER(_u8p), ct.c_void_p)
    _be_more = _bb(_benc, "BrotliEncoderHasMoreOutput", ct.c_int, ct.c_void_p)
    _bd_create = _bb(_bdec, "BrotliDecoderCreateInstance", ct.c_void_p, ct.c_void_p, ct.c_void_p, ct.c_void_p)
    _bd_destroy = _bb(_bdec, "BrotliDecoderDestroyInstance", None, ct.c_void_p)
    _bd_set = _bb(_bdec, "BrotliDecoderSetParameter", ct.c_int, ct.c_void_p, ct.c_int, ct.c_uint32)
    _bd_stream = _bb(_bdec, "BrotliDecoderDecompressStream", ct.c_int, ct.c_void_p,
                     ct.POINTER(_size), ct.POINTER(_u8p), ct.POINTER(_size), ct.POINTER(_u8p), ct.c_void_p)
    try:  # brotli >= 1.1: raw shared dictionaries (what Compression Dictionary Transport uses)
        _be_prepare = _bb(_benc, "BrotliEncoderPrepareDictionary", ct.c_void_p, ct.c_int, _size,
                          ct.c_void_p, ct.c_int, ct.c_void_p, ct.c_void_p, ct.c_void_p)
        _be_attach = _bb(_benc, "BrotliEncoderAttachPreparedDictionary", ct.c_int, ct.c_void_p, ct.c_void_p)
        _be_free_prepared = _bb(_benc, "BrotliEncoderDestroyPreparedDictionary", None, ct.c_void_p)
        _bd_attach = _bb(_bdec, "BrotliDecoderAttachDictionary", ct.c_int, ct.c_void_p, ct.c_int, _size, ct.c_void_p)
        _BROTLI_DICT = True
    except AttributeError:
        _BROTLI_DICT = False
else:
    _BROTLI_DICT = False

_BE_QUALITY, _BE_LGWIN, _BE_LARGE_WINDOW, _BE_SIZE_HINT = 1, 2, 6, 5
_BD_LARGE_WINDOW = 1
_BOP_PROCESS, _BOP_FLUSH, _BOP_FINISH = 0, 1, 2
_BD_ERROR, _BD_SUCCESS, _BD_NEEDS_INPUT, _BD_NEEDS_OUTPUT = 0, 1, 2, 3
_CHUNK = 1 << 16


class _BrotliEnc:
    def __init__(self, quality: int, lgwin: int, dictionary: bytes | None):
        if not _BROTLI_OK:
            raise CodecUnavailable("libbrotlienc not found")
        self.s = _be_create(None, None, None)
        _be_set(self.s, _BE_QUALITY, quality)
        if lgwin > 24:
            _be_set(self.s, _BE_LARGE_WINDOW, 1)
        _be_set(self.s, _BE_LGWIN, lgwin)
        self._dict_buf = self._prepared = None
        if dictionary:
            if not _BROTLI_DICT:
                raise CodecUnavailable("brotli shared dictionaries need brotli >= 1.1")
            self._dict_buf = ct.create_string_buffer(dictionary, len(dictionary))
            self._prepared = _be_prepare(0, len(dictionary), self._dict_buf, 11, None, None, None)
            if not self._prepared or not _be_attach(self.s, self._prepared):
                raise CodecError("brotli dictionary attach failed")
        self._out = ct.create_string_buffer(_CHUNK)

    def run(self, data: bytes, op: int) -> bytes:
        src = ct.create_string_buffer(data, len(data)) if data else ct.create_string_buffer(1)
        avail_in = _size(len(data))
        next_in = ct.cast(src, _u8p)
        parts = []
        while True:
            avail_out = _size(_CHUNK)
            next_out = ct.cast(self._out, _u8p)
            if not _be_stream(self.s, op, ct.byref(avail_in), ct.byref(next_in),
                              ct.byref(avail_out), ct.byref(next_out), None):
                raise CodecError("brotli encoder error")
            parts.append(self._out.raw[:_CHUNK - avail_out.value])
            if avail_in.value == 0 and not _be_more(self.s):
                return b"".join(parts)

    def memory_bytes(self):
        return None

    def close(self):
        if getattr(self, "s", None):
            _be_destroy(self.s)
            self.s = None
        if getattr(self, "_prepared", None):
            _be_free_prepared(self._prepared)
            self._prepared = None


class _BrotliDec:
    def __init__(self, dictionary: bytes | None, large: bool):
        if not _BROTLI_OK:
            raise CodecUnavailable("libbrotlidec not found")
        self.s = _bd_create(None, None, None)
        if large:
            _bd_set(self.s, _BD_LARGE_WINDOW, 1)
        self._dict_buf = None
        if dictionary:
            self._dict_buf = ct.create_string_buffer(dictionary, len(dictionary))
            if not _bd_attach(self.s, 0, len(dictionary), self._dict_buf):
                raise CodecError("brotli dictionary attach failed")
        self._out = ct.create_string_buffer(_CHUNK)
        self.finished = False

    def run(self, data: bytes, limit: int = 1 << 31) -> bytes:
        src = ct.create_string_buffer(data, len(data)) if data else ct.create_string_buffer(1)
        avail_in = _size(len(data))
        next_in = ct.cast(src, _u8p)
        parts, total = [], 0
        while True:
            avail_out = _size(_CHUNK)
            next_out = ct.cast(self._out, _u8p)
            res = _bd_stream(self.s, ct.byref(avail_in), ct.byref(next_in),
                             ct.byref(avail_out), ct.byref(next_out), None)
            got = _CHUNK - avail_out.value
            total += got
            if total > limit:
                raise CodecError("brotli output exceeds limit")
            parts.append(self._out.raw[:got])
            if res == _BD_ERROR:
                raise CodecError("corrupt brotli data")
            if res == _BD_SUCCESS:
                self.finished = True
                if avail_in.value:
                    raise CodecError("trailing data after brotli stream")
                return b"".join(parts)
            # NEEDS_MORE_INPUT is also reported when the output buffer filled up
            # with output still pending, so only stop once output space remains.
            if res == _BD_NEEDS_INPUT and avail_out.value > 0:
                return b"".join(parts)

    def close(self):
        if getattr(self, "s", None):
            _bd_destroy(self.s)
            self.s = None


@dataclass
class BrotliCodec:
    quality: int = 5
    lgwin: int = 22
    dictionary: bytes | None = None

    def __post_init__(self):
        if not _BROTLI_OK:
            raise CodecUnavailable("libbrotli not found")

    @property
    def name(self) -> str:
        return f"brotli-{self.quality}-w{self.lgwin}" + (f"+dict{len(self.dictionary) // 1024}k" if self.dictionary else "")

    def compress(self, data: bytes) -> bytes:
        e = _BrotliEnc(self.quality, self.lgwin, self.dictionary)
        try:
            return e.run(data, _BOP_FINISH)
        finally:
            e.close()

    def decompress(self, blob: bytes) -> bytes:
        d = _BrotliDec(self.dictionary, self.lgwin > 24)
        try:
            out = d.run(blob)
            if not d.finished:
                raise CodecError("truncated brotli stream")
            return out
        finally:
            d.close()


@dataclass
class BrotliStreamCodec:
    quality: int = 5
    lgwin: int = 22
    dictionary: bytes | None = None

    def __post_init__(self):
        if not _BROTLI_OK:
            raise CodecUnavailable("libbrotli not found")

    @property
    def name(self) -> str:
        return f"brotli-{self.quality}-stream-w{self.lgwin}" + (f"+dict{len(self.dictionary) // 1024}k" if self.dictionary else "")

    def encoder(self):
        e = _BrotliEnc(self.quality, self.lgwin, self.dictionary)
        return _Pusher(lambda data: e.run(data, _BOP_FLUSH), e.close)

    def decoder(self):
        d = _BrotliDec(self.dictionary, self.lgwin > 24)
        return _Pusher(d.run, d.close)


# =========================================================================== lz4 (frame format)

_lz4 = _load("lz4", "liblz4.so.1")
_LZ4_OK = _lz4 is not None


class _FrameInfo(ct.Structure):
    _fields_ = [("blockSizeID", ct.c_int), ("blockMode", ct.c_int), ("contentChecksumFlag", ct.c_int),
                ("frameType", ct.c_int), ("contentSize", ct.c_ulonglong), ("dictID", ct.c_uint),
                ("blockChecksumFlag", ct.c_int)]


class _Prefs(ct.Structure):
    _fields_ = [("frameInfo", _FrameInfo), ("compressionLevel", ct.c_int), ("autoFlush", ct.c_uint),
                ("favorDecSpeed", ct.c_uint), ("reserved", ct.c_uint * 3)]


if _LZ4_OK:
    def _lb(name, res, *args):
        f = getattr(_lz4, name)
        f.restype, f.argtypes = res, list(args)
        return f
    _l_iserr = _lb("LZ4F_isError", ct.c_uint, _size)
    _l_errname = _lb("LZ4F_getErrorName", ct.c_char_p, _size)
    _l_frame_bound = _lb("LZ4F_compressFrameBound", _size, _size, ct.c_void_p)
    _l_compress_frame = _lb("LZ4F_compressFrame", _size, ct.c_void_p, _size, ct.c_void_p, _size, ct.c_void_p)
    _l_cctx = _lb("LZ4F_createCompressionContext", _size, ct.POINTER(ct.c_void_p), ct.c_uint)
    _l_free_cctx = _lb("LZ4F_freeCompressionContext", _size, ct.c_void_p)
    _l_begin = _lb("LZ4F_compressBegin", _size, ct.c_void_p, ct.c_void_p, _size, ct.c_void_p)
    _l_bound = _lb("LZ4F_compressBound", _size, _size, ct.c_void_p)
    _l_update = _lb("LZ4F_compressUpdate", _size, ct.c_void_p, ct.c_void_p, _size, ct.c_void_p, _size, ct.c_void_p)
    _l_flush = _lb("LZ4F_flush", _size, ct.c_void_p, ct.c_void_p, _size, ct.c_void_p)
    _l_dctx = _lb("LZ4F_createDecompressionContext", _size, ct.POINTER(ct.c_void_p), ct.c_uint)
    _l_free_dctx = _lb("LZ4F_freeDecompressionContext", _size, ct.c_void_p)
    _l_decompress = _lb("LZ4F_decompress", _size, ct.c_void_p, ct.c_void_p, ct.POINTER(_size),
                        ct.c_void_p, ct.POINTER(_size), ct.c_void_p)


def _lchk(r: int) -> int:
    if _l_iserr(r):
        raise CodecError(_l_errname(r).decode())
    return r


def _lz4_prefs(level: int, linked: bool, content_size: int = 0) -> _Prefs:
    p = _Prefs()
    p.compressionLevel = level
    p.frameInfo.blockMode = 0 if linked else 1
    p.frameInfo.blockSizeID = 4   # 64 KiB blocks
    p.frameInfo.contentSize = content_size
    return p


class _Lz4Dec:
    def __init__(self):
        self.c = ct.c_void_p()
        _lchk(_l_dctx(ct.byref(self.c), 100))
        self.done = False

    def run(self, data: bytes, limit: int = 1 << 31) -> bytes:
        src = ct.create_string_buffer(data, len(data)) if data else ct.create_string_buffer(1)
        pos, parts, total = 0, [], 0
        out = ct.create_string_buffer(1 << 17)
        while True:
            dst_size = _size(len(out))
            src_size = _size(len(data) - pos)
            hint = _lchk(_l_decompress(self.c, out, ct.byref(dst_size),
                                       ct.byref(src, pos) if pos < len(data) else None,
                                       ct.byref(src_size), None))
            pos += src_size.value
            total += dst_size.value
            if total > limit:
                raise CodecError("lz4 output exceeds limit")
            parts.append(out.raw[:dst_size.value])
            if hint == 0:
                self.done = True
                if pos != len(data):
                    raise CodecError("trailing data after lz4 frame")
                return b"".join(parts)
            if pos >= len(data) and dst_size.value == 0:
                return b"".join(parts)

    def close(self):
        if self.c:
            _l_free_dctx(self.c)
            self.c = None


@dataclass
class Lz4Codec:
    level: int = 0          # 0 = fast; 3..12 = LZ4-HC

    def __post_init__(self):
        if not _LZ4_OK:
            raise CodecUnavailable("liblz4 not found")

    @property
    def name(self) -> str:
        return "lz4" if self.level < 3 else f"lz4hc-{self.level}"

    def compress(self, data: bytes) -> bytes:
        p = _lz4_prefs(self.level, linked=True, content_size=len(data))
        cap = _l_frame_bound(len(data), ct.byref(p))
        dst = ct.create_string_buffer(cap)
        n = _lchk(_l_compress_frame(dst, cap, data, len(data), ct.byref(p)))
        return dst.raw[:n]

    def decompress(self, blob: bytes) -> bytes:
        d = _Lz4Dec()
        try:
            out = d.run(blob)
            if not d.done:
                raise CodecError("truncated lz4 frame")
            return out
        finally:
            d.close()


@dataclass
class Lz4StreamCodec:
    """LZ4 frame with linked blocks, flushed per message (64 KiB history)."""
    level: int = 0

    def __post_init__(self):
        if not _LZ4_OK:
            raise CodecUnavailable("liblz4 not found")

    @property
    def name(self) -> str:
        return ("lz4" if self.level < 3 else f"lz4hc-{self.level}") + "-stream"

    def encoder(self):
        c = ct.c_void_p()
        _lchk(_l_cctx(ct.byref(c), 100))
        prefs = _lz4_prefs(self.level, linked=True)
        state = {"begun": False}

        def push(data: bytes) -> bytes:
            out = b""
            if not state["begun"]:
                hdr = ct.create_string_buffer(32)
                n = _lchk(_l_begin(c, hdr, 32, ct.byref(prefs)))
                out, state["begun"] = hdr.raw[:n], True
            cap = _l_bound(len(data), ct.byref(prefs)) + 64
            dst = ct.create_string_buffer(cap)
            n = _lchk(_l_update(c, dst, cap, data, len(data), None)) if data else 0
            m = _lchk(_l_flush(c, ct.byref(dst, n), cap - n, None))
            return out + dst.raw[:n + m]
        return _Pusher(push, lambda: _l_free_cctx(c))

    def decoder(self):
        d = _Lz4Dec()
        return _Pusher(d.run, d.close)


# =========================================================================== registry helpers

def available() -> dict[str, bool]:
    return {"zstd": True, "gzip": True, "xz": True, "brotli": _BROTLI_OK,
            "brotli_dictionary": _BROTLI_DICT, "lz4": _LZ4_OK}


def stream_wire_bytes(chunk: bytes) -> int:
    """Wire cost of one streamed chunk with a minimal length prefix."""
    return len(chunk) + _varint_len(len(chunk))
