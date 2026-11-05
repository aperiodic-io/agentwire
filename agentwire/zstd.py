"""Thin, dependency-free ctypes bindings for system libzstd and libzstd's ZDICT.

Python standard library only. The actual compression/decompression is performed by
libzstd (Zstandard) rather than a Python reimplementation.
"""
from __future__ import annotations

import ctypes as ct
import ctypes.util
from dataclasses import dataclass


class ZstdError(RuntimeError):
    pass


name = ctypes.util.find_library("zstd")
if name is None:
    raise RuntimeError("libzstd is required; install the system libzstd package")
_lib = ct.CDLL(name)
_size = ct.c_size_t
_ptr = ct.c_void_p


def _bind(name: str, restype: object, *args: object):
    fun = getattr(_lib, name)
    fun.restype = restype
    fun.argtypes = list(args)
    return fun


_bound = _bind("ZSTD_compressBound", _size, _size)
_cctx = _bind("ZSTD_createCCtx", _ptr)
_freec = _bind("ZSTD_freeCCtx", _size, _ptr)
_dctx = _bind("ZSTD_createDCtx", _ptr)
_freed = _bind("ZSTD_freeDCtx", _size, _ptr)
_compress_dict = _bind(
    "ZSTD_compress_usingDict", _size, _ptr, _ptr, _size, _ptr, _size,
    _ptr, _size, ct.c_int
)
_decompress_dict = _bind(
    "ZSTD_decompress_usingDict", _size, _ptr, _ptr, _size, _ptr, _size,
    _ptr, _size
)
_is_error = _bind("ZSTD_isError", ct.c_uint, _size)
_get_error = _bind("ZSTD_getErrorName", ct.c_char_p, _size)
_train_dict = _bind(
    "ZDICT_trainFromBuffer", _size, _ptr, _size, _ptr,
    ct.POINTER(_size), ct.c_uint
)
_is_train_error = _bind("ZDICT_isError", ct.c_uint, _size)
_get_train_error = _bind("ZDICT_getErrorName", ct.c_char_p, _size)


def _checked(ret: int):
    if _is_error(ret):
        raise ZstdError(_get_error(ret).decode())
    return ret


def _input(data: bytes):
    # create_string_buffer(data) appends a NUL byte. We always pass explicit size.
    return ct.create_string_buffer(data)


@dataclass(frozen=True)
class Zstd:
    level: int = 3

    def compress(self, data: bytes, dictionary: bytes | None = None) -> bytes:
        if not isinstance(data, bytes):
            raise TypeError("input must be bytes")
        context = _cctx()
        if not context:
            raise MemoryError("ZSTD_createCCtx failed")
        try:
            dst = ct.create_string_buffer(int(_bound(len(data))))
            src = _input(data)
            dic = _input(dictionary or b"")
            n = _checked(_compress_dict(context, dst, len(dst), src, len(data),
                                        dic, len(dictionary or b""), self.level))
            return dst.raw[:n]
        finally:
            _freec(context)

    def decompress(self, data: bytes, expected_size: int,
                   dictionary: bytes | None = None) -> bytes:
        """Bound output memory to the exact expected uncompressed frame size."""
        if expected_size < 0:
            raise ValueError("negative expected_size")
        context = _dctx()
        if not context:
            raise MemoryError("ZSTD_createDCtx failed")
        try:
            dst = ct.create_string_buffer(max(1, expected_size))
            src = _input(data)
            dic = _input(dictionary or b"")
            n = _checked(_decompress_dict(context, dst, expected_size, src,
                                          len(data), dic, len(dictionary or b"")))
            if n != expected_size:
                raise ZstdError(f"size mismatch: expected {expected_size}, decoded {n}")
            return dst.raw[:n]
        finally:
            _freed(context)


# --- Streaming API (context takeover) -------------------------------------
# One long-lived compression context per connection. Each message is fed with
# ZSTD_e_flush, so its bytes are emitted immediately, but the match finder keeps
# the full window of everything sent earlier (plus the dictionary) as history.

class _InBuffer(ct.Structure):
    _fields_ = [("src", _ptr), ("size", _size), ("pos", _size)]


class _OutBuffer(ct.Structure):
    _fields_ = [("dst", _ptr), ("size", _size), ("pos", _size)]


_C_COMPRESSION_LEVEL = 100
_C_WINDOW_LOG = 101
_D_WINDOW_LOG_MAX = 100
_E_FLUSH = 1
MIN_WINDOW_LOG = 10
MAX_WINDOW_LOG = 27

_cctx_set = _bind("ZSTD_CCtx_setParameter", _size, _ptr, ct.c_int, ct.c_int)
_cctx_load_dict = _bind("ZSTD_CCtx_loadDictionary", _size, _ptr, _ptr, _size)
_compress_stream2 = _bind("ZSTD_compressStream2", _size, _ptr,
                          ct.POINTER(_OutBuffer), ct.POINTER(_InBuffer), ct.c_int)
_cstream_out_size = _bind("ZSTD_CStreamOutSize", _size)
_dctx_set = _bind("ZSTD_DCtx_setParameter", _size, _ptr, ct.c_int, ct.c_int)
_dctx_load_dict = _bind("ZSTD_DCtx_loadDictionary", _size, _ptr, _ptr, _size)
_decompress_stream = _bind("ZSTD_decompressStream", _size, _ptr,
                           ct.POINTER(_OutBuffer), ct.POINTER(_InBuffer))


def _check_window_log(window_log: int):
    if not isinstance(window_log, int) or not MIN_WINDOW_LOG <= window_log <= MAX_WINDOW_LOG:
        raise ValueError(f"window_log must be in [{MIN_WINDOW_LOG}, {MAX_WINDOW_LOG}]")


class ZstdStreamCompressor:
    """Stateful compressor: every flush() output depends on all earlier input.

    Memory is held for the life of the object (roughly the window plus match
    tables). Call close() when the connection ends.
    """

    def __init__(self, level: int = 3, dictionary: bytes | None = None,
                 window_log: int = 20):
        _check_window_log(window_log)
        self._ctx = _cctx()
        if not self._ctx:
            raise MemoryError("ZSTD_createCCtx failed")
        try:
            _checked(_cctx_set(self._ctx, _C_COMPRESSION_LEVEL, level))
            _checked(_cctx_set(self._ctx, _C_WINDOW_LOG, window_log))
            if dictionary:
                _checked(_cctx_load_dict(self._ctx, dictionary, len(dictionary)))
        except Exception:
            self.close()
            raise
        self._chunk = int(_cstream_out_size())

    def flush(self, data: bytes) -> bytes:
        """Compress data and flush so a peer can decode it right away."""
        if not isinstance(data, bytes):
            raise TypeError("input must be bytes")
        if not self._ctx:
            raise ZstdError("compressor is closed")
        src = ct.create_string_buffer(data, len(data)) if data else None
        inp = _InBuffer(ct.cast(src, _ptr) if src is not None else None, len(data), 0)
        out_parts: list[bytes] = []
        dst = ct.create_string_buffer(self._chunk)
        while True:
            out = _OutBuffer(ct.cast(dst, _ptr), self._chunk, 0)
            remaining = _checked(_compress_stream2(self._ctx, ct.byref(out),
                                                   ct.byref(inp), _E_FLUSH))
            out_parts.append(dst.raw[:out.pos])
            if remaining == 0 and inp.pos == inp.size:
                return b"".join(out_parts)

    def close(self):
        if getattr(self, "_ctx", None):
            _freec(self._ctx)
            self._ctx = None

    def __del__(self):
        self.close()


class ZstdStreamDecompressor:
    """Stateful decompressor mirroring ZstdStreamCompressor.

    window_log_max caps how much history an untrusted peer can make us keep.
    """

    def __init__(self, dictionary: bytes | None = None, window_log_max: int = 20):
        _check_window_log(window_log_max)
        self._ctx = _dctx()
        if not self._ctx:
            raise MemoryError("ZSTD_createDCtx failed")
        try:
            _checked(_dctx_set(self._ctx, _D_WINDOW_LOG_MAX, window_log_max))
            if dictionary:
                _checked(_dctx_load_dict(self._ctx, dictionary, len(dictionary)))
        except Exception:
            self.close()
            raise

    def decompress(self, data: bytes, expected_size: int) -> bytes:
        """Decode one flushed chunk; output must be exactly expected_size bytes."""
        if expected_size < 0:
            raise ValueError("negative expected_size")
        if not self._ctx:
            raise ZstdError("decompressor is closed")
        src = ct.create_string_buffer(data, len(data)) if data else None
        inp = _InBuffer(ct.cast(src, _ptr) if src is not None else None, len(data), 0)
        # One spare byte detects a chunk that expands past its declared size.
        capacity = expected_size + 1
        dst = ct.create_string_buffer(capacity)
        out = _OutBuffer(ct.cast(dst, _ptr), capacity, 0)
        while inp.pos < inp.size:
            _checked(_decompress_stream(self._ctx, ct.byref(out), ct.byref(inp)))
            if out.pos > expected_size:
                raise ZstdError(f"chunk expands beyond declared {expected_size} bytes")
        if out.pos != expected_size:
            raise ZstdError(f"size mismatch: expected {expected_size}, decoded {out.pos}")
        return dst.raw[:out.pos]

    def close(self):
        if getattr(self, "_ctx", None):
            _freed(self._ctx)
            self._ctx = None

    def __del__(self):
        self.close()


def train_dictionary(samples: list[bytes], capacity: int = 8192) -> bytes:
    """Train a real Zstandard dictionary using independent representative samples."""
    if not samples or any(not isinstance(x, bytes) or not x for x in samples):
        raise ValueError("training needs nonempty byte-string samples")
    if capacity < 256 or capacity > 1024 * 1024:
        raise ValueError("dictionary capacity must be in [256, 1048576]")
    joined = b"".join(samples)
    if len(joined) < capacity * 2:
        raise ValueError("provide substantially more training text than dictionary size")
    sizes = (_size * len(samples))(*(len(s) for s in samples))
    dst = ct.create_string_buffer(capacity)
    src = _input(joined)
    n = _train_dict(dst, capacity, src, sizes, len(samples))
    if _is_train_error(n):
        raise ZstdError(_get_train_error(n).decode())
    return dst.raw[:n]
