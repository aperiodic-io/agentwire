"""Round-trip and property tests for agentwire.codecs (one-shot and streaming)."""
import os
import random

import pytest

from agentwire import codecs as C
from agentwire import zstd as Z


def _corpus(seed: int):
    r = random.Random(seed)
    yield b""
    yield b"a"
    yield bytes(range(256))
    yield os.urandom(5000)
    yield b'{"role":"tool","content":"ok"}\n' * 400
    for _ in range(12):
        n = r.choice([1, 7, 100, 4096, 70000, 200000])
        alphabet = r.choice([b"ab", b"0123456789abcdef", bytes(range(256))])
        yield bytes(r.choice(alphabet) for _ in range(min(n, 3000))) * max(1, n // 3000)


ONE_SHOT = [lambda: C.ZstdCodec(1), lambda: C.ZstdCodec(19), lambda: C.ZstdCodec(3, 27, True),
            lambda: C.GzipCodec(6), lambda: C.XzCodec(6), lambda: C.BrotliCodec(5),
            lambda: C.BrotliCodec(11, 24), lambda: C.BrotliCodec(9, 26), lambda: C.Lz4Codec(0),
            lambda: C.Lz4Codec(9)]
STREAMS = [lambda: C.ZstdStreamCodec(3, 20), lambda: C.ZstdStreamCodec(3, 23, True),
           lambda: C.DeflateStreamCodec(6), lambda: C.BrotliStreamCodec(5, 22),
           lambda: C.Lz4StreamCodec(0)]


@pytest.mark.parametrize("make", ONE_SHOT)
def test_one_shot_roundtrip_property(make):
    codec = make()
    for data in _corpus(1):
        assert codec.decompress(codec.compress(data)) == data, codec.name


@pytest.mark.parametrize("make", STREAMS)
def test_stream_roundtrip_each_message_decodes_immediately(make):
    codec = make()
    enc, dec = codec.encoder(), codec.decoder()
    r = random.Random(2)
    history = []
    for i in range(150):
        msg = r.choice([b"", b"x", os.urandom(r.randrange(1, 900)),
                        b'{"type":"content_block_delta","delta":{"text":"%d"}}' % i,
                        (history[-1] if history else b"seed") + b" appended"])
        history.append(msg)
        chunk = enc.push(msg)
        got = dec.push(chunk, len(msg)) if dec.needs_size else dec.push(chunk)
        assert got == msg, (codec.name, i)
    enc.close()
    dec.close()


def test_stream_history_beats_independent_messages():
    msg = b'{"type":"tool_result","tool_use_id":"toolu_1","content":"collected 12 items, 12 passed"}'
    one = C.ZstdCodec(3)
    for codec in (C.ZstdStreamCodec(3), C.DeflateStreamCodec(), C.BrotliStreamCodec(), C.Lz4StreamCodec()):
        enc = codec.encoder()
        enc.push(msg)
        assert len(enc.push(msg)) < len(one.compress(msg)) / 3, codec.name


def test_zstd_prefix_patch_from_roundtrip_and_requires_prefix():
    old = b"".join(b"turn %d: some context line\n" % i for i in range(5000))
    new = old + b"turn 5000: the new message\n"
    # At fast levels the regular match finder misses the long prefix match;
    # long-distance matching (as `zstd --patch-from --long` uses) finds it.
    frame = Z.compress_advanced(new, level=3, long_distance=True, prefix=old)
    assert len(frame) < 200
    assert len(Z.compress_advanced(new, level=3, prefix=old)) > 10 * len(frame)
    assert Z.decompress_advanced(frame, len(new), prefix=old) == new
    with pytest.raises(Z.ZstdError):
        Z.decompress_advanced(frame, len(new))


def test_zstd_decompress_respects_declared_size_limit():
    frame = Z.compress_advanced(b"x" * 10000)
    with pytest.raises(Z.ZstdError):
        Z.decompress_advanced(frame, 100)


def test_corrupt_input_fails_closed():
    for codec in (C.ZstdCodec(), C.GzipCodec(), C.BrotliCodec(), C.Lz4Codec(), C.XzCodec()):
        blob = bytearray(codec.compress(os.urandom(64) + b"hello" * 200))
        blob = bytes(blob[: len(blob) // 2])
        with pytest.raises(Exception):
            assert codec.decompress(blob) != b"x"


@pytest.mark.skipif(not C.available()["brotli_dictionary"], reason="brotli < 1.1")
def test_brotli_shared_dictionary_roundtrip_and_gain():
    dictionary = b'{"type":"tool_result","tool_use_id":"toolu_","content":"collected items passed failed"}' * 30
    msg = b'{"type":"tool_result","tool_use_id":"toolu_9","content":"collected 3 items, 3 passed"}'
    with_dict = C.BrotliCodec(5, 22, dictionary)
    blob = with_dict.compress(msg)
    assert with_dict.decompress(blob) == msg
    assert len(blob) < len(C.BrotliCodec(5, 22).compress(msg))
