from __future__ import annotations

import asyncio
import random

import pytest

from agentwire.asyncio_transport import tcp_roundtrip_demo
from agentwire.fixtures import workload
from agentwire.stream import (STREAM_MAGIC, StreamEncoder, StreamSessionDecoder,
                              SessionPoisoned, _RESET)
from agentwire.transport import (DictionaryMismatch, DictionaryRegistry,
                                 Message, MissingDictionary, ProtocolError)

PAYLOADS = [b"", b"hello", b"\x00\xff\x80", "Üdvözlet 🛰️".encode(),
            b"{" + b"x" * 9000 + b"}", bytes(range(256))]


@pytest.mark.parametrize("version", [0, 1])
def test_stream_roundtrip_one_byte_chunks(registry, version):
    enc = StreamEncoder(registry, dictionary_version=version)
    original = [Message(2**60 + i, p, i % 256) for i, p in enumerate(PAYLOADS)]
    wire = b"".join(enc.encode([m]) for m in original)
    assert wire.startswith(STREAM_MAGIC)
    dec = StreamSessionDecoder(registry)
    out = []
    for i in range(len(wire)):
        out.extend(dec.feed(wire[i:i + 1]))
    dec.finish()
    assert out == original
    assert dec.dictionary_version == version


def test_each_frame_decodes_immediately(registry):
    """Zero delay: frame N is decodable before frame N+1 exists."""
    enc = StreamEncoder(registry, dictionary_version=1)
    dec = StreamSessionDecoder(registry)
    for m in workload("mixed-agent", 40).messages:
        assert dec.feed(enc.encode([m])) == [m]


def test_context_takeover_shrinks_repeats():
    enc = StreamEncoder()
    payload = b'{"service":"agent-worker","status":"success","items":[1,2,3,4]}' * 8
    first = len(enc.encode([Message(0, payload)]))
    second = len(enc.encode([Message(1, payload)]))
    assert second < first / 3, (first, second)


def test_stream_beats_independent_frames_on_agent_traffic(registry):
    from agentwire.transport import FrameCodec
    messages = workload("mixed-agent", 120).messages
    codec = FrameCodec(registry)
    independent = sum(len(codec.encode([m], i, 1)) for i, m in enumerate(messages))
    enc = StreamEncoder(registry, dictionary_version=1)
    streamed = sum(len(enc.encode([m])) for m in messages)
    assert streamed < independent * 0.7


def test_multi_message_frame(registry):
    enc = StreamEncoder(registry)
    batch = [Message(i, f"m{i}".encode()) for i in range(5)]
    assert StreamSessionDecoder(registry).feed(enc.encode(batch)) == batch


def test_rotation_resets_stream_and_dictionary(registry):
    enc = StreamEncoder(registry, dictionary_version=1)
    a = Message(1, b'{"region":"eu-central"}' * 30)
    b = Message(2, b'{"region":"eu-central"}' * 30)
    wire = enc.encode([a])
    enc.rotate(2)
    wire += enc.encode([b])
    dec = StreamSessionDecoder(registry)
    assert dec.feed(wire) == [a, b]
    assert dec.dictionary_version == 2


def test_missing_and_mismatched_dictionary(registry, dictionaries):
    wire = StreamEncoder(registry, dictionary_version=1).encode([Message(0, b"x" * 50)])
    with pytest.raises(MissingDictionary):
        StreamSessionDecoder(DictionaryRegistry()).feed(wire)
    wrong = DictionaryRegistry()
    wrong.register(1, dictionaries[1])
    with pytest.raises(DictionaryMismatch):
        StreamSessionDecoder(wrong).feed(wire)


def test_peer_window_larger_than_limit_rejected():
    wire = StreamEncoder(window_log=24).encode([Message(0, b"hi")])
    with pytest.raises(ProtocolError, match="window_log"):
        StreamSessionDecoder(max_window_log=20).feed(wire)
    assert StreamSessionDecoder(max_window_log=24).feed(wire) == [Message(0, b"hi")]


def test_corruption_detected_and_session_poisoned():
    enc = StreamEncoder()
    first = enc.encode([Message(0, b"payload " * 200)])
    second = enc.encode([Message(1, b"next")])
    damaged = bytearray(first)
    damaged[-3] ^= 0x41
    dec = StreamSessionDecoder()
    with pytest.raises(ProtocolError):
        dec.feed(bytes(damaged))
    with pytest.raises(SessionPoisoned):
        dec.feed(second)


def test_reordered_frames_fail():
    enc = StreamEncoder()
    frames = [enc.encode([Message(i, b"repeat me " * 40 + bytes([i]))]) for i in range(3)]
    dec = StreamSessionDecoder()
    dec.feed(frames[0])
    with pytest.raises(ProtocolError):
        dec.feed(frames[2])


def test_bad_magic_unknown_type_data_before_reset():
    with pytest.raises(ProtocolError, match="not an AWC1"):
        StreamSessionDecoder().feed(b"AWF1....")
    with pytest.raises(ProtocolError, match="unknown AWC1 frame type"):
        StreamSessionDecoder().feed(STREAM_MAGIC + b"\x07")
    with pytest.raises(ProtocolError, match="before RESET"):
        StreamSessionDecoder().feed(STREAM_MAGIC + b"\x00\x01\x01\x01")


def test_declared_size_limits_checked_before_payload():
    wire = StreamEncoder().encode([Message(0, b"z" * 5000)])
    header_end = len(STREAM_MAGIC) + _RESET.size + 8
    with pytest.raises(ProtocolError, match="size beyond limit"):
        StreamSessionDecoder(max_raw_bytes=1000).feed(wire[:header_end])


def test_truncated_tail_detected():
    wire = StreamEncoder().encode([Message(0, b"abc" * 100)])
    dec = StreamSessionDecoder()
    assert dec.feed(wire[:-2]) == []
    with pytest.raises(ProtocolError, match="truncated"):
        dec.finish()


def test_random_binary_stream_property(registry):
    r = random.Random(1234)
    enc = StreamEncoder(registry, dictionary_version=1)
    dec = StreamSessionDecoder(registry)
    out, original = [], []
    for i in range(300):
        m = Message(r.getrandbits(64), r.randbytes(r.randrange(0, 3000)), r.randrange(256))
        original.append(m)
        frame = enc.encode([m])
        cut = r.randrange(len(frame) + 1)
        out += dec.feed(frame[:cut]) + dec.feed(frame[cut:])
    dec.finish()
    assert out == original


def test_tcp_stream_roundtrip(registry):
    one = DictionaryRegistry()
    one.register(1, registry.get(1))
    result = asyncio.run(tcp_roundtrip_demo(one, workload("mixed-agent", 60).messages, stream=True))
    assert result["frames"] == 60 and result["bytes_exact"]
