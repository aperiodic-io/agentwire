from __future__ import annotations

import os
import random
import struct
import subprocess

import pytest

from agentwire.transport import (
    BoundedBatcher, DictionaryMismatch, DictionaryRegistry, FrameCodec,
    HEADER_SIZE, Message, MissingDictionary, ProtocolError, SequenceError,
    StreamDecoder, _record,
)
from agentwire.zstd import Zstd, ZstdError


@pytest.mark.parametrize("payload", [b"", b"hello", b"\x00\xff\x80", "Üdvözlet 🛰️".encode(),
                                     b"{" + b"x" * 9000 + b"}", bytes(range(256))])
def test_byte_exact_roundtrip(payload):
    codec = FrameCodec()
    event = Message(2**60+4, payload, 255)
    frame = codec.encode([event], 0)
    info, out = codec.decode(frame)
    assert out == [event]
    assert info.count == 1
    assert len(frame) == info.wire_length


def test_zstd_native_compatibility_with_cli():
    message = Message(12, b"compressed payload " * 100)
    codec = FrameCodec()
    frame = codec.encode([message], 0)
    if not (frame[5] & 2):
        proc = subprocess.run(["zstd", "-d", "-q", "-c"],input=frame[HEADER_SIZE:],
                              capture_output=True,check=True)
        assert proc.stdout == _record(message)


def test_dictionary_rotation(registry):
    codec = FrameCodec(registry)
    batcher = BoundedBatcher(codec, dictionary_version=1,
                             max_messages=10,max_batch_bytes=20000)
    first = Message(1,b'{"service":"event-gateway","region":"eu-central"}'*50)
    second = Message(2,b'{"service":"agent-worker","region":"eu-central"}'*50)
    assert batcher.add(first,0) == []
    frames=batcher.rotate(2)
    assert len(frames) == 1
    frames += batcher.add(second,1)
    frames.append(batcher.flush())
    infos=[codec.decode(f)[0] for f in frames]
    assert [info.dictionary_version for info in infos] == [1,2]
    out=StreamDecoder(codec).feed(b"".join(frames))
    assert out == [first,second]


def test_missing_dictionary_rejected(registry):
    codec = FrameCodec(registry)
    frame = codec.encode([Message(0,b'"status":200,"service":"agent-worker"'*20)],0,1)
    assert codec.decode(frame)[0].dictionary_version == 1
    with pytest.raises(MissingDictionary):
        FrameCodec().decode(frame)


def test_wrong_version_payload_rejected(registry,dictionaries):
    codec = FrameCodec(registry)
    frame = codec.encode([Message(0,b'{"status":"success","service":"agent-worker"}'*100)],0,1)
    wrong=DictionaryRegistry()
    wrong.register(1,dictionaries[1])
    with pytest.raises(DictionaryMismatch):
        FrameCodec(wrong).decode(frame)
    with pytest.raises(DictionaryMismatch):
        wrong.register(1,dictionaries[0])


def test_frame_corruption_is_detected():
    codec=FrameCodec()
    frame=codec.encode([Message(1,b"very common repeating text "*100)],0)
    corrupt=bytearray(frame)
    corrupt[-1] ^= 0x01
    with pytest.raises(ProtocolError):
        codec.decode(bytes(corrupt))


def test_frame_header_mismatch_and_trailing_bytes():
    codec=FrameCodec()
    frame=codec.encode([Message(1,b"abc")],0)
    for bad in [b"AWF2"+frame[4:], frame[:-1], frame+b"x", frame[:5]+bytes([255])+frame[6:]]:
        with pytest.raises(ProtocolError):
            codec.decode(bad)


def test_frame_invalid_count():
    codec=FrameCodec()
    frame=bytearray(codec.encode([Message(1,b"hello")],0,force_raw=True))
    frame[6:8]=struct.pack(">H",2)
    with pytest.raises(ProtocolError):
        codec.decode(bytes(frame))


def test_frame_limited_decompression_allocation():
    codec=FrameCodec(max_raw_bytes=200)
    big=FrameCodec().encode([Message(1,b"z"*10000)],0)
    with pytest.raises(ProtocolError, match="beyond limit"):
        codec.decode(big)


def test_noncanonical_varint_rejected():
    # Body has (overlong ID 0, kind 0, payload length 1, payload a)
    # The byte-exact body hash is intentionally valid in this crafted frame.
    from agentwire.transport import _HEADER, _digest, MAGIC, VERSION, FLAG_RAW, ZERO_HASH
    body = b"\x80\x00\x00\x01a"
    frame = _HEADER.pack(MAGIC, VERSION, FLAG_RAW,1,0,0,len(body),len(body),ZERO_HASH,_digest(body))+body
    with pytest.raises(ProtocolError, match="noncanonical"):
        FrameCodec().decode(frame)


def test_encoder_rejects_empty_batch_and_oversized():
    codec=FrameCodec(max_raw_bytes=90,max_messages=1)
    with pytest.raises(ProtocolError):
        codec.encode([],0)
    with pytest.raises(ProtocolError):
        codec.encode([Message(1,b"z"*100)],0)
    with pytest.raises(ProtocolError):
        codec.encode([Message(1,b"a"),Message(2,b"b")],0)


def test_fragmented_transport_and_finish():
    codec=FrameCodec()
    frames=[codec.encode([Message(i,os.urandom(i+3))],i) for i in range(20)]
    dec=StreamDecoder(codec)
    out=[]
    for b in b"".join(frames):
        out.extend(dec.feed(bytes([b])))
    dec.finish()
    assert [m.payload for m in out] == [codec.decode(f)[1][0].payload for f in frames]
    dec2=StreamDecoder(codec)
    dec2.feed(frames[0][:-1])
    with pytest.raises(ProtocolError,match="truncated"):
        dec2.finish()


def test_duplicate_missing_out_of_order_sequences():
    codec=FrameCodec()
    f0=codec.encode([Message(1,b"one")],0)
    f1=codec.encode([Message(2,b"two")],1)
    dec=StreamDecoder(codec)
    assert dec.feed(f0) == [Message(1,b"one")]
    with pytest.raises(SequenceError):
        dec.feed(f0)
    other=StreamDecoder(codec)
    with pytest.raises(SequenceError):
        other.feed(f1)


def test_batching_limits_and_time():
    codec=FrameCodec()
    batch=BoundedBatcher(codec,max_messages=3,max_batch_bytes=100,max_delay_ms=20)
    assert batch.add(Message(1,b"a"*10),0) == []
    assert batch.add(Message(2,b"a"*10),5) == []
    assert batch.poll(19) == []
    due=batch.poll(20)
    assert [m.id for m in codec.decode(due[0])[1]] == [1,2]
    assert batch.add(Message(3,b"a"*10),21) == []
    assert batch.add(Message(4,b"b"*10),23) == []
    count=batch.add(Message(5,b"c"*10),24)
    assert len(count)==1
    assert [m.id for m in codec.decode(count[0])[1]] == [3,4,5]
    assert batch.flush() is None


def test_oversize_add_does_not_flush_pending():
    codec=FrameCodec()
    batch=BoundedBatcher(codec,max_batch_bytes=64,max_delay_ms=5)
    batch.add(Message(0,b"saved"),0)
    with pytest.raises(ProtocolError):
        batch.add(Message(1,b"too big"*100),100)
    assert codec.decode(batch.flush())[1] == [Message(0,b"saved")]


def test_clock_monotonic_and_rotation_missing_dictionary():
    batch=BoundedBatcher(FrameCodec(),max_batch_bytes=100)
    batch.add(Message(1,b"abc"),8)
    with pytest.raises(ValueError):
        batch.poll(4)
    with pytest.raises(MissingDictionary):
        batch.rotate(777)
    assert batch.flush() is not None


def test_random_binary_messages_property(registry):
    r=random.Random(20261104)
    codec=FrameCodec(registry)
    events=[Message(r.getrandbits(63),r.randbytes(r.randint(0,1600)),r.randint(0,255))
            for _ in range(330)]
    for version in [0,1]:
        batch=BoundedBatcher(codec,version,max_messages=9,max_batch_bytes=10000,max_delay_ms=40)
        frames=[]
        for i,m in enumerate(events):
            frames.extend(batch.add(m,i*3))
        if last:=batch.flush(): frames.append(last)
        stream=StreamDecoder(codec)
        output=[]
        for i in range(0,len(b"".join(frames)),1251):
            output.extend(stream.feed(b"".join(frames)[i:i+1251]))
        stream.finish()
        assert output==events


def test_raw_fallback_for_small_incompressible_messages(registry):
    codec=FrameCodec(registry)
    frame=codec.encode([Message(0,b"0123456789")],0,1)
    info,_=codec.decode(frame)
    assert info.raw
    assert info.dictionary_version==0


def test_actual_loopback_tcp_with_backpressure(registry):
    import asyncio
    from agentwire.asyncio_transport import tcp_roundtrip_demo
    messages=[Message(i, (b'{"tool":"query","ok":true,"result":"hello"}'*20) + bytes([i%256]))
              for i in range(81)]
    out=asyncio.run(tcp_roundtrip_demo(registry,messages))
    assert out["bytes_exact"]
    assert out["messages"]==81
    assert 1 <= out["frames"] <=81
    assert out["ack"]=="ACK:81"
