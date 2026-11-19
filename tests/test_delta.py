"""AWD1 context-delta codec: round trips, fragmentation, corruption, ratios."""
import json
import os
import random

import pytest

from agentwire.delta import DeltaDecoder, DeltaEncoder, common_prefix, common_suffix
from agentwire.transport import ProtocolError


def test_common_prefix_suffix_match_naive():
    r = random.Random(3)
    for _ in range(300):
        base = os.urandom(r.randrange(0, 300))
        a = base + os.urandom(r.randrange(0, 20))
        b = base + os.urandom(r.randrange(0, 20))
        naive = next((i for i, (x, y) in enumerate(zip(a, b)) if x != y), min(len(a), len(b)))
        assert common_prefix(a, b) == naive
        assert common_suffix(a[::-1], b[::-1]) == naive
        assert common_suffix(a, b, limit=5) <= 5


def _requests(n=30):
    msgs, out = [], []
    for i in range(n):
        body = json.dumps({"model": "m", "system": "be helpful " * 50, "messages": msgs}).encode()
        out.append(body)
        msgs = msgs + [{"role": "user" if i % 2 else "assistant", "content": f"turn {i} " * (i + 3)}]
    return out


def _random_messages(seed):
    r = random.Random(seed)
    prev = b""
    for _ in range(200):
        x = r.random()
        if x < 0.3:
            m = prev + os.urandom(r.randrange(0, 50))
        elif x < 0.5:
            m = prev[: r.randrange(0, len(prev) + 1)] + os.urandom(r.randrange(0, 30)) + prev[r.randrange(0, len(prev) + 1):]
        elif x < 0.6:
            m = b""
        else:
            m = os.urandom(r.randrange(0, 2000))
        yield m
        prev = m


@pytest.mark.parametrize("refs", [1, 4])
@pytest.mark.parametrize("dictionary", [None, b"messages content role user assistant " * 40])
def test_roundtrip_property_any_bytes(refs, dictionary):
    enc, dec = DeltaEncoder(references=refs, dictionary=dictionary), DeltaDecoder(references=refs, dictionary=dictionary)
    for m in _random_messages(refs):
        assert dec.feed(enc.encode(m)) == [m]
    dec.finish()


def test_fragmented_feed_one_byte_at_a_time():
    enc, dec = DeltaEncoder(), DeltaDecoder()
    msgs = _requests(12)
    wire = b"".join(enc.encode(m) for m in msgs)
    got = []
    for i in range(len(wire)):
        got += dec.feed(wire[i:i + 1])
    dec.finish()
    assert got == msgs


def test_resent_context_costs_only_the_new_suffix():
    enc = DeltaEncoder()
    msgs = _requests(40)
    wire = [enc.encode(m) for m in msgs]
    raw_tail = sum(len(m) for m in msgs[1:])
    assert sum(len(w) for w in wire[1:]) * 50 < raw_tail
    # Every later request costs far less than the turn content it adds.
    assert max(len(w) for w in wire[5:]) < 200


def test_corruption_and_truncation_fail_closed():
    enc = DeltaEncoder()
    msgs = _requests(6)
    wire = b"".join(enc.encode(m) for m in msgs)
    for pos in range(5, len(wire), max(1, len(wire) // 25)):
        bad = bytearray(wire)
        bad[pos] ^= 0x5A
        dec = DeltaDecoder()
        try:
            out = dec.feed(bytes(bad))
            dec.finish()
        except ProtocolError:
            continue
        assert out == msgs, f"corruption at {pos} returned wrong data"   # never silently wrong
    dec = DeltaDecoder()
    dec.feed(wire[:-3])
    with pytest.raises(ProtocolError):
        dec.finish()


def test_decoder_bounds_reconstructed_size():
    enc, dec = DeltaEncoder(), DeltaDecoder(max_message_bytes=1000)
    with pytest.raises(ProtocolError):
        dec.feed(enc.encode(b"x" * 5000))


def test_poisoned_after_error():
    enc, dec = DeltaEncoder(), DeltaDecoder()
    wire = enc.encode(b"hello world" * 10)
    with pytest.raises(ProtocolError):
        dec.feed(b"XXXX" + wire[4:])
    with pytest.raises(ProtocolError):
        dec.feed(wire)
