"""AWJ1 columnar JSON block codec: byte-exact for any input bytes."""
import json
import os
import random

import pytest

from agentwire.jsoncol import decode_block, encode_block, shred_stats


def _random_json(r, depth=0):
    x = r.random()
    if depth > 3 or x < 0.3:
        return r.choice([None, True, False, r.randrange(-10**20, 10**20), r.random() * 1e6,
                         "", "ascii text", "ünïcødé ✓", "line\nbreak \"quoted\" \\ \t", "\ud83d\ude00"])
    if x < 0.65:
        return [_random_json(r, depth + 1) for _ in range(r.randrange(0, 5))]
    return {r.choice(["role", "content", "type", "id", "ü", "", "a b"]) + str(r.randrange(3)): _random_json(r, depth + 1)
            for _ in range(r.randrange(0, 5))}


def _messages(seed):
    r = random.Random(seed)
    styles = [dict(separators=(",", ":"), ensure_ascii=False), dict(separators=(",", ":")), {},
              dict(ensure_ascii=False), dict(indent=2)]
    for i in range(300):
        obj = _random_json(r)
        text = json.dumps(obj, **r.choice(styles)).encode("utf-8", "surrogatepass")
        yield text + r.choice([b"\n", b"", b"\r\n", b"\n\n"])
    yield b""
    yield b"not json at all"
    yield b'{"dup":1,"dup":2}\n'
    yield b'{"n": 1e5}\n'
    yield b'{"a" :1}'
    yield os.urandom(300)
    yield b'"\\u00e9"\n'
    yield b'[1.0, 1.50, -0.0]\n'
    yield b'\xef\xbb\xbf{"bom":true}\n'


@pytest.mark.parametrize("seed", [1, 2, 3])
@pytest.mark.parametrize("level", [3, 19])
def test_roundtrip_property(seed, level):
    msgs = list(_messages(seed))
    blob = encode_block(msgs, level=level)
    assert decode_block(blob) == msgs


def test_empty_block():
    assert decode_block(encode_block([])) == []


def test_shredding_used_for_canonical_lines_and_wins_on_repetitive_records():
    r = random.Random(5)
    msgs = [json.dumps({"type": "assistant", "sessionId": "abc", "uuid": f"{r.getrandbits(64):x}",
                        "message": {"role": "assistant", "usage": {"input_tokens": r.randrange(10**5),
                                                                  "output_tokens": r.randrange(10**3)},
                                    "content": [{"type": "text", "text": f"step {i}"}]}},
                       separators=(",", ":")).encode() + b"\n" for i in range(2000)]
    st = shred_stats(msgs)
    assert st["shredded"] == len(msgs)
    from agentwire.zstd import Zstd
    assert len(encode_block(msgs)) < len(Zstd(3).compress(b"".join(msgs)))


def test_corrupt_block_fails_closed():
    msgs = list(_messages(9))[:50]
    blob = bytearray(encode_block(msgs))
    blob[len(blob) // 2] ^= 0xFF
    with pytest.raises(ValueError):
        decode_block(bytes(blob))
