"""Seekable zstd archive: standard frame/seek-table layout, random access."""
import os
import random
import struct

import pytest

from agentwire.seekable import SEEKABLE_MAGIC, SeekableReader, SeekableWriter
from agentwire.zstd import Zstd


def _messages(n=500, seed=1):
    r = random.Random(seed)
    return [(b'{"i":%d,"text":"' % i) + os.urandom(r.randrange(0, 40)).hex().encode() + b'"}\n' * r.randrange(1, 30)
            for i in range(n)]


@pytest.mark.parametrize("frame_bytes", [1, 4096, 65536, 1 << 30])
@pytest.mark.parametrize("dictionary", [None, b'{"i":,"text":""}\n' * 64])
def test_every_message_and_range_recoverable(frame_bytes, dictionary):
    msgs = _messages()
    w = SeekableWriter(frame_bytes=frame_bytes, dictionary=dictionary)
    for m in msgs:
        w.add(m)
    blob = w.finish()
    rd = SeekableReader(blob, dictionary=dictionary)
    assert rd.message_count == len(msgs)
    order = list(range(len(msgs)))
    random.Random(4).shuffle(order)
    for i in order[:120]:
        assert rd.message(i) == msgs[i]
    whole = b"".join(msgs)
    assert rd.read_all() == whole
    for start, n in [(0, 1), (17, 5000), (len(whole) - 3, 3), (1234, 0)]:
        assert rd.read(start, n) == whole[start:start + n]


def test_layout_matches_seekable_spec_footer_and_is_plain_zstd_concat():
    msgs = _messages(200)
    w = SeekableWriter(frame_bytes=2048)
    for m in msgs:
        w.add(m)
    blob = w.finish()
    nframes, desc, magic = struct.unpack("<IBI", blob[-9:])
    assert magic == SEEKABLE_MAGIC and desc & 0x7C == 0
    assert nframes == len(SeekableReader(blob).frames) + 1 > 2   # + the message-index frame
    # The archive is also valid as a sequence of zstd frames + skippable frames:
    rd = SeekableReader(blob)
    joined = b"".join(Zstd().decompress(blob[f.offset:f.offset + f.csize], f.dsize) for f in rd.frames)
    assert joined == b"".join(msgs)


def test_corruption_and_truncation_fail_closed():
    w = SeekableWriter(frame_bytes=1024)
    for m in _messages(100):
        w.add(m)
    blob = w.finish()
    with pytest.raises(ValueError):
        SeekableReader(blob[:-5])
    bad = bytearray(blob)
    bad[10] ^= 0xFF
    rd = SeekableReader(bytes(bad))
    with pytest.raises(ValueError):
        rd.message(0)


def test_empty_archive():
    rd = SeekableReader(SeekableWriter().finish())
    assert rd.message_count == 0 and rd.read_all() == b""


def test_interoperates_with_independent_seekable_reader():
    pyzstd = pytest.importorskip("pyzstd")
    import io
    msgs = _messages(300)
    whole = b"".join(msgs)
    w = SeekableWriter(frame_bytes=4096)
    for m in msgs:
        w.add(m)
    blob = w.finish()
    assert pyzstd.decompress(blob) == whole
    f = pyzstd.SeekableZstdFile(io.BytesIO(blob))
    f.seek(len(whole) // 3)
    assert f.read(2000) == whole[len(whole) // 3:len(whole) // 3 + 2000]
    out = io.BytesIO()
    with pyzstd.SeekableZstdFile(out, "w", max_frame_content_size=4096) as g:
        g.write(whole)
    assert SeekableReader(out.getvalue()).read_all() == whole
