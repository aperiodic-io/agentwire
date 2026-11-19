"""FastCDC chunking and the deduplicating chunk store."""
import os
import random

import pytest

from agentwire.cdc import ChunkStore, FastCDC


@pytest.mark.parametrize("avg", [1024, 8192])
def test_chunks_concatenate_to_input_and_respect_bounds(avg):
    cdc = FastCDC(avg)
    r = random.Random(avg)
    for n in [0, 1, avg // 4 - 1, avg, 10 * avg, 200_000]:
        data = bytes(r.getrandbits(8) for _ in range(n))
        cuts = cdc.chunks(data)
        assert b"".join(cuts) == data
        for c in cuts[:-1]:
            assert cdc.min_size <= len(c) <= cdc.max_size
        if cuts:
            assert len(cuts[-1]) <= cdc.max_size


def test_deterministic_and_shift_resistant():
    cdc = FastCDC(2048)
    data = os.urandom(300_000)
    a = cdc.chunks(data)
    assert a == cdc.chunks(data)
    b = cdc.chunks(b"inserted!" + data)
    shared = set(a) & set(b)
    assert len(shared) >= len(a) - 3      # only chunks near the edit change


def test_average_size_is_close_to_target():
    cdc = FastCDC(4096)
    cuts = cdc.chunks(os.urandom(2_000_000))
    mean = 2_000_000 / len(cuts)
    assert 2500 < mean < 7000


def test_store_roundtrip_and_dedup(tmp_path):
    store = ChunkStore(FastCDC(2048))
    base = os.urandom(100_000)
    refs = [store.put(base + os.urandom(i * 10)) for i in range(20)]
    for i, ref in enumerate(refs):
        out = store.get(ref)
        assert out[:100_000] == base and len(out) == 100_000 + i * 10
    st = store.stats()
    assert st.logical_bytes > 10 * st.unique_bytes
    blob = store.serialize()
    again = ChunkStore.deserialize(blob, FastCDC(2048))
    assert [again.get(r) for r in refs] == [store.get(r) for r in refs]


def test_store_detects_corruption():
    store = ChunkStore(FastCDC(1024))
    ref = store.put(os.urandom(10_000))
    blob = bytearray(store.serialize())
    blob[len(blob) // 2] ^= 0xFF
    with pytest.raises(ValueError):
        ChunkStore.deserialize(bytes(blob), FastCDC(1024)).get(ref)
