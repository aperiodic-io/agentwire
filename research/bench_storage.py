"""Storage / archive benchmark on the held-out sessions of each corpus.

A "session file" is the session's messages joined with b"\\n" (what a harness
appends to its JSONL log). Variants compress each session file independently
unless marked corpus-level. Every variant is decompressed and compared
byte-for-byte. Throughput: medians of --repeats runs. Random access: mean cost
to fetch one random message, including everything that has to be decompressed.

pyarrow (Parquet) is an optional research-only extra; it is skipped if absent.
"""
from __future__ import annotations

import argparse
import io
import random
import statistics
import time

from bench_transport import split, train
from common import NAMES, load, machine, write_json
from agentwire import codecs as C
from agentwire.cdc import ChunkStore, FastCDC
from agentwire.delta import DeltaDecoder, DeltaEncoder
from agentwire.jsoncol import decode_block, encode_block
from agentwire.seekable import SeekableReader, SeekableWriter
from agentwire.zstd import compress_advanced, decompress_advanced


def per_session(codec):
    def enc(files):
        return [codec.compress(f) for f in files]

    def dec(blobs):
        return [codec.decompress(b) for b in blobs]
    return enc, dec


def corpus_level(codec):
    def enc(files):
        return [codec.compress(b"\n".join(files))]

    def dec(blobs):
        return [codec.decompress(blobs[0])]
    return enc, dec


def awj1(level, corpus=False):
    def enc(files, msgs_by_session):
        if corpus:
            return [encode_block([m for s in msgs_by_session for m in s], level)]
        return [encode_block(s, level) for s in msgs_by_session]

    def dec(blobs):
        return [decode_block(b) for b in blobs]
    return enc, dec


def awd1(level):
    def enc(files, msgs_by_session):
        out = []
        for s in msgs_by_session:
            e = DeltaEncoder(level=level, window_log=23)
            out.append(b"".join(e.encode(m) for m in s))
            e.close()
        return out

    def dec(blobs):
        res = []
        for b in blobs:
            d = DeltaDecoder()
            res.append(d.feed(b))
            d.finish()
        return res
    return enc, dec


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--corpora", nargs="*", default=NAMES)
    ap.add_argument("--max-test-bytes", type=int, default=24 << 20)
    ap.add_argument("--slow", action="store_true", help="include brotli-11 and xz-9e (minutes per corpus)")
    a = ap.parse_args(argv)
    out = {"machine": machine(), "repeats": a.repeats, "corpora": {}}
    for name in a.corpora:
        tr, te = split(load(name))
        test, budget = [], 0
        for s in te:
            if budget > a.max_test_bytes:
                break
            test.append(s)
            budget += sum(len(m.data) for m in s)
        dictionary = train(tr)
        msgs_by_session = [[m.data for m in s] for s in test]
        files = [b"\n".join(s) for s in msgs_by_session]
        raw = sum(map(len, files))
        V = [("gzip-6", per_session(C.GzipCodec(6))), ("gzip-9", per_session(C.GzipCodec(9))),
             ("lz4", per_session(C.Lz4Codec(0))), ("lz4hc-9", per_session(C.Lz4Codec(9))),
             ("zstd-1", per_session(C.ZstdCodec(1))), ("zstd-3", per_session(C.ZstdCodec(3))),
             ("zstd-9", per_session(C.ZstdCodec(9))), ("zstd-19", per_session(C.ZstdCodec(19))),
             ("zstd-19-long-w27", per_session(C.ZstdCodec(19, 27, True))),
             ("zstd-3+dict64k", per_session(C.ZstdCodec(3, dictionary=dictionary))),
             ("zstd-19+dict64k", per_session(C.ZstdCodec(19, dictionary=dictionary))),
             ("brotli-5-w22", per_session(C.BrotliCodec(5, 22))), ("brotli-9-w24", per_session(C.BrotliCodec(9, 24))),
             ("xz-6", per_session(C.XzCodec(6))),
             ("awj1-3 (columnar JSON)", awj1(3)), ("awj1-19 (columnar JSON)", awj1(19)),
             ("awd1-3 (context delta)", awd1(3)), ("awd1-19 (context delta)", awd1(19)),
             ("corpus: zstd-19-long-w27", corpus_level(C.ZstdCodec(19, 27, True))),
             ("corpus: awj1-19", awj1(19, corpus=True))]
        if a.slow:
            V += [("brotli-11-w24", per_session(C.BrotliCodec(11, 24))), ("xz-9e", per_session(C.XzCodec(9, True))),
                  ("corpus: xz-9e", corpus_level(C.XzCodec(9, True))),
                  ("corpus: brotli-11-w24", corpus_level(C.BrotliCodec(11, 24)))]
        rows = []
        for key, (enc, dec) in V:
            structured = key.startswith(("awj1", "awd1", "corpus: awj1"))
            et, dt, size = [], [], None
            for _ in range(a.repeats):
                t0 = time.perf_counter()
                blobs = enc(files, msgs_by_session) if structured else enc(files)
                t1 = time.perf_counter()
                back = dec(blobs)
                t2 = time.perf_counter()
                et.append(t1 - t0)
                dt.append(t2 - t1)
                size = sum(map(len, blobs))
            if structured:
                expect = [[m for s in msgs_by_session for m in s]] if key.startswith("corpus") else msgs_by_session
                assert back == expect, key
            else:
                assert back == ([b"\n".join(files)] if key.startswith("corpus") else files), key
            rows.append({"variant": key, "bytes": size, "ratio": raw / size,
                         "compress_MBps": raw / 1e6 / statistics.median(et),
                         "decompress_MBps": raw / 1e6 / statistics.median(dt)})
            print(f"{name:15} {key:28} {raw / size:7.2f}x  c {rows[-1]['compress_MBps']:8.1f}  d {rows[-1]['decompress_MBps']:8.1f} MB/s", flush=True)

        # ---- dedup: content-defined chunking, then compression
        all_msgs = [m for s in msgs_by_session for m in s]
        for avg in (1024, 4096):
            t0 = time.perf_counter()
            store = ChunkStore(FastCDC(avg), level=3)
            recipes = [store.put(m) for m in all_msgs]
            t1 = time.perf_counter()
            assert [store.get(r) for r in recipes[:200]] == all_msgs[:200]
            st = store.stats()
            # Variant 2: unique chunks in arrival order, packed in one zstd-19 --long stream.
            uniq = b"".join(f for f in _unique_chunks(store, FastCDC(avg), all_msgs))
            packed = len(compress_advanced(uniq, 19, 27, True))
            recipe = sum(len(r) + 2 for r in recipes)
            rows.append({"variant": f"cdc{avg // 1024}k dedup + zstd-3 per chunk", "bytes": st.stored_bytes + recipe,
                         "ratio": raw / (st.stored_bytes + recipe), "compress_MBps": raw / 1e6 / (t1 - t0),
                         "decompress_MBps": None, "dedup_ratio": st.logical_bytes / st.unique_bytes,
                         "unique_chunks": st.unique_chunks, "chunks": st.chunks})
            rows.append({"variant": f"cdc{avg // 1024}k dedup + zstd-19-long pack", "bytes": packed + recipe,
                         "ratio": raw / (packed + recipe), "compress_MBps": None, "decompress_MBps": None,
                         "dedup_ratio": st.logical_bytes / st.unique_bytes})
            print(f"{name:15} cdc{avg} dedup {st.logical_bytes / st.unique_bytes:.2f}x  stored {rows[-2]['ratio']:.2f}x  packed {rows[-1]['ratio']:.2f}x", flush=True)

        # ---- random access archives
        ra = []
        rng = random.Random(7)
        picks = [rng.randrange(len(all_msgs)) for _ in range(300)]
        for frame_kib, use_dict in [(16, True), (64, False), (64, True), (256, False), (1024, False), (4096, False)]:
            w = SeekableWriter(level=19 if frame_kib >= 256 else 9, frame_bytes=frame_kib * 1024,
                               dictionary=dictionary if use_dict else None)
            for m in all_msgs:
                w.add(m)
            blob = w.finish()
            rd = SeekableReader(blob, dictionary=dictionary if use_dict else None)
            t0 = time.perf_counter()
            for i in picks:
                rd._cache = None
                assert rd.message(i) == all_msgs[i]
            per = (time.perf_counter() - t0) / len(picks)
            msg_raw = sum(map(len, all_msgs))
            ra.append({"variant": f"seekable zstd, {frame_kib} KiB frames" + (" + dict" if use_dict else ""),
                       "bytes": len(blob), "ratio": msg_raw / len(blob), "random_fetch_us": per * 1e6,
                       "decompressed_per_fetch_bytes": msg_raw / len(rd.frames)})
        # whole-session files: one fetch = decompress the session file (zstd-19 long)
        frames = [compress_advanced(f, 19, 27, True) for f in files]
        sess_of = [i for i, s in enumerate(msgs_by_session) for _ in s]
        t0 = time.perf_counter()
        for i in picks:
            s = sess_of[i]
            decompress_advanced(frames[s], len(files[s]))
        ra.append({"variant": "per-session zstd-19-long file", "bytes": sum(map(len, frames)), "ratio": raw / sum(map(len, frames)),
                   "random_fetch_us": (time.perf_counter() - t0) / len(picks) * 1e6,
                   "decompressed_per_fetch_bytes": statistics.mean(len(files[sess_of[i]]) for i in picks)})
        ra += parquet_rows(test, picks, all_msgs)
        for r in ra:
            print(f"{name:15} {r['variant']:40} {r['ratio']:7.2f}x  fetch {r['random_fetch_us']:9.0f} us", flush=True)
        out["corpora"][name] = {"test_sessions": len(test), "raw_bytes": raw, "rows": rows, "random_access": ra}
        write_json("storage.json", out)


def _unique_chunks(store, cdc, msgs):
    import hashlib
    seen = set()
    for m in msgs:
        for c in cdc.chunks(m):
            d = hashlib.sha256(c).digest()
            if d not in seen:
                seen.add(d)
                yield c


def parquet_rows(test, picks, all_msgs):
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        return []
    sess = [m.session for s in test for m in s]
    table = pa.table({"session": pa.array(sess, pa.int32()), "t_ms": pa.array([m.t_ms for s in test for m in s], pa.int64()),
                      "kind": pa.array([m.kind for s in test for m in s]).dictionary_encode(),
                      "data": pa.array(all_msgs, pa.binary())})
    rows = []
    raw = sum(map(len, all_msgs))
    for level, rg in [(3, 1024), (19, 1024), (19, 16384)]:
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd", compression_level=level, row_group_size=rg,
                       use_dictionary=["kind"], write_statistics=["session"])
        blob = buf.getvalue()
        pf = pq.ParquetFile(io.BytesIO(blob))
        assert pf.read().column("data").to_pylist() == all_msgs
        starts, acc = [], 0
        for i in range(pf.num_row_groups):
            starts.append(acc)
            acc += pf.metadata.row_group(i).num_rows
        import bisect
        t0 = time.perf_counter()
        for i in picks:
            g = bisect.bisect_right(starts, i) - 1
            got = pf.read_row_group(g, columns=["data"]).column(0)[i - starts[g]].as_py()
            assert got == all_msgs[i]
        rows.append({"variant": f"parquet zstd-{level}, {rg}-row groups", "bytes": len(blob), "ratio": raw / len(blob),
                     "random_fetch_us": (time.perf_counter() - t0) / len(picks) * 1e6,
                     "decompressed_per_fetch_bytes": raw / pf.num_row_groups})
    return rows


if __name__ == "__main__":
    main()
