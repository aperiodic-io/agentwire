"""Live transport benchmark: one connection per session, one message at a time.

Every variant sees the same held-out sessions (newest 40% by session index);
dictionaries are trained only on the older 60%. Ratios are deterministic;
timings are medians of --repeats runs. Every frame is decoded and compared
byte-for-byte.

Framing parity: generic stream codecs are charged the same per-message header
as AWC1 DATA (type byte, original length, payload length, CRC32), so ratio
differences come from compression, not from framing choices.
"""
from __future__ import annotations

import argparse
import gc
import random
import statistics
import time
import zlib

from common import NAMES, load, machine, pct, sessions, write_json, rss_bytes
from agentwire import codecs as C
from agentwire.benchmark import emit_frames_at
from agentwire.delta import DeltaDecoder, DeltaEncoder
from agentwire.stream import StreamEncoder, StreamSessionDecoder
from agentwire.transport import DictionaryRegistry, FrameCodec, Message, StreamDecoder, _varint
from agentwire.zstd import compress_advanced, train_dictionary

DICT_SIZE = 64 * 1024


def split(msgs):
    ss = sessions(msgs)
    cut = int(len(ss) * 0.6)
    return ss[:cut], ss[cut:]


def train(train_sessions, size=DICT_SIZE, seed=1):
    samples = [m.data[:32768] for s in train_sessions for m in s if m.data]
    random.Random(seed).shuffle(samples)
    picked, total = [], 0
    for x in samples:
        if total > 48 << 20:
            break
        picked.append(x)
        total += len(x)
    return train_dictionary(picked, capacity=size)


def _hdr(orig: int, payload: int) -> int:
    return 1 + len(_varint(orig)) + len(_varint(payload)) + 4


# ---------------------------------------------------------------- variant runners
# Each runner(session_msgs) -> (wire_bytes, enc_ns_list, dec_ns_list, verified)

def run_awf1(sess, registry, version, batch):
    codec = FrameCodec(registry, max_raw_bytes=64 << 20, max_wire_bytes=128 << 20)
    msgs = [Message(i, m.data) for i, m in enumerate(sess)]
    times = [m.t_ms for m in sess]
    t0 = time.perf_counter_ns()
    frames = emit_frames_at(msgs, times, codec, version, one=not batch)
    enc = time.perf_counter_ns() - t0
    dec = StreamDecoder(codec)
    t0 = time.perf_counter_ns()
    out = []
    for f, _ in frames:
        out += dec.feed(f)
    d = time.perf_counter_ns() - t0
    assert out == msgs
    per = len(msgs)
    return sum(len(f) for f, _ in frames), [enc / per] * per, [d / per] * per


def run_awc1(sess, registry, version, wlog):
    enc = StreamEncoder(registry, dictionary_version=version, window_log=wlog, max_raw_bytes=64 << 20)
    dec = StreamSessionDecoder(registry, max_raw_bytes=64 << 20, max_wire_bytes=128 << 20, max_window_log=wlog)
    wire, et, dt = 0, [], []
    for i, m in enumerate(sess):
        msg = Message(i, m.data)
        t0 = time.perf_counter_ns()
        w = enc.encode([msg])
        t1 = time.perf_counter_ns()
        got = dec.feed(w)
        t2 = time.perf_counter_ns()
        assert got == [msg]
        wire += len(w)
        et.append(t1 - t0)
        dt.append(t2 - t1)
    enc.close()
    dec.close()
    return wire, et, dt


def run_generic(sess, codec):
    enc, dec = codec.encoder(), codec.decoder()
    wire, et, dt = 0, [], []
    for m in sess:
        t0 = time.perf_counter_ns()
        chunk = enc.push(m.data)
        crc = zlib.crc32(m.data)
        t1 = time.perf_counter_ns()
        got = dec.push(chunk, len(m.data)) if dec.needs_size else dec.push(chunk)
        ok = zlib.crc32(got) == crc
        t2 = time.perf_counter_ns()
        assert ok and got == m.data
        wire += len(chunk) + _hdr(len(m.data), len(chunk))
        et.append(t1 - t0)
        dt.append(t2 - t1)
    enc.close()
    dec.close()
    return wire, et, dt


def run_awd1(sess, dictionary, refs=4, wlog=22):
    enc = DeltaEncoder(references=refs, dictionary=dictionary, window_log=wlog)
    dec = DeltaDecoder(references=refs, dictionary=dictionary)
    wire, et, dt = 0, [], []
    for m in sess:
        t0 = time.perf_counter_ns()
        w = enc.encode(m.data)
        t1 = time.perf_counter_ns()
        got = dec.feed(w)
        t2 = time.perf_counter_ns()
        assert got == [m.data]
        wire += len(w)
        et.append(t1 - t0)
        dt.append(t2 - t1)
    enc.close()
    dec.close()
    return wire, et, dt


def run_offline(sess, level=19):
    data = b"".join(m.data for m in sess)
    t0 = time.perf_counter_ns()
    f = compress_advanced(data, level, window_log=27, long_distance=True)
    return len(f), [(time.perf_counter_ns() - t0) / len(sess)] * len(sess), [0] * len(sess)


def variants(dictionary, registry, gzip_ok=True):
    v = [
        ("awf1/one", "AWF1 baseline: one zstd-3 frame per message", lambda s: run_awf1(s, registry, 0, False)),
        ("awf1/one+dict", "AWF1 one frame per message + 64 KiB dictionary", lambda s: run_awf1(s, registry, 1, False)),
        ("awf1/batch25ms+dict", "AWF1 25 ms / 32 msg / 64 KiB batches + dictionary", lambda s: run_awf1(s, registry, 1, True)),
        ("awc1/w20", "AWC1 zstd-3 stream, 1 MiB window", lambda s: run_awc1(s, registry, 0, 20)),
        ("awc1/w20+dict", "AWC1 stream + dictionary, 1 MiB window", lambda s: run_awc1(s, registry, 1, 20)),
        ("awc1/w23+dict", "AWC1 stream + dictionary, 8 MiB window", lambda s: run_awc1(s, registry, 1, 23)),
        ("zstd1-stream/w23", "zstd-1 stream, 8 MiB window", lambda s: run_generic(s, C.ZstdStreamCodec(1, 23))),
        ("zstd6-stream/w23", "zstd-6 stream, 8 MiB window", lambda s: run_generic(s, C.ZstdStreamCodec(6, 23))),
        ("zstd3-stream/w23-long", "zstd-3 stream, 8 MiB window, long-distance matching", lambda s: run_generic(s, C.ZstdStreamCodec(3, 23, True))),
        ("deflate6-stream", "deflate-6 sync-flush stream (permessage-deflate / HTTP gzip), 32 KiB window", lambda s: run_generic(s, C.DeflateStreamCodec(6))),
        ("brotli5-stream/w22", "brotli-5 flushed stream, 4 MiB window", lambda s: run_generic(s, C.BrotliStreamCodec(5, 22))),
        ("brotli5-stream/w24", "brotli-5 flushed stream, 16 MiB window", lambda s: run_generic(s, C.BrotliStreamCodec(5, 24))),
        ("lz4-stream", "LZ4 frame, linked blocks, flushed (64 KiB window)", lambda s: run_generic(s, C.Lz4StreamCodec(0))),
        ("awd1/w22", "AWD1 context delta (4 refs) + zstd-3 stream, 4 MiB window", lambda s: run_awd1(s, None)),
        ("awd1/w22+dict", "AWD1 context delta + dictionary", lambda s: run_awd1(s, dictionary)),
        ("ref/offline-zstd19-long", "Reference: whole session, one zstd-19 --long frame (not live)", run_offline),
    ]
    if C.available()["brotli_dictionary"]:
        v.insert(12, ("brotli5-stream/w22+dict", "brotli-5 stream + same 64 KiB raw dictionary",
                      lambda s: run_generic(s, C.BrotliStreamCodec(5, 22, dictionary))))
    return v


def memory_per_connection(make_pair, sess_list, n=16):
    """RSS growth per live connection (encoder + decoder, both in this process)."""
    gc.collect()
    before = rss_bytes()
    keep = []
    for s in (sess_list * n)[:n]:
        e, d = make_pair()
        for m in s[:300]:
            e(m.data) if d is None else d(e(m.data))
        keep.append((e, d))
    gc.collect()
    return (rss_bytes() - before) / n


def mem_pairs(dictionary, registry):
    def awc(wlog, version):
        def mk():
            enc = StreamEncoder(registry, dictionary_version=version, window_log=wlog, max_raw_bytes=64 << 20)
            dec = StreamSessionDecoder(registry, max_raw_bytes=64 << 20, max_wire_bytes=128 << 20, max_window_log=wlog)
            c = [0]

            def e(b):
                c[0] += 1
                return enc.encode([Message(c[0], b)])
            return e, dec.feed
        return mk

    def gen(codec):
        def mk():
            enc, dec = codec.encoder(), codec.decoder()
            if dec.needs_size:
                state = {}

                def e(b):
                    state["n"] = len(b)
                    return enc.push(b)
                return e, lambda ch: dec.push(ch, state["n"])
            return enc.push, dec.push
        return mk

    def awd():
        enc, dec = DeltaEncoder(dictionary=dictionary), DeltaDecoder(dictionary=dictionary)
        return enc.encode, dec.feed
    return {"awc1/w20+dict": awc(20, 1), "awc1/w23+dict": awc(23, 1),
            "deflate6-stream": gen(C.DeflateStreamCodec(6)), "brotli5-stream/w22": gen(C.BrotliStreamCodec(5, 22)),
            "brotli5-stream/w24": gen(C.BrotliStreamCodec(5, 24)), "lz4-stream": gen(C.Lz4StreamCodec()),
            "zstd3-stream/w23-long": gen(C.ZstdStreamCodec(3, 23, True)), "awd1/w22+dict": awd}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--corpora", nargs="*", default=[n for n in NAMES if n != "sse"])
    ap.add_argument("--max-test-bytes", type=int, default=24 << 20)
    a = ap.parse_args(argv)
    out = {"machine": machine(), "repeats": a.repeats, "dictionary_bytes": DICT_SIZE, "corpora": {}}
    for name in a.corpora:
        tr, te = split(load(name))
        budget, test = 0, []
        for s in te:                          # cap per-corpus test bytes for runtime
            if budget > a.max_test_bytes:
                break
            test.append(s)
            budget += sum(len(m.data) for m in s)
        t0 = time.perf_counter()
        dictionary = train(tr)
        train_s = time.perf_counter() - t0
        registry = DictionaryRegistry()
        registry.register(1, dictionary)
        raw = sum(len(m.data) for s in test for m in s)
        nmsg = sum(len(s) for s in test)
        rows = []
        for key, desc, runner in variants(dictionary, registry):
            wires, enc_tot, dec_tot, per_enc, per_dec = [], [], [], [], []
            for _ in range(a.repeats):
                w = 0
                e_all, d_all = [], []
                for s in test:
                    wb, et, dt = runner(s)
                    w += wb
                    e_all += et
                    d_all += dt
                wires.append(w)
                enc_tot.append(sum(e_all))
                dec_tot.append(sum(d_all))
                per_enc.append(e_all)
                per_dec.append(d_all)
            mid = sorted(range(a.repeats), key=lambda i: enc_tot[i])[a.repeats // 2]
            lat = [x + y for x, y in zip(per_enc[mid], per_dec[mid])]
            wire = statistics.median(wires)
            rows.append({"variant": key, "description": desc, "raw_bytes": raw, "wire_bytes": int(wire),
                         "ratio": raw / wire,
                         "encode_MBps": raw / 1e6 / (statistics.median(enc_tot) / 1e9),
                         "decode_MBps": (raw / 1e6 / (statistics.median(dec_tot) / 1e9)) if statistics.median(dec_tot) else None,
                         "latency_us_p50": pct(lat, 50) / 1e3, "latency_us_p99": pct(lat, 99) / 1e3})
            print(f"{name:15} {key:26} {raw / wire:7.2f}x  enc {rows[-1]['encode_MBps']:7.1f} MB/s  p99 {rows[-1]['latency_us_p99']:8.0f} us", flush=True)
        mem = {k: memory_per_connection(mk, test[:4]) for k, mk in mem_pairs(dictionary, registry).items()}
        out["corpora"][name] = {"test_sessions": len(test), "test_messages": nmsg, "test_bytes": raw,
                                "train_sessions": len(tr), "dictionary_train_seconds": train_s,
                                "rows": rows, "memory_per_connection_bytes": mem}
        write_json("transport.json", out)


if __name__ == "__main__":
    main()
