"""Streaming-response benchmark: SSE events as LLM APIs send them.

One HTTP response = one compression context. Each event is flushed as soon as it
is produced (that is what makes a token appear on the client). We measure body
bytes per event, codec CPU time per event (encode + decode, p50/p99), and, for
coalescing variants, the delay added by holding events back.

Body bytes exclude transfer framing (HTTP/1.1 chunk headers or HTTP/2 DATA frame
headers), which is the same for every variant including identity. AWC1 and
AWD1 are self-framing, so their bytes include their own headers.
"""
from __future__ import annotations

import argparse
import statistics
import time

from bench_transport import split, train
from common import load, machine, pct, write_json
from agentwire import codecs as C
from agentwire.delta import DeltaDecoder, DeltaEncoder
from agentwire.stream import StreamEncoder, StreamSessionDecoder
from agentwire.transport import DictionaryRegistry, Message


def flush_each(codec):
    def run(stream):
        enc, dec = codec.encoder(), codec.decoder()
        size, lat = 0, []
        for m in stream:
            t0 = time.perf_counter_ns()
            ch = enc.push(m.data)
            got = dec.push(ch, len(m.data)) if dec.needs_size else dec.push(ch)
            lat.append(time.perf_counter_ns() - t0)
            assert got == m.data
            size += len(ch)
        enc.close()
        dec.close()
        return size, lat, [0] * len(stream)
    return run


def coalesce(codec, window_ms):
    """Hold events until window_ms after the first held one, then flush them together."""
    def run(stream):
        enc, dec = codec.encoder(), codec.decoder()
        size, lat, waits, held, since = 0, [], [], [], None

        def flush():
            nonlocal size
            data = b"".join(x.data for x in held)
            t0 = time.perf_counter_ns()
            ch = enc.push(data)
            got = dec.push(ch, len(data)) if dec.needs_size else dec.push(ch)
            lat.extend([(time.perf_counter_ns() - t0) / len(held)] * len(held))
            assert got == data
            size += len(ch)
            for x in held:
                waits.append(since + window_ms - x.t_ms)
            held.clear()
        for m in stream:
            if held and m.t_ms >= since + window_ms:
                flush()
            if not held:
                since = m.t_ms
            held.append(m)
        if held:
            flush()
        enc.close()
        dec.close()
        return size, lat, waits
    return run


def identity(stream):
    return sum(len(m.data) for m in stream), [0] * len(stream), [0] * len(stream)


def awc1(registry, version, wlog):
    def run(stream):
        e = StreamEncoder(registry, dictionary_version=version, window_log=wlog)
        d = StreamSessionDecoder(registry, max_window_log=wlog)
        size, lat = 0, []
        for i, m in enumerate(stream):
            t0 = time.perf_counter_ns()
            w = e.encode([Message(i, m.data)])
            got = d.feed(w)
            lat.append(time.perf_counter_ns() - t0)
            assert got[0].payload == m.data
            size += len(w)
        return size, lat, [0] * len(stream)
    return run


def awd1(dictionary, refs):
    def run(stream):
        e, d = DeltaEncoder(references=refs, dictionary=dictionary, window_log=20), DeltaDecoder(references=refs, dictionary=dictionary)
        size, lat = 0, []
        for m in stream:
            t0 = time.perf_counter_ns()
            w = e.encode(m.data)
            got = d.feed(w)
            lat.append(time.perf_counter_ns() - t0)
            assert got == [m.data]
            size += len(w)
        return size, lat, [0] * len(stream)
    return run


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args(argv)
    tr, te = split(load("sse"))
    dictionary = train(tr, size=16 * 1024)
    reg = DictionaryRegistry()
    reg.register(1, dictionary)
    V = [("identity", identity),
         ("deflate-6 (HTTP gzip, flush/event)", flush_each(C.DeflateStreamCodec(6))),
         ("deflate-1 (flush/event)", flush_each(C.DeflateStreamCodec(1))),
         ("brotli-5 w22 (flush/event)", flush_each(C.BrotliStreamCodec(5, 22))),
         ("brotli-5 w22 + 16 KiB dict (flush/event)", flush_each(C.BrotliStreamCodec(5, 22, dictionary))),
         ("zstd-3 w20 (flush/event)", flush_each(C.ZstdStreamCodec(3, 20))),
         ("zstd-1 w20 (flush/event)", flush_each(C.ZstdStreamCodec(1, 20))),
         ("zstd-3 w20 + 16 KiB dict (flush/event)", flush_each(C.ZstdStreamCodec(3, 20, dictionary=dictionary))),
         ("lz4 (flush/event)", flush_each(C.Lz4StreamCodec())),
         ("AWC1 w20", awc1(reg, 0, 20)), ("AWC1 w20 + dict", awc1(reg, 1, 20)),
         ("AWD1 (1 ref) + zstd-3", awd1(None, 1)), ("AWD1 (4 refs) + zstd-3", awd1(None, 4)),
         ("AWD1 (4 refs) + zstd-3 + dict", awd1(dictionary, 4)),
         ("deflate-6, coalesce 50 ms", coalesce(C.DeflateStreamCodec(6), 50)),
         ("zstd-3, coalesce 50 ms", coalesce(C.ZstdStreamCodec(3, 20), 50)),
         ("zstd-3 + dict, coalesce 50 ms", coalesce(C.ZstdStreamCodec(3, 20, dictionary=dictionary), 50)),
         ("zstd-3, coalesce 200 ms", coalesce(C.ZstdStreamCodec(3, 20), 200))]
    raw = sum(len(m.data) for s in te for m in s)
    n = sum(len(s) for s in te)
    rows = []
    for key, run in V:
        sizes, lats, waits = [], [], []
        for _ in range(a.repeats):
            sz, la, wa = 0, [], []
            for s in te:
                x, l, w = run(s)
                sz += x
                la += l
                wa += w
            sizes.append(sz)
            lats.append(la)
            waits = wa
        mid = sorted(range(a.repeats), key=lambda i: sum(lats[i]))[a.repeats // 2]
        lat = lats[mid]
        rows.append({"variant": key, "bytes": sizes[0], "ratio": raw / sizes[0], "bytes_per_event": sizes[0] / n,
                     "codec_us_p50": pct(lat, 50) / 1e3, "codec_us_p99": pct(lat, 99) / 1e3,
                     "added_wait_ms_p50": pct(waits, 50), "added_wait_ms_p99": pct(waits, 99)})
        r = rows[-1]
        print(f"{key:42} {r['ratio']:6.2f}x {r['bytes_per_event']:7.1f} B/ev  p50 {r['codec_us_p50']:6.1f} us  p99 {r['codec_us_p99']:7.1f} us  wait p99 {r['added_wait_ms_p99']:.0f} ms", flush=True)
    write_json("sse.json", {"machine": machine(), "repeats": a.repeats, "test_streams": len(te), "events": n,
                            "raw_bytes": raw, "mean_event_bytes": raw / n,
                            "mean_event_gap_ms": statistics.mean(b.t_ms - x.t_ms for s in te for x, b in zip(s, s[1:])),
                            "rows": rows})


if __name__ == "__main__":
    main()
