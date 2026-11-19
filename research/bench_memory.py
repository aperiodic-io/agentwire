"""Memory per live connection, measured in a fresh process per variant.

Each child process opens N sender+receiver pairs, feeds each 4 MiB of real
transcript messages (so windows and tables are fully allocated), and reports
RSS growth per pair. For zstd we also report libzstd's own accounting
(ZSTD_sizeof_CCtx / ZSTD_sizeof_DCtx), which is exact.
"""
from __future__ import annotations

import json
import subprocess
import sys

from common import ROOT, machine, write_json

CHILD = r'''
import sys, json, gc
sys.path.insert(0, ROOT); sys.path.insert(0, ROOT + "/research")
from common import load, rss_bytes
from agentwire import codecs as C
from agentwire.delta import DeltaEncoder, DeltaDecoder
variant, n = sys.argv[1], int(sys.argv[2])
msgs = [m.data for m in load("transcripts-cc")]
feed, total = [], 0
for m in msgs:
    feed.append(m); total += len(m)
    if total > 4 << 20: break
def make():
    if variant.startswith("awd1"):
        e, d = DeltaEncoder(window_log=22), DeltaDecoder()
        return (lambda b: d.feed(e.encode(b))), (e, d)
    codec = eval(variant)
    e, d = codec.encoder(), codec.decoder()
    if d.needs_size:
        return (lambda b: d.push(e.push(b), len(b))), (e, d)
    return (lambda b: d.push(e.push(b))), (e, d)
gc.collect(); before = rss_bytes()
keep, zsizes = [], []
for _ in range(n):
    f, pair = make()
    for b in feed: f(b)
    keep.append(pair)
    e, d = pair
    if hasattr(e, "memory_bytes") and e.memory_bytes():
        zsizes.append((e.memory_bytes() or 0) + ((d.memory_bytes() or 0) if hasattr(d, "memory_bytes") else 0))
gc.collect()
print(json.dumps({"rss_per_connection": (rss_bytes() - before) / n,
                  "libzstd_bytes_per_connection": (sum(zsizes) / len(zsizes)) if zsizes else None}))
'''

VARIANTS = {
    "zstd-3 stream w20 (AWC1 default)": "C.ZstdStreamCodec(3, 20)",
    "zstd-3 stream w23": "C.ZstdStreamCodec(3, 23)",
    "zstd-3 stream w23 + long": "C.ZstdStreamCodec(3, 23, True)",
    "zstd-1 stream w20": "C.ZstdStreamCodec(1, 20)",
    "deflate-6 stream (32 KiB window)": "C.DeflateStreamCodec(6)",
    "brotli-5 stream w22": "C.BrotliStreamCodec(5, 22)",
    "brotli-5 stream w24": "C.BrotliStreamCodec(5, 24)",
    "lz4 stream": "C.Lz4StreamCodec(0)",
    "AWD1 (4 refs) + zstd-3 w22": "awd1",
}


def main():
    rows = []
    for name, expr in VARIANTS.items():
        out = subprocess.run([sys.executable, "-c", CHILD.replace("ROOT", repr(str(ROOT))), expr, "32"],
                             capture_output=True, text=True, check=True)
        r = json.loads(out.stdout.strip().splitlines()[-1])
        r["variant"] = name
        rows.append(r)
        z = r["libzstd_bytes_per_connection"]
        print(f"{name:36} RSS {r['rss_per_connection'] / 1024:9.0f} KiB" + (f"   libzstd {z / 1024:8.0f} KiB" if z else ""), flush=True)
    write_json("memory.json", {"machine": machine(), "connections_per_process": 32, "bytes_fed_per_connection": 4 << 20,
                               "rows": rows})


if __name__ == "__main__":
    main()
