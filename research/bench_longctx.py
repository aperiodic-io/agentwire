"""Long-context scaling: what happens when the re-sent context outgrows the window.

Public trajectories have small contexts (median request ~30 KB). Long agent
sessions re-send 0.5-2 MB of context per turn. We build longer conversations by
chaining several OpenHands runs into one message list (content is real; only
the chaining is synthetic). Each request appends one whole run (tens of KB) and
re-sends the full OpenAI-shaped body, like the api-requests corpus but with
fewer, larger turns. Each run uses its own
connection; every message is decoded and verified.
"""
from __future__ import annotations

import json
import random
import time

from common import load, machine, sessions, write_json
from agentwire import codecs as C
from agentwire.delta import DeltaDecoder, DeltaEncoder
from agentwire.stream import StreamEncoder, StreamSessionDecoder
from agentwire.transport import Message

J = lambda o: json.dumps(o, ensure_ascii=False, separators=(",", ":")).encode()


def build(target_bytes: int, seed: int):
    """One long conversation: turns of several OpenHands runs appended until the context reaches target."""
    reqs = sessions(load("api-requests"))
    oh = [s for s in reqs if b'"tools"' in s[0].data]
    r = random.Random(seed)
    tools = json.loads(oh[0][0].data)["tools"]
    msgs, out, size = [], [], 0
    while size < target_bytes:
        # One "turn" appends a whole run's worth of messages (tens of KB), so
        # contexts reach MBs in ~100 requests instead of thousands.
        last = json.loads(r.choice(oh)[-1].data)["messages"]
        msgs = msgs + last[1:]
        body = J({"model": "model-y", "messages": msgs, "tools": tools, "temperature": 0})
        out.append(body)
        size = len(body)
    return out


def run_awc1(reqs, wlog):
    e = StreamEncoder(window_log=wlog, max_raw_bytes=64 << 20)
    d = StreamSessionDecoder(max_raw_bytes=64 << 20, max_wire_bytes=128 << 20, max_window_log=wlog)
    w = 0
    for i, b in enumerate(reqs):
        x = e.encode([Message(i, b)])
        assert d.feed(x)[0].payload == b
        w += len(x)
    return w


def run_awd1(reqs, wlog):
    e, d = DeltaEncoder(window_log=wlog), DeltaDecoder(max_window_log=wlog)
    w = 0
    for b in reqs:
        x = e.encode(b)
        assert d.feed(x) == [b]
        w += len(x)
    return w


def run_generic(reqs, codec):
    e, d = codec.encoder(), codec.decoder()
    w = 0
    for b in reqs:
        x = e.push(b)
        assert (d.push(x, len(b)) if d.needs_size else d.push(x)) == b
        w += len(x) + 8
    return w


def main():
    rows = []
    for target in (256 << 10, 1 << 20, 3 << 20, 6 << 20):
        reqs = build(target, seed=target)
        raw = sum(map(len, reqs))
        row = {"final_context_bytes": len(reqs[-1]), "requests": len(reqs), "raw_bytes": raw}
        V = {"AWC1 w20 (1 MiB)": lambda: run_awc1(reqs, 20), "AWC1 w23 (8 MiB)": lambda: run_awc1(reqs, 23),
             "AWC1 w25 (32 MiB)": lambda: run_awc1(reqs, 25),
             "zstd-3 stream w23 + long": lambda: run_generic(reqs, C.ZstdStreamCodec(3, 23, True)),
             "brotli-5 stream w24 (16 MiB)": lambda: run_generic(reqs, C.BrotliStreamCodec(5, 24)),
             "deflate-6 stream (32 KiB)": lambda: run_generic(reqs, C.DeflateStreamCodec(6)),
             "AWD1 w20": lambda: run_awd1(reqs, 20), "AWD1 w22": lambda: run_awd1(reqs, 22)}
        for k, fn in V.items():
            t0 = time.perf_counter()
            w = fn()
            row[k] = {"ratio": raw / w, "seconds": time.perf_counter() - t0}
        rows.append(row)
        print(json.dumps({k: (v["ratio"] if isinstance(v, dict) else v) for k, v in row.items()}), flush=True)
    write_json("longctx.json", {"machine": machine(), "rows": rows})


if __name__ == "__main__":
    main()
