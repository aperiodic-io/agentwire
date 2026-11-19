"""Gateway multiplexing: many concurrent sessions over one connection vs one each.

Sessions from the held-out split are started together and interleaved by their
(synthetic) timestamps, as a gateway or message bus would see them. Compares
per-session AWC1 connections, one shared AWC1 stream for everything, and AWF1
batching on the shared connection (more messages land in each 25 ms window).
"""
from __future__ import annotations

from bench_transport import split, train
from common import load, machine, write_json
from agentwire.benchmark import emit_frames_at
from agentwire.stream import StreamEncoder
from agentwire.transport import DictionaryRegistry, FrameCodec, Message


def main():
    out = []
    for name in ["collab", "transcripts-cc", "transcripts-oh"]:
        tr, te = split(load(name))
        te = te[:60]
        reg = DictionaryRegistry()
        reg.register(1, train(tr))
        raw = sum(len(m.data) for s in te for m in s)
        merged = sorted(((m.t_ms, si, i, m) for si, s in enumerate(te) for i, m in enumerate(s)))
        per = 0
        for s in te:
            e = StreamEncoder(reg, 1, window_log=20, max_raw_bytes=64 << 20)
            per += sum(len(e.encode([Message(i, m.data)])) for i, m in enumerate(s))
        row = {"corpus": name, "sessions": len(te), "raw_bytes": raw, "awc1_per_session_w20+dict": raw / per}
        for wlog in (20, 23, 25):
            e = StreamEncoder(reg, 1, window_log=wlog, max_raw_bytes=64 << 20)
            row[f"awc1_shared_w{wlog}+dict"] = raw / sum(len(e.encode([Message(k, x[3].data)])) for k, x in enumerate(merged))
        codec = FrameCodec(reg, max_raw_bytes=64 << 20, max_wire_bytes=128 << 20)
        msgs = [Message(k, x[3].data) for k, x in enumerate(merged)]
        frames = emit_frames_at(msgs, [x[0] for x in merged], codec, 1, one=False)
        row["awf1_shared_batch25ms+dict"] = raw / sum(len(f) for f, _ in frames)
        row["awf1_shared_msgs_per_frame"] = len(msgs) / len(frames)
        print(row, flush=True)
        out.append(row)
    write_json("multiplex.json", {"machine": machine(), "rows": out})


if __name__ == "__main__":
    main()
