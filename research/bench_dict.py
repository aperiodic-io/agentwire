"""Dictionary lifecycle: size, drift across harnesses, retraining, AWF1 vs AWC1 benefit.

Dictionaries are trained on train splits (older 60% of sessions) and evaluated on
test splits. "Drift" is modelled as moving to a different harness/workload
(SWE-agent/Claude-Code-style logs -> OpenHands logs -> multi-agent traffic),
which changes keys, tool names and prompt boilerplate, the things a dictionary
captures. Within-harness drift is measured by training on the first 20% of
sessions and evaluating successive later slices.
"""
from __future__ import annotations

import time

from bench_transport import split, train
from common import load, machine, sessions, write_json
from agentwire.stream import StreamEncoder
from agentwire.transport import DictionaryRegistry, FrameCodec, Message


def awf1_one(test, registry, version):
    codec = FrameCodec(registry, max_raw_bytes=64 << 20, max_wire_bytes=128 << 20)
    return sum(len(codec.encode([Message(i, m.data)], i, version)) for s in test for i, m in enumerate(s))


def awc1(test, registry, version, wlog=20):
    total = 0
    for s in test:
        e = StreamEncoder(registry, dictionary_version=version, window_log=wlog, max_raw_bytes=64 << 20)
        total += sum(len(e.encode([Message(i, m.data)])) for i, m in enumerate(s))
        e.close()
    return total


def first_messages(test, k=5):
    return [s[:k] for s in test]


def main():
    data = {n: split(load(n)) for n in ["transcripts-cc", "transcripts-oh", "collab", "tool-outputs"]}
    tests = {n: te[:60] for n, (tr, te) in data.items()}
    dicts, train_s = {}, {}
    for size in (4, 16, 64, 112):
        t0 = time.perf_counter()
        dicts[f"cc-{size}k"] = train(data["transcripts-cc"][0], size * 1024)
        train_s[f"cc-{size}k"] = time.perf_counter() - t0
    for n, tag in [("transcripts-oh", "oh"), ("collab", "collab"), ("tool-outputs", "tools")]:
        t0 = time.perf_counter()
        dicts[f"{tag}-64k"] = train(data[n][0], 64 * 1024)
        train_s[f"{tag}-64k"] = time.perf_counter() - t0
    mixed = [s for n in data for s in data[n][0]]
    t0 = time.perf_counter()
    dicts["mixed-64k"] = train(mixed, 64 * 1024)
    train_s["mixed-64k"] = time.perf_counter() - t0
    reg = DictionaryRegistry()
    ids = {}
    for i, (k, d) in enumerate(dicts.items(), start=1):
        reg.register(i, d)
        ids[k] = i
    rows = []
    for tname, test in tests.items():
        raw = sum(len(m.data) for s in test for m in s)
        first = first_messages(test)
        raw_first = sum(len(m.data) for s in first for m in s)
        base = {"awf1": awf1_one(test, reg, 0), "awc1": awc1(test, reg, 0), "awc1_first5": awc1(first, reg, 0)}
        for dname in ["none"] + list(dicts):
            v = ids.get(dname, 0)
            r = {"test": tname, "dictionary": dname,
                 "awf1_one_ratio": raw / (awf1_one(test, reg, v) if v else base["awf1"]),
                 "awc1_ratio": raw / (awc1(test, reg, v) if v else base["awc1"]),
                 "awc1_first5_ratio": raw_first / (awc1(first, reg, v) if v else base["awc1_first5"])}
            rows.append(r)
            print(f"{tname:15} {dname:10} awf1 {r['awf1_one_ratio']:6.2f}x  awc1 {r['awc1_ratio']:6.2f}x  first5 {r['awc1_first5_ratio']:6.2f}x", flush=True)
    # within-harness staleness: train on first 20% of cc sessions, evaluate later slices
    ss = sessions(load("transcripts-cc"))
    cut = len(ss) // 5
    early = train(ss[:cut], 64 * 1024)
    reg2 = DictionaryRegistry()
    reg2.register(1, early)
    stale = []
    step = (len(ss) - cut) // 4
    for q in range(4):
        sl = ss[cut + q * step: cut + (q + 1) * step][:40]
        raw = sum(len(m.data) for s in sl for m in s)
        stale.append({"slice": q + 1, "awf1_none": raw / awf1_one(sl, reg2, 0), "awf1_dict": raw / awf1_one(sl, reg2, 1),
                      "awc1_none": raw / awc1(sl, reg2, 0), "awc1_dict": raw / awc1(sl, reg2, 1)})
        print("stale", stale[-1], flush=True)
    write_json("dictionaries.json", {"machine": machine(), "train_seconds": train_s,
                                     "dictionary_bytes": {k: len(v) for k, v in dicts.items()},
                                     "rows": rows, "staleness": stale})


if __name__ == "__main__":
    main()
