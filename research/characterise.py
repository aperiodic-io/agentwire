"""Measure the structure of each corpus: sizes, entropy, repetition, near-dups, JSON redundancy.

Writes results/research/characterisation.json and characterisation.md.
"""
from __future__ import annotations

import hashlib
import heapq
import json
import math
import re
import zlib
from collections import Counter, defaultdict

from common import NAMES, RESULTS, load, pct, sessions, write_json
from agentwire.cdc import FastCDC
from agentwire.delta import common_prefix, common_suffix
from agentwire.zstd import Zstd, compress_advanced


def entropy0(data: bytes) -> float:
    if not data:
        return 0.0
    c = Counter(data)
    n = len(data)
    return -sum(v / n * math.log2(v / n) for v in c.values())


def zstd_bpb(data: bytes, level=19) -> float:
    if not data:
        return 0.0
    return 8 * len(compress_advanced(data, level, window_log=27, long_distance=True)) / len(data)


_WORD = re.compile(rb"\w+")


def sketch(data: bytes, k=64) -> list[int]:
    words = _WORD.findall(data[:200_000])
    hs = {zlib.crc32(b" ".join(words[i:i + 5])) for i in range(max(1, len(words) - 4))}
    return heapq.nsmallest(k, hs)


def jaccard_est(a: list[int], b: list[int], k=64) -> float:
    if not a or not b:
        return 0.0
    union = heapq.nsmallest(k, set(a) | set(b))
    sa, sb = set(a), set(b)
    return sum(1 for h in union if h in sa and h in sb) / len(union)


def near_dups(msgs, threshold=0.8):
    """Share of messages (and bytes) that exactly or nearly (est. Jaccard >= 0.8 on
    5-word shingles) duplicate an EARLIER message, within the session / anywhere."""
    seen_exact_global, seen_exact_session = set(), defaultdict(set)
    buckets = defaultdict(list)     # LSH-ish: index sketches by their 4 smallest hashes
    per_session = defaultdict(list)
    res = Counter()
    for m in msgs:
        if len(m.data) < 64:
            continue
        res["n"] += 1
        res["bytes"] += len(m.data)
        h = hashlib.sha256(m.data).digest()
        if h in seen_exact_global:
            res["exact_global"] += 1
            res["exact_global_bytes"] += len(m.data)
        if h in seen_exact_session[m.session]:
            res["exact_session"] += 1
            res["exact_session_bytes"] += len(m.data)
        seen_exact_global.add(h)
        seen_exact_session[m.session].add(h)
        sk = sketch(m.data)
        cands = {j for key in sk[:4] for j in buckets[key][-64:]}   # bounded: recent candidates per bucket
        best_g = max((jaccard_est(sk, all_sk[j]) for j in cands), default=0.0)
        best_s = max((jaccard_est(sk, o) for o in per_session[m.session][-200:]), default=0.0)
        if best_g >= threshold:
            res["near_global"] += 1
            res["near_global_bytes"] += len(m.data)
        if best_s >= threshold:
            res["near_session"] += 1
            res["near_session_bytes"] += len(m.data)
        idx = len(all_sk)
        all_sk.append(sk)
        for key in sk[:4]:
            buckets[key].append(idx)
        per_session[m.session].append(sk)
    return {k: (res[k] / res["n"] if not k.endswith("bytes") else res[k] / max(1, res["bytes"]))
            for k in ["exact_session", "exact_global", "near_session", "near_global",
                      "exact_session_bytes", "exact_global_bytes", "near_session_bytes", "near_global_bytes"]} | {"messages_considered": res["n"]}


all_sk: list[list[int]] = []


def resend_share(msgs):
    """For each message, the share of its bytes that are a prefix+suffix copy of
    the previous message in the same session (whole-context re-send)."""
    copied = total = 0
    shares = []
    for sess in sessions(msgs):
        prev = b""
        for m in sess:
            p = common_prefix(prev, m.data)
            s = common_suffix(prev, m.data, min(len(prev), len(m.data)) - p)
            copied += p + s
            total += len(m.data)
            if prev:
                shares.append((p + s) / max(1, len(m.data)))
            prev = m.data
    return {"copied_from_previous_share": copied / max(1, total),
            "per_message_share_p50": pct(shares, 50), "per_message_share_p10": pct(shares, 10)}


def chunk_dedup(msgs, avg=1024):
    """Share of bytes in FastCDC chunks already seen earlier (session / corpus)."""
    cdc = FastCDC(avg)
    g, sess_seen = set(), defaultdict(set)
    tot = dup_g = dup_s = 0
    for m in msgs:
        for c in cdc.chunks(m.data):
            h = hashlib.blake2b(c, digest_size=16).digest()
            tot += len(c)
            if h in g:
                dup_g += len(c)
            if h in sess_seen[m.session]:
                dup_s += len(c)
            g.add(h)
            sess_seen[m.session].add(h)
    return {"chunk_avg": avg, "dup_bytes_within_session": dup_s / max(1, tot), "dup_bytes_anywhere": dup_g / max(1, tot)}


def json_redundancy(msgs):
    key_bytes = punct_bytes = total = parsed = 0
    schemas, paths = Counter(), set()

    def walk(v, path, acc):
        nonlocal key_bytes, punct_bytes
        if isinstance(v, dict):
            punct_bytes += 2 + max(0, len(v) - 1)
            for k, x in v.items():
                key_bytes += len(json.dumps(k, ensure_ascii=False).encode()) + 1
                p = path + "." + k
                paths.add(p)
                acc.append(p)
                walk(x, p, acc)
        elif isinstance(v, list):
            punct_bytes += 2 + max(0, len(v) - 1)
            for x in v:
                walk(x, path + "[]", acc)
        elif isinstance(v, str):
            punct_bytes += 2

    for m in msgs:
        try:
            obj = json.loads(m.data)
        except ValueError:
            continue
        if not isinstance(obj, (dict, list)):
            continue
        parsed += 1
        total += len(m.data)
        acc: list[str] = []
        walk(obj, "", acc)
        schemas[hashlib.blake2b("\n".join(sorted(set(acc))).encode(), digest_size=8).digest()] += 1
    if not parsed:
        return None
    top = sum(c for _, c in schemas.most_common(10))
    return {"json_messages": parsed, "json_share": parsed / len(msgs), "key_bytes_share": key_bytes / total,
            "punctuation_bytes_share": punct_bytes / total, "distinct_key_paths": len(paths),
            "distinct_schemas": len(schemas), "top10_schema_message_share": top / parsed}


def main():
    report = {}
    for name in NAMES:
        global all_sk
        all_sk = []
        msgs = load(name)
        sizes = [len(m.data) for m in msgs]
        per_session = [sum(len(m.data) for m in s) for s in sessions(msgs)]
        kinds = defaultdict(list)
        for m in msgs:
            kinds[m.kind].append(m.data)
        total = sum(sizes)
        kind_stats = {}
        for k, items in sorted(kinds.items(), key=lambda kv: -sum(map(len, kv[1]))):
            blob = b"".join(items)
            sample = blob[:8 << 20]
            kind_stats[k] = {"messages": len(items), "bytes_share": len(blob) / total,
                             "p50_bytes": pct([len(x) for x in items], 50),
                             "entropy0_bits_per_byte": round(entropy0(sample), 3),
                             "zstd19_long_bits_per_byte": round(zstd_bpb(sample), 3),
                             "zstd3_one_msg_bits_per_byte": round(8 * sum(len(Zstd(3).compress(x)) for x in items[:3000]) /
                                                                  max(1, sum(len(x) for x in items[:3000])), 3)}
        report[name] = {
            "messages": len(msgs), "sessions": len(per_session), "bytes": total,
            "message_bytes": {p: pct(sizes, p) for p in (10, 50, 90, 99)} | {"max": max(sizes), "mean": total / len(sizes)},
            "session_bytes": {p: pct(per_session, p) for p in (10, 50, 90)} | {"max": max(per_session)},
            "share_messages_under_256B": sum(1 for s in sizes if s < 256) / len(sizes),
            "kinds": kind_stats,
            "resend": resend_share(msgs),
            "chunk_dedup": chunk_dedup(msgs),
            "near_duplicates": near_dups(msgs),
            "json": json_redundancy(msgs),
        }
        print(name, flush=True); print(json.dumps({k: report[name][k] for k in ("resend", "chunk_dedup", "json")}), flush=True)
    write_json("characterisation.json", report)
    write_md(report)


def write_md(rep):
    L = ["# Workload characterisation", "",
         "Generated by `research/characterise.py` from the corpora built by `research/build_corpora.py` "
         "(manifest: `corpora-manifest.json`). Byte figures are UTF-8 message payloads.", "",
         "## Sizes", "", "| Corpus | Sessions | Messages | MiB | msg p50 | msg p90 | msg p99 | msg max | <256 B | session p50 |",
         "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for n, r in rep.items():
        mb = r["message_bytes"]
        L.append(f"| {n} | {r['sessions']} | {r['messages']:,} | {r['bytes'] / 2**20:.1f} | {mb[50]:,.0f} | {mb[90]:,.0f} | "
                 f"{mb[99]:,.0f} | {mb['max']:,} | {r['share_messages_under_256B']:.0%} | {r['session_bytes'][50]:,.0f} |")
    L += ["", "## Repetition", "",
          "*Re-send*: bytes of each message that are a prefix+suffix copy of the previous message in the session. "
          "*Chunk dup*: bytes in 1 KiB FastCDC chunks seen earlier. *Near-dup*: messages >= 64 B whose 5-word-shingle "
          "Jaccard estimate to an earlier message is >= 0.8 (bytes-weighted).", "",
          "| Corpus | Re-send share | Chunk dup (session) | Chunk dup (corpus) | Exact dup msgs (corpus) | Near-dup bytes (session) | Near-dup bytes (corpus) |",
          "|---|---:|---:|---:|---:|---:|---:|"]
    for n, r in rep.items():
        d, c, rs = r["near_duplicates"], r["chunk_dedup"], r["resend"]
        L.append(f"| {n} | {rs['copied_from_previous_share']:.1%} | {c['dup_bytes_within_session']:.1%} | "
                 f"{c['dup_bytes_anywhere']:.1%} | {d['exact_global']:.1%} | {d['near_session_bytes']:.1%} | {d['near_global_bytes']:.1%} |")
    L += ["", "## JSON key and schema redundancy", "",
          "| Corpus | JSON msgs | Key bytes | Punctuation bytes | Distinct key paths | Distinct schemas | Top-10 schemas cover |",
          "|---|---:|---:|---:|---:|---:|---:|"]
    for n, r in rep.items():
        j = r["json"]
        if j:
            L.append(f"| {n} | {j['json_share']:.0%} | {j['key_bytes_share']:.1%} | {j['punctuation_bytes_share']:.1%} | "
                     f"{j['distinct_key_paths']} | {j['distinct_schemas']} | {j['top10_schema_message_share']:.0%} |")
    L += ["", "## Per-kind entropy", "",
          "Order-0 entropy ignores context; zstd-19 --long over the concatenated kind is a practical upper bound on "
          "what a strong LZ coder reaches with full history; zstd-3 per message is what independent frames get.", "",
          "| Corpus | Kind | Msgs | Byte share | p50 B | H0 bits/B | zstd-19 long bits/B | zstd-3 per-msg bits/B |",
          "|---|---|---:|---:|---:|---:|---:|---:|"]
    for n, r in rep.items():
        for k, s in list(r["kinds"].items())[:12]:
            L.append(f"| {n} | {k} | {s['messages']:,} | {s['bytes_share']:.1%} | {s['p50_bytes']:,.0f} | "
                     f"{s['entropy0_bits_per_byte']:.2f} | {s['zstd19_long_bits_per_byte']:.2f} | {s['zstd3_one_msg_bits_per_byte']:.2f} |")
    (RESULTS / "characterisation.md").write_text("\n".join(L) + "\n")


if __name__ == "__main__":
    main()
