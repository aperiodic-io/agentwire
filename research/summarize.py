"""Collect results/research/*.json into results/research/SUMMARY.md (+ cost model)."""
from __future__ import annotations

import json

from common import RESULTS

PRICE = {"s3_standard_gb_month": 0.023, "deep_archive_gb_month": 0.00099, "egress_gb": 0.09,
         "vcpu_hour": 0.04}   # AWS us-east-1 list prices retrieved 2026-11-19; vCPU-hour is an assumption


def j(name):
    p = RESULTS / name
    return json.loads(p.read_text()) if p.exists() else None


def f(x, fmt="{:.2f}"):
    return "–" if x is None else fmt.format(x)


TRANSPORT_COLS = ["awf1/one", "awf1/one+dict", "awf1/batch25ms+dict", "awc1/w20", "awc1/w20+dict", "awc1/w23+dict",
                  "deflate6-stream", "brotli5-stream/w22", "brotli5-stream/w22+dict", "lz4-stream",
                  "awd1/w22", "awd1/w22+dict", "ref/offline-zstd19-long"]


def transport(L, t):
    L += ["## Live transport (one connection per session, every message sent immediately)", "",
          f"Held-out sessions only; dictionaries (64 KiB) trained on the older 60%. Ratio = payload bytes / wire bytes "
          f"including all framing. Medians of {t['repeats']} runs. Throughput and latency are Python + ctypes and "
          "dominated by per-call overhead; compare them relative to each other, not as native numbers.", "",
          "| Variant | " + " | ".join(t["corpora"]) + " |", "|---|" + "---:|" * len(t["corpora"])]
    by = {c: {r["variant"]: r for r in v["rows"]} for c, v in t["corpora"].items()}
    for col in TRANSPORT_COLS:
        cells = [f"{by[c][col]['ratio']:.2f}×" if col in by[c] else "–" for c in t["corpora"]]
        L.append(f"| {col} | " + " | ".join(cells) + " |")
    L += ["", "Encode MB/s and p99 per-message latency (encode + decode) on transcripts-cc:", "",
          "| Variant | Encode MB/s | Decode MB/s | p50 µs | p99 µs |", "|---|---:|---:|---:|---:|"]
    ref = "transcripts-cc" if "transcripts-cc" in by else next(iter(by))
    for col in TRANSPORT_COLS:
        r = by[ref].get(col)
        if r:
            L.append(f"| {col} | {r['encode_MBps']:.1f} | {f(r['decode_MBps'], '{:.1f}')} | {r['latency_us_p50']:.0f} | {r['latency_us_p99']:.0f} |")
    mem = j("memory.json")
    if mem:
        L += ["", f"Memory per live connection (sender + receiver), measured in a fresh process with "
              f"{mem['connections_per_process']} connections each fed 4 MiB (`memory.json`). libzstd's own accounting "
              "counts allocated buffers; RSS counts touched pages:", "",
              "| Variant | RSS KiB per connection | libzstd KiB per connection |", "|---|---:|---:|"]
        for r in mem["rows"]:
            L.append(f"| {r['variant']} | {r['rss_per_connection'] / 1024:,.0f} | {f(r['libzstd_bytes_per_connection'] and r['libzstd_bytes_per_connection'] / 1024, '{:,.0f}')} |")
    L.append("")


def storage(L, s):
    L += ["## Session-log storage and archive", "",
          "Each held-out session file compressed on its own unless marked *corpus*. Compress/decompress MB/s are "
          "medians of 3 runs over whole files (one ctypes call per file, so close to native speed).", ""]
    names = list(s["corpora"])
    by = {c: {r["variant"]: r for r in v["rows"]} for c, v in s["corpora"].items()}
    keys = [k for k in by[names[0]]]
    L += ["| Variant | " + " | ".join(names) + " | c MB/s | d MB/s |", "|---|" + "---:|" * (len(names) + 2)]
    ref = "transcripts-cc" if "transcripts-cc" in by else names[0]
    for k in keys:
        cells = [f"{by[c][k]['ratio']:.2f}×" if k in by[c] else "–" for c in names]
        r = by[ref].get(k, {})
        L.append(f"| {k} | " + " | ".join(cells) + f" | {f(r.get('compress_MBps'), '{:.0f}')} | {f(r.get('decompress_MBps'), '{:.0f}')} |")
    L += ["", "### Random access (fetch one random message)", "",
          "| Corpus | Archive | Ratio | µs per fetch | bytes decompressed per fetch |", "|---|---|---:|---:|---:|"]
    for c, v in s["corpora"].items():
        for r in v["random_access"]:
            L.append(f"| {c} | {r['variant']} | {r['ratio']:.2f}× | {r['random_fetch_us']:,.0f} | {r['decompressed_per_fetch_bytes']:,.0f} |")
    L.append("")


def sse(L, x):
    L += ["## Streaming responses (SSE)", "",
          f"{x['test_streams']} held-out streams, {x['events']:,} events, mean event {x['mean_event_bytes']:.0f} B, "
          f"mean gap {x['mean_event_gap_ms']:.0f} ms. Body bytes exclude transfer framing.", "",
          "| Variant | Ratio | Bytes/event | codec p50 µs | codec p99 µs | added wait p99 ms |", "|---|---:|---:|---:|---:|---:|"]
    for r in x["rows"]:
        L.append(f"| {r['variant']} | {r['ratio']:.2f}× | {r['bytes_per_event']:.1f} | {r['codec_us_p50']:.1f} | "
                 f"{r['codec_us_p99']:.1f} | {r['added_wait_ms_p99']:.0f} |")
    L.append("")


def dicts(L, d):
    L += ["## Dictionary lifecycle", "", "Ratios on held-out sessions; *first5* = only the first five messages of each session.", "",
          "| Test corpus | Dictionary | AWF1 one-frame | AWC1 stream | AWC1 first 5 msgs |", "|---|---|---:|---:|---:|"]
    for r in d["rows"]:
        L.append(f"| {r['test']} | {r['dictionary']} | {r['awf1_one_ratio']:.2f}× | {r['awc1_ratio']:.2f}× | {r['awc1_first5_ratio']:.2f}× |")
    L += ["", "Staleness (64 KiB dictionary trained on the first 20% of transcripts-cc sessions, evaluated on later quarters):", "",
          "| Slice | AWF1 none | AWF1 dict | AWC1 none | AWC1 dict |", "|---|---:|---:|---:|---:|"]
    for r in d["staleness"]:
        L.append(f"| {r['slice']} | {r['awf1_none']:.2f}× | {r['awf1_dict']:.2f}× | {r['awc1_none']:.2f}× | {r['awc1_dict']:.2f}× |")
    L += ["", "Training time (seconds): " + ", ".join(f"{k} {v:.1f}" for k, v in d["train_seconds"].items()), ""]


def tokens(L, t):
    L += ["## Tokenizer-aware coding (o200k_base token IDs vs UTF-8)", "",
          "Ratios are UTF-8 bytes / compressed bytes of each representation (higher is better).", "",
          "| Corpus | bytes/token | zstd-19 utf8 | zstd-19 ids-u24 | zstd-19 ids-varint | zstd-19 ids-planar | per-msg zstd-3 utf8 | per-msg zstd-3 ids | tokenize MB/s |",
          "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in t["rows"]:
        L.append(f"| {r['corpus']} | {r['bytes_per_token']:.2f} | {r['utf8_zstd19_ratio_vs_utf8']:.2f}× | {r['ids-u24_zstd19_ratio_vs_utf8']:.2f}× | "
                 f"{r['ids-varint_zstd19_ratio_vs_utf8']:.2f}× | {r['ids-u24-planar_zstd19_ratio_vs_utf8']:.2f}× | "
                 f"{r['per_message_zstd3_ratio_utf8']:.2f}× | {r['per_message_zstd3_ratio_ids_u24']:.2f}× | {r['tokenize_MBps']:.1f} |")
    L.append("")


def reducer(L, r):
    L += ["## Opt-in lossy-but-recoverable layer (observation masking on re-sent context)", "",
          "| Keep last N model turns intact | Request bytes | Reduced | Context reduction | AWD1 wire, original | AWD1 wire, reduced | Archive bytes |",
          "|---:|---:|---:|---:|---:|---:|---:|"]
    for x in r["rows"]:
        L.append(f"| {x['keep_last_model_turns']} | {x['request_bytes']:,} | {x['reduced_request_bytes']:,} | {x['context_reduction_x']:.2f}× | "
                 f"{x['awd1_wire_original']:,} | {x['awd1_wire_reduced']:,} | {x['archive_bytes']:,} |")
    L.append("")


def multiplex(L, m):
    L += ["## Gateway multiplexing (sessions interleaved on one connection)", "",
          "| Corpus | Sessions | AWC1 per session | AWC1 shared w20 | AWC1 shared w23 | AWF1 shared 25 ms batches | msgs/frame |",
          "|---|---:|---:|---:|---:|---:|---:|"]
    for r in m["rows"]:
        L.append(f"| {r['corpus']} | {r['sessions']} | {r['awc1_per_session_w20+dict']:.2f}× | {r['awc1_shared_w20+dict']:.2f}× | "
                 f"{r['awc1_shared_w23+dict']:.2f}× | {r['awf1_shared_batch25ms+dict']:.2f}× | {r['awf1_shared_msgs_per_frame']:.2f} |")
    L.append("")


def longctx(L, x):
    keys = [k for k in x["rows"][0] if isinstance(x["rows"][0][k], dict)]
    L += ["## Long contexts (re-sent context outgrowing the window)", "",
          "Real OpenHands runs chained into one conversation; one full request body per turn; each variant on its own connection.", "",
          "| Final context | Requests | Raw MB | " + " | ".join(keys) + " |", "|---:|---:|---:|" + "---:|" * len(keys)]
    for r in x["rows"]:
        L.append(f"| {r['final_context_bytes'] / 2**20:.2f} MiB | {r['requests']} | {r['raw_bytes'] / 1e6:,.0f} | " +
                 " | ".join(f"{r[k]['ratio']:,.0f}×" for k in keys) + " |")
    L.append("")


def patchfrom(L, x):
    keys = [k for k in x["rows"][0] if k not in ("set", "raw_bytes")]
    L += ["## Per-request frames with the previous request as prefix (`--patch-from` / dcz-style)", "",
          "Each request is an independent standard zstd frame; +40 B per request for a dcz-style header.", "",
          "| Set | " + " | ".join(keys) + " |", "|---|" + "---:|" * len(keys)]
    for r in x["rows"]:
        L.append(f"| {r['set']} | " + " | ".join(f"{r[k]:,.1f}×" for k in keys) + " |")
    L.append("")


def costs(L, t, s):
    """$ per month per 1 TB/month of raw traffic (egress) and per 12 TB retained (storage)."""
    L += ["## Cost model", "",
          f"Assumptions: AWS us-east-1 list prices on 2026-11-19: egress ${PRICE['egress_gb']}/GB (first 10 TB tier), "
          f"S3 Standard ${PRICE['s3_standard_gb_month']}/GB-month, Glacier Deep Archive ${PRICE['deep_archive_gb_month']}/GB-month; "
          "1 TB of raw traffic per month; 12 months of logs retained (12 TB raw); compression CPU at "
          f"${PRICE['vcpu_hour']}/vCPU-hour (an assumption) and the single-core compress speed measured here. Each row applies its "
          "measured ratio to both columns, so read *wire* rows for egress and *store* rows for storage. Wire-row CPU uses the "
          "Python per-message speeds and so overstates native cost.", "",
          "| Corpus | Approach | Ratio | Egress $/month | S3 Standard $/month | Deep Archive $/month | Compress CPU $/TB |",
          "|---|---|---:|---:|---:|---:|---:|"]
    for c in ["transcripts-cc", "transcripts-oh", "api-requests", "tool-outputs", "collab"]:
        rows = []
        if t and c in t["corpora"]:
            by = {r["variant"]: r for r in t["corpora"][c]["rows"]}
            for k in ["awf1/one", "awc1/w20+dict", "brotli5-stream/w22+dict", "awd1/w22+dict"]:
                if k in by:
                    rows.append(("wire: " + k, by[k]["ratio"], by[k]["encode_MBps"]))
        if s and c in s["corpora"]:
            by = {r["variant"]: r for r in s["corpora"][c]["rows"]}
            for k in ["gzip-6", "zstd-3", "zstd-19-long-w27", "zstd-19+dict64k", "xz-9e", "brotli-11-w24",
                      "corpus: zstd-19-long-w27", "corpus: xz-9e"]:
                if k in by:
                    rows.append(("store: " + k, by[k]["ratio"], by[k]["compress_MBps"]))
        L.append(f"| {c} | raw (identity) | 1.00× | {1000 * PRICE['egress_gb']:,.2f} | {12000 * PRICE['s3_standard_gb_month']:,.2f} | {12000 * PRICE['deep_archive_gb_month']:,.2f} | 0.00 |")
        for name, ratio, mbps in rows:
            cpu = 1e6 / mbps / 3600 * PRICE["vcpu_hour"] if mbps else None
            L.append(f"| {c} | {name} | {ratio:.2f}× | {1000 / ratio * PRICE['egress_gb']:,.2f} | "
                     f"{12000 / ratio * PRICE['s3_standard_gb_month']:,.2f} | {12000 / ratio * PRICE['deep_archive_gb_month']:,.2f} | {f(cpu)} |")
    L.append("")


def main():
    t, s = j("transport.json"), j("storage.json")
    any_m = next((x for x in (t, s, j("sse.json"), j("dictionaries.json")) if x), None)
    L = ["# Research results summary", "",
         "Generated by `research/summarize.py`. Corpora: `corpora-manifest.json`; workload structure: `characterisation.md`; "
         "analysis and recommendations: `reports/compression-research.md`.", ""]
    if any_m:
        m = any_m["machine"]
        L += ["Machine: " + ", ".join(f"{k}={v}" for k, v in m.items()), ""]
    for name, fn in [("transport.json", transport), ("storage.json", storage), ("sse.json", sse),
                     ("dictionaries.json", dicts), ("multiplex.json", multiplex), ("tokens.json", tokens),
                     ("reducer.json", reducer), ("longctx.json", longctx), ("patchfrom.json", patchfrom)]:
        data = j(name)
        if data:
            fn(L, data)
    costs(L, t, s)
    (RESULTS / "SUMMARY.md").write_text("\n".join(L) + "\n")
    print(RESULTS / "SUMMARY.md")


if __name__ == "__main__":
    main()
