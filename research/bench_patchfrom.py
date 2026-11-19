"""Per-request independent frames using the previous request as a zstd prefix.

This is what `zstd --patch-from` (or RFC 9842 `dcz` with the previous request
body as the dictionary) gives a stateless HTTP API: each request is one standard
zstd frame; the server needs only the previous request body, which it already
holds for prompt caching. Each frame is decoded with the prefix and verified.
"""
from __future__ import annotations

from bench_longctx import build
from bench_transport import split
from common import load, machine, write_json
from agentwire.zstd import compress_advanced, decompress_advanced


def run(reqs, level, ldm, use_prefix=True):
    prev, wire = b"", 0
    for b in reqs:
        f = compress_advanced(b, level, long_distance=ldm, prefix=prev if (use_prefix and prev) else None)
        assert decompress_advanced(f, len(b), prefix=prev if (use_prefix and prev) else None) == b
        wire += len(f) + 40          # + a dcz-style 40-byte header (magic + SHA-256 of the dictionary)
        prev = b
    return wire


def main():
    rows = []
    _, te = split(load("api-requests"))
    sets = {"api-requests (held-out)": [[m.data for m in s] for s in te]}
    for t in (1 << 20, 6 << 20):
        sets[f"long context {t >> 20} MiB"] = [build(t, seed=t)]
    for name, sess in sets.items():
        raw = sum(len(b) for s in sess for b in s)
        row = {"set": name, "raw_bytes": raw}
        for label, lvl, ldm, pre in [("no prefix, zstd-3", 3, False, False), ("prefix, zstd-3", 3, False, True),
                                     ("prefix, zstd-3 + LDM", 3, True, True), ("prefix, zstd-1 + LDM", 1, True, True),
                                     ("prefix, zstd-9 + LDM", 9, True, True)]:
            row[label] = raw / sum(run(s, lvl, ldm, pre) for s in sess)
        rows.append(row)
        print(row, flush=True)
    write_json("patchfrom.json", {"machine": machine(), "rows": rows})


if __name__ == "__main__":
    main()
