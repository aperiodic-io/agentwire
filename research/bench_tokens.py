"""Tokenizer-aware coding: does storing token IDs beat compressing the UTF-8 text?

Optional research-only dependency: tiktoken (o200k_base). Byte-level BPE is
lossless (decode(encode(x)) == x), verified for every message here.
Representations, each then compressed with the same zstd settings:
  utf8        the text bytes
  ids-u24     token ids as fixed 3-byte little-endian integers
  ids-varint  token ids as uleb128
  ids-u24-planar  the 3 bytes of each id split into three planes (lo/mid/hi)
"""
from __future__ import annotations

import json
import time

from common import load, machine, write_json
from agentwire.transport import _varint
from agentwire.zstd import compress_advanced


def texts(name, limit_bytes=8 << 20):
    out, total = [], 0
    for m in load(name):
        try:
            obj = json.loads(m.data)
        except ValueError:
            obj = None
        if name == "transcripts-cc" and obj:
            c = obj["message"]["content"]
            t = c if isinstance(c, str) else "\n".join(x.get("text") or x.get("content") or json.dumps(x.get("input", "")) for x in c)
        elif name == "transcripts-oh" and obj:
            t = obj["message"].get("content") or ""
        else:
            t = m.data.decode("utf-8", "surrogatepass")
        if t:
            out.append(t)
            total += len(t)
        if total > limit_bytes:
            break
    return out


def main():
    import tiktoken
    enc = tiktoken.get_encoding("o200k_base")
    rows = []
    for name in ["transcripts-cc", "transcripts-oh", "tool-outputs", "ci-logs"]:
        tx = texts(name)
        utf8 = [t.encode("utf-8", "surrogatepass") for t in tx]
        t0 = time.perf_counter()
        ids = [enc.encode(t, disallowed_special=()) for t in tx]
        tok_s = time.perf_counter() - t0
        t0 = time.perf_counter()
        assert all(enc.decode_bytes(i) == u for i, u in zip(ids, utf8))
        detok_s = time.perf_counter() - t0
        ntok = sum(map(len, ids))
        raw = sum(map(len, utf8))
        reps = {"utf8": b"".join(utf8),
                "ids-u24": b"".join(x.to_bytes(3, "little") for i in ids for x in i),
                "ids-varint": b"".join(_varint(x) for i in ids for x in i)}
        u24 = reps["ids-u24"]
        reps["ids-u24-planar"] = u24[0::3] + u24[1::3] + u24[2::3]
        row = {"corpus": name, "utf8_bytes": raw, "tokens": ntok, "bytes_per_token": raw / ntok,
               "tokenize_MBps": raw / 1e6 / tok_s, "detokenize_MBps": raw / 1e6 / detok_s}
        for k, blob in reps.items():
            row[f"{k}_raw_bytes"] = len(blob)
            for lvl in (3, 19):
                row[f"{k}_zstd{lvl}_ratio_vs_utf8"] = raw / len(compress_advanced(blob, lvl, 27, True))
        row["per_message_zstd3_ratio_utf8"] = sum(map(len, utf8[:4000])) / sum(len(compress_advanced(x, 3)) for x in utf8[:4000])
        row["per_message_zstd3_ratio_ids_u24"] = sum(map(len, utf8[:4000])) / sum(
            len(compress_advanced(b"".join(x.to_bytes(3, "little") for x in i), 3)) for i in ids[:4000])
        rows.append(row)
        print(json.dumps({k: (round(v, 3) if isinstance(v, float) else v) for k, v in row.items()}), flush=True)
    write_json("tokens.json", {"machine": machine(), "tokenizer": "tiktoken o200k_base", "rows": rows})


if __name__ == "__main__":
    main()
