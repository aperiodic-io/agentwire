"""Shared helpers for the research scripts: corpus loading, timing, machine info."""
from __future__ import annotations

import json
import os
import platform
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
CORPORA = ROOT / "corpora"
RESULTS = ROOT / "results" / "research"
NAMES = ["transcripts-cc", "transcripts-oh", "api-requests", "tool-outputs", "ci-logs", "collab", "sse"]


@dataclass
class Msg:
    session: int
    t_ms: int
    kind: str
    data: bytes


def load(name: str) -> list[Msg]:
    out = []
    with (CORPORA / f"{name}.jsonl").open(encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            out.append(Msg(r["s"], r["t"], r["k"], r["b"].encode("utf-8", "surrogatepass")))
    return out


def sessions(msgs: list[Msg]) -> list[list[Msg]]:
    by = defaultdict(list)
    for m in msgs:
        by[m.session].append(m)
    return [by[k] for k in sorted(by)]


def pct(values, p):
    if not values:
        return 0
    v = sorted(values)
    k = (len(v) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (k - lo)


def median_of(fn, repeats=3):
    """Run fn() `repeats` times; return (median seconds, last result)."""
    times, result = [], None
    for _ in range(repeats):
        t = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t)
    return statistics.median(times), result


def rss_bytes() -> int:
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")


def machine() -> dict:
    info = {"python": sys.version.split()[0], "platform": platform.platform(),
            "cpu_count": os.cpu_count()}
    try:
        for line in open("/proc/cpuinfo"):
            if line.startswith("model name"):
                info["cpu"] = line.split(":", 1)[1].strip()
                break
        info["mem_gib"] = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1)
    except (OSError, ValueError):
        pass
    import ctypes
    import ctypes.util
    z = ctypes.CDLL(ctypes.util.find_library("zstd"))
    z.ZSTD_versionString.restype = ctypes.c_char_p
    info["libzstd"] = z.ZSTD_versionString().decode()
    import zlib
    info["zlib"] = zlib.ZLIB_RUNTIME_VERSION
    try:
        b = ctypes.CDLL("libbrotlienc.so.1")
        v = b.BrotliEncoderVersion()
        info["libbrotli"] = f"{v >> 24}.{(v >> 12) & 0xFFF}.{v & 0xFFF}"
    except OSError:
        pass
    try:
        l4 = ctypes.CDLL("liblz4.so.1")
        l4.LZ4_versionString.restype = ctypes.c_char_p
        info["liblz4"] = l4.LZ4_versionString().decode()
    except OSError:
        pass
    import lzma
    info["liblzma"] = getattr(lzma, "__version__", "stdlib")
    try:
        info["git_commit"] = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
                                                     text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        pass
    return info


def write_json(name: str, obj):
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / name).write_text(json.dumps(obj, indent=2))
