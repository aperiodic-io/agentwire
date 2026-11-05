"""Benchmark AWF1/AWC1 on real agent session logs stored on this machine.

Sources (both optional): Claude Code transcripts (~/.claude/projects/**/*.jsonl)
and Codex sessions (~/.codex/sessions/**/*.jsonl). Each session file is treated
as one connection, each JSONL line as one message (exact bytes), replayed with
its recorded timestamp.

Privacy: everything runs locally. Only aggregate numbers are written. A trained
dictionary contains fragments of the transcripts, so it is kept in memory unless
--save-dictionary-dir is given; never commit or publish it.

Leakage control: sessions are ordered by first timestamp; the dictionary for
each corpus is trained only on the oldest --train-fraction of sessions and every
metric is measured on the newer remainder.
"""
from __future__ import annotations

import json
import os
import random
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from .benchmark import emit_frames_at
from .stream import StreamEncoder, StreamSessionDecoder
from .transport import DictionaryRegistry, FrameCodec, Message, StreamDecoder
from .zstd import Zstd, train_dictionary

LIMIT = 64 * 1024 * 1024          # real records reach several MiB
SAMPLE_CAP = 32 * 1024            # bytes of each record used for dictionary training
TRAIN_BUDGET = 48 * 1024 * 1024   # total training bytes per corpus

VARIANTS = [
    # name, description
    ("awf1/one", "AWF1, one independent frame per message"),
    ("awf1/one+dict", "AWF1, one frame per message, dictionary"),
    ("awf1/batch", "AWF1, 25 ms / 32 msg / 64 KiB batches at real timing"),
    ("awf1/batch+dict", "AWF1 batches + dictionary"),
    ("awc1/stream", "AWC1 stream, window 1 MiB"),
    ("awc1/stream+dict", "AWC1 stream + dictionary, window 1 MiB"),
    ("awc1/stream+dict/w8M", "AWC1 stream + dictionary, window 8 MiB"),
    ("ref/whole-file", "Reference: whole session file in one Zstd-3 frame (offline, no framing)"),
]


@dataclass
class Session:
    corpus: str
    path: Path
    start_ms: int


@dataclass
class Totals:
    raw: int = 0
    wire: int = 0
    frames: int = 0
    messages: int = 0
    encode_s: float = 0.0
    waits: list[int] = field(default_factory=list)


def _ts_ms(record: dict) -> int | None:
    ts = record.get("timestamp")
    if not isinstance(ts, str):
        return None
    try:
        return int(datetime.fromisoformat(ts).timestamp() * 1000)
    except ValueError:
        return None


def _load(path: Path) -> tuple[list[Message], list[int]]:
    """Exact line bytes plus monotonic arrival times (ms from session start)."""
    lines = path.read_bytes().splitlines(keepends=True)
    messages, times = [], []
    origin = last = None
    for i, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            t = _ts_ms(json.loads(line))
        except (ValueError, UnicodeDecodeError):
            t = None
        if t is not None and origin is None:
            origin = t
        now = 0 if t is None or origin is None else t - origin
        last = now if last is None else max(last, now)   # records without/behind time inherit
        messages.append(Message(i, line))
        times.append(last)
    return messages, times


def _first_ts(path: Path) -> int | None:
    with path.open("rb") as f:
        for line in f:
            try:
                t = _ts_ms(json.loads(line))
            except (ValueError, UnicodeDecodeError):
                continue
            if t is not None:
                return t
    return None


def discover(sources: dict[str, Path], exclude_active_minutes: float) -> list[Session]:
    cutoff = time.time() - exclude_active_minutes * 60
    sessions = []
    for corpus, root in sources.items():
        if not root.is_dir():
            continue
        for path in root.rglob("*.jsonl"):
            if path.stat().st_mtime > cutoff or path.stat().st_size == 0:
                continue  # skip sessions still being written (including this one)
            start = _first_ts(path)
            if start is not None:
                sessions.append(Session(corpus, path, start))
    return sorted(sessions, key=lambda s: (s.corpus, s.start_ms))


def _train(sessions: list[Session], size: int, seed: int) -> bytes:
    samples = []
    for s in sessions:
        samples.extend(m.payload[:SAMPLE_CAP] for m in _load(s.path)[0] if m.payload)
    random.Random(seed).shuffle(samples)
    picked, total = [], 0
    for sample in samples:
        if total >= TRAIN_BUDGET:
            break
        picked.append(sample)
        total += len(sample)
    return train_dictionary(picked, capacity=size)


def _verify(decoder, frames: list[tuple[bytes, int]], messages: list[Message]) -> list[int]:
    out, arrival = [], []
    for frame, ts in frames:
        got = decoder.feed(frame)
        out.extend(got)
        arrival.extend([ts] * len(got))
    decoder.finish()
    if out != messages:
        raise AssertionError("round trip mismatch")
    return arrival


def _run_session(messages, times, registry, version, totals: dict[str, Totals], verify: bool):
    raw = sum(len(m.payload) for m in messages)
    codec = FrameCodec(registry, max_raw_bytes=LIMIT, max_wire_bytes=LIMIT + 64)
    for name, _ in VARIANTS:
        t = totals[name]
        begin = time.perf_counter()
        if name == "ref/whole-file":
            wire = len(Zstd(3).compress(b"".join(m.payload for m in messages)))
            t.encode_s += time.perf_counter() - begin
            t.raw += raw
            t.wire += wire
            t.frames += 1
            t.messages += len(messages)
            continue
        dic = version if "+dict" in name else 0
        if name.startswith("awf1/"):
            frames = emit_frames_at(messages, times, codec, dic, one=name.startswith("awf1/one"))
            new_decoder = lambda: StreamDecoder(codec)
        else:
            window = 23 if name.endswith("/w8M") else 20
            enc = StreamEncoder(registry, dictionary_version=dic, window_log=window,
                                max_raw_bytes=LIMIT)
            frames = [(enc.encode([m]), ts) for m, ts in zip(messages, times)]
            enc.close()
            new_decoder = lambda window=window: StreamSessionDecoder(
                registry, max_raw_bytes=LIMIT, max_wire_bytes=LIMIT + 64, max_window_log=window)
        t.encode_s += time.perf_counter() - begin
        t.raw += raw
        t.wire += sum(len(f) for f, _ in frames)
        t.frames += len(frames)
        t.messages += len(messages)
        if verify:
            arrival = _verify(new_decoder(), frames, messages)
            t.waits.extend(a - s for a, s in zip(arrival, times))


def _pct(values: list[int], p: float) -> int | None:
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int(p * len(values)))]


def run_local_benchmark(output_dir: str | Path, sources: dict[str, Path],
                        train_fraction: float = 0.6, dict_size: int = 65536,
                        max_test_sessions: int | None = None, seed: int = 1,
                        exclude_active_minutes: float = 15.0, verify: bool = True,
                        save_dictionary_dir: str | Path | None = None,
                        progress=print) -> dict:
    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be in (0, 1)")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    sessions = discover(sources, exclude_active_minutes)
    report = {"description": "AgentWire on real local agent session logs (aggregates only)",
              "train_fraction": train_fraction, "dictionary_bytes": dict_size,
              "verified_roundtrip": verify, "corpora": {}}
    for corpus in sorted({s.corpus for s in sessions}):
        group = [s for s in sessions if s.corpus == corpus]
        split = max(1, int(len(group) * train_fraction))
        train, test = group[:split], group[split:]
        if max_test_sessions:
            test = test[:max_test_sessions]
        if not test:
            progress(f"[{corpus}] not enough sessions to split; skipped")
            continue
        progress(f"[{corpus}] training {dict_size}-byte dictionary on {len(train)} older sessions...")
        dictionary = _train(train, dict_size, seed)
        if save_dictionary_dir:
            Path(save_dictionary_dir).mkdir(parents=True, exist_ok=True)
            (Path(save_dictionary_dir) / f"{corpus}-v1.zdict").write_bytes(dictionary)
        registry = DictionaryRegistry()
        registry.register(1, dictionary)
        totals = {name: Totals() for name, _ in VARIANTS}
        sizes, sessions_bytes = [], 0
        for i, s in enumerate(test, 1):
            messages, times = _load(s.path)
            if not messages:
                continue
            sizes.extend(len(m.payload) for m in messages)
            sessions_bytes += sum(len(m.payload) for m in messages)
            _run_session(messages, times, registry, 1, totals, verify)
            if i % 10 == 0 or i == len(test):
                progress(f"[{corpus}] {i}/{len(test)} test sessions, {sessions_bytes / 2**20:.0f} MiB")
        rows = []
        for name, description in VARIANTS:
            t = totals[name]
            rows.append({
                "variant": name, "description": description,
                "raw_bytes": t.raw, "wire_bytes": t.wire,
                "ratio_x": round(t.raw / t.wire, 3) if t.wire else None,
                "frames": t.frames,
                "messages_per_frame": round(t.messages / t.frames, 2) if t.frames else None,
                "wait_p50_ms": _pct(t.waits, .5), "wait_p99_ms": _pct(t.waits, .99),
                "wait_max_ms": max(t.waits) if t.waits else None,
                "encode_mib_s": round(t.raw / 2**20 / t.encode_s, 1) if t.encode_s else None,
            })
        report["corpora"][corpus] = {
            "train_sessions": len(train), "test_sessions": len(test),
            "test_messages": len(sizes), "test_bytes": sessions_bytes,
            "message_bytes_p50": _pct(sizes, .5), "message_bytes_p90": _pct(sizes, .9),
            "message_bytes_p99": _pct(sizes, .99),
            "train_period_end": datetime.fromtimestamp(train[-1].start_ms / 1000).date().isoformat(),
            "test_period": [datetime.fromtimestamp(test[0].start_ms / 1000).date().isoformat(),
                            datetime.fromtimestamp(test[-1].start_ms / 1000).date().isoformat()],
            "variants": rows,
        }
    (output_dir / "local-bench.json").write_text(json.dumps(report, indent=2))
    _write_report(output_dir, report)
    return report


def _write_report(output_dir: Path, report: dict):
    lines = ["# AgentWire on real local agent sessions", "",
             "Each session file = one connection; each JSONL line = one message (exact bytes), replayed at its recorded timestamp. "
             "Dictionaries are trained only on the oldest sessions of each corpus and measured on newer ones. "
             f"Dictionary size: {report['dictionary_bytes']:,} bytes (distribution cost excluded). "
             f"Every frame was decoded and compared byte-for-byte: {report['verified_roundtrip']}.", ""]
    for corpus, c in report["corpora"].items():
        lines += [f"## {corpus}", "",
                  f"- Train: {c['train_sessions']} sessions (up to {c['train_period_end']}); "
                  f"test: {c['test_sessions']} sessions ({c['test_period'][0]} to {c['test_period'][1]}), "
                  f"{c['test_messages']:,} messages, {c['test_bytes'] / 2**20:.1f} MiB",
                  f"- Message size p50 / p90 / p99: {c['message_bytes_p50']:,} / {c['message_bytes_p90']:,} / {c['message_bytes_p99']:,} bytes",
                  "", "| Variant | Ratio | Wire MiB | Msgs/frame | Added wait p50 / p99 / max | Encode MiB/s |",
                  "|---|---:|---:|---:|---:|---:|"]
        for r in c["variants"]:
            wait = ("—" if r["wait_p50_ms"] is None else
                    f'{r["wait_p50_ms"]} / {r["wait_p99_ms"]} / {r["wait_max_ms"]} ms')
            lines.append(f'| {r["variant"]} | {r["ratio_x"]:.2f}× | {r["wire_bytes"] / 2**20:.2f} | '
                         f'{r["messages_per_frame"]} | {wait} | {r["encode_mib_s"]} |')
        lines.append("")
    lines += ["## Caveats", "",
              "- Session logs are a proxy for wire traffic: they are what an agent records, not exactly what it sends to a model or tool API.",
              "- `ref/whole-file` compresses a finished session in one shot. It is an offline reference, not a transport option.",
              "- Encode speed is single-threaded Python + ctypes and includes framing overhead.", ""]
    (output_dir / "RESULTS.md").write_text("\n".join(lines))


def default_sources() -> dict[str, Path]:
    home = Path(os.path.expanduser("~"))
    return {"claude-code": home / ".claude" / "projects", "codex": home / ".codex" / "sessions"}
