"""Repeatable synthetic microbenchmarks; measures framed wire size, not just Zstd."""
from __future__ import annotations

import csv
import json
import statistics
import time
from dataclasses import asdict
from pathlib import Path

from .context import ContentAddressedStore, ContextReducer, encode_json, rehydrate, size_events
from .fixtures import context_events, training_samples, workload
from .stream import StreamEncoder, StreamSessionDecoder
from .transport import (BoundedBatcher, DictionaryRegistry, FrameCodec, Message, StreamDecoder,
                        _record_size)
from .zstd import train_dictionary

CONFIGS = [
    ("raw/one", "raw", True, False, 1, 0),
    ("zstd3/one", "zstd", True, False, 1, 0),
    ("zstd3+dict/one", "dict", True, True, 1, 0),
    ("zstd3/batch-8k", "zstd", False, False, 16, 8192),
    ("zstd3/batch-64k", "zstd", False, False, 32, 65536),
    ("zstd3+dict/batch-64k", "dict", False, True, 32, 65536),
]
# AWC1: one Zstd stream per connection, one frame per message, zero added delay.
STREAM_CONFIGS = [("zstd3/stream", False), ("zstd3+dict/stream", True)]
# Realistic single-session agent traffic: model calls take seconds, so a 25 ms
# batching window almost never holds more than one message.
SLOW_GAP_MS = 1000


def emit_frames(messages: list[Message], interval_ms: int, codec: FrameCodec,
                level_name: str, one: bool, has_dict: bool,
                max_messages: int, max_bytes: int):
    """Returns list of (frame bytes, synthetic emission timestamp)."""
    return emit_frames_at(messages, [i * interval_ms for i in range(len(messages))], codec,
                          1 if has_dict else 0, one, max_messages, max_bytes)


def emit_frames_at(messages: list[Message], times_ms: list[int], codec: FrameCodec,
                   dictionary_version: int, one: bool, max_messages: int = 32,
                   max_bytes: int = 65536, max_delay_ms: int = 25):
    """AWF1 frames for messages arriving at monotonic times_ms.

    Simulates AsyncBatchedSender's timer: a batch is flushed at its deadline,
    not at the next arrival. A message larger than the batch byte budget is
    sent alone in its own frame (after flushing whatever is pending).
    Returns list of (frame bytes, emission timestamp).
    """
    batcher = BoundedBatcher(codec, dictionary_version=dictionary_version,
                             max_messages=max_messages,
                             max_batch_bytes=(max_bytes if not one else codec.max_raw_bytes),
                             max_delay_ms=(max_delay_ms if not one else 0))
    frames: list[tuple[bytes, int]] = []
    for message, now in zip(messages, times_ms):
        # Simulate the sender's timer: fire at the deadline, not at the next arrival.
        if batcher._since_ms is not None and batcher._since_ms + batcher.max_delay_ms <= now:
            deadline = batcher._since_ms + batcher.max_delay_ms
            for frame in batcher.poll(deadline):
                frames.append((frame, deadline))
        if not one and _record_size(message) > batcher.max_batch_bytes:
            if (frame := batcher.flush()) is not None:
                frames.append((frame, now))
            frames.append((codec.encode([message], batcher.next_sequence, dictionary_version), now))
            batcher.next_sequence += 1
            continue
        for frame in batcher.add(message, now):
            frames.append((frame, now))
        if one:
            frame = batcher.flush()
            if frame is not None:
                frames.append((frame, now))
    last = times_ms[-1] if times_ms else 0
    if batcher._since_ms is not None:
        last = max(last, batcher._since_ms + batcher.max_delay_ms)  # timer fires for the tail
    for frame in batcher.poll(last):
        frames.append((frame, last))
    frame = batcher.flush()
    if frame is not None:
        frames.append((frame, last))
    return frames


def emit_stream(messages: list[Message], interval_ms: int, registry: DictionaryRegistry,
                dictionary_version: int):
    """AWC1: every message becomes its own frame the moment it arrives."""
    encoder = StreamEncoder(registry, dictionary_version=dictionary_version)
    try:
        return [(encoder.encode([m]), i * interval_ms) for i, m in enumerate(messages)]
    finally:
        encoder.close()


def run_benchmark(output_dir: str | Path, n: int = 240, repeats: int = 2) -> dict:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    dictionary = train_dictionary(training_samples(), 8192)
    (output_dir / "agent-dict-v1.zdict").write_bytes(dictionary)
    registry = DictionaryRegistry()
    registry.register(1, dictionary)
    rows: list[dict] = []
    for name in ["tool-json", "mixed-agent", "logs-diffs", "high-entropy", "short-chat"]:
        w = workload(name, n, seed=42)
        raw_bytes = sum(len(m.payload) for m in w.messages)
        jobs = []  # (variant, gap_ms, has_dict, encode() -> [(frame, ts)], new decoder)
        for config_name, level_name, one, has_dict, count, max_bytes in CONFIGS:
            gaps = [w.interval_ms] + ([SLOW_GAP_MS] if not one else [])
            for gap in gaps:
                codec = FrameCodec(registry, level=3, max_raw_bytes=1024 * 1024)
                if level_name == "raw":
                    encode = (lambda codec=codec, gap=gap:
                              [(codec.encode([m], i, force_raw=True), i * gap)
                               for i, m in enumerate(w.messages)])
                else:
                    encode = (lambda codec=codec, gap=gap, a=(level_name, one, has_dict, count, max_bytes):
                              emit_frames(w.messages, gap, codec, *a))
                variant = config_name if gap == w.interval_ms else f"{config_name}@{gap // 1000}s"
                jobs.append((variant, gap, has_dict, encode,
                             lambda codec=codec: StreamDecoder(codec)))
        for config_name, has_dict in STREAM_CONFIGS:
            jobs.append((config_name, w.interval_ms, has_dict,
                         lambda has_dict=has_dict: emit_stream(w.messages, w.interval_ms, registry,
                                                               1 if has_dict else 0),
                         lambda: StreamSessionDecoder(registry)))
        for variant, gap, has_dict, encode, new_decoder in jobs:
            wire_samples, enc_ms, dec_ms = [], [], []
            frame_count, max_wait = 0, 0
            for iteration in range(repeats):
                begin = time.perf_counter_ns()
                frames = encode()
                encoded_ms = (time.perf_counter_ns() - begin)/1e6
                start = time.perf_counter_ns()
                stream = new_decoder()
                decoded: list[Message] = []
                arrivals: list[int] = []
                for frame, timestamp in frames:
                    # Each frame is split across a transport-like byte stream.
                    for offset in range(0, len(frame), 1024):
                        got = stream.feed(frame[offset:offset + 1024])
                        decoded.extend(got)
                        arrivals.extend([timestamp] * len(got))
                stream.finish()
                decoded_ms = (time.perf_counter_ns() - start)/1e6
                if decoded != w.messages:
                    raise AssertionError(f"roundtrip failed: {name}/{variant}")
                wire_samples.append(sum(len(frame) for frame,_ in frames))
                enc_ms.append(encoded_ms)
                dec_ms.append(decoded_ms)
                if iteration == 0:
                    frame_count = len(frames)
                    max_wait = max((ts - m.id*gap for m, ts in zip(decoded, arrivals)), default=0)
            wire_bytes = int(statistics.median(wire_samples))
            rows.append({
                "workload": name, "variant": variant, "messages": n,
                "arrival_gap_ms": gap,
                "original_bytes": raw_bytes, "wire_bytes": wire_bytes,
                "ratio_x": round(raw_bytes/wire_bytes, 3),
                "effective_ratio_with_dictionary_x": round(raw_bytes/(wire_bytes + (len(dictionary) if has_dict else 0)), 3),
                "frames": frame_count, "max_simulated_queue_ms": max_wait,
                "encode_ms_median": round(statistics.median(enc_ms), 3),
                "decode_ms_median": round(statistics.median(dec_ms), 3),
            })
    with (output_dir / "bench.csv").open("w", newline="", encoding="utf8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    archive = ContentAddressedStore(output_dir / "archive-awb1")
    events = context_events()
    original_size = size_events(events)
    budget = max(500, original_size // 5)
    reduction = ContextReducer(archive).reduce(events, budget)
    restored = rehydrate(reduction.events, archive)
    if restored != events:
        raise AssertionError("context rehydration failed")
    context_stats = {
        "events": len(events), "condensed_events": reduction.condensed_count,
        "protected_events": reduction.protected_count,
        "original_context_bytes": original_size,
        "reduced_context_bytes": reduction.context_bytes,
        "context_reduction_x": round(original_size/reduction.context_bytes, 3),
        "cold_storage_bytes": sum(f.stat().st_size for f in (output_dir / "archive-awb1").rglob("*") if f.is_file()),
        "fully_rehydratable": True,
    }
    (output_dir / "reduced-context.json").write_bytes(encode_json([asdict(e) for e in reduction.events]))
    (output_dir / "context-stats.json").write_text(json.dumps(context_stats, indent=2))
    report = {"description":"Synthetic agent-output workloads, no real customer data",
              "dictionary_bytes":len(dictionary),"dictionary_manifest":registry.manifest(),
              "max_batch_messages":32,"max_batch_body_bytes":65536,"max_delay_ms":25,"slow_gap_ms":SLOW_GAP_MS,"timer_poll":"simulated at deadline",
              "samples_per_workload":n,"repeats":repeats,"transport":rows,"context":context_stats}
    (output_dir / "bench.json").write_text(json.dumps(report, indent=2))
    _write_report(output_dir, report)
    return report


def _write_report(output_dir: Path, report: dict):
    lines = ["# Measured results — AgentWire PoC", "",
             "All data below was measured on deterministic **synthetic fixtures**, not on production agent traces.",
             "All byte figures include frame headers (56 B per AWF1 frame; 8–12 B per AWC1 frame plus a 28 B stream preamble) and per-message metadata. Dictionary distribution cost is **excluded** from wire bytes and shown in a separate amortized metric.",
             "", "## Wire compression", "",
             "Variants: `*/one` = one AWF1 frame per message; `batch-*` = AWF1 bounded batches (25 ms deadline) "
             "at the workload's synthetic arrival gap; `@1s` = the same batcher when messages arrive 1 s apart; "
             "`*/stream` = AWC1, one Zstd stream per connection, one frame per message, no added delay.",
             "", "### Summary: zero-delay stream vs. batching", "",
             "| Workload | Batch+dict, fast arrivals | Batch+dict, 1 s gap | One frame+dict | **Stream+dict** | Stream, no dict |",
             "|---|---:|---:|---:|---:|---:|"]
    by = {(r["workload"], r["variant"]): r for r in report["transport"]}
    for w in dict.fromkeys(r["workload"] for r in report["transport"]):
        cell = lambda v: f'{by[(w, v)]["ratio_x"]:.2f}× ({by[(w, v)]["max_simulated_queue_ms"]} ms)'
        lines.append(f'| {w} | {cell("zstd3+dict/batch-64k")} | {cell("zstd3+dict/batch-64k@1s")} | '
                     f'{cell("zstd3+dict/one")} | **{cell("zstd3+dict/stream")}** | {cell("zstd3/stream")} |')
    lines += ["", "Ratio (max simulated added wait).", "", "### All measurements", "",
              "| Workload | Variant | Gap | Ratio | Wire bytes | Frames | Max simulated wait | Encode ms | Decode ms |",
              "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for row in report["transport"]:
        lines.append(f'| {row["workload"]} | {row["variant"]} | {row["arrival_gap_ms"]} ms | {row["ratio_x"]:.3f}× | '
                     f'{row["wire_bytes"]:,} | {row["frames"]} | {row["max_simulated_queue_ms"]} ms | '
                     f'{row["encode_ms_median"]:.2f} | {row["decode_ms_median"]:.2f} |')
    c=report["context"]
    lines += ["", "## Optional agent-context reduction (NOT lossless context)", "",
              f'- Context before: **{c["original_context_bytes"]:,} bytes**',
              f'- Context after: **{c["reduced_context_bytes"]:,} bytes** ({c["context_reduction_x"]:.2f}× reduction)',
              f'- Condensed: **{c["condensed_events"]} of {c["events"]} events**',
              f'- Explicitly protected: **{c["protected_events"]} events**',
              f'- Cold archive bytes: **{c["cold_storage_bytes"]:,}** (not counted in context)',
              '- Every condensed observation can be restored exactly as a JSON value from the archive; context alone is intentionally lossy.',
              "", "## Interpretation", "",
              "- This measures payload compression plus wire framing, not HTTP/TLS network performance.",
              "- Batch waits are simulated with a timer that fires at the 25 ms deadline (as AsyncBatchedSender does); AWC1 stream frames add no wait.",
              "- AWC1 has no raw fallback (a message on the stream must be sent compressed), so incompressible data can expand slightly.",
              "- Times are single-process Python ctypes/libzstd microbenchmarks and include Python orchestration overhead.",
              "- Synthetic workload structure favors dictionary reuse; evaluate against real traces before selecting a default.",
              "- A correct production transport requires TLS, delivery acknowledgments/replay, durable dictionary distribution, and telemetry.", ""]
    (output_dir / "RESULTS.md").write_text("\n".join(lines))
