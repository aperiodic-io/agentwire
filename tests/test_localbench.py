from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone

from agentwire.localbench import VARIANTS, _load, run_local_benchmark


def _fake_sessions(root, count=6, lines=120, seed=3):
    """Synthetic Claude-Code-like logs; never touches real user data."""
    r = random.Random(seed)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    root.mkdir(parents=True)
    for s in range(count):
        t = base + timedelta(days=s)
        with (root / f"session-{s}.jsonl").open("w") as f:
            for i in range(lines):
                t += timedelta(milliseconds=r.choice([0, 0, 3, 40, 2000]))
                rec = {"type": r.choice(["user", "assistant", "attachment"]),
                       "sessionId": f"s{s}", "timestamp": t.isoformat().replace("+00:00", "Z"),
                       "message": {"role": "assistant", "content": [
                           {"type": "text", "text": f"step {i} checking build output " * r.randrange(1, 30)}]}}
                if i % 17 == 0:
                    rec.pop("timestamp")  # records without a time inherit the previous one
                f.write(json.dumps(rec) + "\n")


def test_local_benchmark_on_fake_sessions(tmp_path):
    src = tmp_path / "claude"
    _fake_sessions(src)
    report = run_local_benchmark(tmp_path / "out", {"claude-code": src}, train_fraction=0.5,
                                 dict_size=2048, exclude_active_minutes=0, progress=lambda m: None)
    corpus = report["corpora"]["claude-code"]
    assert corpus["train_sessions"] == 3 and corpus["test_sessions"] == 3
    rows = {r["variant"]: r for r in corpus["variants"]}
    assert set(rows) == {name for name, _ in VARIANTS}
    assert rows["awc1/stream+dict"]["ratio_x"] > rows["awf1/one"]["ratio_x"]
    assert rows["awc1/stream"]["wait_max_ms"] == 0
    assert rows["awf1/batch"]["wait_max_ms"] <= 25
    assert rows["awf1/batch"]["messages_per_frame"] > 1
    assert (tmp_path / "out" / "RESULTS.md").read_text().startswith("# AgentWire on real")


def test_load_preserves_bytes_and_monotonic_time(tmp_path):
    path = tmp_path / "x.jsonl"
    path.write_bytes(b'{"timestamp":"2026-01-01T00:00:01Z"}\n{"no":"time"}\r\n'
                     b'{"timestamp":"2026-01-01T00:00:00Z"}\nnot json\n')
    messages, times = _load(path)
    assert b"".join(m.payload for m in messages) == path.read_bytes()
    assert times == sorted(times) and times[0] == 0
