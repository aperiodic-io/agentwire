"""Opt-in lossy-but-recoverable layer on re-sent context (observation masking).

Policy: in each LLM request, tool observations older than the last K model
turns are replaced by agentwire.observation_ref.v1 summaries via the existing
ContextReducer; the full payload goes to the content-addressed archive. Every
reduced request is rehydrated and compared with the original.

This measures bytes only. Whether the agent still solves the task is a separate
question (see the report: SWE-agent and "The Complexity Trap" evaluate this).
"""
from __future__ import annotations

import json
import shutil
import tempfile

from bench_transport import split
from common import load, machine, sessions, write_json
from agentwire.context import AgentEvent, BudgetExceeded, ContentAddressedStore, ContextReducer, rehydrate
from agentwire.delta import DeltaEncoder

J = lambda o: json.dumps(o, ensure_ascii=False, separators=(",", ":")).encode()


def to_events(body: dict, keep_turns: int):
    msgs = body["messages"]
    swe = "system" in body           # Anthropic-shaped (SWE-agent) bodies: observations are user turns
    model_turns = [i for i, m in enumerate(msgs) if m["role"] == "assistant"]
    horizon = model_turns[-keep_turns] if len(model_turns) >= keep_turns else 0
    events = []
    for i, m in enumerate(msgs):
        is_obs = m["role"] == "tool" or (swe and m["role"] == "user" and i > 0)
        events.append(AgentEvent(f"m{i}", "tool" if is_obs else m["role"],
                                 "tool_result" if is_obs else "message", m,
                                 "low" if is_obs and i < horizon else "normal"))
    return events


def reduce_all(reducer, events):
    try:
        return reducer.reduce(events, 2)
    except BudgetExceeded as exc:
        return reducer.reduce(events, exc.required)


def main():
    tmp = tempfile.mkdtemp(prefix="aw-reducer-")
    try:
        rows = []
        _, te = split(load("api-requests"))
        for keep in (3, 10):
            archive = ContentAddressedStore(tmp + f"/k{keep}")
            reducer = ContextReducer(archive, preview_chars=96)
            raw = red = 0
            enc_raw, enc_red = 0, 0
            for s in te[:24]:
                e1, e2 = DeltaEncoder(window_log=23), DeltaEncoder(window_log=23)
                for m in s:
                    body = json.loads(m.data)
                    events = to_events(body, keep)
                    res = reduce_all(reducer, events)
                    assert rehydrate(res.events, archive) == events
                    reduced = dict(body, messages=[e.payload for e in res.events])
                    rb = J(reduced)
                    raw += len(m.data)
                    red += len(rb)
                    enc_raw += len(e1.encode(m.data))
                    enc_red += len(e2.encode(rb))
            arch = sum(p.stat().st_size for p in __import__("pathlib").Path(tmp + f"/k{keep}").rglob("*") if p.is_file())
            rows.append({"keep_last_model_turns": keep, "request_bytes": raw, "reduced_request_bytes": red,
                         "context_reduction_x": raw / red, "awd1_wire_original": enc_raw, "awd1_wire_reduced": enc_red,
                         "archive_bytes": arch, "rehydration_exact": True})
            print(rows[-1], flush=True)
        write_json("reducer.json", {"machine": machine(), "rows": rows})
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
