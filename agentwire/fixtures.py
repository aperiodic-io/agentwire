"""Deterministic, fully synthetic agent-output generators. No customer data."""
from __future__ import annotations

import base64
import json
import random
import string
from dataclasses import dataclass

from .context import AgentEvent, encode_json
from .transport import Message


@dataclass(frozen=True)
class Workload:
    name: str
    messages: list[Message]
    interval_ms: int


TOPICS = ["market microstructure", "distributed traces", "database indexes",
          "trading telemetry", "object storage", "data ingestion", "build systems"]
SERVICES = ["event-gateway", "observability", "agent-worker", "jetstream",
            "object-sink", "vector-index", "planner", "query-engine"]
TOOLS = ["search", "fetch", "execute", "query", "inspect", "compile", "verify"]


def _dump(obj: object) -> bytes:
    return encode_json(obj)


def workload(name: str, n: int = 240, seed: int = 42) -> Workload:
    r = random.Random(seed)
    messages: list[Message] = []
    for i in range(n):
        service = r.choice(SERVICES)
        topic = r.choice(TOPICS)
        tool = r.choice(TOOLS)
        if name == "tool-json":
            obj = {
                "jsonrpc": "2.0", "id": i, "method": "tools/call", "service": service,
                "result": {"tool": tool, "status": "success", "ok": True,
                           "source": "tool-response", "query": f"{topic} request {r.randrange(10000)}",
                           "items": [{"name": f"asset-{r.randrange(100)}", "value": round(r.random()*123.4, 5),
                                      "confidence": round(r.random(), 4), "timestamp": f"2026-11-04T08:{i%60:02}:00Z",
                                      "type": "dataset", "origin": service} for _ in range(5)]}}
            payload = _dump(obj)
        elif name == "mixed-agent":
            t = i % 5
            if t == 0:
                obj = {"role": "assistant", "type": "tool_call", "name": tool,
                       "arguments": {"query": topic, "limit": 30, "timeout_ms": 7000}}
            elif t == 1:
                obj = {"role": "tool", "type": "tool_result", "tool": tool, "ok": True,
                       "results": [{"title": f"{topic} report {j}", "snippet": f"The {service} result shows that {topic} is relevant to the target task.",
                                    "score": round(r.random(),5)} for j in range(9)]}
            elif t == 2:
                obj = {"role": "assistant", "type": "analysis",
                       "content": f"Evaluate the latest {topic} measurements. Compare new evidence against prior {service} data. " * 4,
                       "step": i, "attempt": r.randrange(4)}
            elif t == 3:
                obj = {"role": "assistant", "type": "state_patch", "patch": {"phase": "active", "task": topic,
                       "last_service": service, "seen_ids": [r.randrange(100000) for _ in range(10)]}}
            else:
                obj = {"role": "assistant", "type": "message",
                       "content": f"Summary of {topic}: validation found partial coverage in {service}. " * 5,
                       "sources": [f"file_{r.randrange(100000)}" for _ in range(4)]}
            payload = _dump(obj)
        elif name == "logs-diffs":
            if i % 4:
                obj = {"timestamp": f"2026-11-04T08:{i%60:02}:{r.randrange(60):02}Z",
                       "service": service, "level": "INFO", "trace_id": f"{r.getrandbits(64):016x}",
                       "span_id": f"{r.getrandbits(32):08x}", "message": "completed action successfully",
                       "labels": {"region": "eu-central", "environment": "prod", "service": service},
                       "metrics": {"latency_ms": round(r.expovariate(1/50),4), "bytes": r.randrange(200000)}}
                payload = _dump(obj)
            else:
                h = r.choice(["reduce.rs", "transport.rs", "planner.py"])
                payload = (f"diff --git a/src/{h} b/src/{h}\nindex 12ab56..18eeff 100644\n"
                           f"--- a/src/{h}\n+++ b/src/{h}\n@@ -11,4 +11,5 @@\n"
                           f" context.set_timeout({r.randrange(1000)});\n"
                           f"-    decoder.flush(false);\n+    decoder.flush(true);\n"
                           f"+    trace!(\"message processed\");\n"*2).encode()
        elif name == "high-entropy":
            # Base64 from deterministic random bytes, unlike repetitive text.
            payload = base64.b64encode(r.randbytes(r.randrange(350,750)))
        elif name == "short-chat":
            sentences = ["OK", "done", "processing", "tool started", "no changes",
                         "retry", "agent checkpoint saved", "acknowledged", "status 200"]
            payload = r.choice(sentences).encode() + str(i).encode()
        else:
            raise ValueError(f"unknown workload: {name}")
        messages.append(Message(i, payload, kind=(i % 6)))
    return Workload(name, messages, {"tool-json":2,"mixed-agent":5,"logs-diffs":1,
                                     "high-entropy":3,"short-chat":3}[name])


def training_samples(seed: int = 7) -> list[bytes]:
    # Train on disjoint synthetic samples; no test message appears in training.
    samples = []
    for name in ("tool-json", "mixed-agent", "logs-diffs", "short-chat"):
        samples.extend(m.payload for m in workload(name, 200, seed + 17).messages)
    return samples


def context_events(n: int = 24) -> list[AgentEvent]:
    r = random.Random(189)
    events = [
        AgentEvent("sys", "system", "instruction", "Use deterministic and safe tool calls.", "critical"),
        AgentEvent("user", "user", "message", "Inspect the streaming pipeline and summarize failures.", "critical"),
        AgentEvent("call", "assistant", "tool_call",
                   {"name": "inspect", "arguments": {"cluster": "cluster-east", "namespace": "agents", "timeout_ms": 3500}}, "critical"),
        AgentEvent("state", "assistant", "state_patch",
                   {"path": "/run/status", "value": "in_progress", "cursor": 12948}, "critical"),
    ]
    for i in range(n):
        events.append(AgentEvent(
            f"obs-{i}", "tool", "tool_result",
            {"status": 200, "service": r.choice(SERVICES),
             "rows": [{"time": j, "state": "healthy", "region": "eu-central",
                       "cpu_pct": round(r.random()*100,3),
                       "description": f"successful ongoing background operation with queue checkpoint {j}, no lost updates."}
                      for j in range(15)],
             "result_id": i}, "low"))
    events.append(AgentEvent("failure", "tool", "tool_result",
                             {"status": 503, "error": "unavailable", "message": "keep this critical failure"}, "low"))
    return events
