"""Build the research corpora from pinned public datasets plus synthetic generators.

Nothing here reads private data. Sources (downloaded into corpora/raw/, which is
git-ignored; only the manifest with hashes and sizes is committed):

  * nebius/SWE-agent-trajectories (CC-BY-4.0), shard 0 at a pinned revision:
    SWE-agent runs on public GitHub repos (system prompt, issue, model turns,
    shell observations) plus SWE-bench evaluation logs (real CI-style test runs).
  * SWE-Gym/OpenHands-Sampled-Trajectories, shard 0 at a pinned revision:
    OpenHands runs with OpenAI-style tool_calls and tool messages.
  * logpai/loghub 2k-line samples (10 systems): classic system logs.

Derived corpora (one JSONL per corpus in corpora/, each line
{"s": session, "t": ms since session start, "k": kind, "b": message text}).
A "message" is exactly the UTF-8 bytes of "b".

  transcripts-cc   SWE-agent sessions re-emitted as Claude-Code-style JSONL
                   session-log lines (envelope modelled, content real).
  transcripts-oh   OpenHands sessions as OpenAI-chat-style event-log lines.
  api-requests     Per-turn LLM request bodies: the whole context re-sent each
                   turn (Anthropic Messages shape for SWE-agent, OpenAI chat
                   shape with tool schemas for OpenHands).
  tool-outputs     Every tool observation, classified by kind, plus synthetic
                   JSON API responses, HTML, stack traces and base64 blobs.
  ci-logs          SWE-bench evaluation logs (patch apply + pytest runs) and
                   Loghub system logs, one log line group per message.
  collab           Synthetic multi-agent traffic: orchestrator/sub-agent task
                   assignments, hand-backs, shared-state patches, PR/CI webhook
                   events. Free text is drawn from the real trajectories.
  sse              Streaming responses as LLM APIs send them: real assistant
                   turns tokenized with o200k_base (tiktoken) into deltas and
                   wrapped as Anthropic-style and OpenAI-style SSE events.

Timestamps are synthetic and deterministic (the public datasets carry none):
model turns 2-30 s apart (log-normal), tool results 0.05-5 s, tokens ~60/s.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import random
import re
import sys
import urllib.request
import uuid
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "corpora" / "raw"
OUT = ROOT / "corpora"

HF = "https://huggingface.co/datasets"
SOURCES = {
    "swe_agent_0.parquet": f"{HF}/nebius/SWE-agent-trajectories/resolve/"
                           "68195a1450865274106246d0d0296a1d6807b88e/data/train-00000-of-00012.parquet",
    "openhands_0.parquet": f"{HF}/SWE-Gym/OpenHands-Sampled-Trajectories/resolve/"
                           "baf3a4e4bff514d48ddc08a93a2ade5c126212c7/data/train.raw-00000-of-00003.parquet",
}
LOGHUB = ["HDFS", "Apache", "Linux", "Spark", "OpenSSH", "Hadoop", "Android", "Zookeeper", "BGL", "Mac"]
LOGHUB_COMMIT = "master"
for _n in LOGHUB:
    SOURCES[f"loghub_{_n}_2k.log"] = (f"https://raw.githubusercontent.com/logpai/loghub/"
                                      f"{LOGHUB_COMMIT}/{_n}/{_n}_2k.log")

J = lambda o: json.dumps(o, ensure_ascii=False, separators=(",", ":"))


def fetch():
    RAW.mkdir(parents=True, exist_ok=True)
    for name, url in SOURCES.items():
        target = RAW / name
        if not target.exists():
            print("fetching", url, file=sys.stderr)
            urllib.request.urlretrieve(url, target)


def _uuid(r: random.Random) -> str:
    return str(uuid.UUID(int=r.getrandbits(128), version=4))


def _iso(base_s: float, ms: int) -> str:
    import datetime as dt
    t = dt.datetime.fromtimestamp(base_s + ms / 1000, dt.timezone.utc)
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"


def _gap(r: random.Random, kind: str) -> int:
    if kind == "model":
        return int(min(60000, max(800, r.lognormvariate(math.log(6000), 0.7))))
    return int(min(30000, max(30, r.lognormvariate(math.log(600), 1.2))))


_ACTION = re.compile(r"```\n?(.*?)```", re.S)


def load_swe(n: int, seed: int):
    import pyarrow.parquet as pq
    t = pq.read_table(RAW / "swe_agent_0.parquet",
                      columns=["instance_id", "model_name", "trajectory", "eval_logs"])
    idx = sorted(random.Random(seed).sample(range(t.num_rows), n))
    return [t.slice(i, 1).to_pylist()[0] for i in idx]


def load_oh(n: int, seed: int):
    import pyarrow.parquet as pq
    t = pq.read_table(RAW / "openhands_0.parquet", columns=["instance_id", "run_id", "messages", "tools"])
    idx = sorted(random.Random(seed).sample(range(t.num_rows), n))
    rows = [t.slice(i, 1).to_pylist()[0] for i in idx]
    for row in rows:   # drop always-null columns so the JSON looks like what a harness logs
        row["messages"] = [{k: v for k, v in m.items() if v is not None and k != "function_call"}
                           for m in row["messages"]]
        for m in row["messages"]:
            for tc in m.get("tool_calls") or []:
                tc.pop("index", None)
        row["tools"] = _strip_nulls(row["tools"])
    return rows


def _strip_nulls(v):
    if isinstance(v, dict):
        return {k: _strip_nulls(x) for k, x in v.items() if x is not None}
    if isinstance(v, list):
        return [_strip_nulls(x) for x in v]
    return v


# --------------------------------------------------------------------------- transcripts

def cc_session(row, s: int, r: random.Random):
    """SWE-agent run -> Claude-Code-style session JSONL (envelope modelled)."""
    sid = _uuid(r)
    base = 1.7e9 + r.randrange(10**7)
    model = "model-" + hashlib.sha1(row["model_name"].encode()).hexdigest()[:6]
    common = {"isSidechain": False, "userType": "external", "cwd": "/" + row["instance_id"].split("__")[0],
              "sessionId": sid, "version": "2.0.14", "gitBranch": "main"}
    out, parent, t, tokens = [], None, 0, 0
    pending_tool = None
    system = None
    for m in row["trajectory"]:
        role, text = m["role"], m["text"] or ""
        if role == "system":
            system = m["system_prompt"] or ""
            tokens += len(system) // 4
            continue
        u = _uuid(r)
        if role == "ai":
            t += _gap(r, "model")
            content = []
            acts = _ACTION.findall(text)
            prose = _ACTION.sub("", text).strip()
            if prose:
                content.append({"type": "text", "text": prose})
            tool_id = None
            if acts:
                tool_id = "toolu_" + base64.b32encode(r.randbytes(15)).decode().lower()
                content.append({"type": "tool_use", "id": tool_id, "name": "Bash",
                                "input": {"command": acts[-1].strip(), "description": "Run command"}})
            out_tok = max(1, len(text) // 4)
            rec = dict(parentUuid=parent, **common, message={
                "id": "msg_" + base64.b32encode(r.randbytes(15)).decode().lower(), "type": "message",
                "role": "assistant", "model": model, "content": content,
                "stop_reason": "tool_use" if acts else "end_turn", "stop_sequence": None,
                "usage": {"input_tokens": 4, "cache_creation_input_tokens": out_tok,
                          "cache_read_input_tokens": tokens, "output_tokens": out_tok,
                          "service_tier": "standard"}},
                requestId="req_" + base64.b32encode(r.randbytes(15)).decode().lower(),
                type="assistant", uuid=u, timestamp=_iso(base, t))
            tokens += out_tok
            pending_tool = tool_id
            out.append((t, "assistant", J(rec)))
        else:
            if pending_tool is None:   # first user turn: the task
                rec = dict(parentUuid=parent, **common, type="user",
                           message={"role": "user", "content": text}, uuid=u, timestamp=_iso(base, t))
                kind = "user"
            else:
                t += _gap(r, "tool")
                rec = dict(parentUuid=parent, **common, type="user", message={"role": "user", "content": [
                    {"tool_use_id": pending_tool, "type": "tool_result", "content": text, "is_error": False}]},
                    uuid=u, timestamp=_iso(base, t),
                    toolUseResult={"stdout": text, "stderr": "", "interrupted": False, "isImage": False})
                kind = "tool_result"
            tokens += len(text) // 4
            pending_tool = None
            out.append((t, kind, J(rec)))
        parent = u
    return [{"s": s, "t": t_, "k": k, "b": b} for t_, k, b in out], system


def oh_session(row, s: int, r: random.Random):
    base = 1.7e9 + r.randrange(10**7)
    out, t = [], 0
    for i, m in enumerate(row["messages"]):
        if m["role"] == "assistant":
            t += _gap(r, "model")
        elif m["role"] == "tool":
            t += _gap(r, "tool")
        rec = {"id": i, "timestamp": _iso(base, t), "source": {"assistant": "agent", "tool": "environment"}.get(m["role"], m["role"]),
               "run_id": row["run_id"], "message": m}
        out.append({"s": s, "t": t, "k": {"assistant": "assistant", "tool": "tool_result"}.get(m["role"], m["role"]),
                    "b": J(rec)})
    return out


# --------------------------------------------------------------------------- API requests

def swe_requests(row, s: int, r: random.Random):
    """One Anthropic-Messages-shaped request body per model turn (full context)."""
    system, msgs, out, t = "", [], [], 0
    for m in row["trajectory"]:
        if m["role"] == "system":
            system = m["system_prompt"] or ""
            continue
        if m["role"] == "ai":
            t += _gap(r, "model")
            body = {"model": "model-x", "max_tokens": 4096, "system": system,
                    "messages": msgs, "stream": True}
            out.append({"s": s, "t": t, "k": "request", "b": J(body)})
            msgs = msgs + [{"role": "assistant", "content": m["text"] or ""}]
        else:
            t += _gap(r, "tool")
            msgs = msgs + [{"role": "user", "content": m["text"] or ""}]
    return out


def oh_requests(row, s: int, r: random.Random):
    msgs, out, t = [], [], 0
    for m in row["messages"]:
        if m["role"] == "assistant":
            t += _gap(r, "model")
            body = {"model": "model-y", "messages": msgs, "tools": row["tools"], "temperature": 0}
            out.append({"s": s, "t": t, "k": "request", "b": J(body)})
        elif m["role"] == "tool":
            t += _gap(r, "tool")
        msgs = msgs + [m]
    return out


# --------------------------------------------------------------------------- tool outputs

def classify(text: str) -> str:
    head = text[:4000]
    if "diff --git" in head or re.search(r"^@@ -\d", head, re.M):
        return "diff"
    if "Traceback (most recent call last)" in head or re.search(r"^\s+at [\w.$]+\(", head, re.M):
        return "stack-trace"
    if re.search(r"(=+ test session starts|passed|failed|FAILED|PASSED|collected \d+ items)", head):
        return "test-run"
    if re.search(r"(error|Error|SyntaxError|warning):", head) and re.search(r"line \d+|:\d+:\d+", head):
        return "compiler-error"
    if re.search(r"^\[File: .*\(\d+ lines total\)\]|^Here's the result of running `cat -n`|^\s*\d+\t", head, re.M):
        return "file-read"
    if re.search(r"^Found \d+ matches|^.+:\d+:.+$", head, re.M) and head.count("\n") > 3:
        return "search"
    if re.search(r"^(total \d+|[d-][rwx-]{9})", head, re.M) or head.count("/") > head.count(" ") * 0.3:
        return "listing"
    if not text.strip():
        return "empty"
    return "shell-other"


def synth_tool_outputs(n: int, r: random.Random, words: list[str]):
    out = []
    for i in range(n):
        k = i % 5
        if k == 0:   # JSON API response (GitHub-like issue list)
            body = J([{"id": r.randrange(10**9), "number": r.randrange(1, 5000), "state": r.choice(["open", "closed"]),
                       "title": " ".join(r.choices(words, k=8)), "user": {"login": f"user{r.randrange(500)}",
                       "id": r.randrange(10**7), "type": "User", "site_admin": False},
                       "labels": [{"name": r.choice(["bug", "enhancement", "docs", "triage"]), "color": "d73a4a"}],
                       "comments": r.randrange(30), "created_at": f"2025-0{r.randrange(1,10)}-1{r.randrange(10)}T0{r.randrange(10)}:12:00Z",
                       "body": " ".join(r.choices(words, k=r.randrange(20, 120)))} for _ in range(r.randrange(3, 15))])
            out.append(("json-api", body))
        elif k == 1:   # HTML page fetch
            items = "".join(f'<li class="result"><a href="https://example.org/{r.choice(words)}/{r.randrange(9999)}">'
                            f'{" ".join(r.choices(words, k=6))}</a><p>{" ".join(r.choices(words, k=30))}</p></li>\n'
                            for _ in range(r.randrange(5, 25)))
            body = ('<!DOCTYPE html>\n<html lang="en"><head><meta charset="utf-8"><title>' + " ".join(r.choices(words, k=5)) +
                    '</title><link rel="stylesheet" href="/static/site.css"><script src="/static/app.js" defer></script>'
                    '</head>\n<body><nav class="top"><a href="/">Home</a> <a href="/docs">Docs</a></nav>\n<main><ul>\n'
                    + items + "</ul></main><footer>&copy; 2025 Example</footer></body></html>")
            out.append(("html", body))
        elif k == 2:   # Java-style stack trace
            frames = "".join(f"\tat org.example.{r.choice(words)}.{r.choice(words).title()}Service.{r.choice(words)}"
                             f"({r.choice(words).title()}Service.java:{r.randrange(20, 900)})\n" for _ in range(r.randrange(8, 40)))
            out.append(("stack-trace", f"Exception in thread \"main\" java.lang.IllegalStateException: {' '.join(r.choices(words, k=6))}\n"
                        + frames + "Caused by: java.io.IOException: Connection reset\n" + frames[:400]))
        elif k == 3:   # base64 of an image-like (already compressed) blob
            raw = zlib.compress(r.randbytes(r.randrange(2000, 20000)), 9)
            out.append(("base64-blob", J({"type": "image", "source": {"type": "base64", "media_type": "image/png",
                                                                        "data": base64.b64encode(raw).decode()}})))
        else:          # binary-ish: hexdump of random bytes
            raw = r.randbytes(r.randrange(256, 2048))
            lines = "".join(f"{o:08x}  " + " ".join(f"{b:02x}" for b in raw[o:o + 16]) + "\n" for o in range(0, len(raw), 16))
            out.append(("hexdump", lines))
    return out


# --------------------------------------------------------------------------- collaboration

def collab(swe_rows, r: random.Random, sessions: int = 40):
    """Orchestrator + 3-6 sub-agents sharing a task board; GitHub-style PR/CI events."""
    texts = [m["text"] for row in swe_rows for m in row["trajectory"] if m["role"] == "ai" and m["text"]]
    obs = [m["text"] for row in swe_rows for m in row["trajectory"][2:] if m["role"] == "user" and m["text"]]
    out = []
    for s in range(sessions):
        agents = ["orchestrator"] + [f"worker-{i}" for i in range(r.randrange(3, 7))]
        t, seq = 0, 0
        board = {f"T{i}": {"status": "todo", "owner": None, "title": r.choice(texts)[:80]} for i in range(r.randrange(4, 10))}
        repo, pr = f"org/repo-{s}", r.randrange(100, 9000)
        sha = hashlib.sha1(str(s).encode()).hexdigest()

        def emit(kind, frm, to, body):
            nonlocal seq
            seq += 1
            out.append({"s": s, "t": t, "k": kind, "b": J({"v": 1, "seq": seq, "ts": _iso(1.7e9, t), "from": frm,
                                                         "to": to, "type": kind, "conversation_id": f"conv-{s}", "body": body})})
        for tid, task in board.items():
            t += _gap(r, "tool")
            w = r.choice(agents[1:])
            task.update(status="assigned", owner=w)
            emit("task_assign", "orchestrator", w, {"task_id": tid, "title": task["title"], "instructions": r.choice(texts)[:1500],
                                                    "context_refs": [f"sha256:{hashlib.sha256(str(r.random()).encode()).hexdigest()}"]})
            emit("state_patch", "orchestrator", "*", {"op": "replace", "path": f"/board/{tid}", "value": dict(task)})
        for _ in range(r.randrange(30, 90)):
            t += _gap(r, r.choice(["model", "tool"]))
            w, tid = r.choice(agents[1:]), r.choice(list(board))
            x = r.random()
            if x < 0.35:
                emit("progress", w, "orchestrator", {"task_id": tid, "status": "running", "note": r.choice(texts)[:300],
                                                     "tokens_used": r.randrange(10**3, 10**5)})
            elif x < 0.6:
                board[tid]["status"] = "done"
                emit("handback", w, "orchestrator", {"task_id": tid, "result": "success" if r.random() < .8 else "failed",
                                                     "summary": r.choice(texts)[:2000], "artifacts": [{"kind": "observation", "content": r.choice(obs)[:6000]}]})
                emit("state_patch", "orchestrator", "*", {"op": "replace", "path": f"/board/{tid}/status", "value": "done"})
            elif x < 0.7:
                emit("state_snapshot", "orchestrator", "*", {"board": board, "agents": agents})
            elif x < 0.85:
                emit("github.check_run", "github", "*", {"action": r.choice(["created", "completed"]),
                     "check_run": {"id": r.randrange(10**10), "head_sha": sha, "name": r.choice(["test (3.11)", "test (3.12)", "lint", "build"]),
                                   "status": "completed", "conclusion": r.choice(["success", "failure", "neutral"]),
                                   "output": {"title": "pytest", "summary": r.choice(obs)[:1200]},
                                   "pull_requests": [{"number": pr, "head": {"ref": f"agent/task-{s}", "sha": sha},
                                                      "base": {"ref": "main"}}]},
                     "repository": {"full_name": repo, "private": False, "default_branch": "main"},
                     "sender": {"login": "ci-bot", "type": "Bot"}})
            else:
                emit("github.pull_request_review_comment", "github", "*", {"action": "created", "comment": {
                    "id": r.randrange(10**9), "path": f"src/{r.choice(['core', 'api', 'util'])}.py", "line": r.randrange(1, 400),
                    "body": r.choice(texts)[:500], "user": {"login": "reviewer", "type": "User"}},
                    "pull_request": {"number": pr, "state": "open", "title": board[tid]["title"]},
                    "repository": {"full_name": repo}})
    return out


# --------------------------------------------------------------------------- SSE

def sse_streams(swe_rows, oh_rows, r: random.Random, n: int = 80):
    import tiktoken
    enc = tiktoken.get_encoding("o200k_base")
    texts = [m["text"] for row in swe_rows for m in row["trajectory"] if m["role"] == "ai" and m["text"]]
    calls = [(tc["function"]["name"], tc["function"]["arguments"]) for row in oh_rows for m in row["messages"]
             for tc in (m.get("tool_calls") or [])]
    out = []
    for s in range(n):
        text = r.choice(texts)
        toks = [enc.decode_single_token_bytes(t) for t in enc.encode(text)]
        t = 0
        evs = []
        mid = "msg_" + base64.b32encode(r.randbytes(15)).decode().lower()
        if s % 2 == 0:    # Anthropic-style
            evs.append(("message_start", {"type": "message_start", "message": {"id": mid, "type": "message", "role": "assistant",
                        "model": "model-x", "content": [], "stop_reason": None, "stop_sequence": None,
                        "usage": {"input_tokens": r.randrange(1000, 90000), "output_tokens": 1}}}))
            evs.append(("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}))
            buf = b""
            for tok in toks:
                buf += tok
                try:
                    piece = buf.decode()
                except UnicodeDecodeError:
                    continue
                buf = b""
                evs.append(("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": piece}}))
            evs.append(("content_block_stop", {"type": "content_block_stop", "index": 0}))
            name, args = r.choice(calls)
            evs.append(("content_block_start", {"type": "content_block_start", "index": 1, "content_block": {
                "type": "tool_use", "id": "toolu_" + base64.b32encode(r.randbytes(15)).decode().lower(), "name": name, "input": {}}}))
            for i in range(0, len(args), 12):
                evs.append(("content_block_delta", {"type": "content_block_delta", "index": 1,
                                                    "delta": {"type": "input_json_delta", "partial_json": args[i:i + 12]}}))
            evs.append(("content_block_stop", {"type": "content_block_stop", "index": 1}))
            evs.append(("message_delta", {"type": "message_delta", "delta": {"stop_reason": "tool_use", "stop_sequence": None},
                                          "usage": {"output_tokens": len(toks)}}))
            evs.append(("message_stop", {"type": "message_stop"}))
            lines = [f"event: {e}\ndata: {J(d)}\n\n" for e, d in evs]
        else:             # OpenAI chat.completion.chunk style
            cid = "chatcmpl-" + base64.b32encode(r.randbytes(15)).decode()
            created = 1730000000 + r.randrange(10**6)
            def chunk(delta, finish=None):
                return {"id": cid, "object": "chat.completion.chunk", "created": created, "model": "model-y",
                        "system_fingerprint": "fp_" + cid[-10:].lower(), "choices": [{"index": 0, "delta": delta,
                        "logprobs": None, "finish_reason": finish}]}
            lines = [f"data: {J(chunk({'role': 'assistant', 'content': ''}))}\n\n"]
            buf = b""
            for tok in toks:
                buf += tok
                try:
                    piece = buf.decode()
                except UnicodeDecodeError:
                    continue
                buf = b""
                lines.append(f"data: {J(chunk({'content': piece}))}\n\n")
            lines.append(f"data: {J(chunk({}, 'stop'))}\n\n")
            lines.append("data: [DONE]\n\n")
        for line in lines:
            t += int(r.expovariate(1 / 16)) + 1      # ~60 tokens/s with jitter
            out.append({"s": s, "t": t, "k": "sse", "b": line})
    return out


# --------------------------------------------------------------------------- main

def write(name: str, rows: list[dict]):
    path = OUT / f"{name}.jsonl"
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(J(row) + "\n")
    payload = sum(len(r["b"].encode()) for r in rows)
    return {"corpus": name, "messages": len(rows), "sessions": len({r["s"] for r in rows}),
            "payload_bytes": payload, "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--swe", type=int, default=300)
    ap.add_argument("--oh", type=int, default=300)
    ap.add_argument("--requests", type=int, default=40, help="sessions per source for api-requests")
    ap.add_argument("--seed", type=int, default=20261119)
    a = ap.parse_args(argv)
    fetch()
    r = random.Random(a.seed)
    swe, oh = load_swe(a.swe, a.seed), load_oh(a.oh, a.seed)
    manifest = {"seed": a.seed, "sources": {}, "corpora": []}
    for name, url in SOURCES.items():
        p = RAW / name
        manifest["sources"][name] = {"url": url, "bytes": p.stat().st_size,
                                     "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}

    rows = []
    for s, row in enumerate(swe):
        rows += cc_session(row, s, r)[0]
    manifest["corpora"].append(write("transcripts-cc", rows))
    rows = []
    for s, row in enumerate(oh):
        rows += oh_session(row, s, r)
    manifest["corpora"].append(write("transcripts-oh", rows))

    rows = []
    for s, row in enumerate(swe[:a.requests]):
        rows += swe_requests(row, s, r)
    for s, row in enumerate(oh[:a.requests]):
        rows += oh_requests(row, a.requests + s, r)
    manifest["corpora"].append(write("api-requests", rows))

    rows, s = [], 0
    for src in (swe, oh):
        for row in src:
            t = 0
            msgs = ([m["text"] for m in row["trajectory"][2:] if m["role"] == "user" and m["text"]]
                    if "trajectory" in row else [m["content"] for m in row["messages"] if m["role"] == "tool"])
            for text in msgs:
                t += _gap(r, "model")
                rows.append({"s": s, "t": t, "k": classify(text), "b": text})
            s += 1
    words = sorted({w for row in swe[:50] for m in row["trajectory"] for w in re.findall(r"[a-z]{4,10}", m["text"] or "")})
    t = 0
    for i, (k, body) in enumerate(synth_tool_outputs(1000, r, words)):
        t += _gap(r, "tool")
        rows.append({"s": s + i // 50, "t": t, "k": k, "b": body})
    manifest["corpora"].append(write("tool-outputs", rows))

    rows = []
    for s, row in enumerate(swe):
        t = 0
        for chunk in re.split(r"(?<=\n)(?=\S)", row["eval_logs"] or ""):
            if chunk:
                t += r.randrange(1, 200)
                rows.append({"s": s, "t": t, "k": "swebench-eval", "b": chunk})
    for j, name in enumerate(LOGHUB):
        t = 0
        for line in (RAW / f"loghub_{name}_2k.log").read_text(errors="replace").splitlines(keepends=True):
            t += r.randrange(1, 50)
            rows.append({"s": len(swe) + j, "t": t, "k": f"loghub-{name}", "b": line})
    manifest["corpora"].append(write("ci-logs", rows))

    manifest["corpora"].append(write("collab", collab(swe, r)))
    manifest["corpora"].append(write("sse", sse_streams(swe, oh, r)))
    (ROOT / "results" / "research").mkdir(parents=True, exist_ok=True)
    (ROOT / "results" / "research" / "corpora-manifest.json").write_text(json.dumps(manifest, indent=2))
    for c in manifest["corpora"]:
        print(f'{c["corpus"]:16} {c["sessions"]:5} sessions {c["messages"]:8} msgs {c["payload_bytes"]/2**20:9.2f} MiB')


if __name__ == "__main__":
    main()
