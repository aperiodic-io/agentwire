"""Explicitly LOSSY context reduction with lossless cold storage and rehydration.

The wire protocol never calls this module: lossless transport and lossy context
reduction are independent opt-in layers. Low-priority successful observations
are eligible; instructions, user inputs, tool arguments, state, and failures are not.
"""
from __future__ import annotations

import hashlib
import json
import os
import struct
import tempfile
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

from .zstd import Zstd, ZstdError

SUMMARY_MARKER = "agentwire.observation_ref.v1"
PROTECTED_KINDS = frozenset({"instruction", "tool_call", "state_patch", "state_snapshot", "final"})
PROTECTED_ROLES = frozenset({"system", "developer", "user"})
ELIGIBLE_KINDS = frozenset({"observation", "tool_result"})


def encode_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def size_events(events: list[AgentEvent]) -> int:
    return len(encode_json([asdict(e) for e in events]))


@dataclass(frozen=True)
class AgentEvent:
    id: str
    role: str
    kind: str
    payload: Any
    importance: str = "normal"
    pinned: bool = False
    condensed: bool = False

    def __post_init__(self):
        if not self.id or not isinstance(self.id, str):
            raise ValueError("id must be nonempty string")
        if not isinstance(self.role, str) or not isinstance(self.kind, str):
            raise TypeError("role and kind must be strings")
        if self.importance not in {"critical", "normal", "low"}:
            raise ValueError("importance must be critical/normal/low")
        # Keep data JSON-serializable; fail rather than silently changing NaNs.
        encode_json(self.payload)


def _failure(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("error") or payload.get("exception") or payload.get("ok") is False or payload.get("success") is False:
        return True
    status = payload.get("status")
    if isinstance(status, int) and status >= 400:
        return True
    if isinstance(status, str) and status.casefold() in {"error", "failed", "failure", "denied"}:
        return True
    return False


def protected(event: AgentEvent) -> bool:
    """Fail closed: only explicitly low-priority successful observations qualify."""
    return (event.condensed or event.pinned or event.importance != "low" or
            event.role in PROTECTED_ROLES or event.kind in PROTECTED_KINDS or
            event.kind not in ELIGIBLE_KINDS or _failure(event.payload))


class ArchiveCorruptError(ValueError):
    pass


class ContentAddressedStore:
    """Local content-addressed archive with Zstd-at-rest compression.

    Hashes identify original canonical JSON bytes, not compressed files. Does not
    GC, encrypt, synchronize, replicate, or authorize access to stored blobs.
    The caller is responsible for durable storage, permissions and retention.
    """
    _HEADER = struct.Struct(">4sBI")  # magic, compression flag, raw bytes

    def __init__(self, directory: str | Path, max_blob_bytes: int = 16 * 1024 * 1024):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        if not 1 <= max_blob_bytes <= 2**32 - 1:
            raise ValueError("max_blob_bytes outside supported range")
        self.max_blob_bytes = max_blob_bytes
        self.zstd = Zstd(3)

    def _path(self, fingerprint: str) -> Path:
        if len(fingerprint) != 64 or any(c not in "0123456789abcdef" for c in fingerprint):
            raise ValueError("invalid SHA-256 digest")
        return self.directory / fingerprint[:2] / fingerprint[2:]

    def put(self, content: bytes) -> str:
        if not isinstance(content, bytes) or len(content) > self.max_blob_bytes:
            raise ValueError("archive blob exceeds configured byte limit")
        sha = hashlib.sha256(content).hexdigest()
        output = self._path(sha)
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.exists():
            if self.get(sha) != content:
                raise ArchiveCorruptError("existing blob has different content")
            return sha
        compressed = self.zstd.compress(content)
        compressed_flag = 1 if len(compressed) < len(content) else 0
        stored = self._HEADER.pack(b"AWB1", compressed_flag, len(content)) + (compressed if compressed_flag else content)
        tempname = None
        try:
            with tempfile.NamedTemporaryFile(dir=output.parent, prefix=".pending-", delete=False) as tmp:
                tempname = tmp.name
                tmp.write(stored)
                tmp.flush()
                os.fsync(tmp.fileno())
            os.replace(tempname, output)
        finally:
            if tempname and os.path.exists(tempname):
                os.unlink(tempname)
        return sha

    def get(self, fingerprint: str) -> bytes:
        data = self._path(fingerprint).read_bytes()
        if len(data) < self._HEADER.size:
            raise ArchiveCorruptError("archived blob too short")
        magic,flag,raw_len = self._HEADER.unpack_from(data)
        if magic != b"AWB1" or flag not in (0,1) or raw_len > self.max_blob_bytes:
            raise ArchiveCorruptError("invalid archive blob header")
        encoded = data[self._HEADER.size:]
        try:
            content = self.zstd.decompress(encoded,raw_len) if flag else encoded
        except ZstdError as exc:
            raise ArchiveCorruptError("archive decompression failed") from exc
        if len(content) != raw_len or hashlib.sha256(content).hexdigest() != fingerprint:
            raise ArchiveCorruptError("archived blob SHA-256/length mismatch")
        return content


def _summary(original: Any, digest: str, source_size: int,
             preview_chars: int) -> dict[str, Any]:
    raw = encode_json(original).decode("utf-8")
    # Store a tiny deterministic extract; never pass off as the full observation.
    excerpt = raw[:preview_chars]
    result: dict[str, Any] = {
        "_agentwire": SUMMARY_MARKER,
        "sha256": digest,
        "original_bytes": source_size,
        "preview_truncated": len(raw) > len(excerpt),
        "preview": excerpt,
    }
    if isinstance(original, dict):
        result["top_level_keys"] = list(original)[:8]
        status = original.get("status")
        if isinstance(status, (int, str, bool)):
            result["status"] = status
    return result


class BudgetExceeded(RuntimeError):
    def __init__(self, required: int, budget: int):
        self.required = required
        self.budget = budget
        super().__init__(f"cannot fit context in {budget} bytes; needs at least {required}")


@dataclass(frozen=True)
class ReductionResult:
    events: list[AgentEvent]
    original_bytes: int
    context_bytes: int
    condensed_count: int
    protected_count: int
    max_context_bytes: int


class ContextReducer:
    def __init__(self, archive: ContentAddressedStore, preview_chars: int = 96):
        if not 0 <= preview_chars <= 2048:
            raise ValueError("preview_chars outside [0,2048]")
        self.archive = archive
        self.preview_chars = preview_chars

    def reduce(self, events: list[AgentEvent], budget_bytes: int) -> ReductionResult:
        if budget_bytes < 2:
            raise ValueError("budget must fit a JSON array")
        if len({e.id for e in events}) != len(events):
            raise ValueError("event IDs must be unique")
        original = size_events(events)
        candidates: list[tuple[int, int, AgentEvent]] = []
        condensed = list(events)
        for index, event in enumerate(events):
            if protected(event):
                continue
            data = encode_json(event.payload)
            digest = hashlib.sha256(data).hexdigest()
            summary = _summary(event.payload, digest, len(data), self.preview_chars)
            alternate = replace(event, payload=summary, condensed=True)
            # Only change if it is actually cheaper in serialized context.
            savings = len(encode_json(asdict(event))) - len(encode_json(asdict(alternate)))
            if savings > 0:
                candidates.append((savings, index, alternate))

        current = original
        n_reduced = 0
        for savings, index, alternative in sorted(candidates, reverse=True):
            if current <= budget_bytes:
                break
            # Archive is committed before replacing content in context.
            blob = encode_json(events[index].payload)
            if self.archive.put(blob) != alternative.payload["sha256"]:
                raise ArchiveCorruptError("unexpected archive checksum")
            condensed[index] = alternative
            current -= savings
            n_reduced += 1
        exact = size_events(condensed)
        if exact > budget_bytes:
            raise BudgetExceeded(exact, budget_bytes)
        return ReductionResult(condensed, original, exact, n_reduced,
                               sum(protected(e) for e in events), budget_bytes)


def rehydrate(events: list[AgentEvent], archive: ContentAddressedStore) -> list[AgentEvent]:
    restored: list[AgentEvent] = []
    for event in events:
        data = event.payload
        if event.condensed:
            if not isinstance(data, dict) or data.get("_agentwire") != SUMMARY_MARKER:
                raise ArchiveCorruptError("malformed condensed reference")
            sha = data.get("sha256")
            content = archive.get(sha)
            if len(content) != data.get("original_bytes"):
                raise ArchiveCorruptError("archived blob size mismatch")
            restored.append(replace(event, payload=json.loads(content), condensed=False))
        else:
            restored.append(event)
    return restored
