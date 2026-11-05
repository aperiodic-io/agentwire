from __future__ import annotations

from dataclasses import asdict

import pytest

from agentwire.context import (
    AgentEvent, ArchiveCorruptError, BudgetExceeded, ContentAddressedStore,
    ContextReducer, SUMMARY_MARKER, encode_json, protected, rehydrate, size_events
)
from agentwire.fixtures import context_events


def test_context_reduction_protects_and_rehydrates(tmp_path):
    events=context_events()
    store=ContentAddressedStore(tmp_path)
    output=ContextReducer(store).reduce(events, size_events(events)//5)
    assert output.condensed_count == 24
    assert output.context_bytes <= size_events(events)//5
    assert output.protected_count==5
    assert rehydrate(output.events,store)==events
    for original,reduced in zip(events,output.events):
        if original.kind in {"tool_call","state_patch","instruction"} or original.role=="user":
            assert reduced == original
        if original.id=="failure": assert reduced==original
        if reduced.condensed:
            assert reduced.payload["_agentwire"]==SUMMARY_MARKER
            assert "original_bytes" in reduced.payload
            assert "sha256" in reduced.payload


def test_explicit_low_priority_only(tmp_path):
    events=[AgentEvent("important", "tool", "tool_result", "text "*1000),
            AgentEvent("allowed", "tool", "observation", "text "*1000, "low")]
    out=ContextReducer(ContentAddressedStore(tmp_path)).reduce(events, 6000)
    assert out.events[0]==events[0]
    assert out.events[1].condensed


@pytest.mark.parametrize("event", [
    AgentEvent("x","assistant","tool_call",{"arguments":{"dangerous": "keep"}},"low"),
    AgentEvent("x","assistant","state_patch",{"balance":150},"low"),
    AgentEvent("x","user","message","never condense my input","low"),
    AgentEvent("x","system","instruction","keep instruction","low"),
    AgentEvent("x","tool","tool_result",{"status":503,"body":"failed"*1000},"low"),
    AgentEvent("x","tool","tool_result",{"ok":False,"body":"failed"*1000},"low"),
    AgentEvent("x","tool","tool_result",{"error":"bad request","body":"failed"*1000},"low"),
    AgentEvent("x","tool","tool_result","protect pinned", "low", True),
])
def test_protection_fail_closed(event):
    assert protected(event)


def test_no_silent_loss_when_budget_impossible(tmp_path):
    events=[AgentEvent("critical","assistant","tool_call",{"payload":"z"*3000},"critical")]
    with pytest.raises(BudgetExceeded):
        ContextReducer(ContentAddressedStore(tmp_path)).reduce(events,250)


def test_no_savings_does_not_archive(tmp_path):
    events=[AgentEvent("tiny","tool","tool_result","x", "low")]
    with pytest.raises(BudgetExceeded):
        ContextReducer(ContentAddressedStore(tmp_path)).reduce(events,10)
    assert not [p for p in tmp_path.rglob("*") if p.is_file()]


def test_archive_corruption_detected(tmp_path):
    store=ContentAddressedStore(tmp_path)
    payload=b"test original"
    sha=store.put(payload)
    path=tmp_path/sha[:2]/sha[2:]
    path.write_bytes(b"test tampered")
    with pytest.raises(ArchiveCorruptError):
        store.get(sha)


def test_marker_spoof_does_not_trigger_rehydration(tmp_path):
    event=AgentEvent("spoof","tool","tool_result",{"_agentwire":SUMMARY_MARKER,"sha256":"fake"})
    assert rehydrate([event],ContentAddressedStore(tmp_path))==[event]


def test_duplicate_ids_rejected(tmp_path):
    x=AgentEvent("x","tool","tool_result","small", "low")
    with pytest.raises(ValueError):
        ContextReducer(ContentAddressedStore(tmp_path)).reduce([x,x],100000)


def test_json_nonfinite_numbers_rejected():
    with pytest.raises(ValueError):
        AgentEvent("x","tool","tool_result",float("nan"))


def test_archive_roundtrip_unicode_and_exact_payload(tmp_path):
    event=AgentEvent("unicode", "tool","tool_result",
                     {"unicode":"Ärv íztűrő tükörfúrógép 🦎", "array":[1,2,3], "big":"ß"*800}, "low")
    reducer=ContextReducer(ContentAddressedStore(tmp_path))
    output=reducer.reduce([event],550)
    assert output.events[0].condensed
    assert rehydrate(output.events,reducer.archive)==[event]


def test_budget_is_measured_in_serialized_utf8(tmp_path):
    events=context_events(5)
    budget=size_events(events)
    output=ContextReducer(ContentAddressedStore(tmp_path)).reduce(events,budget)
    assert output.condensed_count==0
    assert size_events(output.events)==budget


def test_archive_stores_compressed_bytes(tmp_path):
    store=ContentAddressedStore(tmp_path)
    raw=b"repeated common observation "*1000
    hash_value=store.put(raw)
    stored=(tmp_path/hash_value[:2]/hash_value[2:]).read_bytes()
    assert stored.startswith(b"AWB1")
    assert len(stored)<len(raw)//5
    assert store.put(raw)==hash_value
    assert store.get(hash_value)==raw


def test_archive_guards_uncompressed_allocation(tmp_path):
    small=ContentAddressedStore(tmp_path,max_blob_bytes=128)
    with pytest.raises(ValueError):
        small.put(b"x"*129)
