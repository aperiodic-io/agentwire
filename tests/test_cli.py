from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import asdict

from agentwire.context import AgentEvent, encode_json, size_events
from agentwire.fixtures import context_events, training_samples


def call(*args):
    return subprocess.run([sys.executable,"-m","agentwire",*map(str,args)],
                          capture_output=True,text=True,check=True)


def test_pack_unpack_exact_bytes_no_dict(tmp_path):
    src=tmp_path/"original.jsonl"
    src.write_bytes(b'{"hello":1}\r\n' + 'Má lta 🐚'.encode() + b'\n' + b'raw\x00\xff binary')
    frame=tmp_path/"stream.awf"
    recovered=tmp_path/"copy.jsonl"
    call("pack","--input",src,"--output",frame)
    call("unpack","--input",frame,"--output",recovered)
    assert src.read_bytes()==recovered.read_bytes()


def test_pack_unpack_exact_bytes_with_dict(tmp_path, dictionaries):
    src=tmp_path/"original.jsonl"
    src.write_bytes(b"".join(b'{"service":"agent-worker","status":"success"}'*20+b'\n' for _ in range(100)))
    dictionary=tmp_path/"dict.zdict"
    dictionary.write_bytes(dictionaries[0])
    frame=tmp_path/"stream.awf"
    recovered=tmp_path/"copy.jsonl"
    call("pack","--input",src,"--output",frame,"--dictionary",dictionary)
    call("unpack","--input",frame,"--output",recovered,"--dictionary",dictionary)
    assert src.read_bytes()==recovered.read_bytes()


def test_cli_context_reduce_restore(tmp_path):
    events=context_events()
    source=tmp_path/"events.jsonl"
    source.write_bytes(b"".join(encode_json(asdict(e))+b"\n" for e in events))
    output=tmp_path/"reduced.json"
    archive=tmp_path/"archive"
    restored=tmp_path/"restored.jsonl"
    call("reduce","--input",source,"--output",output,"--archive-dir",archive,
         "--budget-bytes",size_events(events)//5)
    call("restore","--input",output,"--output",restored,"--archive-dir",archive)
    assert [AgentEvent(**json.loads(x)) for x in restored.read_bytes().splitlines()] == events


def test_cli_demo():
    assert "PASS" in call("demo").stdout


def test_cli_pipeline_demo():
    assert "Full pipeline: PASS" in call("pipeline-demo").stdout


def test_pack_unpack_stream_mode(tmp_path, dictionaries):
    src=tmp_path/"original.jsonl"
    src.write_bytes(b"".join(b'{"service":"agent-worker","n":%d}\n' % i for i in range(200)) + b"tail\x00\xff")
    dictionary=tmp_path/"dict.zdict"
    dictionary.write_bytes(dictionaries[0])
    for extra in ([], ["--dictionary",dictionary]):
        packed=tmp_path/"stream.awc"
        recovered=tmp_path/"copy.jsonl"
        call("pack","--stream","--input",src,"--output",packed,*extra)
        assert packed.read_bytes()[:4]==b"AWC1"
        call("unpack","--input",packed,"--output",recovered,*extra)
        assert src.read_bytes()==recovered.read_bytes()


def test_cli_tcp_demo_stream():
    assert "AWC1 stream" in call("tcp-demo","--stream").stdout
