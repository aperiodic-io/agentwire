"""Run with: python -m agentwire {demo,train,pack,unpack,reduce,restore,bench}."""
from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from dataclasses import asdict
from pathlib import Path

from .benchmark import run_benchmark
from .asyncio_transport import tcp_roundtrip_demo
from .context import (AgentEvent, ContentAddressedStore, ContextReducer,
                      encode_json, rehydrate)
from .fixtures import context_events, training_samples, workload
from .stream import STREAM_MAGIC, StreamEncoder, StreamSessionDecoder
from .transport import BoundedBatcher, DictionaryRegistry, FrameCodec, Message, StreamDecoder
from .zstd import train_dictionary


def registry(args) -> DictionaryRegistry:
    reg = DictionaryRegistry()
    if getattr(args, "dictionary", None):
        reg.register(args.dictionary_version, Path(args.dictionary).read_bytes())
    return reg


def line_bytes(path: str) -> list[bytes]:
    return Path(path).read_bytes().splitlines(keepends=True)


def pack(args):
    data = line_bytes(args.input)
    if args.stream:
        encoder = StreamEncoder(registry(args),
                                dictionary_version=args.dictionary_version if args.dictionary else 0,
                                window_log=args.window_log)
        frames = [encoder.encode([Message(i, chunk)]) for i, chunk in enumerate(data)]
        encoder.close()
        Path(args.output).write_bytes(b"".join(frames))
        print(f"Packed {len(data)} messages (AWC1 stream, one frame each): "
              f"{sum(map(len,data)):,} -> {sum(map(len,frames)):,} bytes")
        return
    codec = FrameCodec(registry(args))
    batcher = BoundedBatcher(codec, dictionary_version=args.dictionary_version if args.dictionary else 0,
                             max_messages=args.batch_messages, max_batch_bytes=args.batch_bytes,
                             max_delay_ms=args.delay_ms)
    frames = []
    for i, chunk in enumerate(data):
        frames.extend(batcher.add(Message(i, chunk), i * args.interval_ms))
    final = batcher.flush()
    if final:
        frames.append(final)
    Path(args.output).write_bytes(b"".join(frames))
    print(f"Packed {len(data)} messages: {sum(map(len,data)):,} -> {sum(map(len,frames)):,} bytes, {len(frames)} frames")


def unpack(args):
    with Path(args.input).open("rb") as stream:
        magic = stream.read(len(STREAM_MAGIC))
    if magic == STREAM_MAGIC:
        decoder = StreamSessionDecoder(registry(args), max_window_log=args.window_log)
    else:
        decoder = StreamDecoder(FrameCodec(registry(args)))
    count = 0
    with Path(args.output).open("wb") as file:
        with Path(args.input).open("rb") as stream:
            while segment := stream.read(4096):
                for msg in decoder.feed(segment):
                    file.write(msg.payload)
                    count += 1
    decoder.finish()
    print(f"Unpacked {count} messages into {args.output}")


def train(args):
    samples = line_bytes(args.input)
    dic = train_dictionary(samples, capacity=args.size)
    Path(args.output).write_bytes(dic)
    print(f"Trained Zstandard dictionary: {len(dic)} bytes -> {args.output}")


def _events_from_lines(path: str) -> list[AgentEvent]:
    return [AgentEvent(**json.loads(line)) for line in line_bytes(path) if line.strip()]


def reduce(args):
    events = _events_from_lines(args.input)
    archive = ContentAddressedStore(args.archive_dir)
    result = ContextReducer(archive, args.preview_chars).reduce(events, args.budget_bytes)
    Path(args.output).write_bytes(encode_json([asdict(event) for event in result.events]))
    print(f"Context {result.original_bytes:,} -> {result.context_bytes:,} bytes; "
          f"condensed {result.condensed_count}, protected {result.protected_count}")


def restore(args):
    items = json.loads(Path(args.input).read_bytes())
    events = [AgentEvent(**item) for item in items]
    restored = rehydrate(events, ContentAddressedStore(args.archive_dir))
    with Path(args.output).open("wb") as out:
        for event in restored:
            out.write(encode_json(asdict(event)) + b"\n")
    print(f"Rehydrated {len(restored)} context events to {args.output}")


def demo():
    with tempfile.TemporaryDirectory() as scratch:
        root = Path(scratch)
        samples = training_samples()
        dic = train_dictionary(samples, 8192)
        registry = DictionaryRegistry()
        registry.register(1,dic)
        codec = FrameCodec(registry)
        batcher = BoundedBatcher(codec, 1, max_messages=8, max_batch_bytes=8192, max_delay_ms=25)
        payloads = [b'{"tool":"search","ok":true,"items":[1,2,3]}',
                    b'{"tool":"fetch","ok":true,"items":[4,5,6]}',
                    b'\x00\xff binary \x00 message',
                    b'Agent generated paragraph. ' * 70]
        original = [Message(i, value, kind=i % 3) for i,value in enumerate(payloads)]
        frames = []
        for i, message in enumerate(original):
            frames.extend(batcher.add(message, i * 5))
        if frame:=batcher.flush():
            frames.append(frame)
        reader = StreamDecoder(codec)
        stream = b"".join(frames)
        recovered = []
        for pos in range(0,len(stream),7):
            recovered.extend(reader.feed(stream[pos:pos+7]))
        reader.finish()
        assert recovered == original
        events = context_events()
        reducer = ContextReducer(ContentAddressedStore(root / "archive"))
        original_bytes = len(encode_json([asdict(e) for e in events]))
        reduced = reducer.reduce(events, original_bytes//5)
        assert rehydrate(reduced.events, reducer.archive) == events
        print("AgentWire end-to-end demo: PASS")
        print(f"  lossless transport: {sum(map(len,payloads))} payload bytes -> {len(stream)} wire bytes across {len(frames)} frames")
        print(f"  strict context protection: {reduced.protected_count} events protected")
        print(f"  lossy context: {reduced.original_bytes} -> {reduced.context_bytes} bytes, {reduced.condensed_count} archived")
        print("  rehydration: every archived payload recovered exactly")


def main():
    parser = argparse.ArgumentParser(prog="agentwire", description="AgentWire agent transport/context PoC")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("demo",help="run in-memory transport/context E2E verification")
    tcp = sub.add_parser("tcp-demo",help="run real loopback TCP sender and receiver with ACK")
    tcp.add_argument("--stream",action="store_true",help="use AWC1 per-connection stream instead of AWF1 batches")
    sub.add_parser("pipeline-demo",help="combine context reduction, TCP transport and archive restoration")
    b = sub.add_parser("bench",help="run synthetic, fully verified wire/context benchmarks")
    b.add_argument("--output-dir",default="results")
    b.add_argument("--messages", type=int,default=240)
    b.add_argument("--repeats",type=int,default=2)
    lb = sub.add_parser("bench-local",help="benchmark on real local Claude Code / Codex session logs (aggregates only)")
    lb.add_argument("--output-dir",default="results/local")
    lb.add_argument("--claude-dir",default=None,help="default ~/.claude/projects; pass '' to skip")
    lb.add_argument("--codex-dir",default=None,help="default ~/.codex/sessions; pass '' to skip")
    lb.add_argument("--train-fraction",type=float,default=0.6)
    lb.add_argument("--dict-size",type=int,default=65536)
    lb.add_argument("--max-test-sessions",type=int,default=None)
    lb.add_argument("--exclude-active-minutes",type=float,default=15.0,help="skip logs modified this recently")
    lb.add_argument("--no-verify",action="store_true",help="skip byte-exact decode check (faster)")
    lb.add_argument("--save-dictionary-dir",default=None,help="also write trained dictionaries (contain transcript fragments!)")
    t = sub.add_parser("train",help="train Zstd dictionary from JSON-lines samples")
    t.add_argument("--input",required=True);t.add_argument("--output",required=True);t.add_argument("--size",type=int,default=8192)
    p = sub.add_parser("pack",help="losslessly frame input file's newline-delimited bytes")
    u = sub.add_parser("unpack",help="decode frames to original newline-delimited bytes")
    for cmd in (p,u):
        cmd.add_argument("--input",required=True);cmd.add_argument("--output",required=True)
        cmd.add_argument("--dictionary");cmd.add_argument("--dictionary-version",type=int,default=1)
        cmd.add_argument("--window-log",type=int,default=20,help="AWC1 Zstd window (pack) / max accepted window (unpack)")
    p.add_argument("--stream",action="store_true",help="AWC1: one Zstd stream, one frame per line, no batching delay")
    p.add_argument("--batch-messages",type=int,default=32)
    p.add_argument("--batch-bytes",type=int,default=65536)
    p.add_argument("--delay-ms",type=int,default=25)
    p.add_argument("--interval-ms",type=int,default=2,help="synthetic CLI per-line arrival interval")
    r = sub.add_parser("reduce",help="selectively condense low-priority observations to a byte budget")
    r.add_argument("--input",required=True);r.add_argument("--output",required=True)
    r.add_argument("--archive-dir",required=True);r.add_argument("--budget-bytes",type=int,required=True)
    r.add_argument("--preview-chars",type=int,default=96)
    x = sub.add_parser("restore",help="rehydrate a previously reduced event collection")
    x.add_argument("--input",required=True);x.add_argument("--output",required=True);x.add_argument("--archive-dir",required=True)
    args=parser.parse_args()
    if args.command == "demo": demo()
    elif args.command == "tcp-demo":
        reg=DictionaryRegistry()
        reg.register(1,train_dictionary(training_samples(),8192))
        outcome=asyncio.run(tcp_roundtrip_demo(reg,workload("mixed-agent",80).messages,stream=args.stream))
        print("TCP roundtrip: PASS",json.dumps(outcome))
    elif args.command == "pipeline-demo":
        events=context_events(15)
        with tempfile.TemporaryDirectory() as tempdir:
            reducer=ContextReducer(ContentAddressedStore(Path(tempdir)/"archive"))
            from .context import size_events
            reduced=reducer.reduce(events,size_events(events)//5)
            data=[Message(i,encode_json(asdict(event)),kind=1) for i,event in enumerate(reduced.events)]
            reg=DictionaryRegistry()
            reg.register(1,train_dictionary(training_samples(),8192))
            result=asyncio.run(tcp_roundtrip_demo(reg,data))
            assert rehydrate(reduced.events,reducer.archive)==events
            print(f"Full pipeline: PASS, {len(events)} events, "
                  f"context {reduced.original_bytes} -> {reduced.context_bytes} bytes, "
                  f"{result['frames']} network frames, archived events restored")
    elif args.command == "bench":
        report=run_benchmark(args.output_dir,args.messages,args.repeats)
        print(f"Benchmarked {len(report['transport'])} scenarios. Results: {args.output_dir}/RESULTS.md")
        print("Context:", json.dumps(report['context']))
    elif args.command == "bench-local":
        from .localbench import default_sources, run_local_benchmark
        sources=default_sources()
        for key,arg in (("claude-code",args.claude_dir),("codex",args.codex_dir)):
            if arg is not None:
                if arg: sources[key]=Path(arg).expanduser()
                else: sources.pop(key)
        run_local_benchmark(args.output_dir,sources,args.train_fraction,args.dict_size,
                            args.max_test_sessions,exclude_active_minutes=args.exclude_active_minutes,
                            verify=not args.no_verify,save_dictionary_dir=args.save_dictionary_dir,
                            progress=lambda m: print(m,flush=True))
        print(f"Results: {args.output_dir}/RESULTS.md")
    elif args.command == "train": train(args)
    elif args.command == "pack": pack(args)
    elif args.command == "unpack": unpack(args)
    elif args.command == "reduce": reduce(args)
    elif args.command == "restore": restore(args)


if __name__ == "__main__":
    main()
