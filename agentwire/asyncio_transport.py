"""Real TCP/asyncio integration for AWF1, with bounded backpressure and timers.

Use inside a reliable, authenticated TCP/TLS session. Dictionary provisioning and
message replay/acknowledgment across reconnects are intentionally out of scope.
"""
from __future__ import annotations

import asyncio
from collections import deque
from typing import Deque

from .stream import StreamEncoder, StreamSessionDecoder
from .transport import BoundedBatcher, FrameCodec, Message, StreamDecoder, _record_size


class AsyncBatchedSender:
    """Batch bytes/count/deadline using a real timer, and await writer.drain().

    send() backpressures on a bounded asyncio queue. close() drains the queue,
    flushes the final batch, and leaves writer ownership with the caller.
    """
    def __init__(self, writer: asyncio.StreamWriter, codec: FrameCodec,
                 dictionary_version: int = 0, max_batch_messages: int = 32,
                 max_batch_bytes: int = 65536, max_delay_ms: int = 25,
                 queue_capacity: int = 64):
        if queue_capacity < 1:
            raise ValueError("queue_capacity must be positive")
        self.writer = writer
        self.batcher = BoundedBatcher(codec, dictionary_version, max_batch_messages,
                                      max_batch_bytes, max_delay_ms)
        self.queue: asyncio.Queue[Message | None] = asyncio.Queue(maxsize=queue_capacity)
        self.task: asyncio.Task | None = None
        self.closed = False
        self.frames_sent = 0
        self.messages_accepted = 0

    def start(self):
        if self.task is not None:
            raise RuntimeError("sender already started")
        self.task = asyncio.create_task(self._pump())
        return self

    async def __aenter__(self):
        return self.start()

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def _ms(self) -> int:
        return int(asyncio.get_running_loop().time() * 1000)

    async def send(self, message: Message):
        if self.closed or self.task is None:
            raise RuntimeError("sender not running")
        if self.task.done():
            self.task.result()
            raise RuntimeError("sender exited")
        # Validate length BEFORE enqueue to avoid failure inside background pump.
        if _record_size(message) > self.batcher.max_batch_bytes:
            raise ValueError("message exceeds max_batch_bytes")
        await self.queue.put(message)
        self.messages_accepted += 1

    async def _deliver(self, frames: list[bytes]):
        for frame in frames:
            self.writer.write(frame)
            await self.writer.drain()
            self.frames_sent += 1

    async def _pump(self):
        while True:
            now = self._ms()
            if self.batcher._since_ms is None:
                item = await self.queue.get()
            else:
                timeout = max(0.0,(self.batcher._since_ms + self.batcher.max_delay_ms - now)/1000)
                try:
                    item = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    await self._deliver(self.batcher.poll(self._ms()))
                    continue
            if item is None:
                if frame := self.batcher.flush():
                    await self._deliver([frame])
                return
            await self._deliver(self.batcher.add(item, self._ms()))

    async def close(self):
        if self.task is None:
            return
        if not self.closed:
            self.closed = True
            await self.queue.put(None)
        await self.task


class AsyncStreamSender:
    """AWC1 sender: each message is compressed onto the connection's Zstd stream
    and written immediately. No batching timer and no added latency;
    writer.drain() still applies TCP backpressure.

    encode() and write() run without an await between them, so concurrent
    send() calls cannot interleave or reorder bytes on the stream.
    """
    def __init__(self, writer: asyncio.StreamWriter, encoder: StreamEncoder):
        self.writer = writer
        self.encoder = encoder
        self.frames_sent = 0
        self.messages_accepted = 0
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def send(self, message: Message):
        if self.closed:
            raise RuntimeError("sender closed")
        self.writer.write(self.encoder.encode([message]))
        self.frames_sent += 1
        self.messages_accepted += 1
        await self.writer.drain()

    async def close(self):
        if not self.closed:
            self.closed = True
            await self.writer.drain()
            self.encoder.close()


class AsyncFramedReceiver:
    """Read individual original messages from an ordered byte stream.

    Pass decoder=StreamSessionDecoder(...) to read AWC1 instead of AWF1.
    """
    def __init__(self, reader: asyncio.StreamReader, codec: FrameCodec | None = None,
                 read_size: int = 4096,
                 decoder: StreamDecoder | StreamSessionDecoder | None = None):
        if read_size < 1:
            raise ValueError("read_size must be positive")
        if (codec is None) == (decoder is None):
            raise ValueError("pass exactly one of codec or decoder")
        self.reader = reader
        self.decoder = decoder if decoder is not None else StreamDecoder(codec)
        self.read_size = read_size
        self.messages: Deque[Message] = deque()
        self.ended = False

    async def recv(self) -> Message | None:
        if self.messages:
            return self.messages.popleft()
        if self.ended:
            return None
        while True:
            chunk = await self.reader.read(self.read_size)
            if not chunk:
                self.decoder.finish()
                self.ended = True
                return None
            self.messages.extend(self.decoder.feed(chunk))
            if self.messages:
                return self.messages.popleft()


async def tcp_roundtrip_demo(registry, messages: list[Message], stream: bool = False) -> dict:
    """Start real loopback TCP server; send messages, receive ACK.

    stream=False: AWF1 bounded batches. stream=True: AWC1, one frame per message.
    """
    codec = FrameCodec(registry)
    version = 1 if registry.manifest() else 0
    wire = {"bytes": 0}
    received: list[Message] = []
    done = asyncio.get_running_loop().create_future()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            if stream:
                receiver = AsyncFramedReceiver(reader, read_size=7,
                                               decoder=StreamSessionDecoder(registry))
            else:
                receiver = AsyncFramedReceiver(reader, codec, read_size=7)
            while (message := await receiver.recv()) is not None:
                received.append(message)
            writer.write(codec.encode([Message(0,b"ACK:"+str(len(received)).encode())],0))
            await writer.drain()
            if not done.done(): done.set_result(True)
        except Exception as exc:
            if not done.done(): done.set_exception(exc)
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handler,"127.0.0.1",0)
    try:
        host,port=server.sockets[0].getsockname()[:2]
        reader,writer=await asyncio.open_connection(host,port)
        raw_write = writer.write
        def counting_write(data):
            wire["bytes"] += len(data)
            raw_write(data)
        writer.write = counting_write
        if stream:
            sender = AsyncStreamSender(writer, StreamEncoder(registry, dictionary_version=version))
        else:
            sender = AsyncBatchedSender(writer,codec,dictionary_version=version,
                                        max_batch_messages=8,max_batch_bytes=16384,
                                        max_delay_ms=12,queue_capacity=8)
        async with sender:
            for i,msg in enumerate(messages):
                await sender.send(msg)
                if i%7==0:
                    await asyncio.sleep(0.002)
        # Send TCP FIN in client -> server direction while still reading reply.
        writer.write_eof()
        ack = await AsyncFramedReceiver(reader,codec).recv()
        await done
        writer.close()
        await writer.wait_closed()
        assert received == messages
        assert ack is not None and ack.payload==b"ACK:"+str(len(messages)).encode()
        return {"mode":"AWC1 stream" if stream else "AWF1 batch",
                "messages":len(messages),"frames":sender.frames_sent,
                "payload_bytes":sum(len(m.payload) for m in messages),
                "wire_bytes":wire["bytes"],"bytes_exact":True,"ack":ack.payload.decode()}
    finally:
        server.close()
        await server.wait_closed()
