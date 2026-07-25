"""Bidirectional streaming test for the ASR example.

Bidi streaming on lite-server runs over **gRPC** — the ``/stream`` WebSocket
path is server-side (stream_predict) only.  This client opens a bidi session,
sends three chunks, and prints every frame the handler emits (on_open,
on_chunk partials, on_close final).

Requires grpcio:  pip install grpcio
"""

import asyncio
import json

import grpc
from lite_server.proto import BidiChunk, BidiData, BidiOpen, BidiClose


async def main() -> None:
    async with grpc.aio.insecure_channel("localhost:8001") as ch:
        bidi = ch.stream_stream(
            "/liteserver.LiteServer/BidiStream",
            request_serializer=BidiChunk.SerializeToString,
            response_deserializer=BidiChunk.FromString,
        )
        call = bidi(timeout=30)

        # open → on_open initial response
        await call.write(BidiChunk(open=BidiOpen(model_name="asr", initial_data=b'{"text": ""}')))
        print("open  :", (await call.read()).data.data.decode())

        # each chunk → on_chunk partial
        for word in ("hello", "world", "test"):
            await call.write(BidiChunk(data=BidiData(data=json.dumps({"text": word}).encode())))
            print("chunk :", (await call.read()).data.data.decode())

        # close → on_close final response
        await call.write(BidiChunk(close=BidiClose()))
        print("close :", (await call.read()).data.data.decode())
        await call.done_writing()


if __name__ == "__main__":
    asyncio.run(main())
