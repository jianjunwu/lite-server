"""h2 bidi transport target for benchmark engine (批次 3 Part A, plan §7.1).

Maps the HTTP/2 ``/bidi`` endpoint (docs/http-bidi.md) onto the
``bidi_session`` IO contract.  Framing is LPM (1B flag=0 + 4B BE length +
``BidiChunk`` protobuf); the client speaks prior-knowledge h2c (no upgrade
negotiation, no TLS — benchmark tooling targets local servers).

One connection per session (parity with the WS transport); h2 stream
multiplexing is a possible future refinement.

Import layering: ``liteserver_pb2`` is module-level (protobuf is a runtime
dependency); ``h2`` stays function-local (dev dependency), imported once
per session in ``session()``/``_reader_loop``.
"""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable
from urllib.parse import urlparse

from lite_server.analyzer.bidi_session import (
    Data,
    Done,
    Error,
    Pacing,
    run_bidi_session,
)
from lite_server.analyzer.benchmark import (
    BidiSessionRecord,
    RequestConnectError,
    RequestError,
    RequestStatusError,
)
from lite_server.proto import liteserver_pb2

MAX_LPM_FRAME = 16 * 1024 * 1024  # 16 MiB (docs/http-bidi.md)


def encode_lpm(payload: bytes) -> bytes:
    """LPM frame: 1B flag (must be 0) + 4B big-endian length + payload."""
    return b"\x00" + len(payload).to_bytes(4, "big") + payload


class LpmDecoder:
    """Stateful LPM frame decoder: feed bytes, get complete frame payloads."""

    def __init__(self):
        self._buf = bytearray()

    def feed(self, data: bytes) -> list[bytes]:
        self._buf.extend(data)
        frames = []
        while len(self._buf) >= 5:
            flag = self._buf[0]
            length = int.from_bytes(self._buf[1:5], "big")
            if flag != 0:
                raise ValueError(f"LPM flag must be 0, got {flag}")
            if length > MAX_LPM_FRAME:
                raise ValueError(f"LPM frame too large: {length}")
            if len(self._buf) < 5 + length:
                break
            frames.append(bytes(self._buf[5:5 + length]))
            del self._buf[:5 + length]
        return frames


def h2_bidi_session(
    url: str,
    *,
    pacing: Pacing,
    idle_timeout: float | None = None,
) -> Callable[[list], Awaitable[BidiSessionRecord]]:
    """Build a bidi session runner for the h2 ``/bidi`` endpoint.

    Args:
        url: Full URL (``http://host:port/v2/models/{m}/bidi``); h2c only.
        pacing: Producer pacing (lock_step / real_time / speedup).
        idle_timeout: Per-frame idle budget in seconds (orchestrator-side).

    Returns:
        ``session(script) -> BidiSessionRecord`` for
        ``BenchmarkEngine.run_bidi()``.  One h2 connection per session.
    """
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 80
    path = parsed.path

    async def session(script: list) -> BidiSessionRecord:
        import h2.config
        import h2.connection

        try:
            reader, writer = await asyncio.open_connection(host, port)
        except OSError as e:
            raise RequestConnectError() from e

        conn = h2.connection.H2Connection(
            config=h2.config.H2Configuration(
                client_side=True, header_encoding="utf-8",
            ),
        )
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()

        queue: asyncio.Queue = asyncio.Queue()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, [
            (":method", "POST"),
            (":scheme", "http"),
            (":authority", f"{host}:{port}"),
            (":path", path),
            ("content-type", "application/x-lite-bidi"),
        ], end_stream=False)
        writer.write(conn.data_to_send())
        await writer.drain()

        read_task = asyncio.create_task(_reader_loop(reader, writer, conn, queue))
        try:
            return await run_bidi_session(
                _H2BidiIO(conn, stream_id, writer, queue), script,
                pacing=pacing, idle_timeout=idle_timeout,
            )
        finally:
            read_task.cancel()
            try:
                await read_task
            except (asyncio.CancelledError, Exception):
                pass
            writer.close()

    return session


async def _reader_loop(reader, writer, conn, queue) -> None:
    """Socket → h2 events → LPM frames → bidi frames into *queue*."""
    import h2.events

    decoder = LpmDecoder()
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                queue.put_nowait(Error("h2 connection closed by peer"))
                return
            for ev in conn.receive_data(data):
                if isinstance(ev, h2.events.ResponseReceived):
                    status = dict(ev.headers).get(":status")
                    if status is not None and status != "200":
                        queue.put_nowait(RequestStatusError(int(status)))
                        return
                elif isinstance(ev, h2.events.DataReceived):
                    conn.acknowledge_received_data(
                        ev.flow_controlled_length, ev.stream_id,
                    )
                    try:
                        payloads = decoder.feed(ev.data)
                    except ValueError as e:
                        queue.put_nowait(Error(f"LPM decode: {e}"))
                        return
                    for payload in payloads:
                        chunk = liteserver_pb2.BidiChunk.FromString(payload)
                        frame = _map_bidi_chunk(chunk)
                        if frame is not None:
                            queue.put_nowait(frame)
                            if isinstance(frame, (Done, Error)):
                                return
                elif isinstance(ev, h2.events.StreamEnded):
                    queue.put_nowait(Error("h2 stream ended without close frame"))
                    return
                elif isinstance(ev, h2.events.StreamReset):
                    queue.put_nowait(
                        Error(f"h2 stream reset: {ev.error_code}"),
                    )
                    return
            out = conn.data_to_send()
            if out:
                writer.write(out)
                await writer.drain()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        queue.put_nowait(Error(f"transport: {e}"))


def _map_bidi_chunk(chunk):
    """BidiChunk → bidi frame; None for tolerated payloads (e.g. open)."""
    kind = chunk.WhichOneof("payload")
    if kind == "data":
        return Data(bytes(chunk.data.data))
    if kind == "close":
        return Done()
    if kind == "error":
        return Error(chunk.error.message)
    return None


class _H2BidiIO:
    """``bidi_session`` IO over an h2 stream."""

    def __init__(self, conn, stream_id, writer, queue):
        self._conn = conn
        self._stream_id = stream_id
        self._writer = writer
        self._queue = queue

    async def send_open(self, payload: bytes) -> None:
        # h2: model/version are URL-authoritative; only initial_data matters
        await self._send(liteserver_pb2.BidiChunk(
            open=liteserver_pb2.BidiOpen(initial_data=payload),
        ))

    async def send_chunk(self, chunk: bytes) -> None:
        await self._send(liteserver_pb2.BidiChunk(
            data=liteserver_pb2.BidiData(data=chunk),
        ))

    async def send_close(self) -> None:
        await self._send(liteserver_pb2.BidiChunk(
            close=liteserver_pb2.BidiClose(),
        ), end_stream=True)

    async def recv(self):
        item = await self._queue.get()
        if isinstance(item, RequestError):
            raise item  # classified (e.g. non-200 status) — keep the bucket
        return item

    async def _send(self, chunk, *, end_stream: bool = False) -> None:
        self._conn.send_data(
            self._stream_id, encode_lpm(chunk.SerializeToString()),
            end_stream=end_stream,
        )
        self._writer.write(self._conn.data_to_send())
        await self._writer.drain()
