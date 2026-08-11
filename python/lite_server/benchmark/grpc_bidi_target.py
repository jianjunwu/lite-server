"""gRPC bidi transport target for benchmark engine (批次 3 Part A, plan §7.1).

Maps the ``BidiStream`` RPC onto the ``bidi_session`` IO contract:
``BidiChunk{open/data/close}`` outbound; inbound ``data`` → ``Data``,
``close`` → ``Done``, ``error`` → ``Error``.  An RPC that ends without a
``close`` frame (EOF) is an abnormal session end → ``Error``.

Import layering: ``liteserver_pb2`` is module-level (protobuf is a runtime
dependency); ``grpc`` / ``liteserver_pb2_grpc`` stay function-local (grpcio
is a dev dependency) — the IO binds ``grpc`` once at construction, which
only happens inside a gRPC session.
"""

from __future__ import annotations

from typing import Awaitable, Callable

from lite_server.benchmark.bidi_session import (
    Data,
    Done,
    Error,
    Pacing,
    run_bidi_session,
)
from lite_server.benchmark.benchmark import BidiSessionRecord
from lite_server.benchmark.grpc_target import _map_rpc_error
from lite_server.proto import liteserver_pb2


def grpc_bidi_session(
    channel,
    model: str,
    *,
    version: str | None = None,
    pacing: Pacing,
    idle_timeout: float | None = None,
    metadata: tuple[tuple[str, str], ...] | None = None,
) -> Callable[[list], Awaitable[BidiSessionRecord]]:
    """Build a bidi session runner for the gRPC ``BidiStream`` RPC.

    Args:
        channel: A ``grpc.aio.Channel`` instance.
        model: Model name (``BidiOpen.model_name``).
        version: Optional model version (empty string = server default).
        pacing: Producer pacing (lock_step / real_time / speedup).
        idle_timeout: Per-frame idle budget in seconds (orchestrator-side).
        metadata: Per-call gRPC metadata (header passthrough).  ``None`` →
            the call is made without a ``metadata`` kwarg (backward compat).

    Returns:
        ``session(script) -> BidiSessionRecord`` for
        ``BenchmarkEngine.run_bidi()``.  One RPC per session.
    """

    async def session(script: list) -> BidiSessionRecord:
        import grpc  # noqa: F401  (lazy; dev-only dependency)
        from lite_server.proto import liteserver_pb2_grpc

        stub = liteserver_pb2_grpc.LiteServerStub(channel)
        if metadata is not None:
            call = stub.BidiStream(metadata=metadata)
        else:
            call = stub.BidiStream()
        try:
            return await run_bidi_session(
                _GrpcBidiIO(call, model, version), script,
                pacing=pacing, idle_timeout=idle_timeout,
            )
        finally:
            call.cancel()

    return session


class _GrpcBidiIO:
    """``bidi_session`` IO over a ``BidiStream`` call (write/read)."""

    def __init__(self, call, model: str, version: str | None):
        import grpc  # lazy; bound once per session (construction is grpc-only)

        self._call = call
        self._model = model
        self._version = version or ""
        self._rpc_error = grpc.aio.AioRpcError
        self._eof = grpc.aio.EOF

    async def send_open(self, payload: bytes) -> None:
        await self._write(liteserver_pb2.BidiChunk(
            open=liteserver_pb2.BidiOpen(
                model_name=self._model, version=self._version,
                initial_data=payload,
            ),
        ))

    async def send_chunk(self, chunk: bytes) -> None:
        await self._write(liteserver_pb2.BidiChunk(
            data=liteserver_pb2.BidiData(data=chunk),
        ))

    async def send_close(self) -> None:
        await self._write(liteserver_pb2.BidiChunk(
            close=liteserver_pb2.BidiClose(),
        ))
        await self._call.done_writing()

    async def _write(self, chunk) -> None:
        try:
            await self._call.write(chunk)
        except self._rpc_error as e:
            raise _map_rpc_error(e) from e

    async def recv(self):
        while True:
            try:
                resp = await self._call.read()
            except self._rpc_error as e:
                raise _map_rpc_error(e) from e
            if resp == self._eof:
                return Error("RPC ended without close frame")
            kind = resp.WhichOneof("payload")
            if kind == "data":
                return Data(bytes(resp.data.data))
            if kind == "close":
                return Done()
            if kind == "error":
                return Error(resp.error.message)
            # "open" (or empty) from server is not expected — tolerate
