"""gRPC streaming target for benchmark engine (批次 1c, plan §2.5.3/§3.4).

Consumes ``StreamInfer`` / ``DecoupledInfer`` RPCs and yields ``StreamChunk``
instances.  All ``grpc`` / pb2_grpc imports are function-local (grpcio is a
dev dependency, not a package dependency).

Error mapping keeps the four-bucket vocabulary (plan §2.5.3):
``UNAVAILABLE`` → connect, ``DEADLINE_EXCEEDED`` → timeout, every other
non-OK status → ``RequestGrpcError`` (kind ``"status"``).
"""

from __future__ import annotations

import json as _stdlib_json
from typing import AsyncIterator, Callable

from lite_server.analyzer.benchmark import (
    RequestConnectError,
    RequestError,
    RequestGrpcError,
    RequestTimeoutError,
    StreamChunk,
)
from lite_server.analyzer.sse_target import default_chunk_meta


def grpc_stream_target(
    channel,
    model: str,
    *,
    version: str | None = None,
    decoupled: bool = False,
    timeout: float | None = None,
) -> Callable[[dict], AsyncIterator[StreamChunk]]:
    """Build a streaming benchmark target for a gRPC endpoint.

    Args:
        channel: A ``grpc.aio.Channel`` instance.
        model: Model name (``StreamInferRequest.model_name``).
        version: Optional model version (empty string = server default).
        decoupled: When ``True``, call ``DecoupledInfer`` instead of
            ``StreamInfer`` and stop after the ``is_final`` frame.
        timeout: Whole-RPC timeout in seconds (gRPC deadline semantics —
            unlike httpx read timeout this does NOT re-arm per chunk).

    Returns:
        A callable ``target(payload) -> AsyncIterator[StreamChunk]``
        compatible with ``BenchmarkEngine.run_stream()``.  The payload is
        sent as JSON bytes, aligned with the HTTP targets.
    """

    async def target(payload: dict) -> AsyncIterator[StreamChunk]:
        import grpc
        from lite_server.proto import liteserver_pb2, liteserver_pb2_grpc

        stub = liteserver_pb2_grpc.LiteServerStub(channel)
        data = _stdlib_json.dumps(payload).encode()
        if decoupled:
            request: object = liteserver_pb2.DecoupledInferRequest(
                model_name=model, version=version or "", data=data,
            )
            call = stub.DecoupledInfer(request, timeout=timeout)
        else:
            request = liteserver_pb2.StreamInferRequest(
                model_name=model, version=version or "", data=data,
            )
            call = stub.StreamInfer(request, timeout=timeout)

        try:
            async for resp in call:
                raw = resp.data
                yield StreamChunk(
                    data=raw, meta=_bytes_meta(raw), size_bytes=len(raw),
                )
                if decoupled and resp.is_final:
                    break
        except grpc.aio.AioRpcError as e:
            raise _map_rpc_error(e) from e

    return target


def _bytes_meta(raw: bytes) -> dict | None:
    """Extract chunk meta from JSON-object bytes; None for non-JSON/binary."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None
    return default_chunk_meta(text)


def _map_rpc_error(err) -> RequestError:
    """Map a ``grpc.aio.AioRpcError`` to the RequestError four buckets."""
    import grpc

    code = err.code()
    if code == grpc.StatusCode.UNAVAILABLE:
        return RequestConnectError()
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return RequestTimeoutError()
    return RequestGrpcError(f"gRPC {code.name}: {err.details() or ''}")
