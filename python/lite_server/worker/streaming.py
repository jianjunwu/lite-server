"""Streaming responses (chunk sequences) and bidirectional sessions.

Split out of ``inference.py`` (0.7.7 debt-free phase 1) — pure move, no
behavior change. Public import paths through ``inference.py`` are preserved
via its bottom re-export block.
"""

import asyncio
import inspect
import json
import logging
from typing import Any

from lite_server.api import LitAPI
from lite_server.context import Headers, RequestContext, RequestMeta
from lite_server.exceptions import HTTPException
from lite_server.pipeline import Pipeline, collect_metrics, unwrap_response
from lite_server.proto import Request, Response, StreamDone, StreamRequest, StreamResponse
from lite_server.worker.common import (
    _format_exc_brief,
    _get_pipeline,
    _make_stream_chunk,
    _make_stream_done,
    _make_stream_error,
    _meta_from_proto,
    _parse_json_payload,
)

def _serialize_route_chunk(item) -> bytes:
    """One streamed item → wire bytes: bytes verbatim, str utf-8, else JSON."""
    if isinstance(item, (bytes, bytearray)):
        return bytes(item)
    if isinstance(item, str):
        return item.encode()
    return json.dumps(item).encode()


async def _send_route_stream(socket, stream_id: str, resp, ctx: RequestContext,
                             log: logging.Logger) -> None:
    """Stream a route handler's ``StreamingResponse`` over the worker channel.

    Frame sequence mirrors inference streaming, prefixed with a ``start``
    frame carrying the handler-chosen HTTP metadata (status / headers /
    media_type) so the Rust side can build the response head before the
    first body chunk. Sync iterators are pulled via ``asyncio.to_thread``
    so a slow ``next()`` never blocks the worker loop.
    """
    from lite_server.exceptions import HTTPException
    from lite_server.proto.liteserver_pb2 import StreamStart

    # Merge ctx.response_headers (e.g. from Cors): explicit headers win,
    # same priority as Pipeline.finalize().
    headers = dict(ctx.response_headers or {})
    headers.update(resp.headers)
    start = StreamStart(status_code=resp.status_code,
                        media_type=resp.media_type or "text/event-stream")
    for k, v in headers.items():
        start.headers[str(k)] = str(v)
    await socket.send(Response(
        uid=f"stream-start-{stream_id}",
        stream=StreamResponse(stream_id=stream_id, start=start),
    ).SerializeToString())

    content = resp.content
    try:
        if hasattr(content, "__aiter__"):
            async for item in content:
                await socket.send(
                    _make_stream_chunk(stream_id, _serialize_route_chunk(item))
                    .SerializeToString())
        else:
            it = iter(content)
            sentinel = object()
            while True:
                item = await asyncio.to_thread(next, it, sentinel)
                if item is sentinel:
                    break
                await socket.send(
                    _make_stream_chunk(stream_id, _serialize_route_chunk(item))
                    .SerializeToString())
    except HTTPException as e:
        await socket.send(
            _make_stream_error(stream_id, e.detail, error_type=e.error_type,
                               code=e.code, param=e.param).SerializeToString())
        return
    except Exception as e:
        log.warning("route stream %s failed mid-iteration: %s",
                    stream_id, _format_exc_brief(e))
        await socket.send(
            _make_stream_error(stream_id, str(e)).SerializeToString())
        return
    await socket.send(_make_stream_done(stream_id).SerializeToString())


# ---------------------------------------------------------------------------
# Streaming Support
# ---------------------------------------------------------------------------

class _BidiSession:
    """Per-stream state for bidirectional streaming."""

    __slots__ = ("handler", "on_chunk", "on_close", "ctx")

    def __init__(self, handler, on_chunk, on_close, ctx: RequestContext):
        self.handler = handler
        self.on_chunk = on_chunk
        self.on_close = on_close
        self.ctx = ctx


async def _close_bidi_quietly(on_close, ctx, stream_id, log) -> Any:
    """Best-effort bidi ``on_close`` (exception-isolated): balances a
    completed ``on_open`` exactly once — close/cancel, worker shutdown,
    or an abandoned open.

    Returns the ``on_close`` output (or ``None`` on exception / when the
    handler returns ``None``) so the caller can deliver it as a final chunk,
    symmetric with ``on_open`` / ``on_chunk``.
    """
    try:
        return await on_close(ctx=ctx)
    except Exception:
        log.debug("bidi on_close error for %s", stream_id, exc_info=True)
        return None


async def _send_bidi_final_chunk(
    lit_api, ctx: RequestContext, output: Any, stream_id: str, socket, log
) -> None:
    """Encode ``on_close``'s output and send it as a bidi chunk (best-effort).

    Mirrors the ``on_open`` / ``on_chunk`` encode path (postprocess → encode
    → chunk).  Exception-isolated: a failure skips the chunk but the caller
    still sends ``StreamDone``.
    """
    ctx.output = output
    ctx.early = None
    try:
        pipe = _get_pipeline(lit_api)
        await pipe.postprocess(ctx)
    except Exception as e:
        log.warning("bidi on_close encode failed for %s: %s", stream_id, _format_exc_brief(e))
        return
    body, _headers = unwrap_response(ctx.response)
    await socket.send(
        _make_stream_chunk(stream_id, json.dumps(body).encode(), is_final=False).SerializeToString()
    )


async def _send_stream_early(socket, stream_id: str, early_response, lit_api: LitAPI) -> None:
    """Send an early-return response as a stream chunk + StreamDone."""
    body, _headers = unwrap_response(early_response)
    resp_bytes = json.dumps(body).encode()
    await socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=False).SerializeToString())
    metrics = collect_metrics(lit_api)
    await socket.send(_make_stream_done(stream_id, metrics).SerializeToString())


async def _handle_stream_async(
    lit_api: LitAPI,
    request: Request,
    socket,
    active_streams: dict[str, Any],
    log: logging.Logger,
):
    """Handle stream open/chunk/close/cancel."""
    stream_req = request.stream
    stream_id = stream_req.stream_id
    action = stream_req.WhichOneof("action")

    if action == "open":
        await _handle_stream_open_async(lit_api, stream_req, socket, active_streams, log)
    elif action == "chunk":
        await _handle_stream_chunk_async(lit_api, stream_req, socket, active_streams, log)
    elif action in ("close", "cancel"):
        entry = active_streams.pop(stream_id, None)
        if isinstance(entry, _BidiSession):
            on_close_output = await _close_bidi_quietly(
                entry.on_close, entry.ctx, stream_id, log
            )
            if action == "close":
                # Deliver on_close's return as a final chunk (symmetric with
                # on_open/on_chunk) before Done.  Encoding failures are
                # isolated — Done is still sent below.
                if on_close_output is not None:
                    await _send_bidi_final_chunk(
                        lit_api, entry.ctx, on_close_output, stream_id, socket, log
                    )
                metrics = collect_metrics(lit_api)
                await socket.send(_make_stream_done(stream_id, metrics).SerializeToString())
        elif isinstance(entry, asyncio.Task):
            entry.cancel()
            try:
                await entry
            except asyncio.CancelledError:
                pass


async def _handle_stream_open_async(
    lit_api: LitAPI,
    stream_req: StreamRequest,
    socket,
    active_streams: dict[str, Any],
    log: logging.Logger,
):
    """Open a stream: bidi handler, stream_predict generator, or predict fallback."""
    stream_id = stream_req.stream_id
    open_req = stream_req.open
    data = open_req.data if open_req else b""
    # §6.3: meta fallback is unified across bidi / predict-fallback /
    # uni-stream — RequestContext.meta is never None.
    meta = _meta_from_proto(open_req.meta) if open_req and open_req.HasField("meta") else RequestMeta(
        route="", headers=Headers(), client_ip="", request_id="", timestamp_ns=0,
    )
    pipe = _get_pipeline(lit_api)

    # --- Bidirectional streaming ------------------------------------------
    if pipe.has_bidi_stream:
        ctx = RequestContext(meta=meta, request={})
        try:
            ctx.request = _parse_json_payload(data)
            await pipe.preprocess(ctx)
        except HTTPException as e:
            log.warning("bidi preprocess rejected for %s: %s", stream_id, e.detail)
            await pipe.run_on_error(ctx, e)
            await socket.send(_make_stream_error(stream_id, e.detail, error_type=e.error_type, code=e.code, param=e.param).SerializeToString())
            return
        except Exception as e:
            log.warning("bidi preprocess failed for %s: %s", stream_id, _format_exc_brief(e))
            await pipe.run_on_error(ctx, e)
            await socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
            return
        if ctx.early is not None:
            await _send_stream_early(socket, stream_id, ctx.early, lit_api)
            return

        try:
            handler = await pipe.bidi_stream(ctx=ctx)
        except HTTPException as e:
            log.warning("bidi_stream rejected for %s: %s", stream_id, e.detail)
            await pipe.run_on_error(ctx, e)
            await socket.send(_make_stream_error(stream_id, e.detail, error_type=e.error_type, code=e.code, param=e.param).SerializeToString())
            return
        except Exception as e:
            log.error("bidi_stream failed for %s: %s", stream_id, _format_exc_brief(e))
            await pipe.run_on_error(ctx, e)
            await socket.send(_make_stream_error(stream_id, f"bidi_stream failed: {e}").SerializeToString())
            return

        on_open, on_chunk, on_close = pipe.adapt_handler(handler)

        try:
            output = await on_open(ctx.input, ctx=ctx)
        except HTTPException as e:
            log.warning("bidi on_open rejected for %s: %s", stream_id, e.detail)
            await pipe.run_on_error(ctx, e)
            await socket.send(_make_stream_error(stream_id, e.detail, error_type=e.error_type, code=e.code, param=e.param).SerializeToString())
            return
        except Exception as e:
            log.error("bidi on_open failed for %s: %s", stream_id, _format_exc_brief(e))
            await pipe.run_on_error(ctx, e)
            await socket.send(_make_stream_error(stream_id, f"on_open failed: {e}").SerializeToString())
            return

        if output is not None:
            ctx.output = output
            try:
                await pipe.postprocess(ctx)
            except HTTPException as e:
                log.warning("bidi on_open encode rejected for %s: %s", stream_id, e.detail)
                await pipe.run_on_error(ctx, e)
                await socket.send(_make_stream_error(stream_id, e.detail, error_type=e.error_type, code=e.code, param=e.param).SerializeToString())
                await _close_bidi_quietly(on_close, ctx, stream_id, log)
                return
            except Exception as e:
                log.error("bidi on_open encode failed for %s: %s", stream_id, _format_exc_brief(e))
                await pipe.run_on_error(ctx, e)
                await socket.send(_make_stream_error(stream_id, f"encode failed: {e}").SerializeToString())
                await _close_bidi_quietly(on_close, ctx, stream_id, log)
                return
            if ctx.early is not None:
                await _send_stream_early(socket, stream_id, ctx.early, lit_api)
                await _close_bidi_quietly(on_close, ctx, stream_id, log)
                return
            body, _ = unwrap_response(ctx.response)
            # Register only after the open fully succeeded — a failed open
            # must not leave a half-open session behind.
            active_streams[stream_id] = _BidiSession(handler, on_chunk, on_close, ctx)
            await socket.send(_make_stream_chunk(stream_id, json.dumps(body).encode(), is_final=False).SerializeToString())
            return
        active_streams[stream_id] = _BidiSession(handler, on_chunk, on_close, ctx)
        return

    # --- Fallback: predict() once, send as single chunk -------------------
    if not pipe.has_stream_predict:
        try:
            resp_bytes, status, metrics, _ = await pipe.run_single(data, meta)
            await socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=True).SerializeToString())
            await socket.send(_make_stream_done(stream_id, metrics).SerializeToString())
        except HTTPException as e:
            log.warning("stream fallback predict rejected for %s: %s", stream_id, e.detail)
            await socket.send(_make_stream_error(stream_id, e.detail, error_type=e.error_type, code=e.code, param=e.param).SerializeToString())
        except Exception as e:
            log.error("stream fallback predict failed for %s: %s", stream_id, _format_exc_brief(e))
            await socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
        return

    # --- Uni-directional streaming -----------------------------------------
    ctx = RequestContext(meta=meta, request={})
    try:
        ctx.request = _parse_json_payload(data)
        await pipe.preprocess(ctx)
    except HTTPException as e:
        log.warning("stream preprocess rejected for %s: %s", stream_id, e.detail)
        await pipe.run_on_error(ctx, e)
        await socket.send(_make_stream_error(stream_id, e.detail, error_type=e.error_type, code=e.code, param=e.param).SerializeToString())
        return
    except Exception as e:
        log.warning("stream preprocess failed for %s: %s", stream_id, _format_exc_brief(e))
        await pipe.run_on_error(ctx, e)
        await socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
        return
    if ctx.early is not None:
        await _send_stream_early(socket, stream_id, ctx.early, lit_api)
        return

    try:
        generator = await pipe.stream_predict(ctx.input, ctx=ctx)
    except HTTPException as e:
        log.warning("stream_predict rejected for %s: %s", stream_id, e.detail)
        await pipe.run_on_error(ctx, e)
        await socket.send(_make_stream_error(stream_id, e.detail, error_type=e.error_type, code=e.code, param=e.param).SerializeToString())
        return
    except Exception as e:
        log.error("stream_predict failed for %s: %s", stream_id, _format_exc_brief(e))
        await pipe.run_on_error(ctx, e)
        await socket.send(_make_stream_error(stream_id, f"stream_predict failed: {e}").SerializeToString())
        return

    task = asyncio.create_task(
        _consume_stream(lit_api, pipe, generator, stream_id, socket, log, ctx)
    )
    active_streams[stream_id] = task


async def _handle_stream_chunk_async(
    lit_api: LitAPI,
    stream_req: StreamRequest,
    socket,
    active_streams: dict[str, Any],
    log: logging.Logger,
):
    """Handle a mid-stream chunk for bidirectional streaming."""
    stream_id = stream_req.stream_id
    session = active_streams.get(stream_id)
    if not isinstance(session, _BidiSession):
        await socket.send(_make_stream_error(stream_id, "bidi stream not found").SerializeToString())
        return

    pipe = _get_pipeline(lit_api)
    data = stream_req.chunk.data if stream_req.chunk else b""

    try:
        raw = _parse_json_payload(data)
        output = await session.on_chunk(raw, ctx=session.ctx)
    except HTTPException as e:
        log.warning("bidi on_chunk rejected for %s: %s", stream_id, e.detail)
        await pipe.run_on_error(session.ctx, e)
        await socket.send(_make_stream_error(stream_id, e.detail, error_type=e.error_type, code=e.code, param=e.param).SerializeToString())
        return
    except Exception as e:
        log.error("bidi on_chunk failed for %s: %s", stream_id, _format_exc_brief(e))
        await pipe.run_on_error(session.ctx, e)
        await socket.send(_make_stream_error(stream_id, f"on_chunk failed: {e}").SerializeToString())
        return

    if output is None:
        return

    ctx = session.ctx
    ctx.output = output
    ctx.early = None
    try:
        await pipe.postprocess(ctx)
    except HTTPException as e:
        log.warning("bidi encode rejected for %s: %s", stream_id, e.detail)
        await pipe.run_on_error(ctx, e)
        await socket.send(_make_stream_error(stream_id, e.detail, error_type=e.error_type, code=e.code, param=e.param).SerializeToString())
        return
    except Exception as e:
        log.error("bidi encode failed for %s: %s", stream_id, _format_exc_brief(e))
        await pipe.run_on_error(ctx, e)
        await socket.send(_make_stream_error(stream_id, f"encode failed: {e}").SerializeToString())
        return
    if ctx.early is not None:
        await _send_stream_early(socket, stream_id, ctx.early, lit_api)
        # Symmetric with the on_open early path: release the session now.
        # Pop first so a later client close/cancel can't double-fire on_close.
        active_streams.pop(stream_id, None)
        await _close_bidi_quietly(session.on_close, ctx, stream_id, log)
        return
    body, _ = unwrap_response(ctx.response)
    await socket.send(_make_stream_chunk(stream_id, json.dumps(body).encode(), is_final=False).SerializeToString())


_SENTINEL = object()


def _next_or_sentinel(generator):
    try:
        return next(generator)
    except StopIteration:
        return _SENTINEL


async def _process_stream_chunk(
    lit_api: LitAPI, pipe: Pipeline, ctx: RequestContext, output: Any,
    stream_id: str, socket, log: logging.Logger,
) -> bool:
    """Run postprocess for one chunk and send it.  Returns False if the
    stream should stop (early return sent, or an error was emitted)."""
    ctx.output = output
    ctx.early = None
    try:
        await pipe.postprocess(ctx)
    except HTTPException as e:
        log.warning("stream encode rejected for %s: %s", stream_id, e.detail)
        await pipe.run_on_error(ctx, e)
        await socket.send(_make_stream_error(stream_id, e.detail, error_type=e.error_type, code=e.code, param=e.param).SerializeToString())
        return False
    except Exception as e:
        log.error("encode failed for stream %s: %s", stream_id, _format_exc_brief(e))
        await pipe.run_on_error(ctx, e)
        await socket.send(_make_stream_error(stream_id, f"encode failed: {e}").SerializeToString())
        return False
    if ctx.early is not None:
        await _send_stream_early(socket, stream_id, ctx.early, lit_api)
        return False
    body, _ = unwrap_response(ctx.response)
    await socket.send(_make_stream_chunk(stream_id, json.dumps(body).encode(), is_final=False).SerializeToString())
    return True


async def _consume_stream(
    lit_api: LitAPI,
    pipe: Pipeline,
    generator,
    stream_id: str,
    socket,
    log: logging.Logger,
    ctx: RequestContext,
):
    """Consume a sync or async generator and send chunks + StreamDone."""
    try:
        if inspect.isasyncgen(generator):
            async for output in generator:
                if not await _process_stream_chunk(lit_api, pipe, ctx, output, stream_id, socket, log):
                    return
        else:
            while True:
                output = await pipe.run_blocking(_next_or_sentinel, generator)
                if output is _SENTINEL:
                    break
                if not await _process_stream_chunk(lit_api, pipe, ctx, output, stream_id, socket, log):
                    return
    except asyncio.CancelledError:
        # Propagate cancellation; try to close the generator without waiting
        if not inspect.isasyncgen(generator):
            try:
                await pipe.run_blocking(generator.close)
            except Exception:
                pass
        raise
    except HTTPException as e:
        log.warning("stream_predict rejected for %s: %s", stream_id, e.detail)
        await pipe.run_on_error(ctx, e)
        await socket.send(_make_stream_error(stream_id, e.detail, error_type=e.error_type, code=e.code, param=e.param).SerializeToString())
        return
    except Exception as e:
        log.error("stream_predict error for %s: %s", stream_id, _format_exc_brief(e))
        await pipe.run_on_error(ctx, e)
        await socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
        return

    metrics = collect_metrics(lit_api)
    await socket.send(_make_stream_done(stream_id, metrics).SerializeToString())


