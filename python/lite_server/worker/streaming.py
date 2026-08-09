"""Streaming responses (chunk sequences) and bidirectional sessions.

Split out of ``inference.py`` (0.7.7 debt-free phase 1) — pure move, no
behavior change. Public import paths through ``inference.py`` are preserved
via its bottom re-export block.
"""

import asyncio
import inspect
import logging
import time
from typing import Any, Callable

from lite_server.api import LitAPI
from lite_server.context import Headers, RequestContext, RequestMeta
from lite_server.exceptions import HTTPException
from lite_server.pipeline import (
    Pipeline,
    _is_json_content_type,
    collect_metrics,
    serialize_body,
    unwrap_response,
)
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
    return serialize_body(item)


#: h2 bidi transport framing type (mirrors BIDI_CONTENT_TYPE in
#: src/http/handlers/bidi.rs). It names the LPM frame protocol, NOT the
#: payload: bidi clients send it on the session POST regardless of what the
#: frames carry, so it must never switch payload dispatch to raw bytes
#: (pre-0.8.3 bidi payloads were always JSON-parsed).
_BIDI_FRAMING_CONTENT_TYPE = "application/x-lite-bidi"


def _payload_content_type(headers: Headers) -> str:
    """The Content-Type describing a stream's *payload*.

    The bidi framing type is treated as absent (JSON default); every other
    value (or a missing header) is the payload's own content type.
    """
    ct = headers.get("content-type", "application/json")
    if ct.split(";", 1)[0].strip().lower() == _BIDI_FRAMING_CONTENT_TYPE:
        return "application/json"
    return ct


async def _send_route_stream(socket, stream_id: str, resp, ctx: RequestContext,
                             pipe, log: logging.Logger) -> None:
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
        await pipe.run_on_stream_close(ctx, "error")
        return
    except Exception as e:
        log.warning("route stream %s failed mid-iteration: %s",
                    stream_id, _format_exc_brief(e))
        await socket.send(
            _make_stream_error(stream_id, str(e)).SerializeToString())
        await pipe.run_on_stream_close(ctx, "error")
        return
    await socket.send(_make_stream_done(stream_id).SerializeToString())
    await pipe.run_on_stream_close(ctx, "done")


# ---------------------------------------------------------------------------
# Streaming Support
# ---------------------------------------------------------------------------

class _BidiSession:
    """Per-stream state for bidirectional streaming.

    ``closed`` is the terminal flag: set by the close/cancel path the moment
    the session is reclaimed. An on_chunk/encode call that was suspended at
    an await when the cancel landed checks it on resume and stops without
    sending frames, re-running on_close, or re-firing on_stream_close.
    """

    __slots__ = ("handler", "on_chunk", "on_close", "ctx", "closed", "chunk_count")

    def __init__(self, handler, on_chunk, on_close, ctx: RequestContext):
        self.handler = handler
        self.on_chunk = on_chunk
        self.on_close = on_close
        self.ctx = ctx
        self.closed = False
        # S3:per-stream 输出 chunk 计数(on_chunk 输出 + final chunk,close 时上报)。
        self.chunk_count = 0


class _DecoupledSession:
    """Per-stream state for decoupled (predict_decoupled) streaming (P9-1).

    The channel stays open after predict_decoupled returns; the model pushes
    via ``sender`` and ends with ``sender.close()``. Outlives the open call,
    like :class:`_BidiSession`, so close/cancel (client disconnect) and
    shutdown can reach the sender.
    """

    __slots__ = ("sender", "ctx")

    def __init__(self, sender: "_ResponseSender", ctx: RequestContext):
        self.sender = sender
        self.ctx = ctx


class _ResponseSender:
    """Concrete :class:`lite_server.api.ResponseSender` (P9-1) handed to
    ``predict_decoupled``. The model pushes via :meth:`send` and ends with
    :meth:`close`; after ``close()``/:meth:`cancel` the sender is closed and
    further calls are no-ops.

    Lives on the worker's single asyncio loop, so no locking is needed — but
    the model must not push concurrently from multiple tasks on one sender
    (``ctx`` is shared across sends).
    """

    def __init__(self, lit_api, pipe, stream_id, socket, log, ctx: RequestContext,
                 active_streams: dict):
        self._lit_api = lit_api
        self._pipe = pipe
        self._stream_id = stream_id
        self._socket = socket
        self._log = log
        self._ctx = ctx
        self._active_streams = active_streams
        self.closed = False
        # S3:per-stream chunk 计数(近似 tokens_generated 口径,close 时上报)。
        self._chunk_count = 0

    async def send(self, obj: Any) -> None:
        if self.closed:
            return
        # Mirror _process_stream_chunk: postprocess (after_predict → encode →
        # after_encode_response) then emit one StreamChunkResponse.
        self._ctx.output = obj
        self._ctx.early = None
        try:
            await self._pipe.postprocess(self._ctx)
        except HTTPException as e:
            self._log.warning("decoupled send rejected for %s: %s", self._stream_id, e.detail)
            if self.closed:
                return  # cancel/close landed mid-await: already terminalized
            # Terminal-first: reclaim the session before the terminal frames so
            # a server cleanup cancel interleaving the awaits finds nothing and
            # cannot double-fire on_stream_close.
            self.closed = True
            self._active_streams.pop(self._stream_id, None)
            await _handle_stream_error(self._pipe, self._ctx, e, self._socket,
                                        self._stream_id, self._lit_api, self._log,
                                        detail=e.detail, error_type=e.error_type,
                                        code=e.code, param=e.param)
            return
        except Exception as e:
            self._log.error("decoupled send failed for %s: %s", self._stream_id, _format_exc_brief(e))
            if self.closed:
                return  # cancel/close landed mid-await: already terminalized
            self.closed = True
            self._active_streams.pop(self._stream_id, None)
            await _handle_stream_error(self._pipe, self._ctx, e, self._socket,
                                        self._stream_id, self._lit_api, self._log,
                                        detail=f"send failed: {e}")
            return
        if self.closed:
            return  # cancel/close landed during postprocess: drop the late chunk
        if self._ctx.early is not None:
            # S3:early 帧也是 1 个 chunk——已发计数 +1 一并上报。
            await _send_stream_early(self._socket, self._stream_id, self._ctx.early,
                                     self._lit_api, tokens_generated=self._chunk_count + 1)
            # An early return ends the stream.
            await self.close()
            return
        body, _ = unwrap_response(self._ctx.response)
        await self._socket.send(_make_stream_chunk(self._stream_id, serialize_body(body), is_final=False).SerializeToString())
        self._chunk_count += 1

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        # B3(审计):terminal-first——先摘会话再 await;否则 cancel 在 close
        # 挂起期间落地会对同一流再 fire 一次 on_stream_close(与 send() 错误
        # 路径同模式;server cancel 路径先 pop 再 fire,此处先 pop 则其找不到
        # 会话)。
        self._active_streams.pop(self._stream_id, None)
        metrics = collect_metrics(self._lit_api, tokens_generated=self._chunk_count)
        await self._socket.send(_make_stream_done(self._stream_id, metrics).SerializeToString())
        await self._pipe.run_on_stream_close(self._ctx, "done")

    def cancel(self) -> None:
        """Cooperative cancel (client disconnect / shutdown). Subsequent
        send()/close() are no-ops. No StreamDone is sent (cancel semantics
        match bidi/uni streams)."""
        self.closed = True


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
) -> bool:
    """Encode ``on_close``'s output and send it as a bidi chunk (best-effort).

    Mirrors the ``on_open`` / ``on_chunk`` encode path (postprocess → encode
    → chunk).  Exception-isolated: a failure skips the chunk but the caller
    still sends ``StreamDone``.

    Returns True iff the chunk was actually sent — the caller counts it
    toward ``tokens_generated`` (S3) only for emitted chunks.
    """
    ctx.output = output
    ctx.early = None
    try:
        pipe = _get_pipeline(lit_api)
        await pipe.postprocess(ctx)
    except Exception as e:
        log.warning("bidi on_close encode failed for %s: %s", stream_id, _format_exc_brief(e))
        return False
    body, _headers = unwrap_response(ctx.response)
    await socket.send(
        _make_stream_chunk(stream_id, serialize_body(body), is_final=False).SerializeToString()
    )
    return True


async def _send_stream_early(socket, stream_id: str, early_response, lit_api: LitAPI,
                             tokens_generated: int | None = None) -> None:
    """Send an early-return response as a stream chunk + StreamDone.

    ``tokens_generated`` (S3):early 帧计入 1 个 chunk,调用点按上下文提供
    已发 chunk 数 + 1(本帧);无上下文(open 时 preprocess early)传 1。
    """
    body, _headers = unwrap_response(early_response)
    resp_bytes = serialize_body(body)
    await socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=False).SerializeToString())
    metrics = collect_metrics(lit_api, tokens_generated=tokens_generated)
    await socket.send(_make_stream_done(stream_id, metrics).SerializeToString())


async def _handle_stream_error(
    pipe: Pipeline,
    ctx: RequestContext,
    exc: Exception,
    socket,
    stream_id: str,
    lit_api: LitAPI,
    log: logging.Logger,
    *,
    detail: str | None = None,
    error_type: str | None = None,
    code: str | None = None,
    param: str | None = None,
    extra_cleanup: Callable | None = None,
) -> None:
    """Run on_error hooks → send terminal stream frame → cleanup → close.

    If an on_error hook returned a Response, sends StreamChunk + StreamDone
    as a graceful custom error response; otherwise sends StreamError per
    the existing contract.  BOTH branches run extra_cleanup (bidi on_close
    stays balanced) and close with reason "error" (the request terminated
    because of an exception — reason describes why the stream ended, not
    which frame format was sent).
    """
    custom = await pipe.run_on_error(ctx, exc)
    if custom is not None:
        await _send_stream_early(socket, stream_id, custom, lit_api)
    else:
        if detail is None:
            detail = str(exc)
        await socket.send(_make_stream_error(
            stream_id, detail,
            error_type=error_type, code=code, param=param,
        ).SerializeToString())
    if extra_cleanup is not None:
        await extra_cleanup()
    await pipe.run_on_stream_close(ctx, "error")


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
        pipe = _get_pipeline(lit_api)
        if isinstance(entry, _BidiSession):
            entry.closed = True  # terminal flag: in-flight chunk processing stops
            on_close_output = await _close_bidi_quietly(
                entry.on_close, entry.ctx, stream_id, log
            )
            if action == "close":
                # Deliver on_close's return as a final chunk (symmetric with
                # on_open/on_chunk) before Done.  Encoding failures are
                # isolated — Done is still sent below; tokens_generated counts
                # only chunks actually emitted (encode failure → not counted).
                final_chunk = 0
                if on_close_output is not None and await _send_bidi_final_chunk(
                    lit_api, entry.ctx, on_close_output, stream_id, socket, log
                ):
                    final_chunk = 1
                # S3:per-stream chunk 数 = on_chunk 输出 + final chunk。
                metrics = collect_metrics(lit_api, tokens_generated=entry.chunk_count + final_chunk)
                await socket.send(_make_stream_done(stream_id, metrics).SerializeToString())
                await pipe.run_on_stream_close(entry.ctx, "done")
            else:  # cancel — on_close balanced, no terminal frame sent
                await pipe.run_on_stream_close(entry.ctx, "cancel")
        elif isinstance(entry, asyncio.Task):
            # Uni-stream cancel: on_stream_close("cancel") is driven inside the
            # task's _consume_stream CancelledError handler.
            entry.cancel()
            try:
                await entry
            except asyncio.CancelledError:
                pass
        elif isinstance(entry, _DecoupledSession):
            # P9-1: cooperative cancel — signal the sender so the model's push
            # loop stops. No StreamDone on cancel (matches bidi/uni semantics).
            entry.sender.cancel()
            await pipe.run_on_stream_close(entry.ctx, "cancel")


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

    # --- Decoupled streaming (predict_decoupled, P9-1) --------------------
    # The additive StreamOpen.decoupled flag is set by the Rust DecoupledInfer
    # handler. Distinct from stream_predict: the channel stays open after
    # predict_decoupled returns — the model pushes via sender and ends with
    # close() (or the server reclaims via idle timeout / client disconnect).
    # Checked before bidi/fallback/stream_predict because the flag is the
    # explicit path signal.
    is_decoupled = bool(open_req.decoupled) if open_req else False
    if is_decoupled:
        if not pipe.has_predict_decoupled:
            log.warning("decoupled stream %s: model does not implement predict_decoupled", stream_id)
            await socket.send(_make_stream_error(
                stream_id, "predict_decoupled is not implemented by this model",
                error_type="not_implemented").SerializeToString())
            return
        ctx = RequestContext(meta=meta, request={}, mode="decoupled")
        try:
            if _is_json_content_type(_payload_content_type(meta.headers)):
                ctx.request = _parse_json_payload(data)
            else:
                ctx.request = data
            await pipe.preprocess(ctx)
        except HTTPException as e:
            log.warning("decoupled preprocess rejected for %s: %s", stream_id, e.detail)
            await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                        detail=e.detail, error_type=e.error_type,
                                        code=e.code, param=e.param)
            return
        except Exception as e:
            log.warning("decoupled preprocess failed for %s: %s", stream_id, _format_exc_brief(e))
            await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                        detail=str(e))
            return
        if ctx.early is not None:
            # S3:open 时 preprocess early——early 是唯一输出,计 1 个 chunk。
            await _send_stream_early(socket, stream_id, ctx.early, lit_api, tokens_generated=1)
            # B4(审计):early 终止同样 fire on_stream_close(uni-stream/bidi
            # early 路径均 fire "done",decoupled 不得例外)。
            await pipe.run_on_stream_close(ctx, "done")
            return

        sender = _ResponseSender(lit_api, pipe, stream_id, socket, log, ctx, active_streams)
        try:
            await pipe.predict_decoupled(ctx.input, sender, ctx=ctx)
        except HTTPException as e:
            log.warning("predict_decoupled rejected for %s: %s", stream_id, e.detail)
            await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                        detail=e.detail, error_type=e.error_type,
                                        code=e.code, param=e.param)
            return
        except Exception as e:
            log.error("predict_decoupled failed for %s: %s", stream_id, _format_exc_brief(e))
            await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                        detail=f"predict_decoupled failed: {e}")
            return
        # The model may have returned without closing (background pushing).
        # Register a session so close/cancel and shutdown can find the sender
        # — this is what keeps the channel open past the open call.
        if not sender.closed:
            active_streams[stream_id] = _DecoupledSession(sender, ctx)
        return

    # --- Bidirectional streaming ------------------------------------------
    if pipe.has_bidi_stream:
        ctx = RequestContext(meta=meta, request={}, mode="bidi")
        try:
            if _is_json_content_type(_payload_content_type(meta.headers)):
                ctx.request = _parse_json_payload(data)
            else:
                ctx.request = data
            await pipe.preprocess(ctx)
        except HTTPException as e:
            log.warning("bidi preprocess rejected for %s: %s", stream_id, e.detail)
            await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                        detail=e.detail, error_type=e.error_type,
                                        code=e.code, param=e.param)
            return
        except Exception as e:
            log.warning("bidi preprocess failed for %s: %s", stream_id, _format_exc_brief(e))
            await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                        detail=str(e))
            return
        if ctx.early is not None:
            await _send_stream_early(socket, stream_id, ctx.early, lit_api)
            return

        try:
            handler = await pipe.bidi_stream(ctx=ctx)
        except HTTPException as e:
            log.warning("bidi_stream rejected for %s: %s", stream_id, e.detail)
            await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                        detail=e.detail, error_type=e.error_type,
                                        code=e.code, param=e.param)
            return
        except Exception as e:
            log.error("bidi_stream failed for %s: %s", stream_id, _format_exc_brief(e))
            await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                        detail=f"bidi_stream failed: {e}")
            return

        on_open, on_chunk, on_close = pipe.adapt_handler(handler)

        try:
            output = await on_open(ctx.input, ctx=ctx)
        except HTTPException as e:
            log.warning("bidi on_open rejected for %s: %s", stream_id, e.detail)
            await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                        detail=e.detail, error_type=e.error_type,
                                        code=e.code, param=e.param)
            return
        except Exception as e:
            log.error("bidi on_open failed for %s: %s", stream_id, _format_exc_brief(e))
            await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                        detail=f"on_open failed: {e}")
            return

        if output is not None:
            ctx.output = output
            try:
                await pipe.postprocess(ctx)
            except HTTPException as e:
                log.warning("bidi on_open encode rejected for %s: %s", stream_id, e.detail)
                await _handle_stream_error(
                    pipe, ctx, e, socket, stream_id, lit_api, log,
                    detail=e.detail, error_type=e.error_type, code=e.code, param=e.param,
                    extra_cleanup=lambda: _close_bidi_quietly(on_close, ctx, stream_id, log),
                )
                return
            except Exception as e:
                log.error("bidi on_open encode failed for %s: %s", stream_id, _format_exc_brief(e))
                await _handle_stream_error(
                    pipe, ctx, e, socket, stream_id, lit_api, log,
                    detail=f"encode failed: {e}",
                    extra_cleanup=lambda: _close_bidi_quietly(on_close, ctx, stream_id, log),
                )
                return
            if ctx.early is not None:
                await _send_stream_early(socket, stream_id, ctx.early, lit_api)
                await _close_bidi_quietly(on_close, ctx, stream_id, log)
                await pipe.run_on_stream_close(ctx, "done")
                return
            body, _ = unwrap_response(ctx.response)
            # Register only after the open fully succeeded — a failed open
            # must not leave a half-open session behind.
            active_streams[stream_id] = _BidiSession(handler, on_chunk, on_close, ctx)
            await socket.send(_make_stream_chunk(stream_id, serialize_body(body), is_final=False).SerializeToString())
            return
        active_streams[stream_id] = _BidiSession(handler, on_chunk, on_close, ctx)
        return

    # --- Fallback: predict() once, send as single chunk -------------------
    if not pipe.has_stream_predict:
        # Build the stream ctx here (not inside run_single) so the terminal
        # can drive on_stream_close on it — keeps "ctx exists ↔ on_stream_close
        # fires" uniform across all stream paths.
        ctx = RequestContext(meta=meta, request={}, mode="stream")
        try:
            resp_bytes, status, metrics, _ = await pipe.run_single(data, meta, ctx=ctx)
            await socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=True).SerializeToString())
            await socket.send(_make_stream_done(stream_id, metrics).SerializeToString())
            await pipe.run_on_stream_close(ctx, "error" if ctx.error_overridden else "done")
        except HTTPException as e:
            log.warning("stream fallback predict rejected for %s: %s", stream_id, e.detail)
            await socket.send(_make_stream_error(stream_id, e.detail, error_type=e.error_type, code=e.code, param=e.param).SerializeToString())
            await pipe.run_on_stream_close(ctx, "error")
        except Exception as e:
            log.error("stream fallback predict failed for %s: %s", stream_id, _format_exc_brief(e))
            await socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
            await pipe.run_on_stream_close(ctx, "error")
        return

    # --- Uni-directional streaming -----------------------------------------
    ctx = RequestContext(meta=meta, request={}, mode="stream")
    try:
        if _is_json_content_type(_payload_content_type(meta.headers)):
            ctx.request = _parse_json_payload(data)
        else:
            ctx.request = data
        await pipe.preprocess(ctx)
    except HTTPException as e:
        log.warning("stream preprocess rejected for %s: %s", stream_id, e.detail)
        await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                    detail=e.detail, error_type=e.error_type,
                                    code=e.code, param=e.param)
        return
    except Exception as e:
        log.warning("stream preprocess failed for %s: %s", stream_id, _format_exc_brief(e))
        await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                    detail=str(e))
        return
    if ctx.early is not None:
        await _send_stream_early(socket, stream_id, ctx.early, lit_api)
        await pipe.run_on_stream_close(ctx, "done")
        return

    try:
        generator = await pipe.stream_predict(ctx.input, ctx=ctx)
    except HTTPException as e:
        log.warning("stream_predict rejected for %s: %s", stream_id, e.detail)
        await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                    detail=e.detail, error_type=e.error_type,
                                    code=e.code, param=e.param)
        return
    except Exception as e:
        log.error("stream_predict failed for %s: %s", stream_id, _format_exc_brief(e))
        await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                    detail=f"stream_predict failed: {e}")
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
        # D9: same Content-Type dispatch as the open path — a stream opened
        # with a non-JSON content-type receives mid-stream chunks as raw
        # bytes; the content-type is locked at open time via the session ctx.
        if _is_json_content_type(_payload_content_type(session.ctx.meta.headers)):
            raw = _parse_json_payload(data)
        else:
            raw = data
        output = await session.on_chunk(raw, ctx=session.ctx)
    except HTTPException as e:
        log.warning("bidi on_chunk rejected for %s: %s", stream_id, e.detail)
        if session.closed:
            return  # cancel landed mid-await: already terminalized
        # Terminal: reclaim the session up front (the stream ends here) so a
        # later server cancel is a no-op; extra_cleanup keeps on_close balanced.
        session.closed = True
        active_streams.pop(stream_id, None)
        await _handle_stream_error(pipe, session.ctx, e, socket, stream_id, lit_api, log,
                                    detail=e.detail, error_type=e.error_type,
                                    code=e.code, param=e.param,
                                    extra_cleanup=lambda: _close_bidi_quietly(
                                        session.on_close, session.ctx, stream_id, log))
        return
    except Exception as e:
        log.error("bidi on_chunk failed for %s: %s", stream_id, _format_exc_brief(e))
        if session.closed:
            return  # cancel landed mid-await: already terminalized
        session.closed = True
        active_streams.pop(stream_id, None)
        await _handle_stream_error(pipe, session.ctx, e, socket, stream_id, lit_api, log,
                                    detail=f"on_chunk failed: {e}",
                                    extra_cleanup=lambda: _close_bidi_quietly(
                                        session.on_close, session.ctx, stream_id, log))
        return

    if session.closed:
        return  # cancel landed during on_chunk: drop the late output
    if output is None:
        return

    ctx = session.ctx
    ctx.output = output
    ctx.early = None
    try:
        await pipe.postprocess(ctx)
    except HTTPException as e:
        log.warning("bidi encode rejected for %s: %s", stream_id, e.detail)
        if session.closed:
            return  # cancel landed mid-await: already terminalized
        # Terminal: same reclaim + on_close balance as the on_chunk error path.
        session.closed = True
        active_streams.pop(stream_id, None)
        await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                    detail=e.detail, error_type=e.error_type,
                                    code=e.code, param=e.param,
                                    extra_cleanup=lambda: _close_bidi_quietly(
                                        session.on_close, ctx, stream_id, log))
        return
    except Exception as e:
        log.error("bidi encode failed for %s: %s", stream_id, _format_exc_brief(e))
        if session.closed:
            return  # cancel landed mid-await: already terminalized
        session.closed = True
        active_streams.pop(stream_id, None)
        await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                    detail=f"encode failed: {e}",
                                    extra_cleanup=lambda: _close_bidi_quietly(
                                        session.on_close, ctx, stream_id, log))
        return
    if session.closed:
        return  # cancel landed during postprocess: drop the late chunk
    if ctx.early is not None:
        # S3:early 帧 = 1 个输出 chunk。
        await _send_stream_early(socket, stream_id, ctx.early, lit_api,
                                 tokens_generated=session.chunk_count + 1)
        # Symmetric with the on_open early path: release the session now.
        # Pop first so a later client close/cancel can't double-fire on_close.
        active_streams.pop(stream_id, None)
        await _close_bidi_quietly(session.on_close, ctx, stream_id, log)
        await pipe.run_on_stream_close(ctx, "done")
        return
    body, _ = unwrap_response(ctx.response)
    await socket.send(_make_stream_chunk(stream_id, serialize_body(body), is_final=False).SerializeToString())
    session.chunk_count += 1


_SENTINEL = object()


def _next_or_sentinel(generator):
    try:
        return next(generator)
    except StopIteration:
        return _SENTINEL


async def _process_stream_chunk(
    lit_api: LitAPI, pipe: Pipeline, ctx: RequestContext, output: Any,
    stream_id: str, socket, log: logging.Logger,
    stats: dict[str, int] | None = None,
) -> bool:
    """Run postprocess for one chunk and send it.  Returns False if the
    stream should stop (early return sent, or an error was emitted)."""
    ctx.output = output
    ctx.early = None
    try:
        await pipe.postprocess(ctx)
    except HTTPException as e:
        log.warning("stream encode rejected for %s: %s", stream_id, e.detail)
        await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                    detail=e.detail, error_type=e.error_type,
                                    code=e.code, param=e.param)
        return False
    except Exception as e:
        log.error("encode failed for stream %s: %s", stream_id, _format_exc_brief(e))
        await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                    detail=f"encode failed: {e}")
        return False
    if ctx.early is not None:
        # S3:early 帧 = 1 个 chunk(stats 尚未 +1)。
        await _send_stream_early(socket, stream_id, ctx.early, lit_api,
                                 tokens_generated=stats["chunks"] + 1 if stats else None)
        await pipe.run_on_stream_close(ctx, "done")
        return False
    body, _ = unwrap_response(ctx.response)
    chunk_bytes = serialize_body(body)
    if stats is not None:
        stats["chunks"] += 1
        stats["bytes"] += len(chunk_bytes)
    await socket.send(_make_stream_chunk(stream_id, chunk_bytes, is_final=False).SerializeToString())
    return True


def _deadline_passed(ctx: RequestContext) -> bool:
    """P-DEADLINE: True when the per-request deadline (UNIX ns) has passed.

    ``None`` (no deadline) → False (unbounded, behavior unchanged). Wall-clock
    comparison because ``deadline_unix_ns`` is an absolute UNIX timestamp.
    """
    ns = ctx.meta.deadline_unix_ns
    return ns is not None and time.time_ns() >= ns


async def _consume_stream(
    lit_api: LitAPI,
    pipe: Pipeline,
    generator,
    stream_id: str,
    socket,
    log: logging.Logger,
    ctx: RequestContext,
):
    """Consume a sync or async generator; sends chunks + a terminal frame.

    Drives on_stream_close once: 'done' (normal end), 'error' (stage failure
    or a server deadline cut), or 'cancel' (client disconnect / cancel).
    ``ctx.stream_stats`` accumulates chunk count / total bytes for the run.
    """
    stats = {"chunks": 0, "bytes": 0}
    ctx.stream_stats = stats
    deadline_cut = False
    try:
        if inspect.isasyncgen(generator):
            async for output in generator:
                if not await _process_stream_chunk(lit_api, pipe, ctx, output, stream_id, socket, log, stats):
                    return
                # P-DEADLINE (§4.0.10): cooperative check between chunks — stop
                # emitting once the deadline has passed (best-effort resource
                # release; the server also hard-closes the stream).
                if _deadline_passed(ctx):
                    log.info("stream %s stopping: deadline reached", stream_id)
                    deadline_cut = True
                    break
        else:
            while True:
                output = await pipe.run_blocking(_next_or_sentinel, generator)
                if output is _SENTINEL:
                    break
                if not await _process_stream_chunk(lit_api, pipe, ctx, output, stream_id, socket, log, stats):
                    return
                if _deadline_passed(ctx):
                    log.info("stream %s stopping: deadline reached", stream_id)
                    deadline_cut = True
                    break
    except asyncio.CancelledError:
        # Propagate cancellation; try to close the generator without waiting
        if not inspect.isasyncgen(generator):
            try:
                await pipe.run_blocking(generator.close)
            except Exception:
                pass
        await pipe.run_on_stream_close(ctx, "cancel")
        raise
    except HTTPException as e:
        log.warning("stream_predict rejected for %s: %s", stream_id, e.detail)
        await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                    detail=e.detail, error_type=e.error_type,
                                    code=e.code, param=e.param)
        return
    except Exception as e:
        log.error("stream_predict error for %s: %s", stream_id, _format_exc_brief(e))
        await _handle_stream_error(pipe, ctx, e, socket, stream_id, lit_api, log,
                                    detail=str(e))
        return

    if deadline_cut:
        # A deadline cut is an abnormal termination (server-imposed time
        # limit), not normal completion → StreamError + reason='error'.
        # (Previously this fell through to StreamDone, hiding the cut as
        # success — fixed alongside the on_stream_close deadline→error map.)
        await socket.send(_make_stream_error(
            stream_id, "deadline exceeded", error_type="deadline_exceeded").SerializeToString())
        await pipe.run_on_stream_close(ctx, "error")
        return
    metrics = collect_metrics(lit_api, tokens_generated=stats["chunks"])
    await socket.send(_make_stream_done(stream_id, metrics).SerializeToString())
    await pipe.run_on_stream_close(ctx, "done")


