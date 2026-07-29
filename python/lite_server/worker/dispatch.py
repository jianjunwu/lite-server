"""Single / batch / custom-route / file-change request dispatch.

Split out of ``inference.py`` (0.7.7 debt-free phase 1) — pure move, no
behavior change. Public import paths through ``inference.py`` are preserved
via its bottom re-export block.
"""

import asyncio
import json
import logging
import traceback
from typing import Any

from lite_server.api import LitAPI
from lite_server.context import Headers, RequestContext, RequestMeta
from lite_server.exceptions import HTTPException
from lite_server.pipeline import (
    Pipeline,
    collect_metrics,
    extract_response_meta,
    unwrap_response,
)
from lite_server.proto import (
    BatchItemResponse,
    BatchRequest,
    BatchResponse,
    Request,
    Response,
    SingleResponse,
    Status,
)
from lite_server.worker.inference import (
    _build_single_response,
    _format_exc_brief,
    _get_pipeline,
    _make_error_response,
    _make_status,
    _merge_err_headers,
    _meta_from_proto,
    _parse_json_payload,
)
from lite_server.worker.streaming import _send_route_stream

async def _handle_route_call(
    lit_api: LitAPI, uid: str, request: Request, meta: RequestMeta, socket,
    log: logging.Logger
) -> Response | None:
    """Dispatch a ``route_call`` to its discovered ``@route`` handler.

    Reuses ``Pipeline.run_route`` (on_request → handler → on_response, with
    on_error on failure). The handler result is encoded as a ``SingleResponse``
    (routes and inference share the response shape — phase 2 unification).
    ``ctx.server`` carries the worker-level ``ServerProxy`` (built at startup
    from ``--server-http``); ``None`` when the server did not pass one (e.g.
    unix-socket HTTP deployments or standalone runs).

    A handler returning ``StreamingResponse`` instead streams inline over
    ``socket`` (start → chunks → done, stream_id == uid) and this returns
    ``None`` — the caller must not send a further reply.
    """
    from lite_server.response import StreamingResponse

    rc = request.route_call
    handlers = getattr(lit_api, "_route_handlers", {})
    route_pipe = getattr(lit_api, "_route_pipeline", None)
    entry = handlers.get(meta.route)
    if entry is None or route_pipe is None:
        return _make_error_response(
            uid, f"No route handler for {meta.route!r}", status_code=404
        )
    handler, _methods = entry
    body = json.loads(rc.data) if rc.data else {}
    ctx = RequestContext(meta=meta, request=body)
    ctx.state["path_params"] = (
        dict(request.meta.path_params) if request.HasField("meta") else {}
    )
    ctx.server = getattr(lit_api, "_server_proxy", None)
    await route_pipe.run_route(ctx, handler)
    if isinstance(ctx.early, StreamingResponse):
        await _send_route_stream(socket, uid, ctx.early, ctx, log)
        return None
    return _build_route_response(uid, ctx)


def _build_route_response(uid: str, ctx: RequestContext) -> Response:
    """Encode a route handler's result (ctx.early / ctx.response) as a
    ``SingleResponse``. Mirrors how inference packs SingleResponse, but the
    route handler's ``Response`` (status_code / media_type / headers) is
    honored directly rather than extracted from header conventions.
    """
    from lite_server.response import Response as LiteResponse

    early = ctx.early
    if isinstance(early, LiteResponse):
        status_code = early.status_code
        media_type = early.media_type or "application/json"
        headers = dict(early.headers)
        content = early.content
    else:
        # Bare return value → JSON body, 200.
        status_code = 200
        media_type = "application/json"
        headers = {}
        content = ctx.response
    # Merge ctx.response_headers (e.g. from Cors): explicit headers win,
    # same priority as Pipeline.finalize().
    merged = dict(ctx.response_headers or {})
    merged.update(headers)
    headers = merged

    if isinstance(content, (bytes, bytearray)):
        data = bytes(content)
    elif isinstance(content, str):
        data = content.encode()
    else:
        data = json.dumps(content).encode()

    # Status.code reflects pipeline execution ("Ok" = no exception), separate
    # from the HTTP status_code the handler chose (may be 4xx/5xx).
    single = SingleResponse(
        data=data,
        status=_make_status(True),
        status_code=status_code,
        media_type=media_type,
    )
    for k, v in headers.items():
        single.headers[str(k)] = str(v)
    return Response(uid=uid, single=single)


async def _handle_request_async(lit_api: LitAPI, request: Request, socket, log: logging.Logger):
    """Process a single request (single / batch) asynchronously."""
    uid = request.uid
    meta = _meta_from_proto(request.meta) if request.HasField("meta") else RequestMeta(
        route="", headers=Headers(), client_ip="", request_id="", timestamp_ns=0,
    )
    pipe = _get_pipeline(lit_api)

    try:
        if request.HasField("route_call"):
            response = await _handle_route_call(lit_api, uid, request, meta, socket, log)
            if response is None:
                return  # streaming route already replied inline over socket

        elif request.HasField("single"):
            # Health check: empty data → skip predict pipeline
            if not request.single.data:
                response = Response(
                    uid=uid,
                    single=SingleResponse(data=b"{}", status=_make_status(True)),
                )
            else:
                resp_bytes, status, metrics, resp_headers = await pipe.run_single(
                    request.single.data, meta
                )
                response = _build_single_response(uid, resp_bytes, status, resp_headers, metrics)

        elif request.HasField("batch"):
            response = await _handle_batch(pipe, uid, request.batch, meta, lit_api, log)

        elif request.HasField("file_changed"):
            response = _handle_file_changed(lit_api, uid, request.file_changed, log)

        else:
            response = _make_error_response(uid, "Unsupported payload type")

    except HTTPException as e:
        log.warning("async request %s rejected: %s", uid, e.detail)
        # Merge response headers: ctx.response_headers (threaded onto the
        # exception by run_single as _response_headers — see B6) first, then
        # the exception's own headers (e.g. Retry-After) win.
        hdrs = dict(getattr(e, "_response_headers", None) or {})
        if e.headers:
            hdrs.update(e.headers)
        response = _make_error_response(
            uid, e.detail, status_code=e.status_code, error_type=e.error_type,
            code=e.code, param=e.param, headers=hdrs or None,
        )
    except Exception as e:
        # One short ERROR line: message + where it raised (deepest frame).
        # A multi-line traceback would be split into many events by the Rust
        # stderr forwarder (it splits on newlines), so we log only the locator.
        log.error("async request %s failed: %s", uid, _format_exc_brief(e))
        hdrs = dict(getattr(e, "_response_headers", None) or {})
        response = _make_error_response(uid, f"{type(e).__name__}: {e}", headers=hdrs or None)

    await socket.send(response.SerializeToString())


def _handle_file_changed(lit_api: LitAPI, uid: str, fc, log: logging.Logger) -> Response:
    """Dispatch a FILE_CHANGED notification to the model's on_file_changed hook.

    Reply contract: data = {"handled": bool}; the server falls back to a
    full worker restart unless every worker of the version replies
    handled=true. A hook exception is logged and reported as handled=false —
    never an error status: the restart fallback IS the error handling.

    The hook runs synchronously on the worker event loop (same as sync
    predict stages): heavy refresh work blocks inference for its duration,
    and refreshing weights while requests are in flight is the model
    author's responsibility.
    """
    try:
        handled = lit_api.on_file_changed(list(fc.paths)) is not None
    except Exception as e:
        log.error("on_file_changed failed: %s", _format_exc_brief(e))
        handled = False
    return Response(
        uid=uid,
        single=SingleResponse(
            data=json.dumps({"handled": handled}).encode(),
            status=_make_status(True),
        ),
    )


async def _handle_batch(pipe: Pipeline, uid: str, batch: BatchRequest,
                        meta: RequestMeta, lit_api: LitAPI,
                        log: logging.Logger) -> Response:
    """Batch request: per-item preprocess → batched predict → per-item postprocess.

    Each item carries its own status_code, media_type, and headers via
    :class:`BatchItemResponse` fields — no batch-level header merging.
    """
    error_map: dict[str, Exception] = {}
    # item_uid → response headers to attach to a failed item (B6)
    err_headers_map: dict[str, dict[str, str] | None] = {}
    # (item_uid → (body_bytes, status_code, media_type, headers))
    final_map: dict[str, tuple[bytes, int, str, dict[str, str] | None]] = {}
    ctx_map: dict[str, RequestContext] = {}

    def _store_result(item_uid: str, body: Any, headers: dict[str, str] | None) -> None:
        sc, mt, clean = extract_response_meta(headers)
        final_map[item_uid] = (json.dumps(body).encode(), sc, mt, clean)

    # Phase 1: per-item on_request → decode → on_input
    for item in batch.items:
        ctx = RequestContext(meta=meta, request={})
        try:
            ctx.request = _parse_json_payload(item.data)
            await pipe.preprocess(ctx)
        except Exception as e:
            log.warning("batch item %s preprocess failed: %s", item.uid, _format_exc_brief(e))
            await pipe.run_on_error(ctx, e)
            err_headers_map[item.uid] = _merge_err_headers(ctx, e)
            error_map[item.uid] = e
            continue
        if ctx.early is not None:
            body, headers = unwrap_response(ctx.early)
            _store_result(item.uid, body, headers)
            continue
        ctx_map[item.uid] = ctx

    # Phase 2: batch → predict → unbatch (or per-item fallback)
    if ctx_map and pipe.has_batch_methods:
        decoded_uids = list(ctx_map.keys())
        try:
            # ctx_list items are the same ctx_map objects, so per-item state
            # set inside batch/predict/unbatch stays visible to postprocess.
            ctx_list = [ctx_map[u] for u in decoded_uids]
            outputs = await pipe.batch_predict(
                [ctx_map[u].input for u in decoded_uids], ctx_list
            )
            for u, out in zip(decoded_uids, outputs):
                ctx_map[u].output = out
        except Exception as e:
            log.error("batch predict failed: %s", _format_exc_brief(e))
            for u in decoded_uids:
                await pipe.run_on_error(ctx_map[u], e)
                err_headers_map[u] = _merge_err_headers(ctx_map[u], e)
                error_map[u] = e
    else:
        results = await asyncio.gather(
            *(pipe.predict_value(ctx_map[u]) for u in ctx_map),
            return_exceptions=True,
        )
        for u, result in zip(list(ctx_map.keys()), results):
            if isinstance(result, Exception):
                log.error("predict failed for %s: %s", u, _format_exc_brief(result))
                await pipe.run_on_error(ctx_map[u], result)
                err_headers_map[u] = _merge_err_headers(ctx_map[u], result)
                error_map[u] = result

    # Phase 3: per-item on_output → encode → on_response
    for item_uid, ctx in ctx_map.items():
        if item_uid in error_map:
            continue
        try:
            if ctx.early is None:
                await pipe.postprocess(ctx)
        except Exception as e:
            log.warning("batch item %s postprocess failed: %s", item_uid, _format_exc_brief(e))
            await pipe.run_on_error(ctx, e)
            err_headers_map[item_uid] = _merge_err_headers(ctx, e)
            error_map[item_uid] = e
            continue
        value = ctx.early if ctx.early is not None else ctx.response
        body, headers = unwrap_response(value)
        _store_result(item_uid, body, headers)

    # Phase 4: assemble BatchResponse — per-item fields, no batch headers
    items = []
    for item in batch.items:
        item_uid = item.uid
        if item_uid in final_map:
            body_bytes, sc, mt, hdrs = final_map[item_uid]
            bir = BatchItemResponse(
                uid=item_uid,
                data=body_bytes,
                status=_make_status(True),
                status_code=sc,
                media_type=mt or "",
            )
            if hdrs:
                bir.headers.update(hdrs)
            items.append(bir)
        else:
            err = error_map.get(item_uid, Exception("unknown error"))
            err_bytes = json.dumps({"error": str(err)}).encode()
            bir = BatchItemResponse(
                uid=item_uid,
                data=err_bytes,
                status=_make_status(False, str(err)),
            )
            hdrs = err_headers_map.get(item_uid)
            if hdrs:
                bir.headers.update(hdrs)
            items.append(bir)

    metrics = collect_metrics(lit_api)
    # BatchResponse.headers is retained for wire compatibility but no
    # longer populated — per-item headers carry everything.
    batch_resp = BatchResponse(items=items)
    return Response(uid=uid, batch=batch_resp, metrics=metrics)


