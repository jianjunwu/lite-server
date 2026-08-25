"""Reverse proxy: /api/i/{id}/* -> instance base_url, streaming."""

from __future__ import annotations

import json
import logging

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.background import BackgroundTask
from starlette.requests import ClientDisconnect

from . import ownership

_logger = logging.getLogger("lite_server.webui")

# Hop-by-hop headers that must not cross the proxy in either direction.
HOP_BY_HOP = frozenset({
    "connection",
    "keep-alive",
    "transfer-encoding",
    "te",
    "trailer",
    "upgrade",
    "host",
    "content-length",
})

# Request direction: browser credentials for the BFF itself must never leak to
# upstream instances (a compromised instance could harvest session JWTs).
# content-length is kept: the streamed body is byte-identical, and having the
# length lets upstreams that don't read chunked bodies work unchanged.
# x-lite-user is BFF-asserted identity — the browser's own value is forged.
_STRIP_REQUEST = (HOP_BY_HOP - {"content-length"}) | {
    "cookie", "authorization", "x-lite-user",
}

# Response direction: upstream cookies must not be planted on the BFF origin.
_STRIP_RESPONSE = HOP_BY_HOP | {"set-cookie"}

_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]

# File-transfer routes: upload finalize (unpack/commit) and first-time
# download pack can take minutes before the first response byte, so they
# must not be bounded by the unary read timeout.
_TRANSFER_LAST_SEGMENTS = frozenset({"upload", "download"})


def _is_transfer_route(tail: str) -> bool:
    segments = tail.split("/")
    return "upload-sessions" in segments or segments[-1] in _TRANSFER_LAST_SEGMENTS


def _upstream_headers(request: Request, inst) -> dict[str, str]:
    headers = {k: v for k, v in request.headers.items() if k.lower() not in _STRIP_REQUEST}
    # Admin key priority: explicit browser header > instance-level key.
    if "x-admin-key" in request.headers:
        headers["x-admin-key"] = request.headers["x-admin-key"]
    elif inst.admin_key:
        headers["x-admin-key"] = inst.admin_key
    else:
        headers.pop("x-admin-key", None)
    # BFF-asserted identity for the instance's audit trail (M2). Set from
    # the authenticated session, never from the browser (stripped above).
    user = getattr(request.state, "user", None)
    if user and user.get("username"):
        headers["x-lite-user"] = user["username"]
    return headers


async def _proxy(request: Request, inst_id: str, tail: str = ""):
    inst = request.app.state.registry.get(inst_id)
    if inst is None:
        return JSONResponse({"error": "unknown_instance", "instance": inst_id}, status_code=404)

    # Ownership policy (M2): version/session-scoped authorization, run after
    # the auth guard has populated request.state.user. Inactive when the app
    # has no user store (single-user local mode).
    user = getattr(request.state, "user", None)
    store = getattr(request.app.state, "user_store", None)
    if store is not None and user is not None:
        # Model whitelist (M3) runs before ownership: it decides whether the
        # caller may touch this model at all on this instance.
        model = ownership.parse_model_path(tail)
        if model is not None:
            write = request.method != "GET" and not ownership.is_inference_tail(tail)
            deny = ownership.check_model_grant(store, inst_id, user, model, write=write)
            if deny is not None:
                return deny
    mutation = ownership.parse_model_mutation(tail)
    if store is not None and user is not None and mutation is not None:
        deny = ownership.check_ownership(store, inst_id, user, mutation,
                                         request.query_params, request.method)
        if deny is not None:
            return deny

    query = request.url.query
    url = f"{inst.base_url}/{tail}" + (f"?{query}" if query else "")

    # SSE responses and file transfers are long-lived: they go through the
    # client with no read timeout; everything else is bounded so a stalled
    # instance can't exhaust the connection pool.
    accept = request.headers.get("accept", "")
    unbounded = "text/event-stream" in accept or _is_transfer_route(tail)
    client: httpx.AsyncClient = (
        request.app.state.http_stream if unbounded else request.app.state.http
    )
    # Stream the request body: large uploads must not be buffered in BFF memory.
    body = None if request.method in ("GET", "HEAD") else request.stream()
    req = client.build_request(
        request.method, url, headers=_upstream_headers(request, inst), content=body
    )
    try:
        upstream = await client.send(req, stream=True)
    except ClientDisconnect:
        # The browser aborted mid-upload; no one is left to receive a
        # response. The send never completed, so there is no upstream
        # response to close.
        _logger.info("client disconnected during upload to instance %s: %s",
                     inst_id, tail)
        return Response(status_code=499)
    except httpx.HTTPError:
        return JSONResponse({"error": "instance_unreachable", "instance": inst_id}, status_code=502)

    headers = {k: v for k, v in upstream.headers.items() if k.lower() not in _STRIP_RESPONSE}

    # Model whitelist read filtering (M3): small-JSON list responses (model
    # lists, repo scan, timeline/alerts/health/info/drift) are buffered and
    # stripped of models the caller has no grant for.
    if (store is not None and user is not None
            and ownership.list_filter_for(tail) is not None
            and ownership.whitelist_active(store, inst_id, user)):
        try:
            raw = await upstream.aread()
        finally:
            await upstream.aclose()
        if 200 <= upstream.status_code < 300:
            try:
                payload = ownership.filter_list_response(
                    store, inst_id, user, tail, json.loads(raw))
                raw = json.dumps(payload).encode()
            except ValueError:
                pass  # non-JSON list response passes through untouched
        return Response(content=raw, status_code=upstream.status_code, headers=headers)

    # Two responses the proxy must see in full: session init (the session id
    # becomes an ownership record) and batch version delete (per-version
    # results decide which ownership rows to drop). Both are small JSON, so
    # these are the only non-streamed paths.
    buffered = (mutation is not None and mutation.kind in ("init", "batch_delete")
                and store is not None and user is not None)
    if buffered:
        try:
            raw = await upstream.aread()
        finally:
            await upstream.aclose()
        if 200 <= upstream.status_code < 300:
            if mutation.kind == "init":
                session_id = None
                try:
                    session_id = json.loads(raw).get("session_id")
                except (ValueError, AttributeError):
                    session_id = None
                ownership.record_success(store, inst_id, user, mutation,
                                         request.method, session_id=session_id)
            else:
                deleted_versions = None
                try:
                    parsed = json.loads(raw).get("deleted")
                    if isinstance(parsed, list):
                        deleted_versions = [v for v in parsed if isinstance(v, str)]
                except (ValueError, AttributeError):
                    deleted_versions = None
                ownership.record_success(store, inst_id, user, mutation,
                                         request.method,
                                         deleted_versions=deleted_versions)
        return Response(content=raw, status_code=upstream.status_code, headers=headers)

    if (mutation is not None and store is not None and user is not None
            and 200 <= upstream.status_code < 300):
        ownership.record_success(store, inst_id, user, mutation, request.method)

    async def body_iter():
        try:
            # Raw bytes: body and content-encoding stay consistent end to end.
            async for chunk in upstream.aiter_raw():
                yield chunk
        finally:
            await upstream.aclose()

    # Send the body as a stream: SSE / chunked responses pass through unbuffered.
    # The BackgroundTask is belt-and-braces next to the generator's finally:
    # if the client disconnects before the generator is first advanced, the
    # finally never runs (unstarted async generator semantics). aclose is
    # idempotent, so the double close is safe.
    return StreamingResponse(
        body_iter(), status_code=upstream.status_code, headers=headers,
        background=BackgroundTask(upstream.aclose),
    )


def create_router() -> APIRouter:
    router = APIRouter()
    router.add_api_route("/api/i/{inst_id}/{tail:path}", _proxy, methods=_METHODS)
    # Trailing-slash-less form: /api/i/{id} proxies to the instance root.
    router.add_api_route("/api/i/{inst_id}", _proxy, methods=_METHODS)
    return router
