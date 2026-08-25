"""Reverse proxy: /api/i/{id}/* -> instance base_url, streaming."""

from __future__ import annotations

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

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
_STRIP_REQUEST = (HOP_BY_HOP - {"content-length"}) | {"cookie", "authorization"}

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
    return headers


async def _proxy(request: Request, inst_id: str, tail: str = ""):
    inst = request.app.state.registry.get(inst_id)
    if inst is None:
        return JSONResponse({"error": "unknown_instance", "instance": inst_id}, status_code=404)

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
    except httpx.HTTPError:
        return JSONResponse({"error": "instance_unreachable", "instance": inst_id}, status_code=502)

    async def body_iter():
        try:
            # Raw bytes: body and content-encoding stay consistent end to end.
            async for chunk in upstream.aiter_raw():
                yield chunk
        finally:
            await upstream.aclose()

    headers = {k: v for k, v in upstream.headers.items() if k.lower() not in _STRIP_RESPONSE}
    # Send the body as a stream: SSE / chunked responses pass through unbuffered.
    return StreamingResponse(body_iter(), status_code=upstream.status_code, headers=headers)


def create_router() -> APIRouter:
    router = APIRouter()
    router.add_api_route("/api/i/{inst_id}/{tail:path}", _proxy, methods=_METHODS)
    # Trailing-slash-less form: /api/i/{id} proxies to the instance root.
    router.add_api_route("/api/i/{inst_id}", _proxy, methods=_METHODS)
    return router
