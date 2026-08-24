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

_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]


def _upstream_headers(request: Request, inst) -> dict[str, str]:
    headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP}
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
    body = None if request.method in ("GET", "HEAD") else await request.body()

    client: httpx.AsyncClient = request.app.state.http
    req = client.build_request(
        request.method, url, headers=_upstream_headers(request, inst), content=body
    )
    try:
        upstream = await client.send(req, stream=True)
    except httpx.HTTPError:
        return JSONResponse({"error": "instance_unreachable", "instance": inst_id}, status_code=502)

    async def body_iter():
        try:
            async for chunk in upstream.aiter_bytes():
                yield chunk
        finally:
            await upstream.aclose()

    headers = {k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP}
    # Send the body as a stream: SSE / chunked responses pass through unbuffered.
    return StreamingResponse(body_iter(), status_code=upstream.status_code, headers=headers)


def create_router() -> APIRouter:
    router = APIRouter()
    router.add_api_route("/api/i/{inst_id}/{tail:path}", _proxy, methods=_METHODS)
    # Trailing-slash-less form: /api/i/{id} proxies to the instance root.
    router.add_api_route("/api/i/{inst_id}", _proxy, methods=_METHODS)
    return router
