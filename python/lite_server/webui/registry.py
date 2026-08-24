"""Instance CRUD routes."""

from __future__ import annotations

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from .config import InstanceConfig, InstanceStore, StoreError

_STATUS_FOR = {"invalid": 400, "duplicate": 409, "not_found": 404, "readonly": 403}


def public_view(instances: list[InstanceConfig]) -> dict:
    return {
        "instances": [
            {
                "id": i.id,
                "name": i.name,
                "base_url": i.base_url,
                "has_admin_key": bool(i.admin_key),
                "readonly": i.readonly,
            }
            for i in instances
        ]
    }


def _error(exc: Exception) -> JSONResponse:
    code = exc.code if isinstance(exc, StoreError) else None
    return JSONResponse({"error": str(exc)}, status_code=_STATUS_FOR.get(code, 500))


async def _probe(client: httpx.AsyncClient, base_url: str) -> bool:
    """Reachability probe: GET {base_url}/info with a short timeout."""
    try:
        res = await client.get(f"{base_url}/info", timeout=2.0)
        return 200 <= res.status_code < 300
    except httpx.HTTPError:
        return False


async def _json_body(request: Request) -> dict | None:
    try:
        body = await request.json()
    except Exception:
        return None
    return body if isinstance(body, dict) else None


def create_router(registry) -> APIRouter:
    router = APIRouter()

    @router.get("/api/instances")
    async def list_instances():
        return public_view(registry.list())

    # Write routes require a mutable store; a plain read-only registry (tests,
    # embedding) only gets the list endpoint.
    if not isinstance(registry, InstanceStore):
        return router
    store = registry

    @router.post("/api/instances")
    async def create_instance(request: Request):
        body = await _json_body(request)
        if body is None:
            return JSONResponse({"error": "invalid json body"}, status_code=400)
        base_url = body.get("base_url")
        if request.query_params.get("probe") == "true" and isinstance(base_url, str):
            if not await _probe(request.app.state.http, base_url.rstrip("/")):
                return JSONResponse(
                    {"error": "instance_unreachable", "base_url": base_url},
                    status_code=422,
                )
        try:
            store.create(body)
        except StoreError as e:
            return _error(e)
        return JSONResponse(public_view(store.list()), status_code=201)

    @router.put("/api/instances/{inst_id}")
    async def update_instance(inst_id: str, request: Request):
        body = await _json_body(request)
        if body is None:
            return JSONResponse({"error": "invalid json body"}, status_code=400)
        try:
            store.update(inst_id, body)
        except StoreError as e:
            return _error(e)
        return public_view(store.list())

    @router.delete("/api/instances/{inst_id}")
    async def delete_instance(inst_id: str):
        try:
            store.remove(inst_id)
        except StoreError as e:
            return _error(e)
        return public_view(store.list())

    return router
