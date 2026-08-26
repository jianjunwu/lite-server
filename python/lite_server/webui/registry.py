"""Instance CRUD routes."""

from __future__ import annotations

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from .config import InstanceConfig, InstanceStore, StoreError

_STATUS_FOR = {"invalid": 400, "duplicate": 409, "not_found": 404, "readonly": 403}


def public_view(instances: list[InstanceConfig], effective_roles: dict | None = None) -> dict:
    return {
        "instances": [
            {
                "id": i.id,
                "name": i.name,
                "base_url": i.base_url,
                "has_admin_key": bool(i.admin_key),
                "readonly": i.readonly,
                **({"effective_role": effective_roles[i.id]} if effective_roles else {}),
            }
            for i in instances
        ]
    }


def _error(exc: Exception) -> JSONResponse:
    code = exc.code if isinstance(exc, StoreError) else None
    return JSONResponse({"error": str(exc)}, status_code=_STATUS_FOR.get(code, 500))


async def _probe(client: httpx.AsyncClient, base_url: str) -> bool:
    """Reachability probe: GET {base_url}/info with a short timeout.

    Any HTTP response counts as reachable: a 401/403 means the instance is
    alive behind an auth gate (access_control classifies /info as admin),
    not down. Only transport errors mean unreachable."""
    try:
        await client.get(f"{base_url}/info", timeout=2.0)
        return True
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
    async def list_instances(request: Request):
        instances = registry.list()
        user_store = getattr(request.app.state, "user_store", None)
        user = getattr(request.state, "user", None)
        if user_store is None or user is None:
            return public_view(instances)
        # Per-instance grants: "none" hides the instance; everything else is
        # annotated so the UI can gate operations on the effective role.
        visible = []
        roles = {}
        for inst in instances:
            eff = user_store.effective_role(user["username"], user["role"], inst.id)
            if eff == "none":
                continue
            visible.append(inst)
            roles[inst.id] = eff
        return public_view(visible, roles)

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
    async def delete_instance(inst_id: str, request: Request):
        try:
            store.remove(inst_id)
        except StoreError as e:
            return _error(e)
        # Cascade: ownership rows are keyed by instance id and would
        # otherwise resurrect if the same id is recreated later.
        user_store = getattr(request.app.state, "user_store", None)
        if user_store is not None:
            user_store.remove_instance_ownership(inst_id)
        return public_view(store.list())

    return router
