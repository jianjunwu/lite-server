"""FastAPI app assembly: auth guard, instance CRUD, proxy, SPA static."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.staticfiles import StaticFiles

from . import auth as auth_module
from . import proxy as proxy_module
from . import registry as registry_module

_logger = logging.getLogger("lite_server.webui.audit")


class _SpaFiles(StaticFiles):
    """Static files with SPA fallback: unknown non-API GET paths serve index.html."""

    async def get_response(self, path: str, scope):
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code == 404 and scope["method"] == "GET":
                return await super().get_response("index.html", scope)
            raise


@asynccontextmanager
async def _lifespan(app: FastAPI):
    yield
    await app.state.http.aclose()


def build_app(registry, *, web_dist=None, user_store=None, auth_enabled: bool = True) -> FastAPI:
    app = FastAPI(lifespan=_lifespan)
    app.state.registry = registry
    # trust_env=False: instance forwarding must not honor system/env proxy
    # settings (the Node BFF's undici client does not either).
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(None, connect=10.0), trust_env=False)

    if user_store is not None:
        app.state.user_store = user_store
        app.state.auth_enabled = auth_enabled

        @app.middleware("http")
        async def guard(request, call_next):
            deny = auth_module.check_request(user_store, auth_enabled, request)
            response = deny if deny is not None else await call_next(request)
            # Audit: every mutation with its actor and outcome.
            if request.method != "GET" and request.url.path.startswith("/api/"):
                user = getattr(request.state, "user", None)
                _logger.info(
                    "ui mutation user=%s method=%s url=%s status=%s",
                    user.get("username") if user else None,
                    request.method,
                    request.url.path,
                    response.status_code,
                )
            return response

    app.include_router(registry_module.create_router(registry))
    app.include_router(proxy_module.create_router())
    if user_store is not None:
        app.include_router(auth_module.create_router(user_store))

    if web_dist and Path(web_dist).exists():
        app.mount("/", _SpaFiles(directory=str(web_dist), html=True), name="spa")

    return app
