"""FastAPI app assembly: auth guard, instance CRUD, proxy, SPA static."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.staticfiles import StaticFiles

from . import auth as auth_module
from . import proxy as proxy_module
from . import registry as registry_module

SECURITY_HEADERS = {
    # antd is CSS-in-JS and injects <style> at runtime, hence style-src
    # 'unsafe-inline'; the bundle itself has no inline scripts.
    "content-security-policy": (
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; font-src 'self' data:; connect-src 'self'; "
        "object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'"
    ),
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
    "referrer-policy": "same-origin",
}


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
    await app.state.http_stream.aclose()


def build_app(registry, *, web_dist=None, user_store=None, auth_enabled: bool = True,
              unary_timeout: float = 60.0, stream_max_connections: int = 500) -> FastAPI:
    app = FastAPI(lifespan=_lifespan)
    app.state.registry = registry
    # trust_env=False: instance forwarding must not honor system/env proxy
    # settings (the Node BFF's undici client does not either).
    # Two clients: bounded for unary calls, unbounded read for SSE streams.
    app.state.http = httpx.AsyncClient(
        timeout=httpx.Timeout(unary_timeout, connect=10.0), trust_env=False
    )
    # Each SSE stream or file transfer pins one upstream connection for its
    # lifetime, so the pool must cover the expected concurrent transfer load
    # (httpx's default of 100 would queue new streams). Each active stream
    # also costs 2 fds (browser + upstream): raise ulimit -n accordingly.
    app.state.http_stream = httpx.AsyncClient(
        timeout=httpx.Timeout(None, connect=10.0), trust_env=False,
        limits=httpx.Limits(max_connections=stream_max_connections),
    )

    if user_store is not None:
        app.state.user_store = user_store
        app.state.auth_enabled = auth_enabled

        @app.middleware("http")
        async def guard(request, call_next):
            deny = auth_module.check_request(user_store, auth_enabled, request)
            response = deny if deny is not None else await call_next(request)
            # Audit: every mutation with its actor and outcome. /api/auth/*
            # emits specific events (login_success, user_created, ...) already.
            if (request.method != "GET"
                    and request.url.path.startswith("/api/")
                    and not request.url.path.startswith("/api/auth/")):
                user = getattr(request.state, "user", None)
                user_store.record_audit(
                    "http_mutation",
                    actor=user.get("username") if user else None,
                    ip=request.client.host if request.client else None,
                    detail={
                        "method": request.method,
                        "path": request.url.path,
                        "status": response.status_code,
                    },
                )
            return response

    app.include_router(registry_module.create_router(registry))
    app.include_router(proxy_module.create_router())
    if user_store is not None:
        app.include_router(auth_module.create_router(user_store))

    if web_dist and Path(web_dist).exists():
        app.mount("/", _SpaFiles(directory=str(web_dist), html=True), name="spa")

    # Registered last so it is the outermost middleware: deny responses from
    # the auth guard must carry the headers too.
    @app.middleware("http")
    async def security_headers(request, call_next):
        response = await call_next(request)
        for key, value in SECURITY_HEADERS.items():
            response.headers.setdefault(key, value)
        return response

    return app
