"""Declarative route declaration for lite-server.

Provides a FastAPI-style decorator API for registering custom routes on
the global router.

Since 0.7.0 the ``callbacks`` parameter replaces ``middleware`` and
accepts :class:`Callback` instances (RequireApiKey, RateLimit, LogRequests,
Cors, or custom).
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, List, Optional, TypedDict

from lite_server.context import Headers

if TYPE_CHECKING:
    from lite_server.callback import Callback as CallbackType


class RouteRequest(TypedDict, total=False):
    """Request object passed to route handlers (legacy dict shape).

    Since 0.7.0 handlers receive a :class:`RequestContext` instead.
    This TypedDict is retained for documentation only.
    """

    method: str            # HTTP method, upper-case
    route: str             # matched route path, e.g. "/health"
    headers: Headers       # case-insensitive
    query: dict[str, str]
    body: Any              # parsed JSON body (None when absent)
    request_id: str        # wire request id, for tracing/log correlation


@dataclass
class RouteDef:
    path: str
    methods: List[str]
    handler: Callable
    callbacks: List[Any] = field(default_factory=list)


class HandlerSignatureError(RuntimeError):
    """Route handler signature violates the 0.7.0 ctx contract (loud).

    A misconfigured route must not be silently swallowed — it fails route
    discovery so the worker reports a startup error instead of dropping
    every route while still reporting ready.
    """


def _validate_handler_signature(fn, route: str) -> None:
    """Reject pre-0.7 (request, server) handlers at discovery time."""
    try:
        params = list(inspect.signature(fn).parameters.values())
    except (TypeError, ValueError):
        return  # C-level callable: let the call site fail
    required = [
        p
        for p in params
        if p.default is inspect.Parameter.empty
        and p.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if len(required) == 1 and required[0].name not in ("request", "server"):
        return
    raise HandlerSignatureError(
        f"Route handler for {route} must take exactly one argument "
        f"(ctx: RequestContext); got signature {fn}. Since 0.7.0: "
        f"request['body'] → ctx.request, request['query'] → ctx.meta.query, "
        f"request['headers'] → ctx.meta.headers, server → ctx.server."
    )


class Router:
    """Global router for decorator-based route registration."""

    def __init__(self):
        self._routes: List[RouteDef] = []

    def get(self, path: str, *, callbacks: Optional[List[Any]] = None):
        return self._route(path, ["GET"], callbacks=callbacks)

    def post(self, path: str, *, callbacks: Optional[List[Any]] = None):
        return self._route(path, ["POST"], callbacks=callbacks)

    def put(self, path: str, *, callbacks: Optional[List[Any]] = None):
        return self._route(path, ["PUT"], callbacks=callbacks)

    def delete(self, path: str, *, callbacks: Optional[List[Any]] = None):
        return self._route(path, ["DELETE"], callbacks=callbacks)

    def patch(self, path: str, *, callbacks: Optional[List[Any]] = None):
        return self._route(path, ["PATCH"], callbacks=callbacks)

    def _route(
        self, path: str, methods: List[str], callbacks: Optional[List[Any]] = None
    ):
        def decorator(fn):
            self._routes.append(
                RouteDef(
                    path=path,
                    methods=methods,
                    handler=fn,
                    callbacks=callbacks or [],
                )
            )
            return fn

        return decorator

    @property
    def routes(self) -> List[RouteDef]:
        return list(self._routes)

    def scan(self, directory: str) -> None:
        """Recursively scan a directory for endpoint modules.

        Modules are loaded and any decorator-registered routes are collected.
        """
        import importlib.util
        from pathlib import Path

        base = Path(directory)
        if not base.exists():
            return

        for py_file in base.rglob("*.py"):
            if py_file.name.startswith("_"):
                continue
            spec = importlib.util.spec_from_file_location(
                f"ep_scan_{py_file.stem}", py_file
            )
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)


# Global singleton — imported by route modules
router = Router()

# Convenience alias matching user-facing API
route = router
