"""Declarative endpoint routing for lite-server.

Provides a FastAPI-style decorator API for registering custom endpoints.
Endpoints are discovered from the ``endpoints/`` subdirectory or via
decorator registration on the global router.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, TypedDict

from lite_server.context import Headers


class EndpointRequest(TypedDict, total=False):
    """Request object passed to endpoint handlers and middleware.

    The endpoint worker protocol is JSON (v0): the body arrives pre-parsed
    and binary/form uploads are not supported at this layer.
    """

    method: str            # HTTP method, upper-case
    route: str             # matched route path, e.g. "/health"
    headers: Headers       # case-insensitive
    query: dict[str, str]
    body: Any              # parsed JSON body (None when absent)


@dataclass
class RouteDef:
    path: str
    methods: List[str]
    handler: Callable
    middleware: List[Callable] = field(default_factory=list)


class EndpointRouter:
    """Global router for decorator-based endpoint registration."""

    def __init__(self):
        self._routes: List[RouteDef] = []

    def get(self, path: str, *, middleware: Optional[List[Callable]] = None):
        return self._route(path, ["GET"], middleware=middleware)

    def post(self, path: str, *, middleware: Optional[List[Callable]] = None):
        return self._route(path, ["POST"], middleware=middleware)

    def put(self, path: str, *, middleware: Optional[List[Callable]] = None):
        return self._route(path, ["PUT"], middleware=middleware)

    def delete(self, path: str, *, middleware: Optional[List[Callable]] = None):
        return self._route(path, ["DELETE"], middleware=middleware)

    def patch(self, path: str, *, middleware: Optional[List[Callable]] = None):
        return self._route(path, ["PATCH"], middleware=middleware)

    def _route(
        self, path: str, methods: List[str], middleware: Optional[List[Callable]] = None
    ):
        def decorator(fn):
            self._routes.append(
                RouteDef(
                    path=path,
                    methods=methods,
                    handler=fn,
                    middleware=middleware or [],
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


# Global singleton — imported by endpoint modules
router = EndpointRouter()

# Convenience alias matching user-facing API
endpoint = router
