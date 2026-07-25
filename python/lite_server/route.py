"""Declarative route declaration for lite-server.

Provides a decorator API (``@route.get`` / ``@route.post`` / ...) for declaring
custom HTTP routes on a :class:`LitAPI` subclass. Routes are discovered by the
model worker at startup (see ``worker/inference.py``: ``_discover_routes``)
and served under ``/v2/models/:m/<tail>`` over the same ZMQ channel as
inference.

The ``route`` object is a *stateless* namespace — decorators only annotate the
wrapped function (``fn.__route_defs__``); no process-global route table is
kept. ``@route`` is intended for LitAPI methods only; module-level decorated
functions have no discovery path.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, List, TypedDict

from lite_server.context import Headers


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
    """A declared route's metadata (path pattern + HTTP methods).

    The bound handler is resolved at discovery time from the LitAPI instance
    (so the same function can serve a path once ``self`` is bound); it is not
    stored here.
    """

    path: str
    methods: List[str]


class HandlerSignatureError(RuntimeError):
    """Route handler signature violates the 0.7.0 ctx contract (loud).

    A misconfigured route must not be silently swallowed — it fails route
    discovery so the worker reports a startup error instead of dropping
    every route while still reporting ready.
    """


def _validate_handler_signature(fn, route: str) -> None:
    """Reject pre-0.7 (request, server) handlers at discovery time.

    Pass the *bound* method (``instance.method``) so ``self`` is already
    consumed and the reported signature is just the handler's own params.
    """
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
    """Stateless decorator namespace for ``@route`` declarations.

    Each decorator attaches a ``__route_defs__`` list of :class:`RouteDef` to
    the wrapped function (stacking decorators on one method unions their
    methods). The worker's ``_discover_routes`` collects these off a LitAPI
    instance and binds ``self``. No routes are stored on the router itself.
    """

    def get(self, path: str):
        return self._route(path, ["GET"])

    def post(self, path: str):
        return self._route(path, ["POST"])

    def put(self, path: str):
        return self._route(path, ["PUT"])

    def delete(self, path: str):
        return self._route(path, ["DELETE"])

    def patch(self, path: str):
        return self._route(path, ["PATCH"])

    def _route(self, path: str, methods: List[str]):
        def decorator(fn):
            defs = list(getattr(fn, "__route_defs__", []))
            defs.append(RouteDef(path=path, methods=methods))
            fn.__route_defs__ = defs
            return fn

        return decorator


# Stateless namespace imported by user code as `from lite_server import route`.
route = Router()
