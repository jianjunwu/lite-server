"""Callback base class, RequestContext, and callback loading for lite-server.

Callbacks observe and transform the inference pipeline at four hook points
around the three model stages::

    on_request → decode_request → on_input → predict
    → on_output → encode_response → on_response

All data hooks receive a single :class:`RequestContext` argument and may be
sync or async.  A data hook may:

- mutate ``ctx`` fields in place, or return a replacement value
- call ``ctx.respond(...)`` or return a ``Response`` for early return
- raise ``HTTPException`` to reject the request (validation, auth, ...)
  — exceptions from data hooks are NOT swallowed; they become error responses

Lifecycle hooks (``on_before_setup`` / ``on_after_setup`` / ``on_teardown``)
run outside the request path and are exception-isolated: failures are logged,
never propagated.
"""

from __future__ import annotations

import importlib
import inspect
import logging
import math
import os
import threading
import time
from typing import TYPE_CHECKING, Any

from lite_server.context import RequestContext

if TYPE_CHECKING:
    from lite_server.api import LitAPI
    from lite_server.context import RequestMeta


class Callback:
    """Base class for inference pipeline callbacks.

    Override any hook to inject custom logic.  All hooks have default no-op
    implementations — define only the ones you use.  Data hooks may be sync
    or async; the pipeline adapts automatically.

    Hook order for a standard request::

        on_request  →  decode_request  →  on_input  →  predict
        →  on_output  →  encode_response  →  on_response
    """

    # ---- Request pipeline (data hooks) ----

    def on_request(self, ctx: RequestContext) -> Any | None:
        """Called on the raw request, before ``decode_request``.

        The right place for auth, schema validation, and cache lookups.
        ``ctx.meta`` carries HTTP headers / route / client IP / request ID.

        Returns:
            Replacement for ``ctx.request``, a ``Response`` for early
            return, or None to pass through.
        """
        pass

    def on_input(self, ctx: RequestContext) -> Any | None:
        """Called after ``decode_request``, before ``predict``.

        The right place for semantic validation of the decoded input.

        Returns:
            Replacement for ``ctx.input``, a ``Response`` for early return,
            or None to pass through.
        """
        pass

    def on_output(self, ctx: RequestContext) -> Any | None:
        """Called after ``predict``, before ``encode_response``.

        In streaming mode this runs once per yielded chunk.

        During a bidirectional (bidi) session ``ctx.request`` and
        ``ctx.input`` always hold the open payload; they do not change
        as chunks arrive.

        Returns:
            Replacement for ``ctx.output``, a ``Response`` for early return,
            or None to pass through.
        """
        pass

    def on_response(self, ctx: RequestContext) -> Any | None:
        """Called after ``encode_response`` — the last hook before sending.

        In streaming mode this runs once per yielded chunk.

        During a bidirectional (bidi) session ``ctx.request`` and
        ``ctx.input`` always hold the open payload; they do not change
        as chunks arrive.

        Returns:
            Replacement for ``ctx.response``, a ``Response`` for early
            return, or None to pass through.
        """
        pass

    # ---- Error hook (exception-isolated) ----

    def on_error(self, ctx: RequestContext, exc: Exception) -> None:
        """Called when the request fails (a hook or stage raised).

        Driven exception-isolated: a failing on_error is logged, never
        masks the original error.  Return value ignored.  May be sync
        or async.  Streaming paths drive it once per failed chunk.
        """
        pass

    # ---- Setup / Teardown (lifecycle hooks, exception-isolated) ----

    def on_before_setup(self, config: dict[str, Any], device: str) -> None:
        """Called before ``LitAPI.setup()``."""
        pass

    def on_after_setup(self, lit_api: "LitAPI") -> None:
        """Called after ``LitAPI.setup()`` completes successfully."""
        pass

    def on_teardown(self, lit_api: "LitAPI") -> None:
        """Called when the model is unloaded / worker shuts down."""
        pass


# Removed pre-0.7 hook names → their replacement.  Defining one of these on
# a Callback subclass is a load-time error with migration instructions.
_REMOVED_HOOKS = {
    "on_before_decode": "on_request",
    "on_after_decode": "on_input",
    "on_before_predict": "on_input",
    "on_after_predict": "on_output",
    "on_before_encode": "on_output",
    "on_after_encode": "on_response",
}

_DATA_HOOKS = ("on_request", "on_input", "on_output", "on_response")

_ERROR_HOOKS = ("on_error",)

_LIFECYCLE_HOOKS = ("on_before_setup", "on_after_setup", "on_teardown")


def _overrides(cls: type, name: str, base: type) -> bool:
    """Return True if *cls* defines *name* anywhere below *base* in its MRO."""
    for klass in cls.__mro__:
        if klass is base:
            return False
        if name in klass.__dict__:
            return True
    return False


def _check_single_ctx_param(
    fn: callable, owner: str, name: str, *, migration: str = ""
) -> None:
    """Require exactly one required positional argument (the ctx).

    A single parameter named ``request``/``meta`` is rejected too — that
    shape is the pre-0.7 hook signature with a default, and accepting it
    would silently bind the ctx to the wrong name.
    """
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return  # C-level or weird callables: let the call site fail
    required = [
        p
        for p in params.values()
        if p.default is inspect.Parameter.empty
        and p.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if len(required) == 1 and required[0].name not in ("request", "meta"):
        return
    msg = (
        f"{owner}.{name} must take exactly one argument "
        f"(ctx: RequestContext); got signature {fn}."
    )
    if migration:
        msg += f" {migration}"
    raise RuntimeError(msg)


def validate_callback(cb: Callback) -> None:
    """Reject pre-0.7 callback shapes with a loud migration error.

    Old-style hooks took ``(value, meta)`` and had before/after names; new
    hooks take a single ``ctx`` argument.  Silently adapting them would hide
    behavior changes (exceptions now reject requests), so we fail at load
    time with exact rename instructions instead.
    """
    cls = type(cb)
    for old_name, new_name in _REMOVED_HOOKS.items():
        if _overrides(cls, old_name, Callback):
            raise RuntimeError(
                f"Callback {cls.__name__} defines removed hook '{old_name}'. "
                f"Since 0.7.0, data hooks take a single RequestContext argument. "
                f"Migrate: rename '{old_name}(self, value, meta)' to "
                f"'{new_name}(self, ctx)' — read/write ctx.{_field_for(new_name)} "
                f"instead of the value parameter, use ctx.meta instead of meta, "
                f"and ctx.state for per-request data."
            )
    for name in _DATA_HOOKS:
        if _overrides(cls, name, Callback):
            _check_single_ctx_param(getattr(cb, name), f"Callback {cls.__name__}", name)
    # on_error takes (self, ctx, exc) — two required positional params after self
    if _overrides(cls, "on_error", Callback):
        _check_on_error_sig(getattr(cb, "on_error"), f"Callback {cls.__name__}")


def _check_on_error_sig(fn: callable, owner: str) -> None:
    """Require exactly two required positional params (ctx, exc) for on_error."""
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return
    required = [
        p
        for p in params.values()
        if p.default is inspect.Parameter.empty
        and p.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if len(required) == 2:
        return
    raise RuntimeError(
        f"{owner}.on_error must take exactly two arguments "
        f"(ctx: RequestContext, exc: Exception); got signature {fn}."
    )


def _field_for(hook_name: str) -> str:
    return {
        "on_request": "request",
        "on_input": "input",
        "on_output": "output",
        "on_response": "response",
    }[hook_name]


def load_callbacks(
    config: dict[str, Any], lit_api: "LitAPI | None" = None
) -> list[Callback]:
    """Load and validate callback instances from config + LitAPI class attribute.

    ``LitAPI.callbacks`` class attribute takes priority — it supports
    constructor arguments.  ``config.yaml`` callbacks are appended
    (fully-qualified class paths, no-arg constructible).

    Both sources pass through :func:`validate_callback` — a silently skipped
    callback could mean auth/validation logic that never runs.
    """
    logger = logging.getLogger("lite_server.callback")
    instances: list[Callback] = []

    # Class attribute (priority) — supports constructor arguments
    if lit_api is not None:
        class_callbacks = getattr(type(lit_api), "callbacks", ()) or ()
        for cb in class_callbacks:
            validate_callback(cb)
            instances.append(cb)
            logger.debug("Loaded callback %s from LitAPI.callbacks", type(cb).__name__)

    # config.yaml (append)
    for path in config.get("callbacks", []) or []:
        try:
            module_path, class_name = path.rsplit(".", 1)
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
        except Exception as e:
            raise RuntimeError(f"Failed to import callback '{path}': {e}") from e
        if not (isinstance(cls, type) and issubclass(cls, Callback)):
            raise RuntimeError(
                f"Callback '{path}' is not a lite_server.Callback subclass"
            )
        try:
            instance = cls()
        except Exception as e:
            raise RuntimeError(
                f"Failed to instantiate callback '{path}' (must be no-arg "
                f"constructible): {e}"
            ) from e
        validate_callback(instance)
        instances.append(instance)
        logger.debug("Loaded callback %s", path)
    return instances


# ---------------------------------------------------------------------------
# Built-in Callback classes
# ---------------------------------------------------------------------------

_POLICY_MANAGED_ENV = "LITE_POLICY_MANAGED"


def _rust_managed() -> bool:
    """True when the Rust HTTP layer executes RateLimit/Cors policies.

    Set via env at worker spawn (new_worker_command).  Read per-instance at
    construction so unit tests can monkeypatch os.environ.
    """
    return os.environ.get(_POLICY_MANAGED_ENV) == "1"


class _TokenBucket:
    """Thread-safe token bucket for local rate limiting fallback."""

    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()
        self.last_access = self.last_update
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> bool:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now
            self.last_access = now
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False


class RequireApiKey(Callback):
    """Reject requests without a valid API key.

    Usage::

        RequireApiKey(header="X-API-Key", keys=["sk-xxx"])
        RequireApiKey(header="Authorization")  # empty keys → any non-empty value passes
    """

    def __init__(self, *, header: str = "X-API-Key", keys: list[str] | None = None):
        self._header = header
        self._keys: frozenset[str] | None = frozenset(keys) if keys else None

    def on_request(self, ctx):
        from lite_server.exceptions import UnauthorizedError

        value = ctx.meta.headers.get(self._header, "")
        if not value:
            raise UnauthorizedError("missing API key", param=self._header)
        if self._keys is not None and value not in self._keys:
            raise UnauthorizedError("invalid API key", param=self._header)


class RateLimit(Callback):
    """Rate-limit policy declaration. Executed in the Rust HTTP layer.

    When the process runs outside a Rust-managed worker (unit tests, local
    dev), falls back to a per-instance local bucket set sharded by ``key``.
    The fallback is per-process and does NOT share state — it exists so the
    policy is testable, not as a production limiter.
    """

    _MAX_BUCKETS = 500

    def __init__(
        self,
        *,
        requests_per_minute: int = 60,
        key: str = "route",
        burst: float | None = None,
    ):
        if key not in ("route", "ip"):
            raise ValueError(f"RateLimit key must be 'route' or 'ip', got {key!r}")
        self.requests_per_minute = requests_per_minute
        self.key = key
        self.burst = float(burst) if burst is not None else requests_per_minute * 1.5
        self._managed = _rust_managed()
        self._buckets: dict[str, _TokenBucket] = {}
        self._lock = threading.Lock()

    def on_request(self, ctx):
        from lite_server.exceptions import HTTPException

        if self._managed:
            return  # Rust 已执行；本实例仅为声明
        bucket_key = ctx.meta.client_ip if self.key == "ip" else ctx.meta.route
        with self._lock:
            bucket = self._buckets.get(bucket_key)
            if bucket is None:
                bucket = _TokenBucket(
                    rate=self.requests_per_minute / 60.0, capacity=self.burst
                )
                self._buckets[bucket_key] = bucket
                if len(self._buckets) > self._MAX_BUCKETS:
                    self._evict_stale()
        if not bucket.acquire():
            if bucket.rate > 0:
                retry = max(1, math.ceil((1.0 - bucket.tokens) / bucket.rate))
            else:
                retry = 60  # zero-rate → effectively disabled
            raise HTTPException(
                429, "rate limit exceeded",
                error_type="rate_limit_exceeded",
                headers={"Retry-After": str(retry)},
            )

    def _evict_stale(self) -> None:
        now = time.monotonic()
        stale = [k for k, b in self._buckets.items() if now - b.last_access > 300]
        for k in stale:
            self._buckets.pop(k, None)


class LogRequests(Callback):
    """Log method, route, status, and elapsed time — including rejections."""

    def __init__(self, *, logger_name: str = "lite_server.requests"):
        self._logger = logging.getLogger(logger_name)
        self._key = f"_logreq_start_{id(self)}"

    def on_request(self, ctx):
        ctx.state[self._key] = time.monotonic()

    def on_response(self, ctx):
        status = ctx.early.status_code if ctx.early is not None else 200
        self._log(ctx, status)

    def on_error(self, ctx, exc):
        from lite_server.exceptions import HTTPException

        status = exc.status_code if isinstance(exc, HTTPException) else 500
        self._log(ctx, status)

    def _log(self, ctx, status: int) -> None:
        start = ctx.state.pop(self._key, None)
        if start is None:
            return
        elapsed_ms = (time.monotonic() - start) * 1000
        self._logger.info(
            "%s %s → %d %.2fms", ctx.meta.method, ctx.meta.route, status, elapsed_ms
        )


class Cors(Callback):
    """CORS policy declaration. Executed in the Rust HTTP layer.

    Rust attaches the headers to every response of the route (success, error,
    and stream start) and answers OPTIONS preflight directly.  Outside a
    Rust-managed worker, falls back to stashing ctx.response_headers.
    """

    def __init__(
        self,
        *,
        allow_origins: list[str] | None = None,
        allow_methods: list[str] | None = None,
        allow_headers: list[str] | None = None,
    ):
        self.allow_origins = list(allow_origins or ["*"])
        self.allow_methods = list(
            allow_methods or ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
        )
        self.allow_headers = list(allow_headers or ["Content-Type", "Authorization"])
        self._managed = _rust_managed()
        self._header_dict = {
            "Access-Control-Allow-Origin": ", ".join(self.allow_origins),
            "Access-Control-Allow-Methods": ", ".join(self.allow_methods),
            "Access-Control-Allow-Headers": ", ".join(self.allow_headers),
        }

    def on_request(self, ctx):
        if self._managed:
            return  # Rust 附加 header 并应答 preflight
        ctx.response_headers.update(self._header_dict)
        if ctx.meta.method == "OPTIONS":
            ctx.respond("", status_code=204, headers=dict(self._header_dict))


# ---------------------------------------------------------------------------
# Policy extraction for worker handshake
# ---------------------------------------------------------------------------


def extract_policies(callbacks: list[Callback]) -> dict[str, Any]:
    """Pull Rust-executed policies out of the merged callback list.

    Embedded in the worker startup handshake.  Last declaration wins.
    """
    policies: dict[str, Any] = {}
    for cb in callbacks:
        if isinstance(cb, RateLimit):
            policies["rate_limit"] = {
                "requests_per_minute": cb.requests_per_minute,
                "key": cb.key,
                "burst": cb.burst,
            }
        elif isinstance(cb, Cors):
            policies["cors"] = {
                "allow_origins": cb.allow_origins,
                "allow_methods": cb.allow_methods,
                "allow_headers": cb.allow_headers,
            }
    return policies
