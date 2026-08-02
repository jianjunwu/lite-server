"""Callback base class and callback loading for lite-server.

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

    # ---- Batch hooks (whole-batch view; has_batch_methods path only) ----

    def on_batch_input(self, ctx_list: list[RequestContext], batched: Any) -> Any | None:
        """Called after ``batch()``, before ``predict()`` — over the batched input.

        The one hook with a view of the whole batched tensor (not per-item):
        use for whole-batch transform / validation / stats.  ``ctx_list``
        aligns positionally with the items.  Returns a replacement for
        ``batched``, or None to pass through.  Raising ``HTTPException``
        rejects the whole batch.  Driven only on the ``has_batch_methods``
        path.
        """
        pass

    def on_batch_output(self, ctx_list: list[RequestContext], outputs: list[Any]) -> Any | None:
        """Called after ``unbatch()`` — over the per-item outputs list.

        Returns a replacement for ``outputs`` (keep length-aligned), or None.
        Raising ``HTTPException`` rejects the whole batch.
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

    def on_stream_close(self, ctx: RequestContext, reason: str) -> None:
        """Called once when a stream terminates (stream/bidi/decoupled paths).

        ``reason`` is ``"done"`` (StreamDone sent — normal end, early return,
        or model-initiated close), ``"error"`` (StreamError sent — including a
        server deadline cut), or ``"cancel"`` (client disconnect / cancel — no
        terminal frame).  Driven exception-isolated: a failing hook is logged,
        never propagated (the stream is already terminal).  CB mode does not
        trigger it (no stream concept).  May be sync or async.
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

# Stream terminal hook: (ctx, reason) — driven once when a stream ends.
_STREAM_HOOKS = ("on_stream_close",)

# Whole-batch hooks: (ctx_list, value) — driven inside Pipeline.batch_predict
# only on the has_batch_methods path.
_BATCH_HOOKS = ("on_batch_input", "on_batch_output")

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
    if not isinstance(cb, Callback):
        raise RuntimeError(
            f"{cb!r} is not a lite_server.Callback instance — subclasses of "
            f"Callback are the only objects allowed in callbacks=[...]."
        )
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
    # on_stream_close takes (self, ctx, reason) — two required positional params.
    if _overrides(cls, "on_stream_close", Callback):
        _check_stream_close_sig(getattr(cb, "on_stream_close"), f"Callback {cls.__name__}")
    # Batch hooks take (self, ctx_list, value) — two required positional params.
    for name in _BATCH_HOOKS:
        if _overrides(cls, name, Callback):
            _check_batch_hook_sig(getattr(cb, name), f"Callback {cls.__name__}", name)


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


def _check_batch_hook_sig(fn: callable, owner: str, name: str) -> None:
    """Require exactly two required positional params (ctx_list, value)."""
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
        f"{owner}.{name} must take exactly two arguments "
        f"(ctx_list, value); got signature {fn}."
    )


def _check_stream_close_sig(fn: callable, owner: str) -> None:
    """Require exactly two required positional params (ctx, reason)."""
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
        f"{owner}.on_stream_close must take exactly two arguments "
        f"(ctx, reason); got signature {fn}."
    )


def _field_for(hook_name: str) -> str:
    return {
        "on_request": "request",
        "on_input": "input",
        "on_output": "output",
        "on_response": "response",
    }[hook_name]


# Policy callbacks retired in 0.7.6 — they are now declarative per-model
# policies in config.yaml, enforced by the Rust server. Loading one via the
# ``callbacks:`` list is a loud error, never a silent skip.
_REMOVED_POLICY_CLASSES = frozenset({
    "RequireApiKey",
    "Cors",
    "RateLimit",
    "LogRequests",
})

_REMOVED_POLICY_HINT = (
    "policy callbacks were removed in 0.7.6. Declare the policy in the "
    "model's config.yaml instead:\n"
    "    policies:\n"
    "      auth: { header: \"X-API-Key\", keys: [\"${API_KEYS}\"] }\n"
    "      rate_limit: { requests_per_minute: 60, key: ip }\n"
    "      cors: { allow_origins: [\"*\"], allow_methods: [\"POST\"], allow_headers: [] }\n"
    "      request_log: {}"
)


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

    # config.yaml (append).  Each entry is either a dotted class path (str,
    # no-arg) or a single-key map {path: kwargs} (constructor args) — same
    # registration model as the LitAPI.callbacks class attribute.
    for entry in config.get("callbacks", []) or []:
        if isinstance(entry, dict):
            if len(entry) != 1:
                raise RuntimeError(
                    f"callbacks: map entry must have exactly one key (the class "
                    f"path); got {len(entry)}: {list(entry)}"
                )
            path, kwargs = next(iter(entry.items()))
            kwargs = dict(kwargs or {})
        elif isinstance(entry, str):
            path, kwargs = entry, {}
        else:
            raise RuntimeError(
                f"callbacks: entry must be a class-path string or a "
                f"'{{path: kwargs}}' map; got {entry!r}"
            )
        module_path, _, class_name = path.rpartition(".")
        if class_name in _REMOVED_POLICY_CLASSES:
            raise RuntimeError(f"Callback '{path}' was removed: {_REMOVED_POLICY_HINT}")
        try:
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
        except Exception as e:
            raise RuntimeError(f"Failed to import callback '{path}': {e}") from e
        if not (isinstance(cls, type) and issubclass(cls, Callback)):
            raise RuntimeError(
                f"Callback '{path}' is not a lite_server.Callback subclass"
            )
        try:
            instance = cls(**kwargs)
        except Exception as e:
            kw_hint = f" with kwargs {kwargs}" if kwargs else " (must be no-arg constructible)"
            raise RuntimeError(
                f"Failed to instantiate callback '{path}'{kw_hint}: {e}"
            ) from e
        validate_callback(instance)
        instances.append(instance)
        logger.debug("Loaded callback %s", path)
    return instances
