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


def _field_for(hook_name: str) -> str:
    return {
        "on_request": "request",
        "on_input": "input",
        "on_output": "output",
        "on_response": "response",
    }[hook_name]


def load_callbacks(config: dict[str, Any]) -> list[Callback]:
    """Load and validate callback instances from model config.

    Expects a ``callbacks`` key in config containing a list of
    fully-qualified class paths::

        callbacks:
          - my_package.callbacks.AuditLogger
          - my_package.callbacks.MetricsCollector

    Each class must be a no-arg constructible :class:`Callback` subclass.
    Loading fails loudly (RuntimeError) on import errors, non-Callback
    classes, or pre-0.7 hook signatures — a silently skipped callback could
    mean auth/validation logic that never runs.
    """
    logger = logging.getLogger("lite_server.callback")
    instances: list[Callback] = []
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
