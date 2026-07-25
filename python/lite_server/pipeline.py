"""Unified inference pipeline engine for lite-server.

Every request type — single, batch, streaming, bidirectional, continuous
batching — flows through the same :class:`Pipeline`: callback hook chains
wrapped around the three model stages::

    on_request → decode_request → on_input → predict
    → on_output → encode_response → on_response

Design rules:

- **Load-time adaptation.**  All ``iscoroutinefunction`` / override detection
  happens once in :meth:`Pipeline.build`; the hot path is a plain iteration
  of pre-wrapped async callables.
- **Unified async.**  Fully-sync models run inline on the event loop (zero
  overhead, identical to the old standard loop).  When any method or hook is
  async, sync model stages are dispatched to a single-thread executor so
  sync code never runs concurrently and never blocks the loop.
- **Engine-level control flow.**  Early return (``ctx.respond(...)`` or a
  hook/stage returning a ``Response``) and error mapping live here exactly
  once — not at every call site.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Awaitable, Callable

from lite_server.api import LitAPI
from lite_server.callback import (
    _DATA_HOOKS,
    _ERROR_HOOKS,
    _LIFECYCLE_HOOKS,
    Callback,
    _check_single_ctx_param,
    _overrides,
)
from lite_server.context import RequestContext, RequestMeta
from lite_server.exceptions import HTTPException
from lite_server.proto import Metrics, MetricValue, Status
from lite_server.response import Response as LiteResponse

logger = logging.getLogger("lite_server.pipeline")

_HOOK_FIELD = {
    "on_request": "request",
    "on_input": "input",
    "on_output": "output",
    "on_response": "response",
}


# ---------------------------------------------------------------------------
# Callable adaptation (sync → async)
# ---------------------------------------------------------------------------


def _parse_request_json(data: bytes | None) -> dict:
    """Parse request JSON, raising HTTPException(400) on invalid JSON so the
    failure flows through run_single's error handling (P3). Empty body → {}."""
    if not data:
        return {}
    try:
        return json.loads(data)
    except json.JSONDecodeError as e:
        raise HTTPException(
            400, f"invalid JSON in request body: {e}",
            error_type="invalid_request_error", code="invalid_json",
        ) from e


def _adapt(fn: Callable, executor: ThreadPoolExecutor | None) -> Callable[..., Awaitable]:
    """Wrap *fn* (sync or async) into an async callable.

    With *executor* None, sync functions run inline (all-sync fast path).
    Otherwise they run on the single-thread executor so sync user code is
    never executed concurrently and never blocks the event loop.
    """
    if inspect.iscoroutinefunction(fn):
        return fn
    if executor is None:

        async def call_inline(*args, **kwargs):
            result = fn(*args, **kwargs)
            # Runtime check — not redundant with the iscoroutinefunction
            # guard above.  A sync function may return a coroutine object
            # (e.g. a sync wrapper that delegates to an async inner), and
            # a callable object with async __call__ is invisible to
            # iscoroutinefunction.
            if asyncio.iscoroutine(result):
                return await result
            return result

        return call_inline

    async def call_in_executor(*args, **kwargs):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            executor, functools.partial(fn, *args, **kwargs)
        )
        # Same runtime guard — see call_inline comment.
        if asyncio.iscoroutine(result):
            return await result
        return result

    return call_in_executor


def _adapt_producer(fn: Callable, executor: ThreadPoolExecutor | None) -> Callable[..., Awaitable]:
    """Wrap ``stream_predict`` — may be a sync/async generator function or a
    coroutine function returning a generator.  Returns an async callable that
    yields the (sync or async) generator itself."""
    if inspect.isasyncgenfunction(fn):

        async def call(*args, **kwargs):
            return fn(*args, **kwargs)

        return call
    inner = _adapt(fn, executor)

    async def call(*args, **kwargs):
        return await inner(*args, **kwargs)

    return call


def _is_asyncish(fn: Callable) -> bool:
    return inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn)


# ---------------------------------------------------------------------------
# Ctx injection infrastructure (0.7.0 context unification)
# ---------------------------------------------------------------------------

# Methods operating across sequences — declaring ``ctx`` is a load-time error.
# batch / unbatch / predict DO receive ctx in batch mode (a list[RequestContext]
# aligned with the inputs); only step is forbidden — its active_sequences
# already carry per-sequence ctx via CBSequence.
_CTX_FORBIDDEN = ("step",)

# Error message fragment for LitAPI hooks that haven't been migrated to ctx.
_API_HOOK_MIGRATION = (
    "Since 0.7.0, LitAPI hooks receive a single RequestContext — same as "
    "Callback hooks. Migrate: 'request' → ctx.request, 'meta' → ctx.meta, "
    "early return via ctx.respond(...) (returning a Response still works)."
)


def _ctx_param(fn: Callable) -> inspect.Parameter | None:
    """Return the parameter named ``ctx`` declared by *fn*, or None."""
    try:
        return inspect.signature(fn).parameters.get("ctx")
    except (TypeError, ValueError):
        return None


def _validate_ctx_injection(lit_api: LitAPI) -> None:
    """Reject ctx declarations on methods that have no per-item context."""
    for name in _CTX_FORBIDDEN:
        if _ctx_param(getattr(lit_api, name)) is not None:
            raise RuntimeError(
                f"LitAPI.{name} declares a 'ctx' parameter, but {name} "
                f"operates across sequences and has no single per-request "
                f"context. Remove the parameter; read per-sequence data "
                f"from CBSequence.state / .meta (each active_sequence "
                f"carries its own ctx)."
            )


def _wrap_ctx_method(fn: Callable, name: str, executor) -> Callable:
    """Adapt *fn* into an async callable with optional ctx injection.

    *fn* receives its declared arguments by default; declaring a parameter
    named exactly ``ctx`` opts into receiving the current RequestContext
    as a keyword argument.  Detection happens once here — never on the
    hot path.  The returned callable always accepts a ``ctx=`` keyword
    (ignored when *fn* didn't declare it).
    """
    call = _adapt(fn, executor)
    p = _ctx_param(fn)
    if p is None:

        async def wrapped(*args, ctx=None):
            return await call(*args)

        return wrapped
    if p.kind is inspect.Parameter.POSITIONAL_ONLY:
        raise RuntimeError(
            f"{name}: 'ctx' must be a normal or keyword-only parameter, "
            f"not positional-only."
        )

    async def wrapped(*args, ctx=None):
        return await call(*args, ctx=ctx)

    return wrapped


def _wrap_ctx_producer(fn: Callable, executor) -> Callable:
    """Same as _wrap_ctx_method but for ``stream_predict`` (generator)."""
    inner = _adapt_producer(fn, executor)
    if _ctx_param(fn) is None:

        async def wrapped(*args, ctx=None):
            return await inner(*args)

        return wrapped

    async def wrapped(*args, ctx=None):
        return await inner(*args, ctx=ctx)

    return wrapped


# ---------------------------------------------------------------------------
# Response unwrapping / metrics (moved here from worker.inference)
# ---------------------------------------------------------------------------


def unwrap_response(encoded: Any) -> tuple[Any, dict[str, str] | None]:
    """Extract ``(body, headers)`` from a pipeline value.

    A :class:`Response` contributes its status_code / media_type via the
    private ``_sc`` / ``_mt`` header keys for downstream extraction; plain
    values pass through with no headers.
    """
    if isinstance(encoded, LiteResponse):
        body = encoded.content
        hdrs = dict(encoded.headers) if encoded.headers else {}
        if encoded.status_code != 200:
            hdrs["_sc"] = str(encoded.status_code)
        media_type = encoded.media_type or ""
        if media_type and media_type != "application/json":
            hdrs["_mt"] = media_type
        return body, hdrs if hdrs else None
    return encoded, None


def extract_response_meta(headers: dict[str, str] | None) -> tuple[int, str, dict[str, str] | None]:
    """Extract ``(status_code, media_type, clean_headers)`` from headers
    embedded by :func:`unwrap_response`."""
    if not headers:
        return 0, "", None
    sc = int(headers.get("_sc", "0") or "0")
    mt = headers.get("_mt", "")
    clean = {k: v for k, v in headers.items() if k not in ("_sc", "_mt")}
    return sc, mt, clean if clean else None


def collect_metrics(lit_api: LitAPI) -> Metrics | None:
    """Collect pre-registered custom metrics from the LitAPI instance.

    The swap (read + reset) is protected by ``_metric_lock`` so concurrent
    ``report_metric`` calls from the executor thread cannot land in the
    window between iteration-end and reset.
    """
    with lit_api._metric_lock:
        values = getattr(lit_api, "_metric_values", None) or []
        lit_api._metric_values = []
    if not values:
        return None
    specs = lit_api._metric_specs
    gauges, counters, histograms = [], [], []
    for mid, val in values:
        if mid < len(specs):
            spec = specs[mid]
            mv = MetricValue(id=spec.metric_id, value=val)
            if spec.metric_type == "gauge":
                gauges.append(mv)
            elif spec.metric_type == "counter":
                counters.append(mv)
            elif spec.metric_type == "histogram":
                histograms.append(mv)
    if not gauges and not counters and not histograms:
        return None
    return Metrics(gauges=gauges, counters=counters, histograms=histograms)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class Pipeline:
    """Per-model inference pipeline: hooks + stages, built once at load.

    Use :meth:`build` to construct.  Request entry points:

    - :meth:`run_single` — full decode → predict → encode for one payload
    - :meth:`preprocess` / :meth:`predict_value` / :meth:`postprocess` —
      composable halves for batch / streaming / CB paths
    - :meth:`finalize` — serialize the terminal ctx value + collect metrics
    """

    def __init__(self, lit_api: LitAPI, callbacks: list[Callback]):
        self.lit_api = lit_api
        self.callbacks = list(callbacks)

        # --- Load-time introspection: async detection ---------------------
        candidate_fns: list[Callable] = [
            lit_api.decode_request,
            lit_api.predict,
            lit_api.encode_response,
            lit_api.batch,
            lit_api.unbatch,
        ]
        if self.has_stream_predict:
            candidate_fns.append(lit_api.stream_predict)
        for cb in self.callbacks:
            for name in _DATA_HOOKS:
                candidate_fns.append(getattr(cb, name))
            for name in _ERROR_HOOKS:
                candidate_fns.append(getattr(cb, name))
        # API hooks (when overridden) also participate in async detection.
        api_cls = type(lit_api)
        if _overrides(api_cls, "on_request", LitAPI):
            candidate_fns.append(lit_api.on_request)
        if _overrides(api_cls, "on_response", LitAPI):
            candidate_fns.append(lit_api.on_response)

        # All-sync fast path: everything runs inline on the event loop
        # (no await points → natural serialization, zero executor overhead).
        # Otherwise sync stages go to a single-thread executor.
        self.any_async = any(_is_asyncish(f) for f in candidate_fns)
        self._executor = (
            ThreadPoolExecutor(max_workers=1, thread_name_prefix="lite-sync")
            if self.any_async
            else None
        )

        # --- Ctx injection validation (load-time) -------------------------
        _validate_ctx_injection(lit_api)
        # predict may declare ctx in BOTH modes: single-request mode injects
        # one RequestContext, batch mode injects a list[RequestContext] aligned
        # with the batch (see batch_predict). No load-time guard needed.

        # --- Model stages (adapted once) ----------------------------------
        self._decode = _wrap_ctx_method(lit_api.decode_request, "decode_request", self._executor)
        self._predict = _wrap_ctx_method(lit_api.predict, "predict", self._executor)
        self._encode = _wrap_ctx_method(lit_api.encode_response, "encode_response", self._executor)
        # batch / unbatch use the same ctx-injecting wrapper: in batch mode
        # the caller passes a list[RequestContext] aligned with the inputs.
        self._batch_fn = _wrap_ctx_method(lit_api.batch, "batch", self._executor)
        self._unbatch_fn = _wrap_ctx_method(lit_api.unbatch, "unbatch", self._executor)
        self._stream_producer = (
            _wrap_ctx_producer(lit_api.stream_predict, self._executor)
            if self.has_stream_predict
            else None
        )
        self._bidi_factory = (
            _wrap_ctx_method(lit_api.bidi_stream, "bidi_stream", self._executor)
            if self.has_bidi_stream
            else None
        )

        # --- Hook chains: one wrapper for LitAPI and Callback hooks -------
        self._chains: dict[str, list[Callable]] = {name: [] for name in _DATA_HOOKS}
        # Base implementations are skipped so a model that doesn't override
        # them costs zero calls per request.
        if _overrides(api_cls, "on_request", LitAPI):
            _check_single_ctx_param(
                lit_api.on_request, f"LitAPI {api_cls.__name__}",
                "on_request", migration=_API_HOOK_MIGRATION,
            )
            self._chains["on_request"].append(
                self._wrap_hook(lit_api.on_request, "on_request")
            )
        for cb in self.callbacks:
            for name in _DATA_HOOKS:
                if _overrides(type(cb), name, Callback):
                    self._chains[name].append(
                        self._wrap_hook(getattr(cb, name), name)
                    )
        if _overrides(api_cls, "on_response", LitAPI):
            _check_single_ctx_param(
                lit_api.on_response, f"LitAPI {api_cls.__name__}",
                "on_response", migration=_API_HOOK_MIGRATION,
            )
            self._chains["on_response"].append(
                self._wrap_hook(lit_api.on_response, "on_response")
            )

        # Lifecycle hooks: exception-isolated, run outside the event loop.
        self._lifecycle: dict[str, list[Callback]] = {name: [] for name in _LIFECYCLE_HOOKS}
        for cb in self.callbacks:
            for name in _LIFECYCLE_HOOKS:
                if _overrides(type(cb), name, Callback):
                    self._lifecycle[name].append(cb)

        # Error hooks: exception-isolated, driven when a request fails.
        self._error_hooks = []
        for cb in self.callbacks:
            for name in _ERROR_HOOKS:
                if _overrides(type(cb), name, Callback):
                    self._error_hooks.append(_adapt(getattr(cb, name), self._executor))

    # ---- Construction ----------------------------------------------------

    @classmethod
    def build(cls, lit_api: LitAPI, callbacks: list[Callback]) -> "Pipeline":
        return cls(lit_api, callbacks)

    @classmethod
    def for_route(cls, callbacks: list[Callback]) -> "Pipeline":
        """Build a hook-only pipeline for custom routes.

        Loud rejection (load time): routes have no decode/predict/encode
        stages, so on_input/on_output hooks would silently never run.
        """
        for cb in callbacks:
            from lite_server.callback import validate_callback as _vc
            _vc(cb)
            cls_type = type(cb)
            if _overrides(cls_type, "on_input", Callback):
                raise RuntimeError(
                    f"Callback {cls_type.__name__} defines 'on_input', but "
                    f"routes have no decode stage — only on_request, "
                    f"on_response, and on_error run. Move the logic into "
                    f"one of those hooks."
                )
            if _overrides(cls_type, "on_output", Callback):
                raise RuntimeError(
                    f"Callback {cls_type.__name__} defines 'on_output', but "
                    f"routes have no encode stage — only on_request, "
                    f"on_response, and on_error run. Move the logic into "
                    f"one of those hooks."
                )

        pipe = cls.__new__(cls)
        pipe.lit_api = None                      # 路由无模型
        pipe.callbacks = list(callbacks)

        # Async detection: only hooks, no model stages.
        candidate_fns: list[Callable] = []
        for cb in callbacks:
            for name in _DATA_HOOKS:
                candidate_fns.append(getattr(cb, name))
            for name in _ERROR_HOOKS:
                candidate_fns.append(getattr(cb, name))

        pipe.any_async = any(_is_asyncish(f) for f in candidate_fns)
        pipe._executor = (
            ThreadPoolExecutor(max_workers=1, thread_name_prefix="ep-sync")
            if pipe.any_async
            else None
        )

        # Hook chains: on_request → handler → on_response
        pipe._chains = {name: [] for name in _DATA_HOOKS}
        for cb in callbacks:
            for name in _DATA_HOOKS:
                if _overrides(type(cb), name, Callback):
                    pipe._chains[name].append(
                        pipe._wrap_hook(getattr(cb, name), name)
                    )
        pipe._error_hooks = []
        for cb in callbacks:
            for name in _ERROR_HOOKS:
                if _overrides(type(cb), name, Callback):
                    pipe._error_hooks.append(_adapt(getattr(cb, name), pipe._executor))
        pipe._lifecycle = {name: [] for name in _LIFECYCLE_HOOKS}
        return pipe

    async def run_route(self, ctx: RequestContext, handler: Callable) -> None:
        """on_request → handler(ctx) → on_response; on_error on failure."""
        try:
            await self._run_chain("on_request", ctx)
            if ctx.early is not None:
                return
            if self._executor is not None and not inspect.iscoroutinefunction(handler):
                result = await self.run_blocking(handler, ctx)
            else:
                result = handler(ctx)
                if asyncio.iscoroutine(result):
                    result = await result
            from lite_server.response import Response as LiteResponse
            if isinstance(result, LiteResponse):
                ctx.early = result
            else:
                ctx.response = result
            if ctx.early is None:
                await self._run_chain("on_response", ctx)
        except Exception as e:
            await self.run_on_error(ctx, e)
            raise

    async def run_on_error(self, ctx: RequestContext, exc: Exception) -> None:
        """Drive on_error hooks, exception-isolated (never masks *exc*)."""
        for hook in self._error_hooks:
            try:
                await hook(ctx, exc)
            except Exception:
                logger.warning("on_error hook failed", exc_info=True)

    # ---- Capability detection (override-based, resolved at load) ---------

    @property
    def has_stream_predict(self) -> bool:
        """True only when the subclass actually overrides ``stream_predict``."""
        return _overrides(type(self.lit_api), "stream_predict", LitAPI)

    @property
    def has_bidi_stream(self) -> bool:
        return _overrides(type(self.lit_api), "bidi_stream", LitAPI)

    @property
    def has_batch_methods(self) -> bool:
        # The worker batch-predicts (batch → predict → unbatch) when EITHER
        # the server is configured to group requests (max_batch_size > 1) OR
        # the model overrides batch()/unbatch() to reshape the batch.
        #   - max_batch_size > 1: a list-aware predict() receives the batch via
        #     the default batch() (returns the list) / unbatch() (list(output)),
        #     without forcing authors to override the defaults.
        #   - overrides: preserves the explicit opt-in for custom reshaping,
        #     even at max_batch_size == 1 (e.g. a model that packs the batch
        #     into tensors itself).
        cls = type(self.lit_api)
        return (
            self.lit_api.max_batch_size > 1
            or (_overrides(cls, "batch", LitAPI) and _overrides(cls, "unbatch", LitAPI))
        )

    # ---- Hook wrapping ---------------------------------------------------

    def _wrap_hook(self, hook: Callable, name: str) -> Callable:
        field = _HOOK_FIELD[name]
        call = _adapt(hook, self._executor)

        async def wrapped(ctx: RequestContext) -> None:
            self._assign(ctx, field, await call(ctx))

        return wrapped

    @staticmethod
    def _assign(ctx: RequestContext, field: str, result: Any) -> None:
        """Normalize a hook return value: Response → early return, non-None
        → replace the field, None → pass through."""
        if result is None:
            return
        if isinstance(result, LiteResponse):
            ctx.early = result
        else:
            setattr(ctx, field, result)

    async def _run_chain(self, name: str, ctx: RequestContext) -> None:
        for hook in self._chains[name]:
            if ctx.early is not None:
                return
            await hook(ctx)

    async def _stage(self, fn: Callable, value: Any, ctx: RequestContext, field: str) -> None:
        result = await fn(value, ctx=ctx)
        if isinstance(result, LiteResponse):
            ctx.early = result
        else:
            setattr(ctx, field, result)

    # ---- Pipeline halves -------------------------------------------------

    async def preprocess(self, ctx: RequestContext) -> None:
        """on_request hooks → decode_request → on_input hooks."""
        await self._run_chain("on_request", ctx)
        if ctx.early is not None:
            return
        await self._stage(self._decode, ctx.request, ctx, "input")
        if ctx.early is not None:
            return
        await self._run_chain("on_input", ctx)

    async def predict_value(self, ctx: RequestContext) -> None:
        """predict (single-item).  Batch/stream paths drive predict themselves."""
        await self._stage(self._predict, ctx.input, ctx, "output")

    async def postprocess(self, ctx: RequestContext) -> None:
        """on_output hooks → encode_response → on_response hooks."""
        await self._run_chain("on_output", ctx)
        if ctx.early is not None:
            return
        await self._stage(self._encode, ctx.output, ctx, "response")
        if ctx.early is not None:
            return
        await self._run_chain("on_response", ctx)

    # ---- Entry points ----------------------------------------------------

    async def run_single(
        self, data: bytes, meta: RequestMeta
    ) -> tuple[bytes, Status, Metrics | None, dict[str, str] | None]:
        """Full pipeline for one request payload.

        Returns ``(body_bytes, status, metrics, headers)`` — same shape as
        the pre-0.7 ``_run_predict*`` functions.
        """
        ctx = RequestContext(meta=meta, request={})
        try:
            ctx.request = _parse_request_json(data)
            await self.preprocess(ctx)
            if ctx.early is None:
                await self.predict_value(ctx)
            if ctx.early is None:
                await self.postprocess(ctx)
        except Exception as e:
            await self.run_on_error(ctx, e)
            # Thread accumulated response headers onto the exception so the
            # unary handler can merge them into the error response (B6). The
            # ctx itself is internal to the pipeline and not visible there.
            if ctx.response_headers:
                e._response_headers = dict(ctx.response_headers)  # type: ignore[attr-defined]
            raise
        return self.finalize(ctx)

    def finalize(
        self, ctx: RequestContext
    ) -> tuple[bytes, Status, Metrics | None, dict[str, str] | None]:
        """Serialize the terminal value (early response or ctx.response)."""
        value = ctx.early if ctx.early is not None else ctx.response
        body, headers = unwrap_response(value)
        # Merge ctx.response_headers (e.g. from Cors): explicit headers win.
        merged = dict(ctx.response_headers)
        if headers:
            merged.update(headers)
        resp_bytes = json.dumps(body).encode()
        return (
            resp_bytes,
            Status(code="Ok"),
            collect_metrics(self.lit_api) if self.lit_api is not None else None,
            merged if merged else None,
        )

    # ---- Model stage access (batch / stream / CB paths) ------------------

    async def batch_predict(
        self, decodeds: list[Any], ctx_list: list[RequestContext]
    ) -> list[Any]:
        """batch → predict → unbatch with arity check.

        *ctx_list* is a per-item RequestContext list aligned positionally
        with *decodeds*.  It is forwarded to batch / predict / unbatch when
        they declare a ``ctx`` parameter (injected as a list); methods that
        don't declare ``ctx`` ignore it (backward compatible).
        """
        batched = await self._batch_fn(decodeds, ctx=ctx_list)
        output = await self._predict(batched, ctx=ctx_list)
        outputs = await self._unbatch_fn(output, ctx=ctx_list)
        if len(outputs) != len(decodeds):
            raise ValueError(
                f"unbatch returned {len(outputs)} outputs, expected {len(decodeds)}"
            )
        return list(outputs)

    async def stream_predict(self, x: Any, ctx: RequestContext):
        """Return the user's (sync or async) generator for streaming."""
        return await self._stream_producer(x, ctx=ctx)

    def adapt_handler(self, handler: Any) -> tuple[Callable, Callable, Callable]:
        """Adapt a BidiStreamHandler's hooks; each may optionally declare ctx."""
        return (
            _wrap_ctx_method(handler.on_open, "on_open", self._executor),
            _wrap_ctx_method(handler.on_chunk, "on_chunk", self._executor),
            _wrap_ctx_method(handler.on_close, "on_close", self._executor),
        )

    async def bidi_stream(self, ctx: RequestContext) -> Any:
        return await self._bidi_factory(ctx=ctx)

    # ---- Lifecycle hooks (sync context, exception-isolated) ---------------

    def trigger_lifecycle(self, name: str, *args: Any) -> None:
        """Run a lifecycle hook on all registered callbacks.

        Must be called from a synchronous context.  Sync hooks run inline;
        async hooks are collected and drained in a single ``asyncio.run``
        call rather than one event loop per hook.

        Lifecycle failures are logged, never propagated — teardown must not
        crash shutdown, and setup hooks must not mask the real setup error.
        """
        pending: list = []
        for cb in self._lifecycle.get(name, ()):
            try:
                result = getattr(cb, name)(*args)
                if asyncio.iscoroutine(result):
                    pending.append(result)
            except Exception:
                logger.warning(
                    "Callback %s.%s failed", type(cb).__name__, name, exc_info=True
                )
        if pending:

            async def _drain() -> None:
                for coro in pending:
                    try:
                        await coro
                    except Exception:
                        logger.warning(
                            "Async lifecycle %s failed", name, exc_info=True
                        )

            asyncio.run(_drain())

    async def run_blocking(self, fn: Callable, *args: Any) -> Any:
        """Run a sync callable under the stage rules.

        When a single-thread executor is present, *fn* is dispatched there
        so sync code never runs concurrently and never blocks the event loop.
        Otherwise (all-sync fast path) *fn* runs inline on the loop thread.
        """
        if self._executor is not None:
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(self._executor, fn, *args)
        else:
            result = fn(*args)
        # Sync callable may still return a coroutine (e.g. an object with
        # async __call__) — same runtime guard as _adapt.
        if asyncio.iscoroutine(result):
            result = await result
        return result

    def close(self) -> None:
        """Shut down the sync-stage executor (worker teardown)."""
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
