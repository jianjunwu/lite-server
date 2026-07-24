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

from lite_server.api import LitAPI, RequestMeta
from lite_server.callback import (
    _DATA_HOOKS,
    _LIFECYCLE_HOOKS,
    Callback,
    RequestContext,
    _overrides,
)
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


def _adapt(fn: Callable, executor: ThreadPoolExecutor | None) -> Callable[..., Awaitable]:
    """Wrap *fn* (sync or async) into an async callable.

    With *executor* None, sync functions run inline (all-sync fast path).
    Otherwise they run on the single-thread executor so sync user code is
    never executed concurrently and never blocks the event loop.
    """
    if inspect.iscoroutinefunction(fn):
        return fn
    if executor is None:

        async def call_inline(*args):
            result = fn(*args)
            if asyncio.iscoroutine(result):
                return await result
            return result

        return call_inline

    async def call_in_executor(*args):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(executor, functools.partial(fn, *args))
        if asyncio.iscoroutine(result):
            return await result
        return result

    return call_in_executor


def _adapt_producer(fn: Callable, executor: ThreadPoolExecutor | None) -> Callable[..., Awaitable]:
    """Wrap ``stream_predict`` — may be a sync/async generator function or a
    coroutine function returning a generator.  Returns an async callable that
    yields the (sync or async) generator itself."""
    if inspect.isasyncgenfunction(fn):

        async def call(*args):
            return fn(*args)

        return call
    inner = _adapt(fn, executor)

    async def call(*args):
        return await inner(*args)

    return call


def _is_asyncish(fn: Callable) -> bool:
    return inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn)


# ---------------------------------------------------------------------------
# Response unwrapping / metrics (moved here from worker.inference)
# ---------------------------------------------------------------------------


def unwrap_response(encoded: Any) -> tuple[Any, dict[str, str] | None]:
    """Extract ``(body, headers)`` from a pipeline value.

    A :class:`Response` contributes its status_code / media_type via the
    private ``_sc`` / ``_mt`` header keys for downstream extraction; plain
    values pass through with no headers.  ``ResponseWithHeaders`` is covered
    by the generic ``Response`` branch (its ``content`` aliases ``body``).
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
            lit_api.on_request,
            lit_api.decode_request,
            lit_api.predict,
            lit_api.encode_response,
            lit_api.on_response,
            lit_api.batch,
            lit_api.unbatch,
        ]
        if self.has_stream_predict:
            candidate_fns.append(lit_api.stream_predict)
        for cb in self.callbacks:
            for name in _DATA_HOOKS:
                candidate_fns.append(getattr(cb, name))

        # All-sync fast path: everything runs inline on the event loop
        # (no await points → natural serialization, zero executor overhead).
        # Otherwise sync stages go to a single-thread executor.
        self.any_async = any(_is_asyncish(f) for f in candidate_fns)
        self._executor = (
            ThreadPoolExecutor(max_workers=1, thread_name_prefix="lite-sync")
            if self.any_async
            else None
        )

        # --- Model stages (adapted once) ----------------------------------
        self._decode = _adapt(lit_api.decode_request, self._executor)
        self._predict = _adapt(lit_api.predict, self._executor)
        self._encode = _adapt(lit_api.encode_response, self._executor)
        self._batch_fn = _adapt(lit_api.batch, self._executor)
        self._unbatch_fn = _adapt(lit_api.unbatch, self._executor)
        self._stream_producer = (
            _adapt_producer(lit_api.stream_predict, self._executor)
            if self.has_stream_predict
            else None
        )
        self._bidi_factory = (
            _adapt(lit_api.bidi_stream, self._executor)
            if self.has_bidi_stream
            else None
        )

        # --- Hook chains --------------------------------------------------
        # LitAPI.on_request runs first, LitAPI.on_response last — same order
        # as the pre-0.7 worker.
        self._chains: dict[str, list[Callable]] = {name: [] for name in _DATA_HOOKS}
        self._chains["on_request"].append(
            self._wrap_api_hook(lit_api.on_request, "request")
        )
        for cb in self.callbacks:
            for name in _DATA_HOOKS:
                if _overrides(type(cb), name, Callback):
                    self._chains[name].append(
                        self._wrap_cb_hook(getattr(cb, name), name)
                    )
        self._chains["on_response"].append(
            self._wrap_api_hook(lit_api.on_response, "response")
        )

        # Lifecycle hooks: exception-isolated, run outside the event loop.
        self._lifecycle: dict[str, list[Callback]] = {name: [] for name in _LIFECYCLE_HOOKS}
        for cb in self.callbacks:
            for name in _LIFECYCLE_HOOKS:
                if _overrides(type(cb), name, Callback):
                    self._lifecycle[name].append(cb)

    # ---- Construction ----------------------------------------------------

    @classmethod
    def build(cls, lit_api: LitAPI, callbacks: list[Callback]) -> "Pipeline":
        return cls(lit_api, callbacks)

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
        cls = type(self.lit_api)
        return _overrides(cls, "batch", LitAPI) and _overrides(cls, "unbatch", LitAPI)

    # ---- Hook wrapping ---------------------------------------------------

    def _wrap_cb_hook(self, hook: Callable, name: str) -> Callable:
        field = _HOOK_FIELD[name]
        call = _adapt(hook, self._executor)

        async def wrapped(ctx: RequestContext) -> None:
            self._assign(ctx, field, await call(ctx))

        return wrapped

    def _wrap_api_hook(self, hook: Callable, field: str) -> Callable:
        """Adapt a LitAPI ``(value, meta)`` hook into the ctx chain."""
        call = _adapt(hook, self._executor)

        async def wrapped(ctx: RequestContext) -> None:
            self._assign(ctx, field, await call(getattr(ctx, field), ctx.meta))

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
        result = await fn(value)
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
        ctx = RequestContext(meta=meta, request=json.loads(data) if data else {})
        await self.preprocess(ctx)
        if ctx.early is None:
            await self.predict_value(ctx)
        if ctx.early is None:
            await self.postprocess(ctx)
        return self.finalize(ctx)

    def finalize(
        self, ctx: RequestContext
    ) -> tuple[bytes, Status, Metrics | None, dict[str, str] | None]:
        """Serialize the terminal value (early response or ctx.response)."""
        value = ctx.early if ctx.early is not None else ctx.response
        body, headers = unwrap_response(value)
        resp_bytes = json.dumps(body).encode()
        return resp_bytes, Status(code="Ok"), collect_metrics(self.lit_api), headers

    # ---- Model stage access (batch / stream / CB paths) ------------------

    async def predict_one(self, x: Any) -> Any:
        return await self._predict(x)

    async def batch_predict(self, decodeds: list[Any]) -> list[Any]:
        """batch → predict → unbatch with arity check."""
        batched = await self._batch_fn(decodeds)
        output = await self._predict(batched)
        outputs = await self._unbatch_fn(output)
        if len(outputs) != len(decodeds):
            raise ValueError(
                f"unbatch returned {len(outputs)} outputs, expected {len(decodeds)}"
            )
        return list(outputs)

    async def stream_predict(self, x: Any):
        """Return the user's (sync or async) generator for streaming."""
        return await self._stream_producer(x)

    def adapt_handler(self, handler: Any) -> tuple[Callable, Callable, Callable]:
        """Adapt a BidiStreamHandler's on_open/on_chunk/on_close for the loop."""
        return (
            _adapt(handler.on_open, self._executor),
            _adapt(handler.on_chunk, self._executor),
            _adapt(handler.on_close, self._executor),
        )

    async def bidi_stream(self) -> Any:
        return await self._bidi_factory()

    # ---- Lifecycle hooks (sync context, exception-isolated) ---------------

    def trigger_lifecycle(self, name: str, *args: Any) -> None:
        """Run a lifecycle hook on all registered callbacks.

        Lifecycle failures are logged, never propagated — teardown must not
        crash shutdown, and setup hooks must not mask the real setup error.
        """
        for cb in self._lifecycle.get(name, ()):
            try:
                result = getattr(cb, name)(*args)
                if asyncio.iscoroutine(result):
                    asyncio.run(result)
            except Exception:
                logger.warning(
                    "Callback %s.%s failed", type(cb).__name__, name, exc_info=True
                )

    async def run_blocking(self, fn: Callable, *args: Any) -> Any:
        """Run a sync callable under the stage rules.

        When a single-thread executor is present, *fn* is dispatched there
        so sync code never runs concurrently and never blocks the event loop.
        Otherwise (all-sync fast path) *fn* runs inline on the loop thread.
        """
        if self._executor is not None:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(self._executor, fn, *args)
        return fn(*args)

    def close(self) -> None:
        """Shut down the sync-stage executor (worker teardown)."""
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
