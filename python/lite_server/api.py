"""LitAPI base class for lite-server.

Model authors subclass ``lite_server.LitAPI`` and implement ``setup`` /
``predict`` (plus optional ``decode_request`` / ``encode_response`` /
``batch`` / ``unbatch`` / streaming and lifecycle hooks).

Since 0.7.0 this class is self-contained (no litserve dependency) and every
model runs on the worker's unified async loop: sync and async methods are
adapted automatically at load time.  ``setup`` stays synchronous.
"""

from __future__ import annotations

import logging
import threading
import warnings
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Iterator, List, Protocol, Tuple

from lite_server.context import RequestContext

# Mirror of the server's MAX_ACCELERATOR_DEVICES (src/metrics/accelerator.rs):
# bound the per-worker accelerator buffer against model-code-controlled
# (device, accel) strings.
_MAX_ACCELERATOR_DEVICES = 64


@dataclass
class _MetricSpec:
    """Internal: pre-registered metric specification."""

    name: str
    metric_type: str  # "gauge" | "counter" | "histogram"
    metric_id: int = 0  # per-type index for Rust-side Vec lookup


class ResponseSender(Protocol):
    """Push handle handed to :meth:`LitAPI.predict_decoupled` (P9-1).

    The model calls ``await send(obj)`` per chunk and ``await close()`` to
    end the stream. After ``close()`` or a server-side cancel, ``closed`` is
    True and further ``send``/``close`` calls are no-ops. The concrete worker
    implementation lives in ``lite_server.worker.streaming``.

    ``closing`` is set by a grace cancel (rolling-recycle eviction): the
    server asks the model to wrap up within a grace window — keep pushing
    any final chunks and ``close()`` itself, and the client still sees a
    normal stream end. Models driving long streams should poll it in their
    push loop. After the window the stream is hard-cancelled.
    """

    closed: bool
    closing: bool

    async def send(self, obj: Any) -> None: ...
    async def close(self) -> None: ...
    def cancel(self) -> None: ...


class BidiStreamHandler:
    """Handler for bidirectional streaming (e.g. ASR, real-time dialogue).

    Override :meth:`on_open`, :meth:`on_chunk`, and :meth:`on_close`
    to process client input and optionally produce output chunks.
    All three may be sync or async.

    Each hook may declare a parameter named exactly ``ctx`` to receive the
    per-session :class:`RequestContext` (detected once when the session is
    established)::

        def on_chunk(self, chunk, ctx):
            count = ctx.state.get("chunks", 0)
            ctx.state["chunks"] = count + 1

    ``ctx.state`` is shared across ``on_open`` / ``on_chunk`` / ``on_close``
    of the same session; ``ctx.meta`` is the request metadata from the
    stream-open request.

    A successfully completed ``on_open`` is always balanced by exactly one
    ``on_close`` — on normal close/cancel, at worker shutdown, or when the
    open is abandoned (post-open failure or early return).  A failed
    ``on_open`` never creates a session and never triggers ``on_close``.
    """

    def on_open(self, initial_data: Any) -> Any | None:
        """Called with the initial payload when the stream opens.

        May declare a ``ctx`` parameter for the session context (see class
        docstring).

        Args:
            initial_data: Decoded output of ``decode_request`` for the open
                payload.

        Returns:
            A response chunk (sent immediately to the client), or None.
        """
        return None

    def on_chunk(self, chunk: Any) -> Any | None:
        """Called with each incoming chunk from the client.

        May declare a ``ctx`` parameter for the session context (see class
        docstring).

        Args:
            chunk: Decoded chunk data (JSON dict from the client).

        Returns:
            A response chunk (sent to the client), or None if no output.
        """
        return None

    def on_close(self) -> None:
        """Called when the stream is closed by the client or server.

        May declare a ``ctx`` parameter for the session context (see class
        docstring).
        """
        pass


class LitAPI:
    """Base class for lite-server inference APIs.

    Minimal usage::

        from lite_server import LitAPI

        class MyModel(LitAPI):
            def setup(self, device):
                self.model = load_model()

            def predict(self, x):
                return self.model(x)

    ``predict`` (and any other method except ``setup``) may be ``async def``;
    the worker adapts automatically — no separate base class is needed.
    """

    # Callbacks declared on the class support constructor arguments and
    # take priority over config.yaml entries.  Use a *tuple* — never a
    # mutable list — to avoid sharing state across instances.
    callbacks: ClassVar[tuple[Any, ...]] = ()

    def __init__(
        self,
        max_batch_size: int = 1,
        batch_timeout: float = 0.0,
        stream: bool = False,
        enable_async: bool = False,
    ):
        if max_batch_size <= 0:
            raise ValueError("max_batch_size must be greater than 0")
        if batch_timeout < 0:
            raise ValueError("batch_timeout must be greater than or equal to 0")

        batch_overridden = type(self).batch.__code__ is not LitAPI.batch.__code__
        unbatch_overridden = type(self).unbatch.__code__ is not LitAPI.unbatch.__code__
        if batch_overridden and unbatch_overridden and max_batch_size == 1:
            warnings.warn(
                "Both batch and unbatch are implemented, but max_batch_size "
                "was not set (default 1 — batching disabled)."
            )

        self.max_batch_size = max_batch_size
        self.batch_timeout = batch_timeout
        self.stream = stream
        # Accepted for backward compatibility; ignored — every model runs on
        # the unified async loop since 0.7.0.
        self.enable_async = enable_async
        self.config: dict[str, Any] = {}
        self._logger: logging.Logger | None = None
        self._metric_specs: List[_MetricSpec] = []
        self._metric_values: List[Tuple[int, float]] = []
        self._metric_lock = threading.Lock()
        # M4: latest accelerator reading per (device, accel); drained into the
        # next response's Metrics proto (see report_accelerator_metrics).
        self._accelerator_readings: Dict[Tuple[str, str], Dict[str, Any]] = {}

    @property
    def logger(self) -> logging.Logger:
        """Lazy logger bound to the model class name."""
        if self._logger is None:
            self._logger = logging.getLogger(
                self.__class__.__module__ + "." + self.__class__.__name__
            )
        return self._logger

    # ===== Core inference methods =====

    def setup(self, device: str) -> None:
        """Load the model and resources. Called once per worker, synchronously."""
        pass

    def decode_request(self, request: Any) -> Any:
        """Convert the raw request payload to your model input.

        To access per-request context, declare a second parameter named
        exactly ``ctx`` — detected once at load time::

            def decode_request(self, request, ctx):
                return {"prompt": request["prompt"],
                        "user_id": ctx.state.get("user_id")}
        """
        return request

    def batch(self, inputs: list) -> Any:
        """Convert a list of decoded inputs to a batched input.

        To access each request's context, declare a parameter named
        exactly ``ctx`` — detected once at load time and injected as a
        ``list[RequestContext]`` aligned positionally with *inputs*
        (one per item)::

            def batch(self, inputs, ctx):
                for c in ctx:
                    self.logger.info("batching %s", c.meta.request_id)
                return torch.stack(inputs)

        Keep ``ctx[i]`` aligned with ``inputs[i]`` — do not reorder the
        inputs, or results will be written back to the wrong requests.
        """
        if hasattr(inputs[0], "__torch_function__"):
            import torch

            return torch.stack(inputs)
        if inputs[0].__class__.__name__ == "ndarray":
            import numpy

            return numpy.stack(inputs)
        return inputs

    def predict(self, x: Any) -> Any:
        """Run the model on the input and return the output.

        To access per-request context, declare a second parameter named
        exactly ``ctx``.  In single-request mode ``ctx`` is one
        :class:`RequestContext`; in batch mode (when ``batch`` /
        ``unbatch`` are overridden) it is a ``list[RequestContext]``
        aligned with the batch, and ``x`` is the batched input.
        """
        raise NotImplementedError("predict is not implemented")

    def unbatch(self, output: Any) -> list:
        """Convert a batched output back to a list of per-input outputs.

        To access each request's context, declare a parameter named exactly
        ``ctx`` — injected as a ``list[RequestContext]`` aligned with the
        original inputs (same order ``batch`` received).  The returned list
        must match that order one-for-one::

            def unbatch(self, output, ctx):
                return list(output)
        """
        return list(output)

    def encode_response(self, output: Any) -> Any:
        """Convert the model output to a response payload.

        To access per-request context, declare a second parameter named
        exactly ``ctx`` — detected once at load time::

            def encode_response(self, output, ctx):
                return {"result": output, "request_id": ctx.meta.request_id}
        """
        return output

    # ===== Custom Metrics =====

    def register_metric(self, name: str, metric_type: str) -> int:
        """Pre-register a custom metric during ``setup()``.

        Returns a numeric ID for use with :meth:`report_metric`.
        Pre-registration lets the server pre-allocate Prometheus objects,
        keeping the hot path zero-allocation.

        Args:
            name: Prometheus metric name (e.g. ``"cache_hit_rate"``).
            metric_type: One of ``"gauge"``, ``"counter"``, or ``"histogram"``.

        Returns:
            Numeric metric ID.
        """
        idx = len(self._metric_specs)
        per_type_id = sum(
            1 for s in self._metric_specs if s.metric_type == metric_type
        )
        self._metric_specs.append(_MetricSpec(name, metric_type, per_type_id))
        return idx

    def report_metric(self, metric_id: int, value: float) -> None:
        """Report a metric value by pre-registered ID.

        Metrics are request-scoped: reported values accumulate in a buffer
        and are collected once at the end of a request (or stream). In
        streaming mode this means values from all chunks are aggregated into
        a single Metrics proto sent with ``StreamDone``.

        Thread-safe: acquires ``_metric_lock`` so concurrent reports from
        the executor thread and collects from the loop thread are serialized.

        Args:
            metric_id: ID returned by :meth:`register_metric`.
            value: Metric value.
        """
        with self._metric_lock:
            self._metric_values.append((metric_id, value))

    def flush_metrics(self):
        """Immediately collect and clear the metric buffer.

        Returns the Metrics proto (or None if empty).  Useful in
        streaming hooks when you want per-chunk metrics instead of
        waiting for the stream to finish.
        """
        from lite_server.pipeline import collect_metrics

        return collect_metrics(self)

    def report_accelerator_metrics(
        self,
        device: str,
        accel: str,
        *,
        utilization_percent: float | None = None,
        memory_used_bytes: float | None = None,
        memory_total_bytes: float | None = None,
        temperature_celsius: float | None = None,
    ) -> None:
        """Report a vendor-neutral accelerator reading (M4).

        Readings are device-scoped, not request-scoped: each call overwrites
        the buffered reading for its ``(device, accel)`` pair, and the latest
        reading per device rides the next response's Metrics proto (the same
        piggyback channel as ``tokens_generated``). With no inference traffic
        nothing is sent until the next response — report from your own timer
        (e.g. a background thread started in ``setup()``) at a modest period
        (~10s); the server keeps the latest value per device.

        The server side exports the ``lite_server_accelerator_*`` Prometheus
        families and serves ``GET /metrics/accelerator``; the core never links
        a vendor SDK, so read the values with whatever your accelerator stack
        provides (pynvml, torch.mlu, torch_npu, ...). Fields left as ``None``
        are reported as absent (some accelerators expose no temperature).

        Thread-safe: shares ``_metric_lock`` with :meth:`report_metric`.

        Args:
            device: Device identifier (e.g. ``"0"`` or ``"cuda:0"``).
            accel: Accelerator kind — ``"cuda"``/``"mlu"``/``"npu"``/...
                (bounded cardinality; one tag per vendor stack).
            utilization_percent: Compute utilization, 0-100.
            memory_used_bytes: Device memory in use.
            memory_total_bytes: Device memory capacity.
            temperature_celsius: Device temperature.
        """
        reading: Dict[str, Any] = {"device": device, "accel": accel}
        if utilization_percent is not None:
            reading["utilization_percent"] = float(utilization_percent)
        if memory_used_bytes is not None:
            reading["memory_used_bytes"] = float(memory_used_bytes)
        if memory_total_bytes is not None:
            reading["memory_total_bytes"] = float(memory_total_bytes)
        if temperature_celsius is not None:
            reading["temperature_celsius"] = float(temperature_celsius)
        with self._metric_lock:
            # Bound the buffer by the server-side cap: (device, accel) keys
            # are model-code-controlled strings, so without a limit a buggy
            # model grows per-worker memory without bound between responses.
            if (
                (device, accel) not in self._accelerator_readings
                and len(self._accelerator_readings) >= _MAX_ACCELERATOR_DEVICES
            ):
                return
            self._accelerator_readings[(device, accel)] = reading

    # ===== Streaming Hooks (optional) =====

    def stream_predict(self, request: Any) -> Iterator[Any]:
        """Generator for server-side streaming (sync or async).

        Override to enable streaming output. Each yielded value is sent
        as a chunk to the client via SSE/WebSocket/gRPC.

        If not overridden, the worker automatically falls back to
        predict() and sends the result as a single chunk.

        To access the per-stream context, declare a second parameter named
        exactly ``ctx`` — detected once at load time::

            def stream_predict(self, request, ctx):
                for token in self.model.stream(request):
                    yield {"token": token, "request_id": ctx.meta.request_id}
        """
        raise NotImplementedError

    def bidi_stream(self) -> "BidiStreamHandler":
        """Return a handler for bidirectional streaming (e.g. ASR).

        To access the per-session context (metadata, state, decoded open
        payload), declare a parameter named exactly ``ctx``::

            def bidi_stream(self, ctx):
                return MyHandler(ctx.state.get("session_config"))

        The handler must implement ``on_open``, ``on_chunk``, and
        ``on_close`` — each may also declare ``ctx``.
        """
        raise NotImplementedError

    async def predict_decoupled(self, data: Any, sender: "ResponseSender") -> None:
        """Decoupled 1:N inference (P9-1 DecoupledInfer).

        Unlike :meth:`stream_predict` (a generator the worker *pulls* from),
        the model receives a push ``sender`` handle and may **return before
        the stream is done** — pushing N responses asynchronously
        (token-by-token, multiple candidates, progress) and ending with
        ``await sender.close()``. The channel stays open after this method
        returns; its lifetime is controlled explicitly by the model (or
        reclaimed by the server via idle timeout / client disconnect).

        Must be ``async`` (the sender is async). Override to enable;
        otherwise a ``DecoupledInfer`` request fails with FailedPrecondition.

        Example::

            async def predict_decoupled(self, data, sender):
                for tok in self.model.generate(data):
                    await sender.send({"token": tok})
                await sender.close()

        To access the per-stream context, declare a parameter named exactly
        ``ctx`` (after ``sender``)::

            async def predict_decoupled(self, data, sender, ctx):
                ...
        """
        raise NotImplementedError

    # ===== Continuous Batching Hooks (optional) =====

    def prefill(self, uid: str, decoded_input: Any) -> None:
        """Add a new sequence to the continuous batching state.

        Called when a new request arrives in CB mode.  Implement this to
        perform the initial forward pass (e.g. KV-cache prefill).

        To access per-request context, declare a fourth parameter named
        exactly ``ctx`` — detected once at session build time::

            def prefill(self, uid, decoded_input, ctx):
                ctx.state["kv_cache"] = allocate_cache(decoded_input)

        Args:
            uid: Unique request identifier.
            decoded_input: Output of decode_request.
        """
        pass

    def step(self, active_sequences: list) -> list[Any]:
        """Run one generation step for all active sequences.

        Each element in ``active_sequences`` is a :class:`CBSequence`
        with attributes ``uid``, ``input``, ``output`` (list of tokens so
        far), ``state`` (per-sequence user data), ``meta`` (request
        metadata), and ``ctx`` (full RequestContext).

        Declaring ``ctx`` on ``step`` is a load-time error — it operates
        across sequences and has no single per-request context.  Read
        per-sequence data from ``CBSequence.state`` / ``.meta`` instead.

        Returns:
            A list of new tokens, one per active sequence.
        """
        pass

    def has_finished(
        self, uid: str, token: Any, generated_sequence: list[Any]
    ) -> bool:
        """Check whether a sequence has finished generating.

        To access per-sequence context, declare a fifth parameter named
        exactly ``ctx``::

            def has_finished(self, uid, token, generated_sequence, ctx):
                return len(generated_sequence) >= ctx.state.get("max_tokens", 100)

        Returns True when the sequence should be removed from the active
        batch and its final response sent to the client.
        """
        return False

    # ===== Lifecycle Hooks =====

    def on_file_changed(self, changed_files: list[str]) -> Any:
        """Called when files in the model directory change (hot reload).

        Override to implement custom reload logic for weights, configs,
        vocab files, or any other model artifacts — refreshing them
        in-process instead of paying a full worker restart.

        The hook runs synchronously on the worker event loop (same as sync
        ``predict``): heavy refresh work blocks inference for its duration,
        and refreshing state while requests are in flight is the model
        author's responsibility.

        Args:
            changed_files: Absolute paths to files that have changed.

        Returns:
            Any non-None value marks the change as handled and suppresses
            the default fallback (a full worker restart). Returning None —
            or raising — lets the server restart the worker instead.
        """
        return None

    def teardown(self) -> None:
        """Called when the model is unloaded.

        Override to release resources (GPU memory, file handles, etc.).
        """
        pass


# Re-export exception classes for convenient access::
#
#     from lite_server import BadRequestError
#     from lite_server.api import HTTPException
#
from lite_server.exceptions import (  # noqa: F401, E402
    BadRequestError,
    ForbiddenError,
    HTTPException,
    InternalServerError,
    NotFoundError,
    ServiceUnavailableError,
    UnauthorizedError,
)
