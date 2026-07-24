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
from typing import Any, Iterator, List, Tuple

from lite_server.context import RequestContext


@dataclass
class _MetricSpec:
    """Internal: pre-registered metric specification."""

    name: str
    metric_type: str  # "gauge" | "counter" | "histogram"
    metric_id: int = 0  # per-type index for Rust-side Vec lookup


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

        Operates across items — declaring ``ctx`` is a load-time error.
        Thread per-request data through the decoded input instead.
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
        exactly ``ctx``.  **Not compatible with** ``batch`` + ``unbatch``:
        when batch is overridden, predict runs once per batch and has no
        per-request context — declaring ``ctx`` is a load-time error.
        Thread per-request data through the decoded input instead
        (``decode_request`` supports ctx).
        """
        raise NotImplementedError("predict is not implemented")

    def unbatch(self, output: Any) -> list:
        """Convert a batched output back to a list of per-input outputs.

        Operates across items — declaring ``ctx`` is a load-time error.
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

    # ===== Request/Response Hooks =====

    def on_request(self, ctx: RequestContext) -> Any | None:
        """Called before decode_request.  Same contract as Callback hooks.

        The place for auth, schema validation, and cache lookups.  Read
        ``ctx.request`` (raw payload) and ``ctx.meta`` (headers, route,
        client IP, request ID); share data with later stages via
        ``ctx.state`` — never via ``self`` attributes.  Raising an
        exception rejects the request with an Error response.

        Returns:
            Replacement for ``ctx.request``, a :class:`Response` (or
            ``ctx.respond(...)``) for early return, or None to pass through.
        """
        return None

    def on_response(self, ctx: RequestContext) -> Any | None:
        """Called after encode_response — the last hook before sending.

        To attach custom HTTP response headers::

            def on_response(self, ctx):
                return ctx.respond(
                    ctx.response,
                    headers={"X-Request-ID": ctx.meta.request_id},
                )

        Returns:
            Replacement for ``ctx.response``, a :class:`Response` (or
            ``ctx.respond(...)``), or None to pass through.
        """
        return None

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
        vocab files, or any other model artifacts.

        Args:
            changed_files: Absolute paths to files that have changed.

        Returns:
            Any non-None value suppresses the default fallback behavior.
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
