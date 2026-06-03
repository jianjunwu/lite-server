"""Enhanced LitAPI with lifecycle hooks for lite-server.

Model authors subclass ``lite_server.api.LitAPI`` instead of
``litserve.LitAPI`` to gain access to framework-level hooks
(teardown, on_file_changed, logger, on_request, on_response).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Tuple

import litserve as ls

if TYPE_CHECKING:
    from litserve.specs.base import LitSpec


@dataclass
class RequestMeta:
    """Metadata about the original HTTP request, passed to hooks."""

    route: str
    headers: dict[str, str]
    client_ip: str
    request_id: str
    timestamp_ns: int
    payload: Any  # decoded original request body


@dataclass
class _MetricSpec:
    """Internal: pre-registered metric specification."""

    name: str
    metric_type: str  # "gauge" | "counter" | "histogram"


class LitAPI(ls.LitAPI):
    """Drop-in replacement for ``ls.LitAPI`` with lite-server hooks.

    Usage is identical to ``ls.LitAPI``::

        from lite_server import LitAPI

        class MyModel(LitAPI):
            def setup(self, device):
                self.model = load_model()

            def predict(self, x):
                return self.model(x)
    """

    def __init__(
        self,
        max_batch_size: int = 1,
        batch_timeout: float = 0.0,
        stream: bool = False,
        loop: Any = "auto",
        spec: LitSpec | None = None,
        mcp: Any = None,
        enable_async: bool = False,
    ):
        super().__init__(
            max_batch_size=max_batch_size,
            batch_timeout=batch_timeout,
            stream=stream,
            loop=loop,
            spec=spec,
            mcp=mcp,
            enable_async=enable_async,
        )
        self.config: dict[str, Any] = {}
        self._logger: logging.Logger | None = None
        self._metric_specs: List[_MetricSpec] = []
        self._metric_values: List[Tuple[int, float]] = []

    @property
    def logger(self) -> logging.Logger:
        """Lazy logger bound to the model class name."""
        if self._logger is None:
            self._logger = logging.getLogger(
                self.__class__.__module__ + "." + self.__class__.__name__
            )
        return self._logger

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
        self._metric_specs.append(_MetricSpec(name, metric_type))
        return idx

    def report_metric(self, metric_id: int, value: float) -> None:
        """Report a metric value by pre-registered ID.

        Hot path — only ``list.append((int, float))``, ~50 ns.

        Args:
            metric_id: ID returned by :meth:`register_metric`.
            value: Metric value.
        """
        self._metric_values.append((metric_id, value))

    # ===== Streaming Hooks (optional) =====

    def stream_predict(self, request: Any) -> Iterator[Any]:
        """Generator for server-side streaming.

        Override to enable streaming output. Each yielded value is sent
        as a chunk to the client via SSE/WebSocket/gRPC.

        If not overridden, the worker automatically falls back to
        predict() and sends the result as a single chunk.
        """
        raise NotImplementedError

    def bidi_stream(self) -> "BidiStreamHandler":
        """Return a handler for bidirectional streaming (e.g. ASR).

        The handler must implement:
          - on_chunk(chunk) -> Optional[output_chunk]
          - on_close()
        """
        raise NotImplementedError

    # ===== Request/Response Hooks =====

    def on_request(self, request: Any, meta: RequestMeta) -> Any:
        """Called before decode_request.

        Override to modify the raw request, perform auth checks, inject context,
        or log request metadata.  Raising an exception rejects the request
        and returns an Error response to the client.

        Args:
            request: The raw request payload (JSON dict from HTTP body).
            meta: Original HTTP request metadata (headers, route, ip, etc.).

        Returns:
            The (possibly modified) raw request to pass to decode_request.
        """
        return request

    def on_response(self, response: Any, meta: RequestMeta) -> Any:
        """Called after encode_response, before sending to the client.

        Override to modify the response, inject headers, or log.

        Args:
            response: The encoded response (output of encode_response).
            meta: Original HTTP request metadata.

        Returns:
            The (possibly modified) response to send to the client.
        """
        return response

    # ===== Continuous Batching Hooks (optional) =====

    def prefill(self, uid: str, decoded_input: Any) -> None:
        """Add a new sequence to the continuous batching state.

        Called when a new request arrives in CB mode.  Implement this to
        perform the initial forward pass (e.g. KV-cache prefill).

        Args:
            uid: Unique request identifier.
            decoded_input: Output of decode_request.
        """
        pass

    def step(self, active_sequences: list[dict]) -> list[Any]:
        """Run one generation step for all active sequences.

        Each element in ``active_sequences`` is a dict with keys:
        ``uid``, ``input``, ``output`` (list of tokens so far).

        Returns:
            A list of new tokens, one per active sequence.
        """
        pass

    def has_finished(self, uid: str, token: Any, generated_sequence: list[Any]) -> bool:
        """Check whether a sequence has finished generating.

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
