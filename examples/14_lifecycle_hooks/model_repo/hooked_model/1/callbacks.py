"""Example callbacks demonstrating the Callback lifecycle hook system.

Two callbacks are defined:
- AuditLogger: records request timing and logs each inference call
- ResponseEnricher: adds request metadata to every response

Multiple callbacks can be registered in config.yaml via the ``callbacks`` key.
They chain in registration order with automatic exception isolation.

Note: Callback exceptions are intentionally swallowed (exception isolation).
Callbacks should transform data or produce side effects, not reject requests.
"""

import time
from lite_server import Callback


class AuditLogger(Callback):
    """Logs inference timing and request metadata for every request."""

    def on_before_decode(self, request, meta):
        # Record start time for latency tracking
        self._start_ns = time.time_ns()
        print(f"[AuditLogger] Request {meta.request_id} from {meta.client_ip} "
              f"route={meta.route}")
        return request

    def on_after_predict(self, output, meta):
        elapsed_ms = (time.time_ns() - self._start_ns) / 1_000_000
        print(f"[AuditLogger] Request {meta.request_id} completed "
              f"in {elapsed_ms:.2f}ms")
        return output

    def on_teardown(self, lit_api):
        print(f"[AuditLogger] Model shutting down. "
              f"Total requests handled: {lit_api.call_count}")


class ResponseEnricher(Callback):
    """Adds request metadata to each response before it is encoded."""

    def on_before_encode(self, output, meta):
        if isinstance(output, dict):
            output["_meta"] = {
                "request_id": meta.request_id,
                "route": meta.route,
                "client_ip": meta.client_ip,
                "latency_ns": time.time_ns() - meta.timestamp_ns,
            }
        return output
