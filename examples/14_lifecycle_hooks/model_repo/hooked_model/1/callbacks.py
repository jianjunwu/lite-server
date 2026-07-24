"""Example callbacks demonstrating the Callback pipeline hook system.

Two callbacks are defined:
- AuditLogger: records request timing and logs each inference call
- ResponseEnricher: adds request metadata to every response

Multiple callbacks can be registered in config.yaml via the ``callbacks`` key.
They chain in registration order.

Key rules for callbacks (since 0.7.0):
- Data hooks receive a single ``ctx`` (RequestContext) argument and may be
  sync or async.
- Per-request data goes in ``ctx.state`` — never in ``self`` attributes,
  which are shared across concurrent requests.
- A hook may reject the request by raising ``HTTPException`` (validation,
  auth, ...), or short-circuit it via ``ctx.respond(...)`` (early return).
  Exceptions from data hooks are NOT swallowed.
"""

import time
from lite_server import Callback


class AuditLogger(Callback):
    """Logs inference timing and request metadata for every request."""

    def on_request(self, ctx):
        # Record start time in per-request state (safe under concurrency)
        ctx.state["start_ns"] = time.time_ns()
        meta = ctx.meta
        print(f"[AuditLogger] Request {meta.request_id} from {meta.client_ip} "
              f"route={meta.route}")

    def on_output(self, ctx):
        elapsed_ms = (time.time_ns() - ctx.state["start_ns"]) / 1_000_000
        print(f"[AuditLogger] Request {ctx.meta.request_id} completed "
              f"in {elapsed_ms:.2f}ms")

    def on_teardown(self, lit_api):
        print(f"[AuditLogger] Model shutting down. "
              f"Total requests handled: {lit_api.call_count}")


class ResponseEnricher(Callback):
    """Adds request metadata to each response before it is encoded."""

    def on_output(self, ctx):
        if isinstance(ctx.output, dict):
            ctx.output["_meta"] = {
                "request_id": ctx.meta.request_id,
                "route": ctx.meta.route,
                "client_ip": ctx.meta.client_ip,
                "latency_ns": time.time_ns() - ctx.meta.timestamp_ns,
            }
