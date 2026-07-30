"""Error-handling callbacks: counts failures via on_error.

on_error runs when the request fails (a hook or stage raised). It is
exception-isolated: a failing on_error is logged, never masks the original error.
"""

import logging

from lite_server import Callback

logger = logging.getLogger("error_demo")


class ErrorMetrics(Callback):
    """Counts failed requests via on_error.

    This is the canonical pattern for collecting error telemetry from inside
    a model worker — counters like this one are cheap, concurrency-safe (one
    event loop per worker), and intended to scrape via a custom route.
    """

    def __init__(self):
        self.failures = 0
        self.by_type: dict = {}

    def on_error(self, ctx, exc):
        self.failures += 1
        exc_type = type(exc).__name__
        self.by_type[exc_type] = self.by_type.get(exc_type, 0) + 1
        logger.warning(
            "[ErrorMetrics] request %s failed: %s: %s (total=%d breakdown=%s)",
            ctx.meta.request_id, exc_type, exc, self.failures, self.by_type,
        )
