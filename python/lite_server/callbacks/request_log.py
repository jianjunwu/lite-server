"""Request logging callback."""

import logging
import time

from lite_server.callbacks._base import Callback


class LogRequests(Callback):
    """Log method, route, status, and elapsed time — including rejections."""

    def __init__(self, *, logger_name: str = "lite_server.requests"):
        self._logger = logging.getLogger(logger_name)
        self._key = f"_logreq_start_{id(self)}"

    def on_request(self, ctx):
        ctx.state[self._key] = time.monotonic()

    def on_response(self, ctx):
        status = ctx.early.status_code if ctx.early is not None else 200
        self._log(ctx, status)

    def on_error(self, ctx, exc):
        from lite_server.exceptions import HTTPException

        status = exc.status_code if isinstance(exc, HTTPException) else 500
        self._log(ctx, status)

    def _log(self, ctx, status: int) -> None:
        start = ctx.state.pop(self._key, None)
        if start is None:
            return
        elapsed_ms = (time.monotonic() - start) * 1000
        self._logger.info(
            "%s %s → %d %.2fms", ctx.meta.method, ctx.meta.route, status, elapsed_ms
        )
