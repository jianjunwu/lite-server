"""Model demonstrating structured logging at different stages, plus
response headers via :meth:`~lite_server.RequestContext.respond`.

Request/response hooks live on a Callback subclass (below), not on LitAPI —
since 0.8.0 LitAPI only carries the pipeline stages (decode/predict/encode).
"""

import logging
import time

from lite_server import Callback, LitAPI, RequestContext


class LoggedModelAPI(LitAPI):
    def setup(self, device):
        self.device = device
        self.call_count = 0

        # Log at different levels; visible output depends on --log-level
        self.logger.debug("Debug: entering setup()")
        self.logger.info("Info: model loading on device=%s", device)
        self.logger.warning("Warning: this is a demo model, not for production")

    async def decode_request(self, request, ctx: RequestContext | None = None):
        self.logger.debug("Debug: decode_request input=%s", request)
        return request.get("input", 0)

    async def predict(self, x, ctx: RequestContext | None = None):
        self.call_count += 1
        start = time.time()

        # Simulate work
        result = x * 2

        elapsed_ms = (time.time() - start) * 1000
        self.logger.info(
            "Info: predict #%s input=%s output=%s elapsed_ms=%.3f",
            self.call_count, x, result, elapsed_ms,
        )
        return {"output": result, "call_count": self.call_count}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        self.logger.debug("Debug: encode_response output=%s", output)
        return output


class RequestResponseLogger(Callback):
    """Request/response hooks (logging + custom response headers).

    Registered via ``callbacks:`` in config.yaml — since 0.8.0 LitAPI only
    carries the pipeline stages; hooks live on Callback subclasses.
    """

    def __init__(self):
        self.call_count = 0
        self.logger = logging.getLogger("RequestResponseLogger")

    def before_decode_request(self, ctx):
        self.call_count += 1
        self.logger.info(
            "Info: request from %s | route=%s | request_id=%s",
            ctx.meta.client_ip, ctx.meta.route, ctx.meta.request_id,
        )
        return ctx.request

    def after_encode_response(self, ctx):
        self.logger.info(
            "Info: response ready | request_id=%s | output=%s",
            ctx.meta.request_id, ctx.response,
        )
        return ctx.respond(
            ctx.response,
            headers={
                "X-Request-ID": ctx.meta.request_id,
                "X-Call-Count": str(self.call_count),
            },
        )
