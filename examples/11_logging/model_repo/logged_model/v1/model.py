"""Model demonstrating structured logging at different stages, plus
response headers via :class:`~lite_server.api.ResponseWithHeaders`."""

import time

from lite_server import LitAPI, ResponseWithHeaders


class LoggedModelAPI(LitAPI):
    def setup(self, device):
        self.device = device
        self.call_count = 0

        # Log at different levels; visible output depends on --log-level
        self.logger.debug("Debug: entering setup()")
        self.logger.info("Info: model loading on device=%s", device)
        self.logger.warning("Warning: this is a demo model, not for production")

    def decode_request(self, request):
        self.logger.debug("Debug: decode_request input=%s", request)
        return request.get("input", 0)

    def predict(self, x):
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

    def encode_response(self, output):
        self.logger.debug("Debug: encode_response output=%s", output)
        return output

    def on_request(self, request, meta):
        self.logger.info(
            "Info: request from %s | route=%s | request_id=%s",
            meta.client_ip, meta.route, meta.request_id,
        )
        return request

    def on_response(self, response, meta):
        self.logger.info(
            "Info: response ready | request_id=%s | output=%s",
            meta.request_id, response,
        )
        return ResponseWithHeaders(
            body=response,
            headers={
                "X-Request-ID": meta.request_id,
                "X-Call-Count": str(self.call_count),
            },
        )
