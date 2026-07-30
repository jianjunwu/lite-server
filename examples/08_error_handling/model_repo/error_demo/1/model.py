"""Error handling demo: exercises the framework's exception-to-HTTP mapping
and per-request timeout.

The model accepts a ``mode`` field to trigger different error paths:

- ``"normal"`` — success (200)
- ``"bad_request"`` — raises BadRequestError (400)
- ``"not_found"`` — raises NotFoundError (404)
- ``"server_error"`` — raises an unhandled RuntimeError (500)
- ``"slow"`` — sleeps past request_timeout to trigger a 504 / Gateway Timeout
"""

import asyncio

from lite_server import LitAPI, RequestContext
from lite_server.exceptions import BadRequestError, NotFoundError

MODES = {"normal", "bad_request", "not_found", "server_error", "slow"}


class ErrorDemoAPI(LitAPI):

    def setup(self, device):
        self.device = device
        # Read request_timeout from config for informational purposes
        cfg_timeout = self.config.get("request_timeout", 0)
        self._timeout_info = f"request_timeout={cfg_timeout}s"

    async def decode_request(self, request, ctx: RequestContext | None = None):
        mode = request.get("mode", "normal")
        if mode not in MODES:
            raise BadRequestError(
                f"unknown mode: '{mode}'. Valid modes: {', '.join(sorted(MODES))}",
                param="mode",
            )
        return {"input": request.get("input", ""), "mode": mode}

    async def predict(self, x, ctx: RequestContext | None = None):
        mode = x["mode"]
        payload = x["input"]

        if mode == "bad_request":
            raise BadRequestError("client sent invalid data", param="input")

        if mode == "not_found":
            raise NotFoundError(f"resource not found: {payload}")

        if mode == "server_error":
            raise RuntimeError(f"simulated crash processing '{payload}'")

        if mode == "slow":
            # Sleep longer than request_timeout (configured as 2s in config.yaml).
            await asyncio.sleep(5.0)
            return {"output": f"this should never arrive", "timeout": self._timeout_info}

        # normal
        return {"output": f"ok: {payload}", "timeout": self._timeout_info}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
