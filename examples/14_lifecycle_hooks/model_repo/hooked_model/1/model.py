"""Lifecycle hooks demo: shell commands and HTTP callbacks.

Demonstrates the full worker lifecycle: on_ready, on_error, on_exit hooks.
Hook commands are configured in config.yaml, not in model.py.
The model itself is a simple echo to verify it works alongside hooks.
"""

from lite_server import LitAPI, RequestContext


class HookedAPI(LitAPI):
    def setup(self, device):
        self.logger.info("setup called on device=%s", device)
        self.device = device
        self.call_count = 0

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request.get("input", "")

    async def predict(self, x, ctx: RequestContext | None = None):
        self.call_count += 1
        if isinstance(x, list):
            return [{"output": item, "count": self.call_count} for item in x]
        return {"output": x, "count": self.call_count}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output

    def teardown(self):
        self.logger.info("teardown called, total requests handled: %d", self.call_count)
