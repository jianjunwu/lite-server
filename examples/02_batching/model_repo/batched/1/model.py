"""Batched model: groups requests into batches for higher throughput.

Batching is configured in config.yaml (max_batch_size, batch_timeout).  When
``max_batch_size > 1`` the server groups concurrent requests and ``predict``
receives them as a list (one call per batch).
"""

from lite_server import LitAPI, RequestContext


class BatchedAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request.get("input", "")

    async def predict(self, x):
        # When batching is enabled, `x` is a list of decoded inputs.
        if isinstance(x, list):
            return [{"output": item, "batch_size": len(x)} for item in x]
        return {"output": x, "batch_size": 1}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
