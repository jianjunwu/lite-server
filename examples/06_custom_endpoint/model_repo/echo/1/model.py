"""Echo model for custom endpoint example."""

from lite_server import LitAPI, RequestContext


class EchoAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request.get("input", 0)

    async def predict(self, x, ctx: RequestContext | None = None):
        if isinstance(x, list):
            return [{"output": item * 2} for item in x]
        return {"output": x * 2}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
