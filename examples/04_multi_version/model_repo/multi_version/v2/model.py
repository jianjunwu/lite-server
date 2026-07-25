"""Multi-version model v2: multiplication (upgraded algorithm)."""

from lite_server import LitAPI, RequestContext


class MyAPI(LitAPI):
    def setup(self, device):
        self.version = "v2"

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request.get("input", 0)

    async def predict(self, x, ctx: RequestContext | None = None):
        if isinstance(x, list):
            return [{"output": item * 2, "version": self.version} for item in x]
        return {"output": x * 2, "version": self.version}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
