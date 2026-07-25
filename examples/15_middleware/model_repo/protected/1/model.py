"""Middleware demo: protected endpoint with auth, rate limiting, CORS, and logging.

The model provides standard inference. Custom endpoints (endpoints/status.py)
demonstrate middleware chains.
"""

from lite_server import LitAPI, RequestContext


class ProtectedAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request.get("input", "")

    async def predict(self, x, ctx: RequestContext | None = None):
        if isinstance(x, list):
            return [{"output": f"protected: {item}"} for item in x]
        return {"output": f"protected: {x}"}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
