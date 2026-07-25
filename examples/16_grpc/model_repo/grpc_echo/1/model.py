"""gRPC demo: same model accessible via HTTP and gRPC.

No special model code needed — gRPC endpoints are auto-generated from the
same LitAPI interface. The config enables gRPC on port 8001.
"""

from lite_server import LitAPI, RequestContext


class GrpcEchoAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request.get("input", "")

    async def predict(self, x, ctx: RequestContext | None = None):
        if isinstance(x, list):
            return [{"output": f"grpc_echo: {item}"} for item in x]
        return {"output": f"grpc_echo: {x}"}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
