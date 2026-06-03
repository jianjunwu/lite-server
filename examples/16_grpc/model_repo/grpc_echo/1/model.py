"""gRPC demo: same model accessible via HTTP and gRPC.

No special model code needed — gRPC endpoints are auto-generated from the
same LitAPI interface. The config enables gRPC on port 8001.
"""

from lite_server import LitAPI


class GrpcEchoAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request.get("input", "")

    def predict(self, x):
        if isinstance(x, list):
            return [{"output": f"grpc_echo: {item}"} for item in x]
        return {"output": f"grpc_echo: {x}"}

    def encode_response(self, output):
        return output
