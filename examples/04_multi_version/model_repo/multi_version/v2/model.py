"""Multi-version model v2: multiplication (upgraded algorithm)."""

from lite_server import LitAPI


class MyAPI(LitAPI):
    def setup(self, device):
        self.version = "v2"

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        if isinstance(x, list):
            return [{"output": item * 2, "version": self.version} for item in x]
        return {"output": x * 2, "version": self.version}

    def encode_response(self, output):
        return output
