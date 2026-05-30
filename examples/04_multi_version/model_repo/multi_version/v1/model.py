"""Multi-version model v1: simple addition."""

from lite_server import LitAPI


class MyAPI(LitAPI):
    def setup(self, device):
        self.version = "v1"

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        if isinstance(x, list):
            return [{"output": item + 1, "version": self.version} for item in x]
        return {"output": x + 1, "version": self.version}

    def encode_response(self, output):
        return output
