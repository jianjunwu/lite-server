"""Basic echo model: returns input * 2."""

from lite_server import LitAPI


class EchoAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        if isinstance(x, list):
            return [{"output": item * 2} for item in x]
        return {"output": x * 2}

    def encode_response(self, output):
        return output
