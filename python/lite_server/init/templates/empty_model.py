from lite_server import LitAPI


class MyAPI(LitAPI):
    def setup(self, device):
        """Load model weights and initialize state."""
        self.model = lambda x: x * 2

    def decode_request(self, request):
        """Convert HTTP request JSON to model input."""
        return request["input"]

    def predict(self, x, **kwargs):
        """Run inference. x is a list when batching is enabled."""
        if isinstance(x, list):
            return [self.model(item) for item in x]
        return self.model(x)

    def encode_response(self, output):
        """Convert model output to HTTP response JSON."""
        return {"result": output}
