"""A deliberately slow model (0.8s per inference) so concurrent requests
saturate max_inflight and trigger the overload controls."""

import time

from lite_server import LitAPI


class SlowEchoAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request, ctx=None):
        return request.get("input")

    def predict(self, x, ctx=None):
        time.sleep(0.8)  # simulate a slow GPU/LLM call
        return {"slow": x}

    def encode_response(self, output, ctx=None):
        return {"output": output}
