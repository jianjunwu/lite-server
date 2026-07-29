"""Echo model: zero-compute mock for benchmarking pure framework overhead.

Returns the request payload unchanged — no sleep, no allocation beyond the
response itself. Both lite-server and LitServe load this model via the LitAPI
interface.
"""

from lite_server import LitAPI


class EchoAPI(LitAPI):
    """A model that echoes its input back."""

    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request.get("input", "")

    def predict(self, inputs):
        if isinstance(inputs, list):
            return [{"output": i} for i in inputs]
        return {"output": inputs}

    def encode_response(self, output):
        return output
