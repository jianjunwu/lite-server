"""Echo stream model: zero-compute streaming mock for perf-smoke.

Yields `n` small chunks (default 20) as fast as the pipeline can deliver —
no sleep, no allocation beyond the chunk itself — so the measurement isolates
server-side stream overhead (SSE frame write + forward loop), not model time.
"""

from lite_server import LitAPI


class EchoStreamAPI(LitAPI):
    """A model that streams `n` echo chunks."""

    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request

    def predict(self, inputs):
        return inputs

    def stream_predict(self, inputs):
        n = int(inputs.get("n", 20)) if isinstance(inputs, dict) else 20
        for i in range(n):
            yield {"chunk": i, "n": n}

    def encode_response(self, output):
        return output
