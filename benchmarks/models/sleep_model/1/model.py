"""Sleep model: CPU-bound mock for benchmarking IPC overhead.

Simulates 10ms compute latency using time.sleep().
Both lite-server and LitServe load this model via the LitAPI interface.
"""

import time

from lite_server import LitAPI


class SleepAPI(LitAPI):
    """A model that sleeps for 10ms per request."""

    SLEEP_TIME = 0.01  # 10ms per request

    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request.get("input", "")

    def predict(self, inputs):
        time.sleep(self.SLEEP_TIME)
        if isinstance(inputs, list):
            return [{"output": i, "sleep_ms": self.SLEEP_TIME * 1000} for i in inputs]
        return {"output": inputs, "sleep_ms": self.SLEEP_TIME * 1000}

    def encode_response(self, output):
        return output
