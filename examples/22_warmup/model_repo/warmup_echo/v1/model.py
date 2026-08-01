"""Warmup model: counts dummy-inference calls (input 42) made by the warmup
pass and exposes them via a custom route, so checks can prove warmup ran."""

import time

from lite_server import LitAPI, route


class WarmupEchoAPI(LitAPI):
    def setup(self, device):
        self.device = device
        self.warmup_count = 0

    def decode_request(self, request, ctx=None):
        return request.get("input")

    def predict(self, x, ctx=None):
        if x == 42:  # the dummy input from warmup/input.json
            self.warmup_count += 1
            time.sleep(0.5)  # simulate engine warm-up work
        return {"warmup_count": self.warmup_count}

    def encode_response(self, output, ctx=None):
        return {"output": output}

    @route.get("/stats")
    def stats(self, ctx):
        return {"warmup_count": self.warmup_count}
