"""Canary version v1: input + 1 (the "old" behavior)."""

from lite_server import LitAPI


class CanaryV1API(LitAPI):
    def setup(self, device):
        self.device = device
        self.version = "v1"

    def decode_request(self, request, ctx=None):
        return request.get("input")

    def predict(self, x, ctx=None):
        return x + 1

    def encode_response(self, output, ctx=None):
        return {"output": output, "version": self.version}
