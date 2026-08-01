"""Canary version v2: input * 2 (the "new" behavior being rolled out)."""

from lite_server import LitAPI


class CanaryV2API(LitAPI):
    def setup(self, device):
        self.device = device
        self.version = "v2"

    def decode_request(self, request, ctx=None):
        return request.get("input")

    def predict(self, x, ctx=None):
        return x * 2

    def encode_response(self, output, ctx=None):
        return {"output": output, "version": self.version}
