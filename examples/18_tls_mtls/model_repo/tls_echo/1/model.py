"""Minimal echo model served over TLS/mTLS — same shape as 01_basic."""

from lite_server import LitAPI


class TlsEchoAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request, ctx=None):
        return request.get("input")

    def predict(self, x, ctx=None):
        return x * 2

    def encode_response(self, output, ctx=None):
        return {"output": output}
