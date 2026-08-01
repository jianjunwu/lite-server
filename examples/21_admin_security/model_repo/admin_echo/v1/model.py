"""Minimal echo model — the admin/security machinery is orthogonal to it."""

from lite_server import LitAPI


class AdminEchoAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request, ctx=None):
        return request.get("input")

    def predict(self, x, ctx=None):
        return {"echo": x}

    def encode_response(self, output, ctx=None):
        return {"output": output}
