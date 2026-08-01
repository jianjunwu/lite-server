"""Echo model that reports the cleansed client IP (P-XFF) and supports
streaming (so the WebSocket endpoint exists for Origin checks)."""

import time

from lite_server import LitAPI


class ProxyEchoAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request, ctx=None):
        return request.get("input")

    def predict(self, x, ctx=None):
        client_ip = ctx.meta.client_ip if ctx is not None else "?"
        return {"echo": x, "client_ip": client_ip}

    def encode_response(self, output, ctx=None):
        return {"output": output}

    def stream_predict(self, request, ctx=None):
        for i in range(3):
            time.sleep(0.05)
            yield {"token": i}
