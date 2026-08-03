"""Bidi echo model: zero-compute bidi streaming mock for chunk-overhead bench.

Echoes each incoming chunk back verbatim — no sleep, no allocation beyond the
reply — so per-chunk round-trip latency isolates plumbing overhead (gRPC + ZMQ
+ worker bidi dispatch), not model time.
"""

from lite_server import LitAPI, BidiStreamHandler, RequestContext


class BidiEchoHandler(BidiStreamHandler):
    """Echo handler: on_open acks, on_chunk echoes, on_close finalizes."""

    def on_open(self, initial_data, ctx: RequestContext | None = None):
        return {"status": "ready"}

    def on_chunk(self, chunk, ctx: RequestContext | None = None):
        return {"echo": chunk}

    def on_close(self, ctx: RequestContext | None = None):
        return {"final": True}


class BidiEchoAPI(LitAPI):
    """Zero-compute bidi model — measures the pipe, not the model."""

    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request

    async def predict(self, x, ctx: RequestContext | None = None):
        return {"output": "sync fallback"}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output

    def bidi_stream(self, ctx: RequestContext | None = None):
        return BidiEchoHandler()
