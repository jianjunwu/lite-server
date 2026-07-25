"""Bidirectional streaming demo: simulates real-time ASR (speech recognition).

Implements bidi_stream() returning a BidiStreamHandler with on_open(),
on_chunk(), and on_close().  Since 0.7.0 each hook may declare a ``ctx``
parameter; per-session state is kept in ``ctx.state`` (shared across the
session's hooks, safe under concurrency).
"""

from lite_server import LitAPI, BidiStreamHandler, RequestContext


class ASRHandler(BidiStreamHandler):
    def __init__(self, model):
        self.model = model

    def on_open(self, initial_data, ctx: RequestContext | None = None):
        """Called when the stream opens. Returns initial response."""
        ctx.state["buffer"] = []
        return {"status": "ready", "sample_rate": 16000}

    def on_chunk(self, chunk, ctx: RequestContext | None = None):
        """Process each incoming audio chunk. Returns partial result if available."""
        text = chunk.get("text", "")
        if text:
            ctx.state["buffer"].append(text)
            partial = " ".join(ctx.state["buffer"])
            return {"partial": partial, "is_final": False}
        return None

    def on_close(self, ctx: RequestContext | None = None):
        """Called when stream closes. Returns final result."""
        buffer = ctx.state.get("buffer", [])
        result = " ".join(buffer)
        return {"final": result, "is_final": True, "buffer": buffer}


class ASRAPI(LitAPI):
    def setup(self, device):
        self.model_loaded = True

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request

    async def predict(self, x, ctx: RequestContext | None = None):
        return {"output": "sync fallback"}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output

    def bidi_stream(self, ctx: RequestContext | None = None):
        return ASRHandler(self)
