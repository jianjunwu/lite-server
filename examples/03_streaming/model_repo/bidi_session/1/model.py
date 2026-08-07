"""Session-based bidirectional streaming: bidi_stream() lifecycle handler.

Demonstrates the on_open → on_chunk → on_close lifecycle with per-session state
stored in ctx.state. Each hook returns response chunks sent back to the client.

Accessed via WebSocket or gRPC bidi. The first-frame payload becomes the
initial_data passed to on_open();
subsequent C→S frames trigger on_chunk().
"""

from lite_server import LitAPI, BidiStreamHandler, RequestContext


class SessionHandler(BidiStreamHandler):
    def on_open(self, initial_data, ctx: RequestContext | None = None):
        """Called once when the session opens. Returns initial response."""
        ctx.state["chunks_seen"] = 0
        ctx.state["session_started"] = True
        return {"event": "open", "message": "session started"}

    def on_chunk(self, chunk, ctx: RequestContext | None = None):
        """Process each incoming client message. Returns echo + counter."""
        ctx.state["chunks_seen"] = ctx.state.get("chunks_seen", 0) + 1
        return {
            "event": "chunk",
            "echo": str(chunk),
            "count": ctx.state["chunks_seen"],
        }

    def on_close(self, ctx: RequestContext | None = None):
        """Called when the session closes. Returns final summary."""
        return {
            "event": "close",
            "total_chunks": ctx.state.get("chunks_seen", 0),
        }


class BidiSessionAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request

    async def predict(self, x, ctx: RequestContext | None = None):
        return {"output": "sync fallback"}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output

    def bidi_stream(self, ctx: RequestContext | None = None):
        return SessionHandler()
