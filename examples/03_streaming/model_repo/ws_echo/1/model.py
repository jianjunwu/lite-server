"""WebSocket echo: demonstrates bidirectional C↔S frames via WebSocket.

Accessed via GET /v2/models/ws_echo/stream (upgrade to WebSocket).
The first frame carries the request payload (Text → JSON, Binary → raw bytes).
Subsequent Binary client→server frames are echoed back as response chunks.
"""

import time

from lite_server import LitAPI, RequestContext


class WsEchoAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        # For WS, the first-frame payload arrives here. Text frames are JSON;
        # Binary frames are raw bytes (normalized to application/octet-stream).
        return {
            "payload": request,
            "count": request.get("count", 3) if isinstance(request, dict) else 3,
        }

    async def predict(self, x, ctx: RequestContext | None = None):
        # Non-streaming fallback
        return {"echo": str(x.get("payload", x))}

    def stream_predict(self, request, ctx: RequestContext | None = None):
        """Yield echo chunks. In WS mode, client C→S frames arrive as
        additional stream inputs; the echoed data flows back via S→C chunks."""
        count = request.get("count", 3)
        payload = request.get("payload", request)
        for i in range(count):
            time.sleep(0.03)
            yield {"chunk": i, "echo": str(payload)}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
