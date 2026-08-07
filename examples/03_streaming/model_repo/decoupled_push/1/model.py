"""Decoupled push streaming: model pushes chunks asynchronously via sender.

Accessed via:
  - POST /v2/models/decoupled_push/decoupled       (SSE decoupled)
  - GET  /v2/models/decoupled_push/decoupled-stream (WebSocket decoupled)

Unlike stream_predict() (pull-based generator), predict_decoupled() receives
a sender handle and may keep pushing after the method returns.  The channel
lifetime is controlled by the model (sender.close()) or reclaimed by the server
(idle timeout / client cancel).
"""

import asyncio

from lite_server import LitAPI, RequestContext


class DecoupledPushAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return {
            "message": request.get("message", "decoupled test"),
            "chunks": request.get("chunks", 5),
        }

    async def predict(self, x, ctx: RequestContext | None = None):
        # Non-streaming fallback
        return {"output": f"sync: {x['message']}"}

    async def predict_decoupled(self, data, sender, ctx: RequestContext | None = None):
        """Push N chunks asynchronously, then close the stream."""
        chunks = data.get("chunks", 5)
        message = data.get("message", "decoupled test")
        for i in range(chunks):
            await asyncio.sleep(0.05)
            await sender.send({"chunk_index": i, "message": message})
        await sender.close()

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
