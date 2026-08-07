"""SSE token streaming: yields tokens one at a time via stream_predict().

Accessed via POST /v2/models/sse_tokens/events — the classic SSE event-stream.
"""

import time

from lite_server import LitAPI, RequestContext


class SseTokensAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return {
            "prompt": request.get("prompt", "hello world streaming test"),
            "max_tokens": request.get("max_tokens", 10),
        }

    async def predict(self, x, ctx: RequestContext | None = None):
        # Non-streaming fallback
        words = x["prompt"].split()[: x["max_tokens"]]
        return {"text": " ".join(words), "tokens": len(words)}

    def stream_predict(self, request, ctx: RequestContext | None = None):
        """Yield tokens one at a time. The server sends each chunk as an SSE event."""
        words = request["prompt"].split()
        max_tokens = request["max_tokens"]
        for i in range(min(max_tokens, len(words))):
            time.sleep(0.02)  # simulate token generation
            yield {"token": words[i], "index": i}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
