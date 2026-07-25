"""Streaming model: yields tokens one at a time for real-time output.

Streaming is enabled via config.yaml (stream: true).
Override stream_predict() to yield chunks.
"""

import time

from lite_server import LitAPI, RequestContext


class StreamingAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return {
            "prompt": request.get("prompt", ""),
            "max_tokens": request.get("max_tokens", 5),
        }

    async def predict(self, x, ctx: RequestContext | None = None):
        # Non-streaming fallback
        if isinstance(x, list):
            return [self._generate(item) for item in x]
        return self._generate(x)

    def stream_predict(self, request, ctx: RequestContext | None = None):
        """Yield tokens one at a time for streaming output."""
        words = request.get("prompt", "hello").split()
        max_tokens = request.get("max_tokens", 5)
        for i in range(min(max_tokens, len(words))):
            time.sleep(0.05)  # simulate token generation latency
            yield {"token": words[i], "index": i}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output

    def _generate(self, x):
        words = x.get("prompt", "hello").split()
        max_tokens = x.get("max_tokens", 5)
        tokens = words[:max_tokens]
        return {"text": " ".join(tokens), "tokens": len(tokens)}
