"""Streaming model: yields tokens one at a time for real-time output.

Streaming is enabled via config.yaml (stream: true).
Override stream_predict() to yield chunks.
"""

import time

from lite_server import LitAPI


class StreamingAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return {
            "prompt": request.get("prompt", ""),
            "max_tokens": request.get("max_tokens", 5),
        }

    def predict(self, x, **kwargs):
        # Non-streaming fallback
        if isinstance(x, list):
            return [self._generate(item) for item in x]
        return self._generate(x)

    def stream_predict(self, request):
        """Yield tokens one at a time for streaming output."""
        words = request.get("prompt", "hello").split()
        max_tokens = request.get("max_tokens", 5)
        for i in range(min(max_tokens, len(words))):
            time.sleep(0.05)  # simulate token generation latency
            yield {"token": words[i], "index": i}

    def encode_response(self, output):
        return output

    def _generate(self, x):
        words = x.get("prompt", "hello").split()
        max_tokens = x.get("max_tokens", 5)
        tokens = words[:max_tokens]
        return {"text": " ".join(tokens), "tokens": len(tokens)}
