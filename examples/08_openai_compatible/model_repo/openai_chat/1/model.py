"""OpenAI-compatible chat completion endpoint.

Demonstrates using OpenAIEndpoint base class to create a /v1/chat/completions
compatible endpoint with zero boilerplate.
"""

import asyncio
import time

from lite_server.specs.openai import OpenAIEndpoint


class ChatModel(OpenAIEndpoint):
    model = "demo-chat"

    def setup(self):
        """Initialize resources."""
        self.start_time = time.time()

    def decode_request(self, request):
        """Extract the last user message as prompt."""
        messages = request.get("messages", [])
        # Find last user message
        for msg in reversed(messages):
            if msg.get("role") == "user":
                return msg.get("content", "")
        return ""

    def predict(self, x):
        """Generate a response (echo with prefix)."""
        return f"Echo: {x}"

    async def stream_predict(self, x):
        """Stream response token by token."""
        response = f"Echo: {x}"
        # Simulate token-by-token generation
        for i, char in enumerate(response):
            yield {
                "choices": [{"delta": {"content": char}, "index": 0}]
            }
            await asyncio.sleep(0.02)  # Simulate latency
        # Final chunk with finish_reason
        yield {
            "choices": [{"delta": {}, "index": 0, "finish_reason": "stop"}]
        }
