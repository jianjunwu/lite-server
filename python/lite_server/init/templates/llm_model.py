from lite_server import LitAPI


class MyAPI(LitAPI):
    """OpenAI-compatible chat completion API."""

    def setup(self, device):
        """Load your LLM here (e.g., transformers, vLLM, llama.cpp)."""
        # Example placeholder — replace with real model loading
        self.model = lambda prompt, max_tokens=128: f"Echo: {prompt}"
        self.tokenizer = None

    def decode_request(self, request):
        """Parse OpenAI-style chat completion request."""
        messages = request.get("messages", [])
        prompt = request.get("prompt", "")
        if messages:
            # Simple concatenation for demo; use apply_chat_template in production
            prompt = "\n".join(f"{m['role']}: {m['content']}" for m in messages)
        max_tokens = request.get("max_tokens", 128)
        temperature = request.get("temperature", 0.7)
        stream = request.get("stream", False)
        return {
            "prompt": prompt,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": stream,
        }

    def predict(self, x, **kwargs):
        """Run LLM inference. x is a list when batching is enabled."""
        if isinstance(x, list):
            return [
                {
                    "text": self.model(item["prompt"], max_tokens=item["max_tokens"]),
                    "usage": {"prompt_tokens": 0, "completion_tokens": 0},
                }
                for item in x
            ]
        text = self.model(x["prompt"], max_tokens=x["max_tokens"])
        return {"text": text, "usage": {"prompt_tokens": 0, "completion_tokens": 0}}

    def encode_response(self, output):
        """Return OpenAI-compatible response."""
        return {
            "id": "chatcmpl-demo",
            "object": "chat.completion",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": output["text"]},
                    "finish_reason": "stop",
                }
            ],
            "usage": output["usage"],
        }
