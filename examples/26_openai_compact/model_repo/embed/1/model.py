"""openai-compact embeddings model: /v1/embeddings (thin-forwarded).

Deterministic pseudo-embedding: dim = len(text), values = code points.
A real model would return a learned vector; the shape contract is the same —
build_embeddings_response wraps it in the OpenAI envelope.
"""

from lite_server import LitAPI
from lite_server.helpers.openai import build_embeddings_response


class EmbedAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request, ctx):
        return request  # {"model": ..., "input": ...}

    def predict(self, x, ctx):
        return {
            "embedding": [float(ord(c)) for c in x["input"]],
            "model": x["model"],
        }

    def encode_response(self, output, ctx):
        return build_embeddings_response(
            output["embedding"], model=output["model"],
            request_id=ctx.meta.request_id)
