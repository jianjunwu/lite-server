"""Streaming captioner — the tail step of the stream_pipeline ensemble.

`stream_predict` yields caption words one at a time; the server forwards each
chunk as an SSE event (or gRPC/WS/h2 chunk) straight from the ensemble DAG —
the DAG itself performs no chunk-content conversion.
"""

import time

from lite_server import LitAPI, RequestContext


class CaptionStreamAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        # The ensemble step's assembled payload: {"input": "<preprocessed text>"}
        return request.get("input", "")

    async def predict(self, x, ctx: RequestContext | None = None):
        # Non-streaming fallback (unary endpoints reject streaming DAGs anyway).
        return {"text": x}

    def stream_predict(self, request, ctx: RequestContext | None = None):
        for i, word in enumerate(str(request).split()):
            time.sleep(0.05)  # simulate caption generation
            yield {"token": word, "index": i}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
