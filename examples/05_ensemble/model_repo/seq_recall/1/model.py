"""Sequence model recall."""

import time

from lite_server import LitAPI, RequestContext


class SeqRecallAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request.get("history", [])

    async def predict(self, x, ctx: RequestContext | None = None):
        time.sleep(0.05)
        return {"items": [f"seq_item_{i}" for i in range(3)]}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
