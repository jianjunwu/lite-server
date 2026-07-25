"""Rank model: select top-k from candidates."""

from lite_server import LitAPI, RequestContext


class RankAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return {
            "candidates": request.get("candidates", []),
            "top_k": request.get("top_k", 5),
        }

    async def predict(self, x, ctx: RequestContext | None = None):
        candidates = x["candidates"]
        top_k = min(x["top_k"], len(candidates))
        return {"results": candidates[:top_k]}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
