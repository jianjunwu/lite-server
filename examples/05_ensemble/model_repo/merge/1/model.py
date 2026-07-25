"""Merge model: deduplicate and combine recall results."""

from lite_server import LitAPI, RequestContext


class MergeAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return {
            "bm25": request.get("bm25", []),
            "cf": request.get("cf", []),
            "visual": request.get("visual", []),
            "seq": request.get("seq", []),
        }

    async def predict(self, x, ctx: RequestContext | None = None):
        all_items = x["bm25"] + x["cf"] + x["visual"] + x["seq"]
        seen = set()
        merged = []
        for item in all_items:
            if item not in seen:
                seen.add(item)
                merged.append(item)
        return {"merged": merged}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
