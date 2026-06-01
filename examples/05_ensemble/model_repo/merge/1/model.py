"""Merge model: deduplicate and combine recall results."""

from lite_server import LitAPI


class MergeAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return {
            "bm25": request.get("bm25", []),
            "cf": request.get("cf", []),
            "visual": request.get("visual", []),
            "seq": request.get("seq", []),
        }

    def predict(self, x):
        all_items = x["bm25"] + x["cf"] + x["visual"] + x["seq"]
        seen = set()
        merged = []
        for item in all_items:
            if item not in seen:
                seen.add(item)
                merged.append(item)
        return {"merged": merged}

    def encode_response(self, output):
        return output
