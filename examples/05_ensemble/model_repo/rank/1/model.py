"""Rank model: select top-k from candidates."""

from lite_server import LitAPI


class RankAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return {
            "candidates": request.get("candidates", []),
            "top_k": request.get("top_k", 5),
        }

    def predict(self, x):
        candidates = x["candidates"]
        top_k = min(x["top_k"], len(candidates))
        return {"results": candidates[:top_k]}

    def encode_response(self, output):
        return output
