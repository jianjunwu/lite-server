"""BM25 text recall model."""

from lite_server import LitAPI


class BM25RecallAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request.get("query", "")

    def predict(self, x):
        import time
        time.sleep(0.05)
        return {"items": [f"bm25_item_{i}" for i in range(3)]}

    def encode_response(self, output):
        return output
