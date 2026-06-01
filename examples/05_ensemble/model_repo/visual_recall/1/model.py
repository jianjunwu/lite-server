"""Visual similarity recall model."""

from lite_server import LitAPI


class VisualRecallAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request.get("image", "")

    def predict(self, x):
        import time
        time.sleep(0.05)
        return {"items": [f"visual_item_{i}" for i in range(3)]}

    def encode_response(self, output):
        return output
