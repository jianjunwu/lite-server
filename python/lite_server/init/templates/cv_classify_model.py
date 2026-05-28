import base64
from io import BytesIO

from lite_server import LitAPI


class MyAPI(LitAPI):
    """Image classification API. Accepts base64-encoded images."""

    def setup(self, device):
        """Load your classification model (e.g., ResNet, EfficientNet)."""
        # Example placeholder — replace with real model loading
        self.model = lambda img: {"class": "cat", "confidence": 0.95}
        self.labels = ["cat", "dog", "bird", "fish"]

    def decode_request(self, request):
        """Decode base64 image string to PIL Image or tensor."""
        image_b64 = request.get("image", "")
        # In production: decode base64 and convert to tensor
        # from PIL import Image
        # img = Image.open(BytesIO(base64.b64decode(image_b64)))
        return {"image_b64": image_b64}

    def predict(self, x, **kwargs):
        """Run classification. x is a list when batching is enabled."""
        if isinstance(x, list):
            return [self.model(item["image_b64"]) for item in x]
        return self.model(x["image_b64"])

    def encode_response(self, output):
        """Format classification result."""
        return {
            "prediction": output["class"],
            "confidence": output["confidence"],
            "labels": self.labels,
        }
