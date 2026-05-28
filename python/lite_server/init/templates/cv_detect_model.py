from lite_server import LitAPI


class MyAPI(LitAPI):
    """Object detection API. Accepts base64-encoded images."""

    def setup(self, device):
        """Load your detection model (e.g., YOLO, DETR, RT-DETR)."""
        # Example placeholder — replace with real model loading
        self.model = lambda img: [
            {"class": "person", "confidence": 0.92, "bbox": [100, 200, 150, 300]},
            {"class": "car", "confidence": 0.88, "bbox": [400, 300, 200, 150]},
        ]

    def decode_request(self, request):
        """Decode base64 image string."""
        image_b64 = request.get("image", "")
        return {"image_b64": image_b64}

    def predict(self, x, **kwargs):
        """Run object detection. x is a list when batching is enabled."""
        if isinstance(x, list):
            return [self.model(item["image_b64"]) for item in x]
        return self.model(x["image_b64"])

    def encode_response(self, output):
        """Format detection results with bounding boxes."""
        return {
            "detections": output,
            "count": len(output),
        }
