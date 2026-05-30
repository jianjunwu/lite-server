"""Threshold classifier — demonstrates custom config parameters.

All fields in config.yaml are accessible via self.config.get(key, default).
"""

from lite_server import LitAPI


class ThresholdClassifier(LitAPI):
    def setup(self, device):
        # Read custom parameters from config.yaml
        self.threshold = self.config.get("threshold", 0.5)
        self.label = self.config.get("label", "positive")
        self.negative_label = self.config.get("negative_label", "negative")

    def decode_request(self, request):
        return request.get("score", 0.0)

    def predict(self, score):
        if score >= self.threshold:
            return {"label": self.label, "score": score, "threshold": self.threshold}
        return {"label": self.negative_label, "score": score, "threshold": self.threshold}

    def encode_response(self, output):
        return output
