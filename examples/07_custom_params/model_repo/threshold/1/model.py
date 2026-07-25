"""Threshold classifier — demonstrates custom config parameters.

All fields in config.yaml are accessible via self.config.get(key, default).
"""

from lite_server import LitAPI, RequestContext


class ThresholdClassifier(LitAPI):
    def setup(self, device):
        # Read custom parameters from config.yaml
        self.threshold = self.config.get("threshold", 0.5)
        self.label = self.config.get("label", "positive")
        self.negative_label = self.config.get("negative_label", "negative")

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request.get("score", 0.0)

    async def predict(self, score, ctx: RequestContext | None = None):
        if score >= self.threshold:
            return {"label": self.label, "score": score, "threshold": self.threshold}
        return {"label": self.negative_label, "score": score, "threshold": self.threshold}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
