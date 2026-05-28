from lite_server import LitAPI


class MyAPI(LitAPI):
    """NLP text classification / sentiment analysis API."""

    def setup(self, device):
        """Load your NLP model (e.g., BERT, RoBERTa, DistilBERT)."""
        # Example placeholder — replace with real model loading
        self.model = lambda text: {
            "label": "positive",
            "score": 0.92,
            "labels": {
                "positive": 0.92,
                "negative": 0.05,
                "neutral": 0.03,
            },
        }

    def decode_request(self, request):
        """Extract text from request."""
        text = request.get("text", "")
        return {"text": text}

    def predict(self, x, **kwargs):
        """Run text classification. x is a list when batching is enabled."""
        if isinstance(x, list):
            return [self.model(item["text"]) for item in x]
        return self.model(x["text"])

    def encode_response(self, output):
        """Format classification result."""
        return {
            "text": output.get("text", ""),
            "label": output["label"],
            "score": output["score"],
            "labels": output["labels"],
        }
