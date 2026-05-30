"""Ensemble step A: preprocessing — adds prefix to input."""

from lite_server import LitAPI


class StepAAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request.get("input", "")

    def predict(self, x):
        if isinstance(x, list):
            return [{"output": f"preprocessed({item})"} for item in x]
        return {"output": f"preprocessed({x})"}

    def encode_response(self, output):
        return output
