"""Ensemble step B: postprocessing — adds suffix to input."""

from lite_server import LitAPI


class StepBAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request.get("input", "")

    def predict(self, x):
        if isinstance(x, list):
            return [{"output": f"{item} -> done"} for item in x]
        return {"output": f"{x} -> done"}

    def encode_response(self, output):
        return output
