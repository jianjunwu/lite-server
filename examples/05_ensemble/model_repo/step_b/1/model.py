"""Ensemble step B: postprocessing — adds suffix to input."""

from lite_server import LitAPI, RequestContext


class StepBAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request.get("input", "")

    async def predict(self, x, ctx: RequestContext | None = None):
        if isinstance(x, list):
            return [{"output": f"{item} -> done"} for item in x]
        return {"output": f"{x} -> done"}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
