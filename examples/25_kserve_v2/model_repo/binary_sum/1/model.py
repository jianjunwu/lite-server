"""KServe V2 Triton Binary Tensor Data Extension: JSON head + binary tail.

tritonclient sends each tensor as `parameters.binary_data_size` plus a block
of the binary tail; the server exposes the blocks as zero-copy memoryviews in
`ctx.binary_data`. `lite_server.kserve.parse_inputs` maps them back to
ndarray views, and `build_response` builds the KServe envelope (the server
binary-izes the response when the client sets `binary_data_output`).
"""

from lite_server import LitAPI
from lite_server.kserve import build_response, parse_inputs


class BinarySumAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request, ctx):
        # {"a": ndarray view, "b": ndarray view} — zero-copy, no serialization.
        return parse_inputs(ctx)

    def predict(self, x):
        return {"output0": x["a"] + x["b"]}

    def encode_response(self, output, ctx):
        return build_response(output, request_id=ctx.meta.request_id)
