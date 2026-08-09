"""KServe V2 raw-bytes tensor path: octet-stream body + x-tensor-* headers.

The client sends the tensor as a raw body (not JSON). decode_request sees
`bytes` and rebuilds the ndarray from the headers — see
docs/protocol.md "Raw Bytes / Tensor Requests".
"""

import math

import numpy as np

from lite_server import LitAPI


class RawTensorAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request, ctx):
        h = ctx.meta.headers
        dtype = np.dtype(h["x-tensor-dtype"])  # e.g. "<f4" (little-endian float32)
        shape = tuple(int(d) for d in h["x-tensor-shape"].split(","))
        expected = math.prod(shape) * dtype.itemsize
        if len(request) != expected:
            raise ValueError(
                f"body {len(request)}B != expected {expected}B for {dtype}{shape}"
            )
        # frombuffer creates a read-only view — call .copy() for a writable array.
        return np.frombuffer(request, dtype=dtype).reshape(shape)

    def predict(self, x):
        return x

    def encode_response(self, output, ctx):
        return {
            "sum": float(output.sum()),
            "shape": list(output.shape),
            "dtype": output.dtype.str,
        }
