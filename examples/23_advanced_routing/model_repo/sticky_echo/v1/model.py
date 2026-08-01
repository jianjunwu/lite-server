"""Sticky routing + decoupled streaming model.

Each response carries the worker's pid, so checks can prove that requests
with the same ``x-sequence-id`` keep landing on the same worker (P8-1).

``predict_decoupled`` (P9-1) pushes 3 chunks then closes the stream — the
channel lifetime is controlled by the model, not the worker.
"""

import os

from lite_server import LitAPI


class StickyEchoAPI(LitAPI):
    def setup(self, device):
        self.device = device
        self.pid = os.getpid()

    def decode_request(self, request, ctx=None):
        return request.get("input")

    def predict(self, x, ctx=None):
        return {"echo": x, "pid": self.pid}

    def encode_response(self, output, ctx=None):
        return {"output": output}

    async def predict_decoupled(self, data, sender, ctx=None):
        for i in range(3):
            await sender.send({"chunk": i, "echo": data, "pid": self.pid})
        await sender.close()
