"""Callback demo model: a simple echo to exercise the full Callback pipeline.

The model itself is minimal — the interesting part is callbacks.py and the
two registration paths:

- ``LitAPI.callbacks`` (class attribute, below): takes priority and supports
  constructor arguments. Runs BEFORE config.yaml callbacks.
- ``callbacks:`` in config.yaml: no-arg constructible classes, appended
  after the class-attribute ones.
"""

import os

from lite_server import LitAPI, RequestContext

from callbacks import ApiKeyAuth


class CallbacksDemoAPI(LitAPI):
    # Class-attribute registration: the right place when a callback needs
    # configuration (here: the accepted API keys, overridable via env var).
    callbacks = (
        ApiKeyAuth(keys=os.environ.get("DEMO_API_KEYS", "demo-key").split(",")),
    )

    def setup(self, device):
        self.device = device

    def decode_request(self, request, ctx: RequestContext | None = None):
        return request.get("input")

    def predict(self, x, ctx: RequestContext | None = None):
        return {"output": x}

    def encode_response(self, output, ctx: RequestContext | None = None):
        return output
