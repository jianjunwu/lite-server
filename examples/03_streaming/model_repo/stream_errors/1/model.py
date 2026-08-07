"""Stream error handling: demonstrates how exceptions propagate in streaming.

The `mode` request parameter controls behavior:
  - "normal":       yields 2 tokens, then Done
  - "bad_request":  raises HTTPException(400) in stream_predict mid-stream
  - "not_found":    raises HTTPException(404) in stream_predict mid-stream
  - "server_error": raises HTTPException(500) in stream_predict mid-stream

Each error mode sends a terminal Error frame (not Done), and the client sees
the error in the SSE/WS stream.
"""

import time

from lite_server import LitAPI, RequestContext
from lite_server.exceptions import HTTPException


class StreamErrorsAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        mode = request.get("mode", "normal")
        if mode not in ("normal", "bad_request", "not_found", "server_error"):
            raise HTTPException(400, f"unknown mode: {mode}")
        return {"mode": mode, "input": request.get("input", "")}

    async def predict(self, x, ctx: RequestContext | None = None):
        # Non-streaming: same behavior
        if x["mode"] == "normal":
            return {"output": f"ok: {x['input']}"}
        status_map = {"bad_request": 400, "not_found": 404, "server_error": 500}
        raise HTTPException(status_map[x["mode"]], f"error: {x['mode']}")

    def stream_predict(self, request, ctx: RequestContext | None = None):
        """Stream tokens or raise errors mid-stream."""
        mode = request["mode"]
        input_val = request["input"]

        # Yield first token
        time.sleep(0.02)
        yield {"token": 0, "value": f"processing: {input_val}"}

        if mode == "normal":
            time.sleep(0.02)
            yield {"token": 1, "value": f"done: {input_val}"}
            return

        # Error modes: raise after the first token
        status_map = {"bad_request": 400, "not_found": 404, "server_error": 500}
        raise HTTPException(status_map[mode], f"stream error: {mode}")

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
