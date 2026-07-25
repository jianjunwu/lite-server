"""Batch ctx-injection demo.

Shows that ``batch`` / ``unbatch`` / ``predict`` may declare ``ctx`` and
receive a ``list[RequestContext]`` aligned positionally with the batch
items — no need to thread per-request data through the decoded input.

Pipeline: decode_request -> batch -> predict -> unbatch -> encode_response
"""

from lite_server import LitAPI, RequestContext


class CtxBatchAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request, ctx: RequestContext | None = None):
        return request["input"]

    def batch(self, inputs, ctx):
        """``ctx`` is a list[RequestContext], one per input, aligned as
        ``ctx[i] <-> inputs[i]``. Demonstrates per-item access (logging,
        tracing, grouping) at batch time."""
        for c in ctx:
            self.logger.info(
                "batching request_id=%s client=%s",
                c.meta.request_id, c.meta.client_ip,
            )
        return inputs

    def predict(self, batched, ctx):
        """Double each value and stamp it with its request_id, reading
        straight from ``ctx`` (aligned with ``batched``) to show per-item
        context is available in the expensive predict stage too."""
        return [
            {"output": v * 2, "request_id": c.meta.request_id}
            for v, c in zip(batched, ctx)
        ]

    def unbatch(self, output, ctx):
        # ctx is the same aligned list batch() received.
        return list(output)

    def encode_response(self, output, ctx: RequestContext | None = None):
        return output
