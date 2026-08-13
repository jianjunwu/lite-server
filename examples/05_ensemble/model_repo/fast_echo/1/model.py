"""µs-level mock worker for ensemble hot-path benchmarks (§6.4).

Echoes the request verbatim with minimal work — per-step framework overhead
(queue dispatch, assembly, spawn) dominates the wall clock, which is exactly
what the P3/P11 spawn-overhead and P1/P8 context-sharing benchmarks measure.
"""

from lite_server import LitAPI, RequestContext


class FastEchoAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request

    async def predict(self, x, ctx: RequestContext | None = None):
        return x

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
