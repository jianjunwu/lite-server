"""Async echo model demonstrating the unified async pipeline."""

import asyncio

from lite_server import LitAPI, RequestContext


class AsyncEchoAPI(LitAPI):
    def setup(self, device):
        # setup is always synchronous (called once at worker start)
        self.device = device
        self.logger.info("AsyncEchoAPI setup on device=%s", device)

    async def decode_request(self, request, ctx: RequestContext | None = None):
        # decode_request may be sync or async — the worker adapts automatically
        await asyncio.sleep(0)
        return request.get("input", "")

    async def predict(self, x, ctx: RequestContext | None = None):
        # Simulate async I/O, e.g. calling an external API or async model library
        await asyncio.sleep(0.05)
        return {"output": f"async_echo: {x}"}

    async def encode_response(self, output, ctx: RequestContext | None = None):
        # encode_response may remain sync
        return output
