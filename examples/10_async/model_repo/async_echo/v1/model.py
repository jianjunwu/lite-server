"""Async echo model demonstrating AsyncLitAPI."""

import asyncio

from lite_server import AsyncLitAPI


class AsyncEchoAPI(AsyncLitAPI):
    async def setup(self, device):
        self.device = device
        self.logger.info("AsyncEchoAPI setup on device=%s", device)

    async def decode_request(self, request):
        # decode_request may also be async
        await asyncio.sleep(0)
        return request.get("input", "")

    async def predict(self, x):
        # Simulate async I/O, e.g. calling an external API or async model library
        await asyncio.sleep(0.05)
        return {"output": f"async_echo: {x}"}

    def encode_response(self, output):
        # encode_response may remain sync
        return output
