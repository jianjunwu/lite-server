"""Asynchronous inference API base class for lite-server."""

from __future__ import annotations

from lite_server.api import LitAPI


class AsyncLitAPI(LitAPI):
    """Asynchronous inference API base class. ``predict()`` must be ``async def``.

    - ``decode_request`` / ``encode_response`` / ``on_request`` / ``on_response``
      may be sync or async; the worker adapts automatically.
    - ``batch`` / ``unbatch`` may be sync or async; the worker adapts automatically.
    - ``stream_predict`` may be an async generator (future phase).

    Usage::

        from lite_server import AsyncLitAPI

        class MyModel(AsyncLitAPI):
            async def predict(self, x):
                await asyncio.sleep(0)
                return {"output": x["input"] * 2}
    """

    def __init__(self, **kwargs):
        kwargs["enable_async"] = True
        super().__init__(**kwargs)

    async def predict(self, x):
        raise NotImplementedError
