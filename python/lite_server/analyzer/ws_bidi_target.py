"""WS bidi transport target for benchmark engine (批次 2, plan §4.6 Phase 2).

Maps the WS ``/stream`` bidi frame protocol (docs/http-bidi.md) onto the
``bidi_session`` IO contract: open = Text JSON first frame, chunks = Binary
frames, close = Text ``{"type": "close"}``; S→C Binary = ``Data``, Text
``{"done": true}`` = ``Done``, ``{"error": ...}`` = ``Error``.

No ``websockets`` import here — the ``connect`` callable is injected (batch
1d pattern) and transport exceptions propagate raw into the orchestrator's
consumer, which converts them to ``Error`` frames.
"""

from __future__ import annotations

import json as _stdlib_json
from typing import Awaitable, Callable

from lite_server.analyzer.bidi_session import (
    Data,
    Done,
    Error,
    Pacing,
    run_bidi_session,
)
from lite_server.analyzer.benchmark import BidiSessionRecord
from lite_server.analyzer.ws_target import _parse_control


def ws_bidi_session(
    connect,
    url: str,
    *,
    pacing: Pacing,
    idle_timeout: float | None = None,
) -> Callable[[list], Awaitable[BidiSessionRecord]]:
    """Build a bidi session runner for a WS endpoint.

    Args:
        connect: ``websockets.asyncio.client.connect`` (or compatible).
        url: Full WS bidi URL (``/v2/models/{m}/stream``).
        pacing: Producer pacing (lock_step / real_time / speedup).
        idle_timeout: Per-frame idle budget in seconds (re-arms per frame).

    Returns:
        ``session(script) -> BidiSessionRecord`` for
        ``BenchmarkEngine.run_bidi()``.  One connection per session.
    """

    async def session(script: list) -> BidiSessionRecord:
        async with connect(url) as ws:
            return await run_bidi_session(
                _WsBidiIO(ws), script, pacing=pacing, idle_timeout=idle_timeout,
            )

    return session


class _WsBidiIO:
    """``bidi_session`` IO over a connected websocket."""

    def __init__(self, ws):
        self._ws = ws

    async def send_open(self, payload: bytes) -> None:
        await self._ws.send(payload.decode("utf-8"))  # Text frame

    async def send_chunk(self, chunk: bytes) -> None:
        await self._ws.send(chunk)  # Binary frame

    async def send_close(self) -> None:
        await self._ws.send(_stdlib_json.dumps({"type": "close"}))

    async def recv(self):
        while True:
            msg = await self._ws.recv()
            if isinstance(msg, bytes):
                return Data(msg)
            done, error = _parse_control(msg)
            if error is not None:
                return Error(error)
            if done:
                return Done()
            # unknown Text frame — tolerated, keep reading
