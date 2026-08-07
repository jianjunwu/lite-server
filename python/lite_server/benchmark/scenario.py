"""Scenario wrappers for streaming targets (批次 3 Part B, plan §7.2).

Transport-agnostic decorators over the ``AsyncIterator[StreamChunk]``
contract (D1 dividend — one implementation covers sse/ws/grpc):

- ``with_cancel_after`` — client-cancel injection (E2): consume N chunks,
  then abort; the inner generator's ``aclose()`` tears down the connection,
  which is what makes the server cancel the worker promptly.
- ``with_read_delay`` — slow-consumer injection (E3): sleep after each
  chunk, so the socket is not read during the delay (true slow drain).
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator, Callable

from lite_server.benchmark.benchmark import RequestCanceledError, StreamChunk


def with_cancel_after(
    target: Callable[[dict], AsyncIterator[StreamChunk]],
    n: int,
) -> Callable[[dict], AsyncIterator[StreamChunk]]:
    """Wrap *target* to cancel the stream after *n* chunks.

    The wrapped generator yields the first *n* chunks, then raises
    ``RequestCanceledError`` (kind ``"canceled"``); the ``finally`` closes
    the inner generator, which releases/aborts the underlying connection.
    When the stream has fewer than *n* chunks it simply completes.
    """

    async def wrapped(payload: dict) -> AsyncIterator[StreamChunk]:
        agen = target(payload)
        count = 0
        try:
            async for chunk in agen:
                yield chunk
                count += 1
                if count >= n:
                    raise RequestCanceledError(
                        f"client cancel after {n} chunks"
                    )
        finally:
            await agen.aclose()

    return wrapped


def with_read_delay(
    target: Callable[[dict], AsyncIterator[StreamChunk]],
    delay_secs: float,
) -> Callable[[dict], AsyncIterator[StreamChunk]]:
    """Wrap *target* with a slow consumer: sleep *delay_secs* after each chunk.

    The sleep happens before the next chunk pull, so the socket is not read
    during the delay — a true slow-drain consumer.  Note the e2e latency
    includes one trailing delay (the sleep after the final chunk, before
    stream exhaustion is observed).
    """

    async def wrapped(payload: dict) -> AsyncIterator[StreamChunk]:
        async for chunk in target(payload):
            yield chunk
            await asyncio.sleep(delay_secs)

    return wrapped
