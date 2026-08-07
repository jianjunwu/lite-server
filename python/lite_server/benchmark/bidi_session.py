"""Bidi session orchestrator — transport-agnostic (批次 2, plan §4.2/§4.8).

A bidi session is the benchmark unit: open → paced chunks → close.  The
orchestrator drives producer/consumer coroutines over an injected IO object;
transports (WS now, h2/gRPC in 批次 3) only map their frame protocol onto
the ``Data`` / ``Done`` / ``Error`` frame tri-state.

IO contract (duck-typed; transports provide an object with these methods)::

    async def send_open(payload: bytes) -> None    # first frame (on_open)
    async def send_chunk(chunk: bytes) -> None     # data frame (on_chunk)
    async def send_close() -> None                 # graceful input end
    async def recv() -> Data | Done | Error        # next S→C frame

Lock-step requires a response per chunk (``on_chunk`` must not return None)
and an ``on_open`` ready response — sparse-response models must use a paced
mode (§4.8 D3).
"""

from __future__ import annotations

import asyncio
import json as _stdlib_json
import time
from dataclasses import dataclass
from typing import Union

from lite_server.benchmark.benchmark import (
    BidiSessionRecord,
    RequestError,
    RequestStreamError,
    RequestTimeoutError,
)


# ── Frame tri-state ──────────────────────────────────────────────────────────


@dataclass
class Data:
    payload: bytes


@dataclass
class Done:
    pass


@dataclass
class Error:
    message: str


Frame = Union[Data, Done, Error]


@dataclass
class Pacing:
    """Producer pacing: ``lock_step`` waits per-chunk response; the paced
    modes sleep ``pace_secs`` between sends (CLI pre-computes the effective
    pace for ``speedup`` = pace / rt_factor)."""

    mode: str = "lock_step"  # "lock_step" | "real_time" | "speedup"
    pace_secs: float = 0.0


_LOCK_STEP_OPEN_MSG = (
    "lock-step requires an on_open ready response (on_open must not return "
    "None); use --pace for models without one"
)
_LOCK_STEP_CHUNK_MSG = (
    "lock-step requires a response per chunk (on_chunk must not return "
    "None); use --pace for sparse-response models"
)


async def run_bidi_session(
    io,
    script: list,
    *,
    pacing: Pacing,
    idle_timeout: float | None = None,
) -> BidiSessionRecord:
    """Run one bidi session and return its measurements.

    ``script[0]`` is the open payload (JSON-serialized, sent via
    ``send_open``); ``script[1:]`` are data chunks.  Raises
    ``RequestStreamError``/``RequestTimeoutError`` on session failure —
    the run_bidi adapter lets those propagate to ``run()``'s error buckets.
    """
    if not script:
        raise ValueError("bidi script must contain at least the open payload")

    rec = BidiSessionRecord()
    frames: asyncio.Queue = asyncio.Queue()
    terminal = asyncio.Event()

    async def consumer() -> None:
        try:
            while True:
                frame = await io.recv()
                ts = time.perf_counter_ns()
                if isinstance(frame, Data):
                    rec.consumer_chunks += 1
                    rec.total_bytes_recv += len(frame.payload)
                frames.put_nowait((frame, ts))
                if isinstance(frame, (Done, Error)):
                    terminal.set()
                    return
        except asyncio.CancelledError:
            raise
        except RequestError as e:
            # Already-classified failures (status/connect/timeout) keep their
            # bucket — forwarded through the queue, re-raised by next_frame.
            frames.put_nowait((e, time.perf_counter_ns()))
            terminal.set()
        except Exception as e:  # transport failure → Error frame
            frames.put_nowait((Error(f"transport: {e}"), time.perf_counter_ns()))
            terminal.set()

    consumer_task = asyncio.create_task(consumer())
    try:
        t_open = time.perf_counter_ns()
        open_bytes = _stdlib_json.dumps(script[0]).encode()
        await io.send_open(open_bytes)
        rec.total_bytes_sent += len(open_bytes)

        async def next_frame(lock_step_msg: str | None = None):
            try:
                frame, ts = await asyncio.wait_for(frames.get(), idle_timeout)
            except asyncio.TimeoutError:
                if lock_step_msg is not None:
                    raise RequestStreamError(lock_step_msg) from None
                raise RequestTimeoutError() from None
            if isinstance(frame, RequestError):
                raise frame  # classified IO failure — keep the bucket
            return frame, ts

        # open latency = first S→C frame (ready response, or whatever comes)
        frame, ts = await next_frame(
            _LOCK_STEP_OPEN_MSG if pacing.mode == "lock_step" else None
        )
        rec.open_latency_ms = (ts - t_open) / 1e6
        if isinstance(frame, Error):
            raise RequestStreamError(frame.message)
        done_ts: int | None = ts if isinstance(frame, Done) else None

        # ── produce chunks ────────────────────────────────────────────
        if done_ts is None:
            if pacing.mode == "lock_step":
                done_ts = await _produce_lock_step(io, script[1:], rec,
                                                   next_frame)
            else:
                done_ts = await _produce_paced(io, script[1:], rec, pacing,
                                               terminal, frames)

        # ── close → final ─────────────────────────────────────────────
        if done_ts is None:
            await io.send_close()
            t_close = time.perf_counter_ns()
            while True:
                frame, ts = await next_frame()
                if isinstance(frame, Error):
                    raise RequestStreamError(frame.message)
                if isinstance(frame, Done):
                    done_ts = ts
                    rec.close_to_final_ms = (ts - t_close) / 1e6
                    break
                # late Data frames after close are counted by consumer already

        rec.session_duration_ms = (done_ts - t_open) / 1e6
        return rec
    finally:
        consumer_task.cancel()
        try:
            await consumer_task
        except (asyncio.CancelledError, Exception):
            pass


async def _produce_lock_step(io, chunks, rec, next_frame) -> int | None:
    """Send a chunk, wait for its response, record the roundtrip."""
    for item in chunks:
        data = _stdlib_json.dumps(item).encode()
        t_send = time.perf_counter_ns()
        await io.send_chunk(data)
        rec.producer_chunks += 1
        rec.total_bytes_sent += len(data)
        frame, ts = await next_frame(_LOCK_STEP_CHUNK_MSG)
        if isinstance(frame, Error):
            raise RequestStreamError(frame.message)
        if isinstance(frame, Done):  # model ended the session early
            return ts
        rec.chunk_roundtrips_ms.append((ts - t_send) / 1e6)
    return None


async def _produce_paced(io, chunks, rec, pacing, terminal, frames) -> int | None:
    """Send chunks on a fixed schedule; responses are not paired."""
    for item in chunks:
        if terminal.is_set():
            break
        data = _stdlib_json.dumps(item).encode()
        await io.send_chunk(data)
        rec.producer_chunks += 1
        rec.total_bytes_sent += len(data)
        if pacing.pace_secs > 0:
            await asyncio.sleep(pacing.pace_secs)
    if terminal.is_set():
        # Done/Error arrived during production — drain to find it
        while not frames.empty():
            frame, ts = frames.get_nowait()
            if isinstance(frame, Error):
                raise RequestStreamError(frame.message)
            if isinstance(frame, Done):
                return ts
    return None
