"""WS streaming target for benchmark engine (批次 1d, plan §2.5.2/§3.4 R4).

Consumes WS ``/stream`` (unidirectional) and ``/decoupled-stream`` endpoints
and yields ``StreamChunk`` instances.  All ``websockets`` imports are
function-local (websockets is a dev dependency, not a package dependency).

Wire protocol (R4): the client sends one Text frame (JSON payload), then
receives Binary frames (chunks) until a Text control frame arrives —
``{"done": true}`` terminal or ``{"error": ...}`` error.  One target serves
both endpoints; only the URL differs.
"""

from __future__ import annotations

import asyncio
import json as _stdlib_json
from typing import AsyncIterator, Callable

from lite_server.benchmark.benchmark import (
    RequestConnectError,
    RequestError,
    RequestStatusError,
    RequestStreamError,
    RequestTimeoutError,
    StreamChunk,
)
from lite_server.benchmark.sse_target import bytes_chunk_meta


def ws_stream_target(
    connect,
    url: str,
    *,
    timeout: float | None = None,
    headers: dict[str, str] | None = None,
    ping_interval: float | None = None,
    ping_timeout: float | None = None,
) -> Callable[[dict], AsyncIterator[StreamChunk]]:
    """Build a streaming benchmark target for a WS endpoint.

    Args:
        connect: ``websockets.asyncio.client.connect`` (or compatible).
        url: Full WS endpoint URL (``ws://`` / ``wss://``).
        timeout: Inter-chunk idle budget in seconds; re-arms per frame,
            matching the SSE read-timeout semantics.  ``None`` = no limit.
        headers: Extra handshake headers, forwarded to *connect* as
            ``additional_headers`` (websockets).  ``None``/empty → the
            connect call is unchanged (backward compat).

    Returns:
        A callable ``target(payload) -> AsyncIterator[StreamChunk]``
        compatible with ``BenchmarkEngine.run_stream()``.
    """

    connect_kwargs = {"additional_headers": headers} if headers else {}
    # K7 (resource-leak-plan): client-side WS keepalive, opt-in (default off,
    # aligned with the server's stream_keepalive_interval_secs).
    if ping_interval is not None:
        connect_kwargs["ping_interval"] = ping_interval
    if ping_timeout is not None:
        connect_kwargs["ping_timeout"] = ping_timeout

    async def target(payload: dict) -> AsyncIterator[StreamChunk]:
        from websockets.exceptions import (
            ConnectionClosedError,
            ConnectionClosedOK,
            InvalidHandshake,
        )

        try:
            async with connect(url, **connect_kwargs) as ws:
                await ws.send(_stdlib_json.dumps(payload))
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout)
                    except asyncio.TimeoutError:
                        raise RequestTimeoutError() from None

                    if isinstance(msg, bytes):
                        yield StreamChunk(
                            data=msg, meta=bytes_chunk_meta(msg),
                            size_bytes=len(msg),
                        )
                        continue

                    # Text frame: control only (R4)
                    done, error = _parse_control(msg)
                    if error is not None:
                        raise RequestStreamError(error)
                    if done:
                        break
        except RequestError:
            raise
        except ConnectionClosedOK:
            return  # clean close without done frame — tolerate (EOF)
        except ConnectionClosedError as e:
            raise RequestStreamError(f"WS closed abnormally: {e}") from e
        except InvalidHandshake as e:
            raise _map_handshake_error(e) from e
        except OSError as e:
            raise RequestConnectError() from e

    return target


def _parse_control(text: str) -> tuple[bool, str | None]:
    """Parse a Text control frame → ``(done, error_message)``.

    Non-JSON text frames are tolerated (ignored); JSON without
    ``done``/``error`` keys is likewise ignored.
    """
    try:
        obj = _stdlib_json.loads(text)
    except Exception:
        return False, None
    if isinstance(obj, dict):
        if "error" in obj:
            return False, str(obj["error"])
        if obj.get("done") is True:
            return True, None
    return False, None


def _map_handshake_error(err) -> RequestError:
    """Map a handshake failure: HTTP-status-bearing rejections go to the
    ``status`` bucket (parity with SSE non-200), the rest to ``connect``.

    Duck-typed over websockets versions: ``InvalidStatusCode.status_code``
    (≤14) / ``InvalidStatus.response.status_code`` (≥13) are both probed.
    """
    status = getattr(err, "status_code", None)
    if status is None:
        response = getattr(err, "response", None)
        status = getattr(response, "status_code", None)
    if isinstance(status, int):
        return RequestStatusError(status)
    return RequestConnectError()
