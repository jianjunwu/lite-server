"""SSE streaming target for benchmark engine (PR-4).

Consumes Server-Sent Events from a lite-server streaming endpoint and
yields ``StreamChunk`` instances.  All ``import httpx`` is function-local
(httpx is a dev dependency, not a package dependency).
"""

from __future__ import annotations

import json as _stdlib_json
from typing import AsyncIterator, Callable

from lite_server.benchmark.benchmark import (
    RequestConnectError,
    RequestError,
    RequestStatusError,
    RequestStreamError,
    RequestTimeoutError,
    RequestTransportError,
    StreamChunk,
)
from lite_server._json import loads as _json_loads


def default_chunk_meta(data: str) -> dict | None:
    """Extract meta from an SSE ``data:`` value.

    When *data* parses as a JSON object (``dict``), return it so the
    engine can pick up ``token_count``, ``audio_duration_ms``, etc.
    Returns ``None`` for non-dict JSON (arrays, primitives) and for
    unparseable data (plain text).
    """
    try:
        obj = _json_loads(data)
    except Exception:
        return None
    if isinstance(obj, dict):
        return obj
    return None


def bytes_chunk_meta(data: bytes) -> dict | None:
    """``default_chunk_meta`` for bytes payloads (WS Binary / gRPC chunks)."""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return None
    return default_chunk_meta(text)


def sse_stream_target(
    client,
    url: str,
    *,
    timeout=None,
    done_marker: str = "[DONE]",
    chunk_meta: Callable[[str], dict | None] = default_chunk_meta,
) -> Callable[[dict], AsyncIterator[StreamChunk]]:
    """Build a streaming benchmark target for an SSE endpoint.

    Args:
        client: An ``httpx.AsyncClient`` instance.
        url: Full SSE endpoint URL.
        timeout: Per-request httpx timeout (e.g. ``httpx.Timeout``).  When
            ``None`` the client default applies.  Note httpx read timeout
            re-arms per chunk read, so this is the inter-chunk idle budget.
        done_marker: The ``data:`` value that signals stream completion.
        chunk_meta: Callable that extracts optional metadata from each
            ``data:`` string (default: parse JSON → dict or None).

    Returns:
        A callable ``target(payload) -> AsyncIterator[StreamChunk]``
        compatible with ``BenchmarkEngine.run_stream()``.
    """

    async def target(payload: dict) -> AsyncIterator[StreamChunk]:
        import httpx

        try:
            request_kwargs: dict = {"json": payload}
            if timeout is not None:
                request_kwargs["timeout"] = timeout
            async with client.stream("POST", url, **request_kwargs) as response:
                if response.status_code != 200:
                    raise RequestStatusError(response.status_code)

                data_lines: list[str] = []

                async for line in response.aiter_lines():
                    if line.startswith("data:"):
                        data = line[5:]
                        if data.startswith(" "):
                            data = data[1:]
                        data_lines.append(data)
                        continue

                    if line == "" and data_lines:
                        # Empty line → event boundary
                        done, chunk = _build_chunk(data_lines, chunk_meta, done_marker)
                        data_lines = []
                        if done:
                            break
                        if chunk is not None:
                            yield chunk

                # Tolerant: trailing event without final empty line
                if data_lines:
                    done, chunk = _build_chunk(data_lines, chunk_meta, done_marker)
                    if not done and chunk is not None:
                        yield chunk

        except (RequestStreamError, RequestError):
            raise
        except httpx.TimeoutException as e:
            raise RequestTimeoutError() from e
        except httpx.ConnectError as e:
            raise RequestConnectError() from e
        except httpx.TransportError as e:
            raise RequestTransportError() from e

    return target


def _build_chunk(
    data_lines: list[str],
    chunk_meta: Callable[[str], dict | None],
    done_marker: str,
) -> tuple[bool, StreamChunk | None]:
    """Build a StreamChunk from accumulated SSE data lines.

    Returns ``(done, chunk)`` where *done* is ``True`` when the event is
    the done marker (stream should stop).  *chunk* is ``None`` for
    keepalive/empty frames.

    Raises ``RequestStreamError`` when the event is an error frame.
    """
    data = "\n".join(data_lines)
    data_lines.clear()

    if data == done_marker:
        return True, None

    # Check for error event (§1.5)
    try:
        obj = _stdlib_json.loads(data)
        if isinstance(obj, dict) and "error" in obj:
            raise RequestStreamError(obj["error"])
    except (RequestStreamError, RequestError):
        raise
    except Exception:
        pass  # Not JSON, or JSON without "error" — that's fine

    meta = chunk_meta(data)
    return False, StreamChunk(data=data, meta=meta, size_bytes=len(data))
