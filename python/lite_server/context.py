"""Request-scoped context types for lite-server.

``RequestMeta`` is the immutable wire-level metadata created once per
request at the Rust→Python boundary.  ``RequestContext`` is the mutable
per-request working context flowing through the inference pipeline::

    on_request → decode_request → on_input → predict
    → on_output → encode_response → on_response

LitAPI hooks and Callback hooks share the same ``RequestContext``
contract since 0.7.0.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lite_server.response import Response


class Headers:
    """Case-insensitive HTTP headers mapping.

    Header keys are normalized to lowercase on storage and lookup.
    Read-only: no public mutation API, safe to share across batch items.
    """

    def __init__(self, raw: dict[str, str] | None = None) -> None:
        self._data: dict[str, list[str]] = {}
        if raw:
            for k, v in raw.items():
                self._data.setdefault(k.lower(), []).append(v)

    def get(self, key: str, default: str | None = None) -> str | None:
        values = self._data.get(key.lower())
        return values[0] if values else default

    def getlist(self, key: str) -> list[str]:
        return list(self._data.get(key.lower(), []))

    def items(self) -> list[tuple[str, str]]:
        return [(k, v[0]) for k, v in self._data.items()]

    def keys(self) -> list[str]:
        return list(self._data.keys())

    def values(self) -> list[str]:
        return [v[0] for v in self._data.values()]

    def __contains__(self, key: str) -> bool:
        return key.lower() in self._data

    def __getitem__(self, key: str) -> str:
        values = self._data.get(key.lower())
        if values:
            return values[0]
        raise KeyError(key)

    def __repr__(self) -> str:
        return f"Headers({dict(self.items())!r})"


@dataclass(frozen=True)
class RequestMeta:
    """Immutable HTTP request metadata from the wire.

    Created once per request at the Rust→Python boundary.  Frozen because
    batch items share a single instance — a per-item hook mutating it
    would corrupt its siblings' view (and tracing/logging/metrics).

    The freeze is shallow by design: field assignment is blocked, but
    values reachable through fields (e.g. the decoded ``ctx.request``
    payload) stay mutable.
    """

    route: str
    headers: Headers
    client_ip: str
    request_id: str
    timestamp_ns: int
    method: str = "POST"                  # HTTP method; POST for inference, from wire for endpoints
    query: dict[str, str] = field(default_factory=dict)  # URL query parameters (endpoints only)


@dataclass
class RequestContext:
    """Per-request state flowing through the inference pipeline.

    One instance per request (per stream in streaming mode, per batch item
    in batch mode, per session in bidi mode, per sequence in CB mode).
    Hooks pass data between stages via :attr:`state` — never via ``self``
    attributes, which are shared across concurrent requests.
    """

    meta: RequestMeta
    request: Any = None  # raw payload (pre-decode)
    input: Any = None  # decode_request output
    output: Any = None  # predict output
    response: Any = None  # encode_response output
    state: dict[str, Any] = field(default_factory=dict)
    early: Response | None = None  # set → pipeline short-circuits
    server: Any = None  # ServerProxy for endpoint handlers; None for inference
    response_headers: dict[str, str] = field(default_factory=dict)  # merged into final response

    def respond(
        self,
        body: Any,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        media_type: str = "application/json",
    ) -> Response:
        """Short-circuit the pipeline with an immediate response.

        Later stages (decode/predict/encode and remaining hooks) are
        skipped; *body* is serialized and sent with the given status and
        headers.  In ``on_response`` this is also the way to attach custom
        headers to the final response::

            def on_response(self, ctx):
                return ctx.respond(
                    ctx.response,
                    headers={"X-Request-ID": ctx.meta.request_id},
                )
        """
        self.early = Response(
            content=body,
            status_code=status_code,
            headers=dict(headers or {}),
            media_type=media_type,
        )
        return self.early


class CBSequence:
    """One active sequence in continuous batching.

    Element type of ``LitAPI.step``'s ``active_sequences`` argument.

    Attributes:
        uid: Unique request identifier.
        input: Output of ``decode_request`` for this sequence.
        output: Tokens generated so far (engine appends one per step).
        meta: Immutable request metadata (same object as ``ctx.meta``).
        ctx: The full RequestContext, for advanced use.

    ``state`` (property) is the sequence's per-request user state bag —
    the same dict as ``ctx.state``; ``prefill``/hooks may write it and
    ``step`` may read it.  ``prefilled`` is engine-internal; user code
    must not rely on it.
    """

    __slots__ = ("uid", "ctx", "input", "output", "meta", "prefilled")

    def __init__(self, uid: str, ctx: RequestContext):
        self.uid = uid
        self.ctx = ctx
        self.input = ctx.input
        self.output: list[Any] = []
        self.meta = ctx.meta
        self.prefilled = False

    @property
    def state(self) -> dict[str, Any]:
        return self.ctx.state
