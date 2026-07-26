"""API-key authentication callback."""

from lite_server.callbacks._base import Callback


class RequireApiKey(Callback):
    """Reject requests without a valid API key.

    Usage::

        RequireApiKey(header="X-API-Key", keys=["sk-xxx"])
        RequireApiKey(header="Authorization")  # empty keys → any non-empty value passes
    """

    def __init__(self, *, header: str = "X-API-Key", keys: list[str] | None = None):
        self._header = header
        self._keys: frozenset[str] | None = frozenset(keys) if keys else None

    def on_request(self, ctx):
        from lite_server.exceptions import UnauthorizedError

        value = ctx.meta.headers.get(self._header, "")
        if not value:
            raise UnauthorizedError("missing API key", param=self._header)
        if self._keys is not None and value not in self._keys:
            raise UnauthorizedError("invalid API key", param=self._header)
