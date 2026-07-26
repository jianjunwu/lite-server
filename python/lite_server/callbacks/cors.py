"""CORS policy callback."""

from lite_server.callbacks._base import Callback
from lite_server.callbacks._internal import _rust_managed


class Cors(Callback):
    """CORS policy declaration. Executed in the Rust HTTP layer.

    Rust attaches the headers to every response of the route (success, error,
    and stream start) and answers OPTIONS preflight directly.  Outside a
    Rust-managed worker, falls back to stashing ctx.response_headers.
    """

    def __init__(
        self,
        *,
        allow_origins: list[str] | None = None,
        allow_methods: list[str] | None = None,
        allow_headers: list[str] | None = None,
    ):
        self.allow_origins = list(allow_origins or ["*"])
        self.allow_methods = list(
            allow_methods or ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
        )
        self.allow_headers = list(allow_headers or ["Content-Type", "Authorization"])
        self._managed = _rust_managed()
        self._header_dict = {
            "Access-Control-Allow-Origin": ", ".join(self.allow_origins),
            "Access-Control-Allow-Methods": ", ".join(self.allow_methods),
            "Access-Control-Allow-Headers": ", ".join(self.allow_headers),
        }

    def on_request(self, ctx):
        if self._managed:
            return  # Rust 附加 header 并应答 preflight
        ctx.response_headers.update(self._header_dict)
        if ctx.meta.method == "OPTIONS":
            ctx.respond("", status_code=204, headers=dict(self._header_dict))
