"""HTTP response classes for lite-server.

Provides a response model that matches Starlette's Response feature set
while remaining independent of any ASGI/web framework.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, AsyncIterator


# ---------------------------------------------------------------------------
# BackgroundTask
# ---------------------------------------------------------------------------

class BackgroundTask:
    """A function to run after the response has been sent."""

    def __init__(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs

    async def __call__(self) -> Any:
        return self.func(*self.args, **self.kwargs)


# ---------------------------------------------------------------------------
# Cookie helpers
# ---------------------------------------------------------------------------

def _render_cookie(
    key: str,
    value: str = "",
    *,
    max_age: int | None = None,
    expires: str | None = None,
    path: str = "/",
    domain: str | None = None,
    secure: bool = False,
    httponly: bool = False,
    samesite: str | None = "lax",
    partitioned: bool = False,
) -> str:
    cookie = f"{key}={value or ''}"
    if max_age is not None:
        cookie += f"; Max-Age={max_age}"
    if expires is not None:
        cookie += f"; Expires={expires}"
    if path:
        cookie += f"; Path={path}"
    if domain:
        cookie += f"; Domain={domain}"
    if secure:
        cookie += "; Secure"
    if httponly:
        cookie += "; HttpOnly"
    if samesite:
        cookie += f"; SameSite={samesite.capitalize()}"
    if partitioned:
        cookie += "; Partitioned"
    return cookie


def _delete_cookie_header(
    key: str,
    *,
    path: str = "/",
    domain: str | None = None,
    secure: bool = False,
    httponly: bool = False,
    samesite: str | None = "lax",
) -> str:
    return _render_cookie(
        key,
        value="",
        max_age=0,
        expires="Thu, 01 Jan 1970 00:00:00 GMT",
        path=path,
        domain=domain,
        secure=secure,
        httponly=httponly,
        samesite=samesite,
    )


# ---------------------------------------------------------------------------
# Response
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class Response:
    """HTTP response with full control over status, headers, and media type.

    Return this from a LitAPI hook or route handler to control the
    full HTTP response sent to the client.
    """

    content: Any = None
    status_code: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    media_type: str = "application/json"
    background: BackgroundTask | None = None

    _cookie_headers: list[tuple[str, str]] = field(default_factory=list, init=False, repr=False)

    def set_cookie(
        self,
        key: str,
        value: str = "",
        *,
        max_age: int | None = None,
        expires: str | None = None,
        path: str = "/",
        domain: str | None = None,
        secure: bool = False,
        httponly: bool = False,
        samesite: str | None = "lax",
        partitioned: bool = False,
    ) -> None:
        """Set a Set-Cookie header on the response."""
        cookie_str = _render_cookie(
            key,
            value=value,
            max_age=max_age,
            expires=expires,
            path=path,
            domain=domain,
            secure=secure,
            httponly=httponly,
            samesite=samesite,
            partitioned=partitioned,
        )
        self._cookie_headers.append(("set-cookie", cookie_str))

    def delete_cookie(
        self,
        key: str,
        *,
        path: str = "/",
        domain: str | None = None,
        secure: bool = False,
        httponly: bool = False,
        samesite: str | None = "lax",
    ) -> None:
        """Remove a cookie by setting its expiry in the past."""
        cookie_str = _delete_cookie_header(
            key,
            path=path,
            domain=domain,
            secure=secure,
            httponly=httponly,
            samesite=samesite,
        )
        self._cookie_headers.append(("set-cookie", cookie_str))


# ---------------------------------------------------------------------------
# Response subclasses
# ---------------------------------------------------------------------------

class JSONResponse(Response):
    """Response with ``application/json`` media type."""

    def __init__(
        self,
        content: Any = None,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        background: BackgroundTask | None = None,
    ) -> None:
        super().__init__(
            content=content,
            status_code=status_code,
            headers=headers or {},
            media_type="application/json",
            background=background,
        )


class HTMLResponse(Response):
    """Response with ``text/html`` media type."""

    def __init__(
        self,
        content: Any = None,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        background: BackgroundTask | None = None,
    ) -> None:
        super().__init__(
            content=content,
            status_code=status_code,
            headers=headers or {},
            media_type="text/html",
            background=background,
        )


class PlainTextResponse(Response):
    """Response with ``text/plain`` media type."""

    def __init__(
        self,
        content: Any = None,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        background: BackgroundTask | None = None,
    ) -> None:
        super().__init__(
            content=content,
            status_code=status_code,
            headers=headers or {},
            media_type="text/plain",
            background=background,
        )


class RedirectResponse(Response):
    """Redirect response (302 by default) with a Location header."""

    def __init__(
        self,
        url: str,
        status_code: int = 302,
        headers: dict[str, str] | None = None,
    ) -> None:
        hdrs = headers or {}
        hdrs["location"] = url
        super().__init__(
            content=b"",
            status_code=status_code,
            headers=hdrs,
            media_type="text/plain",
        )


class FileResponse(Response):
    """Stream a file asynchronously to the client.

    The file transfer is handled by the Rust HTTP layer.  This class
    signals the intent; the actual file streaming happens server-side
    without blocking the Python worker.
    """

    def __init__(
        self,
        path: str,
        filename: str | None = None,
        media_type: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        import os

        if filename is None:
            filename = os.path.basename(path)

        if media_type is None:
            import mimetypes
            media_type, _ = mimetypes.guess_type(path)
            if media_type is None:
                media_type = "application/octet-stream"

        hdrs = {
            "content-disposition": f'attachment; filename="{filename}"',
        }
        if headers:
            hdrs.update(headers)

        super().__init__(
            content=b"",
            status_code=200,
            headers=hdrs,
            media_type=media_type,
        )
        self._file_path = path

    @property
    def file_path(self) -> str:
        return self._file_path


class StreamingResponse(Response):
    """Streaming response for SSE (Server-Sent Events).

    Maps to lite-server's existing server-side streaming mechanism.
    The content should be an async iterator or sync iterator yielding
    chunks.
    """

    def __init__(
        self,
        content: Iterator[Any] | AsyncIterator[Any],
        media_type: str = "text/event-stream",
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            content=content,
            status_code=status_code,
            headers=headers or {},
            media_type=media_type,
        )
