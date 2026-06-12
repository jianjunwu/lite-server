"""HTTP request classes for lite-server.

Provides a request model that matches Starlette's Request feature set
while remaining independent of any ASGI/web framework.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# URL
# ---------------------------------------------------------------------------

@dataclass
class URL:
    """Read-only URL representation."""

    scheme: str = "http"
    hostname: str = "localhost"
    port: int | None = None
    path: str = "/"
    query: str = ""

    @property
    def netloc(self) -> str:
        if self.port:
            return f"{self.hostname}:{self.port}"
        return self.hostname

    @property
    def is_secure(self) -> bool:
        return self.scheme == "https"

    def include_query_params(self, **kwargs: str) -> URL:
        params = dict(QueryParams(self.query).items())
        params.update(kwargs)
        new_query = "&".join(f"{k}={v}" for k, v in params.items())
        return URL(
            scheme=self.scheme,
            hostname=self.hostname,
            port=self.port,
            path=self.path,
            query=new_query,
        )

    def remove_query_params(self, *keys: str) -> URL:
        params = dict(QueryParams(self.query).items())
        for k in keys:
            params.pop(k, None)
        new_query = "&".join(f"{k}={v}" for k, v in params.items())
        return URL(
            scheme=self.scheme,
            hostname=self.hostname,
            port=self.port,
            path=self.path,
            query=new_query,
        )

    def replace(self, **kwargs: str | int | None) -> URL:
        for field_name in ("scheme", "hostname", "port", "path", "query"):
            if field_name not in kwargs:
                kwargs[field_name] = getattr(self, field_name)
        return URL(**kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------

class Headers:
    """Case-insensitive HTTP headers mapping.

    Header keys are normalized to lowercase on storage and lookup.
    """

    def __init__(self, raw: dict[str, str] | None = None) -> None:
        self._data: dict[str, list[str]] = {}
        if raw:
            for k, v in raw.items():
                self._data.setdefault(k.lower(), []).append(v)

    def get(self, key: str, default: str | None = None) -> str | None:
        values = self._data.get(key.lower())
        if values:
            return values[0]
        return default

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


# ---------------------------------------------------------------------------
# QueryParams
# ---------------------------------------------------------------------------

class QueryParams:
    """Immutable multi-value query parameters.

    Supports ``?ids=1&ids=2`` style repeated keys.
    """

    def __init__(self, raw: str | dict[str, str] | None = None) -> None:
        self._data: dict[str, list[str]] = {}
        if raw is None:
            return
        if isinstance(raw, dict):
            for k, v in raw.items():
                self._data[k] = [v]
        else:
            from urllib.parse import parse_qs
            # parse_qs returns {key: [value1, value2]}
            self._data = parse_qs(raw, keep_blank_values=True)

    def get(self, key: str, default: str | None = None) -> str | None:
        values = self._data.get(key)
        if values:
            return values[0]
        return default

    def getlist(self, key: str) -> list[str]:
        return list(self._data.get(key, []))

    def items(self) -> list[tuple[str, str]]:
        return [(k, v[0]) for k, v in self._data.items()]

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __repr__(self) -> str:
        return f"QueryParams({dict(self.items())!r})"


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

@dataclass
class Client:
    """Client connection information."""

    host: str | None = None
    port: int | None = None


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class State:
    """Run-time state bag for middleware to pass data to handlers.

    Supports both dict-style and attribute-style access::

        request.state.user_id = "xxx"
        print(request.state.user_id)
    """

    def __init__(self) -> None:
        self._state: dict[str, Any] = {}

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._state:
            return self._state[name]
        raise AttributeError(f"State has no attribute '{name}'")

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "_state":
            super().__setattr__(name, value)
        else:
            self._state[name] = value

    def __delattr__(self, name: str) -> None:
        if name in self._state:
            del self._state[name]
        else:
            raise AttributeError(name)

    def __repr__(self) -> str:
        return f"State({self._state!r})"


# ---------------------------------------------------------------------------
# UploadFile
# ---------------------------------------------------------------------------

@dataclass
class UploadFile:
    """An uploaded file from a multipart/form-data request."""

    filename: str
    content_type: str = "application/octet-stream"
    size: int = 0
    _data: bytes = b""

    def read(self) -> bytes:
        return self._data

    def seek(self, position: int) -> None:
        pass  # No-op: data is fully in memory


# ---------------------------------------------------------------------------
# Request
# ---------------------------------------------------------------------------

class Request:
    """HTTP request made available to endpoint handlers and LitAPI hooks."""

    def __init__(
        self,
        *,
        method: str = "GET",
        url: URL | None = None,
        headers: dict[str, str] | None = None,
        query_params: dict[str, str] | None = None,
        body: bytes = b"",
        client_host: str | None = None,
        client_port: int | None = None,
    ) -> None:
        self.method = method.upper()
        self.url = url or URL()
        self.headers = Headers(headers or {})
        self.query_params = QueryParams(query_params or {})
        self.body = body
        self.client = Client(host=client_host, port=client_port)
        self.state = State()
        self.cookies: dict[str, str] = self._parse_cookies(self.headers.get("cookie", ""))

    @staticmethod
    def _parse_cookies(cookie_header: str) -> dict[str, str]:
        if not cookie_header:
            return {}
        cookies: dict[str, str] = {}
        for part in cookie_header.split(";"):
            part = part.strip()
            if "=" in part:
                k, v = part.split("=", 1)
                cookies[k.strip()] = v.strip()
        return cookies

    def json(self) -> Any:
        """Parse the request body as JSON."""
        import json
        if not self.body:
            return {}
        return json.loads(self.body)

    def text(self) -> str:
        """Return the request body decoded as UTF-8 text."""
        return self.body.decode("utf-8")

    def form(self) -> dict[str, str | UploadFile]:
        """Parse the request body as form data.

        Supports ``application/x-www-form-urlencoded`` and
        ``multipart/form-data``.
        """
        content_type = self.headers.get("content-type", "")
        if content_type.startswith("application/x-www-form-urlencoded"):
            from urllib.parse import parse_qs
            data = parse_qs(self.text(), keep_blank_values=True)
            return {k: v[0] for k, v in data.items()}

        if content_type.startswith("multipart/form-data"):
            return self._parse_multipart(content_type)

        return {}

    def _parse_multipart(self, content_type: str) -> dict[str, str | UploadFile]:
        import email.parser
        import io

        # Extract boundary
        boundary = None
        for part in content_type.split(";"):
            part = part.strip()
            if part.startswith("boundary="):
                boundary = part[len("boundary="):].strip('"')
                break
        if not boundary:
            return {}

        # Parse multipart body
        result: dict[str, str | UploadFile] = {}
        delimiter = f"--{boundary}".encode()
        full_body = delimiter + b"\r\n" + self.body

        parser = email.parser.BytesFeedParser()
        parser.feed(full_body)

        # Simple multipart parser using manual boundary splitting
        parts = self.body.split(delimiter)
        for part_data in parts:
            if part_data in (b"", b"--", b"--\r\n"):
                continue
            if part_data.startswith(b"\r\n"):
                part_data = part_data[2:]
            if part_data.endswith(b"\r\n"):
                part_data = part_data[:-2]

            # Split headers and body
            header_end = part_data.find(b"\r\n\r\n")
            if header_end == -1:
                continue

            header_bytes = part_data[:header_end]
            body_bytes = part_data[header_end + 4:]

            part_headers = email.parser.BytesParser().parsebytes(
                header_bytes + b"\r\n\r\n"
            )

            # Extract Content-Disposition
            disp = part_headers.get("Content-Disposition", "")
            field_name = None
            filename = None
            for param in disp.split(";"):
                param = param.strip()
                if param.startswith("name="):
                    field_name = param[5:].strip('"')
                elif param.startswith("filename="):
                    filename = param[9:].strip('"')

            if not field_name:
                continue

            if filename:
                part_ct = part_headers.get_content_type()
                result[field_name] = UploadFile(
                    filename=filename,
                    content_type=part_ct or "application/octet-stream",
                    size=len(body_bytes),
                    _data=body_bytes,
                )
            else:
                result[field_name] = body_bytes.decode("utf-8")

        return result

    def __repr__(self) -> str:
        return f"Request(method={self.method!r}, url={self.url!r})"
