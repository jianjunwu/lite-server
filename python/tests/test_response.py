"""Unit tests for lite_server.response."""

import pytest
from lite_server.response import (
    BackgroundTask,
    Response,
    JSONResponse,
    HTMLResponse,
    PlainTextResponse,
    RedirectResponse,
    FileResponse,
    StreamingResponse,
    _render_cookie,
    _delete_cookie_header,
)


# ============================================================================
# _render_cookie
# ============================================================================

class TestRenderCookie:
    def test_minimal_cookie(self):
        c = _render_cookie("session", "abc123")
        assert c == "session=abc123; Path=/; SameSite=Lax"

    def test_empty_value(self):
        c = _render_cookie("flag", "")
        assert c == "flag=; Path=/; SameSite=Lax"

    def test_max_age(self):
        c = _render_cookie("session", "abc", max_age=3600)
        assert "Max-Age=3600" in c

    def test_expires(self):
        c = _render_cookie("session", "abc", expires="Wed, 21 Oct 2025 07:28:00 GMT")
        assert "Expires=Wed, 21 Oct 2025 07:28:00 GMT" in c

    def test_domain(self):
        c = _render_cookie("session", "abc", domain="example.com")
        assert "Domain=example.com" in c

    def test_secure(self):
        c = _render_cookie("session", "abc", secure=True)
        assert "; Secure" in c

    def test_httponly(self):
        c = _render_cookie("session", "abc", httponly=True)
        assert "; HttpOnly" in c

    def test_samesite_strict(self):
        c = _render_cookie("session", "abc", samesite="strict")
        assert "; SameSite=Strict" in c

    def test_samesite_none(self):
        c = _render_cookie("session", "abc", samesite=None)
        assert "SameSite" not in c

    def test_partitioned(self):
        c = _render_cookie("session", "abc", partitioned=True)
        assert "; Partitioned" in c

    def test_custom_path(self):
        c = _render_cookie("session", "abc", path="/api")
        assert "Path=/api" in c

    def test_no_path(self):
        c = _render_cookie("session", "abc", path="")
        assert "Path" not in c

    def test_all_options_combined(self):
        c = _render_cookie(
            "session", "abc123",
            max_age=7200,
            domain=".example.com",
            path="/app",
            secure=True,
            httponly=True,
            samesite="strict",
            partitioned=True,
        )
        assert "session=abc123" in c
        assert "Max-Age=7200" in c
        assert "Domain=.example.com" in c
        assert "Path=/app" in c
        assert "; Secure" in c
        assert "; HttpOnly" in c
        assert "; SameSite=Strict" in c
        assert "; Partitioned" in c


class TestDeleteCookieHeader:
    def test_delete_cookie(self):
        c = _delete_cookie_header("session")
        assert c.startswith("session=;")
        assert "Max-Age=0" in c
        assert "Expires=Thu, 01 Jan 1970 00:00:00 GMT" in c

    def test_delete_with_domain(self):
        c = _delete_cookie_header("session", domain="example.com")
        assert "Domain=example.com" in c


# ============================================================================
# BackgroundTask
# ============================================================================

class TestBackgroundTask:
    @pytest.mark.asyncio
    async def test_call_executes_function(self):
        side_effect = []
        def fn(x):
            side_effect.append(x)

        task = BackgroundTask(fn, 42)
        await task()
        assert side_effect == [42]

    @pytest.mark.asyncio
    async def test_async_call(self):
        side_effect = []
        def fn(x):
            side_effect.append(x)

        task = BackgroundTask(fn, "hello")
        await task()
        assert side_effect == ["hello"]

    @pytest.mark.asyncio
    async def test_args_and_kwargs(self):
        side_effect = []
        def fn(a, b=0, c=None):
            side_effect.append((a, b, c))

        task = BackgroundTask(fn, 1, b=2, c=3)
        await task()
        assert side_effect == [(1, 2, 3)]


# ============================================================================
# Response
# ============================================================================

class TestResponse:
    def test_defaults(self):
        r = Response()
        assert r.status_code == 200
        assert r.media_type == "application/json"
        assert r.headers == {}
        assert r.content is None
        assert r.background is None
        assert r._cookie_headers == []

    def test_custom_status_and_headers(self):
        r = Response(content={"data": 1}, status_code=201, headers={"x-id": "123"})
        assert r.status_code == 201
        assert r.headers == {"x-id": "123"}

    def test_set_cookie_basic(self):
        r = Response()
        r.set_cookie("token", "xyz")
        assert len(r._cookie_headers) == 1
        assert r._cookie_headers[0][0] == "set-cookie"
        assert "token=xyz" in r._cookie_headers[0][1]

    def test_set_multiple_cookies(self):
        r = Response()
        r.set_cookie("a", "1")
        r.set_cookie("b", "2")
        assert len(r._cookie_headers) == 2

    def test_delete_cookie(self):
        r = Response()
        r.delete_cookie("session")
        assert len(r._cookie_headers) == 1
        header_val = r._cookie_headers[0][1]
        assert "Max-Age=0" in header_val

    def test_set_cookie_secure_httponly(self):
        r = Response()
        r.set_cookie("token", "secret", secure=True, httponly=True, samesite="strict")
        header_val = r._cookie_headers[0][1]
        assert "Secure" in header_val
        assert "HttpOnly" in header_val
        assert "SameSite=Strict" in header_val

    def test_set_cookie_partitioned(self):
        r = Response()
        r.set_cookie("token", "val", partitioned=True)
        assert "Partitioned" in r._cookie_headers[0][1]


# ============================================================================
# JSONResponse
# ============================================================================

class TestJSONResponse:
    def test_default_media_type(self):
        r = JSONResponse(content={"key": "value"})
        assert r.media_type == "application/json"

    def test_custom_status(self):
        r = JSONResponse(content={"error": "msg"}, status_code=400)
        assert r.status_code == 400
        assert r.media_type == "application/json"


# ============================================================================
# HTMLResponse
# ============================================================================

class TestHTMLResponse:
    def test_media_type(self):
        r = HTMLResponse(content="<h1>Hello</h1>")
        assert r.media_type == "text/html"


# ============================================================================
# PlainTextResponse
# ============================================================================

class TestPlainTextResponse:
    def test_media_type(self):
        r = PlainTextResponse(content="hello world")
        assert r.media_type == "text/plain"


# ============================================================================
# RedirectResponse
# ============================================================================

class TestRedirectResponse:
    def test_default_302(self):
        r = RedirectResponse(url="/login")
        assert r.status_code == 302
        assert r.headers["location"] == "/login"
        assert r.media_type == "text/plain"
        assert r.content == b""

    def test_permanent_301(self):
        r = RedirectResponse(url="/new-location", status_code=301)
        assert r.status_code == 301
        assert r.headers["location"] == "/new-location"

    def test_307_temporary(self):
        r = RedirectResponse(url="/", status_code=307)
        assert r.status_code == 307

    def test_custom_headers(self):
        r = RedirectResponse(url="/other", headers={"x-custom": "val"})
        assert r.headers["location"] == "/other"
        assert r.headers["x-custom"] == "val"

    def test_headers_dont_override_location(self):
        r = RedirectResponse(url="/new", headers={"location": "/old"})
        # location should be /new (set after headers are merged)
        assert r.headers["location"] == "/new"


# ============================================================================
# FileResponse
# ============================================================================

class TestFileResponse:
    def test_defaults(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_bytes(b"content")
        r = FileResponse(path=str(p))
        assert r.status_code == 200
        assert r.media_type in ("text/plain", "application/octet-stream")
        assert r.file_path == str(p)

    def test_custom_filename(self):
        r = FileResponse(path="/tmp/data.bin", filename="download.bin")
        assert 'filename="download.bin"' in r.headers["content-disposition"]

    def test_uses_basename_when_no_filename(self):
        r = FileResponse(path="/tmp/data.json")
        assert 'filename="data.json"' in r.headers["content-disposition"]

    def test_custom_media_type(self):
        r = FileResponse(path="/tmp/data.bin", media_type="application/custom")
        assert r.media_type == "application/custom"

    def test_known_mime_type(self):
        r = FileResponse(path="/tmp/file.pdf")
        assert r.media_type == "application/pdf"

    def test_unknown_mime_type_fallback(self):
        r = FileResponse(path="/tmp/file.xyzunknown")
        assert r.media_type == "application/octet-stream"

    def test_extra_headers(self):
        r = FileResponse(path="/tmp/data.bin", headers={"x-rate": "100"})
        assert r.headers["x-rate"] == "100"
        assert "content-disposition" in r.headers


# ============================================================================
# StreamingResponse
# ============================================================================

class TestStreamingResponse:
    def test_default_media_type_sse(self):
        def gen():
            yield "chunk1"
            yield "chunk2"
        r = StreamingResponse(content=gen())
        assert r.media_type == "text/event-stream"

    def test_custom_media_type(self):
        def gen():
            yield b"data"
        r = StreamingResponse(content=gen(), media_type="application/octet-stream")
        assert r.media_type == "application/octet-stream"

    def test_custom_status(self):
        def gen():
            yield "chunk"
        r = StreamingResponse(content=gen(), status_code=201)
        assert r.status_code == 201

    def test_async_iterator(self):
        async def gen():
            yield "chunk"
        r = StreamingResponse(content=gen())
        assert r.media_type == "text/event-stream"

    def test_custom_headers(self):
        def gen():
            yield "data"
        r = StreamingResponse(content=gen(), headers={"x-stream": "1"})
        assert r.headers["x-stream"] == "1"
