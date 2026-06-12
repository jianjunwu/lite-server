"""Unit tests for lite_server.request."""

import pytest
from lite_server.request import (
    URL,
    Headers,
    QueryParams,
    Client,
    State,
    UploadFile,
    Request,
)


# ============================================================================
# URL
# ============================================================================

class TestURL:
    def test_defaults(self):
        u = URL()
        assert u.scheme == "http"
        assert u.hostname == "localhost"
        assert u.port is None
        assert u.path == "/"
        assert u.query == ""

    def test_netloc_without_port(self):
        u = URL(hostname="example.com")
        assert u.netloc == "example.com"

    def test_netloc_with_port(self):
        u = URL(hostname="example.com", port=8080)
        assert u.netloc == "example.com:8080"

    def test_is_secure_http(self):
        u = URL(scheme="http")
        assert u.is_secure is False

    def test_is_secure_https(self):
        u = URL(scheme="https")
        assert u.is_secure is True

    def test_include_query_params_adds(self):
        u = URL(query="a=1").include_query_params(b="2")
        assert "a=1" in u.query
        assert "b=2" in u.query

    def test_include_query_params_override(self):
        u = URL(query="a=1").include_query_params(a="99")
        assert u.query in ("a=99", "a=99&", "&a=99")

    def test_include_query_params_on_clean_url(self):
        u = URL().include_query_params(page="2")
        assert u.query == "page=2"

    def test_remove_query_params(self):
        u = URL(query="a=1&b=2").remove_query_params("a")
        assert "a=1" not in u.query
        assert "b=2" in u.query

    def test_remove_nonexistent_key(self):
        u = URL(query="a=1").remove_query_params("x")
        assert "a=1" in u.query

    def test_replace_scheme(self):
        u = URL(scheme="http").replace(scheme="https")
        assert u.scheme == "https"
        assert u.hostname == "localhost"  # unchanged

    def test_replace_hostname(self):
        u = URL(hostname="localhost").replace(hostname="api.example.com")
        assert u.hostname == "api.example.com"

    def test_replace_port(self):
        u = URL(port=80).replace(port=443)
        assert u.port == 443

    def test_replace_path(self):
        u = URL(path="/old").replace(path="/new")
        assert u.path == "/new"


# ============================================================================
# Headers
# ============================================================================

class TestHeaders:
    def test_empty(self):
        h = Headers()
        assert h.items() == []
        assert h.keys() == []
        assert h.values() == []

    def test_case_insensitive(self):
        h = Headers({"Content-Type": "application/json", "CONTENT-LENGTH": "100"})
        assert h.get("content-type") == "application/json"
        assert h.get("Content-Type") == "application/json"
        assert h.get("CONTENT-TYPE") == "application/json"

    def test_get_default(self):
        h = Headers({"Accept": "text/html"})
        assert h.get("X-Missing") is None
        assert h.get("X-Missing", "default") == "default"

    def test_getlist_single_value(self):
        h = Headers({"Accept": "text/html"})
        assert h.getlist("accept") == ["text/html"]

    def test_getlist_multi_values(self):
        h = Headers({"X-Custom": "val1"})
        h._data.setdefault("x-custom", []).append("val2")
        assert h.getlist("x-custom") == ["val1", "val2"]

    def test_items(self):
        h = Headers({"a": "1", "B": "2"})
        items = h.items()
        assert ("a", "1") in items
        assert ("b", "2") in items

    def test_keys(self):
        h = Headers({"A": "1", "b": "2"})
        keys = h.keys()
        assert "a" in keys
        assert "b" in keys

    def test_values(self):
        h = Headers({"A": "1", "b": "2"})
        values = h.values()
        assert "1" in values
        assert "2" in values

    def test_contains(self):
        h = Headers({"Content-Type": "text/plain"})
        assert "content-type" in h
        assert "Content-Type" in h
        assert "missing" not in h

    def test_getitem(self):
        h = Headers({"Accept": "application/json"})
        assert h["Accept"] == "application/json"
        assert h["accept"] == "application/json"

    def test_getitem_keyerror(self):
        h = Headers()
        with pytest.raises(KeyError):
            _ = h["No-Such"]

    def test_repr(self):
        h = Headers({"X-Id": "42"})
        r = repr(h)
        assert "X-Id" in r or "x-id" in r
        assert "42" in r

    def test_deduplicate_on_init(self):
        # Multiple same key should store as list
        h = Headers({"Accept": "json"})
        assert len(h.getlist("accept")) == 1


# ============================================================================
# QueryParams
# ============================================================================

class TestQueryParams:
    def test_empty_no_args(self):
        q = QueryParams()
        assert q.items() == []

    def test_from_dict(self):
        q = QueryParams({"a": "1", "b": "2"})
        assert q.get("a") == "1"
        assert q.get("b") == "2"

    def test_from_string(self):
        q = QueryParams("a=1&b=2")
        assert q.get("a") == "1"
        assert q.get("b") == "2"

    def test_get_default(self):
        q = QueryParams("a=1")
        assert q.get("b") is None
        assert q.get("b", "default") == "default"

    def test_getlist(self):
        q = QueryParams("ids=1&ids=2&ids=3")
        assert q.getlist("ids") == ["1", "2", "3"]

    def test_getlist_missing(self):
        q = QueryParams("a=1")
        assert q.getlist("missing") == []

    def test_items(self):
        q = QueryParams("a=1&b=2")
        items = q.items()
        assert ("a", "1") in items
        assert ("b", "2") in items

    def test_contains(self):
        q = QueryParams("key=value")
        assert "key" in q
        assert "missing" not in q

    def test_repr(self):
        q = QueryParams("a=1")
        r = repr(q)
        assert "a" in r
        assert "1" in r

    def test_keep_blank_values(self):
        q = QueryParams("a=&b=2")
        assert q.get("a") == ""

    def test_url_encoded_values(self):
        q = QueryParams("msg=hello%20world")
        assert q.get("msg") == "hello world"

    def test_none_init(self):
        q = QueryParams(None)
        assert q.items() == []


# ============================================================================
# Client
# ============================================================================

class TestClient:
    def test_defaults(self):
        c = Client()
        assert c.host is None
        assert c.port is None

    def test_with_values(self):
        c = Client(host="192.168.1.1", port=54321)
        assert c.host == "192.168.1.1"
        assert c.port == 54321


# ============================================================================
# State
# ============================================================================

class TestState:
    def test_set_and_get_attribute(self):
        s = State()
        s.user_id = "abc123"
        assert s.user_id == "abc123"

    def test_delete_attribute(self):
        s = State()
        s.key = "value"
        del s.key
        with pytest.raises(AttributeError):
            _ = s.key

    def test_delete_nonexistent(self):
        s = State()
        with pytest.raises(AttributeError):
            del s.no_such_key

    def test_get_nonexistent(self):
        s = State()
        with pytest.raises(AttributeError):
            _ = s.no_such_key

    def test_private_attr_raises(self):
        s = State()
        with pytest.raises(AttributeError):
            _ = s._private

    def test_repr(self):
        s = State()
        s.x = 1
        r = repr(s)
        assert "x" in r
        assert "1" in r

    def test_overwrite(self):
        s = State()
        s.x = 1
        s.x = 2
        assert s.x == 2

    def test_multiple_keys(self):
        s = State()
        s.a = 1
        s.b = "hello"
        assert s.a == 1
        assert s.b == "hello"


# ============================================================================
# UploadFile
# ============================================================================

class TestUploadFile:
    def test_defaults(self):
        f = UploadFile(filename="test.txt", content_type="text/plain", size=5, _data=b"hello")
        assert f.filename == "test.txt"
        assert f.content_type == "text/plain"
        assert f.size == 5

    def test_read_returns_data(self):
        f = UploadFile(filename="test.bin", _data=b"\x00\x01\x02")
        assert f.read() == b"\x00\x01\x02"

    def test_seek_is_noop(self):
        f = UploadFile(filename="test.bin", _data=b"data")
        f.seek(2)
        assert f.read() == b"data"  # data unchanged


# ============================================================================
# Request
# ============================================================================

class TestRequest:
    def test_defaults(self):
        r = Request()
        assert r.method == "GET"
        assert isinstance(r.url, URL)
        assert isinstance(r.headers, Headers)
        assert isinstance(r.query_params, QueryParams)
        assert r.body == b""
        assert isinstance(r.client, Client)
        assert isinstance(r.state, State)
        assert r.cookies == {}

    def test_method_is_uppercased(self):
        r = Request(method="post")
        assert r.method == "POST"

    def test_custom_url(self):
        u = URL(path="/api/v1", query="q=test")
        r = Request(url=u)
        assert r.url.path == "/api/v1"

    def test_custom_headers(self):
        r = Request(headers={"Authorization": "Bearer token", "X-Request-Id": "123"})
        assert r.headers.get("authorization") == "Bearer token"
        assert r.headers.get("x-request-id") == "123"

    def test_custom_query_params(self):
        r = Request(query_params={"page": "1", "limit": "10"})
        assert r.query_params.get("page") == "1"
        assert r.query_params.get("limit") == "10"

    def test_custom_body(self):
        r = Request(body=b'{"key": "value"}')
        assert r.body == b'{"key": "value"}'

    def test_client_info(self):
        r = Request(client_host="10.0.0.1", client_port=50000)
        assert r.client.host == "10.0.0.1"
        assert r.client.port == 50000

    def test_json_parses_valid_body(self):
        r = Request(body=b'{"name": "test", "count": 42}')
        data = r.json()
        assert data == {"name": "test", "count": 42}

    def test_json_returns_empty_dict_for_empty_body(self):
        r = Request(body=b"")
        assert r.json() == {}

    def test_json_parses_array(self):
        r = Request(body=b'[1, 2, 3]')
        data = r.json()
        assert data == [1, 2, 3]

    def test_text_decodes_utf8(self):
        r = Request(body="hello world".encode("utf-8"))
        assert r.text() == "hello world"

    def test_text_unicode(self):
        r = Request(body="héllo 🚀".encode("utf-8"))
        assert r.text() == "héllo 🚀"

    def test_cookies_from_header(self):
        r = Request(headers={"Cookie": "session=abc123; theme=dark"})
        assert r.cookies == {"session": "abc123", "theme": "dark"}

    def test_cookies_empty_when_no_header(self):
        r = Request()
        assert r.cookies == {}

    def test_cookies_whitespace_tolerant(self):
        r = Request(headers={"Cookie": " a = 1 ; b = 2 "})
        assert r.cookies["a"] == "1"
        assert r.cookies["b"] == "2"

    def test_repr(self):
        r = Request(method="POST")
        rep = repr(r)
        assert "POST" in rep


# ============================================================================
# Form parsing
# ============================================================================

class TestFormUrlEncoded:
    def test_urlencoded_form(self):
        r = Request(
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body=b"name=john&age=30",
        )
        form = r.form()
        assert form == {"name": "john", "age": "30"}

    def test_urlencoded_empty(self):
        r = Request(
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body=b"",
        )
        form = r.form()
        assert form == {}

    def test_no_content_type_returns_empty(self):
        r = Request(body=b"name=john")
        form = r.form()
        assert form == {}


class TestMultipartForm:
    def test_multipart_single_field(self):
        body = (
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="username"\r\n'
            b"\r\n"
            b"john\r\n"
            b"--boundary123--\r\n"
        )
        r = Request(
            headers={"Content-Type": "multipart/form-data; boundary=boundary123"},
            body=body,
        )
        form = r.form()
        assert form["username"] == "john"

    def test_multipart_file_upload(self):
        body = (
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="file"; filename="test.txt"\r\n'
            b"Content-Type: text/plain\r\n"
            b"\r\n"
            b"hello world\r\n"
            b"--boundary123--\r\n"
        )
        r = Request(
            headers={"Content-Type": "multipart/form-data; boundary=boundary123"},
            body=body,
        )
        form = r.form()
        f = form["file"]
        assert isinstance(f, UploadFile)
        assert f.filename == "test.txt"
        assert f.content_type == "text/plain"
        assert f.read() == b"hello world"

    def test_multipart_multiple_fields(self):
        body = (
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="name"\r\n'
            b"\r\n"
            b"alice\r\n"
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="age"\r\n'
            b"\r\n"
            b"25\r\n"
            b"--boundary123--\r\n"
        )
        r = Request(
            headers={"Content-Type": "multipart/form-data; boundary=boundary123"},
            body=body,
        )
        form = r.form()
        assert form["name"] == "alice"
        assert form["age"] == "25"

    def test_multipart_mixed_file_and_field(self):
        body = (
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="description"\r\n'
            b"\r\n"
            b"my file\r\n"
            b"--boundary123\r\n"
            b'Content-Disposition: form-data; name="attachment"; filename="data.bin"\r\n'
            b"Content-Type: application/octet-stream\r\n"
            b"\r\n"
            b"\x00\x01\x02\x03\r\n"
            b"--boundary123--\r\n"
        )
        r = Request(
            headers={"Content-Type": "multipart/form-data; boundary=boundary123"},
            body=body,
        )
        form = r.form()
        assert form["description"] == "my file"
        f = form["attachment"]
        assert isinstance(f, UploadFile)
        assert f.filename == "data.bin"
        assert f.read() == b"\x00\x01\x02\x03"

    def test_multipart_no_boundary(self):
        r = Request(
            headers={"Content-Type": "multipart/form-data"},
            body=b"ignored",
        )
        form = r.form()
        assert form == {}
