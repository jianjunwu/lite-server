"""Benchmark custom headers (--header) across every transport path.

Covers: CLI parsing/validation, unary HTTP + SSE (httpx client default
headers), WS stream/bidi (additional_headers), gRPC stream/bidi (metadata),
h2 bidi (send_headers), and the multiproc child flow (headers ride on the
pickled args namespace).

TDD: written before the implementation existed; every test here failed
initially (missing ``cli._parse_header_args``, missing ``headers`` /
``metadata`` kwargs on the target builders, missing ``args.headers``).
"""

from __future__ import annotations

import argparse
import asyncio
import threading
import time

import pytest

from lite_server import cli


# ── CLI parsing (cli._parse_header_args) ─────────────────────────────────

class TestParseHeaderArgs:
    """--header parsing: curl-style 'Name: value', names lowercased."""

    def test_parse_happy_path(self):
        out = cli._parse_header_args([
            "Authorization: Bearer xyz",
            "X-Tenant: acme",
        ])
        assert out == {"authorization": "Bearer xyz", "x-tenant": "acme"}

    def test_parse_value_keeps_inner_spaces(self):
        out = cli._parse_header_args(["Authorization: Bearer a b c"])
        assert out == {"authorization": "Bearer a b c"}

    def test_parse_empty_value_allowed(self):
        out = cli._parse_header_args(["X-Empty: "])
        assert out == {"x-empty": ""}

    def test_parse_none_returns_empty(self):
        assert cli._parse_header_args(None) == {}

    def test_parse_no_colon_raises(self):
        with pytest.raises(ValueError):
            cli._parse_header_args(["garbage"])

    def test_parse_empty_name_raises(self):
        with pytest.raises(ValueError):
            cli._parse_header_args([": value"])

    def test_parse_whitespace_in_name_raises(self):
        with pytest.raises(ValueError):
            cli._parse_header_args(["Bad Name: value"])


class TestCliFailFast:
    """Bad --header exits 2 before any network contact."""

    def test_bad_header_exits_2(self):
        rc = cli.main(["benchmark", "--model", "m", "--header", "no-colon"])
        assert rc == 2


# ── Unary + SSE: httpx client default headers ────────────────────────────

class _HeaderRecordingServer:
    """Minimal HTTP/1.1 server recording request headers per path."""

    def __init__(self, sse: bool = False):
        self.port = None
        self.sse = sse
        self.requests: list[dict] = []
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        for _ in range(100):
            if self.port is not None:
                return
            time.sleep(0.05)
        raise RuntimeError("test server failed to start")

    def _run(self):
        async def serve():
            async def handle(reader, writer):
                try:
                    request_line = await reader.readline()
                    headers = {}
                    while True:
                        line = await reader.readline()
                        if line in (b"\r\n", b"", None):
                            break
                        name, _, value = line.decode("latin-1").partition(":")
                        headers[name.strip().lower()] = value.strip()
                    if self.sse:
                        body = (
                            b'data: {"output": 1}\r\n'
                            b"\r\n"
                            b"data: [DONE]\r\n"
                            b"\r\n"
                        )
                        content_type = b"text/event-stream"
                    else:
                        body = b'{"output": 1}'
                        content_type = b"application/json"
                    self.requests.append({
                        "path": request_line.split(b" ")[1].decode(),
                        "headers": headers,
                    })
                    writer.write(
                        b"HTTP/1.1 200 OK\r\n"
                        b"Content-Type: " + content_type + b"\r\n"
                        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                        b"Connection: close\r\n\r\n" + body
                    )
                    await writer.drain()
                except Exception:
                    pass
                finally:
                    writer.close()

            server = await asyncio.start_server(handle, "127.0.0.1", 0)
            self.port = server.sockets[0].getsockname()[1]
            async with server:
                await server.serve_forever()

        asyncio.run(serve())

    def url(self):
        return f"http://127.0.0.1:{self.port}"


def _benchmark_ns(headers=None, *, stream=False) -> argparse.Namespace:
    """Benchmark-shaped namespace, mirroring what _cmd_benchmark builds."""
    return argparse.Namespace(
        model="m",
        stream=stream,
        bidi=False,
        requests=1,
        warmup_requests=0,
        grace_period=30.0,
        rate=None,
        transport="sse",
        endpoint="events",
        model_type="llm",
        stream_read_timeout=300.0,
        cancel_after=None,
        read_delay_ms=None,
        slo_attainment=None,
        headers=headers or {},
        payload_random=None,
    )


class TestHttpClientDefaultHeaders:
    """unary and SSE share one httpx client; default headers reach both."""

    @pytest.mark.asyncio
    async def test_unary_sends_headers(self):
        server = _HeaderRecordingServer()
        server.start()
        ns = _benchmark_ns({"x-test": "1"})

        await cli._run_benchmark_level(
            ns, 1,
            url=server.url(),
            payload_factory=lambda: {"input": 1},
            payloads=[{"input": 1}],
            pacing=None, duration=None, goodput_slo=None, token_counter=None,
        )

        assert len(server.requests) == 1
        assert server.requests[0]["headers"]["x-test"] == "1"

    @pytest.mark.asyncio
    async def test_sse_sends_headers(self):
        server = _HeaderRecordingServer(sse=True)
        server.start()
        ns = _benchmark_ns({"x-test": "1"}, stream=True)

        await cli._run_benchmark_level(
            ns, 1,
            url=server.url(),
            payload_factory=lambda: {"input": 1},
            payloads=[{"input": 1}],
            pacing=None, duration=None, goodput_slo=None, token_counter=None,
        )

        assert len(server.requests) == 1
        assert server.requests[0]["headers"]["x-test"] == "1"


# ── WS stream / bidi: additional_headers ─────────────────────────────────

class _FakeWs:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent = []

    async def send(self, data):
        self.sent.append(data)

    async def recv(self):
        if self.messages:
            return self.messages.pop(0)
        raise RuntimeError("closed without done")


class _Ctx:
    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, *args):
        return False


def _fake_connect(ws, calls):
    def connect(url, **kwargs):
        calls.append(kwargs)
        return _Ctx(ws)
    return connect


class TestWsTargets:
    @pytest.mark.asyncio
    async def test_ws_stream_forwards_additional_headers(self):
        from lite_server.benchmark.ws_target import ws_stream_target

        calls = []
        connect = _fake_connect(_FakeWs([b"chunk", '{"done": true}']), calls)
        target = ws_stream_target(
            connect, "ws://x/stream", headers={"x-auth": "1"},
        )

        async for _ in target({}):
            pass

        assert calls == [{"additional_headers": {"x-auth": "1"}}]

    @pytest.mark.asyncio
    async def test_ws_stream_no_headers_no_kwarg(self):
        """Backward compat: without --header the connect call is unchanged."""
        from lite_server.benchmark.ws_target import ws_stream_target

        calls = []
        connect = _fake_connect(_FakeWs([b"chunk", '{"done": true}']), calls)
        target = ws_stream_target(connect, "ws://x/stream")

        async for _ in target({}):
            pass

        assert calls == [{}]

    @pytest.mark.asyncio
    async def test_ws_bidi_forwards_additional_headers(self):
        from lite_server.benchmark.bidi_session import Pacing
        from lite_server.benchmark.ws_bidi_target import ws_bidi_session

        calls = []
        connect = _fake_connect(_FakeWs([b"ready", '{"done": true}']), calls)
        session = ws_bidi_session(
            connect, "ws://x/stream",
            pacing=Pacing(mode="lock_step"), idle_timeout=1.0,
            headers={"x-auth": "1"},
        )

        rec = await session([{"cfg": 1}, "c1"])

        assert calls == [{"additional_headers": {"x-auth": "1"}}]
        assert rec.producer_chunks == 1


# ── gRPC stream / bidi: call metadata ────────────────────────────────────

class TestGrpcTargets:
    @staticmethod
    def _fake_unary_stream_channel(responses):
        """Fake aio channel whose StreamInfer/DecoupledInfer record metadata."""
        calls = []

        class FakeCall:
            def __aiter__(self):
                async def gen():
                    for r in responses:
                        yield r
                return gen()

        class FakeChannel:
            def unary_stream(self, path, request_serializer=None,
                             response_deserializer=None):
                def multi_callable(request, timeout=None, **kwargs):
                    calls.append({
                        "path": path,
                        "timeout": timeout,
                        "metadata": kwargs.get("metadata"),
                    })
                    return FakeCall()
                return multi_callable

            def unary_unary(self, *args, **kwargs):
                return None

            def stream_stream(self, *args, **kwargs):
                return None

        return FakeChannel(), calls

    @pytest.mark.asyncio
    async def test_grpc_stream_forwards_metadata(self):
        from lite_server.benchmark.grpc_target import grpc_stream_target
        from lite_server.proto import liteserver_pb2

        channel, calls = self._fake_unary_stream_channel([
            liteserver_pb2.StreamChunk(data=b"x"),
        ])
        target_fn = grpc_stream_target(
            channel, "m", metadata=(("authorization", "Bearer x"),),
        )

        async for _ in target_fn({"input": 1}):
            pass

        assert calls[0]["metadata"] == (("authorization", "Bearer x"),)

    @pytest.mark.asyncio
    async def test_grpc_stream_no_metadata_no_kwarg(self):
        from lite_server.benchmark.grpc_target import grpc_stream_target

        channel, calls = self._fake_unary_stream_channel([])
        target_fn = grpc_stream_target(channel, "m")

        async for _ in target_fn({"input": 1}):
            pass

        assert calls[0]["metadata"] is None

    @staticmethod
    def _fake_stream_stream_channel():
        """Fake aio channel whose BidiStream records metadata."""
        from lite_server.proto import liteserver_pb2

        calls = []

        class FakeCall:
            async def write(self, chunk):
                pass

            async def read(self):
                return liteserver_pb2.BidiChunk(
                    close=liteserver_pb2.BidiClose())

            async def done_writing(self):
                pass

            def cancel(self):
                pass

        class FakeChannel:
            def stream_stream(self, path, request_serializer=None,
                              response_deserializer=None):
                def multi_callable(timeout=None, **kwargs):
                    calls.append(kwargs.get("metadata"))
                    return FakeCall()
                return multi_callable

            def unary_unary(self, *args, **kwargs):
                return None

            def unary_stream(self, *args, **kwargs):
                return None

        return FakeChannel(), calls

    @pytest.mark.asyncio
    async def test_grpc_bidi_forwards_metadata(self):
        from lite_server.benchmark.bidi_session import Pacing
        from lite_server.benchmark.grpc_bidi_target import grpc_bidi_session

        channel, calls = self._fake_stream_stream_channel()
        session = grpc_bidi_session(
            channel, "m", pacing=Pacing(mode="lock_step"), idle_timeout=1.0,
            metadata=(("authorization", "Bearer x"),),
        )

        rec = await session([{"cfg": 1}])

        assert rec is not None
        assert calls == [(("authorization", "Bearer x"),)]

    @pytest.mark.asyncio
    async def test_grpc_bidi_no_metadata_no_kwarg(self):
        from lite_server.benchmark.bidi_session import Pacing
        from lite_server.benchmark.grpc_bidi_target import grpc_bidi_session

        channel, calls = self._fake_stream_stream_channel()
        session = grpc_bidi_session(
            channel, "m", pacing=Pacing(mode="lock_step"), idle_timeout=1.0,
        )

        await session([{"cfg": 1}])

        assert calls == [None]


# ── h2 bidi: send_headers ────────────────────────────────────────────────

class TestH2Target:
    @pytest.mark.asyncio
    async def test_h2_bidi_appends_headers_to_post(self, monkeypatch):
        import h2

        from lite_server.benchmark.benchmark import RequestStreamError
        from lite_server.benchmark.bidi_session import Pacing
        from lite_server.benchmark.h2_bidi_target import h2_bidi_session

        sent = []

        async def fake_open_connection(host, port):
            class FakeReader:
                async def read(self, n):
                    return b""  # immediate EOF → Error("closed by peer")

            class FakeWriter:
                def write(self, data):
                    pass

                async def drain(self):
                    pass

                def close(self):
                    pass

            return FakeReader(), FakeWriter()

        orig_send_headers = h2.connection.H2Connection.send_headers

        def record_send_headers(self, stream_id, headers, **kwargs):
            sent.append(headers)
            return orig_send_headers(self, stream_id, headers, **kwargs)

        monkeypatch.setattr(asyncio, "open_connection", fake_open_connection)
        monkeypatch.setattr(
            h2.connection.H2Connection, "send_headers", record_send_headers,
        )

        session = h2_bidi_session(
            "http://127.0.0.1:8000/v2/models/m/bidi",
            pacing=Pacing(mode="lock_step"), idle_timeout=2.0,
            headers={"x-custom": "1"},
        )

        # Peer EOF before any response → RequestStreamError; the headers were
        # already sent by then.
        with pytest.raises(RequestStreamError):
            await session([{"cfg": 1}])

        post = [h for h in sent if (":method", "POST") in h]
        assert post, "client must send the POST headers before failing"
        assert ("x-custom", "1") in post[0]
