"""Tests for the custom-route API (phase 2 litapi integration).

The ``@route`` decorator annotates LitAPI methods with a ``__route_defs__``
list (no process-global state); the worker's ``_discover_routes`` collects
these off an instance, binds ``self``, and unions stacked methods.
"""

import json

import pytest

from lite_server import LitAPI, route
from lite_server.response import Response
from lite_server.route import HandlerSignatureError, RouteDef


class TestRouteCallServerInjection:
    """``_handle_route_call`` injects the worker-level ServerProxy into ctx."""

    @pytest.mark.asyncio
    async def test_ctx_server_reaches_handler(self):
        import logging

        from lite_server import Headers, RequestMeta
        from lite_server.pipeline import Pipeline
        from lite_server.proto import liteserver_pb2 as pb
        from lite_server.worker.inference import _discover_routes, _handle_route_call

        sentinel = object()

        class M(LitAPI):
            def setup(self, device):
                pass

            def predict(self, x):
                return x

            @route.get("/who")
            def who(self, ctx):
                return {"has_server": ctx.server is sentinel}

        inst = M(max_batch_size=1, batch_timeout=0.0, stream=False)
        _discover_routes(inst)
        inst._route_pipeline = Pipeline.for_route([])
        inst._server_proxy = sentinel

        req = pb.Request(uid="u1")
        req.meta.route = "/who"
        req.meta.method = "GET"
        req.route_call.data = b"{}"
        meta = RequestMeta(
            route="/who", headers=Headers(), client_ip="",
            request_id="", timestamp_ns=0, method="GET",
        )
        resp = await _handle_route_call(
            inst, "u1", req, meta, None, logging.getLogger("test"))
        assert json.loads(resp.single.data) == {"has_server": True}

    @pytest.mark.asyncio
    async def test_ctx_server_defaults_to_none(self):
        import logging

        from lite_server import Headers, RequestMeta
        from lite_server.pipeline import Pipeline
        from lite_server.proto import liteserver_pb2 as pb
        from lite_server.worker.inference import _discover_routes, _handle_route_call

        class M(LitAPI):
            def setup(self, device):
                pass

            def predict(self, x):
                return x

            @route.get("/who")
            def who(self, ctx):
                return {"server_is_none": ctx.server is None}

        inst = M(max_batch_size=1, batch_timeout=0.0, stream=False)
        _discover_routes(inst)
        inst._route_pipeline = Pipeline.for_route([])

        req = pb.Request(uid="u1")
        req.meta.route = "/who"
        req.meta.method = "GET"
        req.route_call.data = b"{}"
        meta = RequestMeta(
            route="/who", headers=Headers(), client_ip="",
            request_id="", timestamp_ns=0, method="GET",
        )
        resp = await _handle_route_call(
            inst, "u1", req, meta, None, logging.getLogger("test"))
        assert json.loads(resp.single.data) == {"server_is_none": True}


class TestRouteStreaming:
    """``@route`` handlers returning ``StreamingResponse`` stream over the
    worker channel: a ``start`` frame (status/headers/media_type), one
    ``chunk`` frame per yielded item, then ``done`` (or ``error``). The
    handler's uid doubles as the stream_id so the Rust side can demux."""

    @staticmethod
    def _fake_socket():
        class FakeSocket:
            def __init__(self):
                self.frames = []

            async def send(self, data):
                self.frames.append(data)

        return FakeSocket()

    @staticmethod
    def _decode(socket):
        from lite_server.proto import liteserver_pb2 as pb
        out = []
        for frame in socket.frames:
            resp = pb.Response()
            resp.ParseFromString(frame)
            out.append(resp)
        return out

    def _make_inst(self, handler):
        from lite_server.pipeline import Pipeline
        from lite_server.worker.inference import _discover_routes

        class M(LitAPI):
            def setup(self, device):
                pass

            def predict(self, x):
                return x

        M.events = route.get("/events")(handler)
        inst = M(max_batch_size=1, batch_timeout=0.0, stream=False)
        _discover_routes(inst)
        inst._route_pipeline = Pipeline.for_route([])
        return inst

    async def _call(self, inst, socket):
        import logging

        from lite_server import Headers, RequestMeta
        from lite_server.proto import liteserver_pb2 as pb
        from lite_server.worker.inference import _handle_route_call

        req = pb.Request(uid="u1")
        req.meta.route = "/events"
        req.meta.method = "GET"
        req.route_call.data = b"{}"
        meta = RequestMeta(
            route="/events", headers=Headers(), client_ip="",
            request_id="", timestamp_ns=0, method="GET",
        )
        return await _handle_route_call(
            inst, "u1", req, meta, socket, logging.getLogger("test"))

    @pytest.mark.asyncio
    async def test_async_generator_sends_start_chunks_done(self):
        from lite_server.response import StreamingResponse

        async def gen():
            yield {"n": 1}
            yield {"n": 2}

        def handler(self, ctx):
            return StreamingResponse(content=gen())

        socket = self._fake_socket()
        result = await self._call(self._make_inst(handler), socket)

        assert result is None  # streaming replies inline, no SingleResponse
        frames = self._decode(socket)
        kinds = [f.stream.WhichOneof("payload") for f in frames]
        assert kinds == ["start", "chunk", "chunk", "done"]
        assert all(f.stream.stream_id == "u1" for f in frames)
        assert frames[0].stream.start.status_code == 200
        assert frames[0].stream.start.media_type == "text/event-stream"
        assert json.loads(frames[1].stream.chunk.data) == {"n": 1}
        assert json.loads(frames[2].stream.chunk.data) == {"n": 2}

    @pytest.mark.asyncio
    async def test_sync_generator_streams(self):
        from lite_server.response import StreamingResponse

        def handler(self, ctx):
            return StreamingResponse(content=iter([{"a": 1}, {"a": 2}]))

        socket = self._fake_socket()
        await self._call(self._make_inst(handler), socket)

        frames = self._decode(socket)
        kinds = [f.stream.WhichOneof("payload") for f in frames]
        assert kinds == ["start", "chunk", "chunk", "done"]

    @pytest.mark.asyncio
    async def test_chunk_serialization_rules(self):
        """bytes pass through, str encodes utf-8, anything else is JSON."""
        from lite_server.response import StreamingResponse

        async def gen():
            yield b"\x00\x01"
            yield "hello"
            yield [1, 2]

        def handler(self, ctx):
            return StreamingResponse(content=gen())

        socket = self._fake_socket()
        await self._call(self._make_inst(handler), socket)

        chunks = [f.stream.chunk.data for f in self._decode(socket)
                  if f.stream.WhichOneof("payload") == "chunk"]
        assert chunks == [b"\x00\x01", b"hello", b"[1,2]"]

    @pytest.mark.asyncio
    async def test_start_frame_carries_status_headers_and_ctx_headers(self):
        from lite_server.response import StreamingResponse

        async def gen():
            yield 1

        def handler(self, ctx):
            ctx.response_headers["X-Cb"] = "1"
            return StreamingResponse(
                content=gen(), status_code=201, headers={"X-H": "2"})

        socket = self._fake_socket()
        await self._call(self._make_inst(handler), socket)

        start = self._decode(socket)[0].stream.start
        assert start.status_code == 201
        assert start.headers["X-H"] == "2"
        assert start.headers["X-Cb"] == "1"

    @pytest.mark.asyncio
    async def test_http_exception_mid_stream_sends_structured_error(self):
        from lite_server.exceptions import HTTPException
        from lite_server.response import StreamingResponse

        async def gen():
            yield {"ok": 1}
            raise HTTPException(status_code=429, detail="slow down")

        def handler(self, ctx):
            return StreamingResponse(content=gen())

        socket = self._fake_socket()
        await self._call(self._make_inst(handler), socket)

        frames = self._decode(socket)
        kinds = [f.stream.WhichOneof("payload") for f in frames]
        assert kinds == ["start", "chunk", "error"]
        err = json.loads(frames[-1].stream.error.message)
        assert err["error"]["message"] == "slow down"

    @pytest.mark.asyncio
    async def test_generic_exception_mid_stream_sends_error(self):
        from lite_server.response import StreamingResponse

        async def gen():
            yield 1
            raise ValueError("boom")

        def handler(self, ctx):
            return StreamingResponse(content=gen())

        socket = self._fake_socket()
        await self._call(self._make_inst(handler), socket)

        frames = self._decode(socket)
        kinds = [f.stream.WhichOneof("payload") for f in frames]
        assert kinds == ["start", "chunk", "error"]
        assert "boom" in frames[-1].stream.error.message


class TestRouterDecorator:
    """``@route`` attaches ``__route_defs__`` to the wrapped function only —
    the ``route`` object is a stateless namespace (no process-global list)."""

    def test_router_is_stateless(self):
        # No process-global route storage survives the standalone-routes removal.
        assert not hasattr(route, "_routes")
        assert not hasattr(route, "routes")

    def test_get_decorator_attaches_route_def(self):
        @route.get("/x")
        def x(self, ctx):
            return {"ok": True}

        assert isinstance(x.__route_defs__, list)
        assert len(x.__route_defs__) == 1
        rd = x.__route_defs__[0]
        assert isinstance(rd, RouteDef)
        assert rd.path == "/x"
        assert rd.methods == ["GET"]

    def test_post_decorator_records_post_method(self):
        @route.post("/y")
        def y(self, ctx):
            return {"ok": True}

        assert y.__route_defs__[0].methods == ["POST"]

    def test_stacked_decorators_append_defs(self):
        @route.get("/z")
        @route.post("/z")
        def z(self, ctx):
            return {"ok": True}

        methods = sorted(rd.methods[0] for rd in z.__route_defs__)
        assert methods == ["GET", "POST"]
        assert all(rd.path == "/z" for rd in z.__route_defs__)


class TestRouteDiscovery:
    """``_discover_routes`` scans a LitAPI instance, binds ``self``, unions
    stacked methods, and rejects misconfigured handlers loudly."""

    @staticmethod
    def _discover(instance):
        from lite_server.worker.inference import _discover_routes
        _discover_routes(instance)
        return instance._route_handlers

    def test_discovers_and_binds_self(self):
        class M(LitAPI):
            def setup(self, device):
                pass

            def predict(self, x):
                return x

            @route.get("/status")
            def status(self, ctx):
                return {"id": id(self)}

        inst = M(max_batch_size=1, batch_timeout=0.0, stream=False)
        handlers = self._discover(inst)
        assert set(handlers) == {"/status"}
        bound, methods = handlers["/status"]
        assert methods == ["GET"]
        # bound method carries self
        assert bound.__self__ is inst

    def test_unions_methods_for_stacked_decorators(self):
        class M(LitAPI):
            def setup(self, device):
                pass

            def predict(self, x):
                return x

            @route.get("/x")
            @route.post("/x")
            def x(self, ctx):
                return {}

        handlers = self._discover(M(max_batch_size=1, batch_timeout=0.0, stream=False))
        assert sorted(handlers["/x"][1]) == ["GET", "POST"]

    def test_rejects_pre_07_signature(self):
        class M(LitAPI):
            def setup(self, device):
                pass

            def predict(self, x):
                return x

            @route.get("/bad")
            def bad(self, request, server):  # pre-0.7 (request, server) signature
                return {}

        inst = M(max_batch_size=1, batch_timeout=0.0, stream=False)
        with pytest.raises(HandlerSignatureError):
            self._discover(inst)

    def test_rejects_duplicate_path_across_handlers(self):
        class M(LitAPI):
            def setup(self, device):
                pass

            def predict(self, x):
                return x

            @route.get("/dup")
            def a(self, ctx):
                return {}

            @route.get("/dup")
            def b(self, ctx):
                return {}

        inst = M(max_batch_size=1, batch_timeout=0.0, stream=False)
        with pytest.raises(HandlerSignatureError):
            self._discover(inst)

    def test_no_routes_yields_empty(self):
        class M(LitAPI):
            def setup(self, device):
                pass

            def predict(self, x):
                return x

        handlers = self._discover(M(max_batch_size=1, batch_timeout=0.0, stream=False))
        assert handlers == {}


class TestRouteResponseEncoding:
    """``_build_route_response`` maps a handler result (``ctx.early`` /
    ``ctx.response``) onto a ``SingleResponse``: plain value → 200 JSON;
    ``Response`` honored for status / headers / media type; body passed
    through verbatim; callback-set headers merged."""

    @staticmethod
    def _build(ctx):
        from lite_server.worker.inference import _build_route_response
        return _build_route_response("u1", ctx).single

    def _ctx(self):
        from lite_server import Headers, RequestContext, RequestMeta
        return RequestContext(
            meta=RequestMeta(
                route="/x", headers=Headers(), client_ip="",
                request_id="", timestamp_ns=0,
            )
        )

    def test_plain_value_becomes_200_json(self):
        ctx = self._ctx()
        ctx.response = {"ok": True}
        single = self._build(ctx)
        assert single.status_code == 200
        assert single.media_type == "application/json"
        assert json.loads(single.data) == {"ok": True}

    def test_response_object_honors_status_and_headers(self):
        ctx = self._ctx()
        ctx.early = Response(content={"error": "x"}, status_code=404, headers={"X-A": "1"})
        single = self._build(ctx)
        assert single.status_code == 404
        assert single.headers["X-A"] == "1"
        assert json.loads(single.data) == {"error": "x"}

    def test_bytes_body_passes_through_with_media_type(self):
        ctx = self._ctx()
        ctx.early = Response(content=b"<html>x</html>", media_type="text/html")
        single = self._build(ctx)
        assert single.data == b"<html>x</html>"
        assert single.media_type == "text/html"

    def test_str_body_encodes(self):
        ctx = self._ctx()
        ctx.early = Response(content="hi", media_type="text/plain")
        single = self._build(ctx)
        assert single.data == b"hi"

    def test_callback_response_headers_merged(self):
        ctx = self._ctx()
        ctx.response = {"ok": True}
        ctx.response_headers["X-Cors"] = "*"
        single = self._build(ctx)
        assert single.headers["X-Cors"] == "*"


class TestRequestModuleRemoved:
    """0.7.0: request.py is deleted — Request, URL, QueryParams, Client,
    State, UploadFile are no longer importable from lite_server."""

    def test_request_module_import_fails(self):
        with pytest.raises(ImportError):
            import lite_server.request  # noqa: F401

    def test_request_class_import_fails(self):
        with pytest.raises(ImportError):
            from lite_server import Request  # noqa: F401

    def test_url_import_fails(self):
        with pytest.raises(ImportError):
            from lite_server import URL  # noqa: F401

    def test_query_params_import_fails(self):
        with pytest.raises(ImportError):
            from lite_server import QueryParams  # noqa: F401

    def test_headers_still_importable_from_context(self):
        """Headers moves to lite_server.context, re-exported from lite_server."""
        from lite_server import Headers
        assert Headers is not None
