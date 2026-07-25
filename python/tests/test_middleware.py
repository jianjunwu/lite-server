"""Tests for unified callback system on routes (previously middleware).

Since 0.7.0, middleware is replaced by the unified Callback API.
TokenBucket tests moved to test_callback.py.
"""

import pytest

from lite_server.context import Headers, RequestContext, RequestMeta
from lite_server.exceptions import HTTPException, UnauthorizedError
from lite_server.callback import (
    Callback,
    Cors,
    LogRequests,
    RateLimit,
    RequireApiKey,
)
from lite_server.route import _validate_handler_signature


# ---------------------------------------------------------------------------
# Handler signature validation (load-time)
# ---------------------------------------------------------------------------


class TestHandlerSignatureValidation:
    def test_new_style_ctx_handler_passes(self):
        def handler(ctx):
            pass

        _validate_handler_signature(handler, "/test")  # no raise

    def test_async_ctx_handler_passes(self):
        async def handler(ctx):
            pass

        _validate_handler_signature(handler, "/test")  # no raise

    def test_old_style_two_param_handler_raises(self):
        def handler(request, server):
            pass

        with pytest.raises(RuntimeError, match="ctx"):
            _validate_handler_signature(handler, "/test")

    def test_param_named_request_raises(self):
        def handler(request):
            pass

        with pytest.raises(RuntimeError, match="ctx"):
            _validate_handler_signature(handler, "/test")

    def test_param_named_server_raises(self):
        def handler(server):
            pass

        with pytest.raises(RuntimeError, match="ctx"):
            _validate_handler_signature(handler, "/test")


# ---------------------------------------------------------------------------
# Route callback integration (using Pipeline.for_route)
# ---------------------------------------------------------------------------


class TestRouteCallbackIntegration:
    @pytest.mark.asyncio
    async def test_require_api_key_rejects_missing_key(self):
        from lite_server.pipeline import Pipeline

        pipe = Pipeline.for_route([RequireApiKey(keys=["sk-123"])])

        async def handler(ctx):
            return {"body": "ok"}

        ctx = RequestContext(
            meta=RequestMeta(
                route="/test",
                headers=Headers({}),
                client_ip="127.0.0.1",
                request_id="r1",
                timestamp_ns=1,
                method="POST",
            )
        )
        with pytest.raises(UnauthorizedError):
            await pipe.run_route(ctx, handler)

    @pytest.mark.asyncio
    async def test_require_api_key_passes_with_valid_key(self):
        from lite_server.pipeline import Pipeline

        pipe = Pipeline.for_route([RequireApiKey(keys=["sk-123"])])

        async def handler(ctx):
            return {"body": "ok"}

        ctx = RequestContext(
            meta=RequestMeta(
                route="/test",
                headers=Headers({"X-API-Key": "sk-123"}),
                client_ip="127.0.0.1",
                request_id="r1",
                timestamp_ns=1,
                method="POST",
            )
        )
        await pipe.run_route(ctx, handler)
        assert ctx.response == {"body": "ok"}

    @pytest.mark.asyncio
    async def test_cors_adds_response_headers(self):
        from lite_server.pipeline import Pipeline

        pipe = Pipeline.for_route([Cors(allow_origins=["https://app.com"])])

        async def handler(ctx):
            return {"data": "ok"}

        ctx = RequestContext(
            meta=RequestMeta(
                route="/test",
                headers=Headers({}),
                client_ip="127.0.0.1",
                request_id="r1",
                timestamp_ns=1,
                method="POST",
            )
        )
        await pipe.run_route(ctx, handler)
        assert "Access-Control-Allow-Origin" in ctx.response_headers
        assert ctx.response_headers["Access-Control-Allow-Origin"] == "https://app.com"

    @pytest.mark.asyncio
    async def test_rate_limit_does_not_block_normal_requests(self):
        from lite_server.pipeline import Pipeline

        pipe = Pipeline.for_route([RateLimit(requests_per_minute=6000)])

        async def handler(ctx):
            return {"body": "ok"}

        ctx = RequestContext(
            meta=RequestMeta(
                route="/test",
                headers=Headers({}),
                client_ip="127.0.0.1",
                request_id="r1",
                timestamp_ns=1,
                method="POST",
            )
        )
        await pipe.run_route(ctx, handler)
        assert ctx.response == {"body": "ok"}

    @pytest.mark.asyncio
    async def test_log_requests_stores_and_logs_timing(self, caplog):
        import logging
        from lite_server.pipeline import Pipeline

        caplog.set_level(logging.INFO)
        logging.getLogger("lite_server.requests").setLevel(logging.INFO)

        pipe = Pipeline.for_route([LogRequests()])

        async def handler(ctx):
            return {"body": "ok"}

        ctx = RequestContext(
            meta=RequestMeta(
                route="/test",
                headers=Headers({}),
                client_ip="127.0.0.1",
                request_id="r1",
                timestamp_ns=1,
                method="GET",
            )
        )
        await pipe.run_route(ctx, handler)
        log_records = [r for r in caplog.records if r.name == "lite_server.requests"]
        assert len(log_records) == 1
        assert "GET" in log_records[0].getMessage()
        assert "200" in log_records[0].getMessage()


# ---------------------------------------------------------------------------
# Verify middleware shim raises ImportError
# ---------------------------------------------------------------------------


class TestMiddlewareShim:
    def test_import_middleware_raises_with_migration_guide(self):
        with pytest.raises(ImportError, match="DEPRECATED"):
            import lite_server.middleware  # noqa: F401

    def test_import_specific_function_raises(self):
        with pytest.raises(ImportError, match="RequireApiKey"):
            from lite_server.middleware import require_api_key  # noqa: F401
