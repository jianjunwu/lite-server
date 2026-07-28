"""Tests for unified callback system on routes (previously middleware).

Since 0.7.0, middleware is replaced by the unified Callback API.
TokenBucket tests moved to test_callback.py.
"""

import pytest

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
# Verify middleware shim raises ImportError
# ---------------------------------------------------------------------------


class TestMiddlewareShim:
    def test_import_middleware_raises_with_migration_guide(self):
        with pytest.raises(ImportError, match="DEPRECATED"):
            import lite_server.middleware  # noqa: F401

    def test_import_specific_function_raises(self):
        with pytest.raises(ImportError, match="require_api_key"):
            from lite_server.middleware import require_api_key  # noqa: F401
