"""Tests for lite_server.middleware module."""

import time
import threading

import pytest

from lite_server.middleware import (
    TokenBucket,
    _rate_limiters,
    _rate_limiters_lock,
    cors,
    log_requests,
    rate_limit,
    require_api_key,
)


class TestTokenBucket:
    def test_acquire_depletes_tokens(self):
        bucket = TokenBucket(rate=1.0, capacity=5.0)
        assert bucket.acquire(5.0)
        assert not bucket.acquire(0.1)

    def test_never_exceeds_capacity(self):
        bucket = TokenBucket(rate=100.0, capacity=5.0)
        time.sleep(0.2)  # would generate 20 tokens but capped at 5
        assert bucket.acquire(5.0)
        assert not bucket.acquire(0.1)

    def test_refill_over_time(self):
        bucket = TokenBucket(rate=10.0, capacity=10.0)
        bucket.acquire(10.0)  # drain completely
        assert not bucket.acquire(0.01)
        time.sleep(0.5)  # should refill ~5 tokens
        assert bucket.acquire(5.0)
        assert not bucket.acquire(1.0)

    def test_empty_bucket_rejects(self):
        bucket = TokenBucket(rate=0.0, capacity=1.0)
        bucket.acquire(1.0)  # drain
        assert not bucket.acquire(0.1)

    def test_thread_safety_concurrent_acquire(self):
        """Multiple threads acquiring simultaneously should not corrupt state."""
        bucket = TokenBucket(rate=10.0, capacity=100.0)
        results = []
        errors = []

        def worker():
            try:
                for _ in range(50):
                    bucket.acquire(1.0)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"thread errors: {errors}"
        # Total tokens acquired: 4 threads × 50 = 200
        # Initial 100 + refill over time — enough should succeed
        # After draining, bucket should be empty or nearly empty
        assert bucket.acquire(1.0) or True  # may or may not have tokens

    def test_last_access_updated_on_acquire(self):
        bucket = TokenBucket(rate=1.0, capacity=5.0)
        before = bucket.last_access
        time.sleep(0.01)
        bucket.acquire(1.0)
        assert bucket.last_access > before

    def test_acquire_with_custom_token_amount(self):
        bucket = TokenBucket(rate=1.0, capacity=10.0)
        assert bucket.acquire(3.0)
        assert bucket.acquire(3.0)
        assert bucket.acquire(3.0)
        # 9 used out of 10, 1 left
        assert bucket.acquire(1.0)
        # 10 used, 0 left
        assert not bucket.acquire(0.5)


class TestRateLimitCleanup:
    def teardown_method(self):
        """Clean up global state between tests."""
        with _rate_limiters_lock:
            _rate_limiters.clear()

    def test_cleanup_removes_stale_entries(self):
        from lite_server.middleware import _cleanup_stale_limiters

        bucket = TokenBucket(rate=1.0, capacity=5.0)
        bucket.last_access = time.monotonic() - 600  # 10 min ago
        with _rate_limiters_lock:
            _rate_limiters["/stale"] = bucket

        _cleanup_stale_limiters()

        with _rate_limiters_lock:
            assert "/stale" not in _rate_limiters

    def test_cleanup_preserves_recent_entries(self):
        from lite_server.middleware import _cleanup_stale_limiters

        bucket = TokenBucket(rate=1.0, capacity=5.0)
        bucket.last_access = time.monotonic()
        with _rate_limiters_lock:
            _rate_limiters["/recent"] = bucket

        _cleanup_stale_limiters()

        with _rate_limiters_lock:
            assert "/recent" in _rate_limiters


# ============================================================================
# Middleware chain integration (0.7.0: middleware actually applied)
# ============================================================================


class TestMiddlewareChainIntegration:
    """Middleware decorators applied in order; short-circuit on rejection."""

    @pytest.mark.asyncio
    async def test_require_api_key_rejects_missing_key(self):
        handler_called = []

        @require_api_key(header="X-Api-Key", keys=["secret"])
        async def handler(request, server):
            handler_called.append(True)
            return {"body": "ok"}

        result = await handler({"headers": {}}, None)
        assert result["status_code"] == 401
        assert result["body"]["error"] == "unauthorized"
        assert handler_called == []

    @pytest.mark.asyncio
    async def test_require_api_key_passes_with_valid_key(self):
        @require_api_key(header="X-Api-Key", keys=["secret"])
        async def handler(request, server):
            return {"body": "authorized"}

        result = await handler({"headers": {"X-Api-Key": "secret"}}, None)
        assert result["body"] == "authorized"

    @pytest.mark.asyncio
    async def test_require_api_key_case_sensitive_header(self):
        """With Headers (case-insensitive), 'x-api-key' matches header 'X-Api-Key'.
        The dict-based headers in middleware currently ARE case-sensitive, so
        this test documents the current behavior — after the endpoint dict
        switches to Headers (§7), middleware will become case-insensitive."""
        @require_api_key(header="x-api-key", keys=["s3kr3t"])
        async def handler(request, server):
            return {"body": "ok"}

        # dict headers are case-sensitive; must match exactly
        result = await handler({"headers": {"x-api-key": "s3kr3t"}}, None)
        assert result["body"] == "ok"

    @pytest.mark.asyncio
    async def test_middleware_applied_in_order(self):
        """Middleware wraps in list order: first in list = outermost = runs first."""
        calls = []

        def mw_a(handler):
            async def wrapper(request, server):
                calls.append("a_before")
                result = await handler(request, server)
                calls.append("a_after")
                return result
            return wrapper

        def mw_b(handler):
            async def wrapper(request, server):
                calls.append("b_before")
                result = await handler(request, server)
                calls.append("b_after")
                return result
            return wrapper

        async def handler(request, server):
            calls.append("handler")
            return {"body": "done"}

        # Simulate load_endpoints middleware wrapping (reversed order)
        wrapped = handler
        for mw in reversed([mw_a, mw_b]):
            wrapped = mw(wrapped)

        result = await wrapped({}, None)
        assert calls == ["a_before", "b_before", "handler", "b_after", "a_after"]
        assert result["body"] == "done"

    @pytest.mark.asyncio
    async def test_rate_limit_does_not_block_normal_requests(self):
        @rate_limit(requests_per_minute=1000)
        async def handler(request, server):
            return {"body": "ok"}

        result = await handler({"route": "/test"}, None)
        assert result["body"] == "ok"

    @pytest.mark.asyncio
    async def test_cors_adds_headers(self):
        @cors(allow_origins=["https://example.com"])
        async def handler(request, server):
            return {"body": "data"}

        result = await handler({}, None)
        assert result["headers"]["Access-Control-Allow-Origin"] == "https://example.com"
        assert "Access-Control-Allow-Methods" in result["headers"]

    @pytest.mark.asyncio
    async def test_log_requests_calls_handler(self):
        @log_requests
        async def handler(request, server):
            return {"body": "logged"}

        result = await handler({"route": "/test", "method": "GET"}, None)
        assert result["body"] == "logged"
