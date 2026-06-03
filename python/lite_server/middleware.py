"""Middleware chain for lite-server endpoints.

Provides reusable middleware decorators for authentication, rate limiting,
logging, and CORS.
"""

from __future__ import annotations

import functools
import threading
import time
from typing import Any, Callable, List, Optional

_MAX_RATE_LIMITER_ENTRIES = 500
_RATE_LIMITER_CLEANUP_INTERVAL = 300  # 5 minutes


class TokenBucket:
    """Thread-safe in-memory token bucket for rate limiting."""

    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()
        self.last_access = self.last_update
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> bool:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now
            self.last_access = now
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False


# Global rate limiter storage: route -> TokenBucket
_rate_limiters: dict[str, TokenBucket] = {}
_rate_limiters_lock = threading.Lock()


def rate_limit(requests_per_minute: int = 60):
    """Rate limit middleware. Returns 429 when limit exceeded.

    Thread-safe: _rate_limiters is protected by a lock.
    Auto-cleans expired entries when size exceeds _MAX_RATE_LIMITER_ENTRIES.
    """

    def decorator(handler: Callable) -> Callable:
        @functools.wraps(handler)
        async def wrapper(request: dict, server: Any) -> dict:
            route = request.get("route", "")
            with _rate_limiters_lock:
                limiter = _rate_limiters.get(route)
                if limiter is None:
                    limiter = TokenBucket(
                        rate=requests_per_minute / 60.0,
                        capacity=float(requests_per_minute),
                    )
                    _rate_limiters[route] = limiter
                    # Clean up stale entries when exceeding max size
                    if len(_rate_limiters) > _MAX_RATE_LIMITER_ENTRIES:
                        _cleanup_stale_limiters()
            if not limiter.acquire():
                return {
                    "status_code": 429,
                    "headers": {"Retry-After": "60"},
                    "body": {"error": "rate limit exceeded"},
                }
            return await _maybe_async(handler, request, server)

        return wrapper

    return decorator


def _cleanup_stale_limiters():
    """Remove TokenBucket entries that haven't been accessed recently."""
    now = time.monotonic()
    stale = [
        route
        for route, bucket in _rate_limiters.items()
        if now - bucket.last_access > _RATE_LIMITER_CLEANUP_INTERVAL
    ]
    for route in stale:
        _rate_limiters.pop(route, None)


def require_api_key(header: str = "X-API-Key", keys: Optional[List[str]] = None):
    """Require a valid API key in the specified header."""
    valid_keys = set(keys or [])

    def decorator(handler: Callable) -> Callable:
        @functools.wraps(handler)
        async def wrapper(request: dict, server: Any) -> dict:
            headers = request.get("headers", {})
            provided = headers.get(header, "")
            if not provided or (valid_keys and provided not in valid_keys):
                return {
                    "status_code": 401,
                    "body": {"error": "unauthorized"},
                }
            return await _maybe_async(handler, request, server)

        return wrapper

    return decorator


def log_requests(handler: Callable) -> Callable:
    """Log request/response timing."""

    @functools.wraps(handler)
    async def wrapper(request: dict, server: Any) -> dict:
        import logging

        logger = logging.getLogger("endpoint.middleware")
        route = request.get("route", "")
        method = request.get("method", "GET")
        start = time.monotonic()
        result = await _maybe_async(handler, request, server)
        elapsed = (time.monotonic() - start) * 1000
        status = result.get("status_code", 200) if isinstance(result, dict) else 200
        logger.debug("%s %s -> %d in %.2fms", method, route, status, elapsed)
        return result

    return wrapper


def cors(
    allow_origins: Optional[List[str]] = None,
    allow_methods: Optional[List[str]] = None,
    allow_headers: Optional[List[str]] = None,
):
    """Attach CORS headers to responses."""
    origins = ", ".join(allow_origins or ["*"])
    methods = ", ".join(allow_methods or ["GET", "POST", "PUT", "DELETE", "OPTIONS"])
    headers = ", ".join(allow_headers or ["Content-Type", "Authorization"])

    def decorator(handler: Callable) -> Callable:
        @functools.wraps(handler)
        async def wrapper(request: dict, server: Any) -> dict:
            result = await _maybe_async(handler, request, server)
            if isinstance(result, dict):
                result.setdefault("headers", {})
                result["headers"]["Access-Control-Allow-Origin"] = origins
                result["headers"]["Access-Control-Allow-Methods"] = methods
                result["headers"]["Access-Control-Allow-Headers"] = headers
            return result

        return wrapper

    return decorator


async def _maybe_async(handler: Callable, request: dict, server: Any) -> Any:
    """Call handler, awaiting if it's a coroutine."""
    import asyncio

    if asyncio.iscoroutinefunction(handler):
        return await handler(request, server)
    result = handler(request, server)
    if asyncio.iscoroutine(result):
        return await result
    return result
