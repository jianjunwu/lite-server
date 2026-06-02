"""Middleware chain for lite-server endpoints.

Provides reusable middleware decorators for authentication, rate limiting,
logging, and CORS.
"""

from __future__ import annotations

import functools
import time
from typing import Any, Callable, Dict, List, Optional


class TokenBucket:
    """Simple in-memory token bucket for rate limiting."""

    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()

    def acquire(self, tokens: float = 1.0) -> bool:
        now = time.monotonic()
        elapsed = now - self.last_update
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_update = now
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False


# Global rate limiter storage: route -> TokenBucket
_rate_limiters: Dict[str, TokenBucket] = {}


def rate_limit(requests_per_minute: int = 60):
    """Rate limit middleware. Returns 429 when limit exceeded."""

    def decorator(handler: Callable) -> Callable:
        @functools.wraps(handler)
        async def wrapper(request: dict, server: Any) -> dict:
            route = request.get("route", "")
            limiter = _rate_limiters.setdefault(
                route,
                TokenBucket(rate=requests_per_minute / 60.0, capacity=requests_per_minute),
            )
            if not limiter.acquire():
                return {
                    "status_code": 429,
                    "headers": {"Retry-After": "60"},
                    "body": {"error": "rate limit exceeded"},
                }
            return await _maybe_async(handler, request, server)

        return wrapper

    return decorator


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
        logger.info("%s %s -> %d in %.2fms", method, route, status, elapsed)
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
