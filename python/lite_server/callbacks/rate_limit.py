"""Rate-limit policy callback with a local token-bucket fallback."""

import math
import threading
import time

from lite_server.callbacks._base import Callback
from lite_server.callbacks._internal import _rust_managed


class _TokenBucket:
    """Thread-safe token bucket for local rate limiting fallback."""

    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()
        self.last_access = self.last_update
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> tuple[bool, float]:
        """Try to take *tokens*. Returns ``(allowed, wait_secs)``.

        ``wait_secs`` is the time until enough tokens have refilled for this
        request, computed UNDER the lock so Retry-After callers don't read a
        stale ``tokens``/``rate`` snapshot after the lock released (C2).
        """
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now
            self.last_access = now
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True, 0.0
            wait = 0.0 if self.rate <= 0 else (tokens - self.tokens) / self.rate
            return False, wait


class RateLimit(Callback):
    """Rate-limit policy declaration. Executed in the Rust HTTP layer.

    When the process runs outside a Rust-managed worker (unit tests, local
    dev), falls back to a per-instance local bucket set sharded by ``key``.
    The fallback is per-process and does NOT share state — it exists so the
    policy is testable, not as a production limiter.
    """

    _MAX_BUCKETS = 500

    def __init__(
        self,
        *,
        requests_per_minute: int = 60,
        key: str = "route",
        burst: float | None = None,
    ):
        if key not in ("route", "ip"):
            raise ValueError(f"RateLimit key must be 'route' or 'ip', got {key!r}")
        if requests_per_minute <= 0:
            raise ValueError(
                f"RateLimit requests_per_minute must be > 0, got {requests_per_minute}"
            )
        if burst is not None and burst <= 0:
            raise ValueError(f"RateLimit burst must be > 0 when set, got {burst}")
        self.requests_per_minute = requests_per_minute
        self.key = key
        self.burst = float(burst) if burst is not None else requests_per_minute * 1.5
        self._managed = _rust_managed()
        self._buckets: dict[str, _TokenBucket] = {}
        self._lock = threading.Lock()

    def on_request(self, ctx):
        from lite_server.exceptions import HTTPException

        if self._managed:
            return  # Rust 已执行；本实例仅为声明
        bucket_key = ctx.meta.client_ip if self.key == "ip" else ctx.meta.route
        with self._lock:
            bucket = self._buckets.get(bucket_key)
            if bucket is None:
                bucket = _TokenBucket(
                    rate=self.requests_per_minute / 60.0, capacity=self.burst
                )
                self._buckets[bucket_key] = bucket
                if len(self._buckets) > self._MAX_BUCKETS:
                    self._evict_stale()
        allowed, wait = bucket.acquire()
        if not allowed:
            # wait was snapshotted under the bucket lock (C2); C1 guarantees
            # rate > 0, so a rejection always yields a positive finite wait.
            retry = max(1, math.ceil(wait)) if wait > 0 else 1
            raise HTTPException(
                429, "rate limit exceeded",
                error_type="rate_limit_exceeded",
                headers={"Retry-After": str(retry)},
            )

    def _evict_stale(self) -> None:
        now = time.monotonic()
        stale = [k for k, b in self._buckets.items() if now - b.last_access > 300]
        for k in stale:
            self._buckets.pop(k, None)
