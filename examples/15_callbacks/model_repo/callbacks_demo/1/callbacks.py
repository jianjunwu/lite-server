"""Callback examples covering the full hook surface.

Pipeline order for one request::

    on_request → decode_request → on_input → predict
    → on_output → encode_response → on_response

Six callbacks are defined:

- ApiKeyAuth:      rejects requests without a valid X-API-Key header (401)
- RequestTimer:    per-request latency via ctx.state (concurrency-safe)
- SimpleCache:     exact-match cache; ctx.respond() short-circuits on hit
- InputValidator:  async semantic validation of the decoded input (400)
- ErrorMetrics:    counts failed requests via on_error
- LifecycleTracer: logs setup/teardown via the lifecycle hooks

Key rules (since 0.7.0):
- Data hooks receive a single ``ctx`` (RequestContext) and may be sync or
  async. They may mutate ctx fields in place or return a replacement value.
- Per-request data goes in ``ctx.state`` — never in ``self`` attributes,
  which are shared across concurrent requests.
- A data hook may reject the request by raising ``HTTPException`` (or a
  subclass like BadRequestError/UnauthorizedError), or short-circuit it
  via ``ctx.respond(...)``. Exceptions from data hooks are NOT swallowed —
  they become error responses.
- Lifecycle hooks (on_before_setup / on_after_setup / on_teardown) and
  on_error are exception-isolated: failures are logged, never propagated.
"""

import json
import logging
import time
from collections import OrderedDict

from lite_server import Callback
from lite_server.exceptions import BadRequestError, UnauthorizedError

# Log via the logging module (worker forwards it to the server over stderr).
# Never print() from a callback — stdout carries the worker startup
# handshake, and writing to it corrupts the protocol.
logger = logging.getLogger("callbacks_demo")


class ApiKeyAuth(Callback):
    """Rejects requests without a valid X-API-Key header with a 401.

    Registered via ``LitAPI.callbacks`` (class attribute) because it needs
    a constructor argument — config.yaml callbacks must be no-arg
    constructible. Class-attribute callbacks run before config.yaml ones,
    so auth always precedes the cache (a cache hit never bypasses auth).

    NOTE: for production auth/rate-limit/CORS, prefer the declarative
    ``policies:`` section in config.yaml — this callback exists to teach
    the hook mechanism.
    """

    def __init__(self, keys):
        self.keys = set(keys)

    def on_request(self, ctx):
        # ctx.meta.headers is case-insensitive
        if ctx.meta.headers.get("x-api-key") not in self.keys:
            raise UnauthorizedError("missing or invalid X-API-Key header")


class RequestTimer(Callback):
    """Measures per-request latency, carried in ctx.state.

    ctx.state is a fresh dict per request — safe under concurrency, unlike
    self attributes which are shared by all requests hitting this instance.
    """

    def on_request(self, ctx):
        ctx.state["start_ns"] = time.time_ns()

    def on_response(self, ctx):
        elapsed_ms = (time.time_ns() - ctx.state["start_ns"]) / 1_000_000
        logger.info("[RequestTimer] request %s took %.2fms",
                    ctx.meta.request_id, elapsed_ms)


class SimpleCache(Callback):
    """Exact-match response cache demonstrating early return.

    on_request checks the cache and short-circuits via ``ctx.respond(...)``
    on a hit — decode/predict/encode and all later hooks are skipped.
    on_output stores the fresh output on a miss.

    The worker is a single event loop and sync hooks run on it, so a plain
    OrderedDict needs no locking here.
    """

    def __init__(self, capacity=128):
        self.capacity = capacity
        self._cache = OrderedDict()

    @staticmethod
    def _key(ctx):
        return json.dumps(ctx.request, sort_keys=True)

    def on_request(self, ctx):
        key = self._key(ctx)
        hit = self._cache.get(key)
        if hit is not None:
            self._cache.move_to_end(key)
            # Short-circuit: body plus custom response headers.
            return ctx.respond({**hit, "cached": True},
                               headers={"X-Cache": "hit"})

    def on_output(self, ctx):
        self._cache[self._key(ctx)] = ctx.output
        if len(self._cache) > self.capacity:
            self._cache.popitem(last=False)


class InputValidator(Callback):
    """Async semantic validation of the decoded input.

    Hooks may be ``async def`` — the pipeline adapts automatically.
    Raising BadRequestError rejects the request with a 400 and a
    machine-readable error body; the model's predict() never runs.
    """

    async def on_input(self, ctx):
        x = ctx.input
        if not isinstance(x, str) or not x.strip():
            raise BadRequestError("input must be a non-empty string",
                                  param="input")
        if len(x) > 1000:
            raise BadRequestError("input exceeds 1000 characters",
                                  param="input")


class ErrorMetrics(Callback):
    """Counts failed requests via on_error.

    on_error runs when the request fails (a hook or stage raised). It is
    exception-isolated: a failing on_error is logged, never masks the
    original error.
    """

    def __init__(self):
        self.failures = 0

    def on_error(self, ctx, exc):
        self.failures += 1
        logger.warning("[ErrorMetrics] request %s failed: %s: %s (total=%d)",
                       ctx.meta.request_id, type(exc).__name__, exc,
                       self.failures)


class LifecycleTracer(Callback):
    """Logs the model lifecycle around setup/teardown.

    Lifecycle hooks run outside the request path and are exception-isolated:
    failures are logged, never propagated.
    """

    def on_before_setup(self, config, device):
        logger.info("[LifecycleTracer] before setup: device=%s", device)

    def on_after_setup(self, lit_api):
        logger.info("[LifecycleTracer] setup done: %s", type(lit_api).__name__)

    def on_teardown(self, lit_api):
        logger.info("[LifecycleTracer] model unloading, teardown")
