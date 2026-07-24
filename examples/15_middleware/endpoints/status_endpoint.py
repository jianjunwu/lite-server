"""Custom endpoint with callback chain: auth + rate limit + CORS + logging.

Demonstrates the unified callback API on a custom endpoint route.
Since 0.7.0: callbacks replace middleware, handlers receive a RequestContext.
"""

from lite_server import Cors, LogRequests, RateLimit, RequireApiKey
from lite_server.endpoint import endpoint

VALID_KEYS = ["secret-api-key-123", "demo-key-456"]


@endpoint.get(
    "/status",
    callbacks=[
        RequireApiKey(header="X-API-Key", keys=VALID_KEYS),
        RateLimit(requests_per_minute=10),
        Cors(allow_origins=["*"]),
        LogRequests(),
    ],
)
def status_handler(ctx):
    """Protected status endpoint: returns server info.

    Requires X-API-Key header, rate-limited to 10 req/min.
    """
    models = ctx.server.registry.list_models() if ctx.server else []
    return {
        "server": "lite-server",
        "loaded_models": models,
        "endpoint": "status (callback-protected)",
        "request_id": ctx.meta.request_id,
    }


@endpoint.get(
    "/public",
    callbacks=[Cors(allow_origins=["*"])],
)
def public_handler(ctx):
    """Public endpoint: no auth or rate limiting, just CORS."""
    return {
        "message": "this endpoint is public",
        "request_id": ctx.meta.request_id,
    }
