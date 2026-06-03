"""Custom endpoint with middleware chain: auth + rate limit + CORS + logging.

Demonstrates stacking middleware decorators on a custom endpoint route.
"""

from lite_server.endpoint import endpoint
from lite_server.middleware import cors, log_requests, rate_limit, require_api_key

VALID_KEYS = ["secret-api-key-123", "demo-key-456"]


@endpoint.get(
    "/status",
    middleware=[
        log_requests,
        require_api_key(header="X-API-Key", keys=VALID_KEYS),
        rate_limit(requests_per_minute=10),
        cors(allow_origins=["*"]),
    ],
)
async def status_handler(request, server):
    """Protected status endpoint: returns server info.

    Requires X-API-Key header, rate-limited to 10 req/min.
    """
    return {
        "server": "lite-server",
        "loaded_models": server.registry.list_models() if hasattr(server, "registry") else [],
        "endpoint": "status (middleware-protected)",
    }


@endpoint.get(
    "/public",
    middleware=[cors(allow_origins=["*"])],
)
async def public_handler(request, server):
    """Public endpoint: no auth or rate limiting, just CORS."""
    return {"message": "this endpoint is public"}
