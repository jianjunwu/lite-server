"""Custom endpoint: GET /status returns server overview.

Since 0.7.0, custom endpoints use the ``@endpoint.get(...)`` decorator and
the handler receives a :class:`RequestContext` (``ctx``) — not the legacy
``handler(request, server)`` module convention.
"""

from lite_server.endpoint import endpoint


@endpoint.get("/status")
def status(ctx):
    """Return a quick status overview of the server."""
    models = ctx.server.registry.list_loaded() if ctx.server else []
    return {
        "server": "lite-server",
        "loaded_models_count": len(models),
        "loaded_models": models,
    }
