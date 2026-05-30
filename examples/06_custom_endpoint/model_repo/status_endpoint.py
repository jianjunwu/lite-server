"""Custom endpoint: GET /status returns server overview."""

methods = ["GET"]


def handler(request, server):
    """Return a quick status overview of the server."""
    models = server.registry.list_loaded()
    return {
        "server": "lite-server",
        "loaded_models_count": len(models),
    }
