"""Callbacks demo: a model-level chain guarding every route of the model.

The ``callbacks`` class attribute attaches an auth + rate-limit + CORS +
logging chain to the whole model — it runs for standard inference
(``/v2/models/protected/infer``) and for custom ``@route`` handlers
(``/v2/models/protected/status``) alike. There is no per-route chain:
policies that belong at the gateway stay at the gateway.
"""

from lite_server import (
    Cors,
    LitAPI,
    LogRequests,
    RateLimit,
    RequireApiKey,
    route,
)

VALID_KEYS = ["secret-api-key-123", "demo-key-456"]


class ProtectedAPI(LitAPI):
    # Global chain: auth first (reject early), then rate limit, CORS headers,
    # and request logging. Applies to inference AND custom routes.
    callbacks = (
        RequireApiKey(header="X-API-Key", keys=VALID_KEYS),
        RateLimit(requests_per_minute=10),
        Cors(allow_origins=["*"]),
        LogRequests(),
    )

    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request.get("input", "")

    def predict(self, x):
        return {"output": f"protected: {x}"}

    @route.get("/status")
    def status(self, ctx):
        """Custom route — guarded by the same callback chain as inference."""
        models = ctx.server.registry.list_loaded() if ctx.server else []
        return {
            "server": "lite-server",
            "loaded_models": models,
            "request_id": ctx.meta.request_id,
        }
