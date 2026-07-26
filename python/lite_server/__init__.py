"""lite-server: High-performance inference server."""

try:
    from importlib.metadata import version

    __version__ = version("lite-server")
except Exception:
    __version__ = "0.0.0"

try:
    from _lite_server import serve, validate_model_config, validate_server_config
except ImportError:
    serve = None  # fallback when extension is not built
    validate_server_config = None
    validate_model_config = None

from lite_server.api import BidiStreamHandler, LitAPI
from lite_server.callbacks import (
    Callback,
    Cors,
    LogRequests,
    RateLimit,
    RequireApiKey,
)
from lite_server.context import CBSequence, Headers, RequestContext, RequestMeta
from lite_server.route import RouteRequest, route
from lite_server.response import (
    BackgroundTask,
    FileResponse,
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from lite_server.server_proxy import ServerProxy

__all__ = [
    "serve",
    "validate_server_config",
    "validate_model_config",
    "LitAPI",
    "BidiStreamHandler",
    "Callback",
    "Cors",
    "LogRequests",
    "RateLimit",
    "RequireApiKey",
    "RequestContext",
    "RequestMeta",
    "CBSequence",
    "RouteRequest",
    "ServerProxy",
    "route",
    "__version__",
    # HTTP request / response
    "Response",
    "JSONResponse",
    "HTMLResponse",
    "PlainTextResponse",
    "RedirectResponse",
    "FileResponse",
    "StreamingResponse",
    "BackgroundTask",
    "Headers",
]
