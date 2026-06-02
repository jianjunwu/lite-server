"""lite-server: High-performance inference server."""

try:
    from importlib.metadata import version

    __version__ = version("lite-server")
except Exception:
    __version__ = "0.0.0"

try:
    from _lite_server import serve
except ImportError:
    serve = None  # fallback when extension is not built

from lite_server.api import LitAPI
from lite_server.api_async import AsyncLitAPI
from lite_server.endpoint import endpoint, router
from lite_server.middleware import cors, log_requests, rate_limit, require_api_key
from lite_server.server_proxy import ServerProxy
from lite_server.specs.openai import OpenAIEndpoint

__all__ = [
    "serve",
    "LitAPI",
    "AsyncLitAPI",
    "OpenAIEndpoint",
    "ServerProxy",
    "cors",
    "endpoint",
    "log_requests",
    "rate_limit",
    "require_api_key",
    "router",
    "__version__",
]
