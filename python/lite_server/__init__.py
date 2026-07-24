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

from lite_server.api import BidiStreamHandler, LitAPI
from lite_server.callback import Callback
from lite_server.context import CBSequence, Headers, RequestContext, RequestMeta
from lite_server.endpoint import endpoint, router
from lite_server.middleware import cors, log_requests, rate_limit, require_api_key
from lite_server.request import Client, QueryParams, Request, State, URL, UploadFile
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
from lite_server.specs.openai import OpenAIEndpoint
from lite_server.specs.base import EndpointSpec

__all__ = [
    "serve",
    "LitAPI",
    "BidiStreamHandler",
    "Callback",
    "RequestContext",
    "RequestMeta",
    "CBSequence",
    "OpenAIEndpoint",
    "EndpointSpec",
    "ServerProxy",
    "cors",
    "endpoint",
    "log_requests",
    "rate_limit",
    "require_api_key",
    "router",
    "__version__",
    # HTTP request / response
    "Request",
    "Response",
    "JSONResponse",
    "HTMLResponse",
    "PlainTextResponse",
    "RedirectResponse",
    "FileResponse",
    "StreamingResponse",
    "BackgroundTask",
    "URL",
    "Headers",
    "QueryParams",
    "Client",
    "State",
    "UploadFile",
]
