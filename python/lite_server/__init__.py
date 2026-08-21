"""lite-server: High-performance inference server."""

try:
    from importlib.metadata import version

    # 0.8.3 起 PyPI 包名改为 miraserver;lite-server 是老安装名,兼容保留。
    try:
        __version__ = version("miraserver")
    except Exception:
        __version__ = version("lite-server")
except Exception:
    __version__ = "0.0.0"

_EXTENSION_EXPORTS = (
    "serve",
    "stop_server",
    "validate_model_config",
    "validate_server_config",
)


def __getattr__(name):
    """Lazy-load the native extension on first access (PEP 562).

    ``_lite_server`` is a ~100MB un stripped debug dylib in dev builds; every
    inference worker imports ``lite_server`` for ``LitAPI`` but never calls
    ``serve`` — an eager import here made every worker spawn pay the dyld
    load/rebase/bind cost for nothing. ``from lite_server import serve``
    keeps working identically for callers that need it.
    """
    if name in _EXTENSION_EXPORTS:
        try:
            import _lite_server
        except ImportError:
            return None  # fallback when extension is not built
        return getattr(_lite_server, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


from lite_server.api import BidiStreamHandler, LitAPI
from lite_server.callbacks import Callback
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
    encode_tensor,
)
from lite_server.server_proxy import ServerProxy
from lite_server.ensemble import (
    DagSet,
    EnsembleDAG,
    InputDecl,
    Step,
    StepOutput,
)

__all__ = [
    "serve",
    "stop_server",
    "validate_server_config",
    "validate_model_config",
    "LitAPI",
    "BidiStreamHandler",
    "Callback",
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
    "encode_tensor",
    "Headers",
    # E9-A: declarative ensemble DAG authoring (declaration only)
    "EnsembleDAG",
    "Step",
    "InputDecl",
    "StepOutput",
    "DagSet",
]
