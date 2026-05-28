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

__all__ = ["serve", "LitAPI", "__version__"]
