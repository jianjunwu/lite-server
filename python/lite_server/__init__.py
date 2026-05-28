"""lite-server: High-performance inference server."""

try:
    from _lite_server import serve
except ImportError:
    serve = None  # fallback when extension is not built

from lite_server.api import LitAPI

__version__ = "0.1.0"

__all__ = ["serve", "LitAPI", "__version__"]
