"""Python custom endpoint worker for lite-server.

Handler/middleware contract::

    (request: EndpointRequest, server: ServerProxy) -> dict | Response

A returned dict carrying ``status_code`` / ``headers`` / ``stream`` /
``chunks`` keys is unpacked as a response frame; any other dict becomes
the response body with status 200.  Middleware are decorator-style
wrappers applied once at load time (list order = execution order).
"""

import argparse
import asyncio
import importlib.util
import json
import logging
import os
import socket
import struct
import sys
from contextlib import contextmanager
from pathlib import Path

from lite_server.context import Headers
from lite_server.exceptions import HTTPException

MAX_FRAME_SIZE = 16 * 1024 * 1024  # 16 MiB

# A dict result carrying any of these keys is a response frame, not plain
# data: the fields are unpacked into the wire response (§7.2-4 contract).
# "body" alone is NOT a trigger — it stays usable as a plain data key.
_RESPONSE_FRAME_TRIGGER_KEYS = ("status_code", "headers", "stream", "chunks")

logger = logging.getLogger("endpoint_worker")

# Production vs development error handling
_IS_DEV = os.environ.get("LITE_SERVER_ENV") == "development"


def _sanitize_error(e: Exception) -> str:
    """Return safe error message for client. Detailed errors go to logs only."""
    if _IS_DEV:
        return str(e)
    return "internal server error"


class _LevelPrefixFormatter(logging.Formatter):
    """Formatter that outputs [WARN] instead of [WARNING] to align with Rust stderr parser."""

    _LEVEL_MAP = {
        logging.DEBUG: "DEBUG",
        logging.INFO: "INFO",
        logging.WARNING: "WARN",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "CRITICAL",
    }

    def format(self, record):
        prefix = self._LEVEL_MAP.get(record.levelno, record.levelname)
        msg = record.getMessage()
        return f"[{prefix}] {msg}"


def setup_logging() -> logging.Logger:
    """Configure endpoint worker logging: plain text to stderr (captured by Rust).

    Returns the configured logger instance.
    """
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(_LevelPrefixFormatter())
        logger.addHandler(handler)
    return logger


def derive_port_from_path(path: str) -> int:
    """Derive a deterministic localhost port from a path string.

    Uses FNV-1a for cross-language consistency.
    Port range: 30000-59999.
    """
    hash_val = 0x811C9DC5
    for b in path.encode("utf-8"):
        hash_val ^= b
        hash_val = (hash_val * 0x01000193) & 0xFFFFFFFF
    return 30000 + (hash_val % 30000)


def create_server_socket(uds_path: str) -> socket.socket:
    """Create a server socket appropriate for the current platform.

    On Unix: AF_UNIX (Unix Domain Socket).
    On Windows: AF_INET TCP on 127.0.0.1 with port derived from uds_path.
    """
    if sys.platform == "win32":
        port = derive_port_from_path(uds_path)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", port))
    else:
        if os.path.exists(uds_path):
            os.remove(uds_path)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(uds_path)
        os.chmod(uds_path, 0o600)
    sock.listen(4)
    sock.setblocking(False)
    return sock


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-path", required=True)
    parser.add_argument("--uds-path", required=True)
    return parser.parse_args()


# Re-export the full-featured proxy from the new module
from lite_server.server_proxy import ServerProxy


def load_endpoints(repo_path: str):
    """Scan repo for endpoint modules.

    Three loading modes (in order):
      1. Subdirectory: endpoints/**/*.py (recursive) — includes EndpointSpec
         subclasses via auto-registration
      2. Decorator: collect routes from the global EndpointRouter
    """
    endpoints = {}
    repo = Path(repo_path)
    if not repo.exists():
        return endpoints

    from lite_server.specs.base import _SPEC_REGISTRY

    # ---- Mode 1: Subdirectory scan (endpoints/**/*.py) ----
    # When repo_path IS the endpoints directory (explicit endpoints_dir
    # config), scan it directly. Otherwise scan the repo/endpoints/ subdirectory.
    if any(repo.glob("*.py")):
        endpoints_dir = repo
    else:
        endpoints_dir = repo / "endpoints"
    if endpoints_dir.exists() and endpoints_dir.is_dir():
        for py_file in endpoints_dir.rglob("*.py"):
            if py_file.name.startswith("_"):
                continue
            try:
                module_name = f"ep_sub_{py_file.stem}"
                spec = importlib.util.spec_from_file_location(module_name, py_file)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)

                # Check for registered EndpointSpec subclasses
                spec_matched = False
                for spec_cls in _SPEC_REGISTRY:
                    instances = spec_cls.detect(mod)
                    for instance in instances:
                        instance.setup()
                        for route_def in instance.get_routes():
                            r = route_def["route"]
                            methods = [m.upper() for m in route_def.get("methods", ["POST"])]
                            endpoints[r] = {
                                "handler": lambda request, _srv, _ep=instance: _ep.handle(request),
                                "methods": methods,
                            }
                            spec_matched = True
                if spec_matched:
                    continue

                # Check for plain handler file (methods + handler)
                handler = getattr(mod, "handler", None)
                if handler is not None and callable(handler):
                    route = f"/{py_file.stem}"
                    methods = getattr(mod, "methods", ["GET"])
                    if isinstance(methods, str):
                        methods = [methods]
                    endpoints[route] = {
                        "handler": handler,
                        "methods": [m.upper() for m in methods],
                    }
                    continue
            except Exception as e:
                logger.error("Failed to load subdirectory endpoint %s: %s", py_file, e, exc_info=True)

    # ---- Mode 2: Decorator-registered routes ----
    try:
        from lite_server.endpoint import router as _global_router
        for route_def in _global_router.routes:
            handler = route_def.handler
            for mw in reversed(route_def.middleware):
                handler = mw(handler)
            endpoints[route_def.path] = {
                "handler": handler,
                "methods": route_def.methods,
            }
    except Exception as e:
        logger.debug("No decorator routes collected: %s", e)

    return endpoints


async def handle_request(endpoints, req_data: dict) -> dict:
    """Dispatch an endpoint request to the appropriate handler."""
    route = req_data.get("route", "")
    ep = endpoints.get(route)
    if not ep:
        return {
            "request_id": req_data.get("request_id", ""),
            "status_code": 404,
            "headers": None,
            "body": {"error": f"Endpoint {route} not found"},
        }

    handler = ep["handler"]
    server = ServerProxy(req_data.get("server_state", {}))

    # Build endpoint request with Headers (case-insensitive)
    request: dict = {
        "method": req_data.get("method", "GET"),
        "route": route,
        "headers": Headers(req_data.get("headers") or {}),
        "query": req_data.get("query", {}),
        "body": req_data.get("body"),
    }

    try:
        if asyncio.iscoroutinefunction(handler):
            result = await handler(request, server)
        else:
            result = handler(request, server)
            # Handler may return a coroutine (e.g. from async lambda wrapper)
            if asyncio.iscoroutine(result):
                result = await result

        # Detect Response objects from lite_server.response
        from lite_server.response import Response as LiteResponse
        if isinstance(result, LiteResponse):
            resp_headers = dict(result.headers) if result.headers else {}
            if result.media_type and result.media_type != "application/json":
                resp_headers["content-type"] = result.media_type
            return {
                "request_id": req_data.get("request_id", ""),
                "status_code": result.status_code,
                "headers": resp_headers if resp_headers else None,
                "body": result.content,
            }

        if not isinstance(result, dict):
            result = {"data": result}

        if any(k in result for k in _RESPONSE_FRAME_TRIGGER_KEYS):
            # Response frame: unpack fields instead of nesting the whole
            # dict into the body.  Middleware short-circuits (401/429),
            # EndpointSpec frames, and streaming frames all take this path.
            # Without a "body" key the remaining non-frame keys are the body
            # (e.g. cors annotating a plain data dict).
            resp = {
                "request_id": req_data.get("request_id", ""),
                "status_code": result.get("status_code", 200),
                "headers": result.get("headers"),
                "body": result.get(
                    "body",
                    {
                        k: v
                        for k, v in result.items()
                        if k not in _RESPONSE_FRAME_TRIGGER_KEYS
                        and k != "request_id"
                    },
                ),
            }
            if result.get("stream"):
                resp["stream"] = True
                resp["chunks"] = result.get("chunks", [])
            return resp

        return {
            "request_id": req_data.get("request_id", ""),
            "status_code": 200,
            "headers": None,
            "body": result,
        }
    except HTTPException as e:
        logger.info("endpoint HTTP error for %s: %s (status=%d, type=%s)",
                    route, e.detail, e.status_code, e.error_type)
        # Four-field error body contract — same shape as the inference path.
        error_body = {
            "type": e.error_type,
            "message": e.detail,
            "code": e.code,
            "param": e.param,
        }
        return {
            "request_id": req_data.get("request_id", ""),
            "status_code": e.status_code,
            "headers": None,
            "body": {"error": error_body},
        }
    except Exception as e:
        logger.error("handler error for %s: %s", route, e, exc_info=True)
        return {
            "request_id": req_data.get("request_id", ""),
            "status_code": 500,
            "headers": None,
            "body": {"error": _sanitize_error(e)},
        }


@contextmanager
def _protect_stdout():
    """Redirect fd 1 (stdout) to stderr during endpoint loading.

    Endpoint modules may import inference frameworks (CANN, ONNX Runtime,
    MagicMind, etc.) whose C-level init writes to fd 1, polluting the
    stdout channel used for the worker-ready handshake.
    """
    saved = os.dup(1)
    try:
        os.dup2(2, 1)
        yield
    finally:
        os.dup2(saved, 1)
        os.close(saved)


async def worker_main():
    setup_logging()
    args = parse_args()

    # Load endpoints
    with _protect_stdout():
        endpoints = load_endpoints(args.repo_path)
    routes = [{"route": r, "methods": ep["methods"]} for r, ep in endpoints.items()]

    # Send startup signal with protocol version
    startup = {
        "status": "ready",
        "routes": routes,
        "protocol_version": "v0",  # JSON only for now; v1 = Protobuf
    }
    print(json.dumps(startup), flush=True)

    # Create server socket (AF_UNIX on Unix, AF_INET TCP on Windows)
    uds_path = args.uds_path
    server = create_server_socket(uds_path)

    loop = asyncio.get_running_loop()

    try:
        while True:
            try:
                conn, _ = await loop.sock_accept(server)
            except Exception as e:
                logger.debug("accept failed: %s", e)
                continue

            asyncio.create_task(handle_connection(conn, endpoints))
    finally:
        server.close()


async def _send_frame(loop, conn, data: dict):
    """Send a length-prefixed JSON frame over the connection."""
    payload = json.dumps(data).encode("utf-8")
    len_prefix = struct.pack(">I", len(payload))
    await loop.sock_sendall(conn, len_prefix + payload)


async def handle_connection(conn, endpoints):
    """Handle a single connection."""
    loop = asyncio.get_running_loop()
    try:
        while True:
            # Read length prefix (4 bytes big-endian)
            len_bytes = b""
            while len(len_bytes) < 4:
                chunk = await loop.sock_recv(conn, 4 - len(len_bytes))
                if not chunk:
                    break
                len_bytes += chunk
            if len(len_bytes) < 4:
                break

            msg_len = struct.unpack(">I", len_bytes)[0]

            if msg_len > MAX_FRAME_SIZE:
                logger.warning("frame too large: %d bytes", msg_len)
                break

            # Read message body
            body = b""
            while len(body) < msg_len:
                chunk = await loop.sock_recv(conn, msg_len - len(body))
                if not chunk:
                    break
                body += chunk
            if len(body) < msg_len:
                break

            # Parse request
            try:
                req_data = json.loads(body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning("invalid request payload: %s", e)
                continue

            # Handle request
            response = await handle_request(endpoints, req_data)

            if response.get("stream"):
                # Streaming: send header, then each chunk, then done
                header = {
                    "request_id": response.get("request_id", ""),
                    "status_code": response.get("status_code", 200),
                    "stream": True,
                }
                await _send_frame(loop, conn, header)

                for chunk in response.get("chunks", []):
                    await _send_frame(loop, conn, chunk)

                await _send_frame(loop, conn, {"type": "done"})
            else:
                # Single response
                await _send_frame(loop, conn, response)
    except Exception as e:
        logger.error("connection handler error: %s", e, exc_info=True)
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(worker_main())
