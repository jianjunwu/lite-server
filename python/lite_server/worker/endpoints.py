"""Python custom endpoint worker for lite-server."""

import argparse
import asyncio
import importlib.util
import json
import os
import socket
import struct
import sys
import time
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-path", required=True)
    parser.add_argument("--uds-path", required=True)
    return parser.parse_args()


class RegistryProxy:
    """Proxy for server.registry compatible with light-server."""

    def __init__(self, snapshot):
        self._snapshot = snapshot

    def list_loaded(self):
        return self._snapshot.get("loaded_models", [])


class ServerProxy:
    """Proxy for server object compatible with light-server."""

    def __init__(self, snapshot):
        self._snapshot = snapshot

    @property
    def registry(self):
        return RegistryProxy(self._snapshot)

    @property
    def config(self):
        return self._snapshot.get("config", {})


def load_endpoints(repo_path: str):
    """Scan repo root for *_endpoint.py and load them."""
    endpoints = {}
    repo = Path(repo_path)
    if not repo.exists():
        return endpoints

    for py_file in repo.glob("*_endpoint.py"):
        stem = py_file.stem
        if not stem.endswith("_endpoint"):
            continue
        route = stem[:-9]  # strip "_endpoint"

        try:
            spec = importlib.util.spec_from_file_location(f"ep_{route}", py_file)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

            handler = getattr(mod, "handler", None)
            methods = getattr(mod, "methods", ["GET"])
            if handler is None:
                continue

            endpoints[f"/{route}"] = {
                "handler": handler,
                "methods": [m.upper() for m in methods],
            }
        except Exception as e:
            print(json.dumps({
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "level": "ERROR",
                "message": f"Failed to load endpoint {py_file}: {e}",
            }), file=sys.stderr, flush=True)

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

    # Build a simple request object
    request = {
        "method": req_data.get("method", "GET"),
        "route": route,
        "headers": req_data.get("headers", {}),
        "query": req_data.get("query", {}),
        "body": req_data.get("body"),
    }

    try:
        if asyncio.iscoroutinefunction(handler):
            result = await handler(request, server)
        else:
            result = handler(request, server)

        if not isinstance(result, dict):
            result = {"data": result}

        return {
            "request_id": req_data.get("request_id", ""),
            "status_code": 200,
            "headers": None,
            "body": result,
        }
    except Exception as e:
        return {
            "request_id": req_data.get("request_id", ""),
            "status_code": 500,
            "headers": None,
            "body": {"error": str(e)},
        }


async def worker_main():
    args = parse_args()

    # Load endpoints
    endpoints = load_endpoints(args.repo_path)
    routes = [{"route": r, "methods": ep["methods"]} for r, ep in endpoints.items()]

    # Send startup signal
    startup = {
        "status": "ready",
        "routes": routes,
    }
    print(json.dumps(startup), flush=True)

    # Create Unix socket server
    uds_path = args.uds_path
    if os.path.exists(uds_path):
        os.remove(uds_path)

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(uds_path)
    server.listen(4)
    server.setblocking(False)

    loop = asyncio.get_event_loop()

    while True:
        try:
            conn, _ = await loop.sock_accept(server)
        except Exception:
            continue

        asyncio.create_task(handle_connection(conn, endpoints))


async def handle_connection(conn, endpoints):
    """Handle a single UDS connection."""
    loop = asyncio.get_event_loop()
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
            except Exception:
                continue

            # Handle request
            response = await handle_request(endpoints, req_data)

            # Send response
            resp_bytes = json.dumps(response).encode("utf-8")
            len_prefix = struct.pack(">I", len(resp_bytes))
            await loop.sock_sendall(conn, len_prefix + resp_bytes)
    except Exception:
        pass
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(worker_main())
