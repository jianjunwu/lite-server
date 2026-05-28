"""Python inference worker entry point for lite-server."""

import argparse
import asyncio
import json
import os
import socket
import struct
import sys
import time
from pathlib import Path

import yaml


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--model-py", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--device", required=True)
    parser.add_argument("--worker-id", type=int, required=True)
    parser.add_argument("--uds-path", required=True)
    return parser.parse_args()


def load_model_config(config_path: str):
    """Load model config from YAML."""
    if not os.path.exists(config_path):
        return {}
    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}


def load_litapi(model_py_path: str, config: dict):
    """Dynamically load LitAPI class from model.py."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("model_module", model_py_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Find LitAPI subclass
    LitAPIClass = None
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if (
            isinstance(attr, type)
            and attr_name != "LitAPI"
            and hasattr(attr, "predict")
        ):
            # Simple heuristic: look for class with predict method
            LitAPIClass = attr
            break

    if LitAPIClass is None:
        raise RuntimeError(f"No LitAPI subclass found in {model_py_path}")

    # Instantiate with config
    max_batch_size = config.get("max_batch_size", 1)
    batch_timeout = config.get("batch_timeout", 0.0)
    stream = config.get("stream", False)
    api_path = config.get("api_path", "/predict")

    instance = LitAPIClass(
        max_batch_size=max_batch_size,
        batch_timeout=batch_timeout,
        api_path=api_path,
        stream=stream,
    )

    # Set config and call pre_setup if available
    instance.config = config
    if hasattr(instance, "pre_setup"):
        instance.pre_setup()

    # Call setup if available (with device)
    if hasattr(instance, "setup"):
        device = config.get("accelerator", "cpu")
        if isinstance(device, list):
            device = device[0] if device else "cpu"
        instance.setup(device)

    return instance


class _RequestState:
    """Internal state for a single request in continuous batching."""

    def __init__(self, uid: str, input_data, stream: bool = False):
        self.uid = uid
        self.input = input_data
        self.state = None
        self.output = None
        self.stream = stream
        self.finished = False


class ContinuousBatchingLoop:
    """Continuous batching loop for iterative model inference.

    Manages a set of active requests, calling ``predict_step()`` repeatedly
    until all requests report completion.  If ``predict_step`` is not
    implemented by the loaded model, falls back to a single-shot ``predict``
    call that completes immediately.
    """

    def __init__(self, lit_api, model_name: str = "unknown", worker_id: int = 0):
        self.lit_api = lit_api
        self.model_name = model_name
        self.worker_id = worker_id
        self.active: dict[str, _RequestState] = {}

    def add_request(self, request: dict) -> str:
        """Decode and add a new request to the active batch."""
        uid = request.get("uid", "")
        payload = request.get("payload", {})
        data = payload.get("data", {})

        if hasattr(self.lit_api, "decode_request") and callable(
            getattr(self.lit_api, "decode_request")
        ):
            input_data = self.lit_api.decode_request(data)
        else:
            input_data = data

        stream = payload.get("stream", False)
        self.active[uid] = _RequestState(uid, input_data, stream=stream)
        return uid

    def step(self) -> list[dict]:
        """Run one generation step for all active requests.

        Returns a list of response dicts for requests that have produced
        output this step (finished requests or streaming chunks).
        """
        if not self.active:
            return []

        uids = list(self.active.keys())
        inputs = []
        states = []
        for uid in uids:
            req = self.active[uid]
            inputs.append(req.input)
            states.append(req.state)

        # Use predict_step if available; otherwise fallback to predict.
        has_step = hasattr(self.lit_api, "predict_step") and callable(
            getattr(self.lit_api, "predict_step")
        )
        if has_step:
            outputs, new_states, dones = self.lit_api.predict_step(inputs, states)
        else:
            outputs = []
            new_states = [None] * len(inputs)
            dones = []
            for inp in inputs:
                outputs.append(self.lit_api.predict(inp))
                dones.append(True)

        # Pad to expected length defensively.
        n = len(inputs)
        if len(outputs) < n:
            outputs.extend([None] * (n - len(outputs)))
        if len(new_states) < n:
            new_states.extend([None] * (n - len(new_states)))
        if len(dones) < n:
            dones.extend([True] * (n - len(dones)))

        completed = []
        has_encode = hasattr(self.lit_api, "encode_response") and callable(
            getattr(self.lit_api, "encode_response")
        )

        for i, uid in enumerate(uids):
            req = self.active[uid]
            req.output = outputs[i]
            req.state = new_states[i]

            if dones[i]:
                req.finished = True
                encoded = self.lit_api.encode_response(req.output) if has_encode else req.output
                completed.append({
                    "uid": uid,
                    "data": encoded if isinstance(encoded, dict) else {"output": encoded},
                    "status": {"code": "Ok"},
                    "worker_id": self.worker_id,
                })
                del self.active[uid]
            elif req.stream:
                encoded = self.lit_api.encode_response(req.output) if has_encode else req.output
                completed.append({
                    "uid": uid,
                    "data": encoded if isinstance(encoded, dict) else {"output": encoded},
                    "status": {"code": "Streaming"},
                    "worker_id": self.worker_id,
                })

        return completed


def pick_loop(config: dict):
    """Select inference loop based on config."""
    if config.get("bidirectional", False):
        return "bidirectional"
    if config.get("continuous_batching", False):
        return "continuous"
    if config.get("max_batch_size", 1) > 1:
        return "batched"
    return "single"


def _make_metrics(model_name: str, duration: float, batch_size: int) -> list:
    """Build worker metric report compatible with light-server."""
    return [
        {
            "name": "lightserver_inference_duration_seconds",
            "value": duration,
            "labels": {"model": model_name},
            "type": "histogram",
        },
        {
            "name": "lightserver_batch_size",
            "value": batch_size,
            "labels": {"model": model_name},
            "type": "histogram",
        },
    ]


async def handle_request(lit_api, request: dict, model_name: str = "unknown") -> dict:
    """Handle a single inference request."""
    payload = request["payload"]
    msg_type = payload.get("type", payload.get("msg_type", "INFER"))

    if msg_type == "INFER":
        data = payload.get("data", {})
        # Call decode_request if available
        if hasattr(lit_api, "decode_request"):
            input_data = lit_api.decode_request(data)
        else:
            input_data = data

        # Call predict method
        if hasattr(lit_api, "predict"):
            start = time.time()
            result = lit_api.predict(input_data)
            duration = time.time() - start

            # Call encode_response if available
            if hasattr(lit_api, "encode_response"):
                result = lit_api.encode_response(result)

            return {
                "uid": request["uid"],
                "data": result if isinstance(result, dict) else {"output": result},
                "status": {"code": "Ok"},
                "worker_id": request.get("worker_id", 0),
                "metrics": _make_metrics(model_name, duration, 1),
            }
        else:
            return {
                "uid": request["uid"],
                "data": None,
                "status": {"code": "Error", "message": "predict method not found"},
                "worker_id": request.get("worker_id", 0),
            }

    elif msg_type == "STREAM_OPEN":
        stream_id = payload.get("stream_id", "")
        if hasattr(lit_api, "stream_open"):
            lit_api.stream_open(stream_id)
        return {
            "uid": request["uid"],
            "data": {"stream_id": stream_id, "status": "opened"},
            "status": {"code": "Ok"},
            "worker_id": request.get("worker_id", 0),
        }

    elif msg_type == "STREAM_CHUNK":
        stream_id = payload.get("stream_id", "")
        chunk = payload.get("chunk", {})
        if hasattr(lit_api, "stream_chunk"):
            result = lit_api.stream_chunk(stream_id, chunk)
            return {
                "uid": request["uid"],
                "data": result if isinstance(result, dict) else {"output": result},
                "status": {"code": "Streaming"},
                "worker_id": request.get("worker_id", 0),
            }
        return {
            "uid": request["uid"],
            "data": None,
            "status": {"code": "FinishStreaming"},
            "worker_id": request.get("worker_id", 0),
        }

    elif msg_type == "STREAM_CLOSE":
        stream_id = payload.get("stream_id", "")
        if hasattr(lit_api, "stream_close"):
            lit_api.stream_close(stream_id)
        return {
            "uid": request["uid"],
            "data": None,
            "status": {"code": "FinishStreaming"},
            "worker_id": request.get("worker_id", 0),
        }

    elif msg_type == "STREAM_CANCEL":
        stream_id = payload.get("stream_id", "")
        if hasattr(lit_api, "stream_cancel"):
            lit_api.stream_cancel(stream_id)
        return {
            "uid": request["uid"],
            "data": None,
            "status": {"code": "FinishStreaming"},
            "worker_id": request.get("worker_id", 0),
        }

    else:
        return {
            "uid": request["uid"],
            "data": None,
            "status": {"code": "Error", "message": f"Unknown msg_type: {msg_type}"},
            "worker_id": request.get("worker_id", 0),
        }


def handle_batch_request(lit_api, request: dict, model_name: str = "unknown") -> dict:
    """Handle a batch inference request.

    Args:
        lit_api: The loaded model instance.
        request: Dict with key "items" or "payload" containing list of {"uid": str, "data": any}.
        model_name: Model name for metrics labels.

    Returns:
        Dict with key "type": "BATCH_RESPONSE" and "items" list of results.
    """
    # Support both direct call {"items": [...]} and full request {"payload": {"items": [...]}}
    payload = request.get("payload", request)
    items = payload.get("items", [])
    if not items:
        return {"type": "BATCH_RESPONSE", "items": []}

    # Check predict exists upfront
    if not hasattr(lit_api, "predict") or not callable(getattr(lit_api, "predict")):
        return {
            "type": "BATCH_RESPONSE",
            "items": [
                {
                    "uid": item["uid"],
                    "data": None,
                    "status": {"code": "Error", "message": "predict method not found"},
                }
                for item in items
            ],
        }

    try:
        # 1. Decode each request
        decoded = []
        for item in items:
            data = item.get("data", {})
            if hasattr(lit_api, "decode_request") and callable(
                getattr(lit_api, "decode_request")
            ):
                decoded.append(lit_api.decode_request(data))
            else:
                decoded.append(data)

        # 2. Batch if available
        has_batch = hasattr(lit_api, "batch") and callable(getattr(lit_api, "batch"))
        if has_batch and len(decoded) > 0:
            batched_input = lit_api.batch(decoded)
        else:
            batched_input = decoded

        # 3. Predict
        start = time.time()
        if isinstance(batched_input, list):
            # No batch method or it returned a list: predict each individually
            results = [lit_api.predict(x) for x in batched_input]
        else:
            results = lit_api.predict(batched_input)
        duration = time.time() - start

        # 4. Unbatch if available
        has_unbatch = hasattr(lit_api, "unbatch") and callable(
            getattr(lit_api, "unbatch")
        )
        if has_unbatch and not isinstance(batched_input, list):
            results = lit_api.unbatch(results)

        # Ensure results is a list matching items length
        if not isinstance(results, list):
            results = [results]

        # Pad or truncate to match items length (defensive)
        if len(results) < len(items):
            results.extend([None] * (len(items) - len(results)))
        elif len(results) > len(items):
            results = results[: len(items)]

        # 5. Encode each response
        response_items = []
        has_encode = hasattr(lit_api, "encode_response") and callable(
            getattr(lit_api, "encode_response")
        )
        for i, item in enumerate(items):
            result = results[i]
            if has_encode:
                encoded = lit_api.encode_response(result)
            else:
                encoded = result

            response_items.append(
                {
                    "uid": item["uid"],
                    "data": encoded if isinstance(encoded, dict) else {"output": encoded},
                    "status": {"code": "Ok"},
                }
            )

        return {
            "type": "BATCH_RESPONSE",
            "items": response_items,
            "metrics": _make_metrics(model_name, duration, len(items)),
        }

    except Exception as e:
        return {
            "type": "BATCH_RESPONSE",
            "items": [
                {
                    "uid": item["uid"],
                    "data": None,
                    "status": {"code": "Error", "message": str(e)},
                }
                for item in items
            ],
        }


async def worker_main():
    args = parse_args()

    # Setup structured logging to stdout
    def log(level: str, message: str, **kwargs):
        record = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "level": level,
            "logger": "inference_worker",
            "message": message,
            "worker_id": args.worker_id,
            "model": args.model_name,
            "version": args.version,
            **kwargs,
        }
        print(json.dumps(record), flush=True, file=sys.stderr)

    log("INFO", f"Worker {args.worker_id} starting", device=args.device)

    # Load config and model
    try:
        config = load_model_config(args.config)
        lit_api = load_litapi(args.model_py, config)
        log("INFO", "Model loaded successfully")
    except Exception as e:
        log("ERROR", f"Failed to load model: {e}")
        startup = {
            "status": "error",
            "worker_id": args.worker_id,
            "message": str(e),
        }
        print(json.dumps(startup), flush=True)
        sys.exit(1)

    # Send ready signal
    startup = {
        "status": "ready",
        "worker_id": args.worker_id,
    }
    print(json.dumps(startup), flush=True)

    # Create Unix socket server (Python worker listens, Rust connects)
    if os.path.exists(args.uds_path):
        os.remove(args.uds_path)

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(args.uds_path)
    server.listen(1)
    server.setblocking(False)

    log("INFO", f"Listening on UDS {args.uds_path}")

    loop = asyncio.get_event_loop()

    while True:
        try:
            conn, _ = await loop.sock_accept(server)
        except Exception as e:
            log("ERROR", f"Accept error: {e}")
            continue

        try:
            while True:
                # Read length prefix (4 bytes)
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
                    request = json.loads(body.decode("utf-8"))
                except Exception as e:
                    log("ERROR", f"Failed to parse request: {e}")
                    continue

                # Handle request
                try:
                    payload = request.get("payload", {})
                    msg_type = payload.get("type", payload.get("msg_type", "INFER"))
                    if msg_type == "BATCH_INFER":
                        response = handle_batch_request(lit_api, request, args.model_name)
                        response["worker_id"] = args.worker_id
                        for item in response.get("items", []):
                            item["worker_id"] = args.worker_id
                    else:
                        response = await handle_request(lit_api, request, args.model_name)
                except Exception as e:
                    log("ERROR", f"Request handling error: {e}")
                    response = {
                        "uid": request.get("uid", ""),
                        "data": None,
                        "status": {"code": "Error", "message": str(e)},
                        "worker_id": args.worker_id,
                    }

                # Send response with length prefix
                resp_bytes = json.dumps(response).encode("utf-8")
                len_prefix = struct.pack(">I", len(resp_bytes))
                await loop.sock_sendall(conn, len_prefix + resp_bytes)

        except Exception as e:
            log("ERROR", f"Connection error: {e}")
        finally:
            conn.close()


if __name__ == "__main__":
    asyncio.run(worker_main())
