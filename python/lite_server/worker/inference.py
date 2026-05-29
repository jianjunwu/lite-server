"""Python inference worker entry point for lite-server (ZMQ + Protobuf)."""

import argparse
import importlib.util
import json
import logging
import os
import sys
import threading
import time
import traceback

import yaml
import zmq

from lite_server.api import LitAPI, RequestMeta
from lite_server.proto import (
    BatchItemResponse,
    BatchRequest,
    BatchResponse,
    CBAddRequest,
    CBCompletedResponse,
    CBRemoveRequest,
    CompletedSequence,
    Request,
    Response,
    SingleRequest,
    SingleResponse,
    Status,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--model-py", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--device", required=True)
    parser.add_argument("--worker-id", type=int, required=True)
    parser.add_argument("--endpoint", required=True, help="ZMQ PAIR endpoint, e.g. ipc:///tmp/lite-server/...")
    parser.add_argument("--continuous-batching", action="store_true", default=False)
    return parser.parse_args()


def setup_logging(worker_id: int):
    """Configure worker logging: plain text to stderr (captured by Rust)."""
    logger = logging.getLogger("inference_worker")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    return logger


def load_model_config(config_path: str):
    if not os.path.exists(config_path):
        return {}
    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}


def load_litapi(model_py_path: str, config: dict):
    spec = importlib.util.spec_from_file_location("model_module", model_py_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    LitAPIClass = None
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, type) and attr_name != "LitAPI" and hasattr(attr, "predict"):
            LitAPIClass = attr
            break

    if LitAPIClass is None:
        raise RuntimeError(f"No LitAPI subclass found in {model_py_path}")

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
    instance.config = config
    if hasattr(instance, "pre_setup"):
        instance.pre_setup()

    if hasattr(instance, "setup"):
        device = config.get("accelerator", "cpu")
        if isinstance(device, list):
            device = device[0] if device else "cpu"
        instance.setup(device)

    return instance


def _make_status(ok: bool, message: str = "") -> Status:
    return Status(code="Ok" if ok else "Error", message=message)


def _make_error_response(uid: str, message: str) -> Response:
    return Response(
        uid=uid,
        single=SingleResponse(
            data=json.dumps({"error": message}).encode(),
            status=_make_status(False, message),
        ),
    )


def _meta_from_proto(meta_pb) -> RequestMeta:
    payload = json.loads(meta_pb.payload) if meta_pb.payload else None
    return RequestMeta(
        route=meta_pb.route,
        headers=dict(meta_pb.headers),
        client_ip=meta_pb.client_ip,
        request_id=meta_pb.request_id,
        timestamp_ns=meta_pb.timestamp_ns,
        payload=payload,
    )


def _run_predict(lit_api: LitAPI, data: bytes, meta: RequestMeta) -> tuple[bytes, Status]:
    """Run the full predict pipeline with hooks."""
    raw = json.loads(data) if data else {}
    decoded = lit_api.decode_request(raw) if hasattr(lit_api, "decode_request") else raw

    # Hook: on_request (skip if not implemented by model)
    if hasattr(lit_api, "on_request"):
        decoded = lit_api.on_request(decoded, meta)

    # Predict
    output = lit_api.predict(decoded)

    # Encode
    encoded = lit_api.encode_response(output) if hasattr(lit_api, "encode_response") else output

    # Hook: on_response (skip if not implemented by model)
    if hasattr(lit_api, "on_response"):
        encoded = lit_api.on_response(encoded, meta)

    resp_bytes = json.dumps(encoded).encode()
    return resp_bytes, _make_status(True)


# ---------------------------------------------------------------------------
# Standard Loop
# ---------------------------------------------------------------------------

def run_standard_loop(lit_api: LitAPI, socket: zmq.Socket, model_name: str):
    """Handle single + batch requests synchronously."""
    while True:
        try:
            req_bytes = socket.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break
            continue

        try:
            request = Request()
            request.ParseFromString(req_bytes)
        except Exception as e:
            socket.send(_make_error_response("", f"Protobuf parse: {e}").SerializeToString())
            continue

        uid = request.uid
        meta = _meta_from_proto(request.meta) if request.HasField("meta") else None

        try:
            if request.HasField("single"):
                resp_bytes, status = _run_predict(lit_api, request.single.data, meta)
                response = Response(
                    uid=uid,
                    single=SingleResponse(data=resp_bytes, status=status),
                )

            elif request.HasField("batch"):
                batch: BatchRequest = request.batch
                items = []
                for item in batch.items:
                    try:
                        resp_bytes, status = _run_predict(lit_api, item.data, meta)
                    except Exception as e:
                        resp_bytes = json.dumps({"error": str(e)}).encode()
                        status = _make_status(False, str(e))
                    items.append(
                        BatchItemResponse(
                            uid=item.uid,
                            data=resp_bytes,
                            status=status,
                        )
                    )
                response = Response(
                    uid=uid,
                    batch=BatchResponse(items=items),
                )
            else:
                response = _make_error_response(uid, "Unsupported payload type")

        except Exception as e:
            response = _make_error_response(uid, f"{type(e).__name__}: {e}")

        socket.send(response.SerializeToString())


# ---------------------------------------------------------------------------
# Continuous Batching Loop
# ---------------------------------------------------------------------------

class CBState:
    def __init__(self, uid: str, decoded_input, meta: RequestMeta):
        self.uid = uid
        self.input = decoded_input
        self.output = []
        self.meta = meta
        self.prefilled = False


def run_cb_loop(lit_api: LitAPI, socket: zmq.Socket, model_name: str):
    """Autonomous continuous batching loop."""
    active: dict[str, CBState] = {}
    lock = threading.Lock()

    def _handle_add(cb_add: CBAddRequest):
        raw = json.loads(cb_add.data) if cb_add.data else {}
        decoded = lit_api.decode_request(raw) if hasattr(lit_api, "decode_request") else raw
        meta = _meta_from_proto(cb_add.meta) if cb_add.HasField("meta") else RequestMeta(
            route="", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=raw,
        )

        try:
            if hasattr(lit_api, "on_request"):
                decoded = lit_api.on_request(decoded, meta)
        except Exception as e:
            # Reject immediately
            err_resp = _make_error_response(cb_add.uid, str(e))
            socket.send(err_resp.SerializeToString())
            return

        state = CBState(cb_add.uid, decoded, meta)
        active[cb_add.uid] = state

        # Prefill
        try:
            lit_api.prefill(cb_add.uid, decoded)
            state.prefilled = True
        except Exception as e:
            del active[cb_add.uid]
            err_resp = _make_error_response(cb_add.uid, f"prefill failed: {e}")
            socket.send(err_resp.SerializeToString())

    def _handle_remove(cb_remove: CBRemoveRequest):
        active.pop(cb_remove.uid, None)

    # Background step thread
    def step_loop():
        while True:
            with lock:
                if not active:
                    time.sleep(0.001)
                    continue

                ready = [s for s in active.values() if s.prefilled]
                if not ready:
                    time.sleep(0.001)
                    continue

                try:
                    outputs = lit_api.step(ready)
                except Exception as e:
                    # Error: fail all active sequences
                    for state in list(active.values()):
                        err_resp = _make_error_response(state.uid, f"step failed: {e}")
                        socket.send(err_resp.SerializeToString())
                    active.clear()
                    continue

                completed = []
                for state, token in zip(ready, outputs):
                    state.output.append(token)
                    if lit_api.has_finished(state.uid, token, state.output):
                        completed.append(state.uid)

                # Send completed sequences
                for uid in completed:
                    state = active.pop(uid)
                    try:
                        encoded = lit_api.encode_response(state.output) if hasattr(lit_api, "encode_response") else state.output
                        if hasattr(lit_api, "on_response"):
                            encoded = lit_api.on_response(encoded, state.meta)
                        resp_bytes = json.dumps(encoded).encode()
                        resp = Response(
                            uid=uid,
                            single=SingleResponse(data=resp_bytes, status=_make_status(True)),
                        )
                        socket.send(resp.SerializeToString())
                    except Exception as e:
                        err_resp = _make_error_response(uid, f"encode failed: {e}")
                        socket.send(err_resp.SerializeToString())

            time.sleep(0.001)

    threading.Thread(target=step_loop, daemon=True).start()

    # Main thread: receive commands
    while True:
        try:
            req_bytes = socket.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break
            continue

        try:
            request = Request()
            request.ParseFromString(req_bytes)
        except Exception:
            continue

        with lock:
            if request.HasField("cb_add"):
                _handle_add(request.cb_add)
            elif request.HasField("cb_remove"):
                _handle_remove(request.cb_remove)


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def worker_main():
    args = parse_args()

    log = setup_logging(args.worker_id)

    log.info(f"Worker {args.worker_id} starting, device={args.device}, endpoint={args.endpoint}")

    # Load config and model
    try:
        config = load_model_config(args.config)
        lit_api = load_litapi(args.model_py, config)
        log.info("Model loaded successfully")
    except Exception as e:
        log.error(f"Failed to load model: {e}")
        print(json.dumps({"status": "error", "worker_id": args.worker_id, "message": str(e)}), flush=True)
        sys.exit(1)

    # Ready signal (keep JSON for startup handshake)
    print(json.dumps({"status": "ready", "worker_id": args.worker_id}), flush=True)

    # Setup ZMQ PAIR
    context = zmq.Context()
    socket = context.socket(zmq.PAIR)
    socket.connect(args.endpoint)
    socket.setsockopt(zmq.LINGER, 0)

    log.info(f"Connected ZMQ PAIR to {args.endpoint}")

    try:
        if args.continuous_batching or config.get("continuous_batching", False):
            run_cb_loop(lit_api, socket, args.model_name)
        else:
            run_standard_loop(lit_api, socket, args.model_name)
    except KeyboardInterrupt:
        log.info("Interrupted, shutting down")
    finally:
        if hasattr(lit_api, "teardown"):
            try:
                lit_api.teardown()
            except Exception as e:
                log.error(f"teardown error: {e}")
        socket.close()
        context.term()


if __name__ == "__main__":
    worker_main()
