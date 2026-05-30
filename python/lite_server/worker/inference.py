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
from typing import Any

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
    StreamCancel,
    StreamChunk,
    StreamChunkResponse,
    StreamClose,
    StreamDone,
    StreamError,
    StreamOpen,
    StreamRequest,
    StreamResponse,
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


def setup_logging(worker_id: int):
    """Configure worker logging: plain text to stderr (captured by Rust)."""
    logger = logging.getLogger("inference_worker")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_LevelPrefixFormatter())
    logger.addHandler(handler)
    return logger


def load_model_config(config_path: str):
    if not os.path.exists(config_path):
        return {}
    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}


def load_litapi(model_py_path: str, config: dict, device: str = "cpu"):
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


def _make_stream_error(stream_id: str, message: str) -> Response:
    return Response(
        uid=f"stream-error-{stream_id}",
        stream=StreamResponse(
            stream_id=stream_id,
            error=StreamError(message=message),
        ),
    )


def _make_stream_chunk(stream_id: str, data: bytes, is_final: bool = False) -> Response:
    return Response(
        uid=f"stream-chunk-{stream_id}",
        stream=StreamResponse(
            stream_id=stream_id,
            chunk=StreamChunkResponse(data=data, is_final=is_final),
        ),
    )


def _make_stream_done(stream_id: str) -> Response:
    return Response(
        uid=f"stream-done-{stream_id}",
        stream=StreamResponse(
            stream_id=stream_id,
            done=StreamDone(),
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

    if hasattr(lit_api, "on_request"):
        decoded = lit_api.on_request(decoded, meta)

    output = lit_api.predict(decoded)

    encoded = lit_api.encode_response(output) if hasattr(lit_api, "encode_response") else output

    if hasattr(lit_api, "on_response"):
        encoded = lit_api.on_response(encoded, meta)

    resp_bytes = json.dumps(encoded).encode()
    return resp_bytes, _make_status(True)


# ---------------------------------------------------------------------------
# Streaming Support
# ---------------------------------------------------------------------------

def _has_stream_predict(lit_api: LitAPI) -> bool:
    return hasattr(lit_api, "stream_predict") and callable(getattr(lit_api, "stream_predict"))


def _consume_stream_generator(lit_api: LitAPI, generator, stream_id: str, socket: zmq.Socket, log: logging.Logger, meta=None):
    """Background thread: consume a stream_predict generator and send chunks."""
    try:
        for output in generator:
            encoded = lit_api.encode_response(output) if hasattr(lit_api, "encode_response") else output
            if hasattr(lit_api, "on_response") and meta is not None:
                encoded = lit_api.on_response(encoded, meta)
            resp_bytes = json.dumps(encoded).encode()
            socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=False).SerializeToString())
    except Exception as e:
        log.error(f"stream_predict error for {stream_id}: {e}")
        socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
        return

    socket.send(_make_stream_done(stream_id).SerializeToString())


def _handle_stream_open(lit_api: LitAPI, stream_req: StreamRequest, socket: zmq.Socket, active_streams: dict, log: logging.Logger):
    stream_id = stream_req.stream_id
    open_req = stream_req.open
    data = open_req.data if open_req else b""
    meta = _meta_from_proto(open_req.meta) if open_req and open_req.HasField("meta") else None

    if not _has_stream_predict(lit_api):
        # Fallback: predict() once, send as single chunk
        try:
            resp_bytes, status = _run_predict(lit_api, data, meta)
            socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=True).SerializeToString())
            socket.send(_make_stream_done(stream_id).SerializeToString())
        except Exception as e:
            socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
        return

    # Normal streaming: start generator
    raw = json.loads(data) if data else {}
    decoded = lit_api.decode_request(raw) if hasattr(lit_api, "decode_request") else raw

    if hasattr(lit_api, "on_request") and meta is not None:
        try:
            decoded = lit_api.on_request(decoded, meta)
        except Exception as e:
            socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
            return

    try:
        generator = lit_api.stream_predict(decoded)
    except Exception as e:
        socket.send(_make_stream_error(stream_id, f"stream_predict failed: {e}").SerializeToString())
        return

    active_streams[stream_id] = generator
    threading.Thread(
        target=_consume_stream_generator,
        args=(lit_api, generator, stream_id, socket, log, meta),
        daemon=True,
    ).start()


def _handle_stream_cancel(stream_id: str, active_streams: dict, log: logging.Logger):
    generator = active_streams.pop(stream_id, None)
    if generator is not None:
        try:
            generator.close()
        except Exception as e:
            log.debug("stream %s close error: %s", stream_id, e)


# ---------------------------------------------------------------------------
# Standard Loop
# ---------------------------------------------------------------------------

def run_standard_loop(lit_api: LitAPI, socket: zmq.Socket, model_name: str, log: logging.Logger):
    """Handle single + batch + stream requests synchronously."""
    active_streams: dict[str, Any] = {}

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
                # Health check: empty data → skip predict pipeline
                if not request.single.data:
                    response = Response(
                        uid=uid,
                        single=SingleResponse(
                            data=b"{}",
                            status=_make_status(True),
                        ),
                    )
                else:
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

            elif request.HasField("stream"):
                stream_req: StreamRequest = request.stream
                action = stream_req.WhichOneof("action")

                if action == "open":
                    _handle_stream_open(lit_api, stream_req, socket, active_streams, log)
                    continue  # chunks sent asynchronously
                elif action == "cancel":
                    _handle_stream_cancel(stream_req.stream_id, active_streams, log)
                    continue
                elif action == "close":
                    _handle_stream_cancel(stream_req.stream_id, active_streams, log)
                    continue
                else:
                    response = _make_error_response(uid, f"Unsupported stream action: {action}")

            else:
                response = _make_error_response(uid, "Unsupported payload type")

        except Exception as e:
            log.error("request %s failed: %s", uid, e, exc_info=True)
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


def run_cb_loop(lit_api: LitAPI, socket: zmq.Socket, model_name: str, log: logging.Logger):
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
            err_resp = _make_error_response(cb_add.uid, str(e))
            socket.send(err_resp.SerializeToString())
            return

        state = CBState(cb_add.uid, decoded, meta)
        active[cb_add.uid] = state

        try:
            lit_api.prefill(cb_add.uid, decoded)
            state.prefilled = True
        except Exception as e:
            del active[cb_add.uid]
            err_resp = _make_error_response(cb_add.uid, f"prefill failed: {e}")
            socket.send(err_resp.SerializeToString())

    def _handle_remove(cb_remove: CBRemoveRequest):
        active.pop(cb_remove.uid, None)

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
                    log.error("cb step error: %s", e, exc_info=True)
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
                        log.error("cb encode error for %s: %s", uid, e, exc_info=True)
                        err_resp = _make_error_response(uid, f"encode failed: {e}")
                        socket.send(err_resp.SerializeToString())

            time.sleep(0.001)

    threading.Thread(target=step_loop, daemon=True).start()

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
            log.warning("cb protobuf parse error: %s", e)
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

    try:
        config = load_model_config(args.config)
        lit_api = load_litapi(args.model_py, config, device=args.device)
        log.info("Model loaded successfully")
    except Exception as e:
        log.error(f"Failed to load model: {e}")
        print(json.dumps({"status": "error", "worker_id": args.worker_id, "message": str(e)}), flush=True)
        sys.exit(1)

    print(json.dumps({"status": "ready", "worker_id": args.worker_id}), flush=True)

    context = zmq.Context()
    socket = context.socket(zmq.PAIR)
    try:
        socket.connect(args.endpoint)
        socket.setsockopt(zmq.LINGER, 0)

        log.info(f"Connected ZMQ PAIR to {args.endpoint}")

        if args.continuous_batching or config.get("continuous_batching", False):
            run_cb_loop(lit_api, socket, args.model_name, log)
        else:
            run_standard_loop(lit_api, socket, args.model_name, log)
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
