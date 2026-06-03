"""Python inference worker entry point for lite-server (ZMQ + Protobuf)."""

import argparse
import asyncio
import importlib.util
import inspect
import json
import logging
import os
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from typing import Any

import yaml
import zmq

from lite_server.api import LitAPI, RequestMeta, BidiStreamHandler
from lite_server.api_async import AsyncLitAPI
from lite_server.proto import (
    BatchItemResponse,
    BatchRequest,
    BatchResponse,
    CBAddRequest,
    CBCompletedResponse,
    CBRemoveRequest,
    CompletedSequence,
    Metrics,
    MetricValue,
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
    parser.add_argument("--log-level", default="warn", help="Logging level: debug, info, warn, error")
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


def setup_logging(worker_id: int, level_str: str = "warn"):
    """Configure worker logging: plain text to stderr (captured by Rust).

    Configures the root logger so that all child loggers (including the
    user's model logger via ``LitAPI.logger``) inherit the handler and level.
    """
    level = getattr(logging, level_str.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)
    if not root.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(_LevelPrefixFormatter())
        root.addHandler(handler)
    return logging.getLogger("inference_worker")


def load_model_config(config_path: str):
    if not os.path.exists(config_path):
        return {}
    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}


def load_litapi(model_py_path: str, config: dict, device: str = "cpu"):
    model_dir = os.path.dirname(os.path.abspath(model_py_path))
    spec = importlib.util.spec_from_file_location("model_module", model_py_path)
    module = importlib.util.module_from_spec(spec)

    # Protect stdout during model module import and setup.
    # C-level inference libraries (CANN, ONNX Runtime, MagicMind, etc.) may
    # write init logs directly to fd 1, which breaks the worker-ready handshake
    # protocol that expects the first stdout line to be valid JSON.
    with _protect_stdout():
        sys.path.insert(0, model_dir)
        try:
            spec.loader.exec_module(module)
        finally:
            sys.path.remove(model_dir)

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

        instance = LitAPIClass(
            max_batch_size=max_batch_size,
            batch_timeout=batch_timeout,
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


def _make_stream_done(stream_id: str, metrics=None) -> Response:
    return Response(
        uid=f"stream-done-{stream_id}",
        stream=StreamResponse(
            stream_id=stream_id,
            done=StreamDone(metrics=metrics),
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


def _collect_metrics(lit_api: LitAPI):
    """Collect pre-registered custom metrics from the LitAPI instance."""
    values = getattr(lit_api, "_metric_values", None)
    if not values:
        return None
    specs = lit_api._metric_specs
    gauges = []
    counters = []
    histograms = []
    for mid, val in values:
        if mid < len(specs):
            spec = specs[mid]
            mv = MetricValue(id=mid, value=val)
            if spec.metric_type == "gauge":
                gauges.append(mv)
            elif spec.metric_type == "counter":
                counters.append(mv)
            elif spec.metric_type == "histogram":
                histograms.append(mv)
    lit_api._metric_values = []
    if not gauges and not counters and not histograms:
        return None
    return Metrics(gauges=gauges, counters=counters, histograms=histograms)


def _has_batch_methods(lit_api) -> bool:
    """Detect whether user has overridden batch/unbatch."""
    import litserve as ls

    batch_func = getattr(lit_api, "batch", None)
    unbatch_func = getattr(lit_api, "unbatch", None)
    if batch_func is None or unbatch_func is None:
        return False
    batch_overridden = batch_func.__code__ is not ls.LitAPI.batch.__code__
    unbatch_overridden = unbatch_func.__code__ is not ls.LitAPI.unbatch.__code__
    return batch_overridden and unbatch_overridden


def _run_predict(lit_api: LitAPI, data: bytes, meta: RequestMeta, log: logging.Logger):
    """Run the full predict pipeline with hooks."""
    raw = json.loads(data) if data else {}

    # 1. on_request (pre-decode)
    if hasattr(lit_api, "on_request"):
        try:
            raw = lit_api.on_request(raw, meta)
        except Exception as e:
            log.warning("on_request hook failed: %s", e, exc_info=True)
            raise

    # 2. decode_request
    try:
        decoded = lit_api.decode_request(raw) if hasattr(lit_api, "decode_request") else raw
    except Exception as e:
        log.warning("decode_request failed: %s", e, exc_info=True)
        raise

    # 3. predict
    try:
        output = lit_api.predict(decoded)
    except Exception as e:
        log.error("predict failed: %s", e, exc_info=True)
        raise

    # 4. encode_response
    try:
        encoded = lit_api.encode_response(output) if hasattr(lit_api, "encode_response") else output
    except Exception as e:
        log.warning("encode_response failed: %s", e, exc_info=True)
        raise

    # 5. on_response
    if hasattr(lit_api, "on_response"):
        try:
            encoded = lit_api.on_response(encoded, meta)
        except Exception as e:
            log.warning("on_response hook failed: %s", e, exc_info=True)
            raise

    resp_bytes = json.dumps(encoded).encode()
    metrics = _collect_metrics(lit_api)
    return resp_bytes, _make_status(True), metrics


# ---------------------------------------------------------------------------
# Async Detection & Helpers
# ---------------------------------------------------------------------------

def _is_async_api(lit_api) -> bool:
    """Detect whether the loaded API instance requires an async event loop."""
    return (
        isinstance(lit_api, AsyncLitAPI)
        or getattr(lit_api, "enable_async", False)
        or asyncio.iscoroutinefunction(lit_api.predict)
        or inspect.isasyncgenfunction(getattr(lit_api, "stream_predict", None))
    )


async def _maybe_await(func, *args, **kwargs):
    """Call ``func`` and await if it (or its return value) is a coroutine."""
    if asyncio.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    result = func(*args, **kwargs)
    if asyncio.iscoroutine(result):
        return await result
    if inspect.isasyncgen(result):
        return result
    return result


async def _run_predict_async(lit_api: LitAPI, data: bytes, meta: RequestMeta, log: logging.Logger):
    """Async version of the full predict pipeline with hooks."""
    raw = json.loads(data) if data else {}

    # 1. on_request (pre-decode)
    if hasattr(lit_api, "on_request"):
        try:
            raw = await _maybe_await(lit_api.on_request, raw, meta)
        except Exception as e:
            log.warning("on_request hook failed: %s", e, exc_info=True)
            raise

    # 2. decode_request
    try:
        if hasattr(lit_api, "decode_request"):
            decoded = await _maybe_await(lit_api.decode_request, raw)
        else:
            decoded = raw
    except Exception as e:
        log.warning("decode_request failed: %s", e, exc_info=True)
        raise

    # 3. predict
    try:
        output = await _maybe_await(lit_api.predict, decoded)
    except Exception as e:
        log.error("predict failed: %s", e, exc_info=True)
        raise

    # 4. encode_response
    try:
        if hasattr(lit_api, "encode_response"):
            encoded = await _maybe_await(lit_api.encode_response, output)
        else:
            encoded = output
    except Exception as e:
        log.warning("encode_response failed: %s", e, exc_info=True)
        raise

    # 5. on_response
    if hasattr(lit_api, "on_response"):
        try:
            encoded = await _maybe_await(lit_api.on_response, encoded, meta)
        except Exception as e:
            log.warning("on_response hook failed: %s", e, exc_info=True)
            raise

    resp_bytes = json.dumps(encoded).encode()
    metrics = _collect_metrics(lit_api)
    return resp_bytes, _make_status(True), metrics


# ---------------------------------------------------------------------------
# Streaming Support
# ---------------------------------------------------------------------------

def _has_stream_predict(lit_api: LitAPI) -> bool:
    return hasattr(lit_api, "stream_predict") and callable(getattr(lit_api, "stream_predict"))


def _has_bidi_stream(lit_api: LitAPI) -> bool:
    """Detect whether the API implements bidirectional streaming."""
    return hasattr(lit_api, "bidi_stream") and callable(getattr(lit_api, "bidi_stream"))


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

    metrics = _collect_metrics(lit_api)
    socket.send(_make_stream_done(stream_id, metrics).SerializeToString())


def _handle_stream_open(lit_api: LitAPI, stream_req: StreamRequest, socket: zmq.Socket, active_streams: dict, log: logging.Logger):
    stream_id = stream_req.stream_id
    open_req = stream_req.open
    data = open_req.data if open_req else b""
    meta = _meta_from_proto(open_req.meta) if open_req and open_req.HasField("meta") else None

    # Try bidirectional streaming first
    if _has_bidi_stream(lit_api):
        raw = json.loads(data) if data else {}

        if hasattr(lit_api, "on_request") and meta is not None:
            try:
                raw = lit_api.on_request(raw, meta)
            except Exception as e:
                socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
                return

        decoded = lit_api.decode_request(raw) if hasattr(lit_api, "decode_request") else raw

        try:
            handler = lit_api.bidi_stream()
        except Exception as e:
            socket.send(_make_stream_error(stream_id, f"bidi_stream failed: {e}").SerializeToString())
            return

        active_streams[stream_id] = handler

        try:
            output = handler.on_open(decoded)
        except Exception as e:
            log.error("bidi on_open failed for %s: %s", stream_id, e, exc_info=True)
            socket.send(_make_stream_error(stream_id, f"on_open failed: {e}").SerializeToString())
            return

        if output is not None:
            try:
                encoded = lit_api.encode_response(output) if hasattr(lit_api, "encode_response") else output
                if hasattr(lit_api, "on_response") and meta is not None:
                    encoded = lit_api.on_response(encoded, meta)
                resp_bytes = json.dumps(encoded).encode()
                socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=False).SerializeToString())
            except Exception as e:
                log.error("bidi on_open encode failed for %s: %s", stream_id, e, exc_info=True)
                socket.send(_make_stream_error(stream_id, f"encode failed: {e}").SerializeToString())
                return
        return

    if not _has_stream_predict(lit_api):
        # Fallback: predict() once, send as single chunk
        try:
            resp_bytes, status, metrics = _run_predict(lit_api, data, meta, log)
            socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=True).SerializeToString())
            socket.send(_make_stream_done(stream_id, metrics).SerializeToString())
        except Exception as e:
            socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
        return

    # Normal uni-directional streaming: start generator
    raw = json.loads(data) if data else {}

    if hasattr(lit_api, "on_request") and meta is not None:
        try:
            raw = lit_api.on_request(raw, meta)
        except Exception as e:
            socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
            return

    decoded = lit_api.decode_request(raw) if hasattr(lit_api, "decode_request") else raw

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


def _handle_stream_chunk(lit_api: LitAPI, stream_req: StreamRequest, socket: zmq.Socket, active_streams: dict, log: logging.Logger, meta=None):
    """Handle a mid-stream chunk for bidirectional streaming (sync worker)."""
    stream_id = stream_req.stream_id
    handler = active_streams.get(stream_id)
    if handler is None:
        socket.send(_make_stream_error(stream_id, "stream not found").SerializeToString())
        return

    data = stream_req.chunk.data if stream_req.chunk else b""
    raw = json.loads(data) if data else {}

    try:
        output = handler.on_chunk(raw)
    except Exception as e:
        log.error("bidi on_chunk failed for %s: %s", stream_id, e, exc_info=True)
        socket.send(_make_stream_error(stream_id, f"on_chunk failed: {e}").SerializeToString())
        return

    if output is not None:
        try:
            encoded = lit_api.encode_response(output) if hasattr(lit_api, "encode_response") else output
            if hasattr(lit_api, "on_response") and meta is not None:
                encoded = lit_api.on_response(encoded, meta)
            resp_bytes = json.dumps(encoded).encode()
            socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=False).SerializeToString())
        except Exception as e:
            log.error("bidi encode failed for %s: %s", stream_id, e, exc_info=True)
            socket.send(_make_stream_error(stream_id, f"encode failed: {e}").SerializeToString())


def _handle_stream_close(stream_id: str, active_streams: dict, socket: zmq.Socket, log: logging.Logger):
    """Close a stream: cleanup generator or bidi handler, send StreamDone."""
    entry = active_streams.pop(stream_id, None)
    if entry is None:
        return

    if isinstance(entry, BidiStreamHandler):
        try:
            entry.on_close()
        except Exception as e:
            log.debug("bidi on_close error for %s: %s", stream_id, e)
    else:
        # It's a generator
        try:
            entry.close()
        except Exception as e:
            log.debug("stream %s close error: %s", stream_id, e)


# ---------------------------------------------------------------------------
# Async Loop
# ---------------------------------------------------------------------------

async def run_async_loop(lit_api: LitAPI, socket, model_name: str, log: logging.Logger):
    """Handle single + batch + stream requests asynchronously."""
    pending_tasks: dict[str, asyncio.Task] = {}
    active_streams: dict[str, asyncio.Task] = {}

    while True:
        try:
            req_bytes = await socket.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break
            continue

        try:
            request = Request()
            request.ParseFromString(req_bytes)
        except Exception as e:
            await socket.send(_make_error_response("", f"Protobuf parse: {e}").SerializeToString())
            continue

        if request.HasField("stream"):
            stream_id = request.stream.stream_id
            action = request.stream.WhichOneof("action")
            task = asyncio.create_task(
                _handle_stream_async(lit_api, request, socket, active_streams, log)
            )
            # Only register in active_streams for open; chunk/close/cancel
            # must not overwrite the existing handler or consumption task.
            if action == "open":
                active_streams[stream_id] = task
            pending_tasks[request.uid] = task
        else:
            task = asyncio.create_task(
                _handle_request_async(lit_api, request, socket, log)
            )
            pending_tasks[request.uid] = task

        # Clean up completed tasks and retrieve exceptions to avoid warnings
        done = [uid for uid, t in pending_tasks.items() if t.done()]
        for uid in done:
            t = pending_tasks.pop(uid)
            # Also clean up active_streams if this was a stream task
            for sid, stream_task in list(active_streams.items()):
                if stream_task is t:
                    active_streams.pop(sid, None)
                    break
            exc = t.exception()
            if exc is not None and not isinstance(exc, asyncio.CancelledError):
                log.error("async task %s failed: %s", uid, exc)

    # Cancel any pending tasks on shutdown
    for uid, t in list(pending_tasks.items()):
        if not t.done():
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


async def _handle_request_async(lit_api: LitAPI, request: Request, socket, log: logging.Logger):
    """Process a single request (single / batch / stream) asynchronously."""
    uid = request.uid
    meta = _meta_from_proto(request.meta) if request.HasField("meta") else None

    try:
        if request.HasField("single"):
            # Health check: empty data → skip predict pipeline
            if not request.single.data:
                response = Response(
                    uid=uid,
                    single=SingleResponse(data=b"{}", status=_make_status(True)),
                )
            else:
                resp_bytes, status, metrics = await _run_predict_async(lit_api, request.single.data, meta, log)
                response = Response(
                    uid=uid,
                    single=SingleResponse(data=resp_bytes, status=status),
                    metrics=metrics,
                )

        elif request.HasField("batch"):
            batch: BatchRequest = request.batch
            batch_items = batch.items

            # Phase 1: per-item async on_request → decode
            decoded_map: dict[str, Any] = {}
            error_map: dict[str, Exception] = {}

            for item in batch_items:
                try:
                    raw = json.loads(item.data) if item.data else {}
                    if hasattr(lit_api, "on_request"):
                        raw = await _maybe_await(lit_api.on_request, raw, meta)
                    if hasattr(lit_api, "decode_request"):
                        decoded = await _maybe_await(lit_api.decode_request, raw)
                    else:
                        decoded = raw
                    decoded_map[item.uid] = decoded
                except Exception as e:
                    error_map[item.uid] = e

            # Phase 2: batch → predict → unbatch
            success_outputs: dict[str, Any] = {}

            if decoded_map and _has_batch_methods(lit_api):
                decoded_uids = list(decoded_map.keys())
                decodeds = [decoded_map[_uid] for _uid in decoded_uids]
                try:
                    batched = await _maybe_await(lit_api.batch, decodeds)
                    output = await _maybe_await(lit_api.predict, batched)
                    outputs = await _maybe_await(lit_api.unbatch, output)
                    if len(outputs) != len(decoded_uids):
                        raise ValueError(f"unbatch: {len(outputs)} vs {len(decoded_uids)}")
                    for _uid, out in zip(decoded_uids, outputs):
                        success_outputs[_uid] = out
                except Exception as e:
                    for _uid in decoded_uids:
                        error_map[_uid] = e
            else:
                # Fallback: concurrent per-item predict
                tasks = []
                uid_list = []
                for _uid, decoded in decoded_map.items():
                    tasks.append(_maybe_await(lit_api.predict, decoded))
                    uid_list.append(_uid)
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for _uid, result in zip(uid_list, results):
                    if isinstance(result, Exception):
                        error_map[_uid] = result
                    else:
                        success_outputs[_uid] = result

            # Phase 3: per-item async encode → on_response
            final_map: dict[str, bytes] = {}

            for item_uid, output in success_outputs.items():
                try:
                    if hasattr(lit_api, "encode_response"):
                        encoded = await _maybe_await(lit_api.encode_response, output)
                    else:
                        encoded = output
                    if hasattr(lit_api, "on_response"):
                        encoded = await _maybe_await(lit_api.on_response, encoded, meta)
                    final_map[item_uid] = json.dumps(encoded).encode()
                except Exception as e:
                    error_map[item_uid] = e

            # Phase 4: assemble BatchResponse
            items = []
            for item in batch_items:
                item_uid = item.uid
                if item_uid in final_map:
                    items.append(
                        BatchItemResponse(
                            uid=item_uid,
                            data=final_map[item_uid],
                            status=_make_status(True),
                        )
                    )
                else:
                    err = error_map.get(item_uid, Exception("unknown error"))
                    err_bytes = json.dumps({"error": str(err)}).encode()
                    items.append(
                        BatchItemResponse(
                            uid=item_uid,
                            data=err_bytes,
                            status=_make_status(False, str(err)),
                        )
                    )

            metrics = _collect_metrics(lit_api)
            response = Response(
                uid=uid,
                batch=BatchResponse(items=items),
                metrics=metrics,
            )

        else:
            response = _make_error_response(uid, "Unsupported payload type")

    except Exception as e:
        log.error("async request %s failed: %s", uid, e, exc_info=True)
        response = _make_error_response(uid, f"{type(e).__name__}: {e}")

    await socket.send(response.SerializeToString())


# ---------------------------------------------------------------------------
# Async Streaming Support
# ---------------------------------------------------------------------------

async def _handle_stream_async(
    lit_api: LitAPI,
    request: Request,
    socket,
    active_streams: dict[str, Any],
    log: logging.Logger,
):
    """Handle stream open/chunk/close/cancel in async loop."""
    stream_req = request.stream
    stream_id = stream_req.stream_id
    action = stream_req.WhichOneof("action")

    if action == "open":
        await _handle_stream_open_async(lit_api, stream_req, socket, active_streams, log)
    elif action == "chunk":
        await _handle_stream_chunk_async(lit_api, stream_req, socket, active_streams, log)
    elif action == "close":
        entry = active_streams.pop(stream_id, None)
        if isinstance(entry, BidiStreamHandler):
            try:
                await _maybe_await(entry.on_close)
            except Exception as e:
                log.debug("bidi on_close error for %s: %s", stream_id, e)
            metrics = _collect_metrics(lit_api)
            await socket.send(_make_stream_done(stream_id, metrics).SerializeToString())
        elif isinstance(entry, asyncio.Task):
            entry.cancel()
            try:
                await entry
            except asyncio.CancelledError:
                pass
    elif action == "cancel":
        entry = active_streams.pop(stream_id, None)
        if isinstance(entry, BidiStreamHandler):
            try:
                await _maybe_await(entry.on_close)
            except Exception as e:
                log.debug("bidi on_close error for %s: %s", stream_id, e)
        elif isinstance(entry, asyncio.Task):
            entry.cancel()
            try:
                await entry
            except asyncio.CancelledError:
                pass


async def _handle_stream_open_async(
    lit_api: LitAPI,
    stream_req: StreamRequest,
    socket,
    active_streams: dict[str, Any],
    log: logging.Logger,
):
    """Open a stream in async loop."""
    stream_id = stream_req.stream_id
    open_req = stream_req.open
    data = open_req.data if open_req else b""
    meta = _meta_from_proto(open_req.meta) if open_req and open_req.HasField("meta") else None

    # Try bidirectional streaming first
    if _has_bidi_stream(lit_api):
        raw = json.loads(data) if data else {}

        if hasattr(lit_api, "on_request") and meta is not None:
            try:
                raw = await _maybe_await(lit_api.on_request, raw, meta)
            except Exception as e:
                log.warning("on_request hook failed for bidi %s: %s", stream_id, e, exc_info=True)
                await socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
                return

        try:
            decoded = await _maybe_await(lit_api.decode_request, raw) if hasattr(lit_api, "decode_request") else raw
        except Exception as e:
            log.warning("decode_request failed for bidi %s: %s", stream_id, e, exc_info=True)
            await socket.send(_make_stream_error(stream_id, f"decode failed: {e}").SerializeToString())
            return

        try:
            handler = await _maybe_await(lit_api.bidi_stream)
        except Exception as e:
            log.error("bidi_stream failed for %s: %s", stream_id, e, exc_info=True)
            await socket.send(_make_stream_error(stream_id, f"bidi_stream failed: {e}").SerializeToString())
            return

        active_streams[stream_id] = handler

        try:
            output = await _maybe_await(handler.on_open, decoded)
        except Exception as e:
            log.error("bidi on_open failed for %s: %s", stream_id, e, exc_info=True)
            await socket.send(_make_stream_error(stream_id, f"on_open failed: {e}").SerializeToString())
            return

        if output is not None:
            try:
                encoded = await _maybe_await(lit_api.encode_response, output) if hasattr(lit_api, "encode_response") else output
                if hasattr(lit_api, "on_response") and meta is not None:
                    encoded = await _maybe_await(lit_api.on_response, encoded, meta)
                resp_bytes = json.dumps(encoded).encode()
                await socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=False).SerializeToString())
            except Exception as e:
                log.error("bidi on_open encode failed for %s: %s", stream_id, e, exc_info=True)
                await socket.send(_make_stream_error(stream_id, f"encode failed: {e}").SerializeToString())
                return
        return

    if not _has_stream_predict(lit_api):
        # Fallback: predict() once, send as single chunk
        try:
            resp_bytes, status, metrics = await _run_predict_async(lit_api, data, meta, log)
            await socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=True).SerializeToString())
            await socket.send(_make_stream_done(stream_id, metrics).SerializeToString())
        except Exception as e:
            log.error("stream fallback predict failed for %s: %s", stream_id, e, exc_info=True)
            await socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
        return

    # Normal uni-directional streaming
    raw = json.loads(data) if data else {}

    if hasattr(lit_api, "on_request") and meta is not None:
        try:
            raw = await _maybe_await(lit_api.on_request, raw, meta)
        except Exception as e:
            log.warning("on_request hook failed for stream %s: %s", stream_id, e, exc_info=True)
            await socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
            return

    try:
        decoded = await _maybe_await(lit_api.decode_request, raw) if hasattr(lit_api, "decode_request") else raw
    except Exception as e:
        log.warning("decode_request failed for stream %s: %s", stream_id, e, exc_info=True)
        await socket.send(_make_stream_error(stream_id, f"decode failed: {e}").SerializeToString())
        return

    try:
        generator = await _maybe_await(lit_api.stream_predict, decoded)
    except Exception as e:
        log.error("stream_predict failed for %s: %s", stream_id, e, exc_info=True)
        await socket.send(_make_stream_error(stream_id, f"stream_predict failed: {e}").SerializeToString())
        return

    if inspect.isasyncgen(generator):
        task = asyncio.create_task(
            _consume_async_stream(lit_api, generator, stream_id, socket, log, meta)
        )
    else:
        task = asyncio.create_task(
            _consume_sync_stream_async(lit_api, generator, stream_id, socket, log, meta)
        )

    active_streams[stream_id] = task


async def _handle_stream_chunk_async(
    lit_api: LitAPI,
    stream_req: StreamRequest,
    socket,
    active_streams: dict[str, Any],
    log: logging.Logger,
):
    """Handle a mid-stream chunk for bidirectional streaming (async loop)."""
    stream_id = stream_req.stream_id
    handler = active_streams.get(stream_id)
    if handler is None or not isinstance(handler, BidiStreamHandler):
        await socket.send(_make_stream_error(stream_id, "bidi stream not found").SerializeToString())
        return

    data = stream_req.chunk.data if stream_req.chunk else b""
    raw = json.loads(data) if data else {}

    try:
        output = await _maybe_await(handler.on_chunk, raw)
    except Exception as e:
        log.error("bidi on_chunk failed for %s: %s", stream_id, e, exc_info=True)
        await socket.send(_make_stream_error(stream_id, f"on_chunk failed: {e}").SerializeToString())
        return

    if output is not None:
        try:
            encoded = await _maybe_await(lit_api.encode_response, output) if hasattr(lit_api, "encode_response") else output
            resp_bytes = json.dumps(encoded).encode()
            await socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=False).SerializeToString())
        except Exception as e:
            log.error("bidi encode failed for %s: %s", stream_id, e, exc_info=True)
            await socket.send(_make_stream_error(stream_id, f"encode failed: {e}").SerializeToString())


async def _consume_async_stream(
    lit_api: LitAPI,
    generator,
    stream_id: str,
    socket,
    log: logging.Logger,
    meta=None,
):
    """Consume an async generator and send chunks."""
    try:
        async for output in generator:
            try:
                encoded = await _maybe_await(lit_api.encode_response, output) if hasattr(lit_api, "encode_response") else output
                if hasattr(lit_api, "on_response") and meta is not None:
                    encoded = await _maybe_await(lit_api.on_response, encoded, meta)
                resp_bytes = json.dumps(encoded).encode()
                await socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=False).SerializeToString())
            except Exception as e:
                log.error("encode/on_response failed for stream %s: %s", stream_id, e, exc_info=True)
                await socket.send(_make_stream_error(stream_id, f"encode failed: {e}").SerializeToString())
                return
    except asyncio.CancelledError:
        # Propagate cancellation so the task is properly cancelled
        raise
    except Exception as e:
        log.error("async stream_predict error for %s: %s", stream_id, e, exc_info=True)
        await socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
        return

    metrics = _collect_metrics(lit_api)
    await socket.send(_make_stream_done(stream_id, metrics).SerializeToString())


_SENTINEL = object()


async def _consume_sync_stream_async(
    lit_api: LitAPI,
    generator,
    stream_id: str,
    socket,
    log: logging.Logger,
    meta=None,
):
    """Consume a sync generator in a background thread and send chunks asynchronously."""

    def _next_item():
        try:
            return next(generator)
        except StopIteration:
            return _SENTINEL

    try:
        while True:
            output = await asyncio.to_thread(_next_item)
            if output is _SENTINEL:
                break

            try:
                encoded = await _maybe_await(lit_api.encode_response, output) if hasattr(lit_api, "encode_response") else output
                if hasattr(lit_api, "on_response") and meta is not None:
                    encoded = await _maybe_await(lit_api.on_response, encoded, meta)
                resp_bytes = json.dumps(encoded).encode()
                await socket.send(_make_stream_chunk(stream_id, resp_bytes, is_final=False).SerializeToString())
            except Exception as e:
                log.error("encode/on_response failed for stream %s: %s", stream_id, e, exc_info=True)
                await socket.send(_make_stream_error(stream_id, f"encode failed: {e}").SerializeToString())
                return
    except asyncio.CancelledError:
        # Try to close the generator; don't wait indefinitely
        try:
            await asyncio.to_thread(generator.close)
        except Exception:
            pass
        raise
    except Exception as e:
        log.error("sync stream_predict error for %s: %s", stream_id, e, exc_info=True)
        await socket.send(_make_stream_error(stream_id, str(e)).SerializeToString())
        return

    metrics = _collect_metrics(lit_api)
    await socket.send(_make_stream_done(stream_id, metrics).SerializeToString())


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
                    resp_bytes, status, metrics = _run_predict(lit_api, request.single.data, meta, log)
                    response = Response(
                        uid=uid,
                        single=SingleResponse(data=resp_bytes, status=status),
                        metrics=metrics,
                    )

            elif request.HasField("batch"):
                batch: BatchRequest = request.batch
                batch_items = batch.items

                # Phase 1: on_request → decode (per item)
                decoded_map: dict[str, Any] = {}
                error_map: dict[str, Exception] = {}

                for item in batch_items:
                    try:
                        raw = json.loads(item.data) if item.data else {}
                        if hasattr(lit_api, "on_request"):
                            raw = lit_api.on_request(raw, meta)
                        decoded = lit_api.decode_request(raw) if hasattr(lit_api, "decode_request") else raw
                        decoded_map[item.uid] = decoded
                    except Exception as e:
                        log.warning("batch item %s on_request/decode failed: %s", item.uid, e, exc_info=True)
                        error_map[item.uid] = e

                # Phase 2: batch → predict → unbatch
                success_outputs: dict[str, Any] = {}

                if decoded_map and _has_batch_methods(lit_api):
                    decoded_uids = list(decoded_map.keys())
                    decodeds = [decoded_map[_uid] for _uid in decoded_uids]
                    try:
                        batched = lit_api.batch(decodeds)
                        output = lit_api.predict(batched)
                        outputs = lit_api.unbatch(output)
                        if len(outputs) != len(decoded_uids):
                            raise ValueError(
                                f"unbatch returned {len(outputs)} outputs, expected {len(decoded_uids)}"
                            )
                        for _uid, out in zip(decoded_uids, outputs):
                            success_outputs[_uid] = out
                    except Exception as e:
                        log.error("batch predict failed: %s", e, exc_info=True)
                        for _uid in decoded_uids:
                            error_map[_uid] = e
                else:
                    # Fallback: per-item predict
                    for _uid, decoded in decoded_map.items():
                        try:
                            success_outputs[_uid] = lit_api.predict(decoded)
                        except Exception as e:
                            log.error("predict failed for %s: %s", _uid, e, exc_info=True)
                            error_map[_uid] = e

                # Phase 3: encode → on_response (per item)
                final_map: dict[str, bytes] = {}

                for _uid, output in success_outputs.items():
                    try:
                        encoded = lit_api.encode_response(output) if hasattr(lit_api, "encode_response") else output
                        if hasattr(lit_api, "on_response"):
                            encoded = lit_api.on_response(encoded, meta)
                        final_map[_uid] = json.dumps(encoded).encode()
                    except Exception as e:
                        log.warning("encode/on_response failed for %s: %s", _uid, e, exc_info=True)
                        error_map[_uid] = e

                # Phase 4: assemble BatchResponse
                items = []
                for item in batch_items:
                    item_uid = item.uid
                    if item_uid in final_map:
                        items.append(
                            BatchItemResponse(
                                uid=item_uid,
                                data=final_map[item_uid],
                                status=_make_status(True),
                            )
                        )
                    else:
                        err = error_map.get(item_uid, Exception("unknown error"))
                        err_bytes = json.dumps({"error": str(err)}).encode()
                        items.append(
                            BatchItemResponse(
                                uid=item_uid,
                                data=err_bytes,
                                status=_make_status(False, str(err)),
                            )
                        )

                metrics = _collect_metrics(lit_api)
                response = Response(
                    uid=uid,
                    batch=BatchResponse(items=items),
                    metrics=metrics,
                )

            elif request.HasField("stream"):
                stream_req: StreamRequest = request.stream
                action = stream_req.WhichOneof("action")

                if action == "open":
                    _handle_stream_open(lit_api, stream_req, socket, active_streams, log)
                    continue  # chunks sent asynchronously
                elif action == "chunk":
                    _handle_stream_chunk(lit_api, stream_req, socket, active_streams, log, meta=None)
                    continue
                elif action == "close":
                    _handle_stream_close(stream_req.stream_id, active_streams, socket, log)
                    metrics = _collect_metrics(lit_api)
                    socket.send(_make_stream_done(stream_req.stream_id, metrics).SerializeToString())
                    continue
                elif action == "cancel":
                    _handle_stream_close(stream_req.stream_id, active_streams, socket, log)
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


def _has_async_methods(lit_api) -> bool:
    """Check whether any of the hooks/methods on ``lit_api`` are async."""
    names = ["decode_request", "on_request", "prefill", "step", "has_finished", "encode_response", "on_response"]
    for name in names:
        func = getattr(lit_api, name, None)
        if func is not None and asyncio.iscoroutinefunction(func):
            return True
    return False


def run_cb_loop(lit_api: LitAPI, socket: zmq.Socket, model_name: str, log: logging.Logger):
    """Autonomous continuous batching loop.

    Supports both sync and async ``prefill`` / ``step`` / ``has_finished`` /
    ``encode_response`` / ``on_request`` / ``on_response`` / ``decode_request``.
    When async methods are detected a dedicated event loop is created inside
    the step thread.
    """
    active: dict[str, CBState] = {}
    lock = threading.Lock()

    use_async = _has_async_methods(lit_api)
    cb_loop: asyncio.AbstractEventLoop | None = None
    if use_async:
        cb_loop = asyncio.new_event_loop()

    def _call_async(coro):
        if cb_loop is None:
            raise RuntimeError("No event loop available for async CB method")
        return cb_loop.run_until_complete(coro)

    def _invoke_method(func, *args, **kwargs):
        """Invoke ``func`` synchronously, driving it via the CB event loop when async."""
        if func is None:
            return None
        if asyncio.iscoroutinefunction(func):
            return _call_async(_maybe_await(func, *args, **kwargs))
        result = func(*args, **kwargs)
        if asyncio.iscoroutine(result):
            return _call_async(result)
        return result

    def _handle_add(cb_add: CBAddRequest):
        raw = json.loads(cb_add.data) if cb_add.data else {}
        meta = _meta_from_proto(cb_add.meta) if cb_add.HasField("meta") else RequestMeta(
            route="", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=raw,
        )

        # on_request moved before decode
        try:
            if hasattr(lit_api, "on_request"):
                raw = _invoke_method(lit_api.on_request, raw, meta)
        except Exception as e:
            err_resp = _make_error_response(cb_add.uid, str(e))
            socket.send(err_resp.SerializeToString())
            return

        if hasattr(lit_api, "decode_request"):
            decoded = _invoke_method(lit_api.decode_request, raw)
        else:
            decoded = raw

        state = CBState(cb_add.uid, decoded, meta)
        active[cb_add.uid] = state

        try:
            _invoke_method(lit_api.prefill, cb_add.uid, decoded)
            state.prefilled = True
        except Exception as e:
            del active[cb_add.uid]
            err_resp = _make_error_response(cb_add.uid, f"prefill failed: {e}")
            socket.send(err_resp.SerializeToString())

    def _handle_remove(cb_remove: CBRemoveRequest):
        active.pop(cb_remove.uid, None)

    def step_loop():
        if cb_loop is not None:
            asyncio.set_event_loop(cb_loop)
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
                    outputs = _invoke_method(lit_api.step, ready)
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
                    if _invoke_method(lit_api.has_finished, state.uid, token, state.output):
                        completed.append(state.uid)

                for uid in completed:
                    state = active.pop(uid)
                    try:
                        if hasattr(lit_api, "encode_response"):
                            encoded = _invoke_method(lit_api.encode_response, state.output)
                        else:
                            encoded = state.output
                        if hasattr(lit_api, "on_response"):
                            encoded = _invoke_method(lit_api.on_response, encoded, state.meta)
                        resp_bytes = json.dumps(encoded).encode()
                        metrics = _collect_metrics(lit_api)
                        resp = Response(
                            uid=uid,
                            single=SingleResponse(data=resp_bytes, status=_make_status(True)),
                            metrics=metrics,
                        )
                        socket.send(resp.SerializeToString())
                    except Exception as e:
                        log.error("cb encode error for %s: %s", uid, e, exc_info=True)
                        err_resp = _make_error_response(uid, f"encode failed: {e}")
                        socket.send(err_resp.SerializeToString())

            time.sleep(0.001)

    step_thread = threading.Thread(target=step_loop, daemon=True)
    step_thread.start()

    try:
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
    finally:
        if cb_loop is not None:
            cb_loop.close()


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

@contextmanager
def _protect_stdout():
    """Redirect fd 1 (stdout) to stderr during model loading.

    C-level inference libraries (CANN, ONNX Runtime, MagicMind, etc.) often
    write init logs directly to fd 1. Since lite-server uses the first line of
    stdout for the worker-ready handshake, any pollution causes a JSON parse
    error and the worker is treated as crashed.

    This context manager redirects fd 1 → stderr at the OS level so that
    library init output is still captured (via Rust's stderr → tracing
    forwarder) while keeping the stdout channel clean for the protocol.
    """
    saved = os.dup(1)
    try:
        os.dup2(2, 1)
        yield
    finally:
        os.dup2(saved, 1)
        os.close(saved)


def _run_teardown(lit_api, log):
    if hasattr(lit_api, "teardown"):
        try:
            lit_api.teardown()
        except Exception as e:
            log.error(f"teardown error: {e}")


def worker_main():
    import zmq

    args = parse_args()

    log = setup_logging(args.worker_id, args.log_level)

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

    if args.continuous_batching or config.get("continuous_batching", False):
        context = zmq.Context()
        socket = context.socket(zmq.PAIR)
        try:
            socket.connect(args.endpoint)
            socket.setsockopt(zmq.LINGER, 0)
            log.info(f"Connected ZMQ PAIR to {args.endpoint}")
            run_cb_loop(lit_api, socket, args.model_name, log)
        except KeyboardInterrupt:
            log.info("Interrupted, shutting down")
        finally:
            _run_teardown(lit_api, log)
            socket.close()
            context.term()
    elif _is_async_api(lit_api):
        import zmq.asyncio

        async_context = zmq.asyncio.Context()
        async_socket = async_context.socket(zmq.PAIR)
        try:
            async_socket.connect(args.endpoint)
            async_socket.setsockopt(zmq.LINGER, 0)
            log.info(f"Connected async ZMQ PAIR to {args.endpoint}")
            asyncio.run(run_async_loop(lit_api, async_socket, args.model_name, log))
        except KeyboardInterrupt:
            log.info("Interrupted, shutting down")
        finally:
            _run_teardown(lit_api, log)
            async_socket.close()
            async_context.term()
    else:
        context = zmq.Context()
        socket = context.socket(zmq.PAIR)
        try:
            socket.connect(args.endpoint)
            socket.setsockopt(zmq.LINGER, 0)
            log.info(f"Connected ZMQ PAIR to {args.endpoint}")
            run_standard_loop(lit_api, socket, args.model_name, log)
        except KeyboardInterrupt:
            log.info("Interrupted, shutting down")
        finally:
            _run_teardown(lit_api, log)
            socket.close()
            context.term()


if __name__ == "__main__":
    worker_main()
