"""Python inference worker entry point for lite-server (ZMQ + Protobuf).

All models run on a single unified asyncio loop.  Sync model methods are
adapted at load time by the Pipeline (inline for fully-sync models, or a
single-thread executor when anything is async), so there is exactly one
request path for single / batch / streaming / bidi / continuous batching.
"""

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

from lite_server.api import LitAPI
from lite_server.callbacks import load_callbacks
from lite_server.context import CBSequence, Headers, RequestContext, RequestMeta
from lite_server.exceptions import HTTPException
from lite_server.pipeline import (
    Pipeline,
    _adapt,
    _wrap_ctx_method,
    extract_response_meta,
)
from lite_server.proto import (
    BatchRequest,
    CBAddRequest,
    CBRemoveRequest,
    Request,
    Response,
    SingleResponse,
    Status,
    StreamChunkResponse,
    StreamDone,
    StreamError,
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
    parser.add_argument("--log-level", default="info", help="Logging level: debug, info, warn, error")
    parser.add_argument("--server-http", default=None,
                        help="Loopback HTTP base URL of the hosting server "
                             "(e.g. http://127.0.0.1:8000); backs ctx.server "
                             "for @route handlers")
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
        s = f"{record.pathname}:{record.lineno} {record.getMessage()}"
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + record.exc_text
        if record.stack_info:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + self.formatStack(record.stack_info)
        # Prefix every line so the Rust stderr parser forwards traceback
        # lines at the correct tracing level.
        return '\n'.join(f"[{prefix}] {ln}" for ln in s.split('\n'))


def setup_logging(worker_id: int, level_str: str = "info"):
    """Configure worker logging: plain text to stderr (captured by Rust).

    Configures the root logger so that all child loggers (including the
    user's model logger via ``LitAPI.logger``) inherit the handler and level.
    Also ensures the ``lite_server`` namespace logger has a handler so builtin
    callback logs are never silently lost.
    """
    level = getattr(logging, level_str.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)
    if root.handlers:
        handler = root.handlers[0]  # reuse an existing handler (e.g. basicConfig)
    else:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(_LevelPrefixFormatter())
        root.addHandler(handler)
    # Ensure lite_server namespace has a path to stderr, without double-emitting
    # via root propagation (B4).
    ls_logger = logging.getLogger("lite_server")
    ls_logger.setLevel(level)
    ls_logger.propagate = False
    if not ls_logger.handlers:
        ls_logger.addHandler(handler)
    return logging.getLogger("inference_worker")


def load_model_config(config_path: str):
    if not os.path.exists(config_path):
        return {}
    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}


def _get_pipeline(lit_api: LitAPI) -> Pipeline:
    """Return the instance's Pipeline, building an empty one on demand.

    ``load_litapi`` always builds the pipeline; the fallback keeps tests and
    direct handler use working without the full loader.
    """
    pipe = getattr(lit_api, "_pipeline", None)
    if pipe is None:
        pipe = Pipeline.build(lit_api, [])
        lit_api._pipeline = pipe
    return pipe


def _discover_routes(instance: LitAPI) -> None:
    """Collect ``@route``-decorated methods onto ``instance._route_handlers``.

    Scans the instance for attributes carrying ``__route_defs__`` (set by the
    ``@route`` decorator on the underlying function; bound methods delegate the
    lookup). Validates each handler's signature against the 0.7 ctx contract,
    binds ``self``, and unions methods when decorators are stacked on one
    method. Result:

      ``instance._route_handlers`` — ``dict[path, (bound_handler, [methods])]``

    Raises :class:`HandlerSignatureError` on a bad signature or on two distinct
    handlers declaring the same path — both fail worker startup loudly.
    """
    from lite_server.route import HandlerSignatureError, _validate_handler_signature

    handlers: dict[str, list] = {}  # path -> [bound_handler, methods_list]
    for name in dir(instance):
        try:
            bound = getattr(instance, name)
        except AttributeError:
            continue
        defs = getattr(bound, "__route_defs__", None)
        if not defs:
            continue
        func = getattr(bound, "__func__", bound)
        for rd in defs:
            _validate_handler_signature(bound, rd.path)
            existing = handlers.get(rd.path)
            if existing is None:
                handlers[rd.path] = [bound, list(rd.methods)]
            elif getattr(existing[0], "__func__", existing[0]) is func:
                # Same method, stacked decorators (e.g. @route.get + .post):
                # union the HTTP methods.
                for m in rd.methods:
                    if m not in existing[1]:
                        existing[1].append(m)
            else:
                raise HandlerSignatureError(
                    f"Route {rd.path!r} is declared on multiple handlers "
                    f"({existing[0].__func__.__qualname__}, "
                    f"{func.__qualname__}); one handler per path."
                )
    instance._route_handlers = {p: (h, ms) for p, (h, ms) in handlers.items()}


def load_litapi(model_py_path: str, config: dict, device: str = "cpu"):
    model_dir = os.path.dirname(os.path.abspath(model_py_path))

    # Protect stdout during model module import and setup.
    # C-level inference libraries (CANN, ONNX Runtime, MagicMind, etc.) may
    # write init logs directly to fd 1, which breaks the worker-ready handshake
    # protocol that expects the first stdout line to be valid JSON.
    with _protect_stdout():
        sys.path.insert(0, model_dir)
        try:
            spec = importlib.util.spec_from_file_location("model_module", model_py_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        finally:
            sys.path.remove(model_dir)

        LitAPIClass = None
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, type) and issubclass(attr, LitAPI) and attr is not LitAPI:
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

        # Load callbacks AFTER the class attribute (LitAPI.callbacks) is
        # readable — class-attr callbacks support constructor arguments
        # and take priority over config.yaml entries.
        sys.path.insert(0, model_dir)
        try:
            callbacks = load_callbacks(config, instance)
        finally:
            sys.path.remove(model_dir)

        pipeline = Pipeline.build(instance, callbacks)

        pipeline.trigger_lifecycle("on_before_setup", config, device)
        if hasattr(instance, "pre_setup"):
            instance.pre_setup()
        instance.setup(device)
        pipeline.trigger_lifecycle("on_after_setup", instance)

    # Store pipeline on the instance for later use
    instance._pipeline = pipeline

    # Discover @route handlers and build the shared route pipeline (reuses the
    # model's global callback chain; per-route callbacks are out of scope —
    # constraint #3, gateway concern).
    _discover_routes(instance)
    if instance._route_handlers:
        instance._route_pipeline = Pipeline.for_route(callbacks)

    return instance


def _make_status(ok: bool, message: str = "") -> Status:
    return Status(code="Ok" if ok else "Error", message=message)


def _make_error_response(uid: str, message: str,
                         status_code: int | None = None,
                         error_type: str | None = None,
                         code: str | None = None,
                         param: str | None = None,
                         headers: dict[str, str] | None = None) -> Response:
    # Default unexpected worker exceptions to a structured 500 server_error.
    # Carrying the HTTP status code in Status.message lets the Rust side route
    # these through ModelError (handlers.rs) so the client sees the real error
    # instead of a sanitized WORKER_CRASHED. WorkerCrashed must be reserved for
    # cases where the worker process is actually dead.
    if status_code is None:
        status_code = 500
        if error_type is None:
            error_type = "server_error"
    # Four-field error body contract: code/param are always present (null
    # when unset), matching the Rust HTTP error response shape.
    error_dict: dict = {
        "type": error_type or "model_error",
        "message": message,
        "code": code,
        "param": param,
    }
    data = json.dumps({"error": error_dict}).encode()
    status = Status(code="Error", message=str(status_code))
    single = SingleResponse(data=data, status=status)
    if headers:
        single.headers.update({str(k): str(v) for k, v in headers.items()})
    return Response(uid=uid, single=single)


def _merge_err_headers(ctx: RequestContext, e: Exception) -> dict[str, str] | None:
    """Headers to attach to an error frame: ctx.response_headers (e.g. from a
    Cors callback) first, then the exception's own headers (e.g. Retry-After
    on 429/503) win. Returns None when neither is set."""
    hdrs = dict(ctx.response_headers)
    extra = getattr(e, "headers", None)
    if extra:
        hdrs.update(extra)
    return hdrs or None


def _parse_json_payload(data: bytes | None) -> dict:
    """Parse request JSON, raising HTTPException(400) on invalid JSON so the
    failure flows through normal error handling (on_error + error frame)
    instead of escaping the pipeline try (P3). Empty/absent body → {}."""
    if not data:
        return {}
    try:
        return json.loads(data)
    except json.JSONDecodeError as e:
        raise HTTPException(
            400, f"invalid JSON in request body: {e}",
            error_type="invalid_request_error", code="invalid_json",
        ) from e


def _format_exc_brief(exc: BaseException) -> str:
    """Exception type, message, and where it raised — on one short line.

    Used for the default-level ERROR log so a failure can be located (the
    deepest frame is almost always the user's model.py) WITHOUT dumping a
    multi-line traceback: the Rust stderr forwarder splits on newlines, so a
    multi-line traceback would explode one failure into many log events, and
    a very long single line is not reliably forwarded either. The full
    multi-line traceback is logged separately at DEBUG.
    """
    frames = traceback.extract_tb(exc.__traceback__) if exc.__traceback__ else []
    if frames:
        fr = frames[-1]
        return f"{type(exc).__name__}: {exc} @ {fr.filename}:{fr.lineno} in {fr.name}"
    return f"{type(exc).__name__}: {exc}"


def _make_stream_error(stream_id: str, message: str,
                       error_type: str | None = None,
                       code: str | None = None,
                       param: str | None = None) -> Response:
    if error_type is not None:
        # Structured error for model-level HTTPException in streaming.
        # The StreamError.message contains a JSON object that the Rust/tonic
        # side parses to produce a structured error event.
        # code/param are always present (null when unset) — see _make_error_response.
        error_dict: dict = {
            "type": error_type,
            "message": message,
            "code": code,
            "param": param,
        }
        msg = json.dumps({"error": error_dict})
    else:
        msg = message
    return Response(
        uid=f"stream-error-{stream_id}",
        stream=StreamResponse(
            stream_id=stream_id,
            error=StreamError(message=msg),
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
    # meta.payload is no longer decoded: nothing in the framework reads it,
    # and the body is already decoded once from item data.  (Proto field
    # stays on the wire; Rust may stop sending it in a later release.)
    # method defaults to POST when unset (inference never sets it; routes do).
    return RequestMeta(
        route=meta_pb.route,
        headers=Headers(dict(meta_pb.headers)),
        client_ip=meta_pb.client_ip,
        request_id=meta_pb.request_id,
        timestamp_ns=meta_pb.timestamp_ns,
        method=meta_pb.method or "POST",
        query=dict(meta_pb.query),
    )


def _build_single_response(uid: str, resp_bytes: bytes, status: Status,
                           resp_headers: dict[str, str] | None, metrics) -> Response:
    """Assemble a SingleResponse proto, unpacking embedded status/media type."""
    sc, mt, clean_headers = extract_response_meta(resp_headers)
    single_resp = SingleResponse(data=resp_bytes, status=status,
                                 status_code=sc, media_type=mt)
    if clean_headers:
        single_resp.headers.update(clean_headers)
    return Response(uid=uid, single=single_resp, metrics=metrics)


# ---------------------------------------------------------------------------
# Main Loop (unified async)
# ---------------------------------------------------------------------------

async def run_async_loop(lit_api: LitAPI, socket, model_name: str, log: logging.Logger):
    """Handle single + batch + stream requests asynchronously."""
    pending_tasks: dict[str, asyncio.Task] = {}
    active_streams: dict[str, asyncio.Task] = {}

    cancelled = False
    try:
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
                # All stream actions are handled inline: bidi chunk/close
                # ordering is protocol-significant (each chunk expects its ack
                # in order), and open must register the session before the next
                # message arrives.  Uni-directional consumption still runs in
                # its own task, created by the open handler.
                try:
                    await _handle_stream_async(lit_api, request, socket, active_streams, log)
                except Exception as e:
                    log.error("stream %s action %s failed: %s", stream_id, action, _format_exc_brief(e))
                if action == "open":
                    # Track the consume task for exception retrieval / shutdown
                    entry = active_streams.get(stream_id)
                    if isinstance(entry, asyncio.Task):
                        pending_tasks[f"stream-consume-{stream_id}"] = entry
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
                if t.cancelled():
                    continue  # t.exception() raises CancelledError on cancelled tasks
                exc = t.exception()
                if exc is not None:
                    log.error("async task %s failed: %s", uid, exc)
    except asyncio.CancelledError:
        # Task cancelled (e.g. asyncio.run SIGINT shutdown): fall through to
        # the shutdown cleanup below instead of skipping it, then re-raise.
        cancelled = True

    # Cancel any pending tasks on shutdown
    for uid, t in list(pending_tasks.items()):
        if not t.done():
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    # Close bidi sessions still open at shutdown so handlers can release
    # per-session resources (on_close is exception-isolated).
    for sid, entry in list(active_streams.items()):
        if isinstance(entry, _BidiSession):
            await _close_bidi_quietly(entry.on_close, entry.ctx, sid, log)
            active_streams.pop(sid, None)

    if cancelled:
        raise asyncio.CancelledError()


# ---------------------------------------------------------------------------
# Continuous Batching Loop
# ---------------------------------------------------------------------------

def run_cb_loop(lit_api: LitAPI, socket: zmq.Socket, model_name: str, log: logging.Logger):
    """Autonomous continuous batching loop.

    ``prefill`` / ``step`` / ``has_finished`` may be sync or async; all are
    driven on a dedicated event loop inside the step thread.  Requests flow
    through the Pipeline, so CB gets the same hook coverage (on_request →
    decode → on_input on add; on_output → encode → on_response on complete)
    and early-return support as every other mode.
    """
    active: dict[str, CBSequence] = {}
    lock = threading.Lock()
    pipe = _get_pipeline(lit_api)
    cb_loop = asyncio.new_event_loop()

    def _drive(coro):
        return cb_loop.run_until_complete(coro)

    prefill = _wrap_ctx_method(lit_api.prefill, "prefill")
    step_fn = _adapt(lit_api.step)  # step prohibits ctx (validated at load)
    has_finished = _wrap_ctx_method(lit_api.has_finished, "has_finished")

    def _send_ctx_response(uid: str, ctx: RequestContext):
        resp_bytes, status, metrics, resp_headers = pipe.finalize(ctx)
        response = _build_single_response(uid, resp_bytes, status, resp_headers, metrics)
        socket.send(response.SerializeToString())

    def _handle_add(cb_add: CBAddRequest):
        meta = _meta_from_proto(cb_add.meta) if cb_add.HasField("meta") else RequestMeta(
            route="", headers=Headers(), client_ip="", request_id="", timestamp_ns=0,
        )
        ctx = RequestContext(meta=meta, request={})
        try:
            ctx.request = _parse_json_payload(cb_add.data)
            _drive(pipe.preprocess(ctx))
        except HTTPException as e:
            _drive(pipe.run_on_error(ctx, e))
            err_resp = _make_error_response(cb_add.uid, e.detail, status_code=e.status_code, error_type=e.error_type, code=e.code, param=e.param, headers=_merge_err_headers(ctx, e))
            socket.send(err_resp.SerializeToString())
            return
        except Exception as e:
            _drive(pipe.run_on_error(ctx, e))
            err_resp = _make_error_response(cb_add.uid, str(e), headers=_merge_err_headers(ctx, e))
            socket.send(err_resp.SerializeToString())
            return
        if ctx.early is not None:
            # Early return (e.g. cache hit in on_request): respond now,
            # skip prefill entirely.
            _send_ctx_response(cb_add.uid, ctx)
            return

        state = CBSequence(cb_add.uid, ctx)
        active[cb_add.uid] = state

        try:
            _drive(prefill(cb_add.uid, ctx.input, ctx=ctx))
            state.prefilled = True
        except HTTPException as e:
            del active[cb_add.uid]
            _drive(pipe.run_on_error(ctx, e))
            err_resp = _make_error_response(cb_add.uid, e.detail, status_code=e.status_code, error_type=e.error_type, code=e.code, param=e.param, headers=_merge_err_headers(ctx, e))
            socket.send(err_resp.SerializeToString())
        except Exception as e:
            del active[cb_add.uid]
            _drive(pipe.run_on_error(ctx, e))
            err_resp = _make_error_response(cb_add.uid, f"prefill failed: {e}", headers=_merge_err_headers(ctx, e))
            socket.send(err_resp.SerializeToString())

    def _handle_remove(cb_remove: CBRemoveRequest):
        active.pop(cb_remove.uid, None)

    def step_loop():
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
                    outputs = _drive(step_fn(ready))
                except HTTPException as e:
                    log.warning("cb step rejected: %s", e.detail)
                    for state in list(active.values()):
                        _drive(pipe.run_on_error(state.ctx, e))
                        err_resp = _make_error_response(state.uid, e.detail, status_code=e.status_code, error_type=e.error_type, code=e.code, param=e.param, headers=_merge_err_headers(state.ctx, e))
                        socket.send(err_resp.SerializeToString())
                    active.clear()
                    continue
                except Exception as e:
                    log.error("cb step error: %s", _format_exc_brief(e))
                    for state in list(active.values()):
                        _drive(pipe.run_on_error(state.ctx, e))
                        err_resp = _make_error_response(state.uid, f"step failed: {e}", headers=_merge_err_headers(state.ctx, e))
                        socket.send(err_resp.SerializeToString())
                    active.clear()
                    continue

                completed = []
                for state, token in zip(ready, outputs):
                    state.output.append(token)
                    # B8: has_finished raising must not escape — the step loop
                    # runs on a daemon thread, so an uncaught exception here
                    # would kill it and hang every subsequent CB request.
                    try:
                        finished = _drive(has_finished(state.uid, token, state.output, ctx=state.ctx))
                    except Exception as e:
                        log.error("cb has_finished error for %s: %s", state.uid, _format_exc_brief(e))
                        _drive(pipe.run_on_error(state.ctx, e))
                        active.pop(state.uid, None)
                        socket.send(_make_error_response(
                            state.uid, f"has_finished failed: {e}",
                            headers=_merge_err_headers(state.ctx, e),
                        ).SerializeToString())
                        continue
                    if finished:
                        completed.append(state.uid)

                for uid in completed:
                    state = active.pop(uid)
                    state.ctx.output = state.output
                    state.ctx.early = None
                    try:
                        _drive(pipe.postprocess(state.ctx))
                        _send_ctx_response(uid, state.ctx)
                    except HTTPException as e:
                        log.warning("cb encode rejected for %s: %s", uid, e.detail)
                        _drive(pipe.run_on_error(state.ctx, e))
                        err_resp = _make_error_response(uid, e.detail, status_code=e.status_code, error_type=e.error_type, code=e.code, param=e.param, headers=_merge_err_headers(state.ctx, e))
                        socket.send(err_resp.SerializeToString())
                    except Exception as e:
                        log.error("cb encode error for %s: %s", uid, _format_exc_brief(e))
                        _drive(pipe.run_on_error(state.ctx, e))
                        err_resp = _make_error_response(uid, f"encode failed: {e}", headers=_merge_err_headers(state.ctx, e))
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
                elif request.HasField("single"):
                    # Route standard SingleRequest through CB pipeline
                    cb_add = CBAddRequest()
                    cb_add.uid = request.uid
                    cb_add.data = request.single.data
                    if request.HasField("meta"):
                        cb_add.meta.CopyFrom(request.meta)
                    _handle_add(cb_add)
    finally:
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
    # Fire callback teardown hooks
    pipe = getattr(lit_api, "_pipeline", None)
    if pipe is not None:
        pipe.trigger_lifecycle("on_teardown", lit_api)
    try:
        lit_api.teardown()
    except Exception as e:
        log.error(f"teardown error: {e}")
    if pipe is not None:
        pipe.close()


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

    # One ServerProxy per worker, shared by all @route calls (cheap: no I/O
    # at construction; queries go out lazily per call).
    if args.server_http:
        from lite_server.server_proxy import ServerProxy
        lit_api._server_proxy = ServerProxy.for_model(
            args.server_http, args.model_name, args.version)

    specs = [{"name": s.name, "metric_type": s.metric_type}
             for s in getattr(lit_api, '_metric_specs', [])]
    pipeline = getattr(lit_api, "_pipeline", None)
    custom_routes = [
        {"route": path, "methods": methods}
        for path, (_handler, methods) in getattr(lit_api, "_route_handlers", {}).items()
    ]
    print(json.dumps({
        "status": "ready",
        "worker_id": args.worker_id,
        "metric_specs": specs,
        "custom_routes": custom_routes,
    }), flush=True)

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
    else:
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

# ---------------------------------------------------------------------------
# Re-exports: dispatch / streaming were split out of this module (0.7.7).
# The historical import path ``lite_server.worker.inference.X`` keeps working
# for callers and tests. These imports sit at the bottom so this module's
# helpers are fully defined before the sibling modules import them back.
from lite_server.worker.dispatch import (  # noqa: E402,F401
    _build_route_response,
    _handle_batch,
    _handle_file_changed,
    _handle_request_async,
    _handle_route_call,
)
from lite_server.worker.streaming import (  # noqa: E402,F401
    _BidiSession,
    _close_bidi_quietly,
    _consume_stream,
    _handle_stream_async,
    _handle_stream_chunk_async,
    _handle_stream_open_async,
    _next_or_sentinel,
    _process_stream_chunk,
    _send_bidi_final_chunk,
    _send_route_stream,
    _send_stream_early,
    _serialize_route_chunk,
)

if __name__ == "__main__":
    worker_main()