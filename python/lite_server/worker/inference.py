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
from lite_server.context import Headers, RequestContext, RequestMeta
from lite_server.exceptions import HTTPException
from lite_server.pipeline import (
    Pipeline,
    extract_response_meta,
)
from lite_server.worker.common import (  # noqa: F401 — re-export; the
# historical ``lite_server.worker.inference.X`` path keeps working.
    _build_single_response,
    _format_exc_brief,
    _get_pipeline,
    _make_error_response,
    _make_status,
    _make_stream_chunk,
    _make_stream_done,
    _make_stream_error,
    _merge_err_headers,
    _meta_from_proto,
    _parse_json_payload,
)
from lite_server.proto import (
    BatchRequest,
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

        pipeline.trigger_lifecycle("before_setup", config, device)
        instance.setup(device)
        pipeline.trigger_lifecycle("after_setup", instance)

    # Store pipeline on the instance for later use
    instance._pipeline = pipeline

    # Discover @route handlers and build the shared route pipeline (reuses the
    # model's global callback chain; per-route callbacks are out of scope —
    # constraint #3, gateway concern).
    _discover_routes(instance)
    if instance._route_handlers:
        instance._route_pipeline = Pipeline.for_route(callbacks)

    return instance


def _start_parent_watchpoint(log: logging.Logger, server_pid=None):
    """Best-effort 1Hz self-terminate if our parent (the server) dies.

    Production-default (was test-only, gated by LITESERVER_DIE_WITH_PARENT).
    Covers SIGKILL / abort of the server — paths its own watchdog and
    ``kill_on_drop`` cannot reach. ``os._exit`` is intentional: the ZMQ sockets
    are dead once the server is gone, so graceful teardown would just hang.
    Best-effort 1Hz — inference must yield to the asyncio loop for the tick to
    fire. Returns the watcher task (caller holds the strong ref) or exits the
    process immediately if already orphaned at startup.

    *server_pid* is the pid of the server process captured when ``worker_main``
    first runs — before any model loading or I/O that could delay execution.
    Two outcomes are possible at this point:

    * **Match** — the current ppid still equals *server_pid*: the server is
      alive; install a 1 Hz watcher that exits the worker if the ppid ever
      changes.
    * **Mismatch** — the ppid already differs from *server_pid* (e.g. the OS
      reparented us to init 1): the server died during worker init; exit
      immediately so we don't orphan.

    When *server_pid* is ``None`` (legacy callers / tests) the old heuristic
    is preserved: ppid == 1 is assumed to mean orphaned.  Production code
    always sets *server_pid* explicitly, which fixes a false positive when
    lite-server itself is pid 1 inside a container.
    """
    parent_pid = os.getppid()

    if server_pid is not None:
        # Precise check: bail out only if the ppid already changed.
        if parent_pid != server_pid:
            log.warning(
                "server pid %s but parent is %s; server died during worker init",
                server_pid, parent_pid,
            )
            os._exit(0)
    else:
        # Legacy heuristic (no server_pid captured) — kept for back-compat.
        if parent_pid == 1:
            os._exit(0)

    async def _watch_parent():
        while True:
            await asyncio.sleep(1)
            if os.getppid() != parent_pid:
                log.warning(
                    "parent (server) pid %s exited; worker self-terminating",
                    parent_pid,
                )
                os._exit(0)

    return asyncio.create_task(_watch_parent())


def _start_parent_watchpoint_sync(log: logging.Logger, server_pid=None, stop_event=None):
    """Daemon-thread variant of :func:`_start_parent_watchpoint` for use with
    the synchronous CB loop.  Same semantics: compare *server_pid* against the
    live ``getppid()``; return a started daemon thread that exits the process
    if the parent ever changes.  See the async version for details on the
    *server_pid* contract.

    *stop_event* is a test hook: when given, the 1 Hz tick waits on it and the
    thread returns cleanly once set — letting tests ``join()`` the thread
    before monkeypatch teardown restores the real ``os.getppid``/``os._exit``
    (an unstoppable thread would fire the real ``os._exit`` in the middle of
    the suite).  Production never passes it: the daemon thread dies with the
    process.
    """
    parent_pid = os.getppid()

    if server_pid is not None:
        if parent_pid != server_pid:
            log.warning(
                "server pid %s but parent is %s; server died during worker init",
                server_pid, parent_pid,
            )
            os._exit(0)
    else:
        if parent_pid == 1:
            os._exit(0)

    def _watch():
        while True:
            if stop_event is not None:
                if stop_event.wait(1):
                    return
            else:
                time.sleep(1)
            if os.getppid() != parent_pid:
                log.warning(
                    "parent (server) pid %s exited; worker self-terminating",
                    parent_pid,
                )
                os._exit(0)

    t = threading.Thread(target=_watch, daemon=True)
    t.start()
    return t


def _track_task(pending_tasks: dict, active_streams: dict, uid: str, task: asyncio.Task, log: logging.Logger) -> None:
    """Register *task* for shutdown-cancel and exception retrieval.

    Completion cleanup runs in the task's done callback — NOT in a
    per-message scan of the pending map (which cost O(in-flight) on every
    received message and only reaped a finished task when the NEXT message
    arrived). The callback pops the pending entry, clears the stream alias
    for uni-stream consume tasks, and retrieves the exception so asyncio
    never reports "Task exception was never retrieved".
    """
    pending_tasks[uid] = task

    def _on_done(t: asyncio.Task) -> None:
        pending_tasks.pop(uid, None)
        # A uni-stream consume task is also aliased in active_streams.
        for sid, stream_task in list(active_streams.items()):
            if stream_task is t:
                active_streams.pop(sid, None)
                break
        if t.cancelled():
            return  # t.exception() raises CancelledError on cancelled tasks
        exc = t.exception()
        if exc is not None:
            log.error("async task %s failed: %s", uid, exc)

    task.add_done_callback(_on_done)


async def run_async_loop(lit_api: LitAPI, socket, model_name: str, log: logging.Logger, server_pid=None, _pending=None):
    """Handle single + batch + stream requests asynchronously.

    *_pending* is a test hook: when given, it replaces the loop's internal
    pending-task map so tests can observe reaping. Production never passes
    it (mirrors the ``stop_event`` hook on ``_start_parent_watchpoint_sync``).
    """
    pending_tasks: dict[str, asyncio.Task] = _pending if _pending is not None else {}
    active_streams: dict[str, asyncio.Task] = {}

    # Self-terminate if the server (our parent) dies — including SIGKILL, which
    # the server's own watchdog can't catch. Production-default (was test-only).
    # Hold a strong reference so the watcher task isn't garbage-collected.
    _parent_watcher = _start_parent_watchpoint(log, server_pid=server_pid)

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

            if request.HasField("stop"):
                # Graceful stop (server unload / shutdown): break the recv
                # loop — the cleanup below (cancel pending tasks, close bidi
                # sessions) runs, then worker_main's finally fires
                # _run_teardown (LitAPI.teardown + lifecycle callbacks).
                break

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
                        _track_task(pending_tasks, active_streams, f"stream-consume-{stream_id}", entry, log)
            else:
                task = asyncio.create_task(
                    _handle_request_async(lit_api, request, socket, log)
                )
                _track_task(pending_tasks, active_streams, request.uid, task, log)
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
        elif isinstance(entry, _DecoupledSession):
            # P9-1: signal the decoupled sender so the model's push loop can
            # release resources (cooperative cancel, no StreamDone).
            entry.sender.cancel()
            # B5(审计):shutdown 回收也须 fire on_stream_close——消息 cancel
            # 路径对同会话类型 fire "cancel"(streaming.py),uni-stream 任务
            # 取消也经 CancelledError handler fire。
            pipe = _get_pipeline(lit_api)
            if pipe is not None:
                await pipe.run_on_stream_close(entry.ctx, "cancel")
            active_streams.pop(sid, None)

    if cancelled:
        raise asyncio.CancelledError()



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
        pipe.trigger_lifecycle("before_teardown", lit_api)
    try:
        lit_api.teardown()
    except Exception as e:
        log.error(f"teardown error: {e}")
    else:
        # Unload-done signal — mirrors after_setup: fires only on success.
        if pipe is not None:
            pipe.trigger_lifecycle("after_teardown", lit_api)
    if pipe is not None:
        pipe.close()


def worker_main():
    # Capture the server pid *before* anything else — model loading may take
    # many seconds and the server could die in that window.  When we reach
    # _start_parent_watchpoint we compare the live ppid against this snapshot
    # to decide whether we were orphaned during init.  Using a snapshot also
    # fixes the ppid==1 false-positive when lite-server itself is pid 1 in a
    # container (docker commit / docker run without --init).
    server_pid = os.getppid()

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
    ready_payload = json.dumps({
        "status": "ready",
        "worker_id": args.worker_id,
        "metric_specs": specs,
        "custom_routes": custom_routes,
    })

    if args.continuous_batching or config.get("continuous_batching", False):
        context = zmq.Context()
        socket = context.socket(zmq.PAIR)
        try:
            socket.connect(args.endpoint)
            socket.setsockopt(zmq.LINGER, 0)
            # Signal ready only after connect() is issued: the server marks the
            # version ready on this line and a request can arrive immediately —
            # the handshake must already be in flight so the server's send
            # doesn't out-wait its sndtimeo racing a not-yet-connected peer.
            print(ready_payload, flush=True)
            log.info(f"Connected ZMQ PAIR to {args.endpoint}")
            # Start the parent watcher as a daemon thread (CB is sync ZMQ —
            # no asyncio event loop, so we can't use _start_parent_watchpoint).
            _watcher_thread = _start_parent_watchpoint_sync(log, server_pid)
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
            # See above: ready follows connect, never precedes it.
            print(ready_payload, flush=True)
            log.info(f"Connected async ZMQ PAIR to {args.endpoint}")
            asyncio.run(run_async_loop(lit_api, async_socket, args.model_name, log, server_pid=server_pid))
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
from lite_server.worker.cb_loop import CBLoop, run_cb_loop  # noqa: E402,F401
from lite_server.worker.streaming import (  # noqa: E402,F401
    _BidiSession,
    _close_bidi_quietly,
    _consume_stream,
    _DecoupledSession,
    _handle_stream_async,
    _handle_stream_chunk_async,
    _handle_stream_error,
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