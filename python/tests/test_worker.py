"""Tests for lite_server.worker.inference — unified async worker loop.

All models run on the single asyncio loop; sync methods are adapted by the
Pipeline.  Loop-level tests use mock sockets; pipeline-level tests call
``Pipeline.run_single`` directly.  Models must subclass ``LitAPI`` (the
loader and pipeline rely on the base methods existing).
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import textwrap
import threading
import time

import pytest

from lite_server.api import BidiStreamHandler, LitAPI
from lite_server.callbacks import Callback
from lite_server.context import Headers, RequestContext, RequestMeta
from lite_server.pipeline import Pipeline
from lite_server.response import Response as LiteResponse
from lite_server.proto import (
    BatchItem,
    BatchRequest,
    Request,
    Response,
    SingleRequest,
    StreamCancel,
    StreamChunk,
    StreamClose,
    StreamOpen,
    StreamRequest,
)
from lite_server.worker import inference

log = logging.getLogger("test_worker")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

class AsyncMockSocket:
    """Mock zmq.asyncio socket: inject() feeds recv, _msgs captures send."""

    def __init__(self):
        self._msgs = []
        self._incoming = []

    async def send(self, data):
        self._msgs.append(data)

    async def recv(self):
        while not self._incoming:
            await asyncio.sleep(0.001)
        return self._incoming.pop(0)

    def inject(self, data):
        self._incoming.append(data)


class SyncMockSocket:
    """Mock sync zmq socket for the CB loop."""

    def __init__(self):
        self._msgs = []
        self._incoming = []

    def send(self, data):
        self._msgs.append(data)

    def recv(self):
        while not self._incoming:
            time.sleep(0.001)
        return self._incoming.pop(0)

    def inject(self, data):
        self._incoming.append(data)


def drive_loop(model, socket, delay=0.05, timeout=5.0):
    """Run run_async_loop briefly: start → drain injected messages → cancel.

    Waits until the socket's injected queue is consumed (deadline-bounded)
    before the final settle delay, instead of relying on a fixed wall-clock
    sleep alone — a bare fixed delay races under xdist CPU contention and
    flakes (loop cancelled before all messages were processed).
    """

    async def runner():
        task = asyncio.create_task(inference.run_async_loop(model, socket, "test", log))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while socket._incoming and loop.time() < deadline:
            await asyncio.sleep(0.001)
        # Settle: let in-flight handler tasks (created per request) finish.
        await asyncio.sleep(delay)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(runner())


def start_cb_loop(model, socket):
    """Start run_cb_loop on a daemon thread."""
    t = threading.Thread(
        target=inference.run_cb_loop, args=(model, socket, "test_model", log), daemon=True
    )
    t.start()
    return t


def wait_for_response(socket, uid, timeout=5.0) -> Response:
    deadline = time.time() + timeout
    while time.time() < deadline:
        for msg in list(socket._msgs):
            resp = Response()
            resp.ParseFromString(msg)
            if resp.uid == uid:
                return resp
        time.sleep(0.01)
    raise AssertionError(f"No response for {uid} within {timeout}s")


def run_single(model, data=None, meta=None, callbacks=()):
    """Drive Pipeline.run_single synchronously for one request."""
    if data is None:
        data = json.dumps({"input": 5}).encode()
    if meta is None:
        meta = RequestMeta(
            route="/predict", headers=Headers(), client_ip="",
            request_id="", timestamp_ns=0,
        )
    pipe = Pipeline.build(model, list(callbacks))
    return asyncio.run(pipe.run_single(data, meta))


class TestInferenceModule:
    def test_has_parse_args(self):
        assert hasattr(inference, "parse_args")

    def test_has_worker_main(self):
        assert hasattr(inference, "worker_main")

    def test_has_unified_loops(self):
        assert hasattr(inference, "run_async_loop")
        assert hasattr(inference, "run_cb_loop")
        # The old sync standard loop is gone (0.7.0: unified async).
        assert not hasattr(inference, "run_standard_loop")


class TestLoadModelConfig:
    def test_loads_valid_yaml(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("max_batch_size: 4\nstream: true\n")
        assert inference.load_model_config(str(cfg)) == {"max_batch_size": 4, "stream": True}

    def test_missing_file_returns_empty(self, tmp_path):
        assert inference.load_model_config(str(tmp_path / "nope.yaml")) == {}


class TestLoadLitAPI:
    def test_loads_class_with_predict(self, tmp_path):
        model_py = tmp_path / "model.py"
        model_py.write_text(textwrap.dedent('''
            from lite_server import LitAPI

            class MyModel(LitAPI):
                def predict(self, x):
                    return {"output": x * 2}
        '''))
        api = inference.load_litapi(str(model_py), {})
        assert hasattr(api, "predict")
        assert api.predict(5) == {"output": 10}
        # The pipeline is built and attached at load time
        assert isinstance(api._pipeline, Pipeline)

    def test_calls_setup_and_pre_setup(self, tmp_path):
        model_py = tmp_path / "model.py"
        model_py.write_text(textwrap.dedent('''
            from lite_server import LitAPI

            class MyModel(LitAPI):
                def pre_setup(self):
                    self.pre = True
                def setup(self, device):
                    self.dev = device
                def predict(self, x):
                    return x
        '''))
        api = inference.load_litapi(str(model_py), {}, device="cpu:0")
        assert api.pre is True
        assert api.dev == "cpu:0"

    def test_raises_when_no_predict(self, tmp_path):
        model_py = tmp_path / "model.py"
        model_py.write_text(textwrap.dedent('''
            class MyModel:
                pass
        '''))
        with pytest.raises(RuntimeError, match="No LitAPI subclass"):
            inference.load_litapi(str(model_py), {})

    def test_raises_when_not_litapi_subclass(self, tmp_path):
        """Plain classes with predict are rejected — the pipeline relies on
        the LitAPI base methods existing."""
        model_py = tmp_path / "model.py"
        model_py.write_text(textwrap.dedent('''
            class MyModel:
                def __init__(self, **kwargs):
                    pass
                def predict(self, x):
                    return x
        '''))
        with pytest.raises(RuntimeError, match="No LitAPI subclass"):
            inference.load_litapi(str(model_py), {})

    def test_loads_with_local_sibling_import(self, tmp_path):
        """model.py that imports a sibling module (e.g. utils) should load successfully."""
        utils_py = tmp_path / "utils.py"
        utils_py.write_text(textwrap.dedent('''
            def add(x):
                return x + 1
        '''))
        model_py = tmp_path / "model.py"
        model_py.write_text(textwrap.dedent('''
            from lite_server import LitAPI
            from utils import add

            class MyModel(LitAPI):
                def predict(self, x):
                    return {"result": add(x)}
        '''))
        api = inference.load_litapi(str(model_py), {})
        assert api.predict(5) == {"result": 6}

    def test_sys_path_cleaned_after_load(self, tmp_path):
        """model_dir should not remain in sys.path after load_litapi returns."""
        saved_utils = sys.modules.pop("utils", None)
        try:
            utils_py = tmp_path / "utils.py"
            utils_py.write_text("def noop(): pass\n")
            model_py = tmp_path / "model.py"
            model_py.write_text(textwrap.dedent('''
                from lite_server import LitAPI
                from utils import noop

                class MyModel(LitAPI):
                    def predict(self, x):
                        return x
            '''))
            model_dir = str(tmp_path)
            assert model_dir not in sys.path
            inference.load_litapi(str(model_py), {})
            assert model_dir not in sys.path
        finally:
            sys.modules.pop("utils", None)
            if saved_utils is not None:
                sys.modules["utils"] = saved_utils

    def test_loads_callbacks_from_model_dir(self, tmp_path):
        """Callbacks defined in the model directory should be loaded."""
        saved = sys.modules.pop("callbacks", None)
        try:
            callbacks_py = tmp_path / "callbacks.py"
            callbacks_py.write_text(textwrap.dedent('''
                from lite_server.callbacks import Callback

                class Tracker(Callback):
                    def on_request(self, ctx):
                        ctx.request["_hooked"] = True
            '''))

            model_py = tmp_path / "model.py"
            model_py.write_text(textwrap.dedent('''
                from lite_server import LitAPI

                class MyModel(LitAPI):
                    def predict(self, x):
                        return x
            '''))

            config = {"callbacks": ["callbacks.Tracker"]}
            api = inference.load_litapi(str(model_py), config)
            assert hasattr(api, "predict")
            callbacks = api._pipeline.callbacks
            assert len(callbacks) == 1
            assert type(callbacks[0]).__name__ == "Tracker"
        finally:
            sys.modules.pop("callbacks", None)
            if saved is not None:
                sys.modules["callbacks"] = saved

    def test_on_output_callback_ok_when_no_route_handlers(self, tmp_path):
        """A model with on_output callbacks but NO @route handlers should load:
        on_output is valid for the model pipeline (decode→predict→encode).
        Regression test for Pipeline.for_route rejecting valid model callbacks."""
        saved = sys.modules.pop("callbacks", None)
        try:
            callbacks_py = tmp_path / "callbacks.py"
            callbacks_py.write_text(textwrap.dedent('''
                from lite_server.callbacks import Callback

                class AuditLogger(Callback):
                    def on_request(self, ctx):
                        pass

                    def on_output(self, ctx):
                        pass
            '''))
            model_py = tmp_path / "model.py"
            model_py.write_text(textwrap.dedent('''
                from lite_server import LitAPI

                class MyModel(LitAPI):
                    def decode_request(self, request, ctx=None):
                        return request.get("input")

                    def predict(self, x):
                        return x

                    def encode_response(self, output, ctx=None):
                        return {"output": output}
            '''))
            config = {"callbacks": ["callbacks.AuditLogger"]}
            api = inference.load_litapi(str(model_py), config)
            assert hasattr(api, "_pipeline")
            # on_output should be registered on the model pipeline
            assert len(api._pipeline._chains.get("on_output", [])) == 1
            # No @route handlers → _route_pipeline must NOT be built
            assert not hasattr(api, "_route_pipeline")
        finally:
            sys.modules.pop("callbacks", None)
            if saved is not None:
                sys.modules["callbacks"] = saved

    def test_on_output_callback_rejected_when_route_handlers_present(self, tmp_path):
        """When a model has @route handlers AND on_output callbacks, it must
        still be rejected — routes have no encode stage."""
        saved = sys.modules.pop("callbacks", None)
        try:
            callbacks_py = tmp_path / "callbacks.py"
            callbacks_py.write_text(textwrap.dedent('''
                from lite_server.callbacks import Callback

                class AuditLogger(Callback):
                    def on_request(self, ctx):
                        pass

                    def on_output(self, ctx):
                        pass
            '''))
            model_py = tmp_path / "model.py"
            model_py.write_text(textwrap.dedent('''
                from lite_server import LitAPI, route

                class MyModel(LitAPI):
                    def predict(self, x):
                        return x

                    @route.get("/custom")
                    async def my_route(self, ctx):
                        return {"ok": True}
            '''))
            config = {"callbacks": ["callbacks.AuditLogger"]}
            with pytest.raises(RuntimeError, match="on_output"):
                inference.load_litapi(str(model_py), config)
        finally:
            sys.modules.pop("callbacks", None)
            if saved is not None:
                sys.modules["callbacks"] = saved

    def test_old_signature_callback_fails_loudly(self, tmp_path):
        """Pre-0.7 (value, meta) callbacks are rejected at load time."""
        saved = sys.modules.pop("callbacks", None)
        try:
            callbacks_py = tmp_path / "callbacks.py"
            callbacks_py.write_text(textwrap.dedent('''
                from lite_server.callbacks import Callback

                class Old(Callback):
                    def on_before_decode(self, request, meta):
                        return request
            '''))
            model_py = tmp_path / "model.py"
            model_py.write_text(textwrap.dedent('''
                from lite_server import LitAPI

                class MyModel(LitAPI):
                    def predict(self, x):
                        return x
            '''))
            with pytest.raises(RuntimeError, match="on_before_decode"):
                inference.load_litapi(str(model_py), {"callbacks": ["callbacks.Old"]})
        finally:
            sys.modules.pop("callbacks", None)
            if saved is not None:
                sys.modules["callbacks"] = saved


# ---------------------------------------------------------------------------
# Pipeline-level single-request tests
# ---------------------------------------------------------------------------

class TestRunSingle:
    def test_basic_predict(self):
        class MockAPI(LitAPI):
            def predict(self, x):
                return {"output": x.get("input", 0) * 2}

        resp_bytes, status, metrics, _ = run_single(MockAPI())
        assert json.loads(resp_bytes) == {"output": 10}
        assert status.code == "Ok"

    def test_on_request_hook_can_modify(self):
        class HookAPI(LitAPI):
            def on_request(self, ctx):
                ctx.request["input"] = ctx.request["input"] + 1
                return ctx.request

            def predict(self, x):
                return {"output": x["input"] * 2}

        resp_bytes, status, metrics, _ = run_single(HookAPI())
        assert json.loads(resp_bytes) == {"output": 12}

    def test_on_request_hook_can_reject(self):
        class RejectAPI(LitAPI):
            def on_request(self, ctx):
                raise ValueError("rejected")

            def predict(self, x):
                return x

        with pytest.raises(ValueError, match="rejected"):
            run_single(RejectAPI())

    def test_skips_hooks_when_not_implemented(self):
        class PlainAPI(LitAPI):
            def predict(self, x):
                return {"output": x.get("input", 0) * 2}

        resp_bytes, status, metrics, _ = run_single(PlainAPI())
        assert json.loads(resp_bytes) == {"output": 10}

    def test_on_response_with_headers_returns_headers_in_tuple(self):
        class HeaderAPI(LitAPI):
            def predict(self, x):
                return {"output": x.get("input", 0) * 2}

            def on_response(self, ctx):
                return ctx.respond(
                    ctx.response,
                    headers={"X-Custom": "hello", "X-Other": "world"},
                )

        resp_bytes, status, metrics, headers = run_single(HeaderAPI())
        assert json.loads(resp_bytes) == {"output": 10}
        assert status.code == "Ok"
        assert headers == {"X-Custom": "hello", "X-Other": "world"}

    def test_async_on_response_with_headers(self):
        class AsyncHeaderAPI(LitAPI):
            async def predict(self, x):
                return {"output": x.get("input", 0) * 2}

            async def on_response(self, ctx):
                return ctx.respond(
                    ctx.response, headers={"X-Async": "true"},
                )

        resp_bytes, status, metrics, headers = run_single(AsyncHeaderAPI())
        assert json.loads(resp_bytes) == {"output": 10}
        assert headers == {"X-Async": "true"}

    def test_async_predict_with_optional_hooks(self):
        class SimpleAsyncAPI(LitAPI):
            async def predict(self, x):
                return {"output": x["input"] * 3}

        resp_bytes, status, metrics, _ = run_single(SimpleAsyncAPI())
        assert json.loads(resp_bytes) == {"output": 15}
        assert status.code == "Ok"

    def test_mixed_sync_async_hooks(self):
        class MixedAPI(LitAPI):
            async def on_request(self, ctx):
                ctx.request["hooked"] = True
                return ctx.request

            async def predict(self, x):
                return x

            def on_response(self, ctx):
                ctx.response["sync_hook"] = True
                return ctx.response

        resp_bytes, status, metrics, _ = run_single(
            MixedAPI(), data=json.dumps({"input": 1}).encode()
        )
        assert json.loads(resp_bytes) == {"input": 1, "hooked": True, "sync_hook": True}

    def test_async_predict_without_optional_hooks(self):
        class PlainAPI(LitAPI):
            def predict(self, x):
                return {"output": x.get("input", 0) * 2}

            def on_response(self, ctx):
                return {"wrapped": ctx.response}

        resp_bytes, status, metrics, headers = run_single(PlainAPI())
        assert json.loads(resp_bytes) == {"wrapped": {"output": 10}}
        assert headers is None

    def test_no_on_response_still_returns_none_headers(self):
        class NoHookAPI(LitAPI):
            def predict(self, x):
                return {"output": x.get("input", 0) + 1}

        resp_bytes, status, metrics, headers = run_single(NoHookAPI())
        assert json.loads(resp_bytes) == {"output": 6}
        assert headers is None

    def test_async_predict_pipeline(self):
        class AsyncAPI(LitAPI):
            async def decode_request(self, req):
                return {"decoded": req["input"]}

            async def on_request(self, ctx):
                ctx.request["on_request"] = True
                return ctx.request

            async def predict(self, x):
                return {"output": x["decoded"] * 2}

            async def encode_response(self, out):
                return {"encoded": out["output"]}

            async def on_response(self, ctx):
                ctx.response["on_response"] = True
                return ctx.response

        resp_bytes, status, metrics, _ = run_single(AsyncAPI())
        assert json.loads(resp_bytes) == {"encoded": 10, "on_response": True}
        assert status.code == "Ok"


class TestOuterHandlerErrorTraceback:
    """The outer request handler emits exactly one single-line ERROR record
    per failure — visible at the default INFO level so failures can be
    located in the user's model.py — while the client still receives a
    structured 500 (no WORKER_CRASHED regression)."""

    def test_async_loop_logs_traceback_once_at_error(self, caplog):
        class BoomModel(LitAPI):
            def predict(self, x):
                return {"output": 1 / 0}  # ZeroDivisionError, like the user's 1/0

        socket = AsyncMockSocket()
        req = Request(uid="err-async", single=SingleRequest(data=json.dumps({"input": 1}).encode()))
        socket.inject(req.SerializeToString())

        with caplog.at_level(logging.ERROR):
            drive_loop(BoomModel(), socket)

        tb_records = [r for r in caplog.records
                      if r.levelno >= logging.ERROR
                      and "ZeroDivisionError" in r.getMessage()
                      and "predict" in r.getMessage()]
        assert len(tb_records) == 1, (
            "expected exactly one ERROR record for the failure (no explosion), got "
            f"{len(tb_records)}: {[r.getMessage() for r in caplog.records]}"
        )
        msg = tb_records[0].getMessage()
        assert "\n" not in msg, "must stay a single line (Rust forwarder splits on newlines)"
        assert "test_worker.py" in msg, f"location frame missing from: {msg}"

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "err-async"
        assert resp.single.status.code == "Error"
        assert resp.single.status.message == "500"
        assert json.loads(resp.single.data)["error"]["type"] == "server_error"

    def test_cb_loop_logs_step_failure_as_one_error_line(self, caplog):
        """Continuous-batching step() failure must also be one ERROR line."""

        class BoomCBModel(LitAPI):
            def decode_request(self, req):
                return req.get("input", 0)

            def prefill(self, uid, decoded_input):
                pass

            def step(self, active_sequences):
                return [1 / 0 for _ in active_sequences]  # ZeroDivisionError

            def has_finished(self, uid, token, generated_sequence):
                return True

            def encode_response(self, output):
                return output

        socket = SyncMockSocket()
        req = Request()
        req.uid = "cb-err-1"
        req.single.data = json.dumps({"input": 1}).encode()
        socket.inject(req.SerializeToString())

        with caplog.at_level(logging.ERROR):
            start_cb_loop(BoomCBModel(), socket)
            deadline = time.time() + 5
            while time.time() < deadline and not socket._msgs:
                time.sleep(0.01)
            time.sleep(0.3)  # let the ERROR log record settle

        tb_records = [r for r in caplog.records
                      if r.levelno >= logging.ERROR
                      and "ZeroDivisionError" in r.getMessage()
                      and "cb step error" in r.getMessage()]
        assert len(tb_records) == 1, (
            "expected exactly one ERROR record for the cb step failure, got "
            f"{len(tb_records)}: {[r.getMessage() for r in caplog.records]}"
        )
        msg = tb_records[0].getMessage()
        assert "\n" not in msg, "must stay a single line (Rust forwarder splits on newlines)"
        assert "in step" in msg, f"location frame missing from: {msg}"

        assert socket._msgs, "no error response sent for the failed sequence"
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "cb-err-1"
        assert resp.single.status.code == "Error"


class TestMakeErrorResponse:
    def test_error_response_structure(self):
        resp = inference._make_error_response("uid-1", "something broke")
        assert resp.uid == "uid-1"
        assert resp.single.status.code == "Error"
        # Default (no explicit status_code) is a structured 500 server_error
        # so the client sees the real error instead of a sanitized WORKER_CRASHED.
        assert resp.single.status.message == "500"
        body = json.loads(resp.single.data)
        assert body["error"]["type"] == "server_error"
        assert body["error"]["message"] == "something broke"

    def test_error_response_with_code_and_param(self):
        resp = inference._make_error_response(
            "uid-1", "bad input", status_code=400,
            error_type="invalid_request_error",
            code="invalid_input", param="temperature")
        body = json.loads(resp.single.data)
        assert body["error"]["code"] == "invalid_input"
        assert body["error"]["param"] == "temperature"

    def test_error_response_code_param_always_present(self):
        # Four-field error body contract: code/param are always present,
        # null when not set — same shape as the Rust HTTP error responses.
        resp = inference._make_error_response("uid-1", "boom")
        body = json.loads(resp.single.data)
        assert body["error"]["code"] is None
        assert body["error"]["param"] is None


class TestMakeStreamError:
    def test_stream_error_with_code_and_param(self):
        resp = inference._make_stream_error(
            "sid-1", "bad input", error_type="invalid_request_error",
            code="invalid_input", param="temperature")
        body = json.loads(resp.stream.error.message)
        assert body["error"]["type"] == "invalid_request_error"
        assert body["error"]["code"] == "invalid_input"
        assert body["error"]["param"] == "temperature"

    def test_stream_error_code_param_always_present(self):
        resp = inference._make_stream_error("sid-1", "boom", error_type="server_error")
        body = json.loads(resp.stream.error.message)
        assert body["error"]["code"] is None
        assert body["error"]["param"] is None


class TestMetaFromProto:
    def test_decodes_meta(self):
        from lite_server.proto import RequestMeta as ProtoMeta

        meta_pb = ProtoMeta(
            route="/predict",
            headers={"x-auth": "token"},
            client_ip="127.0.0.1",
            request_id="req-1",
            timestamp_ns=123456789,
        )
        meta = inference._meta_from_proto(meta_pb)
        assert meta.route == "/predict"
        assert meta.headers.get("x-auth") == "token"
        assert meta.client_ip == "127.0.0.1"
        assert meta.request_id == "req-1"
        assert meta.timestamp_ns == 123456789
        assert not hasattr(meta, "payload")

    def test_empty_meta(self):
        from lite_server.proto import RequestMeta as ProtoMeta

        meta_pb = ProtoMeta(route="/", headers={}, client_ip="", request_id="", timestamp_ns=0)
        meta = inference._meta_from_proto(meta_pb)
        assert meta.route == "/"
        assert meta.request_id == ""


class TestStdoutProtection:
    """C-level writes to fd 1 during model loading must not pollute the handshake."""

    def test_ready_signal_clean_after_os_write_to_fd1(self, tmp_path):
        """Simulate CANN/ONNX Runtime init writing to fd 1 during setup()."""
        model_py = tmp_path / "model.py"
        model_py.write_text(textwrap.dedent("""\
            import os as _os
            from lite_server import LitAPI

            class MyModel(LitAPI):
                def setup(self, device):
                    _os.write(1, b"[CANN] Initializing NPU...\\n")
                    _os.write(1, b"[CANN] Driver: 8.0.rc1\\n")
                def predict(self, x):
                    return {"output": x * 2}
        """))
        (tmp_path / "config.yaml").write_text("")

        this_dir = os.path.dirname(os.path.abspath(__file__))
        python_dir = os.path.dirname(this_dir)  # python/tests -> python/

        script = textwrap.dedent(f"""\
            import sys, json
            sys.path.insert(0, {repr(python_dir)})
            from lite_server.worker import inference
            api = inference.load_litapi({repr(str(model_py))}, {{}}, device="npu:0")
            print(json.dumps({{"status": "ready", "worker_id": 0}}), flush=True)
        """)

        env = os.environ.copy()
        env["PYTHONPATH"] = python_dir

        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True, text=True, timeout=30,
            cwd=str(tmp_path), env=env,
        )

        stdout_lines = [l for l in result.stdout.strip().split("\n") if l]
        assert len(stdout_lines) == 1, (
            f"Expected exactly 1 line on stdout (the ready signal), "
            f"got {len(stdout_lines)}: {stdout_lines}"
        )
        parsed = json.loads(stdout_lines[0])
        assert parsed["status"] == "ready"
        assert parsed["worker_id"] == 0


# ---------------------------------------------------------------------------
# run_async_loop integration tests
# ---------------------------------------------------------------------------

class TestAsyncLoop:
    def test_async_single_request(self):
        class AsyncModel(LitAPI):
            async def predict(self, x):
                return {"result": x["input"] * 2}

        socket = AsyncMockSocket()
        req = Request(uid="req-1", single=SingleRequest(data=json.dumps({"input": 5}).encode()))
        socket.inject(req.SerializeToString())
        drive_loop(AsyncModel(), socket)

        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "req-1"
        assert json.loads(resp.single.data) == {"result": 10}
        assert resp.single.status.code == "Ok"

    def test_sync_model_on_unified_loop(self):
        """Fully-sync models run on the same asyncio loop (fast path)."""

        class SyncModel(LitAPI):
            def predict(self, x):
                return {"result": x["input"] * 3}

        socket = AsyncMockSocket()
        req = Request(uid="req-sync", single=SingleRequest(data=json.dumps({"input": 4}).encode()))
        socket.inject(req.SerializeToString())
        drive_loop(SyncModel(), socket)

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert json.loads(resp.single.data) == {"result": 12}
        assert resp.single.status.code == "Ok"

    def test_async_health_check(self):
        class AsyncModel(LitAPI):
            async def predict(self, x):
                raise RuntimeError("should not be called")

        socket = AsyncMockSocket()
        req = Request(uid="health-1", single=SingleRequest(data=b""))
        socket.inject(req.SerializeToString())
        drive_loop(AsyncModel(), socket)

        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "health-1"
        assert resp.single.data == b"{}"
        assert resp.single.status.code == "Ok"

    def test_async_batch_request(self):
        class AsyncModel(LitAPI):
            async def predict(self, x):
                return {"result": x["input"] + 1}

        socket = AsyncMockSocket()
        req = Request(
            uid="batch-1",
            batch=BatchRequest(items=[
                BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="i2", data=json.dumps({"input": 2}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())
        drive_loop(AsyncModel(), socket)

        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "batch-1"
        assert len(resp.batch.items) == 2
        assert json.loads(resp.batch.items[0].data) == {"result": 2}
        assert json.loads(resp.batch.items[1].data) == {"result": 3}

    def test_async_error_in_predict(self):
        class BrokenModel(LitAPI):
            async def predict(self, x):
                raise ValueError("boom")

        socket = AsyncMockSocket()
        req = Request(uid="err-1", single=SingleRequest(data=json.dumps({"input": 1}).encode()))
        socket.inject(req.SerializeToString())
        drive_loop(BrokenModel(), socket)

        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "err-1"
        assert resp.single.status.code == "Error"
        assert resp.single.status.message == "500"
        body = json.loads(resp.single.data)
        assert body["error"]["type"] == "server_error"
        assert "boom" in body["error"]["message"]

    def test_async_http_exception_carries_response_headers(self):
        """A6: HTTPException headers (e.g. Retry-After) reach the unary error
        response instead of being silently dropped."""
        from lite_server.exceptions import HTTPException

        class RateLimitedModel(LitAPI):
            async def predict(self, x):
                raise HTTPException(
                    503, "slow down", headers={"Retry-After": "5"}
                )

        socket = AsyncMockSocket()
        req = Request(uid="rl-1", single=SingleRequest(data=json.dumps({"input": 1}).encode()))
        socket.inject(req.SerializeToString())
        drive_loop(RateLimitedModel(), socket)

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.single.status.code == "Error"
        assert resp.single.status.message == "503"
        assert resp.single.headers["Retry-After"] == "5"

    def test_unary_error_carries_ctx_response_headers(self):
        """B6: ctx.response_headers set by a callback reach the unary error
        response via the run_single _response_headers channel."""
        from lite_server.exceptions import HTTPException

        class HeaderCB(Callback):
            def on_request(self, ctx):
                ctx.response_headers["x-uni"] = "1"

        class FailModel(LitAPI):
            async def predict(self, x):
                raise HTTPException(503, "no")

        model = FailModel()
        model._pipeline = Pipeline.build(model, [HeaderCB()])
        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="u", single=SingleRequest(data=json.dumps({"input": 1}).encode()),
        ).SerializeToString())
        drive_loop(model, socket)

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.single.status.code == "Error"
        assert resp.single.headers["x-uni"] == "1"

    def test_unary_plain_exception_carries_on_error_headers(self):
        """Plain exceptions (RuntimeError etc.) carry headers set by on_error
        callback via ctx.response_headers — not just HTTPException."""
        class OnErrorHeaderCB(Callback):
            def on_error(self, ctx, exc):
                ctx.response_headers["X-On-Error"] = "caught"
                ctx.response_headers["X-Exc-Type"] = type(exc).__name__

        class CrashModel(LitAPI):
            async def predict(self, x):
                return 1 / 0  # ZeroDivisionError, not HTTPException

        model = CrashModel()
        model._pipeline = Pipeline.build(model, [OnErrorHeaderCB()])
        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="u", single=SingleRequest(data=json.dumps({"input": 1}).encode()),
        ).SerializeToString())
        drive_loop(model, socket)

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.single.status.code == "Error"
        assert resp.single.status.message == "500"
        assert resp.single.headers["X-On-Error"] == "caught"
        assert resp.single.headers["X-Exc-Type"] == "ZeroDivisionError"

    def test_route_plain_exception_carries_on_error_headers(self):
        """Route path end-to-end: run_route threads ctx.response_headers onto
        the exception and the worker's generic except merges them into the
        error response — parity with the unary single path."""
        from lite_server import route

        class OnErrorHeaderCB(Callback):
            def on_error(self, ctx, exc):
                ctx.response_headers["X-On-Error"] = "caught"
                ctx.response_headers["X-Exc-Type"] = type(exc).__name__

        class RouteModel(LitAPI):
            async def predict(self, x):
                return x

            @route.get("/boom")
            def boom(self, ctx):
                raise ValueError("route crash")

        model = RouteModel()
        inference._discover_routes(model)
        model._route_pipeline = Pipeline.for_route([OnErrorHeaderCB()])
        socket = AsyncMockSocket()
        req = Request(uid="u")
        req.meta.route = "/boom"
        req.meta.method = "GET"
        req.route_call.data = b"{}"
        socket.inject(req.SerializeToString())
        drive_loop(model, socket)

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.single.status.code == "Error"
        assert resp.single.status.message == "500"
        assert resp.single.headers["X-On-Error"] == "caught"
        assert resp.single.headers["X-Exc-Type"] == "ValueError"

    def test_unary_invalid_json_returns_400_and_drives_on_error(self):
        """P3: invalid JSON on the unary path yields a 400 (not 500) and
        drives on_error instead of escaping before the pipeline try."""
        from lite_server.exceptions import HTTPException

        seen = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                seen.append(exc)

        class EchoModel(LitAPI):
            async def predict(self, x):
                return x

        model = EchoModel()
        model._pipeline = Pipeline.build(model, [ErrCB()])
        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="bad", single=SingleRequest(data=b"{not json"),
        ).SerializeToString())
        drive_loop(model, socket)

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.single.status.code == "Error"
        assert resp.single.status.message == "400"
        assert len(seen) == 1
        assert isinstance(seen[0], HTTPException)

    def test_async_stream_with_async_generator(self):
        class AsyncStreamModel(LitAPI):
            async def predict(self, x):
                return x

            async def stream_predict(self, x):
                for i in range(3):
                    await asyncio.sleep(0.001)
                    yield {"token": i, "input": x.get("input")}

        socket = AsyncMockSocket()
        req = Request(
            uid="stream-1",
            stream=StreamRequest(stream_id="s1", open=StreamOpen(data=json.dumps({"input": 42}).encode())),
        )
        socket.inject(req.SerializeToString())
        drive_loop(AsyncStreamModel(), socket)

        # Expect: 3 chunks + 1 done = 4 messages
        assert len(socket._msgs) == 4, f"Expected 4 messages, got {len(socket._msgs)}"
        for i in range(3):
            resp = Response()
            resp.ParseFromString(socket._msgs[i])
            assert resp.stream.stream_id == "s1"
            assert resp.stream.chunk.is_final is False
            data = json.loads(resp.stream.chunk.data)
            assert data["token"] == i
            assert data["input"] == 42
        done_resp = Response()
        done_resp.ParseFromString(socket._msgs[3])
        assert done_resp.stream.HasField("done")

    def test_async_stream_with_sync_generator(self):
        class SyncStreamModel(LitAPI):
            def predict(self, x):
                return x

            def stream_predict(self, x):
                for i in range(3):
                    yield {"token": i, "input": x.get("input")}

        socket = AsyncMockSocket()
        req = Request(
            uid="stream-2",
            stream=StreamRequest(stream_id="s2", open=StreamOpen(data=json.dumps({"input": 99}).encode())),
        )
        socket.inject(req.SerializeToString())
        drive_loop(SyncStreamModel(), socket)

        assert len(socket._msgs) == 4, f"Expected 4 messages, got {len(socket._msgs)}"
        for i in range(3):
            resp = Response()
            resp.ParseFromString(socket._msgs[i])
            data = json.loads(resp.stream.chunk.data)
            assert data["token"] == i
            assert data["input"] == 99

    def test_async_stream_fallback_no_stream_predict(self):
        class NoStreamModel(LitAPI):
            async def predict(self, x):
                return {"fallback": True, "input": x.get("input")}

        socket = AsyncMockSocket()
        req = Request(
            uid="stream-3",
            stream=StreamRequest(stream_id="s3", open=StreamOpen(data=json.dumps({"input": 7}).encode())),
        )
        socket.inject(req.SerializeToString())
        drive_loop(NoStreamModel(), socket)

        # Expect: 1 final chunk + 1 done = 2 messages
        assert len(socket._msgs) == 2, f"Expected 2 messages, got {len(socket._msgs)}"
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.stream.stream_id == "s3"
        assert resp.stream.chunk.is_final is True
        data = json.loads(resp.stream.chunk.data)
        assert data["fallback"] is True
        assert data["input"] == 7

    def test_async_stream_error_in_stream_predict(self):
        class BrokenStreamModel(LitAPI):
            async def stream_predict(self, x):
                yield {"token": 0}
                raise ValueError("stream broke")

        socket = AsyncMockSocket()
        req = Request(
            uid="stream-4",
            stream=StreamRequest(stream_id="s4", open=StreamOpen(data=b"{}")),
        )
        socket.inject(req.SerializeToString())
        drive_loop(BrokenStreamModel(), socket)

        # Expect: 1 chunk + 1 error = 2 messages
        assert len(socket._msgs) == 2, f"Expected 2 messages, got {len(socket._msgs)}"
        resp = Response()
        resp.ParseFromString(socket._msgs[1])
        assert resp.stream.stream_id == "s4"
        assert resp.stream.HasField("error")
        assert "stream broke" in resp.stream.error.message

    def test_stream_predict_open_failure_drives_on_error(self):
        """B5: stream_predict failing at open drives on_error; the stream
        error frame is still sent unchanged."""
        from lite_server.exceptions import HTTPException

        seen = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                seen.append(exc)

        class FailStreamModel(LitAPI):
            def stream_predict(self, x):
                raise HTTPException(400, "stream rejected")

        model = FailStreamModel()
        model._pipeline = Pipeline.build(model, [ErrCB()])
        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="sf", stream=StreamRequest(stream_id="sf", open=StreamOpen(data=b"{}")),
        ).SerializeToString())
        drive_loop(model, socket)

        assert len(seen) == 1
        assert isinstance(seen[0], HTTPException)
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.stream.HasField("error")
        assert "stream rejected" in resp.stream.error.message

    def test_stream_open_invalid_json_drives_on_error(self):
        """P3: invalid JSON at stream open drives on_error and sends a stream
        error frame instead of escaping."""
        from lite_server.exceptions import HTTPException

        seen = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                seen.append(exc)

        class StreamModel(LitAPI):
            async def stream_predict(self, x):
                yield {"t": 1}

        model = StreamModel()
        model._pipeline = Pipeline.build(model, [ErrCB()])
        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="so", stream=StreamRequest(stream_id="so", open=StreamOpen(data=b"{bad")),
        ).SerializeToString())
        drive_loop(model, socket)

        assert len(seen) == 1
        assert isinstance(seen[0], HTTPException)
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.stream.HasField("error")

    def test_async_stream_cancel(self):
        class SlowStreamModel(LitAPI):
            async def stream_predict(self, x):
                for i in range(100):
                    await asyncio.sleep(0.01)
                    yield {"token": i}

        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="stream-open-5",
            stream=StreamRequest(stream_id="s5", open=StreamOpen(data=b"{}")),
        ).SerializeToString())

        async def delayed_cancel():
            await asyncio.sleep(0.03)
            socket.inject(Request(
                uid="stream-cancel-5",
                stream=StreamRequest(stream_id="s5", cancel=StreamCancel()),
            ).SerializeToString())

        async def runner():
            task = asyncio.create_task(
                inference.run_async_loop(SlowStreamModel(), socket, "test", log)
            )
            asyncio.create_task(delayed_cancel())
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        # Some chunks arrived before cancel; the stream task was cancelled
        assert len(socket._msgs) >= 1

    def test_async_concurrent_requests(self):
        class SlowModel(LitAPI):
            def __init__(self):
                super().__init__()
                self.order = []

            async def predict(self, x):
                await asyncio.sleep(0.01)
                self.order.append(x["input"])
                return {"result": x["input"] * 2}

        socket = AsyncMockSocket()
        model = SlowModel()
        socket.inject(Request(uid="c1", single=SingleRequest(data=json.dumps({"input": 1}).encode())).SerializeToString())
        socket.inject(Request(uid="c2", single=SingleRequest(data=json.dumps({"input": 2}).encode())).SerializeToString())
        drive_loop(model, socket)

        assert len(socket._msgs) == 2
        resp_uids = []
        for m in socket._msgs:
            r = Response()
            r.ParseFromString(m)
            resp_uids.append(r.uid)
        assert sorted(resp_uids) == ["c1", "c2"]

    def test_async_protobuf_parse_error(self):
        class AsyncModel(LitAPI):
            async def predict(self, x):
                return x

        socket = AsyncMockSocket()
        socket.inject(b"invalid protobuf bytes")
        drive_loop(AsyncModel(), socket)

        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.single.status.code == "Error"
        assert resp.single.status.message == "500"
        body = json.loads(resp.single.data)
        assert body["error"]["type"] == "server_error"
        assert "Protobuf parse" in body["error"]["message"]

    def test_async_batch_partial_failure(self):
        class PartialFailModel(LitAPI):
            async def predict(self, x):
                if x["input"] == 2:
                    raise ValueError("item 2 fails")
                return {"result": x["input"] * 2}

        socket = AsyncMockSocket()
        req = Request(
            uid="batch-partial",
            batch=BatchRequest(items=[
                BatchItem(uid="ok", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="fail", data=json.dumps({"input": 2}).encode()),
                BatchItem(uid="ok2", data=json.dumps({"input": 3}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())
        drive_loop(PartialFailModel(), socket)

        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "batch-partial"
        assert len(resp.batch.items) == 3
        assert json.loads(resp.batch.items[0].data) == {"result": 2}
        assert resp.batch.items[0].status.code == "Ok"
        assert resp.batch.items[1].status.code == "Error"
        assert "item 2 fails" in resp.batch.items[1].status.message
        assert json.loads(resp.batch.items[2].data) == {"result": 6}
        assert resp.batch.items[2].status.code == "Ok"


class TestAsyncLoopEdgeCases:
    def test_async_loop_cancels_pending_on_shutdown(self):
        class SlowModel(LitAPI):
            async def predict(self, x):
                await asyncio.sleep(10)
                return x

        socket = AsyncMockSocket()
        req = Request(uid="slow-1", single=SingleRequest(data=json.dumps({"input": 1}).encode()))
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(
                inference.run_async_loop(SlowModel(), socket, "test", log)
            )
            await asyncio.sleep(0.02)  # let task start but not finish
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        # The slow task was cancelled on shutdown, no response sent
        assert len(socket._msgs) == 0


# ---------------------------------------------------------------------------
# Batch predict (batch → predict → unbatch)
# ---------------------------------------------------------------------------

class TestBatchPredict:
    def test_batch_predict_full_path(self):
        class BatchModel(LitAPI):
            def batch(self, inputs):
                return {"values": [x["input"] for x in inputs], "batch_size": len(inputs)}

            def predict(self, batched):
                return [{"result": v * 2, "batch_size": batched["batch_size"]} for v in batched["values"]]

            def unbatch(self, output):
                return output

        socket = AsyncMockSocket()
        req = Request(
            uid="batch-1",
            batch=BatchRequest(items=[
                BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="i2", data=json.dumps({"input": 2}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())
        drive_loop(BatchModel(), socket)

        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "batch-1"
        assert len(resp.batch.items) == 2
        assert json.loads(resp.batch.items[0].data)["result"] == 2
        assert json.loads(resp.batch.items[1].data)["result"] == 4

    def test_batch_predict_ctx_list_aligned_to_items(self):
        """batch/unbatch/predict may declare ctx; in batch mode they receive
        a list[RequestContext] aligned positionally with the batch items,
        end-to-end through the worker loop."""
        seen: dict = {}

        class CtxBatchModel(LitAPI):
            def batch(self, inputs, ctx):
                seen["batch_len"] = len(ctx)
                seen["batch_ctx_requests"] = [c.request for c in ctx]
                return inputs

            def predict(self, batched, ctx):
                seen["predict_len"] = len(ctx)
                # Use ctx (aligned with items) to produce per-item output.
                return [{"result": c.request["input"] * 2} for c in ctx]

            def unbatch(self, output, ctx):
                seen["unbatch_len"] = len(ctx)
                return list(output)

        socket = AsyncMockSocket()
        req = Request(
            uid="batch-ctx",
            batch=BatchRequest(items=[
                BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="i2", data=json.dumps({"input": 2}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())
        drive_loop(CtxBatchModel(), socket)

        # ctx[i] aligns with inputs[i] / item i.
        assert seen["batch_len"] == 2
        assert seen["batch_ctx_requests"] == [{"input": 1}, {"input": 2}]
        assert seen["predict_len"] == 2
        assert seen["unbatch_len"] == 2

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "batch-ctx"
        # Results written back to the correct items (no misalignment).
        assert json.loads(resp.batch.items[0].data)["result"] == 2
        assert json.loads(resp.batch.items[1].data)["result"] == 4

    def test_batch_predict_fallback_no_batch_methods(self):
        class SimpleModel(LitAPI):
            def predict(self, x):
                return {"result": x["input"] + 1}

        socket = AsyncMockSocket()
        req = Request(
            uid="batch-2",
            batch=BatchRequest(items=[
                BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="i2", data=json.dumps({"input": 2}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())
        drive_loop(SimpleModel(), socket)

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "batch-2"
        assert len(resp.batch.items) == 2
        assert json.loads(resp.batch.items[0].data)["result"] == 2
        assert json.loads(resp.batch.items[1].data)["result"] == 3

    def test_batch_predict_whole_batch_fails(self):
        class BrokenBatchModel(LitAPI):
            def batch(self, inputs):
                return inputs

            def predict(self, batched):
                raise ValueError("batch predict boom")

            def unbatch(self, output):
                return output

        socket = AsyncMockSocket()
        req = Request(
            uid="batch-3",
            batch=BatchRequest(items=[
                BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="i2", data=json.dumps({"input": 2}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())
        drive_loop(BrokenBatchModel(), socket)

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "batch-3"
        assert len(resp.batch.items) == 2
        for item in resp.batch.items:
            assert item.status.code == "Error"
            assert "batch predict boom" in item.status.message

    def test_whole_batch_predict_failure_drives_on_error_per_item(self):
        """A5: when batched predict fails, every item's on_error must fire."""
        seen = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                seen.append(exc)

        class BrokenBatchModel(LitAPI):
            def batch(self, inputs):
                return inputs

            def predict(self, batched):
                raise ValueError("batch predict boom")

            def unbatch(self, output):
                return output

        pipe = Pipeline.build(BrokenBatchModel(), [ErrCB()])
        batch = BatchRequest(items=[
            BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
            BatchItem(uid="i2", data=json.dumps({"input": 2}).encode()),
        ])
        meta = RequestMeta(
            route="/predict", headers=Headers(), client_ip="",
            request_id="b1", timestamp_ns=0,
        )
        resp = asyncio.run(
            inference._handle_batch(pipe, "b1", batch, meta, BrokenBatchModel(), log)
        )
        assert len(seen) == 2  # one on_error per item
        assert all(isinstance(e, ValueError) for e in seen)
        for item in resp.batch.items:
            assert item.status.code == "Error"

    def test_per_item_predict_failure_drives_on_error_only_for_failed(self):
        """A5: in the per-item fallback, a single failed item drives on_error
        exactly once; successful items are unaffected."""
        seen = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                seen.append(ctx.request)

        class SimpleModel(LitAPI):
            def predict(self, x):
                if x["input"] == 2:
                    raise ValueError("per-item boom")
                return {"result": x["input"]}

        pipe = Pipeline.build(SimpleModel(), [ErrCB()])
        batch = BatchRequest(items=[
            BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
            BatchItem(uid="i2", data=json.dumps({"input": 2}).encode()),
        ])
        meta = RequestMeta(
            route="/predict", headers=Headers(), client_ip="",
            request_id="b2", timestamp_ns=0,
        )
        resp = asyncio.run(
            inference._handle_batch(pipe, "b2", batch, meta, SimpleModel(), log)
        )
        assert len(seen) == 1
        assert seen[0] == {"input": 2}
        codes = {item.uid: item.status.code for item in resp.batch.items}
        assert codes == {"i1": "Ok", "i2": "Error"}

    def test_batch_error_item_carries_response_headers(self):
        """B6: ctx.response_headers set by a callback reach the failed item."""
        class HeaderCB(Callback):
            def on_request(self, ctx):
                ctx.response_headers["x-batch"] = "1"

        class BrokenBatchModel(LitAPI):
            def batch(self, inputs):
                return inputs

            def predict(self, batched):
                raise ValueError("boom")

            def unbatch(self, output):
                return output

        pipe = Pipeline.build(BrokenBatchModel(), [HeaderCB()])
        batch = BatchRequest(items=[
            BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
        ])
        meta = RequestMeta(
            route="/predict", headers=Headers(), client_ip="",
            request_id="bh", timestamp_ns=0,
        )
        resp = asyncio.run(
            inference._handle_batch(pipe, "bh", batch, meta, BrokenBatchModel(), log)
        )
        item = resp.batch.items[0]
        assert item.status.code == "Error"
        assert item.headers["x-batch"] == "1"

    def test_batch_invalid_json_item_drives_on_error(self):
        """P3: invalid JSON in a batch item drives on_error and yields an
        error item instead of crashing the batch."""
        from lite_server.exceptions import HTTPException

        seen = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                seen.append(exc)

        class EchoModel(LitAPI):
            def predict(self, x):
                return x

        pipe = Pipeline.build(EchoModel(), [ErrCB()])
        batch = BatchRequest(items=[BatchItem(uid="i1", data=b"{not json")])
        meta = RequestMeta(
            route="/predict", headers=Headers(), client_ip="",
            request_id="bj", timestamp_ns=0,
        )
        resp = asyncio.run(
            inference._handle_batch(pipe, "bj", batch, meta, EchoModel(), log)
        )
        item = resp.batch.items[0]
        assert item.status.code == "Error"
        assert len(seen) == 1
        assert isinstance(seen[0], HTTPException)

    def test_async_batch_predict_full_path(self):
        class AsyncBatchModel(LitAPI):
            async def batch(self, inputs):
                return {"values": [x["input"] for x in inputs], "batch_size": len(inputs)}

            async def predict(self, batched):
                return [{"result": v * 2, "batch_size": batched["batch_size"]} for v in batched["values"]]

            async def unbatch(self, output):
                return output

        socket = AsyncMockSocket()
        req = Request(
            uid="async-batch-1",
            batch=BatchRequest(items=[
                BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="i2", data=json.dumps({"input": 3}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())
        drive_loop(AsyncBatchModel(), socket)

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "async-batch-1"
        assert len(resp.batch.items) == 2
        assert json.loads(resp.batch.items[0].data)["result"] == 2
        assert json.loads(resp.batch.items[1].data)["result"] == 6

    def test_async_batch_predict_fallback_concurrent(self):
        class AsyncSimpleModel(LitAPI):
            async def predict(self, x):
                await asyncio.sleep(0.01)
                return {"result": x["input"] + 1}

        socket = AsyncMockSocket()
        req = Request(
            uid="async-batch-2",
            batch=BatchRequest(items=[
                BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="i2", data=json.dumps({"input": 2}).encode()),
                BatchItem(uid="i3", data=json.dumps({"input": 3}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())
        drive_loop(AsyncSimpleModel(), socket)

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "async-batch-2"
        assert len(resp.batch.items) == 3
        for i, item in enumerate(resp.batch.items):
            assert json.loads(item.data)["result"] == i + 2

    def test_batch_per_item_status_code_and_headers(self):
        """F10: each batch item must carry its own status_code, media_type,
        and headers.  BatchResponse.headers must be empty (no cross-item
        leakage, no _sc/_mt internal keys)."""
        from lite_server.response import Response as LiteResponse

        class BatchModel(LitAPI):
            def batch(self, inputs):
                return inputs

            def predict(self, batched):
                return batched

            def unbatch(self, output):
                return output

            def encode_response(self, output):
                uid = output.get("uid")
                if uid == "i1":
                    # early 400 + custom header
                    return LiteResponse(
                        content={"error": "bad"}, status_code=400,
                        headers={"X-Item": "1"},
                    )
                if uid == "i2":
                    # non-JSON media type
                    return LiteResponse(
                        content="<p>ok</p>", media_type="text/html",
                    )
                if uid == "i3":
                    # custom headers via Response
                    return LiteResponse(
                        content={"result": "z"}, headers={"X-Custom": "v3"},
                    )
                return output

        socket = AsyncMockSocket()
        items = [
            BatchItem(uid="i1", data=json.dumps({"uid": "i1", "value": "x"}).encode()),
            BatchItem(uid="i2", data=json.dumps({"uid": "i2", "value": "y"}).encode()),
            BatchItem(uid="i3", data=json.dumps({"uid": "i3", "value": "z"}).encode()),
        ]
        req = Request(uid="batch-hdr", batch=BatchRequest(items=items))
        socket.inject(req.SerializeToString())
        drive_loop(BatchModel(), socket)

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "batch-hdr"
        assert len(resp.batch.items) == 3

        # Item 1 (uid=i1): early 400, custom header
        i1 = resp.batch.items[0]
        assert i1.uid == "i1"
        assert i1.status_code == 400
        assert json.loads(i1.data) == {"error": "bad"}
        assert dict(i1.headers) == {"X-Item": "1"}

        # Item 2 (uid=i2): text/html media type
        i2 = resp.batch.items[1]
        assert i2.uid == "i2"
        assert i2.status_code == 0  # not set → default
        assert i2.media_type == "text/html"
        assert i2.data == b'"<p>ok</p>"'

        # Item 3 (uid=i3): custom headers via Response
        i3 = resp.batch.items[2]
        assert i3.uid == "i3"
        assert dict(i3.headers) == {"X-Custom": "v3"}

        # Batch-level headers must be empty (no cross-contamination)
        assert dict(resp.batch.headers) == {}, (
            f"batch-level headers must be empty, got {dict(resp.batch.headers)}"
        )

    def test_single_on_request_before_decode(self):
        class ModelWithHook(LitAPI):
            def on_request(self, ctx):
                ctx.request["injected"] = True
                return ctx.request

            def decode_request(self, request):
                assert request.get("injected") is True
                return {"decoded": request}

            def predict(self, x):
                return {"has_injected": x["decoded"].get("injected", False)}

        socket = AsyncMockSocket()
        req = Request(uid="hook-1", single=SingleRequest(data=json.dumps({"input": 1}).encode()))
        socket.inject(req.SerializeToString())
        drive_loop(ModelWithHook(), socket)

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "hook-1"
        assert json.loads(resp.single.data)["has_injected"] is True

    def test_async_single_on_request_before_decode(self):
        class AsyncModelWithHook(LitAPI):
            async def on_request(self, ctx):
                ctx.request["async_injected"] = True
                return ctx.request

            async def decode_request(self, request):
                assert request.get("async_injected") is True
                return {"decoded": request}

            async def predict(self, x):
                return {"has_injected": x["decoded"].get("async_injected", False)}

        socket = AsyncMockSocket()
        req = Request(uid="async-hook-1", single=SingleRequest(data=json.dumps({"input": 1}).encode()))
        socket.inject(req.SerializeToString())
        drive_loop(AsyncModelWithHook(), socket)

        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "async-hook-1"
        assert json.loads(resp.single.data)["has_injected"] is True


# ---------------------------------------------------------------------------
# Continuous batching loop
# ---------------------------------------------------------------------------

class TestCBLoop:
    def test_cb_loop_handles_single_request(self):
        """A standard SingleRequest should be processed through the CB pipeline."""

        class EchoCBModel(LitAPI):
            def decode_request(self, req):
                return req.get("input", "")

            def prefill(self, uid, decoded_input):
                pass  # side-effect only

            def step(self, active_sequences):
                # Echo the input for each active sequence
                return [f"cb_echo: {s.input}" for s in active_sequences]

            def has_finished(self, uid, token, generated_sequence):
                return True  # one and done

            def encode_response(self, output):
                return {"output": output}

        socket = SyncMockSocket()
        req = Request()
        req.uid = "req-001"
        req.single.data = json.dumps({"input": "hello"}).encode()
        socket.inject(req.SerializeToString())
        start_cb_loop(EchoCBModel(), socket)

        response = wait_for_response(socket, "req-001")
        data = json.loads(response.single.data)
        # CB encode_response receives accumulated token list
        assert data == {"output": ["cb_echo: hello"]}, f"Unexpected output: {data}"

    def test_cb_loop_single_request_with_multiple_sequences(self):
        """Multiple concurrent SingleRequests should batch in CB pipeline."""

        class MultiCBModel(LitAPI):
            def decode_request(self, req):
                return req.get("text", "")

            def prefill(self, uid, decoded_input):
                pass

            def step(self, active_sequences):
                return [f"token({s.input})" for s in active_sequences]

            def has_finished(self, uid, token, generated_sequence):
                return True  # one step then done

            def encode_response(self, output):
                return {"result": output}

        socket = SyncMockSocket()
        for i, uid in enumerate(["req-a", "req-b"]):
            req = Request()
            req.uid = uid
            req.single.data = json.dumps({"text": f"msg-{i}"}).encode()
            socket.inject(req.SerializeToString())
        start_cb_loop(MultiCBModel(), socket)

        responses = {}
        deadline = time.time() + 5
        while time.time() < deadline and len(responses) < 2:
            for msg in list(socket._msgs):
                resp = Response()
                resp.ParseFromString(msg)
                if resp.uid in ("req-a", "req-b") and resp.uid not in responses:
                    responses[resp.uid] = json.loads(resp.single.data)
            time.sleep(0.01)

        assert len(responses) == 2, f"Expected 2 responses, got {len(responses)}"
        assert responses["req-a"] == {"result": ["token(msg-0)"]}
        assert responses["req-b"] == {"result": ["token(msg-1)"]}

    def test_cb_loop_with_async_prefill_and_step(self):
        """Async prefill/step/has_finished are driven on the CB event loop."""

        class AsyncCBModel(LitAPI):
            def __init__(self):
                super().__init__()
                self._states = {}

            async def decode_request(self, req):
                return req

            async def prefill(self, uid, decoded_input):
                self._states[uid] = {"tokens": [decoded_input["start"]]}

            async def step(self, active_sequences):
                return [{"token": s.input["start"] + len(s.output)} for s in active_sequences]

            async def has_finished(self, uid, token, generated_sequence):
                return len(generated_sequence) >= 2

            async def encode_response(self, output):
                return {"tokens": output}

        model = AsyncCBModel()
        socket = SyncMockSocket()
        req = Request()
        req.uid = "cb-async-1"
        req.single.data = json.dumps({"start": 10}).encode()
        socket.inject(req.SerializeToString())
        start_cb_loop(model, socket)

        response = wait_for_response(socket, "cb-async-1")
        data = json.loads(response.single.data)
        assert data == {"tokens": [{"token": 10}, {"token": 11}]}
        assert model._states == {"cb-async-1": {"tokens": [10]}}

    def test_cb_loop_runs_callback_hooks(self):
        """CB mode gets the same hook coverage as every other mode (0.7.0)."""
        calls = []

        class CBTracker(Callback):
            def on_request(self, ctx):
                calls.append("on_request")

            def on_input(self, ctx):
                calls.append("on_input")

            def on_output(self, ctx):
                calls.append("on_output")

            def on_response(self, ctx):
                calls.append("on_response")

        class CBModel(LitAPI):
            def decode_request(self, req):
                return req.get("input", "")

            def prefill(self, uid, decoded_input):
                pass

            def step(self, active_sequences):
                return [f"tok:{s.input}" for s in active_sequences]

            def has_finished(self, uid, token, generated_sequence):
                return True

            def encode_response(self, output):
                return {"out": output}

        model = CBModel()
        model._pipeline = Pipeline.build(model, [CBTracker()])
        socket = SyncMockSocket()
        req = Request()
        req.uid = "cb-hooks"
        req.single.data = json.dumps({"input": "x"}).encode()
        socket.inject(req.SerializeToString())
        start_cb_loop(model, socket)

        response = wait_for_response(socket, "cb-hooks")
        assert json.loads(response.single.data) == {"out": ["tok:x"]}
        assert calls == ["on_request", "on_input", "on_output", "on_response"]

    def test_cb_loop_early_return_skips_prefill(self):
        """Early return from a hook responds immediately — no prefill/step."""

        class CacheCB(Callback):
            def on_request(self, ctx):
                ctx.respond({"cached": True})

        class CBModel(LitAPI):
            def decode_request(self, req):
                return req

            def prefill(self, uid, decoded_input):
                raise AssertionError("prefill must not run on early return")

            def step(self, active_sequences):
                raise AssertionError("step must not run on early return")

            def has_finished(self, uid, token, generated_sequence):
                return True

        model = CBModel()
        model._pipeline = Pipeline.build(model, [CacheCB()])
        socket = SyncMockSocket()
        req = Request()
        req.uid = "cb-early"
        req.single.data = json.dumps({"input": "x"}).encode()
        socket.inject(req.SerializeToString())
        start_cb_loop(model, socket)

        response = wait_for_response(socket, "cb-early")
        assert json.loads(response.single.data) == {"cached": True}
        assert response.single.status.code == "Ok"

    def test_cb_loop_hook_rejection_returns_structured_error(self):
        """HTTPException from a CB-path hook maps to a structured error."""
        from lite_server.exceptions import BadRequestError

        class RejectCB(Callback):
            def on_request(self, ctx):
                raise BadRequestError("cb rejected")

        class CBModel(LitAPI):
            def decode_request(self, req):
                return req

            def prefill(self, uid, decoded_input):
                pass

            def step(self, active_sequences):
                return []

            def has_finished(self, uid, token, generated_sequence):
                return True

        model = CBModel()
        model._pipeline = Pipeline.build(model, [RejectCB()])
        socket = SyncMockSocket()
        req = Request()
        req.uid = "cb-reject"
        req.single.data = json.dumps({"input": "x"}).encode()
        socket.inject(req.SerializeToString())
        start_cb_loop(model, socket)

        response = wait_for_response(socket, "cb-reject")
        assert response.single.status.code == "Error"
        assert response.single.status.message == "400"
        body = json.loads(response.single.data)
        assert "cb rejected" in body["error"]["message"]

    def test_cb_error_carries_response_headers(self):
        """B6: ctx.response_headers set on the CB path reach the error response."""
        from lite_server.exceptions import BadRequestError

        class HeaderCB(Callback):
            def on_request(self, ctx):
                ctx.response_headers["x-cb"] = "yes"
                raise BadRequestError("cb rejected")

        class CBModel(LitAPI):
            def decode_request(self, req):
                return req

            def prefill(self, uid, decoded_input):
                pass

            def step(self, active_sequences):
                return []

            def has_finished(self, uid, token, generated_sequence):
                return True

        model = CBModel()
        model._pipeline = Pipeline.build(model, [HeaderCB()])
        socket = SyncMockSocket()
        req = Request()
        req.uid = "cb-hdr"
        req.single.data = json.dumps({"input": "x"}).encode()
        socket.inject(req.SerializeToString())
        start_cb_loop(model, socket)

        response = wait_for_response(socket, "cb-hdr")
        assert response.single.status.code == "Error"
        assert response.single.headers["x-cb"] == "yes"

    def test_cb_has_finished_failure_does_not_kill_step_thread(self):
        """B8: has_finished raising must not escape and kill the step thread —
        the offending sequence gets an error response (and on_error) instead of
        hanging every subsequent CB request."""
        seen = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                seen.append(exc)

        class CBModel(LitAPI):
            def decode_request(self, req):
                return req

            def prefill(self, uid, decoded_input):
                pass

            def step(self, active_sequences):
                return ["tok" for _ in active_sequences]

            def has_finished(self, uid, token, generated_sequence):
                raise RuntimeError("has_finished broke")

            def encode_response(self, output):
                return {"out": output}

        model = CBModel()
        model._pipeline = Pipeline.build(model, [ErrCB()])
        socket = SyncMockSocket()
        req = Request()
        req.uid = "cb-hf"
        req.single.data = json.dumps({"input": "x"}).encode()
        socket.inject(req.SerializeToString())
        start_cb_loop(model, socket)

        response = wait_for_response(socket, "cb-hf")
        assert response.single.status.code == "Error"
        assert "has_finished" in json.loads(response.single.data)["error"]["message"]
        assert len(seen) == 1
        assert isinstance(seen[0], RuntimeError)

    def test_cb_add_invalid_json_returns_400_and_drives_on_error(self):
        """P3: invalid JSON on a CB add yields a 400 error frame and drives
        on_error instead of crashing the add handler."""
        from lite_server.exceptions import HTTPException

        seen = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                seen.append(exc)

        class CBModel(LitAPI):
            def decode_request(self, req):
                return req

            def prefill(self, uid, decoded_input):
                pass

            def step(self, active_sequences):
                return []

            def has_finished(self, uid, token, generated_sequence):
                return True

        model = CBModel()
        model._pipeline = Pipeline.build(model, [ErrCB()])
        socket = SyncMockSocket()
        req = Request()
        req.uid = "cb-bad"
        req.single.data = b"{not json"
        socket.inject(req.SerializeToString())
        start_cb_loop(model, socket)

        response = wait_for_response(socket, "cb-bad")
        assert response.single.status.code == "Error"
        assert response.single.status.message == "400"
        assert len(seen) == 1
        assert isinstance(seen[0], HTTPException)


# ---------------------------------------------------------------------------
# CB ctx injection (0.7.0 context unification)
# ---------------------------------------------------------------------------


class TestCBCtxInjection:
    """prefill / has_finished support ctx injection; step receives CBSequence."""

    def test_cb_prefill_ctx_injection(self):
        captured_ctx = []
        captured_state = []

        class CBModel(LitAPI):
            def decode_request(self, req):
                return req.get("input", "")

            def prefill(self, uid, decoded_input, ctx):
                captured_ctx.append(ctx)
                ctx.state["prefill_seen"] = True

            def step(self, active_sequences):
                captured_state.append(active_sequences[0].state.get("prefill_seen"))
                return [f"ok({s.input})" for s in active_sequences]

            def has_finished(self, uid, token, generated_sequence):
                return True

            def encode_response(self, output):
                return {"out": output}

        model = CBModel()
        socket = SyncMockSocket()
        req = Request()
        req.uid = "cb-ctx-1"
        req.single.data = json.dumps({"input": "hello"}).encode()
        socket.inject(req.SerializeToString())
        start_cb_loop(model, socket)

        response = wait_for_response(socket, "cb-ctx-1")
        assert len(captured_ctx) == 1
        assert captured_ctx[0].meta is not None
        assert captured_state == [True]

    def test_cb_has_finished_ctx_injection(self):
        captured = []

        class CBModel(LitAPI):
            def decode_request(self, req):
                return req.get("input", "")

            def prefill(self, uid, decoded_input):
                pass

            def step(self, active_sequences):
                return [f"t({s.input})" for s in active_sequences]

            def has_finished(self, uid, token, generated_sequence, ctx):
                captured.append((uid, ctx.meta.request_id))
                return True

            def encode_response(self, output):
                return {"out": output}

        model = CBModel()
        socket = SyncMockSocket()
        req = Request()
        req.uid = "cb-hf-1"
        req.single.data = json.dumps({"input": "x"}).encode()
        socket.inject(req.SerializeToString())
        start_cb_loop(model, socket)

        response = wait_for_response(socket, "cb-hf-1")
        assert len(captured) == 1
        assert captured[0][0] == "cb-hf-1"

    def test_cb_step_receives_cbsequence(self):
        """step() elements are CBSequence objects with attribute access,
        NOT dicts — fixes the docstring/impl mismatch."""
        captured_type = []
        captured_attrs = []

        class CBModel(LitAPI):
            def decode_request(self, req):
                return req

            def prefill(self, uid, decoded_input):
                pass

            def step(self, active_sequences):
                s = active_sequences[0]
                captured_type.append(type(s).__name__)
                captured_attrs.append(
                    (hasattr(s, "uid"), hasattr(s, "input"),
                     hasattr(s, "output"), hasattr(s, "state"),
                     hasattr(s, "meta"), hasattr(s, "ctx"))
                )
                return [f"r({s.input['val']})"]

            def has_finished(self, uid, token, generated_sequence):
                return True

            def encode_response(self, output):
                return {"out": output}

        model = CBModel()
        socket = SyncMockSocket()
        req = Request()
        req.uid = "cb-seq-1"
        req.single.data = json.dumps({"val": 42}).encode()
        socket.inject(req.SerializeToString())
        start_cb_loop(model, socket)

        response = wait_for_response(socket, "cb-seq-1")
        assert captured_type == ["CBSequence"]
        assert captured_attrs == [(True, True, True, True, True, True)]

    def test_cb_methods_run_without_pipeline_executor(self):
        """CB prefill/step/has_finished run inline on the cb_loop (executor=None)
        — never on the Pipeline's ThreadPoolExecutor.  This is the concurrency
        model invariant: step and prefill must never run concurrently with
        each other or with model state access."""

        class CBModel(LitAPI):
            def decode_request(self, req):
                return req.get("input", "")

            def prefill(self, uid, decoded_input):
                pass

            def step(self, active_sequences):
                return ["tok" for _ in active_sequences]

            def has_finished(self, uid, token, generated_sequence):
                return True

            def encode_response(self, output):
                return output

        model = CBModel()
        socket = SyncMockSocket()
        req = Request()
        req.uid = "cb-exec-1"
        req.single.data = json.dumps({"input": "x"}).encode()
        socket.inject(req.SerializeToString())
        start_cb_loop(model, socket)

        response = wait_for_response(socket, "cb-exec-1")
        assert response.single.status.code == "Ok"

    def test_cb_sequence_state_is_ctx_state(self):
        """CBSequence.state is the same dict as ctx.state — mutations
        made in prefill (via ctx) are visible in step (via seq.state)."""
        step_seen = []

        class CBModel(LitAPI):
            def decode_request(self, req):
                return req.get("input", "")

            def prefill(self, uid, decoded_input, ctx):
                ctx.state["marker"] = f"set-by-{uid}"

            def step(self, active_sequences):
                step_seen.append(active_sequences[0].state["marker"])
                return ["ok"]

            def has_finished(self, uid, token, generated_sequence):
                return True

            def encode_response(self, output):
                return output

        model = CBModel()
        socket = SyncMockSocket()
        req = Request()
        req.uid = "cb-state-1"
        req.single.data = json.dumps({"input": "x"}).encode()
        socket.inject(req.SerializeToString())
        start_cb_loop(model, socket)

        response = wait_for_response(socket, "cb-state-1")
        assert step_seen == ["set-by-cb-state-1"]


class TestTeardownHelper:
    def test_teardown_called_and_exceptions_caught(self):
        from lite_server.worker.inference import _run_teardown

        called = []
        errors = []

        class Model(LitAPI):
            def teardown(self):
                called.append(True)
                raise RuntimeError("teardown boom")

        test_log = logging.getLogger("test_teardown")
        original_error = test_log.error

        def capture_error(msg, *args):
            errors.append(msg % args)

        test_log.error = capture_error
        try:
            _run_teardown(Model(), test_log)
        finally:
            test_log.error = original_error

        assert called == [True]
        assert any("teardown boom" in e for e in errors)

    def test_teardown_skipped_when_missing(self):
        from lite_server.worker.inference import _run_teardown

        test_log = logging.getLogger("test_teardown_none")
        # Base-class teardown is a no-op — must not raise
        _run_teardown(LitAPI(), test_log)

    def test_callback_on_teardown_fires(self):
        from lite_server.worker.inference import _run_teardown

        calls = []

        class TeardownCB(Callback):
            def on_teardown(self, lit_api):
                calls.append("on_teardown")

        model = LitAPI()
        model._pipeline = Pipeline.build(model, [TeardownCB()])
        _run_teardown(model, log)
        assert calls == ["on_teardown"]


# ---------------------------------------------------------------------------
# Streaming metrics
# ---------------------------------------------------------------------------

class TestAsyncStreamingMetrics:
    def test_streaming_metrics_accumulate_until_done(self):
        class MetricStreamModel(LitAPI):
            async def stream_predict(self, x):
                for i in range(3):
                    self.report_metric(self.token_counter, 1.0)
                    yield {"token": i}

        model = MetricStreamModel()
        model.token_counter = model.register_metric("tokens", "counter")

        socket = AsyncMockSocket()
        req = Request(
            uid="stream-m1",
            stream=StreamRequest(stream_id="sm1", open=StreamOpen(data=b"{}")),
        )
        socket.inject(req.SerializeToString())
        drive_loop(model, socket)

        # 3 chunks + 1 done
        assert len(socket._msgs) == 4
        done_resp = Response()
        done_resp.ParseFromString(socket._msgs[3])
        assert done_resp.stream.HasField("done")
        assert done_resp.stream.done.metrics is not None
        assert len(done_resp.stream.done.metrics.counters) == 3
        assert sum(c.value for c in done_resp.stream.done.metrics.counters) == 3.0

    def test_flush_metrics_clears_buffer(self):
        api = LitAPI()
        g = api.register_metric("x", "gauge")
        api.report_metric(g, 10.0)
        api.report_metric(g, 20.0)

        assert len(api._metric_values) == 2

        m = api.flush_metrics()
        assert m is not None
        assert len(m.gauges) == 2
        assert api._metric_values == []

        # Second flush returns None
        assert api.flush_metrics() is None

    def test_streaming_metrics_cleared_after_done(self):
        class MetricStreamModel(LitAPI):
            async def stream_predict(self, x):
                self.report_metric(self.counter_id, 1.0)
                yield {"token": 0}

        model = MetricStreamModel()
        model.counter_id = model.register_metric("cnt", "counter")

        socket = AsyncMockSocket()
        req = Request(
            uid="stream-m2",
            stream=StreamRequest(stream_id="sm2", open=StreamOpen(data=b"{}")),
        )
        socket.inject(req.SerializeToString())
        drive_loop(model, socket)

        # Buffer should be cleared after stream_done
        assert model._metric_values == []


# ---------------------------------------------------------------------------
# Bidirectional streaming
# ---------------------------------------------------------------------------

class TestBidiStreamDetection:
    def test_no_bidi_when_not_overridden(self):
        assert Pipeline.build(LitAPI(), []).has_bidi_stream is False

    def test_no_bidi_when_only_stream_predict(self):
        class StreamingModel(LitAPI):
            def stream_predict(self, request):
                yield {"token": "hello"}

        assert Pipeline.build(StreamingModel(), []).has_bidi_stream is False

    def test_has_bidi_when_overridden(self):
        class BidiModel(LitAPI):
            def bidi_stream(self):
                return BidiStreamHandler()

        assert Pipeline.build(BidiModel(), []).has_bidi_stream is True


class TestBidiStreamingAsyncLoop:
    def test_bidi_open_chunk_close(self):
        class AsyncBidiHandler(BidiStreamHandler):
            def __init__(self):
                self.chunks = []

            def on_open(self, initial_data):
                return {"type": "open_ack", "data": initial_data}

            def on_chunk(self, chunk):
                self.chunks.append(chunk)
                return {"type": "chunk_ack", "received": chunk}

            def on_close(self):
                self.chunks.append("closed")

        class AsyncBidiModel(LitAPI):
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return AsyncBidiHandler()

        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="ab1",
            stream=StreamRequest(stream_id="abs1", open=StreamOpen(data=json.dumps({"hello": 1}).encode())),
        ).SerializeToString())
        socket.inject(Request(
            uid="ab2",
            stream=StreamRequest(stream_id="abs1", chunk=StreamChunk(data=json.dumps({"msg": "a"}).encode())),
        ).SerializeToString())
        socket.inject(Request(
            uid="ab3",
            stream=StreamRequest(stream_id="abs1", close=StreamClose()),
        ).SerializeToString())
        drive_loop(AsyncBidiModel(), socket)

        # open_ack + chunk_ack + stream_done = at least 3 messages
        assert len(socket._msgs) >= 3, f"Expected at least 3 messages, got {len(socket._msgs)}"

        resp1 = Response()
        resp1.ParseFromString(socket._msgs[0])
        data1 = json.loads(resp1.stream.chunk.data)
        assert data1["type"] == "open_ack"

        resp2 = Response()
        resp2.ParseFromString(socket._msgs[1])
        data2 = json.loads(resp2.stream.chunk.data)
        assert data2["type"] == "chunk_ack"

        resp3 = Response()
        resp3.ParseFromString(socket._msgs[2])
        assert resp3.stream.HasField("done")

    def test_bidi_chunk_when_stream_not_found(self):
        class NoBidiModel(LitAPI):
            async def predict(self, x):
                return x

        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="ab1",
            stream=StreamRequest(stream_id="nx", chunk=StreamChunk(data=b"{}")),
        ).SerializeToString())
        drive_loop(NoBidiModel(), socket)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.stream.error.message == "bidi stream not found"

    def test_bidi_on_chunk_failure_drives_on_error(self):
        """B5: a failing bidi on_chunk drives on_error; error frame still sent."""
        seen = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                seen.append(exc)

        class FailChunkHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                raise ValueError("chunk boom")

            def on_close(self):
                return None

        class BidiModel(LitAPI):
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return FailChunkHandler()

        model = BidiModel()
        model._pipeline = Pipeline.build(model, [ErrCB()])
        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="o", stream=StreamRequest(stream_id="bc", open=StreamOpen(data=b"{}")),
        ).SerializeToString())
        socket.inject(Request(
            uid="c", stream=StreamRequest(stream_id="bc", chunk=StreamChunk(data=b"{}")),
        ).SerializeToString())
        drive_loop(model, socket)

        assert len(seen) == 1
        assert isinstance(seen[0], ValueError)
        messages = []
        for m in socket._msgs:
            r = Response()
            r.ParseFromString(m)
            if r.stream.HasField("error"):
                messages.append(r.stream.error.message)
        assert any("chunk boom" in msg for msg in messages)

    def test_bidi_encode_failure_drives_on_error(self):
        """B5: a failing encode (postprocess) on a bidi chunk drives on_error."""
        seen = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                seen.append(exc)

        class AckHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                return {"echo": chunk}

            def on_close(self):
                return None

        class FailEncodeModel(LitAPI):
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return AckHandler()

            def encode_response(self, output):
                raise ValueError("encode boom")

        model = FailEncodeModel()
        model._pipeline = Pipeline.build(model, [ErrCB()])
        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="o", stream=StreamRequest(stream_id="be", open=StreamOpen(data=b"{}")),
        ).SerializeToString())
        socket.inject(Request(
            uid="c",
            stream=StreamRequest(stream_id="be", chunk=StreamChunk(data=json.dumps({"m": 1}).encode())),
        ).SerializeToString())
        drive_loop(model, socket)

        assert len(seen) == 1
        assert isinstance(seen[0], ValueError)
        messages = []
        for m in socket._msgs:
            r = Response()
            r.ParseFromString(m)
            if r.stream.HasField("error"):
                messages.append(r.stream.error.message)
        assert any("encode" in msg for msg in messages)

    def test_bidi_chunk_invalid_json_drives_on_error(self):
        """P3: invalid JSON in a bidi chunk drives on_error and replies with a
        stream error frame (one-for-one chunk ack preserved)."""
        from lite_server.exceptions import HTTPException

        seen = []

        class ErrCB(Callback):
            def on_error(self, ctx, exc):
                seen.append(exc)

        class AckHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                return None

            def on_close(self):
                return None

        class BidiModel(LitAPI):
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return AckHandler()

        model = BidiModel()
        model._pipeline = Pipeline.build(model, [ErrCB()])
        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="o", stream=StreamRequest(stream_id="bj", open=StreamOpen(data=b"{}")),
        ).SerializeToString())
        socket.inject(Request(
            uid="c", stream=StreamRequest(stream_id="bj", chunk=StreamChunk(data=b"{bad")),
        ).SerializeToString())
        drive_loop(model, socket)

        assert len(seen) == 1
        assert isinstance(seen[0], HTTPException)
        messages = []
        for m in socket._msgs:
            r = Response()
            r.ParseFromString(m)
            if r.stream.HasField("error"):
                messages.append(r.stream.error.message)
        assert messages  # a stream error frame was sent for the bad chunk

    def test_bidi_cancel_calls_on_close(self):
        class TrackHandler(BidiStreamHandler):
            def __init__(self):
                self.closed = False

            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                return None

            def on_close(self):
                self.closed = True

        class TrackModel(LitAPI):
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return TrackHandler()

        socket = AsyncMockSocket()
        model = TrackModel()
        handler = None

        original_bidi_stream = model.bidi_stream

        def capture_bidi_stream():
            nonlocal handler
            handler = original_bidi_stream()
            return handler

        model.bidi_stream = capture_bidi_stream
        # Rebuild the pipeline so the monkey-patched factory is picked up
        model._pipeline = Pipeline.build(model, [])

        socket.inject(Request(
            uid="ac1",
            stream=StreamRequest(stream_id="acs1", open=StreamOpen(data=b"{}")),
        ).SerializeToString())
        socket.inject(Request(
            uid="ac2",
            stream=StreamRequest(stream_id="acs1", cancel=StreamCancel()),
        ).SerializeToString())
        drive_loop(model, socket)

        assert handler is not None
        assert handler.closed is True

    def test_bidi_session_closed_on_loop_cancellation(self):
        """A bidi session still open when the loop task is cancelled must
        receive on_close — shutdown cleanup runs on CancelledError too,
        not only on the ETERM break path (asyncio.run's SIGINT shutdown
        cancels the main task)."""

        class TrackHandler(BidiStreamHandler):
            def __init__(self):
                self.closed = False

            def on_open(self, initial_data):
                return None

            def on_close(self):
                self.closed = True

        class TrackModel(LitAPI):
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return TrackHandler()

        socket = AsyncMockSocket()
        model = TrackModel()
        handler = None

        original_bidi_stream = model.bidi_stream

        def capture_bidi_stream():
            nonlocal handler
            handler = original_bidi_stream()
            return handler

        model.bidi_stream = capture_bidi_stream
        model._pipeline = Pipeline.build(model, [])

        # Only an open — the session is still active when drive_loop
        # cancels the loop task.
        socket.inject(Request(
            uid="cl1",
            stream=StreamRequest(stream_id="cls1", open=StreamOpen(data=b"{}")),
        ).SerializeToString())
        drive_loop(model, socket)

        assert handler is not None
        assert handler.closed is True, (
            "on_close must run when the loop task is cancelled with an "
            "active bidi session (shutdown cleanup was unreachable on "
            "CancelledError)"
        )

    def test_bidi_error_in_on_open(self):
        class BadOpenHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                raise RuntimeError("open failed")

        class BadOpenModel(LitAPI):
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return BadOpenHandler()

        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="ae1",
            stream=StreamRequest(stream_id="ae1", open=StreamOpen(data=b"{}")),
        ).SerializeToString())
        drive_loop(BadOpenModel(), socket)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.stream.error.message == "on_open failed: open failed"

    def test_bidi_error_in_on_chunk(self):
        class BadChunkHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                raise RuntimeError("chunk failed")

        class BadChunkModel(LitAPI):
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return BadChunkHandler()

        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="ae2",
            stream=StreamRequest(stream_id="ae2", open=StreamOpen(data=b"{}")),
        ).SerializeToString())
        socket.inject(Request(
            uid="ae3",
            stream=StreamRequest(stream_id="ae2", chunk=StreamChunk(data=b"{}")),
        ).SerializeToString())
        drive_loop(BadChunkModel(), socket)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.stream.error.message == "on_chunk failed: chunk failed"

    def test_bidi_metrics_collected_on_close(self):
        class MetricHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                return None

            def on_close(self):
                pass

        class MetricBidiModel(LitAPI):
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return MetricHandler()

        model = MetricBidiModel()
        cid = model.register_metric("c1", "counter")
        model.report_metric(cid, 1.0)

        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="am1",
            stream=StreamRequest(stream_id="ams1", open=StreamOpen(data=b"{}")),
        ).SerializeToString())
        socket.inject(Request(
            uid="am2",
            stream=StreamRequest(stream_id="ams1", chunk=StreamChunk(data=b"{}")),
        ).SerializeToString())
        socket.inject(Request(
            uid="am3",
            stream=StreamRequest(stream_id="ams1", close=StreamClose()),
        ).SerializeToString())
        drive_loop(model, socket)

        done_resp = None
        for msg in socket._msgs:
            r = Response()
            r.ParseFromString(msg)
            if r.stream.HasField("done"):
                done_resp = r
                break
        assert done_resp is not None
        assert len(done_resp.stream.done.metrics.counters) == 1
        assert done_resp.stream.done.metrics.counters[0].value == 1.0
        assert model._metric_values == []

    def test_bidi_open_no_response(self):
        class SilentHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                return None

            def on_close(self):
                pass

        class SilentModel(LitAPI):
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return SilentHandler()

        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="an1",
            stream=StreamRequest(stream_id="ans1", open=StreamOpen(data=b"{}")),
        ).SerializeToString())
        socket.inject(Request(
            uid="an2",
            stream=StreamRequest(stream_id="ans1", close=StreamClose()),
        ).SerializeToString())
        drive_loop(SilentModel(), socket)

        # Only StreamDone, no chunks
        assert len(socket._msgs) >= 1
        done_resp = Response()
        done_resp.ParseFromString(socket._msgs[0])
        assert done_resp.stream.HasField("done")


# ---------------------------------------------------------------------------
# Streaming hooks via run_async_loop
# ---------------------------------------------------------------------------

class TestAsyncLoopStreamingHooks:
    def test_async_stream_with_hooks(self):
        class HookedStreamModel(LitAPI):
            async def on_request(self, ctx):
                ctx.request["hooked"] = True
                return ctx.request

            async def stream_predict(self, x):
                yield {"token": 0, "hooked": x.get("hooked")}

            async def on_response(self, ctx):
                ctx.response["async_hook"] = True
                return ctx.response

        socket = AsyncMockSocket()
        from lite_server.proto import RequestMeta as ProtoMeta
        socket.inject(Request(
            uid="ah1",
            stream=StreamRequest(
                stream_id="ahs1",
                open=StreamOpen(
                    data=json.dumps({"input": 1}).encode(),
                    meta=ProtoMeta(route="/stream", headers={}, client_ip="", request_id="", timestamp_ns=0),
                ),
            ),
        ).SerializeToString())
        drive_loop(HookedStreamModel(), socket, delay=0.03)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        data = json.loads(resp.stream.chunk.data)
        assert data["hooked"] is True
        assert data["async_hook"] is True


# ---------------------------------------------------------------------------
# Non-streaming edge cases via run_async_loop
# ---------------------------------------------------------------------------

class TestAsyncLoopNonStreaming:
    def test_async_single_request_error_in_predict(self):
        class BadAPI(LitAPI):
            async def predict(self, x):
                raise RuntimeError("async predict boom")

        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="aerr1",
            single=SingleRequest(data=json.dumps({"input": 1}).encode()),
        ).SerializeToString())
        drive_loop(BadAPI(), socket)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.single.status.code == "Error"
        assert resp.single.status.message == "500"
        body = json.loads(resp.single.data)
        assert body["error"]["type"] == "server_error"
        assert "async predict boom" in body["error"]["message"]

    def test_async_batch_partial_failure(self):
        class PartialAPI(LitAPI):
            async def decode_request(self, req):
                if req.get("bad"):
                    raise ValueError("bad input")
                return req

            async def predict(self, x):
                return {"result": x["input"] + 1}

        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="abp1",
            batch=BatchRequest(items=[
                BatchItem(uid="ok", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="fail", data=json.dumps({"bad": True}).encode()),
            ]),
        ).SerializeToString())
        drive_loop(PartialAPI(), socket)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert len(resp.batch.items) == 2
        assert resp.batch.items[0].status.code == "Ok"
        assert json.loads(resp.batch.items[0].data)["result"] == 2
        assert resp.batch.items[1].status.code == "Error"

    def test_async_health_check_empty_data(self):
        class StrictAPI(LitAPI):
            async def predict(self, x):
                return {"output": x["required_field"]}

        socket = AsyncMockSocket()
        socket.inject(Request(
            uid="ahc1",
            single=SingleRequest(data=b""),
        ).SerializeToString())
        drive_loop(StrictAPI(), socket)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.single.status.code == "Ok"
        assert resp.single.data == b"{}"


# ---------------------------------------------------------------------------
# _LevelPrefixFormatter tests
# ---------------------------------------------------------------------------

class TestLevelPrefixFormatter:
    def test_format_includes_exception_traceback(self):
        """Formatter must include exception traceback (exc_info), not just the message."""
        import io
        fmt = inference._LevelPrefixFormatter()
        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_fmt_exc")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        try:
            raise ValueError("boom")
        except ValueError:
            logger.exception("something failed")

        output = buf.getvalue()
        assert "[ERROR]" in output
        assert "something failed" in output
        assert "ValueError" in output, f"traceback missing from: {output!r}"
        assert "boom" in output, f"exception message missing from: {output!r}"

    def test_format_each_line_prefixed_with_level(self):
        """Every line of multi-line output must start with [LEVEL] so the
        Rust stderr parser forwards all traceback lines at the correct level."""
        import io
        fmt = inference._LevelPrefixFormatter()
        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_fmt_lines")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        try:
            raise RuntimeError("multi-line-test")
        except RuntimeError:
            logger.exception("line1\nline2")

        output = buf.getvalue()
        for line in output.rstrip('\n').split('\n'):
            assert line.startswith("[ERROR]"), f"line not prefixed: {line!r}"

    def test_format_simple_info_message(self):
        """Simple INFO message without exception should still work."""
        fmt = inference._LevelPrefixFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="t.py", lineno=1,
            msg="hello %s", args=("world",), exc_info=None,
        )
        output = fmt.format(record)
        assert output == "[INFO] t.py:1 hello world"

    def test_format_warn_message(self):
        """WARN message with level prefix."""
        fmt = inference._LevelPrefixFormatter()
        record = logging.LogRecord(
            name="test", level=logging.WARNING, pathname="t.py", lineno=1,
            msg="careful", args=(), exc_info=None,
        )
        output = fmt.format(record)
        assert output == "[WARN] t.py:1 careful"

    def test_format_includes_pathname_lineno(self):
        """Log messages must include file path and line number."""
        fmt = inference._LevelPrefixFormatter()
        record = logging.LogRecord(
            name="test", level=logging.ERROR,
            pathname="/p/inference.py", lineno=278,
            msg="predict failed: boom", args=(), exc_info=None,
        )
        output = fmt.format(record)
        assert "/p/inference.py:278" in output, \
            f"expected pathname:lineno in: {output!r}"

    def test_format_exc_text_already_set_includes_location(self):
        """When record.exc_text is pre-set, output still includes pathname:lineno."""
        fmt = inference._LevelPrefixFormatter()
        record = logging.LogRecord(
            name="test", level=logging.ERROR,
            pathname="/p/inference.py", lineno=278,
            msg="predict failed", args=(), exc_info=None,
        )
        record.exc_text = "ValueError: boom"
        output = fmt.format(record)
        assert "/p/inference.py:278" in output, \
            f"expected pathname:lineno in: {output!r}"
        assert "ValueError" in output


class TestSetupLogging:
    """B4: setup_logging must reuse an existing root/worker handler (no
    NameError when one is already present) and detach the lite_server logger
    from root so builtin-callback logs don't print twice."""

    def _run(self, setup_fn):
        root = logging.getLogger()
        ls = logging.getLogger("lite_server")
        saved = (list(root.handlers), list(ls.handlers),
                 ls.propagate, ls.level, root.level)
        try:
            # Precondition: root already has a handler (pytest / basicConfig).
            if not root.handlers:
                root.addHandler(logging.StreamHandler())
            setup_fn()  # must not raise NameError
            assert ls.propagate is False  # no double output via root
        finally:
            root.handlers = list(saved[0])
            ls.handlers = list(saved[1])
            ls.propagate = saved[2]
            ls.setLevel(saved[3])
            root.setLevel(saved[4])

    def test_inference_setup_logging_handles_existing_handler(self):
        self._run(lambda: inference.setup_logging(0, "info"))
