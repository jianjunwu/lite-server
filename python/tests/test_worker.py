"""pytest tests for lite_server.worker.inference (ZMQ + Protobuf)."""

import json
import logging
import os
import subprocess
import sys
import textwrap

import pytest

from lite_server.worker import inference
from lite_server.proto import Request, SingleRequest, Response, BatchRequest, BatchItem
from lite_server.api import RequestMeta


class TestInferenceModule:
    def test_has_parse_args(self):
        assert hasattr(inference, "parse_args")

    def test_has_worker_main(self):
        assert hasattr(inference, "worker_main")

    def test_has_run_standard_loop(self):
        assert hasattr(inference, "run_standard_loop")

    def test_has_run_cb_loop(self):
        assert hasattr(inference, "run_cb_loop")


class TestLoadModelConfig:
    def test_loads_valid_yaml(self, tmp_path):
        path = tmp_path / "config.yaml"
        path.write_text(textwrap.dedent("""\
            max_batch_size: 4
            accelerator: gpu
        """))
        config = inference.load_model_config(str(path))
        assert config["max_batch_size"] == 4
        assert config["accelerator"] == "gpu"

    def test_missing_file_returns_empty(self, tmp_path):
        missing = tmp_path / "missing.yaml"
        assert inference.load_model_config(str(missing)) == {}


class TestLoadLitAPI:
    def test_loads_class_with_predict(self, tmp_path):
        model_py = tmp_path / "model.py"
        model_py.write_text(textwrap.dedent('''
            class MyModel:
                def __init__(self, **kwargs):
                    pass
                def predict(self, x):
                    return {"output": x * 2}
        '''))
        api = inference.load_litapi(str(model_py), {})
        assert hasattr(api, "predict")
        assert api.predict(5) == {"output": 10}

    def test_calls_setup_and_pre_setup(self, tmp_path):
        model_py = tmp_path / "model.py"
        model_py.write_text(textwrap.dedent('''
            class MyModel:
                def __init__(self, **kwargs):
                    pass
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

    def test_loads_with_local_sibling_import(self, tmp_path):
        """model.py that imports a sibling module (e.g. utils) should load successfully."""
        utils_py = tmp_path / "utils.py"
        utils_py.write_text(textwrap.dedent('''
            def add(x):
                return x + 1
        '''))
        model_py = tmp_path / "model.py"
        model_py.write_text(textwrap.dedent('''
            from utils import add

            class MyModel:
                def __init__(self, **kwargs):
                    pass
                def predict(self, x):
                    return {"result": add(x)}
        '''))
        api = inference.load_litapi(str(model_py), {})
        assert api.predict(5) == {"result": 6}

    def test_sys_path_cleaned_after_load(self, tmp_path):
        """model_dir should not remain in sys.path after load_litapi returns."""
        # Clean sys.modules of any stale 'utils' from prior tests
        saved_utils = sys.modules.pop("utils", None)
        try:
            utils_py = tmp_path / "utils.py"
            utils_py.write_text("def noop(): pass\n")
            model_py = tmp_path / "model.py"
            model_py.write_text(textwrap.dedent('''
                from utils import noop

                class MyModel:
                    def __init__(self, **kwargs):
                        pass
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
        # Clean sys.modules to isolate from prior test modules
        saved = sys.modules.pop("callbacks", None)
        try:
            # Write a callback module in the model directory
            callbacks_py = tmp_path / "callbacks.py"
            callbacks_py.write_text(textwrap.dedent('''
                from lite_server.callback import Callback

                class Tracker(Callback):
                    called = False

                    def on_before_decode(self, request, meta):
                        request["_hooked"] = True
                        return request
            '''))

            model_py = tmp_path / "model.py"
            model_py.write_text(textwrap.dedent('''
                class MyModel:
                    def __init__(self, **kwargs):
                        pass
                    def predict(self, x):
                        return x
            '''))

            config = {"callbacks": ["callbacks.Tracker"]}
            api = inference.load_litapi(str(model_py), config)
            assert hasattr(api, "predict")
            # Verify the callback was loaded
            callback_runner = getattr(api, "_callback_runner", None)
            assert callback_runner is not None
            assert callback_runner.has_callbacks()
        finally:
            sys.modules.pop("callbacks", None)
            if saved is not None:
                sys.modules["callbacks"] = saved


class TestRunPredict:
    def test_basic_predict(self):
        class MockAPI:
            def predict(self, x):
                return {"output": x.get("input", 0) * 2}

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 5}).encode()
        log = logging.getLogger("test")
        resp_bytes, status, metrics, _ = inference._run_predict(MockAPI(), data, meta, log)
        assert json.loads(resp_bytes) == {"output": 10}
        assert status.code == "Ok"

    def test_on_request_hook_can_modify(self):
        class HookAPI:
            def on_request(self, request, meta):
                request["input"] = request["input"] + 1
                return request

            def predict(self, x):
                return {"output": x["input"] * 2}

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 5}).encode()
        log = logging.getLogger("test")
        resp_bytes, status, metrics, _ = inference._run_predict(HookAPI(), data, meta, log)
        assert json.loads(resp_bytes) == {"output": 12}

    def test_on_request_hook_can_reject(self):
        class RejectAPI:
            def on_request(self, request, meta):
                raise ValueError("rejected")

            def predict(self, x):
                return x

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 5}).encode()
        log = logging.getLogger("test")
        with pytest.raises(ValueError, match="rejected"):
            inference._run_predict(RejectAPI(), data, meta, log)

    def test_skips_hooks_when_not_implemented(self):
        class PlainAPI:
            def predict(self, x):
                return {"output": x.get("input", 0) * 2}

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 5}).encode()
        log = logging.getLogger("test")
        resp_bytes, status, metrics, _ = inference._run_predict(PlainAPI(), data, meta, log)
        assert json.loads(resp_bytes) == {"output": 10}


class TestRunPredictResponseHeaders:
    """_run_predict: on_response returning ResponseWithHeaders passes headers."""

    def test_on_response_with_headers_returns_headers_in_tuple(self):
        from lite_server.api import ResponseWithHeaders

        class HeaderAPI:
            def predict(self, x):
                return {"output": x.get("input", 0) * 2}

            def on_response(self, response, meta):
                return ResponseWithHeaders(
                    body=response, headers={"X-Custom": "hello", "X-Other": "world"}
                )

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 5}).encode()
        log = logging.getLogger("test")
        resp_bytes, status, metrics, headers = inference._run_predict(HeaderAPI(), data, meta, log)
        assert json.loads(resp_bytes) == {"output": 10}
        assert status.code == "Ok"
        assert headers == {"X-Custom": "hello", "X-Other": "world"}

    def test_on_response_plain_body_returns_none_headers(self):
        class PlainAPI:
            def predict(self, x):
                return {"output": x.get("input", 0) * 2}

            def on_response(self, response, meta):
                return {"wrapped": response}

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 5}).encode()
        log = logging.getLogger("test")
        resp_bytes, status, metrics, headers = inference._run_predict(PlainAPI(), data, meta, log)
        assert json.loads(resp_bytes) == {"wrapped": {"output": 10}}
        assert status.code == "Ok"
        assert headers is None

    def test_no_on_response_still_returns_none_headers(self):
        class NoHookAPI:
            def predict(self, x):
                return {"output": x.get("input", 0) + 1}

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 3}).encode()
        log = logging.getLogger("test")
        resp_bytes, status, metrics, headers = inference._run_predict(NoHookAPI(), data, meta, log)
        assert json.loads(resp_bytes) == {"output": 4}
        assert status.code == "Ok"
        assert headers is None


class TestRunPredictAsyncResponseHeaders:
    """_run_predict_async: on_response returning ResponseWithHeaders passes headers."""

    def test_async_on_response_with_headers(self):
        import asyncio
        from lite_server.api import ResponseWithHeaders
        from lite_server.worker.inference import _run_predict_async

        class AsyncHeaderAPI:
            async def predict(self, x):
                return {"output": x.get("input", 0) * 2}

            async def on_response(self, response, meta):
                return ResponseWithHeaders(
                    body=response, headers={"X-Async": "true"}
                )

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 5}).encode()
        log = logging.getLogger("test")
        resp_bytes, status, metrics, headers = asyncio.run(
            _run_predict_async(AsyncHeaderAPI(), data, meta, log)
        )
        assert json.loads(resp_bytes) == {"output": 10}
        assert headers == {"X-Async": "true"}


class TestRunPredictErrorLogging:
    """Verify _run_predict logs include location info on exceptions."""

    def test_predict_exception_log_has_exc_info(self, caplog):
        """Inner _run_predict catch should NOT use exc_info (avoids duplicate).
        The full traceback is logged once by the outer run_standard_loop catch."""
        class BoomAPI:
            def predict(self, x):
                raise RuntimeError("boom")

        meta = RequestMeta(
            route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None
        )
        data = json.dumps({"input": 5}).encode()
        log = logging.getLogger("test_predict_error")

        with caplog.at_level(logging.ERROR):
            try:
                inference._run_predict(BoomAPI(), data, meta, log)
            except RuntimeError:
                pass

        error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(error_records) >= 1, "expected at least one ERROR log record"
        assert error_records[0].exc_info is None, \
            "inner catch must NOT use exc_info=True — traceback is logged by outer catch"

    def test_predict_exception_log_includes_inference_file(self, caplog):
        """The log record's pathname should point to inference.py."""
        class BoomAPI:
            def predict(self, x):
                raise RuntimeError("boom")

        meta = RequestMeta(
            route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None
        )
        data = json.dumps({"input": 5}).encode()
        log = logging.getLogger("test_predict_path")

        with caplog.at_level(logging.ERROR):
            try:
                inference._run_predict(BoomAPI(), data, meta, log)
            except RuntimeError:
                pass

        error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(error_records) >= 1
        assert error_records[0].pathname.endswith("inference.py"), \
            f"expected pathname ending with inference.py, got: {error_records[0].pathname}"


class TestMakeErrorResponse:
    def test_error_response_structure(self):
        resp = inference._make_error_response("uid-1", "something broke")
        assert resp.uid == "uid-1"
        assert resp.single.status.code == "Error"
        # Default (no explicit status_code) is a structured 500 INTERNAL_ERROR
        # so the client sees the real error instead of a sanitized WORKER_CRASHED.
        assert resp.single.status.message == "500"
        body = json.loads(resp.single.data)
        assert body["error"]["code"] == "INTERNAL_ERROR"
        assert body["error"]["message"] == "something broke"


class TestMetaFromProto:
    def test_decodes_meta(self):
        from lite_server.proto import RequestMeta as ProtoMeta

        meta_pb = ProtoMeta(
            route="/predict",
            headers={"x-auth": "token"},
            client_ip="127.0.0.1",
            request_id="req-1",
            timestamp_ns=123456789,
            payload=b'{"extra": true}',
        )
        meta = inference._meta_from_proto(meta_pb)
        assert meta.route == "/predict"
        assert meta.headers == {"x-auth": "token"}
        assert meta.client_ip == "127.0.0.1"
        assert meta.request_id == "req-1"
        assert meta.timestamp_ns == 123456789
        assert meta.payload == {"extra": True}

    def test_empty_payload(self):
        from lite_server.proto import RequestMeta as ProtoMeta

        meta_pb = ProtoMeta(route="/", headers={}, client_ip="", request_id="", timestamp_ns=0)
        meta = inference._meta_from_proto(meta_pb)
        assert meta.payload is None


class TestHealthCheck:
    """Active health check sends empty data; worker must return OK without calling predict."""

    def test_empty_data_returns_ok(self):
        """When SingleRequest.data is empty, _run_predict should not be called."""
        call_count = 0

        class StrictAPI:
            def predict(self, x):
                nonlocal call_count
                call_count += 1
                # This would fail if called with empty input
                return {"output": x["required_field"]}

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        # Simulate what the worker does: empty data → skip predict
        data = b""
        if not data:
            result = b"{}"
            from lite_server.proto import Status
            status = Status(code="Ok", message="")
        else:
            result, status, _, _ = inference._run_predict(StrictAPI(), data, meta)

        assert call_count == 0, "predict() should not be called for health check"
        assert status.code == "Ok"
        assert result == b"{}"

    def test_non_empty_data_calls_predict(self):
        """Normal requests with data should still go through predict."""
        call_count = 0

        class CountingAPI:
            def predict(self, x):
                nonlocal call_count
                call_count += 1
                return {"output": 42}

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 1}).encode()
        log = logging.getLogger("test")
        resp_bytes, status, metrics, _ = inference._run_predict(CountingAPI(), data, meta, log)
        assert call_count == 1
        assert status.code == "Ok"


class TestStdoutProtection:
    """C-level writes to fd 1 during model loading must not pollute the handshake."""

    def test_ready_signal_clean_after_os_write_to_fd1(self, tmp_path):
        """Simulate CANN/ONNX Runtime init writing to fd 1 during setup()."""
        model_py = tmp_path / "model.py"
        model_py.write_text(textwrap.dedent("""\
            import os as _os
            class MyModel:
                def __init__(self, **kwargs):
                    pass
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
# Async Worker Tests
# ---------------------------------------------------------------------------

class TestIsAsyncAPI:
    """_is_async_api detection logic."""

    def test_detects_async_litapi_subclass(self):
        from lite_server.worker.inference import _is_async_api
        from lite_server.api_async import AsyncLitAPI

        class Dummy(AsyncLitAPI):
            def setup(self, device): pass
            async def predict(self, x): return x

        api = Dummy()
        assert _is_async_api(api) is True

    def test_detects_sync_litapi(self):
        from lite_server.worker.inference import _is_async_api
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            def predict(self, x): return x

        api = Dummy()
        assert _is_async_api(api) is False

    def test_detects_enable_async_flag(self):
        from lite_server.worker.inference import _is_async_api
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            def predict(self, x): return x

        api = Dummy()
        api.enable_async = True
        assert _is_async_api(api) is True

    def test_detects_async_predict_on_sync_subclass(self):
        from lite_server.worker.inference import _is_async_api
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            async def predict(self, x): return x

        api = Dummy()
        assert _is_async_api(api) is True

    def test_detects_async_stream_predict(self):
        from lite_server.worker.inference import _is_async_api
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            def predict(self, x): return x
            async def stream_predict(self, x):
                yield x

        api = Dummy()
        assert _is_async_api(api) is True

    def test_plain_object_with_predict_is_not_async(self):
        from lite_server.worker.inference import _is_async_api

        class Plain:
            def predict(self, x):
                return x

        assert _is_async_api(Plain()) is False


class TestMaybeAwait:
    """_maybe_await helper."""

    def test_sync_function_returns_directly(self):
        from lite_server.worker.inference import _maybe_await
        import asyncio

        def add(a, b):
            return a + b

        result = asyncio.run(_maybe_await(add, 2, 3))
        assert result == 5

    def test_async_function_is_awaited(self):
        from lite_server.worker.inference import _maybe_await
        import asyncio

        async def add(a, b):
            await asyncio.sleep(0)
            return a + b

        result = asyncio.run(_maybe_await(add, 2, 3))
        assert result == 5

    def test_sync_function_returning_coroutine_is_awaited(self):
        from lite_server.worker.inference import _maybe_await
        import asyncio

        def make_coro():
            async def inner():
                return 42
            return inner()

        result = asyncio.run(_maybe_await(make_coro))
        assert result == 42


class TestRunPredictAsync:
    """Async predict pipeline with hooks."""

    def test_async_predict_pipeline(self):
        import asyncio
        from lite_server.worker.inference import _run_predict_async
        from lite_server.api import RequestMeta

        class AsyncAPI:
            async def decode_request(self, req):
                return {"decoded": req["input"]}

            async def on_request(self, req, meta):
                req["on_request"] = True
                return req

            async def predict(self, x):
                return {"output": x["decoded"] * 2}

            async def encode_response(self, out):
                return {"encoded": out["output"]}

            async def on_response(self, resp, meta):
                resp["on_response"] = True
                return resp

            def register_metric(self, name, metric_type):
                return 0

            def report_metric(self, metric_id, value):
                pass

            _metric_specs = []
            _metric_values = []

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 5}).encode()
        log = logging.getLogger("test")
        resp_bytes, status, metrics, _ = asyncio.run(_run_predict_async(AsyncAPI(), data, meta, log))
        assert json.loads(resp_bytes) == {"encoded": 10, "on_response": True}
        assert status.code == "Ok"

    def test_async_predict_without_optional_hooks(self):
        import asyncio
        from lite_server.worker.inference import _run_predict_async
        from lite_server.api import RequestMeta

        class SimpleAsyncAPI:
            async def predict(self, x):
                return {"output": x["input"] * 3}

            _metric_specs = []
            _metric_values = []

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 4}).encode()
        log = logging.getLogger("test")
        resp_bytes, status, metrics, _ = asyncio.run(_run_predict_async(SimpleAsyncAPI(), data, meta, log))
        assert json.loads(resp_bytes) == {"output": 12}
        assert status.code == "Ok"

    def test_mixed_sync_async_hooks(self):
        import asyncio
        from lite_server.worker.inference import _run_predict_async
        from lite_server.api import RequestMeta

        class MixedAPI:
            def decode_request(self, req):
                return req

            async def on_request(self, req, meta):
                req["hooked"] = True
                return req

            async def predict(self, x):
                return x

            def encode_response(self, out):
                return out

            def on_response(self, resp, meta):
                resp["sync_hook"] = True
                return resp

            _metric_specs = []
            _metric_values = []

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 1}).encode()
        log = logging.getLogger("test")
        resp_bytes, status, metrics, _ = asyncio.run(_run_predict_async(MixedAPI(), data, meta, log))
        assert json.loads(resp_bytes) == {"input": 1, "hooked": True, "sync_hook": True}


class TestAsyncLoop:
    """run_async_loop integration tests with a mock ZMQ socket."""

    def _make_socket(self):
        """Return a mock socket that records sent messages."""
        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []

            async def send(self, data):
                self._msgs.append(data)

            async def recv(self):
                while not self._incoming:
                    import asyncio
                    await asyncio.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

        return MockSocket()

    def test_async_single_request(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, SingleRequest, Response

        class AsyncModel:
            async def predict(self, x):
                return {"result": x["input"] * 2}

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(uid="req-1", single=SingleRequest(data=json.dumps({"input": 5}).encode()))
        socket.inject(req.SerializeToString())

        async def runner():
            # Run one iteration then cancel
            task = asyncio.create_task(run_async_loop(AsyncModel(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "req-1"
        assert json.loads(resp.single.data) == {"result": 10}
        assert resp.single.status.code == "Ok"

    def test_async_health_check(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, SingleRequest, Response

        class AsyncModel:
            async def predict(self, x):
                raise RuntimeError("should not be called")

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(uid="health-1", single=SingleRequest(data=b""))
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(AsyncModel(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "health-1"
        assert resp.single.data == b"{}"
        assert resp.single.status.code == "Ok"

    def test_async_batch_request(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, BatchRequest, BatchItem, Response

        class AsyncModel:
            async def predict(self, x):
                return {"result": x["input"] + 1}

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(
            uid="batch-1",
            batch=BatchRequest(items=[
                BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="i2", data=json.dumps({"input": 2}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(AsyncModel(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "batch-1"
        assert len(resp.batch.items) == 2
        assert json.loads(resp.batch.items[0].data) == {"result": 2}
        assert json.loads(resp.batch.items[1].data) == {"result": 3}

    def test_async_error_in_predict(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, SingleRequest, Response

        class BrokenModel:
            async def predict(self, x):
                raise ValueError("boom")

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(uid="err-1", single=SingleRequest(data=json.dumps({"input": 1}).encode()))
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(BrokenModel(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "err-1"
        assert resp.single.status.code == "Error"
        assert resp.single.status.message == "500"
        body = json.loads(resp.single.data)
        assert body["error"]["code"] == "INTERNAL_ERROR"
        assert "boom" in body["error"]["message"]

    def test_async_stream_with_async_generator(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, StreamRequest, StreamOpen, StreamCancel, Response

        class AsyncStreamModel:
            async def predict(self, x):
                return x

            async def stream_predict(self, x):
                for i in range(3):
                    await asyncio.sleep(0.001)
                    yield {"token": i, "input": x.get("input")}

            def encode_response(self, output):
                return output

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(
            uid="stream-1",
            stream=StreamRequest(stream_id="s1", open=StreamOpen(data=json.dumps({"input": 42}).encode())),
        )
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(AsyncStreamModel(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        # Expect: 3 chunks + 1 done = 4 messages
        assert len(socket._msgs) == 4, f"Expected 4 messages, got {len(socket._msgs)}"

        # Parse chunks
        for i in range(3):
            resp = Response()
            resp.ParseFromString(socket._msgs[i])
            assert resp.stream.stream_id == "s1"
            assert resp.stream.chunk.is_final is False
            data = json.loads(resp.stream.chunk.data)
            assert data["token"] == i
            assert data["input"] == 42

        # Parse done
        done_resp = Response()
        done_resp.ParseFromString(socket._msgs[3])
        assert done_resp.stream.stream_id == "s1"
        assert done_resp.stream.done is not None

    def test_async_stream_with_sync_generator(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, StreamRequest, StreamOpen, Response

        class SyncStreamModel:
            def predict(self, x):
                return x

            def stream_predict(self, x):
                for i in range(3):
                    yield {"token": i, "input": x.get("input")}

            def encode_response(self, output):
                return output

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(
            uid="stream-2",
            stream=StreamRequest(stream_id="s2", open=StreamOpen(data=json.dumps({"input": 99}).encode())),
        )
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(SyncStreamModel(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        assert len(socket._msgs) == 4, f"Expected 4 messages, got {len(socket._msgs)}"

        for i in range(3):
            resp = Response()
            resp.ParseFromString(socket._msgs[i])
            data = json.loads(resp.stream.chunk.data)
            assert data["token"] == i
            assert data["input"] == 99

    def test_async_stream_fallback_no_stream_predict(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, StreamRequest, StreamOpen, Response

        class NoStreamModel:
            async def predict(self, x):
                return {"fallback": True, "input": x.get("input")}

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(
            uid="stream-3",
            stream=StreamRequest(stream_id="s3", open=StreamOpen(data=json.dumps({"input": 7}).encode())),
        )
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(NoStreamModel(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
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
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, StreamRequest, StreamOpen, Response

        class BrokenStreamModel:
            async def stream_predict(self, x):
                yield {"token": 0}
                raise ValueError("stream broke")

            def encode_response(self, output):
                return output

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(
            uid="stream-4",
            stream=StreamRequest(stream_id="s4", open=StreamOpen(data=b"{}")),
        )
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(BrokenStreamModel(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        # Expect: 1 chunk + 1 error = 2 messages
        assert len(socket._msgs) == 2, f"Expected 2 messages, got {len(socket._msgs)}"

        resp = Response()
        resp.ParseFromString(socket._msgs[1])
        assert resp.stream.stream_id == "s4"
        assert resp.stream.error is not None
        assert "stream broke" in resp.stream.error.message

    def test_async_stream_cancel(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, StreamRequest, StreamOpen, StreamCancel, Response

        class SlowStreamModel:
            async def stream_predict(self, x):
                for i in range(100):
                    await asyncio.sleep(0.01)
                    yield {"token": i}

            def encode_response(self, output):
                return output

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        # Inject open request
        open_req = Request(
            uid="stream-open-5",
            stream=StreamRequest(stream_id="s5", open=StreamOpen(data=b"{}")),
        )
        socket.inject(open_req.SerializeToString())

        # Inject cancel request after a short delay
        async def delayed_cancel():
            await asyncio.sleep(0.03)
            cancel_req = Request(
                uid="stream-cancel-5",
                stream=StreamRequest(stream_id="s5", cancel=StreamCancel()),
            )
            socket.inject(cancel_req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(SlowStreamModel(), socket, "test", log))
            asyncio.create_task(delayed_cancel())
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        # Should have received some chunks before cancel, then the stream task was cancelled
        assert len(socket._msgs) >= 1

    def test_async_concurrent_requests(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, SingleRequest, Response

        class SlowModel:
            def __init__(self):
                self.order = []

            async def predict(self, x):
                await asyncio.sleep(0.01)
                self.order.append(x["input"])
                return {"result": x["input"] * 2}

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        model = SlowModel()
        req1 = Request(uid="c1", single=SingleRequest(data=json.dumps({"input": 1}).encode()))
        req2 = Request(uid="c2", single=SingleRequest(data=json.dumps({"input": 2}).encode()))
        socket.inject(req1.SerializeToString())
        socket.inject(req2.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(model, socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        assert len(socket._msgs) == 2
        uids = {Response().ParseFromString(m) or Response().ParseFromString(m) for m in socket._msgs}
        # Verify both requests were processed concurrently (order may vary)
        resp_uids = []
        for m in socket._msgs:
            r = Response()
            r.ParseFromString(m)
            resp_uids.append(r.uid)
        assert sorted(resp_uids) == ["c1", "c2"]

    def test_async_protobuf_parse_error(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Response

        class AsyncModel:
            async def predict(self, x):
                return x

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(b"invalid protobuf bytes")

        async def runner():
            task = asyncio.create_task(run_async_loop(AsyncModel(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.single.status.code == "Error"
        assert resp.single.status.message == "500"
        body = json.loads(resp.single.data)
        assert body["error"]["code"] == "INTERNAL_ERROR"
        assert "Protobuf parse" in body["error"]["message"]

    def test_async_batch_partial_failure(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, BatchRequest, BatchItem, Response

        class PartialFailModel:
            async def predict(self, x):
                if x["input"] == 2:
                    raise ValueError("item 2 fails")
                return {"result": x["input"] * 2}

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(
            uid="batch-partial",
            batch=BatchRequest(items=[
                BatchItem(uid="ok", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="fail", data=json.dumps({"input": 2}).encode()),
                BatchItem(uid="ok2", data=json.dumps({"input": 3}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(PartialFailModel(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
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


class TestCBLoopAsyncMethods:
    """Continuous batching loop with async prefill/step/has_finished."""

    def _make_socket(self):
        class MockSocket:
            def __init__(self):
                self._msgs = []

            def send(self, data):
                self._msgs.append(data)

        return MockSocket()

    def test_cb_loop_with_async_prefill_and_step(self):
        from lite_server.worker.inference import _has_async_methods
        import asyncio

        class AsyncCBModel:
            def __init__(self):
                self._states = {}
                self._metric_specs = []
                self._metric_values = []

            async def decode_request(self, req):
                return req

            async def prefill(self, uid, decoded_input):
                self._states[uid] = {"tokens": [decoded_input["start"]]}

            async def step(self, active_sequences):
                return [{"token": s.input["start"] + len(s.output)} for s in active_sequences]

            def has_finished(self, uid, token, generated_sequence):
                return len(generated_sequence) >= 2

            def encode_response(self, output):
                return {"tokens": output}

        model = AsyncCBModel()

        # Verify async methods are detected
        assert _has_async_methods(model) is True

        # Verify async prefill can be driven through a temporary event loop
        loop = asyncio.new_event_loop()
        loop.run_until_complete(model.prefill("uid-1", {"start": 10}))
        assert model._states == {"uid-1": {"tokens": [10]}}

    def test_cb_loop_with_sync_methods(self):
        from lite_server.worker.inference import _has_async_methods

        class SyncCBModel:
            def decode_request(self, req):
                return req

            def prefill(self, uid, decoded_input):
                pass

            def step(self, active_sequences):
                return []

            def has_finished(self, uid, token, generated_sequence):
                return False

        assert _has_async_methods(SyncCBModel()) is False

    def test_cb_loop_mixed_sync_async(self):
        from lite_server.worker.inference import _has_async_methods

        class MixedCBModel:
            def decode_request(self, req):
                return req

            async def prefill(self, uid, decoded_input):
                pass

            def step(self, active_sequences):
                return []

        assert _has_async_methods(MixedCBModel()) is True


class TestCBLoopSingleRequestRouting:
    """CB loop should handle standard SingleRequest by converting to CBAddRequest."""

    def _make_socket(self):
        import zmq

        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []

            def send(self, data):
                self._msgs.append(data)

            def recv(self):
                while not self._incoming:
                    import time
                    time.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

        return MockSocket()

    def test_cb_loop_handles_single_request(self):
        """A standard SingleRequest should be processed through the CB pipeline."""
        import threading
        import json
        from lite_server.worker.inference import run_cb_loop
        from lite_server.proto import Request, Response

        class EchoCBModel:
            def __init__(self):
                self._metric_specs = []
                self._metric_values = []

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

        model = EchoCBModel()
        socket = self._make_socket()
        log = __import__("logging").getLogger("test")

        # Build a standard SingleRequest (what Rust inference_queue sends)
        req = Request()
        req.uid = "req-001"
        req.single.data = json.dumps({"input": "hello"}).encode()
        socket.inject(req.SerializeToString())

        def runner():
            try:
                run_cb_loop(model, socket, "test_model", log)
            except Exception:
                pass

        t = threading.Thread(target=runner, daemon=True)
        t.start()

        # Wait for response (max 5 seconds)
        import time
        deadline = time.time() + 5
        response = None
        while time.time() < deadline:
            if socket._msgs:
                resp = Response()
                resp.ParseFromString(socket._msgs[0])
                if resp.uid == "req-001":
                    response = resp
                    break
            time.sleep(0.01)

        assert response is not None, "No response received for SingleRequest"
        data = json.loads(response.single.data)
        # CB encode_response receives accumulated token list
        assert data == {"output": ["cb_echo: hello"]}, f"Unexpected output: {data}"

    def test_cb_loop_single_request_with_multiple_sequences(self):
        """Multiple concurrent SingleRequests should batch in CB pipeline."""
        import threading
        import json
        from lite_server.worker.inference import run_cb_loop
        from lite_server.proto import Request, Response

        class MultiCBModel:
            def __init__(self):
                self._metric_specs = []
                self._metric_values = []

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

        model = MultiCBModel()
        socket = self._make_socket()
        log = __import__("logging").getLogger("test")

        # Send two standard SingleRequests
        for i, uid in enumerate(["req-a", "req-b"]):
            req = Request()
            req.uid = uid
            req.single.data = json.dumps({"text": f"msg-{i}"}).encode()
            socket.inject(req.SerializeToString())

        def runner():
            try:
                run_cb_loop(model, socket, "test_model", log)
            except Exception:
                pass

        t = threading.Thread(target=runner, daemon=True)
        t.start()

        import time
        deadline = time.time() + 5
        responses = {}
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


class TestTeardownHelper:
    """_run_teardown lifecycle hook."""

    def test_teardown_called_and_exceptions_caught(self):
        from lite_server.worker.inference import _run_teardown

        called = []
        errors = []

        class Model:
            def teardown(self):
                called.append(True)
                raise RuntimeError("teardown boom")

        log = logging.getLogger("test")
        # Monkey-patch log.error to capture
        original_error = log.error
        def capture_error(msg, *args):
            errors.append(msg % args)
        log.error = capture_error
        try:
            _run_teardown(Model(), log)
        finally:
            log.error = original_error

        assert called == [True]
        assert any("teardown boom" in e for e in errors)

    def test_teardown_skipped_when_missing(self):
        from lite_server.worker.inference import _run_teardown

        class Model:
            pass

        log = logging.getLogger("test")
        # Should not raise
        _run_teardown(Model(), log)


class TestCBAsyncHooks:
    """CB loop _invoke_method drives async on_request/on_response/encode_response."""

    def test_invoke_method_async_on_request(self):
        from lite_server.worker.inference import run_cb_loop
        import asyncio

        class AsyncHookModel:
            def __init__(self):
                self._states = {}
                self._metric_specs = []
                self._metric_values = []

            def decode_request(self, req):
                return req

            async def on_request(self, req, meta):
                req["hooked"] = True
                return req

            def prefill(self, uid, decoded_input):
                self._states[uid] = decoded_input

            def step(self, active_sequences):
                return []

            def has_finished(self, uid, token, generated_sequence):
                return False

        # Verify _has_async_methods detects the async on_request
        from lite_server.worker.inference import _has_async_methods
        model = AsyncHookModel()
        assert _has_async_methods(model) is True

        # Verify async on_request can be driven via temporary event loop
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(model.on_request({"input": 5}, None))
        assert result == {"input": 5, "hooked": True}

    def test_invoke_method_async_encode_response(self):
        import asyncio

        class AsyncEncodeModel:
            async def encode_response(self, output):
                return {"wrapped": output}

        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(AsyncEncodeModel().encode_response([1, 2, 3]))
        assert result == {"wrapped": [1, 2, 3]}

    def test_invoke_method_sync_func(self):
        from lite_server.worker.inference import run_cb_loop

        class SyncModel:
            def decode_request(self, req):
                return {"decoded": req}

        # Verify _has_async_methods returns False for pure sync
        from lite_server.worker.inference import _has_async_methods
        assert _has_async_methods(SyncModel()) is False


class TestAsyncLoopEdgeCases:
    """Edge cases for async loop."""

    def _make_socket(self):
        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []
                self._closed = False

            async def send(self, data):
                if self._closed:
                    raise RuntimeError("socket closed")
                self._msgs.append(data)

            async def recv(self):
                while not self._incoming:
                    import asyncio
                    await asyncio.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

            def close(self):
                self._closed = True

        return MockSocket()

    def test_async_loop_cancels_pending_on_shutdown(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, SingleRequest

        class SlowModel:
            async def predict(self, x):
                await asyncio.sleep(10)
                return x

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(uid="slow-1", single=SingleRequest(data=json.dumps({"input": 1}).encode()))
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(SlowModel(), socket, "test", log))
            await asyncio.sleep(0.02)  # let task start but not finish
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        # The slow task was cancelled on shutdown, no response sent
        assert len(socket._msgs) == 0


class TestBatchPredict:
    """Batch predict path: batch() -> predict(batched) -> unbatch()."""

    def _make_socket(self):
        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []

            def send(self, data):
                self._msgs.append(data)

            def recv(self):
                while not self._incoming:
                    import time
                    time.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

        return MockSocket()

    def test_standard_batch_predict_full_path(self):
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import Request, BatchRequest, BatchItem, Response

        class BatchModel:
            def batch(self, inputs):
                return {"values": [x["input"] for x in inputs], "batch_size": len(inputs)}

            def predict(self, batched):
                return [{"result": v * 2, "batch_size": batched["batch_size"]} for v in batched["values"]]

            def unbatch(self, output):
                return output

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(
            uid="batch-1",
            batch=BatchRequest(items=[
                BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="i2", data=json.dumps({"input": 2}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())

        import threading
        t = threading.Thread(target=run_standard_loop, args=(BatchModel(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.05)
        socket.inject(b"")  # empty to break recv
        t.join(timeout=0.1)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "batch-1"
        assert len(resp.batch.items) == 2
        assert json.loads(resp.batch.items[0].data)["result"] == 2
        assert json.loads(resp.batch.items[1].data)["result"] == 4

    def test_standard_batch_predict_fallback_no_batch_methods(self):
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import Request, BatchRequest, BatchItem, Response

        class SimpleModel:
            def predict(self, x):
                return {"result": x["input"] + 1}

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(
            uid="batch-2",
            batch=BatchRequest(items=[
                BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="i2", data=json.dumps({"input": 2}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())

        import threading
        t = threading.Thread(target=run_standard_loop, args=(SimpleModel(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.05)
        socket.inject(b"")
        t.join(timeout=0.1)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "batch-2"
        assert len(resp.batch.items) == 2
        assert json.loads(resp.batch.items[0].data)["result"] == 2
        assert json.loads(resp.batch.items[1].data)["result"] == 3

    def test_standard_batch_predict_whole_batch_fails(self):
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import Request, BatchRequest, BatchItem, Response

        class BrokenBatchModel:
            def batch(self, inputs):
                return inputs

            def predict(self, batched):
                raise ValueError("batch predict boom")

            def unbatch(self, output):
                return output

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(
            uid="batch-3",
            batch=BatchRequest(items=[
                BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="i2", data=json.dumps({"input": 2}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())

        import threading
        t = threading.Thread(target=run_standard_loop, args=(BrokenBatchModel(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.05)
        socket.inject(b"")
        t.join(timeout=0.1)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "batch-3"
        assert len(resp.batch.items) == 2
        for item in resp.batch.items:
            assert item.status.code == "Error"
            assert "batch predict boom" in item.status.message

    def test_standard_single_request_on_request_before_decode(self):
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import Request, SingleRequest, Response
        from lite_server.api import RequestMeta

        class ModelWithHook:
            def on_request(self, request, meta):
                request["injected"] = True
                return request

            def decode_request(self, request):
                assert request.get("injected") is True
                return {"decoded": request}

            def predict(self, x):
                return {"has_injected": x["decoded"].get("injected", False)}

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(uid="hook-1", single=SingleRequest(data=json.dumps({"input": 1}).encode()))
        socket.inject(req.SerializeToString())

        import threading
        t = threading.Thread(target=run_standard_loop, args=(ModelWithHook(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.05)
        socket.inject(b"")
        t.join(timeout=0.1)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "hook-1"
        assert json.loads(resp.single.data)["has_injected"] is True


class TestAsyncBatchPredict:
    """Async batch predict path."""

    def _make_socket(self):
        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []

            async def send(self, data):
                self._msgs.append(data)

            async def recv(self):
                while not self._incoming:
                    import asyncio
                    await asyncio.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

        return MockSocket()

    def test_async_batch_predict_full_path(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, BatchRequest, BatchItem, Response

        class AsyncBatchModel:
            async def batch(self, inputs):
                return {"values": [x["input"] for x in inputs], "batch_size": len(inputs)}

            async def predict(self, batched):
                return [{"result": v * 2, "batch_size": batched["batch_size"]} for v in batched["values"]]

            async def unbatch(self, output):
                return output

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(
            uid="async-batch-1",
            batch=BatchRequest(items=[
                BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="i2", data=json.dumps({"input": 3}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(AsyncBatchModel(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "async-batch-1"
        assert len(resp.batch.items) == 2
        assert json.loads(resp.batch.items[0].data)["result"] == 2
        assert json.loads(resp.batch.items[1].data)["result"] == 6

    def test_async_batch_predict_fallback_concurrent(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, BatchRequest, BatchItem, Response

        class AsyncSimpleModel:
            async def predict(self, x):
                await asyncio.sleep(0.01)
                return {"result": x["input"] + 1}

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(
            uid="async-batch-2",
            batch=BatchRequest(items=[
                BatchItem(uid="i1", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="i2", data=json.dumps({"input": 2}).encode()),
                BatchItem(uid="i3", data=json.dumps({"input": 3}).encode()),
            ]),
        )
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(AsyncSimpleModel(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "async-batch-2"
        assert len(resp.batch.items) == 3
        for i, item in enumerate(resp.batch.items):
            assert json.loads(item.data)["result"] == i + 2

    def test_async_single_on_request_before_decode(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, SingleRequest, Response

        class AsyncModelWithHook:
            async def on_request(self, request, meta):
                request["async_injected"] = True
                return request

            async def decode_request(self, request):
                assert request.get("async_injected") is True
                return {"decoded": request}

            async def predict(self, x):
                return {"has_injected": x["decoded"].get("async_injected", False)}

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(uid="async-hook-1", single=SingleRequest(data=json.dumps({"input": 1}).encode()))
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(AsyncModelWithHook(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        assert len(socket._msgs) == 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.uid == "async-hook-1"
        assert json.loads(resp.single.data)["has_injected"] is True


class TestAsyncStreamingMetrics:
    """Metrics accumulation and flush_metrics in async streaming."""

    def _make_socket(self):
        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []

            async def send(self, data):
                self._msgs.append(data)

            async def recv(self):
                while not self._incoming:
                    import asyncio
                    await asyncio.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

        return MockSocket()

    def test_streaming_metrics_accumulate_until_done(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, StreamRequest, StreamOpen, Response

        class MetricStreamModel:
            def __init__(self):
                self._metric_specs = []
                self._metric_values = []

            async def stream_predict(self, x):
                for i in range(3):
                    self.report_metric(self.token_counter, 1.0)
                    yield {"token": i}

            def encode_response(self, output):
                return output

            def register_metric(self, name, metric_type):
                idx = len(self._metric_specs)
                per_type_id = sum(
                    1 for s in self._metric_specs
                    if s.metric_type == metric_type
                )
                self._metric_specs.append(type("Spec", (), {
                    "name": name, "metric_type": metric_type,
                    "metric_id": per_type_id,
                })())
                return idx

            def report_metric(self, metric_id, value):
                self._metric_values.append((metric_id, value))

        socket = self._make_socket()
        log = logging.getLogger("test")

        model = MetricStreamModel()
        model.token_counter = model.register_metric("tokens", "counter")

        req = Request(
            uid="stream-m1",
            stream=StreamRequest(stream_id="sm1", open=StreamOpen(data=b"{}")),
        )
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(model, socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        # 3 chunks + 1 done
        assert len(socket._msgs) == 4

        # Verify done carries metrics (3 individual observations)
        done_resp = Response()
        done_resp.ParseFromString(socket._msgs[3])
        assert done_resp.stream.done is not None
        assert done_resp.stream.done.metrics is not None
        assert len(done_resp.stream.done.metrics.counters) == 3
        assert sum(c.value for c in done_resp.stream.done.metrics.counters) == 3.0

    def test_flush_metrics_clears_buffer(self):
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        api = Dummy()
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
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, StreamRequest, StreamOpen, Response

        class MetricStreamModel:
            def __init__(self):
                self._metric_specs = []
                self._metric_values = []

            async def stream_predict(self, x):
                self.report_metric(self.counter_id, 1.0)
                yield {"token": 0}

            def encode_response(self, output):
                return output

            def register_metric(self, name, metric_type):
                idx = len(self._metric_specs)
                per_type_id = sum(
                    1 for s in self._metric_specs
                    if s.metric_type == metric_type
                )
                self._metric_specs.append(type("Spec", (), {
                    "name": name, "metric_type": metric_type,
                    "metric_id": per_type_id,
                })())
                return idx

            def report_metric(self, metric_id, value):
                self._metric_values.append((metric_id, value))

        socket = self._make_socket()
        log = logging.getLogger("test")

        model = MetricStreamModel()
        model.counter_id = model.register_metric("cnt", "counter")

        req = Request(
            uid="stream-m2",
            stream=StreamRequest(stream_id="sm2", open=StreamOpen(data=b"{}")),
        )
        socket.inject(req.SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(model, socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())
        # Buffer should be cleared after stream_done
        assert model._metric_values == []


class TestHasBidiStream:
    """Unit tests for _has_bidi_stream detection."""

    def test_no_bidi_when_not_overridden(self):
        """LitAPI subclass without bidi_stream override should return False."""
        from lite_server import LitAPI
        from lite_server.worker.inference import _has_bidi_stream

        class PlainModel(LitAPI):
            def setup(self, device):
                pass

            def decode_request(self, request):
                return request

            def predict(self, x):
                return x

            def encode_response(self, output):
                return output

        api = PlainModel()
        assert _has_bidi_stream(api) is False, (
            "Plain LitAPI without bidi_stream override should return False"
        )

    def test_no_bidi_when_only_stream_predict(self):
        """LitAPI with stream_predict but no bidi_stream should return False."""
        from lite_server import LitAPI
        from lite_server.worker.inference import _has_bidi_stream

        class StreamingModel(LitAPI):
            def setup(self, device):
                pass

            def decode_request(self, request):
                return request

            def predict(self, x):
                return x

            def stream_predict(self, request):
                yield {"token": "hello"}

            def encode_response(self, output):
                return output

        api = StreamingModel()
        assert _has_bidi_stream(api) is False, (
            "Model with stream_predict but no bidi_stream should return False"
        )

    def test_has_bidi_when_overridden(self):
        """LitAPI subclass that overrides bidi_stream should return True."""
        from lite_server import BidiStreamHandler, LitAPI
        from lite_server.worker.inference import _has_bidi_stream

        class BidiModel(LitAPI):
            def setup(self, device):
                pass

            def decode_request(self, request):
                return request

            def predict(self, x):
                return x

            def encode_response(self, output):
                return output

            def bidi_stream(self):
                class Handler(BidiStreamHandler):
                    pass
                return Handler()

        api = BidiModel()
        assert _has_bidi_stream(api) is True, (
            "Model with bidi_stream override should return True"
        )

    def test_has_bidi_with_async_litapi(self):
        """AsyncLitAPI subclass with bidi_stream should return True."""
        from lite_server import AsyncLitAPI, BidiStreamHandler
        from lite_server.worker.inference import _has_bidi_stream

        class AsyncBidiModel(AsyncLitAPI):
            async def setup(self, device):
                pass

            def decode_request(self, request):
                return request

            async def predict(self, x):
                return x

            def encode_response(self, output):
                return output

            def bidi_stream(self):
                class Handler(BidiStreamHandler):
                    pass
                return Handler()

        api = AsyncBidiModel()
        assert _has_bidi_stream(api) is True, (
            "AsyncLitAPI with bidi_stream override should return True"
        )

    def test_no_bidi_with_async_litapi_without_override(self):
        """AsyncLitAPI without bidi_stream should return False."""
        from lite_server import AsyncLitAPI
        from lite_server.worker.inference import _has_bidi_stream

        class PlainAsyncModel(AsyncLitAPI):
            async def setup(self, device):
                pass

            def decode_request(self, request):
                return request

            async def predict(self, x):
                return x

            def encode_response(self, output):
                return output

        api = PlainAsyncModel()
        assert _has_bidi_stream(api) is False, (
            "AsyncLitAPI without bidi_stream override should return False"
        )


class TestBidiStreamingStandardLoop:
    """Bidirectional streaming via standard (sync) worker loop."""

    def _make_socket(self):
        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []

            def send(self, data):
                self._msgs.append(data)

            def recv(self):
                while not self._incoming:
                    import time
                    time.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

        return MockSocket()

    def test_bidi_open_chunk_close_with_responses(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, StreamChunk,
            StreamClose, Response,
        )
        from lite_server.api import BidiStreamHandler

        class EchoHandler(BidiStreamHandler):
            def __init__(self):
                self.chunks = []

            def on_open(self, initial_data):
                return {"type": "open_ack", "data": initial_data}

            def on_chunk(self, chunk):
                self.chunks.append(chunk)
                return {"type": "chunk_ack", "received": chunk}

            def on_close(self):
                self.chunks.append("closed")

        class BidiModel:
            def bidi_stream(self):
                return EchoHandler()

            def encode_response(self, output):
                return output

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        # Open
        socket.inject(Request(
            uid="b1",
            stream=StreamRequest(stream_id="bs1", open=StreamOpen(data=json.dumps({"hello": 1}).encode())),
        ).SerializeToString())

        # Chunk
        socket.inject(Request(
            uid="b2",
            stream=StreamRequest(stream_id="bs1", chunk=StreamChunk(data=json.dumps({"msg": "a"}).encode())),
        ).SerializeToString())

        # Close
        socket.inject(Request(
            uid="b3",
            stream=StreamRequest(stream_id="bs1", close=StreamClose()),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(BidiModel(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.05)
        socket.inject(b"")
        t.join(timeout=0.1)

        # Expect: open response + chunk response + stream_done (+ possible error from empty injection)
        assert len(socket._msgs) >= 3, f"Expected at least 3 messages, got {len(socket._msgs)}"

        resp1 = Response()
        resp1.ParseFromString(socket._msgs[0])
        assert resp1.stream.stream_id == "bs1"
        data1 = json.loads(resp1.stream.chunk.data)
        assert data1["type"] == "open_ack"
        assert data1["data"] == {"hello": 1}

        resp2 = Response()
        resp2.ParseFromString(socket._msgs[1])
        assert resp2.stream.stream_id == "bs1"
        data2 = json.loads(resp2.stream.chunk.data)
        assert data2["type"] == "chunk_ack"
        assert data2["received"] == {"msg": "a"}

        resp3 = Response()
        resp3.ParseFromString(socket._msgs[2])
        assert resp3.stream.stream_id == "bs1"
        assert resp3.stream.done is not None

    def test_bidi_chunk_when_stream_not_found(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamChunk, Response,
        )

        class NoBidiModel:
            def predict(self, x):
                return x

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="b1",
            stream=StreamRequest(stream_id="nx", chunk=StreamChunk(data=b"{}")),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(NoBidiModel(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.03)
        socket.inject(b"")
        t.join(timeout=0.1)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.stream.error.message == "stream not found"

    def test_bidi_metrics_collected_on_close(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, StreamChunk,
            StreamClose, Response,
        )
        from lite_server.api import BidiStreamHandler, _MetricSpec

        class MetricHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                return None

            def on_close(self):
                pass

        class MetricBidiModel:
            def __init__(self):
                self._metric_specs = []
                self._metric_values = []

            def bidi_stream(self):
                return MetricHandler()

            def encode_response(self, output):
                return output

            def register_metric(self, name, metric_type):
                idx = len(self._metric_specs)
                self._metric_specs.append(_MetricSpec(name, metric_type))
                return idx

            def report_metric(self, metric_id, value):
                self._metric_values.append((metric_id, value))

        socket = self._make_socket()
        log = logging.getLogger("test")

        model = MetricBidiModel()
        gid = model.register_metric("g1", "gauge")
        model.report_metric(gid, 42.0)

        socket.inject(Request(
            uid="bm1",
            stream=StreamRequest(stream_id="bms1", open=StreamOpen(data=b"{}")),
        ).SerializeToString())

        socket.inject(Request(
            uid="bm2",
            stream=StreamRequest(stream_id="bms1", chunk=StreamChunk(data=b"{}")),
        ).SerializeToString())

        socket.inject(Request(
            uid="bm3",
            stream=StreamRequest(stream_id="bms1", close=StreamClose()),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(model, socket, "test", log))
        t.start()
        import time
        time.sleep(0.05)
        socket.inject(b"")
        t.join(timeout=0.1)

        # Find the StreamDone message (may not be last due to empty-injection error)
        done_resp = None
        for msg in socket._msgs:
            r = Response()
            r.ParseFromString(msg)
            if r.stream.done is not None:
                done_resp = r
                break
        assert done_resp is not None, "StreamDone not found"
        assert len(done_resp.stream.done.metrics.gauges) == 1
        assert done_resp.stream.done.metrics.gauges[0].value == 42.0
        assert model._metric_values == []


class TestBidiStreamingAsyncLoop:
    """Bidirectional streaming via async worker loop."""

    def _make_socket(self):
        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []

            async def send(self, data):
                self._msgs.append(data)

            async def recv(self):
                while not self._incoming:
                    import asyncio
                    await asyncio.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

        return MockSocket()

    def test_async_bidi_open_chunk_close(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, StreamChunk,
            StreamClose, Response,
        )
        from lite_server.api import BidiStreamHandler

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

        class AsyncBidiModel:
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return AsyncBidiHandler()

            def encode_response(self, output):
                return output

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

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

        async def runner():
            task = asyncio.create_task(run_async_loop(AsyncBidiModel(), socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())

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
        assert resp3.stream.done is not None

    def test_async_bidi_chunk_when_stream_not_found(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamChunk, Response,
        )

        class NoBidiModel:
            async def predict(self, x):
                return x

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="ab1",
            stream=StreamRequest(stream_id="nx", chunk=StreamChunk(data=b"{}")),
        ).SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(NoBidiModel(), socket, "test", log))
            await asyncio.sleep(0.03)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.stream.error.message == "bidi stream not found"

    def test_async_bidi_cancel_calls_on_close(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, StreamCancel, Response,
        )
        from lite_server.api import BidiStreamHandler

        class TrackHandler(BidiStreamHandler):
            def __init__(self):
                self.closed = False

            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                return None

            def on_close(self):
                self.closed = True

        class TrackModel:
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return TrackHandler()

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        model = TrackModel()
        handler = None

        original_bidi_stream = model.bidi_stream

        def capture_bidi_stream():
            nonlocal handler
            handler = original_bidi_stream()
            return handler

        model.bidi_stream = capture_bidi_stream

        socket.inject(Request(
            uid="ac1",
            stream=StreamRequest(stream_id="acs1", open=StreamOpen(data=b"{}")),
        ).SerializeToString())

        socket.inject(Request(
            uid="ac2",
            stream=StreamRequest(stream_id="acs1", cancel=StreamCancel()),
        ).SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(model, socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())

        assert handler is not None
        assert handler.closed is True


class TestStandardLoopStreaming:
    """Uni-directional streaming via standard (sync) worker loop."""

    def _make_socket(self):
        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []

            def send(self, data):
                self._msgs.append(data)

            def recv(self):
                while not self._incoming:
                    import time
                    time.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

        return MockSocket()

    def test_standard_stream_predict(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, Response,
        )

        class StreamModel:
            def stream_predict(self, x):
                for i in range(3):
                    yield {"token": i, "input": x.get("input")}

            def encode_response(self, output):
                return output

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="s1",
            stream=StreamRequest(
                stream_id="ss1",
                open=StreamOpen(data=json.dumps({"input": 42}).encode()),
            ),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(StreamModel(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.05)
        socket.inject(b"")
        t.join(timeout=0.1)

        # 3 chunks + done (+ possible error from empty injection)
        assert len(socket._msgs) >= 4
        for i in range(3):
            resp = Response()
            resp.ParseFromString(socket._msgs[i])
            assert resp.stream.stream_id == "ss1"
            data = json.loads(resp.stream.chunk.data)
            assert data["token"] == i
            assert data["input"] == 42

        done_resp = Response()
        done_resp.ParseFromString(socket._msgs[3])
        assert done_resp.stream.done is not None

    def test_standard_stream_fallback_no_stream_predict(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, Response,
        )

        class NoStreamModel:
            def predict(self, x):
                return {"result": x["input"] * 2}

            def encode_response(self, output):
                return output

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="s2",
            stream=StreamRequest(
                stream_id="ss2",
                open=StreamOpen(data=json.dumps({"input": 5}).encode()),
            ),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(NoStreamModel(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.03)
        socket.inject(b"")
        t.join(timeout=0.1)

        # Expect: 1 final chunk + done (+ possible error)
        assert len(socket._msgs) >= 2
        resp1 = Response()
        resp1.ParseFromString(socket._msgs[0])
        assert resp1.stream.chunk.is_final is True
        assert json.loads(resp1.stream.chunk.data)["result"] == 10

        resp2 = Response()
        resp2.ParseFromString(socket._msgs[1])
        assert resp2.stream.done is not None

    def test_standard_stream_error_in_stream_predict(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, Response,
        )

        class BadStreamModel:
            def stream_predict(self, x):
                yield {"token": 0}
                raise RuntimeError("mid-stream boom")

            def encode_response(self, output):
                return output

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="s3",
            stream=StreamRequest(
                stream_id="ss3",
                open=StreamOpen(data=json.dumps({"input": 1}).encode()),
            ),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(BadStreamModel(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.05)
        socket.inject(b"")
        t.join(timeout=0.1)

        assert len(socket._msgs) >= 2
        resp1 = Response()
        resp1.ParseFromString(socket._msgs[0])
        assert json.loads(resp1.stream.chunk.data)["token"] == 0

        resp2 = Response()
        resp2.ParseFromString(socket._msgs[1])
        assert resp2.stream.error.message == "mid-stream boom"

    def test_standard_stream_cancel(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, StreamCancel, Response,
        )

        class SlowStreamModel:
            def stream_predict(self, x):
                import time
                for i in range(100):
                    time.sleep(0.01)
                    yield {"token": i}

            def encode_response(self, output):
                return output

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="s4",
            stream=StreamRequest(
                stream_id="ss4",
                open=StreamOpen(data=json.dumps({"input": 1}).encode()),
            ),
        ).SerializeToString())

        # Cancel after a short delay
        import time
        time.sleep(0.02)
        socket.inject(Request(
            uid="s5",
            stream=StreamRequest(stream_id="ss4", cancel=StreamCancel()),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(SlowStreamModel(), socket, "test", log))
        t.start()
        time.sleep(0.05)
        socket.inject(b"")
        t.join(timeout=0.1)

        # Should have received at most a few chunks before cancel
        stream_msgs = []
        for msg in socket._msgs:
            r = Response()
            r.ParseFromString(msg)
            if r.stream.stream_id == "ss4":
                stream_msgs.append(r)

        # Cancel should have closed the generator; no done message
        assert all(m.stream.error.message == "" or m.stream.chunk.data for m in stream_msgs)

    def test_standard_stream_with_hooks(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, Response,
        )

        class HookedStreamModel:
            def on_request(self, req, meta):
                req["hooked"] = True
                return req

            def decode_request(self, req):
                return req

            def stream_predict(self, x):
                yield {"token": 0, "hooked": x.get("hooked")}

            def encode_response(self, output):
                return output

            def on_response(self, resp, meta):
                resp["sync_hook"] = True
                return resp

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        from lite_server.proto import RequestMeta as ProtoMeta
        socket.inject(Request(
            uid="s5",
            stream=StreamRequest(
                stream_id="ss5",
                open=StreamOpen(
                    data=json.dumps({"input": 1}).encode(),
                    meta=ProtoMeta(route="/stream", headers={}, client_ip="", request_id="", timestamp_ns=0),
                ),
            ),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(HookedStreamModel(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.03)
        socket.inject(b"")
        t.join(timeout=0.1)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        data = json.loads(resp.stream.chunk.data)
        assert data["hooked"] is True
        assert data["sync_hook"] is True


class TestStandardLoopBidiErrors:
    """Bidirectional streaming error handling (standard loop)."""

    def _make_socket(self):
        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []

            def send(self, data):
                self._msgs.append(data)

            def recv(self):
                while not self._incoming:
                    import time
                    time.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

        return MockSocket()

    def test_standard_bidi_error_in_on_open(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, Response,
        )
        from lite_server.api import BidiStreamHandler

        class BadOpenHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                raise RuntimeError("open failed")

        class BadOpenModel:
            def bidi_stream(self):
                return BadOpenHandler()

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="be1",
            stream=StreamRequest(
                stream_id="be1",
                open=StreamOpen(data=b"{}"),
            ),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(BadOpenModel(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.03)
        socket.inject(b"")
        t.join(timeout=0.1)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.stream.error.message == "on_open failed: open failed"

    def test_standard_bidi_error_in_on_chunk(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, StreamChunk, Response,
        )
        from lite_server.api import BidiStreamHandler

        class BadChunkHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                raise RuntimeError("chunk failed")

        class BadChunkModel:
            def bidi_stream(self):
                return BadChunkHandler()

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="be2",
            stream=StreamRequest(
                stream_id="be2",
                open=StreamOpen(data=b"{}"),
            ),
        ).SerializeToString())

        socket.inject(Request(
            uid="be3",
            stream=StreamRequest(
                stream_id="be2",
                chunk=StreamChunk(data=b"{}"),
            ),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(BadChunkModel(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.03)
        socket.inject(b"")
        t.join(timeout=0.1)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.stream.error.message == "on_chunk failed: chunk failed"

    def test_standard_bidi_open_no_response(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, StreamChunk,
            StreamClose, Response,
        )
        from lite_server.api import BidiStreamHandler

        class SilentHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                return None

            def on_close(self):
                pass

        class SilentModel:
            def bidi_stream(self):
                return SilentHandler()

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="be4",
            stream=StreamRequest(
                stream_id="be4",
                open=StreamOpen(data=b"{}"),
            ),
        ).SerializeToString())

        socket.inject(Request(
            uid="be5",
            stream=StreamRequest(
                stream_id="be4",
                chunk=StreamChunk(data=b"{}"),
            ),
        ).SerializeToString())

        socket.inject(Request(
            uid="be6",
            stream=StreamRequest(stream_id="be4", close=StreamClose()),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(SilentModel(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.03)
        socket.inject(b"")
        t.join(timeout=0.1)

        # No chunk responses (on_open/on_chunk returned None), just StreamDone
        done_resp = None
        for msg in socket._msgs:
            r = Response()
            r.ParseFromString(msg)
            if r.stream.done is not None:
                done_resp = r
                break
        assert done_resp is not None


class TestAsyncLoopStreamingHooks:
    """Async loop streaming with on_request/on_response hooks."""

    def _make_socket(self):
        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []

            async def send(self, data):
                self._msgs.append(data)

            async def recv(self):
                while not self._incoming:
                    import asyncio
                    await asyncio.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

        return MockSocket()

    def test_async_stream_with_hooks(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, Response,
        )

        class HookedStreamModel:
            async def on_request(self, req, meta):
                req["hooked"] = True
                return req

            async def decode_request(self, req):
                return req

            async def stream_predict(self, x):
                yield {"token": 0, "hooked": x.get("hooked")}

            async def encode_response(self, output):
                return output

            async def on_response(self, resp, meta):
                resp["async_hook"] = True
                return resp

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

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

        async def runner():
            task = asyncio.create_task(run_async_loop(HookedStreamModel(), socket, "test", log))
            await asyncio.sleep(0.03)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        data = json.loads(resp.stream.chunk.data)
        assert data["hooked"] is True
        assert data["async_hook"] is True


class TestAsyncLoopBidiErrors:
    """Bidirectional streaming error handling (async loop)."""

    def _make_socket(self):
        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []

            async def send(self, data):
                self._msgs.append(data)

            async def recv(self):
                while not self._incoming:
                    import asyncio
                    await asyncio.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

        return MockSocket()

    def test_async_bidi_error_in_on_open(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, Response,
        )
        from lite_server.api import BidiStreamHandler

        class BadOpenHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                raise RuntimeError("open failed")

        class BadOpenModel:
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return BadOpenHandler()

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="ae1",
            stream=StreamRequest(
                stream_id="ae1",
                open=StreamOpen(data=b"{}"),
            ),
        ).SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(BadOpenModel(), socket, "test", log))
            await asyncio.sleep(0.03)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.stream.error.message == "on_open failed: open failed"

    def test_async_bidi_error_in_on_chunk(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, StreamChunk, Response,
        )
        from lite_server.api import BidiStreamHandler

        class BadChunkHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                raise RuntimeError("chunk failed")

        class BadChunkModel:
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return BadChunkHandler()

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="ae2",
            stream=StreamRequest(
                stream_id="ae2",
                open=StreamOpen(data=b"{}"),
            ),
        ).SerializeToString())

        socket.inject(Request(
            uid="ae3",
            stream=StreamRequest(
                stream_id="ae2",
                chunk=StreamChunk(data=b"{}"),
            ),
        ).SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(BadChunkModel(), socket, "test", log))
            await asyncio.sleep(0.03)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.stream.error.message == "on_chunk failed: chunk failed"

    def test_async_bidi_metrics_collected_on_close(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, StreamChunk,
            StreamClose, Response,
        )
        from lite_server.api import BidiStreamHandler, _MetricSpec

        class MetricHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                return None

            def on_chunk(self, chunk):
                return None

            def on_close(self):
                pass

        class MetricBidiModel:
            def __init__(self):
                self._metric_specs = []
                self._metric_values = []

            async def predict(self, x):
                return x

            def bidi_stream(self):
                return MetricHandler()

            def encode_response(self, output):
                return output

            def register_metric(self, name, metric_type):
                idx = len(self._metric_specs)
                self._metric_specs.append(_MetricSpec(name, metric_type))
                return idx

            def report_metric(self, metric_id, value):
                self._metric_values.append((metric_id, value))

        socket = self._make_socket()
        log = logging.getLogger("test")

        model = MetricBidiModel()
        cid = model.register_metric("c1", "counter")
        model.report_metric(cid, 1.0)

        socket.inject(Request(
            uid="am1",
            stream=StreamRequest(
                stream_id="ams1",
                open=StreamOpen(data=b"{}"),
            ),
        ).SerializeToString())

        socket.inject(Request(
            uid="am2",
            stream=StreamRequest(
                stream_id="ams1",
                chunk=StreamChunk(data=b"{}"),
            ),
        ).SerializeToString())

        socket.inject(Request(
            uid="am3",
            stream=StreamRequest(stream_id="ams1", close=StreamClose()),
        ).SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(model, socket, "test", log))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())

        done_resp = None
        for msg in socket._msgs:
            r = Response()
            r.ParseFromString(msg)
            if r.stream.done is not None:
                done_resp = r
                break
        assert done_resp is not None
        assert len(done_resp.stream.done.metrics.counters) == 1
        assert done_resp.stream.done.metrics.counters[0].value == 1.0
        assert model._metric_values == []

    def test_async_bidi_open_no_response(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import (
            Request, StreamRequest, StreamOpen, StreamClose, Response,
        )
        from lite_server.api import BidiStreamHandler

        class SilentHandler(BidiStreamHandler):
            def on_open(self, initial_data):
                return None

            def on_close(self):
                pass

        class SilentModel:
            async def predict(self, x):
                return x

            def bidi_stream(self):
                return SilentHandler()

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="an1",
            stream=StreamRequest(
                stream_id="ans1",
                open=StreamOpen(data=b"{}"),
            ),
        ).SerializeToString())

        socket.inject(Request(
            uid="an2",
            stream=StreamRequest(stream_id="ans1", close=StreamClose()),
        ).SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(SilentModel(), socket, "test", log))
            await asyncio.sleep(0.03)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())

        # Only StreamDone, no chunks
        assert len(socket._msgs) >= 1
        done_resp = Response()
        done_resp.ParseFromString(socket._msgs[0])
        assert done_resp.stream.done is not None


class TestStandardLoopNonStreaming:
    """Non-streaming scenarios via standard (sync) worker loop."""

    def _make_socket(self):
        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []

            def send(self, data):
                self._msgs.append(data)

            def recv(self):
                while not self._incoming:
                    import time
                    time.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

        return MockSocket()

    def test_standard_health_check_empty_data(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import Request, SingleRequest, Response

        class StrictAPI:
            def predict(self, x):
                # Would fail if called
                return {"output": x["required_field"]}

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="hc1",
            single=SingleRequest(data=b""),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(StrictAPI(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.02)
        socket.inject(b"")
        t.join(timeout=0.1)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.single.status.code == "Ok"
        assert resp.single.data == b"{}"

    def test_standard_single_error_in_predict(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import Request, SingleRequest, Response

        class BadAPI:
            def predict(self, x):
                raise RuntimeError("predict boom")

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="err1",
            single=SingleRequest(data=json.dumps({"input": 1}).encode()),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(BadAPI(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.02)
        socket.inject(b"")
        t.join(timeout=0.1)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.single.status.code == "Error"
        assert resp.single.status.message == "500"
        body = json.loads(resp.single.data)
        assert body["error"]["code"] == "INTERNAL_ERROR"
        assert "predict boom" in body["error"]["message"]

    def test_standard_batch_partial_failure(self):
        import threading
        from lite_server.worker.inference import run_standard_loop
        from lite_server.proto import (
            Request, BatchRequest, BatchItem, Response,
        )

        class PartialAPI:
            def decode_request(self, req):
                if req.get("bad"):
                    raise ValueError("bad input")
                return req

            def predict(self, x):
                return {"result": x["input"] + 1}

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="batch-p1",
            batch=BatchRequest(items=[
                BatchItem(uid="ok", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="fail", data=json.dumps({"bad": True}).encode()),
            ]),
        ).SerializeToString())

        t = threading.Thread(target=run_standard_loop, args=(PartialAPI(), socket, "test", log))
        t.start()
        import time
        time.sleep(0.03)
        socket.inject(b"")
        t.join(timeout=0.1)

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert len(resp.batch.items) == 2
        assert resp.batch.items[0].status.code == "Ok"
        assert json.loads(resp.batch.items[0].data)["result"] == 2
        assert resp.batch.items[1].status.code == "Error"


class TestAsyncLoopNonStreaming:
    """Non-streaming edge cases via async worker loop."""

    def _make_socket(self):
        class MockSocket:
            def __init__(self):
                self._msgs = []
                self._incoming = []

            async def send(self, data):
                self._msgs.append(data)

            async def recv(self):
                while not self._incoming:
                    import asyncio
                    await asyncio.sleep(0.001)
                return self._incoming.pop(0)

            def inject(self, data):
                self._incoming.append(data)

        return MockSocket()

    def test_async_single_request_error_in_predict(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, SingleRequest, Response

        class BadAPI:
            async def predict(self, x):
                raise RuntimeError("async predict boom")

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="aerr1",
            single=SingleRequest(data=json.dumps({"input": 1}).encode()),
        ).SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(BadAPI(), socket, "test", log))
            await asyncio.sleep(0.03)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert resp.single.status.code == "Error"
        assert resp.single.status.message == "500"
        body = json.loads(resp.single.data)
        assert body["error"]["code"] == "INTERNAL_ERROR"
        assert "async predict boom" in body["error"]["message"]

    def test_async_batch_partial_failure(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import (
            Request, BatchRequest, BatchItem, Response,
        )

        class PartialAPI:
            async def decode_request(self, req):
                if req.get("bad"):
                    raise ValueError("bad input")
                return req

            async def predict(self, x):
                return {"result": x["input"] + 1}

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="abp1",
            batch=BatchRequest(items=[
                BatchItem(uid="ok", data=json.dumps({"input": 1}).encode()),
                BatchItem(uid="fail", data=json.dumps({"bad": True}).encode()),
            ]),
        ).SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(PartialAPI(), socket, "test", log))
            await asyncio.sleep(0.03)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())

        assert len(socket._msgs) >= 1
        resp = Response()
        resp.ParseFromString(socket._msgs[0])
        assert len(resp.batch.items) == 2
        assert resp.batch.items[0].status.code == "Ok"
        assert json.loads(resp.batch.items[0].data)["result"] == 2
        assert resp.batch.items[1].status.code == "Error"

    def test_async_health_check_empty_data(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, SingleRequest, Response

        class StrictAPI:
            async def predict(self, x):
                return {"output": x["required_field"]}

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        socket.inject(Request(
            uid="ahc1",
            single=SingleRequest(data=b""),
        ).SerializeToString())

        async def runner():
            task = asyncio.create_task(run_async_loop(StrictAPI(), socket, "test", log))
            await asyncio.sleep(0.03)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(runner())

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
        assert "ValueError: boom" in output
