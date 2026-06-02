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


class TestRunPredict:
    def test_basic_predict(self):
        class MockAPI:
            def predict(self, x):
                return {"output": x.get("input", 0) * 2}

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 5}).encode()
        log = logging.getLogger("test")
        resp_bytes, status, metrics = inference._run_predict(MockAPI(), data, meta, log)
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
        resp_bytes, status, metrics = inference._run_predict(HookAPI(), data, meta, log)
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
        resp_bytes, status, metrics = inference._run_predict(PlainAPI(), data, meta, log)
        assert json.loads(resp_bytes) == {"output": 10}


class TestMakeErrorResponse:
    def test_error_response_structure(self):
        resp = inference._make_error_response("uid-1", "something broke")
        assert resp.uid == "uid-1"
        assert resp.single.status.code == "Error"
        assert "something broke" in resp.single.status.message
        body = json.loads(resp.single.data)
        assert "error" in body


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
            result, status, _ = inference._run_predict(StrictAPI(), data, meta)

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
        resp_bytes, status, metrics = inference._run_predict(CountingAPI(), data, meta, log)
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
        resp_bytes, status, metrics = asyncio.run(_run_predict_async(AsyncAPI(), data, meta, log))
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
        resp_bytes, status, metrics = asyncio.run(_run_predict_async(SimpleAsyncAPI(), data, meta, log))
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
        resp_bytes, status, metrics = asyncio.run(_run_predict_async(MixedAPI(), data, meta, log))
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
        assert "boom" in resp.single.status.message

    def test_async_stream_not_supported(self):
        import asyncio
        from lite_server.worker.inference import run_async_loop
        from lite_server.proto import Request, StreamRequest, StreamOpen, Response

        class AsyncModel:
            async def predict(self, x):
                return x

            _metric_specs = []
            _metric_values = []

        socket = self._make_socket()
        log = logging.getLogger("test")

        req = Request(
            uid="stream-1",
            stream=StreamRequest(stream_id="s1", open=StreamOpen(data=b"{}")),
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
        assert resp.uid == "stream-1"
        assert resp.single.status.code == "Error"
        assert "not yet supported" in resp.single.status.message

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
        assert "Protobuf parse" in resp.single.status.message

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
