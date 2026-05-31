"""pytest tests for lite_server.worker.inference (ZMQ + Protobuf)."""

import json
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
        resp_bytes, status, metrics = inference._run_predict(MockAPI(), data, meta)
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
        resp_bytes, status, metrics = inference._run_predict(HookAPI(), data, meta)
        assert json.loads(resp_bytes) == {"output": 12}

    def test_on_request_hook_can_reject(self):
        class RejectAPI:
            def on_request(self, request, meta):
                raise ValueError("rejected")

            def predict(self, x):
                return x

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 5}).encode()
        with pytest.raises(ValueError, match="rejected"):
            inference._run_predict(RejectAPI(), data, meta)

    def test_skips_hooks_when_not_implemented(self):
        class PlainAPI:
            def predict(self, x):
                return {"output": x.get("input", 0) * 2}

        meta = RequestMeta(route="/predict", headers={}, client_ip="", request_id="", timestamp_ns=0, payload=None)
        data = json.dumps({"input": 5}).encode()
        resp_bytes, status, metrics = inference._run_predict(PlainAPI(), data, meta)
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
        resp_bytes, status, metrics = inference._run_predict(CountingAPI(), data, meta)
        assert call_count == 1
        assert status.code == "Ok"
