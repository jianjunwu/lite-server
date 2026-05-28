"""pytest unit tests for lite_server.worker.inference."""

import json
import textwrap

import pytest
import yaml

from lite_server.worker.inference import (
    handle_request,
    load_litapi,
    load_model_config,
    pick_loop,
)


class TestLoadModelConfig:
    def test_missing_file_returns_empty(self, tmp_path):
        assert load_model_config(str(tmp_path / "nonexistent.yaml")) == {}

    def test_valid_yaml(self, tmp_path):
        path = tmp_path / "config.yaml"
        path.write_text(yaml.safe_dump({"max_batch_size": 4, "stream": True}))
        cfg = load_model_config(str(path))
        assert cfg["max_batch_size"] == 4
        assert cfg["stream"] is True


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
        api = load_litapi(str(model_py), {})
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
        api = load_litapi(str(model_py), {"accelerator": "cpu"})
        assert api.pre is True
        assert api.dev == ["cpu"]

    def test_raises_when_no_predict(self, tmp_path):
        model_py = tmp_path / "model.py"
        model_py.write_text(textwrap.dedent('''
            class MyModel:
                pass
        '''))
        with pytest.raises(RuntimeError, match="No LitAPI subclass"):
            load_litapi(str(model_py), {})

    def test_passes_config_to_constructor(self, tmp_path):
        model_py = tmp_path / "model.py"
        model_py.write_text(textwrap.dedent('''
            class MyModel:
                def __init__(self, max_batch_size=1, batch_timeout=0.0, stream=False):
                    self.max_batch_size = max_batch_size
                    self.stream = stream
                def predict(self, x):
                    return x
        '''))
        api = load_litapi(str(model_py), {"max_batch_size": 8, "stream": True})
        assert api.max_batch_size == 8
        assert api.stream is True


class TestPickLoop:
    def test_default_returns_single(self):
        assert pick_loop({}) == "single"

    def test_bidirectional_returns_bidirectional(self):
        assert pick_loop({"bidirectional": True}) == "bidirectional"

    def test_continuous_batching_returns_continuous(self):
        assert pick_loop({"continuous_batching": True}) == "continuous"

    def test_batch_size_greater_than_one_returns_batched(self):
        assert pick_loop({"max_batch_size": 4}) == "batched"


class TestHandleRequest:
    @pytest.fixture
    def lit_api(self):
        class SimpleAPI:
            def decode_request(self, request):
                return request.get("input", 0)

            def predict(self, x):
                return {"output": x * 2}

            def encode_response(self, output):
                return output

        return SimpleAPI()

    @pytest.mark.asyncio
    async def test_infer_ok(self, lit_api):
        req = {
            "uid": "test-1",
            "payload": {"type": "INFER", "data": {"input": 5}},
        }
        resp = await handle_request(lit_api, req)
        assert resp["status"]["code"] == "Ok"
        assert resp["data"]["output"] == 10
        assert resp["uid"] == "test-1"

    @pytest.mark.asyncio
    async def test_infer_without_decode(self):
        class NoDecode:
            def predict(self, x):
                return x

        req = {
            "uid": "test-2",
            "payload": {"type": "INFER", "data": {"input": 3}},
        }
        resp = await handle_request(NoDecode(), req)
        assert resp["status"]["code"] == "Ok"
        # predict returns a dict directly, so data is the dict itself
        assert resp["data"] == {"input": 3}

    @pytest.mark.asyncio
    async def test_infer_without_encode(self):
        class NoEncode:
            def predict(self, x):
                return x * 3

        req = {
            "uid": "test-3",
            "payload": {"type": "INFER", "data": 4},
        }
        resp = await handle_request(NoEncode(), req)
        assert resp["status"]["code"] == "Ok"
        assert resp["data"]["output"] == 12

    @pytest.mark.asyncio
    async def test_infer_missing_predict(self):
        class NoPredict:
            pass

        req = {
            "uid": "test-4",
            "payload": {"type": "INFER", "data": {}},
        }
        resp = await handle_request(NoPredict(), req)
        assert resp["status"]["code"] == "Error"
        assert "predict method not found" in resp["status"]["message"]

    @pytest.mark.asyncio
    async def test_stream_open(self, lit_api):
        req = {
            "uid": "test-5",
            "payload": {"type": "STREAM_OPEN", "stream_id": "s1"},
        }
        resp = await handle_request(lit_api, req)
        assert resp["status"]["code"] == "Ok"
        assert resp["data"]["stream_id"] == "s1"

    @pytest.mark.asyncio
    async def test_stream_chunk_with_handler(self, lit_api):
        class StreamAPI:
            def stream_chunk(self, stream_id, chunk):
                return {"echo": chunk}

        req = {
            "uid": "test-6",
            "payload": {"type": "STREAM_CHUNK", "stream_id": "s1", "chunk": {"x": 1}},
        }
        resp = await handle_request(StreamAPI(), req)
        assert resp["status"]["code"] == "Streaming"
        assert resp["data"]["echo"]["x"] == 1

    @pytest.mark.asyncio
    async def test_stream_chunk_without_handler(self, lit_api):
        req = {
            "uid": "test-7",
            "payload": {"type": "STREAM_CHUNK", "stream_id": "s1", "chunk": {}},
        }
        resp = await handle_request(lit_api, req)
        assert resp["status"]["code"] == "FinishStreaming"

    @pytest.mark.asyncio
    async def test_stream_close(self, lit_api):
        class StreamCloseAPI:
            def stream_close(self, stream_id):
                self.closed = stream_id

        api = StreamCloseAPI()
        req = {
            "uid": "test-8",
            "payload": {"type": "STREAM_CLOSE", "stream_id": "s1"},
        }
        resp = await handle_request(api, req)
        assert resp["status"]["code"] == "FinishStreaming"
        assert api.closed == "s1"

    @pytest.mark.asyncio
    async def test_stream_cancel(self, lit_api):
        class StreamCancelAPI:
            def stream_cancel(self, stream_id):
                self.cancelled = stream_id

        api = StreamCancelAPI()
        req = {
            "uid": "test-9",
            "payload": {"type": "STREAM_CANCEL", "stream_id": "s2"},
        }
        resp = await handle_request(api, req)
        assert resp["status"]["code"] == "FinishStreaming"
        assert api.cancelled == "s2"

    @pytest.mark.asyncio
    async def test_unknown_msg_type(self, lit_api):
        req = {
            "uid": "test-10",
            "payload": {"type": "UNKNOWN", "data": {}},
        }
        resp = await handle_request(lit_api, req)
        assert resp["status"]["code"] == "Error"
        assert "Unknown msg_type" in resp["status"]["message"]

    @pytest.mark.asyncio
    async def test_predict_exception_propagates(self):
        """handle_request does not swallow predict exceptions; caller must catch."""
        class BrokenAPI:
            def predict(self, x):
                raise ValueError("boom")

        req = {
            "uid": "test-11",
            "payload": {"type": "INFER", "data": {}},
        }
        with pytest.raises(ValueError, match="boom"):
            await handle_request(BrokenAPI(), req)

    @pytest.mark.asyncio
    async def test_legacy_msg_type_key(self, lit_api):
        """Ensure backward compatibility with msg_type key."""
        req = {
            "uid": "test-12",
            "payload": {"msg_type": "INFER", "data": {"input": 2}},
        }
        resp = await handle_request(lit_api, req)
        assert resp["status"]["code"] == "Ok"
        assert resp["data"]["output"] == 4
