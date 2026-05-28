"""pytest tests converted from original worker_test.py unittest."""

import textwrap

import pytest

from lite_server.worker import inference
from lite_server import cli


class TestInferenceModule:
    def test_has_parse_args(self):
        assert hasattr(inference, "parse_args")

    def test_has_worker_main(self):
        assert hasattr(inference, "worker_main")

    def test_has_continuous_batching_loop(self):
        assert hasattr(inference, "ContinuousBatchingLoop")


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


class TestHandleRequestInfer:
    @pytest.mark.asyncio
    async def test_basic_inference(self):
        class MockAPI:
            def predict(self, data):
                return {"output": data.get("input", 0) * 2}

        request = {
            "uid": "test-123",
            "payload": {"type": "INFER", "data": {"input": 5}},
        }
        response = await inference.handle_request(MockAPI(), request)
        assert response["uid"] == "test-123"
        assert response["data"]["output"] == 10
        assert response["status"]["code"] == "Ok"


class TestCLI:
    def test_cli_has_main(self):
        assert hasattr(cli, "main")
