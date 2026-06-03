"""pytest unit tests for lite_server.cli commands."""

import json
import os
import textwrap
import zipfile
from pathlib import Path

import pytest

from lite_server import cli


# ===== version =====

class TestVersion:
    def test_version_flag_matches_package_version(self, capsys):
        """--version must output the same version as lite_server.__version__."""
        import lite_server
        expected = f"lite-server {lite_server.__version__}"
        with pytest.raises(SystemExit) as exc_info:
            cli.main(["--version"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert expected in captured.out


# ===== analyze =====

class TestAnalyze:
    def test_analyze_valid_model(self, tmp_path, monkeypatch):
        repo = tmp_path / "model_repo"
        model_dir = repo / "test_model" / "1"
        model_dir.mkdir(parents=True)

        model_py = model_dir / "model.py"
        model_py.write_text(textwrap.dedent('''
            class MyModel:
                def setup(self, device):
                    pass
                def decode_request(self, request):
                    return request.get("input", 0)
                def predict(self, x):
                    return {"output": x * 2}
                def encode_response(self, output):
                    return output
        '''))

        config_yaml = model_dir / "config.yaml"
        config_yaml.write_text("max_batch_size: 4\nbatch_timeout: 0.01\naccelerator: cpu\n")

        out_dir = tmp_path / "reports"
        args = type("Args", (), {
            "model_repo": str(repo),
            "model": "test_model",
            "output_dir": str(out_dir),
        })()
        assert cli._cmd_analyze(args) == 0
        assert out_dir.exists()
        report_files = list(out_dir.iterdir())
        assert len(report_files) == 1
        report = json.loads(report_files[0].read_text())
        assert report["model_name"] == "test_model"
        assert report["version"] == "1"
        assert report["has_model_py"] is True
        assert report["has_config"] is True
        assert "predict" in report["methods"]
        assert "decode_request" in report["methods"]
        assert "encode_response" in report["methods"]
        assert report["config"]["max_batch_size"] == 4

    def test_analyze_missing_model(self, tmp_path, caplog):
        args = type("Args", (), {
            "model_repo": str(tmp_path / "model_repo"),
            "model": "missing",
            "output_dir": str(tmp_path / "reports"),
        })()
        assert cli._cmd_analyze(args) == 1
        assert "not found" in caplog.text.lower()

    def test_analyze_missing_predict(self, tmp_path, monkeypatch):
        repo = tmp_path / "model_repo"
        model_dir = repo / "bad_model" / "1"
        model_dir.mkdir(parents=True)

        model_py = model_dir / "model.py"
        model_py.write_text(textwrap.dedent('''
            class BadModel:
                def setup(self, device):
                    pass
        '''))

        out_dir = tmp_path / "reports"
        args = type("Args", (), {
            "model_repo": str(repo),
            "model": "bad_model",
            "output_dir": str(out_dir),
        })()
        assert cli._cmd_analyze(args) == 0
        report_files = list(out_dir.iterdir())
        report = json.loads(report_files[0].read_text())
        assert report["has_model_py"] is True
        assert "predict" not in report["methods"]
        assert report["warnings"]

    def test_analyze_no_config(self, tmp_path):
        repo = tmp_path / "model_repo"
        model_dir = repo / "test_model" / "1"
        model_dir.mkdir(parents=True)
        model_dir.joinpath("model.py").write_text(textwrap.dedent('''
            class MyModel:
                def predict(self, x):
                    return x
        '''))

        out_dir = tmp_path / "reports"
        args = type("Args", (), {
            "model_repo": str(repo),
            "model": "test_model",
            "output_dir": str(out_dir),
        })()
        assert cli._cmd_analyze(args) == 0
        report_files = list(out_dir.iterdir())
        report = json.loads(report_files[0].read_text())
        assert report["has_config"] is False
        assert report["config"] == {}


# ===== pack / unpack =====

class TestPackUnpack:
    def test_pack_valid_model(self, tmp_path):
        model_dir = tmp_path / "my_model"
        vdir = model_dir / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("# model")
        (vdir / "config.yaml").write_text("max_batch_size: 1\n")

        out_dir = tmp_path / "artifacts"
        args = type("Args", (), {
            "model_dir": str(model_dir),
            "version": "1",
            "output": str(out_dir),
            "name": None,
        })()
        assert cli._cmd_pack(args) == 0
        artifact = out_dir / "my_model_v1.lma"
        assert artifact.exists()
        with zipfile.ZipFile(artifact, "r") as zf:
            assert "manifest.json" in zf.namelist()

    def test_pack_missing_dir(self, tmp_path, capsys):
        args = type("Args", (), {
            "model_dir": str(tmp_path / "nope"),
            "version": "1",
            "output": str(tmp_path),
            "name": None,
        })()
        assert cli._cmd_pack(args) == 1

    def test_pack_rejects_invalid_version(self, tmp_path, caplog):
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        args = type("Args", (), {
            "model_dir": str(model_dir),
            "version": "abc",
            "output": str(tmp_path),
            "name": None,
        })()
        assert cli._cmd_pack(args) == 1
        assert "invalid version" in caplog.text.lower()

    def test_pack_with_name_override(self, tmp_path):
        model_dir = tmp_path / "my_model"
        vdir = model_dir / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("# model")

        out_dir = tmp_path / "artifacts"
        args = type("Args", (), {
            "model_dir": str(model_dir),
            "version": "1",
            "output": str(out_dir),
            "name": "custom",
        })()
        assert cli._cmd_pack(args) == 0
        assert (out_dir / "custom_v1.lma").exists()

    def test_unpack_valid_artifact(self, tmp_path):
        from lite_server.artifact import ModelPacker, ModelUnpacker

        model_dir = tmp_path / "src_model"
        vdir = model_dir / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("# model")
        (vdir / "config.yaml").write_text("max_batch_size: 1\n")

        packer = ModelPacker(model_dir, version="1")
        artifact = packer.pack(tmp_path / "artifacts")

        target = tmp_path / "unpacked"
        args = type("Args", (), {
            "artifact": str(artifact),
            "target_dir": str(target),
            "flat": False,
        })()
        assert cli._cmd_unpack(args) == 0
        assert (target / "src_model" / "1" / "model.py").exists()

    def test_unpack_missing_artifact(self, tmp_path, capsys):
        args = type("Args", (), {
            "artifact": str(tmp_path / "nope.lma"),
            "target_dir": str(tmp_path),
            "flat": False,
        })()
        assert cli._cmd_unpack(args) == 1

    def test_pack_with_signing(self, tmp_path, monkeypatch):
        key_dir = tmp_path / "keys"
        key_dir.mkdir()
        key_file = key_dir / "sign.key"
        monkeypatch.setenv("LITE_SERVER_SIGN_KEY", str(key_file))

        model_dir = tmp_path / "signed_model"
        vdir = model_dir / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("# model")

        out_dir = tmp_path / "artifacts"
        args = type("Args", (), {
            "model_dir": str(model_dir),
            "version": "1",
            "output": str(out_dir),
            "name": None,
        })()
        assert cli._cmd_pack(args) == 0
        artifact = out_dir / "signed_model_v1.lma"
        assert artifact.exists()
        with zipfile.ZipFile(artifact, "r") as zf:
            manifest = json.loads(zf.read("manifest.json"))
        assert manifest["signature"] != ""
        assert key_file.exists(), "signing key should have been auto-generated"

    def test_unpack_with_signature_verification(self, tmp_path, monkeypatch):
        key_dir = tmp_path / "keys"
        key_dir.mkdir()
        key_file = key_dir / "sign.key"
        monkeypatch.setenv("LITE_SERVER_SIGN_KEY", str(key_file))

        model_dir = tmp_path / "src_model"
        vdir = model_dir / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("# model")
        (vdir / "config.yaml").write_text("max_batch_size: 1\n")

        artifact_dir = tmp_path / "artifacts"
        pack_args = type("Args", (), {
            "model_dir": str(model_dir),
            "version": "1",
            "output": str(artifact_dir),
            "name": None,
        })()
        assert cli._cmd_pack(pack_args) == 0

        target = tmp_path / "unpacked"
        unpack_args = type("Args", (), {
            "artifact": str(artifact_dir / "src_model_v1.lma"),
            "target_dir": str(target),
            "flat": False,
        })()
        assert cli._cmd_unpack(unpack_args) == 0
        assert (target / "src_model" / "1" / "model.py").exists()

    def test_unpack_tampered_artifact_fails(self, tmp_path, monkeypatch):
        key_dir = tmp_path / "keys"
        key_dir.mkdir()
        key_file = key_dir / "sign.key"
        monkeypatch.setenv("LITE_SERVER_SIGN_KEY", str(key_file))

        model_dir = tmp_path / "src_model"
        vdir = model_dir / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("# original")

        artifact_dir = tmp_path / "artifacts"
        pack_args = type("Args", (), {
            "model_dir": str(model_dir),
            "version": "1",
            "output": str(artifact_dir),
            "name": None,
        })()
        assert cli._cmd_pack(pack_args) == 0

        artifact = artifact_dir / "src_model_v1.lma"
        with zipfile.ZipFile(artifact, "a") as zf:
            zf.writestr("1/model.py", "# tampered")

        target = tmp_path / "unpacked"
        unpack_args = type("Args", (), {
            "artifact": str(artifact),
            "target_dir": str(target),
            "flat": False,
        })()
        assert cli._cmd_unpack(unpack_args) == 1


# ===== init =====

# ===== config-check =====

class TestConfigCheck:
    def test_config_check_valid_server_yaml(self, tmp_path):
        config = tmp_path / "server.yaml"
        config.write_text(textwrap.dedent("""\
            server:
              http_port: 8000
              host: 0.0.0.0
        """))
        args = type("Args", (), {"config": str(config)})()
        assert cli._cmd_config_check(args) == 0

    def test_config_check_invalid_yaml(self, tmp_path, caplog):
        config = tmp_path / "bad.yaml"
        config.write_text("server: [invalid")
        args = type("Args", (), {"config": str(config)})()
        assert cli._cmd_config_check(args) == 1
        assert "error" in caplog.text.lower()

    def test_config_check_validates_model_config(self, tmp_path):
        """config-check should validate model config YAML syntax, not just server config."""
        config = tmp_path / "model_config.yaml"
        config.write_text(textwrap.dedent("""\
            max_batch_size: 4
            stream: true
            accelerator: cpu
            max_queue_size: 100
        """))
        args = type("Args", (), {"config": str(config)})()
        assert cli._cmd_config_check(args) == 0


class TestInit:
    def test_init_empty_template(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = type("Args", (), {
            "project_name": "empty_proj",
            "template": "empty",
            "wizard": False,
        })()
        assert cli._cmd_init(args) == 0
        proj = tmp_path / "empty_proj"
        assert (proj / "model_repo" / "my_model" / "1" / "model.py").exists()
        assert (proj / "model_repo" / "my_model" / "1" / "config.yaml").exists()
        assert (proj / "server.yaml").exists()
        assert (proj / "test_request.py").exists()
        assert (proj / "README.md").exists()
        assert (proj / "Dockerfile").exists()
        assert (proj / "Makefile").exists()
        assert (proj / "docker-compose.yml").exists()
        assert (proj / "requirements.txt").exists()
        assert (proj / ".gitignore").exists()
        assert (proj / ".github" / "workflows" / "ci.yml").exists()
        assert not (proj / "model_repo" / "orchestration.yaml").exists()
        assert (proj / "model_repo" / "my_model" / "1" / "config.yaml.example").exists()
        # Verify test_request.py uses requests (not httpx) and has health check
        tr = (proj / "test_request.py").read_text()
        assert "import requests" in tr
        assert "test_health" in tr

    def test_init_existing_dir_fails(self, tmp_path, monkeypatch, caplog):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "exists").mkdir()
        args = type("Args", (), {
            "project_name": "exists",
            "template": "empty",
            "wizard": False,
        })()
        assert cli._cmd_init(args) == 1
        assert "already exists" in caplog.text.lower()

    def test_init_default_project_name(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = type("Args", (), {
            "project_name": None,
            "template": "empty",
            "wizard": False,
        })()
        assert cli._cmd_init(args) == 0
        assert (tmp_path / "my_project").exists()

    def test_init_wizard_mode(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        inputs = iter(["wiz_proj", "", "", "", "", "", "", ""])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))
        args = type("Args", (), {
            "project_name": None,
            "template": "empty",
            "wizard": True,
        })()
        assert cli._cmd_init(args) == 0
        assert (tmp_path / "wiz_proj").exists()


# ===== benchmark =====

class TestBenchmark:
    def test_benchmark_uses_client_context_manager(self, monkeypatch):
        """Verify benchmark reuses a single httpx.AsyncClient instead of creating
        a new connection per request."""
        import time
        import sys
        import asyncio

        mock_response = type("Response", (), {"status_code": 200})()
        post_calls = []
        client_instances = []

        class FakeLimits:
            def __init__(self, *args, **kwargs):
                pass

        class FakeAsyncClient:
            def __init__(self, *args, **kwargs):
                client_instances.append(self)
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                return False
            async def post(self, url, **kwargs):
                post_calls.append((url, kwargs))
                return mock_response

        fake_httpx = type("FakeHttpx", (), {
            "AsyncClient": FakeAsyncClient,
            "Limits": FakeLimits,
            "Timeout": lambda *args, **kwargs: type("FakeTimeout", (), {})(),
        })()
        monkeypatch.setitem(sys.modules, "httpx", fake_httpx)

        # time.monotonic is called extensively; make it monotonically increase
        # so the benchmark loop runs a few rounds then exits.
        t = [0.0]
        def fake_monotonic():
            t[0] += 0.001
            return t[0]

        monkeypatch.setattr(time, "monotonic", fake_monotonic)

        args = type("Args", (), {
            "url": "http://127.0.0.1:8000",
            "model": "test_model",
            "version": None,
            "concurrency": 1,
            "duration": 0.005,
        })()
        cli._cmd_benchmark(args)

        # Exactly one AsyncClient instance should be created
        assert len(client_instances) == 1, f"Expected 1 AsyncClient, got {len(client_instances)}"
        assert len(post_calls) >= 1
        for url, kwargs in post_calls:
            assert kwargs.get("json") == {"input": 1.0}


# ===== serve CLI args parity with Rust =====

class TestServeArgs:
    def test_serve_help_excludes_transport(self, capsys):
        """--transport was removed from Rust; Python CLI must not advertise it."""
        with pytest.raises(SystemExit) as exc_info:
            cli.main(["serve", "--help"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "--transport" not in captured.out

    def test_serve_help_includes_metrics_port(self, capsys):
        """--metrics-port must be available in Python CLI (parity with Rust)."""
        with pytest.raises(SystemExit) as exc_info:
            cli.main(["serve", "--help"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "--metrics-port" in captured.out

    def test_serve_help_includes_health_check_interval(self, capsys):
        """--health-check-interval must be available in Python CLI (parity with Rust)."""
        with pytest.raises(SystemExit) as exc_info:
            cli.main(["serve", "--help"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "--health-check-interval" in captured.out

    def test_cmd_serve_forwards_correct_kwargs(self, monkeypatch):
        """_cmd_serve must forward exactly the params Rust serve() accepts."""
        called_with = {}

        def fake_serve(**kwargs):
            called_with.update(kwargs)

        monkeypatch.setattr("lite_server.serve", fake_serve)

        args = type("Args", (), {
            "config": None,
            "port": 9000,
            "host": "127.0.0.1",
            "model_repo": "./models",
            "threads": 4,
            "timeout": 60.0,
            "log_level": "debug",
            "log_info_output": "/tmp/info.log",
            "log_error_output": "/tmp/error.log",
            "log_rotation": "daily",
            "no_metrics": True,
            "grpc_port": 50051,
            "no_grpc": True,
            "no_streaming_metrics": True,
            "max_queue_size": 200,
            "max_requests": 1000,
            "max_requests_jitter": 50,
            "request_timeout": 30.0,
            "graceful_timeout": 60.0,
            "keepalive_timeout": 10.0,
            "metrics_port": 9090,
            "health_check_interval": 20.0,
            "endpoints_dir": None,
        })()
        assert cli._cmd_serve(args) == 0

        assert called_with["port"] == 9000
        assert called_with["host"] == "127.0.0.1"
        assert called_with["metrics_port"] == 9090
        assert called_with["health_check_interval"] == 20.0
        assert "transport" not in called_with
