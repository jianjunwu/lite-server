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

    def test_analyze_missing_model(self, tmp_path, capsys):
        args = type("Args", (), {
            "model_repo": str(tmp_path / "model_repo"),
            "model": "missing",
            "output_dir": str(tmp_path / "reports"),
        })()
        assert cli._cmd_analyze(args) == 1
        captured = capsys.readouterr()
        assert "not found" in captured.err.lower()

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
        model_dir.mkdir()
        (model_dir / "model.py").write_text("# model")
        (model_dir / "config.yaml").write_text("max_batch_size: 1\n")

        out_dir = tmp_path / "artifacts"
        args = type("Args", (), {
            "model_dir": str(model_dir),
            "version": "1",
            "output": str(out_dir),
        })()
        assert cli._cmd_pack(args) == 0
        artifact = out_dir / "my_model_v1.lma"
        assert artifact.exists()
        # Verify it's a valid zip with manifest.json
        with zipfile.ZipFile(artifact, "r") as zf:
            assert "manifest.json" in zf.namelist()

    def test_pack_missing_dir(self, tmp_path, capsys):
        args = type("Args", (), {
            "model_dir": str(tmp_path / "nope"),
            "version": "1",
            "output": str(tmp_path),
        })()
        assert cli._cmd_pack(args) == 1

    def test_unpack_valid_artifact(self, tmp_path):
        from lite_server.artifact import ModelPacker, ModelUnpacker

        model_dir = tmp_path / "src_model"
        model_dir.mkdir()
        (model_dir / "model.py").write_text("# model")
        (model_dir / "config.yaml").write_text("max_batch_size: 1\n")

        packer = ModelPacker(model_dir, version="1")
        artifact = packer.pack(tmp_path / "artifacts")

        target = tmp_path / "unpacked"
        args = type("Args", (), {
            "artifact": str(artifact),
            "target_dir": str(target),
        })()
        assert cli._cmd_unpack(args) == 0
        assert (target / "model.py").exists()

    def test_unpack_missing_artifact(self, tmp_path, capsys):
        args = type("Args", (), {
            "artifact": str(tmp_path / "nope.lma"),
            "target_dir": str(tmp_path),
        })()
        assert cli._cmd_unpack(args) == 1

    def test_pack_with_signing(self, tmp_path, monkeypatch):
        key_dir = tmp_path / "keys"
        key_dir.mkdir()
        key_file = key_dir / "sign.key"
        monkeypatch.setenv("LITE_SERVER_SIGN_KEY", str(key_file))

        model_dir = tmp_path / "signed_model"
        model_dir.mkdir()
        (model_dir / "model.py").write_text("# model")

        out_dir = tmp_path / "artifacts"
        args = type("Args", (), {
            "model_dir": str(model_dir),
            "version": "2",
            "output": str(out_dir),
        })()
        assert cli._cmd_pack(args) == 0
        artifact = out_dir / "signed_model_v2.lma"
        assert artifact.exists()
        # Verify manifest has signature
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
        model_dir.mkdir()
        (model_dir / "model.py").write_text("# model")
        (model_dir / "config.yaml").write_text("max_batch_size: 1\n")

        artifact_dir = tmp_path / "artifacts"
        pack_args = type("Args", (), {
            "model_dir": str(model_dir),
            "version": "1",
            "output": str(artifact_dir),
        })()
        assert cli._cmd_pack(pack_args) == 0

        target = tmp_path / "unpacked"
        unpack_args = type("Args", (), {
            "artifact": str(artifact_dir / "src_model_v1.lma"),
            "target_dir": str(target),
        })()
        assert cli._cmd_unpack(unpack_args) == 0
        assert (target / "model.py").exists()

    def test_unpack_tampered_artifact_fails(self, tmp_path, monkeypatch):
        key_dir = tmp_path / "keys"
        key_dir.mkdir()
        key_file = key_dir / "sign.key"
        monkeypatch.setenv("LITE_SERVER_SIGN_KEY", str(key_file))

        model_dir = tmp_path / "src_model"
        model_dir.mkdir()
        (model_dir / "model.py").write_text("# original")

        artifact_dir = tmp_path / "artifacts"
        pack_args = type("Args", (), {
            "model_dir": str(model_dir),
            "version": "1",
            "output": str(artifact_dir),
        })()
        assert cli._cmd_pack(pack_args) == 0

        # Tamper with the artifact by appending a modified file
        artifact = artifact_dir / "src_model_v1.lma"
        with zipfile.ZipFile(artifact, "a") as zf:
            zf.writestr("model.py", "# tampered")

        target = tmp_path / "unpacked"
        unpack_args = type("Args", (), {
            "artifact": str(artifact),
            "target_dir": str(target),
        })()
        assert cli._cmd_unpack(unpack_args) == 1


# ===== init =====

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
        assert (proj / "model_repo" / "orchestration.yaml").exists()
        # Verify test_request.py uses requests (not httpx) and has health check
        tr = (proj / "test_request.py").read_text()
        assert "import requests" in tr
        assert "test_health" in tr

    def test_init_llm_template(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = type("Args", (), {
            "project_name": "llm_proj",
            "template": "llm",
            "wizard": False,
        })()
        assert cli._cmd_init(args) == 0
        model_py = tmp_path / "llm_proj" / "model_repo" / "my_model" / "1" / "model.py"
        assert model_py.exists()
        content = model_py.read_text()
        assert "messages" in content
        config = tmp_path / "llm_proj" / "model_repo" / "my_model" / "1" / "config.yaml"
        assert "stream: true" in config.read_text()

    def test_init_nlp_template(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = type("Args", (), {
            "project_name": "nlp_proj",
            "template": "nlp",
            "wizard": False,
        })()
        assert cli._cmd_init(args) == 0
        config = tmp_path / "nlp_proj" / "model_repo" / "my_model" / "1" / "config.yaml"
        assert "max_batch_size" in config.read_text()
        model_py = tmp_path / "nlp_proj" / "model_repo" / "my_model" / "1" / "model.py"
        content = model_py.read_text()
        assert "label" in content.lower()

    def test_init_existing_dir_fails(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "exists").mkdir()
        args = type("Args", (), {
            "project_name": "exists",
            "template": "empty",
            "wizard": False,
        })()
        assert cli._cmd_init(args) == 1
        captured = capsys.readouterr()
        assert "already exists" in captured.err.lower()

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
        """Verify benchmark reuses a single httpx.Client instead of creating
        a new connection per request."""
        import time
        import sys

        mock_response = type("Response", (), {"status_code": 200})()
        post_calls = []
        client_instances = []

        class FakeClient:
            def __init__(self, *args, **kwargs):
                client_instances.append(self)
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def post(self, url, **kwargs):
                post_calls.append((url, kwargs))
                return mock_response

        fake_httpx = type("FakeHttpx", (), {"Client": FakeClient})()
        monkeypatch.setitem(sys.modules, "httpx", fake_httpx)

        # time.time is called extensively; make it monotonically increase
        # so the benchmark loop runs a few rounds then exits.
        t = [0.0]
        def fake_time():
            t[0] += 0.001
            return t[0]

        monkeypatch.setattr(time, "time", fake_time)

        args = type("Args", (), {
            "url": "http://127.0.0.1:8000",
            "model": "test_model",
            "version": None,
            "concurrency": 1,
            "duration": 0.005,
        })()
        cli._cmd_benchmark(args)

        # Exactly one Client instance should be created
        assert len(client_instances) == 1, f"Expected 1 Client, got {len(client_instances)}"
        assert len(post_calls) >= 1
        for url, kwargs in post_calls:
            assert kwargs.get("json") == {"input": 1.0}
