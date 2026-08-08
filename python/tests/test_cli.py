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
    """CLI is a thin shell over StaticAnalyzer: args, rendering, exit codes.

    Exit code protocol: 0 = no error-level findings, 1 = findings at the
    configured --fail-severity, 2 = analysis itself failed.
    """

    def _args(self, repo, model, output_dir=None, **overrides):
        base = {
            "model_repo": str(repo),
            "model": model,
            "version": None,
            "format": "json",
            "output_dir": str(output_dir) if output_dir else None,
            "fail_severity": "error",
            "strict": False,
            "deep": False,
            "deep_timeout": 30.0,
            "interop": None,
        }
        base.update(overrides)
        return type("Args", (), base)()

    def _make_model(self, repo, name, model_py, config=None):
        vdir = repo / name / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text(textwrap.dedent(model_py))
        if config:
            (vdir / "config.yaml").write_text(config)
        return vdir

    def test_analyze_valid_model(self, tmp_path, capsys):
        repo = tmp_path / "model_repo"
        self._make_model(repo, "test_model", '''
            from lite_server import LitAPI

            class MyModel(LitAPI):
                def setup(self, device):
                    pass
                def predict(self, x):
                    return {"output": x * 2}
                def encode_response(self, output):
                    return output
        ''', config="max_batch_size: 1\n")
        out_dir = tmp_path / "reports"

        rc = cli._cmd_analyze(self._args(repo, "test_model", out_dir))
        assert rc == 0

        # stdout carries the authoritative schema v1 JSON
        report = json.loads(capsys.readouterr().out)
        assert report["schema_version"] == 1
        assert report["target"]["model_name"] == "test_model"
        assert report["target"]["executed_user_code"] is False
        assert report["api_class"]["name"] == "MyModel"
        assert report["api_class"]["confidence"] == "exact"
        assert report["methods"]["core_required"]["predict"] == "implemented"
        assert report["config"]["max_batch_size"] == 1

        # --output-dir additionally persists json + markdown
        files = {f.name for f in out_dir.iterdir()}
        assert files == {"test_model_analysis.json", "test_model_analysis.md"}

    def test_analyze_missing_model(self, tmp_path, caplog):
        repo = tmp_path / "model_repo"
        repo.mkdir()
        rc = cli._cmd_analyze(self._args(repo, "missing"))
        assert rc == 2
        assert "not found" in caplog.text.lower()

    def test_analyze_missing_predict_exits_1(self, tmp_path, capsys):
        repo = tmp_path / "model_repo"
        self._make_model(repo, "bad_model", '''
            from lite_server import LitAPI

            class BadModel(LitAPI):
                def setup(self, device):
                    pass
        ''')
        rc = cli._cmd_analyze(self._args(repo, "bad_model"))
        assert rc == 1
        report = json.loads(capsys.readouterr().out)
        ls001 = [f for f in report["findings"] if f["rule_id"] == "LS001"]
        assert ls001 and ls001[0]["severity"] == "error"

    def test_analyze_no_config(self, tmp_path, capsys):
        repo = tmp_path / "model_repo"
        self._make_model(repo, "test_model", '''
            from lite_server import LitAPI

            class MyModel(LitAPI):
                def setup(self, device):
                    pass
                def predict(self, x):
                    return x
        ''')
        rc = cli._cmd_analyze(self._args(repo, "test_model"))
        assert rc == 0
        report = json.loads(capsys.readouterr().out)
        assert report["files"]["has_config"] is False
        assert report["config"] == {}

    def test_analyze_format_markdown(self, tmp_path, capsys):
        repo = tmp_path / "model_repo"
        self._make_model(repo, "test_model", '''
            from lite_server import LitAPI

            class MyModel(LitAPI):
                def setup(self, device):
                    pass
                def predict(self, x):
                    return x
        ''')
        rc = cli._cmd_analyze(self._args(repo, "test_model", format="markdown"))
        assert rc == 0
        out = capsys.readouterr().out
        assert "# Analysis Report: test_model" in out
        assert "MyModel" in out

    def test_analyze_strict_promotes_warnings(self, tmp_path, capsys):
        repo = tmp_path / "model_repo"
        # missing setup → LS102 warning; implicit latest → LS111 warning
        self._make_model(repo, "test_model", '''
            from lite_server import LitAPI

            class MyModel(LitAPI):
                def predict(self, x):
                    return x
        ''')
        capsys.readouterr()
        assert cli._cmd_analyze(self._args(repo, "test_model")) == 0
        capsys.readouterr()
        assert cli._cmd_analyze(self._args(repo, "test_model", strict=True)) == 1

    def test_analyze_path_traversal_returns_2(self, tmp_path, caplog):
        repo = tmp_path / "model_repo"
        repo.mkdir()
        rc = cli._cmd_analyze(self._args(repo, "../outside"))
        assert rc == 2


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

    def test_config_check_rejects_non_integer_port(self, tmp_path):
        """config-check should reject http_port when it's not an integer.

        Rust serde deserialization fails on type mismatch (e.g. u16 ← string),
        but the Python config-check only validates YAML syntax, not value types.
        A config that passes config-check should be loadable by the server.
        """
        config = tmp_path / "bad_port.yaml"
        config.write_text("server:\n  http_port: not_a_number\n")
        args = type("Args", (), {"config": str(config)})()
        result = cli._cmd_config_check(args)
        assert result == 1, (
            f"Expected config-check to reject non-integer http_port, got exit code {result}. "
            "The server (Rust serde) would reject this config at startup."
        )

    def test_config_check_rejects_non_bool_stream(self, tmp_path):
        """config-check should reject model config stream field when it's not a bool."""
        config = tmp_path / "bad_stream.yaml"
        config.write_text("stream: yes_i_am_a_string\n")
        args = type("Args", (), {"config": str(config)})()
        result = cli._cmd_config_check(args)
        assert result == 1, (
            f"Expected config-check to reject non-boolean stream value, got exit code {result}. "
            "The server (Rust serde) would reject this config at startup."
        )

    def test_config_check_rejects_non_integer_max_batch_size(self, tmp_path):
        """config-check should reject max_batch_size when it's not an integer."""
        config = tmp_path / "bad_batch.yaml"
        config.write_text("max_batch_size: large\n")
        args = type("Args", (), {"config": str(config)})()
        result = cli._cmd_config_check(args)
        assert result == 1, (
            f"Expected config-check to reject non-integer max_batch_size, got exit code {result}. "
            "The server (Rust serde) would reject this config at startup."
        )


class TestInit:
    def test_init_empty_template(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = type("Args", (), {
            "project_name": "empty_proj",
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
            "wizard": False,
        })()
        assert cli._cmd_init(args) == 1
        assert "already exists" in caplog.text.lower()

    def test_init_model_only(self, tmp_path, monkeypatch):
        """--model-only: model version dir only, no project shell."""
        monkeypatch.chdir(tmp_path)
        args = type("Args", (), {
            "project_name": "demo_model",
            "wizard": False,
            "model_only": True,
        })()
        assert cli._cmd_init(args) == 0
        model_dir = tmp_path / "model_repo" / "demo_model" / "1"
        assert (model_dir / "model.py").exists()
        assert (model_dir / "config.yaml").exists()
        assert (model_dir / "config.yaml.example").exists()
        assert (model_dir / "callbacks.py").exists()
        assert not (tmp_path / "server.yaml").exists()
        assert not (tmp_path / "README.md").exists()

    def test_init_model_only_existing_fails(self, tmp_path, monkeypatch, caplog):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "model_repo" / "demo" / "1").mkdir(parents=True)
        args = type("Args", (), {
            "project_name": "demo",
            "wizard": False,
            "model_only": True,
        })()
        assert cli._cmd_init(args) == 1
        assert "already exists" in caplog.text.lower()

    def test_init_default_project_name(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = type("Args", (), {
            "project_name": None,
            "wizard": False,
        })()
        assert cli._cmd_init(args) == 0
        assert (tmp_path / "my_project").exists()

    def test_init_wizard_mode(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        inputs = iter(["wiz_proj", "", ""])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))
        args = type("Args", (), {
            "project_name": None,
            "wizard": True,
        })()
        assert cli._cmd_init(args) == 0
        assert (tmp_path / "wiz_proj").exists()


# ===== benchmark =====

class TestBenchmark:
    """CLI is a thin shell over BenchmarkEngine: arg parsing, httpx target
    construction, output rendering, exit codes — no benchmark logic inline."""

    def _fake_httpx(self, status_code=200):
        mock_response = type("Response", (), {"status_code": status_code})()
        calls = []
        instances = []

        class FakeTimeout:
            def __init__(self, *args, **kwargs):
                pass

        class FakeLimits:
            def __init__(self, *args, **kwargs):
                pass

        class FakeAsyncClient:
            def __init__(self, *args, **kwargs):
                instances.append(self)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def post(self, url, **kwargs):
                calls.append((url, kwargs))
                return mock_response

        fake = type("FakeHttpx", (), {
            "AsyncClient": FakeAsyncClient,
            "Limits": FakeLimits,
            "Timeout": FakeTimeout,
            "TimeoutException": type("TimeoutException", (Exception,), {}),
            "ConnectError": type("ConnectError", (Exception,), {}),
            "TransportError": type("TransportError", (Exception,), {}),
        })()
        return fake, calls, instances

    def _args(self, **overrides):
        base = {
            "url": "http://127.0.0.1:8000",
            "model": "test_model",
            "version": None,
            "concurrency": "1",
            "duration": None,
            "requests": 5,
            "warmup_requests": 0,
            "grace_period": 1.0,
            "payload": None,
            "payload_file": None,
            "payload_random": None,
            "export": None,
            "max_error_rate": None,
            "max_p99": None,
            "rate": None,
        }
        base.update(overrides)
        return type("Args", (), base)()

    def test_delegates_with_single_client_and_warmup(self, monkeypatch, capsys):
        """One AsyncClient reused; warmup requests sent but not counted."""
        import sys

        fake, calls, instances = self._fake_httpx()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._args(requests=5, warmup_requests=2))

        assert rc == 0
        assert len(instances) == 1, f"Expected 1 AsyncClient, got {len(instances)}"
        assert len(calls) == 5 + 2, "5 measured + 2 warmup requests"
        for url, kwargs in calls:
            assert kwargs.get("json") == {"input": 1.0}
        out = capsys.readouterr().out
        assert "closed-loop" in out
        assert "p95" in out

    def test_duration_and_requests_mutually_exclusive(self):
        import pytest

        with pytest.raises(SystemExit) as exc_info:
            cli.main([
                "benchmark", "--model", "m",
                "--duration", "5", "--requests", "10",
            ])
        assert exc_info.value.code == 2

    def test_max_error_rate_violation_exits_99(self, monkeypatch):
        import sys

        fake, calls, _ = self._fake_httpx(status_code=500)
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._args(requests=3, max_error_rate=0.01))
        assert rc == 99

    def test_max_p99_violation_exits_99(self, monkeypatch):
        import sys

        fake, calls, _ = self._fake_httpx()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._args(requests=3, max_p99=-1.0))
        assert rc == 99

    def test_export_writes_json(self, monkeypatch, tmp_path):
        import sys
        import json

        fake, calls, _ = self._fake_httpx()
        monkeypatch.setitem(sys.modules, "httpx", fake)
        export_path = tmp_path / "result.json"

        rc = cli._cmd_benchmark(self._args(requests=5, export=str(export_path)))

        assert rc == 0
        data = json.loads(export_path.read_text())
        assert data["load_mode"] == "closed-loop"
        assert data["latency_basis"] == "service-time"
        assert data["percentile_method"] == "linear"
        assert data["successful"] == 5
        assert data["config"]["model"] == "test_model"
        assert data["config"]["requests"] == 5

    def test_payload_inline_json(self, monkeypatch):
        import sys

        fake, calls, _ = self._fake_httpx()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._args(requests=2, payload='{"x": 9}'))

        assert rc == 0
        assert all(kwargs["json"] == {"x": 9} for _, kwargs in calls)

    def test_payload_file_round_robin(self, monkeypatch, tmp_path):
        import sys

        p1 = tmp_path / "p1.json"
        p2 = tmp_path / "p2.json"
        p1.write_text('{"a": 1}')
        p2.write_text('{"b": 2}')
        fake, calls, _ = self._fake_httpx()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(
            self._args(requests=4, payload_file=[str(p1), str(p2)])
        )

        assert rc == 0
        bodies = [kwargs["json"] for _, kwargs in calls]
        assert bodies == [{"a": 1}, {"b": 2}, {"a": 1}, {"b": 2}]

    def test_payload_file_missing_returns_2(self, caplog):
        rc = cli._cmd_benchmark(self._args(payload_file=["/nonexistent/x.json"]))
        assert rc == 2

    def test_invalid_payload_json_returns_2(self, caplog):
        rc = cli._cmd_benchmark(self._args(payload="{not json"))
        assert rc == 2


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
            "ejection_error_threshold": 5,
            "ejection_timeout": 45.0,
            "ejection_max_percent": 40,
            "ejection_max_timeout": 30.0,
            "max_retries": 2,
            "startup_timeout": 90.0,
            "health_check_timeout": 8.0,
            "health_check_kill_threshold": 3,
            "worker_kill_timeout": 15.0,
            "hook_http_timeout": 7.0,
        })()
        assert cli._cmd_serve(args) == 0

        assert called_with["port"] == 9000
        assert called_with["host"] == "127.0.0.1"
        assert called_with["metrics_port"] == 9090
        assert called_with["health_check_interval"] == 20.0
        # Worker resilience (§3) must be forwarded.
        assert called_with["max_retries"] == 2
        assert called_with["startup_timeout"] == 90.0
        assert called_with["ejection_error_threshold"] == 5
        assert called_with["ejection_max_timeout"] == 30.0
        assert called_with["hook_http_timeout"] == 7.0
        assert "transport" not in called_with


class TestPayloadRandom:
    """--payload-random: per-request template randomization."""

    def test_factory_randomizes_id_field(self):
        from lite_server.cli import _random_payload_factory
        fab = _random_payload_factory({"input": 1.0, "id": "fixed"})
        ids = {fab()["id"] for _ in range(10)}
        # With 10 UUIDs the chance of collision is astronomically low
        assert len(ids) == 10

    def test_factory_randomizes_request_id_field(self):
        from lite_server.cli import _random_payload_factory
        fab = _random_payload_factory({"request_id": "fixed"})
        vals = {fab()["request_id"] for _ in range(10)}
        assert len(vals) == 10

    def test_factory_injects_nonce_when_no_id_field(self):
        from lite_server.cli import _random_payload_factory
        fab = _random_payload_factory({"input": 1.0})
        samples = [fab() for _ in range(10)]
        assert all("_r" in s for s in samples)
        nonces = {s["_r"] for s in samples}
        assert len(nonces) == 10

    def test_factory_preserves_other_fields(self):
        from lite_server.cli import _random_payload_factory
        fab = _random_payload_factory({"input": 42, "stream": True})
        sample = fab()
        assert sample["input"] == 42
        assert sample["stream"] is True
