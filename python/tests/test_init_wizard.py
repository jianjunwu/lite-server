"""Tests for lite_server.init.wizard — interactive project initialization."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from lite_server.init.wizard import run_wizard


class TestRunWizard:
    """Interactive wizard with mocked input."""

    def test_generates_project_with_defaults(self, tmp_path, monkeypatch):
        # project_name, template(default=empty), model_name(default=my_model),
        # grpc(y), metrics(y), batch(n), stream(n)
        inputs = iter(["myproj", "", "", "", "", "", ""])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

        run_wizard(output_dir=str(tmp_path))

        root = tmp_path / "myproj"
        assert root.exists()
        assert (root / "server.yaml").exists()

    def test_uses_empty_template_by_default(self, tmp_path, monkeypatch):
        inputs = iter(["myproj", "", "", "", "", "", ""])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

        run_wizard(output_dir=str(tmp_path))

        root = tmp_path / "myproj"
        model_py = root / "model_repo" / "my_model" / "1" / "model.py"
        assert model_py.exists()
        text = model_py.read_text()
        assert "class MyAPI" in text or "class MyModel" in text

    def test_custom_model_name(self, tmp_path, monkeypatch):
        inputs = iter(["myproj", "", "classifier", "", "", "", ""])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

        run_wizard(output_dir=str(tmp_path))

        root = tmp_path / "myproj"
        assert (root / "model_repo" / "classifier").exists()

    def test_enables_features_when_yes(self, tmp_path, monkeypatch):
        inputs = iter(["myproj", "", "", "y", "y", "y", "y"])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

        run_wizard(output_dir=str(tmp_path))

        root = tmp_path / "myproj"
        cfg = (root / "server.yaml").read_text()
        assert "enabled: true" in cfg

    def test_skips_features_when_no(self, tmp_path, monkeypatch):
        inputs = iter(["myproj", "", "", "n", "n", "n", "n"])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

        run_wizard(output_dir=str(tmp_path))

        root = tmp_path / "myproj"
        cfg = (root / "server.yaml").read_text()
        # At least some features should be disabled
        assert "enabled: false" in cfg or "enabled: true" not in cfg

    def test_empty_project_name_aborts(self, tmp_path, monkeypatch):
        inputs = iter([""])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

        with pytest.raises(SystemExit):
            run_wizard(output_dir=str(tmp_path))

    def test_keyboard_interrupt_aborts(self, tmp_path, monkeypatch):
        def raise_interrupt(_=""):
            raise KeyboardInterrupt

        monkeypatch.setattr("builtins.input", raise_interrupt)

        with pytest.raises(SystemExit):
            run_wizard(output_dir=str(tmp_path))


class TestWizardOptionsPropagation:
    """Wizard batch/stream options must affect model config.yaml."""

    def test_batch_yes_sets_max_batch_size(self, tmp_path, monkeypatch):
        # proj, template(empty), model_name, grpc(y), metrics(y), batch(y), stream(n)
        inputs = iter(["myproj", "", "", "", "", "y", ""])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

        run_wizard(output_dir=str(tmp_path))

        root = tmp_path / "myproj"
        cfg = (root / "model_repo" / "my_model" / "1" / "config.yaml").read_text()
        assert "max_batch_size:" in cfg
        # Should be an active (uncommented) config line
        for line in cfg.splitlines():
            if "max_batch_size:" in line and not line.strip().startswith("#"):
                return
        pytest.fail("max_batch_size should be uncommented when batch=yes")

    def test_stream_yes_sets_stream_true(self, tmp_path, monkeypatch):
        # proj, template(empty), model_name, grpc(y), metrics(y), batch(n), stream(y)
        inputs = iter(["myproj", "", "", "", "", "", "y"])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

        run_wizard(output_dir=str(tmp_path))

        root = tmp_path / "myproj"
        cfg = (root / "model_repo" / "my_model" / "1" / "config.yaml").read_text()
        assert "stream:" in cfg
        for line in cfg.splitlines():
            if "stream:" in line and not line.strip().startswith("#"):
                assert "true" in line
                return
        pytest.fail("stream should be uncommented and true when stream=yes")
