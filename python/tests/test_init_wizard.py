"""Tests for lite_server.init.wizard — interactive project initialization."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from lite_server.init.wizard import run_wizard


class TestRunWizard:
    """Interactive wizard with mocked input."""

    def test_generates_project_with_defaults(self, tmp_path, monkeypatch):
        # project_name, model_name(default=my_model), confirm(y)
        inputs = iter(["myproj", "", ""])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

        run_wizard(output_dir=str(tmp_path))

        root = tmp_path / "myproj"
        assert root.exists()
        assert (root / "server.yaml").exists()

    def test_uses_empty_template_by_default(self, tmp_path, monkeypatch):
        inputs = iter(["myproj", "", ""])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

        run_wizard(output_dir=str(tmp_path))

        root = tmp_path / "myproj"
        model_py = root / "model_repo" / "my_model" / "1" / "model.py"
        assert model_py.exists()
        text = model_py.read_text()
        assert "class MyAPI" in text or "class MyModel" in text

    def test_custom_model_name(self, tmp_path, monkeypatch):
        inputs = iter(["myproj", "classifier", ""])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

        run_wizard(output_dir=str(tmp_path))

        root = tmp_path / "myproj"
        assert (root / "model_repo" / "classifier").exists()

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

    def test_confirm_no_aborts_without_generating(self, tmp_path, monkeypatch):
        # project_name, model_name(default), confirm(n)
        inputs = iter(["myproj", "", "n"])
        monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

        with pytest.raises(SystemExit):
            run_wizard(output_dir=str(tmp_path))
        assert not (tmp_path / "myproj").exists()
