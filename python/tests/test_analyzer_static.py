"""Tests for lite_server.analyzer.static — static model analysis."""

import json
from pathlib import Path

import pytest

from lite_server.analyzer.static import StaticAnalyzer


class TestStaticAnalyzerBasics:
    """Construction and basic validation."""

    def test_analyzer_requires_existing_repo(self, tmp_path):
        with pytest.raises(ValueError, match="does not exist"):
            StaticAnalyzer(tmp_path / "nonexistent")

    def test_analyzer_accepts_existing_repo(self, tmp_path):
        (tmp_path / "model_repo").mkdir()
        analyzer = StaticAnalyzer(tmp_path)
        assert analyzer.repo_path == tmp_path


class TestAnalyzeModel:
    """Model analysis for a single model."""

    @pytest.fixture
    def repo(self, tmp_path):
        repo = tmp_path / "model_repo"
        repo.mkdir()
        return repo

    def test_model_not_found(self, repo):
        analyzer = StaticAnalyzer(repo)
        result = analyzer.analyze_model("missing")
        assert result["model_name"] == "missing"
        assert result["found"] is False
        assert "not found" in result["warnings"][0]

    def test_model_without_version_dir(self, repo):
        (repo / "test_model").mkdir()
        analyzer = StaticAnalyzer(repo)
        result = analyzer.analyze_model("test_model")
        assert result["found"] is True
        assert result["versions"] == []
        assert any("no version" in w.lower() for w in result["warnings"])

    def test_model_with_model_py(self, repo):
        vdir = repo / "test_model" / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text(
            "from lite_server import LitAPI\n\n"
            "class MyModel(LitAPI):\n"
            "    def setup(self, device): pass\n"
            "    def decode_request(self, request): return request\n"
            "    def predict(self, x): return x\n"
            "    def encode_response(self, output): return output\n"
        )
        analyzer = StaticAnalyzer(repo)
        result = analyzer.analyze_model("test_model")
        assert result["found"] is True
        assert result["versions"] == ["1"]
        assert result["has_model_py"] is True
        assert "predict" in result["methods"]
        assert "setup" in result["methods"]

    def test_model_missing_predict_warns(self, repo):
        vdir = repo / "test_model" / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text(
            "from lite_server import LitAPI\n\n"
            "class BadModel(LitAPI):\n"
            "    def setup(self, device): pass\n"
        )
        analyzer = StaticAnalyzer(repo)
        result = analyzer.analyze_model("test_model")
        assert any("predict" in w.lower() for w in result["warnings"])

    def test_model_with_config_yaml(self, repo):
        vdir = repo / "test_model" / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text(
            "from lite_server import LitAPI\n\n"
            "class MyModel(LitAPI):\n"
            "    def setup(self, device): pass\n"
            "    def predict(self, x): return x\n"
        )
        (vdir / "config.yaml").write_text("max_batch_size: 4\nstream: true\n")
        analyzer = StaticAnalyzer(repo)
        result = analyzer.analyze_model("test_model")
        assert result["has_config"] is True
        assert result["config"]["max_batch_size"] == 4
        assert result["config"]["stream"] is True

    def test_model_with_requirements_txt(self, repo):
        mdir = repo / "test_model"
        mdir.mkdir(parents=True)
        vdir = mdir / "1"
        vdir.mkdir()
        (vdir / "model.py").write_text(
            "from lite_server import LitAPI\n\n"
            "class MyModel(LitAPI):\n"
            "    def setup(self, device): pass\n"
            "    def predict(self, x): return x\n"
        )
        (mdir / "requirements.txt").write_text("torch\ntransformers\n")
        analyzer = StaticAnalyzer(repo)
        result = analyzer.analyze_model("test_model")
        assert result["has_requirements"] is True
        assert "torch" in result["dependencies"]
        assert "transformers" in result["dependencies"]

    def test_multiple_versions(self, repo):
        for ver in ("1", "2"):
            vdir = repo / "test_model" / ver
            vdir.mkdir(parents=True)
            (vdir / "model.py").write_text(
                "from lite_server import LitAPI\n\n"
                "class MyModel(LitAPI):\n"
                "    def setup(self, device): pass\n"
                "    def predict(self, x): return x\n"
            )
        analyzer = StaticAnalyzer(repo)
        result = analyzer.analyze_model("test_model")
        assert sorted(result["versions"]) == ["1", "2"]

    def test_model_py_syntax_error_warns(self, repo):
        vdir = repo / "test_model" / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("this is not valid python!!!")
        analyzer = StaticAnalyzer(repo.parent)
        result = analyzer.analyze_model("test_model")
        assert any("syntax" in w.lower() or "load" in w.lower() for w in result["warnings"])

    def test_benchmark_model_loads_with_lite_server_import(self, repo):
        """B1 (fixed): benchmark models must import from lite_server, not litserve.

        Since 0.7.0, litserve is no longer a project dependency.  The benchmark
        model files under benchmarks/models/ use ``from lite_server import LitAPI``
        and must load cleanly through the StaticAnalyzer without import warnings.
        """
        vdir = repo / "sleep_1ms_model" / "1"
        vdir.mkdir(parents=True)
        (vdir / "config.yaml").write_text("max_batch_size: 1\n")
        (vdir / "model.py").write_text(
            '"""Sleep model (1ms): CPU-bound mock for benchmarking IPC overhead.\n'
            '\n'
            'Simulates 1ms compute latency using time.sleep().\n'
            'Both lite-server and LitServe load this model via the LitAPI interface.\n'
            '"""\n'
            '\n'
            'import time\n'
            '\n'
            'from lite_server import LitAPI\n'
            '\n'
            '\n'
            'class Sleep1msAPI(LitAPI):\n'
            '    """A model that sleeps for 1ms per request."""\n'
            '\n'
            '    SLEEP_TIME = 0.001  # 1ms per request\n'
            '\n'
            '    def setup(self, device):\n'
            '        self.device = device\n'
            '\n'
            '    def decode_request(self, request):\n'
            '        return request.get("input", "")\n'
            '\n'
            '    def predict(self, inputs):\n'
            '        time.sleep(self.SLEEP_TIME)\n'
            '        if isinstance(inputs, list):\n'
            '            return [{"output": i, "sleep_ms": self.SLEEP_TIME * 1000} for i in inputs]\n'
            '        return {"output": inputs, "sleep_ms": self.SLEEP_TIME * 1000}\n'
            '\n'
            '    def encode_response(self, output):\n'
            '        return output\n'
        )
        analyzer = StaticAnalyzer(repo)
        result = analyzer.analyze_model("sleep_1ms_model")
        # No import warnings — the model loads cleanly with lite_server.LitAPI
        import_warnings = [w for w in result["warnings"] if "import check" in w]
        assert not import_warnings, (
            f"Unexpected import warning: {import_warnings}"
        )
