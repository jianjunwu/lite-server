"""Tests for lite_server.analyzer.static — static model analysis.

New contract (analyze schema v1): pure-AST analysis (never executes user
code), path whitelist, rule_id findings with error/warning/info severity,
and exit-code semantics (0 = no error findings, 1 = error findings at the
configured fail severity, 2 = analysis itself failed → exception raised).
"""

import json
from pathlib import Path

import pytest

from lite_server.analyzer.static import AnalysisReport, Finding, StaticAnalyzer

MODEL_PY = (
    "from lite_server import LitAPI\n\n"
    "class MyModel(LitAPI):\n"
    "    def setup(self, device): pass\n"
    "    def decode_request(self, request): return request\n"
    "    def predict(self, x): return x\n"
    "    def encode_response(self, output): return output\n"
)


def _make_model(repo: Path, name: str = "test_model", version: str = "1",
                model_py: str = MODEL_PY, config: str | None = None,
                requirements: str | None = None) -> Path:
    vdir = repo / name / version
    vdir.mkdir(parents=True, exist_ok=True)
    if model_py is not None:
        (vdir / "model.py").write_text(model_py)
    if config is not None:
        (vdir / "config.yaml").write_text(config)
    if requirements is not None:
        (repo / name / "requirements.txt").write_text(requirements)
    return vdir


def _rule_ids(report: AnalysisReport) -> list[str]:
    return [f.rule_id for f in report.findings]


class TestConstruction:
    def test_analyzer_requires_existing_repo(self, tmp_path):
        with pytest.raises(ValueError, match="does not exist"):
            StaticAnalyzer(tmp_path / "nonexistent")

    def test_analyzer_accepts_existing_repo(self, tmp_path):
        analyzer = StaticAnalyzer(tmp_path)
        assert analyzer.repo_path == tmp_path


class TestPathWhitelist:
    """P0 security: nothing outside repo_root is ever touched."""

    def test_path_traversal_rejected(self, tmp_path):
        analyzer = StaticAnalyzer(tmp_path)
        with pytest.raises(ValueError, match="Invalid model name"):
            analyzer.analyze_model("../outside")

    def test_slash_in_name_rejected(self, tmp_path):
        analyzer = StaticAnalyzer(tmp_path)
        with pytest.raises(ValueError, match="Invalid model name"):
            analyzer.analyze_model("a/b")

    def test_empty_name_rejected(self, tmp_path):
        analyzer = StaticAnalyzer(tmp_path)
        with pytest.raises(ValueError, match="Invalid model name"):
            analyzer.analyze_model("")

    def test_symlink_escape_rejected(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        outside = tmp_path / "outside"
        outside.mkdir()
        (repo / "linked").symlink_to(outside)
        analyzer = StaticAnalyzer(repo)
        with pytest.raises((ValueError, FileNotFoundError)):
            analyzer.analyze_model("linked")

    def test_model_not_found_raises(self, tmp_path):
        analyzer = StaticAnalyzer(tmp_path)
        with pytest.raises(FileNotFoundError, match="not found"):
            analyzer.analyze_model("missing")


class TestZeroExecution:
    """P0 security: analysis never executes user code."""

    def test_top_level_code_not_executed(self, tmp_path):
        marker = tmp_path / "EXECUTED_MARKER"
        payload = (
            "import pathlib\n"
            f"pathlib.Path({str(marker)!r}).write_text('pwned')\n"
            "from lite_server import LitAPI\n\n"
            "class MyModel(LitAPI):\n"
            "    def predict(self, x): return x\n"
        )
        _make_model(tmp_path, model_py=payload)
        analyzer = StaticAnalyzer(tmp_path)
        report = analyzer.analyze_model("test_model")
        assert not marker.exists(), "analyze executed top-level user code"
        assert report.executed_user_code is False


class TestClassDetection:
    def test_exact_confidence_direct_import(self, tmp_path):
        _make_model(tmp_path)
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert report.api_class["name"] == "MyModel"
        assert report.api_class["confidence"] == "exact"
        assert report.api_class["location"]["file"] == "model.py"
        assert report.api_class["location"]["line"] == 3

    def test_exact_confidence_aliased_import(self, tmp_path):
        src = (
            "from lite_server import LitAPI as Base\n\n"
            "class MyModel(Base):\n"
            "    def predict(self, x): return x\n"
        )
        _make_model(tmp_path, model_py=src)
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert report.api_class["name"] == "MyModel"
        assert report.api_class["confidence"] == "exact"

    def test_exact_confidence_module_attribute(self, tmp_path):
        src = (
            "import lite_server\n\n"
            "class MyModel(lite_server.LitAPI):\n"
            "    def predict(self, x): return x\n"
        )
        _make_model(tmp_path, model_py=src)
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert report.api_class["confidence"] == "exact"

    def test_transitive_confidence_cross_file(self, tmp_path):
        vdir = _make_model(tmp_path, model_py=None)
        (vdir / "base.py").write_text(
            "from lite_server import LitAPI\n\n"
            "class MyBase(LitAPI):\n"
            "    def predict(self, x): return x\n"
        )
        (vdir / "model.py").write_text(
            "from base import MyBase\n\n"
            "class MyModel(MyBase):\n"
            "    pass\n"
        )
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert report.api_class["name"] == "MyModel"
        assert report.api_class["confidence"] == "transitive"
        # predict inherited from repo-internal base counts as implemented
        assert report.methods["core_required"]["predict"] == "implemented"

    def test_zero_subclasses_is_ls002_error(self, tmp_path):
        _make_model(tmp_path, model_py="class NotAnAPI:\n    pass\n")
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        errors = [f for f in report.findings if f.rule_id == "LS002"]
        assert errors and errors[0].severity == "error"
        assert report.api_class is None
        assert report.exit_code("error") == 1

    def test_multiple_subclasses_is_ls002_error(self, tmp_path):
        src = (
            "from lite_server import LitAPI\n\n"
            "class A(LitAPI):\n"
            "    def predict(self, x): return x\n\n"
            "class B(LitAPI):\n"
            "    def predict(self, x): return x\n"
        )
        _make_model(tmp_path, model_py=src)
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert "LS002" in _rule_ids(report)
        assert report.exit_code("error") == 1

    def test_unresolved_base_visible_when_no_hit(self, tmp_path):
        src = (
            "class MyModel(make_api_class()):\n"
            "    def predict(self, x): return x\n"
        )
        _make_model(tmp_path, model_py=src)
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        # LS002 for zero resolved hits, plus an info finding that a candidate
        # with an unresolvable base exists (no silent false negatives)
        assert "LS002" in _rule_ids(report)
        infos = [f for f in report.findings if f.severity == "info"]
        assert any("unresolved" in f.message.lower() for f in infos)

    def test_syntax_error_is_ls005_error_not_crash(self, tmp_path):
        _make_model(tmp_path, model_py="this is not valid python!!!")
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        ls005 = [f for f in report.findings if f.rule_id == "LS005"]
        assert ls005 and ls005[0].severity == "error"
        assert report.exit_code("error") == 1


class TestMethodRules:
    def test_missing_predict_is_ls001_error(self, tmp_path):
        src = (
            "from lite_server import LitAPI\n\n"
            "class BadModel(LitAPI):\n"
            "    def setup(self, device): pass\n"
        )
        _make_model(tmp_path, model_py=src)
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        ls001 = [f for f in report.findings if f.rule_id == "LS001"]
        assert ls001 and ls001[0].severity == "error"
        assert report.methods["core_required"]["predict"] == "missing"

    def test_full_model_passes_core_checks(self, tmp_path):
        _make_model(tmp_path)
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert "LS001" not in _rule_ids(report)
        assert "predict-implemented" in report.checks_passed
        assert "exactly-one-litapi-subclass" in report.checks_passed
        assert report.exit_code("error") == 0

    def test_missing_setup_is_ls102_warning(self, tmp_path):
        src = (
            "from lite_server import LitAPI\n\n"
            "class MyModel(LitAPI):\n"
            "    def predict(self, x): return x\n"
        )
        _make_model(tmp_path, model_py=src)
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert "LS102" in _rule_ids(report)
        # warning does not fail the default (error) gate, but fails --strict
        assert report.exit_code("error") == 0
        assert report.exit_code("warning") == 1

    def test_ls101_batching_contract(self, tmp_path):
        src = (
            "from lite_server import LitAPI\n\n"
            "class MyModel(LitAPI):\n"
            "    def predict(self, x): return x\n"
        )
        _make_model(tmp_path, model_py=src, config="max_batch_size: 8\n")
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert "LS101" in _rule_ids(report)
        assert report.methods["batching"]["required_by"] == "max_batch_size=8"

    def test_ls101_not_triggered_at_batch_size_1(self, tmp_path):
        src = (
            "from lite_server import LitAPI\n\n"
            "class MyModel(LitAPI):\n"
            "    def predict(self, x): return x\n"
        )
        _make_model(tmp_path, model_py=src, config="max_batch_size: 1\n")
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert "LS101" not in _rule_ids(report)

    def test_ls103_streaming_contract(self, tmp_path):
        src = (
            "from lite_server import LitAPI\n\n"
            "class MyModel(LitAPI):\n"
            "    def predict(self, x): return x\n"
        )
        _make_model(tmp_path, model_py=src, config="stream: true\n")
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert "LS103" in _rule_ids(report)
        assert report.methods["streaming"]["required_by"] == "stream=true"

    def test_ls103_satisfied_by_generator_stream_predict(self, tmp_path):
        src = (
            "from lite_server import LitAPI\n\n"
            "class MyModel(LitAPI):\n"
            "    def predict(self, x): return x\n"
            "    def stream_predict(self, request):\n"
            "        yield {\"token\": 1}\n"
        )
        _make_model(tmp_path, model_py=src, config="stream: true\n")
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert "LS103" not in _rule_ids(report)
        assert report.methods["streaming"]["stream_predict"] == "implemented"

    def test_ls201_ops_hooks_info(self, tmp_path):
        _make_model(tmp_path)
        # explicit version keeps LS111 out of the way
        report = StaticAnalyzer(tmp_path).analyze_model("test_model", version="1")
        ls201 = [f for f in report.findings if f.rule_id == "LS201"]
        assert ls201 and ls201[0].severity == "info"
        # info never fails either gate
        assert report.exit_code("warning") == 0


class TestVersions:
    def test_latest_resolves_numerically(self, tmp_path):
        for ver in ("1", "2", "10"):
            _make_model(tmp_path, version=ver)
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert report.versions_found == ["1", "2", "10"]
        assert report.resolved_version == "10"
        assert report.implicit_latest is True
        assert "LS111" in _rule_ids(report)

    def test_explicit_version_no_ls111(self, tmp_path):
        for ver in ("1", "2"):
            _make_model(tmp_path, version=ver)
        report = StaticAnalyzer(tmp_path).analyze_model("test_model", version="1")
        assert report.resolved_version == "1"
        assert report.implicit_latest is False
        assert "LS111" not in _rule_ids(report)

    def test_explicit_missing_version_raises(self, tmp_path):
        _make_model(tmp_path, version="1")
        analyzer = StaticAnalyzer(tmp_path)
        with pytest.raises(FileNotFoundError, match="version"):
            analyzer.analyze_model("test_model", version="9")

    def test_no_version_dirs_raises(self, tmp_path):
        (tmp_path / "test_model").mkdir()
        analyzer = StaticAnalyzer(tmp_path)
        with pytest.raises(FileNotFoundError, match="version"):
            analyzer.analyze_model("test_model")


class TestConfigAndDeps:
    def test_config_parsed(self, tmp_path):
        _make_model(tmp_path, config="max_batch_size: 4\nstream: false\n")
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert report.files["has_config"] is True
        assert report.config["max_batch_size"] == 4

    def test_config_invalid_is_ls004_error(self, tmp_path):
        # Rust serde path rejects wrong types (aggregate, not first-error)
        _make_model(tmp_path, config='max_batch_size: "not_an_int"\n')
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert "LS004" in _rule_ids(report)
        assert report.exit_code("error") == 1

    def test_config_non_mapping_is_ls004(self, tmp_path):
        _make_model(tmp_path, config="just_a_string_value")
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert "LS004" in _rule_ids(report)

    def test_requirements_parsed(self, tmp_path):
        _make_model(tmp_path, requirements="torch>=2.0\n# comment\ntransformers\n")
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        assert report.files["has_requirements"] is True
        assert "torch>=2.0" in report.dependencies
        assert "transformers" in report.dependencies

    def test_requirements_bad_line_is_ls104_warning(self, tmp_path):
        _make_model(tmp_path, requirements="torch\ngit+https://bad[bad\n")
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        ls104 = [f for f in report.findings if f.rule_id == "LS104"]
        assert ls104 and ls104[0].severity == "warning"


class TestReportContract:
    def test_to_dict_schema_v1_envelope(self, tmp_path):
        _make_model(tmp_path)
        report = StaticAnalyzer(tmp_path).analyze_model("test_model")
        d = report.to_dict(tool_version="lite-server 0.0.0-test",
                           command="lite-server analyze --model test_model")
        assert d["schema_version"] == 1
        assert d["tool_version"] == "lite-server 0.0.0-test"
        assert d["command"].startswith("lite-server analyze")
        assert "generated_at" in d
        assert d["target"]["model_name"] == "test_model"
        assert d["target"]["executed_user_code"] is False
        # summary counts are consistent with findings
        counts = {"error": 0, "warning": 0, "info": 0}
        for f in d["findings"]:
            counts[f["severity"]] += 1
        assert d["summary"]["errors"] == counts["error"]
        assert d["summary"]["warnings"] == counts["warning"]
        assert d["summary"]["infos"] == counts["info"]
        assert d["summary"]["checks_passed"] == len(d["checks_passed"])
        # method groups present
        for group in ("core_required", "codec", "batching", "streaming", "ops_hooks"):
            assert group in d["methods"]
        # JSON-serializable as-is
        json.dumps(d)

    def test_list_models_unchanged(self, tmp_path):
        _make_model(tmp_path, name="alpha")
        _make_model(tmp_path, name="beta")
        assert StaticAnalyzer(tmp_path).list_models() == ["alpha", "beta"]
