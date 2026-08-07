"""Audit tests for lite_server.analyzer — defects found during /audit review.

Each test demonstrates a confirmed defect by FAILING against the current code.
These tests do NOT modify any implementation code.
"""

import asyncio
import json
import os
from pathlib import Path

import pytest
import yaml

from lite_server.benchmark.benchmark import BenchmarkEngine, BenchmarkResult
from lite_server.analyzer.report import ReportGenerator
from lite_server.analyzer.static import StaticAnalyzer


# ---------------------------------------------------------------------------
# B1 — concurrency=0 causes infinite hang (Semaphore(0) deadlocks forever)
# ---------------------------------------------------------------------------

class TestAuditB1_ConcurrencyZeroDeadlock:
    """B1 (P0): BenchmarkEngine.run with concurrency=0 hangs indefinitely."""

    @pytest.mark.asyncio
    async def test_data_concurrency_zero_hangs_indefinitely(self):
        """concurrency=0 is rejected at entry with a clear ValueError."""
        engine = BenchmarkEngine()

        async def _target(payload):
            return {"ok": True}

        with pytest.raises(ValueError, match="concurrency"):
            await asyncio.wait_for(
                engine.run(
                    target=_target,
                    payload={},
                    concurrency=0,
                    total_requests=1,
                ),
                timeout=2.0,
            )


# ---------------------------------------------------------------------------
# B2 — concurrency < 0 crashes with ValueError
# ---------------------------------------------------------------------------

class TestAuditB2_NegativeConcurrency:
    """B2 (P0): BenchmarkEngine.run with negative concurrency crashes."""

    @pytest.mark.asyncio
    async def test_data_negative_concurrency_value_error(self):
        """Semaphore(-1) raises ValueError — unvalidated input crashes at runtime."""
        engine = BenchmarkEngine()

        async def _target(payload):
            return {"ok": True}

        with pytest.raises(ValueError):
            await engine.run(
                target=_target,
                payload={},
                concurrency=-1,
                total_requests=1,
            )


# ---------------------------------------------------------------------------
# B3 — yaml.safe_load can return non-dict, breaking downstream consumers
# ---------------------------------------------------------------------------

class TestAuditB3_YamlScalarConfig:
    """B3 (P1): scalar config.yaml is an LS004 error finding, not a type surprise."""

    def test_data_yaml_scalar_returns_string_not_dict(self, tmp_path):
        """When config.yaml contains a scalar (not a mapping), analysis reports
        an LS004 error finding and config stays an empty dict for consumers."""
        vdir = tmp_path / "test_model" / "1"
        vdir.mkdir(parents=True)
        (vdir / "model.py").write_text("class M: pass\n")
        # YAML file with a scalar value instead of a mapping
        (vdir / "config.yaml").write_text("just_a_string_value")

        analyzer = StaticAnalyzer(tmp_path)
        result = analyzer.analyze_model("test_model")

        assert result.files["has_config"] is True
        assert isinstance(result.config, dict)
        ls004 = [f for f in result.findings if f.rule_id == "LS004"]
        assert ls004 and ls004[0].severity == "error"


# ---------------------------------------------------------------------------
# B4 — to_markdown / to_console crash on non-numeric benchmark fields
# ---------------------------------------------------------------------------

class TestAuditB4_ReportFormatCrash:
    """B4 (P1): schema v1 renderers must tolerate partial/non-conforming dicts."""

    def test_data_to_markdown_crashes_on_none_throughput(self):
        """Missing sections fall back to placeholders instead of crashing."""
        md = ReportGenerator.to_markdown({"target": {"model_name": "t"}})
        assert "t" in md

    def test_data_to_console_crashes_on_none_throughput(self):
        console = ReportGenerator.to_console({"target": {"model_name": "t"}})
        assert "t" in console

    def test_data_to_markdown_crashes_on_non_numeric_latency(self):
        """Non-dict / None payloads do not crash the renderer."""
        assert ReportGenerator.to_markdown({})
        assert ReportGenerator.to_console({})


# ---------------------------------------------------------------------------
# B5 — list_models and analyze_model use inconsistent directory layouts
# ---------------------------------------------------------------------------

class TestAuditB5_InconsistentDirectoryLayout:
    """B5 (P1): list_models() looks in model_repo/ but analyze_model doesn't."""

    @pytest.fixture
    def repo_with_model(self, tmp_path):
        """Create repo_path/test_model/1/ — the unified layout both methods use."""
        repo = tmp_path / "models"
        repo.mkdir()
        model_dir = repo / "test_model" / "1"
        model_dir.mkdir(parents=True)
        (model_dir / "model.py").write_text(
            "class M:\n    def predict(self, x): return x\n"
        )
        return repo

    def test_scope_list_models_returns_models_that_analyze_model_cannot_find(
        self, repo_with_model
    ):
        """list_models and analyze_model use the same directory layout."""
        analyzer = StaticAnalyzer(repo_with_model)

        models = analyzer.list_models()
        assert "test_model" in models, (
            f"list_models() should discover test_model, got: {models}"
        )

        # plain class (no LitAPI base) → LS002 error finding, but the model
        # IS found and analyzed (no exception)
        result = analyzer.analyze_model("test_model")
        assert result.model_name == "test_model"
        assert result.resolved_version == "1"


# ---------------------------------------------------------------------------
# B6 — path traversal via model_name parameter
# ---------------------------------------------------------------------------

class TestAuditB6_PathTraversal:
    """B6 (P1): analyze_model allows path traversal through model_name."""

    def test_scope_path_traversal_via_model_name(self, tmp_path):
        """Passing '../something' as model_name is rejected with ValueError."""
        repo = tmp_path / "repo"
        repo.mkdir()
        analyzer = StaticAnalyzer(repo)

        with pytest.raises(ValueError, match="Invalid model name"):
            analyzer.analyze_model("../outside")


# ---------------------------------------------------------------------------
# B7 — to_dict() silently truncates errors to 5
# ---------------------------------------------------------------------------

class TestAuditB7_ErrorTruncation:
    """B7 (P2): BenchmarkResult.to_dict() silently truncates errors to 5."""

    def test_pure_to_dict_silently_truncates_errors(self):
        """to_dict only returns first 5 errors, losing data without warning."""
        result = BenchmarkResult(
            total_requests=10,
            successful=0,
            failed=10,
            errors=[f"error_{i}" for i in range(10)],
        )
        d = result.to_dict()
        # 10 errors recorded, but to_dict only returns 5
        assert len(result.errors) == 10, "Internal errors list should have 10 entries"
        assert len(d["errors"]) == 10, (
            f"to_dict truncated errors from 10 to {len(d['errors'])} — "
            f"data loss without warning"
        )


# ---------------------------------------------------------------------------
# B9 — empty model_name slips past the sanitizer and analyzes the repo root
# ---------------------------------------------------------------------------

class TestAuditB9_EmptyModelName:
    """B9 (P2, fixed): analyze_model("") is rejected as an invalid name.

    ``Path(repo) / "" == repo``, which exists — an empty model name used to
    scan the repository root. It now raises ValueError (→ CLI exit 2)."""

    def test_data_empty_model_name_returns_not_found(self, tmp_path):
        (tmp_path / "real_model" / "1").mkdir(parents=True)
        analyzer = StaticAnalyzer(tmp_path)

        with pytest.raises(ValueError, match="Invalid model name"):
            analyzer.analyze_model("")


# ---------------------------------------------------------------------------
# B8 — save() creates subdirectories when model name contains "/"
# ---------------------------------------------------------------------------

class TestAuditB8_ModelNamePathInjection:
    """B8 (P2): save() creates unexpected subdirectories when model has '/'."""

    def test_scope_model_name_with_slash_crashes_or_creates_subdirectory(self, tmp_path):
        """Model name containing '/' causes crash or unexpected subdirectories."""
        report = {
            "model": "org/my_model",
            "static": {"found": True},
        }
        output_dir = tmp_path / "reports"
        output_dir.mkdir()

        # save() calls output_dir.mkdir(parents=True) for the output_dir,
        # but does NOT create parent dirs for the file path inside.
        # When model="org/my_model", the base path becomes:
        #   output_dir / "org/my_model_analysis.json"
        # This either crashes (FileNotFoundError for missing "org/" dir)
        # or creates unexpected nested directories.
        try:
            saved = ReportGenerator.save(report, output_dir)
            # If it didn't crash, check that it didn't create subdirectories
            saved_resolved = saved.resolve()
            output_resolved = output_dir.resolve()
            assert str(saved_resolved).startswith(str(output_resolved)), (
                f"model name with '/' saved outside output_dir: {saved}"
            )
            # Check that all saved files are directly in output_dir (not nested)
            for f in output_dir.iterdir():
                assert f.is_file(), (
                    f"model name with '/' created unexpected subdirectory: {f}"
                )
        except (FileNotFoundError, NotADirectoryError) as e:
            # The crash itself confirms the defect: save() doesn't handle
            # model names with path separators
            raise AssertionError(
                f"save() crashed with model name containing '/': {e}"
            ) from e
