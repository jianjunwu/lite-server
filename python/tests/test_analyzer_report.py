"""Tests for lite_server.analyzer.report — schema v1 renderers.

JSON is the single authoritative representation; Markdown/console are
one-way renderings of the same data model (no drift possible).
"""

import json
from pathlib import Path

import pytest

from lite_server.analyzer.report import ReportGenerator


def _schema_v1_report(**overrides) -> dict:
    report = {
        "schema_version": 1,
        "tool_version": "lite-server 0.0.0-test",
        "generated_at": "2026-08-04T00:00:00+00:00",
        "command": "lite-server analyze --model my_model",
        "target": {
            "model_name": "my_model",
            "requested_version": None,
            "resolved_version": "2",
            "executed_user_code": False,
        },
        "summary": {"errors": 0, "warnings": 1, "infos": 1, "checks_passed": 3},
        "versions": {"found": ["1", "2"], "resolved": "2", "implicit_latest": True},
        "api_class": {
            "name": "MyModel",
            "bases": ["LitAPI"],
            "confidence": "exact",
            "location": {"file": "model.py", "line": 3},
        },
        "methods": {
            "core_required": {"setup": "implemented", "predict": "implemented"},
            "codec": {"decode_request": "implemented", "encode_response": "default"},
            "batching": {"batch": "default", "unbatch": "default",
                         "required_by": "max_batch_size=8"},
            "streaming": {"stream_predict": "default", "predict_decoupled": "default",
                          "required_by": None},
            "ops_hooks": {"teardown": "default", "on_file_changed": "default"},
        },
        "files": {"has_model_py": True, "has_config": True, "has_requirements": False},
        "config": {"max_batch_size": 8},
        "dependencies": [],
        "findings": [
            {
                "rule_id": "LS111",
                "severity": "warning",
                "location": {"file": None, "line": None},
                "message": "未指定 --version，已按 latest(1) 解析到版本 2",
                "hint": "生产环境建议显式锁定",
            },
            {
                "rule_id": "LS201",
                "severity": "info",
                "location": {"file": "model.py", "line": 3},
                "message": "生命周期钩子未覆写",
                "hint": None,
            },
        ],
        "checks_passed": ["exactly-one-litapi-subclass", "predict-implemented",
                          "config-yaml-valid"],
    }
    report.update(overrides)
    return report


class TestToJson:
    def test_roundtrip(self):
        parsed = json.loads(ReportGenerator.to_json(_schema_v1_report()))
        assert parsed["schema_version"] == 1
        assert parsed["target"]["model_name"] == "my_model"

    def test_tolerates_non_string_values(self):
        json.loads(ReportGenerator.to_json(_schema_v1_report(config={"x": object()})))


class TestToMarkdown:
    def test_renders_all_sections(self):
        md = ReportGenerator.to_markdown(_schema_v1_report())
        assert "my_model" in md
        assert "schema" in md.lower() or "v1" in md
        assert "MyModel" in md
        assert "exact" in md
        assert "LS111" in md
        assert "LS201" in md
        assert "max_batch_size=8" in md
        assert "predict-implemented" in md

    def test_findings_grouped_by_severity(self):
        md = ReportGenerator.to_markdown(_schema_v1_report())
        assert "warning" in md.lower()
        assert "info" in md.lower()

    def test_tolerates_missing_sections(self):
        # robustness (audit B4): partial dicts must not crash the renderer
        md = ReportGenerator.to_markdown({"target": {"model_name": "x"}})
        assert "x" in md
        assert ReportGenerator.to_markdown({})


class TestToConsole:
    def test_renders_summary(self):
        text = ReportGenerator.to_console(_schema_v1_report())
        assert "my_model" in text
        assert "MyModel" in text
        assert "LS111" in text

    def test_tolerates_missing_sections(self):
        assert ReportGenerator.to_console({})


class TestSave:
    def test_writes_json_and_markdown(self, tmp_path):
        json_path = ReportGenerator.save(_schema_v1_report(), tmp_path)
        assert json_path.exists()
        md_path = json_path.with_suffix(".md")
        assert md_path.exists()
        data = json.loads(json_path.read_text())
        assert data["schema_version"] == 1
        assert "LS111" in md_path.read_text()

    def test_model_name_with_slash_sanitized(self, tmp_path):
        # audit B8: model names containing path separators must not create
        # subdirectories or escape the output dir
        report = _schema_v1_report()
        report["target"]["model_name"] = "org/evil"
        saved = ReportGenerator.save(report, tmp_path)
        assert saved.resolve().parent == tmp_path.resolve()
        for f in tmp_path.iterdir():
            assert f.is_file()
