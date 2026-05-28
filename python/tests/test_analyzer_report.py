"""Tests for lite_server.analyzer.report — report generation."""

import json
from pathlib import Path

import pytest

from lite_server.analyzer.benchmark import BenchmarkResult
from lite_server.analyzer.report import ReportGenerator


class TestReportToJson:
    """JSON report output."""

    def test_basic_json_structure(self):
        report = {
            "model": "test_model",
            "analyzed_at": "2024-01-01T00:00:00+00:00",
            "static": {"found": True, "versions": ["1"]},
            "recommendations": [],
        }
        json_str = ReportGenerator.to_json(report)
        parsed = json.loads(json_str)
        assert parsed["model"] == "test_model"
        assert parsed["static"]["found"] is True

    def test_json_roundtrip(self):
        report = {
            "model": "m",
            "benchmark": BenchmarkResult(
                total_requests=10, successful=10, latencies=[1, 2, 3]
            ).to_dict(),
        }
        json_str = ReportGenerator.to_json(report)
        parsed = json.loads(json_str)
        assert parsed["benchmark"]["total_requests"] == 10


class TestReportToMarkdown:
    """Markdown report output."""

    def test_contains_model_name(self):
        report = {
            "model": "my_model",
            "analyzed_at": "2024-01-01T00:00:00+00:00",
            "static": {"found": True},
            "recommendations": [{"field": "max_batch_size", "value": 4, "reason": "test"}],
        }
        md = ReportGenerator.to_markdown(report)
        assert "my_model" in md
        assert "max_batch_size" in md

    def test_shows_not_found(self):
        report = {
            "model": "missing",
            "static": {"found": False},
            "recommendations": [],
        }
        md = ReportGenerator.to_markdown(report)
        assert "not found" in md.lower() or "missing" in md.lower()


class TestReportToConsole:
    """Console summary output."""

    def test_non_empty_string(self):
        report = {
            "model": "m",
            "static": {"found": True},
            "recommendations": [],
        }
        text = ReportGenerator.to_console(report)
        assert isinstance(text, str)
        assert len(text) > 0

    def test_shows_warnings(self):
        report = {
            "model": "m",
            "static": {"found": True, "warnings": ["No predict method"]},
            "recommendations": [],
        }
        text = ReportGenerator.to_console(report)
        assert "predict" in text
