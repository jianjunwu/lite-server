"""Report generation for analyzer results."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _num(value: Any, default: float = 0.0) -> float:
    """Coerce value to float, falling back to default for None/non-numeric."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class ReportGenerator:
    """Generate human-readable reports from analysis results."""

    @staticmethod
    def to_json(report: dict[str, Any], indent: int = 2) -> str:
        """Serialize report to JSON string."""
        return json.dumps(report, indent=indent, default=str, ensure_ascii=False)

    @staticmethod
    def to_markdown(report: dict[str, Any]) -> str:
        """Generate a Markdown report."""
        lines: list[str] = []
        model = report.get("model", "unknown")
        lines.append(f"# Analysis Report: {model}\n")

        # Timestamp
        when = report.get("analyzed_at", datetime.now(timezone.utc).isoformat())
        lines.append(f"**Analyzed at:** {when}\n")

        # Static analysis
        static = report.get("static", {})
        lines.append("## Static Analysis\n")
        if not static.get("found"):
            lines.append("Model not found in repository.\n")
        else:
            lines.append(f"- **Versions:** {', '.join(static.get('versions', []))}\n")
            lines.append(f"- **Has model.py:** {'yes' if static.get('has_model_py') else 'no'}\n")
            lines.append(f"- **Has config.yaml:** {'yes' if static.get('has_config') else 'no'}\n")
            lines.append(f"- **Has requirements.txt:** {'yes' if static.get('has_requirements') else 'no'}\n")
            if static.get("methods"):
                lines.append(f"- **Methods:** {', '.join(static['methods'])}\n")
            if static.get("optional_methods"):
                lines.append(f"- **Optional methods:** {', '.join(static['optional_methods'])}\n")

        # Warnings
        warnings = static.get("warnings", [])
        if warnings:
            lines.append("\n### Warnings\n")
            for w in warnings:
                lines.append(f"- {w}\n")

        # Benchmark results
        benchmark = report.get("benchmark")
        if benchmark:
            lines.append("\n## Benchmark Results\n")
            if isinstance(benchmark, dict):
                lines.append(f"- **Total requests:** {benchmark.get('total_requests', 0)}\n")
                lines.append(f"- **Successful:** {benchmark.get('successful', 0)}\n")
                lines.append(f"- **Failed:** {benchmark.get('failed', 0)}\n")
                lines.append(f"- **Throughput:** {_num(benchmark.get('throughput')):.2f} req/s\n")
                lat = benchmark.get("latency_ms", {})
                if lat:
                    lines.append(f"- **Latency (ms):** mean={_num(lat.get('mean')):.2f}, "
                                 f"p50={_num(lat.get('p50')):.2f}, "
                                 f"p90={_num(lat.get('p90')):.2f}, "
                                 f"p99={_num(lat.get('p99')):.2f}\n")

        # Recommendations
        recommendations = report.get("recommendations", [])
        if recommendations:
            lines.append("\n## Recommendations\n")
            for rec in recommendations:
                lines.append(f"- **{rec.get('field', '?')}** = {rec.get('value', '?')} — {rec.get('reason', '')}\n")

        return "\n".join(lines)

    @staticmethod
    def to_console(report: dict[str, Any]) -> str:
        """Generate a concise console summary."""
        lines: list[str] = []
        model = report.get("model", "unknown")
        lines.append(f"Analysis: {model}")
        lines.append("-" * 40)

        static = report.get("static", {})
        if not static.get("found"):
            lines.append("  Model not found")
        else:
            lines.append(f"  Versions: {', '.join(static.get('versions', []))}")
            lines.append(f"  Methods: {', '.join(static.get('methods', []))}")

        warnings = static.get("warnings", [])
        if warnings:
            lines.append("  Warnings:")
            for w in warnings:
                lines.append(f"    - {w}")

        benchmark = report.get("benchmark")
        if benchmark and isinstance(benchmark, dict):
            lines.append(f"  Benchmark: {benchmark.get('total_requests', 0)} requests, "
                         f"{_num(benchmark.get('throughput')):.1f} req/s")

        recommendations = report.get("recommendations", [])
        if recommendations:
            lines.append("  Recommendations:")
            for rec in recommendations:
                lines.append(f"    - {rec.get('field')} = {rec.get('value')}")

        return "\n".join(lines)

    @staticmethod
    def save(report: dict[str, Any], output_dir: Path | str) -> Path:
        """Save JSON and Markdown reports to output_dir."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        model = report.get("model", "unknown")
        safe_model = str(model).replace("/", "-").replace("\\", "-")
        base = output_dir / f"{safe_model}_analysis"

        json_path = base.with_suffix(".json")
        json_path.write_text(ReportGenerator.to_json(report), encoding="utf-8")

        md_path = base.with_suffix(".md")
        md_path.write_text(ReportGenerator.to_markdown(report), encoding="utf-8")

        return json_path
