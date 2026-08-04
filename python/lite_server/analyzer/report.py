"""Report renderers for analyze results.

JSON (produced by AnalysisReport.to_dict) is the single authoritative
representation; Markdown/console are one-way renderings of that same
schema v1 dict — there is no second writer, so the formats cannot drift.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ReportGenerator:
    """Render analyze schema v1 reports to JSON / Markdown / console."""

    @staticmethod
    def to_json(report: dict[str, Any], indent: int = 2) -> str:
        """Serialize report to JSON string."""
        return json.dumps(report, indent=indent, default=str, ensure_ascii=False)

    @staticmethod
    def to_markdown(report: dict[str, Any]) -> str:
        """Render a schema v1 report dict as Markdown (tolerates missing keys)."""
        lines: list[str] = []
        target = report.get("target") or {}
        model = target.get("model_name", "unknown")
        lines.append(f"# Analysis Report: {model}\n")

        lines.append(f"- **Tool:** {report.get('tool_version', '?')}")
        lines.append(f"- **Schema:** v{report.get('schema_version', '?')}")
        lines.append(f"- **Generated:** {report.get('generated_at', '?')}")
        lines.append(f"- **Command:** `{report.get('command', '?')}`")
        lines.append(
            f"- **Version:** {target.get('resolved_version', '?')}"
            f" (requested: {target.get('requested_version') or 'latest'})"
        )
        lines.append(
            f"- **Executed user code:** {target.get('executed_user_code', False)}\n"
        )

        summary = report.get("summary") or {}
        lines.append(
            f"**Summary:** {summary.get('errors', 0)} errors / "
            f"{summary.get('warnings', 0)} warnings / "
            f"{summary.get('infos', 0)} infos / "
            f"{summary.get('checks_passed', 0)} checks passed\n"
        )

        versions = report.get("versions") or {}
        if versions.get("found"):
            lines.append(
                f"**Versions found:** {', '.join(str(v) for v in versions['found'])}"
                f" → resolved {versions.get('resolved', '?')}"
                f"{' (implicit latest)' if versions.get('implicit_latest') else ''}\n"
            )

        api_class = report.get("api_class")
        if api_class:
            loc = api_class.get("location") or {}
            lines.append("## API Class\n")
            lines.append(f"- **Name:** {api_class.get('name', '?')}")
            lines.append(f"- **Bases:** {', '.join(api_class.get('bases') or [])}")
            lines.append(f"- **Confidence:** {api_class.get('confidence', '?')}")
            lines.append(f"- **Location:** {loc.get('file', '?')}:{loc.get('line', '?')}\n")

        methods = report.get("methods") or {}
        if methods:
            lines.append("## Methods\n")
            for group, entries in methods.items():
                lines.append(f"### {group}\n")
                for name, status in entries.items():
                    if name == "required_by" and status:
                        lines.append(f"- required by: `{status}`")
                    elif name != "required_by":
                        lines.append(f"- `{name}`: {status}")
                lines.append("")

        findings = report.get("findings") or []
        if findings:
            lines.append("## Findings\n")
            for severity in ("error", "warning", "info"):
                group = [f for f in findings if f.get("severity") == severity]
                if not group:
                    continue
                lines.append(f"### {severity.upper()} ({len(group)})\n")
                for f in group:
                    loc = f.get("location") or {}
                    where = (
                        f" ({loc['file']}:{loc['line']})" if loc.get("file") else ""
                    )
                    lines.append(f"- **{f.get('rule_id', '?')}**{where}: "
                                 f"{f.get('message', '')}")
                    if f.get("hint"):
                        lines.append(f"  - hint: {f['hint']}")
                lines.append("")

        checks = report.get("checks_passed") or []
        if checks:
            lines.append("## Checks Passed\n")
            for c in checks:
                lines.append(f"- {c}")

        return "\n".join(lines)

    @staticmethod
    def to_console(report: dict[str, Any]) -> str:
        """Concise console summary of a schema v1 report (tolerates missing keys)."""
        lines: list[str] = []
        target = report.get("target") or {}
        model = target.get("model_name", "unknown")
        lines.append(f"Analysis: {model} (version {target.get('resolved_version', '?')})")
        lines.append("-" * 40)

        api_class = report.get("api_class")
        if api_class:
            lines.append(f"  API class: {api_class.get('name', '?')} "
                         f"({api_class.get('confidence', '?')})")

        summary = report.get("summary") or {}
        lines.append(f"  errors={summary.get('errors', 0)} "
                     f"warnings={summary.get('warnings', 0)} "
                     f"infos={summary.get('infos', 0)}")

        for f in report.get("findings") or []:
            lines.append(f"  [{f.get('severity', '?')}] {f.get('rule_id', '?')}: "
                         f"{f.get('message', '')}")

        return "\n".join(lines)

    @staticmethod
    def save(report: dict[str, Any], output_dir: Path | str) -> Path:
        """Save JSON and Markdown reports to output_dir; returns the JSON path.

        Model names with path separators are sanitized so the files always
        land directly inside output_dir (audit B8).
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        target = report.get("target") or {}
        model = str(target.get("model_name", "unknown"))
        safe_model = model.replace("/", "-").replace("\\", "-")
        base = output_dir / f"{safe_model}_analysis"

        json_path = base.with_suffix(".json")
        json_path.write_text(ReportGenerator.to_json(report), encoding="utf-8")

        md_path = base.with_suffix(".md")
        md_path.write_text(ReportGenerator.to_markdown(report), encoding="utf-8")

        return json_path
