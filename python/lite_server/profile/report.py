"""Top-N comparison report (plan §2.8/§2.9 batch 3).

Markdown is rendered one-way from the same structured ranking the console
uses (no second writer of authoritative data — the summary.json checkpoint
stays authoritative). Resource columns report n/a for remote servers.
"""

from __future__ import annotations

from lite_server.profile.rank import Ranking, improvement_percent


def render_top_n_markdown(ranking: Ranking) -> str:
    """Markdown table of the top-N with tp/p50/p95/p99/max + improvement +
    resource column (n/a when the server is remote or sampling was off)."""
    lines = [
        f"# Profile results — objective: `{ranking.objective}`",
        "",
    ]
    if ranking.baseline is not None:
        lines.append(
            f"Baseline (current config): "
            f"objective value {_obj_str(ranking.baseline, ranking.objective)}"
        )
        lines.append("")
    if not ranking.recommendations():
        lines.append("**No trial satisfies the constraints — nothing to recommend.**")
        return "\n".join(lines) + "\n"

    lines.append(f"## Top-{len(ranking.recommendations())} recommendations")
    lines.append("")
    lines.append("| config | tp | p50 | p95 | p99 | max | imp% | rssMB | cpu% |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for t in ranking.recommendations():
        m = t.metrics or {}
        lat = m.get("latency_ms") or {}
        resources = m.get("resources") or {}
        imp = improvement_percent(t, ranking.baseline, ranking.objective)
        imp_str = f"{imp:+.1f}" if imp is not None else "n/a"
        rss = resources.get("rss_max_mb")
        rss_str = f"{rss:.0f}" if rss is not None else "n/a"
        cpu = resources.get("cpu_mean")
        cpu_str = f"{cpu:.0f}" if cpu is not None else "n/a"
        lines.append(
            f"| `{t.config_point}` | {m.get('throughput', 0):.2f} "
            f"| {lat.get('p50', 0):.1f} | {lat.get('p95', 0):.1f} "
            f"| {lat.get('p99', 0):.1f} | {lat.get('max', 0):.1f} "
            f"| {imp_str} | {rss_str} | {cpu_str} |"
        )
    lines.append("")
    lines.append(
        "Failed trials are excluded from ranking but kept in the checkpoint "
        "(auditable)."
    )
    return "\n".join(lines) + "\n"


def _obj_str(trial, objective: str) -> str:
    from lite_server.profile.rank import objective_value

    value = objective_value(trial, objective)
    return f"{value:.2f}" if value is not None else "n/a"
