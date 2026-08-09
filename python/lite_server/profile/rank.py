"""Constraint filtering, objective ranking, and recommendations (plan §2.7).

Constraints follow the existing benchmark vocabulary (--max-p99 /
--max-error-rate are the same flags); new ones are only the scenario faces:
--min-throughput, --max-ttft-ms / --max-rtf / goodput SLO for streaming,
--max-session-ms / --max-chunk-roundtrip-ms for bidi, --max-rss-mb (local
servers only). Failed trials never participate in filtering or ranking but
stay in the checkpoint (auditable).

Objective legality matrix (fail-fast, exit 2): goodput requires an SLO
expression; sessions_per_sec is bidi-only; --max-rss-mb is local-only.
"""

from __future__ import annotations

import difflib
from dataclasses import dataclass, field

from lite_server.profile.checkpoint import TrialRecord


class RankError(ValueError):
    """Constraint/objective misuse (→ CLI exit 2)."""


@dataclass
class Constraints:
    max_p99: float | None = None
    min_throughput: float | None = None
    max_error_rate: float | None = None
    max_ttft_ms: float | None = None
    max_rtf: float | None = None
    slo_attainment: float | None = None  # requires --goodput
    max_session_ms: float | None = None
    max_chunk_roundtrip_ms: float | None = None
    max_rss_mb: float | None = None


@dataclass
class Ranking:
    objective: str
    top: list[TrialRecord] = field(default_factory=list)
    passed: list[TrialRecord] = field(default_factory=list)
    failed: list[TrialRecord] = field(default_factory=list)
    baseline: TrialRecord | None = None

    def recommendations(self) -> list[TrialRecord]:
        return self.top


def validate_objective(
    objective: str,
    constraints: Constraints,
    scenario: dict,
    local: bool,
    goodput_given: bool,
) -> None:
    """Objective legality matrix (§2.7); any violation → RankError."""
    if objective == "goodput" and not goodput_given:
        raise RankError(
            "--objective goodput requires --goodput 'EXPR' (SLO params — "
            "without them goodput is undefined)"
        )
    if objective == "sessions_per_sec" and not scenario.get("bidi"):
        raise RankError("--objective sessions_per_sec is only legal for bidi scenarios")
    if constraints.slo_attainment is not None and not goodput_given:
        raise RankError("--slo-attainment requires --goodput")
    if constraints.max_ttft_ms is not None and not scenario.get("stream"):
        raise RankError("--max-ttft-ms requires a streaming scenario")
    if constraints.max_rtf is not None and scenario.get("model_type") not in ("tts", "stt"):
        raise RankError("--max-rtf requires --model-type tts or stt")
    if (constraints.max_session_ms is not None or
            constraints.max_chunk_roundtrip_ms is not None) and not scenario.get("bidi"):
        raise RankError("--max-session-ms/--max-chunk-roundtrip-ms require --bidi")
    if constraints.max_rss_mb is not None and not local:
        raise RankError(
            "--max-rss-mb is only legal against a local server "
            "(--server-pid or port lookup)"
        )


def objective_value(trial: TrialRecord, objective: str) -> float | None:
    """Per-trial objective value; None when the metric is unavailable."""
    m = trial.metrics or {}
    if objective == "throughput":
        return m.get("throughput")
    if objective == "sessions_per_sec":
        bidi = m.get("bidi") or {}
        return bidi.get("sessions_per_sec")
    if objective == "goodput":
        stream = m.get("stream") or {}
        g = stream.get("goodput") or {}
        attainment = g.get("attainment")
        if attainment is None:
            return None
        # goodput = attainment × throughput — the SLO-qualified throughput
        stream_tp = m.get("throughput")
        return attainment * (stream_tp if stream_tp is not None else 0.0)
    return None


def passes_constraints(trial: TrialRecord, constraints: Constraints, scenario: dict) -> tuple[bool, list[str]]:
    """Constraint check per scenario face (§2.7). Returns (ok, violations)."""
    if trial.status != "ok" or trial.metrics is None:
        return False, ["trial not ok"]
    m = trial.metrics
    violations: list[str] = []

    total = m.get("total_requests") or 0
    if constraints.max_error_rate is not None:
        rate = (m.get("failed") or 0) / total if total else 1.0
        if rate > constraints.max_error_rate:
            violations.append(f"error rate {rate:.3f} > {constraints.max_error_rate}")

    latency = m.get("latency_ms") or {}
    if constraints.max_p99 is not None:
        p99 = latency.get("p99")
        if p99 is not None and p99 > constraints.max_p99:
            violations.append(f"p99 {p99:.2f}ms > {constraints.max_p99}ms")

    if constraints.min_throughput is not None:
        tp = m.get("throughput")
        if tp is not None and tp < constraints.min_throughput:
            violations.append(f"throughput {tp:.2f} < {constraints.min_throughput}")

    stream = m.get("stream") or {}
    if constraints.max_ttft_ms is not None:
        ttft = (stream.get("ttft_ms") or {}).get("p99")
        if ttft is not None and ttft > constraints.max_ttft_ms:
            violations.append(f"TTFT p99 {ttft:.2f}ms > {constraints.max_ttft_ms}ms")

    if constraints.max_rtf is not None:
        rtf = (stream.get("rtf") or {}).get("p99")
        if rtf is not None and rtf > constraints.max_rtf:
            violations.append(f"RTF p99 {rtf:.2f} > {constraints.max_rtf}")

    g = stream.get("goodput") or {}
    if constraints.slo_attainment is not None:
        attainment = g.get("attainment")
        if attainment is not None and attainment < constraints.slo_attainment:
            violations.append(
                f"SLO attainment {attainment:.3f} < {constraints.slo_attainment}"
            )

    bidi = m.get("bidi") or {}
    if constraints.max_session_ms is not None:
        dur = (bidi.get("session_duration_ms") or {}).get("p99")
        if dur is not None and dur > constraints.max_session_ms:
            violations.append(f"session p99 {dur:.2f}ms > {constraints.max_session_ms}ms")

    if constraints.max_chunk_roundtrip_ms is not None:
        rt = (bidi.get("chunk_roundtrip_ms") or {}).get("p99")
        if rt is not None and rt > constraints.max_chunk_roundtrip_ms:
            violations.append(
                f"chunk roundtrip p99 {rt:.2f}ms > {constraints.max_chunk_roundtrip_ms}ms"
            )

    if constraints.max_rss_mb is not None:
        resources = m.get("resources") or {}
        rss_max = resources.get("rss_max_mb")
        if rss_max is not None and rss_max > constraints.max_rss_mb:
            violations.append(f"rss {rss_max:.1f}MB > {constraints.max_rss_mb}MB")

    return not violations, violations


def rank_trials(
    trials: list[TrialRecord],
    objective: str,
    constraints: Constraints,
    scenario: dict,
    top_n: int = 3,
) -> Ranking:
    """Filter → rank by objective desc → top-N. Baseline (empty config point)
    is reported separately for improvement percentages."""
    failed = [t for t in trials if t.status != "ok"]
    ok_trials = [t for t in trials if t.status == "ok"]
    baseline = next((t for t in ok_trials if not t.config_point), None)

    passed: list[TrialRecord] = []
    for t in ok_trials:
        ok, _ = passes_constraints(t, constraints, scenario)
        if ok:
            passed.append(t)
    passed.sort(
        key=lambda t: (objective_value(t, objective) is not None,
                       objective_value(t, objective) or 0.0),
        reverse=True,
    )
    return Ranking(
        objective=objective,
        top=passed[:top_n],
        passed=passed,
        failed=failed,
        baseline=baseline,
    )


def improvement_percent(trial: TrialRecord, baseline: TrialRecord | None, objective: str) -> float | None:
    """Improvement of trial over baseline for the objective; None when either
    is unavailable. Negative = regression."""
    if baseline is None or baseline.metrics is None:
        return None
    base = objective_value(baseline, objective)
    val = objective_value(trial, objective)
    if base is None or val is None or base == 0:
        return None
    return round((val - base) / base * 100.0, 1)


def config_diff(original_text: str, recommended_text: str) -> str:
    """Unified diff between the original config.yaml and the recommended one."""
    return "".join(difflib.unified_diff(
        original_text.splitlines(keepends=True),
        recommended_text.splitlines(keepends=True),
        fromfile="config.yaml (original)",
        tofile="config.yaml (recommended)",
    ))
