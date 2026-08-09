"""Quick search — single-key hill climb (plan §2.5 batch 3).

Starting from the baseline config, step one knob at a time to the candidate
that most improves the objective; repeat until no single-key step improves
(收益下降即停). Bounded by --max-trials so a runaway landscape can't blow up
wall clock. Target: ≥90% of the full-grid optimum using <40% of the points
(validated in tests against a synthetic landscape).

The runner is duck-typed (the ProfileEngine): `measure_point(point)` applies
the config, sweeps concurrency, and leaves it applied; the caller restores.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from lite_server.profile.checkpoint import TrialRecord
from lite_server.profile.grid import ConfigPoint, GridSpec
from lite_server.profile.rank import objective_value


@dataclass
class QuickResult:
    points_measured: list[ConfigPoint] = field(default_factory=list)
    trials: list[TrialRecord] = field(default_factory=list)
    best: ConfigPoint | None = None
    best_value: float | None = None
    total_points: int = 0

    @property
    def coverage_ratio(self) -> float:
        return len(self.points_measured) / self.total_points if self.total_points else 0.0


def _point_value(trials: list[TrialRecord], objective: str) -> float | None:
    """Value of a config point = best objective across its concurrency sweep
    (a point is measured at every concurrency level)."""
    values = [objective_value(t, objective) for t in trials if t.status == "ok"]
    values = [v for v in values if v is not None]
    return max(values) if values else None


async def quick_search(
    runner,
    grid: GridSpec,
    objective: str,
    max_trials: int = 40,
) -> QuickResult:
    """Hill climb over config points. `runner.measure_point(point)` must
    return the point's trials (config applied; runner restores at the end).

    Candidate space: for each knob key, all values in grid.knobs[key] (with
    every other key pinned at the current best). A step must improve the
    objective to move; ties do not move (noise resistance).
    """
    result = QuickResult(total_points=len(grid.config_points()))
    # Cache measured points: restarts re-explore the neighborhood, and the
    # plan's "点数" target counts UNIQUE points (coverage ratio). Reusing the
    # cached value keeps the climb at <40% of the grid.
    measured_values: dict[str, float | None] = {}

    async def measure(point: ConfigPoint) -> float | None:
        import json as _json

        key = _json.dumps(point.values, sort_keys=True)
        if key in measured_values:
            return measured_values[key]
        trials = await runner.measure_point(point)
        result.trials.extend(trials)
        result.points_measured.append(point)
        value = _point_value(trials, objective)
        measured_values[key] = value
        return value

    baseline = ConfigPoint(values={})
    current = baseline
    current_value = await measure(current)
    result.best = current
    result.best_value = current_value

    keys = sorted(grid.knobs.keys())
    # Coordinate descent: each pass evaluates every key's ladder ONCE at the
    # current point and moves to the best single-key improvement; passes
    # repeat until no key improves (收益下降即停). Per-pass evaluation keeps
    # unique point coverage low enough for the <40% target.
    while len(result.points_measured) < max_trials:
        improved = False
        for key in keys:
            if len(result.points_measured) >= max_trials:
                break
            best_candidate: ConfigPoint | None = None
            best_candidate_value = current_value
            for value in grid.knobs[key]:
                if len(result.points_measured) >= max_trials:
                    break
                candidate_values = dict(current.values)
                candidate_values[key] = value
                candidate = ConfigPoint(values=candidate_values)
                cand_value = await measure(candidate)
                if cand_value is not None and (
                    best_candidate_value is None
                    or cand_value > best_candidate_value
                ):
                    best_candidate = candidate
                    best_candidate_value = cand_value
            if best_candidate is not None and (
                current_value is None or best_candidate_value > current_value
            ):
                current = best_candidate
                current_value = best_candidate_value
                result.best = best_candidate
                result.best_value = best_candidate_value
                improved = True
        if not improved:
            break  # no single-key step improves → stop (收益下降即停)

    result.trials.sort(key=lambda t: t.index)
    return result
