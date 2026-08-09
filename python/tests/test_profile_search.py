"""quick search: single-key hill climb against a synthetic landscape — must
reach ≥90% of the full-grid optimum using <40% of the points (plan §2.5)."""

import pytest

from lite_server.profile.checkpoint import TrialRecord
from lite_server.profile.grid import ConfigPoint, GridSpec
from lite_server.profile.search import _point_value, quick_search


class _FakeRunner:
    """Fake runner: measure_point returns one ok trial per point whose
    throughput = landscape(point.values); records the call order."""

    def __init__(self, landscape):
        self.landscape = landscape
        self.calls: list[dict] = []

    async def measure_point(self, point: ConfigPoint):
        self.calls.append(point.to_dict())
        value = self.landscape(point.values)
        return [TrialRecord(
            index=len(self.calls) - 1, config_point=point.to_dict(), concurrency=1,
            status="ok", metrics={"throughput": value, "total_requests": 1,
                                  "failed": 0, "latency_ms": {"p99": 1}},
        )]


def _landscape_runner(landscape) -> _FakeRunner:
    return _FakeRunner(landscape)


def _monotonic(steps: dict, default: float = 1.0):
    """Value = default + sum of per-key contributions along each ladder —
    every axis improves monotonically, and multi-key optima are additive."""
    def fn(values: dict) -> float:
        total = default
        for key, ladder in steps.items():
            v = values.get(key)
            if v in ladder:
                total += ladder[v]
        return total
    return fn


class TestQuickSearch:
    @pytest.mark.asyncio
    async def test_hill_climb_finds_peak(self):
        grid = GridSpec(
            batching_declared=False,
            knobs={"workers_per_device": [1, 2, 4]},
            concurrency=[1],
        )
        # Monotonic ladder: the climb walks 1 → 2 → 4.
        landscape = _monotonic({"workers_per_device": {1: 10.0, 2: 40.0, 4: 100.0}},
                             default=0.0)
        runner = _landscape_runner(landscape)
        result = await quick_search(runner, grid, "throughput", max_trials=20)
        assert result.best is not None
        assert result.best.values == {"workers_per_device": 4}
        assert result.best_value == 100.0

    @pytest.mark.asyncio
    async def test_stops_when_no_improvement(self):
        grid = GridSpec(
            batching_declared=False,
            knobs={"workers_per_device": [1, 2, 4]},
            concurrency=[1],
        )
        # Flat landscape: baseline value = default; no step improves → stops
        # after the baseline + one failed exploration round.
        runner = _landscape_runner(_monotonic({}))
        result = await quick_search(runner, grid, "throughput", max_trials=20)
        assert len(result.points_measured) <= 4, (
            "flat landscape must stop immediately, got %d points"
            % len(result.points_measured)
        )

    @pytest.mark.asyncio
    async def test_quality_and_coverage_targets(self):
        """≥90% of grid optimum with <40% of the points (plan §2.5)."""
        grid = GridSpec(
            batching_declared=True,
            knobs={
                "max_batch_size": [2, 4, 8, 16],
                "batch_timeout": [0.0, 0.005, 0.02],
                "workers_per_device": [1, 2, 4, 8],
            },
            concurrency=[1],
        )
        total = len(grid.config_points())
        # Multi-key optimum: every axis improves monotonically, so the climb
        # steps mbs fully, then timeout, then wpd → (16, 0.02, 4) = 21.
        landscape = _monotonic({
            "max_batch_size": {2: 5.0, 4: 10.0, 8: 15.0, 16: 16.0},
            "batch_timeout": {0.0: 0.0, 0.005: 1.0, 0.02: 2.0},
            "workers_per_device": {1: 0.0, 2: 2.0, 4: 4.0},
        }, default=3.0)
        # full-grid optimum
        best = 0.0
        for p in grid.config_points():
            best = max(best, landscape(p.values))
        assert best == 3.0 + 16.0 + 2.0 + 4.0

        runner = _landscape_runner(landscape)
        result = await quick_search(runner, grid, "throughput", max_trials=40)
        ratio = result.best_value / best if best else 0.0
        assert ratio >= 0.9, (
            f"quick search must reach ≥90% of optimum, got {ratio:.2f} "
            f"(best {result.best_value} vs optimum {best})"
        )
        assert result.coverage_ratio < 0.4, (
            f"quick search must use <40% of points, got {result.coverage_ratio:.2f} "
            f"({len(result.points_measured)}/{total})"
        )

    @pytest.mark.asyncio
    async def test_compounding_steps_across_keys(self):
        """The climb compounds single-key steps: mbs fully, then wpd — the
        optimum (16, 4) needs both keys."""
        grid = GridSpec(
            batching_declared=True,
            knobs={"max_batch_size": [2, 4, 8, 16],
                   "workers_per_device": [1, 2, 4]},
            concurrency=[1],
        )
        landscape = _monotonic({
            "max_batch_size": {2: 5.0, 4: 10.0, 8: 15.0, 16: 16.0},
            "workers_per_device": {1: 0.0, 2: 2.0, 4: 4.0},
        }, default=3.0)
        runner = _landscape_runner(landscape)
        result = await quick_search(runner, grid, "throughput", max_trials=20)
        assert result.best is not None
        assert result.best.values == {"max_batch_size": 16,
                                      "workers_per_device": 4}
        assert result.best_value == 3.0 + 16.0 + 4.0

    @pytest.mark.asyncio
    async def test_max_trials_bounded(self):
        grid = GridSpec(
            batching_declared=False,
            knobs={"workers_per_device": [1, 2, 4, 8, 16, 32]},
            concurrency=[1],
        )
        # Strictly increasing → the climb wants to walk the whole ladder;
        # max_trials must cap it.
        landscape = _monotonic({"workers_per_device": {
            1: 1.0, 2: 2.0, 4: 4.0, 8: 8.0, 16: 16.0, 32: 32.0}})
        runner = _landscape_runner(landscape)
        result = await quick_search(runner, grid, "throughput", max_trials=3)
        assert len(result.points_measured) <= 3

    def test_point_value_is_best_over_concurrency(self):
        trials = [
            TrialRecord(index=0, config_point={}, concurrency=1, status="ok",
                        metrics={"throughput": 10.0}),
            TrialRecord(index=1, config_point={}, concurrency=4, status="ok",
                        metrics={"throughput": 40.0}),
            TrialRecord(index=2, config_point={}, concurrency=8, status="failed"),
        ]
        assert _point_value(trials, "throughput") == 40.0
