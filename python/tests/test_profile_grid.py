"""GridSpec: nested-grid enumeration, declaration-state constraints, and the
cross-product cap (plan §2.3/§2.4)."""

import pytest

from lite_server.profile.grid import (
    DEFAULT_BATCH_GRID,
    GridError,
    GridSpec,
    default_knobs,
)


class TestDefaultKnobs:
    def test_declared_batching_gets_batch_keys(self):
        knobs = default_knobs(True, continuous_batching=False)
        assert knobs["max_batch_size"] == list(DEFAULT_BATCH_GRID)
        assert 1 not in knobs["max_batch_size"], "never sweep 1 when batching is declared"
        assert "workers_per_device" in knobs

    def test_undeclared_batching_drops_batch_keys(self):
        knobs = default_knobs(False, continuous_batching=False)
        assert "max_batch_size" not in knobs
        assert "batch_timeout" not in knobs
        assert "workers_per_device" in knobs

    def test_unknown_detection_drops_batch_keys_conservatively(self):
        knobs = default_knobs(None, continuous_batching=False)
        assert "max_batch_size" not in knobs, "undeterminable AST → treat as undeclared"

    def test_continuous_batching_drops_workers(self):
        knobs = default_knobs(True, continuous_batching=True)
        assert "workers_per_device" not in knobs, "continuous_batching forces =1"
        assert "max_batch_size" in knobs


class TestDeclarationRules:
    def test_undeclared_but_swept_batch_key_rejected(self):
        with pytest.raises(GridError, match="dropped"):
            GridSpec(
                batching_declared=False,
                knobs={"max_batch_size": [2, 4]},
            )

    def test_unknown_detection_swept_batch_key_rejected(self):
        with pytest.raises(GridError, match="cannot determine"):
            GridSpec(
                batching_declared=None,
                knobs={"max_batch_size": [2, 4]},
            )

    def test_declared_with_one_rejected(self):
        with pytest.raises(GridError, match="contains 1"):
            GridSpec(
                batching_declared=True,
                knobs={"max_batch_size": [1, 2]},
            )

    def test_continuous_batching_swept_workers_rejected(self):
        with pytest.raises(GridError, match="forces workers_per_device=1"):
            GridSpec(
                batching_declared=True,
                continuous_batching=True,
                knobs={"workers_per_device": [1, 2]},
            )

    def test_negative_values_rejected(self):
        with pytest.raises(GridError, match=">= 1"):
            GridSpec(batching_declared=True, knobs={"max_batch_size": [0]})
        with pytest.raises(GridError, match=">= 0"):
            GridSpec(batching_declared=True, knobs={"batch_timeout": [-1.0]})

    def test_empty_values_rejected(self):
        with pytest.raises(GridError, match="empty"):
            GridSpec(batching_declared=True, knobs={"max_batch_size": []})


class TestNestedGrid:
    def test_config_points_cross_product(self):
        spec = GridSpec(
            batching_declared=True,
            knobs={"max_batch_size": [2, 4], "batch_timeout": [0.0, 0.005]},
            concurrency=[1, 2],
        )
        points = spec.config_points()
        assert len(points) == 4
        assert spec.total_trials() == 8

    def test_config_points_ordered_by_reload_cost(self):
        """workers_per_device (process-count change, heaviest) is outermost → varies slowest."""
        spec = GridSpec(
            batching_declared=True,
            knobs={
                "max_batch_size": [2, 4],
                "workers_per_device": [1, 2],
            },
        )
        wpd_seq = [p.values["workers_per_device"] for p in spec.config_points()]
        # Grouped by wpd: 1,1,2,2 — consecutive points change the process count least
        assert wpd_seq == [1, 1, 2, 2]

    def test_empty_knobs_single_baseline_point(self):
        spec = GridSpec(batching_declared=False, knobs={}, concurrency=[1, 2])
        # Empty knobs = baseline-only point (empty values), nothing swept
        points = spec.config_points()
        assert len(points) == 1 and points[0].values == {}

    def test_cap_enforced(self):
        spec = GridSpec(
            batching_declared=True,
            knobs={"max_batch_size": [2, 4, 8, 16], "workers_per_device": [1, 2, 4]},
            concurrency=list(range(1, 17)),
            max_trials=64,
        )
        with pytest.raises(GridError, match="exceeds cap"):
            spec.check_cap()

    def test_cap_ok_within_limit(self):
        spec = GridSpec(
            batching_declared=True,
            knobs={"max_batch_size": [2, 4]},
            concurrency=[1, 2, 4],
            max_trials=64,
        )
        spec.check_cap()  # no raise
