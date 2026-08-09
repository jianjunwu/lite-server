"""Profile search grid (plan §2.3/§2.4/§2.8).

Nested grid: the OUTER layer is config points (cross product of
max_batch_size × workers_per_device × batch_timeout), each applied with one
unload/load (Admin ReloadModel); the INNER layer is concurrency levels,
scanned in sequence within a point at zero reload cost. Trial granularity =
(config point, concurrency level).

The search space is decided by the model's declaration state (§2.4):
- batching declared (batch/unbatch overridden) → batch keys sweep from 2,
  never 1;
- not declared / undeterminable → batch keys dropped (measuring the default
  heuristic is fake data);
- continuous_batching → workers_per_device dropped (server forces =1 + warn).

The default grid is bounded (§2.3): config points × concurrency levels ≤
max_trials, exceeding it is an error asking the user to converge manually.
Config points are ordered by reload cost: workers_per_device (process-count
change, heaviest) varies slowest on the outermost axis.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Fixed key ordering: workers_per_device (process-count change) outermost,
# remaining keys in this order.
_RELOAD_COST_ORDER = ("workers_per_device", "max_batch_size", "batch_timeout")

# Default grids when batching is not declared (workers_per_device + concurrency only)
DEFAULT_WORKERS_GRID = (1, 2, 4)
DEFAULT_BATCH_GRID = (2, 4, 8, 16)
DEFAULT_BATCH_TIMEOUTS_MS = (0, 5, 20)
DEFAULT_CONCURRENCY = (1, 2, 4, 8, 16)


class GridError(ValueError):
    """Grid construction conflict (→ CLI exit 2). Message is user-facing."""


@dataclass(frozen=True)
class ConfigPoint:
    """One config point: swept key → value (all other keys untouched)."""

    values: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return dict(self.values)


@dataclass
class GridSpec:
    """Grid specification. batching_declared=None means the AST could not
    decide → batch keys are conservatively dropped."""

    batching_declared: bool | None
    continuous_batching: bool = False
    knobs: dict[str, list] = field(default_factory=dict)
    concurrency: list[int] = field(default_factory=lambda: list(DEFAULT_CONCURRENCY))
    max_trials: int = 64

    def __post_init__(self) -> None:
        _validate_knob_values(self.knobs)
        self._validate_concurrency()
        self._enforce_declaration_rules()

    def _validate_concurrency(self) -> None:
        """Concurrency levels are benchmark worker counts — 0/negative builds
        zero workers (an empty trial with throughput 0, fake data that burns a
        reload cycle instead of failing fast)."""
        for c in self.concurrency:
            if not isinstance(c, int) or isinstance(c, bool) or c < 1:
                raise GridError(
                    f"--concurrency values must be integers >= 1, got {c!r}"
                )

    # ---- declaration-state constraints (§2.4) --------------------------------

    def _enforce_declaration_rules(self) -> None:
        batch_keys = ("max_batch_size", "batch_timeout")
        swept_batch = [k for k in batch_keys if k in self.knobs]
        if swept_batch and self.batching_declared is not True:
            state = (
                "model does not override batch/unbatch (LS101 scenario)"
                if self.batching_declared is False
                else "cannot determine whether batch/unbatch are overridden"
            )
            raise GridError(
                f"--sweep-knob {swept_batch[0]} dropped: {state}. Sweeping the "
                f"default heuristic measures fake data. Override batch/unbatch "
                f"first, or inspect with analyze"
            )
        if swept_batch:
            if "max_batch_size" in self.knobs and 1 in self.knobs["max_batch_size"]:
                raise GridError(
                    "--sweep-knob max_batch_size contains 1: with batch/unbatch "
                    "declared, max_batch_size=1 fails model load (api.py guard, "
                    "a guaranteed-fail point). Sweep from 2"
                )
            if "max_batch_size" in self.knobs:
                for v in self.knobs["max_batch_size"]:
                    if v < 1:
                        raise GridError(f"max_batch_size values must be >= 1, got {v}")
            if "batch_timeout" in self.knobs:
                for v in self.knobs["batch_timeout"]:
                    if v < 0:
                        raise GridError(f"batch_timeout values must be >= 0, got {v}")
        if "workers_per_device" in self.knobs and self.continuous_batching:
            raise GridError(
                "--sweep-knob workers_per_device dropped: with "
                "continuous_batching=true the server forces workers_per_device=1 "
                "and warns (worker/lifecycle.rs:225); sweeping it is fake data"
            )
        if "workers_per_device" in self.knobs:
            for v in self.knobs["workers_per_device"]:
                if v < 1:
                    raise GridError(f"workers_per_device values must be >= 1, got {v}")

    # ---- grid enumeration (§2.3 nested) --------------------------------------

    def config_points(self) -> list[ConfigPoint]:
        """Config-point cross product, ordered by reload cost (workers_per_device outermost)."""
        keys = sorted(
            self.knobs.keys(),
            key=lambda k: (_RELOAD_COST_ORDER.index(k) if k in _RELOAD_COST_ORDER else len(_RELOAD_COST_ORDER), k),
        )
        if not keys:
            return [ConfigPoint(values={})]
        import itertools

        points: list[ConfigPoint] = []
        for combo in itertools.product(*[self.knobs[k] for k in keys]):
            points.append(ConfigPoint(values=dict(zip(keys, combo))))
        return points

    def total_trials(self) -> int:
        return len(self.config_points()) * len(self.concurrency)

    def check_cap(self) -> None:
        """Cross-product cap (§2.3): exceeding max_trials is an error asking to converge."""
        n_points = len(self.config_points())
        if n_points * len(self.concurrency) > self.max_trials:
            raise GridError(
                f"grid exceeds cap: config points {n_points} × concurrency "
                f"{len(self.concurrency)} = {n_points * len(self.concurrency)} "
                f"trials > --max-trials {self.max_trials}. Converge manually "
                f"(--sweep-knob / --concurrency)"
            )


def default_knobs(
    batching_declared: bool | None,
    continuous_batching: bool,
) -> dict[str, list]:
    """v1 default grid (§2.3): decided by declaration state, no conflicting keys."""
    knobs: dict[str, list] = {}
    if batching_declared is True:
        knobs["max_batch_size"] = list(DEFAULT_BATCH_GRID)
        knobs["batch_timeout"] = [t / 1000.0 for t in DEFAULT_BATCH_TIMEOUTS_MS]
    if not continuous_batching:
        knobs["workers_per_device"] = list(DEFAULT_WORKERS_GRID)
    return knobs


def _validate_knob_values(knobs: dict[str, list]) -> None:
    for key, values in knobs.items():
        if not values:
            raise GridError(f"--sweep-knob {key} value list is empty")
        for v in values:
            if not isinstance(v, (int, float)) or isinstance(v, bool):
                raise GridError(f"--sweep-knob {key} values must be numeric, got {v!r}")
