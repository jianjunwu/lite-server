"""Trial records and campaign hashing (plan §2.8).

Trial granularity = (config point, concurrency level); each trial is one JSON
record, i.e. the checkpoint. campaign_hash(model + version + knob set +
scenario + metric caliber) guards `--resume` against reusing old data for a
new grid (used by batch 2; fields land in batch 1).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

# Metric-caliber version: bump when the benchmark's CO labeling / percentile
# conventions / streaming metrics evolve.
METRIC_CALIBER = "v1"

# trial JSON schema_version (since batch 1)
TRIAL_SCHEMA_VERSION = 1


@dataclass
class TrialRecord:
    index: int
    config_point: dict[str, object]
    concurrency: int
    status: str  # ok | failed
    reason: str | None = None
    batching_declared: bool | None = None
    continuous_batching: bool = False
    token_count_basis: str | None = None
    metrics: dict | None = None
    server_metrics: dict | None = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        data = asdict(self)
        data["schema_version"] = TRIAL_SCHEMA_VERSION
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "TrialRecord":
        data = dict(data)
        data.pop("schema_version", None)
        return cls(**data)


def campaign_hash(
    model: str,
    version: str,
    knobs: dict[str, list],
    scenario: dict[str, object],
    caliber: str = METRIC_CALIBER,
) -> str:
    """Campaign fingerprint: changing the grid/scenario/caliber must change the
    hash so `--resume` rejects stale data."""
    canonical = json.dumps(
        {
            "model": model,
            "version": version,
            "knobs": {k: list(v) for k, v in sorted(knobs.items())},
            "scenario": scenario,
            "caliber": caliber,
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
