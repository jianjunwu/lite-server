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
from pathlib import Path

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
    concurrency: list[int] | None = None,
    caliber: str = METRIC_CALIBER,
) -> str:
    """Campaign fingerprint: changing the grid (knobs + concurrency) /
    scenario / caliber must change the hash so `--resume` rejects stale data."""
    payload: dict = {
        "model": model,
        "version": version,
        "knobs": {k: list(v) for k, v in sorted(knobs.items())},
        "scenario": scenario,
        "caliber": caliber,
    }
    if concurrency is not None:
        payload["concurrency"] = list(concurrency)
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


# ---- checkpoint persistence (plan §2.8/§2.10) -------------------------------


def trial_key(trial: TrialRecord) -> str:
    """Stable identity: (config point, concurrency)."""
    point = json.dumps(trial.config_point, sort_keys=True, ensure_ascii=False)
    return f"{point}@{trial.concurrency}"


def write_trials(export_dir: Path, trials: list[TrialRecord], summary: dict) -> None:
    """Persist per-trial JSON + summary.json (dedupes by trial identity)."""
    export_dir = Path(export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()
    for t in trials:
        key = trial_key(t)
        if key in seen:
            continue
        seen.add(key)
        (export_dir / f"trial-{t.index:03d}.json").write_text(
            json.dumps(t.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8",
        )
    (export_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8",
    )


def read_trials(export_dir: Path) -> list[TrialRecord]:
    """Read per-trial JSONs back (order by index)."""
    export_dir = Path(export_dir)
    records: list[TrialRecord] = []
    for path in sorted(export_dir.glob("trial-*.json")):
        try:
            records.append(TrialRecord.from_dict(
                json.loads(path.read_text(encoding="utf-8"))
            ))
        except (OSError, json.JSONDecodeError, TypeError):
            continue
    return records


def read_summary(export_dir: Path) -> dict | None:
    summary_path = Path(export_dir) / "summary.json"
    if not summary_path.exists():
        return None
    try:
        return json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def completed_keys(trials: list[TrialRecord]) -> set[str]:
    return {trial_key(t) for t in trials}
