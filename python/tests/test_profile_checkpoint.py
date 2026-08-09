"""checkpoint: trial persistence round-trip, dedup, resume identity keys
(plan §2.8/§2.10)."""

import json

from lite_server.profile.checkpoint import (
    TrialRecord,
    campaign_hash,
    completed_keys,
    read_summary,
    read_trials,
    trial_key,
    write_trials,
)


def _trial(cfg: dict, concurrency: int, index: int, status: str = "ok") -> TrialRecord:
    return TrialRecord(
        index=index, config_point=cfg, concurrency=concurrency,
        status=status, metrics={"throughput": 10.0} if status == "ok" else None,
    )


class TestCampaignHash:
    def test_hash_changes_with_knobs(self):
        a = campaign_hash("m", "1", {"max_batch_size": [2, 4]}, {})
        b = campaign_hash("m", "1", {"max_batch_size": [2, 8]}, {})
        assert a != b

    def test_hash_changes_with_scenario(self):
        a = campaign_hash("m", "1", {}, {"stream": False})
        b = campaign_hash("m", "1", {}, {"stream": True})
        assert a != b

    def test_hash_stable_across_knob_order(self):
        a = campaign_hash("m", "1", {"a": [1], "b": [2]}, {})
        b = campaign_hash("m", "1", {"b": [2], "a": [1]}, {})
        assert a == b


class TestTrialPersistence:
    def test_roundtrip_preserves_fields(self, tmp_path):
        trials = [
            _trial({}, 1, 0),
            _trial({"max_batch_size": 4}, 2, 1),
            _trial({"max_batch_size": 4}, 4, 2, status="failed"),
        ]
        summary = {"campaign_hash": "abc", "trials": [t.to_dict() for t in trials]}
        write_trials(tmp_path, trials, summary)
        back = read_trials(tmp_path)
        assert len(back) == 3
        assert back[1].config_point == {"max_batch_size": 4}
        assert back[1].concurrency == 2
        assert back[2].status == "failed"
        assert read_summary(tmp_path)["campaign_hash"] == "abc"

    def test_write_dedupes_by_identity(self, tmp_path):
        a = _trial({"max_batch_size": 4}, 1, 0)
        b = _trial({"max_batch_size": 4}, 1, 1)  # same (point, concurrency)
        write_trials(tmp_path, [a, b], {"campaign_hash": "x"})
        assert len(read_trials(tmp_path)) == 1

    def test_trial_key_identity(self):
        a = _trial({"max_batch_size": 4}, 2, 0)
        b = _trial({"max_batch_size": 4}, 2, 9)  # index irrelevant
        assert trial_key(a) == trial_key(b)
        c = _trial({"max_batch_size": 8}, 2, 0)
        assert trial_key(a) != trial_key(c)
        d = _trial({"max_batch_size": 4}, 4, 0)
        assert trial_key(a) != trial_key(d)

    def test_completed_keys(self):
        trials = [_trial({}, 1, 0), _trial({"a": 1}, 1, 1)]
        keys = completed_keys(trials)
        assert len(keys) == 2
        assert f'{json.dumps({}, sort_keys=True)}@1' in keys

    def test_missing_summary_returns_none(self, tmp_path):
        assert read_summary(tmp_path) is None
