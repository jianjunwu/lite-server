"""rank: constraint filtering per scenario face, objective legality matrix,
objective ranking, improvement percentages, config diff (plan §2.7/§2.8)."""

import pytest

from lite_server.profile.checkpoint import TrialRecord
from lite_server.profile.rank import (
    Constraints,
    RankError,
    config_diff,
    improvement_percent,
    objective_value,
    passes_constraints,
    rank_trials,
    validate_objective,
)

UNARY_SCENARIO = {"stream": False, "bidi": False, "model_type": "llm"}
STREAM_SCENARIO = {"stream": True, "bidi": False, "model_type": "llm"}
BIDI_SCENARIO = {"stream": False, "bidi": True, "model_type": "generic"}


def _trial(cfg: dict, concurrency: int = 1, metrics: dict | None = None,
           status: str = "ok", server_metrics: dict | None = None) -> TrialRecord:
    return TrialRecord(
        index=0, config_point=cfg, concurrency=concurrency,
        status=status, metrics=metrics, server_metrics=server_metrics,
    )


def _unary_metrics(tp: float, p99: float, failed: int = 0, total: int = 100) -> dict:
    return {
        "throughput": tp, "total_requests": total, "failed": failed,
        "latency_ms": {"p99": p99},
    }


class TestObjectiveLegality:
    def test_goodput_requires_slo(self):
        with pytest.raises(RankError, match="requires --goodput"):
            validate_objective("goodput", Constraints(), UNARY_SCENARIO,
                               local=True, goodput_given=False)

    def test_goodput_ok_with_slo(self):
        validate_objective("goodput", Constraints(), UNARY_SCENARIO,
                           local=True, goodput_given=True)

    def test_sessions_per_sec_bidi_only(self):
        with pytest.raises(RankError, match="bidi"):
            validate_objective("sessions_per_sec", Constraints(), UNARY_SCENARIO,
                               local=True, goodput_given=False)
        validate_objective("sessions_per_sec", Constraints(), BIDI_SCENARIO,
                           local=True, goodput_given=False)

    def test_rss_constraint_local_only(self):
        with pytest.raises(RankError, match="local"):
            validate_objective("throughput", Constraints(max_rss_mb=100),
                               UNARY_SCENARIO, local=False, goodput_given=False)
        validate_objective("throughput", Constraints(max_rss_mb=100),
                           UNARY_SCENARIO, local=True, goodput_given=False)

    def test_stream_constraints_require_stream(self):
        with pytest.raises(RankError, match="streaming"):
            validate_objective("throughput", Constraints(max_ttft_ms=500),
                               UNARY_SCENARIO, local=True, goodput_given=False)

    def test_rtf_requires_tts_stt(self):
        with pytest.raises(RankError, match="tts or stt"):
            validate_objective("throughput", Constraints(max_rtf=1.0),
                               STREAM_SCENARIO, local=True, goodput_given=False)
        validate_objective("throughput", Constraints(max_rtf=1.0),
                           {"stream": True, "bidi": False, "model_type": "tts"},
                           local=True, goodput_given=False)

    def test_bidi_constraints_require_bidi(self):
        with pytest.raises(RankError, match="require --bidi"):
            validate_objective("throughput", Constraints(max_session_ms=30000),
                               UNARY_SCENARIO, local=True, goodput_given=False)


class TestConstraintFiltering:
    def test_unary_constraints(self):
        ok_trial = _trial({}, metrics=_unary_metrics(tp=80, p99=150))
        slow = _trial({}, metrics=_unary_metrics(tp=80, p99=350))
        low_tp = _trial({}, metrics=_unary_metrics(tp=10, p99=100))
        err = _trial({}, metrics=_unary_metrics(tp=80, p99=100, failed=10, total=100))
        c = Constraints(max_p99=300, min_throughput=50, max_error_rate=0.01)
        assert passes_constraints(ok_trial, c, UNARY_SCENARIO) == (True, [])
        assert "p99" in passes_constraints(slow, c, UNARY_SCENARIO)[1][0]
        assert "throughput" in passes_constraints(low_tp, c, UNARY_SCENARIO)[1][0]
        assert "error rate" in passes_constraints(err, c, UNARY_SCENARIO)[1][0]

    def test_failed_trial_never_passes(self):
        t = _trial({}, metrics=_unary_metrics(80, 100), status="failed")
        assert passes_constraints(t, Constraints(), UNARY_SCENARIO) == (False, ["trial not ok"])

    def test_stream_ttft_constraint(self):
        t = _trial({}, metrics={
            "throughput": 10, "total_requests": 10, "failed": 0,
            "latency_ms": {"p99": 100},
            "stream": {"ttft_ms": {"p99": 900}},
        })
        ok, v = passes_constraints(t, Constraints(max_ttft_ms=500), STREAM_SCENARIO)
        assert not ok and "TTFT" in v[0]

    def test_stream_goodput_attainment(self):
        t = _trial({}, metrics={
            "throughput": 10, "total_requests": 10, "failed": 0,
            "latency_ms": {"p99": 100},
            "stream": {"goodput": {"attainment": 0.80, "attainment_target": 0.95}},
        })
        ok, v = passes_constraints(t, Constraints(slo_attainment=0.95), STREAM_SCENARIO)
        assert not ok and "SLO attainment" in v[0]

    def test_rtf_constraint(self):
        t = _trial({}, metrics={
            "throughput": 10, "total_requests": 10, "failed": 0,
            "latency_ms": {"p99": 100},
            "stream": {"rtf": {"p99": 2.5}},
        })
        ok, v = passes_constraints(t, Constraints(max_rtf=1.0),
                                   {"stream": True, "bidi": False, "model_type": "tts"})
        assert not ok and "RTF" in v[0]

    def test_bidi_session_constraint(self):
        t = _trial({}, metrics={
            "throughput": 10, "total_requests": 10, "failed": 0,
            "latency_ms": {"p99": 100},
            "bidi": {"session_duration_ms": {"p99": 60000}},
        })
        ok, v = passes_constraints(t, Constraints(max_session_ms=30000), BIDI_SCENARIO)
        assert not ok and "session p99" in v[0]

    def test_rss_constraint(self):
        m = _unary_metrics(80, 100)
        m["resources"] = {"rss_max_mb": 250.0}
        t = _trial({}, metrics=m)
        ok, v = passes_constraints(t, Constraints(max_rss_mb=200), UNARY_SCENARIO)
        assert not ok and "rss" in v[0]


class TestRanking:
    def test_rank_desc_by_throughput(self):
        trials = [
            _trial({"max_batch_size": 2}, metrics=_unary_metrics(30, 100)),
            _trial({"max_batch_size": 8}, metrics=_unary_metrics(90, 100)),
            _trial({"max_batch_size": 4}, metrics=_unary_metrics(60, 100)),
        ]
        r = rank_trials(trials, "throughput", Constraints(), UNARY_SCENARIO, top_n=2)
        assert [t.config_point["max_batch_size"] for t in r.top] == [8, 4]

    def test_constraints_exclude_from_ranking(self):
        trials = [
            _trial({}, metrics=_unary_metrics(90, 500)),  # violates p99 budget
            _trial({}, metrics=_unary_metrics(40, 100)),
        ]
        r = rank_trials(trials, "throughput", Constraints(max_p99=300), UNARY_SCENARIO)
        assert [objective_value(t, "throughput") for t in r.top] == [40]
        assert len(r.passed) == 1 and len(r.failed) == 0

    def test_failed_trials_kept_auditable(self):
        bad = _trial({}, status="failed", metrics=None)
        good = _trial({}, metrics=_unary_metrics(50, 100))
        r = rank_trials([bad, good], "throughput", Constraints(), UNARY_SCENARIO)
        assert r.failed == [bad]
        assert r.top == [good]

    def test_baseline_identified_by_empty_config_point(self):
        trials = [
            _trial({}, metrics=_unary_metrics(50, 100)),
            _trial({"max_batch_size": 4}, metrics=_unary_metrics(75, 100)),
        ]
        r = rank_trials(trials, "throughput", Constraints(), UNARY_SCENARIO)
        assert r.baseline is trials[0]
        assert improvement_percent(trials[1], r.baseline, "throughput") == 50.0

    def test_sessions_per_sec_objective(self):
        t = _trial({}, metrics={
            "throughput": 99, "total_requests": 10, "failed": 0,
            "latency_ms": {"p99": 100},
            "bidi": {"sessions_per_sec": 7.5},
        })
        assert objective_value(t, "sessions_per_sec") == 7.5


class TestConfigDiff:
    def test_diff_shows_changed_lines(self):
        original = "max_batch_size: 1\nbatch_timeout: 0.0\n"
        recommended = "max_batch_size: 8\nbatch_timeout: 0.0\n"
        d = config_diff(original, recommended)
        assert "-max_batch_size: 1" in d and "+max_batch_size: 8" in d
        # unchanged lines appear as context (no +/- prefix)
        assert " batch_timeout: 0.0" in d


class TestMarkdownReport:
    def test_render_top_n_markdown(self):
        from lite_server.profile.report import render_top_n_markdown

        trials = [
            _trial({"max_batch_size": 8}, metrics={
                "throughput": 90.0, "total_requests": 100, "failed": 0,
                "latency_ms": {"p50": 1, "p95": 2, "p99": 3, "max": 5},
                "resources": {"rss_max_mb": 120.0},
            }),
            _trial({"max_batch_size": 4}, metrics={
                "throughput": 85.0, "total_requests": 100, "failed": 0,
                "latency_ms": {"p50": 1, "p95": 2, "p99": 3, "max": 5},
            }),
        ]
        r = rank_trials(trials, "throughput", Constraints(), UNARY_SCENARIO, top_n=2)
        md = render_top_n_markdown(r)
        assert "## Top-2 recommendations" in md
        assert "`{'max_batch_size': 8}`" in md
        assert "90.00" in md and "120" in md
        assert "n/a" in md  # the runner-up has no resource sampling

    def test_render_no_recommendation(self):
        from lite_server.profile.report import render_top_n_markdown

        bad = _trial({}, metrics=_unary_metrics(90, 500))
        r = rank_trials([bad], "throughput", Constraints(max_p99=300),
                        UNARY_SCENARIO, top_n=3)
        md = render_top_n_markdown(r)
        assert "nothing to recommend" in md
