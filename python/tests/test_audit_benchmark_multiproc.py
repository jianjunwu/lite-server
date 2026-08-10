"""Audit repro tests for 9fafc2a (benchmark multi-process client).

Each test proves one violated assumption in the --processes N machinery.
They FAIL on the audited commit and must not be "fixed" by editing these
tests — the implementation is what changes.

Defect index:
- B1  run_benchmark_children joins children before draining result_q:
      a pickled outcome larger than the OS pipe buffer can never flush
      (multiprocessing blocks process exit until the queue feeder drains),
      so the parent kills a healthy child as a false "timeout".
- B2  merge_child_results copies results[0].target_rate, but children each
      ran rate/processes — the merged report undercounts target rate N×.
- B3  _run_benchmark_level_multiproc budgets timeout as
      duration + grace + 360s; in requests mode (duration=None) the run can
      legitimately take far longer (requests / rate), so long runs are
      killed mid-measurement and reported as crashes.
- B4  run_benchmark_children dequeues outcomes in completion order while
      its contract promises spec order (ChildPool honors it; this path
      does not).
- B5  every child runs the full --warmup-requests, yet the merged result
      reports one child's count — the report says N× fewer warmups than
      were actually issued to the server.
"""

import argparse
import asyncio
import time

from lite_server import cli
from lite_server.benchmark.benchmark import BenchmarkResult
from lite_server.benchmark.multiproc import (
    ChildOutcome,
    merge_child_results,
    run_benchmark_children,
)

# ---------------------------------------------------------------------------
# Spawn entries (module-level so the spawn child can import them)
# ---------------------------------------------------------------------------


def _audit_huge_result_entry(arg):
    """Return a payload far larger than any OS pipe buffer (64KB-1MB)."""
    return "x" * arg


def _audit_sleepy_entry(arg):
    """Sleep ``arg`` seconds, then echo it back (identifies the spec)."""
    time.sleep(arg)
    return arg


# ---------------------------------------------------------------------------
# B1 — control-flow/resource assumption: join() before draining the queue
# ---------------------------------------------------------------------------


class TestAuditPipeBufferDeadlock:
    def test_large_outcome_survives_transfer(self):
        """A healthy child returning a large result must come back as a
        result, not a timeout crash.  8MB >> pipe buffer: the child blocks
        at exit waiting for the feeder to drain, while the parent blocks in
        join() without reading — a documented multiprocessing deadlock."""
        outcomes = run_benchmark_children(
            _audit_huge_result_entry, [(8_000_000, {})], timeout_secs=5
        )
        assert outcomes[0].crash is None
        assert outcomes[0].result == "x" * 8_000_000


# ---------------------------------------------------------------------------
# B4 — ordering assumption: outcomes must align with specs
# ---------------------------------------------------------------------------


class TestAuditOutcomeOrdering:
    def test_outcomes_follow_spec_order(self):
        """Docstring: 'Returns one ChildOutcome per spec, in spec order.'
        The slow first spec completes last; spec order must still hold."""
        specs = [(2.0, {}), (0.0, {})]
        outcomes = run_benchmark_children(_audit_sleepy_entry, specs, timeout_secs=30)
        assert [o.result for o in outcomes] == [2.0, 0.0]


# ---------------------------------------------------------------------------
# B2 / B5 — data assumptions in merge_child_results
# ---------------------------------------------------------------------------


def _child_result(*, target_rate=None, warmup=0):
    r = BenchmarkResult()
    r.latencies = [1.0]
    r.send_times_ns = [1_000]
    r.first_t0_ns = 1_000
    r.last_t1_ns = 2_000
    r.successful = 1
    r.total_requests = 1
    r.window = 1e-6
    r.load_mode = "open-loop" if target_rate is not None else "closed-loop"
    r.target_rate = target_rate
    r.warmup_requests = warmup
    return r


class TestAuditMergeScalarFields:
    def test_merge_sums_split_target_rate(self):
        """Children each ran rate/processes; the merged run's target rate is
        their sum — copying one child's value undercounts N× in to_dict()
        (export JSON + profile metrics)."""
        a = _child_result(target_rate=50.0)
        b = _child_result(target_rate=50.0)
        merged = merge_child_results(
            [ChildOutcome(a, None), ChildOutcome(b, None)], concurrency=2
        )
        assert merged.target_rate == 100.0

    def test_merge_sums_warmup_requests(self):
        """Each child ran the full warmup count, so the merged run performed
        sum(children) warmups against the server; reporting one child's
        value misstates the actual load N×."""
        a = _child_result(warmup=10)
        b = _child_result(warmup=10)
        merged = merge_child_results(
            [ChildOutcome(a, None), ChildOutcome(b, None)], concurrency=2
        )
        assert merged.warmup_requests == 20


# ---------------------------------------------------------------------------
# B3 — data assumption: timeout budget must cover the requested workload
# ---------------------------------------------------------------------------


class TestAuditTimeoutBudget:
    def test_requests_mode_timeout_covers_schedule(self, monkeypatch):
        """requests mode (duration=None): 100k requests at 10 req/s needs
        ~10000s, but the budget is grace + 360s — the children are killed
        mid-run and the whole benchmark errors out as 'child timed out'."""
        captured = {}

        def fake_children(entry, specs, timeout_secs):
            captured["timeout_secs"] = timeout_secs
            return [ChildOutcome(_child_result(), None) for _ in specs]

        monkeypatch.setattr(
            "lite_server.benchmark.multiproc.run_benchmark_children",
            fake_children,
        )
        args = argparse.Namespace(
            requests=100_000,
            rate=10.0,
            grace_period=30.0,
            warmup_requests=0,
            model_type="llm",
            slo_attainment=None,
            bidi=False,
            min_sessions=30,
        )
        asyncio.run(
            cli._run_benchmark_level_multiproc(
                args, 2,
                url="http://127.0.0.1:8000",
                payloads=[{"input": 1}],
                pacing=None,
                duration=None,
                goodput_slo=None,
                trust_env=False,
                processes=2,
            )
        )
        # The budget must cover the open-loop schedule plus drain grace.
        assert captured["timeout_secs"] >= 100_000 / 10.0 + 30.0
