"""Multi-process benchmark client (方案 A): split / merge / spawn.

Test matrix:
- Pure split/merge units (no processes spawned).
- Child entry rebuild logic (in-process, fake httpx).
- Real unified-spawn end-to-end against a minimal HTTP server.

The parent merges *raw* samples across children (latencies, send times,
stream/bidi records), so percentiles and stream metrics are recomputed on
the union — exact, not an approximation of per-child percentiles.
"""

import argparse
import asyncio
import json
import sys
import threading

import numpy as np
import pytest

from lite_server import cli
from lite_server.benchmark.benchmark import (
    BidiSessionRecord,
    BenchmarkResult,
    StreamRequestRecord,
)
from lite_server.benchmark.bidi_metrics import compute_bidi_metrics
from lite_server.benchmark.multiproc import (
    ChildOutcome,
    merge_child_results,
    run_benchmark_children,
    split_work,
)
from lite_server.benchmark.stream_metrics import compute_stream_metrics


# ---------------------------------------------------------------------------
# split_work: exact split of a total across processes
# ---------------------------------------------------------------------------

class TestSplitWork:
    def test_even_split(self):
        assert split_work(10, 2) == [5, 5]

    def test_uneven_split_spreads_remainder_first(self):
        assert split_work(10, 3) == [4, 3, 3]

    def test_single_process_is_identity(self):
        assert split_work(7, 1) == [7]

    def test_round_trip_sum(self):
        assert sum(split_work(137, 8)) == 137

    def test_more_processes_than_work_tails_are_zero(self):
        assert split_work(2, 4) == [1, 1, 0, 0]


# ---------------------------------------------------------------------------
# merge_child_results: exact aggregation of raw samples
# ---------------------------------------------------------------------------

def _result(latencies, *, first_t0_ns, last_t1_ns, failed=0,
            error_kinds=None, send_times=None, warnings=None):
    r = BenchmarkResult()
    r.latencies = list(latencies)
    r.send_times_ns = (
        list(send_times) if send_times is not None
        else [first_t0_ns + i * 1_000_000 for i in range(len(latencies))]
    )
    r.first_t0_ns = first_t0_ns
    r.last_t1_ns = last_t1_ns
    r.successful = len(latencies)
    r.failed = failed
    r.total_requests = len(latencies) + failed
    r.error_kinds = dict(error_kinds or {})
    r.warnings = list(warnings or [])
    r.window = (last_t1_ns - first_t0_ns) / 1e9
    return r


class TestMergeChildResults:
    def test_merge_unary_percentiles_exact(self):
        """Percentiles on merged raw samples == percentiles of the union."""
        a = _result([1.0, 2.0, 3.0], first_t0_ns=1_000, last_t1_ns=3_000)
        b = _result([4.0, 5.0, 6.0], first_t0_ns=1_500, last_t1_ns=3_500)
        merged = merge_child_results(
            [ChildOutcome(a, None), ChildOutcome(b, None)], concurrency=2
        )

        union = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
        assert merged.successful == 6
        assert merged.total_requests == 6
        assert merged.p50 == pytest.approx(float(np.percentile(union, 50)))
        assert merged.p99 == pytest.approx(float(np.percentile(union, 99)))
        assert merged.latencies == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]

    def test_merge_window_is_union(self):
        """Merged window spans min first_t0 .. max last_t1 across children."""
        a = _result([1.0], first_t0_ns=1_000_000_000, last_t1_ns=2_000_000_000)
        b = _result([1.0], first_t0_ns=1_500_000_000, last_t1_ns=4_000_000_000)
        merged = merge_child_results(
            [ChildOutcome(a, None), ChildOutcome(b, None)], concurrency=2
        )
        assert merged.window == pytest.approx(3.0)  # (4s - 1s) window union

    def test_merge_counts_and_error_kinds_sum(self):
        a = _result([1.0], first_t0_ns=1_000, last_t1_ns=2_000,
                    failed=1, error_kinds={"timeout": 1})
        b = _result([1.0], first_t0_ns=1_000, last_t1_ns=2_000,
                    failed=2, error_kinds={"status": 2})
        merged = merge_child_results(
            [ChildOutcome(a, None), ChildOutcome(b, None)], concurrency=2
        )
        assert merged.failed == 3
        assert merged.total_requests == 5
        assert merged.error_kinds == {"timeout": 1, "status": 2}

    def test_merge_crash_raises(self):
        a = _result([1.0], first_t0_ns=1_000, last_t1_ns=2_000)
        with pytest.raises(ValueError, match="boom"):
            merge_child_results(
                [ChildOutcome(a, None), ChildOutcome(None, "boom")],
                concurrency=2,
            )

    def test_merge_all_crashed_raises(self):
        with pytest.raises(ValueError, match="crash"):
            merge_child_results(
                [ChildOutcome(None, "crash"), ChildOutcome(None, "crash")],
                concurrency=2,
            )

    def test_sample_size_warning_uses_merged_count_and_total_concurrency(self):
        """Children suppress their own sample-size warning (min_samples=1);
        the parent re-checks with max(300, 10 * total_concurrency)."""
        a = _result([1.0] * 10, first_t0_ns=1_000, last_t1_ns=2_000)
        b = _result([1.0] * 10, first_t0_ns=1_000, last_t1_ns=2_000)
        merged = merge_child_results(
            [ChildOutcome(a, None), ChildOutcome(b, None)], concurrency=8
        )
        assert any("Sample size" in w for w in merged.warnings)
        assert any("max(300, 10*concurrency)" in w for w in merged.warnings)

    def test_no_sample_size_warning_when_merged_above_threshold(self):
        a = _result([1.0] * 200, first_t0_ns=1_000, last_t1_ns=2_000)
        b = _result([1.0] * 200, first_t0_ns=1_000, last_t1_ns=2_000)
        merged = merge_child_results(
            [ChildOutcome(a, None), ChildOutcome(b, None)], concurrency=1
        )
        assert not any("Sample size" in w for w in merged.warnings)

    def test_bidi_sample_size_warning_uses_min_sessions(self):
        a = _result([1.0] * 10, first_t0_ns=1_000, last_t1_ns=2_000)
        merged = merge_child_results(
            [ChildOutcome(a, None)], concurrency=1, bidi=True, min_sessions=30
        )
        assert any("Sample size" in w for w in merged.warnings)

    def test_merge_stream_records_exact(self):
        """Stream metrics recomputed on the union of raw records."""
        recs_a = [
            StreamRequestRecord(chunk_count=2, total_bytes=10, ttft_ms=5.0,
                                total_ms=40.0, inter_chunk_ms=[20.0],
                                meta_totals={"token_count": 2.0}),
            StreamRequestRecord(chunk_count=3, total_bytes=15, ttft_ms=6.0,
                                total_ms=60.0, inter_chunk_ms=[18.0, 22.0],
                                meta_totals={"token_count": 3.0}),
        ]
        recs_b = [
            StreamRequestRecord(chunk_count=1, total_bytes=5, ttft_ms=7.0,
                                total_ms=30.0, inter_chunk_ms=[],
                                meta_totals={"token_count": 1.0}),
        ]
        a = _result([1.0], first_t0_ns=1_000, last_t1_ns=2_000)
        b = _result([1.0], first_t0_ns=1_000, last_t1_ns=2_000)
        a.stream_records = recs_a
        b.stream_records = recs_b
        from lite_server.benchmark.benchmark import StreamMetrics

        a.stream_metrics = StreamMetrics(
            model_type="llm", requests=2, zero_chunk_requests=0,
            total_chunks=5, total_bytes=25,
            chunks_per_request={}, ttft_ms={}, total_ms={},
        )
        b.stream_metrics = a.stream_metrics

        merged = merge_child_results(
            [ChildOutcome(a, None), ChildOutcome(b, None)],
            concurrency=2, model_type="llm",
        )

        all_recs = recs_a + recs_b
        expected = compute_stream_metrics(all_recs, "llm", window_secs=merged.window)
        assert merged.stream_metrics.to_dict() == expected.to_dict()
        assert merged.stream_metrics.requests == 3
        assert merged.stream_metrics.total_chunks == 6

    def test_merge_bidi_records_exact(self):
        recs_a = [BidiSessionRecord(open_latency_ms=5.0, close_to_final_ms=10.0,
                                    session_duration_ms=50.0, consumer_chunks=3,
                                    producer_chunks=3)]
        recs_b = [BidiSessionRecord(open_latency_ms=6.0, close_to_final_ms=11.0,
                                    session_duration_ms=55.0, consumer_chunks=2,
                                    producer_chunks=2)]
        a = _result([1.0], first_t0_ns=1_000, last_t1_ns=2_000)
        b = _result([1.0], first_t0_ns=1_000, last_t1_ns=2_000)
        a.bidi_records = recs_a
        b.bidi_records = recs_b
        from lite_server.benchmark.benchmark import BidiSessionMetrics

        base = BidiSessionMetrics(transport="ws", pacing_mode="lock_step",
                                  sessions=1, failed_sessions=0,
                                  open_latency_ms={}, close_to_final_ms={},
                                  session_duration_ms={}, chunks_per_session={})
        a.bidi_metrics = base
        b.bidi_metrics = base

        merged = merge_child_results(
            [ChildOutcome(a, None), ChildOutcome(b, None)],
            concurrency=2, bidi=True,
        )

        expected = compute_bidi_metrics(
            recs_a + recs_b, transport="ws", pacing_mode="lock_step",
            failed_sessions=0, window_secs=merged.window,
        )
        assert merged.bidi_metrics.to_dict() == expected.to_dict()
        assert merged.bidi_metrics.sessions == 2

    def test_merge_preserves_child_warnings(self):
        a = _result([1.0], first_t0_ns=1_000, last_t1_ns=2_000,
                    warnings=["child warning"])
        merged = merge_child_results(
            [ChildOutcome(a, None)], concurrency=1
        )
        assert "child warning" in merged.warnings


# ---------------------------------------------------------------------------
# run_benchmark_children: unified-spawn orchestration
# ---------------------------------------------------------------------------

def _echo_entry(*args, **kwargs):
    """Child entry stand-in: returns a raw picklable result — the spawn
    wrapper wraps it in a ChildOutcome."""
    return _result([1.0], first_t0_ns=1_000, last_t1_ns=2_000)


def _crash_entry(*args, **kwargs):
    raise RuntimeError("entry exploded")


def _pickle_test_entry(payload):
    """Module-level picklable entry that round-trips its argument."""
    return payload


class TestRunBenchmarkChildren:
    def test_spawns_all_children_and_collects_outcomes(self):
        spec = ({"a": 1}, {})
        outcomes = run_benchmark_children(
            _echo_entry, [spec, spec, spec], timeout_secs=60
        )
        assert len(outcomes) == 3
        assert all(o.result is not None and o.crash is None for o in outcomes)
        assert [o.result.latencies for o in outcomes] == [[1.0]] * 3

    def test_args_round_trip_through_spawn(self):
        """Arguments are pickled across the spawn boundary verbatim."""
        sentinel = {"x": [1, 2, 3], "y": None}
        outcomes = run_benchmark_children(
            _pickle_test_entry, [(sentinel, {})], timeout_secs=60
        )
        assert outcomes[0].result == sentinel

    def test_child_exception_reports_crash(self):
        outcomes = run_benchmark_children(
            _crash_entry, [(None, {})], timeout_secs=60
        )
        assert outcomes[0].result is None
        assert "exploded" in outcomes[0].crash


# ---------------------------------------------------------------------------
# _benchmark_child_process: rebuild closures, apply per-child overrides
# (in-process; the same function runs inside the spawned children)
# ---------------------------------------------------------------------------

class _FakeResponse:
    status_code = 200


class _FakeAsyncClient:
    def __init__(self, *a, **kw):
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _FakeResponse()


def _fake_httpx(monkeypatch):
    calls = []

    class FakeClient:
        def __init__(self, *a, **kw):
            self.calls = calls

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, **kwargs):
            self.calls.append((url, kwargs))
            return _FakeResponse()

    fake = type("FakeHttpx", (), {
        "AsyncClient": FakeClient,
        "Limits": lambda *a, **kw: None,
        "Timeout": lambda *a, **kw: None,
        "TimeoutException": type("TimeoutException", (Exception,), {}),
        "ConnectError": type("ConnectError", (Exception,), {}),
        "TransportError": type("TransportError", (Exception,), {}),
    })()
    monkeypatch.setitem(sys.modules, "httpx", fake)
    return calls


def _benchmark_args(**overrides):
    base = dict(
        url="http://127.0.0.1:8000",
        model="test_model",
        version=None,
        concurrency="1",
        duration=None,
        requests=5,
        warmup_requests=0,
        grace_period=1.0,
        payload=None,
        payload_file=None,
        payload_random=None,
        export=None,
        max_error_rate=None,
        max_p99=None,
        rate=None,
        stream=False,
        model_type="llm",
        stream_read_timeout=300.0,
        max_ttft_ms=None,
        max_rtf=None,
        endpoint="events",
        transport="sse",
        bidi=False,
        pace=None,
        rt_factor=None,
        min_sessions=30,
        cancel_after=None,
        read_delay_ms=None,
        goodput=None,
        slo_attainment=None,
        tokenizer=None,
        text_field=None,
        processes=1,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


class TestBenchmarkChildProcess:
    def test_child_rebuilds_payload_factory_from_args(self, monkeypatch):
        """payload_random is a closure in the parent — the child rebuilds it
        from args and randomizes id per request."""
        calls = _fake_httpx(monkeypatch)
        args = _benchmark_args(payload_random='{"id": "seed", "input": 1.0}')

        outcome = cli._benchmark_child_process(
            args, concurrency=1, url=args.url, payloads=[{"input": 1.0}], pacing=None, duration=None,
            goodput_slo=None, trust_env=True, child_requests=3,
        )

        assert outcome.total_requests == 3
        ids = [kw["json"]["id"] for _, kw in calls]
        assert len(ids) == 3
        assert all(i != "seed" for i in ids)
        assert len(set(ids)) == 3  # unique per request

    def test_child_requests_override_does_not_mutate_args(self, monkeypatch):
        calls = _fake_httpx(monkeypatch)
        args = _benchmark_args(requests=10)

        outcome = cli._benchmark_child_process(
            args, concurrency=1, url=args.url, payloads=[{"input": 1.0}], pacing=None, duration=None,
            goodput_slo=None, trust_env=True, child_requests=3,
        )

        assert outcome.total_requests == 3
        assert len(calls) == 3
        assert args.requests == 10  # caller's namespace untouched

    def test_child_rate_override(self, monkeypatch):
        calls = _fake_httpx(monkeypatch)
        args = _benchmark_args(requests=None)

        outcome = cli._benchmark_child_process(
            args, concurrency=1, url=args.url, payloads=[{"input": 1.0}], pacing=None, duration=1.0,
            goodput_slo=None, trust_env=True, child_rate=25.0,
        )

        assert outcome.target_rate == 25.0
        assert outcome.load_mode == "open-loop"

    def test_child_min_samples_suppressed(self, monkeypatch):
        """Children pass min_samples=1 so only the parent's merged
        sample-size warning fires."""
        _fake_httpx(monkeypatch)
        args = _benchmark_args(requests=3)

        outcome = cli._benchmark_child_process(
            args, concurrency=1, url=args.url, payloads=[{"input": 1.0}], pacing=None, duration=None,
            goodput_slo=None, trust_env=True, child_requests=3,
        )

        assert not any("Sample size" in w for w in outcome.warnings)


# ---------------------------------------------------------------------------
# CLI end-to-end with real spawn + real httpx + real HTTP server
# ---------------------------------------------------------------------------

class _MinimalHttpServer:
    """Tiny HTTP/1.1 server answering /v2/models/{m}/infer with 200 JSON."""

    def __init__(self):
        self.port = None
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        for _ in range(100):  # wait for the port to be bound
            if self.port is not None:
                return
            import time
            time.sleep(0.05)
        raise RuntimeError("minimal HTTP server failed to start")

    def _run(self):
        async def serve():
            async def handle(reader, writer):
                try:
                    while True:
                        line = await reader.readline()
                        if line in (b"\r\n", b"", None):
                            break
                    body = b'{"output": 1.0}'
                    writer.write(
                        b"HTTP/1.1 200 OK\r\n"
                        b"Content-Type: application/json\r\n"
                        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                        b"Connection: close\r\n\r\n" + body
                    )
                    await writer.drain()
                except Exception:
                    pass
                finally:
                    writer.close()

            server = await asyncio.start_server(handle, "127.0.0.1", 0)
            self.port = server.sockets[0].getsockname()[1]
            async with server:
                await server.serve_forever()

        asyncio.run(serve())

    def url(self):
        return f"http://127.0.0.1:{self.port}"


class TestBenchmarkCliMultiprocess:
    def test_processes_zero_is_rejected(self):
        rc = cli.main(["benchmark", "--model", "m", "--processes", "0"])
        assert rc == 2

    def test_processes_end_to_end_spawn(self, tmp_path, capsys):
        """Two real spawn children against a real server; merged export is
        the authoritative record (successful == total requests)."""
        server = _MinimalHttpServer()
        server.start()
        export_path = tmp_path / "mp.json"

        rc = cli.main([
            "benchmark", "--url", server.url(), "--model", "m",
            "--requests", "6", "--processes", "2",
            "--export", str(export_path),
        ])

        assert rc == 0
        data = json.loads(export_path.read_text())
        assert data["successful"] == 6
        assert data["total_requests"] == 6
        assert data["failed"] == 0
        assert data["config"]["processes"] == 2
        assert data["window"] > 0

    def test_processes_single_is_control(self, tmp_path):
        """Single-process control for the same workload (no spawn)."""
        server = _MinimalHttpServer()
        server.start()
        export_path = tmp_path / "single.json"

        rc = cli.main([
            "benchmark", "--url", server.url(), "--model", "m",
            "--requests", "6", "--processes", "1",
            "--export", str(export_path),
        ])

        assert rc == 0
        data = json.loads(export_path.read_text())
        assert data["successful"] == 6
        assert data["config"]["processes"] == 1


class TestProfileProcessesFlag:
    def test_profile_help_mentions_processes(self, capsys):
        """profile shares the --processes flag with benchmark (both drive
        _run_benchmark_level)."""
        with pytest.raises(SystemExit) as exc_info:
            cli.main(["profile", "--help"])
        assert exc_info.value.code == 0
        assert "--processes" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# ChildPool: reusable spawn pool (profile trial reuse, 方案 A batch 2)
# ---------------------------------------------------------------------------

import queue as _queue
import time as _time

from lite_server.benchmark.multiproc import ChildPool


def _delayed_entry(payload):
    """Entry that sleeps ``payload["delay"]``s and returns its marker."""
    if payload.get("delay", 0) > 0:
        _time.sleep(payload["delay"])
    return payload["marker"]


def _stuck_entry(payload):
    _time.sleep(60)
    return "never"


def _pid_entry(payload):
    import os
    return {"pid": os.getpid(), "tag": payload}


class TestChildPool:
    def test_reuses_workers_across_runs(self):
        """Second run uses the same live processes — no re-spawn."""
        pool = ChildPool(_pid_entry, 2)
        try:
            first = pool.run([("run-1", {})], timeout_secs=30)
            pids = [o.result["pid"] for o in first]
            assert all(o.result["tag"] == "run-1" for o in first)
            assert len(pids) == 1
            assert all(p.is_alive() for p in pool.procs)

            second = pool.run([("run-2", {})], timeout_secs=30)
            assert all(o.result["tag"] == "run-2" for o in second)
            assert [o.result["pid"] for o in second] == pids  # same processes
            assert all(p.is_alive() for p in pool.procs)
        finally:
            pool.close()

    def test_outcomes_ordered_by_dispatch_not_completion(self):
        """Slow first task completes last; outcomes keep dispatch order."""
        pool = ChildPool(_delayed_entry, 2)
        try:
            outcomes = pool.run(
                [
                    ({"delay": 1.0, "marker": "slow"}, {}),
                    ({"delay": 0.0, "marker": "fast"}, {}),
                ],
                timeout_secs=30,
            )
            assert [o.result for o in outcomes] == ["slow", "fast"]
            assert all(o.crash is None for o in outcomes)
        finally:
            pool.close()

def _boom_entry(payload):
    raise RuntimeError(f"boom {payload}")


class TestChildPool:
    def test_task_exception_keeps_pool_alive(self):
        """A crashing task reports a crash outcome; later tasks still run."""
        pool = ChildPool(_boom_entry, 2)
        try:
            outcomes = pool.run([("one", {}), ("two", {})], timeout_secs=30)
            assert all(o.result is None for o in outcomes)
            assert all("boom" in o.crash for o in outcomes)
            # A different entry function still works with a fresh pool.
            pool2 = ChildPool(_pid_entry, 2)
            try:
                ok = pool2.run([("x", {})], timeout_secs=30)
                assert ok[0].result is not None
            finally:
                pool2.close()
        finally:
            pool.close()

    def test_task_timeout_closes_pool_and_reports_crash(self):
        pool = ChildPool(_stuck_entry, 1)
        outcomes = pool.run([("x", {})], timeout_secs=2)
        assert outcomes[0].result is None
        assert "timed out" in outcomes[0].crash
        assert pool._closed
        with pytest.raises(RuntimeError, match="closed"):
            pool.run([("y", {})], timeout_secs=2)

    def test_close_is_idempotent(self):
        pool = ChildPool(_pid_entry, 1)
        pool.close()
        pool.close()  # second close is a no-op
        assert pool._closed

    def test_run_after_close_raises(self):
        pool = ChildPool(_pid_entry, 1)
        pool.close()
        with pytest.raises(RuntimeError, match="closed"):
            pool.run([("x", {})], timeout_secs=2)


class TestChildPoolWithChildEntry:
    def test_pool_reuses_processes_across_benchmark_levels(self):
        """Profile-style reuse: two benchmark levels on the same pool —
        same worker processes, both runs correct."""
        server = _MinimalHttpServer()
        server.start()
        args = _benchmark_args(requests=3)
        spec = (
            args,
            dict(concurrency=1, url=server.url(), payloads=[{"input": 1.0}],
                 pacing=None, duration=None, goodput_slo=None,
                 trust_env=True, child_requests=3),
        )
        pool = ChildPool(cli._benchmark_child_process, 2)
        try:
            first = pool.run([spec], timeout_secs=60)
            assert first[0].result.total_requests == 3
            assert first[0].result.successful == 3, (
                f"failed={first[0].result.error_kinds} "
                f"errors={first[0].result.errors[:2]}")
            pids = [p.pid for p in pool.procs]

            second = pool.run([spec], timeout_secs=60)
            assert second[0].result.total_requests == 3
            assert second[0].result.successful == 3
            assert [p.pid for p in pool.procs] == pids  # reused, not re-spawned
        finally:
            pool.close()
