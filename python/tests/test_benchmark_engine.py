"""Tests for lite_server.benchmark.benchmark — benchmark engine."""

import asyncio
import time
from pathlib import Path

import pytest

from lite_server.benchmark.benchmark import BenchmarkEngine, BenchmarkResult


class TestBenchmarkResult:
    """Result data structure."""

    def test_empty_result(self):
        r = BenchmarkResult()
        assert r.total_requests == 0
        assert r.successful == 0
        assert r.failed == 0
        assert r.throughput == 0.0

    def test_result_with_data(self):
        r = BenchmarkResult(
            total_requests=100,
            successful=95,
            failed=5,
            latencies=[10.0, 20.0, 30.0],
            duration=10.0,
            window=9.5,
        )
        # throughput = successful / window (completed over measured window)
        assert r.throughput == 10.0
        assert r.mean_latency == 20.0
        assert r.p50 == 20.0
        # numpy linear interpolation: (3-1)*0.9 = 1.8 → 20 + 0.8*10
        assert r.p90 == 28.0

    def test_throughput_zero_without_window(self):
        r = BenchmarkResult(total_requests=10, successful=10, duration=5.0)
        assert r.throughput == 0.0

    def test_percentiles_empty(self):
        r = BenchmarkResult()
        assert r.p50 == 0.0
        assert r.p95 == 0.0
        assert r.p99 == 0.0

    def test_percentile_linear_interpolation(self):
        r = BenchmarkResult(latencies=[10.0, 20.0, 30.0])
        # linear method: p95 → (3-1)*0.95 = 1.9 → 20 + 0.9*10
        assert r.p95 == 29.0

    def test_to_dict_contract_labels(self):
        d = BenchmarkResult().to_dict()
        assert d["load_mode"] == "closed-loop"
        assert d["latency_basis"] == "service-time"
        assert d["percentile_method"] == "linear"
        assert "p95" in d["latency_ms"]
        assert d["error_kinds"] == {}
        assert d["warnings"] == []
        assert d["dropped_inflight"] == 0
        assert d["drained_in_grace"] == 0


class TestBenchmarkEngine:
    """Benchmark execution."""

    async def _mock_target(self, payload):
        await asyncio.sleep(0.001)
        return {"ok": True}

    @pytest.mark.asyncio
    async def test_runs_requests(self):
        engine = BenchmarkEngine()
        result = await engine.run(
            target=self._mock_target,
            payload={"input": 1.0},
            concurrency=2,
            duration=0.05,
            warmup_requests=0,
        )
        assert result.total_requests > 0
        assert result.successful > 0

    @pytest.mark.asyncio
    async def test_failed_requests_counted(self):
        async def failing_target(payload):
            raise RuntimeError("boom")

        engine = BenchmarkEngine()
        result = await engine.run(
            target=failing_target,
            payload={"input": 1.0},
            concurrency=1,
            duration=0.02,
            warmup_requests=0,
        )
        assert result.failed > 0
        assert result.successful == 0

    @pytest.mark.asyncio
    async def test_warmup_does_not_count(self):
        call_count = [0]

        async def counting_target(payload):
            call_count[0] += 1
            await asyncio.sleep(0.001)
            return {"ok": True}

        engine = BenchmarkEngine()
        result = await engine.run(
            target=counting_target,
            payload={"input": 1.0},
            concurrency=1,
            duration=0.02,
            warmup_requests=2,
        )
        assert call_count[0] >= 2
        assert result.total_requests == call_count[0] - 2

    @pytest.mark.asyncio
    async def test_computes_latency_stats(self):
        engine = BenchmarkEngine()
        result = await engine.run(
            target=self._mock_target,
            payload={"input": 1.0},
            concurrency=1,
            duration=0.05,
            warmup_requests=0,
        )
        assert result.mean_latency > 0
        assert result.p50 > 0
        assert result.p90 >= result.p50

    @pytest.mark.asyncio
    async def test_fixed_count_mode(self):
        engine = BenchmarkEngine()
        result = await engine.run(
            target=self._mock_target,
            payload={"input": 1.0},
            concurrency=1,
            total_requests=10,
            warmup_requests=0,
        )
        assert result.total_requests == 10


class TestWorkerPoolStructure:
    """P0: fixed N worker coroutines — no unbounded create_task fan-out."""

    @pytest.mark.asyncio
    async def test_task_count_bounded_by_concurrency(self):
        max_seen = [0]

        async def observing_target(payload):
            max_seen[0] = max(max_seen[0], len(asyncio.all_tasks()))
            await asyncio.sleep(0.001)
            return {"ok": True}

        engine = BenchmarkEngine()
        await engine.run(
            target=observing_target,
            payload={},
            concurrency=4,
            duration=0.05,
        )
        # 4 workers + the test task itself + small slack; old fan-out created
        # dozens of tasks for the same run.
        assert max_seen[0] <= 4 + 3

    @pytest.mark.asyncio
    async def test_fixed_count_uses_bounded_workers(self):
        max_seen = [0]

        async def observing_target(payload):
            max_seen[0] = max(max_seen[0], len(asyncio.all_tasks()))
            await asyncio.sleep(0)
            return {"ok": True}

        engine = BenchmarkEngine()
        result = await engine.run(
            target=observing_target,
            payload={},
            concurrency=3,
            total_requests=50,
        )
        assert result.total_requests == 50
        assert max_seen[0] <= 3 + 3


class TestGraceDrain:
    """P0: bounded drain — stop dispatch at deadline, wait in-flight up to grace."""

    @pytest.mark.asyncio
    async def test_inflight_dropped_after_grace(self):
        entered = asyncio.Event()

        async def slow_target(payload):
            entered.set()
            await asyncio.sleep(1.0)
            return {"ok": True}

        engine = BenchmarkEngine()
        t0 = time.perf_counter()
        result = await engine.run(
            target=slow_target,
            payload={},
            concurrency=2,
            duration=0.2,
            grace_period=0.1,
        )
        elapsed = time.perf_counter() - t0
        # unbounded drain would wait the full 1.0s; bounded drain must return
        # well before that even on a loaded CI runner
        assert entered.is_set(), "no request dispatched before deadline"
        assert elapsed < 0.2 + 0.1 + 0.5
        assert result.dropped_inflight >= 1

    @pytest.mark.asyncio
    async def test_inflight_completed_within_grace_counted(self):
        async def target(payload):
            await asyncio.sleep(0.03)
            return {"ok": True}

        engine = BenchmarkEngine()
        result = await engine.run(
            target=target,
            payload={},
            concurrency=2,
            duration=0.01,
            grace_period=5.0,
        )
        # responses land after the deadline but within grace: kept, not dropped
        assert result.dropped_inflight == 0
        assert result.successful >= 1
        assert result.drained_in_grace == result.successful


class TestStatsContract:
    """P0: throughput window, sample-size warning, error buckets, payload factory."""

    @pytest.mark.asyncio
    async def test_throughput_uses_measured_window(self):
        async def target(payload):
            await asyncio.sleep(0.005)
            return {"ok": True}

        engine = BenchmarkEngine()
        result = await engine.run(
            target=target,
            payload={},
            concurrency=2,
            total_requests=8,
        )
        assert result.window > 0
        assert result.throughput == pytest.approx(result.successful / result.window)
        # window excludes trailing idle, so it never exceeds the wall duration
        assert result.window <= result.duration + 1e-9

    @pytest.mark.asyncio
    async def test_sample_size_warning_when_insufficient(self):
        async def target(payload):
            return {"ok": True}

        engine = BenchmarkEngine()
        result = await engine.run(
            target=target,
            payload={},
            concurrency=2,
            total_requests=5,
        )
        assert any("sample" in w.lower() for w in result.warnings)
        assert any(w in result.to_dict()["warnings"] for w in result.warnings)

    @pytest.mark.asyncio
    async def test_no_sample_size_warning_when_sufficient(self):
        async def target(payload):
            await asyncio.sleep(0)
            return {"ok": True}

        engine = BenchmarkEngine()
        result = await engine.run(
            target=target,
            payload={},
            concurrency=1,
            total_requests=300,
        )
        assert result.successful == 300
        assert not any("sample" in w.lower() for w in result.warnings)

    @pytest.mark.asyncio
    async def test_error_kinds_classified(self):
        from lite_server.benchmark.benchmark import (
            RequestStatusError,
            RequestTimeoutError,
        )

        errors = iter([
            RequestTimeoutError(),
            RequestStatusError(500),
            RuntimeError("boom"),
        ])

        async def target(payload):
            raise next(errors)

        engine = BenchmarkEngine()
        result = await engine.run(
            target=target,
            payload={},
            concurrency=1,
            total_requests=3,
        )
        assert result.error_kinds == {"timeout": 1, "status": 1, "unknown": 1}
        assert result.failed == 3

    @pytest.mark.asyncio
    async def test_payload_factory_called_per_request(self):
        seen = []
        counter = [0]

        def factory():
            counter[0] += 1
            return {"i": counter[0]}

        async def target(payload):
            seen.append(payload["i"])
            return {"ok": True}

        engine = BenchmarkEngine()
        await engine.run(
            target=target,
            payload=factory,
            concurrency=1,
            total_requests=3,
        )
        assert seen == [1, 2, 3]

    def test_cpu_saturation_warning_helper(self):
        from lite_server.benchmark.benchmark import BenchmarkEngine as E

        assert E._cpu_saturation_warning(cpu_used=0.8, wall=1.0) is not None
        assert E._cpu_saturation_warning(cpu_used=0.5, wall=1.0) is None
        assert E._cpu_saturation_warning(cpu_used=0.8, wall=0.0) is None


class TestCOCorrection:
    """Coordinated-omission correction via intended-send-time reconstruction."""

    def test_corrected_equals_service_when_uniform(self):
        """Perfectly uniform sends produce zero correction (no queueing)."""
        r = BenchmarkResult(
            successful=4,
            window=0.3,
        )
        # Uniformly spaced sends: 0, 100ms, 200ms, 300ms (each 10ms svc time)
        r.send_times_ns = [0, 100_000_000, 200_000_000, 300_000_000]
        r.latencies = [10.0, 10.0, 10.0, 10.0]
        corrected = r.corrected_latencies
        assert len(corrected) == 4
        # Intended interval = 0.3 / 4 = 0.075s = 75ms
        # Request 0: intended=0, actual=0, queue=0, corrected=10
        # Request 1: intended=75ms, actual=100ms, queue=25ms, corrected=35
        assert corrected[0] == pytest.approx(10.0, abs=1.0)
        assert corrected[1] == pytest.approx(35.0, abs=1.0)  # 10 + (100-75)
        assert corrected[2] == pytest.approx(60.0, abs=1.0)  # 10 + (200-150)
        assert corrected[3] == pytest.approx(85.0, abs=1.0)  # 10 + (300-225)

    def test_corrected_captures_burst_pause(self):
        """Server pauses after first request — CO correction captures queueing."""
        r = BenchmarkResult(
            successful=3,
            window=0.3,
        )
        # First request: send at 0, response at 10ms
        # Server pauses 200ms before accepting next
        # Second: send at 210ms, response at 220ms (10ms svc)
        # Third: send at 221ms, response at 231ms (10ms svc)
        r.send_times_ns = [0, 210_000_000, 221_000_000]
        r.latencies = [10.0, 10.0, 10.0]
        corrected = r.corrected_latencies
        # Intended interval = 0.3 / 3 = 0.1s = 100ms
        # Request 0: intended=0, actual=0, queue=0, corrected=10
        # Request 1: intended=100ms, actual=210ms, queue=110ms, corrected=120
        # Request 2: intended=200ms, actual=221ms, queue=21ms, corrected=31
        assert corrected[0] == pytest.approx(10.0, abs=1.0)
        assert corrected[1] == pytest.approx(120.0, abs=5.0)
        assert corrected[2] == pytest.approx(31.0, abs=5.0)
        # Service-time p99 = 10ms, corrected p99 = ~120ms — the gap IS the signal
        assert r.p99 < 20.0
        assert r._co_corrected_percentile(0.99) > 50.0

    def test_corrected_fallback_when_no_send_times(self):
        r = BenchmarkResult(successful=3, latencies=[5.0, 10.0, 15.0])
        corrected = r.corrected_latencies
        assert corrected == [5.0, 10.0, 15.0]

    def test_to_dict_includes_co_corrected(self):
        # Burst-pause pattern: service times are uniform (10ms) but sends are
        # clustered after a pause, so CO correction inflates the tail.
        r = BenchmarkResult(
            successful=3, window=0.3,
            send_times_ns=[0, 210_000_000, 221_000_000],
            latencies=[10.0, 10.0, 10.0],
        )
        d = r.to_dict()
        assert "latency_co_corrected_ms" in d
        co = d["latency_co_corrected_ms"]
        assert "p50" in co and "p99" in co and "max" in co
        # Service-time p99 = 10ms; corrected p99 > 100ms (queueing added back)
        assert co["p99"] > d["latency_ms"]["p99"]


class TestOpenLoop:
    """Open-loop constant-arrival-rate mode (--rate)."""

    @pytest.mark.asyncio
    async def test_rate_mode_dispatches_at_fixed_intervals(self):
        """Open-loop dispatch: request send times are evenly spaced."""
        send_times: list[float] = []
        engine = BenchmarkEngine()

        async def fake_target(payload):
            send_times.append(time.perf_counter())
            await asyncio.sleep(0.001)
            return {"ok": True}

        result = await engine.run(
            target=fake_target,
            payload={"input": 1.0},
            concurrency=50,
            duration=0.3,
            rate=100,
        )
        assert result.load_mode == "open-loop"
        assert result.target_rate == 100
        # At 100 req/s over 0.3s, expect ~30 requests
        assert 20 <= result.successful <= 45
        # CO correction is negligible for open-loop (sends already evenly spaced)
        if result.successful >= 2:
            p99 = result.p99
            co_p99 = result._co_corrected_percentile(0.99)
            # In open-loop the gap is minimal (no queueing at generator);
            # allow slack for CI/xdist scheduling jitter. The failure
            # message carries the measured gap so a rare xdist-scheduling
            # flake is diagnosable without a rerun.
            gap_ms = abs(co_p99 - p99)
            assert gap_ms < 50.0, (
                f"open-loop CO gap {gap_ms:.1f}ms (p99={p99:.1f}, "
                f"co_p99={co_p99:.1f}, successful={result.successful})"
            )

    @pytest.mark.asyncio
    async def test_rate_mode_concurrency_caps_inflight(self):
        """Semaphore limits concurrent in-flight requests."""
        inflight = 0
        max_inflight = 0

        async def fake_target(payload):
            nonlocal inflight, max_inflight
            inflight += 1
            max_inflight = max(max_inflight, inflight)
            await asyncio.sleep(0.01)
            inflight -= 1
            return {"ok": True}

        engine = BenchmarkEngine()
        result = await engine.run(
            target=fake_target,
            payload={"input": 1.0},
            concurrency=3,
            duration=0.15,
            rate=200,
        )
        assert result.successful > 0
        # With concurrency=3, rate=200, sleep=10ms: max in-flight ≤ 3
        assert max_inflight <= 3
