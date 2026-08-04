"""Tests for lite_server.analyzer.benchmark — benchmark engine."""

import asyncio
import time
from pathlib import Path

import pytest

from lite_server.analyzer.benchmark import BenchmarkEngine, BenchmarkResult


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
        from lite_server.analyzer.benchmark import (
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
        from lite_server.analyzer.benchmark import BenchmarkEngine as E

        assert E._cpu_saturation_warning(cpu_used=0.8, wall=1.0) is not None
        assert E._cpu_saturation_warning(cpu_used=0.5, wall=1.0) is None
        assert E._cpu_saturation_warning(cpu_used=0.8, wall=0.0) is None
