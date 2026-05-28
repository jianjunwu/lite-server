"""Tests for lite_server.analyzer.benchmark — benchmark engine."""

import asyncio
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
        )
        assert r.throughput == 10.0
        assert r.mean_latency == 20.0
        assert r.p50 == 20.0
        assert r.p90 == 30.0

    def test_percentiles_empty(self):
        r = BenchmarkResult()
        assert r.p50 == 0.0
        assert r.p99 == 0.0


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
