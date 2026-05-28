"""Benchmark engine for lite-server analyzer."""

from __future__ import annotations

import asyncio
import statistics
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""

    total_requests: int = 0
    successful: int = 0
    failed: int = 0
    latencies: list[float] = field(default_factory=list)
    duration: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def throughput(self) -> float:
        return self.total_requests / self.duration if self.duration > 0 else 0.0

    @property
    def mean_latency(self) -> float:
        return statistics.mean(self.latencies) if self.latencies else 0.0

    @property
    def p50(self) -> float:
        return self._percentile(0.5)

    @property
    def p90(self) -> float:
        return self._percentile(0.9)

    @property
    def p99(self) -> float:
        return self._percentile(0.99)

    @property
    def min_latency(self) -> float:
        return min(self.latencies) if self.latencies else 0.0

    @property
    def max_latency(self) -> float:
        return max(self.latencies) if self.latencies else 0.0

    def _percentile(self, p: float) -> float:
        if not self.latencies:
            return 0.0
        sorted_lat = sorted(self.latencies)
        idx = int(len(sorted_lat) * p)
        idx = min(idx, len(sorted_lat) - 1)
        return sorted_lat[idx]

    def to_dict(self) -> dict:
        return {
            "total_requests": self.total_requests,
            "successful": self.successful,
            "failed": self.failed,
            "throughput": round(self.throughput, 2),
            "duration": round(self.duration, 3),
            "latency_ms": {
                "mean": round(self.mean_latency, 2),
                "p50": round(self.p50, 2),
                "p90": round(self.p90, 2),
                "p99": round(self.p99, 2),
                "min": round(self.min_latency, 2),
                "max": round(self.max_latency, 2),
            },
            "errors": self.errors[:5],  # cap error list
        }


class BenchmarkEngine:
    """Run async benchmarks against an inference target."""

    async def run(
        self,
        target: Callable[..., Awaitable[dict]],
        payload: dict,
        concurrency: int = 1,
        duration: float | None = None,
        total_requests: int | None = None,
        warmup_requests: int = 0,
    ) -> BenchmarkResult:
        """Run benchmark and return results.

        Args:
            target: Async callable that sends one inference request.
            payload: Request payload dict.
            concurrency: Number of concurrent requesters.
            duration: Run for N seconds (fixed-duration mode).
            total_requests: Run exactly N requests (fixed-count mode).
            warmup_requests: Number of warmup requests before measurement.

        Either ``duration`` or ``total_requests`` must be provided.
        """
        if duration is None and total_requests is None:
            raise ValueError("Either duration or total_requests must be provided")

        result = BenchmarkResult()

        # Warmup
        for _ in range(warmup_requests):
            try:
                await target(payload)
            except Exception:
                pass

        start_time = time.perf_counter()

        if total_requests is not None:
            result = await self._run_fixed_count(
                target, payload, concurrency, total_requests
            )
        else:
            result = await self._run_fixed_duration(
                target, payload, concurrency, duration or 0.0
            )

        result.duration = time.perf_counter() - start_time
        return result

    async def _run_fixed_duration(
        self,
        target: Callable[..., Awaitable[dict]],
        payload: dict,
        concurrency: int,
        duration: float,
    ) -> BenchmarkResult:
        result = BenchmarkResult()
        end_time = time.perf_counter() + duration
        semaphore = asyncio.Semaphore(concurrency)

        async def _send() -> None:
            async with semaphore:
                if time.perf_counter() >= end_time:
                    return
                t0 = time.perf_counter()
                try:
                    await target(payload)
                    t1 = time.perf_counter()
                    result.latencies.append((t1 - t0) * 1000)
                    result.successful += 1
                except Exception as e:
                    result.failed += 1
                    if len(result.errors) < 5:
                        result.errors.append(str(e))
                result.total_requests += 1

        tasks = []
        while time.perf_counter() < end_time:
            tasks.append(asyncio.create_task(_send()))
            # Throttle task creation to avoid unbounded growth
            if len(tasks) >= concurrency * 4:
                await asyncio.gather(*tasks[:concurrency])
                tasks = tasks[concurrency:]

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        return result

    async def _run_fixed_count(
        self,
        target: Callable[..., Awaitable[dict]],
        payload: dict,
        concurrency: int,
        total_requests: int,
    ) -> BenchmarkResult:
        result = BenchmarkResult()
        counter = 0
        semaphore = asyncio.Semaphore(concurrency)

        async def _send() -> None:
            nonlocal counter
            async with semaphore:
                if counter >= total_requests:
                    return
                counter += 1
                t0 = time.perf_counter()
                try:
                    await target(payload)
                    t1 = time.perf_counter()
                    result.latencies.append((t1 - t0) * 1000)
                    result.successful += 1
                except Exception as e:
                    result.failed += 1
                    if len(result.errors) < 5:
                        result.errors.append(str(e))
                result.total_requests += 1

        tasks = [asyncio.create_task(_send()) for _ in range(total_requests)]
        await asyncio.gather(*tasks, return_exceptions=True)
        return result
