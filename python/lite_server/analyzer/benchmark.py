"""Benchmark engine for lite-server analyzer.

Closed-loop load generation: a fixed pool of N worker coroutines sends
requests serially (k6 constant-vus / perf_analyzer concurrency mode).
Reported latencies are service-time only — queueing at the load generator
is not measured (coordinated omission is inherent to closed-loop); the
output contract labels this explicitly via ``load_mode``/``latency_basis``.

Streaming (PR-4): ``run_stream()`` wraps a streaming target (async iterator
over ``StreamChunk``) as a unary callable, delegates to the unmodified
``run()``, and attaches ``StreamMetrics`` to the result.  Model-specific
metric computation lives in ``stream_metrics.py``.
"""

from __future__ import annotations

import asyncio
import os
import statistics
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Awaitable, Callable, Union

import numpy as np

# ── Streaming datatypes (PR-4) ───────────────────────────────────────────


@dataclass
class StreamChunk:
    """One chunk from a streaming response (SSE event, WS frame, etc.)."""

    data: Any = None
    meta: dict | None = None  # {"token_count": 3} or {"audio_duration_ms": 320}
    size_bytes: int | None = None  # falls back to len(data) for bytes/str


@dataclass
class StreamRequestRecord:
    """Per-request raw stream measurements (one per successful stream)."""

    chunk_count: int = 0
    total_bytes: int = 0
    ttft_ms: float | None = None  # first chunk offset from send time
    total_ms: float | None = None  # last chunk offset (== e2e latency)
    inter_chunk_ms: list[float] = field(default_factory=list)
    chunk_metas: list[dict] = field(default_factory=list)
    meta_totals: dict[str, float] = field(default_factory=dict)
    request_meta: dict | None = None  # STT input audio_duration_ms


@dataclass
class StreamMetrics:
    """Aggregated streaming metrics for one benchmark run.

    ``model_type`` is stored on the dataclass; ``to_dict()`` emits it as
    ``"mode"`` (§1.12.4).
    """

    model_type: str
    requests: int
    zero_chunk_requests: int
    total_chunks: int
    total_bytes: int
    chunks_per_request: dict  # mean/p50/p90/p95/p99/min/max
    ttft_ms: dict  # same percentile shape
    total_ms: dict  # e2e stream latency

    # LLM-specific (None for TTS/STT/generic)
    itl_ms: dict | None = None  # pooled inter-chunk latency
    tpot_ms: dict | None = None  # per-request TPOT percentiles (R2)
    tokens_per_sec: float | None = None  # decode-phase throughput
    tokens_per_sec_e2e: float | None = None  # incl. TTFT
    tokens_per_sec_aggregate: float | None = None  # total_tokens / window (R2)
    tokens_per_request: dict | None = None  # per-request token counts
    token_count_basis: str | None = None  # "exact" | "estimated" | "mixed"

    # TTS/STT-specific (None for LLM/generic)
    rtf: dict | None = None  # real-time factor percentiles

    def to_dict(self) -> dict:
        d: dict = {
            "mode": self.model_type,
            "requests": self.requests,
            "zero_chunk_requests": self.zero_chunk_requests,
            "total_chunks": self.total_chunks,
            "total_bytes": self.total_bytes,
            "chunks_per_request": _round_percentiles(self.chunks_per_request),
            "ttft_ms": _round_percentiles(self.ttft_ms),
            "total_ms": _round_percentiles(self.total_ms),
        }
        # LLM section
        if self.model_type == "llm":
            if self.itl_ms is not None:
                d["itl_ms"] = _round_percentiles(self.itl_ms)
            if self.tpot_ms is not None:
                d["tpot_ms"] = _round_percentiles(self.tpot_ms)
            if self.tokens_per_sec is not None:
                d["tokens_per_sec"] = round(self.tokens_per_sec, 2)
            if self.tokens_per_sec_e2e is not None:
                d["tokens_per_sec_e2e"] = round(self.tokens_per_sec_e2e, 2)
            if self.tokens_per_sec_aggregate is not None:
                d["tokens_per_sec_aggregate"] = round(self.tokens_per_sec_aggregate, 2)
            if self.tokens_per_request is not None:
                d["tokens_per_request"] = _round_percentiles(self.tokens_per_request)
            if self.token_count_basis is not None:
                d["token_count_basis"] = self.token_count_basis
        # TTS / STT section
        if self.model_type in ("tts", "stt") and self.rtf is not None:
            d["rtf"] = _round_percentiles(self.rtf)
        return d


def _round_percentiles(p: dict) -> dict:
    """Round all values in a percentile dict to 2 decimal places."""
    return {k: round(v, 2) for k, v in p.items()}


Payload = Union[dict, Callable[[], dict]]

#: Below this many completed requests, percentile conclusions (esp. p99) are
#: unreliable: max(300, 10 * concurrency). 300 = AIPerf SLA "coarse" tier;
#: 10x concurrency = rule of thumb for reaching steady state per slot.
MIN_SAMPLES_BASE = 300


class RequestError(Exception):
    """Base for classified request failures (see ``error_kinds``)."""

    kind = "transport"


class RequestTimeoutError(RequestError):
    kind = "timeout"


class RequestConnectError(RequestError):
    kind = "connect"


class RequestTransportError(RequestError):
    kind = "transport"


class RequestStatusError(RequestError):
    kind = "status"

    def __init__(self, status_code: int):
        super().__init__(f"HTTP status {status_code}")
        self.status_code = status_code


class RequestStreamError(RequestError):
    """SSE ``{"error": "..."}`` event or stream-level protocol error."""

    kind = "stream"


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""

    total_requests: int = 0
    successful: int = 0
    failed: int = 0
    latencies: list[float] = field(default_factory=list)
    #: Per-request send timestamps (perf_counter_ns), stored alongside
    #: latencies so coordinated-omission correction can be computed post-hoc.
    send_times_ns: list[int] = field(default_factory=list)
    duration: float = 0.0
    errors: list[str] = field(default_factory=list)
    # Measured window in seconds: first request sent -> last successful
    # response received. Throughput is computed over this window so trailing
    # idle time and failed fast-paths do not distort the rate.
    window: float = 0.0
    warmup_requests: int = 0
    drained_in_grace: int = 0
    dropped_inflight: int = 0
    error_kinds: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    load_mode: str = "closed-loop"
    target_rate: float | None = None
    stream_metrics: StreamMetrics | None = None

    @property
    def throughput(self) -> float:
        return self.successful / self.window if self.window > 0 else 0.0

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
    def p95(self) -> float:
        return self._percentile(0.95)

    @property
    def p99(self) -> float:
        return self._percentile(0.99)

    @property
    def min_latency(self) -> float:
        return min(self.latencies) if self.latencies else 0.0

    @property
    def max_latency(self) -> float:
        return max(self.latencies) if self.latencies else 0.0

    @property
    def corrected_latencies(self) -> list[float]:
        """CO-corrected latencies (ms) using intended-send-time correction.

        Reconstructs a notional open-loop schedule: requests are evenly
        spaced across the measured window.  Queueing delay at the load
        generator (coordinated omission) is added back so the corrected
        latency approximates what an open-loop client would observe.

        Returns a copy of ``latencies`` when there are < 2 samples or
        no send timestamps are stored.
        """
        if len(self.latencies) < 2 or len(self.send_times_ns) != len(self.latencies):
            return list(self.latencies)
        pairs = sorted(zip(self.send_times_ns, self.latencies))
        first_send = pairs[0][0]
        interval = self.window / len(pairs) if self.window > 0 else 0.0
        if interval <= 0:
            return list(self.latencies)
        corrected: list[float] = []
        for i, (actual_send_ns, svc_ms) in enumerate(pairs):
            intended_send_ns = first_send + int(i * interval * 1e9)
            # Queueing delay = how much later than intended this request was
            # actually sent.  Add it to the service-time latency.
            queue_delay_ms = max(0.0, (actual_send_ns - intended_send_ns) / 1e6)
            corrected.append(svc_ms + queue_delay_ms)
        return corrected

    def _co_corrected_percentile(self, p: float) -> float:
        corrected = self.corrected_latencies
        if not corrected:
            return 0.0
        return float(np.percentile(corrected, p * 100, method="linear"))

    def _percentile(self, p: float) -> float:
        if not self.latencies:
            return 0.0
        return float(np.percentile(self.latencies, p * 100, method="linear"))

    def to_dict(self) -> dict:
        achieved_rate = self.total_requests / self.window if self.window > 0 else 0.0
        d: dict = {
            "load_mode": self.load_mode,
            "latency_basis": "service-time",
            "percentile_method": "linear",
            "total_requests": self.total_requests,
            "successful": self.successful,
            "failed": self.failed,
            "error_kinds": dict(self.error_kinds),
            "throughput": round(self.throughput, 2),
            "achieved_rate": round(achieved_rate, 2),
            "duration": round(self.duration, 3),
            "window": round(self.window, 3),
            "warmup_requests": self.warmup_requests,
            "drained_in_grace": self.drained_in_grace,
            "dropped_inflight": self.dropped_inflight,
            "latency_ms": {
                "mean": round(self.mean_latency, 2),
                "stddev": round(statistics.stdev(self.latencies) if len(self.latencies) >= 2 else 0.0, 2),
                "p50": round(self.p50, 2),
                "p90": round(self.p90, 2),
                "p95": round(self.p95, 2),
                "p99": round(self.p99, 2),
                "min": round(self.min_latency, 2),
                "max": round(self.max_latency, 2),
            },
            "latency_co_corrected_ms": {
                "p50": round(self._co_corrected_percentile(0.50), 2),
                "p90": round(self._co_corrected_percentile(0.90), 2),
                "p95": round(self._co_corrected_percentile(0.95), 2),
                "p99": round(self._co_corrected_percentile(0.99), 2),
                "max": round(max(self.corrected_latencies) if self.corrected_latencies else 0.0, 2),
            },
            "warnings": list(self.warnings),
            "errors": self.errors,
            "stream": self.stream_metrics.to_dict() if self.stream_metrics else None,
        }
        if self.target_rate is not None:
            d["target_rate"] = self.target_rate
        return d


class BenchmarkEngine:
    """Run async benchmarks against an inference target."""

    async def run(
        self,
        target: Callable[[dict], Awaitable[dict]],
        payload: Payload,
        concurrency: int = 1,
        duration: float | None = None,
        total_requests: int | None = None,
        warmup_requests: int = 0,
        grace_period: float = 30.0,
        rate: float | None = None,
    ) -> BenchmarkResult:
        """Run benchmark and return results.

        Args:
            target: Async callable that sends one inference request.
            payload: Request payload dict, or a zero-arg callable returning a
                fresh payload per request (e.g. round-robin over files).
            concurrency: For closed-loop: number of worker coroutines.  For
                open-loop (``rate`` set): max in-flight requests via semaphore.
            duration: Run for N seconds (fixed-duration mode).
            total_requests: Run exactly N requests (fixed-count mode).
            warmup_requests: Number of warmup requests before measurement;
                their samples are discarded.
            grace_period: After the deadline, stop dispatching and wait at
                most this many seconds for in-flight requests (drain).
            rate: Constant arrival rate in req/s (open-loop).  When set,
                requests are dispatched on a fixed-interval schedule
                independent of response times, eliminating coordinated
                omission at the load generator.

        Either ``duration`` or ``total_requests`` must be provided.
        """
        if duration is None and total_requests is None:
            raise ValueError("Either duration or total_requests must be provided")
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {concurrency}")
        if rate is not None and rate <= 0:
            raise ValueError(f"rate must be > 0, got {rate}")

        payload_factory = payload if callable(payload) else (lambda: payload)

        cpu0 = os.times()
        wall0 = time.perf_counter()

        # Warmup — samples discarded, not measured.
        for _ in range(warmup_requests):
            try:
                await target(payload_factory())
            except Exception:
                pass

        start_time = time.perf_counter()

        if rate is not None:
            result = await self._run_open_loop(
                target, payload_factory, concurrency, rate,
                duration or 0.0, total_requests, grace_period,
            )
            result.load_mode = "open-loop"
            result.target_rate = rate
        elif total_requests is not None:
            result = await self._run_fixed_count(
                target, payload_factory, concurrency, total_requests
            )
        else:
            result = await self._run_fixed_duration(
                target, payload_factory, concurrency, duration or 0.0, grace_period
            )

        result.duration = time.perf_counter() - start_time
        result.warmup_requests = warmup_requests

        min_samples = max(MIN_SAMPLES_BASE, 10 * concurrency)
        if result.successful < min_samples:
            result.warnings.append(
                f"Sample size {result.successful} < {min_samples} "
                f"(max(300, 10*concurrency)); latency percentiles (esp. p99) "
                f"may be unreliable — increase duration/requests"
            )

        cpu1 = os.times()
        cpu_used = (cpu1.user + cpu1.system) - (cpu0.user + cpu0.system)
        warning = self._cpu_saturation_warning(
            cpu_used=cpu_used, wall=time.perf_counter() - wall0
        )
        if warning:
            result.warnings.append(warning)

        return result

    async def run_stream(
        self,
        target: Callable[[dict], AsyncIterator[StreamChunk]],
        payload: Payload,
        concurrency: int = 1,
        duration: float | None = None,
        total_requests: int | None = None,
        warmup_requests: int = 0,
        grace_period: float = 30.0,
        rate: float | None = None,
        model_type: str = "llm",
        request_meta: Callable[[dict], dict | None] | None = None,
    ) -> BenchmarkResult:
        """Run streaming benchmark — adapter over unmodified ``run()``.

        Wraps a streaming *target* (``async for chunk in target(payload)``)
        as a unary callable that fully consumes the stream, records raw
        ``StreamRequestRecord`` timings, and returns ``{"ok": True}``.
        The adapter is passed to the unmodified ``run()``, so warmup,
        worker pool, grace drain, window, sample-size/CPU warnings are
        all reused with zero changes.

        After ``run()`` returns, ``compute_stream_metrics`` is called to
        produce model-specific metrics and attached to ``result.stream_metrics``.
        """
        from lite_server.analyzer.stream_metrics import MODEL_TYPES, compute_stream_metrics

        if model_type not in MODEL_TYPES:
            raise ValueError(
                f"model_type must be one of {MODEL_TYPES}, got {model_type!r}"
            )

        payload_factory = payload if callable(payload) else (lambda: payload)

        records: list[StreamRequestRecord] = []
        warmup_left = warmup_requests

        async def adapter(p: dict) -> dict:
            nonlocal warmup_left
            # Warmup calls (run() discards their samples) must not leak
            # into stream metrics either — record only measured requests.
            is_warmup = warmup_left > 0
            if is_warmup:
                warmup_left -= 1

            t0 = time.perf_counter_ns()
            rec = StreamRequestRecord()
            if request_meta is not None:
                rec.request_meta = request_meta(p)

            prev_ts_ns: int | None = None
            chunk_idx = 0

            try:
                async for chunk in target(p):
                    now_ns = time.perf_counter_ns()

                    # Empty-chunk filtering (§1.12.1): chunks with no data
                    # are not counted toward chunk_count / TTFT / ITL, but
                    # their bytes still count and their meta still aggregates.
                    is_empty = chunk.data is None or (
                        isinstance(chunk.data, (str, bytes)) and len(chunk.data) == 0
                    )

                    if not is_empty:
                        chunk_idx += 1
                        if rec.ttft_ms is None:
                            rec.ttft_ms = (now_ns - t0) / 1e6
                        if prev_ts_ns is not None:
                            rec.inter_chunk_ms.append((now_ns - prev_ts_ns) / 1e6)
                        prev_ts_ns = now_ns

                    # Bytes: use explicit size or fall back to len(data)
                    if chunk.size_bytes is not None:
                        rec.total_bytes += chunk.size_bytes
                    elif isinstance(chunk.data, (str, bytes)):
                        rec.total_bytes += len(chunk.data)

                    # Meta aggregation
                    if chunk.meta is not None:
                        rec.chunk_metas.append(chunk.meta)
                        for mk, mv in chunk.meta.items():
                            if isinstance(mv, (int, float)):
                                rec.meta_totals[mk] = rec.meta_totals.get(mk, 0.0) + mv

            except asyncio.CancelledError:
                raise
            except Exception:
                raise

            rec.chunk_count = chunk_idx
            t1 = time.perf_counter_ns()
            if chunk_idx > 0:
                rec.total_ms = (t1 - t0) / 1e6
            if not is_warmup:
                records.append(rec)
            return {"ok": True}

        # Call unmodified run() — the adapter is a normal unary target
        result = await self.run(
            target=adapter,
            payload=payload_factory,
            concurrency=concurrency,
            duration=duration,
            total_requests=total_requests,
            warmup_requests=warmup_requests,
            grace_period=grace_period,
            rate=rate,
        )

        # Compute stream metrics and attach
        sm = compute_stream_metrics(records, model_type, window_secs=result.window or None)
        result.stream_metrics = sm

        if sm.zero_chunk_requests > 0:
            result.warnings.append(
                f"{sm.zero_chunk_requests}/{sm.requests} requests "
                f"received zero chunks — check model output format or "
                f"increase stream-read-timeout"
            )

        if sm.token_count_basis == "mixed":
            result.warnings.append(
                "Mixed token count basis: some requests reported "
                "token_count metadata, others fell back to chunk_count "
                "estimation — token metrics are partially estimated"
            )

        return result

    @staticmethod
    def _cpu_saturation_warning(cpu_used: float, wall: float) -> str | None:
        """Warn when the load generator itself nears single-core saturation."""
        if wall <= 0:
            return None
        if cpu_used / wall > 0.7:
            return (
                "Benchmark client CPU usage exceeded 70% of one core — the "
                "load generator may be the bottleneck and results may be "
                "distorted; cross-check with lower concurrency"
            )
        return None

    async def _send(
        self,
        target: Callable[[dict], Awaitable[dict]],
        payload_factory: Callable[[], dict],
        result: BenchmarkResult,
        state: dict,
        deadline_ns: int | None,
    ) -> None:
        """Send one request and record the outcome into ``result``/``state``."""
        payload = payload_factory()
        state["started"] += 1
        t0 = time.perf_counter_ns()
        if state["first_t0_ns"] is None:
            state["first_t0_ns"] = t0
        try:
            await target(payload)
        except asyncio.CancelledError:
            # In-flight at grace expiry: must propagate; NOT counted in
            # total_requests — dropped_inflight = started - total_requests.
            raise
        except Exception as e:
            kind = e.kind if isinstance(e, RequestError) else "unknown"
            result.error_kinds[kind] = result.error_kinds.get(kind, 0) + 1
            result.failed += 1
            result.errors.append(str(e))
            result.total_requests += 1
        else:
            t1 = time.perf_counter_ns()
            result.latencies.append((t1 - t0) / 1e6)
            result.send_times_ns.append(t0)
            result.successful += 1
            result.total_requests += 1
            state["last_t1_ns"] = t1
            if deadline_ns is not None and t1 > deadline_ns:
                result.drained_in_grace += 1

    def _finish_window(self, result: BenchmarkResult, state: dict) -> None:
        if state["first_t0_ns"] is not None and state["last_t1_ns"] is not None:
            result.window = (state["last_t1_ns"] - state["first_t0_ns"]) / 1e9

    async def _run_fixed_duration(
        self,
        target: Callable[[dict], Awaitable[dict]],
        payload_factory: Callable[[], dict],
        concurrency: int,
        duration: float,
        grace_period: float,
    ) -> BenchmarkResult:
        result = BenchmarkResult()
        state = {"started": 0, "first_t0_ns": None, "last_t1_ns": None}
        deadline_ns = time.perf_counter_ns() + int(duration * 1e9)

        async def worker() -> None:
            while time.perf_counter_ns() < deadline_ns:
                await self._send(target, payload_factory, result, state, deadline_ns)

        workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
        remaining = (deadline_ns - time.perf_counter_ns()) / 1e9
        try:
            await asyncio.wait_for(
                asyncio.gather(*workers), timeout=max(remaining, 0.0) + grace_period
            )
        except asyncio.TimeoutError:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        result.dropped_inflight = state["started"] - result.total_requests
        self._finish_window(result, state)
        return result

    async def _run_open_loop(
        self,
        target: Callable[[dict], Awaitable[dict]],
        payload_factory: Callable[[], dict],
        concurrency: int,
        rate: float,
        duration: float,
        total_requests: int | None,
        grace_period: float,
    ) -> BenchmarkResult:
        """Open-loop: dispatch requests at ``rate`` req/s, cap at ``concurrency``.

        Requests are sent on a fixed-interval schedule independent of response
        times, so coordinated omission is structurally avoided.

        Terminates when *either* the duration deadline expires or
        ``total_requests`` have been dispatched (whichever comes first).
        """
        result = BenchmarkResult()
        state: dict = {
            "started": 0, "first_t0_ns": None, "last_t1_ns": None,
            "schedule_misses": 0, "sem_wait_total_ns": 0,
        }
        interval_s = 1.0 / rate
        sem = asyncio.Semaphore(concurrency)
        deadline_ns = time.perf_counter_ns() + int(duration * 1e9)

        async def do_one() -> None:
            _before_sem = time.perf_counter_ns()
            async with sem:
                _after_sem = time.perf_counter_ns()
                state["sem_wait_total_ns"] += _after_sem - _before_sem
                await self._send(target, payload_factory, result, state, deadline_ns)

        tasks: list[asyncio.Task] = []
        next_tick = time.perf_counter_ns()
        dispatched = 0

        def _should_continue() -> bool:
            if total_requests is not None and dispatched >= total_requests:
                return False
            if duration > 0 and time.perf_counter_ns() >= deadline_ns:
                return False
            return True

        while _should_continue():
            tasks.append(asyncio.create_task(do_one()))
            dispatched += 1
            next_tick += int(interval_s * 1e9)
            now_ns = time.perf_counter_ns()
            sleep_ns = next_tick - now_ns
            if sleep_ns > 0:
                await asyncio.sleep(sleep_ns / 1e9)
            else:
                state["schedule_misses"] += 1

        # Drain: wait for in-flight requests up to grace_period
        if tasks:
            remaining = (deadline_ns - time.perf_counter_ns()) / 1e9
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks), timeout=max(remaining, 0.0) + grace_period,
                )
            except asyncio.TimeoutError:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        result.dropped_inflight = state["started"] - result.total_requests
        self._finish_window(result, state)

        # A7: warn when the load generator cannot sustain the target rate
        dispatched_count = dispatched
        if dispatched_count > 0 and result.window > 0:
            achieved = dispatched_count / result.window
            shortfall = (rate - achieved) / rate if rate > 0 else 0.0
            miss_pct = state["schedule_misses"] / dispatched_count
            sem_wait_ms = state["sem_wait_total_ns"] / 1e6
            if miss_pct > 0.05:
                result.warnings.append(
                    f"Open-loop dispatch fell behind schedule on "
                    f"{state['schedule_misses']}/{dispatched_count} ticks "
                    f"({miss_pct:.1%}); target rate {rate:.0f} req/s but "
                    f"achieved only {achieved:.0f} req/s "
                    f"(shortfall {shortfall:.1%})"
                )
            elif shortfall > 0.1:
                result.warnings.append(
                    f"Target rate {rate:.0f} req/s but achieved dispatch "
                    f"rate is only {achieved:.0f} req/s "
                    f"(shortfall {shortfall:.1%}); semaphore wait "
                    f"{sem_wait_ms:.0f}ms — concurrency={concurrency} "
                    f"may be the bottleneck"
                )

        return result

    async def _run_fixed_count(
        self,
        target: Callable[[dict], Awaitable[dict]],
        payload_factory: Callable[[], dict],
        concurrency: int,
        total_requests: int,
    ) -> BenchmarkResult:
        result = BenchmarkResult()
        state = {"started": 0, "first_t0_ns": None, "last_t1_ns": None}
        counter = 0

        async def worker() -> None:
            nonlocal counter
            while True:
                if counter >= total_requests:
                    return
                counter += 1  # no await between check and increment — atomic
                await self._send(target, payload_factory, result, state, None)

        worker_count = min(concurrency, total_requests)
        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        await asyncio.gather(*workers)

        self._finish_window(result, state)
        return result
