"""Multi-process load generation (方案 A).

One benchmark level is split across ``processes`` OS processes, each
running its own asyncio event loop + transport client.  The GIL limits a
single event loop to one core, so children are how the client uses
multiple cores; per-core capacity scales the achievable load.

Children return *raw* samples (latencies, send timestamps, stream/bidi
records); the parent merges them exactly — percentiles are recomputed on
the union, stream/bidi metrics on the union of raw records, and the window
is the union of first/last timestamps.  The unified spawn context
(``multiprocessing.get_context("spawn")``) makes behavior identical on
Linux/macOS/Windows.
"""

from __future__ import annotations

import atexit
import multiprocessing
import queue
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from lite_server.benchmark.benchmark import MIN_SAMPLES_BASE, BenchmarkResult

# Collection poll tick for run_benchmark_children: bounds both the
# hard-crash detection latency and the deadline overshoot.
_POLL_SECS = 0.2


@dataclass
class ChildOutcome:
    """Picklable result of one child process run.

    Exactly one of ``result``/``crash`` is set; a crash (or timeout)
    poisons the whole merge rather than producing partial data.
    """

    result: BenchmarkResult | None = None
    crash: str | None = None


def split_work(total: int, processes: int) -> list[int]:
    """Split ``total`` across ``processes`` as evenly as possible.

    Each child gets ``total // processes``; the remainder goes to the first
    children one unit at a time, so tail children may get 0 when
    ``processes > total`` (callers skip zero-work children).
    """
    if processes < 1:
        raise ValueError(f"processes must be >= 1, got {processes}")
    base, extra = divmod(total, processes)
    return [base + (1 if i < extra else 0) for i in range(processes)]


def _child_wrapper(
    entry: Callable, result_q: Any, task_id: int, arg: Any, kwargs: dict
) -> None:
    """Spawn target: run ``entry(arg, **kwargs)`` and always put exactly one
    ``(task_id, ChildOutcome)`` — the same wire protocol as ``_pool_worker``.

    ``entry`` is pickled by reference (must be a module-level function), so
    closures stay in the parent and children rebuild them from args.
    """
    try:
        result_q.put((task_id, ChildOutcome(entry(arg, **kwargs), None)))
    except BaseException as e:  # noqa: BLE001 — child must not die silently
        result_q.put((task_id, ChildOutcome(None, f"{type(e).__name__}: {e}")))


def _pool_worker(entry: Callable, task_q: Any, result_q: Any) -> None:
    """Reusable-pool worker: serve tasks until the None sentinel.

    A task exception becomes a crash ChildOutcome and the worker moves on —
    one bad task must not kill the pool (profile retries later trials).
    """
    while True:
        task = task_q.get()
        if task is None:
            return
        task_id, arg, kwargs = task
        try:
            result_q.put((task_id, ChildOutcome(entry(arg, **kwargs), None)))
        except BaseException as e:  # noqa: BLE001 — child must not die silently
            result_q.put((task_id, ChildOutcome(None, f"{type(e).__name__}: {e}")))


class ChildPool:
    """Reusable spawn pool: worker processes live across measurement tasks.

    Spawning costs ~0.5s/process (import + interpreter boot); profile's
    coordinate-descent runs dozens of trials, so the pool pays the spawn
    cost once and serves every trial.  Tasks are ``(arg, kwargs)`` pairs run
    as ``entry(arg, **kwargs)``; outcomes come back in dispatch order.

    A task that exceeds ``timeout_secs`` terminates the whole pool (a stuck
    worker would corrupt later tasks) and reports a crash outcome.  ``close``
    is idempotent and registered with ``atexit`` so CLI exit paths of any
    shape (early returns, interruptions) never leave orphan workers.
    """

    def __init__(self, entry: Callable, n_processes: int):
        ctx = multiprocessing.get_context("spawn")
        self.entry = entry
        self.task_q = ctx.Queue()
        self.result_q = ctx.Queue()
        self.procs = [
            ctx.Process(target=_pool_worker, args=(entry, self.task_q, self.result_q))
            for _ in range(n_processes)
        ]
        for p in self.procs:
            p.start()
        self._next_id = 0
        self._closed = False
        atexit.register(self.close)

    def run(self, specs: list[tuple[Any, dict]], timeout_secs: float | None) -> list[ChildOutcome]:
        """Dispatch one batch of tasks and collect results in spec order.

        ``timeout_secs=None`` waits indefinitely (unbounded workloads)."""
        if self._closed:
            raise RuntimeError("ChildPool is closed")
        ids = []
        for arg, kwargs in specs:
            task_id = self._next_id
            self._next_id += 1
            self.task_q.put((task_id, arg, kwargs))
            ids.append(task_id)

        outcomes: dict[int, ChildOutcome] = {}
        timed_out = False
        for task_id in ids:
            while task_id not in outcomes:
                if timed_out:
                    outcomes[task_id] = ChildOutcome(
                        None, "pool terminated by task timeout")
                    break
                try:
                    got_id, outcome = self.result_q.get(timeout=timeout_secs)
                except queue.Empty:
                    timed_out = True
                    self._terminate()
                    outcomes[task_id] = ChildOutcome(
                        None, f"task timed out after {timeout_secs}s")
                    break
                outcomes[got_id] = outcome
        return [outcomes[t] for t in ids]

    def close(self) -> None:
        """Shut the pool down; idempotent, safe to call from atexit."""
        if self._closed:
            return
        self._closed = True
        try:
            for _ in self.procs:
                self.task_q.put(None)
            for p in self.procs:
                p.join(10)
        finally:
            for p in self.procs:
                if p.is_alive():
                    p.terminate()
                    p.join(5)
            self.task_q.close()
            self.result_q.close()

    def _terminate(self) -> None:
        """Hard-kill all workers after a task timeout; pool unusable after."""
        self._closed = True
        for p in self.procs:
            if p.is_alive():
                p.terminate()
        for p in self.procs:
            p.join(5)


def run_benchmark_children(
    entry: Callable,
    specs: list[tuple[Any, dict]],
    timeout_secs: float | None,
) -> list[ChildOutcome]:
    """Spawn ``entry`` once per spec (unified spawn context), collect results.

    ``specs`` is a list of ``(arg, kwargs)`` tuples, each invoking
    ``entry(arg, **kwargs)``.  Outcomes carry their spec index (same wire
    protocol as :class:`ChildPool`), so the batch is returned in spec order
    regardless of completion order.

    Outcomes are collected *while* children run: a child cannot exit until
    its queue buffer flushes, and the buffer only drains when the parent
    reads — joining before reading deadlocks once an outcome exceeds the
    OS pipe buffer.  ``timeout_secs`` is one deadline for the whole batch
    (children run concurrently); ``None`` waits indefinitely for unbounded
    workloads (closed-loop requests mode).  A child that dies without
    queueing (hard crash — the wrapper queues on every catchable path) is
    reaped on the next poll tick; children still alive at the deadline are
    terminated and reported as crashes.
    """
    ctx = multiprocessing.get_context("spawn")
    result_q = ctx.Queue()
    procs = []
    for task_id, (arg, kwargs) in enumerate(specs):
        p = ctx.Process(
            target=_child_wrapper, args=(entry, result_q, task_id, arg, kwargs)
        )
        p.start()
        procs.append(p)

    outcomes: dict[int, ChildOutcome] = {}
    deadline = None if timeout_secs is None else time.monotonic() + timeout_secs
    while len(outcomes) < len(procs):
        timeout = _POLL_SECS
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            timeout = min(timeout, remaining)
        try:
            got_id, outcome = result_q.get(timeout=timeout)
            outcomes[got_id] = outcome
            continue
        except queue.Empty:
            pass
        # Poll tick: reap children that died without queueing an outcome.
        # A just-exited child's outcome may still be in the pipe, so drain
        # once before declaring a crash.
        for task_id, p in enumerate(procs):
            if task_id in outcomes or p.is_alive():
                continue
            try:
                got_id, outcome = result_q.get(timeout=_POLL_SECS)
                outcomes[got_id] = outcome
            except queue.Empty:
                outcomes[task_id] = ChildOutcome(
                    None, f"child {task_id} produced no result")

    for task_id, p in enumerate(procs):
        if task_id not in outcomes:
            p.terminate()
            outcomes[task_id] = ChildOutcome(
                None, f"child {task_id} timed out after {timeout_secs}s")
        p.join(5)
        if p.is_alive():
            p.terminate()
            p.join(1)
    return [outcomes[i] for i in range(len(procs))]


def merge_child_results(
    outcomes: list[ChildOutcome],
    *,
    concurrency: int,
    model_type: str = "llm",
    goodput_slo: dict | None = None,
    slo_attainment: float = 0.95,
    bidi: bool = False,
    min_sessions: int = 30,
) -> BenchmarkResult:
    """Merge child outcomes into one authoritative BenchmarkResult.

    Exact aggregation: raw samples are concatenated and every aggregate
    (percentiles, stream/bidi metrics) is recomputed on the union.  The
    sample-size warning is re-checked on the merged count with the
    single-process basis (``max(300, 10*concurrency)``; ``min_sessions``
    for bidi) — children suppress their own via ``min_samples=1``.
    """
    crashed = [o.crash for o in outcomes if o.result is None]
    if crashed:
        raise ValueError("benchmark child process failed: " + "; ".join(crashed))

    results = [o.result for o in outcomes]
    merged = BenchmarkResult()
    for r in results:
        merged.total_requests += r.total_requests
        merged.successful += r.successful
        merged.failed += r.failed
        merged.latencies.extend(r.latencies)
        merged.send_times_ns.extend(r.send_times_ns)
        merged.errors.extend(r.errors)
        merged.drained_in_grace += r.drained_in_grace
        merged.dropped_inflight += r.dropped_inflight
        for kind, count in r.error_kinds.items():
            merged.error_kinds[kind] = merged.error_kinds.get(kind, 0) + count
    merged.duration = max(r.duration for r in results)
    # Scalar fields are per-child *shares* of the user's request (each child
    # ran rate/processes and its warmup slice) — sum them back to the whole.
    merged.warmup_requests = sum(r.warmup_requests for r in results)
    merged.load_mode = results[0].load_mode
    rates = [r.target_rate for r in results if r.target_rate is not None]
    merged.target_rate = sum(rates) if rates else None

    first_t0s = [r.first_t0_ns for r in results if r.first_t0_ns is not None]
    last_t1s = [r.last_t1_ns for r in results if r.last_t1_ns is not None]
    if first_t0s and last_t1s:
        merged.window = (max(last_t1s) - min(first_t0s)) / 1e9

    for r in results:
        merged.warnings.extend(r.warnings)

    if results[0].stream_metrics is not None:
        from lite_server.benchmark.stream_metrics import compute_stream_metrics

        records = [rec for r in results for rec in (r.stream_records or [])]
        sm = compute_stream_metrics(records, model_type, window_secs=merged.window or None)
        if goodput_slo is not None:
            from lite_server.benchmark.stream_metrics import compute_goodput

            sm.goodput = compute_goodput(
                records, goodput_slo, model_type=model_type,
                throughput=merged.throughput,
                attainment_target=slo_attainment,
            )
        merged.stream_metrics = sm

    if results[0].bidi_metrics is not None:
        from lite_server.benchmark.bidi_metrics import compute_bidi_metrics

        records = [rec for r in results for rec in (r.bidi_records or [])]
        first = results[0].bidi_metrics
        merged.bidi_metrics = compute_bidi_metrics(
            records, transport=first.transport, pacing_mode=first.pacing_mode,
            failed_sessions=merged.failed, window_secs=merged.window or None,
        )

    threshold = min_sessions if bidi else max(MIN_SAMPLES_BASE, 10 * concurrency)
    if merged.successful < threshold:
        basis = "min_sessions" if bidi else "max(300, 10*concurrency)"
        merged.warnings.append(
            f"Sample size {merged.successful} < {threshold} "
            f"({basis}); latency percentiles (esp. p99) "
            f"may be unreliable — increase duration/requests"
        )
    return merged
