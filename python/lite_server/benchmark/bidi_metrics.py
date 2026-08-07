"""Bidi session metrics computation — pure functions (批次 2, plan §4.4).

Aggregates per-session ``BidiSessionRecord`` timings into
``BidiSessionMetrics``.  Mirrors ``stream_metrics.py``'s layering: the
engine records raw timings, this module is the single point of
session-metric math.
"""

from __future__ import annotations

from lite_server.benchmark.benchmark import BidiSessionMetrics, BidiSessionRecord
from lite_server.benchmark.stream_metrics import _percentiles

PACING_MODES = ("lock_step", "real_time", "speedup")


def compute_bidi_metrics(
    records: list[BidiSessionRecord],
    *,
    transport: str,
    pacing_mode: str,
    failed_sessions: int = 0,
    window_secs: float | None = None,
) -> BidiSessionMetrics:
    """Compute aggregated session metrics from per-session records.

    Args:
        records: Per-session raw measurements (successful sessions only).
        transport: Transport label (``"ws"`` / ``"h2"`` / ``"grpc"``).
        pacing_mode: One of ``PACING_MODES``.
        failed_sessions: Sessions that errored (counted by ``run()``).
        window_secs: Measured window for ``sessions_per_sec``.

    Raises:
        ValueError: If *pacing_mode* is not one of the known modes.
    """
    if pacing_mode not in PACING_MODES:
        raise ValueError(
            f"pacing_mode must be one of {PACING_MODES}, got {pacing_mode!r}"
        )

    if not records:
        zero = _percentiles([])
        return BidiSessionMetrics(
            transport=transport,
            pacing_mode=pacing_mode,
            sessions=0,
            failed_sessions=failed_sessions,
            open_latency_ms=zero,
            close_to_final_ms=zero,
            session_duration_ms=zero,
            chunks_per_session=zero,
        )

    open_vals = [r.open_latency_ms for r in records if r.open_latency_ms is not None]
    close_vals = [
        r.close_to_final_ms for r in records if r.close_to_final_ms is not None
    ]
    duration_vals = [
        r.session_duration_ms for r in records if r.session_duration_ms is not None
    ]
    chunk_counts = [float(r.consumer_chunks) for r in records]

    roundtrips: list[float] = []
    for r in records:
        roundtrips.extend(r.chunk_roundtrips_ms)

    sessions_per_sec = None
    if window_secs is not None and window_secs > 0:
        sessions_per_sec = len(records) / window_secs

    return BidiSessionMetrics(
        transport=transport,
        pacing_mode=pacing_mode,
        sessions=len(records),
        failed_sessions=failed_sessions,
        open_latency_ms=_percentiles(open_vals),
        close_to_final_ms=_percentiles(close_vals),
        session_duration_ms=_percentiles(duration_vals),
        chunks_per_session=_percentiles(chunk_counts),
        chunk_roundtrip_ms=_percentiles(roundtrips) if roundtrips else None,
        sessions_per_sec=sessions_per_sec,
    )
