"""Streaming metrics computation — pure functions (PR-4).

Model-specific math on top of raw ``StreamRequestRecord`` timings.
Engine is unaware of LLM/TTS/STT; this module is the single point of
model-type variance.

Metric semantics per model type:

  LLM  TTFT = time-to-first-token
        ITL  = inter-token latency (pooled across all requests)
        TPOT = per-request (total_ms - ttft_ms) / max(tokens-1, 1)
        tokens_per_sec        = decode-phase throughput (per-request agg)
        tokens_per_sec_e2e    = incl. TTFT
        tokens_per_sec_aggregate = total_tokens / window_secs (system throughput)

  TTS  TTFT = time-to-first-audio-chunk (TTFAC)
        RTF  = total_ms / meta_totals["audio_duration_ms"]

  STT  TTFT = first transcription result latency
        RTF  = total_ms / request_meta["audio_duration_ms"]
"""

from __future__ import annotations

import numpy as np

from lite_server.analyzer.benchmark import StreamMetrics, StreamRequestRecord

MODEL_TYPES = ("llm", "tts", "stt")

_PERCENTILE_KEYS = ("mean", "p50", "p90", "p95", "p99", "min", "max")


def _percentiles(values: list[float]) -> dict:
    """Compute mean/p50/p90/p95/p99/min/max for a list of floats.

    Uses numpy linear interpolation, consistent with
    ``BenchmarkResult._percentile``.
    """
    if not values:
        return {k: 0.0 for k in _PERCENTILE_KEYS}
    arr = np.array(values, dtype=np.float64)
    return {
        "mean": float(np.mean(arr)),
        "p50": float(np.percentile(arr, 50, method="linear")),
        "p90": float(np.percentile(arr, 90, method="linear")),
        "p95": float(np.percentile(arr, 95, method="linear")),
        "p99": float(np.percentile(arr, 99, method="linear")),
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
    }


def compute_stream_metrics(
    records: list[StreamRequestRecord],
    model_type: str,
    *,
    window_secs: float | None = None,
) -> StreamMetrics:
    """Compute aggregated streaming metrics from per-request records.

    Args:
        records: Per-request raw measurements from ``run_stream()``.
        model_type: One of ``"llm"``, ``"tts"``, ``"stt"``.
        window_secs: Measured window for aggregate throughput (R2).
            When provided, ``tokens_per_sec_aggregate`` is computed as
            total_tokens / window_secs.

    Raises:
        ValueError: If *model_type* is not one of the known types.
    """
    if model_type not in MODEL_TYPES:
        raise ValueError(
            f"model_type must be one of {MODEL_TYPES}, got {model_type!r}"
        )

    total_requests = len(records)
    if total_requests == 0:
        zero = _percentiles([])
        return StreamMetrics(
            model_type=model_type,
            requests=0,
            zero_chunk_requests=0,
            total_chunks=0,
            total_bytes=0,
            chunks_per_request=zero,
            ttft_ms=zero,
            total_ms=zero,
        )

    # Filter: records with chunk_count == 0 are zero-chunk
    zero_chunk_requests = sum(1 for r in records if r.chunk_count == 0)
    active = [r for r in records if r.chunk_count > 0]

    total_chunks = sum(r.chunk_count for r in records)
    total_bytes = sum(r.total_bytes for r in records)

    # TTFT / total_ms / chunks per request — from active records only
    ttft_vals = [r.ttft_ms for r in active if r.ttft_ms is not None]
    total_ms_vals = [r.total_ms for r in active if r.total_ms is not None]
    chunk_counts = [r.chunk_count for r in active]

    sm = StreamMetrics(
        model_type=model_type,
        requests=total_requests,
        zero_chunk_requests=zero_chunk_requests,
        total_chunks=total_chunks,
        total_bytes=total_bytes,
        chunks_per_request=_percentiles([float(c) for c in chunk_counts]),
        ttft_ms=_percentiles(ttft_vals),
        total_ms=_percentiles(total_ms_vals),
    )

    # ── Model-specific ──────────────────────────────────────────────

    if model_type == "llm":
        _compute_llm(sm, active, window_secs)
    elif model_type in ("tts", "stt"):
        _compute_rtf(sm, active, model_type)

    return sm


# ── LLM helpers ──────────────────────────────────────────────────────────


def _compute_llm(
    sm: StreamMetrics,
    records: list[StreamRequestRecord],
    window_secs: float | None,
) -> None:
    # Token counting: per-request token counts + basis determination
    token_counts: list[float] = []
    has_exact = False
    has_estimated = False

    for r in records:
        tc = r.meta_totals.get("token_count")
        if tc is not None:
            token_counts.append(tc)
            has_exact = True
        else:
            # Default: chunk == token
            token_counts.append(float(r.chunk_count))
            has_estimated = True

    if has_exact and has_estimated:
        sm.token_count_basis = "mixed"
    elif has_exact:
        sm.token_count_basis = "exact"
    else:
        sm.token_count_basis = "estimated"

    sm.tokens_per_request = _percentiles(token_counts)

    # ITL: pooled inter-chunk latencies across all requests
    all_inter: list[float] = []
    for r in records:
        all_inter.extend(r.inter_chunk_ms)
    sm.itl_ms = _percentiles(all_inter) if all_inter else None

    # TPOT: per-request (total_ms - ttft_ms) / max(tokens - 1, 1)
    tpot_vals: list[float] = []
    for r, tc in zip(records, token_counts):
        if r.total_ms is not None and r.ttft_ms is not None and tc > 1:
            tpot = (r.total_ms - r.ttft_ms) / (tc - 1)
        elif r.total_ms is not None and r.ttft_ms is not None:
            tpot = r.total_ms - r.ttft_ms  # single token → whole decode = TPOT
        else:
            continue
        tpot_vals.append(tpot)
    sm.tpot_ms = _percentiles(tpot_vals) if tpot_vals else None

    total_tokens = sum(token_counts)

    # decode-phase throughput: total_tokens / sum(decode_time_per_request)
    decode_ms = 0.0
    for r, tc in zip(records, token_counts):
        if r.total_ms is not None and r.ttft_ms is not None:
            decode_ms += r.total_ms - r.ttft_ms
    if decode_ms > 0:
        sm.tokens_per_sec = (total_tokens / decode_ms) * 1000.0

    # e2e throughput: total_tokens / sum(total_ms)
    e2e_ms = sum(r.total_ms for r in records if r.total_ms is not None)
    if e2e_ms > 0:
        sm.tokens_per_sec_e2e = (total_tokens / e2e_ms) * 1000.0

    # aggregate (system) throughput: total_tokens / window_secs
    if window_secs is not None and window_secs > 0:
        sm.tokens_per_sec_aggregate = total_tokens / window_secs


# ── RTF helpers (TTS / STT) ─────────────────────────────────────────────


def _compute_rtf(
    sm: StreamMetrics,
    records: list[StreamRequestRecord],
    model_type: str,
) -> None:
    rtf_vals: list[float] = []

    for r in records:
        if r.total_ms is None or r.total_ms <= 0:
            continue

        if model_type == "tts":
            audio_ms = r.meta_totals.get("audio_duration_ms")
        else:  # stt
            if r.request_meta is None:
                continue
            audio_ms = r.request_meta.get("audio_duration_ms")

        if audio_ms is None or audio_ms <= 0:
            continue

        rtf_vals.append(r.total_ms / audio_ms)

    sm.rtf = _percentiles(rtf_vals) if rtf_vals else None
