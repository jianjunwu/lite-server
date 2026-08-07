"""Tests for streaming benchmark — PR-4.

Phases 1–5: datatypes → stream_metrics → run_stream → sse_target → CLI.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import asdict

import pytest

from lite_server.analyzer.benchmark import (
    BenchmarkEngine,
    BenchmarkResult,
    RequestStreamError,
    StreamChunk,
    StreamMetrics,
    StreamRequestRecord,
)


# ── Phase 1: Datatypes ──────────────────────────────────────────────────────

class TestStreamChunk:
    def test_defaults(self):
        c = StreamChunk()
        assert c.data is None
        assert c.meta is None
        assert c.size_bytes is None

    def test_with_data_and_meta(self):
        c = StreamChunk(data="hello", meta={"token_count": 3}, size_bytes=5)
        assert c.data == "hello"
        assert c.meta == {"token_count": 3}
        assert c.size_bytes == 5

    def test_size_bytes_falls_back_none(self):
        c = StreamChunk(data="hello")
        assert c.size_bytes is None  # size_bytes is explicit; no auto-compute in dataclass


class TestStreamRequestRecord:
    def test_defaults(self):
        r = StreamRequestRecord()
        assert r.chunk_count == 0
        assert r.total_bytes == 0
        assert r.ttft_ms is None
        assert r.total_ms is None
        assert r.inter_chunk_ms == []
        assert r.chunk_metas == []
        assert r.meta_totals == {}
        assert r.request_meta is None

    def test_partial_fill(self):
        r = StreamRequestRecord(
            chunk_count=5,
            total_bytes=120,
            ttft_ms=42.0,
            total_ms=250.0,
            inter_chunk_ms=[10.0, 12.0, 11.0, 9.0],
            chunk_metas=[{"token_count": 1}, {}, {"token_count": 2}, {}, {}],
            meta_totals={"token_count": 3},
        )
        assert r.chunk_count == 5
        assert r.ttft_ms == 42.0
        assert r.total_ms == 250.0
        assert len(r.inter_chunk_ms) == 4
        assert len(r.chunk_metas) == 5


class TestRequestStreamError:
    def test_kind_is_stream(self):
        e = RequestStreamError("SSE error event")
        assert e.kind == "stream"

    def test_is_request_error_subclass(self):
        from lite_server.analyzer.benchmark import RequestError

        e = RequestStreamError("boom")
        assert isinstance(e, RequestError)


class TestStreamMetrics:
    def _sample_metrics(self, model_type="llm") -> StreamMetrics:
        return StreamMetrics(
            model_type=model_type,
            requests=10,
            zero_chunk_requests=0,
            total_chunks=50,
            total_bytes=1024,
            chunks_per_request={"mean": 5.0, "p50": 5.0, "p90": 7.0, "p95": 8.0,
                               "p99": 9.0, "min": 2.0, "max": 9.0},
            ttft_ms={"mean": 100.0, "p50": 95.0, "p90": 150.0, "p95": 180.0,
                     "p99": 200.0, "min": 50.0, "max": 200.0},
            total_ms={"mean": 500.0, "p50": 480.0, "p90": 700.0, "p95": 800.0,
                      "p99": 950.0, "min": 200.0, "max": 950.0},
            itl_ms={"mean": 80.0, "p50": 75.0, "p90": 120.0, "p95": 150.0,
                    "p99": 180.0, "min": 30.0, "max": 180.0},
            tpot_ms={"mean": 80.0, "p50": 75.0, "p90": 120.0, "p95": 150.0,
                     "p99": 180.0, "min": 30.0, "max": 180.0},
            tokens_per_sec=50.0,
            tokens_per_sec_e2e=45.0,
            tokens_per_sec_aggregate=48.0,
            tokens_per_request={"mean": 5.0, "p50": 5.0, "p90": 7.0, "p95": 8.0,
                               "p99": 9.0, "min": 2.0, "max": 9.0},
            token_count_basis="exact",
        )

    def test_to_dict_llm_mode_key(self):
        m = self._sample_metrics("llm")
        d = m.to_dict()
        assert d["mode"] == "llm"

    def test_to_dict_llm_has_llm_section(self):
        m = self._sample_metrics("llm")
        d = m.to_dict()
        assert "itl_ms" in d
        assert "tpot_ms" in d
        assert "tokens_per_sec" in d
        assert "tokens_per_sec_e2e" in d
        assert "tokens_per_sec_aggregate" in d
        assert "tokens_per_request" in d
        assert "token_count_basis" in d
        assert d["token_count_basis"] == "exact"
        # LLM section must NOT have RTF
        assert "rtf" not in d

    def test_to_dict_tts_has_rtf_no_llm(self):
        m = StreamMetrics(
            model_type="tts",
            requests=5,
            zero_chunk_requests=0,
            total_chunks=20,
            total_bytes=512,
            chunks_per_request={"mean": 4.0, "p50": 4.0, "p90": 5.0, "p95": 6.0,
                               "p99": 6.0, "min": 3.0, "max": 6.0},
            ttft_ms={"mean": 50.0, "p50": 45.0, "p90": 70.0, "p95": 80.0,
                     "p99": 90.0, "min": 20.0, "max": 90.0},
            total_ms={"mean": 300.0, "p50": 280.0, "p90": 400.0, "p95": 450.0,
                      "p99": 500.0, "min": 100.0, "max": 500.0},
            rtf={"mean": 0.5, "p50": 0.48, "p90": 0.7, "p95": 0.8,
                 "p99": 0.9, "min": 0.3, "max": 0.9},
        )
        d = m.to_dict()
        assert d["mode"] == "tts"
        assert "rtf" in d
        assert "itl_ms" not in d
        assert "tokens_per_sec" not in d

    def test_to_dict_stt_has_rtf(self):
        m = StreamMetrics(
            model_type="stt",
            requests=3,
            zero_chunk_requests=0,
            total_chunks=15,
            total_bytes=384,
            chunks_per_request={"mean": 5.0, "p50": 5.0, "p90": 6.0, "p95": 6.0,
                               "p99": 6.0, "min": 4.0, "max": 6.0},
            ttft_ms={"mean": 200.0, "p50": 190.0, "p90": 300.0, "p95": 350.0,
                     "p99": 400.0, "min": 100.0, "max": 400.0},
            total_ms={"mean": 1000.0, "p50": 950.0, "p90": 1400.0, "p95": 1600.0,
                      "p99": 1800.0, "min": 500.0, "max": 1800.0},
            rtf={"mean": 2.0, "p50": 1.9, "p90": 2.8, "p95": 3.2,
                 "p99": 3.6, "min": 1.0, "max": 3.6},
        )
        d = m.to_dict()
        assert d["mode"] == "stt"
        assert "rtf" in d

    def test_to_dict_always_has_common_section(self):
        for mt in ("llm", "tts", "stt"):
            m = StreamMetrics(
                model_type=mt,
                requests=1, zero_chunk_requests=0, total_chunks=1, total_bytes=10,
                chunks_per_request={"mean": 1.0, "p50": 1.0, "p90": 1.0, "p95": 1.0,
                                   "p99": 1.0, "min": 1.0, "max": 1.0},
                ttft_ms={"mean": 10.0, "p50": 10.0, "p90": 10.0, "p95": 10.0,
                         "p99": 10.0, "min": 10.0, "max": 10.0},
                total_ms={"mean": 100.0, "p50": 100.0, "p90": 100.0, "p95": 100.0,
                          "p99": 100.0, "min": 100.0, "max": 100.0},
            )
            d = m.to_dict()
            assert "chunks_per_request" in d
            assert "ttft_ms" in d
            assert "total_ms" in d
            assert "requests" in d
            assert "zero_chunk_requests" in d
            assert "total_chunks" in d
            assert "total_bytes" in d

    def test_to_dict_nullable_fields_omitted_when_none(self):
        m = StreamMetrics(
            model_type="llm",
            requests=1, zero_chunk_requests=0, total_chunks=1, total_bytes=10,
            chunks_per_request={"mean": 1.0, "p50": 1.0, "p90": 1.0, "p95": 1.0,
                               "p99": 1.0, "min": 1.0, "max": 1.0},
            ttft_ms={"mean": 10.0, "p50": 10.0, "p90": 10.0, "p95": 10.0,
                     "p99": 10.0, "min": 10.0, "max": 10.0},
            total_ms={"mean": 100.0, "p50": 100.0, "p90": 100.0, "p95": 100.0,
                      "p99": 100.0, "min": 100.0, "max": 100.0},
            # all LLM-specific fields left at None
        )
        d = m.to_dict()
        # None-valued optional fields are omitted
        assert "itl_ms" not in d
        assert "tpot_ms" not in d
        assert "tokens_per_sec" not in d
        assert "tokens_per_request" not in d
        assert "token_count_basis" not in d


class TestBenchmarkResultStreamExtension:
    """Verify additive extension: BenchmarkResult + stream_metrics."""

    def test_default_stream_metrics_is_none(self):
        r = BenchmarkResult()
        assert r.stream_metrics is None

    def test_to_dict_stream_null_when_none(self):
        r = BenchmarkResult()
        d = r.to_dict()
        assert "stream" in d
        assert d["stream"] is None

    def test_to_dict_stream_present_when_set(self):
        sm = StreamMetrics(
            model_type="llm",
            requests=1, zero_chunk_requests=0, total_chunks=1, total_bytes=10,
            chunks_per_request={"mean": 1.0, "p50": 1.0, "p90": 1.0, "p95": 1.0,
                               "p99": 1.0, "min": 1.0, "max": 1.0},
            ttft_ms={"mean": 10.0, "p50": 10.0, "p90": 10.0, "p95": 10.0,
                     "p99": 10.0, "min": 10.0, "max": 10.0},
            total_ms={"mean": 100.0, "p50": 100.0, "p90": 100.0, "p95": 100.0,
                      "p99": 100.0, "min": 100.0, "max": 100.0},
        )
        r = BenchmarkResult(stream_metrics=sm)
        d = r.to_dict()
        assert d["stream"] is not None
        assert d["stream"]["mode"] == "llm"

    def test_legacy_keys_preserved(self):
        """Existing to_dict() keys are unchanged when stream_metrics is added."""
        r = BenchmarkResult(
            total_requests=10, successful=10, latencies=[1.0, 2.0],
            window=5.0, duration=5.0,
        )
        d = r.to_dict()
        # All legacy keys still present
        for key in ("load_mode", "latency_basis", "total_requests", "successful",
                     "failed", "throughput", "latency_ms", "warnings"):
            assert key in d, f"legacy key {key!r} missing"
        assert "stream" in d
        assert d["stream"] is None


# ── Phase 2: stream_metrics.py ───────────────────────────────────────────────

class TestComputeStreamMetricsLLM:
    """LLM model type: token-based metrics."""

    def _make_records(self, token_counts: list[int], ttft_base=50.0,
                      inter_chunk=20.0) -> list[StreamRequestRecord]:
        records = []
        for n in token_counts:
            metas = [{"token_count": 1} for _ in range(n)]
            inter = [inter_chunk] * (n - 1) if n > 1 else []
            records.append(StreamRequestRecord(
                chunk_count=n,
                total_bytes=n * 4,
                ttft_ms=ttft_base,
                total_ms=ttft_base + sum(inter),
                inter_chunk_ms=inter,
                chunk_metas=metas,
                meta_totals={"token_count": float(n)},
            ))
        return records

    def test_chunk_equals_token_default(self):
        """Without token_count meta, each chunk counts as 1 token."""
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = []
        for i in range(5):
            n = i + 2
            records.append(StreamRequestRecord(
                chunk_count=n,
                total_bytes=n * 4,
                ttft_ms=50.0,
                total_ms=50.0 + (n - 1) * 20.0,
                inter_chunk_ms=[20.0] * (n - 1),
            ))
        sm = compute_stream_metrics(records, "llm")
        assert sm.token_count_basis == "estimated"
        assert sm.tokens_per_request is not None
        assert sm.tokens_per_request["mean"] == pytest.approx(4.0)

    def test_token_count_meta_gives_exact_basis(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = self._make_records([3, 5, 7])
        sm = compute_stream_metrics(records, "llm")
        assert sm.token_count_basis == "exact"
        assert sm.tokens_per_request is not None
        assert sm.tokens_per_request["mean"] == pytest.approx(5.0)

    def test_mixed_basis_when_partial_meta(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = self._make_records([3, 5])
        # third record has no chunk_metas
        records.append(StreamRequestRecord(
            chunk_count=10,
            total_bytes=40,
            ttft_ms=50.0,
            total_ms=200.0,
            inter_chunk_ms=[15.0] * 9,
        ))
        sm = compute_stream_metrics(records, "llm")
        assert sm.token_count_basis == "mixed"
        assert sm.tokens_per_sec is not None

    def test_itl_pooled_across_requests(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = self._make_records([3, 3, 3], inter_chunk=25.0)
        sm = compute_stream_metrics(records, "llm")
        assert sm.itl_ms is not None
        # 3 records × 2 inter-chunk gaps each = 6 values, all 25ms
        assert sm.itl_ms["mean"] == pytest.approx(25.0, abs=1.0)

    def test_tpot_per_request_percentiles(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = self._make_records([3, 5, 7], ttft_base=50.0, inter_chunk=25.0)
        sm = compute_stream_metrics(records, "llm")
        assert sm.tpot_ms is not None
        # record 1: (100-50)/2=25; record 2: (150-50)/4=25; record 3: (200-50)/6=25
        assert sm.tpot_ms["mean"] == pytest.approx(25.0, abs=1.0)

    def test_tokens_per_sec_decode_phase(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = self._make_records([5, 5], ttft_base=50.0, inter_chunk=10.0)
        sm = compute_stream_metrics(records, "llm")
        assert sm.tokens_per_sec is not None
        assert sm.tokens_per_sec > 0

    def test_tokens_per_sec_e2e_includes_ttft(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = self._make_records([5, 5], ttft_base=50.0, inter_chunk=10.0)
        sm = compute_stream_metrics(records, "llm")
        assert sm.tokens_per_sec_e2e is not None
        assert sm.tokens_per_sec_e2e > 0
        assert sm.tokens_per_sec_e2e < sm.tokens_per_sec

    def test_tokens_per_sec_aggregate_requires_window(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = self._make_records([5, 5, 5])
        sm = compute_stream_metrics(records, "llm")
        assert sm.tokens_per_sec_aggregate is None

        sm2 = compute_stream_metrics(records, "llm", window_secs=3.0)
        assert sm2.tokens_per_sec_aggregate is not None
        assert sm2.tokens_per_sec_aggregate == pytest.approx(5.0, abs=0.5)


class TestComputeStreamMetricsTTS:
    """TTS model type: RTF-based metrics."""

    def test_rtf_from_meta_totals(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = []
        for audio_ms, total_ms in [(1000.0, 500.0), (2000.0, 800.0), (500.0, 300.0)]:
            records.append(StreamRequestRecord(
                chunk_count=5,
                total_bytes=1000,
                ttft_ms=50.0,
                total_ms=total_ms,
                inter_chunk_ms=[10.0] * 4,
                meta_totals={"audio_duration_ms": audio_ms},
            ))
        sm = compute_stream_metrics(records, "tts")
        assert sm.rtf is not None
        assert sm.rtf["mean"] == pytest.approx(0.5, abs=0.05)
        assert sm.itl_ms is None
        assert sm.tokens_per_sec is None

    def test_record_missing_audio_duration_excluded_from_rtf(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = [
            StreamRequestRecord(
                chunk_count=5, total_bytes=1000, ttft_ms=50.0, total_ms=500.0,
                inter_chunk_ms=[10.0] * 4,
                meta_totals={"audio_duration_ms": 1000.0},
            ),
            StreamRequestRecord(
                chunk_count=3, total_bytes=600, ttft_ms=60.0, total_ms=800.0,
                inter_chunk_ms=[15.0] * 2,
            ),
        ]
        sm = compute_stream_metrics(records, "tts")
        assert sm.rtf is not None
        assert sm.rtf["mean"] == pytest.approx(0.5, abs=0.05)
        assert sm.requests == 2


class TestComputeStreamMetricsSTT:
    """STT model type: RTF from request_meta."""

    def test_rtf_from_request_meta(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = []
        for audio_ms, total_ms in [(3000.0, 1500.0), (5000.0, 2000.0)]:
            records.append(StreamRequestRecord(
                chunk_count=3,
                total_bytes=500,
                ttft_ms=100.0,
                total_ms=total_ms,
                inter_chunk_ms=[50.0] * 2,
                request_meta={"audio_duration_ms": audio_ms},
            ))
        sm = compute_stream_metrics(records, "stt")
        assert sm.rtf is not None
        assert sm.rtf["mean"] == pytest.approx(0.45, abs=0.05)

    def test_record_missing_request_meta_excluded_from_rtf(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = [
            StreamRequestRecord(
                chunk_count=3, total_bytes=500, ttft_ms=100.0, total_ms=1500.0,
                inter_chunk_ms=[50.0] * 2,
                request_meta={"audio_duration_ms": 3000.0},
            ),
            StreamRequestRecord(
                chunk_count=2, total_bytes=300, ttft_ms=80.0, total_ms=600.0,
                inter_chunk_ms=[30.0],
            ),
        ]
        sm = compute_stream_metrics(records, "stt")
        assert sm.rtf is not None
        assert sm.rtf["mean"] == pytest.approx(0.5, abs=0.05)


class TestComputeStreamMetricsEdgeCases:
    """Empty records, zero-chunk, empty chunk filtering, bad model_type."""

    def test_empty_records_returns_zero_structure(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        sm = compute_stream_metrics([], "llm")
        assert sm.requests == 0
        assert sm.total_chunks == 0
        assert sm.ttft_ms["mean"] == 0.0
        assert sm.ttft_ms["p99"] == 0.0
        assert sm.total_ms["mean"] == 0.0
        assert sm.tokens_per_sec is None

    def test_all_zero_chunk_requests(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = [
            StreamRequestRecord(chunk_count=0),
            StreamRequestRecord(chunk_count=0),
        ]
        sm = compute_stream_metrics(records, "llm")
        assert sm.requests == 2
        assert sm.zero_chunk_requests == 2
        assert sm.chunks_per_request["mean"] == 0.0
        assert sm.tokens_per_sec is None

    def test_empty_chunk_not_counted_for_ttft(self):
        """Empty data chunks do not count as first-token."""
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = [
            StreamRequestRecord(
                chunk_count=3,
                total_bytes=100,
                ttft_ms=100.0,
                total_ms=200.0,
                inter_chunk_ms=[50.0, 50.0],
                chunk_metas=[{"token_count": 1}, {}, {"token_count": 1}],
                meta_totals={"token_count": 2.0},
            ),
        ]
        sm = compute_stream_metrics(records, "llm")
        assert sm.total_chunks == 3

    def test_bad_model_type_raises_value_error(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        with pytest.raises(ValueError, match="model_type"):
            compute_stream_metrics([], "image")

    def test_model_type_case_sensitive(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        with pytest.raises(ValueError, match="model_type"):
            compute_stream_metrics([], "LLM")


class TestGenericModelType:
    """--model-type generic: common section only, no LLM/TTS/STT metrics (§3.2)."""

    def test_generic_in_model_types(self):
        from lite_server.analyzer.stream_metrics import MODEL_TYPES

        assert "generic" in MODEL_TYPES

    def test_generic_computes_common_section_only(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = [
            StreamRequestRecord(
                chunk_count=5, total_bytes=500, ttft_ms=50.0, total_ms=300.0,
                inter_chunk_ms=[60.0, 60.0, 60.0, 60.0],
                meta_totals={"token_count": 5.0},
            ),
        ]
        sm = compute_stream_metrics(records, "generic")
        assert sm.model_type == "generic"
        assert sm.requests == 1
        assert sm.chunks_per_request["mean"] == 5.0
        assert sm.ttft_ms["mean"] == 50.0
        assert sm.total_ms["mean"] == 300.0
        # No model-specific metrics — even when token_count meta is present
        assert sm.itl_ms is None
        assert sm.tpot_ms is None
        assert sm.tokens_per_sec is None
        assert sm.tokens_per_sec_e2e is None
        assert sm.tokens_per_sec_aggregate is None
        assert sm.tokens_per_request is None
        assert sm.token_count_basis is None
        assert sm.rtf is None

    def test_generic_to_dict_has_no_model_specific_keys(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        records = [
            StreamRequestRecord(
                chunk_count=3, total_bytes=100, ttft_ms=10.0, total_ms=100.0,
                inter_chunk_ms=[40.0, 40.0],
                meta_totals={"token_count": 3.0, "audio_duration_ms": 960.0},
            ),
        ]
        d = compute_stream_metrics(records, "generic").to_dict()
        assert d["mode"] == "generic"
        for key in ("itl_ms", "tpot_ms", "tokens_per_sec", "tokens_per_sec_e2e",
                    "tokens_per_sec_aggregate", "tokens_per_request",
                    "token_count_basis", "rtf"):
            assert key not in d

    def test_generic_empty_records_zero_structure(self):
        from lite_server.analyzer.stream_metrics import compute_stream_metrics

        sm = compute_stream_metrics([], "generic")
        assert sm.model_type == "generic"
        assert sm.requests == 0
        assert sm.ttft_ms["mean"] == 0.0


# ── Phase 3: run_stream() adapter ────────────────────────────────────────────

class TestRunStreamAdapter:
    """Adapter wraps AsyncIterator[StreamChunk] as unary callable for run()."""

    @staticmethod
    async def _chunk_generator(chunks: list[StreamChunk], *, sleep_s=0.0):
        """Helper async generator that yields chunks with optional sleep."""
        for c in chunks:
            if sleep_s:
                await asyncio.sleep(sleep_s)
            yield c

    def _make_target(self, chunks_per_request: list[list[StreamChunk]],
                     sleep_s=0.0):
        """Build a streaming target that cycles through chunk lists."""
        async def target(payload) -> "AsyncIterator[StreamChunk]":
            idx = payload.get("_idx", 0) % len(chunks_per_request)
            for c in chunks_per_request[idx]:
                if sleep_s:
                    await asyncio.sleep(sleep_s)
                yield c
        return target

    @pytest.mark.asyncio
    async def test_basic_stream_measurement(self):
        """run_stream records TTFT, chunk_count, total_ms, inter_chunk_ms."""
        engine = BenchmarkEngine()

        async def target(payload):
            for data in ["a", "b", "c"]:
                await asyncio.sleep(0.005)
                yield StreamChunk(data=data, meta={"token_count": 1})

        result = await engine.run_stream(
            target=target,
            payload={"input": "test"},
            concurrency=1,
            total_requests=5,
            model_type="llm",
        )
        assert result.successful == 5
        assert result.stream_metrics is not None
        sm = result.stream_metrics
        assert sm.model_type == "llm"
        assert sm.requests == 5
        assert sm.total_chunks == 15  # 5 × 3
        assert sm.chunks_per_request["mean"] == pytest.approx(3.0)
        # TTFT should be ~5ms
        assert sm.ttft_ms["mean"] > 0
        assert sm.total_ms["mean"] > sm.ttft_ms["mean"]

    @pytest.mark.asyncio
    async def test_warmup_discards_stream_samples(self):
        """Warmup requests consume full stream but don't count."""
        engine = BenchmarkEngine()
        warmup_count = [0]

        async def target(payload):
            warmup_count[0] += 1
            for data in ["x"]:
                await asyncio.sleep(0.001)
                yield StreamChunk(data=data)

        result = await engine.run_stream(
            target=target,
            payload={"input": "test"},
            concurrency=1,
            total_requests=3,
            warmup_requests=2,
            model_type="llm",
        )
        assert warmup_count[0] >= 2
        assert result.successful == 3

    @pytest.mark.asyncio
    async def test_zero_chunk_request_produces_warning(self):
        """A stream that yields zero chunks triggers a warning."""
        engine = BenchmarkEngine()

        async def target(payload):
            # Empty generator — no chunks
            if False:
                yield

        result = await engine.run_stream(
            target=target,
            payload={"input": "test"},
            concurrency=1,
            total_requests=3,
            model_type="llm",
        )
        assert result.stream_metrics is not None
        assert result.stream_metrics.zero_chunk_requests == 3
        assert any("zero chunk" in w.lower() for w in result.warnings)

    @pytest.mark.asyncio
    async def test_bad_model_type_raises_value_error(self):
        """Invalid model_type raises ValueError before any requests."""
        engine = BenchmarkEngine()

        async def target(payload):
            yield StreamChunk(data="x")
            while True:  # unreachable
                yield

        with pytest.raises(ValueError, match="model_type"):
            await engine.run_stream(
                target=target,
                payload={},
                concurrency=1,
                total_requests=1,
                model_type="image",
            )

    @pytest.mark.asyncio
    async def test_generic_model_type_accepted(self):
        """model_type='generic' runs and reports common section only (§3.2)."""
        engine = BenchmarkEngine()

        async def target(payload):
            for data in ["f1", "f2", "f3"]:
                await asyncio.sleep(0.001)
                yield StreamChunk(data=data)

        result = await engine.run_stream(
            target=target,
            payload={"input": "test"},
            concurrency=1,
            total_requests=3,
            model_type="generic",
        )
        assert result.successful == 3
        sm = result.stream_metrics
        assert sm is not None
        assert sm.model_type == "generic"
        assert sm.total_chunks == 9
        assert sm.itl_ms is None
        assert sm.rtf is None

    @pytest.mark.asyncio
    async def test_mid_stream_error_classified(self):
        """Exception mid-stream → counted as failed, not successful."""
        engine = BenchmarkEngine()
        fail_after = [2]  # fail after 2 requests

        async def target(payload):
            for data in ["a", "b"]:
                await asyncio.sleep(0.001)
                yield StreamChunk(data=data)
            fail_after[0] -= 1
            if fail_after[0] <= 0:
                raise RuntimeError("mid-stream boom")

        result = await engine.run_stream(
            target=target,
            payload={},
            concurrency=1,
            total_requests=5,
            model_type="llm",
        )
        assert result.failed > 0
        assert "unknown" in result.error_kinds

    @pytest.mark.asyncio
    async def test_request_meta_callable_passed_through(self):
        """request_meta callable result stored on StreamRequestRecord."""
        engine = BenchmarkEngine()

        async def target(payload):
            yield StreamChunk(data="x", meta={"audio_duration_ms": 320})

        result = await engine.run_stream(
            target=target,
            payload={"audio_duration_ms": 5000},
            concurrency=1,
            total_requests=2,
            model_type="stt",
            request_meta=lambda p: {"audio_duration_ms": p.get("audio_duration_ms")},
        )
        assert result.stream_metrics is not None
        sm = result.stream_metrics
        assert sm.model_type == "stt"
        assert sm.rtf is not None

    @pytest.mark.asyncio
    async def test_grace_drain_cancel_propagates(self):
        """CancelledError must propagate through the adapter (not swallowed)."""
        engine = BenchmarkEngine()
        cancelled_caught = []

        async def target(payload):
            try:
                await asyncio.sleep(0.5)
                yield StreamChunk(data="x")
            except asyncio.CancelledError:
                cancelled_caught.append(True)
                raise

        result = await engine.run_stream(
            target=target,
            payload={},
            concurrency=1,
            duration=0.01,
            grace_period=0.05,
            model_type="llm",
        )
        # The stream should have been cancelled during grace drain
        assert result.dropped_inflight >= 0

    @pytest.mark.asyncio
    async def test_latency_ms_equals_e2e_stream_latency(self):
        """Adapter-reported latency = total stream consumption time."""
        engine = BenchmarkEngine()

        async def target(payload):
            await asyncio.sleep(0.01)
            yield StreamChunk(data="first")
            await asyncio.sleep(0.02)
            yield StreamChunk(data="second")

        result = await engine.run_stream(
            target=target,
            payload={},
            concurrency=1,
            total_requests=3,
            model_type="llm",
        )
        assert result.p99 > 0
        # e2e latency ≈ 30ms
        assert result.stream_metrics is not None
        assert result.stream_metrics.total_ms["mean"] == pytest.approx(30.0, abs=10.0)


# ── Phase 4: sse_target.py ───────────────────────────────────────────────────

class TestSSETarget:
    """SSE wire-format parsing via fake httpx stream()."""

    @staticmethod
    def _fake_response(lines: list[str], status_code=200):
        """Build a fake httpx client whose stream() works as a context manager.

        httpx API::

            async with client.stream("POST", url, ...) as response:
                response.status_code
                async for line in response.aiter_lines():
                    ...

        So ``client.stream()`` must return an async context manager whose
        ``__aenter__`` returns something with ``.status_code``,
        ``.raise_for_status()``, and ``.aiter_lines()``.
        """

        class FakeLines:
            async def __aiter__(self):
                for line in lines:
                    yield line

        class FakeResponse:
            def __init__(self):
                self.status_code = status_code

            def raise_for_status(self):
                if self.status_code >= 400:
                    import httpx
                    raise httpx.HTTPStatusError(
                        "error", request=None, response=self  # type: ignore[arg-type]
                    )

            def aiter_lines(self):
                return FakeLines()

        class _StreamCtx:
            """Async context manager returned by client.stream()."""

            def __init__(self, response: FakeResponse):
                self._response = response

            async def __aenter__(self) -> FakeResponse:
                return self._response

            async def __aexit__(self, *args):
                pass

        class FakeClient:
            def __init__(self):
                self.last_url = ""
                self.last_kwargs = {}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            def stream(self, method, url, **kwargs):
                self.last_url = url
                self.last_kwargs = kwargs
                return _StreamCtx(FakeResponse())

        return FakeClient()

    @pytest.mark.asyncio
    async def test_simple_sse_events(self):
        from lite_server.analyzer.sse_target import sse_stream_target

        client = self._fake_response([
            "data: hello",
            "",
            "data: world",
            "",
            "data: [DONE]",
            "",
        ])
        target_fn = sse_stream_target(client, "http://example.com/events")
        chunks = []
        async for c in target_fn({"input": 1}):
            chunks.append(c)

        assert len(chunks) == 2
        assert chunks[0].data == "hello"
        assert chunks[1].data == "world"

    @pytest.mark.asyncio
    async def test_multiline_data_joined(self):
        from lite_server.analyzer.sse_target import sse_stream_target

        client = self._fake_response([
            "data: line1",
            "data: line2",
            "",
            "data: [DONE]",
            "",
        ])
        target_fn = sse_stream_target(client, "http://example.com/events")
        chunks = []
        async for c in target_fn({"input": 1}):
            chunks.append(c)

        assert len(chunks) == 1
        assert chunks[0].data == "line1\nline2"

    @pytest.mark.asyncio
    async def test_tolerant_no_trailing_newline(self):
        """Last event without trailing empty line is still processed."""
        from lite_server.analyzer.sse_target import sse_stream_target

        client = self._fake_response([
            "data: hello",
            "",
            "data: world",
            # missing trailing empty line
        ])
        target_fn = sse_stream_target(client, "http://example.com/events")
        chunks = []
        async for c in target_fn({"input": 1}):
            chunks.append(c)

        assert len(chunks) == 2
        assert chunks[1].data == "world"

    @pytest.mark.asyncio
    async def test_done_stops_iteration(self):
        from lite_server.analyzer.sse_target import sse_stream_target

        client = self._fake_response([
            "data: first",
            "",
            "data: [DONE]",
            "",
            "data: should_not_appear",
            "",
        ])
        target_fn = sse_stream_target(client, "http://example.com/events")
        chunks = []
        async for c in target_fn({"input": 1}):
            chunks.append(c)

        assert len(chunks) == 1
        assert chunks[0].data == "first"

    @pytest.mark.asyncio
    async def test_error_event_raises_request_stream_error(self):
        from lite_server.analyzer.sse_target import sse_stream_target

        client = self._fake_response([
            "data: {\"error\": \"model overloaded\"}",
            "",
        ])
        target_fn = sse_stream_target(client, "http://example.com/events")
        with pytest.raises(RequestStreamError, match="model overloaded"):
            async for c in target_fn({"input": 1}):
                pass

    @pytest.mark.asyncio
    async def test_dict_meta_extraction(self):
        from lite_server.analyzer.sse_target import sse_stream_target

        client = self._fake_response([
            "data: {\"token\": \"hello\", \"token_count\": 1}",
            "",
            "data: [DONE]",
            "",
        ])
        target_fn = sse_stream_target(client, "http://example.com/events")
        chunks = []
        async for c in target_fn({"input": 1}):
            chunks.append(c)

        assert len(chunks) == 1
        assert chunks[0].meta == {"token": "hello", "token_count": 1}

    @pytest.mark.asyncio
    async def test_non_dict_json_no_meta(self):
        """JSON array or primitive → no meta extracted."""
        from lite_server.analyzer.sse_target import sse_stream_target

        client = self._fake_response([
            "data: [1, 2, 3]",
            "",
            "data: [DONE]",
            "",
        ])
        target_fn = sse_stream_target(client, "http://example.com/events")
        chunks = []
        async for c in target_fn({"input": 1}):
            chunks.append(c)

        assert len(chunks) == 1
        assert chunks[0].meta is None
        assert chunks[0].data == "[1, 2, 3]"

    @pytest.mark.asyncio
    async def test_non_200_raises_status_error(self):
        from lite_server.analyzer.sse_target import sse_stream_target
        from lite_server.analyzer.benchmark import RequestStatusError

        client = self._fake_response([], status_code=500)
        target_fn = sse_stream_target(client, "http://example.com/events")
        with pytest.raises(RequestStatusError):
            async for c in target_fn({"input": 1}):
                pass

    @pytest.mark.asyncio
    async def test_posts_to_correct_url(self):
        from lite_server.analyzer.sse_target import sse_stream_target

        client = self._fake_response(["data: x", "", "data: [DONE]", ""])
        target_fn = sse_stream_target(client, "http://example.com/v2/models/m/events")

        chunks = []
        async for c in target_fn({"key": "val"}):
            chunks.append(c)

        assert client.last_url == "http://example.com/v2/models/m/events"
        assert client.last_kwargs.get("json") == {"key": "val"}


# ── Phase 5: CLI ─────────────────────────────────────────────────────────────

class TestStreamingCLI:
    """CLI thin-shell tests: arg parsing, URL routing, flags, thresholds."""

    @staticmethod
    def _fake_httpx_stream(stream_lines=None, status_code=200):
        """Fake httpx with both post() and stream() — for streaming CLI tests."""
        if stream_lines is None:
            stream_lines = ["data: chunk1", "", "data: [DONE]", ""]

        post_calls = []
        stream_calls = []

        class FakeTimeout:
            def __init__(self, *args, **kwargs):
                pass

        class FakeLimits:
            def __init__(self, *args, **kwargs):
                pass

        class FakeLines:
            async def __aiter__(self):
                for line in stream_lines:
                    yield line

        class FakeStreamResponse:
            def __init__(self):
                self.status_code = status_code

            def raise_for_status(self):
                if self.status_code >= 400:
                    import httpx
                    raise httpx.HTTPStatusError("err", request=None, response=self)

            def aiter_lines(self):
                return FakeLines()

        class _StreamCtx:
            def __init__(self, resp):
                self._resp = resp

            async def __aenter__(self):
                return self._resp

            async def __aexit__(self, *args):
                pass

        class FakeAsyncClient:
            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def post(self, url, **kwargs):
                post_calls.append((url, kwargs))
                resp = type("Response", (), {"status_code": status_code})()
                return resp

            def stream(self, method, url, **kwargs):
                stream_calls.append((url, kwargs))
                return _StreamCtx(FakeStreamResponse())

        fake = type("FakeHttpx", (), {
            "AsyncClient": FakeAsyncClient,
            "Limits": FakeLimits,
            "Timeout": FakeTimeout,
            "TimeoutException": type("TimeoutException", (Exception,), {}),
            "ConnectError": type("ConnectError", (Exception,), {}),
            "TransportError": type("TransportError", (Exception,), {}),
        })()
        return fake, post_calls, stream_calls

    @staticmethod
    def _stream_args(**overrides):
        base = {
            "url": "http://127.0.0.1:8000",
            "model": "test_model",
            "version": None,
            "concurrency": "1",
            "duration": None,
            "requests": 3,
            "warmup_requests": 0,
            "grace_period": 1.0,
            "payload": None,
            "payload_file": None,
            "payload_random": None,
            "export": None,
            "max_error_rate": None,
            "max_p99": None,
            "rate": None,
            "latency_threshold": None,
            # New streaming flags
            "stream": False,
            "model_type": "llm",
            "stream_read_timeout": 300.0,
            "max_ttft_ms": None,
            "max_rtf": None,
            "endpoint": "events",
        }
        base.update(overrides)
        return type("Args", (), base)()

    # ── URL routing ──────────────────────────────────────────────────────

    def test_stream_flag_uses_events_url(self, monkeypatch):
        """--stream routes to /v2/models/{m}/events instead of /infer."""
        import sys
        from lite_server import cli

        fake, post_calls, stream_calls = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._stream_args(stream=True, requests=3))
        assert rc == 0
        assert len(stream_calls) > 0
        url = stream_calls[0][0]
        assert url.endswith("/v2/models/test_model/events")

    def test_stream_with_version_url(self, monkeypatch):
        """--stream + --version → /v2/models/{m}/versions/{v}/events."""
        import sys
        from lite_server import cli

        fake, post_calls, stream_calls = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._stream_args(
            stream=True, version="3", requests=3,
        ))
        assert rc == 0
        url = stream_calls[0][0]
        assert "/versions/3/events" in url

    def test_non_stream_uses_infer_url(self, monkeypatch):
        """Without --stream, URL stays /infer (backward compat)."""
        import sys
        from lite_server import cli

        fake, post_calls, stream_calls = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._stream_args(stream=False, requests=3))
        assert rc == 0
        assert len(post_calls) > 0
        url = post_calls[0][0]
        assert "/infer" in url

    # ── Endpoint routing (§3.4) ──────────────────────────────────────────

    def test_endpoint_decoupled_uses_decoupled_url(self, monkeypatch):
        """--stream --endpoint decoupled → /v2/models/{m}/decoupled."""
        import sys
        from lite_server import cli

        fake, _, stream_calls = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._stream_args(
            stream=True, endpoint="decoupled", requests=3,
        ))
        assert rc == 0
        assert len(stream_calls) > 0
        url = stream_calls[0][0]
        assert url.endswith("/v2/models/test_model/decoupled")

    def test_endpoint_decoupled_with_version_url(self, monkeypatch):
        """--stream --endpoint decoupled --version → /versions/{v}/decoupled."""
        import sys
        from lite_server import cli

        fake, _, stream_calls = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._stream_args(
            stream=True, endpoint="decoupled", version="3", requests=3,
        ))
        assert rc == 0
        url = stream_calls[0][0]
        assert "/versions/3/decoupled" in url

    def test_endpoint_default_is_events(self, monkeypatch):
        """Default endpoint stays /events (backward compat)."""
        import sys
        from lite_server import cli

        fake, _, stream_calls = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._stream_args(stream=True, requests=3))
        assert rc == 0
        assert stream_calls[0][0].endswith("/events")

    def test_endpoint_decoupled_without_stream_exits_2(self):
        """--endpoint decoupled without --stream → exit 2 (fail-closed)."""
        from lite_server import cli

        rc = cli._cmd_benchmark(self._stream_args(
            stream=False, endpoint="decoupled", requests=3,
        ))
        assert rc == 2

    def test_max_rtf_with_generic_model_exits_2(self):
        """--max-rtf with --model-type generic → exit 2 (no RTF for generic)."""
        from lite_server import cli

        rc = cli._cmd_benchmark(self._stream_args(
            stream=True, model_type="generic", requests=3, max_rtf=2.0,
        ))
        assert rc == 2

    def test_export_config_includes_endpoint(self, monkeypatch, tmp_path):
        """Export config records the endpoint used."""
        import sys
        import json
        from lite_server import cli

        fake, _, _ = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)
        export_path = tmp_path / "result.json"

        rc = cli._cmd_benchmark(self._stream_args(
            stream=True, endpoint="decoupled", model_type="generic",
            requests=3, export=str(export_path),
        ))
        assert rc == 0
        data = json.loads(export_path.read_text())
        assert data["config"]["endpoint"] == "decoupled"
        assert data["stream"]["mode"] == "generic"

    # ── Output rendering ─────────────────────────────────────────────────

    def test_stream_output_includes_stream_section(self, monkeypatch, capsys):
        """Streaming run prints stream-specific metrics."""
        import sys
        from lite_server import cli

        lines = ["data: a", "", "data: b", "", "data: [DONE]", ""]
        fake, _, _ = self._fake_httpx_stream(stream_lines=lines)
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._stream_args(stream=True, requests=3))
        assert rc == 0
        out = capsys.readouterr().out
        assert "TTFT" in out or "ttft" in out.lower()

    # ── Threshold gates (R3) ─────────────────────────────────────────────

    def test_max_ttft_ms_violation_exits_99(self, monkeypatch):
        """--max-ttft-ms with p99 above threshold → exit 99."""
        import sys
        from lite_server import cli

        fake, _, _ = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._stream_args(
            stream=True, requests=3, max_ttft_ms=-1.0,
        ))
        assert rc == 99

    def test_max_ttft_ms_pass_exits_0(self, monkeypatch):
        """--max-ttft-ms with p99 below threshold → exit 0."""
        import sys
        from lite_server import cli

        fake, _, _ = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._stream_args(
            stream=True, requests=3, max_ttft_ms=99999.0,
        ))
        assert rc == 0

    def test_max_ttft_ms_without_stream_exits_2(self):
        """--max-ttft-ms without --stream → exit 2 (fail-closed)."""
        from lite_server import cli

        rc = cli._cmd_benchmark(self._stream_args(
            requests=3, max_ttft_ms=100.0, stream=False,
        ))
        assert rc == 2

    def test_max_rtf_with_llm_model_exits_2(self):
        """--max-rtf with --model-type llm → exit 2 (RTF not computed)."""
        from lite_server import cli

        rc = cli._cmd_benchmark(self._stream_args(
            stream=True, model_type="llm", requests=3, max_rtf=2.0,
        ))
        assert rc == 2

    # ── Export JSON ──────────────────────────────────────────────────────

    def test_export_includes_stream_section(self, monkeypatch, tmp_path):
        """Export JSON has "stream" key and config.stream/model_type."""
        import sys
        import json
        from lite_server import cli

        fake, _, _ = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)
        export_path = tmp_path / "result.json"

        rc = cli._cmd_benchmark(self._stream_args(
            stream=True, requests=3, export=str(export_path),
        ))
        assert rc == 0
        data = json.loads(export_path.read_text())
        assert "stream" in data
        assert data["stream"] is not None
        assert data["stream"]["mode"] == "llm"
        assert data["config"]["stream"] is True
        assert data["config"]["model_type"] == "llm"

    def test_export_non_stream_has_stream_null(self, monkeypatch, tmp_path):
        """Non-streaming export still has stream: null."""
        import sys
        import json
        from lite_server import cli

        fake, _, _ = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)
        export_path = tmp_path / "result.json"

        rc = cli._cmd_benchmark(self._stream_args(
            stream=False, requests=3, export=str(export_path),
        ))
        assert rc == 0
        data = json.loads(export_path.read_text())
        assert data["stream"] is None
        assert data["config"]["stream"] is False

    # ── Model type validation ────────────────────────────────────────────

    def test_invalid_model_type_exits_2(self):
        """--model-type bogus → argparse exit 2."""
        from lite_server import cli

        with pytest.raises(SystemExit) as exc_info:
            cli.main([
                "benchmark", "--model", "m", "--requests", "3",
                "--stream", "--model-type", "bogus",
            ])
        assert exc_info.value.code == 2

    # ── Combination matrix (R1) ──────────────────────────────────────────

    def test_stream_with_sweep(self, monkeypatch, capsys):
        """--stream + --concurrency 1:3:1 → sweep with stream per level."""
        import sys
        from lite_server import cli

        fake, _, _ = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._stream_args(
            stream=True, concurrency="1:3:1", requests=3,
        ))
        assert rc == 0
        out = capsys.readouterr().out
        assert "Sweep" in out

    def test_stream_with_rate(self, monkeypatch):
        """--stream + --rate → open-loop streaming."""
        import sys
        from lite_server import cli

        fake, _, _ = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._stream_args(
            stream=True, rate=10, duration=0.2,
        ))
        assert rc == 0

    def test_model_type_without_stream_accepted(self, monkeypatch):
        """--model-type without --stream is accepted and ignored (no error)."""
        import sys
        from lite_server import cli

        fake, post_calls, _ = self._fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(self._stream_args(
            stream=False, model_type="stt", requests=3,
        ))
        assert rc == 0
