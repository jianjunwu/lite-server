"""Audit tests for PR-4 streaming benchmark (commit 8cf6768).

Each test reproduces one confirmed defect against
.claude/streaming-benchmark-plan.md.  All tests FAIL on the current
implementation — they are the evidence, not new features.

Defect index:
  B1  --stream-read-timeout never reaches httpx (plan §1.5/§1.6)
  B2  warmup stream records pollute stream metrics (D4/run() contract)
  B3  mixed token_count_basis adds no result warning (§1.4 R2/§1.13 R2)
  B4  all-zero-chunk + window → tokens_per_sec_aggregate == 0.0, not None (§1.4 R5)
  B5  SSE non-200 non-error status (201/3xx) silently treated as success (§1.5)
"""

import sys

import pytest

from lite_server.analyzer.benchmark import (
    BenchmarkEngine,
    RequestStatusError,
    StreamChunk,
    StreamRequestRecord,
)
from lite_server.analyzer.stream_metrics import compute_stream_metrics
from lite_server.analyzer.sse_target import sse_stream_target


# ── Minimal fake httpx (self-contained; mirrors test_streaming_benchmark.py) ──

def _fake_httpx_stream(stream_lines=None, status_code=200):
    """Fake httpx recording AsyncClient ctor kwargs and stream() kwargs."""
    if stream_lines is None:
        stream_lines = ["data: chunk1", "", "data: [DONE]", ""]

    client_ctor_kwargs = []
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
            # Real httpx raises only for 4xx/5xx
            if self.status_code >= 400:
                import httpx
                raise httpx.HTTPStatusError("err", request=None, response=self)

        def aiter_lines(self):
            return FakeLines()

    class _StreamCtx:
        async def __aenter__(self):
            return FakeStreamResponse()

        async def __aexit__(self, *args):
            pass

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            client_ctor_kwargs.append(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def stream(self, method, url, **kwargs):
            stream_calls.append((url, kwargs))
            return _StreamCtx()

    fake = type("FakeHttpx", (), {
        "AsyncClient": FakeAsyncClient,
        "Limits": FakeLimits,
        "Timeout": FakeTimeout,
        "TimeoutException": type("TimeoutException", (Exception,), {}),
        "ConnectError": type("ConnectError", (Exception,), {}),
        "TransportError": type("TransportError", (Exception,), {}),
    })()
    return fake, client_ctor_kwargs, stream_calls


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
        "stream": False,
        "model_type": "llm",
        "stream_read_timeout": 300.0,
        "max_ttft_ms": None,
        "max_rtf": None,
    }
    base.update(overrides)
    return type("Args", (), base)()


# ── B1: --stream-read-timeout never reaches httpx ────────────────────────────

class TestAuditStreamReadTimeout:
    def test_stream_read_timeout_reaches_transport(self, monkeypatch):
        """Plan §1.6: streaming timeout must use stream_read_timeout.

        The CLI builds ``httpx.Timeout(args.stream_read_timeout, ...)`` but
        never passes it to the streaming path — neither as an AsyncClient
        default nor to ``client.stream(...)``.  httpx's built-in default
        (5 s read) therefore applies, so any TTFT/inter-chunk gap > 5 s
        fails with ReadTimeout regardless of the flag.
        """
        from lite_server import cli

        fake, client_ctor_kwargs, stream_calls = _fake_httpx_stream()
        monkeypatch.setitem(sys.modules, "httpx", fake)

        rc = cli._cmd_benchmark(_stream_args(
            stream=True, requests=2, stream_read_timeout=123.0,
        ))
        assert rc == 0
        assert stream_calls, "streaming endpoint was never called"

        client_timeout = client_ctor_kwargs[0].get("timeout") if client_ctor_kwargs else None
        request_timeout = stream_calls[0][1].get("timeout")
        assert client_timeout is not None or request_timeout is not None, (
            "--stream-read-timeout=123.0 was dropped: neither AsyncClient "
            "nor client.stream() received any timeout (httpx default 5s applies)"
        )


# ── B2: warmup stream records pollute stream metrics ─────────────────────────

class TestAuditWarmupPollution:
    @pytest.mark.asyncio
    async def test_warmup_records_excluded_from_stream_metrics(self):
        """run() discards warmup samples; stream metrics must do the same.

        The adapter appends a StreamRequestRecord for every call — including
        warmup calls run() makes before the measurement window — so warmup
        TTFT/chunk/token data contaminates every stream percentile.
        """
        engine = BenchmarkEngine()

        async def target(payload):
            yield StreamChunk(data="x", meta={"token_count": 1})

        result = await engine.run_stream(
            target=target,
            payload={"input": "test"},
            concurrency=1,
            total_requests=3,
            warmup_requests=2,
            model_type="llm",
        )
        sm = result.stream_metrics
        assert result.successful == 3
        assert sm.requests == 3, (
            f"warmup records leaked into stream metrics: "
            f"sm.requests={sm.requests} but only 3 measured requests ran"
        )
        assert sm.total_chunks == 3


# ── B3: mixed token_count_basis adds no result warning ───────────────────────

class TestAuditMixedBasisWarning:
    @pytest.mark.asyncio
    async def test_mixed_basis_produces_estimation_warning(self):
        """Plan §1.4 (R2) + §1.13 R2: mixed basis must add a result warning
        about estimation pollution.  docs/cli.md already promises "metrics
        still compute with a warning" — no warning is emitted today.
        """
        engine = BenchmarkEngine()
        calls = [0]

        async def target(payload):
            calls[0] += 1
            if calls[0] % 2 == 1:
                yield StreamChunk(data="a", meta={"token_count": 1})
            else:
                yield StreamChunk(data="b")  # no token_count meta

        result = await engine.run_stream(
            target=target,
            payload={},
            concurrency=1,
            total_requests=2,
            model_type="llm",
        )
        sm = result.stream_metrics
        assert sm.token_count_basis == "mixed"
        assert any(
            "mixed" in w.lower() or "estimat" in w.lower()
            for w in result.warnings
        ), f"expected a mixed-basis estimation warning, got: {result.warnings}"


# ── B4: all-zero-chunk + window → aggregate 0.0 instead of None ──────────────

class TestAuditZeroChunkAggregate:
    def test_all_zero_chunk_requests_aggregate_is_none(self):
        """Plan §1.4 (R5): when all requests are zero-chunk, every
        tokens_per_sec* metric must be None (no meaningful data), not 0.0.
        decode/e2e variants guard on ms > 0 and correctly yield None;
        the aggregate path does not guard and returns 0.0.
        """
        records = [
            StreamRequestRecord(chunk_count=0, total_bytes=0),
            StreamRequestRecord(chunk_count=0, total_bytes=0),
        ]
        sm = compute_stream_metrics(records, "llm", window_secs=10.0)
        assert sm.requests == 2
        assert sm.zero_chunk_requests == 2
        assert sm.tokens_per_sec is None
        assert sm.tokens_per_sec_e2e is None
        assert sm.tokens_per_sec_aggregate is None, (
            f"aggregate throughput over zero tokens should be None, "
            f"got {sm.tokens_per_sec_aggregate}"
        )


# ── B5: SSE non-200 non-error status silently treated as success ─────────────

class TestAuditNon200Status:
    @pytest.mark.asyncio
    async def test_status_201_raises_request_status_error(self):
        """Plan §1.5: any non-200 status → RequestStatusError.

        The guard relies on ``response.raise_for_status()`` throwing, but
        httpx only raises for 4xx/5xx — a 201/3xx (e.g. proxy or
        misconfigured gateway) falls through and its body is parsed as SSE
        instead of failing the request.
        """
        class FakeResponse:
            status_code = 201

            def raise_for_status(self):
                pass  # real httpx: no-op for 2xx

            def aiter_lines(self):
                async def _gen():
                    yield "data: hi"
                    yield ""
                return _gen()

        class FakeCtx:
            async def __aenter__(self):
                return FakeResponse()

            async def __aexit__(self, *args):
                pass

        class FakeClient:
            def stream(self, method, url, **kwargs):
                return FakeCtx()

        target = sse_stream_target(FakeClient(), "http://x/events")
        with pytest.raises(RequestStatusError):
            async for _ in target({}):
                pass
