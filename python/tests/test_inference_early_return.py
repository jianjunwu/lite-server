"""Tests for ResponseWithHeaders early-return detection in inference pipeline."""

import json
import logging
import pytest
from lite_server.api import LitAPI, RequestMeta, ResponseWithHeaders
from lite_server.response import Response
from lite_server.callback import Callback, CallbackRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_meta(route="/predict", payload=None):
    return RequestMeta(
        route=route,
        headers={"content-type": "application/json"},
        client_ip="127.0.0.1",
        request_id="req-1",
        timestamp_ns=123456789,
        payload=payload or {"input": "hello"},
    )


def _make_log():
    return logging.getLogger("test_inference_early_return")


# ---------------------------------------------------------------------------
# _check_early_return unit tests
# ---------------------------------------------------------------------------

class TestCheckEarlyReturn:
    """Tests for _check_early_return helper function."""

    def test_normal_value_returns_unchanged(self):
        from lite_server.worker.inference import _check_early_return

        body, headers, is_early = _check_early_return({"result": "ok"})
        assert body == {"result": "ok"}
        assert headers is None
        assert is_early is False

    def test_response_with_headers_detected(self):
        from lite_server.worker.inference import _check_early_return

        rwh = ResponseWithHeaders(body={"msg": "done"}, headers={"X-Custom": "1"})
        body, headers, is_early = _check_early_return(rwh)
        assert body == {"msg": "done"}
        assert headers == {"X-Custom": "1"}
        assert is_early is True

    def test_response_with_headers_empty_headers(self):
        from lite_server.worker.inference import _check_early_return

        rwh = ResponseWithHeaders(body={"msg": "done"})
        body, headers, is_early = _check_early_return(rwh)
        assert body == {"msg": "done"}
        assert headers is None
        assert is_early is True

    def test_string_body(self):
        from lite_server.worker.inference import _check_early_return

        rwh = ResponseWithHeaders(body="plain text", headers={"X-Trace": "abc"})
        body, headers, is_early = _check_early_return(rwh)
        assert body == "plain text"
        assert headers == {"X-Trace": "abc"}
        assert is_early is True

    def test_none_body(self):
        from lite_server.worker.inference import _check_early_return

        rwh = ResponseWithHeaders(body=None, headers={"X-Empty": "1"})
        body, headers, is_early = _check_early_return(rwh)
        assert body is None
        assert headers == {"X-Empty": "1"}
        assert is_early is True

    def test_none_value_is_not_early_return(self):
        from lite_server.worker.inference import _check_early_return

        body, headers, is_early = _check_early_return(None)
        assert body is None
        assert headers is None
        assert is_early is False

    def test_list_value_is_not_early_return(self):
        from lite_server.worker.inference import _check_early_return

        body, headers, is_early = _check_early_return([1, 2, 3])
        assert body == [1, 2, 3]
        assert headers is None
        assert is_early is False

    # ---- Response (plain, not ResponseWithHeaders) ----

    def test_response_detected_as_early_return(self):
        from lite_server.worker.inference import _check_early_return

        resp = Response(content={"msg": "error"}, status_code=401)
        body, headers, is_early = _check_early_return(resp)
        assert body == {"msg": "error"}
        assert headers == {"_sc": "401"}
        assert is_early is True

    def test_response_status_200_no_sc_header(self):
        from lite_server.worker.inference import _check_early_return

        resp = Response(content={"msg": "ok"}, status_code=200)
        body, headers, is_early = _check_early_return(resp)
        assert body == {"msg": "ok"}
        assert headers is None
        assert is_early is True

    def test_response_with_custom_headers(self):
        from lite_server.worker.inference import _check_early_return

        resp = Response(content="done", status_code=200, headers={"X-Custom": "1"})
        body, headers, is_early = _check_early_return(resp)
        assert body == "done"
        assert headers == {"X-Custom": "1"}
        assert is_early is True

    def test_response_with_non_json_media_type(self):
        from lite_server.worker.inference import _check_early_return

        resp = Response(content="<h1>hi</h1>", media_type="text/html")
        body, headers, is_early = _check_early_return(resp)
        assert body == "<h1>hi</h1>"
        assert headers == {"_mt": "text/html"}
        assert is_early is True


# ---------------------------------------------------------------------------
# _serialize_early_return unit tests
# ---------------------------------------------------------------------------

class TestSerializeEarlyReturn:
    """Tests for _serialize_early_return helper function."""

    def test_serializes_dict_body(self):
        from lite_server.worker.inference import _serialize_early_return

        class FakeAPI:
            _metric_specs = []
            _metric_values = []

        api = FakeAPI()
        resp_bytes, status, metrics, headers = _serialize_early_return(
            {"msg": "ok"}, {"X-Custom": "1"}, api
        )
        assert resp_bytes == b'{"msg": "ok"}'
        assert status.code == "Ok"
        assert headers == {"X-Custom": "1"}

    def test_serializes_with_none_headers(self):
        from lite_server.worker.inference import _serialize_early_return

        class FakeAPI:
            _metric_specs = []
            _metric_values = []

        api = FakeAPI()
        resp_bytes, status, metrics, headers = _serialize_early_return(
            {"result": 42}, None, api
        )
        assert resp_bytes == b'{"result": 42}'
        assert status.code == "Ok"
        assert headers is None

    def test_collects_metrics(self):
        from lite_server.worker.inference import _serialize_early_return
        from lite_server.api import _MetricSpec

        class FakeAPI:
            _metric_specs = [_MetricSpec("my_gauge", "gauge", 0)]
            _metric_values = [(0, 3.14)]

        api = FakeAPI()
        resp_bytes, status, metrics, headers = _serialize_early_return(
            "done", None, api
        )
        assert resp_bytes == b'"done"'
        assert metrics is not None
        assert len(metrics.gauges) == 1
        # protobuf float (f32) has limited precision for 3.14
        assert metrics.gauges[0].value == pytest.approx(3.14, rel=1e-5)


# ---------------------------------------------------------------------------
# _run_predict early-return integration tests
# ---------------------------------------------------------------------------

class _TrackerAPI(LitAPI):
    """A minimal LitAPI that records which methods were called."""

    def __init__(self):
        super().__init__()
        self.calls = []
        self._callback_runner = None

    def decode_request(self, request):
        self.calls.append("decode_request")
        return request

    def predict(self, x):
        self.calls.append("predict")
        return x

    def encode_response(self, output):
        self.calls.append("encode_response")
        return output

    def on_request(self, request, meta):
        self.calls.append("on_request")
        return request

    def on_response(self, response, meta):
        self.calls.append("on_response")
        return response


class TestRunPredictEarlyReturn:
    """Tests that _run_predict short-circuits when any stage returns ResponseWithHeaders."""

    def _run(self, lit_api, data=None, meta=None):
        from lite_server.worker.inference import _run_predict

        if data is None:
            data = json.dumps({"input": "hello"}).encode()
        if meta is None:
            meta = _make_meta()
        return _run_predict(lit_api, data, meta, _make_log())

    # ---- LitAPI on_request ----

    def test_early_return_at_on_request(self):
        api = _TrackerAPI()
        api.on_request = lambda req, meta: ResponseWithHeaders(
            body={"early": True}, headers={"X-Stage": "on_request"}
        )

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"early": true}'
        assert headers == {"X-Stage": "on_request"}
        # decode_request, predict, encode_response must NOT be called
        assert "decode_request" not in api.calls
        assert "predict" not in api.calls
        assert "encode_response" not in api.calls

    # ---- Callback: on_before_decode ----

    def test_early_return_at_on_before_decode(self):
        class EarlyCallback(Callback):
            def on_before_decode(self, request, meta):
                return ResponseWithHeaders(body={"from_cb": 1}, headers={"X-Cb": "before_decode"})

        api = _TrackerAPI()
        api._callback_runner = CallbackRunner([EarlyCallback()])

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"from_cb": 1}'
        assert headers == {"X-Cb": "before_decode"}
        assert "decode_request" not in api.calls
        assert "predict" not in api.calls

    # ---- LitAPI decode_request ----

    def test_early_return_at_decode_request(self):
        api = _TrackerAPI()
        api.decode_request = lambda req: ResponseWithHeaders(
            body={"decoded": True}, headers={"X-Stage": "decode_request"}
        )

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"decoded": true}'
        assert headers == {"X-Stage": "decode_request"}
        assert "predict" not in api.calls
        assert "encode_response" not in api.calls

    # ---- Callback: on_after_decode ----

    def test_early_return_at_on_after_decode(self):
        class EarlyCallback(Callback):
            def on_after_decode(self, decoded, meta):
                return ResponseWithHeaders(body={"from_cb": 2}, headers={"X-Cb": "after_decode"})

        api = _TrackerAPI()
        api._callback_runner = CallbackRunner([EarlyCallback()])

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"from_cb": 2}'
        assert headers == {"X-Cb": "after_decode"}
        assert "predict" not in api.calls

    # ---- Callback: on_before_predict ----

    def test_early_return_at_on_before_predict(self):
        class EarlyCallback(Callback):
            def on_before_predict(self, decoded, meta):
                return ResponseWithHeaders(body={"from_cb": 3}, headers={"X-Cb": "before_predict"})

        api = _TrackerAPI()
        api._callback_runner = CallbackRunner([EarlyCallback()])

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"from_cb": 3}'
        assert headers == {"X-Cb": "before_predict"}
        assert "predict" not in api.calls

    # ---- LitAPI predict ----

    def test_early_return_at_predict(self):
        api = _TrackerAPI()
        api.predict = lambda x: ResponseWithHeaders(
            body={"predicted": True}, headers={"X-Stage": "predict"}
        )

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"predicted": true}'
        assert headers == {"X-Stage": "predict"}
        assert "encode_response" not in api.calls

    # ---- Callback: on_after_predict ----

    def test_early_return_at_on_after_predict(self):
        class EarlyCallback(Callback):
            def on_after_predict(self, output, meta):
                return ResponseWithHeaders(body={"from_cb": 4}, headers={"X-Cb": "after_predict"})

        api = _TrackerAPI()
        api._callback_runner = CallbackRunner([EarlyCallback()])

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"from_cb": 4}'
        assert headers == {"X-Cb": "after_predict"}
        assert "encode_response" not in api.calls

    # ---- Callback: on_before_encode ----

    def test_early_return_at_on_before_encode(self):
        class EarlyCallback(Callback):
            def on_before_encode(self, output, meta):
                return ResponseWithHeaders(body={"from_cb": 5}, headers={"X-Cb": "before_encode"})

        api = _TrackerAPI()
        api._callback_runner = CallbackRunner([EarlyCallback()])

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"from_cb": 5}'
        assert headers == {"X-Cb": "before_encode"}
        assert "encode_response" not in api.calls

    # ---- LitAPI encode_response ----

    def test_early_return_at_encode_response(self):
        api = _TrackerAPI()
        api.encode_response = lambda out: ResponseWithHeaders(
            body={"encoded": True}, headers={"X-Stage": "encode_response"}
        )

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"encoded": true}'
        assert headers == {"X-Stage": "encode_response"}

    # ---- Callback: on_after_encode ----

    def test_early_return_at_on_after_encode(self):
        class EarlyCallback(Callback):
            def on_after_encode(self, encoded, meta):
                return ResponseWithHeaders(body={"from_cb": 6}, headers={"X-Cb": "after_encode"})

        api = _TrackerAPI()
        api._callback_runner = CallbackRunner([EarlyCallback()])

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"from_cb": 6}'
        assert headers == {"X-Cb": "after_encode"}

    # ---- LitAPI on_response ----

    def test_early_return_at_on_response(self):
        api = _TrackerAPI()
        api.on_response = lambda resp, meta: ResponseWithHeaders(
            body={"final": True}, headers={"X-Stage": "on_response"}
        )

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"final": true}'
        assert headers == {"X-Stage": "on_response"}

    # ---- LitAPI decode_request with plain Response ----

    def test_early_return_at_decode_request_with_plain_response(self):
        api = _TrackerAPI()
        api.decode_request = lambda req: Response(
            content={"error": "bad input"}, status_code=400
        )

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"error": "bad input"}'
        assert headers == {"_sc": "400"}
        assert "predict" not in api.calls
        assert "encode_response" not in api.calls

    # ---- LitAPI predict with plain Response ----

    def test_early_return_at_predict_with_plain_response(self):
        api = _TrackerAPI()
        api.predict = lambda x: Response(
            content={"rejected": True}, status_code=422
        )

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"rejected": true}'
        assert headers == {"_sc": "422"}
        assert "encode_response" not in api.calls

    # ---- No early return (normal flow) ----

    def test_normal_flow_still_works(self):
        api = _TrackerAPI()

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"input": "hello"}'
        assert headers is None
        assert "decode_request" in api.calls
        assert "predict" in api.calls
        assert "encode_response" in api.calls
        assert "on_request" in api.calls
        assert "on_response" in api.calls

    # ---- No callback runner at all ----

    def test_no_callback_runner_normal_flow(self):
        """Normal flow works when _callback_runner is None."""
        api = _TrackerAPI()
        api._callback_runner = None

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"input": "hello"}'
        assert headers is None

    # ---- Callback chain: pipeline stops after hook stage, but all
    #      callbacks for that stage are still invoked (exception isolation) ----

    def test_early_return_stops_pipeline_not_callback_chain(self):
        """When callback_A returns early at on_before_decode, the pipeline
        short-circuits after the runner returns.  But callback_B's
        on_before_decode IS still called by the runner (exception isolation
        guarantees every callback runs).  Callback_B's later-stage hooks
        (e.g. on_after_encode) are NOT called because the pipeline stops."""
        calls = []

        class CallbackA(Callback):
            def on_before_decode(self, request, meta):
                calls.append("A_on_before_decode")
                return ResponseWithHeaders(body={"early": True}, headers={"X-Cb": "A"})

        class CallbackB(Callback):
            def on_before_decode(self, request, meta):
                calls.append("B_on_before_decode")
                return request

            def on_after_encode(self, encoded, meta):
                calls.append("B_on_after_encode")
                return encoded

        api = _TrackerAPI()
        api._callback_runner = CallbackRunner([CallbackA(), CallbackB()])

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"early": true}'
        assert headers == {"X-Cb": "A"}
        # Both callbacks run for on_before_decode (exception isolation)
        assert "A_on_before_decode" in calls
        assert "B_on_before_decode" in calls
        # But pipeline stops — later stages are never reached
        assert "B_on_after_encode" not in calls
        assert "predict" not in api.calls

    # ---- Exception in callback before early-return callback ----

    def test_failing_callback_before_early_return_callback(self):
        """If an earlier callback raises, the next callback can still
        trigger early return (exception isolation)."""
        class BadCallback(Callback):
            def on_before_decode(self, request, meta):
                raise RuntimeError("boom")

        class EarlyCallback(Callback):
            def on_before_decode(self, request, meta):
                return ResponseWithHeaders(body={"recovered": True}, headers={"X-Recovered": "1"})

        api = _TrackerAPI()
        api._callback_runner = CallbackRunner([BadCallback(), EarlyCallback()])

        resp_bytes, status, metrics, headers = self._run(api)
        assert resp_bytes == b'{"recovered": true}'
        assert headers == {"X-Recovered": "1"}


# ---------------------------------------------------------------------------
# _run_predict_async early-return integration tests
# ---------------------------------------------------------------------------

class _AsyncTrackerAPI(LitAPI):
    def __init__(self):
        super().__init__()
        self.calls = []
        self._callback_runner = None

    async def decode_request(self, request):
        self.calls.append("decode_request")
        return request

    async def predict(self, x):
        self.calls.append("predict")
        return x

    async def encode_response(self, output):
        self.calls.append("encode_response")
        return output

    async def on_request(self, request, meta):
        self.calls.append("on_request")
        return request

    async def on_response(self, response, meta):
        self.calls.append("on_response")
        return response


class TestRunPredictAsyncEarlyReturn:
    """Tests that _run_predict_async short-circuits when any stage returns ResponseWithHeaders."""

    async def _run(self, lit_api, data=None, meta=None):
        from lite_server.worker.inference import _run_predict_async

        if data is None:
            data = json.dumps({"input": "hello"}).encode()
        if meta is None:
            meta = _make_meta()
        return await _run_predict_async(lit_api, data, meta, _make_log())

    @pytest.mark.asyncio
    async def test_early_return_at_on_request(self):
        api = _AsyncTrackerAPI()
        api.on_request = _make_async(lambda req, meta: ResponseWithHeaders(
            body={"early": True}, headers={"X-Stage": "on_request"}
        ))

        resp_bytes, status, metrics, headers = await self._run(api)
        assert resp_bytes == b'{"early": true}'
        assert headers == {"X-Stage": "on_request"}
        assert "decode_request" not in api.calls
        assert "predict" not in api.calls

    @pytest.mark.asyncio
    async def test_early_return_at_on_before_decode_callback(self):
        class EarlyCallback(Callback):
            def on_before_decode(self, request, meta):
                return ResponseWithHeaders(body={"cb": 1}, headers={"X-Cb": "before_decode"})

        api = _AsyncTrackerAPI()
        api._callback_runner = CallbackRunner([EarlyCallback()])

        resp_bytes, status, metrics, headers = await self._run(api)
        assert resp_bytes == b'{"cb": 1}'
        assert headers == {"X-Cb": "before_decode"}
        assert "decode_request" not in api.calls

    @pytest.mark.asyncio
    async def test_early_return_at_decode_request(self):
        api = _AsyncTrackerAPI()
        api.decode_request = _make_async(lambda req: ResponseWithHeaders(
            body={"decoded": True}, headers={"X-Stage": "decode"}
        ))

        resp_bytes, status, metrics, headers = await self._run(api)
        assert resp_bytes == b'{"decoded": true}'
        assert headers == {"X-Stage": "decode"}
        assert "predict" not in api.calls

    @pytest.mark.asyncio
    async def test_early_return_at_on_after_decode_async_callback(self):
        class AsyncEarlyCallback(Callback):
            async def on_after_decode(self, decoded, meta):
                return ResponseWithHeaders(body={"cb": 2}, headers={"X-Cb": "after_decode"})

        api = _AsyncTrackerAPI()
        api._callback_runner = CallbackRunner([AsyncEarlyCallback()])

        resp_bytes, status, metrics, headers = await self._run(api)
        assert resp_bytes == b'{"cb": 2}'
        assert headers == {"X-Cb": "after_decode"}
        assert "predict" not in api.calls

    @pytest.mark.asyncio
    async def test_early_return_at_predict(self):
        api = _AsyncTrackerAPI()
        api.predict = _make_async(lambda x: ResponseWithHeaders(
            body={"pred": True}, headers={"X-Stage": "predict"}
        ))

        resp_bytes, status, metrics, headers = await self._run(api)
        assert resp_bytes == b'{"pred": true}'
        assert headers == {"X-Stage": "predict"}
        assert "encode_response" not in api.calls

    @pytest.mark.asyncio
    async def test_early_return_at_encode_response(self):
        api = _AsyncTrackerAPI()
        api.encode_response = _make_async(lambda out: ResponseWithHeaders(
            body={"enc": True}, headers={"X-Stage": "encode"}
        ))

        resp_bytes, status, metrics, headers = await self._run(api)
        assert resp_bytes == b'{"enc": true}'
        assert headers == {"X-Stage": "encode"}

    @pytest.mark.asyncio
    async def test_early_return_at_on_response(self):
        api = _AsyncTrackerAPI()
        api.on_response = _make_async(lambda resp, meta: ResponseWithHeaders(
            body={"final": True}, headers={"X-Stage": "on_response"}
        ))

        resp_bytes, status, metrics, headers = await self._run(api)
        assert resp_bytes == b'{"final": true}'
        assert headers == {"X-Stage": "on_response"}

    @pytest.mark.asyncio
    async def test_normal_flow_still_works(self):
        api = _AsyncTrackerAPI()

        resp_bytes, status, metrics, headers = await self._run(api)
        assert resp_bytes == b'{"input": "hello"}'
        assert headers is None
        assert "decode_request" in api.calls
        assert "predict" in api.calls
        assert "encode_response" in api.calls


def _make_async(fn):
    """Wrap a sync function into an async function."""
    async def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)
    return wrapper
