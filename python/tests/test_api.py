"""Tests for lite_server.api — Enhanced LitAPI."""

import logging
from typing import Any

import pytest


class TestLitAPIBasics:
    """LitAPI subclassing and parameter forwarding."""

    def test_can_instantiate_subclass(self):
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device):
                pass

            def decode_request(self, request):
                return request

            def predict(self, x):
                return x

            def encode_response(self, output):
                return output

        api = Dummy()
        assert api is not None

    def test_constructor_params_stored(self):
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        api = Dummy(max_batch_size=4, batch_timeout=0.01, stream=True)
        assert api.max_batch_size == 4
        assert api.batch_timeout == 0.01
        assert api.stream is True

    def test_config_attribute_exists(self):
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        api = Dummy()
        assert hasattr(api, "config")
        assert api.config == {}


class TestTeardown:
    """teardown() lifecycle hook."""

    def test_teardown_exists_and_is_callable(self):
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        api = Dummy()
        assert hasattr(api, "teardown")
        assert callable(api.teardown)

    def test_default_teardown_does_nothing(self):
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        api = Dummy()
        result = api.teardown()
        assert result is None

    def test_custom_teardown_is_called(self):
        from lite_server.api import LitAPI

        called = []

        class Dummy(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

            def teardown(self):
                called.append(True)

        api = Dummy()
        api.teardown()
        assert called == [True]


class TestOnFileChanged:
    """on_file_changed() hot-reload hook."""

    def test_on_file_changed_exists_and_returns_none_by_default(self):
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        api = Dummy()
        assert hasattr(api, "on_file_changed")
        assert callable(api.on_file_changed)
        assert api.on_file_changed(["/foo.py"]) is None

    def test_custom_on_file_changed_can_suppress_default(self):
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

            def on_file_changed(self, changed_files):
                return "handled"

        api = Dummy()
        assert api.on_file_changed(["/foo.py"]) == "handled"

    def test_on_file_changed_receives_list_of_strings(self):
        from lite_server.api import LitAPI

        received = []

        class Dummy(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

            def on_file_changed(self, changed_files):
                received.extend(changed_files)
                return None

        api = Dummy()
        api.on_file_changed(["a.py", "b.yaml"])
        assert received == ["a.py", "b.yaml"]


class TestLogger:
    """Lazy logger property."""

    def test_logger_returns_logging_logger(self):
        from lite_server.api import LitAPI

        class MyModel(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        api = MyModel()
        assert isinstance(api.logger, logging.Logger)

    def test_logger_name_format(self):
        from lite_server.api import LitAPI

        class MyModel(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        api = MyModel()
        expected = f"{MyModel.__module__}.{MyModel.__name__}"
        assert api.logger.name == expected

    def test_logger_is_cached(self):
        from lite_server.api import LitAPI

        class MyModel(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        api = MyModel()
        logger1 = api.logger
        logger2 = api.logger
        assert logger1 is logger2

    def test_logger_different_classes_have_different_names(self):
        from lite_server.api import LitAPI

        class ModelA(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        class ModelB(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        a = ModelA()
        b = ModelB()
        assert a.logger.name != b.logger.name
        assert "ModelA" in a.logger.name
        assert "ModelB" in b.logger.name


class TestCustomMetrics:
    """register_metric() and report_metric() custom metrics API."""

    def _make_api(self):
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        return Dummy()

    def test_register_metric_returns_sequential_ids(self):
        api = self._make_api()
        id0 = api.register_metric("batch_size", "gauge")
        id1 = api.register_metric("latency_ms", "histogram")
        id2 = api.register_metric("errors", "counter")
        assert id0 == 0
        assert id1 == 1
        assert id2 == 2

    def test_register_metric_stores_specs(self):
        api = self._make_api()
        api.register_metric("a", "gauge")
        api.register_metric("b", "counter")
        assert len(api._metric_specs) == 2
        assert api._metric_specs[0].name == "a"
        assert api._metric_specs[0].metric_type == "gauge"
        assert api._metric_specs[1].name == "b"
        assert api._metric_specs[1].metric_type == "counter"

    def test_report_metric_appends_to_buffer(self):
        api = self._make_api()
        g = api.register_metric("x", "gauge")
        api.report_metric(g, 42.0)
        api.report_metric(g, 99.5)
        assert len(api._metric_values) == 2
        assert api._metric_values[0] == (0, 42.0)
        assert api._metric_values[1] == (0, 99.5)

    def test_report_metric_initial_buffer_is_empty(self):
        api = self._make_api()
        assert api._metric_values == []

    def test_multiple_metric_types(self):
        api = self._make_api()
        g = api.register_metric("g1", "gauge")
        c = api.register_metric("c1", "counter")
        h = api.register_metric("h1", "histogram")
        api.report_metric(g, 1.0)
        api.report_metric(c, 2.0)
        api.report_metric(h, 3.0)
        assert api._metric_values == [(0, 1.0), (1, 2.0), (2, 3.0)]


class TestCollectMetrics:
    """collect_metrics() gathers metric values into a Metrics proto."""

    def test_collect_returns_none_when_empty(self):
        from lite_server.pipeline import collect_metrics

        api = self._make_api()
        assert collect_metrics(api) is None

    def test_collect_returns_metrics_proto(self):
        from lite_server.pipeline import collect_metrics
        from lite_server.proto import Metrics

        api = self._make_api()
        g = api.register_metric("test_g", "gauge")
        c = api.register_metric("test_c", "counter")
        h = api.register_metric("test_h", "histogram")
        api.report_metric(g, 10.0)
        api.report_metric(c, 20.0)
        api.report_metric(h, 30.0)

        m = collect_metrics(api)
        assert isinstance(m, Metrics)
        assert len(m.gauges) == 1
        assert m.gauges[0].id == 0  # per-type index: first gauge
        assert m.gauges[0].value == 10.0
        assert len(m.counters) == 1
        assert m.counters[0].id == 0  # per-type index: first counter
        assert m.counters[0].value == 20.0
        assert len(m.histograms) == 1
        assert m.histograms[0].id == 0  # per-type index: first histogram
        assert m.histograms[0].value == 30.0

    def test_collect_clears_buffer(self):
        from lite_server.pipeline import collect_metrics

        api = self._make_api()
        g = api.register_metric("x", "gauge")
        api.report_metric(g, 1.0)
        collect_metrics(api)
        assert api._metric_values == []

    def test_collect_returns_none_when_no_specs(self):
        from lite_server.pipeline import collect_metrics

        api = self._make_api()
        # Manually add a value without registering — should be ignored
        api._metric_values.append((99, 1.0))
        assert collect_metrics(api) is None

    def _make_api(self):
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        return Dummy()


class TestUnwrapResponse:
    """_unwrap_response: extract body and headers from on_response result."""

    def test_unwrap_plain_body_returns_none_headers(self):
        from lite_server.pipeline import unwrap_response as _unwrap_response
        body, headers = _unwrap_response({"output": 1})
        assert body == {"output": 1}
        assert headers is None

    def test_unwrap_response_with_headers_extracts_both(self):
        from lite_server.response import Response
        from lite_server.pipeline import unwrap_response as _unwrap_response
        body, headers = _unwrap_response(
            Response(content={"out": 99}, headers={"X-Trace": "abc"})
        )
        assert body == {"out": 99}
        assert headers == {"X-Trace": "abc"}

    def test_unwrap_none_body(self):
        from lite_server.pipeline import unwrap_response as _unwrap_response
        body, headers = _unwrap_response(None)
        assert body is None
        assert headers is None

    def test_unwrap_string_body(self):
        from lite_server.pipeline import unwrap_response as _unwrap_response
        body, headers = _unwrap_response("plain string")
        assert body == "plain string"
        assert headers is None


# ---------------------------------------------------------------------------
# F1: Metrics race condition — _metric_values cross-thread safety
# ---------------------------------------------------------------------------


class TestMetricsRaceCondition:
    """F1: ``_metric_values`` list is mutated by ``report_metric`` (potentially
    on the executor thread in mixed mode) and read/reset by ``collect_metrics``
    (on the loop thread) without synchronization.  A ``report_metric`` that
    lands between iteration-end and reset is silently lost."""

    def test_report_during_collect_reset_window_is_not_lost(self):
        """Deterministic reproduction of the race window.

        An instrumented LitAPI subclass blocks inside the ``_metric_values``
        setter (the reset), creating a controlled window.  A concurrent
        ``report_metric`` during this window must land in *either* the
        current collect or the next one — never be silently dropped.
        """
        import threading

        from lite_server.api import LitAPI
        from lite_server.pipeline import collect_metrics

        block_reset = threading.Event()
        in_reset = threading.Event()

        class _InstrumentedAPI(LitAPI):
            def __init__(self):
                self._real_values = []
                self._in_reset = in_reset
                self._block_reset = block_reset
                self._init_done = False
                super().__init__()
                self._init_done = True

            @property
            def _metric_values(self):
                return self._real_values

            @_metric_values.setter
            def _metric_values(self, new):
                if self._init_done:
                    self._in_reset.set()
                    self._block_reset.wait()
                self._real_values = new

        api = _InstrumentedAPI()
        api._in_reset.clear()  # clear the init-time trigger
        g = api.register_metric("g", "gauge")
        api.report_metric(g, 1.0)  # pre-populate so collect is non-empty

        collected_first = None
        collected_second = None

        def _do_collect():
            nonlocal collected_first
            collected_first = collect_metrics(api)

        t = threading.Thread(target=_do_collect)
        t.start()

        # Wait until collect_metrics has iterated and is about to reset
        in_reset.wait()

        # Concurrent report from a *separate* thread — lands in the window
        # between iteration-end and reset.  Without a lock this append hits
        # the old list that is about to be discarded.  With the lock, this
        # thread blocks until collect releases the lock after the swap, so
        # the value lands in the new list.
        report_done = threading.Event()

        def _do_report():
            api.report_metric(g, 2.0)
            report_done.set()

        rt = threading.Thread(target=_do_report)
        rt.start()

        # Release the reset barrier — collect thread can now swap and
        # release the lock, unblocking the report thread.
        block_reset.set()
        t.join()
        rt.join()

        # Second collect — with the lock fix the 2.0 should be here
        # (blocked until after the swap, then appended to the new list).
        collected_second = collect_metrics(api)

        found_in_first = (
            collected_first is not None
            and len(collected_first.gauges) >= 2
            and any(mv.value == 2.0 for mv in collected_first.gauges)
        )
        found_in_second = (
            collected_second is not None
            and len(collected_second.gauges) >= 1
            and any(mv.value == 2.0 for mv in collected_second.gauges)
        )

        assert found_in_first or found_in_second, (
            f"reported value 2.0 was silently lost! "
            f"first={collected_first}, second={collected_second}"
        )

    def test_concurrent_report_and_collect_conservation(self):
        """Stress test: N threads reporting + concurrent collects must not
        lose or duplicate any metric value.  Σcollected == Σreported."""
        import threading

        from lite_server.api import LitAPI
        from lite_server.pipeline import collect_metrics

        api = LitAPI()
        g = api.register_metric("g", "gauge")
        c = api.register_metric("c", "counter")

        total_reported = 0
        total_collected = 0
        report_lock = threading.Lock()
        done = threading.Event()

        def _reporter():
            nonlocal total_reported
            for _ in range(500):
                api.report_metric(g, 1.0)
                api.report_metric(c, 1.0)
                with report_lock:
                    total_reported += 2

        def _collector():
            nonlocal total_collected
            while not done.is_set():
                m = collect_metrics(api)
                if m is not None:
                    n = (len(m.gauges) + len(m.counters) +
                         len(m.histograms))
                    total_collected += n

        threads = [threading.Thread(target=_reporter) for _ in range(4)]
        coll = threading.Thread(target=_collector)

        for t in threads:
            t.start()
        coll.start()

        for t in threads:
            t.join()
        done.set()
        coll.join()

        # Drain remaining buffered values
        m = collect_metrics(api)
        if m is not None:
            total_collected += (len(m.gauges) + len(m.counters) +
                              len(m.histograms))

        assert total_collected == total_reported, (
            f"metrics lost or duplicated: "
            f"collected={total_collected}, reported={total_reported}"
        )
