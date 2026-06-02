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

    def test_default_params_forwarded_to_litserve(self):
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
    """_collect_metrics() gathers metric values into a Metrics proto."""

    def test_collect_returns_none_when_empty(self):
        from lite_server.worker.inference import _collect_metrics

        api = self._make_api()
        assert _collect_metrics(api) is None

    def test_collect_returns_metrics_proto(self):
        from lite_server.worker.inference import _collect_metrics
        from lite_server.proto import Metrics

        api = self._make_api()
        g = api.register_metric("test_g", "gauge")
        c = api.register_metric("test_c", "counter")
        h = api.register_metric("test_h", "histogram")
        api.report_metric(g, 10.0)
        api.report_metric(c, 20.0)
        api.report_metric(h, 30.0)

        m = _collect_metrics(api)
        assert isinstance(m, Metrics)
        assert len(m.gauges) == 1
        assert m.gauges[0].id == g
        assert m.gauges[0].value == 10.0
        assert len(m.counters) == 1
        assert m.counters[0].id == c
        assert m.counters[0].value == 20.0
        assert len(m.histograms) == 1
        assert m.histograms[0].id == h
        assert m.histograms[0].value == 30.0

    def test_collect_clears_buffer(self):
        from lite_server.worker.inference import _collect_metrics

        api = self._make_api()
        g = api.register_metric("x", "gauge")
        api.report_metric(g, 1.0)
        _collect_metrics(api)
        assert api._metric_values == []

    def test_collect_returns_none_when_no_specs(self):
        from lite_server.worker.inference import _collect_metrics

        api = self._make_api()
        # Manually add a value without registering — should be ignored
        api._metric_values.append((99, 1.0))
        assert _collect_metrics(api) is None

    def _make_api(self):
        from lite_server.api import LitAPI

        class Dummy(LitAPI):
            def setup(self, device): pass
            def decode_request(self, request): return request
            def predict(self, x): return x
            def encode_response(self, output): return output

        return Dummy()


class TestAsyncLitAPI:
    """AsyncLitAPI subclassing and constraints."""

    def test_async_litapi_forces_max_batch_size_to_one(self):
        from lite_server.api_async import AsyncLitAPI

        class Dummy(AsyncLitAPI):
            def setup(self, device): pass
            async def predict(self, x): return x

        api = Dummy(max_batch_size=8)
        assert api.max_batch_size == 1

    def test_async_litapi_sets_enable_async(self):
        from lite_server.api_async import AsyncLitAPI

        class Dummy(AsyncLitAPI):
            def setup(self, device): pass
            async def predict(self, x): return x

        api = Dummy()
        assert api.enable_async is True

    def test_async_predict_must_be_implemented(self):
        from lite_server.api_async import AsyncLitAPI

        class Dummy(AsyncLitAPI):
            def setup(self, device): pass

        api = Dummy()
        import asyncio
        with pytest.raises(NotImplementedError):
            asyncio.run(api.predict({}))

    def test_async_litapi_is_instance_of_litapi(self):
        from lite_server.api import LitAPI
        from lite_server.api_async import AsyncLitAPI

        class Dummy(AsyncLitAPI):
            def setup(self, device): pass
            async def predict(self, x): return x

        api = Dummy()
        assert isinstance(api, LitAPI)

    def test_async_litapi_exported_from_package(self):
        from lite_server import AsyncLitAPI
        assert AsyncLitAPI is not None
