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
