"""Unit tests for lite_server.exceptions module."""

import pytest
from lite_server.exceptions import (
    BadRequestError,
    ForbiddenError,
    HTTPException,
    InternalServerError,
    NotFoundError,
    ServiceUnavailableError,
    UnauthorizedError,
)


class TestHTTPException:
    def test_basic_properties(self):
        e = HTTPException(400, "test msg", "TEST_CODE")
        assert e.status_code == 400
        assert e.detail == "test msg"
        assert e.error_type == "TEST_CODE"
        assert str(e) == "test msg"

    def test_default_error_type(self):
        e = HTTPException(418, "im a teapot")
        assert e.error_type == "model_error"

    def test_str_returns_detail(self):
        e = HTTPException(500, "something broke")
        assert str(e) == "something broke"


class TestSubclassDefaults:
    def test_bad_request(self):
        e = BadRequestError("bad input")
        assert e.status_code == 400
        assert e.error_type == "invalid_request_error"
        assert isinstance(e, HTTPException)

    def test_bad_request_custom_type(self):
        e = BadRequestError("bad input", "invalid_input")
        assert e.status_code == 400
        assert e.error_type == "invalid_input"

    def test_unauthorized(self):
        e = UnauthorizedError("token expired")
        assert e.status_code == 401
        assert e.error_type == "authentication_error"

    def test_forbidden(self):
        e = ForbiddenError("access denied")
        assert e.status_code == 403
        assert e.error_type == "permission_denied_error"

    def test_not_found(self):
        e = NotFoundError("item not in vocab")
        assert e.status_code == 404
        assert e.error_type == "not_found_error"

    def test_internal_server_error(self):
        e = InternalServerError("gpu oom")
        assert e.status_code == 500
        assert e.error_type == "server_error"

    def test_internal_server_error_defaults(self):
        e = InternalServerError()
        assert e.status_code == 500
        assert e.error_type == "server_error"
        assert e.detail == "internal server error"

    def test_service_unavailable(self):
        e = ServiceUnavailableError("model loading")
        assert e.status_code == 503
        assert e.error_type == "service_unavailable"

    def test_service_unavailable_defaults(self):
        e = ServiceUnavailableError()
        assert e.status_code == 503
        assert e.detail == "service unavailable"


class TestCatchByBaseClass:
    def test_bad_request_caught_by_http_exception(self):
        try:
            raise BadRequestError("test")
        except HTTPException:
            pass
        else:
            pytest.fail("BadRequestError should be caught by except HTTPException")

    def test_all_subclasses_caught_by_http_exception(self):
        for exc_cls in [
            BadRequestError,
            UnauthorizedError,
            ForbiddenError,
            NotFoundError,
            InternalServerError,
            ServiceUnavailableError,
        ]:
            try:
                raise exc_cls("test")
            except HTTPException:
                pass
            else:
                pytest.fail(f"{exc_cls.__name__} should be caught by except HTTPException")

    def test_not_caught_by_other_subclass(self):
        """A BadRequestError should not be caught by except NotFoundError."""
        try:
            try:
                raise BadRequestError("test")
            except NotFoundError:
                pytest.fail("BadRequestError should not be caught by NotFoundError")
        except BadRequestError:
            pass

    def test_isinstance_all_subclasses(self):
        for exc_cls in [
            BadRequestError,
            UnauthorizedError,
            ForbiddenError,
            NotFoundError,
            InternalServerError,
            ServiceUnavailableError,
        ]:
            e = exc_cls("test")
            assert isinstance(e, HTTPException)
            assert isinstance(e, Exception)


class TestCustomSubclass:
    """Users can subclass HTTPException for custom status codes."""

    def test_custom_status_code(self):
        class PaymentRequiredError(HTTPException):
            def __init__(self, detail, error_type="payment_required"):
                super().__init__(402, detail, error_type)

        e = PaymentRequiredError("need payment")
        assert e.status_code == 402
        assert e.error_type == "payment_required"
        assert isinstance(e, HTTPException)


class TestCodeAndParam:
    """New code/param fields for programmatic error handling."""

    def test_http_exception_with_code_and_param(self):
        e = HTTPException(400, "bad input", "invalid_request_error",
                          code="invalid_input", param="temperature")
        assert e.code == "invalid_input"
        assert e.param == "temperature"

    def test_http_exception_defaults(self):
        e = HTTPException(400, "bad input")
        assert e.code is None
        assert e.param is None

    def test_bad_request_error_with_code_and_param(self):
        e = BadRequestError("bad input", code="invalid_input", param="temperature")
        assert e.code == "invalid_input"
        assert e.param == "temperature"
        assert e.status_code == 400
        assert e.error_type == "invalid_request_error"

    def test_bad_request_error_defaults(self):
        e = BadRequestError("bad input")
        assert e.code is None
        assert e.param is None

    def test_internal_server_error_with_code_and_param(self):
        e = InternalServerError("gpu oom", code="out_of_memory", param="gpu_id")
        assert e.code == "out_of_memory"
        assert e.param == "gpu_id"

    def test_service_unavailable_with_code(self):
        e = ServiceUnavailableError("model loading", code="model_not_loaded")
        assert e.code == "model_not_loaded"
        assert e.param is None
