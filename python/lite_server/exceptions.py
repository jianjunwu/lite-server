"""Exception classes for model inference error handling.

Model authors raise these to return typed HTTP errors to the client::

    from lite_server.exceptions import BadRequestError, ServiceUnavailableError

    class MyModel(LitAPI):
        def predict(self, x):
            if x < 0:
                raise BadRequestError("input must be non-negative", "invalid_input")
            if self.model is None:
                raise ServiceUnavailableError("model not loaded yet")
            return self.model(x)

The error response sent to the client follows the format::

    {"error": {"type": "<error_type>", "message": "<detail>"}}

where ``type`` is a snake_case category (e.g. ``"invalid_request_error"``,
``"server_error"``, ``"service_unavailable"``) and ``message`` is the
human-readable detail string.
"""


class HTTPException(Exception):
    """Base exception for returning a specific HTTP error from model code.

    Subclasses pre-set the status code for common cases::

        raise BadRequestError("invalid input")
        raise ServiceUnavailableError("model not loaded")

    Args:
        status_code: HTTP status code (400-599).
        detail: Human-readable error message, returned to the client.
        error_type: Machine-readable error type string (snake_case).
        code: Optional machine-readable error code for programmatic handling.
        param: Optional parameter name that caused the error.
    """

    def __init__(
        self, status_code: int, detail: str, error_type: str = "model_error",
        code: str | None = None, param: str | None = None,
    ):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.error_type = error_type
        self.code = code
        self.param = param


class BadRequestError(HTTPException):
    """HTTP 400 — invalid request / validation error.

    Usage::

        raise BadRequestError("input must be non-negative", "invalid_input")
    """

    def __init__(self, detail: str, error_type: str = "invalid_request_error",
                 code: str | None = None, param: str | None = None):
        super().__init__(400, detail, error_type, code=code, param=param)


class UnauthorizedError(HTTPException):
    """HTTP 401 — authentication required."""

    def __init__(self, detail: str, error_type: str = "authentication_error",
                 code: str | None = None, param: str | None = None):
        super().__init__(401, detail, error_type, code=code, param=param)


class ForbiddenError(HTTPException):
    """HTTP 403 — access denied."""

    def __init__(self, detail: str, error_type: str = "permission_denied_error",
                 code: str | None = None, param: str | None = None):
        super().__init__(403, detail, error_type, code=code, param=param)


class NotFoundError(HTTPException):
    """HTTP 404 — resource not found."""

    def __init__(self, detail: str, error_type: str = "not_found_error",
                 code: str | None = None, param: str | None = None):
        super().__init__(404, detail, error_type, code=code, param=param)


class InternalServerError(HTTPException):
    """HTTP 500 — internal error.

    Warning: The detail message will reach the client un-sanitized.
    Use only when you intentionally want to expose the message.
    """

    def __init__(
        self, detail: str = "internal server error", error_type: str = "server_error",
        code: str | None = None, param: str | None = None,
    ):
        super().__init__(500, detail, error_type, code=code, param=param)


class ServiceUnavailableError(HTTPException):
    """HTTP 503 — model temporarily unavailable."""

    def __init__(
        self,
        detail: str = "service unavailable",
        error_type: str = "service_unavailable",
        code: str | None = None,
        param: str | None = None,
    ):
        super().__init__(503, detail, error_type, code=code, param=param)
