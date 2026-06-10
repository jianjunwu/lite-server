"""Exception classes for model inference error handling.

Model authors raise these to return typed HTTP errors to the client::

    from lite_server.exceptions import BadRequestError, ServiceUnavailableError

    class MyModel(LitAPI):
        def predict(self, x):
            if x < 0:
                raise BadRequestError("input must be non-negative", "INVALID_INPUT")
            if self.model is None:
                raise ServiceUnavailableError("model not loaded yet")
            return self.model(x)
"""


class HTTPException(Exception):
    """Base exception for returning a specific HTTP error from model code.

    Subclasses pre-set the status code for common cases::

        raise BadRequestError("invalid input")
        raise ServiceUnavailableError("model not loaded")

    Args:
        status_code: HTTP status code (400-599).
        detail: Human-readable error message, returned to the client.
        error_code: Machine-readable error code string.
    """

    def __init__(
        self, status_code: int, detail: str, error_code: str = "MODEL_ERROR"
    ):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.error_code = error_code


class BadRequestError(HTTPException):
    """HTTP 400 — invalid request / validation error.

    Usage::

        raise BadRequestError("input must be non-negative", "INVALID_INPUT")
    """

    def __init__(self, detail: str, error_code: str = "BAD_REQUEST"):
        super().__init__(400, detail, error_code)


class UnauthorizedError(HTTPException):
    """HTTP 401 — authentication required."""

    def __init__(self, detail: str, error_code: str = "UNAUTHORIZED"):
        super().__init__(401, detail, error_code)


class ForbiddenError(HTTPException):
    """HTTP 403 — access denied."""

    def __init__(self, detail: str, error_code: str = "FORBIDDEN"):
        super().__init__(403, detail, error_code)


class NotFoundError(HTTPException):
    """HTTP 404 — resource not found."""

    def __init__(self, detail: str, error_code: str = "NOT_FOUND"):
        super().__init__(404, detail, error_code)


class InternalServerError(HTTPException):
    """HTTP 500 — internal error.

    Warning: The detail message will reach the client un-sanitized.
    Use only when you intentionally want to expose the message.
    """

    def __init__(
        self, detail: str = "internal server error", error_code: str = "INTERNAL_ERROR"
    ):
        super().__init__(500, detail, error_code)


class ServiceUnavailableError(HTTPException):
    """HTTP 503 — model temporarily unavailable."""

    def __init__(
        self,
        detail: str = "service unavailable",
        error_code: str = "SERVICE_UNAVAILABLE",
    ):
        super().__init__(503, detail, error_code)
