"""Callbacks: base class and loading.

Callbacks observe and transform the inference pipeline at four hook points
around the three model stages::

    before_decode_request → decode_request → after_decode_request → predict
    → after_predict → encode_response → after_encode_response

All data hooks receive a single :class:`RequestContext` argument and may be
sync or async.  A data hook may:

- mutate ``ctx`` fields in place, or return a replacement value
- call ``ctx.respond(...)`` or return a ``Response`` for early return
- raise ``HTTPException`` to reject the request (validation, auth, ...)
  — exceptions from data hooks are NOT swallowed; they become error responses

Lifecycle hooks (``before_setup`` / ``after_setup`` / ``before_teardown`` /
``after_teardown``)
run outside the request path and are exception-isolated: failures are logged,
never propagated.
"""

from lite_server.callbacks._base import Callback, load_callbacks, validate_callback
from lite_server.callbacks.builtin import JsonSchemaValidator

__all__ = [
    "Callback",
    "JsonSchemaValidator",
    "load_callbacks",
    "validate_callback",
]
