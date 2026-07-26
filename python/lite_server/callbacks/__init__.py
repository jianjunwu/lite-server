"""Callbacks: base class, loading, and built-in policies.

Callbacks observe and transform the inference pipeline at four hook points
around the three model stages::

    on_request → decode_request → on_input → predict
    → on_output → encode_response → on_response

All data hooks receive a single :class:`RequestContext` argument and may be
sync or async.  A data hook may:

- mutate ``ctx`` fields in place, or return a replacement value
- call ``ctx.respond(...)`` or return a ``Response`` for early return
- raise ``HTTPException`` to reject the request (validation, auth, ...)
  — exceptions from data hooks are NOT swallowed; they become error responses

Lifecycle hooks (``on_before_setup`` / ``on_after_setup`` / ``on_teardown``)
run outside the request path and are exception-isolated: failures are logged,
never propagated.
"""

from lite_server.callbacks._base import Callback, load_callbacks, validate_callback
from lite_server.callbacks.auth import RequireApiKey
from lite_server.callbacks.cors import Cors
from lite_server.callbacks.policies import extract_policies
from lite_server.callbacks.rate_limit import RateLimit
from lite_server.callbacks.request_log import LogRequests

__all__ = [
    "Callback",
    "Cors",
    "LogRequests",
    "RateLimit",
    "RequireApiKey",
    "extract_policies",
    "load_callbacks",
    "validate_callback",
]
