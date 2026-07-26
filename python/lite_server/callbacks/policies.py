"""Policy extraction for the worker startup handshake."""

from typing import Any

from lite_server.callbacks._base import Callback
from lite_server.callbacks.cors import Cors
from lite_server.callbacks.rate_limit import RateLimit


def extract_policies(callbacks: list[Callback]) -> dict[str, Any]:
    """Pull Rust-executed policies out of the merged callback list.

    Embedded in the worker startup handshake.  Last declaration wins.
    """
    policies: dict[str, Any] = {}
    for cb in callbacks:
        if isinstance(cb, RateLimit):
            policies["rate_limit"] = {
                "requests_per_minute": cb.requests_per_minute,
                "key": cb.key,
                "burst": cb.burst,
            }
        elif isinstance(cb, Cors):
            policies["cors"] = {
                "allow_origins": cb.allow_origins,
                "allow_methods": cb.allow_methods,
                "allow_headers": cb.allow_headers,
            }
    return policies
