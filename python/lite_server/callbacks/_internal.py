"""Shared internals for Rust-managed policy callbacks."""

import os

_POLICY_MANAGED_ENV = "LITE_POLICY_MANAGED"


def _rust_managed() -> bool:
    """True when the Rust HTTP layer executes RateLimit/Cors policies.

    Set via env at worker spawn (new_worker_command).  Read per-instance at
    construction so unit tests can monkeypatch os.environ.
    """
    return os.environ.get(_POLICY_MANAGED_ENV) == "1"
