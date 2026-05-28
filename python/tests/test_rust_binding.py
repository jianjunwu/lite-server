"""Tests for the Rust PyO3 extension module."""

import inspect


def test_serve_is_callable():
    """lite_server.serve should be importable and callable."""
    from lite_server import serve

    assert callable(serve)


def test_serve_signature():
    """serve should accept optional config parameter."""
    from lite_server import serve

    sig = inspect.signature(serve)
    params = list(sig.parameters.keys())
    assert "config" in params
