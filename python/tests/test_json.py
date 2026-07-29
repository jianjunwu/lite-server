"""Contract tests for lite_server._json and the unified request-JSON parser
(P1: orjson migration)."""

import json

import pytest

from lite_server import _json
from lite_server.exceptions import HTTPException


def test_dumps_returns_bytes():
    assert isinstance(_json.dumps({"a": 1}), bytes)


def test_round_trip_nested_and_unicode():
    obj = {"list": [1, 2.5, "三"], "nested": {"ok": True}, "none": None}
    assert _json.loads(_json.dumps(obj)) == obj


def test_loads_accepts_bytes_and_str():
    assert _json.loads(b'{"x": 1}') == {"x": 1}
    assert _json.loads('{"x": 1}') == {"x": 1}


def test_loads_invalid_raises_json_decode_error():
    # Callers except on json.JSONDecodeError; orjson's error subclasses it.
    with pytest.raises(json.JSONDecodeError):
        _json.loads(b"{not json")


def test_parse_request_json_contract():
    from lite_server.pipeline import _parse_request_json

    assert _parse_request_json(None) == {}
    assert _parse_request_json(b"") == {}
    assert _parse_request_json(b'{"input": "hi"}') == {"input": "hi"}
    with pytest.raises(HTTPException) as exc_info:
        _parse_request_json(b"{bad")
    assert exc_info.value.status_code == 400
    assert exc_info.value.code == "invalid_json"


def test_worker_parse_is_pipeline_parse():
    # P1 merge: the worker-side duplicate (_parse_json_payload) must be the
    # pipeline's single implementation, re-exported under its old name.
    from lite_server.pipeline import _parse_request_json
    from lite_server.worker.common import _parse_json_payload

    assert _parse_json_payload is _parse_request_json


@pytest.mark.skipif(not _json.HAS_ORJSON, reason="orjson not installed")
def test_orjson_nan_serializes_as_null():
    assert _json.dumps({"v": float("nan")}) == b'{"v":null}'


@pytest.mark.skipif(not _json.HAS_ORJSON, reason="orjson not installed")
def test_orjson_non_str_keys_raise():
    with pytest.raises(TypeError):
        _json.dumps({1: "a"})
