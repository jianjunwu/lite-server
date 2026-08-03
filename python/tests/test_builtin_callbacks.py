"""Tests for built-in callback classes (JsonSchemaValidator, ...)."""

import pytest

from lite_server.callbacks import JsonSchemaValidator, load_callbacks
from lite_server.context import Headers, RequestContext, RequestMeta
from lite_server.exceptions import BadRequestError


def _ctx(request=None, response=None, mode="unary"):
    ctx = RequestContext(
        meta=RequestMeta(
            route="/p", headers=Headers({}), client_ip="",
            request_id="r", timestamp_ns=0,
        ),
        mode=mode,
    )
    ctx.request = request
    ctx.response = response
    return ctx


INPUT_SCHEMA = {
    "type": "object",
    "required": ["prompt"],
    "additionalProperties": False,
    "properties": {"prompt": {"type": "string", "minLength": 1}},
}


class TestJsonSchemaValidator:
    def test_valid_request_passes_unchanged(self):
        v = JsonSchemaValidator(input_schema=INPUT_SCHEMA)
        ctx = _ctx(request={"prompt": "hello"})
        v.before_decode_request(ctx)  # no raise
        assert ctx.request == {"prompt": "hello"}

    def test_invalid_request_raises_400_at_body(self):
        v = JsonSchemaValidator(input_schema=INPUT_SCHEMA)
        ctx = _ctx(request={})  # missing required "prompt"
        with pytest.raises(BadRequestError) as ei:
            v.before_decode_request(ctx)
        assert ei.value.status_code == 400
        assert ei.value.error_type == "invalid_request_error"
        assert ei.value.param == "body"
        assert "prompt" in ei.value.detail

    def test_invalid_nested_request_param_is_pointer(self):
        schema = {"type": "object", "properties": {"n": {"type": "integer"}}}
        v = JsonSchemaValidator(input_schema=schema)
        ctx = _ctx(request={"n": "not-int"})
        with pytest.raises(BadRequestError) as ei:
            v.before_decode_request(ctx)
        assert ei.value.param == "body/n"

    def test_output_validated_in_unary_mode(self):
        v = JsonSchemaValidator(output_schema={"type": "object", "required": ["text"]})
        ctx = _ctx(response={"nope": 1}, mode="unary")
        with pytest.raises(BadRequestError):
            v.after_encode_response(ctx)

    def test_output_skipped_in_stream_mode(self):
        v = JsonSchemaValidator(output_schema={"type": "object", "required": ["text"]})
        ctx = _ctx(response={"nope": 1}, mode="stream")
        v.after_encode_response(ctx)  # no raise — stream chunks not validated

    def test_scalar_request_violates_object_schema(self):
        """ctx.request is always the parsed JSON body — a scalar/null body
        fails an object schema's top-level type (no dict/list skip on the
        request side)."""
        v = JsonSchemaValidator(input_schema=INPUT_SCHEMA)
        with pytest.raises(BadRequestError):
            v.before_decode_request(_ctx(request="plain text"))
        with pytest.raises(BadRequestError):
            v.before_decode_request(_ctx(request=None))

    def test_non_json_response_is_skipped(self):
        """Text/bytes passthrough responses are genuinely non-JSON — an
        object/array output schema does not apply to them."""
        v = JsonSchemaValidator(output_schema={"type": "object", "required": ["x"]})
        v.after_encode_response(_ctx(response="plain text"))  # no raise
        v.after_encode_response(_ctx(response=b"\x89PNG"))  # no raise

    def test_output_validated_on_route_mode(self):
        """Route ctx (mode None) carries a complete payload — output_schema applies."""
        v = JsonSchemaValidator(output_schema={"type": "object", "required": ["text"]})
        with pytest.raises(BadRequestError):
            v.after_encode_response(_ctx(response={"nope": 1}, mode=None))

    def test_string_schema_validates_string_request(self):
        """A scalar top-level schema must NOT be skipped by the dict/list rule."""
        v = JsonSchemaValidator(input_schema={"type": "string", "minLength": 1})
        ctx = _ctx(request="")
        with pytest.raises(BadRequestError) as ei:
            v.before_decode_request(ctx)
        assert ei.value.param == "body"  # top-level scalar → no JSON pointer
        v.before_decode_request(_ctx(request="hello"))  # valid string passes

    def test_string_schema_rejects_none(self):
        v = JsonSchemaValidator(input_schema={"type": "string"})
        ctx = _ctx(request=None)  # empty body parsed to {}
        with pytest.raises(BadRequestError):
            v.before_decode_request(ctx)

    def test_no_schema_skips_both_directions(self):
        v = JsonSchemaValidator()  # no schemas → nothing validated
        ctx = _ctx(request={"anything": 1}, response={"too": 2})
        v.before_decode_request(ctx)
        v.after_encode_response(ctx)  # no raise

    def test_bad_schema_rejected_at_construct(self):
        from jsonschema.exceptions import SchemaError

        with pytest.raises(SchemaError):
            JsonSchemaValidator(input_schema={"type": "not-a-real-type"})

    def test_draft2020_picked_via_dollar_schema(self):
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["x"],
        }
        v = JsonSchemaValidator(input_schema=schema)
        with pytest.raises(BadRequestError):
            v.before_decode_request(_ctx(request={}))

    def test_config_map_registration(self):
        cbs = load_callbacks(
            {"callbacks": [
                {"lite_server.callbacks.JsonSchemaValidator":
                    {"input_schema": INPUT_SCHEMA}},
            ]}
        )
        assert len(cbs) == 1
        assert isinstance(cbs[0], JsonSchemaValidator)
        with pytest.raises(BadRequestError):
            cbs[0].before_decode_request(_ctx(request={}))
