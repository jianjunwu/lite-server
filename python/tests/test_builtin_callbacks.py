"""Tests for built-in callback classes (JsonSchemaValidator, ...)."""

import pytest

from lite_server.callbacks import JsonSchemaValidator, load_callbacks
from lite_server.context import Headers, RequestContext, RequestMeta
from lite_server.exceptions import BadRequestError


def _ctx(input=None, output=None, mode="unary"):
    ctx = RequestContext(
        meta=RequestMeta(
            route="/p", headers=Headers({}), client_ip="",
            request_id="r", timestamp_ns=0,
        ),
        mode=mode,
    )
    ctx.input = input
    ctx.output = output
    return ctx


INPUT_SCHEMA = {
    "type": "object",
    "required": ["prompt"],
    "additionalProperties": False,
    "properties": {"prompt": {"type": "string", "minLength": 1}},
}


class TestJsonSchemaValidator:
    def test_valid_input_passes_unchanged(self):
        v = JsonSchemaValidator(input_schema=INPUT_SCHEMA)
        ctx = _ctx(input={"prompt": "hello"})
        v.on_input(ctx)  # no raise
        assert ctx.input == {"prompt": "hello"}

    def test_invalid_input_raises_400_at_body(self):
        v = JsonSchemaValidator(input_schema=INPUT_SCHEMA)
        ctx = _ctx(input={})  # missing required "prompt"
        with pytest.raises(BadRequestError) as ei:
            v.on_input(ctx)
        assert ei.value.status_code == 400
        assert ei.value.error_type == "invalid_request_error"
        assert ei.value.param == "body"
        assert "prompt" in ei.value.detail

    def test_invalid_nested_input_param_is_pointer(self):
        schema = {"type": "object", "properties": {"n": {"type": "integer"}}}
        v = JsonSchemaValidator(input_schema=schema)
        ctx = _ctx(input={"n": "not-int"})
        with pytest.raises(BadRequestError) as ei:
            v.on_input(ctx)
        assert ei.value.param == "body/n"

    def test_output_validated_in_unary_mode(self):
        v = JsonSchemaValidator(output_schema={"type": "object", "required": ["text"]})
        ctx = _ctx(output={"nope": 1}, mode="unary")
        with pytest.raises(BadRequestError):
            v.on_output(ctx)

    def test_output_skipped_in_stream_mode(self):
        v = JsonSchemaValidator(output_schema={"type": "object", "required": ["text"]})
        ctx = _ctx(output={"nope": 1}, mode="stream")
        v.on_output(ctx)  # no raise — stream chunks not validated

    def test_non_dict_input_is_skipped(self):
        v = JsonSchemaValidator(input_schema=INPUT_SCHEMA)
        ctx = _ctx(input="plain text")  # str, not dict/list
        v.on_input(ctx)  # no raise

    def test_no_schema_skips_both_directions(self):
        v = JsonSchemaValidator()  # no schemas → nothing validated
        ctx = _ctx(input={"anything": 1}, output={"too": 2})
        v.on_input(ctx)
        v.on_output(ctx)  # no raise

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
            v.on_input(_ctx(input={}))

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
            cbs[0].on_input(_ctx(input={}))
