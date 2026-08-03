"""Audit reproduction tests for commits 7c256c9 + f3902c1 (hook rename +
JsonSchemaValidator move to wire-payload hooks).

Each test encodes the contract stated in the commit messages / docstrings and
currently FAILS — proving the defect. No implementation code is modified.
"""

import json

import pytest

from lite_server.api import LitAPI
from lite_server.callbacks import Callback, JsonSchemaValidator
from lite_server.context import Headers, RequestContext, RequestMeta
from lite_server.exceptions import BadRequestError
from lite_server.pipeline import Pipeline


def _meta(route="/predict"):
    return RequestMeta(
        route=route,
        headers=Headers({}),
        client_ip="127.0.0.1",
        request_id="req-1",
        timestamp_ns=1,
    )


class EchoAPI(LitAPI):
    def setup(self, device):
        pass

    def predict(self, x):
        return {"echo": x}


class TestWirePayloadValidationGaps:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("body", [b"null", b"123", b'"plain text"'])
    async def test_data_scalar_body_bypasses_object_input_schema(self, body):
        """7c256c9 contract: an invalid request is rejected 400 and no model
        code (decode included) ever runs.  A JSON scalar / null body violates
        a type:object input_schema, yet _reject's dict/list guard (designed
        for decoded tensors in the on_input era) skips it — the scalar walks
        straight into decode_request."""
        calls = []

        class Guarded(EchoAPI):
            def decode_request(self, request):
                calls.append("decode")
                return request

        v = JsonSchemaValidator(
            input_schema={"type": "object", "required": ["sessionId"]}
        )
        pipe = Pipeline.build(Guarded(), [v])
        with pytest.raises(BadRequestError):
            await pipe.run_single(body, _meta())
        assert calls == []

    @pytest.mark.asyncio
    async def test_order_respond_finalization_skips_output_validation(self):
        """The documented header pattern — `return ctx.respond(ctx.response,
        headers=...)` in after_encode_response (context.py docstring, example
        11) — sets ctx.early, and _run_chain stops before later hooks.  With
        the header callback registered before JsonSchemaValidator,
        output_schema validation silently never runs: invalid responses ship
        as 200.  Pre-7c256c9 the validator lived in the on_output chain, which
        always ran before any on_response-time respond."""

        class AddHeaders(Callback):
            def after_encode_response(self, ctx):
                return ctx.respond(
                    ctx.response, headers={"X-Request-ID": ctx.meta.request_id}
                )

        class BadModel(EchoAPI):
            def predict(self, x):
                return {"wrong": 1}

            def encode_response(self, output):
                return output

        v = JsonSchemaValidator(
            output_schema={"type": "object", "required": ["text"]}
        )
        pipe = Pipeline.build(BadModel(), [AddHeaders(), v])
        with pytest.raises(BadRequestError):
            await pipe.run_single(b'{"x": 1}', _meta())

    @pytest.mark.asyncio
    async def test_order_validator_first_still_validates(self):
        """Control: validator registered before the header callback validates
        (passes on current code) — pinning that the gap above is purely
        registration-order dependent."""

        class AddHeaders(Callback):
            def after_encode_response(self, ctx):
                return ctx.respond(
                    ctx.response, headers={"X-Request-ID": ctx.meta.request_id}
                )

        class BadModel(EchoAPI):
            def predict(self, x):
                return {"wrong": 1}

            def encode_response(self, output):
                return output

        v = JsonSchemaValidator(
            output_schema={"type": "object", "required": ["text"]}
        )
        pipe = Pipeline.build(BadModel(), [v, AddHeaders()])
        with pytest.raises(BadRequestError):
            await pipe.run_single(b'{"x": 1}', _meta())

    @pytest.mark.asyncio
    async def test_scope_output_schema_on_route_is_silent_noop(self):
        """for_route loudly rejects after_decode_request / after_predict hooks
        precisely because they could never run on a route — yet a
        JsonSchemaValidator with only output_schema is accepted while its
        after_encode_response self-gates on ctx.mode in ("unary", "batch")
        and route ctx.mode is None.  A route response is a complete payload
        (unlike stream chunks), so the skip rationale does not apply:
        output_schema on a route silently validates nothing."""
        v = JsonSchemaValidator(
            output_schema={"type": "object", "required": ["text"]}
        )
        pipe = Pipeline.for_route([v])
        ctx = RequestContext(meta=_meta(route="/custom"), request={})

        def handler(c):
            return {"wrong": 1}

        with pytest.raises(BadRequestError):
            await pipe.run_route(ctx, handler)
