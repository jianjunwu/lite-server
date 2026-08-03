"""Built-in callback classes for lite-server.

Each builtin takes its configuration as constructor arguments — register via
the class attribute (``LitAPI.callbacks``) or a single-key map entry in
``config.yaml`` (``{path: kwargs}``).

``JsonSchemaValidator`` requires the optional ``validation`` extra
(``pip install lite-server[validation]``); jsonschema is imported lazily so
this module loads even without the extra (the class raises on construction).
"""

from __future__ import annotations

from typing import Any

from lite_server.callbacks import Callback
from lite_server.context import RequestContext
from lite_server.exceptions import BadRequestError

try:
    import jsonschema
    from jsonschema import Draft7Validator
    from jsonschema.exceptions import best_match
    from jsonschema.validators import validator_for

    _HAS_JSONSCHEMA = True
except ImportError:  # pragma: no cover - only hit without the extra
    _HAS_JSONSCHEMA = False
    Draft7Validator = None  # type: ignore[assignment]
    best_match = None  # type: ignore[assignment]
    validator_for = None  # type: ignore[assignment]
    jsonschema = None  # type: ignore[assignment]


class JsonSchemaValidator(Callback):
    """Validate the request body / response body against JSON Schemas.

    Both schemas describe the **wire payload** — what the client sends and
    what the client receives — not the model's internal shapes:

    - ``input_schema`` validates ``ctx.request`` in ``before_decode_request``, before
      ``decode_request`` runs: an invalid request is rejected with 400 and
      no model code (decode included) ever sees it.  ``ctx.request`` is
      always the parsed JSON body, so a scalar / ``null`` body is a schema
      violation like any other.
    - ``output_schema`` validates ``ctx.response`` in ``after_encode_response``, after
      ``encode_response`` — unary/batch and custom-route responses only.
      Streaming chunks are partial JSON and never match a full schema, so
      they are skipped via ``ctx.mode``.  A text/bytes passthrough response
      is genuinely non-JSON, so object/array schemas skip it.

    On failure raises a structured ``BadRequestError`` (400) carrying the
    single best-match error: ``param`` is the JSON Pointer to the failing
    location (prefixed ``body/``), ``message`` is the error text.  The schema
    draft is auto-detected from ``$schema`` (default Draft 7); a malformed
    schema is rejected at construction (loud — a silent skip would mean
    validation never ran).
    """

    def __init__(
        self,
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
    ) -> None:
        if not _HAS_JSONSCHEMA:
            raise ImportError(
                "JsonSchemaValidator requires the 'validation' extra: "
                "pip install lite-server[validation]"
            )
        self._input_validator = self._build(input_schema) if input_schema else None
        self._output_validator = self._build(output_schema) if output_schema else None

    @staticmethod
    def _build(schema: dict[str, Any]):
        cls = validator_for(schema, default=Draft7Validator)
        cls.check_schema(schema)  # loud: malformed schema → error at load
        return cls(schema)

    def _reject(self, value: Any, validator, *, skip_non_json: bool) -> None:
        if skip_non_json:
            top = validator.schema.get("type")
            types = (top,) if isinstance(top, str) else tuple(top or ())
            if "object" in types or "array" in types:
                if not isinstance(value, (dict, list)):
                    return  # non-JSON payload (text / bytes passthrough) → skip
        errors = list(validator.iter_errors(value))
        if not errors:
            return
        best = best_match(errors)
        pointer = "/" + "/".join(str(p) for p in best.path) if best.path else ""
        param = f"body{pointer}" if pointer else "body"
        raise BadRequestError(best.message, param=param)

    def before_decode_request(self, ctx: RequestContext) -> None:
        if self._input_validator is not None:
            # ctx.request is always the parsed JSON body on every path — a
            # scalar / null here violates the schema's top-level type, it is
            # not a non-JSON payload.
            self._reject(ctx.request, self._input_validator, skip_non_json=False)

    def after_encode_response(self, ctx: RequestContext) -> None:
        if self._output_validator is None:
            return
        # mode None = custom route: a complete payload, validated like unary.
        if ctx.mode is not None and ctx.mode not in ("unary", "batch"):
            return  # stream/bidi/decoupled/cb chunks aren't validated
        self._reject(ctx.response, self._output_validator, skip_non_json=True)
