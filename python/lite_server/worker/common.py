"""Shared helpers for the worker request path: protobuf frame builders,
JSON payload parsing, error formatting, and pipeline lookup.

Split out of ``inference.py`` (0.7.7 debt-free phase 1) as the leaf module
of the worker package — ``inference`` / ``dispatch`` / ``streaming`` /
``cb_loop`` all import from here, so there are no import cycles.
"""

import json
import traceback

from lite_server.api import LitAPI
from lite_server.context import Headers, RequestContext, RequestMeta
from lite_server.exceptions import HTTPException
from lite_server.pipeline import Pipeline, extract_response_meta

# Single implementation lives in pipeline (P1 merge); re-exported under the
# historical worker-side name so dispatch/streaming/inference imports are
# unchanged.
from lite_server.pipeline import _parse_request_json as _parse_json_payload  # noqa: F401
from lite_server.proto import (
    Response,
    SingleResponse,
    Status,
    StreamChunkResponse,
    StreamDone,
    StreamError,
    StreamResponse,
)

def _get_pipeline(lit_api: LitAPI) -> Pipeline:
    """Return the instance's Pipeline, building an empty one on demand.

    ``load_litapi`` always builds the pipeline; the fallback keeps tests and
    direct handler use working without the full loader.
    """
    pipe = getattr(lit_api, "_pipeline", None)
    if pipe is None:
        pipe = Pipeline.build(lit_api, [])
        lit_api._pipeline = pipe
    return pipe




def _make_status(ok: bool, message: str = "") -> Status:
    return Status(code="Ok" if ok else "Error", message=message)


def _make_error_response(uid: str, message: str,
                         status_code: int | None = None,
                         error_type: str | None = None,
                         code: str | None = None,
                         param: str | None = None,
                         headers: dict[str, str] | None = None) -> Response:
    # Default unexpected worker exceptions to a structured 500 server_error.
    # Carrying the HTTP status code in Status.message lets the Rust side route
    # these through ModelError (handlers.rs) so the client sees the real error
    # instead of a sanitized WORKER_CRASHED. WorkerCrashed must be reserved for
    # cases where the worker process is actually dead.
    if status_code is None:
        status_code = 500
        if error_type is None:
            error_type = "server_error"
    # Four-field error body contract: code/param are always present (null
    # when unset), matching the Rust HTTP error response shape.
    error_dict: dict = {
        "type": error_type or "model_error",
        "message": message,
        "code": code,
        "param": param,
    }
    data = json.dumps({"error": error_dict}).encode()
    status = Status(code="Error", message=str(status_code))
    single = SingleResponse(data=data, status=status)
    if headers:
        single.headers.update({str(k): str(v) for k, v in headers.items()})
    return Response(uid=uid, single=single)


def _merge_err_headers(ctx: RequestContext, e: Exception) -> dict[str, str] | None:
    """Headers to attach to an error frame: ctx.response_headers (e.g. from a
    Cors callback) first, then the exception's own headers (e.g. Retry-After
    on 429/503) win. Returns None when neither is set."""
    hdrs = dict(ctx.response_headers)
    extra = getattr(e, "headers", None)
    if extra:
        hdrs.update(extra)
    return hdrs or None


def _format_exc_brief(exc: BaseException) -> str:
    """Exception type, message, and where it raised — on one short line.

    Used for the default-level ERROR log so a failure can be located (the
    deepest frame is almost always the user's model.py) WITHOUT dumping a
    multi-line traceback: the Rust stderr forwarder splits on newlines, so a
    multi-line traceback would explode one failure into many log events, and
    a very long single line is not reliably forwarded either. The full
    multi-line traceback is logged separately at DEBUG.
    """
    frames = traceback.extract_tb(exc.__traceback__) if exc.__traceback__ else []
    if frames:
        fr = frames[-1]
        return f"{type(exc).__name__}: {exc} @ {fr.filename}:{fr.lineno} in {fr.name}"
    return f"{type(exc).__name__}: {exc}"


def _make_stream_error(stream_id: str, message: str,
                       error_type: str | None = None,
                       code: str | None = None,
                       param: str | None = None) -> Response:
    if error_type is not None:
        # Structured error for model-level HTTPException in streaming.
        # The StreamError.message contains a JSON object that the Rust/tonic
        # side parses to produce a structured error event.
        # code/param are always present (null when unset) — see _make_error_response.
        error_dict: dict = {
            "type": error_type,
            "message": message,
            "code": code,
            "param": param,
        }
        msg = json.dumps({"error": error_dict})
    else:
        msg = message
    return Response(
        uid=f"stream-error-{stream_id}",
        stream=StreamResponse(
            stream_id=stream_id,
            error=StreamError(message=msg),
        ),
    )


def _make_stream_chunk(stream_id: str, data: bytes, is_final: bool = False) -> Response:
    return Response(
        uid=f"stream-chunk-{stream_id}",
        stream=StreamResponse(
            stream_id=stream_id,
            chunk=StreamChunkResponse(data=data, is_final=is_final),
        ),
    )


def _make_stream_done(stream_id: str, metrics=None) -> Response:
    return Response(
        uid=f"stream-done-{stream_id}",
        stream=StreamResponse(
            stream_id=stream_id,
            done=StreamDone(metrics=metrics),
        ),
    )


def _meta_from_proto(meta_pb) -> RequestMeta:
    # meta.payload is no longer decoded: nothing in the framework reads it,
    # and the body is already decoded once from item data.  (Proto field
    # stays on the wire; Rust may stop sending it in a later release.)
    # method defaults to POST when unset (inference never sets it; routes do).
    # P-DEADLINE: deadline_unix_ns is an optional field — present only when the
    # Rust server resolved a deadline (client x-lite-timeout / grpc-timeout, or
    # the server.timeout fallback).
    deadline_unix_ns = (
        meta_pb.deadline_unix_ns if meta_pb.HasField("deadline_unix_ns") else None
    )
    return RequestMeta(
        route=meta_pb.route,
        headers=Headers(dict(meta_pb.headers)),
        client_ip=meta_pb.client_ip,
        request_id=meta_pb.request_id,
        timestamp_ns=meta_pb.timestamp_ns,
        method=meta_pb.method or "POST",
        query=dict(meta_pb.query),
        deadline_unix_ns=deadline_unix_ns,
    )


def _build_single_response(uid: str, resp_bytes: bytes, status: Status,
                           resp_headers: dict[str, str] | None, metrics) -> Response:
    """Assemble a SingleResponse proto, unpacking embedded status/media type."""
    sc, mt, clean_headers = extract_response_meta(resp_headers)
    single_resp = SingleResponse(data=resp_bytes, status=status,
                                 status_code=sc, media_type=mt)
    if clean_headers:
        single_resp.headers.update(clean_headers)
    return Response(uid=uid, single=single_resp, metrics=metrics)


# ---------------------------------------------------------------------------
# Main Loop (unified async)
# ---------------------------------------------------------------------------

