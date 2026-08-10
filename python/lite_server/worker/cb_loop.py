"""Continuous batching loop.

Split out of ``inference.py`` (0.7.7 debt-free phase 1). The former
``run_cb_loop`` closures (``_drive`` / ``_handle_add`` / ``step_loop`` …) are
now explicit ``CBLoop`` attributes/methods so the state survives the module
split; ``run_cb_loop`` remains as a thin wrapper keeping the historical call
signature (``model_name`` is accepted but unused, as before).
"""

import asyncio
import threading
import time

import zmq

from lite_server.api import LitAPI
from lite_server.context import CBSequence, Headers, RequestContext, RequestMeta
from lite_server.exceptions import HTTPException
from lite_server.pipeline import _adapt, _wrap_ctx_method
from lite_server.proto import CBAddRequest, Request, Response, SingleResponse
from lite_server.worker.common import (
    _build_single_response,
    _format_exc_brief,
    _get_pipeline,
    _is_health_probe,
    _make_error_response,
    _make_status,
    _make_stream_error,
    _merge_err_headers,
    _meta_from_proto,
    _parse_json_payload,
)
from lite_server.worker.dispatch import _handle_file_changed


class CBLoop:
    """Autonomous continuous batching loop.

    ``prefill`` / ``step`` / ``has_finished`` may be sync or async; all are
    driven on a dedicated event loop inside the step thread.  Requests flow
    through the Pipeline, so CB gets the same hook coverage (before_decode_request →
    decode → after_decode_request on add; after_predict → encode → after_encode_response on complete)
    and early-return support as every other mode.
    """

    def __init__(self, lit_api: LitAPI, socket: zmq.Socket, log):
        self.lit_api = lit_api
        self.socket = socket
        self.log = log
        self.active: dict[str, CBSequence] = {}
        self.lock = threading.Lock()
        self.pipe = _get_pipeline(lit_api)
        self.loop = asyncio.new_event_loop()
        self.prefill = _wrap_ctx_method(lit_api.prefill, "prefill")
        self.step_fn = _adapt(lit_api.step)  # step prohibits ctx (validated at load)
        self.has_finished = _wrap_ctx_method(lit_api.has_finished, "has_finished")

    def _drive(self, coro):
        return self.loop.run_until_complete(coro)

    def _send_ctx_response(self, uid: str, ctx: RequestContext):
        resp_bytes, status, metrics, resp_headers = self.pipe.finalize(ctx)
        response = _build_single_response(uid, resp_bytes, status, resp_headers, metrics)
        self.socket.send(response.SerializeToString())

    def _handle_add(self, cb_add: CBAddRequest):
        meta = _meta_from_proto(cb_add.meta) if cb_add.HasField("meta") else RequestMeta(
            route="", headers=Headers(), client_ip="", request_id="", timestamp_ns=0,
        )
        ctx = RequestContext(meta=meta, request={}, mode="cb")
        # B8: deadline already passed → skip the add entirely. Cooperative
        # stop (no synthetic frame): the server's own deadline wait produces
        # the client-facing 504 — mirrors streaming's _deadline_passed.
        remaining_ms = ctx.deadline_remaining_ms()
        if remaining_ms is not None and remaining_ms <= 0:
            self.log.info("cb add %s skipped: deadline already passed", cb_add.uid)
            return
        try:
            ctx.request = _parse_json_payload(cb_add.data)
            self._drive(self.pipe.preprocess(ctx))
        except HTTPException as e:
            self._drive(self.pipe.run_on_error(ctx, e))
            err_resp = _make_error_response(cb_add.uid, e.detail, status_code=e.status_code, error_type=e.error_type, code=e.code, param=e.param, headers=_merge_err_headers(ctx, e))
            self.socket.send(err_resp.SerializeToString())
            return
        except Exception as e:
            self._drive(self.pipe.run_on_error(ctx, e))
            err_resp = _make_error_response(cb_add.uid, str(e), headers=_merge_err_headers(ctx, e))
            self.socket.send(err_resp.SerializeToString())
            return
        if ctx.early is not None:
            # Early return (e.g. cache hit in before_decode_request): respond now,
            # skip prefill entirely.
            self._send_ctx_response(cb_add.uid, ctx)
            return

        state = CBSequence(cb_add.uid, ctx)
        self.active[cb_add.uid] = state

        try:
            self._drive(self.prefill(cb_add.uid, ctx.input, ctx=ctx))
            state.prefilled = True
        except HTTPException as e:
            del self.active[cb_add.uid]
            self._drive(self.pipe.run_on_error(ctx, e))
            err_resp = _make_error_response(cb_add.uid, e.detail, status_code=e.status_code, error_type=e.error_type, code=e.code, param=e.param, headers=_merge_err_headers(ctx, e))
            self.socket.send(err_resp.SerializeToString())
        except Exception as e:
            del self.active[cb_add.uid]
            self._drive(self.pipe.run_on_error(ctx, e))
            err_resp = _make_error_response(cb_add.uid, f"prefill failed: {e}", headers=_merge_err_headers(ctx, e))
            self.socket.send(err_resp.SerializeToString())

    def _step_loop(self):
        asyncio.set_event_loop(self.loop)
        while True:
            with self.lock:
                # Snapshot under the lock; the idle sleep stays OUTSIDE it.
                # Sequences CAN vanish before the work block re-acquires
                # (cb_remove / deadline expiry / prefill failure) — the work
                # block therefore re-filters `ready` against live membership
                # and the completion path pops with a default.
                # B8: drop sequences whose deadline passed mid-generation —
                # cooperative stop, mirrors streaming's per-chunk check.
                expired = []
                for uid, s in self.active.items():
                    rem = s.ctx.deadline_remaining_ms()
                    if rem is not None and rem <= 0:
                        expired.append(uid)
                for uid in expired:
                    self.active.pop(uid, None)
                    self.log.info("cb sequence %s dropped: deadline reached", uid)
                ready = [s for s in self.active.values() if s.prefilled]
            if not ready:
                time.sleep(0.001)
                continue

            with self.lock:
                # Re-filter against live membership: a cb_remove/expiry that
                # landed between the snapshot and this re-acquire must not
                # burn one more step of compute on a removed sequence.
                ready = [s for s in ready if s.uid in self.active]
                if not ready:
                    continue
                try:
                    outputs = self._drive(self.step_fn(ready))
                except HTTPException as e:
                    self.log.warning("cb step rejected: %s", e.detail)
                    for state in list(self.active.values()):
                        self._drive(self.pipe.run_on_error(state.ctx, e))
                        err_resp = _make_error_response(state.uid, e.detail, status_code=e.status_code, error_type=e.error_type, code=e.code, param=e.param, headers=_merge_err_headers(state.ctx, e))
                        self.socket.send(err_resp.SerializeToString())
                    self.active.clear()
                    continue
                except Exception as e:
                    self.log.error("cb step error: %s", _format_exc_brief(e))
                    for state in list(self.active.values()):
                        self._drive(self.pipe.run_on_error(state.ctx, e))
                        err_resp = _make_error_response(state.uid, f"step failed: {e}", headers=_merge_err_headers(state.ctx, e))
                        self.socket.send(err_resp.SerializeToString())
                    self.active.clear()
                    continue

                completed = []
                for state, token in zip(ready, outputs):
                    state.output.append(token)
                    # B8: has_finished raising must not escape — the step loop
                    # runs on a daemon thread, so an uncaught exception here
                    # would kill it and hang every subsequent CB request.
                    try:
                        finished = self._drive(self.has_finished(state.uid, token, state.output, ctx=state.ctx))
                    except Exception as e:
                        self.log.error("cb has_finished error for %s: %s", state.uid, _format_exc_brief(e))
                        self._drive(self.pipe.run_on_error(state.ctx, e))
                        self.active.pop(state.uid, None)
                        self.socket.send(_make_error_response(
                            state.uid, f"has_finished failed: {e}",
                            headers=_merge_err_headers(state.ctx, e),
                        ).SerializeToString())
                        continue
                    if finished:
                        completed.append(state.uid)

                for uid in completed:
                    # cb_remove / deadline expiry may have popped the sequence
                    # between the ready snapshot and now — skip instead of
                    # KeyError (an uncaught exception would kill this daemon
                    # step thread and hang every subsequent CB request).
                    state = self.active.pop(uid, None)
                    if state is None:
                        continue
                    state.ctx.output = state.output
                    state.ctx.early = None
                    try:
                        self._drive(self.pipe.postprocess(state.ctx))
                        self._send_ctx_response(uid, state.ctx)
                    except HTTPException as e:
                        self.log.warning("cb encode rejected for %s: %s", uid, e.detail)
                        self._drive(self.pipe.run_on_error(state.ctx, e))
                        err_resp = _make_error_response(uid, e.detail, status_code=e.status_code, error_type=e.error_type, code=e.code, param=e.param, headers=_merge_err_headers(state.ctx, e))
                        self.socket.send(err_resp.SerializeToString())
                    except Exception as e:
                        self.log.error("cb encode error for %s: %s", uid, _format_exc_brief(e))
                        self._drive(self.pipe.run_on_error(state.ctx, e))
                        err_resp = _make_error_response(uid, f"encode failed: {e}", headers=_merge_err_headers(state.ctx, e))
                        self.socket.send(err_resp.SerializeToString())

            time.sleep(0.001)

    def run(self):
        step_thread = threading.Thread(target=self._step_loop, daemon=True)
        step_thread.start()

        try:
            while True:
                try:
                    req_bytes = self.socket.recv()
                except zmq.ZMQError as e:
                    if e.errno == zmq.ETERM:
                        break
                    continue

                try:
                    request = Request()
                    request.ParseFromString(req_bytes)
                except Exception as e:
                    self.log.warning("cb protobuf parse error: %s", e)
                    continue

                if request.HasField("stop"):
                    # Graceful stop (server unload / shutdown): break the recv
                    # loop — run_cb_loop returns and worker_main's finally
                    # fires _run_teardown (LitAPI.teardown + lifecycle hooks).
                    break

                with self.lock:
                    if request.HasField("single"):
                        # N5: health probe — reply Ok directly; a probe must
                        # not prefill a fake CB sequence (each one otherwise
                        # burns a full prefill + generation).
                        if request.HasField("meta") and _is_health_probe(
                            _meta_from_proto(request.meta)
                        ):
                            probe_resp = Response(
                                uid=request.uid,
                                single=SingleResponse(
                                    data=b"{}", status=_make_status(True),
                                ),
                            )
                            self.socket.send(probe_resp.SerializeToString())
                            continue
                        # Route standard SingleRequest through CB pipeline
                        cb_add = CBAddRequest()
                        cb_add.uid = request.uid
                        cb_add.data = request.single.data
                        if request.HasField("meta"):
                            cb_add.meta.CopyFrom(request.meta)
                        self._handle_add(cb_add)
                    elif request.HasField("cb_remove"):
                        # B2: client disconnect / server-side timeout evicted
                        # the pending reply — stop generating for this uid.
                        # Idempotent: an unknown uid (already completed /
                        # never added) is a no-op.
                        rm_uid = request.cb_remove.uid
                        if self.active.pop(rm_uid, None) is not None:
                            self.log.info("cb sequence %s removed by server", rm_uid)
                    elif request.HasField("stream"):
                        # B5: CB workers do not serve streams — answer stream
                        # opens with an explicit terminal error instead of
                        # swallowing (the client otherwise hangs until its
                        # own deadline). Late chunk/close/cancel frames for a
                        # stream that never opened are ignored: the server
                        # drops the route on the terminal error.
                        if request.stream.HasField("open"):
                            self.socket.send(_make_stream_error(
                                request.stream.stream_id,
                                "continuous_batching model does not support streaming",
                                error_type="not_implemented",
                            ).SerializeToString())
                    elif request.HasField("file_changed"):
                        # B6: same hot-reload contract as dispatch workers —
                        # run on_file_changed and reply handled=true/false so
                        # the server does not stall for file_changed_timeout
                        # (default 60s) and then force-restart the version.
                        resp = _handle_file_changed(
                            self.lit_api, request.uid, request.file_changed, self.log,
                        )
                        self.socket.send(resp.SerializeToString())
        finally:
            self.loop.close()


def run_cb_loop(lit_api: LitAPI, socket: zmq.Socket, model_name: str, log):
    """Start the continuous batching loop (see :class:`CBLoop`).

    ``model_name`` is accepted for historical call-signature compatibility
    but unused.
    """
    CBLoop(lit_api, socket, log).run()
