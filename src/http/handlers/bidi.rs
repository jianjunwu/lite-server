//! HTTP/2 bidirectional streaming handler (D2/D3).
//! POST /v2/models/{m}/bidi  (h2 only; h1 returns 426 Upgrade Required).
//!
//! Wire format: LPM frames (§6.3) — 1B flag + 4B BE length + prost BidiChunk.
//! The first frame must be `BidiOpen`; model/version/headers are taken from the
//! URL path and HTTP headers (not from the `BidiOpen` fields on this path).

use super::inference::{build_request_meta, resolve_version};
use super::{enforce_auth, enforce_rate_limit, is_json_content_type};
use crate::error::{AppError, ProtocolError};
use crate::http::state::AppState;
use crate::metrics::prometheus;
use crate::proto::liteserver as pb;
use crate::registry::types::ModelType;
use crate::request_context::RequestContext;
use crate::streaming;
use crate::streaming::lpm;
use crate::transport::zmq::WorkerZmqClient;
use axum::{
    body::Body,
    extract::{Path, State},
    http::HeaderMap,
    response::Response,
};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use tokio::sync::mpsc;
use futures::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::Instrument;

/// Content-Type for the LPM bidi response stream.
const BIDI_CONTENT_TYPE: &str = "application/x-lite-bidi";

pub async fn h2_bidi_handler(
    state: State<Arc<AppState>>,
    path: Path<String>,
    headers: HeaderMap,
    cx: RequestContext,
    request: axum::extract::Request,
) -> Result<Response, ProtocolError> {
    let model_name = path.0;
    let protocol = cx.api_protocol.unwrap_or(crate::protocol::ApiProtocol::Legacy);
    h2_bidi_entry(state, &model_name, None, headers, cx, request)
        .await
        .map_err(|error| ProtocolError { error, protocol })
}

pub async fn h2_bidi_version_handler(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
    headers: HeaderMap,
    cx: RequestContext,
    request: axum::extract::Request,
) -> Result<Response, ProtocolError> {
    let (model_name, version) = path.0;
    let protocol = cx.api_protocol.unwrap_or(crate::protocol::ApiProtocol::Legacy);
    h2_bidi_entry(state, &model_name, Some(version), headers, cx, request)
        .await
        .map_err(|error| ProtocolError { error, protocol })
}

async fn h2_bidi_entry(
    State(state): State<Arc<AppState>>,
    model_name: &str,
    version: Option<String>,
    headers: HeaderMap,
    cx: RequestContext,
    request: axum::extract::Request,
) -> Result<Response, AppError> {
    // S1(b)/D7:open 前的早期拒绝也计一次请求(对齐 gRPC wrapper 双点语义)。
    // label:resolve 成功后用 resolved_version,失败用请求原值(可为空串)。
    let start = std::time::Instant::now();
    let mut label_version = version.clone().unwrap_or_default();
    let result = h2_bidi_entry_impl(
        state,
        model_name,
        version,
        headers,
        cx,
        request,
        &mut label_version,
    )
    .await;
    if let Err(e) = &result {
        prometheus::record_stream_rejected(
            model_name,
            &label_version,
            super::status_family(e.http_status().as_u16() as i32),
            start.elapsed().as_secs_f64(),
        );
    }
    result
}

async fn h2_bidi_entry_impl(
    state: Arc<AppState>,
    model_name: &str,
    version: Option<String>,
    headers: HeaderMap,
    cx: RequestContext,
    request: axum::extract::Request,
    label_version: &mut String,
) -> Result<Response, AppError> {
    // 1. h2 version gate: h1 → 426 Upgrade Required.
    if request.version() != axum::http::Version::HTTP_2 {
        return Response::builder()
            .status(axum::http::StatusCode::UPGRADE_REQUIRED)
            .header(axum::http::header::UPGRADE, "h2c")
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"error":{"message":"HTTP/2 bidirectional streaming requires h2. Use prior knowledge or TLS ALPN.","type":"upgrade_required"}}"#,
            ))
            .map_err(|e| AppError::Internal(format!("build response: {e}")));
    }

    // D6: Content-Encoding is not supported — reject before reading the body
    // stream so clients get a consistent 415 on all inference routes.
    if headers.contains_key(axum::http::header::CONTENT_ENCODING) {
        return Err(AppError::UnsupportedMediaType(
            "Content-Encoding (compressed request body) is not supported; \
             send the request body uncompressed"
                .into(),
        ));
    }

    // 2. Path validation → resolve_version → ready → auth → rate limit.
    crate::validation::validate_identifier(model_name)?;
    if let Some(ref v) = version {
        crate::validation::validate_version(v)?;
    }
    let (resolved_version, pinned) = resolve_version(&state, model_name, version, &headers).await?;
    *label_version = resolved_version.clone();
    if !state.registry.is_ready(model_name, Some(&resolved_version)) {
        return Err(AppError::ModelNotReady(format!(
            "{} version {} is not ready",
            model_name, resolved_version
        )));
    }
    if let Some(mv) = state.registry.get(model_name, Some(&resolved_version)) {
        enforce_auth(mv.policies.auth.as_ref(), &headers)?;
        enforce_rate_limit(&state, mv.policies.rate_limit.as_ref(), model_name, &cx.client_ip)
            .await?;
    }

    // 3. Read the first LPM frame (must be Open) from the body stream, with
    // an idle budget from server.timeout. Returns the frame + buffered
    // remaining bytes + the body stream itself, which the incoming task keeps
    // reading for the rest of the session (bidi C→S direction).
    // FD-5: when server.timeout is disabled (<=0), fall back to the always-on
    // decoupled idle budget so a connected-but-silent client cannot pin the
    // handler forever; only both budgets disabled leaves the wait unbounded.
    let first_frame_idle = crate::deadline::idle_budget(state.config.server.timeout)
        .or_else(|| crate::deadline::idle_budget(state.config.server.decoupled_idle_timeout_secs));
    let body_stream = request.into_body().into_data_stream();
    let (first_frame, remainder_buf, body_stream) =
        read_first_lpm_frame(body_stream, first_frame_idle).await?;

    let initial_data = match &first_frame.payload {
        Some(pb::bidi_chunk::Payload::Open(open)) => open.initial_data.clone(),
        _ => {
            return Err(AppError::InvalidRequestBody(
                "first LPM frame must be BidiOpen".to_string(),
            ));
        }
    };

    // B1: validate JSON initial_data before it reaches the worker
    // (mirrors Python _payload_content_type, streaming.py:48).
    // Empty initial_data is legal and skips validation (E2: Python
    // _parse_request_json(b"") → {}).
    let initial_is_json = bidi_initial_data_is_json(&headers);
    let initial_body_len = initial_data.len();
    if !initial_data.is_empty() && initial_is_json {
        serde_json::from_slice::<&serde_json::value::RawValue>(&initial_data)
            .map_err(|e| AppError::InvalidRequestBody(
                format!("invalid JSON in BidiOpen initial_data: {e}")))?;
    }

    // 4. Build RequestMeta from HTTP headers + initial_data bytes.
    let deadline = crate::deadline::resolve_from_http(&headers, state.config.server.timeout);
    let meta = build_request_meta(
        &headers,
        initial_data,
        "/predict",
        &cx,
        deadline.unix_ns,
    );

    let stream_id = format!("http-bidi-{}", uuid::Uuid::new_v4());

    // §4.3 endpoint adaptation: ensemble models have no workers — the
    // upstream is AGGREGATED into one root input (D17), the trigger is
    // half-close (body EOF, D33), and the DAG's tail stream replaces the
    // worker stream below (same outgoing forward loop).
    let is_ensemble = state
        .registry
        .get(model_name, Some(&resolved_version))
        .map(|mv| mv.model_type == ModelType::Ensemble)
        .unwrap_or(false);

    // P10 (D40): semaphore permit held for the outgoing task's lifetime.
    let mut ensemble_permit = None;
    // D35 (E5): the tail step's timeout cap, captured at the ensemble
    // dispatch (which precedes the stream_deadline computation) and folded
    // into the recv overall bound below.
    let mut ensemble_step_deadline: Option<std::time::Instant> = None;
    // D18: chain handles + chain-tree abort held for the outgoing task's
    // cancellation (pipeline chains broadcast over every streaming step).
    let mut ensemble_chain: Option<Arc<std::sync::Mutex<Vec<crate::ensemble::StreamHandle>>>> = None;
    let mut ensemble_abort: Option<tokio::task::AbortHandle> = None;
    // §4.1 指标行: streaming-step latency is recorded at stream close.
    let mut ensemble_tail: Option<(String, String, String)> = None;

    // S5 (§4.4): on the declared-inputs ensemble path the DAG executes on
    // the first envelope frame; the rest of the body is watched by a small
    // reader (spawned once the outgoing channel exists) that answers late
    // data frames with one LPM Error frame instead of dropping them
    // silently. (buf remainder, body stream)
    let mut post_trigger_body: Option<(BytesMut, axum::body::BodyDataStream)> = None;

    // 5. Open worker stream (or run the DAG for ensemble). The incoming task
    // (client→worker chunk forwarding) is spawned ONLY on the non-ensemble
    // path — ensemble aggregates the upstream inline and half-close (body
    // EOF) ends input, so no frames can follow (h2 semantics, D33).
    let (worker_client, mut chunk_rx, incoming_task) = if is_ensemble {
        let max_body = state
            .config
            .server
            .max_request_body_bytes
            .unwrap_or(64 * 1024 * 1024);
        let ct = headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        // D33: a declared-inputs ensemble executes on the FIRST envelope
        // frame — the remaining body stream is handed to the post-trigger
        // reader (S5: late data frames get one LPM Error frame); half-close
        // is then the client's normal finish.
        let declared = crate::ensemble::ensemble_declares_inputs(
            &state, model_name, &resolved_version,
            crate::ensemble::dag_selector_from_http(&headers)?.as_deref(),
        )
        .await?;
        let value = if declared {
            post_trigger_body = Some((remainder_buf, body_stream));
            // initial_data moved into meta above — meta.payload is the same
            // bytes.
            crate::ensemble::bidi_envelope_frame(&meta.payload, initial_is_json, ct.clone())?
        } else {
        let mut aggregator = crate::ensemble::BidiAggregator::new(max_body);
        // initial_data moved into meta above — meta.payload is the same bytes.
        aggregator.push(meta.payload.clone(), initial_is_json, ct.as_deref())?;
        // Aggregation loop: decode LPM frames until the body stream EOFs
        // (half-close → trigger). Chunk-idle budget applies between frames
        // (an abandoned aggregating client is reclaimed); the overall
        // deadline runs since the FIRST frame (aggregation eats the DAG
        // budget — the client is effectively streaming its body).
        let mut buf = remainder_buf;
        let mut body_stream = body_stream;
        let agg_idle = crate::deadline::idle_budget(
            state.config.server.decoupled_idle_timeout_secs,
        );
        let agg_deadline = if deadline.client_specified {
            crate::deadline::to_instant(deadline.unix_ns)
        } else {
            None
        };
        let aggregate_start = std::time::Instant::now();
        let mut last_activity = aggregate_start;
        loop {
            if let Some(idle) = agg_idle {
                if last_activity.elapsed() > idle {
                    return Err(AppError::InferenceTimeout(format!(
                        "bidi aggregation idle timeout for {}",
                        model_name
                    )));
                }
            }
            if let Some(d) = agg_deadline {
                if std::time::Instant::now() >= d {
                    return Err(AppError::InferenceTimeout(
                        "bidi aggregation exceeded the overall deadline".to_string(),
                    ));
                }
            }
            match lpm::try_decode_frame(&mut buf) {
                Ok(Some(chunk)) => match chunk.payload {
                    Some(pb::bidi_chunk::Payload::Data(data)) => {
                        aggregator.push(data.data, initial_is_json, ct.as_deref())?;
                        last_activity = std::time::Instant::now();
                    }
                    Some(pb::bidi_chunk::Payload::Close(_)) => break,
                    _ => {}
                },
                Ok(None) => {
                    // D17: the recv itself must be bounded — a loop-top idle
                    // check between blocking recvs would never fire for a
                    // silent client (it would pin the aggregation buffer
                    // forever; N connections = N× max_request_body_bytes).
                    // Bound = min(overall remaining, idle remaining), the
                    // recv_chunk two-stage semantics.
                    let now = std::time::Instant::now();
                    let idle_rem = agg_idle.map(|i| i.saturating_sub(last_activity.elapsed()));
                    let overall_rem = agg_deadline.map(|d| d.saturating_duration_since(now));
                    let bound = match (overall_rem, idle_rem) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (Some(a), None) => Some(a),
                        (None, Some(b)) => Some(b),
                        (None, None) => None,
                    };
                    let next = match bound {
                        Some(b) => match tokio::time::timeout(b, body_stream.next()).await {
                            Ok(v) => v,
                            Err(_) => {
                                let deadline_fired = agg_deadline
                                    .map(|d| d <= std::time::Instant::now())
                                    .unwrap_or(false);
                                return Err(AppError::InferenceTimeout(if deadline_fired {
                                    "bidi aggregation exceeded the overall deadline".to_string()
                                } else {
                                    format!("bidi aggregation idle timeout for {}", model_name)
                                }));
                            }
                        },
                        None => body_stream.next().await,
                    };
                    match next {
                        Some(Ok(bytes)) => buf.extend_from_slice(&bytes),
                        // Body EOF → half-close → trigger (D33).
                        None => break,
                        // Transport error mid-aggregation is NOT a clean
                        // half-close: abandon the execution (§4.4), never run
                        // the DAG on possibly-truncated input.
                        Some(Err(e)) => {
                            return Err(AppError::Internal(format!(
                                "bidi body stream error during aggregation: {e}"
                            )));
                        }
                    }
                }
                Err(_) => {
                    return Err(AppError::InvalidRequestBody(
                        "bidi LPM frame error during aggregation".to_string(),
                    ));
                }
            }
        }
            crate::metrics::prometheus::record_ensemble_bidi_aggregate(
                aggregator.total_bytes(),
                aggregate_start.elapsed().as_secs_f64(),
            );
            aggregator.finish()?
        };
        // E8-1 (D38): the dag selector rides the h2 stream headers.
        let opts = crate::ensemble::EnsembleExecOpts {
            client_ip: cx.client_ip.clone(),
            deadline_unix_ns: deadline.unix_ns,
            decoupled: false,
            dag_selector: crate::ensemble::dag_selector_from_http(&headers)?,
        };
        match crate::ensemble::execute_ensemble(
            state.clone(),
            model_name,
            &resolved_version,
            value,
            &cx.request_id,
            opts,
        )
        .await?
        {
            crate::ensemble::EnsembleOutcome::Stream(mut s) => {
                ensemble_step_deadline = s.step_deadline;
                ensemble_permit = s.permit.take();
                ensemble_chain = Some(s.chain.clone());
                ensemble_abort = Some(s.abort.clone());
                ensemble_tail = Some((s.tail_step.clone(), s.tail_model.clone(), s.tail_version.clone()));
                (s.cancel_client, s.chunk_rx, None)
            }
            crate::ensemble::EnsembleOutcome::Unary(_) => {
                return Err(AppError::InvalidRequestBody(
                    "ensemble DAG has no streaming step; use a unary endpoint"
                        .to_string(),
                ));
            }
        }
    } else {
        let (wc, rx) = open_worker_stream_bidi(&state, model_name, &resolved_version, meta, &stream_id)
            .await?;
        // Incoming task: decode LPM frames from the request body → worker
        // (fire-and-forget). The body stream is OWNED here for the whole
        // session — frames may arrive long after Open (full-duplex).
        let stream_id_inc = stream_id.clone();
        let incoming_client = Arc::clone(&wc);
        let incoming_task = tokio::spawn(async move {
            let mut buf = remainder_buf;
            let mut body_stream = body_stream;
            let mut close_sent = false;
            loop {
                match lpm::try_decode_frame(&mut buf) {
                    Ok(Some(chunk)) => {
                        match chunk.payload {
                            Some(pb::bidi_chunk::Payload::Data(data)) => {
                                let req = streaming::build_stream_chunk(
                                    stream_id_inc.clone(),
                                    data.data,
                                );
                                let _ = incoming_client.send_raw(req).await;
                            }
                            Some(pb::bidi_chunk::Payload::Close(_)) => {
                                let req = streaming::build_stream_close(
                                    stream_id_inc.clone(),
                                );
                                let _ = incoming_client.send_raw(req).await;
                                close_sent = true;
                                break; // graceful close
                            }
                            _ => {} // Open after first frame → ignore; Error → ignore
                        }
                    }
                    Ok(None) => {
                        // Need more data — read the next body chunk.
                        match body_stream.next().await {
                            Some(Ok(bytes)) => buf.extend_from_slice(&bytes),
                            // Transport error / body EOF → D4 close below.
                            Some(Err(_)) | None => break,
                        }
                    }
                    // F-09: a frame decode error must NOT skip the D4 close
                    // below — break so the worker's input still half-closes
                    // (otherwise its generator waits until its own timeout).
                    Err(e) => {
                        tracing::warn!(
                            stream_id = %stream_id_inc,
                            error = %e,
                            "h2 bidi: malformed client frame; closing worker input"
                        );
                        break;
                    }
                }
            }
            // D4: body EOF (or transport error) without an explicit Close
            // frame → gracefully end worker input (same half-close
            // semantics as gRPC bidi, PR-1).
            if !close_sent {
                let _ = incoming_client
                    .send_raw(streaming::build_stream_close(stream_id_inc))
                    .await;
            }
        });
        (wc, rx, Some(incoming_task))
    };

    // Task D: fire InferenceRequest.
    let cb_runner = state.callback_runner.clone();
    let req_ctx = crate::callback::InferenceContext {
        model_name: model_name.to_string(),
        version: resolved_version.clone(),
        route: "/predict".to_string(),
        protocol: crate::callback::Protocol::Http2,
        request_id: cx.request_id.clone(),
        client_ip: cx.client_ip.clone(),
        elapsed_us: None,
    };
    crate::callback::fire_inference_request(&cb_runner, &req_ctx);

    let stream_metrics = state.config.features.streaming_metrics;
    if stream_metrics {
        prometheus::record_stream_open(model_name, &resolved_version, "http2", &stream_id, false);
    }

    // P-DEADLINE: overall deadline client-specified only; chunk-idle always on.
    let stream_deadline = crate::deadline::min_instant(
        if deadline.client_specified {
            crate::deadline::to_instant(deadline.unix_ns)
        } else {
            None
        },
        // D35 (E5): the ensemble tail step's timeout cap.
        ensemble_step_deadline,
    );
    let stream_idle =
        crate::deadline::idle_budget(state.config.server.decoupled_idle_timeout_secs);

    // Spawn forwarder: incoming → worker, worker → outgoing → response body.
    let (tx, rx) = mpsc::channel::<Bytes>(64);

    // S5 (§4.4): declared-inputs ensemble — data frames after the envelope
    // trigger are a protocol violation; answer the first one with ONE LPM
    // Error frame on the response stream, then stop reading.
    if let Some((mut late_buf, mut late_stream)) = post_trigger_body {
        let tx_violation = tx.clone();
        let stream_id_violation = stream_id.clone();
        tokio::spawn(async move {
            loop {
                match lpm::try_decode_frame(&mut late_buf) {
                    Ok(Some(chunk)) => {
                        if matches!(chunk.payload, Some(pb::bidi_chunk::Payload::Data(_))) {
                            let frame = lpm::encode_frame(&pb::BidiChunk {
                                stream_id: stream_id_violation.clone(),
                                payload: Some(pb::bidi_chunk::Payload::Error(pb::BidiError {
                                    message: "frames after the aggregation trigger are rejected (multi-round)".to_string(),
                                    error_type: String::new(),
                                })),
                            });
                            let _ = tx_violation.send(frame).await;
                            break;
                        }
                    }
                    Ok(None) => match late_stream.next().await {
                        Some(Ok(bytes)) => late_buf.extend_from_slice(&bytes),
                        _ => break,
                    },
                    Err(_) => break,
                }
            }
        });
    }
    let stream_id_out = stream_id.clone();
    let cancel_client = Arc::clone(&worker_client);
    let metrics_model = model_name.to_string();
    let metrics_version = resolved_version.clone();

    let span = tracing::info_span!(
        "inference",
        model = %model_name,
        version = %resolved_version,
        request_id = %cx.request_id,
        pinned_version = tracing::field::Empty,
            trace_id = tracing::field::Empty,
            span_id = tracing::field::Empty,
        body_bytes = tracing::field::Empty,
        body_kind = tracing::field::Empty,
    );
    crate::telemetry::link_parent(&span, &cx.trace_cx);
    // P5-2: pin recorded after span creation (resolve ran on the handler
    // span; gRPC bidi pattern).
    if let Some(p) = &pinned {
        span.record("pinned_version", p.as_str());
    }
    // D11: record body size with content-type label (unary/SSE/WS parity).
    let body_kind_str: &str = if initial_is_json { "json" } else { "raw" };
    prometheus::record_request_body_bytes(body_kind_str, "/predict", initial_body_len);
    span.record("body_bytes", initial_body_len as i64);
    span.record("body_kind", body_kind_str);

    tokio::spawn(
        async move {
            // P10 (D40): held for the outgoing task's lifetime — released on
            // drop (terminal frame / idle / disconnect; D18 teardown path).
            let _ensemble_permit = ensemble_permit;
            // Outgoing loop: worker chunks → LPM frames → tx.
            let open_time = std::time::Instant::now();
            let mut first_chunk = true;
            let mut last_chunk_time = open_time;
            // S1/S2:收口枚举——各 break 点只置 reason,尾部 record_stream_terminal
            // 统一消费(family/cancelled 单一来源)。
            let reason;
            // S6:per-stream 输出字节(Σ chunk.data.len(),收口统一上报)。
            let mut output_bytes: u64 = 0;
            // G5:per-stream chunk 数(close 日志字段,收口统一上报,非 metric)。
            let mut chunks: u64 = 0;

            loop {
                let chunk =
                    match streaming::recv_chunk(&mut chunk_rx, stream_deadline, stream_idle)
                        .await
                    {
                        Ok(Some(c)) => c,
                        Ok(None) => {
                            reason = prometheus::StreamCloseReason::WorkerEof;
                            break;
                        }
                        Err(elapsed) => {
                            tracing::warn!(
                                ?elapsed, stream_id = %stream_id_out,
                                "h2 bidi stream closed: deadline/idle elapsed"
                            );
                            // D35 (§4.4, F-11): a mid-stream reclaim —
                            // deadline OR idle — is terminal: an error frame
                            // + close (mirror the worker Error arm), so
                            // truncated output is distinguishable from the
                            // worker's normal EOF.
                            let msg = match elapsed {
                                crate::streaming::RecvElapsed::Deadline => {
                                    reason = prometheus::StreamCloseReason::Deadline;
                                    "stream closed: deadline exceeded"
                                }
                                crate::streaming::RecvElapsed::Idle => {
                                    reason = prometheus::StreamCloseReason::Idle;
                                    "stream closed: idle timeout"
                                }
                            };
                            let frame = lpm::encode_frame(&pb::BidiChunk {
                                stream_id: stream_id_out.clone(),
                                payload: Some(pb::bidi_chunk::Payload::Error(
                                    pb::BidiError {
                                        message: msg.to_string(),
                                        error_type: String::new(),
                                    },
                                )),
                            });
                            let _ = tx.send(frame).await;
                            crate::callback::fire_inference_response(
                                &cb_runner,
                                &req_ctx,
                                open_time,
                            );
                            break;
                        }
                    };
                match chunk.payload {
                    Some(pb::stream_response::Payload::Chunk(c)) => {
                        output_bytes += c.data.len() as u64;
                        chunks += 1;
                        if stream_metrics {
                            if first_chunk {
                                prometheus::record_stream_ttft(
                                    &metrics_model,
                                    &metrics_version,
                                    "http2",
                                    open_time.elapsed().as_secs_f64(),
                                );
                                first_chunk = false;
                            } else {
                                prometheus::record_stream_tbt(
                                    &metrics_model,
                                    &metrics_version,
                                    "http2",
                                    last_chunk_time.elapsed().as_secs_f64(),
                                );
                            }
                            last_chunk_time = std::time::Instant::now();
                            prometheus::record_stream_chunk(
                                &metrics_model,
                                &metrics_version,
                                "http2",
                            );
                        }
                        let frame = lpm::encode_frame(&pb::BidiChunk {
                            stream_id: stream_id_out.clone(),
                            payload: Some(pb::bidi_chunk::Payload::Data(pb::BidiData {
                                data: c.data.clone(),
                            })),
                        });
                        if tx.send(frame).await.is_err() {
                            reason = prometheus::StreamCloseReason::Cancel;
                            break;
                        }
                    }
                    Some(pb::stream_response::Payload::Error(e)) => {
                        reason = prometheus::StreamCloseReason::Error;
                        let frame = lpm::encode_frame(&pb::BidiChunk {
                            stream_id: stream_id_out.clone(),
                            payload: Some(pb::bidi_chunk::Payload::Error(pb::BidiError {
                                message: e.message.clone(),
                                error_type: String::new(),
                            })),
                        });
                        let _ = tx.send(frame).await;
                        crate::callback::fire_inference_response(
                            &cb_runner,
                            &req_ctx,
                            open_time,
                        );
                        break;
                    }
                    Some(pb::stream_response::Payload::Done(done)) => {
                        reason = prometheus::StreamCloseReason::Done;
                        prometheus::record_worker_metrics(
                            &metrics_model,
                            &metrics_version,
                            done.metrics.as_ref(),
                        );
                        let frame = lpm::encode_frame(&pb::BidiChunk {
                            stream_id: stream_id_out.clone(),
                            payload: Some(pb::bidi_chunk::Payload::Close(pb::BidiClose {})),
                        });
                        let _ = tx.send(frame).await;
                        crate::callback::fire_inference_response(
                            &cb_runner,
                            &req_ctx,
                            open_time,
                        );
                        break;
                    }
                    _ => {}
                }
            }
            // S1/S2/S4/S6 收口:无条件 record_request_end + 门控内 cancelled/errors/duration/bytes/close。
            prometheus::record_stream_terminal(
                &metrics_model,
                &metrics_version,
                "http2",
                "http2",
                open_time,
                reason.status_family(),
                reason,
                stream_metrics,
                output_bytes,
                chunks,
            );
            // §4.1 指标行: the streaming step's latency, measured at stream close.
            if let Some((tail_step, tail_model, tail_version)) = ensemble_tail {
                prometheus::record_ensemble_step_latency(
                    &metrics_model,
                    &tail_step,
                    &tail_model,
                    &tail_version,
                    0, // tail step of a top-level ensemble = depth 0 (nested ensembles cannot stream, D4)
                    open_time.elapsed().as_secs_f64(),
                );
            }

            // Targeted cancel.
            if is_ensemble {
                crate::ensemble::cancel_chain(
                    ensemble_chain.as_ref(),
                    ensemble_abort.as_ref(),
                    &stream_id_out,
                    &cancel_client,
                )
                .await;
            } else {
                let cancel_req = streaming::build_stream_cancel(stream_id_out);
                let _ = cancel_client.send_raw(cancel_req).await;
            }

            if let Some(task) = incoming_task {
                streaming::observe_or_abort(task).await;
            }
        }
        .instrument(span),
    );

    // 7. 200 + application/x-lite-bidi + streaming body.
    let body = Body::from_stream(
        ReceiverStream::new(rx).map(Ok::<Bytes, axum::Error>),
    );
    Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, BIDI_CONTENT_TYPE)
        .body(body)
        .map_err(|e| AppError::Internal(format!("build response: {e}")))
}

/// Open a worker stream for bidi.
async fn open_worker_stream_bidi(
    state: &Arc<AppState>,
    model_name: &str,
    resolved_version: &str,
    meta: pb::RequestMeta,
    stream_id: &str,
) -> Result<(Arc<WorkerZmqClient>, mpsc::Receiver<pb::StreamResponse>), AppError> {
    let mv = state
        .registry
        .get(model_name, Some(resolved_version))
        .ok_or_else(|| {
            AppError::ModelNotFound(format!("{} version {}", model_name, resolved_version))
        })?;

    let num_workers = mv.workers.len();
    if num_workers == 0 {
        return Err(AppError::WorkerCrashed(format!(
            "{} has no workers",
            model_name
        )));
    }

    let clients = state
        .worker_manager
        .get_zmq_clients(model_name, resolved_version)
        .await
        .ok_or_else(|| {
            AppError::WorkerCrashed(format!(
                "{} {} has no ZMQ clients",
                model_name, resolved_version
            ))
        })?;

    let outlier = state
        .worker_manager
        .get_outlier_state(model_name, resolved_version)
        .await;
    let seq_registry = state.inference_queue.sequence_registry();
    let worker_id = crate::worker::pick_streaming_worker(
        &meta,
        num_workers,
        outlier.as_deref(),
        seq_registry,
        model_name,
        resolved_version,
    )
    .map_err(|e| AppError::Validation(e.0))?;

    if worker_id >= clients.len() {
        return Err(AppError::WorkerCrashed("invalid worker index".to_string()));
    }

    // S8:per-worker dispatch 计数(gRPC/SSE 同位:pick 成功后立即记)。
    prometheus::record_worker_inference(model_name, resolved_version, worker_id, 1);

    let client = &clients[worker_id];
    let open_req = streaming::build_stream_open(
        stream_id.to_string(),
        meta.payload.clone(),
        Some(meta),
        false,
    );

    let chunk_rx = client.send_stream(open_req, stream_id.to_string()).await?;
    Ok((Arc::clone(client), chunk_rx))
}

/// Read the first LPM frame from a body data stream, bounded by an idle
/// budget (`None` = unbounded — only when the operator disabled both
/// server.timeout and the decoupled idle backstop, FD-5). Returns the decoded
/// `BidiChunk`, a buffer of any remaining (unconsumed) bytes, and the body
/// stream itself — the caller (incoming task) must keep reading it for the
/// rest of the bidi session.
async fn read_first_lpm_frame<S>(
    mut body_stream: S,
    idle: Option<std::time::Duration>,
) -> Result<(pb::BidiChunk, BytesMut, S), AppError>
where
    S: futures::Stream<Item = Result<Bytes, axum::Error>> + Unpin,
{
    use futures::StreamExt;

    let mut buf = BytesMut::new();

    loop {
        // Try to decode from current buffer.
        if let Some(chunk) = lpm::try_decode_frame(&mut buf).map_err(lpm_error_to_app)? {
            return Ok((chunk, buf, body_stream));
        }

        // Need more data — read next chunk from body stream.
        let next = if let Some(budget) = idle {
            match tokio::time::timeout(budget, body_stream.next()).await {
                Ok(Some(Ok(bytes))) => bytes,
                Ok(Some(Err(e))) => return Err(AppError::Transport(e.to_string())),
                Ok(None) => {
                    return Err(AppError::InvalidRequestBody(
                        "body stream ended before first LPM frame".to_string(),
                    ));
                }
                Err(_) => {
                    return Err(AppError::InferenceTimeout(
                        "timed out waiting for first LPM frame".to_string(),
                    ));
                }
            }
        } else {
            match body_stream.next().await {
                Some(Ok(bytes)) => bytes,
                Some(Err(e)) => return Err(AppError::Transport(e.to_string())),
                None => {
                    return Err(AppError::InvalidRequestBody(
                        "body stream ended before first LPM frame".to_string(),
                    ));
                }
            }
        };

        buf.extend_from_slice(&next);
    }
}

/// Convert an LPM error to an AppError.
fn lpm_error_to_app(e: lpm::LpmError) -> AppError {
    AppError::InvalidRequestBody(format!("LPM frame error: {e}"))
}

/// B1 (tensor-bytes-consistency): replicate Python `_payload_content_type`
/// (streaming.py:48) so Rust and Python share one dispatch rule for bidi
/// initial_data. The bidi framing type `application/x-lite-bidi` is a
/// transport wrapper — it says nothing about the payload, so it is treated
/// as absent (→ JSON default), exactly as the Python side does.
///
/// Returns true when the initial_data MUST be valid JSON; false when it
/// MUST be treated as opaque bytes.
fn bidi_initial_data_is_json(headers: &HeaderMap) -> bool {
    match headers.get(axum::http::header::CONTENT_TYPE) {
        None => true, // missing → JSON default (D2)
        Some(v) => {
            let base = v
                .to_str()
                .unwrap_or("")
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_lowercase();
            if base == "application/x-lite-bidi" {
                true // framing CT → JSON default (E1, mirror Python)
            } else {
                is_json_content_type(v) // delegate to D1 (parse failure → raw)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callback::CallbackRunner;
    use crate::config::ModelConfig;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::types::{ModelType, WorkerInfo, WorkerStatus};
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;
    use axum::body::Body;
    use prost::Message;
    use std::sync::Arc;

    fn ipc_endpoint(tag: &str) -> String {
        #[cfg(unix)]
        {
            format!(
                "ipc://{}",
                std::env::temp_dir()
                    .join(format!("bidi-{}-{}.sock", tag, std::process::id()))
                    .display()
            )
        }
        #[cfg(not(unix))]
        {
            format!("tcp://127.0.0.1:{}", 37000 + std::process::id() % 1000)
        }
    }

    async fn wait_for<F: Fn() -> bool>(cond: F, label: &str) {
        for _ in 0..60 {
            if cond() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        panic!("condition never met within ~1.5s: {}", label);
    }

    fn make_state(cb: Arc<CallbackRunner>) -> Arc<AppState> {
        make_state_with_config(cb, crate::config::Config::default())
    }

    fn make_state_with_config(cb: Arc<CallbackRunner>, config: crate::config::Config) -> Arc<AppState> {
        let registry = Arc::new(ModelRegistry::new());
        let queue = Arc::new(InferenceQueue::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::path::PathBuf::new(),
            queue.clone(),
            "warn".to_string(),
            cb.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            wm,
            queue,
            config,
            std::path::PathBuf::new(),
            cb,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    async fn ready_state(model: &str, endpoint: String) -> Arc<AppState> {
        ready_state_with_config(model, endpoint, crate::config::Config::default()).await
    }

    async fn ready_state_with_config(
        model: &str,
        endpoint: String,
        config: crate::config::Config,
    ) -> Arc<AppState> {
        let cb = Arc::new(CallbackRunner::new());
        let state = make_state_with_config(cb, config);
        state
            .registry
            .register(
                model,
                "1",
                ModelConfig::default(),
                ModelType::LitAPI,
                std::path::PathBuf::new(),
            )
            .unwrap();
        state.registry.mark_ready(model, "1").unwrap();
        state
            .registry
            .set_workers(
                model,
                "1",
                vec![WorkerInfo {
                    worker_id: 0,
                    device: "cpu:0".to_string(),
                    endpoint: String::new(),
                    pid: None,
                    status: WorkerStatus::Ready,
                    capacity: None,
                }],
            )
            .unwrap();
        let client = Arc::new(WorkerZmqClient::new(endpoint));
        state
            .worker_manager
            .insert_zmq_clients_for_test(model, "1", vec![client])
            .await;
        state
    }

    /// PAIR worker: records actions; on Open → one Chunk + Done.
    fn spawn_chunk_done_recording_worker(
        endpoint: String,
    ) -> (std::thread::JoinHandle<()>, std::sync::mpsc::Receiver<String>) {
        let (action_tx, action_rx) = std::sync::mpsc::channel::<String>();
        let handle = std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(10000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let Some(pb::request::Payload::Stream(st)) = req.payload else {
                    let _ = s.send(
                        pb::Response { uid: req.uid, ..Default::default() }.encode_to_vec(),
                        0,
                    );
                    continue;
                };
                let mk = |payload| pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: st.stream_id.clone(),
                        payload: Some(payload),
                    })),
                    ..Default::default()
                };
                match st.action {
                    Some(pb::stream_request::Action::Open(_)) => {
                        let _ = action_tx.send("open".to_string());
                        let _ = s.send(
                            mk(pb::stream_response::Payload::Chunk(pb::StreamChunkResponse {
                                data: Bytes::from_static(b"{}"),
                                is_final: false,
                            }))
                            .encode_to_vec(),
                            0,
                        );
                        let _ = s.send(
                            mk(pb::stream_response::Payload::Done(pb::StreamDone::default()))
                                .encode_to_vec(),
                            0,
                        );
                    }
                    Some(pb::stream_request::Action::Chunk(_)) => {
                        let _ = action_tx.send("chunk".to_string());
                    }
                    Some(pb::stream_request::Action::Close(_)) => {
                        let _ = action_tx.send("close".to_string());
                    }
                    Some(pb::stream_request::Action::Cancel(_)) => {
                        let _ = action_tx.send("cancel".to_string());
                    }
                    None => {}
                }
            }
        });
        (handle, action_rx)
    }

    /// PAIR worker: Open → ONE chunk, then stall (no Done / close) — for the
    /// client-disconnect (cancel) test: the forwarder reaches tx.send with the
    /// downstream already dropped → send Err → break(cancel), without waiting
    /// out the idle reclaim.
    fn spawn_stall_chunk_worker(endpoint: String) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let Some(pb::request::Payload::Stream(st)) = req.payload else {
                    let _ = s.send(
                        pb::Response { uid: req.uid, ..Default::default() }.encode_to_vec(),
                        0,
                    );
                    continue;
                };
                if !matches!(
                    st.action,
                    Some(pb::stream_request::Action::Open(_))
                ) {
                    continue;
                }
                let chunk = pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: st.stream_id.clone(),
                        payload: Some(pb::stream_response::Payload::Chunk(
                            pb::StreamChunkResponse {
                                data: Bytes::from_static(b"{}"),
                                is_final: false,
                            },
                        )),
                    })),
                    ..Default::default()
                };
                let _ = s.send(chunk.encode_to_vec(), 0);
                // STALL: send no Done / close.
            }
        })
    }

    /// PAIR worker: Open → Error frame, then nothing.
    fn spawn_error_worker_bidi(endpoint: String) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(5000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let is_open = matches!(
                    req.payload,
                    Some(pb::request::Payload::Stream(pb::StreamRequest {
                        action: Some(pb::stream_request::Action::Open(_)),
                        ..
                    }))
                );
                if !is_open {
                    let _ = s.send(
                        pb::Response { uid: req.uid, ..Default::default() }.encode_to_vec(),
                        0,
                    );
                    continue;
                }
                let Some(pb::request::Payload::Stream(st)) = req.payload else {
                    continue;
                };
                let _ = s.send(
                    pb::Response {
                        payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                            stream_id: st.stream_id.clone(),
                            payload: Some(pb::stream_response::Payload::Error(
                                pb::StreamError { message: "boom".to_string() },
                            )),
                        })),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                    0,
                );
            }
        })
    }

    /// Decode all complete LPM frames from a collected response body.
    fn decode_all_frames(bytes: &[u8]) -> Vec<pb::BidiChunk> {
        let mut buf = BytesMut::from(bytes);
        let mut out = Vec::new();
        while let Ok(Some(c)) = lpm::try_decode_frame(&mut buf) {
            out.push(c);
        }
        out
    }

    fn test_cx() -> RequestContext {
        RequestContext {
            request_id: "bidi-rid".to_string(),
            client_ip: "127.0.0.1".to_string(),
            trace_cx: opentelemetry::Context::new(),
            protocol: crate::callback::Protocol::Http2,
            principal: None,
            api_protocol: None,
        }
    }

    /// §6.9-2/5: full handler session — worker Chunk+Done → downstream LPM
    /// [Data, Close] frames + body EOF; the worker observes Open plus the D4
    /// Close from the client's body half-close.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h2_bidi_handler_full_session_frames_and_worker_actions() {
        let model = "bidi_full";
        let endpoint = ipc_endpoint(model);
        let (_w, actions) = spawn_chunk_done_recording_worker(endpoint.clone());
        let state = ready_state(model, endpoint).await;
        state.registry.activate_version(model, "1").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Body: Open frame only, then EOF (client pipelines + half-closes).
        let (body_tx, resp) = start_bidi_session(model, state).await;
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        assert_eq!(
            resp.headers().get(axum::http::header::CONTENT_TYPE).unwrap(),
            BIDI_CONTENT_TYPE
        );
        drop(body_tx); // body EOF → D4 Close

        let body_bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .expect("drain body");
        let frames = decode_all_frames(&body_bytes);
        assert_eq!(frames.len(), 2, "expected [Data, Close] frames, got {frames:?}");
        assert!(
            matches!(frames[0].payload, Some(pb::bidi_chunk::Payload::Data(_))),
            "first downstream frame must be Data"
        );
        assert!(
            matches!(frames[1].payload, Some(pb::bidi_chunk::Payload::Close(_))),
            "terminal downstream frame must be Close"
        );
        // §6.5: server-generated stream_id echoed on every frame.
        assert!(frames[0].stream_id.starts_with("http-bidi-"));
        assert_eq!(frames[0].stream_id, frames[1].stream_id);

        // Worker saw Open; the D4 half-close delivered Close (the EOF-Close
        // may race the worker's own Done, so don't assert full ordering).
        let mut seen = Vec::new();
        while let Ok(a) = actions.recv_timeout(std::time::Duration::from_secs(3)) {
            seen.push(a);
            if seen.len() >= 3 {
                break;
            }
        }
        assert_eq!(seen.first().map(String::as_str), Some("open"));
        assert!(
            seen.iter().any(|a| a == "close"),
            "D4 close must reach the worker, got {seen:?}"
        );
    }

    /// §6.9-5: worker Error → in-band error frame + body EOF (terminal).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h2_bidi_worker_error_yields_error_frame_then_eof() {
        let model = "bidi_err";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_error_worker_bidi(endpoint.clone());
        let state = ready_state(model, endpoint).await;
        state.registry.activate_version(model, "1").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let (body_tx, resp) = start_bidi_session(model, state).await;
        drop(body_tx);
        let body_bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .expect("drain body");
        let frames = decode_all_frames(&body_bytes);
        assert_eq!(frames.len(), 1, "expected exactly one error frame, got {frames:?}");
        match &frames[0].payload {
            Some(pb::bidi_chunk::Payload::Error(e)) => assert_eq!(e.message, "boom"),
            other => panic!("expected error frame, got {other:?}"),
        }
    }

    /// §6.9-5: the first LPM frame must be BidiOpen — anything else is
    /// rejected before the response commits (400-class HTTP error).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h2_bidi_first_frame_not_open_is_rejected() {
        let model = "bidi_first";
        let endpoint = ipc_endpoint(model);
        let state = ready_state(model, endpoint).await;
        state.registry.activate_version(model, "1").unwrap();

        let data_first = lpm::encode_frame(&pb::BidiChunk {
            stream_id: String::new(),
            payload: Some(pb::bidi_chunk::Payload::Data(pb::BidiData {
                data: Bytes::from_static(b"x"),
            })),
        });
        let req = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/v2/models/{}/bidi", model))
            .version(axum::http::Version::HTTP_2)
            .header("content-type", BIDI_CONTENT_TYPE)
            .body(Body::from(data_first.to_vec()))
            .unwrap();

        let err = h2_bidi_handler(
            State(state),
            Path(model.to_string()),
            HeaderMap::new(),
            test_cx(),
            req,
        )
        .await
        .expect_err("first frame not Open must be rejected");
        assert!(
            matches!(err.error, AppError::InvalidRequestBody(_)),
            "expected InvalidRequestBody (400), got {err:?}"
        );
    }

    /// §6.9-7: `features.http_bidi = false` unmounts /bidi (404); the default
    /// (true) mounts it — an h1 request then reaches the version gate (426).
    #[tokio::test]
    async fn h2_bidi_route_gated_by_feature_flag() {
        use tower::ServiceExt;

        let mk_req = || {
            axum::http::Request::builder()
                .method("POST")
                .uri("/v2/models/m/bidi")
                .version(axum::http::Version::HTTP_11)
                .header("content-type", BIDI_CONTENT_TYPE)
                .body(Body::empty())
                .unwrap()
        };

        // Default (true) → mounted: h1 hits the handler's version gate → 426.
        let cb = Arc::new(CallbackRunner::new());
        let app = crate::http::routes::create_routes(make_state(cb.clone()));
        let resp = app.oneshot(mk_req()).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::UPGRADE_REQUIRED,
            "mounted /bidi must 426 on h1"
        );

        // http_bidi = false → route unmounted → 404.
        let mut cfg = crate::config::Config::default();
        cfg.features.http_bidi = false;
        let app = crate::http::routes::create_routes(make_state_with_config(cb, cfg));
        let resp = app.oneshot(mk_req()).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::NOT_FOUND,
            "http_bidi=false must unmount /bidi"
        );
    }

    #[tokio::test]
    async fn h2_bidi_version_gate_h1_returns_426() {
        use tower::ServiceExt;

        let model = "bidi_vg";
        let endpoint = ipc_endpoint(model);
        let state = ready_state(model, endpoint).await;

        let req = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/v2/models/{}/bidi", model))
            .version(axum::http::Version::HTTP_11)
            .header("content-type", BIDI_CONTENT_TYPE)
            .body(Body::empty())
            .unwrap();

        // Build a minimal Router and send the request.
        let app = axum::Router::new()
            .route(
                "/v2/models/:model_name/bidi",
                axum::routing::post(h2_bidi_handler),
            )
            .with_state(state);

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::UPGRADE_REQUIRED,
            "h1 request to /bidi must return 426"
        );
    }

    // ===== AUDIT-REPRO (2026-08-05, /audit http-bidi) =======================
    // B1: the incoming (C→S) direction of the h2 bidi handler drops the
    // request body stream — `read_first_lpm_frame` consumes it and returns
    // only the bytes buffered alongside the FIRST frame. LPM frames the
    // client sends afterwards (the entire point of bidi) never reach the
    // worker, and D4 (body EOF → worker Close) is not implemented.

    /// PAIR worker that records every StreamRequest action it receives and
    /// holds the stream open until Close arrives (then replies Done).
    fn spawn_recording_worker(
        endpoint: String,
    ) -> (std::thread::JoinHandle<()>, std::sync::mpsc::Receiver<String>) {
        let (action_tx, action_rx) = std::sync::mpsc::channel::<String>();
        let handle = std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(10000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let req = match pb::Request::decode(bytes.as_slice()) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let Some(pb::request::Payload::Stream(st)) = req.payload else {
                    // Handshake / unary — ack so the client never stalls.
                    let _ = s.send(
                        pb::Response { uid: req.uid, ..Default::default() }.encode_to_vec(),
                        0,
                    );
                    continue;
                };
                match st.action {
                    Some(pb::stream_request::Action::Open(_)) => {
                        let _ = action_tx.send("open".to_string());
                    }
                    Some(pb::stream_request::Action::Chunk(_)) => {
                        let _ = action_tx.send("chunk".to_string());
                    }
                    Some(pb::stream_request::Action::Close(_)) => {
                        let _ = action_tx.send("close".to_string());
                        let done = pb::Response {
                            payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                                stream_id: st.stream_id.clone(),
                                payload: Some(pb::stream_response::Payload::Done(
                                    pb::StreamDone::default(),
                                )),
                            })),
                            ..Default::default()
                        };
                        let _ = s.send(done.encode_to_vec(), 0);
                    }
                    Some(pb::stream_request::Action::Cancel(_)) => {
                        let _ = action_tx.send("cancel".to_string());
                    }
                    None => {}
                }
            }
        });
        (handle, action_rx)
    }

    fn open_frame_bytes() -> Bytes {
        lpm::encode_frame(&pb::BidiChunk {
            stream_id: String::new(),
            payload: Some(pb::bidi_chunk::Payload::Open(pb::BidiOpen {
                model_name: String::new(),
                version: String::new(),
                initial_data: Bytes::from_static(b"{}"),
                ..Default::default()
            })),
        })
    }

    /// Build the handler request with a live, streamed h2 body and invoke the
    /// handler directly. Returns (body sender, response).
    async fn start_bidi_session(
        model: &str,
        state: Arc<AppState>,
    ) -> (
        tokio::sync::mpsc::Sender<Result<Bytes, axum::Error>>,
        Response,
    ) {
        let (body_tx, body_rx) =
            tokio::sync::mpsc::channel::<Result<Bytes, axum::Error>>(8);
        let body = Body::from_stream(ReceiverStream::new(body_rx));
        let req = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/v2/models/{}/bidi", model))
            .version(axum::http::Version::HTTP_2)
            .header("content-type", BIDI_CONTENT_TYPE)
            .body(body)
            .unwrap();

        body_tx
            .send(Ok(open_frame_bytes()))
            .await
            .expect("send Open frame");

        let resp = h2_bidi_handler(
            State(state),
            Path(model.to_string()),
            HeaderMap::new(),
            test_cx(),
            req,
        )
        .await
        .expect("handler must accept the session");
        (body_tx, resp)
    }

    /// B1 repro #1: an LPM Data frame sent AFTER the Open frame (the normal
    /// full-duplex case) must reach the worker as StreamRequest::Chunk.
    /// Current code drops the body stream after the first frame → the Chunk
    /// never arrives → this test FAILS until the incoming task keeps reading
    /// the body stream.
    // NOTE: multi_thread — the std-mpsc recv_timeout waits below must not
    // starve the spawned handler/forwarder tasks (current_thread runtime
    // would be blocked by a blocking wait on the test future itself).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn audit_h2_bidi_data_frames_after_open_reach_worker() {
        let model = "bidi_audit_data";
        let endpoint = ipc_endpoint(model);
        let (_w, actions) = spawn_recording_worker(endpoint.clone());
        let state = ready_state(model, endpoint).await;
        state.registry.activate_version(model, "1").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let (body_tx, resp) = start_bidi_session(model, state).await;
        // Drain the response so the outgoing task never blocks on tx.
        tokio::spawn(async move {
            let _ = axum::body::to_bytes(resp.into_body(), 1 << 20).await;
        });

        assert_eq!(
            actions.recv_timeout(std::time::Duration::from_secs(3)).as_deref(),
            Ok("open"),
            "worker must see StreamOpen"
        );

        // The bidi moment: client sends more input mid-stream.
        let data_frame = lpm::encode_frame(&pb::BidiChunk {
            stream_id: String::new(),
            payload: Some(pb::bidi_chunk::Payload::Data(pb::BidiData {
                data: Bytes::from_static(b"more-input"),
            })),
        });
        body_tx
            .send(Ok(data_frame))
            .await
            .expect("body stream must stay alive for the bidi session");

        assert_eq!(
            actions.recv_timeout(std::time::Duration::from_secs(2)).as_deref(),
            Ok("chunk"),
            "worker must receive StreamRequest::Chunk for a Data frame sent after Open"
        );
    }

    /// B1 repro #2 (D4, plan §6.6 step 6 + §6.9-3): body EOF without a Close
    /// frame must send StreamRequest::Close to the worker (same half-close
    /// semantics PR-1 added to gRPC bidi). Current code has no EOF handling —
    /// the incoming task exits on an empty remainder → FAILS until D4 lands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn audit_h2_bidi_body_eof_sends_close_to_worker() {
        let model = "bidi_audit_eof";
        let endpoint = ipc_endpoint(model);
        let (_w, actions) = spawn_recording_worker(endpoint.clone());
        let state = ready_state(model, endpoint).await;
        state.registry.activate_version(model, "1").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let (body_tx, resp) = start_bidi_session(model, state).await;
        tokio::spawn(async move {
            let _ = axum::body::to_bytes(resp.into_body(), 1 << 20).await;
        });

        assert_eq!(
            actions.recv_timeout(std::time::Duration::from_secs(3)).as_deref(),
            Ok("open"),
            "worker must see StreamOpen"
        );

        // Client ends its input at the transport level: body EOF, no Close frame.
        drop(body_tx);

        assert_eq!(
            actions.recv_timeout(std::time::Duration::from_secs(2)).as_deref(),
            Ok("close"),
            "D4: body EOF must send StreamRequest::Close to the worker"
        );
    }

    // === B1: bidi_initial_data_is_json 7-state unit tests ===

    fn ct_header(val: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(axum::http::header::CONTENT_TYPE, val.parse().unwrap());
        h
    }

    #[test]
    fn bidi_is_json_missing_ct_is_true() {
        assert!(bidi_initial_data_is_json(&HeaderMap::new()));
    }

    #[test]
    fn bidi_is_json_framing_ct_is_true() {
        assert!(bidi_initial_data_is_json(&ct_header("application/x-lite-bidi")));
    }

    #[test]
    fn bidi_is_json_framing_ct_with_params_is_true() {
        assert!(bidi_initial_data_is_json(&ct_header(
            "application/x-lite-bidi; charset=utf-8"
        )));
    }

    #[test]
    fn bidi_is_json_application_json_is_true() {
        assert!(bidi_initial_data_is_json(&ct_header("application/json")));
    }

    #[test]
    fn bidi_is_json_suffix_json_is_true() {
        assert!(bidi_initial_data_is_json(&ct_header(
            "application/vnd.api+json"
        )));
    }

    #[test]
    fn bidi_is_json_image_png_is_false() {
        assert!(!bidi_initial_data_is_json(&ct_header("image/png")));
    }

    #[test]
    fn bidi_is_json_octet_stream_is_false() {
        assert!(!bidi_initial_data_is_json(&ct_header(
            "application/octet-stream"
        )));
    }

    #[test]
    fn bidi_is_json_malformed_ct_is_false() {
        assert!(!bidi_initial_data_is_json(&ct_header(
            "not-a-valid/content-type!!!"
        )));
    }

    /// FD-5 (audit 2026-08-06): with server.timeout disabled (<=0), the
    /// first-LPM-frame wait falls back to the always-on decoupled idle
    /// budget — a client that connects but never sends BidiOpen is reclaimed
    /// instead of pinning the handler forever. (Bounded-red: the outer
    /// timeout fails this test in 2s while the wait is unbounded.)
    #[tokio::test]
    async fn h2_bidi_first_frame_wait_backstopped_by_decoupled_idle() {
        let model = "bidi_fd5";
        let endpoint = ipc_endpoint(model);
        let mut cfg = crate::config::Config::default();
        cfg.server.timeout = 0.0; // deadline disabled
        cfg.server.decoupled_idle_timeout_secs = 0.05;
        let state = ready_state_with_config(model, endpoint, cfg).await;
        state.registry.activate_version(model, "1").unwrap();

        // Body stream that never yields and never EOFs (sender held).
        let (_body_tx, body_rx) =
            tokio::sync::mpsc::channel::<Result<Bytes, axum::Error>>(1);
        let body = Body::from_stream(ReceiverStream::new(body_rx));
        let req = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/v2/models/{}/bidi", model))
            .version(axum::http::Version::HTTP_2)
            .header("content-type", BIDI_CONTENT_TYPE)
            .body(body)
            .unwrap();

        let res = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            h2_bidi_handler(
                State(state),
                Path(model.to_string()),
                HeaderMap::new(),
                test_cx(),
                req,
            ),
        )
        .await;
        let err = res
            .expect("handler must return within the idle backstop")
            .expect_err("first-frame wait must be reclaimed");
        assert!(
            matches!(err.error, AppError::InferenceTimeout(_)),
            "expected InferenceTimeout, got {err:?}"
        );
    }

    // ===== S1/S2/S8 (批次 1):h2 bidi 请求级计数 + 取消计数 + per-worker dispatch =====

    /// S1:h2 bidi 正常完成(Done → Close 帧)→ REQUESTS_TOTAL{2xx} +1。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h2_bidi_done_records_requests_total_2xx() {
        let model = "bidi_req_done";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_chunk_done_recording_worker(endpoint.clone());
        let state = ready_state(model, endpoint).await;
        state.registry.activate_version(model, "1").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let counter = prometheus::REQUESTS_TOTAL.with_label_values(&[model, "1", "2xx"]);
        let before = counter.get();
        let (body_tx, resp) = start_bidi_session(model, state).await;
        tokio::spawn(async move {
            let _ = axum::body::to_bytes(resp.into_body(), 1 << 20).await;
            drop(body_tx);
        });
        wait_for(|| counter.get() >= before + 1.0, "bidi requests_total 2xx").await;
        assert_eq!(counter.get(), before + 1.0, "h2 bidi done must record one 2xx");
    }

    /// S1:h2 bidi Error 帧 → REQUESTS_TOTAL{5xx} +1。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h2_bidi_error_records_requests_total_5xx() {
        let model = "bidi_req_err";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_error_worker_bidi(endpoint.clone());
        let state = ready_state(model, endpoint).await;
        state.registry.activate_version(model, "1").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let counter = prometheus::REQUESTS_TOTAL.with_label_values(&[model, "1", "5xx"]);
        let before = counter.get();
        // S4/S5:Error 帧 → kind=worker_error,stream_kind=http2。before 须在
        // 流开始前取——errors 与 5xx 在同一次收口原子记录,晚取会漏计数。
        let errs = prometheus::STREAM_ERRORS_TOTAL
            .with_label_values(&[model, "1", "http2", "worker_error"]);
        let e_before = errs.get();
        let (body_tx, resp) = start_bidi_session(model, state).await;
        tokio::spawn(async move {
            let _ = axum::body::to_bytes(resp.into_body(), 1 << 20).await;
            drop(body_tx);
        });
        wait_for(|| counter.get() >= before + 1.0, "bidi requests_total 5xx").await;
        assert_eq!(counter.get(), before + 1.0, "h2 bidi error must record one 5xx");
        assert_eq!(errs.get(), e_before + 1.0, "S4: Error frame must count kind=worker_error");
    }

    /// D7:h2 bidi 早期拒绝(首帧非 BidiOpen)→ REQUESTS_TOTAL{4xx} +1。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h2_bidi_first_frame_not_open_records_4xx() {
        let model = "bidi_early_4xx";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_chunk_done_recording_worker(endpoint.clone());
        let state = ready_state(model, endpoint).await;
        state.registry.activate_version(model, "1").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let counter = prometheus::REQUESTS_TOTAL.with_label_values(&[model, "1", "4xx"]);
        let before = counter.get();
        // 首帧发 Data 而非 Open → InvalidRequestBody(400)。
        let (body_tx, body_rx) =
            tokio::sync::mpsc::channel::<Result<Bytes, axum::Error>>(8);
        let body = Body::from_stream(ReceiverStream::new(body_rx));
        let req = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/v2/models/{}/bidi", model))
            .version(axum::http::Version::HTTP_2)
            .header("content-type", BIDI_CONTENT_TYPE)
            .body(body)
            .unwrap();
        body_tx
            .send(Ok(lpm::encode_frame(&pb::BidiChunk {
                stream_id: String::new(),
                payload: Some(pb::bidi_chunk::Payload::Data(pb::BidiData {
                    data: Bytes::from_static(b"oops"),
                })),
            })))
            .await
            .expect("send non-Open first frame");
        let err = h2_bidi_handler(
            State(state),
            Path(model.to_string()),
            HeaderMap::new(),
            test_cx(),
            req,
        )
        .await
        .expect_err("first frame must be rejected");
        assert!(matches!(err.error, AppError::InvalidRequestBody(_)));
        assert_eq!(
            counter.get(),
            before + 1.0,
            "D7: early rejection must record 4xx"
        );
    }

    /// S2:客户端断开(响应 body 被 drop → tx.send Err)→ STREAM_CANCELLED_TOTAL +1。
    /// worker 须先发一个 chunk,forwarder 到 tx.send 时下游已 drop 才会 Err;
    /// 若 worker 不发任何帧,forwarder 阻塞在 recv_chunk(idle 300s),测试挂死。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h2_bidi_disconnect_records_cancelled() {
        let model = "bidi_req_cancel";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_stall_chunk_worker(endpoint.clone());
        let state = ready_state(model, endpoint).await;
        state.registry.activate_version(model, "1").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let canc = prometheus::STREAM_CANCELLED_TOTAL.with_label_values(&[model, "1", "http2"]);
        let before = canc.get();
        let (body_tx, resp) = start_bidi_session(model, state).await;
        // 立即丢弃响应 body 不 drain:outgoing 循环 tx.send Err → break(cancel)。
        drop(body_tx);
        drop(resp);
        wait_for(|| canc.get() >= before + 1.0, "bidi cancelled").await;
        assert_eq!(canc.get(), before + 1.0, "S2: h2 bidi disconnect must count");
    }

    /// S8:h2 bidi open 成功后 per-worker dispatch 计数 +1。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h2_bidi_open_records_worker_inference() {
        let model = "bidi_wi";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_recording_worker(endpoint.clone());
        let state = ready_state(model, endpoint).await;
        state.registry.activate_version(model, "1").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let before = prometheus::worker_inference_count(model, "1", 0);
        let (body_tx, resp) = start_bidi_session(model, state).await;
        tokio::spawn(async move {
            let _ = axum::body::to_bytes(resp.into_body(), 1 << 20).await;
            drop(body_tx);
        });
        wait_for(|| prometheus::worker_inference_count(model, "1", 0) > before, "bidi worker inference").await;
    }

    // ===== F-09 (functional-defects-plan) ==================================
    // A malformed LPM frame after Open must still end the worker's input.
    // The incoming task's `Err(_) => return` (bidi.rs:429) skips the D4
    // body-EOF Close (bidi.rs:435-439, reached by the `Some(Err(_)) | None
    // => break` arm), so the worker's generator never learns the input ended
    // and waits until its own timeout; the client gets no error frame either.
    // FAILS on current code: the worker must observe StreamRequest::Close
    // after the bad frame.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_f09_bad_frame_must_close_worker_input() {
        let model = "bidi_f09";
        let endpoint = ipc_endpoint(model);
        let (_w, actions) = spawn_recording_worker(endpoint.clone());
        let state = ready_state(model, endpoint).await;
        state.registry.activate_version(model, "1").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let (body_tx, resp) = start_bidi_session(model, state).await;
        tokio::spawn(async move {
            let _ = axum::body::to_bytes(resp.into_body(), 1 << 20).await;
        });

        assert_eq!(
            actions.recv_timeout(std::time::Duration::from_secs(3)).as_deref(),
            Ok("open"),
            "worker must see StreamOpen"
        );

        // A malformed LPM frame (non-zero flag byte → LpmError::BadFlag).
        body_tx
            .send(Ok(Bytes::from_static(&[0xff, 0x00, 0x00, 0x00, 0x00])))
            .await
            .expect("send bad frame");

        assert_eq!(
            actions.recv_timeout(std::time::Duration::from_secs(2)).as_deref(),
            Ok("close"),
            "F-09: a decode error in the incoming task must still deliver the \
             D4 half-close (StreamRequest::Close) so the worker's generator \
             sees the input end; the current `Err(_) => return` skips it and \
             the worker waits until its own timeout"
        );
    }
}
