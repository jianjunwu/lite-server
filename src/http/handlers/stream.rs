use super::*;
use super::inference::{build_request_meta, resolve_version};
use crate::error::{AppError, ProtocolError};
use crate::http::state::AppState;
use crate::metrics::prometheus;
use crate::proto::liteserver as pb;
use crate::request_context::RequestContext;
use crate::streaming;
use crate::transport::zmq::WorkerZmqClient;
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, Json, Response},
};
use axum::extract::ws::{Message, WebSocket};
use axum::response::sse::{Event, Sse};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::Instrument;
use uuid::Uuid;

async fn open_worker_stream(
    state: &Arc<AppState>,
    model_name: &str,
    resolved_version: &str,
    meta: pb::RequestMeta,
    payload_bytes: bytes::Bytes,
    decoupled: bool,
) -> Result<(String, Arc<WorkerZmqClient>, mpsc::Receiver<pb::StreamResponse>), AppError> {
    let mv = state
        .registry
        .get(model_name, Some(resolved_version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", model_name, resolved_version)))?;

    let num_workers = mv.workers.len();
    if num_workers == 0 {
        return Err(AppError::WorkerCrashed(format!("{} has no workers", model_name)));
    }

    let clients = state
        .worker_manager
        .get_zmq_clients(model_name, resolved_version)
        .await
        .ok_or_else(|| AppError::WorkerCrashed(format!("{} {} has no ZMQ clients", model_name, resolved_version)))?;

    // Skip ejected workers for streaming requests. P8-1: when the request
    // carries a sequence_id already mapped to a registered, non-ejected worker,
    // connect directly to it (sticky); otherwise the normal pick, then record.
    let outlier = state.worker_manager.get_outlier_state(model_name, resolved_version).await;
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

    // S8:per-worker dispatch 计数(gRPC 同位先例:pick 成功后立即记;pick/open
    // 失败不记)。
    prometheus::record_worker_inference(model_name, resolved_version, worker_id, 1);

    let client = &clients[worker_id];
    let stream_id = format!("stream-{}", Uuid::new_v4());
    let open_req = streaming::build_stream_open(stream_id.clone(), payload_bytes, Some(meta), decoupled);

    let chunk_rx = client.send_stream(open_req, stream_id.clone()).await?;
    Ok((stream_id, Arc::clone(client), chunk_rx))
}

// ===== SSE Streaming =====

pub async fn sse_infer_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiBody(body): ApiBody,
) -> Result<Response, ProtocolError> {
    // P-CORS: CORS headers are attached by `cors_middleware` (no longer per-handler).
    // D11 P2.1:错误按 T1 预筛协议渲染(SSE 客户端不发 IHCL → Legacy,byte-identical)。
    let protocol = cx.api_protocol.unwrap_or(crate::protocol::ApiProtocol::Legacy);
    sse_infer_entry(&state, &model_name, None, headers, body, cx, false)
        .await
        .map_err(|error| ProtocolError { error, protocol })
}

pub async fn sse_infer_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiBody(body): ApiBody,
) -> Result<Response, ProtocolError> {
    let protocol = cx.api_protocol.unwrap_or(crate::protocol::ApiProtocol::Legacy);
    sse_infer_entry(
        &state, &model_name, Some(version), headers, body, cx, false,
    )
    .await
    .map_err(|error| ProtocolError { error, protocol })
}

// ===== SSE Decoupled =====

pub async fn sse_decoupled_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiBody(body): ApiBody,
) -> Result<Response, ProtocolError> {
    let protocol = cx.api_protocol.unwrap_or(crate::protocol::ApiProtocol::Legacy);
    sse_infer_entry(&state, &model_name, None, headers, body, cx, true)
        .await
        .map_err(|error| ProtocolError { error, protocol })
}

pub async fn sse_decoupled_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiBody(body): ApiBody,
) -> Result<Response, ProtocolError> {
    let protocol = cx.api_protocol.unwrap_or(crate::protocol::ApiProtocol::Legacy);
    sse_infer_entry(&state, &model_name, Some(version), headers, body, cx, true)
        .await
        .map_err(|error| ProtocolError { error, protocol })
}

/// SSE 帧风格(批次 4,D9):Legacy = /events 自有格式(`data: <chunk>` +
/// `data: [DONE]`);Generate = Triton Generate extension(`data: <完整 JSON>`
/// 逐 chunk,错误携带在事件内,结束即连接关闭——无 [DONE] 标记)。
#[derive(Clone, Copy, PartialEq, Eq)]
enum SseFrameStyle {
    Legacy,
    Generate,
}

/// generate_stream 入口(批次 4,D9):与 [`sse_infer_entry`] 同包裹(早期拒绝
/// 计数),帧封装 = Generate 风格。非流式 worker 行为与 /events 完全一致
/// (J4:同路径同管线,唯一差异是帧封装)。
async fn generate_stream_entry(
    state: &Arc<AppState>,
    model_name: &str,
    version: Option<String>,
    headers: HeaderMap,
    body: RequestBody,
    cx: RequestContext,
) -> Result<Response, AppError> {
    let start = std::time::Instant::now();
    let mut label_version = version.clone().unwrap_or_default();
    let result = sse_infer_entry_impl(
        state,
        model_name,
        version,
        headers,
        body,
        cx,
        false,
        &mut label_version,
        SseFrameStyle::Generate,
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

/// D9:/generate_stream(SSE,每 `data:` 一个 JSON 响应)。随 `streaming+sse`
/// 开关族挂载(J3,与 /events 同批)。
pub async fn generate_stream_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiBody(body): ApiBody,
) -> Result<Response, ProtocolError> {
    let protocol = cx.api_protocol.unwrap_or(crate::protocol::ApiProtocol::Legacy);
    generate_stream_entry(&state, &model_name, None, headers, body, cx)
        .await
        .map_err(|error| ProtocolError { error, protocol })
}

pub async fn generate_stream_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiBody(body): ApiBody,
) -> Result<Response, ProtocolError> {
    let protocol = cx.api_protocol.unwrap_or(crate::protocol::ApiProtocol::Legacy);
    generate_stream_entry(&state, &model_name, Some(version), headers, body, cx)
        .await
        .map_err(|error| ProtocolError { error, protocol })
}

/// Shared entry for SSE inference: validation, ready check, rate limiting,
/// and stream setup. Returns a `Response` so the caller can uniformly wrap
/// CORS around both the success stream-start and any early error.
async fn sse_infer_entry(
    state: &Arc<AppState>,
    model_name: &str,
    version: Option<String>,
    headers: HeaderMap,
    body: RequestBody,
    cx: RequestContext,
    decoupled: bool,
) -> Result<Response, AppError> {
    // S1(b)/D7:open 前的早期拒绝也计一次请求(对齐 gRPC wrapper 双点语义)。
    // label:resolve 成功后用 resolved_version,失败用请求原值(可为空串)。
    let start = std::time::Instant::now();
    let mut label_version = version.clone().unwrap_or_default();
    let result = sse_infer_entry_impl(
        state,
        model_name,
        version,
        headers,
        body,
        cx,
        decoupled,
        &mut label_version,
        SseFrameStyle::Legacy,
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

#[allow(clippy::too_many_arguments)] // 内部入口:状态/请求/上下文组合参数,struct 化收益低(同 watcher.rs 先例)
async fn sse_infer_entry_impl(
    state: &Arc<AppState>,
    model_name: &str,
    version: Option<String>,
    headers: HeaderMap,
    body: RequestBody,
    cx: RequestContext,
    decoupled: bool,
    label_version: &mut String,
    frame: SseFrameStyle,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(model_name)?;
    if let Some(ref v) = version {
        crate::validation::validate_version(v)?;
    }
    // 阶段 2(D10):SSE 是文本通道,不能携带二进制输出——KServe 信封请求
    // 带 binary_data_output flag → 400(双条件,防自有格式撞名);二进制
    // 流式维持 WS/h2 bidi 自有协议。
    if crate::http::kserve::request_binary_output_flag(&body) {
        return Err(AppError::Validation(
            "SSE streaming cannot return binary_data_output; binary streaming uses WS/h2 bidi"
                .to_string(),
        ));
    }
    let resolved_version = resolve_version(state, model_name, version, &headers).await?;
    *label_version = resolved_version.clone();
    if !state.registry.is_ready(model_name, Some(&resolved_version)) {
        return Err(AppError::ModelNotReady(format!(
            "{} version {} is not ready",
            model_name, resolved_version
        )));
    }
    // Auth + rate limit (rate limit shares the /predict bucket with unary infer).
    if let Some(mv) = state.registry.get(model_name, Some(&resolved_version)) {
        enforce_auth(mv.policies.auth.as_ref(), &headers)?;
        enforce_rate_limit(state, mv.policies.rate_limit.as_ref(), model_name, &cx.client_ip).await?;
    }
    let sse = sse_infer_impl(
        state.clone(),
        model_name.to_string(),
        resolved_version,
        headers,
        body,
        cx,
        decoupled,
        frame,
    )
    .await?;
    Ok(sse.into_response())
}

#[allow(clippy::too_many_arguments)] // 内部管线:状态/请求/上下文组合参数(同 sse_infer_entry_impl 先例)
async fn sse_infer_impl(
    state: Arc<AppState>,
    model_name: String,
    resolved_version: String,
    headers: HeaderMap,
    body: RequestBody,
    cx: RequestContext,
    decoupled: bool,
    frame: SseFrameStyle,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, AppError> {

    // Task B: inference span (parity with gRPC + unary HTTP). Created before
    // build_request_meta and entered around it via in_scope so telemetry::inject
    // (Context::current()) picks up THIS span, not the ambient http.server span
    // — the worker then becomes a child of the inference span. pinned_version is
    // left Empty (resolve_version already ran in sse_infer_entry; SSE has no
    // canary path).
    let span = tracing::info_span!(
        "inference",
        model = %model_name,
        version = %resolved_version,
        request_id = %cx.request_id,
        pinned_version = tracing::field::Empty,
        body_bytes = tracing::field::Empty,
        body_kind = tracing::field::Empty,
    );
    crate::telemetry::link_parent(&span, &cx.trace_cx);

    let deadline = crate::deadline::resolve_from_http(&headers, state.config.server.timeout);
    let payload_bytes = body.bytes();
    // D11: record body size with content-type label.
    prometheus::record_request_body_bytes(body.kind(), "/predict", payload_bytes.len());
    // Record onto the inference span explicitly — `Span::current()` here is
    // the ambient (http.server) span since this span is only entered via
    // in_scope below; recording on current() silently dropped the fields
    // (0.8.3 audit B4).
    span.record("body_bytes", payload_bytes.len() as i64);
    span.record("body_kind", body.kind());
    let meta = span.in_scope(|| build_request_meta(&headers, payload_bytes.clone(), "/predict", &cx, deadline.unix_ns));
    // P-DEADLINE (方案 C): overall deadline only when the CLIENT specified one;
    // chunk-idle reclaim is ALWAYS on (decoupled parity) so a stuck stream is
    // recovered instead of hanging unbounded. Long streams keep flowing untouched.
    let stream_deadline = if deadline.client_specified {
        crate::deadline::to_instant(deadline.unix_ns)
    } else {
        None
    };
    let stream_idle = crate::deadline::idle_budget(state.config.server.decoupled_idle_timeout_secs);
    let (stream_id, worker_client, mut chunk_rx) = open_worker_stream(&state, &model_name, &resolved_version, meta, payload_bytes, decoupled).await?;

    // Task D: fire InferenceRequest once the worker stream opened and arm the
    // response callback. cx is not captured by the spawn, so request_id /
    // client_ip are cloned here and moved in. open_time (inside the spawn) is
    // the elapsed reference for the response.
    let cb_runner = state.callback_runner.clone();
    let req_ctx = crate::callback::InferenceContext {
        model_name: model_name.clone(),
        version: resolved_version.clone(),
        route: "/predict".to_string(),
        protocol: crate::callback::Protocol::Sse,
        request_id: cx.request_id.clone(),
        client_ip: cx.client_ip.clone(),
        elapsed_us: None,
    };
    crate::callback::fire_inference_request(&cb_runner, &req_ctx);

    let stream_metrics = state.config.features.streaming_metrics;
    if stream_metrics {
        prometheus::record_stream_open(&model_name, &resolved_version, "sse", &stream_id, decoupled);
    }

    let (event_tx, event_rx) = mpsc::channel(64);

    // D4: decoupled path keeps the client for targeted cancel (vs coupled
    // broadcast). Clone before the spawn so the Arc stays alive.
    let cancel_client = if decoupled {
        Some(Arc::clone(&worker_client))
    } else {
        None
    };

    tokio::spawn(async move {
        let open_time = std::time::Instant::now();
        let mut first_chunk = true;
        let mut last_chunk_time = open_time;
        // S1/S2:收口枚举——各 break 点只置 reason,尾部 record_stream_terminal
        // 统一消费(family/cancelled 单一来源,exactly-once 由单一收口保证)。
        let reason;
        // S6:per-stream 输出字节(Σ chunk.data.len(),收口统一上报)。
        let mut output_bytes: u64 = 0;
        // G5:per-stream chunk 数(close 日志字段,收口统一上报,非 metric)。
        let mut chunks: u64 = 0;

        loop {
            let chunk = match streaming::recv_chunk(&mut chunk_rx, stream_deadline, stream_idle)
                .await
            {
                Ok(Some(c)) => c,
                Ok(None) => {
                    reason = prometheus::StreamCloseReason::WorkerEof;
                    break; // worker closed the stream
                }
                Err(elapsed) => {
                    // P-DEADLINE (§4.0.4): overall deadline or chunk-idle fired.
                    tracing::warn!(
                        ?elapsed, stream_id = %stream_id,
                        "sse stream closed: deadline/idle elapsed"
                    );
                    reason = match elapsed {
                        crate::streaming::RecvElapsed::Deadline => {
                            prometheus::StreamCloseReason::Deadline
                        }
                        crate::streaming::RecvElapsed::Idle => {
                            prometheus::StreamCloseReason::Idle
                        }
                    };
                    break;
                }
            };
            let event = match &chunk.payload {
                Some(pb::stream_response::Payload::Chunk(c)) => {
                    output_bytes += c.data.len() as u64;
                    chunks += 1;
                    if stream_metrics {
                        if first_chunk {
                            prometheus::record_stream_ttft(&model_name, &resolved_version, "sse", open_time.elapsed().as_secs_f64());
                            first_chunk = false;
                        } else {
                            prometheus::record_stream_tbt(&model_name, &resolved_version, "sse", last_chunk_time.elapsed().as_secs_f64());
                        }
                        last_chunk_time = std::time::Instant::now();
                        prometheus::record_stream_chunk(&model_name, &resolved_version, "sse");
                    }
                    let data = String::from_utf8_lossy(&c.data);
                    Some(Event::default().data(&data))
                }
                Some(pb::stream_response::Payload::Error(e)) => {
                    // Try to parse as structured error from HTTPException
                    let event_data = match serde_json::from_str::<serde_json::Value>(&e.message) {
                        Ok(val) if val.get("error").and_then(|err| err.get("type")).is_some() => {
                            json!({"error": val["error"]}).to_string()
                        }
                        _ => json!({"error": e.message}).to_string(),
                    };
                    // Task D: terminal Error frame → InferenceResponse.
                    crate::callback::fire_inference_response(&cb_runner, &req_ctx, open_time);
                    Some(Event::default().data(event_data))
                }
                Some(pb::stream_response::Payload::Done(done)) => {
                    prometheus::record_worker_metrics(&model_name, &resolved_version, done.metrics.as_ref());
                    // Task D: terminal Done frame → InferenceResponse.
                    crate::callback::fire_inference_response(&cb_runner, &req_ctx, open_time);
                    // 帧封装差异(批次 4):Legacy 发 `data: [DONE]`;Generate
                    // 风格结束即连接关闭,无终止标记(D9/Triton 行为)。
                    match frame {
                        SseFrameStyle::Legacy => Some(Event::default().data("[DONE]")),
                        SseFrameStyle::Generate => None,
                    }
                }
                _ => None,
            };
            if let Some(event) = event {
                if event_tx.send(Ok(event)).await.is_err() {
                    reason = prometheus::StreamCloseReason::Cancel;
                    break;
                }
            }
            // Terminal frames end the stream. Error is terminal by contract
            // (callback.rs) — breaking here also guarantees the
            // InferenceResponse callback fires exactly once even if a worker
            // sends Error followed by Done (the chunk path normally terminates
            // on Error, but the forwarder must not rely on that side effect).
            // reason 在此统一置位——is_stream_terminal 对编译器是不透明谓词,
            // 分支内赋值无法被流分析跨 if 识别(无初值声明的构造保证)。
            if is_stream_terminal(&chunk) {
                reason = match chunk.payload {
                    Some(pb::stream_response::Payload::Done(_)) => {
                        prometheus::StreamCloseReason::Done
                    }
                    _ => prometheus::StreamCloseReason::Error,
                };
                break;
            }
        }
        // S1/S2/S4/S6 收口:无条件 record_request_end + 门控内 cancelled/errors/duration/bytes/close。
        prometheus::record_stream_terminal(
            &model_name,
            &resolved_version,
            "sse",
            "sse",
            open_time,
            reason.status_family(),
            reason,
            stream_metrics,
            output_bytes,
            chunks,
        );
        // Ensure stream is cleaned up on worker side.
        // D4: decoupled → targeted cancel (parity with WS/gRPC);
        // coupled → broadcast (existing behavior, unchanged).
        let cancel_req = streaming::build_stream_cancel(stream_id);
        if let Some(client) = cancel_client {
            let _ = client.send_raw(cancel_req).await;
        } else {
            open_worker_stream_cancel(&state, &model_name, &resolved_version, cancel_req).await;
        }
    }
    .instrument(span.clone()));

    Ok(Sse::new(ReceiverStream::new(event_rx)))
}

/// Send a stream cancel request to all workers for a model version.
/// Best-effort: workers that don't own the stream will ignore it.
async fn open_worker_stream_cancel(
    state: &Arc<AppState>,
    model_name: &str,
    version: &str,
    cancel_req: pb::Request,
) {
    let clients = state
        .worker_manager
        .get_zmq_clients(model_name, version)
        .await;
    match clients {
        Some(list) => {
            for client in &list {
                // Fire-and-forget: a stream Cancel carries no unary reply — the
                // worker just signals the generator to stop — so send_raw must
                // be used. client.send would register a pending reply slot and
                // block up to ZMQ_RESPONSE_TIMEOUT (300s) for a reply that
                // never comes. Because this runs inline after [DONE] while the
                // SSE/WS `event_tx`/socket is still held, that 300s wait would
                // keep the response stream open and hang any client draining it.
                let _ = client.send_raw(cancel_req.clone()).await;
            }
        }
        None => {
            // Worker may already be unloaded; nothing to cancel
        }
    }
}

// ===== WebSocket Streaming =====

pub async fn ws_stream_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
    ws: axum::extract::WebSocketUpgrade,
    cx: RequestContext,
) -> Response {
    if let Err(e) = crate::validation::validate_identifier(&model_name) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    // P-CORS (评审 1.3): browsers send no preflight for WS, so the CORS
    // middleware can't stop cross-site WS hijacking — check Origin at upgrade.
    if !crate::http::cors::ws_origin_allowed(&state, &model_name, None, &headers) {
        return (axum::http::StatusCode::FORBIDDEN, "WebSocket Origin not allowed").into_response();
    }
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, None, headers, socket, cx, false))
}

pub async fn ws_stream_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    ws: axum::extract::WebSocketUpgrade,
    cx: RequestContext,
) -> Response {
    if let Err(e) = crate::validation::validate_identifier(&model_name) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    if let Err(e) = crate::validation::validate_version(&version) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    if !crate::http::cors::ws_origin_allowed(&state, &model_name, Some(&version), &headers) {
        return (axum::http::StatusCode::FORBIDDEN, "WebSocket Origin not allowed").into_response();
    }
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, Some(version), headers, socket, cx, false))
}

// ===== WS Decoupled =====

pub async fn ws_decoupled_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
    ws: axum::extract::WebSocketUpgrade,
    cx: RequestContext,
) -> Response {
    if let Err(e) = crate::validation::validate_identifier(&model_name) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    if !crate::http::cors::ws_origin_allowed(&state, &model_name, None, &headers) {
        return (axum::http::StatusCode::FORBIDDEN, "WebSocket Origin not allowed").into_response();
    }
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, None, headers, socket, cx, true))
}

pub async fn ws_decoupled_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    ws: axum::extract::WebSocketUpgrade,
    cx: RequestContext,
) -> Response {
    if let Err(e) = crate::validation::validate_identifier(&model_name) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    if let Err(e) = crate::validation::validate_version(&version) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    if !crate::http::cors::ws_origin_allowed(&state, &model_name, Some(&version), &headers) {
        return (axum::http::StatusCode::FORBIDDEN, "WebSocket Origin not allowed").into_response();
    }
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, Some(version), headers, socket, cx, true))
}

/// Detect a WS bidi app-level close control frame: `{"type":"close"}`.
fn is_close_frame(text: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|v| {
            v.get("type")
                .and_then(|t| t.as_str())
                .map(|s| s == "close")
        })
        .unwrap_or(false)
}

/// Whether a worker stream response chunk terminates the forwarder loop.
///
/// Both `Done` and `Error` are terminal by contract (`callback.rs` documents
/// `StreamError` as a terminal frame). Single source for the loop break
/// condition, shared between SSE + WS + test assertions so they can't drift.
fn is_stream_terminal(chunk: &pb::StreamResponse) -> bool {
    matches!(
        chunk.payload,
        Some(pb::stream_response::Payload::Done(_) | pb::stream_response::Payload::Error(_))
    )
}

/// Detect a WS decoupled cancel-or-close control frame (D1):
/// `{"type":"cancel"}` or `{"type":"close"}` — aliases in decoupled mode.
fn is_cancel_or_close_frame(text: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|v| {
            v.get("type")
                .and_then(|t| t.as_str())
                .map(|s| s == "cancel" || s == "close")
        })
        .unwrap_or(false)
}

/// B2 (E3): the first WS frame carries the request payload; frame type is
/// the sole dispatch signal. Text → JSON (RawValue-validated legacy path);
/// Binary → opaque bytes (no validation). The browser WebSocket API cannot
/// set custom headers on the upgrade request, so frame type is the only
/// client-controlled signal.
enum FirstFrame {
    Json(String),
    Raw(bytes::Bytes),
}

impl FirstFrame {
    /// D11 body_kind label derived from frame type (E4: Text → json,
    /// Binary → raw), aligned with the unary/SSE D11 semantics.
    fn body_kind(&self) -> &'static str {
        match self {
            FirstFrame::Json(_) => "json",
            FirstFrame::Raw(_) => "raw",
        }
    }
}

/// B2 (E4): normalize the content-type written into meta.headers for a
/// Binary first frame, so the Python side's D9 dispatch receives a
/// single-source-of-truth header:
///   missing CT  → inject application/octet-stream
///   non-JSON CT → keep as-is (payload metadata, usable by the model)
///   JSON CT     → rewrite to application/octet-stream + warn
///     (contradictory signal: frame type wins over header)
fn normalize_binary_first_frame_ct(headers: &mut HeaderMap, model_name: &str) {
    let existing_ct = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    match existing_ct {
        None => {
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/octet-stream"),
            );
        }
        Some(ref ct) if is_json_content_type(
            &axum::http::HeaderValue::from_str(ct).unwrap_or_else(|_| {
                axum::http::HeaderValue::from_static("application/octet-stream")
            }),
        ) => {
            tracing::warn!(
                model = %model_name,
                content_type = %ct,
                "WS Binary first frame with JSON Content-Type; \
                 frame type wins — rewriting to application/octet-stream"
            );
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/octet-stream"),
            );
        }
        Some(_) => {
            // Non-JSON CT preserved as-is (payload metadata).
        }
    }
}

async fn handle_ws_stream(
    state: Arc<AppState>,
    model_name: String,
    version: Option<String>,
    mut headers: HeaderMap,
    mut socket: WebSocket,
    cx: RequestContext,
    decoupled: bool,
) {
    // S1(b)/D7:握手后早退也计一次请求(WS 已升级 101 无 HTTP status,family
    // 按原因归类);ws_start 同时供 writer panic 臂的收口时长使用。
    let ws_start = std::time::Instant::now();
    // resolve_version 按值消费 version——先提取请求原值 label 供失败分支使用。
    let request_version_label = version.as_deref().unwrap_or("").to_string();
    let resolved_version = match resolve_version(&state, &model_name, version, &headers).await {
        Ok(v) => v,
        Err(_) => {
            // model 解析失败 → 4xx;version 未解析用请求原值(可为空串)。
            prometheus::record_stream_rejected(
                &model_name,
                &request_version_label,
                "4xx",
                ws_start.elapsed().as_secs_f64(),
            );
            let _ = socket.close().await;
            return;
        }
    };

    if !state.registry.is_ready(&model_name, Some(&resolved_version)) {
        // 未就绪 → 5xx。
        prometheus::record_stream_rejected(
            &model_name,
            &resolved_version,
            "5xx",
            ws_start.elapsed().as_secs_f64(),
        );
        let _ = socket.close().await;
        return;
    }

    // Rate limit (same logic as HTTP infer). client_ip comes from the
    // RequestContext filled once by context_middleware (including the
    // direct-connection peer fallback), so key="ip" limits per real client.
    if let Some(mv) = state.registry.get(&model_name, Some(&resolved_version)) {
        // Auth is checked before rate limiting (short-circuit), and each
        // failure reports its OWN reason — the handshake already upgraded to
        // 101 so there's no HTTP status to set; send an accurate error frame
        // then close.
        let auth_failed = enforce_auth(mv.policies.auth.as_ref(), &headers).is_err();
        let rl_failed = !auth_failed
            && enforce_rate_limit(
                &state,
                mv.policies.rate_limit.as_ref(),
                &model_name,
                &cx.client_ip,
            )
            .await
            .is_err();
        if auth_failed || rl_failed {
            // 鉴权/限流 → 4xx。
            prometheus::record_stream_rejected(
                &model_name,
                &resolved_version,
                "4xx",
                ws_start.elapsed().as_secs_f64(),
            );
            let reason = if auth_failed {
                "unauthorized"
            } else {
                "rate limit exceeded"
            };
            let _ = socket
                .send(Message::Text(json!({ "error": reason }).to_string()))
                .await;
            let _ = socket.close().await;
            return;
        }
    }

    // Wait for first message from client (the request payload). Bound the wait
    // by server.timeout when one is configured; with server.timeout <= 0 the
    // wait is unbounded (a client that upgrades but never sends keeps the
    // handler open indefinitely).
    //
    // B2 (E3): Text → JSON (RawValue validation, legacy path); Binary →
    // opaque bytes (skip validation). See FirstFrame / E4 normalization.
    let first_frame = {
        let recv = match crate::deadline::idle_budget(state.config.server.timeout) {
            Some(budget) => match tokio::time::timeout(budget, socket.recv()).await {
                Ok(r) => r,
                Err(_) => {
                    // 首帧超时 → 4xx。
                    prometheus::record_stream_rejected(
                        &model_name,
                        &resolved_version,
                        "4xx",
                        ws_start.elapsed().as_secs_f64(),
                    );
                    let _ = socket.close().await;
                    return;
                }
            },
            None => socket.recv().await,
        };
        match recv {
            Some(Ok(Message::Text(text))) => FirstFrame::Json(text),
            Some(Ok(Message::Binary(bin))) => FirstFrame::Raw(bytes::Bytes::from(bin)),
            _ => {
                // 首帧非法(Close/Err 帧)→ 4xx。
                prometheus::record_stream_rejected(
                    &model_name,
                    &resolved_version,
                    "4xx",
                    ws_start.elapsed().as_secs_f64(),
                );
                let _ = socket.close().await;
                return;
            }
        }
    };

    // D3 / B2: JSON validation only applies to Text first frames.
    // Binary first frames skip validation entirely (E3, frame type wins).
    match &first_frame {
        FirstFrame::Json(text) => {
            if serde_json::from_slice::<&serde_json::value::RawValue>(text.as_bytes()).is_err() {
                // 非法 JSON → 4xx。
                prometheus::record_stream_rejected(
                    &model_name,
                    &resolved_version,
                    "4xx",
                    ws_start.elapsed().as_secs_f64(),
                );
                let _ = socket.send(Message::Text(json!({"error": "invalid JSON"}).to_string())).await;
                let _ = socket.close().await;
                return;
            }
        }
        FirstFrame::Raw(_) => {
            // No validation for Binary first frame (E3).
        }
    }

    // The original upgrade-request headers flow into meta; client_ip /
    // request_id come from the RequestContext (P-MW single fill).
    // Task B: inference span (parity with gRPC + SSE). Entered around
    // build_request_meta via in_scope so telemetry::inject picks up THIS span.
    let span = tracing::info_span!(
        "inference",
        model = %model_name,
        version = %resolved_version,
        request_id = %cx.request_id,
        pinned_version = tracing::field::Empty,
        body_bytes = tracing::field::Empty,
        body_kind = tracing::field::Empty,
    );
    crate::telemetry::link_parent(&span, &cx.trace_cx);
    let deadline = crate::deadline::resolve_from_http(&headers, state.config.server.timeout);
    // B2 (E4): payload_bytes from frame type; body_kind from frame type
    // (Text → json, Binary → raw). CT normalization for Binary frames writes
    // the effective content-type into meta.headers so the Python side's D9
    // dispatch receives a single-source-of-truth header. Matched by value so
    // the legacy Text path stays zero-copy (String::into_bytes).
    let body_kind = first_frame.body_kind();
    let payload_bytes = match first_frame {
        FirstFrame::Json(text) => {
            // Text first frame → JSON (legacy path, no CT normalization).
            bytes::Bytes::from(text.into_bytes())
        }
        FirstFrame::Raw(bin) => {
            normalize_binary_first_frame_ct(&mut headers, &model_name);
            bin
        }
    };
    span.record("body_bytes", payload_bytes.len() as i64);
    span.record("body_kind", body_kind);
    let meta = span.in_scope(|| build_request_meta(&headers, payload_bytes.clone(), "/predict", &cx, deadline.unix_ns));
    // P-DEADLINE (方案 C): overall deadline only when the CLIENT specified one;
    // chunk-idle reclaim is ALWAYS on (decoupled parity) so a stuck stream is
    // recovered instead of hanging unbounded. Long streams keep flowing untouched.
    let stream_deadline = if deadline.client_specified {
        crate::deadline::to_instant(deadline.unix_ns)
    } else {
        None
    };
    let stream_idle = crate::deadline::idle_budget(state.config.server.decoupled_idle_timeout_secs);

    let (stream_id, worker_client, mut chunk_rx) = match open_worker_stream(&state, &model_name, &resolved_version, meta, payload_bytes, decoupled).await {
        Ok(r) => r,
        Err(e) => {
            // open 失败 → 5xx。
            prometheus::record_stream_rejected(
                &model_name,
                &resolved_version,
                "5xx",
                ws_start.elapsed().as_secs_f64(),
            );
            let _ = socket.send(Message::Text(json!({"error": e.to_string()}).to_string())).await;
            let _ = socket.close().await;
            return;
        }
    };

    // Task D: fire InferenceRequest once the worker stream opened and arm the
    // response callback. cx is not captured by the spawn, so request_id /
    // client_ip are cloned here and moved in. open_time (inside the spawn) is
    // the elapsed reference for the response.
    let cb_runner = state.callback_runner.clone();
    let req_ctx = crate::callback::InferenceContext {
        model_name: model_name.clone(),
        version: resolved_version.clone(),
        route: "/predict".to_string(),
        protocol: crate::callback::Protocol::WebSocket,
        request_id: cx.request_id.clone(),
        client_ip: cx.client_ip.clone(),
        elapsed_us: None,
    };
    crate::callback::fire_inference_request(&cb_runner, &req_ctx);

    let stream_metrics = state.config.features.streaming_metrics;
    if stream_metrics {
        prometheus::record_stream_open(&model_name, &resolved_version, "websocket", &stream_id, decoupled);
    }

    // Split socket for independent read/write halves (bidi).
    let (mut ws_sink, mut ws_stream) = socket.split();

    // gone_tx: carries an optional error message from the reader to the writer.
    // None = no signal (writer keeps running); Some("") = client disconnect
    // (writer breaks); Some("msg") = protocol error (writer sends msg then breaks).
    // The main task holds the primary sender so the watch channel stays alive
    // after the reader exits (preventing spurious wake-ups on drop).
    let (gone_tx, mut gone_rx) = tokio::sync::watch::channel::<Option<String>>(None);
    let gone_tx_reader = gone_tx.clone();

    // Reader task: C→S frames. Forwards binary chunks and close to worker.
    // Exits silently on app-level close (writer keeps going); signals gone on
    // hard disconnect so the writer can terminate promptly.
    let reader_stream_id = stream_id.clone();
    let reader_client = Arc::clone(&worker_client);
    let reader = tokio::spawn(async move {
        while let Some(msg) = ws_stream.next().await {
            match msg {
                // D1: decoupled stream accepts no data frames after the first.
                Ok(Message::Binary(_bin)) if decoupled => {
                    let _ = gone_tx_reader.send(Some(
                        "decoupled stream accepts no data frames".to_string(),
                    ));
                    break;
                }
                // Coupled: forward Binary as stream chunk (existing behavior).
                Ok(Message::Binary(bin)) => {
                    let chunk_req = streaming::build_stream_chunk(
                        reader_stream_id.clone(),
                        bytes::Bytes::from(bin),
                    );
                    let _ = reader_client.send_raw(chunk_req).await;
                }
                // D1: decoupled cancel/close are aliases → cancel worker, signal gone.
                Ok(Message::Text(t)) if decoupled && is_cancel_or_close_frame(&t) => {
                    let cancel_req =
                        streaming::build_stream_cancel(reader_stream_id.clone());
                    let _ = reader_client.send_raw(cancel_req).await;
                    let _ = gone_tx_reader.send(Some(String::new()));
                    break;
                }
                // Coupled: app-level close → send close to worker (existing).
                Ok(Message::Text(t)) if !decoupled && is_close_frame(&t) => {
                    let close_req =
                        streaming::build_stream_close(reader_stream_id.clone());
                    let _ = reader_client.send_raw(close_req).await;
                    return; // graceful: don't signal gone, writer continues
                }
                Ok(Message::Close(_)) | Err(_) => {
                    // Client disconnect → signal writer to terminate.
                    let _ = gone_tx_reader.send(Some(String::new()));
                    break;
                }
                Ok(Message::Text(_)) => {
                    // Unknown control frame → protocol error.
                    let _ = gone_tx_reader.send(Some("unknown control frame".to_string()));
                    break;
                }
                _ => {} // Ping/Pong ignored
            }
        }
    });

    // Clone before move so cancel can reference them after the tasks complete.
    let stream_id_for_writer = stream_id.clone();
    let cancel_client = Arc::clone(&worker_client);

    // S1(b)/S2:ws_start 同时供 writer 收口与 panic 臂使用(Ok/Err 互斥,
    // exactly-once 由两条路径各收口一次保证)。writer 闭包(move)会带走
    // model_name/resolved_version 的所有权——panic 臂需自己的副本。
    let stream_open_time = std::time::Instant::now();
    let panic_metrics_model = model_name.clone();
    let panic_metrics_version = resolved_version.clone();

    // Writer task: S→C. Forwards worker chunks to the WebSocket, with early
    // termination via gone_rx when the client disconnects. Closes ws_sink on
    // exit so the main task doesn't need to hold a reference.
    let send_task = tokio::spawn(async move {
        let open_time = stream_open_time;
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
            let chunk = tokio::select! {
                c = streaming::recv_chunk(&mut chunk_rx, stream_deadline, stream_idle) => c,
                _ = gone_rx.changed() => {
                    // Reader signalled: send error (if any) to client, then break.
                    // gone=Some(msg) → protocol 违规;gone=Some("")/None → 客户端
                    // 断开(cancel)。
                    let err_msg = match &*gone_rx.borrow() {
                        Some(msg) if !msg.is_empty() => Some(msg.clone()),
                        _ => None,
                    };
                    reason = if err_msg.is_some() {
                        prometheus::StreamCloseReason::Protocol
                    } else {
                        prometheus::StreamCloseReason::Cancel
                    };
                    drop(gone_rx.borrow());
                    if let Some(msg) = err_msg {
                        let _ = ws_sink.send(Message::Text(
                            json!({"error": msg}).to_string(),
                        )).await;
                    }
                    break;
                }
            };
            let chunk = match chunk {
                Ok(Some(c)) => c,
                Ok(None) => {
                    reason = prometheus::StreamCloseReason::WorkerEof;
                    break; // worker closed the stream
                }
                Err(elapsed) => {
                    // P-DEADLINE (§4.0.4): overall deadline or chunk-idle fired.
                    tracing::warn!(
                        ?elapsed, stream_id = %stream_id_for_writer,
                        "websocket stream closed: deadline/idle elapsed"
                    );
                    reason = match elapsed {
                        crate::streaming::RecvElapsed::Deadline => {
                            prometheus::StreamCloseReason::Deadline
                        }
                        crate::streaming::RecvElapsed::Idle => {
                            prometheus::StreamCloseReason::Idle
                        }
                    };
                    break;
                }
            };
            let msg = match &chunk.payload {
                Some(pb::stream_response::Payload::Chunk(c)) => {
                    output_bytes += c.data.len() as u64;
                    chunks += 1;
                    if stream_metrics {
                        if first_chunk {
                            prometheus::record_stream_ttft(&model_name, &resolved_version, "websocket", open_time.elapsed().as_secs_f64());
                            first_chunk = false;
                        } else {
                            prometheus::record_stream_tbt(&model_name, &resolved_version, "websocket", last_chunk_time.elapsed().as_secs_f64());
                        }
                        last_chunk_time = std::time::Instant::now();
                        prometheus::record_stream_chunk(&model_name, &resolved_version, "websocket");
                    }
                    Message::Binary(c.data.to_vec())
                }
                Some(pb::stream_response::Payload::Error(e)) => {
                    // Try to parse as structured error from HTTPException
                    let event_data = match serde_json::from_str::<serde_json::Value>(&e.message) {
                        Ok(val) if val.get("error").and_then(|err| err.get("type")).is_some() => {
                            json!({"error": val["error"]}).to_string()
                        }
                        _ => json!({"error": e.message}).to_string(),
                    };
                    // Task D: terminal Error frame → InferenceResponse.
                    crate::callback::fire_inference_response(&cb_runner, &req_ctx, open_time);
                    Message::Text(event_data)
                }
                Some(pb::stream_response::Payload::Done(done)) => {
                    prometheus::record_worker_metrics(&model_name, &resolved_version, done.metrics.as_ref());
                    // Task D: terminal Done frame → InferenceResponse.
                    crate::callback::fire_inference_response(&cb_runner, &req_ctx, open_time);
                    Message::Text(json!({"done": true}).to_string())
                }
                _ => continue,
            };
            if ws_sink.send(msg).await.is_err() {
                reason = prometheus::StreamCloseReason::Cancel;
                break;
            }
            // reason 统一在 terminal break 处置位(同 SSE 注释,无初值声明的构造保证)。
            if is_stream_terminal(&chunk) {
                reason = match chunk.payload {
                    Some(pb::stream_response::Payload::Done(_)) => {
                        prometheus::StreamCloseReason::Done
                    }
                    _ => prometheus::StreamCloseReason::Error,
                };
                break;
            }
        }
        // S1/S2/S4/S6 收口:无条件 record_request_end + 门控内 cancelled/errors/duration/bytes/close。
        prometheus::record_stream_terminal(
            &model_name,
            &resolved_version,
            "websocket",
            "ws",
            open_time,
            reason.status_family(),
            reason,
            stream_metrics,
            output_bytes,
            chunks,
        );
        // Close the sink gracefully (moved here so main doesn't need ws_sink).
        let _ = ws_sink.close().await;
        // drop(gone_rx) is implicit when the closure exits
        stream_id_for_writer
    }
    .instrument(span.clone()));

    // Wait for the writer task to complete (terminal frame or error).
    let completed_stream_id = match send_task.await {
        Ok(sid) => sid,
        Err(_) => {
            // Writer panicked — still clean up the reader and worker. S1/S2:
            // panic 由外层补记(Ok/Err 互斥,exactly-once 保持)。
            prometheus::record_stream_terminal(
                &panic_metrics_model,
                &panic_metrics_version,
                "websocket",
                "ws",
                stream_open_time,
                prometheus::StreamCloseReason::Panic.status_family(),
                prometheus::StreamCloseReason::Panic,
                stream_metrics,
                0,
                0,
            );
            drop(gone_tx);
            let cancel_req = streaming::build_stream_cancel(stream_id);
            let _ = worker_client.send_raw(cancel_req).await;
            streaming::observe_or_abort(reader).await;
            return;
        }
    };

    drop(gone_tx);

    // Targeted cancel (replaces broadcast open_worker_stream_cancel).
    let cancel_req = streaming::build_stream_cancel(completed_stream_id);
    let _ = cancel_client.send_raw(cancel_req).await;

    // Observe reader task for panics.
    streaming::observe_or_abort(reader).await;
}

#[cfg(test)]
mod tests {
    //! Task D (HTTP): SSE inference callbacks. The forwarder spawns with a
    //! 64-event buffer, so it processes the worker's Chunk + Done (and fires the
    //! callback) independently of client draining — these tests therefore hold the
    //! Sse and poll for the callback count rather than draining the body. WS uses
    //! the identical fire_inference_request/response helpers (structural parity;
    //! integration-covered).

    use super::*;

    /// Test helper: wrap a JSON `Value` as `RequestBody::Json(Bytes)`.
    fn json_body(v: serde_json::Value) -> RequestBody {
        RequestBody::Json(bytes::Bytes::from(serde_json::to_vec(&v).unwrap()))
    }
    use crate::callback::{Callback, CallbackRunner, InferenceContext, Protocol};
    use crate::config::ModelConfig;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::types::{ModelType, WorkerInfo, WorkerStatus};
    use crate::registry::ModelRegistry;
    use crate::request_context::RequestContext;
    use crate::transport::zmq::WorkerZmqClient;
    use crate::worker::WorkerManager;
    use bytes::Bytes;
    use prost::Message;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;

    fn ipc_endpoint(tag: &str) -> String {
        #[cfg(unix)]
        {
            format!(
                "ipc://{}",
                std::env::temp_dir()
                    .join(format!("sse-{}-{}.sock", tag, std::process::id()))
                    .display()
            )
        }
        #[cfg(not(unix))]
        {
            format!("tcp://127.0.0.1:{}", 37000 + std::process::id() % 1000)
        }
    }

    fn make_state(cb: Arc<CallbackRunner>) -> Arc<AppState> {
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
            crate::config::Config::default(),
            std::path::PathBuf::new(),
            cb,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    /// Register a ready model backed by one ZMQ stream client (test hook).
    async fn ready_state(model: &str, endpoint: String, cb: Arc<CallbackRunner>) -> Arc<AppState> {
        let state = make_state(cb);
        state
            .registry
            .register(model, "1", ModelConfig::default(), ModelType::LitAPI, std::path::PathBuf::new())
            .unwrap();
        state.registry.mark_ready(model, "1").unwrap();
        // open_worker_stream checks mv.workers.len() > 0.
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

    /// PAIR worker: Open → one Chunk + Done.
    fn spawn_done_worker(endpoint: String) -> std::thread::JoinHandle<()> {
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
                    let _ = s.send(pb::Response { uid: req.uid, ..Default::default() }.encode_to_vec(), 0);
                    continue;
                }
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                let mk = |payload| pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: st.stream_id.clone(),
                        payload: Some(payload),
                    })),
                    ..Default::default()
                };
                let _ = s.send(
                    mk(pb::stream_response::Payload::Chunk(pb::StreamChunkResponse {
                        data: Bytes::from_static(b"{}"),
                        is_final: false,
                    }))
                    .encode_to_vec(),
                    0,
                );
                let _ = s.send(mk(pb::stream_response::Payload::Done(pb::StreamDone::default())).encode_to_vec(), 0);
            }
        })
    }

    /// PAIR worker: Open → one Error frame, then close (forwarder breaks on the
    /// peer disconnect after the Error callback fires).
    fn spawn_error_worker(endpoint: String) -> std::thread::JoinHandle<()> {
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
                    continue;
                }
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                let resp = pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: st.stream_id.clone(),
                        payload: Some(pb::stream_response::Payload::Error(pb::StreamError {
                            message: "boom".to_string(),
                        })),
                    })),
                    ..Default::default()
                };
                let _ = s.send(resp.encode_to_vec(), 0);
                return; // close → forwarder observes disconnect and breaks
            }
        })
    }

    /// PAIR worker: Open → Error frame THEN Done frame, then close. Reproduces
    /// the forwarder not terminating after the terminal Error frame: a streaming
    /// framework surfacing an exception and then closing the channel sends Done
    /// after Error, which today fires InferenceResponse a second time.
    fn spawn_error_then_done_worker(endpoint: String) -> std::thread::JoinHandle<()> {
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
                    continue;
                }
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                let mk = |payload| pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: st.stream_id.clone(),
                        payload: Some(payload),
                    })),
                    ..Default::default()
                };
                let _ = s.send(
                    mk(pb::stream_response::Payload::Error(pb::StreamError {
                        message: "boom".to_string(),
                    }))
                    .encode_to_vec(),
                    0,
                );
                let _ = s.send(
                    mk(pb::stream_response::Payload::Done(pb::StreamDone::default()))
                        .encode_to_vec(),
                    0,
                );
                // Stay alive (loop) so both frames are reliably delivered — a PAIR
                // socket closed immediately after send can drop the trailing Done.
                continue;
            }
        })
    }

    struct CountingCallback {
        req: AtomicUsize,
        resp: AtomicUsize,
        last: Mutex<Option<InferenceContext>>,
    }

    #[async_trait::async_trait]
    impl Callback for CountingCallback {
        async fn on_inference_request(&self, _ctx: &InferenceContext) {
            self.req.fetch_add(1, Ordering::Relaxed);
        }
        async fn on_inference_response(&self, ctx: &InferenceContext) {
            self.resp.fetch_add(1, Ordering::Relaxed);
            *self.last.lock().unwrap() = Some(ctx.clone());
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

    fn test_cx() -> RequestContext {
        RequestContext {
            request_id: "sse-rid".to_string(),
            client_ip: "127.0.0.1".to_string(),
            trace_cx: opentelemetry::Context::new(),
            protocol: Protocol::Http,
            principal: None,
            api_protocol: None,
        }
    }

    #[tokio::test]
    async fn sse_callbacks_fire_on_done() {
        let model = "sse_done";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_done_worker(endpoint.clone());
        let cb = Arc::new(CountingCallback {
            req: AtomicUsize::new(0),
            resp: AtomicUsize::new(0),
            last: Mutex::new(None),
        });
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let state = ready_state(model, endpoint, runner).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            false,
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");
        // Hold the Sse so event_rx stays alive while the forwarder processes Done.
        wait_for(|| cb.resp.load(Ordering::Relaxed) >= 1, "resp>=1").await;
        drop(sse);

        assert_eq!(cb.req.load(Ordering::Relaxed), 1, "request fires once");
        assert_eq!(cb.resp.load(Ordering::Relaxed), 1, "Done fires response");
        let protocol = cb
            .last
            .lock()
            .unwrap()
            .as_ref()
            .map(|c| c.protocol)
            .unwrap_or(Protocol::Http);
        assert_eq!(protocol, Protocol::Sse, "response ctx must carry the Sse protocol");
        assert!(
            cb.last.lock().unwrap().as_ref().unwrap().elapsed_us.is_some(),
            "response elapsed_us must be set"
        );
    }

    #[tokio::test]
    async fn sse_callbacks_fire_on_error() {
        let model = "sse_err";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_error_worker(endpoint.clone());
        let cb = Arc::new(CountingCallback {
            req: AtomicUsize::new(0),
            resp: AtomicUsize::new(0),
            last: Mutex::new(None),
        });
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let state = ready_state(model, endpoint, runner).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            false,
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");
        wait_for(|| cb.resp.load(Ordering::Relaxed) >= 1, "resp>=1").await;
        drop(sse);

        assert_eq!(cb.req.load(Ordering::Relaxed), 1);
        assert_eq!(cb.resp.load(Ordering::Relaxed), 1, "Error frame fires response");
    }

    #[tokio::test]
    async fn sse_callback_fires_once_when_error_then_done() {
        // Audit (0.8.0-rc0): the forwarder breaks only on the Done frame
        // (stream.rs:261-263), not on the terminal Error frame. A worker that
        // sends Error then Done (a framework surfacing an exception and then
        // closing the channel) fires InferenceResponse a SECOND time on the
        // trailing Done, and lets a [DONE] event follow the error event.
        // callback.rs documents Error as a terminal frame, so it must fire once.
        let model = "sse_err_done";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_error_then_done_worker(endpoint.clone());
        let cb = Arc::new(CountingCallback {
            req: AtomicUsize::new(0),
            resp: AtomicUsize::new(0),
            last: Mutex::new(None),
        });
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let state = ready_state(model, endpoint, runner).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            false,
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");
        // Wait for the Error frame to fire the first response...
        wait_for(|| cb.resp.load(Ordering::Relaxed) >= 1, "resp>=1").await;
        // ...then let the trailing Done frame be forwarded. The worker sends
        // Error+Done back-to-back, so the Done is processed within a few ms;
        // 150ms is generous. The response count must NOT climb to 2.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        drop(sse);

        assert_eq!(cb.req.load(Ordering::Relaxed), 1);
        assert_eq!(
            cb.resp.load(Ordering::Relaxed),
            1,
            "terminal Error frame must fire InferenceResponse exactly once \
             (current code fires twice on Error→Done)"
        );
    }

    #[tokio::test]
    async fn sse_callback_not_fired_when_stream_dropped() {
        let model = "sse_drop";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_done_worker(endpoint.clone());
        let cb = Arc::new(CountingCallback {
            req: AtomicUsize::new(0),
            resp: AtomicUsize::new(0),
            last: Mutex::new(None),
        });
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let state = ready_state(model, endpoint, runner).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            false,
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");
        // Drop immediately → event_rx goes away → the forwarder's first
        // event_tx.send errors, so it breaks before the Done frame: no response.
        drop(sse);

        wait_for(|| cb.req.load(Ordering::Relaxed) >= 1, "req>=1").await;
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert_eq!(cb.req.load(Ordering::Relaxed), 1, "request still fires on cancel");
        assert_eq!(cb.resp.load(Ordering::Relaxed), 0, "cancel must NOT fire response");
    }

    /// Task B: SSE creates an `inference` span with model/version/request_id
    /// fields (parity with gRPC + unary HTTP). Uses the same double-dispatch
    /// anti-poisoning trick as `grpc::request_metrics_tests` — two live dispatches
    /// (anchor + recording) defeat the callsite-interest fast path so the
    /// recording layer always sees the span. The model is registered + ready but
    /// has NO ZMQ client, so resolve succeeds, the span is built, and open fails
    /// (WorkerCrashed) — the span is what we assert, not the stream.
    #[test]
    fn sse_creates_inference_span_with_fields() {
        use tracing::field::Visit;
        use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

        #[derive(Default)]
        struct FieldCollector(Vec<(String, String)>);
        impl Visit for FieldCollector {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push((field.name().to_string(), format!("{:?}", value)));
            }
        }

        type Recorded = std::sync::Arc<std::sync::Mutex<Vec<(String, Vec<(String, String)>)>>>;
        struct SpanLayer(Recorded);
        impl<S: tracing::Subscriber> Layer<S> for SpanLayer {
            fn on_new_span(
                &self,
                attrs: &tracing::span::Attributes<'_>,
                _id: &tracing::span::Id,
                _ctx: Context<'_, S>,
            ) {
                let mut collector = FieldCollector::default();
                attrs.record(&mut collector);
                self.0
                    .lock()
                    .unwrap()
                    .push((attrs.metadata().name().to_string(), collector.0));
            }
        }

        let recorded: Recorded = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded_thread = recorded.clone();
        let _anchor = tracing::Dispatch::new(
            tracing_subscriber::registry().with(SpanLayer(std::sync::Arc::new(std::sync::Mutex::new(
                Vec::new(),
            )))),
        );
        let recording = tracing::Dispatch::new(
            tracing_subscriber::registry().with(SpanLayer(recorded_thread)),
        );
        let handle = std::thread::spawn(move || {
            let _guard = tracing::dispatcher::set_default(&recording);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let state = make_state(Arc::new(CallbackRunner::new()));
                state
                    .registry
                    .register("span_model", "1", ModelConfig::default(), ModelType::LitAPI, std::path::PathBuf::new())
                    .unwrap();
                state.registry.mark_ready("span_model", "1").unwrap();
                // No ZMQ client / workers → open_worker_stream fails, but the
                // span is built before that. Explicit version so resolve
                // succeeds without an active-version cutover.
                let _ = sse_infer_entry(&state, "span_model", Some("1".to_string()), HeaderMap::new(), json_body(json!({})), test_cx(), false).await;
            });
        });
        handle.join().expect("span test thread must not panic");

        let spans = recorded.lock().unwrap();
        let inference = spans
            .iter()
            .find(|(name, _)| name == "inference")
            .expect("SSE must create an inference span");
        let field = |key: &str| -> String {
            inference
                .1
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        };
        assert!(field("model").contains("span_model"), "span model field: {:?}", inference.1);
        assert!(field("version").contains("1"), "span version field: {:?}", inference.1);
        assert!(field("request_id").contains("sse-rid"), "span request_id field: {:?}", inference.1);
    }

    /// /audit fdbd1c9 (D11): the SSE inference span must carry body_bytes /
    /// body_kind like the unary path (do_infer records them inside its
    /// instrumented block). sse_infer_impl records via
    /// `tracing::Span::current()` while the inference span is never entered —
    /// the fields land on the ambient span (or nowhere), leaving the
    /// inference span's body fields permanently Empty.
    #[test]
    fn sse_inference_span_records_body_fields() {
        use tracing::field::Visit;
        use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

        #[derive(Default)]
        struct Rec {
            names: std::collections::HashMap<tracing::span::Id, String>,
            records: std::collections::HashMap<tracing::span::Id, Vec<(String, String)>>,
        }
        struct RecLayer(std::sync::Arc<std::sync::Mutex<Rec>>);
        struct FieldVisitor(Vec<(String, String)>);
        impl Visit for FieldVisitor {
            fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
                self.0.push((field.name().to_string(), value.to_string()));
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                self.0.push((field.name().to_string(), value.to_string()));
            }
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push((field.name().to_string(), format!("{:?}", value)));
            }
        }
        impl<S: tracing::Subscriber> Layer<S> for RecLayer {
            fn on_new_span(
                &self,
                attrs: &tracing::span::Attributes<'_>,
                id: &tracing::span::Id,
                _ctx: Context<'_, S>,
            ) {
                self.0
                    .lock()
                    .unwrap()
                    .names
                    .insert(id.clone(), attrs.metadata().name().to_string());
            }
            fn on_record(
                &self,
                id: &tracing::span::Id,
                values: &tracing::span::Record<'_>,
                _ctx: Context<'_, S>,
            ) {
                let mut vis = FieldVisitor(Vec::new());
                values.record(&mut vis);
                self.0
                    .lock()
                    .unwrap()
                    .records
                    .entry(id.clone())
                    .or_default()
                    .extend(vis.0);
            }
        }

        let rec = std::sync::Arc::new(std::sync::Mutex::new(Rec::default()));
        let rec_thread = rec.clone();
        // Same double-dispatch anti-poisoning trick as
        // sse_creates_inference_span_with_fields.
        let _anchor = tracing::Dispatch::new(
            tracing_subscriber::registry().with(RecLayer(std::sync::Arc::new(
                std::sync::Mutex::new(Rec::default()),
            ))),
        );
        let recording = tracing::Dispatch::new(
            tracing_subscriber::registry().with(RecLayer(rec_thread)),
        );
        let handle = std::thread::spawn(move || {
            let _guard = tracing::dispatcher::set_default(&recording);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let state = make_state(Arc::new(CallbackRunner::new()));
                state
                    .registry
                    .register("span_body_model", "1", ModelConfig::default(), ModelType::LitAPI, std::path::PathBuf::new())
                    .unwrap();
                state.registry.mark_ready("span_body_model", "1").unwrap();
                // No workers/ZMQ → open_worker_stream fails AFTER the
                // body-field record calls run — the span is what we assert.
                let _ = sse_infer_entry(
                    &state,
                    "span_body_model",
                    Some("1".to_string()),
                    HeaderMap::new(),
                    json_body(json!({"x": 1})),
                    test_cx(),
                    false,
                )
                .await;
            });
        });
        handle.join().expect("span test thread must not panic");

        let rec = rec.lock().unwrap();
        let inference_id = rec
            .names
            .iter()
            .find(|(_, n)| *n == "inference")
            .map(|(id, _)| id.clone())
            .expect("SSE must create an inference span");
        let fields = rec.records.get(&inference_id).cloned().unwrap_or_default();
        assert!(
            fields.iter().any(|(k, _)| k == "body_bytes"),
            "inference span must record body_bytes (D11); got records: {fields:?}"
        );
        assert!(
            fields.iter().any(|(k, v)| k == "body_kind" && v == "json"),
            "inference span must record body_kind=json (D11); got records: {fields:?}"
        );
    }

    /// Task E: a stalled SSE stream (worker sends one chunk, then hangs) with no
    /// client deadline is reclaimed by the always-on chunk-idle (方案 C), not
    /// left unbounded. The forwarder breaks on idle-elapsed without emitting
    /// `[DONE]`, so draining the body completes within ~idle (not 300s).
    fn make_state_with_idle(cb: Arc<CallbackRunner>, idle_secs: f32) -> Arc<AppState> {
        let mut config = crate::config::Config::default();
        config.server.decoupled_idle_timeout_secs = idle_secs;
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

    async fn ready_state_with_idle(
        model: &str,
        endpoint: String,
        cb: Arc<CallbackRunner>,
        idle_secs: f32,
    ) -> Arc<AppState> {
        let state = make_state_with_idle(cb, idle_secs);
        state
            .registry
            .register(model, "1", ModelConfig::default(), ModelType::LitAPI, std::path::PathBuf::new())
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

    /// PAIR worker: Open → ONE chunk, then stall (no Done / close).
    fn spawn_stall_worker(endpoint: String) -> std::thread::JoinHandle<()> {
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
                    let _ = s.send(pb::Response { uid: req.uid, ..Default::default() }.encode_to_vec(), 0);
                    continue;
                }
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                let chunk = pb::Response {
                    payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                        stream_id: st.stream_id.clone(),
                        payload: Some(pb::stream_response::Payload::Chunk(pb::StreamChunkResponse {
                            data: Bytes::from_static(b"{}"),
                            is_final: false,
                        })),
                    })),
                    ..Default::default()
                };
                let _ = s.send(chunk.encode_to_vec(), 0);
                // STALL: send no Done / close.
            }
        })
    }

    #[tokio::test]
    async fn sse_idle_reclaims_a_stalled_stream() {
        let model = "sse_stall";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_stall_worker(endpoint.clone());
        let state =
            ready_state_with_idle(model, endpoint, Arc::new(CallbackRunner::new()), 0.2).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let start = std::time::Instant::now();
        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            false,
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");
        let resp = sse.into_response();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .expect("drain sse body");
        let elapsed = start.elapsed();

        let body = String::from_utf8_lossy(&bytes);
        assert!(!body.contains("[DONE]"), "stalled stream must not reach Done: {body}");
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "stalled stream must be reclaimed by the always-on idle (~200ms), not hang; took {elapsed:?}"
        );
    }

    // ===== WS bidi (§5.5): loopback WS server + PAIR recording worker =======

    /// Loopback axum server exposing the WS stream routes (incl. versioned —
    /// used by the not-ready early-rejection test which needs an explicit
    /// version to pass the resolve step).
    async fn spawn_ws_server(state: Arc<AppState>) -> String {
        let app = axum::Router::new()
            .route(
                "/v2/models/:model_name/stream",
                axum::routing::get(ws_stream_handler),
            )
            .route(
                "/v2/models/:model_name/:version/stream",
                axum::routing::get(ws_stream_version_handler),
            )
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("ws://127.0.0.1:{}/v2/models", port)
    }

    /// PAIR worker: records (action, chunk-bytes); holds the stream after
    /// Open (no spontaneous output); on Close → record + reply Done so the
    /// downstream completes; on Cancel → record.
    fn spawn_recording_worker(
        endpoint: String,
    ) -> (
        std::thread::JoinHandle<()>,
        std::sync::mpsc::Receiver<(String, Vec<u8>)>,
    ) {
        let (tx, rx) = std::sync::mpsc::channel::<(String, Vec<u8>)>();
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
                match st.action {
                    Some(pb::stream_request::Action::Open(_)) => {
                        let _ = tx.send(("open".to_string(), Vec::new()));
                    }
                    Some(pb::stream_request::Action::Chunk(c)) => {
                        let _ = tx.send(("chunk".to_string(), c.data.to_vec()));
                    }
                    Some(pb::stream_request::Action::Close(_)) => {
                        let _ = tx.send(("close".to_string(), Vec::new()));
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
                        let _ = tx.send(("cancel".to_string(), Vec::new()));
                    }
                    None => {}
                }
            }
        });
        (handle, rx)
    }

    /// §5.5-1/2: Binary frames after the first frame reach the worker as
    /// byte-identical ordered Chunks; an app-level {"type":"close"} reaches
    /// the worker as Close and the downstream terminal frame still arrives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ws_bidi_binary_chunks_and_app_close_reach_worker() {
        use futures::{SinkExt, StreamExt};

        let model = "ws_rec";
        let endpoint = ipc_endpoint(model);
        let (_w, actions) = spawn_recording_worker(endpoint.clone());
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        state.registry.activate_version(model, "1").unwrap();
        let base = spawn_ws_server(state).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{}/{}/stream", base, model))
            .await
            .expect("WS connect failed");
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"input": 1}"#.to_string(),
        ))
        .await
        .unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Binary(vec![1, 2, 3]))
            .await
            .unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Binary(vec![4, 5, 6]))
            .await
            .unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"type":"close"}"#.to_string(),
        ))
        .await
        .unwrap();

        // Worker receives, in order: open, chunk(1,2,3), chunk(4,5,6), close.
        let mut seen = Vec::new();
        for _ in 0..4 {
            seen.push(
                actions
                    .recv_timeout(std::time::Duration::from_secs(3))
                    .expect("worker action"),
            );
        }
        assert_eq!(seen[0].0, "open");
        assert_eq!(seen[1], ("chunk".to_string(), vec![1u8, 2, 3]));
        assert_eq!(seen[2], ("chunk".to_string(), vec![4u8, 5, 6]));
        assert_eq!(seen[3].0, "close");

        // Downstream: the terminal {"done":true} still arrives after the
        // app-level close (input closed ≠ output closed).
        let mut got_done = false;
        while let Ok(Some(Ok(msg))) =
            tokio::time::timeout(std::time::Duration::from_secs(3), ws.next()).await
        {
            if let tokio_tungstenite::tungstenite::Message::Text(t) = msg {
                if t.contains("\"done\":true") {
                    got_done = true;
                    break;
                }
            }
        }
        assert!(got_done, "terminal Done frame must arrive after app-level close");
    }

    /// §5.5-4: an unknown Text control frame → client gets the protocol error
    /// AND the worker receives Cancel (not just an idle-timeout cleanup).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ws_bidi_unknown_control_frame_errors_and_cancels_worker() {
        use futures::{SinkExt, StreamExt};

        let model = "ws_bogus";
        let endpoint = ipc_endpoint(model);
        let (_w, actions) = spawn_recording_worker(endpoint.clone());
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        state.registry.activate_version(model, "1").unwrap();
        let base = spawn_ws_server(state).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{}/{}/stream", base, model))
            .await
            .expect("WS connect failed");
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"input": 1}"#.to_string(),
        ))
        .await
        .unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"type":"bogus"}"#.to_string(),
        ))
        .await
        .unwrap();

        let mut got_error = false;
        while let Ok(Some(Ok(msg))) =
            tokio::time::timeout(std::time::Duration::from_secs(3), ws.next()).await
        {
            if let tokio_tungstenite::tungstenite::Message::Text(t) = msg {
                assert!(
                    t.contains("unknown control frame"),
                    "unexpected text frame: {t}"
                );
                got_error = true;
            }
        }
        assert!(got_error, "client must receive the protocol error frame");

        let mut got_cancel = false;
        while let Ok((a, _)) = actions.recv_timeout(std::time::Duration::from_secs(3)) {
            if a == "cancel" {
                got_cancel = true;
                break;
            }
        }
        assert!(got_cancel, "worker must receive Cancel after protocol error");
    }

    /// §5.5-5: hard client disconnect → the gone signal terminates the writer
    /// and the worker receives Cancel promptly (seconds), not via the 300s
    /// chunk-idle fallback.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ws_bidi_client_disconnect_cancels_worker_promptly() {
        use futures::SinkExt;

        let model = "ws_disc";
        let endpoint = ipc_endpoint(model);
        let (_w, actions) = spawn_recording_worker(endpoint.clone());
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        state.registry.activate_version(model, "1").unwrap();
        let base = spawn_ws_server(state).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{}/{}/stream", base, model))
            .await
            .expect("WS connect failed");
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"input": 1}"#.to_string(),
        ))
        .await
        .unwrap();
        assert_eq!(
            actions
                .recv_timeout(std::time::Duration::from_secs(3))
                .map(|a| a.0),
            Ok("open".to_string())
        );

        // Abrupt drop: no WS close handshake — the reader must see the
        // transport failure and signal gone.
        drop(ws);

        let mut got_cancel = false;
        while let Ok((a, _)) = actions.recv_timeout(std::time::Duration::from_secs(5)) {
            if a == "cancel" {
                got_cancel = true;
                break;
            }
        }
        assert!(
            got_cancel,
            "worker must receive Cancel promptly on client disconnect (gone signal)"
        );
    }

    /// §5.5-6: terminal Error fires InferenceResponse exactly once — an
    /// Error→Done worker (framework raises, then closes the channel) must
    /// not double-fire (SSE parity: Error is terminal).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ws_bidi_callback_fires_once_when_error_then_done() {
        use futures::{SinkExt, StreamExt};

        let model = "ws_err_done";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_error_then_done_worker(endpoint.clone());
        let cb = Arc::new(CountingCallback {
            req: AtomicUsize::new(0),
            resp: AtomicUsize::new(0),
            last: Mutex::new(None),
        });
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let state = ready_state(model, endpoint, runner).await;
        state.registry.activate_version(model, "1").unwrap();
        let base = spawn_ws_server(state).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{}/{}/stream", base, model))
            .await
            .expect("WS connect failed");
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"input": 1}"#.to_string(),
        ))
        .await
        .unwrap();

        // Drain until the server closes the socket.
        while let Ok(Some(Ok(_))) =
            tokio::time::timeout(std::time::Duration::from_secs(3), ws.next()).await
        {}
        // Generous window for a (buggy) trailing-Done second fire.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        assert_eq!(cb.req.load(Ordering::Relaxed), 1, "request fires once");
        assert_eq!(
            cb.resp.load(Ordering::Relaxed),
            1,
            "terminal Error frame must fire InferenceResponse exactly once \
             (current code fires twice on Error→Done)"
        );
    }

    /// §D1 audit (2026-08-07): `Error` is a terminal frame by contract
    /// (callback.rs), same as `Done`. Regression guard: the WS writer loop
    /// breaks via this helper, so it must keep matching Error.
    #[test]
    fn is_stream_terminal_error_is_terminal() {
        let error_chunk = pb::StreamResponse {
            stream_id: "s".into(),
            payload: Some(pb::stream_response::Payload::Error(pb::StreamError {
                message: "boom".into(),
            })),
        };
        assert!(
            is_stream_terminal(&error_chunk),
            "Error is a terminal frame (callback.rs) — \
             is_stream_terminal must return true so the WS loop breaks on Error"
        );
    }

    /// §D1 audit (2026-08-07): replicate the WS writer inner-loop logic with
    /// Error→Done chunks fed directly through an mpsc channel (bypassing the
    /// ZMQ route-removal backstop that would otherwise drop the trailing
    /// Done). The callback must fire exactly once, on the Error frame.
    #[tokio::test]
    async fn ws_writer_loop_error_then_done_fires_callback_once() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<pb::StreamResponse>(4);

        let error_chunk = pb::StreamResponse {
            stream_id: "s".into(),
            payload: Some(pb::stream_response::Payload::Error(pb::StreamError {
                message: "boom".into(),
            })),
        };
        let done_chunk = pb::StreamResponse {
            stream_id: "s".into(),
            payload: Some(pb::stream_response::Payload::Done(pb::StreamDone::default())),
        };

        tx.send(error_chunk).await.unwrap();
        tx.send(done_chunk).await.unwrap();
        drop(tx);

        let cb = Arc::new(CountingCallback {
            req: AtomicUsize::new(0),
            resp: AtomicUsize::new(0),
            last: Mutex::new(None),
        });
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let req_ctx = crate::callback::InferenceContext {
            model_name: "m".into(),
            version: "1".into(),
            route: "/predict".into(),
            protocol: crate::callback::Protocol::WebSocket,
            request_id: "rid".into(),
            client_ip: "127.0.0.1".into(),
            elapsed_us: None,
        };
        let open_time = std::time::Instant::now();

        // Mirror of the WS writer inner loop in `handle_ws_stream`.
        // Uses is_stream_terminal — the same check the handler uses.
        loop {
            let chunk = match streaming::recv_chunk(&mut rx, None, None).await {
                Ok(Some(c)) => c,
                Ok(None) => break,
                Err(_) => break,
            };
            match &chunk.payload {
                Some(pb::stream_response::Payload::Error(_)) => {
                    crate::callback::fire_inference_response(&runner, &req_ctx, open_time);
                }
                Some(pb::stream_response::Payload::Done(_)) => {
                    crate::callback::fire_inference_response(&runner, &req_ctx, open_time);
                }
                _ => {}
            }
            if is_stream_terminal(&chunk) {
                break;
            }
        }

        // fire_inference_response is tokio::spawn — let the callbacks land.
        wait_for(|| cb.resp.load(Ordering::Relaxed) >= 1, "resp>=1").await;
        // Give a regressed second fire time to land before asserting.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        assert_eq!(
            cb.resp.load(Ordering::Relaxed),
            1,
            "Error→Done must fire the callback once: if is_stream_terminal \
             stops matching Error, the loop falls through to Done and fires \
             a second time (fired {n})",
            n = cb.resp.load(Ordering::Relaxed)
        );
    }

    // ==== SSE decoupled (PR-1) test helpers ====

    /// PAIR worker: records (action, decoupled_flag_from_open); after Open,
    /// sends `respond_chunks` chunks then Done. `respond_chunks == 0` means
    /// stall after Open (no Done, no chunks — for idle-reclaim tests).
    fn spawn_decoupled_recording_worker(
        endpoint: String,
        respond_chunks: usize,
    ) -> (
        std::thread::JoinHandle<()>,
        std::sync::mpsc::Receiver<(String, Option<bool>)>,
    ) {
        let (tx, rx) = std::sync::mpsc::channel::<(String, Option<bool>)>();
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
                match st.action {
                    Some(pb::stream_request::Action::Open(o)) => {
                        let decoupled = o.decoupled.unwrap_or(false);
                        let _ = tx.send(("open".to_string(), Some(decoupled)));
                        let mk = |payload| pb::Response {
                            payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                                stream_id: st.stream_id.clone(),
                                payload: Some(payload),
                            })),
                            ..Default::default()
                        };
                        for i in 0..respond_chunks {
                            let _ = s.send(
                                mk(pb::stream_response::Payload::Chunk(pb::StreamChunkResponse {
                                    data: bytes::Bytes::from(format!("chunk-{}", i)),
                                    is_final: false,
                                }))
                                .encode_to_vec(),
                                0,
                            );
                        }
                        if respond_chunks > 0 {
                            let _ = s.send(
                                mk(pb::stream_response::Payload::Done(
                                    pb::StreamDone::default(),
                                ))
                                .encode_to_vec(),
                                0,
                            );
                        }
                        // Stay alive to receive Cancel (or stall if respond_chunks==0).
                    }
                    Some(pb::stream_request::Action::Cancel(_)) => {
                        let _ = tx.send(("cancel".to_string(), None));
                    }
                    _ => {}
                }
            }
        });
        (handle, rx)
    }

    /// PAIR worker: records actions received but sends nothing back. Used as
    /// the "bystander" worker in targeted-cancel contract tests — it proves
    /// the Cancel does NOT broadcast.
    fn spawn_silent_recording_worker(
        endpoint: String,
    ) -> (
        std::thread::JoinHandle<()>,
        std::sync::mpsc::Receiver<String>,
    ) {
        let (tx, rx) = std::sync::mpsc::channel::<String>();
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
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                match st.action {
                    Some(pb::stream_request::Action::Open(_)) => {
                        let _ = tx.send("open".to_string());
                    }
                    Some(pb::stream_request::Action::Cancel(_)) => {
                        let _ = tx.send("cancel".to_string());
                    }
                    _ => {}
                }
            }
        });
        (handle, rx)
    }

    /// Register two workers (id 0 and 1) for the same model version, each
    /// with its own ZMQ endpoint. Used by the targeted-cancel contract test.
    async fn ready_state_two_workers(
        model: &str,
        endpoints: [String; 2],
        cb: Arc<CallbackRunner>,
    ) -> Arc<AppState> {
        let state = make_state(cb);
        state
            .registry
            .register(
                model, "1", ModelConfig::default(), ModelType::LitAPI,
                std::path::PathBuf::new(),
            )
            .unwrap();
        state.registry.mark_ready(model, "1").unwrap();
        state
            .registry
            .set_workers(
                model, "1",
                vec![
                    WorkerInfo {
                        worker_id: 0, device: "cpu:0".to_string(),
                        endpoint: String::new(), pid: None,
                        status: WorkerStatus::Ready, capacity: None,
                    },
                    WorkerInfo {
                        worker_id: 1, device: "cpu:1".to_string(),
                        endpoint: String::new(), pid: None,
                        status: WorkerStatus::Ready, capacity: None,
                    },
                ],
            )
            .unwrap();
        let client0 = Arc::new(WorkerZmqClient::new(endpoints[0].clone()));
        let client1 = Arc::new(WorkerZmqClient::new(endpoints[1].clone()));
        state
            .worker_manager
            .insert_zmq_clients_for_test(model, "1", vec![client0, client1])
            .await;
        state
    }

    /// Build an AppState with a custom FeaturesConfig (for feature-gating
    /// route-mount tests).
    fn make_state_with_features(
        features: crate::config::FeaturesConfig,
        cb: Arc<CallbackRunner>,
    ) -> Arc<AppState> {
        let config = crate::config::Config {
            features,
            ..Default::default()
        };
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

    // ==== SSE decoupled tests ====

    /// PR-1 test 1: the decoupled path sets StreamOpen.decoupled = true.
    #[tokio::test]
    async fn sse_decoupled_open_carries_decoupled_true() {
        let model = "sse_dc_open";
        let endpoint = ipc_endpoint(model);
        let (_w, actions) = spawn_decoupled_recording_worker(endpoint.clone(), 1);
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            true, // decoupled
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");

        let (action, decoupled) = actions
            .recv_timeout(std::time::Duration::from_secs(3))
            .expect("open action");
        assert_eq!(action, "open");
        assert_eq!(
            decoupled,
            Some(true),
            "StreamOpen.decoupled must be true for decoupled path"
        );
        drop(sse);
    }

    /// PR-1 test 2: decoupled SSE forwards 2 chunks + [DONE]; CountingCallback
    /// fires once per request/response with Protocol::Sse and elapsed_us.
    #[tokio::test]
    async fn sse_decoupled_chunks_done_and_callbacks() {
        let model = "sse_dc_2ch";
        let endpoint = ipc_endpoint(model);
        let (_w, _actions) = spawn_decoupled_recording_worker(endpoint.clone(), 2);
        let cb = Arc::new(CountingCallback {
            req: AtomicUsize::new(0),
            resp: AtomicUsize::new(0),
            last: Mutex::new(None),
        });
        let runner = Arc::new(CallbackRunner::new());
        runner.register(cb.clone()).await;
        let state = ready_state(model, endpoint, runner).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            true, // decoupled
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");

        let resp = sse.into_response();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .expect("drain sse body");
        let body = String::from_utf8_lossy(&bytes);

        // 2 data chunks + [DONE]
        assert!(
            body.contains("data: chunk-0"),
            "must contain first chunk; got: {body}"
        );
        assert!(
            body.contains("data: chunk-1"),
            "must contain second chunk; got: {body}"
        );
        assert!(
            body.contains("data: [DONE]"),
            "must contain terminal [DONE]; got: {body}"
        );

        wait_for(|| cb.resp.load(Ordering::Relaxed) >= 1, "resp>=1").await;
        assert_eq!(cb.req.load(Ordering::Relaxed), 1, "request fires once");
        assert_eq!(cb.resp.load(Ordering::Relaxed), 1, "Done fires response");
        let protocol = cb
            .last
            .lock()
            .unwrap()
            .as_ref()
            .map(|c| c.protocol)
            .unwrap_or(Protocol::Http);
        assert_eq!(
            protocol, Protocol::Sse,
            "response ctx must carry the Sse protocol"
        );
        assert!(
            cb.last.lock().unwrap().as_ref().unwrap().elapsed_us.is_some(),
            "response elapsed_us must be set"
        );
    }

    /// PR-1 test 3 (D4 contract): targeted cancel only reaches the pinned
    /// worker — the bystander receives no frames at all.
    #[tokio::test]
    async fn sse_decoupled_targeted_cancel_reaches_only_pinned_worker() {
        let model = "sse_dc_pin";
        let ep0 = ipc_endpoint(&format!("{}-0", model));
        let ep1 = ipc_endpoint(&format!("{}-1", model));

        // Worker 0: responds with 1 chunk + Done (the stream completes).
        let (_w0, actions0) = spawn_decoupled_recording_worker(ep0.clone(), 1);
        // Worker 1: silent bystander — records everything, sends nothing.
        let (_w1, actions1) = spawn_silent_recording_worker(ep1.clone());

        let state =
            ready_state_two_workers(model, [ep0, ep1], Arc::new(CallbackRunner::new())).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Pin to worker 0 via x-lite-worker-id header.
        let mut headers = HeaderMap::new();
        headers.insert("x-lite-worker-id", "0".parse().unwrap());

        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            headers,
            json_body(json!({})),
            test_cx(),
            true, // decoupled
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");

        // Drain to completion so the targeted cancel fires.
        let resp = sse.into_response();
        let _bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .expect("drain sse body");

        // Worker 0 must see open + cancel.
        let a0: Vec<String> = std::iter::from_fn(|| {
            actions0
                .recv_timeout(std::time::Duration::from_secs(3))
                .ok()
                .map(|(a, _)| a)
        })
        .collect();
        assert!(
            a0.contains(&"open".to_string()),
            "worker 0 must receive open; got: {a0:?}"
        );
        assert!(
            a0.contains(&"cancel".to_string()),
            "worker 0 must receive targeted cancel; got: {a0:?}"
        );

        // Worker 1 must receive NOTHING — no broadcast.
        let a1: Vec<String> = std::iter::from_fn(|| {
            actions1
                .recv_timeout(std::time::Duration::from_millis(500))
                .ok()
        })
        .collect();
        assert!(
            a1.is_empty(),
            "worker 1 must receive no frames (targeted cancel, not broadcast); got: {a1:?}"
        );
    }

    /// PR-1 test 4: a stalled decoupled SSE stream (worker sends one chunk,
    /// then hangs) is reclaimed by the always-on chunk-idle, not left
    /// unbounded.
    #[tokio::test]
    async fn sse_decoupled_idle_reclaims_stalled_stream() {
        let model = "sse_dc_stall";
        let endpoint = ipc_endpoint(model);
        // respond_chunks=0: worker sends nothing after Open → stall.
        let (_w, _actions) = spawn_decoupled_recording_worker(endpoint.clone(), 0);
        let state =
            ready_state_with_idle(model, endpoint, Arc::new(CallbackRunner::new()), 0.2).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let start = std::time::Instant::now();
        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            true, // decoupled
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");
        let resp = sse.into_response();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .expect("drain sse body");
        let elapsed = start.elapsed();

        let body = String::from_utf8_lossy(&bytes);
        assert!(
            !body.contains("[DONE]"),
            "stalled stream must not reach Done: {body}"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "stalled decoupled stream must be reclaimed by idle (~200ms), not hang; took {elapsed:?}"
        );
    }

    /// PR-1 test 5: when features.decoupled=false, the decoupled route is
    /// not mounted — POST returns 404.
    #[tokio::test]
    async fn decoupled_routes_unmounted_when_feature_off() {
        let features = crate::config::FeaturesConfig {
            decoupled: false,
            ..Default::default()
        };
        let state = make_state_with_features(features, Arc::new(CallbackRunner::new()));
        // Register a model so route matching doesn't fail before the
        // feature gate check; the fallback handler needs the registry
        // to contain the model.
        state
            .registry
            .register(
                "m", "1", ModelConfig::default(), ModelType::LitAPI,
                std::path::PathBuf::new(),
            )
            .unwrap();

        let app = crate::http::routes::create_routes(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v2/models/m/decoupled")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(r#"{}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            axum::http::StatusCode::NOT_FOUND,
            "decoupled route must 404 when features.decoupled=false"
        );
    }

    // ==== WS decoupled (PR-2) test helpers ====

    /// Loopback axum server exposing the WS decoupled-stream route.
    async fn spawn_ws_decoupled_server(state: Arc<AppState>) -> String {
        let app = axum::Router::new()
            .route(
                "/v2/models/:model_name/decoupled-stream",
                axum::routing::get(ws_decoupled_handler),
            )
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("ws://127.0.0.1:{}/v2/models", port)
    }

    // ==== WS decoupled tests ====

    /// PR-2 test 1: first frame opens a decoupled stream; worker pushes 2
    /// chunks + Done → client receives Binary×2 + {"done":true}.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ws_decoupled_open_flag_and_chunks_reach_client() {
        use futures::{SinkExt, StreamExt};

        let model = "ws_dc_open";
        let endpoint = ipc_endpoint(model);
        let (_w, actions) = spawn_decoupled_recording_worker(endpoint.clone(), 2);
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        state.registry.activate_version(model, "1").unwrap();
        let base = spawn_ws_decoupled_server(state).await;

        let (mut ws, _) =
            tokio_tungstenite::connect_async(format!("{}/{}/decoupled-stream", base, model))
                .await
                .expect("WS connect failed");
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"input": 1}"#.to_string(),
        ))
        .await
        .unwrap();

        // Collect all messages from server.
        let mut msgs: Vec<String> = Vec::new();
        while let Ok(Some(Ok(msg))) =
            tokio::time::timeout(std::time::Duration::from_secs(5), ws.next()).await
        {
            match msg {
                tokio_tungstenite::tungstenite::Message::Binary(b) => {
                    msgs.push(format!("bin:{}", String::from_utf8_lossy(&b)));
                }
                tokio_tungstenite::tungstenite::Message::Text(t) => {
                    msgs.push(format!("txt:{}", t));
                }
                _ => {}
            }
        }

        // 2 Binary chunks + terminal {"done":true}
        assert_eq!(msgs.len(), 3, "expected 3 messages; got: {msgs:?}");
        assert!(msgs[0].starts_with("bin:chunk-0"), "chunk 0: {msgs:?}");
        assert!(msgs[1].starts_with("bin:chunk-1"), "chunk 1: {msgs:?}");
        assert_eq!(msgs[2], "txt:{\"done\":true}", "terminal Done: {msgs:?}");

        // Worker must have received Open with decoupled=true.
        let (action, decoupled) = actions
            .recv_timeout(std::time::Duration::from_secs(3))
            .expect("open action");
        assert_eq!(action, "open");
        assert_eq!(decoupled, Some(true), "StreamOpen.decoupled must be true");
    }

    /// PR-2 test 2 (D1): `{"type":"cancel"}` → worker receives Cancel →
    /// WS closes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ws_decoupled_cancel_frame_cancels_worker_and_closes() {
        use futures::SinkExt;

        let model = "ws_dc_cancel";
        let endpoint = ipc_endpoint(model);
        // respond_chunks=0: stall after Open, no spontaneous Done.
        let (_w, actions) = spawn_decoupled_recording_worker(endpoint.clone(), 0);
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        state.registry.activate_version(model, "1").unwrap();
        let base = spawn_ws_decoupled_server(state).await;

        let (mut ws, _) =
            tokio_tungstenite::connect_async(format!("{}/{}/decoupled-stream", base, model))
                .await
                .expect("WS connect failed");
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"input": 1}"#.to_string(),
        ))
        .await
        .unwrap();

        // Wait for open to be recorded.
        assert_eq!(
            actions
                .recv_timeout(std::time::Duration::from_secs(3))
                .map(|a| a.0),
            Ok("open".to_string())
        );

        // Send cancel frame.
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"type":"cancel"}"#.to_string(),
        ))
        .await
        .unwrap();

        // Worker must receive Cancel.
        let mut got_cancel = false;
        while let Ok((a, _)) = actions.recv_timeout(std::time::Duration::from_secs(3)) {
            if a == "cancel" {
                got_cancel = true;
                break;
            }
        }
        assert!(got_cancel, "worker must receive Cancel after cancel frame");
    }

    /// PR-2 test 3 (D1): `{"type":"close"}` is a cancel alias — worker
    /// receives Cancel + WS closes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ws_decoupled_close_frame_is_cancel_alias() {
        use futures::SinkExt;

        let model = "ws_dc_close_alias";
        let endpoint = ipc_endpoint(model);
        let (_w, actions) = spawn_decoupled_recording_worker(endpoint.clone(), 0);
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        state.registry.activate_version(model, "1").unwrap();
        let base = spawn_ws_decoupled_server(state).await;

        let (mut ws, _) =
            tokio_tungstenite::connect_async(format!("{}/{}/decoupled-stream", base, model))
                .await
                .expect("WS connect failed");
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"input": 1}"#.to_string(),
        ))
        .await
        .unwrap();

        assert_eq!(
            actions
                .recv_timeout(std::time::Duration::from_secs(3))
                .map(|a| a.0),
            Ok("open".to_string())
        );

        // Send close frame (alias for cancel in decoupled mode).
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"type":"close"}"#.to_string(),
        ))
        .await
        .unwrap();

        let mut got_cancel = false;
        while let Ok((a, _)) = actions.recv_timeout(std::time::Duration::from_secs(3)) {
            if a == "cancel" {
                got_cancel = true;
                break;
            }
        }
        assert!(
            got_cancel,
            "worker must receive Cancel after close frame (decoupled alias)"
        );
    }

    /// PR-2 test 4 (D1): Binary frame after the first payload frame →
    /// protocol error → client gets error → worker receives Cancel.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ws_decoupled_binary_frame_is_protocol_error() {
        use futures::{SinkExt, StreamExt};

        let model = "ws_dc_bin_err";
        let endpoint = ipc_endpoint(model);
        let (_w, actions) = spawn_decoupled_recording_worker(endpoint.clone(), 0);
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        state.registry.activate_version(model, "1").unwrap();
        let base = spawn_ws_decoupled_server(state).await;

        let (mut ws, _) =
            tokio_tungstenite::connect_async(format!("{}/{}/decoupled-stream", base, model))
                .await
                .expect("WS connect failed");
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"input": 1}"#.to_string(),
        ))
        .await
        .unwrap();

        assert_eq!(
            actions
                .recv_timeout(std::time::Duration::from_secs(3))
                .map(|a| a.0),
            Ok("open".to_string())
        );

        // Send Binary after first frame → protocol error.
        ws.send(tokio_tungstenite::tungstenite::Message::Binary(vec![
            1, 2, 3,
        ]))
        .await
        .unwrap();

        // Client must receive the protocol error frame.
        let mut got_error = false;
        while let Ok(Some(Ok(msg))) =
            tokio::time::timeout(std::time::Duration::from_secs(3), ws.next()).await
        {
            if let tokio_tungstenite::tungstenite::Message::Text(t) = msg {
                assert!(
                    t.contains("no data frames"),
                    "unexpected text frame: {t}"
                );
                got_error = true;
            }
        }
        assert!(got_error, "client must receive the protocol error frame");

        // Worker must receive Cancel.
        let mut got_cancel = false;
        while let Ok((a, _)) = actions.recv_timeout(std::time::Duration::from_secs(3)) {
            if a == "cancel" {
                got_cancel = true;
                break;
            }
        }
        assert!(got_cancel, "worker must receive Cancel after protocol error");
    }

    /// PR-2 test 5 (D1): hard client disconnect → gone signal →
    /// worker receives Cancel promptly (not via idle timeout).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ws_decoupled_client_disconnect_cancels_worker_promptly() {
        use futures::SinkExt;

        let model = "ws_dc_disc";
        let endpoint = ipc_endpoint(model);
        let (_w, actions) = spawn_decoupled_recording_worker(endpoint.clone(), 0);
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        state.registry.activate_version(model, "1").unwrap();
        let base = spawn_ws_decoupled_server(state).await;

        let (mut ws, _) =
            tokio_tungstenite::connect_async(format!("{}/{}/decoupled-stream", base, model))
                .await
                .expect("WS connect failed");
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"input": 1}"#.to_string(),
        ))
        .await
        .unwrap();

        assert_eq!(
            actions
                .recv_timeout(std::time::Duration::from_secs(3))
                .map(|a| a.0),
            Ok("open".to_string())
        );

        // Abrupt drop: no WS close handshake.
        drop(ws);

        let mut got_cancel = false;
        while let Ok((a, _)) = actions.recv_timeout(std::time::Duration::from_secs(5)) {
            if a == "cancel" {
                got_cancel = true;
                break;
            }
        }
        assert!(
            got_cancel,
            "worker must receive Cancel promptly on client disconnect (gone signal)"
        );
    }

    // === B2: first-frame dispatch unit tests (E3/E4) ===

    fn ct_map(val: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(axum::http::header::CONTENT_TYPE, val.parse().unwrap());
        h
    }

    fn ct_of(h: &HeaderMap) -> String {
        h.get(axum::http::header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap_or("").to_string())
            .unwrap_or_default()
    }

    #[test]
    fn b2_ct_normalization_missing_injects_octet_stream() {
        let mut h = HeaderMap::new();
        normalize_binary_first_frame_ct(&mut h, "m");
        assert_eq!(ct_of(&h), "application/octet-stream");
    }

    #[test]
    fn b2_ct_normalization_non_json_preserved() {
        let mut h = ct_map("image/png");
        normalize_binary_first_frame_ct(&mut h, "m");
        assert_eq!(ct_of(&h), "image/png", "payload metadata must pass through");
    }

    #[test]
    fn b2_ct_normalization_json_rewritten() {
        for ct in ["application/json", "application/vnd.api+json"] {
            let mut h = ct_map(ct);
            normalize_binary_first_frame_ct(&mut h, "m");
            assert_eq!(
                ct_of(&h),
                "application/octet-stream",
                "JSON CT with a Binary frame is contradictory — frame type wins ({ct})"
            );
        }
    }

    #[test]
    fn b2_body_kind_from_frame_type() {
        assert_eq!(FirstFrame::Json("{}".to_string()).body_kind(), "json");
        assert_eq!(
            FirstFrame::Raw(bytes::Bytes::from_static(b"\x00")).body_kind(),
            "raw"
        );
    }

    // ===== S1/S2/S8 (批次 1):HTTP 流式请求级计数 + 取消计数 + per-worker dispatch =====

    /// S1:SSE 正常完成(Done)→ REQUESTS_TOTAL{2xx} +1。
    #[tokio::test]
    async fn sse_done_records_requests_total_2xx() {
        let model = "sse_req_done";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_done_worker(endpoint.clone());
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let counter = prometheus::REQUESTS_TOTAL.with_label_values(&[model, "1", "2xx"]);
        let before = counter.get();
        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            false,
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");
        wait_for(|| counter.get() >= before + 1.0, "requests_total 2xx").await;
        drop(sse);
        assert_eq!(
            counter.get(),
            before + 1.0,
            "SSE done must record exactly one 2xx request"
        );
    }

    /// S1:SSE Error 帧 → REQUESTS_TOTAL{5xx} +1。
    #[tokio::test]
    async fn sse_error_frame_records_requests_total_5xx() {
        let model = "sse_req_err";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_error_worker(endpoint.clone());
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let counter = prometheus::REQUESTS_TOTAL.with_label_values(&[model, "1", "5xx"]);
        let before = counter.get();
        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            false,
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");
        wait_for(|| counter.get() >= before + 1.0, "requests_total 5xx").await;
        drop(sse);
        assert_eq!(
            counter.get(),
            before + 1.0,
            "SSE Error frame must record exactly one 5xx request"
        );
    }

    /// S2/D1:客户端断开(event_rx 关闭 → event_tx.send Err)→ STREAM_CANCELLED_TOTAL +1,
    /// REQUESTS_TOTAL 仍 2xx(服务器确实处理了,由独立 cancel counter 区分)。
    #[tokio::test]
    async fn sse_client_disconnect_records_cancelled_2xx() {
        let model = "sse_req_cancel";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_stall_worker(endpoint.clone());
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let req = prometheus::REQUESTS_TOTAL.with_label_values(&[model, "1", "2xx"]);
        let canc = prometheus::STREAM_CANCELLED_TOTAL.with_label_values(&[model, "1", "sse"]);
        let req_before = req.get();
        let canc_before = canc.get();

        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            false,
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");
        // 立即 drop:forwarder 从 stall worker 收到 chunk 后 event_tx.send 失败。
        drop(sse);
        wait_for(|| canc.get() >= canc_before + 1.0, "cancelled").await;
        assert_eq!(
            req.get(),
            req_before + 1.0,
            "D1: disconnect keeps 2xx family + separate cancel counter"
        );
    }

    /// D7:SSE 早期拒绝(显式版本已解析但未就绪)→ REQUESTS_TOTAL{5xx} +1
    /// (handler 层)。无版本请求对未 ready 模型在 resolve 阶段即 404(无 active
    /// version)——5xx 语义对应"已解析未就绪",故走 version handler。
    #[tokio::test]
    async fn sse_rejects_not_ready_records_5xx() {
        let model = "sse_early_5xx";
        let state = make_state(Arc::new(CallbackRunner::new()));
        state
            .registry
            .register(model, "1", ModelConfig::default(), ModelType::LitAPI, std::path::PathBuf::new())
            .unwrap();
        // 未 mark_ready
        let counter = prometheus::REQUESTS_TOTAL.with_label_values(&[model, "1", "5xx"]);
        let before = counter.get();
        let resp = sse_infer_version_handler(
            State(state),
            Path((model.to_string(), "1".to_string())),
            HeaderMap::new(),
            test_cx(),
            ApiBody(json_body(json!({}))),
        )
        .await;
        assert_eq!(resp.into_response().status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            counter.get(),
            before + 1.0,
            "D7: early rejection must record 5xx"
        );
    }

    /// D7:模型不存在 + 无版本(resolve_version 失败)→ 4xx,version label 用
    /// 请求原值(无版本 → 空串)。
    #[tokio::test]
    async fn sse_rejects_missing_model_records_4xx() {
        let model = "sse_early_4xx";
        let state = make_state(Arc::new(CallbackRunner::new()));
        let counter = prometheus::REQUESTS_TOTAL.with_label_values(&[model, "", "4xx"]);
        let before = counter.get();
        let resp = sse_infer_handler(
            State(state.clone()),
            Path(model.to_string()),
            HeaderMap::new(),
            test_cx(),
            ApiBody(json_body(json!({}))),
        )
        .await;
        assert_eq!(resp.into_response().status(), axum::http::StatusCode::NOT_FOUND);
        assert_eq!(
            counter.get(),
            before + 1.0,
            "unresolved-version rejection must record 4xx with the request version label"
        );
    }

    /// D7:鉴权失败 → 4xx(handler 层)。
    #[tokio::test]
    async fn sse_rejects_unauthorized_records_4xx() {
        let model = "sse_early_auth";
        let state = make_state(Arc::new(CallbackRunner::new()));
        state
            .registry
            .register(model, "1", ModelConfig::default(), ModelType::LitAPI, std::path::PathBuf::new())
            .unwrap();
        // register 不携带 policies(独立字段)——用 set_policies 配置 auth。
        state.registry.set_policies(
            model,
            "1",
            Some(crate::config::ModelPolicies {
                auth: Some(crate::config::AuthPolicy {
                    header: "x-api-key".to_string(),
                    keys: vec!["sk-a".to_string()],
                }),
                ..Default::default()
            }),
        );
        state.registry.mark_ready(model, "1").unwrap();
        state.registry.activate_version(model, "1").unwrap();
        let counter = prometheus::REQUESTS_TOTAL.with_label_values(&[model, "1", "4xx"]);
        let before = counter.get();
        // 无 x-api-key 头 → Unauthorized。
        let resp = sse_infer_handler(
            State(state.clone()),
            Path(model.to_string()),
            HeaderMap::new(),
            test_cx(),
            ApiBody(json_body(json!({}))),
        )
        .await;
        assert_eq!(resp.into_response().status(), axum::http::StatusCode::UNAUTHORIZED);
        assert_eq!(counter.get(), before + 1.0, "auth rejection must record 4xx");
    }

    /// S8:SSE open 成功后 per-worker dispatch 计数 +1。
    #[tokio::test]
    async fn sse_open_records_worker_inference() {
        let model = "sse_wi";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_done_worker(endpoint.clone());
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let before = prometheus::worker_inference_count(model, "1", 0);
        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            false,
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        drop(sse);
        assert_eq!(
            prometheus::worker_inference_count(model, "1", 0),
            before + 1,
            "S8: pick-success must record per-worker dispatch (gRPC parity)"
        );
    }

    // ===== S4/S5/S6 (批次 3):流错误计数 + stream_kind label + duration/bytes =====

    /// S4/S5:SSE Error 帧 → STREAM_ERRORS_TOTAL{stream_kind=sse, kind=worker_error} +1。
    #[tokio::test]
    async fn sse_error_frame_records_stream_error_kind() {
        let model = "sse_err_kind";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_error_worker(endpoint.clone());
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let errs = prometheus::STREAM_ERRORS_TOTAL
            .with_label_values(&[model, "1", "sse", "worker_error"]);
        let before = errs.get();
        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            false,
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");
        wait_for(|| errs.get() >= before + 1.0, "sse errors kind=worker_error").await;
        drop(sse);
    }

    /// S4:SSE idle 回收 → STREAM_ERRORS_TOTAL{kind=idle} +1。
    #[tokio::test]
    async fn sse_idle_records_stream_error_idle() {
        let model = "sse_idle_kind";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_stall_worker(endpoint.clone());
        let state =
            ready_state_with_idle(model, endpoint, Arc::new(CallbackRunner::new()), 0.2).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let errs = prometheus::STREAM_ERRORS_TOTAL
            .with_label_values(&[model, "1", "sse", "idle"]);
        let before = errs.get();
        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            false,
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");
        // 不 drain:forwarder 收到 stall 后 idle(0.2s)回收 → kind=idle。
        wait_for(|| errs.get() >= before + 1.0, "sse errors kind=idle").await;
        drop(sse);
    }

    /// S6:SSE 正常完成 → STREAM_DURATION_SECONDS 有观测、bytes 累加
    /// (spawn_done_worker 的 chunk 是 b"{}" = 2 字节)。
    #[tokio::test]
    async fn sse_done_records_duration_and_output_bytes() {
        let model = "sse_s6";
        let endpoint = ipc_endpoint(model);
        let _w = spawn_done_worker(endpoint.clone());
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let dur = prometheus::STREAM_DURATION_SECONDS
            .with_label_values(&[model, "1", "sse"]);
        let bytes = prometheus::STREAM_OUTPUT_BYTES_TOTAL
            .with_label_values(&[model, "1", "sse"]);
        let dur_before = dur.get_sample_count();
        let bytes_before = bytes.get();
        let sse = sse_infer_impl(
            state,
            model.to_string(),
            "1".to_string(),
            HeaderMap::new(),
            json_body(json!({})),
            test_cx(),
            false,
            SseFrameStyle::Legacy,
        )
        .await
        .expect("sse must open");
        wait_for(|| dur.get_sample_count() > dur_before, "sse duration").await;
        drop(sse);
        assert_eq!(
            bytes.get(),
            bytes_before + 2.0,
            "S6: chunk bytes must accumulate (b\"{{}}\" = 2 bytes)"
        );
    }

    // ===== S1/S2 (批次 1):WS 请求级计数 =====

    /// S1:WS 正常完成 → REQUESTS_TOTAL{2xx} +1。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ws_done_records_requests_total_2xx() {
        let model = "ws_req_done";
        let endpoint = ipc_endpoint(model);
        let (_w, _actions) = spawn_recording_worker(endpoint.clone());
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        state.registry.activate_version(model, "1").unwrap();
        let base = spawn_ws_server(state).await;

        let counter = prometheus::REQUESTS_TOTAL.with_label_values(&[model, "1", "2xx"]);
        let before = counter.get();
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{}/{}/stream", base, model))
            .await
            .expect("WS connect failed");
        ws.send(tokio_tungstenite::tungstenite::Message::Text(r#"{"input": 1}"#.to_string()))
            .await
            .unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(r#"{"type":"close"}"#.to_string()))
            .await
            .unwrap();
        // 等 writer 处理 close → worker 回 Done → 收口记录 2xx。
        wait_for(|| counter.get() >= before + 1.0, "ws requests_total 2xx").await;
        let _ = ws.close(None).await;
    }

    /// S1/D7:WS 未知控制帧(协议违规)→ REQUESTS_TOTAL{4xx} +1。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ws_unknown_control_frame_records_4xx() {
        let model = "ws_req_proto";
        let endpoint = ipc_endpoint(model);
        let (_w, _actions) = spawn_recording_worker(endpoint.clone());
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        state.registry.activate_version(model, "1").unwrap();
        let base = spawn_ws_server(state).await;

        let counter = prometheus::REQUESTS_TOTAL.with_label_values(&[model, "1", "4xx"]);
        let before = counter.get();
        // S4:协议违规 → kind=protocol(stream_kind=ws)。before 在流开始前取
        // (errors 与 4xx 同一次收口原子记录)。
        let errs = prometheus::STREAM_ERRORS_TOTAL
            .with_label_values(&[model, "1", "ws", "protocol"]);
        let e_before = errs.get();
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{}/{}/stream", base, model))
            .await
            .expect("WS connect failed");
        ws.send(tokio_tungstenite::tungstenite::Message::Text(r#"{"input": 1}"#.to_string()))
            .await
            .unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(r#"{"type":"bogus"}"#.to_string()))
            .await
            .unwrap();
        wait_for(|| counter.get() >= before + 1.0, "ws requests_total 4xx").await;
        assert_eq!(errs.get(), e_before + 1.0, "S4: protocol violation must count kind=protocol");
        let _ = ws.close(None).await;
    }

    /// S2:WS 客户端硬断开(gone 信号)→ STREAM_CANCELLED_TOTAL +1。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ws_client_disconnect_records_cancelled() {
        let model = "ws_req_cancel";
        let endpoint = ipc_endpoint(model);
        let (_w, _actions) = spawn_recording_worker(endpoint.clone());
        let state = ready_state(model, endpoint, Arc::new(CallbackRunner::new())).await;
        state.registry.activate_version(model, "1").unwrap();
        let base = spawn_ws_server(state).await;

        let canc = prometheus::STREAM_CANCELLED_TOTAL.with_label_values(&[model, "1", "websocket"]);
        let before = canc.get();
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{}/{}/stream", base, model))
            .await
            .expect("WS connect failed");
        ws.send(tokio_tungstenite::tungstenite::Message::Text(r#"{"input": 1}"#.to_string()))
            .await
            .unwrap();
        // 直接断开:writer 的 gone_rx 收到 Some("") → break(cancel)。
        let _ = ws.close(None).await;
        wait_for(|| canc.get() >= before + 1.0, "ws cancelled").await;
    }

    /// D7:WS 握手后未就绪早退(显式版本已解析)→ 5xx。无版本请求对未 ready
    /// 模型在 resolve 阶段即失败(4xx)——5xx 语义对应"已解析未就绪",走
    /// version 路由。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ws_rejects_not_ready_records_5xx() {
        let model = "ws_early_5xx";
        let state = make_state(Arc::new(CallbackRunner::new()));
        state
            .registry
            .register(model, "1", ModelConfig::default(), ModelType::LitAPI, std::path::PathBuf::new())
            .unwrap();
        // 未 mark_ready
        let base = spawn_ws_server(state).await;

        let counter = prometheus::REQUESTS_TOTAL.with_label_values(&[model, "1", "5xx"]);
        let before = counter.get();
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{}/{}/1/stream", base, model))
            .await
            .expect("WS connect failed");
        // 服务器升级 101 后立即 close(未就绪早退)。
        let _ = ws.send(tokio_tungstenite::tungstenite::Message::Text(r#"{"input": 1}"#.to_string())).await;
        wait_for(|| counter.get() >= before + 1.0, "ws early 5xx").await;
        let _ = ws.close(None).await;
    }
}

