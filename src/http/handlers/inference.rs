use super::*;
use crate::error::{AppError, ProtocolError};
use crate::http::state::AppState;
use crate::metrics::prometheus;
use crate::proto::liteserver as pb;
use crate::registry::types::ModelType;
use crate::request_context::RequestContext;
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, Json, Response},
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::oneshot;
use tracing::error;
use tracing::Instrument;
use uuid::Uuid;

// ===== Inference =====

/// Shared RequestBody → EnsembleValue translation for the ensemble dispatch
/// (unary + SSE/OpenAI/decoupled endpoints — thin wire adaptation, the DAG
/// orchestration itself stays in ensemble.rs).
pub(crate) fn ensemble_input_from_body(
    body: &RequestBody,
) -> Result<crate::ensemble::EnsembleValue, AppError> {
    match body {
        RequestBody::Json(b) => {
            let v = serde_json::from_slice::<Value>(b).map_err(|e| {
                AppError::InvalidRequestBody(format!("ensemble requires valid JSON input: {e}"))
            })?;
            Ok(crate::ensemble::EnsembleValue::Json(v))
        }
        // B3 (E6): binary root input — Raw bytes go straight to the first layer.
        RequestBody::Raw(bytes, ct) => {
            Ok(crate::ensemble::EnsembleValue::Binary(bytes.clone(), ct.clone(), None, None))
        }
        // MIMO (D31/D32): transport de-framing only — the TritonBinary
        // container splits into the KServe JSON head + binary tail; all
        // envelope VALIDATION stays single-point in parse_root_inputs (D39).
        // Undeclared ensembles keep their historical 400 there.
        RequestBody::TritonBinary { body, json_head_len } => {
            let head: Value = serde_json::from_slice(&body[..*json_head_len]).map_err(|e| {
                AppError::InvalidRequestBody(format!(
                    "ensemble requires valid JSON in the Triton head: {e}"
                ))
            })?;
            Ok(crate::ensemble::EnsembleValue::Envelope {
                head,
                tail: body.slice(*json_head_len..),
            })
        }
    }
}

pub async fn infer_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiBody(body): ApiBody,
) -> Result<Response, ProtocolError> {
    // P-CORS: CORS headers are attached by `cors_middleware` (no longer per-handler).
    run_infer(
        state, model_name, None,
        "/predict".to_string(), headers, body, cx,
    ).await
}

pub async fn infer_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiBody(body): ApiBody,
) -> Result<Response, ProtocolError> {
    run_infer(
        state, model_name, Some(version),
        "/predict".to_string(), headers, body, cx,
    ).await
}

// ===== Triton Generate extension(阶段 4,批次 4,D9) =====

/// D9:/generate = /infer 的 JSON 别名(unary 单响应,复用 run_infer)。
/// 端点本身即信号,无请求级 opt-in;无 feature gate(J3:unary 即 infer
/// 别名)。请求侧复用 ApiBody(含 TritonBinary——generate + 多 tensor
/// 二进制组合裁定允许,透传哲学,worker 语义归 worker)。
pub async fn generate_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiBody(body): ApiBody,
) -> Result<Response, ProtocolError> {
    run_infer(
        state, model_name, None,
        "/predict".to_string(), headers, body, cx,
    ).await
}

pub async fn generate_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiBody(body): ApiBody,
) -> Result<Response, ProtocolError> {
    run_infer(
        state, model_name, Some(version),
        "/predict".to_string(), headers, body, cx,
    ).await
}

/// 协议感知的 infer 入口(D11 P2.1):T1(预筛)+ T2(信封主判)→ 语义协议,
/// 错误挂 [`ProtocolError`](crate::error::ProtocolError) 边界渲染。
/// 阶段 2 的 `binary_data_output` 响应转换在此边界后处理。
pub async fn run_infer(
    state: Arc<AppState>,
    model_name: String,
    version: Option<String>,
    route: String,
    headers: HeaderMap,
    body: RequestBody,
    cx: RequestContext,
) -> Result<Response, ProtocolError> {
    // 早期错误(validate,发生在 T2 判定前)按 T1 预筛值渲染(C9)。
    crate::validation::validate_identifier(&model_name)
        .map_err(|e| ProtocolError {
            error: e,
            protocol: cx.api_protocol.unwrap_or(crate::protocol::ApiProtocol::Legacy),
        })?;
    if let Some(ref v) = version {
        crate::validation::validate_version(v).map_err(|e| ProtocolError {
            error: e,
            protocol: cx.api_protocol.unwrap_or(crate::protocol::ApiProtocol::Legacy),
        })?;
    }
    // T2 信封主判:仅 T1 缺失时计算(T1 强信号短路,二进制/Raw 路径零成本)。
    let protocol = crate::protocol::detect::resolve(cx.api_protocol, &body.bytes());
    // 阶段 2(D5/D6):KServe-mode 请求带 binary_data_output flag → 响应
    // 转换开关。默认关(非 KServe 请求零成本);双条件防御防自有 schema 撞名。
    let binary_output = if protocol == crate::protocol::ApiProtocol::Kserve {
        crate::http::kserve::parse_binary_output_request(&body, &model_name, version.clone())
    } else {
        None
    };
    let resp = do_infer(state, model_name, version, route, headers, body, cx)
        .await
        .map_err(|error| ProtocolError { error, protocol })?;
    if let Some(req) = binary_output {
        return crate::http::kserve::convert_response(resp, &req)
            .await
            .map_err(|error| ProtocolError { error, protocol });
    }
    Ok(resp)
}
async fn do_infer(
    state: Arc<AppState>,
    model_name: String,
    version: Option<String>,
    route: String,
    headers: HeaderMap,
    body: RequestBody,
    cx: RequestContext,
) -> Result<Response, AppError> {
    let request_id = cx.request_id.clone();
    let span = tracing::info_span!(
        "inference",
        model = %model_name,
        version = version.as_deref().unwrap_or("auto"),
        request_id = %request_id,
        // P5-2: recorded by the caller below (resolve_version returns the pin).
        pinned_version = tracing::field::Empty,
        // D11: body metadata — recorded once body is available.
        body_bytes = tracing::field::Empty,
        body_kind = tracing::field::Empty,
    );
    async move {
    // A1 (round2): pre-dispatch rejections (404/503/401/429/400) must count
    // toward requests_total — streaming parity (S1b). Post-dispatch failures
    // record at their own sites below; this closure covers only exits before
    // a successful queue submit. Version label: the resolved version, or the
    // requested one ("" when absent) if resolution itself failed.
    let handler_start = Instant::now();
    let record_reject = |version: &str, e: &AppError| {
        prometheus::record_request_end(
            &model_name,
            version,
            status_family(e.http_status().as_u16() as i32),
            handler_start.elapsed().as_secs_f64(),
        );
    };
    // F-03: gate unknown-model probes — when any model on the server
    // declares policies.auth, a resolution failure must not be
    // distinguishable from "no credentials" (see auth_gate_for_unknown_model).
    let reject_version = version.clone().unwrap_or_default();
    let (resolved_version, pinned) = match resolve_version(&state, &model_name, version, &headers).await {
        Ok(v) => v,
        Err(e) => {
            let e = auth_gate_for_unknown_model(&state, &headers, e);
            record_reject(&reject_version, &e);
            return Err(e);
        }
    };
    // P5-2: record the honored pin on the inference span (Span::current() is
    // this span — the block is instrumented; same pattern as the body fields
    // below). resolve_version itself is side-effect-free.
    if let Some(p) = &pinned {
        tracing::Span::current().record("pinned_version", p.as_str());
    }
    // P-DEADLINE (§4.0.10): resolved once — shared by the ensemble cascade and
    // the unary worker wait. Client `x-lite-timeout` else server.timeout.
    let deadline = crate::deadline::resolve_from_http(&headers, state.config.server.timeout);

    // D11: record body size with content-type label (before any processing).
    prometheus::record_request_body_bytes(body.kind(), &route, body.bytes().len());
    // D11: enrich inference span with body metadata.
    tracing::Span::current().record("body_bytes", body.bytes().len() as i64);
    tracing::Span::current().record("body_kind", body.kind());

    // F-03: policy checks BEFORE the readiness check — an unauthenticated
    // probe must get 401 whether the model is missing, not ready, or fine
    // (auth first, then rate limit).
    let Some(mv) = state.registry.get(&model_name, Some(&resolved_version)) else {
        let e = auth_gate_for_unknown_model(
            &state,
            &headers,
            AppError::ModelNotFound(format!("{} version {}", model_name, resolved_version)),
        );
        record_reject(&resolved_version, &e);
        return Err(e);
    };
    if let Err(e) = enforce_auth(mv.policies.auth.as_ref(), &headers) {
        record_reject(&resolved_version, &e);
        return Err(e);
    }
    if let Err(e) = enforce_rate_limit(&state, mv.policies.rate_limit.as_ref(), &model_name, &cx.client_ip).await {
        record_reject(&resolved_version, &e);
        return Err(e);
    }

    // Check ready
    if !state.registry.is_ready(&model_name, Some(&resolved_version)) {
        let e = AppError::ModelNotReady(format!(
            "{} version {} is not ready",
            model_name, resolved_version
        ));
        record_reject(&resolved_version, &e);
        return Err(e);
    }

    // Handle ensemble
    if mv.model_type == ModelType::Ensemble {
        let ensemble_input = match ensemble_input_from_body(&body) {
            Ok(v) => v,
            Err(e) => {
                record_reject(&resolved_version, &e);
                return Err(e);
            }
        };
        // D37 (batch 0): signature converged — execution-face opts; later
        // batches add fields (dag_selector) without touching call sites.
        // E8-1 (D38): the dag selector rides the HTTP request header.
        let dag_selector = match crate::ensemble::dag_selector_from_http(&headers) {
            Ok(v) => v,
            Err(e) => {
                record_reject(&resolved_version, &e);
                return Err(e);
            }
        };
        let opts = crate::ensemble::EnsembleExecOpts {
            client_ip: cx.client_ip.clone(),
            deadline_unix_ns: deadline.unix_ns,
            decoupled: false,
            dag_selector,
        };
        let result = match crate::ensemble::execute_ensemble(
            state, &model_name, &resolved_version, ensemble_input, &request_id, opts,
        ).await {
            Ok(v) => v,
            Err(e) => {
                record_reject(&resolved_version, &e);
                return Err(e);
            }
        };
        // B3 (E6) egress: Json → historical Json(response).into_response();
        // Binary → body bytes + content-type header (mirror unary passthrough,
        // inference.rs:266-283).
        return ensemble_outcome_response(&model_name, &resolved_version, handler_start, result);
    }

    // Pick worker info (needed for both paths)
    let num_workers = mv.workers.len();
    if num_workers == 0 {
        let e = AppError::WorkerCrashed(format!("{} has no workers", model_name));
        record_reject(&resolved_version, &e);
        return Err(e);
    }

    let uid = format!("{}_{}-{}-{}", model_name, resolved_version, Uuid::new_v4(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos());

    let start = Instant::now();

    // Build request metadata
    let mut header_map: HashMap<String, String> = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
        .collect();
    // P-TRACE: inject the active inference span's trace context into the worker
    // RequestMeta.headers (overwrites client traceparent → worker is a child).
    crate::telemetry::inject(&mut header_map);
    let client_ip = cx.client_ip.clone();
    let timestamp_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    let payload_bytes = body.bytes();

    // P8-1: cross-request sequence_id affinity hint (optional, unauthenticated).
    let sequence_id = headers
        .get("x-sequence-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    // P8-1 (B3): envelope hints — debug 记录；消费点在队列（priority/affinity_key/direct_worker_id）。
    let hints = crate::request_context::RequestHints::from_http(&headers);
    if !hints.is_empty() {
        tracing::debug!(?hints, "envelope hints received");
    }

    // Fire InferenceRequest callback (before values are moved into meta)
    let req_ctx = crate::callback::InferenceContext {
        model_name: model_name.clone(),
        version: resolved_version.clone(),
        route: route.clone(),
        protocol: crate::callback::Protocol::Http,
        request_id: request_id.clone(),
        client_ip: client_ip.clone(),
        elapsed_us: None,
    };
    crate::callback::fire_inference_request(&state.callback_runner, &req_ctx);

    let meta = pb::RequestMeta {
        route,
        headers: header_map,
        client_ip,
        request_id,
        timestamp_ns,
        payload: payload_bytes,
        sequence_id,
        deadline_unix_ns: deadline.unix_ns,
        ..Default::default()
    };

    // All requests go through the unified inference queue
    let (response_tx, response_rx) = oneshot::channel();

    let item = crate::inference_queue::QueueItem {
        uid: uid.clone(),
        data: meta.payload.clone(),
        meta: Some(std::sync::Arc::new(meta)),
        response_tx,
        inflight_guard: None,
        enqueued_at: std::time::Instant::now(),
        is_warmup: false,
    };

    match state.inference_queue.try_submit(&model_name, &resolved_version, item) {
        Ok(()) => {}
        Err(crate::inference_queue::QueueError::Full) => {
            let e = AppError::QueueFull(format!(
                "Queue full for {} {}", model_name, resolved_version
            ));
            record_reject(&resolved_version, &e);
            return Err(e);
        }
        Err(crate::inference_queue::QueueError::InvalidWorker(msg)) => {
            // B3 direct-mode: x-lite-worker-id 不存在/已剔除 → 400（客户端错误）。
            let e = AppError::Validation(msg);
            record_reject(&resolved_version, &e);
            return Err(e);
        }
        Err(crate::inference_queue::QueueError::NoLiveWorkers(msg)) => {
            // Crash-death gate: every worker process has exited → 503
            // fail-fast（而非挂到 request_timeout）。
            let e = AppError::ModelNotReady(msg);
            record_reject(&resolved_version, &e);
            return Err(e);
        }
        Err(_) => {
            let e = AppError::ModelNotReady(format!(
                "Queue not available for {} {}", model_name, resolved_version
            ));
            record_reject(&resolved_version, &e);
            return Err(e);
        }
    }

    // P-DEADLINE (§4.0.10): bound the worker wait by the resolved deadline;
    // no deadline (no client spec AND server.timeout<=0) → unbounded.
    let response = match crate::deadline::remaining(deadline.unix_ns) {
        Some(timeout_duration) => match tokio::time::timeout(timeout_duration, response_rx).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(_)) => {
                // F-18: response channel closed (worker crash/recycle) is
                // "upstream unavailable" — 502, matching gRPC Unavailable
                // (was 504 InferenceTimeout, which mislabels an upstream
                // death as a timeout).
                error!(timeout_secs = %timeout_duration.as_secs(), "Response channel closed");
                prometheus::record_request_end(&model_name, &resolved_version, "5xx", start.elapsed().as_secs_f64());
                return Err(AppError::BadGateway("response channel closed".to_string()));
            }
            Err(_) => {
                error!(timeout_secs = %timeout_duration.as_secs(), elapsed_ms = %start.elapsed().as_millis(), "Inference request timed out");
                prometheus::record_request_end(&model_name, &resolved_version, "5xx", start.elapsed().as_secs_f64());
                return Err(AppError::InferenceTimeout("request timeout".to_string()));
            }
        },
        None => match response_rx.await {
            Ok(resp) => resp,
            Err(_) => {
                error!("Response channel closed");
                prometheus::record_request_end(&model_name, &resolved_version, "5xx", start.elapsed().as_secs_f64());
                return Err(AppError::BadGateway("response channel closed".to_string()));
            }
        },
    };

    let duration = start.elapsed().as_secs_f64();
    prometheus::record_worker_metrics(&model_name, &resolved_version, response.metrics.as_ref());

    // Parse protobuf response
    match response.payload {
        Some(pb::response::Payload::Single(single)) => {
            let code = single.status.as_ref().map(|s| s.code.as_str()).unwrap_or("Ok");
            match code {
                "Ok" => {
                    prometheus::record_request_end(&model_name, &resolved_version, status_family(single.status_code), duration);
                    // Fire InferenceResponse callback
                    crate::callback::fire_inference_response(&state.callback_runner, &req_ctx, start);
                    // P1 (unary binary passthrough): a non-JSON media_type
                    // declares the payload opaque bytes — forward `data`
                    // verbatim (no parse, no re-encode). The JSON path
                    // (media_type empty or application/json) keeps the
                    // parse/re-encode, byte-identical to before.
                    let passthrough = !single.media_type.is_empty()
                        && !single.media_type.starts_with("application/json");
                    let (body, content_type) = if passthrough {
                        (axum::body::Body::from(single.data), single.media_type)
                    } else if single.data.is_empty() {
                        (
                            axum::body::Body::from(bytes::Bytes::from_static(b"{}")),
                            "application/json; charset=utf-8".to_string(),
                        )
                    } else {
                        // F-02: the JSON path (declared application/json, or
                        // legacy empty media_type) must carry VALID JSON —
                        // parse strictly and re-encode (byte-identical guard:
                        // sorted-key re-encode is pinned). A corrupt payload
                        // is an upstream failure (502), never a fabricated
                        // `{}` (was: unwrap_or(json!({}))).
                        match serde_json::from_slice::<serde_json::Value>(&single.data) {
                            Ok(data) => {
                                let json_body = serde_json::to_string(&data).unwrap_or_default();
                                (
                                    axum::body::Body::from(json_body),
                                    "application/json; charset=utf-8".to_string(),
                                )
                            }
                            Err(e) => {
                                tracing::error!(
                                    model = %model_name,
                                    version = %resolved_version,
                                    error = %e,
                                    first_bytes = ?&single.data[..single.data.len().min(32)],
                                    "worker returned invalid JSON on the JSON path"
                                );
                                return Err(AppError::BadGateway(format!(
                                    "{model_name} version {resolved_version} returned invalid JSON"
                                )));
                            }
                        }
                    };
                    let mut builder = Response::builder()
                        .header("content-type", content_type);
                    if single.status_code > 0 {
                        use axum::http::StatusCode;
                        if let Ok(sc) = StatusCode::from_u16(single.status_code as u16) {
                            builder = builder.status(sc);
                        }
                    }
                    let builder = inject_response_headers(builder, &single.headers);
                    builder
                        .body(body)
                        .map_err(|e| AppError::Internal(format!("build response: {}", e)))
                }
                "Error" => {
                    // Only the error arm needs the parsed JSON value (to
                    // extract the structured error body); the Ok arm forwards
                    // `data` verbatim when media_type declares opaque bytes.
                    let data = if single.data.is_empty() {
                        json!({})
                    } else {
                        serde_json::from_slice(&single.data).unwrap_or(json!({}))
                    };
                    let msg = single.status.as_ref().and_then(|s| {
                        if s.message.is_empty() { None } else { Some(s.message.clone()) }
                    }).unwrap_or_else(|| "unknown worker error".to_string());

                    // If Status.message parses as a u16 HTTP status code, the
                    // worker is signalling a model-level HTTPException with a
                    // structured error body in `data`. Return it to the client
                    // without sanitization.
                    if let Ok(http_status) = msg.parse::<u16>() {
                        let err_obj = data.get("error");
                        let error_type = err_obj
                            .and_then(|e| e.get("type"))
                            .and_then(|t| t.as_str())
                            .unwrap_or("model_error")
                            .to_string();
                        let error_message = err_obj
                            .and_then(|e| e.get("message"))
                            .and_then(|m| m.as_str())
                            .unwrap_or("model error")
                            .to_string();
                        let error_code = err_obj
                            .and_then(|e| e.get("code"))
                            .and_then(|c| c.as_str())
                            .map(|s| s.to_string());
                        let error_param = err_obj
                            .and_then(|e| e.get("param"))
                            .and_then(|p| p.as_str())
                            .map(|s| s.to_string());
                        let status_family = match http_status / 100 {
                            4 => "4xx",
                            _ => "5xx",
                        };
                        prometheus::record_request_end(&model_name, &resolved_version, status_family, duration);
                        return Err(AppError::ModelError(Box::new(
                            crate::error::ModelErrorData {
                                status_code: http_status,
                                error_type,
                                detail: error_message,
                                code: error_code,
                                param: error_param,
                                headers: if single.headers.is_empty() {
                                    None
                                } else {
                                    Some(single.headers.clone())
                                },
                            },
                        )));
                    }

                    // Not a numeric status code — internal worker error, sanitize.
                    error!(worker_error = %msg, duration_ms = %(duration * 1000.0) as u64, "Worker returned error");
                    prometheus::record_request_end(&model_name, &resolved_version, "5xx", duration);
                    Err(AppError::WorkerCrashed(msg))
                }
                _ => {
                    prometheus::record_request_end(&model_name, &resolved_version, status_family(single.status_code), duration);
                    let data = if single.data.is_empty() {
                        json!({})
                    } else {
                        serde_json::from_slice(&single.data).unwrap_or(json!({}))
                    };
                    let json_body = serde_json::to_string(&data).unwrap_or_default();
                    let content_type = if single.media_type.is_empty() {
                        "application/json; charset=utf-8"
                    } else {
                        &single.media_type
                    };
                    let mut builder = Response::builder()
                        .header("content-type", content_type);
                    if single.status_code > 0 {
                        use axum::http::StatusCode;
                        if let Ok(sc) = StatusCode::from_u16(single.status_code as u16) {
                            builder = builder.status(sc);
                        }
                    }
                    let builder = inject_response_headers(builder, &single.headers);
                    builder
                        .body(axum::body::Body::from(json_body))
                        .map_err(|e| AppError::Internal(format!("build response: {}", e)))
                }
            }
        }
        _ => {
            prometheus::record_request_end(&model_name, &resolved_version, "5xx", duration);
            Err(AppError::WorkerCrashed("unexpected response type".to_string()))
        }
    }
    }.instrument(span).await
}

/// B7 (round2): ensemble unary egress — every exit records the top-level
/// request exactly once (success 2xx; streaming-DAG-on-unary contract
/// violation 4xx; response-build failure 5xx). Sub-model steps are not HTTP
/// requests and are not counted: one request = one count, matching the
/// non-ensemble unary path.
fn ensemble_outcome_response(
    model_name: &str,
    resolved_version: &str,
    handler_start: Instant,
    result: crate::ensemble::EnsembleOutcome,
) -> Result<Response, AppError> {
    let finish = |resp: Result<Response, AppError>| {
        let family = match &resp {
            Ok(_) => "2xx",
            Err(e) => status_family(e.http_status().as_u16() as i32),
        };
        prometheus::record_request_end(
            model_name,
            resolved_version,
            family,
            handler_start.elapsed().as_secs_f64(),
        );
        resp
    };
    match result {
        crate::ensemble::EnsembleOutcome::Unary(crate::ensemble::EnsembleValue::Json(v)) => {
            finish(Ok(Json(v).into_response()))
        }
        // P2 (batch 6): a raw-resident whole output emits its ORIGINAL
        // bytes — zero copy, zero re-serialize at the response boundary.
        crate::ensemble::EnsembleOutcome::Unary(crate::ensemble::EnsembleValue::RawJson(raw)) => {
            finish(
                Response::builder()
                    .status(axum::http::StatusCode::OK)
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(raw.bytes.clone()))
                    .map_err(|e| AppError::Internal(format!("build response: {e}"))),
            )
        }
        crate::ensemble::EnsembleOutcome::Unary(crate::ensemble::EnsembleValue::Binary(data, ct, ..)) => {
            // Same builder-error handling as the unary passthrough
            // (inference.rs:291-302): a worker-supplied media_type that is
            // not a valid header value must become a 500, never a panic.
            finish(
                Response::builder()
                    .status(axum::http::StatusCode::OK)
                    .header(axum::http::header::CONTENT_TYPE, &ct)
                    .body(axum::body::Body::from(data))
                    .map_err(|e| AppError::Internal(format!("build response: {e}"))),
            )
        }
        crate::ensemble::EnsembleOutcome::Stream(_) => {
            // D1: a unary endpoint calling a streaming DAG is a client
            // contract violation — 400 (aggregating chunks would fake
            // unary semantics and hide backpressure/memory risk).
            finish(Err(AppError::InvalidRequestBody(
                "DAG contains a streaming step; use a streaming endpoint".to_string(),
            )))
        }
        // E7 (D31): the multi-sink response — JSON head + binary tail
        // with the Inference-Header-Content-Length split header
        // (kserve.rs convention). A tail-less envelope degrades to
        // plain JSON (build_response never emits one with an empty
        // tail — the guard is defensive).
        crate::ensemble::EnsembleOutcome::Unary(crate::ensemble::EnsembleValue::Envelope { head, tail }) => {
            if tail.is_empty() {
                return finish(Ok(Json(head).into_response()));
            }
            let head_bytes = match serde_json::to_vec(&head) {
                Ok(b) => b,
                Err(e) => return finish(Err(AppError::Internal(format!("serialize envelope head: {e}")))),
            };
            let mut body = head_bytes.clone();
            body.extend_from_slice(&tail);
            finish(
                Response::builder()
                    .status(axum::http::StatusCode::OK)
                    .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
                    .header("inference-header-content-length", head_bytes.len())
                    .body(axum::body::Body::from(body))
                    .map_err(|e| AppError::Internal(format!("build response: {e}"))),
            )
        }
    }
}

// ===== Streaming Helpers =====

pub(super) async fn resolve_version(
    state: &AppState,
    model_name: &str,
    version: Option<String>,
    headers: &HeaderMap,
) -> Result<(String, Option<String>), AppError> {
    // Precedence (§4.3/§4.4): explicit version > x-lite-version header pin
    // (features.canary_override=on, P5-2) > weighted routing pick > active
    // version. Returns (resolved, honored pin) — callers record the pin on
    // their inference span, which for SSE/WS/h2 bidi is created AFTER this
    // resolves (Span::current() here is the handler span; gRPC bidi pattern,
    // grpc/rpc/bidi.rs).
    let pin = headers
        .get("x-lite-version")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty());
    // P5-2 (蓝图 §4.4, D16): 开关关 → pin 完全不参与解析（debug 日志，
    // 连非法值也不校验），与 gRPC canary_pin 行为一致。
    let pin = match pin {
        Some(p) if !state.config.features.canary_override => {
            tracing::debug!(
                model = %model_name,
                pinned_version = %p,
                "x-lite-version pin ignored (features.canary_override=false)"
            );
            None
        }
        other => other,
    };
    let (resolved, honored_pin) = match version {
        Some(v) => (v, None),
        None => {
            if let Some(pin) = pin {
                // Same guard as versioned URL paths — an invalid pin is
                // rejected (400), not silently honored or fallen back.
                crate::validation::validate_version(pin)?;
                // P5-2 (蓝图 §4.4): pin 版本不存在 → 404（区别于未就绪的 503）。
                if state.registry.get(model_name, Some(pin)).is_none() {
                    return Err(AppError::ModelNotFound(format!(
                        "{} version {} not found",
                        model_name, pin
                    )));
                }
                let v = pin.to_string();
                (v, Some(pin.to_string()))
            } else if let Some(picked) = state.registry.routing_pick(model_name) {
                (picked, None)
            } else {
                (
                    state.registry.get_active_version(model_name).ok_or_else(|| {
                        AppError::ModelNotFound(format!("{} has no active version", model_name))
                    })?,
                    None,
                )
            }
        }
    };
    // LRU touch (§4.2): coarse, no-op for unknown versions.
    state.registry.touch_last_used(model_name, &resolved);
    Ok((resolved, honored_pin))
}

pub(super) fn build_request_meta(headers: &HeaderMap, payload_bytes: Bytes, route: &str, cx: &RequestContext, deadline_unix_ns: Option<i64>) -> pb::RequestMeta {
    let mut header_map: HashMap<String, String> = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
        .collect();
    // P-TRACE: inject the active span's trace context (overwrites any client
    // traceparent → worker is a child of the server span; D8 Rust-only).
    crate::telemetry::inject(&mut header_map);
    let timestamp_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    // P8-1: sequence_id affinity hint.
    let sequence_id = headers
        .get("x-sequence-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    pb::RequestMeta {
        route: route.to_string(),
        headers: header_map,
        client_ip: cx.client_ip.clone(),
        request_id: cx.request_id.clone(),
        timestamp_ns,
        payload: payload_bytes,
        sequence_id,
        deadline_unix_ns,
        ..Default::default()
    }
}
#[cfg(test)]
mod streaming_tests {
    use super::*;
    use axum::http::HeaderMap;

    fn test_cx(request_id: &str, client_ip: &str) -> RequestContext {
        RequestContext {
            request_id: request_id.to_string(),
            client_ip: client_ip.to_string(),
            trace_cx: opentelemetry::Context::new(),
            protocol: crate::callback::Protocol::Http,
            principal: None,
            api_protocol: None,
        }
    }

    #[test]
    fn test_build_request_meta_payload_passthrough() {
        // build_request_meta takes raw Bytes and passes them through
        // into meta.payload byte-identical (no JSON round-trip).
        let headers = HeaderMap::new();
        let payload_bytes = bytes::Bytes::from_static(b"{\"prompt\": \"hello\", \"max_tokens\": 100}");
        let cx = test_cx("test-id-001", "");

        let meta = build_request_meta(&headers, payload_bytes.clone(), "/predict", &cx, None);

        assert_eq!(meta.payload, payload_bytes,
            "meta.payload must preserve the input bytes byte-identical");
    }

    #[test]
    fn test_build_request_meta_reads_context() {
        // P-MW: request_id / client_ip come from the RequestContext filled
        // once by context_middleware — never re-extracted from headers here.
        let headers = HeaderMap::new();
        let payload_bytes = bytes::Bytes::from_static(b"{\"x\": 1}");
        let cx = test_cx("test-id-002", "10.0.0.7");
        let meta = build_request_meta(&headers, payload_bytes, "/custom", &cx, None);

        assert_eq!(meta.route, "/custom");
        assert_eq!(meta.client_ip, "10.0.0.7");
        assert_eq!(meta.request_id, "test-id-002");
    }
}

/// Round2 A1: unary pre-dispatch rejections must count toward
/// `requests_total` (streaming parity, S1b) — 404/503/401/429 exits were
/// silent before.
#[cfg(test)]
mod rejection_metrics_tests {
    use super::*;
    use crate::config::Config;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    fn test_cx() -> RequestContext {
        RequestContext {
            request_id: "a1-test".to_string(),
            client_ip: String::new(),
            trace_cx: opentelemetry::Context::new(),
            protocol: crate::callback::Protocol::Http,
            principal: None,
            api_protocol: None,
        }
    }

    fn test_state() -> Arc<AppState> {
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(crate::callback::CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            std::path::PathBuf::new(),
            inference_queue.clone(),
            "warn".to_string(),
            callback_runner.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            worker_manager,
            inference_queue,
            Config::default(),
            std::path::PathBuf::new(),
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    fn requests_total(model: &str, version: &str, family: &str) -> f64 {
        prometheus::REQUESTS_TOTAL
            .with_label_values(&[model, version, family])
            .get()
    }

    fn json_body() -> RequestBody {
        RequestBody::Json(bytes::Bytes::from_static(b"{\"x\": 1}"))
    }

    /// Unknown model → 404 (resolution failure) must record requests_total.
    #[tokio::test]
    async fn unary_unknown_model_records_4xx() {
        let _ = prometheus::register_metrics();
        let state = test_state();
        let before = requests_total("a1_unknown", "", "4xx");
        let result = do_infer(
            state,
            "a1_unknown".to_string(),
            None,
            "/predict".to_string(),
            HeaderMap::new(),
            json_body(),
            test_cx(),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(requests_total("a1_unknown", "", "4xx"), before + 1.0);
    }

    /// Registered but not ready → 503 must record requests_total under the
    /// resolved version label.
    #[tokio::test]
    async fn unary_model_not_ready_records_5xx() {
        let _ = prometheus::register_metrics();
        let state = test_state();
        state
            .registry
            .register(
                "a1_notready",
                "1",
                Default::default(),
                ModelType::LitAPI,
                std::path::PathBuf::new(),
            )
            .unwrap();
        let before = requests_total("a1_notready", "1", "5xx");
        let result = do_infer(
            state,
            "a1_notready".to_string(),
            Some("1".to_string()),
            "/predict".to_string(),
            HeaderMap::new(),
            json_body(),
            test_cx(),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(requests_total("a1_notready", "1", "5xx"), before + 1.0);
    }

    /// Auth declared but no credential header → 401 must record requests_total.
    #[tokio::test]
    async fn unary_auth_rejection_records_4xx() {
        let _ = prometheus::register_metrics();
        let state = test_state();
        state
            .registry
            .register(
                "a1_auth",
                "1",
                Default::default(),
                ModelType::LitAPI,
                std::path::PathBuf::new(),
            )
            .unwrap();
        state
            .registry
            .set_policies(
                "a1_auth",
                "1",
                Some(crate::config::ModelPolicies {
                    auth: Some(crate::config::AuthPolicy {
                        header: "x-api-key".to_string(),
                        keys: vec![],
                    }),
                    ..Default::default()
                }),
            );
        state.registry.mark_ready("a1_auth", "1").unwrap();
        let before = requests_total("a1_auth", "1", "4xx");
        let result = do_infer(
            state,
            "a1_auth".to_string(),
            Some("1".to_string()),
            "/predict".to_string(),
            HeaderMap::new(),
            json_body(),
            test_cx(),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(requests_total("a1_auth", "1", "4xx"), before + 1.0);
    }

    // ===== Round2 B7: ensemble unary success counts requests_total =====

    /// Ensemble unary success (Json egress) must record the top-level
    /// request as 2xx — sub-model steps are not counted (one request = one
    /// count, same as the non-ensemble unary path).
    #[test]
    fn ensemble_unary_json_success_records_2xx() {
        let _ = prometheus::register_metrics();
        let before = requests_total("b7_ens", "1", "2xx");
        let outcome = crate::ensemble::EnsembleOutcome::Unary(
            crate::ensemble::EnsembleValue::Json(json!({"ok": true})),
        );
        let resp = ensemble_outcome_response("b7_ens", "1", Instant::now(), outcome);
        assert!(resp.is_ok());
        assert_eq!(requests_total("b7_ens", "1", "2xx"), before + 1.0);
    }

    /// Envelope egress with a non-empty tail is also a 2xx success.
    #[test]
    fn ensemble_unary_envelope_success_records_2xx() {
        let _ = prometheus::register_metrics();
        let before = requests_total("b7_ens_env", "1", "2xx");
        let outcome = crate::ensemble::EnsembleOutcome::Unary(
            crate::ensemble::EnsembleValue::Envelope {
                head: json!({"h": 1}),
                tail: bytes::Bytes::from_static(b"\x00\x01"),
            },
        );
        let resp = ensemble_outcome_response("b7_ens_env", "1", Instant::now(), outcome);
        assert!(resp.is_ok());
        assert_eq!(requests_total("b7_ens_env", "1", "2xx"), before + 1.0);
    }
}
