use super::*;
use crate::error::AppError;
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
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tracing::error;
use tracing::Instrument;
use uuid::Uuid;

// ===== Inference =====

pub async fn infer_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiJson(payload): ApiJson<Value>,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let result = do_infer(
        state.clone(), model_name.clone(), None,
        "/predict".to_string(), headers, payload, cx,
    ).await;
    Ok(attach_cors_headers(&state, &model_name, result))
}

pub async fn infer_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    cx: RequestContext,
    ApiJson(payload): ApiJson<Value>,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    let result = do_infer(
        state.clone(), model_name.clone(), Some(version),
        "/predict".to_string(), headers, payload, cx,
    ).await;
    Ok(attach_cors_headers(&state, &model_name, result))
}
async fn do_infer(
    state: Arc<AppState>,
    model_name: String,
    version: Option<String>,
    route: String,
    headers: HeaderMap,
    payload: Value,
    cx: RequestContext,
) -> Result<Response, AppError> {
    let request_id = cx.request_id.clone();
    let span = tracing::info_span!(
        "inference",
        model = %model_name,
        version = version.as_deref().unwrap_or("auto"),
        request_id = %request_id,
    );
    async move {
    let resolved_version = resolve_version(&state, &model_name, version, &headers).await?;

    // Check ready
    if !state.registry.is_ready(&model_name, Some(&resolved_version)) {
        return Err(AppError::ModelNotReady(format!(
            "{} version {} is not ready",
            model_name, resolved_version
        )));
    }

    // Get model version info
    let mv = state.registry.get(&model_name, Some(&resolved_version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", model_name, resolved_version)))?;

    // Policy checks (before ensemble, after mv resolution): auth first, then rate limit
    enforce_auth(mv.policies.auth.as_ref(), &headers)?;
    enforce_rate_limit(&state, mv.policies.rate_limit.as_ref(), &model_name, &cx.client_ip).await?;

    // Handle ensemble
    if mv.model_type == ModelType::Ensemble {
        let result = crate::ensemble::execute_ensemble(state, &model_name, &resolved_version, payload, &request_id, &cx.client_ip).await?;
        return Ok(Json(result).into_response());
    }

    // Pick worker info (needed for both paths)
    let num_workers = mv.workers.len();
    if num_workers == 0 {
        return Err(AppError::WorkerCrashed(format!("{} has no workers", model_name)));
    }

    let uid = format!("{}_{}-{}-{}", model_name, resolved_version, Uuid::new_v4(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos());

    let start = Instant::now();

    // Build request metadata
    let header_map: HashMap<String, String> = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
        .collect();
    let client_ip = cx.client_ip.clone();
    let timestamp_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    let payload_bytes = serde_json::to_vec(&payload).unwrap_or_default();

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
    let cb_runner = state.callback_runner.clone();
    let req_ctx_clone = req_ctx.clone();
    tokio::spawn(async move {
        cb_runner.on_inference_request(&req_ctx_clone).await;
    });

    let meta = pb::RequestMeta {
        route,
        headers: header_map,
        client_ip,
        request_id,
        timestamp_ns,
        payload: bytes::Bytes::from(payload_bytes),
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
    };

    match state.inference_queue.try_submit(&model_name, &resolved_version, item) {
        Ok(()) => {}
        Err(crate::inference_queue::QueueError::Full) => {
            return Err(AppError::QueueFull(format!(
                "Queue full for {} {}", model_name, resolved_version
            )));
        }
        Err(_) => {
            return Err(AppError::ModelNotReady(format!(
                "Queue not available for {} {}", model_name, resolved_version
            )));
        }
    }

    let timeout_duration = Duration::from_secs_f64(state.config.server.timeout as f64);
    let response = match tokio::time::timeout(timeout_duration, response_rx).await {
        Ok(Ok(resp)) => resp,
        Ok(Err(_)) => {
            error!(timeout_secs = %timeout_duration.as_secs(), "Response channel closed");
            prometheus::record_request_end(&model_name, &resolved_version, "5xx", start.elapsed().as_secs_f64());
            return Err(AppError::InferenceTimeout("response channel closed".to_string()));
        }
        Err(_) => {
            error!(timeout_secs = %timeout_duration.as_secs(), elapsed_ms = %start.elapsed().as_millis(), "Inference request timed out");
            prometheus::record_request_end(&model_name, &resolved_version, "5xx", start.elapsed().as_secs_f64());
            return Err(AppError::InferenceTimeout("request timeout".to_string()));
        }
    };

    let duration = start.elapsed().as_secs_f64();
    prometheus::record_worker_metrics(&model_name, &resolved_version, response.metrics.as_ref());

    // Parse protobuf response
    match response.payload {
        Some(pb::response::Payload::Single(single)) => {
            let data = if single.data.is_empty() {
                json!({})
            } else {
                serde_json::from_slice(&single.data).unwrap_or(json!({}))
            };
            let code = single.status.as_ref().map(|s| s.code.as_str()).unwrap_or("Ok");
            match code {
                "Ok" => {
                    prometheus::record_request_end(&model_name, &resolved_version, status_family(single.status_code), duration);
                    // Fire InferenceResponse callback
                    let resp_ctx = crate::callback::InferenceContext {
                        elapsed_us: Some((duration * 1_000_000.0) as u64),
                        ..req_ctx.clone()
                    };
                    let cb_runner = state.callback_runner.clone();
                    tokio::spawn(async move { cb_runner.on_inference_response(&resp_ctx).await; });
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
                "Error" => {
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

// ===== Streaming Helpers =====

pub(super) async fn resolve_version(
    state: &AppState,
    model_name: &str,
    version: Option<String>,
    headers: &HeaderMap,
) -> Result<String, AppError> {
    // Precedence (§4.3): explicit version > x-lite-version header pin >
    // weighted routing pick > active version.
    let resolved = match version {
        Some(v) => v,
        None => {
            if let Some(pin) = headers
                .get("x-lite-version")
                .and_then(|v| v.to_str().ok())
                .filter(|s| !s.is_empty())
            {
                // Same guard as versioned URL paths — an invalid pin is
                // rejected (400), not silently honored or fallen back.
                crate::validation::validate_version(pin)?;
                pin.to_string()
            } else if let Some(picked) = state.registry.routing_pick(model_name) {
                picked
            } else {
                state.registry.get_active_version(model_name).ok_or_else(|| {
                    AppError::ModelNotFound(format!("{} has no active version", model_name))
                })?
            }
        }
    };
    // LRU touch (§4.2): coarse, no-op for unknown versions.
    state.registry.touch_last_used(model_name, &resolved);
    Ok(resolved)
}

pub(super) fn build_request_meta(headers: &HeaderMap, payload: &Value, route: &str, cx: &RequestContext) -> pb::RequestMeta {
    let header_map: HashMap<String, String> = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
        .collect();
    let timestamp_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    let payload_bytes = bytes::Bytes::from(serde_json::to_vec(payload).unwrap_or_default());

    pb::RequestMeta {
        route: route.to_string(),
        headers: header_map,
        client_ip: cx.client_ip.clone(),
        request_id: cx.request_id.clone(),
        timestamp_ns,
        payload: payload_bytes,
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
        }
    }

    #[test]
    fn test_build_request_meta_payload_matches_serialized() {
        // build_request_meta already serializes payload into meta.payload.
        // This test proves that meta.payload is identical to direct serialization,
        // confirming that streaming handlers don't need a separate serde_json::to_vec call.
        let headers = HeaderMap::new();
        let payload = serde_json::json!({"prompt": "hello", "max_tokens": 100});
        let direct_bytes = bytes::Bytes::from(serde_json::to_vec(&payload).unwrap_or_default());
        let cx = test_cx("test-id-001", "");

        let meta = build_request_meta(&headers, &payload, "/predict", &cx);

        assert_eq!(meta.payload, direct_bytes,
            "meta.payload should equal direct serde_json::to_vec output");
    }

    #[test]
    fn test_build_request_meta_reads_context() {
        // P-MW: request_id / client_ip come from the RequestContext filled
        // once by context_middleware — never re-extracted from headers here.
        let headers = HeaderMap::new();
        let payload = serde_json::json!({"x": 1});
        let cx = test_cx("test-id-002", "10.0.0.7");
        let meta = build_request_meta(&headers, &payload, "/custom", &cx);

        assert_eq!(meta.route, "/custom");
        assert_eq!(meta.client_ip, "10.0.0.7");
        assert_eq!(meta.request_id, "test-id-002");
    }
}
