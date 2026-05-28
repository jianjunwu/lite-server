use crate::config::{Config, ModelConfig, OrchestrationConfig, ModelStrategyConfig};
use crate::error::AppError;
use crate::http::state::AppState;
use crate::metrics::prometheus;
use crate::registry::types::{VersionStatus, ModelType};
use crate::transport::uds::send_to_worker;
use crate::worker::protocol::{EndpointRequest, InferenceRequest, InferenceResponse, RequestPayload, ResponseStatus, ServerSnapshot};
use crate::worker::endpoint_manager::EndpointManager;
use crate::worker::pick_worker_random;
use axum::{
    extract::{Path, Query, State, WebSocketUpgrade},
    response::{IntoResponse, Json, Response},
};
use axum::extract::ws::{Message, WebSocket};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct VersionQuery {
    version: Option<String>,
}

// ===== Health =====

pub async fn health_handler() -> impl IntoResponse {
    "ok"
}

// ===== Info =====

pub async fn info_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let models = state.registry.list_loaded().await;
    let loaded: Vec<String> = models.iter().map(|(n, v, _)| format!("{}/{}", n, v)).collect();
    Json(json!({
        "server": "lite-server",
        "version": env!("CARGO_PKG_VERSION"),
        "loaded_models": loaded,
    }))
}

// ===== Metrics =====

pub async fn metrics_handler() -> impl IntoResponse {
    let body = prometheus::gather_metrics();
    Response::builder()
        .header("content-type", "text/plain; charset=utf-8")
        .body(body)
        .unwrap()
}

// ===== List Models =====

pub async fn list_models_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let models = state.registry.list_loaded().await;
    let models_json: Vec<Value> = models
        .into_iter()
        .map(|(name, version, mv)| {
            json!({
                "name": name,
                "version": version,
                "status": format!("{:?}", mv.status),
                "model_type": format!("{:?}", mv.model_type),
                "workers": mv.workers.len(),
            })
        })
        .collect();
    Json(json!({"models": models_json}))
}

// ===== List Versions =====

pub async fn list_versions_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
) -> Result<Json<Value>, AppError> {
    let versions = state.registry.list_versions(&model_name).await;
    let active = state.registry.get_active_version(&model_name).await;
    Ok(Json(json!({
        "name": model_name,
        "active_version": active,
        "versions": versions,
    })))
}

// ===== Model Ready =====

pub async fn model_ready_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    Query(query): Query<VersionQuery>,
) -> Result<Json<Value>, AppError> {
    let ready = state.registry.is_ready(&model_name, query.version.as_deref()).await;
    let active_version = state.registry.get_active_version(&model_name).await;
    Ok(Json(json!({
        "name": model_name,
        "version": query.version.or(active_version.clone()),
        "ready": ready,
        "active_version": active_version,
    })))
}

// ===== Repository Index =====

pub async fn repository_index_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let models = scan_repository(&state.repo_path).await;
    Json(json!({"models": models}))
}

async fn scan_repository(repo_path: &std::path::Path) -> Vec<Value> {
    let mut models = Vec::new();
    let mut entries = match tokio::fs::read_dir(repo_path).await {
        Ok(e) => e,
        Err(_) => return models,
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let model_dir = entry.path();
        if !model_dir.is_dir() {
            continue;
        }
        let model_name = model_dir.file_name().unwrap().to_string_lossy().to_string();

        let mut versions = Vec::new();
        if let Ok(mut version_entries) = tokio::fs::read_dir(&model_dir).await {
            while let Ok(Some(ventry)) = version_entries.next_entry().await {
                let version_dir = ventry.path();
                if !version_dir.is_dir() {
                    continue;
                }
                let version = version_dir.file_name().unwrap().to_string_lossy().to_string();
                let model_py = version_dir.join("model.py");
                let config_yaml = version_dir.join("config.yaml");

                let mut is_ensemble = false;
                if config_yaml.exists() {
                    if let Ok(content) = tokio::fs::read_to_string(&config_yaml).await {
                        is_ensemble = content.contains("ensemble:");
                    }
                }

                if model_py.exists() || is_ensemble {
                    versions.push(json!({
                        "name": model_name.clone(),
                        "version": version,
                        "path": version_dir.to_string_lossy().to_string(),
                        "has_config": config_yaml.exists(),
                        "type": if is_ensemble { "ensemble" } else { "litapi" },
                    }));
                }
            }
        }

        models.extend(versions);
    }

    // Scan .lma artifacts
    if let Ok(mut entries) = tokio::fs::read_dir(repo_path).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().map(|e| e == "lma").unwrap_or(false) {
                // Simplified: just list the artifact file
                models.push(json!({
                    "name": path.file_stem().unwrap().to_string_lossy().to_string(),
                    "version": "1",
                    "path": path.to_string_lossy().to_string(),
                    "has_config": false,
                    "type": "artifact",
                    "artifact_source": path.to_string_lossy().to_string(),
                }));
            }
        }
    }

    models
}

// ===== Load Model =====

pub async fn load_model_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    Query(query): Query<VersionQuery>,
) -> Result<Json<Value>, AppError> {
    let version = query.version.unwrap_or_else(|| "1".to_string());

    // Load model config
    let config_path = state.repo_path.join(&model_name).join(&version).join("config.yaml");
    let config = crate::config::load_model_config(&config_path).unwrap_or_default();

    state.worker_manager.load_model(&model_name, &version, &config).await?;

    // Auto-activate if no active version
    let active = state.registry.get_active_version(&model_name).await;
    if active.is_none() {
        state.registry.activate_version(&model_name, &version).await?;
    }

    Ok(Json(json!({
        "success": true,
        "message": format!("Model {} version {} loaded", model_name, version),
    })))
}

// ===== Unload Model =====

pub async fn unload_model_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    Query(query): Query<VersionQuery>,
) -> Result<Json<Value>, AppError> {
    let success = state.worker_manager.unload_model(&model_name, query.version.as_deref()).await?;
    if !success {
        return Err(AppError::ModelNotFound(format!("{} not loaded", model_name)));
    }
    Ok(Json(json!({
        "success": true,
        "message": format!("Model {} unloaded", model_name),
    })))
}

// ===== Reload Model =====

pub async fn reload_model_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    Query(query): Query<VersionQuery>,
) -> Result<Json<Value>, AppError> {
    let success = state.worker_manager.reload_model(&model_name, query.version.as_deref()).await?;
    if !success {
        return Err(AppError::ModelNotFound(format!("{} not loaded", model_name)));
    }
    Ok(Json(json!({
        "success": true,
        "message": format!("Model {} reloaded", model_name),
    })))
}

// ===== Delete Version =====

pub async fn delete_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    // Unload first if loaded
    let _ = state.worker_manager.unload_model(&model_name, Some(&version)).await;

    // Delete directory
    let version_dir = state.repo_path.join(&model_name).join(&version);
    if version_dir.exists() {
        tokio::fs::remove_dir_all(&version_dir)
            .await
            .map_err(|e| AppError::Io(e))?;
    }

    Ok(Json(json!({
        "success": true,
        "message": format!("Model {} version {} deleted", model_name, version),
    })))
}

// ===== Activate Version =====

pub async fn activate_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let success = state.registry.activate_version(&model_name, &version).await?;
    if !success {
        return Err(AppError::ModelNotReady(format!(
            "Model {} version {} is not ready",
            model_name, version
        )));
    }
    prometheus::record_version_switch(&model_name);
    Ok(Json(json!({
        "success": true,
        "message": format!("Model {} version {} is now active", model_name, version),
        "active_version": version,
    })))
}

// ===== Inference =====

pub async fn infer_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, AppError> {
    do_infer(state, model_name, None, payload).await
}

pub async fn infer_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, AppError> {
    do_infer(state, model_name, Some(version), payload).await
}

async fn do_infer(
    state: Arc<AppState>,
    model_name: String,
    version: Option<String>,
    payload: Value,
) -> Result<Json<Value>, AppError> {
    let resolved_version = match &version {
        Some(v) => v.clone(),
        None => state.registry.get_active_version(&model_name).await
            .ok_or_else(|| AppError::ModelNotFound(format!("{} has no active version", model_name)))?,
    };

    // Check ready
    if !state.registry.is_ready(&model_name, version.as_deref()).await {
        return Err(AppError::ModelNotReady(format!(
            "{} version {} is not ready",
            model_name, resolved_version
        )));
    }

    // Get model version info
    let mv = state.registry.get(&model_name, Some(&resolved_version)).await
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", model_name, resolved_version)))?;

    // Handle ensemble
    if mv.model_type == ModelType::Ensemble {
        let result = crate::ensemble::execute_ensemble(state, &model_name, &resolved_version, payload).await?;
        return Ok(Json(result));
    }

    // Pick worker info (needed for both paths)
    let num_workers = mv.workers.len();
    if num_workers == 0 {
        return Err(AppError::WorkerCrashed(format!("{} has no workers", model_name)));
    }

    let uid = format!("{}_{}-{}-{}", model_name, resolved_version, Uuid::new_v4(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());

    let start = Instant::now();

    // Fast path: no batching, direct UDS (avoids queue overhead)
    if mv.config.max_batch_size <= 1 {
        let worker_id = pick_worker_random(num_workers);
        let worker = &mv.workers[worker_id];
        let uds_path = worker.uds_path.clone();

        let request = InferenceRequest {
            uid: uid.clone(),
            payload: RequestPayload::Infer { data: payload },
        };

        let response = match send_to_worker(&uds_path, request).await {
            Ok(resp) => resp,
            Err(e) => {
                prometheus::record_request_end(&model_name, &resolved_version, "5xx", start.elapsed().as_secs_f64());
                return Err(e);
            }
        };

        let duration = start.elapsed().as_secs_f64();
        prometheus::record_worker_metrics(&model_name, &response.metrics);
        return match response.status.code.as_str() {
            "Ok" => {
                prometheus::record_request_end(&model_name, &resolved_version, "2xx", duration);
                Ok(Json(response.data.unwrap_or(json!({}))))
            }
            "Error" => {
                let msg = response.status.message.unwrap_or_else(|| "unknown worker error".to_string());
                prometheus::record_request_end(&model_name, &resolved_version, "5xx", duration);
                Err(AppError::WorkerCrashed(msg))
            }
            _ => {
                prometheus::record_request_end(&model_name, &resolved_version, "2xx", duration);
                Ok(Json(response.data.unwrap_or(json!({}))))
            }
        };
    }

    // Queue path: batching enabled
    let (response_tx, response_rx) = oneshot::channel();
    let item = crate::inference_queue::QueueItem {
        uid: uid.clone(),
        data: payload,
        response_tx,
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
            prometheus::record_request_end(&model_name, &resolved_version, "5xx", start.elapsed().as_secs_f64());
            return Err(AppError::InferenceTimeout("response channel closed".to_string()));
        }
        Err(_) => {
            prometheus::record_request_end(&model_name, &resolved_version, "5xx", start.elapsed().as_secs_f64());
            return Err(AppError::InferenceTimeout("request timeout".to_string()));
        }
    };

    let duration = start.elapsed().as_secs_f64();
    prometheus::record_worker_metrics(&model_name, &response.metrics);
    match response.status.code.as_str() {
        "Ok" => {
            prometheus::record_request_end(&model_name, &resolved_version, "2xx", duration);
            Ok(Json(response.data.unwrap_or(json!({}))))
        }
        "Error" => {
            let msg = response.status.message.unwrap_or_else(|| "unknown worker error".to_string());
            prometheus::record_request_end(&model_name, &resolved_version, "5xx", duration);
            Err(AppError::WorkerCrashed(msg))
        }
        _ => {
            prometheus::record_request_end(&model_name, &resolved_version, "2xx", duration);
            Ok(Json(response.data.unwrap_or(json!({}))))
        }
    }
}

// ===== WebSocket Streaming =====

pub async fn ws_stream_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, None, socket))
}

pub async fn ws_stream_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, Some(version), socket))
}

async fn handle_ws_stream(
    state: Arc<AppState>,
    model_name: String,
    version: Option<String>,
    mut socket: WebSocket,
) {
    let resolved_version = match &version {
        Some(v) => v.clone(),
        None => match state.registry.get_active_version(&model_name).await {
            Some(v) => v,
            None => {
                let _ = socket.close().await;
                return;
            }
        },
    };

    if !state.registry.is_ready(&model_name, version.as_deref()).await {
        let _ = socket.close().await;
        return;
    }

    let mv = match state.registry.get(&model_name, Some(&resolved_version)).await {
        Some(mv) => mv,
        None => {
            let _ = socket.close().await;
            return;
        }
    };

    let num_workers = mv.workers.len();
    if num_workers == 0 {
        let _ = socket.close().await;
        return;
    }

    let worker_id = pick_worker_random(num_workers);
    let worker = &mv.workers[worker_id];
    let uds_path = worker.uds_path.clone();

    let stream_id = format!("ws-{}", Uuid::new_v4());

    // Send STREAM_OPEN
    let open_req = InferenceRequest {
        uid: format!("{}_{}-stream-open", model_name, resolved_version),
        payload: RequestPayload::StreamOpen { stream_id: stream_id.clone() },
    };

    if let Err(e) = send_to_worker(&uds_path, open_req).await {
        error!("Stream open failed: {}", e);
        let _ = socket.close().await;
        return;
    }

    // Read from WebSocket and forward to worker
    while let Some(msg) = socket.recv().await {
        match msg {
            Ok(Message::Text(text)) => {
                let chunk: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let req = InferenceRequest {
                    uid: format!("{}_{}-stream-{}", model_name, resolved_version, Uuid::new_v4()),
                    payload: RequestPayload::StreamChunk { stream_id: stream_id.clone(), chunk },
                };
                if let Err(e) = send_to_worker(&uds_path, req).await {
                    error!("Stream chunk failed: {}", e);
                    break;
                }
            }
            Ok(Message::Close(_)) => break,
            Ok(Message::Binary(bin)) => {
                // Forward binary data as JSON
                let chunk = json!({"__binary__": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bin)});
                let req = InferenceRequest {
                    uid: format!("{}_{}-stream-{}", model_name, resolved_version, Uuid::new_v4()),
                    payload: RequestPayload::StreamChunk { stream_id: stream_id.clone(), chunk },
                };
                if let Err(e) = send_to_worker(&uds_path, req).await {
                    error!("Stream chunk failed: {}", e);
                    break;
                }
            }
            Err(_) => break,
            _ => {}
        }
    }

    // Send STREAM_CLOSE
    let close_req = InferenceRequest {
        uid: format!("{}_{}-stream-close", model_name, resolved_version),
        payload: RequestPayload::StreamClose { stream_id: stream_id.clone() },
    };
    let _ = send_to_worker(&uds_path, close_req).await;
    let _ = socket.close().await;
}

// ===== Custom Endpoint Handler =====

pub async fn custom_endpoint_handler(
    State(state): State<Arc<AppState>>,
    request: axum::http::Request<axum::body::Body>,
) -> Result<Response, AppError> {
    let ep_mgr = match &state.endpoint_manager {
        Some(mgr) => mgr,
        None => return Err(AppError::Internal("endpoint manager not available".to_string())),
    };

    let route = request.uri().path().to_string();
    let method = request.method().to_string();

    // Extract query params
    let query: HashMap<String, String> = request
        .uri()
        .query()
        .map(|q| {
            q.split('&')
                .filter_map(|pair| {
                    let mut parts = pair.splitn(2, '=');
                    let key = parts.next()?.to_string();
                    let val = parts.next().map(|v| v.to_string()).unwrap_or_default();
                    Some((key, val))
                })
                .collect()
        })
        .unwrap_or_default();

    // Extract headers
    let mut headers = HashMap::new();
    for (key, value) in request.headers() {
        if let Ok(v) = value.to_str() {
            headers.insert(key.to_string(), v.to_string());
        }
    }

    // Read body
    let body_bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .map_err(|e| AppError::Transport(format!("read body: {}", e)))?;
    let body = if body_bytes.is_empty() {
        None
    } else {
        Some(serde_json::from_slice(&body_bytes).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(&body_bytes).to_string())
        }))
    };

    let snapshot = ep_mgr.build_snapshot().await;

    let req = EndpointRequest {
        request_id: Uuid::new_v4().to_string(),
        route,
        method,
        headers,
        query,
        body,
        server_state: snapshot,
    };

    let response = ep_mgr.send_request(req).await?;

    let mut builder = Response::builder().status(response.status_code);
    if let Some(hdrs) = response.headers {
        for (k, v) in hdrs {
            builder = builder.header(k, v);
        }
    }
    let resp = builder
        .body(axum::body::Body::from(response.body.to_string()))
        .map_err(|e| AppError::Internal(format!("build response: {}", e)))?;

    Ok(resp)
}

// ===== Timeline =====

pub async fn timeline_handler() -> impl IntoResponse {
    let snapshots = crate::metrics::aggregator::TIMELINE.all_snapshots();
    Json(json!({ "snapshots": snapshots }))
}

#[derive(Deserialize)]
pub struct TimelineQuery {
    version: Option<String>,
}

pub async fn timeline_model_handler(
    Path(model_name): Path<String>,
    Query(query): Query<TimelineQuery>,
) -> impl IntoResponse {
    let version = query.version.unwrap_or_else(|| "1".to_string());
    let entries = crate::metrics::aggregator::TIMELINE.get_timeline(&model_name, &version);
    Json(json!({
        "model": model_name,
        "version": version,
        "entries": entries,
    }))
}

// ===== Alerts =====

pub async fn alerts_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let alerts = state.alert_engine.evaluate(&crate::metrics::aggregator::TIMELINE);
    Json(json!({ "alerts": alerts }))
}

// ===== Version Compare =====

pub async fn compare_versions_handler(
    Path(model_name): Path<String>,
) -> Result<Json<Value>, AppError> {
    match crate::metrics::aggregator::VersionComparator::compare(
        &crate::metrics::aggregator::TIMELINE,
        &model_name,
    ) {
        Some(comp) => Ok(Json(json!(comp))),
        None => Err(AppError::ModelNotFound(format!(
            "No timeline data for model {}",
            model_name
        ))),
    }
}
