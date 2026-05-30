use crate::error::AppError;
use crate::http::state::AppState;
use crate::metrics::prometheus;
use crate::proto::liteserver as pb;
use crate::registry::types::ModelType;
use crate::streaming;
use axum::{
    extract::{Path, Query, State},
    response::{IntoResponse, Json, Response},
};
use axum::extract::ws::{Message, WebSocket};
use axum::http::header::HeaderMap;
use axum::response::sse::{Event, Sse};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn, Instrument};
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
    let models = state.registry.list_loaded();
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
        .expect("metrics response: builder should not fail with string body")
}

// ===== List Models =====

pub async fn list_models_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let models = state.registry.list_loaded();
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
    crate::validation::validate_identifier(&model_name)?;
    let versions = state.registry.list_versions(&model_name);
    let active = state.registry.get_active_version(&model_name);
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
    crate::validation::validate_identifier(&model_name)?;
    if let Some(ref v) = query.version {
        crate::validation::validate_identifier(v)?;
    }
    let ready = state.registry.is_ready(&model_name, query.version.as_deref());
    let active_version = state.registry.get_active_version(&model_name);
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
        let model_name = model_dir.file_name().unwrap_or_default().to_string_lossy().to_string();

        let mut versions = Vec::new();
        if let Ok(mut version_entries) = tokio::fs::read_dir(&model_dir).await {
            while let Ok(Some(ventry)) = version_entries.next_entry().await {
                let version_dir = ventry.path();
                if !version_dir.is_dir() {
                    continue;
                }
                let version = version_dir.file_name().unwrap_or_default().to_string_lossy().to_string();
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
                    "name": path.file_stem().unwrap_or_default().to_string_lossy().to_string(),
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
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_identifier(&version)?;

    // Load model config
    let config_path = state.repo_path.join(&model_name).join(&version).join("config.yaml");
    let config = crate::config::load_model_config(&config_path).unwrap_or_default();

    state.worker_manager.load_model(&model_name, &version, &config).await?;

    // Auto-activate if no active version
    let active = state.registry.get_active_version(&model_name);
    if active.is_none() {
        state.registry.activate_version(&model_name, &version)?;
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
    crate::validation::validate_identifier(&model_name)?;
    if let Some(ref v) = query.version {
        crate::validation::validate_identifier(v)?;
    }
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
    crate::validation::validate_identifier(&model_name)?;
    if let Some(ref v) = query.version {
        crate::validation::validate_identifier(v)?;
    }
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
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_identifier(&version)?;

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
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_identifier(&version)?;

    let success = state.registry.activate_version(&model_name, &version)?;
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
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    do_infer(state, model_name, None, "/predict".to_string(), headers, payload).await
}

pub async fn infer_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_identifier(&version)?;
    do_infer(state, model_name, Some(version), "/predict".to_string(), headers, payload).await
}

fn extract_client_ip(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .or_else(|| headers.get("x-real-ip").and_then(|v| v.to_str().ok()))
        .unwrap_or("")
        .to_string()
}

async fn do_infer(
    state: Arc<AppState>,
    model_name: String,
    version: Option<String>,
    route: String,
    headers: HeaderMap,
    payload: Value,
) -> Result<Json<Value>, AppError> {
    let request_id = Uuid::new_v4().to_string();
    let span = tracing::info_span!(
        "inference",
        model = %model_name,
        version = version.as_deref().unwrap_or("auto"),
        request_id = %request_id,
    );
    async move {
    let resolved_version = match &version {
        Some(v) => v.clone(),
        None => state.registry.get_active_version(&model_name)
            .ok_or_else(|| AppError::ModelNotFound(format!("{} has no active version", model_name)))?,
    };

    // Check ready
    if !state.registry.is_ready(&model_name, version.as_deref()) {
        return Err(AppError::ModelNotReady(format!(
            "{} version {} is not ready",
            model_name, resolved_version
        )));
    }

    // Get model version info
    let mv = state.registry.get(&model_name, Some(&resolved_version))
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
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos());

    let start = Instant::now();

    // Build request metadata
    let header_map: HashMap<String, String> = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
        .collect();
    let client_ip = extract_client_ip(&headers);
    let timestamp_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    let payload_bytes = serde_json::to_vec(&payload).unwrap_or_default();

    let meta = pb::RequestMeta {
        route,
        headers: header_map,
        client_ip,
        request_id,
        timestamp_ns,
        payload: payload_bytes,
    };

    // All requests go through the unified inference queue
    let (response_tx, response_rx) = oneshot::channel();
    let item = crate::inference_queue::QueueItem {
        uid: uid.clone(),
        data: bytes::Bytes::from(meta.payload.clone()),
        meta: Some(std::sync::Arc::new(meta)),
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
            error!(timeout_secs = %timeout_duration.as_secs(), "Response channel closed");
            prometheus::record_request_end(&model_name, &resolved_version, "5xx", start.elapsed().as_secs_f64()).await;
            return Err(AppError::InferenceTimeout("response channel closed".to_string()));
        }
        Err(_) => {
            error!(timeout_secs = %timeout_duration.as_secs(), elapsed_ms = %start.elapsed().as_millis(), "Inference request timed out");
            prometheus::record_request_end(&model_name, &resolved_version, "5xx", start.elapsed().as_secs_f64()).await;
            return Err(AppError::InferenceTimeout("request timeout".to_string()));
        }
    };

    let duration = start.elapsed().as_secs_f64();
    prometheus::record_worker_metrics(&model_name, response.metrics.as_ref());

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
                    prometheus::record_request_end(&model_name, &resolved_version, "2xx", duration).await;
                    Ok(Json(data))
                }
                "Error" => {
                    let msg = single.status.as_ref().and_then(|s| {
                        if s.message.is_empty() { None } else { Some(s.message.clone()) }
                    }).unwrap_or_else(|| "unknown worker error".to_string());
                    error!(worker_error = %msg, duration_ms = %(duration * 1000.0) as u64, "Worker returned error");
                    prometheus::record_request_end(&model_name, &resolved_version, "5xx", duration).await;
                    Err(AppError::WorkerCrashed(msg))
                }
                _ => {
                    prometheus::record_request_end(&model_name, &resolved_version, "2xx", duration).await;
                    Ok(Json(data))
                }
            }
        }
        _ => {
            prometheus::record_request_end(&model_name, &resolved_version, "5xx", duration).await;
            Err(AppError::WorkerCrashed("unexpected response type".to_string()))
        }
    }
    }.instrument(span).await
}

// ===== Streaming Helpers =====

async fn resolve_version(
    state: &AppState,
    model_name: &str,
    version: Option<String>,
) -> Result<String, AppError> {
    match version {
        Some(v) => Ok(v),
        None => state.registry.get_active_version(model_name).ok_or_else(|| {
            AppError::ModelNotFound(format!("{} has no active version", model_name))
        }),
    }
}

fn build_request_meta(headers: &HeaderMap, payload: &Value, route: &str) -> pb::RequestMeta {
    let header_map: HashMap<String, String> = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
        .collect();
    let client_ip = extract_client_ip(headers);
    let request_id = Uuid::new_v4().to_string();
    let timestamp_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    let payload_bytes = serde_json::to_vec(payload).unwrap_or_default();

    pb::RequestMeta {
        route: route.to_string(),
        headers: header_map,
        client_ip,
        request_id,
        timestamp_ns,
        payload: payload_bytes,
    }
}

async fn open_worker_stream(
    state: &Arc<AppState>,
    model_name: &str,
    resolved_version: &str,
    meta: pb::RequestMeta,
    payload_bytes: Vec<u8>,
) -> Result<(String, mpsc::Receiver<pb::StreamResponse>), AppError> {
    let mv = state
        .registry
        .get(model_name, Some(resolved_version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", model_name, resolved_version)))?;

    let num_workers = mv.workers.len();
    if num_workers == 0 {
        return Err(AppError::WorkerCrashed(format!("{} has no workers", model_name)));
    }

    let worker_id = crate::worker::pick_worker_random(num_workers);
    let clients = state
        .worker_manager
        .get_zmq_clients(model_name, resolved_version)
        .await
        .ok_or_else(|| AppError::WorkerCrashed(format!("{} {} has no ZMQ clients", model_name, resolved_version)))?;

    if worker_id >= clients.len() {
        return Err(AppError::WorkerCrashed("invalid worker index".to_string()));
    }

    let client = &clients[worker_id];
    let stream_id = format!("stream-{}", Uuid::new_v4());
    let open_req = streaming::build_stream_open(stream_id.clone(), payload_bytes, Some(meta));

    let chunk_rx = client.send_stream(open_req, stream_id.clone()).await?;
    Ok((stream_id, chunk_rx))
}

// ===== SSE Streaming =====

pub async fn sse_infer_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let resolved_version = resolve_version(&state, &model_name, None).await?;

    if !state.registry.is_ready(&model_name, None) {
        return Err(AppError::ModelNotReady(format!(
            "{} version {} is not ready",
            model_name, resolved_version
        )));
    }

    let meta = build_request_meta(&headers, &payload, "/predict");
    let payload_bytes = serde_json::to_vec(&payload).unwrap_or_default();
    let (stream_id, mut chunk_rx) = open_worker_stream(&state, &model_name, &resolved_version, meta, payload_bytes).await?;

    let stream_metrics = state.config.features.streaming_metrics;
    if stream_metrics {
        prometheus::record_stream_open(&model_name, &resolved_version, "sse");
    }

    let (event_tx, event_rx) = mpsc::channel(64);

    tokio::spawn(async move {
        let open_time = std::time::Instant::now();
        let mut first_chunk = true;
        let mut last_chunk_time = open_time;

        while let Some(chunk) = chunk_rx.recv().await {
            let event = match &chunk.payload {
                Some(pb::stream_response::Payload::Chunk(c)) => {
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
                    Event::default().data(data.to_string())
                }
                Some(pb::stream_response::Payload::Error(e)) => {
                    Event::default().data(json!({"error": e.message}).to_string())
                }
                Some(pb::stream_response::Payload::Done(_)) => {
                    Event::default().data("[DONE]")
                }
                _ => continue,
            };
            if event_tx.send(Ok(event)).await.is_err() {
                break;
            }
            if matches!(chunk.payload, Some(pb::stream_response::Payload::Done(_))) {
                break;
            }
        }
        if stream_metrics {
            prometheus::record_stream_close(&model_name, &resolved_version, "sse");
        }
        // Ensure stream is cleaned up on worker side
        let cancel_req = streaming::build_stream_cancel(stream_id);
        let _ = open_worker_stream_cancel(state, cancel_req).await;
    });

    Ok(Sse::new(ReceiverStream::new(event_rx)))
}

pub async fn sse_infer_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_identifier(&version)?;
    let resolved_version = resolve_version(&state, &model_name, Some(version)).await?;

    if !state.registry.is_ready(&model_name, Some(&resolved_version)) {
        return Err(AppError::ModelNotReady(format!(
            "{} version {} is not ready",
            model_name, resolved_version
        )));
    }

    let meta = build_request_meta(&headers, &payload, "/predict");
    let payload_bytes = serde_json::to_vec(&payload).unwrap_or_default();
    let (stream_id, mut chunk_rx) = open_worker_stream(&state, &model_name, &resolved_version, meta, payload_bytes).await?;

    let stream_metrics = state.config.features.streaming_metrics;
    if stream_metrics {
        prometheus::record_stream_open(&model_name, &resolved_version, "sse");
    }

    let (event_tx, event_rx) = mpsc::channel(64);

    tokio::spawn(async move {
        let open_time = std::time::Instant::now();
        let mut first_chunk = true;
        let mut last_chunk_time = open_time;

        while let Some(chunk) = chunk_rx.recv().await {
            let event = match &chunk.payload {
                Some(pb::stream_response::Payload::Chunk(c)) => {
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
                    Event::default().data(data.to_string())
                }
                Some(pb::stream_response::Payload::Error(e)) => {
                    Event::default().data(json!({"error": e.message}).to_string())
                }
                Some(pb::stream_response::Payload::Done(_)) => {
                    Event::default().data("[DONE]")
                }
                _ => continue,
            };
            if event_tx.send(Ok(event)).await.is_err() {
                break;
            }
            if matches!(chunk.payload, Some(pb::stream_response::Payload::Done(_))) {
                break;
            }
        }
        if stream_metrics {
            prometheus::record_stream_close(&model_name, &resolved_version, "sse");
        }
        let cancel_req = streaming::build_stream_cancel(stream_id);
        let _ = open_worker_stream_cancel(state, cancel_req).await;
    });

    Ok(Sse::new(ReceiverStream::new(event_rx)))
}

async fn open_worker_stream_cancel(state: Arc<AppState>, cancel_req: pb::Request) -> Result<(), AppError> {
    // Fire-and-forget cancel to worker; best-effort
    // We need a worker to send cancel to, but we don't know which one.
    // For now, skip explicit cancel - the worker handles client disconnect via stream timeout.
    let _ = (state, cancel_req);
    Ok(())
}

// ===== WebSocket Streaming =====

pub async fn ws_stream_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    ws: axum::extract::WebSocketUpgrade,
) -> Response {
    if let Err(e) = crate::validation::validate_identifier(&model_name) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, None, socket))
}

pub async fn ws_stream_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    ws: axum::extract::WebSocketUpgrade,
) -> Response {
    if let Err(e) = crate::validation::validate_identifier(&model_name) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    if let Err(e) = crate::validation::validate_identifier(&version) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, Some(version), socket))
}

async fn handle_ws_stream(
    state: Arc<AppState>,
    model_name: String,
    version: Option<String>,
    mut socket: WebSocket,
) {
    let resolved_version = match resolve_version(&state, &model_name, version).await {
        Ok(v) => v,
        Err(_) => {
            let _ = socket.close().await;
            return;
        }
    };

    if !state.registry.is_ready(&model_name, None) {
        let _ = socket.close().await;
        return;
    }

    // Wait for first message from client (the request payload)
    let first_msg = match socket.recv().await {
        Some(Ok(Message::Text(text))) => text,
        Some(Ok(Message::Binary(bin))) => String::from_utf8_lossy(&bin).to_string(),
        _ => {
            let _ = socket.close().await;
            return;
        }
    };

    let payload: Value = match serde_json::from_str(&first_msg) {
        Ok(v) => v,
        Err(_) => {
            let _ = socket.send(Message::Text(json!({"error": "invalid JSON"}).to_string())).await;
            let _ = socket.close().await;
            return;
        }
    };

    let headers = HeaderMap::new();
    let meta = build_request_meta(&headers, &payload, "/predict");
    let payload_bytes = serde_json::to_vec(&payload).unwrap_or_default();

    let (stream_id, mut chunk_rx) = match open_worker_stream(&state, &model_name, &resolved_version, meta, payload_bytes).await {
        Ok(r) => r,
        Err(e) => {
            let _ = socket.send(Message::Text(json!({"error": e.to_string()}).to_string())).await;
            let _ = socket.close().await;
            return;
        }
    };

    let stream_metrics = state.config.features.streaming_metrics;
    if stream_metrics {
        prometheus::record_stream_open(&model_name, &resolved_version, "websocket");
    }

    // Spawn task to forward worker chunks -> WebSocket
    let mut send_task = tokio::spawn(async move {
        let open_time = std::time::Instant::now();
        let mut first_chunk = true;
        let mut last_chunk_time = open_time;

        while let Some(chunk) = chunk_rx.recv().await {
            let msg = match &chunk.payload {
                Some(pb::stream_response::Payload::Chunk(c)) => {
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
                    Message::Binary(c.data.clone())
                }
                Some(pb::stream_response::Payload::Error(e)) => {
                    Message::Text(json!({"error": e.message}).to_string())
                }
                Some(pb::stream_response::Payload::Done(_)) => {
                    Message::Text(json!({"done": true}).to_string())
                }
                _ => continue,
            };
            if socket.send(msg).await.is_err() {
                break;
            }
            if matches!(chunk.payload, Some(pb::stream_response::Payload::Done(_))) {
                let _ = socket.close().await;
                break;
            }
        }
        if stream_metrics {
            prometheus::record_stream_close(&model_name, &resolved_version, "websocket");
        }
        stream_id
    });

    // For now, we don't handle bidirectional streaming over WebSocket
    // Just wait for the send task to complete
    let completed_stream_id = match send_task.await {
        Ok(sid) => sid,
        Err(_) => return,
    };

    // Send cancel to clean up
    let cancel_req = streaming::build_stream_cancel(completed_stream_id);
    let _ = open_worker_stream_cancel(state, cancel_req).await;
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

    let req = crate::worker::protocol::EndpointRequest {
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
    let snapshots = crate::metrics::aggregator::TIMELINE.all_snapshots().await;
    Json(json!({ "snapshots": snapshots }))
}

#[derive(Deserialize)]
pub struct TimelineQuery {
    version: Option<String>,
}

pub async fn timeline_model_handler(
    Path(model_name): Path<String>,
    Query(query): Query<TimelineQuery>,
) -> Result<Json<Value>, AppError> {
    let version = query.version.unwrap_or_else(|| "1".to_string());
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_identifier(&version)?;
    let entries = crate::metrics::aggregator::TIMELINE.get_timeline(&model_name, &version).await;
    Ok(Json(json!({
        "model": model_name,
        "version": version,
        "entries": entries,
    })))
}

// ===== Alerts =====

pub async fn alerts_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let alerts = state.alert_engine.evaluate(&crate::metrics::aggregator::TIMELINE).await;
    Json(json!({ "alerts": alerts }))
}

// ===== Version Compare =====

pub async fn compare_versions_handler(
    Path(model_name): Path<String>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    match crate::metrics::aggregator::VersionComparator::compare(
        &crate::metrics::aggregator::TIMELINE,
        &model_name,
    ).await {
        Some(comp) => Ok(Json(json!(comp))),
        None => Err(AppError::ModelNotFound(format!(
            "No timeline data for model {}",
            model_name
        ))),
    }
}
