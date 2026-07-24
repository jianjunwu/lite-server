use crate::error::AppError;
use crate::http::state::AppState;
use crate::http::RequestId;
use crate::metrics::prometheus;
use crate::proto::liteserver as pb;
use crate::registry::types::ModelType;
use crate::streaming;
use axum::{
    extract::{Multipart, Path, Query, State},
    response::{IntoResponse, Json, Response},
};
use axum::extract::ws::{Message, WebSocket};

/// Map an HTTP status code to its Prometheus label family.
///
/// ``Status.code`` in the worker protocol describes *execution* state
/// (Ok = pipeline completed normally, Error = exception), while
/// ``SingleResponse.status_code`` carries the HTTP status.  This
/// function bridges the two so early 4xx / 5xx responses are recorded
/// under the correct Prometheus label rather than hardcoded "2xx".
fn status_family(status_code: i32) -> &'static str {
    match status_code / 100 {
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "2xx", // 0 (unset) counts as success
    }
}

/// JSON body extractor that converts axum's plain-text `JsonRejection`
/// into a standardized `AppError::InvalidRequestBody` response.
pub struct ApiJson<T>(pub T);

#[axum::async_trait]
impl<S, T> axum::extract::FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    Json<T>: axum::extract::FromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
{
    type Rejection = AppError;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(value)) => Ok(ApiJson(value)),
            Err(rejection) => Err(AppError::InvalidRequestBody(rejection.body_text())),
        }
    }
}

/// Query extractor that converts axum's plain-text `QueryRejection`
/// into a standardized `AppError::InvalidQueryParam` response.
pub struct ApiQuery<T>(pub T);

#[axum::async_trait]
impl<S, T> axum::extract::FromRequestParts<S> for ApiQuery<T>
where
    S: Send + Sync,
    Query<T>: axum::extract::FromRequestParts<S, Rejection = axum::extract::rejection::QueryRejection>,
{
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(value)) => Ok(ApiQuery(value)),
            Err(rejection) => Err(AppError::InvalidQueryParam(rejection.body_text())),
        }
    }
}
use axum::http::header::{HeaderMap, CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::response::sse::{Event, Sse};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
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

// ===== OPTIONS preflight for inference routes =====

pub async fn inference_options_handler(
    State(state): State<Arc<AppState>>,
    Path(params): Path<HashMap<String, String>>,
) -> Response {
    use axum::http::StatusCode;
    // The handler is registered on both 1-param (`/v2/models/:model_name/infer`)
    // and 2-param (`/v2/models/:model_name/versions/:version/infer`) routes.
    // Path<String> only accepts exactly one captured param, so the versioned
    // routes returned 500. HashMap captures all params regardless of count.
    let Some(model_name) = params.get("model_name") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match state.registry.active_cors_headers(model_name) {
        Some(headers) => {
            (StatusCode::NO_CONTENT, (*headers).clone()).into_response()
        }
        None => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

// ===== OPTIONS preflight for custom endpoint routes =====

/// Answer OPTIONS preflight for a custom endpoint with a RUNTIME Cors policy
/// lookup (not a startup snapshot), so endpoint restarts that change a Cors
/// declaration take effect without re-registering routes. Returns 405 when the
/// route has no Cors policy.
pub async fn endpoint_options_handler(
    State(state): State<Arc<AppState>>,
    matched_path: Option<axum::extract::MatchedPath>,
) -> Response {
    use axum::http::StatusCode;
    let pattern = matched_path
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_default();
    if let Some(mgr) = &state.endpoint_manager {
        if let Some(cors) = mgr.cors_headers(&pattern).await {
            return (StatusCode::NO_CONTENT, (*cors).clone()).into_response();
        }
    }
    AppError::MethodNotAllowed.into_response()
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
    ApiQuery(query): ApiQuery<VersionQuery>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    if let Some(ref v) = query.version {
        crate::validation::validate_version(v)?;
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

// ===== Model Health =====

pub async fn model_health_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    ApiQuery(query): ApiQuery<VersionQuery>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    if let Some(ref v) = query.version {
        crate::validation::validate_version(v)?;
    }

    let resolved_version = match &query.version {
        Some(v) => v.clone(),
        None => state.registry.get_active_version(&model_name)
            .ok_or_else(|| AppError::ModelNotFound(format!("{} has no active version", model_name)))?,
    };

    let mv = state.registry.get(&model_name, Some(&resolved_version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", model_name, resolved_version)))?;

    let total_workers = mv.workers.len();

    if let Some(outlier) = state.inference_queue.get_outlier_state(&model_name, &resolved_version) {
        let mut workers_json = Vec::with_capacity(total_workers);
        let mut healthy_count = 0usize;
        for i in 0..total_workers {
            let healthy = !outlier.is_ejected(i);
            if healthy {
                healthy_count += 1;
            }
            workers_json.push(json!({
                "worker_id": i,
                "healthy": healthy,
            }));
        }
        Ok(Json(json!({
            "model": model_name,
            "version": resolved_version,
            "healthy_workers": healthy_count,
            "total_workers": total_workers,
            "workers": workers_json,
        })))
    } else {
        // No outlier state means no active queue — report all unknown
        let workers_json: Vec<Value> = (0..total_workers)
            .map(|i| json!({"worker_id": i, "healthy": true}))
            .collect();
        Ok(Json(json!({
            "model": model_name,
            "version": resolved_version,
            "healthy_workers": total_workers,
            "total_workers": total_workers,
            "workers": workers_json,
        })))
    }
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
    ApiQuery(query): ApiQuery<VersionQuery>,
) -> Result<Json<Value>, AppError> {
    let version = query.version.unwrap_or_else(|| "1".to_string());
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    // Load model config
    let config_path = state.repo_path.join(&model_name).join(&version).join("config.yaml");
    let config = crate::config::load_model_config(&config_path).unwrap_or_default();

    info!(model = %model_name, version = %version, "load model requested");
    state.worker_manager.load_model(&model_name, &version, &config).await?;

    // Update hot reload flag so watcher picks up events for this model
    if config.hot_reload {
        state.has_hot_reload.store(true, Ordering::Relaxed);
    }

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
    ApiQuery(query): ApiQuery<VersionQuery>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    if let Some(ref v) = query.version {
        crate::validation::validate_version(v)?;
    }
    info!(model = %model_name, version = ?query.version, "unload model requested");
    let success = state.worker_manager.unload_model(&model_name, query.version.as_deref()).await?;
    if !success {
        return Err(AppError::ModelNotFound(format!("{} not loaded", model_name)));
    }

    // Re-check if any loaded models still have hot_reload enabled
    let any_hot_reload = state.registry.list_loaded().iter().any(|(_, _, mv)| mv.config.hot_reload);
    state.has_hot_reload.store(any_hot_reload, Ordering::Relaxed);

    Ok(Json(json!({
        "success": true,
        "message": format!("Model {} unloaded", model_name),
    })))
}

// ===== Reload Model =====

pub async fn reload_model_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    ApiQuery(query): ApiQuery<VersionQuery>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    if let Some(ref v) = query.version {
        crate::validation::validate_version(v)?;
    }
    info!(model = %model_name, version = ?query.version, "reload model requested");
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
    crate::validation::validate_version(&version)?;

    info!(model = %model_name, version = %version, "delete version requested");
    // Unload first if loaded
    let _ = state.worker_manager.unload_model(&model_name, Some(&version)).await;

    // Delete directory
    let version_dir = state.repo_path.join(&model_name).join(&version);
    if version_dir.exists() {
        tokio::fs::remove_dir_all(&version_dir)
            .await
            .map_err(AppError::Io)?;
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
    crate::validation::validate_version(&version)?;

    info!(model = %model_name, version = %version, "activate version requested");
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
    RequestId(request_id): RequestId,
    ApiJson(payload): ApiJson<Value>,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let result = do_infer(
        state.clone(), model_name.clone(), None,
        "/predict".to_string(), headers, payload, request_id,
    ).await;
    Ok(attach_cors_headers(&state, &model_name, result))
}

pub async fn infer_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    RequestId(request_id): RequestId,
    ApiJson(payload): ApiJson<Value>,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    let result = do_infer(
        state.clone(), model_name.clone(), Some(version),
        "/predict".to_string(), headers, payload, request_id,
    ).await;
    Ok(attach_cors_headers(&state, &model_name, result))
}

fn extract_client_ip(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .or_else(|| headers.get("x-real-ip").and_then(|v| v.to_str().ok()))
        .unwrap_or("")
        .to_string()
}

/// Acquire a rate-limit token for a model. Shared by unary infer, SSE, and WS.
///
/// `scope` derives from the policy key: `"ip"` → client IP from headers,
/// otherwise the constant `"/predict"` route scope so all inference paths for
/// a model share one bucket. Returns `RateLimitExceeded` (429 + Retry-After)
/// when the bucket is empty.
async fn enforce_rate_limit(
    state: &Arc<AppState>,
    rl: Option<&crate::worker::protocol::RateLimitPolicy>,
    model_name: &str,
    headers: &HeaderMap,
) -> Result<(), AppError> {
    let Some(rl) = rl else {
        return Ok(());
    };
    let scope = match rl.key.as_str() {
        "ip" => extract_client_ip(headers),
        _ => "/predict".to_string(),
    };
    let burst = rl.burst.unwrap_or(rl.requests_per_minute * 1.5);
    let key = format!("{}:{}", model_name, scope);
    match state
        .rate_limiter
        .acquire(&key, rl.requests_per_minute, burst)
        .await
    {
        crate::rate_limit::AcquireResult::Rejected { retry_after_secs } => {
            Err(AppError::RateLimitExceeded { retry_after_secs })
        }
        crate::rate_limit::AcquireResult::Allowed => Ok(()),
    }
}

/// Headers that must NOT be overridden by user code — managed by the HTTP
/// library or carry hop-by-hop semantics (RFC 7230 §6.1).
const BLOCKED_RESPONSE_HEADERS: &[&str] = &[
    "content-type",
    "content-length",
    "transfer-encoding",
    "content-encoding",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "upgrade",
];

/// Attach CORS headers to an inference response (success or error).
fn attach_cors_headers(
    state: &AppState,
    model: &str,
    result: Result<Response, AppError>,
) -> Response {
    let mut resp = match result {
        Ok(r) => r,
        Err(e) => e.into_response(),
    };
    if let Some(cors) = state.registry.active_cors_headers(model) {
        extend_cors_headers(resp.headers_mut(), &cors);
    }
    resp
}

/// Copy pre-built CORS headers (Arc-shared, cached at policy ingest — B9) into
/// a response in place. Entries are cloned since the source is shared behind an
/// `Arc<HeaderMap>`.
fn extend_cors_headers(target: &mut HeaderMap, src: &axum::http::HeaderMap) {
    for (name, value) in src {
        target.append(name.clone(), value.clone());
    }
}

fn inject_response_headers(
    builder: axum::http::response::Builder,
    headers: &std::collections::HashMap<String, String>,
) -> axum::http::response::Builder {
    let mut builder = builder;
    for (k, v) in headers {
        let lower = k.to_ascii_lowercase();
        if !BLOCKED_RESPONSE_HEADERS.contains(&lower.as_str()) {
            builder = builder.header(k.as_str(), v.as_str());
        }
    }
    builder
}

/// Inject response headers into an existing `HeaderMap` in place — the
/// error-response path builds via `into_response()` (no Builder), so it needs
/// this mutating variant. Skips hop-by-hop / library-managed headers and
/// silently drops headers with invalid names/values.
pub(crate) fn inject_response_headers_into(
    map: &mut HeaderMap,
    headers: &std::collections::HashMap<String, String>,
) {
    for (k, v) in headers {
        if BLOCKED_RESPONSE_HEADERS.contains(&k.to_ascii_lowercase().as_str()) {
            continue;
        }
        if let (Ok(name), Ok(val)) = (
            k.parse::<axum::http::HeaderName>(),
            axum::http::HeaderValue::from_str(v),
        ) {
            map.insert(name, val);
        }
    }
}

async fn do_infer(
    state: Arc<AppState>,
    model_name: String,
    version: Option<String>,
    route: String,
    headers: HeaderMap,
    payload: Value,
    request_id: String,
) -> Result<Response, AppError> {
    let span = tracing::info_span!(
        "inference",
        model = %model_name,
        version = version.as_deref().unwrap_or("auto"),
        request_id = %request_id,
    );
    async move {
    let resolved_version = resolve_version(&state, &model_name, version).await?;

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

    // Rate limit check (before ensemble, after mv resolution)
    enforce_rate_limit(&state, mv.policies.rate_limit.as_ref(), &model_name, &headers).await?;

    // Handle ensemble
    if mv.model_type == ModelType::Ensemble {
        let result = crate::ensemble::execute_ensemble(state, &model_name, &resolved_version, payload, &request_id).await?;
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
    let client_ip = extract_client_ip(&headers);
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
    };

    // All requests go through the unified inference queue
    let (response_tx, response_rx) = oneshot::channel();

    let item = crate::inference_queue::QueueItem {
        uid: uid.clone(),
        data: meta.payload.clone(),
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
                    prometheus::record_request_end(&model_name, &resolved_version, status_family(single.status_code), duration).await;
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
                        prometheus::record_request_end(&model_name, &resolved_version, status_family, duration).await;
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
                    prometheus::record_request_end(&model_name, &resolved_version, "5xx", duration).await;
                    Err(AppError::WorkerCrashed(msg))
                }
                _ => {
                    prometheus::record_request_end(&model_name, &resolved_version, status_family(single.status_code), duration).await;
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

fn build_request_meta(headers: &HeaderMap, payload: &Value, route: &str, request_id: String) -> pb::RequestMeta {
    let header_map: HashMap<String, String> = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
        .collect();
    let client_ip = extract_client_ip(headers);
    let timestamp_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    let payload_bytes = bytes::Bytes::from(serde_json::to_vec(payload).unwrap_or_default());

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
    payload_bytes: bytes::Bytes,
) -> Result<(String, mpsc::Receiver<pb::StreamResponse>), AppError> {
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

    // Skip ejected workers for streaming requests
    let worker_id = if let Some(outlier) = state.worker_manager.get_outlier_state(model_name, resolved_version).await {
        crate::worker::pick_worker_skip_ejected(num_workers, &outlier)
    } else {
        crate::worker::pick_worker_random(num_workers)
    };

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
    RequestId(request_id): RequestId,
    ApiJson(payload): ApiJson<Value>,
) -> Response {
    let result =
        sse_infer_entry(&state, &model_name, None, headers, payload, request_id).await;
    attach_cors_headers(&state, &model_name, result)
}

pub async fn sse_infer_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    headers: HeaderMap,
    RequestId(request_id): RequestId,
    ApiJson(payload): ApiJson<Value>,
) -> Response {
    let result = sse_infer_entry(
        &state, &model_name, Some(version), headers, payload, request_id,
    )
    .await;
    attach_cors_headers(&state, &model_name, result)
}

/// Shared entry for SSE inference: validation, ready check, rate limiting,
/// and stream setup. Returns a `Response` so the caller can uniformly wrap
/// CORS around both the success stream-start and any early error.
async fn sse_infer_entry(
    state: &Arc<AppState>,
    model_name: &str,
    version: Option<String>,
    headers: HeaderMap,
    payload: Value,
    request_id: String,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(model_name)?;
    if let Some(ref v) = version {
        crate::validation::validate_version(v)?;
    }
    let resolved_version = resolve_version(state, model_name, version).await?;
    if !state.registry.is_ready(model_name, Some(&resolved_version)) {
        return Err(AppError::ModelNotReady(format!(
            "{} version {} is not ready",
            model_name, resolved_version
        )));
    }
    // Rate limit (shares the /predict bucket with unary infer).
    if let Some(mv) = state.registry.get(model_name, Some(&resolved_version)) {
        enforce_rate_limit(state, mv.policies.rate_limit.as_ref(), model_name, &headers).await?;
    }
    let sse = sse_infer_impl(
        state.clone(),
        model_name.to_string(),
        resolved_version,
        headers,
        payload,
        request_id,
    )
    .await?;
    Ok(sse.into_response())
}

async fn sse_infer_impl(
    state: Arc<AppState>,
    model_name: String,
    resolved_version: String,
    headers: HeaderMap,
    payload: Value,
    request_id: String,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, AppError> {

    let meta = build_request_meta(&headers, &payload, "/predict", request_id);
    let payload_bytes = meta.payload.clone();
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
                    Event::default().data(&data)
                }
                Some(pb::stream_response::Payload::Error(e)) => {
                    // Try to parse as structured error from HTTPException
                    let event_data = match serde_json::from_str::<serde_json::Value>(&e.message) {
                        Ok(val) if val.get("error").and_then(|err| err.get("type")).is_some() => {
                            json!({"error": val["error"]}).to_string()
                        }
                        _ => json!({"error": e.message}).to_string(),
                    };
                    Event::default().data(event_data)
                }
                Some(pb::stream_response::Payload::Done(done)) => {
                    prometheus::record_worker_metrics(&model_name, done.metrics.as_ref());
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
        open_worker_stream_cancel(&state, &model_name, &resolved_version, cancel_req).await;
    });

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
                let _ = client.send(cancel_req.clone()).await;
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
    ws: axum::extract::WebSocketUpgrade,
    RequestId(request_id): RequestId,
) -> Response {
    if let Err(e) = crate::validation::validate_identifier(&model_name) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, None, socket, request_id))
}

pub async fn ws_stream_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    ws: axum::extract::WebSocketUpgrade,
    RequestId(request_id): RequestId,
) -> Response {
    if let Err(e) = crate::validation::validate_identifier(&model_name) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    if let Err(e) = crate::validation::validate_version(&version) {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }
    ws.on_upgrade(move |socket| handle_ws_stream(state, model_name, Some(version), socket, request_id))
}

async fn handle_ws_stream(
    state: Arc<AppState>,
    model_name: String,
    version: Option<String>,
    mut socket: WebSocket,
    request_id: String,
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

    // Rate limit (same logic as HTTP infer). The upgraded WebSocket has no
    // HTTP headers, so key="ip" collapses every connection to one shared
    // bucket per model — a known limitation; front WS with a reverse proxy
    // for per-client limiting. Rejection closes the socket with an error frame.
    if let Some(mv) = state.registry.get(&model_name, Some(&resolved_version)) {
        if enforce_rate_limit(
            &state,
            mv.policies.rate_limit.as_ref(),
            &model_name,
            &HeaderMap::new(),
        )
        .await
        .is_err()
        {
            let _ = socket
                .send(Message::Text(
                    json!({"error": "rate limit exceeded"}).to_string(),
                ))
                .await;
            let _ = socket.close().await;
            return;
        }
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
    let meta = build_request_meta(&headers, &payload, "/predict", request_id);
    let payload_bytes = meta.payload.clone();

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

    // Clone before move so cancel can reference them after the task completes
    let model_name_owned = model_name.clone();
    let version_owned = resolved_version.clone();

    // Spawn task to forward worker chunks -> WebSocket
    let send_task = tokio::spawn(async move {
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
                    Message::Text(event_data)
                }
                Some(pb::stream_response::Payload::Done(done)) => {
                    prometheus::record_worker_metrics(&model_name, done.metrics.as_ref());
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
    open_worker_stream_cancel(&state, &model_name_owned, &version_owned, cancel_req).await;
}

// ===== Custom Endpoint Handler =====

pub async fn custom_endpoint_handler(
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
    matched_path: Option<axum::extract::MatchedPath>,
    request: axum::http::Request<axum::body::Body>,
) -> Response {
    let route_pattern = matched_path
        .as_ref()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let result =
        custom_endpoint_impl(state.clone(), request_id, route_pattern.clone(), request).await;
    let mut resp = match result {
        Ok(r) => r,
        Err(e) => e.into_response(),
    };
    // CORS wraps every endpoint response (success, 429, stream start, transport
    // error) — previously only the non-stream success branch attached it, so
    // early `?` returns and streaming responses leaked without CORS headers.
    if let Some(mgr) = &state.endpoint_manager {
        if let Some(cors) = mgr.cors_headers(&route_pattern).await {
            extend_cors_headers(resp.headers_mut(), &cors);
        }
    }
    resp
}

async fn custom_endpoint_impl(
    state: Arc<AppState>,
    request_id: String,
    route_pattern: String,
    request: axum::http::Request<axum::body::Body>,
) -> Result<Response, AppError> {
    let ep_mgr = match &state.endpoint_manager {
        Some(mgr) => mgr,
        None => return Err(AppError::Internal("endpoint manager not available".to_string())),
    };

    let route = request.uri().path().to_string();
    let method = request.method().to_string();

    let span = tracing::info_span!(
        "endpoint_request",
        route = %route,
        method = %method,
        request_id = %request_id,
    );
    let _enter = span.enter();

    // Extract query params with proper URL decoding
    let query: HashMap<String, String> = request
        .uri()
        .query()
        .map(|q| {
            form_urlencoded::parse(q.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_default();

    // Extract headers + client IP before consuming the request body
    let mut headers = HashMap::new();
    let client_ip = extract_client_ip(request.headers());
    let timestamp_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
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

    // Check if client requested streaming
    let is_stream = body
        .as_ref()
        .and_then(|b| b.get("stream"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Rate limit check for custom endpoints
    if let Some(rl) = ep_mgr
        .rate_limit_policy(&route_pattern)
        .await
    {
        let scope = match rl.key.as_str() {
            "ip" => client_ip.clone(),
            _ => route_pattern.clone(),
        };
        let burst = rl.burst.unwrap_or(rl.requests_per_minute * 1.5);
        let key = format!("ep:{}", scope);
        match state.rate_limiter.acquire(&key, rl.requests_per_minute, burst).await {
            crate::rate_limit::AcquireResult::Rejected { retry_after_secs } => {
                return Err(AppError::RateLimitExceeded { retry_after_secs });
            }
            crate::rate_limit::AcquireResult::Allowed => {}
        }
    }

    let snapshot = ep_mgr.build_snapshot().await;

    let req = crate::worker::protocol::EndpointRequest {
        request_id: request_id.to_string(),
        route,
        method,
        headers,
        query,
        body,
        server_state: snapshot,
        client_ip,
        timestamp_ns,
        route_pattern: route_pattern.clone(),
    };

    if is_stream {
        // Streaming: return SSE response
        let mut chunk_rx = ep_mgr.send_stream_request(req).await?;

        let (event_tx, event_rx) = mpsc::channel(64);

        tokio::spawn(async move {
            while let Some(chunk) = chunk_rx.recv().await {
                // Check for error chunks
                if let Some(err) = chunk.get("error") {
                    let event = Event::default()
                        .event("error")
                        .data(json!({"error": err}).to_string());
                    if event_tx.send(Ok::<_, Infallible>(event)).await.is_err() {
                        break;
                    }
                    break;
                }

                let data = chunk.to_string();
                let event = Event::default().data(data);
                if event_tx.send(Ok::<_, Infallible>(event)).await.is_err() {
                    break;
                }
            }
            // Send done event
            let _ = event_tx
                .send(Ok::<_, Infallible>(Event::default().data("[DONE]")))
                .await;
        });

        Ok(Sse::new(ReceiverStream::new(event_rx)).into_response())
    } else {
        // Non-streaming: single response
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
}

// ===== Upload Model =====

#[derive(Deserialize)]
pub struct UploadQuery {
    pub load: Option<bool>,
}

pub async fn upload_model_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    ApiQuery(query): ApiQuery<UploadQuery>,
    mut multipart: Multipart,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    let target_dir = crate::validation::resolve_model_dir(&state.repo_path, &model_name, &version)?;
    tokio::fs::create_dir_all(&target_dir)
        .await
        .map_err(AppError::Io)?;

    let mut uploaded_files: Vec<String> = Vec::new();

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        AppError::Validation(format!("multipart error: {}", e))
    })? {
        let filename = field
            .file_name()
            .unwrap_or("unnamed")
            .to_string();

        let data = field.bytes().await.map_err(|e| {
            AppError::Transport(format!("read upload field: {}", e))
        })?;

        if filename.ends_with(".lma") {
            // Save .lma to temp file, then unpack via Python CLI
            let tmp_dir = std::env::temp_dir().join(format!(
                "lite-server-upload-{}",
                uuid::Uuid::new_v4()
            ));
            tokio::fs::create_dir_all(&tmp_dir)
                .await
                .map_err(AppError::Io)?;
            let tmp_file = tmp_dir.join(&filename);
            tokio::fs::write(&tmp_file, &data)
                .await
                .map_err(AppError::Io)?;

            let output = tokio::process::Command::new("python")
                .args([
                    "-m",
                    "lite_server",
                    "unpack",
                    tmp_file.to_str().unwrap_or(""),
                    "--to",
                    target_dir.to_str().unwrap_or(""),
                    "--flat",
                ])
                .output()
                .await
                .map_err(|e| AppError::Internal(format!("failed to run python unpack: {}", e)))?;

            // Clean up temp dir
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(AppError::Validation(format!(
                    "artifact unpack failed: {}",
                    stderr.trim()
                )));
            }
            uploaded_files.push(filename);
        } else {
            // Raw file: write directly to target dir
            // Sanitize filename: strip any path components
            let safe_name = std::path::Path::new(&filename)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            if safe_name.is_empty() || safe_name.starts_with('.') {
                continue;
            }
            let file_path = target_dir.join(&safe_name);
            tokio::fs::write(&file_path, &data)
                .await
                .map_err(AppError::Io)?;
            uploaded_files.push(safe_name);
        }
    }

    if uploaded_files.is_empty() {
        return Err(AppError::Validation("no files uploaded".to_string()));
    }

    // Optionally auto-load after upload
    let auto_load = query.load.unwrap_or(true);
    if auto_load {
        let config_path = target_dir.join("config.yaml");
        let config = crate::config::load_model_config(&config_path).unwrap_or_default();
        if let Err(e) = state.worker_manager.load_model(&model_name, &version, &config).await {
            warn!("Auto-load after upload failed: {}", e);
        }
        let active = state.registry.get_active_version(&model_name);
        if active.is_none() {
            let _ = state.registry.activate_version(&model_name, &version);
        }
    }

    info!(
        model = %model_name,
        version = %version,
        files = ?uploaded_files,
        "Model uploaded"
    );

    Ok(Json(json!({
        "success": true,
        "model": model_name,
        "version": version,
        "files": uploaded_files,
        "loaded": auto_load,
    })))
}

// ===== Download Model =====

#[derive(Deserialize)]
pub struct DownloadQuery {
    pub file: Option<String>,
}

pub async fn download_model_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    ApiQuery(query): ApiQuery<DownloadQuery>,
) -> Result<Response, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    let model_dir = crate::validation::resolve_model_dir(&state.repo_path, &model_name, &version)?;

    if !model_dir.exists() {
        return Err(AppError::ModelNotFound(format!(
            "{} version {} not found",
            model_name, version
        )));
    }

    // Single file download
    if let Some(ref file_name) = query.file {
        // Validate file name doesn't contain path separators
        if file_name.contains("..") || file_name.contains('/') || file_name.contains('\\') {
            return Err(AppError::Validation("invalid file name".to_string()));
        }
        let file_path = model_dir.join(file_name);
        // Ensure resolved path is inside model_dir
        let canonical_file = file_path.canonicalize().map_err(AppError::Io)?;
        let canonical_dir = model_dir.canonicalize().map_err(AppError::Io)?;
        if !canonical_file.starts_with(&canonical_dir) {
            return Err(AppError::Validation("path traversal rejected".to_string()));
        }

        let data = tokio::fs::read(&canonical_file)
            .await
            .map_err(AppError::Io)?;
        let content_type = if file_name.ends_with(".py") || file_name.ends_with(".yaml") || file_name.ends_with(".yml") || file_name.ends_with(".json") || file_name.ends_with(".txt") || file_name.ends_with(".md") {
            "text/plain; charset=utf-8"
        } else {
            "application/octet-stream"
        };

        let response = Response::builder()
            .header(CONTENT_TYPE, content_type)
            .header(
                CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}\"", file_name),
            )
            .body(axum::body::Body::from(data))
            .map_err(|e| AppError::Internal(format!("build response: {}", e)))?;
        return Ok(response);
    }

    // Full directory download as .lma
    let tmp_dir = std::env::temp_dir().join(format!("lite-server-download-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&tmp_dir)
        .await
        .map_err(AppError::Io)?;

    let output = tokio::process::Command::new("python")
        .args([
            "-m",
            "lite_server",
            "pack",
            model_dir.to_str().unwrap_or(""),
            "--version",
            &version,
            "--output",
            tmp_dir.to_str().unwrap_or(""),
        ])
        .output()
        .await
        .map_err(|e| AppError::Internal(format!("failed to run python pack: {}", e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
        return Err(AppError::Internal(format!("pack failed: {}", stderr.trim())));
    }

    // Find the generated .lma file
    let mut lma_file = None;
    let mut entries = tokio::fs::read_dir(&tmp_dir)
        .await
        .map_err(AppError::Io)?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if entry.path().extension().map(|e| e == "lma").unwrap_or(false) {
            lma_file = Some(entry.path());
            break;
        }
    }

    let lma_path = lma_file.ok_or_else(|| {
        AppError::Internal("pack produced no .lma file".to_string())
    })?;

    let data = tokio::fs::read(&lma_path)
        .await
        .map_err(AppError::Io)?;
    let artifact_name = lma_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    // Clean up temp dir
    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;

    let response = Response::builder()
        .header(CONTENT_TYPE, "application/octet-stream")
        .header(
            CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", artifact_name),
        )
        .body(axum::body::Body::from(data))
        .map_err(|e| AppError::Internal(format!("build response: {}", e)))?;
    Ok(response)
}

// ===== List Files =====

pub async fn list_files_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    let model_dir = crate::validation::resolve_model_dir(&state.repo_path, &model_name, &version)?;

    if !model_dir.exists() {
        return Err(AppError::ModelNotFound(format!(
            "{} version {} not found",
            model_name, version
        )));
    }

    let mut files = Vec::new();
    let mut entries = tokio::fs::read_dir(&model_dir)
        .await
        .map_err(AppError::Io)?;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        let metadata = entry.metadata().await.ok();
        let size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
        let modified = metadata
            .as_ref()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs());

        files.push(json!({
            "name": name,
            "size": size,
            "modified": modified,
            "is_dir": path.is_dir(),
        }));
    }

    Ok(Json(json!({
        "model": model_name,
        "version": version,
        "files": files,
    })))
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
    ApiQuery(query): ApiQuery<TimelineQuery>,
) -> Result<Json<Value>, AppError> {
    let version = query.version.unwrap_or_else(|| "1".to_string());
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
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

#[cfg(test)]
mod upload_download_tests {
    use super::*;
    use crate::config::Config;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use tower::ServiceExt;

    fn test_app_state(repo_path: std::path::PathBuf) -> Arc<AppState> {
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(crate::callback::CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            repo_path.clone(),
            inference_queue.clone(),
            "warn".to_string(),
            callback_runner.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            worker_manager,
            inference_queue,
            None,
            Config::default(),
            repo_path,
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::new()),
        ))
    }

    fn test_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route(
                "/v2/repository/models/:model_name/versions/:version/upload",
                axum::routing::post(upload_model_handler),
            )
            .route(
                "/v2/repository/models/:model_name/versions/:version/download",
                axum::routing::get(download_model_handler),
            )
            .route(
                "/v2/repository/models/:model_name/versions/:version/files",
                axum::routing::get(list_files_handler),
            )
            .with_state(state)
    }

    // ===== List Files Tests =====

    #[tokio::test]
    async fn test_list_files_returns_directory_contents() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-list-test-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "print('hello')")
            .await
            .unwrap();
        tokio::fs::write(model_dir.join("config.yaml"), "max_batch_size: 1")
            .await
            .unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/files")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["model"], "mymodel");
        assert_eq!(json["version"], "1");
        let files = json["files"].as_array().unwrap();
        assert_eq!(files.len(), 2);

        let names: Vec<&str> = files.iter().map(|f| f["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"model.py"));
        assert!(names.contains(&"config.yaml"));

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_list_files_returns_404_for_missing_model() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-list-404-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/nonexistent/versions/1/files")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== Download Single File Tests =====

    #[tokio::test]
    async fn test_download_single_file() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-dl-test-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "def predict(x): return x")
            .await
            .unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/download?file=model.py")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let disposition = response
            .headers()
            .get(CONTENT_DISPOSITION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(disposition.contains("model.py"));

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"def predict(x): return x");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_download_rejects_path_traversal() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-dl-traversal-{}",
            std::process::id()
        ));
        let model_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();
        tokio::fs::write(model_dir.join("model.py"), "test")
            .await
            .unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v2/repository/models/mymodel/versions/1/download?file=../../../etc/passwd")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== Upload Tests =====

    #[tokio::test]
    async fn test_upload_raw_file() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-upload-test-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        // Build multipart body manually
        let boundary = "----testboundary123";
        let body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"model.py\"\r\n\
             Content-Type: text/x-python\r\n\r\n\
             def predict(x): return x\r\n\
             --{boundary}--\r\n"
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/mymodel/versions/1/upload?load=false")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["model"], "mymodel");
        assert_eq!(json["version"], "1");
        let files = json["files"].as_array().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], "model.py");

        // Verify file was written
        let content = tokio::fs::read_to_string(tmp.join("mymodel").join("1").join("model.py"))
            .await
            .unwrap();
        assert_eq!(content, "def predict(x): return x");

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn test_upload_rejects_invalid_model_name() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-upload-invalid-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let state = test_app_state(tmp.clone());
        let app = test_router(state);

        let boundary = "----testboundary123";
        let body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"model.py\"\r\n\r\n\
             test\r\n\
             --{boundary}--\r\n"
        );

        // Use a model name with invalid characters (space) that still matches the route
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/repository/models/bad%20name/versions/1/upload")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={}", boundary),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use axum::http::HeaderMap;

    #[test]
    fn test_build_request_meta_payload_matches_serialized() {
        // build_request_meta already serializes payload into meta.payload.
        // This test proves that meta.payload is identical to direct serialization,
        // confirming that streaming handlers don't need a separate serde_json::to_vec call.
        let headers = HeaderMap::new();
        let payload = serde_json::json!({"prompt": "hello", "max_tokens": 100});
        let direct_bytes = bytes::Bytes::from(serde_json::to_vec(&payload).unwrap_or_default());
        let request_id = "test-id-001".to_string();

        let meta = build_request_meta(&headers, &payload, "/predict", request_id);

        assert_eq!(meta.payload, direct_bytes,
            "meta.payload should equal direct serde_json::to_vec output");
    }

    #[test]
    fn test_build_request_meta_returns_correct_route() {
        let headers = HeaderMap::new();
        let payload = serde_json::json!({"x": 1});
        let request_id = "test-id-002".to_string();
        let meta = build_request_meta(&headers, &payload, "/custom", request_id);

        assert_eq!(meta.route, "/custom");
        assert_eq!(meta.client_ip, "");
        assert_eq!(meta.request_id, "test-id-002");
    }
}

#[cfg(test)]
mod extractor_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    async fn read_error_body(response: Response) -> Value {
        let body_bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        serde_json::from_slice(&body_bytes).unwrap()
    }

    #[tokio::test]
    async fn test_api_json_valid_body_passes() {
        let app = axum::Router::new().route("/t", axum::routing::post(
            |ApiJson(v): ApiJson<Value>| async move { v.to_string() }));

        let response = app
            .oneshot(Request::builder().uri("/t").method("POST")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"input": 1}"#)).unwrap())
            .await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_api_json_malformed_body_standardized_error() {
        let app = axum::Router::new().route("/t", axum::routing::post(
            |ApiJson(v): ApiJson<Value>| async move { v.to_string() }));

        let response = app
            .oneshot(Request::builder().uri("/t").method("POST")
                .header("content-type", "application/json")
                .body(Body::from("{not json")).unwrap())
            .await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["code"], "invalid_request_body");
        assert_eq!(body["error"]["param"], Value::Null);
    }

    #[tokio::test]
    async fn test_api_json_missing_content_type_standardized_error() {
        let app = axum::Router::new().route("/t", axum::routing::post(
            |ApiJson(v): ApiJson<Value>| async move { v.to_string() }));

        let response = app
            .oneshot(Request::builder().uri("/t").method("POST")
                .body(Body::from(r#"{"input": 1}"#)).unwrap())
            .await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["code"], "invalid_request_body");
    }

    #[tokio::test]
    async fn test_api_query_invalid_param_standardized_error() {
        #[derive(serde::Deserialize)]
        struct TestQuery { #[allow(dead_code)] flag: bool }

        let app = axum::Router::new().route("/t", axum::routing::get(
            |ApiQuery(q): ApiQuery<TestQuery>| async move { q.flag.to_string() }));

        let response = app
            .oneshot(Request::builder().uri("/t?flag=notabool").body(Body::empty()).unwrap())
            .await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = read_error_body(response).await;
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["code"], "invalid_query_param");
        assert_eq!(body["error"]["param"], Value::Null);
    }
}

#[cfg(test)]
mod status_family_tests {
    use super::status_family;

    #[test]
    fn test_status_family_mapping() {
        assert_eq!(status_family(0), "2xx");     // unset → success
        assert_eq!(status_family(200), "2xx");
        assert_eq!(status_family(201), "2xx");
        assert_eq!(status_family(302), "3xx");
        assert_eq!(status_family(400), "4xx");
        assert_eq!(status_family(404), "4xx");
        assert_eq!(status_family(500), "5xx");
        assert_eq!(status_family(503), "5xx");
    }
}
