use crate::error::AppError;
use crate::http::state::AppState;
use crate::metrics::prometheus;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

// ===== Health =====

/// Liveness: the process is up — deliberately checks nothing else (no
/// models/ZMQ/DB) so a model failure never cascades into a pod restart.
pub async fn livez_handler() -> impl IntoResponse {
    Json(json!({"status": "alive"}))
}

/// Readiness: 200 while at least one version can serve (Ready or Degraded);
/// 503 otherwise. A single failed model never takes readiness down.
pub async fn readyz_handler(State(state): State<Arc<AppState>>) -> Response {
    let status = state.registry.server_status();
    if status.has_serving() {
        (
            StatusCode::OK,
            Json(json!({"status": "ready", "models": status.serving_model_names()})),
        )
            .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "not_ready", "reason": "no models available"})),
        )
            .into_response()
    }
}

/// Startup: 503 while any version is Pending/Loading so slow model loads
/// don't trip liveness (pair with K8s startupProbe.failureThreshold).
pub async fn startupz_handler(State(state): State<Arc<AppState>>) -> Response {
    let pending: Vec<String> = state
        .registry
        .server_status()
        .initializing()
        .into_iter()
        .map(|(name, version)| format!("{}/{}", name, version))
        .collect();
    if pending.is_empty() {
        (StatusCode::OK, Json(json!({"status": "started"}))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "initializing", "pending": pending})),
        )
            .into_response()
    }
}

/// Combined summary: always 200 (informational), per-version status and
/// loaded_at. `status` mirrors the readyz predicate.
pub async fn health_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let status = state.registry.server_status();
    // §4.5: group per-version entries under their model, carrying the
    // active_version pointer. Entries are sorted by (name, version), so a
    // model's versions are contiguous.
    let mut models: Vec<Value> = Vec::new();
    for e in &status.entries {
        let version_json = json!({
            "version": e.version,
            "status": e.status,
            "workers": e.workers,
            "loaded_at": e.loaded_at
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs()),
        });
        if let Some(last) = models.last_mut() {
            if last["name"] == e.name {
                last["versions"].as_array_mut().unwrap().push(version_json);
                continue;
            }
        }
        models.push(json!({
            "name": e.name,
            "active_version": state.registry.get_active_version(&e.name),
            "versions": [version_json],
        }));
    }
    Json(json!({
        "status": if status.has_serving() { "ready" } else { "not_ready" },
        "models": models,
    }))
}

// ===== OPTIONS preflight for inference routes =====

pub async fn inference_options_handler(
    State(state): State<Arc<AppState>>,
    Path(params): Path<HashMap<String, String>>,
) -> Response {
    // The handler is registered on both 1-param (`/v2/models/:model_name/infer`)
    // and 2-param (`/v2/models/:model_name/versions/:version/infer`) routes.
    // Path<String> only accepts exactly one captured param, so the versioned
    // routes returned 500. HashMap captures all params regardless of count.
    let Some(model_name) = params.get("model_name") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // §4.4: versioned routes answer with the hit version's CORS policy;
    // bare routes use the active version's.
    let cors = match params.get("version") {
        Some(v) => state.registry.cors_headers_for(model_name, v),
        None => state.registry.active_cors_headers(model_name),
    };
    match cors {
        Some(headers) => {
            (StatusCode::NO_CONTENT, (*headers).clone()).into_response()
        }
        None => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
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

// ===== Timeline =====

pub async fn timeline_handler() -> impl IntoResponse {
    let snapshots = crate::metrics::aggregator::TIMELINE.all_snapshots().await;
    Json(json!({ "snapshots": snapshots }))
}

async fn timeline_json(model_name: &str, version: &str) -> Json<Value> {
    let entries = crate::metrics::aggregator::TIMELINE.get_timeline(model_name, version).await;
    Json(json!({
        "model": model_name,
        "version": version,
        "entries": entries,
    }))
}

/// Bare timeline = the active version (§4.4; the old default was the literal
/// string "1" regardless of what is actually active).
pub async fn timeline_model_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let version = state.registry.get_active_version(&model_name).ok_or_else(|| {
        AppError::ModelNotFound(format!("{} has no active version", model_name))
    })?;
    Ok(timeline_json(&model_name, &version).await)
}

/// Versioned timeline = the explicit version (§4.4).
pub async fn timeline_model_version_handler(
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    Ok(timeline_json(&model_name, &version).await)
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

