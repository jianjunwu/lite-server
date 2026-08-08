use super::*;
use super::inference::resolve_version;
use crate::error::AppError;
use crate::http::state::AppState;
use crate::metrics::prometheus;
use crate::registry::types::ModelType;
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, Json},
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::info;

// ===== KServe V2 管理面(阶段 3,批次 3) =====

/// G13/J1:model_type → KServe platform 映射。Ensemble → `ensemble`;
/// LitAPI 无 KServe 标准枚举(`<project>_<format>`)对应 → 兜底 `custom`
/// (J1 裁定:KServe 自身填空串,我们按规范填 = 超集,不学空串)。
pub(crate) fn model_type_to_platform(model_type: &ModelType) -> &'static str {
    match model_type {
        ModelType::Ensemble => "ensemble",
        ModelType::LitAPI => "custom",
    }
}

/// D8:/v2 server metadata——能力发现(G11)。extensions 随能力落地增删。
pub async fn v2_server_metadata_handler() -> Json<Value> {
    Json(json!({
        "name": "lite-server",
        "version": env!("CARGO_PKG_VERSION"),
        "extensions": ["binary_tensor_data"],
    }))
}

/// D8:/v2/models/:m 规范形状模型元数据(G13/G16):name/versions/platform/
/// inputs/outputs 必填;state 不是规范字段(C2)不返回。inputs/outputs
/// 缺省空数组(合法降级,tritonclient 不校验非空);worker get_metadata()
/// 回调是后续可选增强。
async fn model_metadata_impl(
    state: &AppState,
    model_name: &str,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(model_name)?;
    let versions = state.registry.list_versions(model_name);
    if versions.is_empty() {
        return Err(AppError::ModelNotFound(model_name.to_string()));
    }
    // platform 取 active 版本优先(admin ops 不跟加权路由),无 active 取首个。
    let model_type = state
        .registry
        .get_active_version(model_name)
        .and_then(|v| state.registry.get(model_name, Some(&v)))
        .map(|mv| mv.model_type.clone())
        .or_else(|| versions.first().map(|mv| mv.model_type.clone()))
        .unwrap_or(ModelType::LitAPI);
    Ok(Json(json!({
        "name": model_name,
        "versions": versions.iter().map(|mv| mv.version.clone()).collect::<Vec<_>>(),
        "platform": model_type_to_platform(&model_type),
        "inputs": [],
        "outputs": [],
    })))
}

pub async fn model_metadata_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
) -> Result<Json<Value>, AppError> {
    model_metadata_impl(&state, &model_name).await
}

/// G16:versioned 模型元数据(规范路径含可选 versions 段)。
pub async fn model_metadata_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    if state.registry.get(&model_name, Some(&version)).is_none() {
        return Err(AppError::ModelNotFound(format!("{model_name}/{version}")));
    }
    let json = model_metadata_impl(&state, &model_name).await?;
    // 请求的版本必须存在于 versions 列表(versioned 路径语义)
    let versions = json["versions"].as_array().cloned().unwrap_or_default();
    if !versions.iter().any(|v| v == &json!(version)) {
        return Err(AppError::ModelNotFound(format!("{model_name}/{version}")));
    }
    Ok(json)
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
                "status": mv.status,
                "model_type": format!("{:?}", mv.model_type),
                "workers": mv.workers.len(),
            })
        })
        .collect();
    Json(json!({"models": models_json}))
}

// ===== List Versions =====

/// Multi-version overview (§4.5): per-version status / active / weight /
/// worker counts / loaded_at.
pub async fn list_versions_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let versions = state.registry.list_versions(&model_name);
    if versions.is_empty() {
        return Err(AppError::ModelNotFound(model_name));
    }
    let active = state.registry.get_active_version(&model_name);
    let versions_json: Vec<Value> = versions
        .iter()
        .map(|mv| {
            let ready_workers = mv
                .workers
                .iter()
                .filter(|w| w.status == crate::registry::types::WorkerStatus::Ready)
                .count();
            json!({
                "version": mv.version,
                "status": mv.status,
                "active": active.as_deref() == Some(mv.version.as_str()),
                "weight": mv.weight,
                "workers": {
                    "ready": ready_workers,
                    "total": mv.workers.len(),
                },
                "loaded_at": mv.loaded_at
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs()),
            })
        })
        .collect();
    Ok(Json(json!({
        "name": model_name,
        "active_version": active,
        "versions": versions_json,
    })))
}

// ===== Model Ready =====

fn model_ready_json(state: &AppState, model_name: &str, version: &str) -> Json<Value> {
    let ready = state.registry.is_ready(model_name, Some(version));
    let active_version = state.registry.get_active_version(model_name);
    Json(json!({
        "name": model_name,
        "version": version,
        "ready": ready,
        "active_version": active_version,
    }))
}

/// Bare readiness = the active version (§4.4). Always 200: a readiness
/// probe answers the boolean question even when nothing is active
/// (`ready: false`), instead of conflating "not ready" with "not found".
pub async fn model_ready_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let active_version = state.registry.get_active_version(&model_name);
    let ready = active_version
        .as_deref()
        .is_some_and(|v| state.registry.is_ready(&model_name, Some(v)));
    Ok(Json(json!({
        "name": model_name,
        "version": active_version.clone(),
        "ready": ready,
        "active_version": active_version,
    })))
}

/// Versioned readiness = the explicit version (§4.4).
pub async fn model_ready_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    Ok(model_ready_json(&state, &model_name, &version))
}

// ===== Model Health =====

fn model_health_json(state: &AppState, model_name: &str, resolved_version: &str) -> Result<Json<Value>, AppError> {
    let mv = state.registry.get(model_name, Some(resolved_version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", model_name, resolved_version)))?;

    let total_workers = mv.workers.len();

    if let Some(outlier) = state.inference_queue.get_outlier_state(model_name, resolved_version) {
        let mut workers_json = Vec::with_capacity(total_workers);
        let mut healthy_count = 0usize;
        for i in 0..total_workers {
            let ejected = outlier.is_ejected(i);
            if !ejected {
                healthy_count += 1;
            }
            workers_json.push(json!({
                "worker_id": i,
                "healthy": !ejected,
                "ejected": ejected,
            }));
        }
        Ok(Json(json!({
            "name": model_name,
            "version": resolved_version,
            "healthy_workers": healthy_count,
            "total_workers": total_workers,
            "workers": workers_json,
        })))
    } else {
        // No outlier state means no active queue — report all unknown
        let workers_json: Vec<Value> = (0..total_workers)
            .map(|i| json!({"worker_id": i, "healthy": true, "ejected": false}))
            .collect();
        Ok(Json(json!({
            "name": model_name,
            "version": resolved_version,
            "healthy_workers": total_workers,
            "total_workers": total_workers,
            "workers": workers_json,
        })))
    }
}

/// Bare model health = the version the router would serve (§4.3/§4.5).
pub async fn model_health_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let resolved_version = resolve_version(&state, &model_name, None, &headers).await?;
    model_health_json(&state, &model_name, &resolved_version)
}

/// Versioned model health = the explicit version (§4.4).
pub async fn model_health_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    model_health_json(&state, &model_name, &version)
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
                        is_ensemble = crate::config::config_content_is_ensemble(&content);
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

/// G14 (批次 3):bare load aliases 到 active 版本——admin ops 不跟加权路由。
/// 幂等(C10):active 存在即 200(KServe load 语义,重复调用不报错);无
/// active → 明确错误。响应 KServe 形状 {"name","load":true}(J2)。
/// 「上传后 bare load」流程(上传不 load 须先 versioned load/activate)写入
/// 已知偏差文档(D8)。
pub async fn bare_load_model_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    state.registry.get_active_version(&model_name).ok_or_else(|| {
        AppError::ModelNotFound(format!("{} has no active version", model_name))
    })?;
    Ok(Json(json!({"name": model_name, "load": true})))
}

/// Load is versioned-only (§4.4): the old bare endpoint silently defaulted
/// to version "1".
pub async fn load_model_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    // Load model config
    let config_path = state.repo_path.join(&model_name).join(&version).join("config.yaml");
    let mut config = crate::config::load_model_config(&config_path).unwrap_or_default();
    // Apply CLI model-default overrides, same as initial load (server.rs) and
    // hot reload — otherwise --max-queue-size etc. silently don't apply here.
    state.config.apply_model_defaults(&mut config);

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

async fn unload_model_impl(
    state: &AppState,
    model_name: &str,
    version: &str,
) -> Result<(), AppError> {
    info!(model = %model_name, version = %version, "unload model requested");
    let success = state.worker_manager.unload_model(model_name, Some(version)).await?;
    if !success {
        return Err(AppError::ModelNotFound(format!(
            "{} version {} not loaded",
            model_name, version
        )));
    }

    // Re-check if any loaded models still have hot_reload enabled
    let any_hot_reload = state.registry.list_loaded().iter().any(|(_, _, mv)| mv.config.hot_reload);
    state.has_hot_reload.store(any_hot_reload, Ordering::Relaxed);

    Ok(())
}

/// Bare unload targets the **active** version (§4.4): admin ops never follow
/// the weighted routing pick. 响应 KServe 形状 {"name","unload":true}(J2,
/// v2_endpoints.py:196-215 实证;bare 消费者是 KServe SDK)。
pub async fn unload_model_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let active = state.registry.get_active_version(&model_name).ok_or_else(|| {
        AppError::ModelNotFound(format!("{} has no active version", model_name))
    })?;
    unload_model_impl(&state, &model_name, &active).await?;
    Ok(Json(json!({"name": model_name, "unload": true})))
}

/// Versioned unload targets the explicit version (§4.4).
pub async fn unload_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    unload_model_impl(&state, &model_name, &version).await?;
    Ok(Json(json!({
        "success": true,
        "message": format!("Model {} version {} unloaded", model_name, version),
    })))
}

// ===== Reload Model =====

/// Bare reload targets the **active** version (§4.4).
pub async fn reload_model_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let active = state.registry.get_active_version(&model_name).ok_or_else(|| {
        AppError::ModelNotFound(format!("{} has no active version", model_name))
    })?;
    reload_model_impl(&state, &model_name, &active).await
}

/// Versioned reload targets the explicit version (§4.4).
pub async fn reload_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    reload_model_impl(&state, &model_name, &version).await
}

async fn reload_model_impl(
    state: &AppState,
    model_name: &str,
    version: &str,
) -> Result<Json<Value>, AppError> {
    info!(model = %model_name, version = %version, "reload model requested");
    let success = state.worker_manager.reload_model(model_name, Some(version)).await?;
    if !success {
        return Err(AppError::ModelNotFound(format!(
            "{} version {} not loaded",
            model_name, version
        )));
    }
    Ok(Json(json!({
        "success": true,
        "message": format!("Model {} version {} reloaded", model_name, version),
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

/// Request body for `PUT /v2/models/:m/routing` (§4.3): atomically sets all
/// traffic weights; versions not listed get weight 0.
#[derive(Debug, serde::Deserialize)]
pub struct SetRoutingRequest {
    pub weights: HashMap<String, u32>,
}

pub async fn set_routing_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    ApiJson(body): ApiJson<SetRoutingRequest>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    for v in body.weights.keys() {
        crate::validation::validate_version(v)?;
    }
    info!(model = %model_name, weights = ?body.weights, "set routing weights requested");
    state.registry.set_weights(&model_name, &body.weights)?;
    // The gauge mirrors every loaded version; unlisted ones were zeroed.
    for mv in state.registry.list_versions(&model_name) {
        prometheus::set_version_weight(&model_name, &mv.version, mv.weight as f64);
    }
    Ok(Json(json!({
        "success": true,
        "weights": body.weights,
    })))
}

pub async fn activate_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    info!(model = %model_name, version = %version, "activate version requested");
    let previous = state.registry.get_active_version(&model_name);
    let success = state.registry.activate_version(&model_name, &version)?;
    if !success {
        return Err(AppError::ModelNotReady(format!(
            "Model {} version {} is not ready",
            model_name, version
        )));
    }
    // Explicit activate is a hard cutover (§4.3): the target version gets all
    // weighted traffic. Registry-level activate stays pointer-only so
    // internal re-activations (auto-recycle reload) never clobber a canary.
    state
        .registry
        .set_weights(&model_name, &HashMap::from([(version.clone(), 100u32)]))?;
    for mv in state.registry.list_versions(&model_name) {
        prometheus::set_version_weight(&model_name, &mv.version, mv.weight as f64);
    }
    // A switch is counted only when a different version was active before —
    // first activation and re-activation of the same version are not switches.
    if let Some(from) = previous {
        if from != version {
            prometheus::record_version_switch(&model_name, &from, &version);
        }
    }
    state.callback_runner.on_model_activate(&crate::callback::ModelLifecycleContext {
        model_name: model_name.clone(),
        version: version.clone(),
        device: None,
    }).await;
    Ok(Json(json!({
        "success": true,
        "message": format!("Model {} version {} is now active", model_name, version),
        "active_version": version,
    })))
}

// ===== B2 regression guard: ensemble detection in scan_repository =====

#[cfg(test)]
mod scan_repository_ensemble_tests {
    use super::*;

    /// Regression guard (fixed in e5e45f4): `scan_repository` used to detect
    /// ensemble models via `content.contains("ensemble:")` rather than
    /// structural YAML parsing. A config.yaml with "ensemble:" appearing
    /// ONLY in a YAML comment or description string was misclassified as
    /// `type: "ensemble"`, breaking the repository index API.
    #[tokio::test]
    async fn test_scan_repository_ensemble_detection_false_positive_on_comment() {
        let repo = std::env::temp_dir().join(format!(
            "lite-server-scan-ensemble-fp-{}",
            std::process::id()
        ));
        let model_dir = repo.join("test_model").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();

        // A model.py exists — this is a LitAPI model.
        tokio::fs::write(model_dir.join("model.py"), "# dummy model")
            .await
            .unwrap();

        // config.yaml where "ensemble:" appears only in a YAML comment.
        // A properly-structured check would classify this as LitAPI.
        tokio::fs::write(
            model_dir.join("config.yaml"),
            "# ensemble: this is only a comment, not a real ensemble\nmax_batch_size: 4\n",
        )
        .await
        .unwrap();

        let result = scan_repository(&repo).await;

        // The directory should be discovered (model.py exists).
        assert_eq!(result.len(), 1, "model with model.py must be discovered");

        let entry = &result[0];
        assert_eq!(entry["name"], "test_model");
        assert_eq!(entry["version"], "1");

        // Regression guard (fixed in e5e45f4): the string-contains check
        // would have matched the "ensemble:" comment and reported the type
        // as "ensemble"; structural detection reports "litapi".
        assert_eq!(
            entry["type"].as_str().unwrap(),
            "litapi",
            "model with model.py and 'ensemble:' only in a comment must be \
             classified as 'litapi', not '{}'.",
            entry["type"].as_str().unwrap_or("")
        );

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    /// B2b regression guard: no model.py, only config.yaml with "ensemble:"
    /// in a comment — structural detection must not treat this as an
    /// ensemble model (the pre-e5e45f4 string-contains check did).
    #[tokio::test]
    async fn test_scan_repository_ensemble_detection_pure_false_positive() {
        let repo = std::env::temp_dir().join(format!(
            "lite-server-scan-ensemble-fp2-{}",
            std::process::id()
        ));
        let model_dir = repo.join("no_py_model").join("1");
        tokio::fs::create_dir_all(&model_dir).await.unwrap();

        // config.yaml where "ensemble:" only appears in a YAML comment.
        // No model.py — a genuine ensemble would have `ensemble:` as a YAML
        // key; this has it only in a comment/description.
        tokio::fs::write(
            model_dir.join("config.yaml"),
            "description: \"This is NOT an ensemble\"\n# ensemble:\n#   steps: ...\nmax_batch_size: 4\n",
        )
        .await
        .unwrap();

        let result = scan_repository(&repo).await;

        // Regression guard (fixed in e5e45f4): the string-contains check
        // matched the "# ensemble:" comment and discovered this directory as
        // an ensemble model even though it has no model.py and the ensemble
        // key is commented out. Structural detection must skip it.
        assert!(
            result.is_empty(),
            "directory with 'ensemble:' only in a comment and no model.py \
             must NOT be discovered as a model. Got {} entry(s): {:?}",
            result.len(),
            result.iter().map(|e| format!("{}:{}", e["name"], e["version"])).collect::<Vec<_>>()
        );

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }
}
