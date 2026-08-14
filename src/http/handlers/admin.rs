use super::*;
use super::inference::resolve_version;
use crate::callback::Protocol;
use crate::error::AppError;
use crate::http::state::AppState;
use crate::metrics::prometheus;
use crate::registry::types::ModelType;
use crate::request_context::RequestContext;
use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Json},
};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{error, info, warn};

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
    let (resolved_version, _) = resolve_version(&state, &model_name, None, &headers).await?;
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
        // Legal model names cannot start with a dot (IDENTIFIER_RE in
        // validation.rs), so dot directories (.artifacts, .git, staging
        // dirs) can never be models — skip them.
        if model_name.starts_with('.') {
            continue;
        }

        let mut versions = Vec::new();
        if let Ok(mut version_entries) = tokio::fs::read_dir(&model_dir).await {
            while let Ok(Some(ventry)) = version_entries.next_entry().await {
                let version_dir = ventry.path();
                if !version_dir.is_dir() {
                    continue;
                }
                let version = version_dir.file_name().unwrap_or_default().to_string_lossy().to_string();
                // Same rule as model dirs: dot-prefixed names are staging or
                // backup leftovers, never model versions.
                if version.starts_with('.') {
                    continue;
                }
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
                // K2: the version lives inside the package manifest, not
                // the filename — report null instead of a fake "1".
                models.push(json!({
                    "name": path.file_stem().unwrap_or_default().to_string_lossy().to_string(),
                    "version": Value::Null,
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
    cx: RequestContext,
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
    let auto_activated = active.is_none();
    if auto_activated {
        state.registry.activate_version(&model_name, &version)?;
    }

    crate::audit::control_plane(
        Some(&cx),
        &state.access_control,
        Protocol::Http,
        "load",
        &model_name,
        Some(&version),
        &format!("loaded; auto_activated={auto_activated}"),
    );

    Ok(Json(json!({
        "success": true,
        "message": format!("Model {} version {} loaded", model_name, version),
    })))
}

// ===== Unload Model =====

async fn unload_model_impl(
    state: &AppState,
    cx: &RequestContext,
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

    crate::audit::control_plane(Some(cx), &state.access_control, Protocol::Http, "unload", model_name, Some(version), "unloaded");

    Ok(())
}

/// Bare unload targets the **active** version (§4.4): admin ops never follow
/// the weighted routing pick. 响应 KServe 形状 {"name","unload":true}(J2,
/// v2_endpoints.py:196-215 实证;bare 消费者是 KServe SDK)。
pub async fn unload_model_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    cx: RequestContext,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let active = state.registry.get_active_version(&model_name).ok_or_else(|| {
        AppError::ModelNotFound(format!("{} has no active version", model_name))
    })?;
    unload_model_impl(&state, &cx, &model_name, &active).await?;
    Ok(Json(json!({"name": model_name, "unload": true})))
}

/// Versioned unload targets the explicit version (§4.4).
pub async fn unload_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    cx: RequestContext,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    unload_model_impl(&state, &cx, &model_name, &version).await?;
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
    cx: RequestContext,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let active = state.registry.get_active_version(&model_name).ok_or_else(|| {
        AppError::ModelNotFound(format!("{} has no active version", model_name))
    })?;
    reload_model_impl(&state, &cx, &model_name, &active).await
}

/// Versioned reload targets the explicit version (§4.4).
pub async fn reload_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    cx: RequestContext,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    reload_model_impl(&state, &cx, &model_name, &version).await
}

async fn reload_model_impl(
    state: &AppState,
    cx: &RequestContext,
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
    crate::audit::control_plane(Some(cx), &state.access_control, Protocol::Http, "reload", model_name, Some(version), "reloaded");
    Ok(Json(json!({
        "success": true,
        "message": format!("Model {} version {} reloaded", model_name, version),
    })))
}

// ===== Delete Version =====

/// G5: remove the linked artifacts of a deleted version — the
/// scanner-placed root `.lma` and the F10a `.artifacts/` copy. Both use
/// the packer output naming convention `{name}_v{version}.lma`. Returns
/// the paths actually removed (for the audit record).
pub(crate) async fn remove_linked_artifacts(
    repo_path: &std::path::Path,
    model_name: &str,
    version: &str,
) -> Vec<String> {
    let artifact_name = format!("{}_v{}.lma", model_name, version);
    let mut removed = Vec::new();
    for dir in [repo_path.to_path_buf(), repo_path.join(".artifacts")] {
        let path = dir.join(&artifact_name);
        if path.is_file() && tokio::fs::remove_file(&path).await.is_ok() {
            removed.push(path.display().to_string());
        }
    }
    removed
}

/// Shared delete-version logic (HTTP handler, E2 batch delete and the
/// future gRPC DeleteVersion RPC all funnel through here). Unloads the
/// worker if loaded, removes the version directory, cleans the linked
/// artifacts (G5) and emits the control-plane audit.
pub(crate) async fn delete_version_impl(
    state: &AppState,
    cx: &RequestContext,
    model_name: &str,
    version: &str,
    force: bool,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(model_name)?;
    crate::validation::validate_version(version)?;

    info!(model = %model_name, version = %version, "delete version requested");
    // G4: capture the active pin BEFORE unload — unload clears it via
    // registry.remove (auto-fallback may already have moved the pointer).
    let was_active = state.registry.get_active_version(model_name).as_deref() == Some(version);
    // E3: deleting the active version is an accident-prone, traffic-shaping
    // mutation — refuse by default, force overrides (G4 then logs the
    // resulting active state below).
    if was_active && !force {
        return Err(AppError::Conflict(format!(
            "version {} of model {} is the active version; pass ?force=true to override",
            version, model_name
        )));
    }
    let was_loaded = state.registry.get(model_name, Some(version)).is_some();

    // Unload first if loaded. H5: a failed graceful unload must not block
    // the delete — escalate to force termination so no live worker
    // survives on a deleted directory.
    if was_loaded {
        if let Err(e) = state
            .worker_manager
            .unload_model(model_name, Some(version))
            .await
        {
            error!(
                model = %model_name, version = %version, error = %e,
                "graceful unload failed during delete; force-terminating workers"
            );
            state
                .worker_manager
                .force_unload_version(model_name, version)
                .await?;
        }
    }

    // Delete directory
    let version_dir = state.repo_path.join(model_name).join(version);
    if version_dir.exists() {
        tokio::fs::remove_dir_all(&version_dir)
            .await
            .map_err(AppError::Io)?;
    }

    // G5: linked artifact cleanup — the scanner-placed root .lma and the
    // .artifacts/ copy would otherwise resurrect the version on restart.
    let linked = remove_linked_artifacts(&state.repo_path, model_name, version).await;

    // G4: deleting the active version is a traffic-shaping event — make the
    // resulting active state explicit in the logs.
    if was_active {
        match state.registry.get_active_version(model_name) {
            Some(v) => info!(
                model = %model_name, version = %version, new_active = %v,
                "deleted active version; active pointer fell back to another ready version"
            ),
            None => warn!(
                model = %model_name, version = %version,
                "deleted active version; no active version remains"
            ),
        }
    }

    let details = if linked.is_empty() {
        "deleted".to_string()
    } else {
        format!("deleted; linked artifacts removed: {}", linked.join(", "))
    };

    // H8: rewrite the registry snapshot so a later re-upload of the same
    // version cannot inherit the deleted version's stale strategy/pins.
    // B3: the rewrite is post-commit housekeeping — its failure must not
    // mask the delete that already happened (a 500 here would misreport
    // reality, and E2 would list the deleted version under `failed`).
    if state.config.server.cache_registry {
        if let Err(e) = crate::registry::cache::save(&state.registry, &state.repo_path).await {
            warn!(
                model = %model_name, version = %version, error = %e,
                "registry snapshot rewrite after delete failed"
            );
        }
    }

    crate::audit::control_plane(
        Some(cx),
        &state.access_control,
        Protocol::Http,
        "delete",
        model_name,
        Some(version),
        &details,
    );

    Ok(Json(json!({
        "success": true,
        "message": format!("Model {} version {} deleted", model_name, version),
    })))
}

/// E3: query params for delete endpoints (?force=true overrides the
/// active-version protection).
#[derive(Debug, Default, serde::Deserialize)]
pub struct DeleteQuery {
    pub force: Option<bool>,
}

pub async fn delete_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    Query(query): Query<DeleteQuery>,
    cx: RequestContext,
) -> Result<Json<Value>, AppError> {
    delete_version_impl(&state, &cx, &model_name, &version, query.force.unwrap_or(false)).await
}

// ===== Repository Drift (E4) =====

/// E4: configured version set for a model — the explicit references
/// (versions_to_load / default_version / weights keys), adjusted for the
/// dynamic load policies the same way reconcile computes its target
/// ("all" = every disk version, "latest" = the highest disk version).
fn configured_versions(
    strategy: Option<&crate::config::ModelStrategyConfig>,
    disk: &[String],
) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    let Some(s) = strategy else { return set };
    set.extend(s.versions_to_load.iter().cloned());
    if let Some(d) = &s.default_version {
        set.insert(d.clone());
    }
    if let Some(w) = &s.weights {
        set.extend(w.keys().cloned());
    }
    match s.load_policy.as_str() {
        "all" => set.extend(disk.iter().cloned()),
        "latest" => {
            if let Some(latest) = crate::server::pick_latest_version(disk) {
                set.insert(latest);
            }
        }
        _ => {}
    }
    set
}

/// Model directories on disk (dot-prefixed entries skipped).
async fn disk_models(repo_path: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    let Ok(mut entries) = tokio::fs::read_dir(repo_path).await else {
        return out;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if let Some(name) = path.file_name().map(|n| n.to_string_lossy().to_string()) {
            if !name.starts_with('.') {
                out.push(name);
            }
        }
    }
    out
}

/// Recursive size of a directory (iterative — async fn recursion needs
/// boxing). Missing dir = 0.
async fn dir_size_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
            continue;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if let Ok(md) = tokio::fs::metadata(&p).await {
                total += md.len();
            }
        }
    }
    total
}

/// (model, version) pairs referenced by any ensemble chain node on disk.
/// Parsed structurally — a chain referencing a deleted version must still
/// be REPORTED here, which is exactly what this endpoint exists for.
async fn ensemble_referenced_pairs(repo_path: &std::path::Path) -> BTreeSet<(String, String)> {
    let mut refs = BTreeSet::new();
    for model in disk_models(repo_path).await {
        for version in disk_versions(repo_path, &model).await {
            let cfg_path = repo_path.join(&model).join(&version).join("config.yaml");
            let Ok(content) = tokio::fs::read_to_string(&cfg_path).await else {
                continue;
            };
            if !crate::config::config_content_is_ensemble(&content) {
                continue;
            }
            let Ok(cfg) = serde_yaml::from_str::<crate::ensemble::EnsembleConfig>(&content) else {
                continue;
            };
            let mut steps: Vec<&crate::ensemble::EnsembleStepRaw> =
                cfg.ensemble.steps.iter().collect();
            if let Some(dags) = &cfg.ensemble.dags {
                for set in dags.values() {
                    steps.extend(set.steps.iter());
                }
            }
            for step in steps {
                if let Some(v) = &step.version {
                    refs.insert((step.model.clone(), v.clone()));
                }
            }
        }
    }
    refs
}

/// E4: config↔disk drift report (read-only). `configured_missing` lists
/// versions the configuration references but that are absent on disk;
/// `on_disk_unconfigured` lists disk versions nothing references (retire
/// candidates in explicit mode), with size and ensemble-chain-reference
/// hints to support the decision.
pub async fn repository_drift_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, AppError> {
    let orch = &state.config.orchestration;
    let ensemble_refs = ensemble_referenced_pairs(&state.repo_path).await;

    let mut names: BTreeSet<String> = BTreeSet::new();
    names.extend(orch.load_models.iter().cloned());
    names.extend(orch.models.iter().map(|s| s.name.clone()));
    names.extend(disk_models(&state.repo_path).await);

    let mut configured_missing: Vec<Value> = Vec::new();
    let mut on_disk_unconfigured: Vec<Value> = Vec::new();
    for name in names {
        let disk = disk_versions(&state.repo_path, &name).await;
        let disk_set: BTreeSet<String> = disk.iter().cloned().collect();
        let strategy = orch.models.iter().find(|s| s.name == name);
        let in_scope = orch.load_models.iter().any(|m| m == &name) || strategy.is_some();
        let configured: BTreeSet<String> = match strategy {
            // Legacy: a load_models entry without a strategy loads every
            // disk version, so nothing can be missing or unconfigured.
            None if in_scope => disk_set.clone(),
            _ => configured_versions(strategy, &disk),
        };

        for v in configured.difference(&disk_set) {
            configured_missing.push(json!({ "model": name, "version": v }));
        }
        for v in &disk {
            if configured.contains(v) {
                continue;
            }
            on_disk_unconfigured.push(json!({
                "model": name,
                "version": v,
                "size_bytes": dir_size_bytes(&state.repo_path.join(&name).join(v)).await,
                "ensemble_referenced": ensemble_refs.contains(&(name.clone(), v.clone())),
            }));
        }
    }

    let sort_key = |e: &Value| {
        (
            e["model"].as_str().unwrap_or("").to_string(),
            e["version"].as_str().unwrap_or("").to_string(),
        )
    };
    configured_missing.sort_by_key(&sort_key);
    on_disk_unconfigured.sort_by_key(&sort_key);

    Ok(Json(json!({
        "configured_missing": configured_missing,
        "on_disk_unconfigured": on_disk_unconfigured,
    })))
}

// ===== Delete Versions (batch retire, E2) =====

/// E2: versions to delete for `keep=N` — all but the N highest. Semver-
/// lenient comparison (same parser as `load_policy: latest`); versions that
/// do not parse count as lowest and are deleted first.
fn versions_to_delete_keep(versions: &[String], keep: usize) -> Vec<String> {
    if versions.len() <= keep {
        return Vec::new();
    }
    let mut ranked: Vec<(&String, Option<semver::Version>)> = versions
        .iter()
        .map(|v| (v, crate::server::parse_lenient_semver(v)))
        .collect();
    ranked.sort_by(|a, b| a.1.cmp(&b.1));
    ranked
        .iter()
        .take(versions.len() - keep)
        .map(|(v, _)| (*v).clone())
        .collect()
}

/// Disk versions of a model directory — same acceptance rule as
/// scan_repository: `model.py` present, or an ensemble `config.yaml`.
/// Dot-prefixed entries (staging leftovers) are skipped.
async fn disk_versions(repo_path: &std::path::Path, model_name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let Ok(mut entries) = tokio::fs::read_dir(repo_path.join(model_name)).await else {
        return out;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().map(|n| n.to_string_lossy().to_string()) else {
            continue;
        };
        if name.starts_with('.') {
            continue;
        }
        let mut is_ensemble = false;
        let config_yaml = path.join("config.yaml");
        if config_yaml.exists() {
            if let Ok(content) = tokio::fs::read_to_string(&config_yaml).await {
                is_ensemble = crate::config::config_content_is_ensemble(&content);
            }
        }
        if path.join("model.py").exists() || is_ensemble {
            out.push(name);
        }
    }
    out
}

/// E2: request body for batch retire — `keep` (retain the N highest
/// versions) or an explicit `versions` list.
#[derive(Debug, serde::Deserialize)]
pub struct DeleteVersionsRequest {
    pub keep: Option<usize>,
    pub versions: Option<Vec<String>>,
}

pub async fn delete_versions_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    Query(query): Query<DeleteQuery>,
    cx: RequestContext,
    ApiJson(body): ApiJson<DeleteVersionsRequest>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let force = query.force.unwrap_or(false);

    let to_delete: Vec<String> = match (body.keep, body.versions) {
        (Some(keep), None) => {
            if keep == 0 {
                return Err(AppError::Validation("keep must be >= 1".to_string()));
            }
            versions_to_delete_keep(&disk_versions(&state.repo_path, &model_name).await, keep)
        }
        (None, Some(versions)) => {
            for v in &versions {
                crate::validation::validate_version(v)?;
            }
            versions
        }
        _ => {
            return Err(AppError::Validation(
                "body must contain either \"keep\" or \"versions\"".to_string(),
            ))
        }
    };

    // Partial-failure semantics: run per version, any success → 200; only
    // an all-failure batch is an error.
    let mut deleted: Vec<String> = Vec::new();
    let mut failed: Vec<Value> = Vec::new();
    for version in to_delete {
        match delete_version_impl(&state, &cx, &model_name, &version, force).await {
            Ok(_) => deleted.push(version),
            Err(e) => failed.push(json!({ "version": version, "error": e.to_string() })),
        }
    }

    if deleted.is_empty() && !failed.is_empty() {
        let summary = failed
            .iter()
            .map(|f| format!("{}: {}", f["version"], f["error"]))
            .collect::<Vec<_>>()
            .join("; ");
        return Err(AppError::Internal(format!(
            "all {} version(s) failed to delete: {}",
            failed.len(),
            summary
        )));
    }

    Ok(Json(json!({ "deleted": deleted, "failed": failed })))
}

// ===== Delete Model (E1) =====

/// E1/G5: remove every linked artifact of a model — all
/// `.artifacts/{name}_v*.lma` copies plus root `{name}_v*.lma` files
/// (filename-convention matching, per the plan). Returns the paths
/// actually removed (for the audit record).
async fn remove_model_artifacts(repo_path: &std::path::Path, model_name: &str) -> Vec<String> {
    let mut removed = Vec::new();
    let prefix = format!("{}_v", model_name);
    for dir in [repo_path.to_path_buf(), repo_path.join(".artifacts")] {
        let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
            continue;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(fname) = path.file_name().map(|n| n.to_string_lossy().to_string()) else {
                continue;
            };
            if fname.starts_with(&prefix)
                && fname.ends_with(".lma")
                && tokio::fs::remove_file(&path).await.is_ok()
            {
                removed.push(path.display().to_string());
            }
        }
    }
    removed
}

pub async fn delete_model_handler(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
    Query(query): Query<DeleteQuery>,
    cx: RequestContext,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    let force = query.force.unwrap_or(false);

    info!(model = %model_name, "delete model requested");
    // E3: whole-model delete is the biggest accident surface — refuse while
    // an active version exists unless forced.
    if state.registry.get_active_version(&model_name).is_some() && !force {
        return Err(AppError::Conflict(format!(
            "model {} has an active version; pass ?force=true to override",
            model_name
        )));
    }

    // Unload every loaded version (registry-driven — disk-only versions are
    // covered by the directory removal below).
    let loaded: Vec<String> = state
        .registry
        .list_versions(&model_name)
        .iter()
        .map(|mv| mv.version.clone())
        .collect();
    for version in &loaded {
        state
            .worker_manager
            .unload_model(&model_name, Some(version))
            .await?;
    }

    // Delete the whole model directory (idempotent when absent).
    let model_dir = state.repo_path.join(&model_name);
    if model_dir.exists() {
        tokio::fs::remove_dir_all(&model_dir)
            .await
            .map_err(AppError::Io)?;
    }

    // G5: linked artifacts — root .lma files would resurrect the model via
    // auto-unpack on restart; .artifacts/ copies are F10a's.
    let removed = remove_model_artifacts(&state.repo_path, &model_name).await;

    let details = if removed.is_empty() {
        "model deleted".to_string()
    } else {
        format!("model deleted; linked artifacts removed: {}", removed.join(", "))
    };

    // H8: same snapshot rewrite as the version-level delete — no ghost
    // strategy/pins for the deleted model. B3: best-effort housekeeping —
    // a rewrite failure must not mask the committed delete.
    if state.config.server.cache_registry {
        if let Err(e) = crate::registry::cache::save(&state.registry, &state.repo_path).await {
            warn!(
                model = %model_name, error = %e,
                "registry snapshot rewrite after model delete failed"
            );
        }
    }

    crate::audit::control_plane(
        Some(&cx),
        &state.access_control,
        Protocol::Http,
        "delete_model",
        &model_name,
        None,
        &details,
    );

    Ok(Json(json!({
        "success": true,
        "message": format!("Model {} deleted", model_name),
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
    cx: RequestContext,
    ApiJson(body): ApiJson<SetRoutingRequest>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    for v in body.weights.keys() {
        crate::validation::validate_version(v)?;
    }
    info!(model = %model_name, weights = ?body.weights, "set routing weights requested");
    // D27 前后值：变更前的权重分布（list_versions 只含已加载版本；未列出的
    // 版本权重为 0）。
    let before: HashMap<String, u32> = state
        .registry
        .list_versions(&model_name)
        .into_iter()
        .map(|mv| (mv.version.clone(), mv.weight))
        .collect();
    state.registry.set_weights(&model_name, &body.weights)?;
    // The gauge mirrors every loaded version; unlisted ones were zeroed.
    for mv in state.registry.list_versions(&model_name) {
        prometheus::set_version_weight(&model_name, &mv.version, mv.weight as f64);
    }
    crate::audit::control_plane(
        Some(&cx),
        &state.access_control,
        Protocol::Http,
        "set_routing",
        &model_name,
        None,
        &format!("weights {before:?} -> {:?}", body.weights),
    );
    Ok(Json(json!({
        "success": true,
        "weights": body.weights,
    })))
}

pub async fn activate_version_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    cx: RequestContext,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;

    info!(model = %model_name, version = %version, "activate version requested");
    let previous = state.registry.get_active_version(&model_name);
    let success = state.registry.activate_version(&model_name, &version)?;
    if !success {
        // D27 失败也审计（与 gRPC Admin activate 对齐）。
        crate::audit::control_plane(
            Some(&cx),
            &state.access_control,
            Protocol::Http,
            "activate",
            &model_name,
            Some(&version),
            &format!("failed: not ready; previous_active={previous:?}"),
        );
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
    if let Some(from) = previous.as_ref() {
        if from != &version {
            prometheus::record_version_switch(&model_name, from, &version);
        }
    }
    state.callback_runner.on_model_activate(&crate::callback::ModelLifecycleContext {
        model_name: model_name.clone(),
        version: version.clone(),
        device: None,
    }).await;
    crate::audit::control_plane(
        Some(&cx),
        &state.access_control,
        Protocol::Http,
        "activate",
        &model_name,
        Some(&version),
        &format!("previous_active={previous:?} -> {version}"),
    );
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

    /// K2: root `.lma` entries in the repository index must report
    /// version null — the version lives inside the package manifest, not
    /// the filename — instead of the hardcoded "1".
    #[tokio::test]
    async fn test_scan_repository_artifact_entry_version_null() {
        let repo = std::env::temp_dir().join(format!(
            "lite-server-index-artifact-null-{}",
            std::process::id()
        ));
        let _ = tokio::fs::remove_dir_all(&repo).await;
        tokio::fs::create_dir_all(&repo).await.unwrap();
        tokio::fs::write(repo.join("mymodel_v2.lma"), b"fake")
            .await
            .unwrap();

        let result = scan_repository(&repo).await;
        let entry = result
            .iter()
            .find(|e| e["type"] == "artifact")
            .expect("artifact entry must be listed");
        assert!(
            entry["version"].is_null(),
            "artifact entries must report version null: {entry}"
        );

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    /// K1: `scan_repository` must skip dot-prefixed directories at the
    /// model level — staging dirs (`.tmp-upload-*`, `.artifacts`, VCS dirs)
    /// can never be models and must not leak into the repository index.
    #[tokio::test]
    async fn test_scan_repository_skips_dot_directories() {
        let repo = std::env::temp_dir().join(format!(
            "lite-server-index-dotskip-{}",
            std::process::id()
        ));
        let _ = tokio::fs::remove_dir_all(&repo).await;

        // A staging dir shaped like a model: version dir with model.py.
        let staging = repo.join(".tmp-upload-abc").join("1");
        tokio::fs::create_dir_all(&staging).await.unwrap();
        tokio::fs::write(staging.join("model.py"), "def predict(x): return x")
            .await
            .unwrap();

        // An artifacts dir (F10a target) with a fake model dir inside.
        let artifacts = repo.join(".artifacts").join("fake").join("1");
        tokio::fs::create_dir_all(&artifacts).await.unwrap();
        tokio::fs::write(artifacts.join("model.py"), "def predict(x): return x")
            .await
            .unwrap();

        // A real model must still be indexed.
        let real = repo.join("real_model").join("1");
        tokio::fs::create_dir_all(&real).await.unwrap();
        tokio::fs::write(real.join("model.py"), "def predict(x): return x")
            .await
            .unwrap();

        let result = scan_repository(&repo).await;

        assert_eq!(
            result.len(),
            1,
            "dot directories must be skipped, got {:?}",
            result.iter().map(|e| format!("{}:{}", e["name"], e["version"])).collect::<Vec<_>>()
        );
        assert_eq!(result[0]["name"], "real_model");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }
}

#[cfg(test)]
pub(super) mod audit_tests {
    //! D27（对账 A2②）：HTTP admin 控制面 mutation 必须产出结构化审计记录
    //! （action/model/version/request_id/client_ip/principal/key_fingerprint/
    //! details 含前后值），与 gRPC Admin 同形状。
    use super::*;
    use crate::access_control::AccessControl;
    use crate::callback::CallbackRunner;
    use crate::config::{AccessControlConfig, EndpointControl, ModelConfig, ProtocolControl};
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;
    use std::sync::atomic::AtomicBool;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Rec {
        fields: Vec<(String, String)>,
    }

    /// 捕获 lite_server::audit 目标事件的字段（G3 同款：scoped dispatch +
    /// rebuild_interest_cache，防 callsite interest 缓存 NEVER 短路）。
    struct AuditLayer(Arc<Mutex<Rec>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AuditLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() != "lite_server::audit" {
                return;
            }
            struct V<'a>(&'a mut Vec<(String, String)>);
            impl tracing::field::Visit for V<'_> {
                fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                    self.0.push((f.name().to_string(), format!("{v:?}")));
                }
                fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                    self.0.push((f.name().to_string(), v.to_string()));
                }
            }
            let mut v = V(&mut self.0.lock().unwrap().fields);
            event.record(&mut v);
        }
    }

    pub(crate) fn test_state(model: &str) -> Arc<AppState> {
        let registry = Arc::new(ModelRegistry::new());
        let dir = std::env::temp_dir().join(format!("lite-server-audit-{}-{}", model, std::process::id()));
        for v in ["1", "2"] {
            registry
                .register(
                    model,
                    v,
                    ModelConfig { max_batch_size: 1, ..Default::default() },
                    ModelType::LitAPI,
                    dir.clone(),
                )
                .unwrap();
        }
        registry.set_weights(model, &HashMap::from([("1".to_string(), 100u32)])).unwrap();
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::env::temp_dir(),
            Arc::new(InferenceQueue::new()),
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        let mut state = AppState::new(
            registry.clone(),
            wm.clone(),
            wm.inference_queue().clone(),
            crate::config::Config::default(),
            std::env::temp_dir(),
            Arc::new(CallbackRunner::new()),
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        );
        state.access_control = key_ac();
        Arc::new(state)
    }

    fn key_ac() -> Arc<AccessControl> {
        Arc::new(
            AccessControl::build(&AccessControlConfig {
                admin: ProtocolControl {
                    http: Some(EndpointControl::Key {
                        key: "x-admin-key".to_string(),
                        value: Some("audit-secret".to_string()),
                        value_env: None,
                        value_file: None,
                    }),
                    grpc: None,
                },
                ..Default::default()
            })
            .unwrap(),
        )
    }

    pub(crate) fn test_cx() -> RequestContext {
        RequestContext {
            request_id: "audit-rid".to_string(),
            client_ip: "127.0.0.1".to_string(),
            trace_cx: opentelemetry::Context::new(),
            protocol: Protocol::Http,
            principal: None,
            api_protocol: None,
        }
    }

    /// 在 scoped dispatch 线程内跑 handler，返回捕获到的字段集。
    pub(crate) fn run_captured<Fut: std::future::Future<Output = ()>>(f: impl FnOnce() -> Fut + Send + 'static) -> Vec<(String, String)> {
        use tracing_subscriber::layer::SubscriberExt;
        let rec: Arc<Mutex<Rec>> = Default::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(AuditLayer(rec.clone())));
        let handle = std::thread::spawn(move || {
            let _guard = tracing::dispatcher::set_default(&dispatch);
            tracing::callsite::rebuild_interest_cache();
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(f());
        });
        handle.join().unwrap();
        let mut guard = rec.lock().unwrap();
        std::mem::take(&mut guard.fields)
    }

    fn field<'a>(fields: &'a [(String, String)], name: &str) -> Option<&'a str> {
        fields.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str())
    }

    #[test]
    fn set_routing_emits_structured_audit_with_fingerprint_and_before_after() {
        let fields = run_captured(|| async {
            let state = test_state("audit_rt");
            let _resp = set_routing_handler(
                State(state),
                Path("audit_rt".to_string()),
                test_cx(),
                ApiJson(SetRoutingRequest { weights: HashMap::from([("2".to_string(), 100u32)]) }),
            )
            .await
            .expect("set_routing 应成功");
        });
        assert_eq!(field(&fields, "action"), Some("set_routing"), "{fields:?}");
        assert_eq!(field(&fields, "model"), Some("audit_rt"));
        assert_eq!(field(&fields, "request_id"), Some("audit-rid"));
        let details = field(&fields, "details").unwrap_or("");
        assert!(details.contains("->"), "details 须含前后值: {details}");
        assert!(details.contains("\"1\": 100"), "须含变更前权重: {details}");
        assert!(details.contains("\"2\": 100"), "须含变更后权重: {details}");
        let fp = field(&fields, "key_fingerprint").unwrap_or("");
        assert!(fp.contains("Some("), "key 模式必须有指纹: {fp}");
        assert!(!fp.contains("audit-secret"), "指纹不得含密钥明文: {fp}");
    }

    #[test]
    fn activate_not_ready_emits_failure_audit() {
        let fields = run_captured(|| async {
            let state = test_state("audit_act");
            // 版本 "2" 已注册但非 ready → activate 失败也须审计（gRPC parity）
            let _ = activate_version_handler(
                State(state),
                Path(("audit_act".to_string(), "2".to_string())),
                test_cx(),
            )
            .await;
        });
        assert_eq!(field(&fields, "action"), Some("activate"), "{fields:?}");
        let details = field(&fields, "details").unwrap_or("");
        assert!(details.contains("failed: not ready"), "失败审计须含原因: {details}");
    }

    /// G1: delete must emit a `control_plane` audit record with
    /// action=delete and the model/version fields.
    #[test]
    fn delete_version_emits_structured_audit() {
        let fields = run_captured(|| async {
            let state = test_state("audit_del");
            let _resp = delete_version_handler(
                State(state),
                Path(("audit_del".to_string(), "1".to_string())),
                Query(DeleteQuery { force: None }),
                test_cx(),
            )
            .await
            .expect("delete must succeed");
        });
        assert_eq!(field(&fields, "action"), Some("delete"), "{fields:?}");
        assert_eq!(field(&fields, "model"), Some("audit_del"), "{fields:?}");
        assert_eq!(
            field(&fields, "version"),
            Some("Some(\"1\")"),
            "version must be recorded: {fields:?}"
        );
    }

    /// E3: deleting the active version is refused (409) unless force.
    #[test]
    fn delete_active_version_without_force_is_conflict() {
        run_captured(|| async {
            let state = test_state("audit_del_cf");
            state.registry.force_pin_active_version("audit_del_cf", "1");
            let resp = delete_version_handler(
                State(state),
                Path(("audit_del_cf".to_string(), "1".to_string())),
                Query(DeleteQuery { force: None }),
                test_cx(),
            )
            .await;
            let err = resp.expect_err("delete of active version without force must 409");
            assert_eq!(err.http_status(), axum::http::StatusCode::CONFLICT, "{err:?}");
            assert_eq!(err.error_code(), "conflict");
            assert!(
                format!("{err}").contains("force"),
                "conflict message must mention the force override: {err}"
            );
        });
    }

    /// E3: with force the active version is deleted and the active pointer
    /// falls back to another ready version.
    #[test]
    fn delete_active_version_with_force_succeeds_and_falls_back() {
        run_captured(|| async {
            let state = test_state("audit_del_force");
            state.registry.mark_ready("audit_del_force", "2").unwrap();
            state.registry.force_pin_active_version("audit_del_force", "1");
            let resp = delete_version_handler(
                State(state.clone()),
                Path(("audit_del_force".to_string(), "1".to_string())),
                Query(DeleteQuery { force: Some(true) }),
                test_cx(),
            )
            .await
            .expect("forced delete must succeed");
            assert_eq!(resp["success"], true);
            assert_eq!(
                state.registry.get_active_version("audit_del_force"),
                Some("2".to_string()),
                "active pointer must fall back to another ready version"
            );
        });
    }
}

/// E2: batch retire — keep=N semver-lenient sorting (unparseable versions
/// count as lowest, per the plan).
#[cfg(test)]
mod delete_model_tests {
    use super::*;

    /// E1: model-level delete must emit a `control_plane` audit record with
    /// action=delete_model (version is None — whole model).
    #[test]
    fn delete_model_emits_structured_audit() {
        // Reuse the audit_tests capture helpers (same file, sibling mod).
        let fields = audit_tests::run_captured(|| async {
            let state = audit_tests::test_state("audit_del_m");
            let _resp = delete_model_handler(
                State(state),
                Path("audit_del_m".to_string()),
                Query(DeleteQuery { force: None }),
                audit_tests::test_cx(),
            )
            .await
            .expect("model delete must succeed");
        });
        assert_eq!(fields.iter().find(|(n, _)| n == "action").map(|(_, v)| v.as_str()), Some("delete_model"), "{fields:?}");
        assert_eq!(fields.iter().find(|(n, _)| n == "model").map(|(_, v)| v.as_str()), Some("audit_del_m"), "{fields:?}");
    }
}

/// E4: drift report — pure helper semantics + handler-level coverage.
#[cfg(test)]
mod drift_tests {
    use super::*;
    use crate::config::{Config, ModelStrategyConfig, OrchestrationConfig};
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn configured_versions_collects_all_reference_sources() {
        let s = ModelStrategyConfig {
            name: "m".into(),
            load_policy: "explicit".into(),
            versions_to_load: vec!["1".into(), "2".into()],
            default_version: Some("3".into()),
            weights: Some(HashMap::from([("4".into(), 50u32)])),
            ..Default::default()
        };
        let got = configured_versions(Some(&s), &["2".into(), "9".into()]);
        assert_eq!(
            got,
            BTreeSet::from(["1".into(), "2".into(), "3".into(), "4".into()])
        );
    }

    #[test]
    fn configured_versions_all_policy_covers_disk() {
        let s = ModelStrategyConfig {
            name: "m".into(),
            load_policy: "all".into(),
            ..Default::default()
        };
        let got = configured_versions(Some(&s), &["1".into(), "7".into()]);
        assert_eq!(got, BTreeSet::from(["1".into(), "7".into()]));
    }

    #[test]
    fn configured_versions_latest_policy_picks_highest() {
        let s = ModelStrategyConfig {
            name: "m".into(),
            load_policy: "latest".into(),
            ..Default::default()
        };
        let got = configured_versions(Some(&s), &["1".into(), "10".into()]);
        assert_eq!(got, BTreeSet::from(["10".into()]));
    }

    pub(super) fn state_with_repo(config: Config, repo: &std::path::Path) -> Arc<AppState> {
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(crate::callback::CallbackRunner::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            repo.to_path_buf(),
            inference_queue.clone(),
            "warn".to_string(),
            callback_runner.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            wm,
            inference_queue,
            config,
            repo.to_path_buf(),
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    pub(super) fn config_with(orch: OrchestrationConfig) -> Config {
        Config {
            orchestration: orch,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn drift_reports_missing_and_unconfigured_with_ensemble_hint() {
        let repo = std::env::temp_dir().join(format!("lite-server-drift-{}", std::process::id()));
        let _ = tokio::fs::remove_dir_all(&repo).await;

        // Disk: mymodel/2 (configured), mymodel/3 (unconfigured + ensemble
        // chain-referenced), chain/1 (an ensemble whose node references
        // mymodel/3 — itself unconfigured because it has no config entry).
        tokio::fs::create_dir_all(repo.join("mymodel").join("2")).await.unwrap();
        tokio::fs::write(repo.join("mymodel").join("2").join("model.py"), "x = 1").await.unwrap();
        tokio::fs::create_dir_all(repo.join("mymodel").join("3")).await.unwrap();
        tokio::fs::write(repo.join("mymodel").join("3").join("model.py"), "x = 2").await.unwrap();
        tokio::fs::write(repo.join("mymodel").join("3").join("data.txt"), "0123456789").await.unwrap();
        tokio::fs::create_dir_all(repo.join("chain").join("1")).await.unwrap();
        tokio::fs::write(
            repo.join("chain").join("1").join("config.yaml"),
            "ensemble:\n  steps:\n    - name: s1\n      model: mymodel\n      version: \"3\"\n      inputs: {}\n",
        )
        .await
        .unwrap();

        let orch = OrchestrationConfig {
            control_mode: "explicit".into(),
            poll_interval: 30,
            load_models: vec!["mymodel".into()],
            models: vec![ModelStrategyConfig {
                name: "mymodel".into(),
                load_policy: "explicit".into(),
                versions_to_load: vec!["1".into(), "2".into()],
                ..Default::default()
            }],
        };
        let state = state_with_repo(config_with(orch), &repo);
        let resp = repository_drift_handler(State(state)).await.unwrap();

        let missing = resp["configured_missing"].as_array().unwrap();
        assert_eq!(missing.len(), 1, "{resp:?}");
        assert_eq!(missing[0]["model"], "mymodel");
        assert_eq!(missing[0]["version"], "1");

        let unconfigured = resp["on_disk_unconfigured"].as_array().unwrap();
        assert_eq!(unconfigured.len(), 2, "{resp:?}");
        let m3 = unconfigured
            .iter()
            .find(|e| e["model"] == "mymodel")
            .expect("mymodel/3 must be listed");
        assert_eq!(m3["version"], "3");
        assert_eq!(m3["size_bytes"], 15, "5-byte model.py + 10-byte data.txt");
        assert_eq!(m3["ensemble_referenced"], true, "chain node references mymodel/3");
        let chain1 = unconfigured
            .iter()
            .find(|e| e["model"] == "chain")
            .expect("chain/1 must be listed");
        assert_eq!(chain1["ensemble_referenced"], false);

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn drift_empty_when_config_matches_disk() {
        let repo = std::env::temp_dir().join(format!("lite-server-drift-ok-{}", std::process::id()));
        let _ = tokio::fs::remove_dir_all(&repo).await;
        tokio::fs::create_dir_all(repo.join("mymodel").join("1")).await.unwrap();
        tokio::fs::write(repo.join("mymodel").join("1").join("model.py"), "x = 1").await.unwrap();

        // load_models without a strategy: every disk version is configured.
        let orch = OrchestrationConfig {
            control_mode: "explicit".into(),
            poll_interval: 30,
            load_models: vec!["mymodel".into()],
            models: vec![],
        };
        let state = state_with_repo(config_with(orch), &repo);
        let resp = repository_drift_handler(State(state)).await.unwrap();
        assert_eq!(resp["configured_missing"], json!([]), "{resp:?}");
        assert_eq!(resp["on_disk_unconfigured"], json!([]), "{resp:?}");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }
}

/// H8: delete must rewrite the on-disk registry snapshot so a later
/// re-upload of the same version cannot inherit stale strategy/pins.
#[cfg(test)]
mod delete_snapshot_tests {
    use super::*;
    use crate::config::{Config, ModelConfig};
    use crate::registry::types::ModelType;

    fn cache_on_state(repo: &std::path::Path) -> Arc<AppState> {
        let mut config = Config::default();
        config.server.cache_registry = true;
        drift_tests::state_with_repo(config, repo)
    }

    #[tokio::test]
    async fn delete_version_rewrites_snapshot_without_deleted_version() {
        let repo =
            std::env::temp_dir().join(format!("lite-server-h8-{}", std::process::id()));
        let _ = tokio::fs::remove_dir_all(&repo).await;
        tokio::fs::create_dir_all(repo.join("mymodel").join("1"))
            .await
            .unwrap();
        tokio::fs::write(repo.join("mymodel").join("1").join("model.py"), "x = 1")
            .await
            .unwrap();

        let state = cache_on_state(&repo);
        state
            .registry
            .register("mymodel", "1", ModelConfig::default(), ModelType::LitAPI, repo.join("mymodel").join("1"))
            .unwrap();
        state
            .registry
            .set_weights("mymodel", &HashMap::from([("1".into(), 80u32)]))
            .unwrap();

        // Pre-write a snapshot carrying the version (what a previous run
        // would have left behind).
        crate::registry::cache::save(&state.registry, &repo).await.unwrap();

        let cx = audit_tests::test_cx();
        let _ = delete_version_impl(&state, &cx, "mymodel", "1", true)
            .await
            .expect("delete must succeed");

        let raw = std::fs::read_to_string(repo.join(".lite-server-registry.json"))
            .expect("snapshot must be rewritten");
        let snap: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert!(
            snap["models"].get("mymodel").is_none(),
            "deleted version must not linger in the snapshot: {snap}"
        );
        assert!(
            snap["active_versions"].get("mymodel").is_none(),
            "deleted version must not linger as a pin: {snap}"
        );

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }
}

/// E2: batch retire — keep=N semver-lenient sorting (unparseable versions
/// count as lowest, per the plan).
#[cfg(test)]
mod delete_versions_tests {
    use super::*;

    #[test]
    fn keep_two_deletes_only_the_lowest() {
        assert_eq!(
            versions_to_delete_keep(&["1".to_string(), "2".to_string(), "10".to_string()], 2),
            vec!["1".to_string()],
            "semver: 10 > 2 > 1, so keep=2 deletes 1"
        );
    }

    #[test]
    fn keep_one_handles_v_prefixes_semantically() {
        assert_eq!(
            versions_to_delete_keep(&["v2".to_string(), "v10".to_string()], 1),
            vec!["v2".to_string()],
            "v10 > v2 semantically, keep=1 deletes v2"
        );
    }

    #[test]
    fn keep_treats_unparseable_versions_as_lowest() {
        assert_eq!(
            versions_to_delete_keep(&["foo".to_string(), "1".to_string(), "2".to_string()], 2),
            vec!["foo".to_string()],
            "unparseable versions count as lowest and are deleted first"
        );
    }

    #[test]
    fn keep_larger_than_version_count_deletes_nothing() {
        assert_eq!(
            versions_to_delete_keep(&["1".to_string(), "2".to_string()], 5),
            Vec::<String>::new(),
            "keep >= version count must delete nothing"
        );
    }
}

#[cfg(test)]
mod delete_snapshot_failure_tests {
    use super::*;
    use crate::config::Config;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;
    use std::sync::atomic::AtomicBool;

    fn state_with_repo(repo_path: std::path::PathBuf, config: Config) -> Arc<AppState> {
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
            config,
            repo_path,
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    /// Audit B3 (error-path assumption): H8 rewrites the registry snapshot
    /// AFTER the version directory has been removed, and the rewrite's
    /// failure propagates through `?` — a successful delete is reported as
    /// a 500. The response then misrepresents reality (in an E2 batch the
    /// already-deleted version would even be listed under `failed`). The
    /// snapshot write is best-effort housekeeping; it must not mask the
    /// committed mutation.
    #[tokio::test]
    async fn test_delete_version_snapshot_failure_does_not_mask_delete() {
        let tmp = std::env::temp_dir().join(format!(
            "lite-server-del-snapshot-{}",
            std::process::id()
        ));
        let _ = tokio::fs::remove_dir_all(&tmp).await;
        let version_dir = tmp.join("mymodel").join("1");
        tokio::fs::create_dir_all(&version_dir).await.unwrap();
        tokio::fs::write(version_dir.join("model.py"), "def predict(x): return x\n")
            .await
            .unwrap();

        let mut config = Config::default();
        config.server.cache_registry = true;
        // Block the snapshot destination: rename(tmp, dest) onto an
        // existing directory fails, so cache::save returns an error.
        tokio::fs::create_dir_all(tmp.join(".lite-server-registry.json"))
            .await
            .unwrap();

        let state = state_with_repo(tmp.clone(), config);
        let cx = audit_tests::test_cx();
        let result = delete_version_impl(&state, &cx, "mymodel", "1", false).await;

        assert!(
            !version_dir.exists(),
            "the version directory must be deleted regardless"
        );
        assert!(
            result.is_ok(),
            "snapshot rewrite failure must not mask a successful delete: {result:?}"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }
}
