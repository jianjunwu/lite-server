//! P6 Admin gRPC service (蓝图 §4.1).
//!
//! Mirrors the HTTP admin REST handlers as a thin translation layer (D3):
//! parse proto → call `ModelRegistry` / `WorkerManager` / prometheus →
//! serialize response. Control-plane mutations (Load / Unload / Reload /
//! Activate / SetRouting) emit a structured audit log (D27, target
//! `lite_server::audit` — underscore form so the default `lite_server=<level>`
//! EnvFilter prefix matches it) carrying principal / peer / request_id /
//! operation /
//! target / before-after / result.
//!
//! Errors reuse the centralized mapper [`super::app_error_to_grpc_status`]
//! (D4 — no inline mapping here). Access control (fail-closed admin) lands in
//! P7-1; until then Admin is reachable wherever the gRPC server binds (parity
//! with the current HTTP admin surface, which is open to the bind address).

use crate::access_control::AccessControl;
use crate::callback::{CallbackRunner, ModelLifecycleContext, Protocol};
use crate::config::Config;
use crate::error::AppError;
use crate::metrics::prometheus;
use crate::proto::liteserver as pb;
use crate::registry::types::{VersionStatus, WorkerStatus};
use crate::registry::ModelRegistry;
use crate::request_context::RequestContext;
use crate::worker::WorkerManager;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub use pb::admin_server::{Admin, AdminServer};

pub struct GrpcAdminService {
    registry: Arc<ModelRegistry>,
    worker_manager: Arc<WorkerManager>,
    callback_runner: Arc<CallbackRunner>,
    config: Arc<Config>,
    /// File-watcher enable flag; load/unload recompute it (mirrors HTTP).
    has_hot_reload: Arc<AtomicBool>,
    /// D27 审计的 key 指纹来源（配置在 build 期解析，fail-fast 已在启动路径）。
    access_control: Arc<AccessControl>,
}

impl GrpcAdminService {
    pub fn new(
        registry: Arc<ModelRegistry>,
        worker_manager: Arc<WorkerManager>,
        callback_runner: Arc<CallbackRunner>,
        config: Arc<Config>,
        has_hot_reload: Arc<AtomicBool>,
        access_control: Arc<AccessControl>,
    ) -> Self {
        Self {
            registry,
            worker_manager,
            callback_runner,
            config,
            has_hot_reload,
            access_control,
        }
    }

    /// D27 控制面审计——委托共享助手（`src/audit.rs`），与 HTTP admin 同
    /// 记录形状（含 key 指纹）。
    fn audit(&self, cx: &Option<RequestContext>, action: &str, model: &str, version: Option<&str>, details: &str) {
        crate::audit::control_plane(cx.as_ref(), &self.access_control, Protocol::Grpc, action, model, version, details);
    }
}

/// Map an [`AppError`] to a gRPC [`Status`], routed through the centralized
/// mapper and the graded logger (D4 + P1-1 parity).
fn to_status(e: AppError) -> Status {
    super::err(super::app_error_to_grpc_status(&e))
}

/// serde `rename_all = "snake_case"` value of a [`VersionStatus`], as a plain
/// string (mirrors the HTTP JSON `status` field exactly).
fn version_status_str(s: &VersionStatus) -> &'static str {
    use VersionStatus::*;
    match s {
        Pending => "pending",
        Loading => "loading",
        WarmingUp => "warming_up",
        Ready => "ready",
        Degraded => "degraded",
        Failed => "failed",
        Unloading => "unloading",
    }
}

#[tonic::async_trait]
impl Admin for GrpcAdminService {
    async fn get_info(
        &self,
        _req: Request<pb::GetInfoRequest>,
    ) -> Result<Response<pb::GetInfoResponse>, Status> {
        let loaded_models = self
            .registry
            .list_loaded()
            .into_iter()
            .map(|(n, v, _)| format!("{}/{}", n, v))
            .collect();
        Ok(Response::new(pb::GetInfoResponse {
            server: "lite-server".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            loaded_models,
        }))
    }

    async fn list_models(
        &self,
        _req: Request<pb::ListModelsRequest>,
    ) -> Result<Response<pb::ListModelsResponse>, Status> {
        let models = self
            .registry
            .list_loaded()
            .into_iter()
            .map(|(name, version, mv)| pb::ModelSummary {
                name,
                version,
                status: version_status_str(&mv.status).to_string(),
                model_type: format!("{:?}", mv.model_type),
                workers: mv.workers.len() as u32,
            })
            .collect();
        Ok(Response::new(pb::ListModelsResponse { models }))
    }

    async fn list_versions(
        &self,
        req: Request<pb::ListVersionsRequest>,
    ) -> Result<Response<pb::ListVersionsResponse>, Status> {
        let model_name = req.get_ref().model_name.clone();
        crate::validation::validate_identifier(&model_name).map_err(to_status)?;
        let versions = self.registry.list_versions(&model_name);
        if versions.is_empty() {
            return Err(to_status(AppError::ModelNotFound(model_name)));
        }
        let active = self.registry.get_active_version(&model_name);
        let versions_v: Vec<pb::VersionSummary> = versions
            .iter()
            .map(|mv| {
                let ready_workers = mv
                    .workers
                    .iter()
                    .filter(|w| w.status == WorkerStatus::Ready)
                    .count() as u32;
                pb::VersionSummary {
                    version: mv.version.clone(),
                    status: version_status_str(&mv.status).to_string(),
                    active: active.as_deref() == Some(mv.version.as_str()),
                    weight: mv.weight,
                    ready_workers,
                    total_workers: mv.workers.len() as u32,
                    loaded_at: mv
                        .loaded_at
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_secs()),
                }
            })
            .collect();
        Ok(Response::new(pb::ListVersionsResponse {
            name: model_name,
            active_version: active,
            versions: versions_v,
        }))
    }

    async fn model_ready(
        &self,
        req: Request<pb::ModelReadyRequest>,
    ) -> Result<Response<pb::ModelReadyResponse>, Status> {
        let model_name = req.get_ref().model_name.clone();
        crate::validation::validate_identifier(&model_name).map_err(to_status)?;
        // Bare (version absent) → active version; versioned → explicit version.
        let (version, ready) = match req.get_ref().version.as_deref() {
            Some(v) => {
                crate::validation::validate_version(v).map_err(to_status)?;
                (Some(v.to_string()), self.registry.is_ready(&model_name, Some(v)))
            }
            None => {
                let active = self.registry.get_active_version(&model_name);
                let ready = active
                    .as_deref()
                    .is_some_and(|v| self.registry.is_ready(&model_name, Some(v)));
                (active.clone(), ready)
            }
        };
        let active_version = self.registry.get_active_version(&model_name);
        Ok(Response::new(pb::ModelReadyResponse {
            name: model_name,
            version,
            ready,
            active_version,
        }))
    }

    async fn model_health(
        &self,
        req: Request<pb::ModelHealthRequest>,
    ) -> Result<Response<pb::ModelHealthResponse>, Status> {
        let model_name = req.get_ref().model_name.clone();
        crate::validation::validate_identifier(&model_name).map_err(to_status)?;
        // Bare → router-resolved version (admin ops follow routing, not the
        // per-request canary pin — which has no headers on this path); explicit
        // version passes through. Mirrors HTTP model_health_handler.
        let resolved = match req.get_ref().version.as_deref() {
            Some(v) => {
                crate::validation::validate_version(v).map_err(to_status)?;
                v.to_string()
            }
            None => self
                .registry
                .routing_pick(&model_name)
                .or_else(|| self.registry.get_active_version(&model_name))
                .ok_or_else(|| {
                    to_status(AppError::ModelNotFound(format!(
                        "{} has no active version",
                        model_name
                    )))
                })?,
        };
        let mv = self
            .registry
            .get(&model_name, Some(&resolved))
            .ok_or_else(|| {
                to_status(AppError::ModelNotFound(format!(
                    "{} version {}",
                    model_name, resolved
                )))
            })?;
        let total = mv.workers.len();
        let outlier = self
            .worker_manager
            .inference_queue()
            .get_outlier_state(&model_name, &resolved);
        let (workers, healthy_workers) = match outlier {
            Some(out) => {
                let mut ws = Vec::with_capacity(total);
                let mut healthy = 0u32;
                for i in 0..total {
                    let ejected = out.is_ejected(i);
                    if !ejected {
                        healthy += 1;
                    }
                    ws.push(pb::WorkerHealth {
                        worker_id: i as u32,
                        healthy: !ejected,
                        ejected,
                    });
                }
                (ws, healthy)
            }
            // No outlier state → no active queue; report all healthy (HTTP parity).
            None => {
                let ws: Vec<pb::WorkerHealth> = (0..total)
                    .map(|i| pb::WorkerHealth {
                        worker_id: i as u32,
                        healthy: true,
                        ejected: false,
                    })
                    .collect();
                (ws, total as u32)
            }
        };
        Ok(Response::new(pb::ModelHealthResponse {
            name: model_name,
            version: resolved,
            healthy_workers,
            total_workers: total as u32,
            workers,
        }))
    }

    async fn load_model(
        &self,
        req: Request<pb::LoadModelRequest>,
    ) -> Result<Response<pb::LoadModelResponse>, Status> {
        let model_name = req.get_ref().model_name.clone();
        let version = req.get_ref().version.clone();
        let cx = req.extensions().get::<RequestContext>().cloned();
        crate::validation::validate_identifier(&model_name).map_err(to_status)?;
        crate::validation::validate_version(&version).map_err(to_status)?;

        // Read config.yaml from the model repository (mirrors HTTP
        // load_model_handler); WorkerManager.load_model re-resolves model_dir.
        let config_path = std::path::PathBuf::from(&self.config.model_repository.path)
            .join(&model_name)
            .join(&version)
            .join("config.yaml");
        let mut config = crate::config::load_model_config(&config_path).unwrap_or_default();
        self.config.apply_model_defaults(&mut config);

        self.worker_manager
            .load_model(&model_name, &version, &config)
            .await
            .map_err(to_status)?;

        // File-watcher flag: enable if this model opts into hot reload.
        if config.hot_reload {
            self.has_hot_reload.store(true, Ordering::Relaxed);
        }

        // Auto-activate if no active version (mirrors HTTP).
        let prev_active = self.registry.get_active_version(&model_name);
        if prev_active.is_none() {
            self.registry
                .activate_version(&model_name, &version)
                .map_err(to_status)?;
        }
        self.audit(
            &cx,
            "load",
            &model_name,
            Some(&version),
            &format!("loaded; auto_activated={}", prev_active.is_none()),
        );
        Ok(Response::new(pb::LoadModelResponse {
            success: true,
            message: format!("Model {} version {} loaded", model_name, version),
        }))
    }

    async fn unload_model(
        &self,
        req: Request<pb::UnloadModelRequest>,
    ) -> Result<Response<pb::UnloadModelResponse>, Status> {
        let model_name = req.get_ref().model_name.clone();
        let cx = req.extensions().get::<RequestContext>().cloned();
        crate::validation::validate_identifier(&model_name).map_err(to_status)?;
        let version = match req.get_ref().version.as_deref() {
            Some(v) => {
                crate::validation::validate_version(v).map_err(to_status)?;
                v.to_string()
            }
            None => self
                .registry
                .get_active_version(&model_name)
                .ok_or_else(|| {
                    to_status(AppError::ModelNotFound(format!(
                        "{} has no active version",
                        model_name
                    )))
                })?,
        };
        let success = self
            .worker_manager
            .unload_model(&model_name, Some(&version))
            .await
            .map_err(to_status)?;
        if !success {
            return Err(to_status(AppError::ModelNotFound(format!(
                "{} version {} not loaded",
                model_name, version
            ))));
        }
        // Recompute the file-watcher flag (mirrors HTTP unload_model_impl).
        let any_hot_reload = self
            .registry
            .list_loaded()
            .iter()
            .any(|(_, _, mv)| mv.config.hot_reload);
        self.has_hot_reload.store(any_hot_reload, Ordering::Relaxed);
        self.audit(&cx, "unload", &model_name, Some(&version), "unloaded");
        Ok(Response::new(pb::UnloadModelResponse {
            success: true,
            message: format!("Model {} version {} unloaded", model_name, version),
        }))
    }

    async fn reload_model(
        &self,
        req: Request<pb::ReloadModelRequest>,
    ) -> Result<Response<pb::ReloadModelResponse>, Status> {
        let model_name = req.get_ref().model_name.clone();
        let cx = req.extensions().get::<RequestContext>().cloned();
        crate::validation::validate_identifier(&model_name).map_err(to_status)?;
        let version = match req.get_ref().version.as_deref() {
            Some(v) => {
                crate::validation::validate_version(v).map_err(to_status)?;
                v.to_string()
            }
            None => self
                .registry
                .get_active_version(&model_name)
                .ok_or_else(|| {
                    to_status(AppError::ModelNotFound(format!(
                        "{} has no active version",
                        model_name
                    )))
                })?,
        };
        let success = self
            .worker_manager
            .reload_model(&model_name, Some(&version))
            .await
            .map_err(to_status)?;
        if !success {
            return Err(to_status(AppError::ModelNotFound(format!(
                "{} version {} not loaded",
                model_name, version
            ))));
        }
        self.audit(&cx, "reload", &model_name, Some(&version), "reloaded");
        Ok(Response::new(pb::ReloadModelResponse {
            success: true,
            message: format!("Model {} version {} reloaded", model_name, version),
        }))
    }

    async fn activate_version(
        &self,
        req: Request<pb::ActivateVersionRequest>,
    ) -> Result<Response<pb::ActivateVersionResponse>, Status> {
        let model_name = req.get_ref().model_name.clone();
        let version = req.get_ref().version.clone();
        let cx = req.extensions().get::<RequestContext>().cloned();
        crate::validation::validate_identifier(&model_name).map_err(to_status)?;
        crate::validation::validate_version(&version).map_err(to_status)?;

        let previous = self.registry.get_active_version(&model_name);
        let success = self
            .registry
            .activate_version(&model_name, &version)
            .map_err(to_status)?;
        if !success {
            self.audit(
                &cx,
                "activate",
                &model_name,
                Some(&version),
                &format!("failed: not ready; previous_active={:?}", previous),
            );
            return Err(to_status(AppError::ModelNotReady(format!(
                "Model {} version {} is not ready",
                model_name, version
            ))));
        }
        // Explicit activate is a hard cutover (§4.3): target gets 100% weight.
        // Registry activate is pointer-only so internal re-activations don't
        // clobber a canary split.
        self.registry
            .set_weights(&model_name, &HashMap::from([(version.clone(), 100u32)]))
            .map_err(to_status)?;
        for mv in self.registry.list_versions(&model_name) {
            prometheus::set_version_weight(&model_name, &mv.version, mv.weight as f64);
        }
        if let Some(from) = &previous {
            if from != &version {
                prometheus::record_version_switch(&model_name, from, &version);
            }
        }
        self.callback_runner
            .on_model_activate(&ModelLifecycleContext {
                model_name: model_name.clone(),
                version: version.clone(),
                device: None,
            })
            .await;
        self.audit(
            &cx,
            "activate",
            &model_name,
            Some(&version),
            &format!("previous_active={:?} -> {}", previous, version),
        );
        Ok(Response::new(pb::ActivateVersionResponse {
            success: true,
            message: format!("Model {} version {} is now active", model_name, version),
            active_version: version,
        }))
    }

    async fn set_routing(
        &self,
        req: Request<pb::SetRoutingRequest>,
    ) -> Result<Response<pb::SetRoutingResponse>, Status> {
        let model_name = req.get_ref().model_name.clone();
        let cx = req.extensions().get::<RequestContext>().cloned();
        crate::validation::validate_identifier(&model_name).map_err(to_status)?;

        // Resolve effective weights: explicit `weights` XOR canary sugar.
        let effective: HashMap<String, u32> = if !req.get_ref().weights.is_empty() {
            if req.get_ref().canary_version.is_some() || req.get_ref().canary_percent.is_some() {
                return Err(super::err(Status::invalid_argument(
                    "SetRouting: `weights` and `canary_*` are mutually exclusive",
                )));
            }
            for v in req.get_ref().weights.keys() {
                crate::validation::validate_version(v).map_err(to_status)?;
            }
            req.get_ref().weights.clone()
        } else {
            // P5-2 canary 糖: convert (canary_version, canary_percent) to weights.
            let canary_version = req
                .get_ref()
                .canary_version
                .as_deref()
                .ok_or_else(|| {
                    super::err(Status::invalid_argument(
                        "SetRouting: `canary_version` required when `weights` is absent",
                    ))
                })?;
            let canary_percent = req.get_ref().canary_percent.ok_or_else(|| {
                super::err(Status::invalid_argument(
                    "SetRouting: `canary_percent` required when `weights` is absent",
                ))
            })?;
            crate::validation::validate_version(canary_version).map_err(to_status)?;
            if canary_percent > 100 {
                return Err(super::err(Status::invalid_argument(
                    "SetRouting: `canary_percent` must be 0..=100",
                )));
            }
            if self.registry.get(&model_name, Some(canary_version)).is_none() {
                return Err(to_status(AppError::VersionNotFound(
                    model_name.clone(),
                    canary_version.to_string(),
                )));
            }
            // Stable side = currently active version (must exist and differ).
            let stable = self.registry.get_active_version(&model_name).ok_or_else(|| {
                super::err(Status::failed_precondition(format!(
                    "SetRouting: model `{}` has no active version to use as the stable side of the canary split",
                    model_name
                )))
            })?;
            if stable == canary_version {
                return Err(super::err(Status::invalid_argument(
                    "SetRouting: `canary_version` must differ from the active version",
                )));
            }
            let mut w = HashMap::new();
            w.insert(canary_version.to_string(), canary_percent);
            w.insert(stable, 100 - canary_percent);
            w
        };

        // Capture old weights for the audit before/after.
        let old: HashMap<String, u32> = self
            .registry
            .list_versions(&model_name)
            .into_iter()
            .map(|mv| (mv.version, mv.weight))
            .collect();
        self.registry
            .set_weights(&model_name, &effective)
            .map_err(to_status)?;
        for mv in self.registry.list_versions(&model_name) {
            prometheus::set_version_weight(&model_name, &mv.version, mv.weight as f64);
        }
        self.audit(
            &cx,
            "set_routing",
            &model_name,
            None,
            &format!("weights {:?} -> {:?}", old, effective),
        );
        Ok(Response::new(pb::SetRoutingResponse {
            success: true,
            weights: effective,
        }))
    }

    async fn get_model_stats(
        &self,
        req: Request<pb::GetModelStatsRequest>,
    ) -> Result<Response<pb::GetModelStatsResponse>, Status> {
        let model_name = req.get_ref().model_name.clone();
        crate::validation::validate_identifier(&model_name).map_err(to_status)?;
        let target_versions: Vec<String> = match req.get_ref().version.as_deref() {
            Some(v) => {
                crate::validation::validate_version(v).map_err(to_status)?;
                if self.registry.get(&model_name, Some(v)).is_none() {
                    return Err(to_status(AppError::VersionNotFound(
                        model_name,
                        v.to_string(),
                    )));
                }
                vec![v.to_string()]
            }
            None => self
                .registry
                .list_versions(&model_name)
                .into_iter()
                .map(|mv| mv.version)
                .collect(),
        };

        let mut stats = Vec::with_capacity(target_versions.len());
        for version in target_versions {
            let Some(mv) = self.registry.get(&model_name, Some(&version)) else {
                continue;
            };
            // inference_count = Σ REQUESTS_TOTAL across status families.
            let mut inference_count = 0u64;
            for fam in ["2xx", "3xx", "4xx", "5xx"] {
                inference_count +=
                    prometheus::REQUESTS_TOTAL.with_label_values(&[&model_name, &version, fam]).get()
                        as u64;
            }
            let hist = prometheus::REQUEST_DURATION.with_label_values(&[&model_name, &version]);
            let samples = hist.get_sample_count();
            let avg_duration_ms = if samples > 0 {
                hist.get_sample_sum() / samples as f64 * 1000.0
            } else {
                0.0
            };
            let queue_depth =
                prometheus::QUEUE_DEPTH.with_label_values(&[&model_name, &version]).get() as i64;

            let outlier = self
                .worker_manager
                .inference_queue()
                .get_outlier_state(&model_name, &version);
            let workers: Vec<pb::WorkerStats> = (0..mv.workers.len() as u32)
                .map(|i| {
                    let healthy = outlier
                        .as_ref()
                        .map(|o| !o.is_ejected(i as usize))
                        .unwrap_or(true);
                    let worker_inferences =
                        prometheus::worker_inference_count(&model_name, &version, i as usize);
                    pb::WorkerStats {
                        worker_id: i,
                        healthy,
                        inference_count: worker_inferences,
                    }
                })
                .collect();

            stats.push(pb::ModelStats {
                model: model_name.clone(),
                version: version.clone(),
                inference_count,
                avg_duration_ms,
                queue_depth,
                workers,
            });
        }
        Ok(Response::new(pb::GetModelStatsResponse { stats }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callback::CallbackRunner;
    use crate::config::ModelConfig;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::types::ModelType;

    /// A two-version registry (v1, v2) both Ready; v1 active. For outlier-backed
    /// ModelHealth tests pass `with_queue=true` to register a queue.
    fn two_version_registry(model: &str) -> Arc<ModelRegistry> {
        let reg = Arc::new(ModelRegistry::new());
        for v in ["1", "2"] {
            reg.register(
                model,
                v,
                ModelConfig::default(),
                ModelType::LitAPI,
                std::env::temp_dir(),
            )
            .unwrap();
            reg.mark_ready(model, v).unwrap();
        }
        reg.activate_version(model, "1").unwrap();
        reg
    }

    fn build_admin_service(registry: Arc<ModelRegistry>) -> GrpcAdminService {
        let queue = Arc::new(InferenceQueue::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::env::temp_dir(),
            queue,
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        GrpcAdminService::new(
            registry,
            wm,
            Arc::new(CallbackRunner::new()),
            Arc::new(Config::default()),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AccessControl::default()),
        )
    }

    // ===== GetInfo / ListModels / ListVersions =====

    #[tokio::test]
    async fn get_info_returns_loaded_models_as_name_slash_version() {
        let reg = two_version_registry("info_m");
        let svc = build_admin_service(reg);
        let resp = svc
            .get_info(Request::new(pb::GetInfoRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.server, "lite-server");
        assert!(!resp.version.is_empty());
        assert!(resp.loaded_models.contains(&"info_m/1".to_string()));
        assert!(resp.loaded_models.contains(&"info_m/2".to_string()));
    }

    #[tokio::test]
    async fn list_models_returns_summaries() {
        let reg = two_version_registry("lm_m");
        let svc = build_admin_service(reg);
        let resp = svc
            .list_models(Request::new(pb::ListModelsRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.models.len(), 2);
        let m1 = resp.models.iter().find(|m| m.version == "1").unwrap();
        assert_eq!(m1.name, "lm_m");
        assert_eq!(m1.status, "ready");
        assert_eq!(m1.model_type, "LitAPI");
    }

    #[tokio::test]
    async fn list_versions_returns_active_and_weights() {
        let reg = two_version_registry("lv_m");
        reg.set_weights("lv_m", &HashMap::from([("1".into(), 90u32), ("2".into(), 10)]))
            .unwrap();
        let svc = build_admin_service(reg);
        let resp = svc
            .list_versions(Request::new(pb::ListVersionsRequest {
                model_name: "lv_m".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.name, "lv_m");
        assert_eq!(resp.active_version.as_deref(), Some("1"));
        let v1 = resp.versions.iter().find(|v| v.version == "1").unwrap();
        assert!(v1.active);
        assert_eq!(v1.weight, 90);
        let v2 = resp.versions.iter().find(|v| v.version == "2").unwrap();
        assert!(!v2.active);
        assert_eq!(v2.weight, 10);
    }

    #[tokio::test]
    async fn list_versions_unknown_model_is_not_found() {
        let svc = build_admin_service(Arc::new(ModelRegistry::new()));
        let err = svc
            .list_versions(Request::new(pb::ListVersionsRequest {
                model_name: "nope".to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    // ===== ModelReady =====

    #[tokio::test]
    async fn model_ready_versioned_reports_explicit_version() {
        let reg = two_version_registry("mr_m");
        let svc = build_admin_service(reg);
        let resp = svc
            .model_ready(Request::new(pb::ModelReadyRequest {
                model_name: "mr_m".to_string(),
                version: Some("2".to_string()),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.version.as_deref(), Some("2"));
        assert!(resp.ready);
        assert_eq!(resp.active_version.as_deref(), Some("1"));
    }

    #[tokio::test]
    async fn model_ready_bare_uses_active_version() {
        let reg = two_version_registry("mrb_m");
        let svc = build_admin_service(reg);
        let resp = svc
            .model_ready(Request::new(pb::ModelReadyRequest {
                model_name: "mrb_m".to_string(),
                version: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.version.as_deref(), Some("1"), "bare → active version");
        assert!(resp.ready);
    }

    #[tokio::test]
    async fn model_ready_false_for_non_ready_version() {
        // Register v3 but leave it Pending (not mark_ready) → ready=false.
        let reg = two_version_registry("mrp_m");
        reg.register(
            "mrp_m",
            "3",
            ModelConfig::default(),
            ModelType::LitAPI,
            std::env::temp_dir(),
        )
        .unwrap();
        let svc = build_admin_service(reg);
        let resp = svc
            .model_ready(Request::new(pb::ModelReadyRequest {
                model_name: "mrp_m".to_string(),
                version: Some("3".to_string()),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ready, "Pending version must report ready=false");
    }

    // ===== ModelHealth =====

    #[tokio::test]
    async fn model_health_reports_all_healthy_without_outlier_state() {
        // mv.workers is empty here (no real workers) → total=0, healthy=0.
        let reg = two_version_registry("mh_m");
        let svc = build_admin_service(reg);
        let resp = svc
            .model_health(Request::new(pb::ModelHealthRequest {
                model_name: "mh_m".to_string(),
                version: Some("1".to_string()),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.version, "1");
        assert_eq!(resp.total_workers, 0);
        assert_eq!(resp.healthy_workers, 0);
        assert!(resp.workers.is_empty());
    }

    #[tokio::test]
    async fn model_health_unknown_version_is_not_found() {
        let reg = two_version_registry("mh404_m");
        let svc = build_admin_service(reg);
        let err = svc
            .model_health(Request::new(pb::ModelHealthRequest {
                model_name: "mh404_m".to_string(),
                version: Some("9".to_string()),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    // ===== ActivateVersion =====

    #[tokio::test]
    async fn activate_version_hard_cutover_sets_weight_100() {
        let reg = two_version_registry("act_m");
        // Pre-set a split; activate must hard-cutover to {2:100}.
        reg.set_weights("act_m", &HashMap::from([("1".into(), 100u32)]))
            .unwrap();
        let svc = build_admin_service(reg.clone());
        let resp = svc
            .activate_version(Request::new(pb::ActivateVersionRequest {
                model_name: "act_m".to_string(),
                version: "2".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.success);
        assert_eq!(resp.active_version, "2");
        // Active pointer + weight 100 on v2, 0 on v1.
        assert_eq!(reg.get_active_version("act_m").as_deref(), Some("2"));
        assert_eq!(reg.get("act_m", Some("2")).unwrap().weight, 100);
        assert_eq!(reg.get("act_m", Some("1")).unwrap().weight, 0);
    }

    #[tokio::test]
    async fn activate_version_not_ready_is_unavailable() {
        // v3 registered but Pending → activate returns Ok(false) → error.
        let reg = two_version_registry("actnr_m");
        reg.register(
            "actnr_m",
            "3",
            ModelConfig::default(),
            ModelType::LitAPI,
            std::env::temp_dir(),
        )
        .unwrap();
        let svc = build_admin_service(reg);
        let err = svc
            .activate_version(Request::new(pb::ActivateVersionRequest {
                model_name: "actnr_m".to_string(),
                version: "3".to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    // ===== SetRouting =====

    #[tokio::test]
    async fn set_routing_explicit_weights_applied() {
        let reg = two_version_registry("sr_m");
        let svc = build_admin_service(reg.clone());
        let resp = svc
            .set_routing(Request::new(pb::SetRoutingRequest {
                model_name: "sr_m".to_string(),
                weights: HashMap::from([("1".into(), 70u32), ("2".into(), 30)]),
                canary_version: None,
                canary_percent: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.success);
        assert_eq!(resp.weights.get("1"), Some(&70));
        assert_eq!(resp.weights.get("2"), Some(&30));
        assert_eq!(reg.get("sr_m", Some("1")).unwrap().weight, 70);
    }

    #[tokio::test]
    async fn set_routing_canary_sugar_converts_to_weights() {
        // active=v1; canary v2 @ 10% → weights {2:10, 1:90}.
        let reg = two_version_registry("canary_m");
        let svc = build_admin_service(reg.clone());
        let resp = svc
            .set_routing(Request::new(pb::SetRoutingRequest {
                model_name: "canary_m".to_string(),
                weights: HashMap::new(),
                canary_version: Some("2".to_string()),
                canary_percent: Some(10),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.weights.get("2"), Some(&10), "canary gets canary_percent");
        assert_eq!(
            resp.weights.get("1"),
            Some(&90),
            "stable (active) gets the remainder"
        );
        assert_eq!(reg.get("canary_m", Some("2")).unwrap().weight, 10);
    }

    #[tokio::test]
    async fn set_routing_weights_and_canary_are_mutually_exclusive() {
        let reg = two_version_registry("sx_m");
        let svc = build_admin_service(reg);
        let err = svc
            .set_routing(Request::new(pb::SetRoutingRequest {
                model_name: "sx_m".to_string(),
                weights: HashMap::from([("1".into(), 100u32)]),
                canary_version: Some("2".to_string()),
                canary_percent: Some(10),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn set_routing_canary_must_differ_from_active() {
        let reg = two_version_registry("scd_m"); // active = v1
        let svc = build_admin_service(reg);
        let err = svc
            .set_routing(Request::new(pb::SetRoutingRequest {
                model_name: "scd_m".to_string(),
                weights: HashMap::new(),
                canary_version: Some("1".to_string()),
                canary_percent: Some(10),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // ===== GetModelStats (reads the new per-worker counter) =====

    #[tokio::test]
    async fn get_model_stats_reads_inference_counters() {
        let model = "gms_m";
        let reg = two_version_registry(model);
        let svc = build_admin_service(reg);
        // Seed the per-worker counter (P6) and a request counter + duration.
        prometheus::record_worker_inference(model, "1", 0, 7);
        prometheus::record_request_end(model, "1", "2xx", 0.020);

        let resp = svc
            .get_model_stats(Request::new(pb::GetModelStatsRequest {
                model_name: model.to_string(),
                version: Some("1".to_string()),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.stats.len(), 1);
        let s = &resp.stats[0];
        assert_eq!(s.version, "1");
        assert!(s.inference_count >= 1, "REQUESTS_TOTAL 2xx must be counted");
        assert!(s.avg_duration_ms > 0.0, "avg_duration must reflect the sample");
        // mv.workers is empty (no real workers) → no WorkerStats rows; the
        // per-worker counter is still read for any worker index that exists.
        assert!(s.workers.is_empty());
    }

    #[tokio::test]
    async fn get_model_stats_unknown_version_is_not_found() {
        let reg = two_version_registry("gms404_m");
        let svc = build_admin_service(reg);
        let err = svc
            .get_model_stats(Request::new(pb::GetModelStatsRequest {
                model_name: "gms404_m".to_string(),
                version: Some("9".to_string()),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    // ===== Unload / Reload error paths (no real worker spawn) =====

    #[tokio::test]
    async fn unload_version_not_loaded_is_not_found() {
        let reg = two_version_registry("ul_m");
        let svc = build_admin_service(reg);
        // v3 was never loaded → unload returns false → NotFound.
        let err = svc
            .unload_model(Request::new(pb::UnloadModelRequest {
                model_name: "ul_m".to_string(),
                version: Some("3".to_string()),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn unload_no_active_version_is_not_found() {
        // A registered-but-not-active model has no active version for bare unload.
        let reg = Arc::new(ModelRegistry::new());
        reg.register(
            "ulna_m",
            "1",
            ModelConfig::default(),
            ModelType::LitAPI,
            std::env::temp_dir(),
        )
        .unwrap();
        let svc = build_admin_service(reg);
        let err = svc
            .unload_model(Request::new(pb::UnloadModelRequest {
                model_name: "ulna_m".to_string(),
                version: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn reload_version_not_loaded_is_not_found() {
        let reg = two_version_registry("rl_m");
        let svc = build_admin_service(reg);
        let err = svc
            .reload_model(Request::new(pb::ReloadModelRequest {
                model_name: "rl_m".to_string(),
                version: Some("3".to_string()),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    // ===== End-to-end: Admin service over a real tonic transport =====
    //
    // Boots `start_grpc_server` in-process (with the Admin service registered)
    // and calls GetInfo over a real tonic client. This is the one layer the
    // direct trait tests above do not cover: the 3-service router + the
    // context_interceptor mounted on Admin. GetInfo is registry-only, so no
    // Python worker is spawned (avoids the orphan-process integration hang).

    async fn spawn_admin_grpc_server(
        registry: Arc<ModelRegistry>,
    ) -> (
        u16,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), AppError>>,
    ) {
        // Ephemeral TCP port (tiny TOCTOU race, fails clearly rather than hangs).
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let queue = Arc::new(InferenceQueue::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::env::temp_dir(),
            queue,
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(crate::grpc::start_grpc_server(
            crate::grpc::GrpcServerOptions {
                host: "127.0.0.1".to_string(),
                port,
                registry,
                worker_manager: wm,
                streaming_metrics: false,
                canary_override: false,
                callback_runner: Arc::new(CallbackRunner::new()),
                shutdown_state: Arc::new(crate::server::ShutdownState::new()),
                server_timeout: std::time::Duration::from_secs(5),
                grpc_config: crate::config::GrpcConfig::default(),
                rate_limiter: Arc::new(crate::rate_limit::RateLimiter::default()),
                tls: None,
                config: Config::default(),
                has_hot_reload: Arc::new(AtomicBool::new(false)),
            },
            shutdown_rx,
        ));
        (port, shutdown_tx, handle)
    }

    #[tokio::test]
    async fn admin_get_info_reachable_over_real_grpc_transport() {
        let reg = two_version_registry("e2e_m");
        let (port, shutdown_tx, _handle) = spawn_admin_grpc_server(reg).await;
        // Let the server bind before the client connects.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let channel = tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{}", port))
            .unwrap()
            .connect()
            .await
            .expect("must connect to the Admin gRPC server");
        let mut client = pb::admin_client::AdminClient::new(channel);
        let resp = client
            .get_info(pb::GetInfoRequest {})
            .await
            .expect("GetInfo RPC must succeed")
            .into_inner();
        assert_eq!(resp.server, "lite-server");
        assert!(resp.loaded_models.iter().any(|m| m == "e2e_m/1"));
        assert!(resp.loaded_models.iter().any(|m| m == "e2e_m/2"));

        let _ = shutdown_tx.send(());
    }
}
