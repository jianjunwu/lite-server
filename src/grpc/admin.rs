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
use crate::callback::{ModelLifecycleContext, Protocol};
use crate::error::AppError;
use crate::http::state::AppState;
use crate::metrics::prometheus;
use crate::proto::liteserver as pb;
use crate::registry::types::{VersionStatus, WorkerStatus};
use crate::request_context::RequestContext;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub use pb::admin_server::{Admin, AdminServer};

pub struct GrpcAdminService {
    /// Shared state with the HTTP handlers (registry / worker_manager /
    /// config / repo_path / ...). The gRPC server builds the same AppState
    /// as the HTTP path (grpc/mod.rs), so the repository RPCs reuse the
    /// HTTP handlers' shared cores directly (G3/E5/F3 parity — no dual
    /// implementation drift).
    app_state: Arc<AppState>,
    /// D27 审计的 key 指纹来源（配置在 build 期解析，fail-fast 已在启动路径）。
    /// The AppState built by the gRPC path carries the default instance —
    /// audits use THIS resolved one instead.
    access_control: Arc<AccessControl>,
}

impl GrpcAdminService {
    pub fn new(app_state: Arc<AppState>, access_control: Arc<AccessControl>) -> Self {
        Self {
            app_state,
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

/// F-13: bound on a single download-chunk send — a stalled (connected but
/// not reading) client must not park the download task and its file handle
/// forever.
const DOWNLOAD_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Pump `file` into `tx` in 1 MiB chunks (wire 定稿) with a terminal frame
/// carrying sha256 + size (DecoupledResponse idiom). Every send — chunk,
/// mid-stream read error, and terminal frame — is bounded by
/// [DOWNLOAD_SEND_TIMEOUT]: a stalled (connected but not reading) client
/// must not park the download task, its file handle, or the pack temp-dir
/// cleanup guard riding in the spawned task (F-13/L2).
async fn pump_download_chunks(
    mut file: tokio::fs::File,
    path: std::path::PathBuf,
    tx: tokio::sync::mpsc::Sender<Result<pb::DownloadModelChunk, Status>>,
) {
    let mut hasher = Sha256::new();
    let mut sent: u64 = 0;
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        match tokio::io::AsyncReadExt::read(&mut file, &mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                hasher.update(&buf[..n]);
                sent += n as u64;
                let chunk = pb::DownloadModelChunk {
                    data: Bytes::copy_from_slice(&buf[..n]),
                    is_final: false,
                    sha256: String::new(),
                    size: 0,
                };
                // Bounded send: a stalled client must not park this
                // task (and its file handle) forever.
                match tokio::time::timeout(DOWNLOAD_SEND_TIMEOUT, tx.send(Ok(chunk))).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => return, // client gone — stream ends here
                    Err(_) => {
                        tracing::warn!("DownloadModel: client stalled; aborting send");
                        return;
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    "DownloadModel: read {} failed: {}",
                    path.display(),
                    e
                );
                // Surface the truncation explicitly — a clean end
                // here would read as a successful short download.
                let _ = tokio::time::timeout(
                    DOWNLOAD_SEND_TIMEOUT,
                    tx.send(Err(Status::internal(format!(
                        "read {} failed mid-download",
                        path.display()
                    )))),
                )
                .await;
                return;
            }
        }
    }
    // L2: the terminal frame gets the same bound as chunk sends — a client
    // stalled exactly at EOF with a full channel must not park this task.
    let _ = tokio::time::timeout(
        DOWNLOAD_SEND_TIMEOUT,
        tx.send(Ok(pb::DownloadModelChunk {
            data: Bytes::new(),
            is_final: true,
            sha256: format!("{:x}", hasher.finalize()),
            size: sent,
        })),
    )
    .await;
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
    type DownloadModelStream = ReceiverStream<Result<pb::DownloadModelChunk, Status>>;

    async fn get_info(
        &self,
        _req: Request<pb::GetInfoRequest>,
    ) -> Result<Response<pb::GetInfoResponse>, Status> {
        let loaded_models = self
            .app_state.registry
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
            .app_state.registry
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
        let versions = self.app_state.registry.list_versions(&model_name);
        if versions.is_empty() {
            return Err(to_status(AppError::ModelNotFound(model_name)));
        }
        let active = self.app_state.registry.get_active_version(&model_name);
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
                (Some(v.to_string()), self.app_state.registry.is_ready(&model_name, Some(v)))
            }
            None => {
                let active = self.app_state.registry.get_active_version(&model_name);
                let ready = active
                    .as_deref()
                    .is_some_and(|v| self.app_state.registry.is_ready(&model_name, Some(v)));
                (active.clone(), ready)
            }
        };
        let active_version = self.app_state.registry.get_active_version(&model_name);
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
                .app_state.registry
                .routing_pick(&model_name)
                .or_else(|| self.app_state.registry.get_active_version(&model_name))
                .ok_or_else(|| {
                    to_status(AppError::ModelNotFound(format!(
                        "{} has no active version",
                        model_name
                    )))
                })?,
        };
        let mv = self
            .app_state.registry
            .get(&model_name, Some(&resolved))
            .ok_or_else(|| {
                to_status(AppError::ModelNotFound(format!(
                    "{} version {}",
                    model_name, resolved
                )))
            })?;
        let total = mv.workers.len();
        let outlier = self
            .app_state.worker_manager
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
        let config_path = std::path::PathBuf::from(&self.app_state.config.model_repository.path)
            .join(&model_name)
            .join(&version)
            .join("config.yaml");
        let mut config = crate::config::load_model_config(&config_path).unwrap_or_default();
        self.app_state.config.apply_model_defaults(&mut config);

        self.app_state.worker_manager
            .load_model(&model_name, &version, &config)
            .await
            .map_err(to_status)?;

        // File-watcher flag: enable if this model opts into hot reload.
        if config.hot_reload {
            self.app_state.has_hot_reload.store(true, Ordering::Relaxed);
        }

        // Auto-activate if no active version (mirrors HTTP).
        let prev_active = self.app_state.registry.get_active_version(&model_name);
        if prev_active.is_none() {
            self.app_state.registry
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
                .app_state.registry
                .get_active_version(&model_name)
                .ok_or_else(|| {
                    to_status(AppError::ModelNotFound(format!(
                        "{} has no active version",
                        model_name
                    )))
                })?,
        };
        let success = self
            .app_state.worker_manager
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
            .app_state.registry
            .list_loaded()
            .iter()
            .any(|(_, _, mv)| mv.config.hot_reload);
        self.app_state.has_hot_reload.store(any_hot_reload, Ordering::Relaxed);
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
                .app_state.registry
                .get_active_version(&model_name)
                .ok_or_else(|| {
                    to_status(AppError::ModelNotFound(format!(
                        "{} has no active version",
                        model_name
                    )))
                })?,
        };
        let success = self
            .app_state.worker_manager
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

        let previous = self.app_state.registry.get_active_version(&model_name);
        let success = self
            .app_state.registry
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
        self.app_state.registry
            .set_weights(&model_name, &HashMap::from([(version.clone(), 100u32)]))
            .map_err(to_status)?;
        for mv in self.app_state.registry.list_versions(&model_name) {
            prometheus::set_version_weight(&model_name, &mv.version, mv.weight as f64);
        }
        if let Some(from) = &previous {
            if from != &version {
                prometheus::record_version_switch(&model_name, from, &version);
            }
        }
        self.app_state.callback_runner
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
            if self.app_state.registry.get(&model_name, Some(canary_version)).is_none() {
                return Err(to_status(AppError::VersionNotFound(
                    model_name.clone(),
                    canary_version.to_string(),
                )));
            }
            // Stable side = currently active version (must exist and differ).
            let stable = self.app_state.registry.get_active_version(&model_name).ok_or_else(|| {
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
            .app_state.registry
            .list_versions(&model_name)
            .into_iter()
            .map(|mv| (mv.version, mv.weight))
            .collect();
        self.app_state.registry
            .set_weights(&model_name, &effective)
            .map_err(to_status)?;
        for mv in self.app_state.registry.list_versions(&model_name) {
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
                if self.app_state.registry.get(&model_name, Some(v)).is_none() {
                    return Err(to_status(AppError::VersionNotFound(
                        model_name,
                        v.to_string(),
                    )));
                }
                vec![v.to_string()]
            }
            None => self
                .app_state.registry
                .list_versions(&model_name)
                .into_iter()
                .map(|mv| mv.version)
                .collect(),
        };

        let mut stats = Vec::with_capacity(target_versions.len());
        for version in target_versions {
            let Some(mv) = self.app_state.registry.get(&model_name, Some(&version)) else {
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
                .app_state.worker_manager
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

    // ===== Repository lifecycle (G3 + E5 + F3) =====
    //
    // These mirror the HTTP repository endpoints by reusing the shared
    // cores extracted from the HTTP handlers (delete/scan logic in
    // http/handlers/admin.rs, file logic in http/handlers/files.rs) — one
    // implementation, no drift. Errors map through app_error_to_grpc_status
    // like every other Admin RPC.

    async fn delete_version(
        &self,
        req: Request<pb::DeleteVersionRequest>,
    ) -> Result<Response<pb::DeleteVersionResponse>, Status> {
        let cx = req.extensions().get::<RequestContext>().cloned();
        let r = req.into_inner();
        crate::http::handlers::admin::delete_version_impl(
            &self.app_state,
            &self.access_control,
            cx.as_ref(),
            Protocol::Grpc,
            &r.model_name,
            &r.version,
            r.force,
        )
        .await
        .map_err(to_status)?;
        Ok(Response::new(pb::DeleteVersionResponse {
            success: true,
            message: format!("Model {} version {} deleted", r.model_name, r.version),
        }))
    }

    async fn delete_model(
        &self,
        req: Request<pb::DeleteModelRequest>,
    ) -> Result<Response<pb::DeleteModelResponse>, Status> {
        let cx = req.extensions().get::<RequestContext>().cloned();
        let r = req.into_inner();
        crate::http::handlers::admin::delete_model_core(
            &self.app_state,
            &self.access_control,
            cx.as_ref(),
            Protocol::Grpc,
            &r.model_name,
            r.force,
        )
        .await
        .map_err(to_status)?;
        Ok(Response::new(pb::DeleteModelResponse {
            success: true,
            message: format!("Model {} deleted", r.model_name),
        }))
    }

    async fn delete_versions(
        &self,
        req: Request<pb::DeleteVersionsRequest>,
    ) -> Result<Response<pb::DeleteVersionsResponse>, Status> {
        let cx = req.extensions().get::<RequestContext>().cloned();
        let r = req.into_inner();
        // keep == 0 means unset (wire 定稿: the HTTP keep=0 validation
        // cannot apply to a proto uint32, where absence is 0).
        let keep = r.keep.filter(|k| *k > 0).map(|k| k as usize);
        let versions = if r.versions.is_empty() {
            None
        } else {
            Some(r.versions)
        };
        let (deleted, failed) = crate::http::handlers::admin::delete_versions_core(
            &self.app_state,
            &self.access_control,
            cx.as_ref(),
            Protocol::Grpc,
            &r.model_name,
            keep,
            versions,
            r.force,
        )
        .await
        .map_err(to_status)?;
        Ok(Response::new(pb::DeleteVersionsResponse {
            deleted,
            failed: failed
                .into_iter()
                .map(|(version, error)| pb::DeleteFailure { version, error })
                .collect(),
        }))
    }

    async fn repository_drift(
        &self,
        req: Request<pb::RepositoryDriftRequest>,
    ) -> Result<Response<pb::RepositoryDriftResponse>, Status> {
        let r = req.into_inner();
        if let Some(m) = &r.model_name {
            crate::validation::validate_identifier(m).map_err(to_status)?;
        }
        let (missing, unconfigured) = crate::http::handlers::admin::repository_drift_core(
            &self.app_state,
            r.model_name.as_deref(),
        )
        .await
        .map_err(to_status)?;
        Ok(Response::new(pb::RepositoryDriftResponse {
            configured_missing: missing
                .into_iter()
                .map(|e| pb::DriftMissingEntry {
                    model: e.model,
                    version: e.version,
                })
                .collect(),
            on_disk_unconfigured: unconfigured
                .into_iter()
                .map(|e| pb::DriftDiskEntry {
                    model: e.model,
                    version: e.version,
                    size_bytes: e.size_bytes,
                    ensemble_referenced: e.ensemble_referenced,
                })
                .collect(),
        }))
    }

    async fn upload_model(
        &self,
        req: Request<tonic::Streaming<pb::UploadModelRequest>>,
    ) -> Result<Response<pb::UploadModelResponse>, Status> {
        let cx = req.extensions().get::<RequestContext>().cloned();
        let mut stream = req.into_inner();

        // Loose-field wire (定稿): the FIRST message carries the metadata;
        // every subsequent message carries file content.
        let first = stream
            .message()
            .await
            .map_err(|e| to_status(AppError::Transport(format!("read upload stream: {}", e))))?
            .ok_or_else(|| to_status(AppError::Validation("empty upload stream".to_string())))?;
        // Reject file content on the metadata message outright — silently
        // dropping it would corrupt the upload without any error signal.
        if !first.file_name.is_empty() || !first.data.is_empty() {
            return Err(to_status(AppError::InvalidRequestBody(
                "the first UploadModel message carries metadata only \
                 (model_name/version/load); send file_name/data in subsequent messages"
                    .to_string(),
            )));
        }
        let model_name = first.model_name.clone();
        crate::validation::validate_identifier(&model_name).map_err(to_status)?;
        let url_version = first.version.clone();
        if let Some(v) = &url_version {
            crate::validation::validate_version(v).map_err(to_status)?;
        }
        // Optional bool: absent → true, aligning with the HTTP ?load=
        // default.
        let load = first.load.unwrap_or(true);

        // H3: stage everything first, commit atomically at the end — same
        // pipeline as the HTTP multipart handlers (finalize_upload below).
        let staging = self
            .app_state
            .repo_path
            .join(format!(".tmp-upload-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&staging)
            .await
            .map_err(|e| to_status(AppError::Io(e)))?;
        let _staging_guard = crate::http::handlers::files::StagingGuard(staging.clone());

        let mut staged: Vec<crate::http::handlers::files::StagedUploadFile> = Vec::new();
        let mut total_bytes: u64 = 0;
        // Chunked writes: consecutive messages sharing a file_name append
        // to one staged file; re-using a name after switching away is
        // rejected (interleaved chunks would silently corrupt the file).
        let mut open: Option<(String, tokio::fs::File)> = None;
        let mut closed: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut lma_count = 0usize;

        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| to_status(AppError::Transport(format!("read upload stream: {}", e))))?
        {
            let file_name = msg.file_name.clone();
            if file_name.is_empty() {
                return Err(to_status(AppError::InvalidRequestBody(
                    "file message lacks a file_name".to_string(),
                )));
            }
            // B1: strip path components (same rule as the HTTP branch) — a
            // `../` or absolute name would otherwise escape the staging dir.
            let safe_name = std::path::Path::new(&file_name)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            if safe_name.is_empty() || safe_name.starts_with('.') {
                return Err(to_status(AppError::InvalidRequestBody(format!(
                    "invalid file name: {}",
                    file_name
                ))));
            }
            let is_lma = safe_name.ends_with(".lma");
            if url_version.is_none() && !is_lma {
                return Err(to_status(AppError::InvalidRequestBody(format!(
                    "model-level upload accepts a single .lma artifact (got '{}'); \
                     raw files need the versioned endpoint \
                     /v2/repository/models/{}/versions/{{v}}/upload",
                    file_name, model_name
                ))));
            }

            total_bytes = total_bytes.saturating_add(msg.data.len() as u64);
            // F11b: cumulative upload size cap.
            if let Some(max) = self.app_state.config.server.max_upload_bytes {
                if total_bytes > max {
                    return Err(to_status(AppError::PayloadTooLarge {
                        max_size: max as usize,
                        actual_size: Some(total_bytes),
                    }));
                }
            }

            // Open (or keep) the staged file for this name.
            match &open {
                Some((name, _)) if name == &safe_name => {}
                _ => {
                    // Switching files: the reassignment below closes the
                    // previous staged file.
                    if closed.contains(&safe_name) {
                        return Err(to_status(AppError::InvalidRequestBody(format!(
                            "file '{}' re-appears after another file — chunks must be contiguous",
                            file_name
                        ))));
                    }
                    // Wire 定稿: at most one .lma per UploadModel — counted
                    // per FILE, so chunked .lma streams are not rejected.
                    if is_lma {
                        lma_count += 1;
                        if lma_count > 1 {
                            return Err(to_status(AppError::InvalidRequestBody(
                                "UploadModel accepts at most one .lma artifact".to_string(),
                            )));
                        }
                    }
                    let dest = if is_lma {
                        staging.join(&safe_name)
                    } else {
                        let version_dir = staging.join(url_version.as_deref().unwrap_or_default());
                        tokio::fs::create_dir_all(&version_dir)
                            .await
                            .map_err(|e| to_status(AppError::Io(e)))?;
                        version_dir.join(&safe_name)
                    };
                    let file = tokio::fs::File::create(&dest)
                        .await
                        .map_err(|e| to_status(AppError::Io(e)))?;
                    open = Some((safe_name.clone(), file));
                    closed.insert(safe_name.clone());
                    staged.push(crate::http::handlers::files::StagedUploadFile {
                        name: safe_name,
                        path: dest,
                        is_lma,
                    });
                }
            }
            if let Some((_, file)) = open.as_mut() {
                tokio::io::AsyncWriteExt::write_all(file, &msg.data)
                    .await
                    .map_err(|e| to_status(AppError::Io(e)))?;
            }
        }

        if staged.is_empty() {
            return Err(to_status(AppError::Validation("no files uploaded".to_string())));
        }

        let outcome = crate::http::handlers::files::finalize_upload(
            &self.app_state,
            &model_name,
            &staging,
            &staged,
            url_version.as_deref(),
            load,
            // The UploadModelRequest proto has no force field, so the gRPC
            // surface keeps its historical implicit-overwrite behavior
            // until the proto grows one (wire change, tracked separately).
            true,
        )
        .await
        .map_err(to_status)?;

        self.audit(
            &cx,
            "upload",
            &model_name,
            Some(&outcome.version),
            if url_version.is_some() {
                "gRPC versioned"
            } else {
                "model-level"
            },
        );

        Ok(Response::new(pb::UploadModelResponse {
            success: true,
            model: model_name,
            version: outcome.version,
            files: outcome.files,
            loaded: load && outcome.load_error.is_none(),
            load_error: outcome.load_error,
        }))
    }

    async fn download_model(
        &self,
        req: Request<pb::DownloadModelRequest>,
    ) -> Result<Response<Self::DownloadModelStream>, Status> {
        let cx = req.extensions().get::<RequestContext>().cloned();
        let r = req.into_inner();
        crate::validation::validate_identifier(&r.model_name).map_err(to_status)?;
        // Bare → active version (F9, same semantics as bare ready).
        let version = match r.version {
            Some(v) => {
                crate::validation::validate_version(&v).map_err(to_status)?;
                v
            }
            None => self
                .app_state
                .registry
                .get_active_version(&r.model_name)
                .ok_or_else(|| {
                    to_status(AppError::ModelNotFound(format!(
                        "{} has no active version; pass version explicitly",
                        r.model_name
                    )))
                })?,
        };

        let src = crate::http::handlers::files::resolve_download_source(
            &self.app_state,
            &r.model_name,
            &version,
            r.file.as_deref(),
        )
        .await
        .map_err(to_status)?;

        self.audit(
            &cx,
            "download",
            &r.model_name,
            Some(&version),
            &src.audit_detail,
        );

        // Stream the file in 1 MiB chunks (wire 定稿) with a terminal frame
        // carrying sha256 + size (DecoupledResponse idiom). The source — and
        // its pack temp-dir cleanup guard — lives until the spawned task
        // ends (stream done or client gone).
        //
        // F-13: open the file BEFORE replying — an open failure must fail
        // the RPC itself (a zero-chunk OK stream is indistinguishable from a
        // successful empty download), and a mid-stream read failure must
        // surface as an Err item instead of a silent truncation.
        let file = match tokio::fs::File::open(&src.path).await {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("DownloadModel: open {} failed: {}", src.path.display(), e);
                return Err(match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        Status::not_found(format!("{} no longer exists", src.path.display()))
                    }
                    _ => Status::internal(format!("failed to open {}", src.path.display())),
                });
            }
        };
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<pb::DownloadModelChunk, Status>>(4);
        let path = src.path.clone();
        tokio::spawn(async move {
            // The source's pack temp-dir cleanup guard rides along — it is
            // dropped (temp dir removed) only when this task ends (stream
            // done or client gone).
            let _guard = src;
            pump_download_chunks(file, path, tx).await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_model_config(
        &self,
        req: Request<pb::GetModelConfigRequest>,
    ) -> Result<Response<pb::GetModelConfigResponse>, Status> {
        let r = req.get_ref();
        crate::validation::validate_identifier(&r.model_name).map_err(to_status)?;
        crate::validation::validate_version(&r.version).map_err(to_status)?;
        // Shared core with the HTTP handler — same redaction and etag.
        let out = crate::http::handlers::config::model_version_config_json(
            &self.app_state,
            &r.model_name,
            &r.version,
        )
        .await
        .map_err(to_status)?;
        Ok(Response::new(pb::GetModelConfigResponse {
            model: r.model_name.clone(),
            version: r.version.clone(),
            config_json: out["config"].to_string(),
            has_file: out["has_file"].as_bool().unwrap_or(false),
            redacted: out["redacted"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default(),
            etag: out["etag"].as_str().map(String::from),
            loaded_at: out["loaded_at"].as_u64(),
        }))
    }

    async fn update_model_config(
        &self,
        req: Request<pb::UpdateModelConfigRequest>,
    ) -> Result<Response<pb::UpdateModelConfigResponse>, Status> {
        use crate::http::handlers::config::{
            ConfigPatchError, ConfigPatchMode, ConfigPatchRequest,
        };
        let cx = req.extensions().get::<RequestContext>().cloned();
        let r = req.into_inner();
        crate::validation::validate_identifier(&r.model_name).map_err(to_status)?;
        crate::validation::validate_version(&r.version).map_err(to_status)?;
        let patch: serde_json::Value = serde_json::from_str(&r.patch_json)
            .map_err(|e| Status::invalid_argument(format!("patch_json is not valid JSON: {e}")))?;
        let mode = match r.mode.as_str() {
            "" | "apply_reload" => ConfigPatchMode::ApplyReload,
            "write_only" => ConfigPatchMode::WriteOnly,
            "dry_run" => ConfigPatchMode::DryRun,
            other => {
                return Err(Status::invalid_argument(format!(
                    "unknown mode `{other}` (apply_reload | write_only | dry_run)"
                )))
            }
        };
        let preq = ConfigPatchRequest {
            patch,
            if_match: r.if_match,
            force: r.force,
            mode,
        };
        // Shared core with the HTTP handler — same merge/validate/write/
        // reload-rollback semantics.
        let out = crate::http::handlers::config::model_version_config_patch(
            &self.app_state,
            &r.model_name,
            &r.version,
            &preq,
        )
        .await
        .map_err(|e| match e {
            ConfigPatchError::App(e) => to_status(e),
            ConfigPatchError::Conflict { .. } => Status::failed_precondition(
                "config.yaml changed since the provided etag; re-read and retry, or set force",
            ),
            ConfigPatchError::Invalid { message, .. } => Status::invalid_argument(message),
            ConfigPatchError::ReloadFailed { message, rolled_back } => Status::internal(
                if rolled_back {
                    format!("reload failed: {message}; config.yaml rolled back to the previous content")
                } else {
                    format!("reload failed: {message}")
                },
            ),
        })?;
        self.audit(
            &cx,
            "config_update",
            &r.model_name,
            Some(&r.version),
            &format!("mode={} reloaded={}", r.mode, out["reloaded"]),
        );
        Ok(Response::new(pb::UpdateModelConfigResponse {
            valid: out["valid"].as_bool().unwrap_or(false),
            written: out["written"].as_bool().unwrap_or(false),
            reloaded: out["reloaded"].as_bool().unwrap_or(false),
            etag: out["etag"].as_str().map(String::from),
            warnings: out["warnings"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default(),
        }))
    }

    async fn get_server_config(
        &self,
        _req: Request<pb::GetServerConfigRequest>,
    ) -> Result<Response<pb::GetServerConfigResponse>, Status> {
        // Shared core with the HTTP handler — same redaction and sources.
        let out = crate::http::handlers::config::server_config_json(&self.app_state)
            .map_err(to_status)?;
        Ok(Response::new(pb::GetServerConfigResponse {
            config_json: out["config"].to_string(),
            sources: out["sources"]
                .as_object()
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default(),
            redacted: out["redacted"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default(),
        }))
    }

    async fn list_files(
        &self,
        req: Request<pb::ListFilesRequest>,
    ) -> Result<Response<pb::ListFilesResponse>, Status> {
        let r = req.into_inner();
        let entries = crate::http::handlers::files::list_files_impl(
            &self.app_state,
            &r.model_name,
            &r.version,
        )
        .await
        .map_err(to_status)?;
        Ok(Response::new(pb::ListFilesResponse {
            model: r.model_name,
            version: r.version,
            files: entries
                .into_iter()
                .map(|f| pb::FileEntry {
                    name: f.name,
                    size: f.size,
                    modified: f.modified,
                    is_dir: f.is_dir,
                })
                .collect(),
        }))
    }}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callback::CallbackRunner;
    use crate::config::{Config, ModelConfig};
    use crate::inference_queue::InferenceQueue;
    use crate::registry::types::ModelType;
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;
    use std::sync::atomic::AtomicBool;

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
        build_admin_service_with_repo(registry, std::env::temp_dir())
    }

    /// Same as build_admin_service but with an explicit repo path — the
    /// repository RPC tests operate on real on-disk model dirs.
    fn build_admin_service_with_repo(
        registry: Arc<ModelRegistry>,
        repo_path: std::path::PathBuf,
    ) -> GrpcAdminService {
        let queue = Arc::new(InferenceQueue::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            repo_path.clone(),
            queue.clone(),
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        let app_state = Arc::new(AppState::new(
            registry,
            wm,
            queue,
            Config::default(),
            repo_path,
            Arc::new(CallbackRunner::new()),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ));
        GrpcAdminService::new(app_state, Arc::new(AccessControl::default()))
    }

    /// build_admin_service with a caller-provided config (drift tests need
    /// a configured orchestration).
    fn build_admin_service_with_config(
        registry: Arc<ModelRegistry>,
        repo_path: std::path::PathBuf,
        config: Config,
    ) -> GrpcAdminService {
        let queue = Arc::new(InferenceQueue::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            repo_path.clone(),
            queue.clone(),
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        let app_state = Arc::new(AppState::new(
            registry,
            wm,
            queue,
            config,
            repo_path,
            Arc::new(CallbackRunner::new()),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ));
        GrpcAdminService::new(app_state, Arc::new(AccessControl::default()))
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
        spawn_admin_grpc_server_with_repo(registry, std::env::temp_dir(), Config::default()).await
    }

    /// E2E harness for the repository RPCs: real gRPC transport against a
    /// repo of the caller's choice (the server's AppState takes its
    /// repo_path from config.model_repository.path).
    async fn spawn_admin_grpc_server_with_repo(
        registry: Arc<ModelRegistry>,
        repo_path: std::path::PathBuf,
        config: Config,
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
            repo_path,
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
                config,
                has_hot_reload: Arc::new(AtomicBool::new(false)),
                cli_overrides: crate::config::CliOverrides::default(),
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

    // ===== Repository RPCs (G3/E5/F3, model-upload-and-retire plan) =====
    //
    // Parity tests aligned with the HTTP cases (plan 3.5): force semantics,
    // keep sorting, partial-failure detail, drift structure, the three file
    // RPCs, DownloadModel stream integrity (sha256 terminal frame) and
    // DeleteVersion linked-artifact cleanup (G5 alignment, D4).

    fn unique_repo(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "lite-server-grpc-repo-{}-{}-{}",
            tag,
            std::process::id(),
            uuid::Uuid::new_v4()
        ))
    }

    fn config_with_repo(repo: &std::path::Path) -> Config {
        let mut config = Config::default();
        config.model_repository.path = repo.display().to_string();
        config
    }

    async fn make_disk_version(repo: &std::path::Path, model: &str, version: &str) {
        let dir = repo.join(model).join(version);
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("model.py"), "def predict(x): return x")
            .await
            .unwrap();
    }

    async fn connect_admin(port: u16) -> pb::admin_client::AdminClient<tonic::transport::Channel> {
        let channel = tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{}", port))
            .unwrap()
            .connect()
            .await
            .expect("must connect to the Admin gRPC server");
        pb::admin_client::AdminClient::new(channel)
    }

    // ===== DeleteVersion (G3, aligned with the HTTP G1/E3 cases) =====

    #[tokio::test]
    async fn delete_version_removes_disk_version_and_is_idempotent() {
        let repo = unique_repo("delv");
        make_disk_version(&repo, "mymodel", "1").await;
        let svc = build_admin_service_with_repo(Arc::new(ModelRegistry::new()), repo.clone());

        let resp = svc
            .delete_version(Request::new(pb::DeleteVersionRequest {
                model_name: "mymodel".to_string(),
                version: "1".to_string(),
                force: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.success);
        assert!(!repo.join("mymodel").join("1").exists(), "version dir must be removed");

        // Idempotent (HTTP parity): deleting a nonexistent version succeeds.
        let resp2 = svc
            .delete_version(Request::new(pb::DeleteVersionRequest {
                model_name: "mymodel".to_string(),
                version: "1".to_string(),
                force: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp2.success);

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn delete_version_active_without_force_is_failed_precondition() {
        let repo = unique_repo("delv-active");
        make_disk_version(&repo, "mymodel", "1").await;
        let reg = two_version_registry("mymodel"); // v1 active, v2 ready
        let svc = build_admin_service_with_repo(reg, repo.clone());

        let err = svc
            .delete_version(Request::new(pb::DeleteVersionRequest {
                model_name: "mymodel".to_string(),
                version: "1".to_string(),
                force: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err}");
        assert!(
            repo.join("mymodel").join("1").exists(),
            "refused delete must leave the version dir intact"
        );

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn delete_version_force_deletes_active_version() {
        let repo = unique_repo("delv-force");
        make_disk_version(&repo, "mymodel", "1").await;
        let reg = two_version_registry("mymodel");
        let svc = build_admin_service_with_repo(reg, repo.clone());

        let resp = svc
            .delete_version(Request::new(pb::DeleteVersionRequest {
                model_name: "mymodel".to_string(),
                version: "1".to_string(),
                force: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.success);
        assert!(!repo.join("mymodel").join("1").exists());

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn delete_version_cleans_linked_artifacts_g5() {
        let repo = unique_repo("delv-g5");
        make_disk_version(&repo, "mymodel", "1").await;
        let root_artifact = repo.join("mymodel_v1.lma");
        tokio::fs::write(&root_artifact, b"artifact").await.unwrap();
        let artifacts_dir = repo.join(".artifacts");
        tokio::fs::create_dir_all(&artifacts_dir).await.unwrap();
        let copy = artifacts_dir.join("mymodel_v1.lma");
        tokio::fs::write(&copy, b"artifact").await.unwrap();

        let svc = build_admin_service_with_repo(Arc::new(ModelRegistry::new()), repo.clone());
        svc.delete_version(Request::new(pb::DeleteVersionRequest {
            model_name: "mymodel".to_string(),
            version: "1".to_string(),
            force: false,
        }))
        .await
        .unwrap();

        assert!(!root_artifact.exists(), "root .lma must be removed (G5)");
        assert!(!copy.exists(), ".artifacts copy must be removed (G5)");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    // ===== DeleteVersions (E2 parity) =====

    #[tokio::test]
    async fn delete_versions_keep_retains_highest() {
        let repo = unique_repo("delvs-keep");
        for v in ["1", "2", "3"] {
            make_disk_version(&repo, "mymodel", v).await;
        }
        let svc = build_admin_service_with_repo(Arc::new(ModelRegistry::new()), repo.clone());

        let resp = svc
            .delete_versions(Request::new(pb::DeleteVersionsRequest {
                model_name: "mymodel".to_string(),
                keep: Some(2),
                versions: vec![],
                force: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.deleted, vec!["1".to_string()], "lowest version deleted");
        assert!(resp.failed.is_empty());
        assert!(!repo.join("mymodel").join("1").exists());
        assert!(repo.join("mymodel").join("2").exists());
        assert!(repo.join("mymodel").join("3").exists());

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn delete_versions_partial_failure_reports_active_without_force() {
        let repo = unique_repo("delvs-partial");
        make_disk_version(&repo, "mymodel", "1").await;
        make_disk_version(&repo, "mymodel", "2").await;
        let reg = two_version_registry("mymodel"); // v1 active
        let svc = build_admin_service_with_repo(reg, repo.clone());

        let resp = svc
            .delete_versions(Request::new(pb::DeleteVersionsRequest {
                model_name: "mymodel".to_string(),
                keep: None,
                versions: vec!["1".to_string(), "2".to_string()],
                force: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.deleted, vec!["2".to_string()]);
        assert_eq!(resp.failed.len(), 1, "active v1 without force fails individually");
        assert_eq!(resp.failed[0].version, "1");
        assert!(!resp.failed[0].error.is_empty());
        assert!(repo.join("mymodel").join("1").exists());

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn delete_versions_all_failed_is_internal() {
        let repo = unique_repo("delvs-allfail");
        let reg = two_version_registry("mymodel");
        let svc = build_admin_service_with_repo(reg, repo.clone());

        let err = svc
            .delete_versions(Request::new(pb::DeleteVersionsRequest {
                model_name: "mymodel".to_string(),
                keep: None,
                versions: vec!["1".to_string()],
                force: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal, "{err}");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    // ===== DeleteModel (E1 parity) =====

    #[tokio::test]
    async fn delete_model_removes_dir_and_all_linked_artifacts() {
        let repo = unique_repo("delm");
        make_disk_version(&repo, "mymodel", "1").await;
        make_disk_version(&repo, "mymodel", "2").await;
        let root_artifact = repo.join("mymodel_v1.lma");
        tokio::fs::write(&root_artifact, b"artifact").await.unwrap();
        let artifacts_dir = repo.join(".artifacts");
        tokio::fs::create_dir_all(&artifacts_dir).await.unwrap();
        let copy = artifacts_dir.join("mymodel_v1.lma");
        tokio::fs::write(&copy, b"artifact").await.unwrap();

        let svc = build_admin_service_with_repo(Arc::new(ModelRegistry::new()), repo.clone());
        let resp = svc
            .delete_model(Request::new(pb::DeleteModelRequest {
                model_name: "mymodel".to_string(),
                force: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.success);
        assert!(!repo.join("mymodel").exists(), "whole model dir must be removed");
        assert!(!root_artifact.exists(), "root .lma must be removed (G5)");
        assert!(!copy.exists(), ".artifacts copy must be removed (G5)");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn delete_model_with_active_requires_force() {
        let repo = unique_repo("delm-active");
        make_disk_version(&repo, "mymodel", "1").await;
        let reg = two_version_registry("mymodel");
        let svc = build_admin_service_with_repo(reg, repo.clone());

        let err = svc
            .delete_model(Request::new(pb::DeleteModelRequest {
                model_name: "mymodel".to_string(),
                force: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err}");
        assert!(repo.join("mymodel").exists());

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    // ===== RepositoryDrift (E4 parity) =====

    fn strategy(name: &str, versions_to_load: &[&str]) -> crate::config::ModelStrategyConfig {
        crate::config::ModelStrategyConfig {
            name: name.to_string(),
            load_policy: "explicit".to_string(),
            versions_to_load: versions_to_load.iter().map(|v| v.to_string()).collect(),
            default_version: None,
            max_loaded_versions: None,
            weights: None,
        }
    }

    #[tokio::test]
    async fn repository_drift_reports_missing_and_unconfigured_with_size() {
        let repo = unique_repo("drift");
        make_disk_version(&repo, "mymodel", "2").await;
        tokio::fs::write(repo.join("mymodel").join("2").join("model.py"), "payload")
            .await
            .unwrap();

        let mut config = config_with_repo(&repo);
        config.orchestration.load_models = vec!["mymodel".to_string()];
        config.orchestration.models = vec![strategy("mymodel", &["1"])];
        let svc = build_admin_service_with_config(
            Arc::new(ModelRegistry::new()),
            repo.clone(),
            config,
        );

        let resp = svc
            .repository_drift(Request::new(pb::RepositoryDriftRequest { model_name: None }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.configured_missing.len(), 1);
        assert_eq!(resp.configured_missing[0].model, "mymodel");
        assert_eq!(resp.configured_missing[0].version, "1");
        assert_eq!(resp.on_disk_unconfigured.len(), 1);
        assert_eq!(resp.on_disk_unconfigured[0].version, "2");
        assert!(resp.on_disk_unconfigured[0].size_bytes > 0);
        assert!(!resp.on_disk_unconfigured[0].ensemble_referenced);

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn repository_drift_model_filter_limits_scope() {
        let repo = unique_repo("drift-filter");
        make_disk_version(&repo, "model_a", "1").await;
        make_disk_version(&repo, "model_b", "1").await;
        let svc = build_admin_service_with_repo(Arc::new(ModelRegistry::new()), repo.clone());

        let resp = svc
            .repository_drift(Request::new(pb::RepositoryDriftRequest {
                model_name: Some("model_a".to_string()),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.on_disk_unconfigured.len(), 1);
        assert_eq!(resp.on_disk_unconfigured[0].model, "model_a");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    // ===== UploadModel / DownloadModel / ListFiles (F3, wire 定稿) =====

    #[tokio::test]
    async fn upload_list_download_round_trip_over_real_transport() {
        let repo = unique_repo("e2e-files");
        let (port, shutdown_tx, _handle) = spawn_admin_grpc_server_with_repo(
            Arc::new(ModelRegistry::new()),
            repo.clone(),
            config_with_repo(&repo),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut client = connect_admin(port).await;

        // Chunked raw upload: model.py split across two messages sharing one
        // file_name (loose-field wire: metadata on the first message only).
        let content = b"def predict(x):\n    return x\n";
        let split = 10;
        let resp = client
            .upload_model(tokio_stream::iter(vec![
                pb::UploadModelRequest {
                    model_name: "mymodel".to_string(),
                    version: Some("1".to_string()),
                    load: Some(false),
                    file_name: String::new(),
                    data: Bytes::new(),
                },
                pb::UploadModelRequest {
                    model_name: String::new(),
                    version: None,
                    load: None,
                    file_name: "model.py".to_string(),
                    data: Bytes::copy_from_slice(&content[..split]),
                },
                pb::UploadModelRequest {
                    model_name: String::new(),
                    version: None,
                    load: None,
                    file_name: "model.py".to_string(),
                    data: Bytes::copy_from_slice(&content[split..]),
                },
            ]))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.success);
        assert_eq!(resp.model, "mymodel");
        assert_eq!(resp.version, "1");
        assert_eq!(resp.files, vec!["model.py".to_string()]);
        assert!(!resp.loaded, "load=false must not auto-load");
        assert!(repo.join("mymodel").join("1").join("model.py").exists());

        // ListFiles sees the uploaded file.
        let list = client
            .list_files(pb::ListFilesRequest {
                model_name: "mymodel".to_string(),
                version: "1".to_string(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(list.files.len(), 1);
        assert_eq!(list.files[0].name, "model.py");
        assert_eq!(list.files[0].size, content.len() as u64);
        assert!(!list.files[0].is_dir);

        // DownloadModel single file: reassemble the chunks and verify the
        // terminal frame (sha256 + size, DecoupledResponse idiom).
        let mut stream = client
            .download_model(pb::DownloadModelRequest {
                model_name: "mymodel".to_string(),
                version: Some("1".to_string()),
                file: Some("model.py".to_string()),
            })
            .await
            .unwrap()
            .into_inner();
        let mut assembled = Vec::new();
        let mut terminal: Option<pb::DownloadModelChunk> = None;
        while let Some(chunk) = stream.message().await.unwrap() {
            if chunk.is_final {
                terminal = Some(chunk);
            } else {
                assembled.extend_from_slice(&chunk.data);
            }
        }
        assert_eq!(assembled, content, "streamed bytes must match the upload");
        let term = terminal.expect("the stream must end with a terminal frame");
        let mut hasher = Sha256::new();
        hasher.update(content);
        assert_eq!(term.sha256, format!("{:x}", hasher.finalize()));
        assert_eq!(term.size, content.len() as u64);

        let _ = shutdown_tx.send(());
        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn download_model_serves_original_artifact_without_repack() {
        let repo = unique_repo("e2e-artifact");
        make_disk_version(&repo, "mymodel", "1").await;
        // F10b passthrough: a retained artifact is served byte-identical
        // (no python pack involved).
        let artifact_bytes = b"FAKE-ORIGINAL-ARTIFACT-BYTES";
        tokio::fs::write(repo.join("mymodel_v1.lma"), artifact_bytes)
            .await
            .unwrap();
        let (port, shutdown_tx, _handle) = spawn_admin_grpc_server_with_repo(
            Arc::new(ModelRegistry::new()),
            repo.clone(),
            config_with_repo(&repo),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut client = connect_admin(port).await;

        let mut stream = client
            .download_model(pb::DownloadModelRequest {
                model_name: "mymodel".to_string(),
                version: Some("1".to_string()),
                file: None,
            })
            .await
            .unwrap()
            .into_inner();
        let mut assembled = Vec::new();
        let mut terminal: Option<pb::DownloadModelChunk> = None;
        while let Some(chunk) = stream.message().await.unwrap() {
            if chunk.is_final {
                terminal = Some(chunk);
            } else {
                assembled.extend_from_slice(&chunk.data);
            }
        }
        assert_eq!(
            assembled, artifact_bytes,
            "full download must serve the original artifact byte-identical"
        );
        let term = terminal.expect("terminal frame required");
        assert_eq!(term.size, artifact_bytes.len() as u64);

        let _ = shutdown_tx.send(());
        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn download_model_bare_uses_active_version() {
        let repo = unique_repo("e2e-bare");
        make_disk_version(&repo, "mymodel", "1").await;
        tokio::fs::write(repo.join("mymodel").join("1").join("model.py"), b"ACTIVE-ONE")
            .await
            .unwrap();
        let reg = two_version_registry("mymodel"); // v1 active
        let (port, shutdown_tx, _handle) =
            spawn_admin_grpc_server_with_repo(reg, repo.clone(), config_with_repo(&repo)).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut client = connect_admin(port).await;

        let mut stream = client
            .download_model(pb::DownloadModelRequest {
                model_name: "mymodel".to_string(),
                version: None,
                file: Some("model.py".to_string()),
            })
            .await
            .unwrap()
            .into_inner();
        let mut assembled = Vec::new();
        while let Some(chunk) = stream.message().await.unwrap() {
            if !chunk.is_final {
                assembled.extend_from_slice(&chunk.data);
            }
        }
        assert_eq!(assembled, b"ACTIVE-ONE", "bare download must target the active version");

        let _ = shutdown_tx.send(());
        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    /// F-13 (functional-defects-plan.md): `File::open` failure inside the
    /// download task is only logged and swallowed (warn + return) — the
    /// RPC already replied OK, so the client receives a zero-chunk empty
    /// stream indistinguishable from a successful download. The fix must
    /// surface the failure as a non-OK status (NotFound/Internal) through
    /// the channel. Red: the current implementation returns OK + empty
    /// stream. (A chmod-000 file is a deterministic open-failure: exists /
    /// canonicalize / metadata all succeed, open(O_RDONLY) fails EACCES —
    /// no delete race needed.)
    #[cfg(unix)]
    #[tokio::test]
    async fn test_f13_download_open_failure_yields_error_status() {
        let repo = unique_repo("f13-open-fail");
        make_disk_version(&repo, "mymodel", "1").await;
        std::fs::set_permissions(
            repo.join("mymodel").join("1").join("model.py"),
            std::os::unix::fs::PermissionsExt::from_mode(0o000),
        )
        .unwrap();

        let (port, shutdown_tx, _handle) = spawn_admin_grpc_server_with_repo(
            Arc::new(ModelRegistry::new()),
            repo.clone(),
            config_with_repo(&repo),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut client = connect_admin(port).await;

        let resp = client
            .download_model(pb::DownloadModelRequest {
                model_name: "mymodel".to_string(),
                version: Some("1".to_string()),
                file: Some("model.py".to_string()),
            })
            .await;
        assert!(
            resp.is_err(),
            "an open failure must fail the RPC; today it returns OK with \
             a zero-chunk stream"
        );

        let _ = shutdown_tx.send(());
        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    /// L2 (leak-gap-audit-0820): the terminal frame's send must be bounded
    /// by DOWNLOAD_SEND_TIMEOUT like every other send in the pump. A client
    /// that stops reading with the channel exactly full at EOF must not
    /// park the download task (file handle + pack temp-dir guard) forever.
    #[tokio::test(start_paused = true)]
    async fn download_terminal_frame_send_is_bounded() {
        let tmp = unique_repo("dl-terminal-timeout");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let path = tmp.join("big.bin");
        // Exactly 4 MiB: four 1 MiB chunks fill the 4-slot channel
        // precisely; the pump then hits EOF with the channel full and the
        // terminal-frame send stalls (the receiver is never polled).
        tokio::fs::write(&path, vec![7u8; 4 * 1024 * 1024]).await.unwrap();
        let file = tokio::fs::File::open(&path).await.unwrap();
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<Result<pb::DownloadModelChunk, Status>>(4);
        let task = tokio::spawn(pump_download_chunks(file, path.clone(), tx));

        // Virtual clock: the pump's 60s send bound must fire long before
        // this 300s guard; without it the task parks forever and the guard
        // trips.
        tokio::time::timeout(std::time::Duration::from_secs(300), task)
            .await
            .expect("the download task must exit once the stalled terminal send times out")
            .unwrap();

        // The 4 buffered chunks drain; the sender is gone — no terminal
        // frame could be delivered.
        let mut chunks = 0;
        while let Some(item) = rx.recv().await {
            let c = item.expect("chunks sent before the stall are valid");
            assert!(!c.is_final);
            chunks += 1;
        }
        assert_eq!(chunks, 4);

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn upload_model_model_level_rejects_raw_file_with_guidance() {
        let repo = unique_repo("e2e-reject");
        let (port, shutdown_tx, _handle) = spawn_admin_grpc_server_with_repo(
            Arc::new(ModelRegistry::new()),
            repo.clone(),
            config_with_repo(&repo),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut client = connect_admin(port).await;

        let err = client
            .upload_model(tokio_stream::iter(vec![
                pb::UploadModelRequest {
                    model_name: "mymodel".to_string(),
                    version: None,
                    load: Some(false),
                    file_name: String::new(),
                    data: Bytes::new(),
                },
                pb::UploadModelRequest {
                    model_name: String::new(),
                    version: None,
                    load: None,
                    file_name: "model.py".to_string(),
                    data: Bytes::from_static(b"x = 1"),
                },
            ]))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("versioned endpoint"),
            "the error must guide the client to the versioned endpoint: {err}"
        );

        let _ = shutdown_tx.send(());
        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn upload_model_lma_uses_manifest_version_and_lands_flat() {
        // Pack a real .lma with the python CLI (same helper pattern as the
        // HTTP e2e tests), then model-level upload it: the version must
        // come from the manifest (wire 定稿: version absent → F8).
        let pack_tmp = unique_repo("pack-src");
        let model_dir = pack_tmp.join("pkgmodel").join("2");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join("model.py"), "def predict(x): return x").unwrap();
        std::fs::write(model_dir.join("config.yaml"), "max_batch_size: 1\nbatch_timeout: 0.0\n")
            .unwrap();
        let output = std::process::Command::new(crate::python::resolve_python_interpreter())
            .args([
                "-m",
                "lite_server.cli",
                "pack",
                pack_tmp.join("pkgmodel").to_str().unwrap(),
                "--version",
                "2",
                "--output",
                pack_tmp.to_str().unwrap(),
            ])
            .output()
            .expect("failed to run lite-server pack");
        assert!(
            output.status.success(),
            "pack failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let artifact = pack_tmp.join("pkgmodel_v2.lma");
        let artifact_bytes = tokio::fs::read(&artifact).await.unwrap();

        let repo = unique_repo("e2e-lma");
        let (port, shutdown_tx, _handle) = spawn_admin_grpc_server_with_repo(
            Arc::new(ModelRegistry::new()),
            repo.clone(),
            config_with_repo(&repo),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut client = connect_admin(port).await;

        // Chunked .lma stream, model-level (no version on the first message).
        let mid = artifact_bytes.len() / 2;
        let resp = client
            .upload_model(tokio_stream::iter(vec![
                pb::UploadModelRequest {
                    model_name: "pkgmodel".to_string(),
                    version: None,
                    load: Some(false),
                    file_name: String::new(),
                    data: Bytes::new(),
                },
                pb::UploadModelRequest {
                    model_name: String::new(),
                    version: None,
                    load: None,
                    file_name: "pkgmodel_v2.lma".to_string(),
                    data: Bytes::copy_from_slice(&artifact_bytes[..mid]),
                },
                pb::UploadModelRequest {
                    model_name: String::new(),
                    version: None,
                    load: None,
                    file_name: "pkgmodel_v2.lma".to_string(),
                    data: Bytes::copy_from_slice(&artifact_bytes[mid..]),
                },
            ]))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.success);
        assert_eq!(resp.version, "2", "version must come from the manifest");
        // Flat landing (batch-0 placement fix): no {name}/{v}/{v}/ nesting.
        assert!(repo.join("pkgmodel").join("2").join("model.py").exists());
        assert!(!repo.join("pkgmodel").join("2").join("2").exists());
        // F10a retention feeds the F10b passthrough.
        assert!(repo.join(".artifacts").join("pkgmodel_v2.lma").exists());

        // Full download serves the retained artifact byte-identical
        // (F10b passthrough — no repack).
        let mut stream = client
            .download_model(pb::DownloadModelRequest {
                model_name: "pkgmodel".to_string(),
                version: Some("2".to_string()),
                file: None,
            })
            .await
            .unwrap()
            .into_inner();
        let mut assembled = Vec::new();
        let mut terminal: Option<pb::DownloadModelChunk> = None;
        while let Some(chunk) = stream.message().await.unwrap() {
            if chunk.is_final {
                terminal = Some(chunk);
            } else {
                assembled.extend_from_slice(&chunk.data);
            }
        }
        assert_eq!(assembled, artifact_bytes, "download must serve the retained artifact");
        let term = terminal.expect("terminal frame required");
        assert_eq!(term.size, artifact_bytes.len() as u64);

        let _ = shutdown_tx.send(());
        let _ = tokio::fs::remove_dir_all(&repo).await;
        let _ = tokio::fs::remove_dir_all(&pack_tmp).await;
    }

    // ===== Audit-evidence tests (batch 3+4 review) =====

    #[tokio::test]
    async fn upload_model_first_message_file_data_is_not_silently_dropped() {
        // Loose-field wire: the first message carries the metadata. A client
        // that also packs file content into it must get either a rejection
        // or faithful storage — silently dropping those bytes corrupts the
        // uploaded model without any error.
        let repo = unique_repo("first-chunk");
        let (port, shutdown_tx, _handle) = spawn_admin_grpc_server_with_repo(
            Arc::new(ModelRegistry::new()),
            repo.clone(),
            config_with_repo(&repo),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut client = connect_admin(port).await;

        let result = client
            .upload_model(tokio_stream::iter(vec![
                pb::UploadModelRequest {
                    model_name: "mymodel".to_string(),
                    version: Some("1".to_string()),
                    load: Some(false),
                    file_name: "model.py".to_string(),
                    data: Bytes::from_static(b"FIRST-"),
                },
                pb::UploadModelRequest {
                    model_name: String::new(),
                    version: None,
                    load: None,
                    file_name: "model.py".to_string(),
                    data: Bytes::from_static(b"SECOND"),
                },
            ]))
            .await;
        match result {
            Ok(resp) => {
                assert!(resp.into_inner().success);
                let content = tokio::fs::read(repo.join("mymodel").join("1").join("model.py"))
                    .await
                    .unwrap();
                assert_eq!(
                    content, b"FIRST-SECOND",
                    "the first message's file bytes must not be silently dropped"
                );
            }
            Err(e) => assert_eq!(
                e.code(),
                tonic::Code::InvalidArgument,
                "a wire violation should surface as a client error, not silent loss: {e}"
            ),
        }

        let _ = shutdown_tx.send(());
        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    /// Capture `lite_server::audit` target events (same pattern as
    /// audit.rs tests: scoped dispatch + rebuild_interest_cache so the
    /// callsite interest cache cannot short-circuit the capture).
    #[derive(Default)]
    struct AuditRec {
        fields: Vec<(String, String)>,
    }

    struct AuditCapture(std::sync::Arc<std::sync::Mutex<AuditRec>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AuditCapture {
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

    #[test]
    fn delete_version_audit_carries_request_context() {
        // D27: control-plane mutations audit principal / peer / request_id.
        // The gRPC interceptor stashes a RequestContext in the request
        // extensions (the older Admin RPCs read it from there); the
        // repository RPCs must thread it into the audit record too.
        let repo = std::env::temp_dir().join(format!(
            "lite-server-grpc-audit-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let repo2 = repo.clone();
        let rec = std::sync::Arc::new(std::sync::Mutex::new(AuditRec::default()));
        let rec2 = rec.clone();
        use tracing_subscriber::layer::SubscriberExt;
        let dispatch =
            tracing::Dispatch::new(tracing_subscriber::registry().with(AuditCapture(rec)));
        // Without the global always-on subscriber, a parallel test thread
        // on the no-op dispatcher can cache NEVER for the audit callsite
        // between the rebuild below and the audit event (the module-only
        // flake: full-suite runs install it early via event_counts tests).
        crate::test_tracing::ensure_always_on_subscriber();
        let handle = std::thread::spawn(move || {
            let _guard = tracing::dispatcher::set_default(&dispatch);
            tracing::callsite::rebuild_interest_cache();
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let dir = repo2.join("mymodel").join("1");
                tokio::fs::create_dir_all(&dir).await.unwrap();
                tokio::fs::write(dir.join("model.py"), "def predict(x): return x")
                    .await
                    .unwrap();
                let svc = build_admin_service_with_repo(
                    Arc::new(ModelRegistry::new()),
                    repo2.clone(),
                );
                let mut req = Request::new(pb::DeleteVersionRequest {
                    model_name: "mymodel".to_string(),
                    version: "1".to_string(),
                    force: false,
                });
                req.extensions_mut().insert(RequestContext {
                    request_id: "grpc-rid-1".to_string(),
                    client_ip: "10.1.2.3".to_string(),
                    trace_cx: opentelemetry::Context::new(),
                    protocol: Protocol::Grpc,
                    principal: None,
                    api_protocol: None,
                });
                svc.delete_version(req).await.expect("delete must succeed");
                let _ = tokio::fs::remove_dir_all(&repo2).await;
            });
        });
        handle.join().unwrap();

        let fields = &rec2.lock().unwrap().fields;
        let get = |name: &str| {
            fields
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| panic!("audit field {name} missing: {fields:?}"))
        };
        assert_eq!(get("action"), "delete");
        assert_eq!(
            get("request_id"),
            "grpc-rid-1",
            "the audit record must carry the request_id from the gRPC interceptor context"
        );
        assert_eq!(get("client_ip"), "10.1.2.3");
    }

    // ===== Wire-validation + pack-path coverage (plan F3/F11 test matrix) =====

    #[tokio::test]
    async fn upload_model_cumulative_cap_is_resource_exhausted() {
        // F11b: the cumulative cap applies across chunks of the stream.
        let repo = unique_repo("cap");
        let mut config = config_with_repo(&repo);
        config.server.max_upload_bytes = Some(10);
        let (port, shutdown_tx, _handle) = spawn_admin_grpc_server_with_repo(
            Arc::new(ModelRegistry::new()),
            repo.clone(),
            config,
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut client = connect_admin(port).await;

        let err = client
            .upload_model(tokio_stream::iter(vec![
                pb::UploadModelRequest {
                    model_name: "mymodel".to_string(),
                    version: Some("1".to_string()),
                    load: Some(false),
                    file_name: String::new(),
                    data: Bytes::new(),
                },
                pb::UploadModelRequest {
                    model_name: String::new(),
                    version: None,
                    load: None,
                    file_name: "model.py".to_string(),
                    data: Bytes::from_static(b"12345678"),
                },
                pb::UploadModelRequest {
                    model_name: String::new(),
                    version: None,
                    load: None,
                    file_name: "model.py".to_string(),
                    data: Bytes::from_static(b"9abcdef0"), // cumulative 16 > 10
                },
            ]))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted, "{err}");

        // The staging dir must be cleaned on rejection (StagingGuard drops;
        // the removal is spawned, so poll briefly).
        for _ in 0..50 {
            let mut entries = tokio::fs::read_dir(&repo).await.unwrap();
            if entries.next_entry().await.unwrap().is_none() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let mut entries = tokio::fs::read_dir(&repo).await.unwrap();
        assert!(
            entries.next_entry().await.unwrap().is_none(),
            "cap rejection must leave no staging residue"
        );

        let _ = shutdown_tx.send(());
        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn upload_model_rejects_multiple_lma_artifacts() {
        // Wire 定稿: at most one .lma per UploadModel (counted per FILE, so
        // chunked single-.lma streams pass).
        let repo = unique_repo("multi-lma");
        let (port, shutdown_tx, _handle) = spawn_admin_grpc_server_with_repo(
            Arc::new(ModelRegistry::new()),
            repo.clone(),
            config_with_repo(&repo),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut client = connect_admin(port).await;

        let err = client
            .upload_model(tokio_stream::iter(vec![
                pb::UploadModelRequest {
                    model_name: "mymodel".to_string(),
                    version: Some("1".to_string()),
                    load: Some(false),
                    file_name: String::new(),
                    data: Bytes::new(),
                },
                pb::UploadModelRequest {
                    model_name: String::new(),
                    version: None,
                    load: None,
                    file_name: "a.lma".to_string(),
                    data: Bytes::from_static(b"x"),
                },
                pb::UploadModelRequest {
                    model_name: String::new(),
                    version: None,
                    load: None,
                    file_name: "b.lma".to_string(),
                    data: Bytes::from_static(b"y"),
                },
            ]))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err}");
        assert!(
            err.message().contains("at most one .lma"),
            "error must name the rule: {err}"
        );

        let _ = shutdown_tx.send(());
        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn upload_model_rejects_interleaved_file_chunks() {
        // Chunks of one file must be contiguous: A, B, A would silently
        // corrupt A if the second A were appended (or truncated if
        // recreated) — rejected instead.
        let repo = unique_repo("interleave");
        let (port, shutdown_tx, _handle) = spawn_admin_grpc_server_with_repo(
            Arc::new(ModelRegistry::new()),
            repo.clone(),
            config_with_repo(&repo),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut client = connect_admin(port).await;

        let err = client
            .upload_model(tokio_stream::iter(vec![
                pb::UploadModelRequest {
                    model_name: "mymodel".to_string(),
                    version: Some("1".to_string()),
                    load: Some(false),
                    file_name: String::new(),
                    data: Bytes::new(),
                },
                pb::UploadModelRequest {
                    model_name: String::new(),
                    version: None,
                    load: None,
                    file_name: "a.py".to_string(),
                    data: Bytes::from_static(b"x"),
                },
                pb::UploadModelRequest {
                    model_name: String::new(),
                    version: None,
                    load: None,
                    file_name: "b.py".to_string(),
                    data: Bytes::from_static(b"y"),
                },
                pb::UploadModelRequest {
                    model_name: String::new(),
                    version: None,
                    load: None,
                    file_name: "a.py".to_string(),
                    data: Bytes::from_static(b"z"),
                },
            ]))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err}");
        assert!(
            err.message().contains("contiguous"),
            "error must name the rule: {err}"
        );

        let _ = shutdown_tx.send(());
        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    #[serial_test::serial(download_tmp)]
    async fn download_model_full_packs_fresh_when_no_artifact() {
        // No retained artifact anywhere → the version dir is packed into a
        // temp .lma and streamed; the stream must reassemble into a package
        // that unpacks back to the original tree (round-trip), and the
        // terminal frame must describe exactly those bytes. (Temp-dir
        // cleanup itself is unit-tested via DownloadCleanup in files.rs;
        // asserting against the shared global temp here would race parallel
        // tests.)
        let repo = unique_repo("fresh-pack");
        make_disk_version(&repo, "mymodel", "1").await;
        let (port, shutdown_tx, _handle) = spawn_admin_grpc_server_with_repo(
            Arc::new(ModelRegistry::new()),
            repo.clone(),
            config_with_repo(&repo),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut client = connect_admin(port).await;

        let mut stream = client
            .download_model(pb::DownloadModelRequest {
                model_name: "mymodel".to_string(),
                version: Some("1".to_string()),
                file: None,
            })
            .await
            .unwrap()
            .into_inner();
        let mut assembled = Vec::new();
        let mut terminal: Option<pb::DownloadModelChunk> = None;
        while let Some(chunk) = stream.message().await.unwrap() {
            if chunk.is_final {
                terminal = Some(chunk);
            } else {
                assembled.extend_from_slice(&chunk.data);
            }
        }
        let term = terminal.expect("the stream must end with a terminal frame");
        let mut hasher = Sha256::new();
        hasher.update(&assembled);
        assert_eq!(term.sha256, format!("{:x}", hasher.finalize()));
        assert_eq!(term.size, assembled.len() as u64);
        assert_eq!(&assembled[..2], b"PK", "a fresh pack must be a zip");

        // Round-trip: unpack the streamed package and compare the tree.
        let out = unique_repo("fresh-pack-out");
        tokio::fs::create_dir_all(&out).await.unwrap();
        let lma = out.join("mymodel_v1.lma");
        tokio::fs::write(&lma, &assembled).await.unwrap();
        let unpack = crate::http::handlers::files::run_unpack(
            &crate::python::resolve_python_interpreter(),
            &lma,
            &out,
            Some("1"),
            std::time::Duration::from_secs(60),
        )
        .await
        .expect("unpack must run");
        assert!(
            unpack.status.success(),
            "unpack failed: {}",
            String::from_utf8_lossy(&unpack.stderr)
        );
        let model_py = tokio::fs::read(out.join("1").join("model.py")).await.unwrap();
        assert_eq!(model_py, b"def predict(x): return x");

        let _ = shutdown_tx.send(());
        let _ = tokio::fs::remove_dir_all(&repo).await;
        let _ = tokio::fs::remove_dir_all(&out).await;
    }

    // ===== E5 error-code mapping coverage (2026-08-14 audit gap) =====

    /// E5: HTTP 400-class validation failures map to InvalidArgument.
    #[tokio::test]
    async fn delete_versions_invalid_requests_are_invalid_argument() {
        let repo = unique_repo("delvs-invalid");
        make_disk_version(&repo, "mymodel", "1").await;
        let svc = build_admin_service_with_repo(Arc::new(ModelRegistry::new()), repo.clone());

        // keep=0 (unset on the wire) + no versions list → neither selector.
        let err = svc
            .delete_versions(Request::new(pb::DeleteVersionsRequest {
                model_name: "mymodel".to_string(),
                keep: Some(0),
                versions: vec![],
                force: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "no selector: {err}");

        // An invalid version string in the list.
        let err = svc
            .delete_versions(Request::new(pb::DeleteVersionsRequest {
                model_name: "mymodel".to_string(),
                keep: None,
                versions: vec!["bad ver!".to_string()],
                force: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "bad version: {err}");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    /// E5: drift with an invalid model name → InvalidArgument.
    #[tokio::test]
    async fn repository_drift_invalid_model_name_is_invalid_argument() {
        let repo = unique_repo("drift-invalid");
        let svc = build_admin_service_with_repo(Arc::new(ModelRegistry::new()), repo.clone());

        let err = svc
            .repository_drift(Request::new(pb::RepositoryDriftRequest {
                model_name: Some("bad name!".to_string()),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err}");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    /// E5: a missing model maps to NotFound (list files + download).
    #[tokio::test]
    async fn missing_model_is_not_found() {
        let repo = unique_repo("missing-model");
        let svc = build_admin_service_with_repo(Arc::new(ModelRegistry::new()), repo.clone());

        let err = svc
            .list_files(Request::new(pb::ListFilesRequest {
                model_name: "ghost".to_string(),
                version: "1".to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound, "list_files: {err}");

        let err = svc
            .download_model(Request::new(pb::DownloadModelRequest {
                model_name: "ghost".to_string(),
                version: Some("1".to_string()),
                file: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound, "download_model: {err}");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    // ===== GetModelConfig (M1) =====

    #[tokio::test]
    async fn get_model_config_returns_redacted_tree_and_etag() {
        let repo = unique_repo("get-model-config");
        let dir = repo.join("cfg_m").join("1");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(
            dir.join("config.yaml"),
            "max_batch_size: 8\npolicies:\n  auth:\n    keys: [alpha, beta]\n",
        )
        .await
        .unwrap();
        let svc = build_admin_service_with_repo(Arc::new(ModelRegistry::new()), repo.clone());

        let resp = svc
            .get_model_config(Request::new(pb::GetModelConfigRequest {
                model_name: "cfg_m".to_string(),
                version: "1".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.has_file);
        assert_eq!(resp.etag.as_deref().map(str::len), Some(16));
        assert_eq!(resp.redacted, vec!["policies.auth.keys".to_string()]);
        let config: serde_json::Value = serde_json::from_str(&resp.config_json).unwrap();
        assert_eq!(config["max_batch_size"], serde_json::json!(8));
        assert_eq!(
            config["policies"]["auth"]["keys"],
            serde_json::json!(["***", "***"])
        );

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn get_model_config_unknown_version_is_not_found() {
        let svc = build_admin_service_with_repo(
            Arc::new(ModelRegistry::new()),
            unique_repo("get-model-config-404"),
        );
        let err = svc
            .get_model_config(Request::new(pb::GetModelConfigRequest {
                model_name: "ghost".to_string(),
                version: "9".to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    // ===== GetServerConfig (M5) =====

    #[tokio::test]
    async fn get_server_config_returns_tree_sources_and_redaction() {
        use crate::config::{EndpointControl, ProtocolControl};
        let mut config = Config::default();
        config.metrics.timeline_max_points = 1440;
        config.access_control.admin = ProtocolControl {
            http: Some(EndpointControl::Key {
                key: "x-api-key".to_string(),
                value: Some("topsecret".to_string()),
                value_env: None,
                value_file: None,
            }),
            grpc: None,
        };
        let svc = build_admin_service_with_config(
            Arc::new(ModelRegistry::new()),
            std::env::temp_dir(),
            config,
        );

        let resp = svc
            .get_server_config(Request::new(pb::GetServerConfigRequest {}))
            .await
            .unwrap()
            .into_inner();
        let tree: serde_json::Value = serde_json::from_str(&resp.config_json).unwrap();
        assert_eq!(tree["metrics"]["timeline_max_points"], serde_json::json!(1440));
        // Secret redacted; source labels still attached per leaf.
        assert_eq!(
            tree["access_control"]["admin"]["http"]["value"],
            serde_json::json!("***")
        );
        assert!(!resp.config_json.contains("topsecret"));
        assert_eq!(
            resp.sources.get("metrics.timeline_max_points").map(String::as_str),
            Some("file")
        );
        assert_eq!(
            resp.sources.get("server.http_port").map(String::as_str),
            Some("default")
        );
        assert!(resp
            .redacted
            .contains(&"access_control.admin.http.value".to_string()));
    }

    // ===== UpdateModelConfig (M2) =====

    #[tokio::test]
    async fn update_model_config_write_only_writes_and_returns_etag() {
        let repo = unique_repo("update-model-config");
        let dir = repo.join("cfg_m").join("1");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("config.yaml"), "max_batch_size: 8\n")
            .await
            .unwrap();
        let svc = build_admin_service_with_repo(Arc::new(ModelRegistry::new()), repo.clone());

        let resp = svc
            .update_model_config(Request::new(pb::UpdateModelConfigRequest {
                model_name: "cfg_m".to_string(),
                version: "1".to_string(),
                patch_json: r#"{"max_batch_size": 32}"#.to_string(),
                if_match: None,
                force: false,
                mode: "write_only".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.valid);
        assert!(resp.written);
        assert!(!resp.reloaded);
        assert_eq!(resp.etag.as_deref().map(str::len), Some(16));

        let written = tokio::fs::read_to_string(dir.join("config.yaml")).await.unwrap();
        assert!(written.contains("max_batch_size: 32"));
        // Backup holds the pre-write bytes.
        let bak = tokio::fs::read_to_string(dir.join("config.yaml.bak")).await.unwrap();
        assert_eq!(bak, "max_batch_size: 8\n");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn update_model_config_dry_run_does_not_write() {
        let repo = unique_repo("update-model-config-dry");
        let dir = repo.join("cfg_m").join("1");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("config.yaml"), "max_batch_size: 8\n")
            .await
            .unwrap();
        let svc = build_admin_service_with_repo(Arc::new(ModelRegistry::new()), repo.clone());

        let resp = svc
            .update_model_config(Request::new(pb::UpdateModelConfigRequest {
                model_name: "cfg_m".to_string(),
                version: "1".to_string(),
                patch_json: r#"{"max_batch_size": "oops"}"#.to_string(),
                if_match: None,
                force: false,
                mode: "dry_run".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.valid);
        assert!(!resp.written);
        assert!(!resp.warnings.is_empty());
        let on_disk = tokio::fs::read_to_string(dir.join("config.yaml")).await.unwrap();
        assert_eq!(on_disk, "max_batch_size: 8\n");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn update_model_config_stale_etag_is_failed_precondition() {
        let repo = unique_repo("update-model-config-409");
        let dir = repo.join("cfg_m").join("1");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("config.yaml"), "max_batch_size: 8\n")
            .await
            .unwrap();
        let svc = build_admin_service_with_repo(Arc::new(ModelRegistry::new()), repo.clone());

        let err = svc
            .update_model_config(Request::new(pb::UpdateModelConfigRequest {
                model_name: "cfg_m".to_string(),
                version: "1".to_string(),
                patch_json: r#"{"max_batch_size": 32}"#.to_string(),
                if_match: Some("0000000000000000".to_string()),
                force: false,
                mode: "write_only".to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        let on_disk = tokio::fs::read_to_string(dir.join("config.yaml")).await.unwrap();
        assert_eq!(on_disk, "max_batch_size: 8\n");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn update_model_config_invalid_merge_is_invalid_argument() {
        let repo = unique_repo("update-model-config-400");
        let dir = repo.join("cfg_m").join("1");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("config.yaml"), "max_batch_size: 8\n")
            .await
            .unwrap();
        let svc = build_admin_service_with_repo(Arc::new(ModelRegistry::new()), repo.clone());

        let err = svc
            .update_model_config(Request::new(pb::UpdateModelConfigRequest {
                model_name: "cfg_m".to_string(),
                version: "1".to_string(),
                patch_json: r#"{"max_batch_size": "oops"}"#.to_string(),
                if_match: None,
                force: false,
                mode: "write_only".to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        let on_disk = tokio::fs::read_to_string(dir.join("config.yaml")).await.unwrap();
        assert_eq!(on_disk, "max_batch_size: 8\n");

        let _ = tokio::fs::remove_dir_all(&repo).await;
    }

    #[tokio::test]
    async fn update_model_config_unknown_version_is_not_found() {
        let svc = build_admin_service_with_repo(
            Arc::new(ModelRegistry::new()),
            unique_repo("update-model-config-404"),
        );
        let err = svc
            .update_model_config(Request::new(pb::UpdateModelConfigRequest {
                model_name: "ghost".to_string(),
                version: "9".to_string(),
                patch_json: r#"{"max_batch_size": 1}"#.to_string(),
                if_match: None,
                force: false,
                mode: "".to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }
}
