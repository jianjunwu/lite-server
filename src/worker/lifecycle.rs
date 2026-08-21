//! Model lifecycle: load/unload/reload, warmup, LRU version-cap eviction,
//! max_requests auto-recycle listener, and FILE_CHANGED hot-reload notify.

use super::hooks::{execute_hook, policies_from_config};
use super::process::{
    classify_stderr_line, drain_worker_stderr, emit_stderr_line, new_worker_command,
    spawn_worker_monitor, strip_level_prefix, worker_endpoint,
};
use super::{WorkerManager, WorkerProcess};
use crate::callback::ModelLifecycleContext;
use crate::config::ModelConfig;
use crate::error::AppError;
use crate::inference_queue::{model_version_key, OutlierState};
use crate::registry::types::*;
use crate::streaming::StreamCancelGuard;
use crate::transport::zmq::WorkerZmqClient;
use crate::worker::protocol::*;
use futures::StreamExt;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{error, info, warn};

/// M1 (S3b): may phase-2 registration continue for this version? False once
/// a concurrent unload removed the registry entry or flipped its status out
/// of Loading, or shutdown was requested — continuing would re-create
/// just-torn-down map entries and strand live workers nobody will ever stop.
fn load_registration_alive(
    registry: &crate::registry::ModelRegistry,
    shutdown_token: &tokio_util::sync::CancellationToken,
    model_name: &str,
    version: &str,
) -> bool {
    !shutdown_token.is_cancelled()
        && registry
            .get(model_name, Some(version))
            .is_some_and(|mv| mv.status == VersionStatus::Loading)
}

/// G1: one warmup execution unit — the sample to run plus the worker it is
/// pinned to (`None` when the version has no workers to pin; the unit then
/// rides normal least-loaded selection).
#[derive(Debug, Clone, PartialEq, Eq)]
struct WarmupUnit {
    sample: usize,
    worker_pin: Option<usize>,
}

/// G6: a warmup failure paired with its terminal kind — the kind feeds the
/// closed status label of `liteserver_model_warmup_total`; the reason stays
/// human-readable for `last_failure` and logs.
struct WarmupFailure {
    kind: crate::metrics::prometheus::WarmupStatus,
    reason: String,
}

impl WarmupFailure {
    fn failure(reason: String) -> Self {
        Self {
            kind: crate::metrics::prometheus::WarmupStatus::Failure,
            reason,
        }
    }

    fn timeout(reason: String) -> Self {
        Self {
            kind: crate::metrics::prometheus::WarmupStatus::Timeout,
            reason,
        }
    }
}

/// L3 (leak-gap-audit-0820): fallback total budget for an unbudgeted warmup
/// (policy total_timeout_secs=0) — max(startup_timeout, 300s). Applies to the
/// DETACHED respawn re-warm (a hung replacement would otherwise park the
/// detached task forever: it holds an Arc<WorkerManager> + the whole
/// ModelConfig clone, and the slot never force-ejects) and, since the
/// shutdown-window work, to the LOAD-time warmup (a hung worker would
/// otherwise park load_model past startup_timeout). 300s floors the budget
/// for configs with a tiny startup_timeout.
fn warmup_fallback_budget(model_config: &ModelConfig) -> Duration {
    Duration::from_secs_f32(model_config.startup_timeout).max(Duration::from_secs(300))
}

/// G1: build the warmup execution plan. `worker` scope (default) replays the
/// full sample set on EVERY worker process (worker-major order) — each process
/// owns separate engine state, so a version-wide pass would leave N-1 of N
/// workers cold. `version` scope keeps the configured total
/// (Σ samples×iterations) and round-robins units across workers. Pins ride
/// the existing `x-lite-worker-id` direct-pin header (B3).
fn build_warmup_plan(policy: &crate::config::WarmupPolicy, worker_count: usize) -> Vec<WarmupUnit> {
    let sample_units = || {
        policy
            .samples
            .iter()
            .enumerate()
            .flat_map(|(si, s)| std::iter::repeat_n(si, s.iterations.max(1) as usize))
    };
    match policy.scope {
        crate::config::WarmupScope::Worker if worker_count > 0 => (0..worker_count)
            .flat_map(|w| {
                sample_units().map(move |si| WarmupUnit {
                    sample: si,
                    worker_pin: Some(w),
                })
            })
            .collect(),
        // No pin targets: a single unpinned pass (defensive — load registers
        // workers before warmup, so this only fires on a 0-worker version).
        crate::config::WarmupScope::Worker => sample_units()
            .map(|si| WarmupUnit {
                sample: si,
                worker_pin: None,
            })
            .collect(),
        crate::config::WarmupScope::Version => sample_units()
            .enumerate()
            .map(|(g, si)| WarmupUnit {
                sample: si,
                worker_pin: (worker_count > 0).then_some(g % worker_count),
            })
            .collect(),
    }
}

/// G2: plan for re-warming ONE replacement worker after respawn — the full
/// sample set pinned to its slot, regardless of `scope` (the goal is "this
/// process is warm"; a version-wide fraction would leave it mostly cold).
fn build_warmup_plan_for_worker(
    policy: &crate::config::WarmupPolicy,
    worker_id: u32,
) -> Vec<WarmupUnit> {
    policy
        .samples
        .iter()
        .enumerate()
        .flat_map(|(si, s)| std::iter::repeat_n(si, s.iterations.max(1) as usize))
        .map(|si| WarmupUnit {
            sample: si,
            worker_pin: Some(worker_id as usize),
        })
        .collect()
}

/// G5: group plan units by worker pin, preserving first-occurrence order.
/// Every unit within a group shares one pin, so any batch the collector
/// forms from a group is pin-unanimous (`batch_direct_pin` never conflicts);
/// callers execute groups sequentially so units of different pins never
/// share a collector window (a mixed batch would silently drop the pin via
/// the conflict fallback).
fn group_units_by_pin(plan: &[WarmupUnit]) -> Vec<Vec<WarmupUnit>> {
    // Ordered linear scan (pins ≤ worker count, so the group list is tiny) —
    // no HashMap, no unwrap on the lookup-back.
    let mut groups: Vec<(Option<usize>, Vec<WarmupUnit>)> = Vec::new();
    for u in plan {
        match groups.iter_mut().find(|(pin, _)| *pin == u.worker_pin) {
            Some((_, units)) => units.push(u.clone()),
            None => groups.push((u.worker_pin, vec![u.clone()])),
        }
    }
    groups.into_iter().map(|(_, units)| units).collect()
}

/// Shared warmup response check (queue and RouteCall paths alike): a non-Ok
/// status (or a non-Single payload) fails the attempt.
fn validate_warmup_response(
    response: crate::proto::liteserver::Response,
) -> Result<(), WarmupFailure> {
    let ok = matches!(
        response.payload,
        Some(crate::proto::liteserver::response::Payload::Single(ref s))
            if s.status.as_ref().map(|st| st.code.as_str()).unwrap_or("Ok") != "Error"
    );
    if ok {
        return Ok(());
    }
    let detail = match response.payload {
        Some(crate::proto::liteserver::response::Payload::Single(ref s)) => s
            .status
            .as_ref()
            .map(|st| st.message.clone())
            .unwrap_or_default(),
        _ => "unexpected response payload".to_string(),
    };
    Err(WarmupFailure::failure(format!(
        "warmup inference returned error: {}",
        detail
    )))
}

/// Phase-1 output of the load spawn driver: one handshaken worker process,
/// not yet registered. Registration (metrics / routes / monitor / maps)
/// happens in phase 2, strictly in worker_id order.
struct WorkerReady {
    worker_id: usize,
    device: String,
    endpoint: String,
    child: tokio::process::Child,
    startup: WorkerStartup,
}

impl WorkerManager {
    /// Phase 1 of the load spawn driver: spawn ONE worker process, wait for
    /// its ready handshake (bounded by startup_timeout or the shutdown
    /// token, whichever fires first), and start the post-ready pipe drainers
    /// so a fast worker cannot block on a full pipe while slower siblings
    /// are still loading. Shared registration (metrics / routes / monitor /
    /// maps) happens in phase 2, in worker_id order. Safe to run
    /// concurrently across worker_ids: every touched resource (socket path,
    /// pipes, child process) is per-worker.
    // allow: 单 worker spawn 上下文(model/version/config/dir/device 布局)
    // 原样透传,phase-1 并发共享同一借用,引入参数结构体只增间接。
    #[allow(clippy::too_many_arguments)]
    async fn spawn_and_handshake(
        &self,
        model_name: &str,
        version: &str,
        model_config: &ModelConfig,
        model_dir: &std::path::Path,
        worker_id: usize,
        device_index: usize,
        accelerator: &str,
    ) -> Result<WorkerReady, AppError> {
        let device = format!("{}:{}", accelerator, device_index);
        let endpoint = worker_endpoint(model_name, version, worker_id);

        // Remove stale socket (Unix only — TCP ports are released on process exit)
        #[cfg(unix)]
        {
            let socket_str = endpoint.strip_prefix("ipc://").unwrap_or(&endpoint);
            let socket_path = std::path::Path::new(socket_str);
            let _ = tokio::fs::remove_file(socket_path).await;
            if let Some(parent) = socket_path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
        }

        // Find python module path
        let mut cmd = new_worker_command(&Self::find_python_module_path().unwrap_or_default());
        cmd.current_dir(model_dir);

        let mut child = cmd
            .arg("-m")
            .arg("lite_server.worker.inference")
            .arg("--model-name")
            .arg(model_name)
            .arg("--version")
            .arg(version)
            .arg("--model-py")
            .arg(model_dir.join("model.py"))
            .arg("--config")
            .arg(model_dir.join("config.yaml"))
            .arg("--device")
            .arg(&device)
            .arg("--worker-id")
            .arg(worker_id.to_string())
            .arg("--endpoint")
            .arg(&endpoint)
            .arg("--log-level")
            .arg(&self.log_level);

        if model_config.continuous_batching {
            child = child.arg("--continuous-batching");
        }

        if let Some(ref server_http) = self.server_http {
            child = child.arg("--server-http").arg(server_http);
        }

        let mut child = child
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                warn!(model = %model_name, version = %version, worker_id, "failed to spawn worker: {}", e);
                AppError::Python(format!("failed to spawn worker: {}", e))
            })?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AppError::Internal("worker stdout not piped".to_string()))?;
        // Option: the n==0 diagnostic below takes the pipe out for the
        // stderr drain; the logger spawn takes it back. A bare binding
        // moved on one branch defeats the async capture analysis.
        let mut stderr = Some(
            child
                .stderr
                .take()
                .ok_or_else(|| AppError::Internal("worker stderr not piped".to_string()))?,
        );

        // Wait for "ready" signal. The shutdown token outraces
        // startup_timeout: a worker stuck in setup() unwinds the whole
        // load (via the spawn block's error path) as soon as shutdown
        // starts instead of holding the process until the timeout.
        let mut reader = BufReader::new(stdout);
        let mut ready_line = String::new();
        let n = tokio::select! {
            _ = self.shutdown_token.cancelled() => Err(AppError::Internal(
                "load cancelled: server shutting down".to_string(),
            )),
            result = timeout(
                Duration::from_secs_f32(model_config.startup_timeout),
                reader.read_line(&mut ready_line),
            ) => result
                .map_err(|_| AppError::InferenceTimeout("worker startup timeout".to_string()))
                .and_then(|r| r.map_err(AppError::Io)),
        }?;
        let exited_check: Result<(), AppError> = if n == 0 {
            let stderr_tail =
                drain_worker_stderr(stderr.take().unwrap(), &self.server_tunables).await;
            let msg = if stderr_tail.trim().is_empty() {
                "worker exited before ready".to_string()
            } else {
                format!("worker exited before ready: {stderr_tail}")
            };
            Err(AppError::WorkerCrashed(msg))
        } else {
            Ok(())
        };
        exited_check?;
        let stdout = reader.into_inner();

        let startup: WorkerStartup = serde_json::from_str(ready_line.trim())
            .map_err(|e| AppError::Internal(format!("worker startup JSON parse error: {}", e)))?;

        let not_ready: Result<(), AppError> = if startup.status != "ready" {
            Err(AppError::WorkerCrashed(format!(
                "worker {} startup failed: {:?}",
                worker_id, startup.message
            )))
        } else {
            Ok(())
        };
        not_ready?;

        info!(
            "Worker {} for {} v{} ready (pid={:?})",
            worker_id,
            model_name,
            version,
            child.id()
        );

        // Fire on_ready lifecycle hook
        execute_hook(
            "ready",
            &model_config.hooks,
            vec![
                ("$MODEL".to_string(), model_name.to_string()),
                ("$VERSION".to_string(), version.to_string()),
                ("$WORKER_ID".to_string(), worker_id.to_string()),
            ],
            &self.hook_tasks,
        );

        // Drain stdout so the worker does not get SIGPIPE/BrokenPipeError
        // if anything writes to stdout after the ready signal.
        tokio::spawn(async move {
            let mut discard = [0u8; 1024];
            let mut stdout = stdout;
            loop {
                match stdout.read(&mut discard).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });

        // Start stderr logger
        let model_name_clone = model_name.to_string();
        let version_clone = version.to_string();
        let worker_id_clone = worker_id;
        // Still Some: the n==0 take above diverges via exited_check.
        let stderr = stderr.take().unwrap();
        // B14: per-line buffer cap (host memory bound for newline-less prints).
        let stderr_line_cap = self.server_tunables.worker_stderr_tail_bytes;
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut buf = Vec::with_capacity(1024);
            loop {
                match super::process::read_stderr_line_bounded(&mut reader, &mut buf, stderr_line_cap).await {
                    Ok(0) => {
                        tracing::debug!(worker_id = worker_id_clone, "Worker stderr EOF");
                        break;
                    }
                    Ok(_) => {
                        // Strip trailing newline / carriage-return
                        while buf.last() == Some(&b'\n') || buf.last() == Some(&b'\r') {
                            buf.pop();
                        }
                        let line = String::from_utf8_lossy(&buf);
                        let level = classify_stderr_line(line.trim());
                        let msg = strip_level_prefix(line.trim());
                        emit_stderr_line(
                            level,
                            msg,
                            worker_id_clone,
                            &model_name_clone,
                            &version_clone,
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            worker_id = worker_id_clone,
                            "Worker stderr read error: {}",
                            e
                        );
                        break;
                    }
                }
            }
        });

        Ok(WorkerReady {
            worker_id,
            device,
            endpoint,
            child,
            startup,
        })
    }

    /// Hot reload without restart (P3): send FILE_CHANGED to every worker of
    /// a model version so each worker's `on_file_changed` hook can refresh
    /// weights/configs in-process. Returns true only if at least one worker
    /// exists and ALL workers replied {"handled": true}; any send error,
    /// timeout, or handled=false/malformed reply (including pre-FILE_CHANGED
    /// workers replying "Unsupported payload type") means false — the caller
    /// then falls back to a full restart of the version.
    pub async fn notify_file_changed(
        &self,
        model_name: &str,
        version: &str,
        paths: &[String],
    ) -> bool {
        // Hook invocations may reload large weights; bounded well under the
        // 300s unary backstop so a hung hook can't stall the watch-event
        // task for minutes. Configurable via tunables.file_changed_timeout_secs.
        let file_changed_timeout =
            Duration::from_secs_f32(self.server_tunables.file_changed_timeout_secs);

        let clients = self
            .get_zmq_clients(model_name, version)
            .await
            .unwrap_or_default();
        if clients.is_empty() {
            return false;
        }
        for client in clients {
            let request = crate::proto::liteserver::Request {
                uid: format!(
                    "file_changed_{}_{}-{}",
                    model_name,
                    version,
                    uuid::Uuid::new_v4()
                ),
                meta: None,
                payload: Some(crate::proto::liteserver::request::Payload::FileChanged(
                    crate::proto::liteserver::FileChangedRequest {
                        paths: paths.to_vec(),
                    },
                )),
            };
            let handled = match client
                .send_with_timeout(request, file_changed_timeout)
                .await
            {
                Ok(resp) => resp
                    .payload
                    .and_then(|p| match p {
                        crate::proto::liteserver::response::Payload::Single(s) => Some(s.data),
                        _ => None,
                    })
                    .and_then(|data| serde_json::from_slice::<serde_json::Value>(&data).ok())
                    .and_then(|v| v.get("handled")?.as_bool())
                    .unwrap_or(false),
                Err(e) => {
                    warn!(
                        "FILE_CHANGED to {} version {} failed: {}",
                        model_name, version, e
                    );
                    false
                }
            };
            if !handled {
                // One worker can't refresh in-process → the whole version
                // gets restarted anyway; no point notifying the rest.
                return false;
            }
        }
        true
    }

    pub async fn load_model(

        &self,
        model_name: &str,
        version: &str,
        config: &ModelConfig,
    ) -> Result<(), AppError> {
        // Fail fast on bad float tunables (negative/NaN) before they reach
        // Duration::from_secs_f* and panic. Catches every load path: YAML,
        // CLI model_defaults, Admin API, ensemble.
        config
            .validate()
            .map_err(|e| AppError::Config(e.to_string()))?;

        info!("Loading model {} version {}", model_name, version);

        let model_dir = crate::validation::resolve_model_dir(&self.repo_path, model_name, version)?;
        if !model_dir.exists() {
            return Err(AppError::ModelNotFound(format!(
                "{} version {} not found",
                model_name, version
            )));
        }

        let model_py = model_dir.join("model.py");
        let config_yaml = model_dir.join("config.yaml");

        // Check for ensemble (structural YAML parse — string-contains would
        // false-positive on comments mentioning "ensemble:")
        let mut is_ensemble = false;
        let mut ensemble_content: Option<String> = None;
        if config_yaml.exists() {
            if let Ok(content) = tokio::fs::read_to_string(&config_yaml).await {
                if crate::config::config_content_is_ensemble(&content) {
                    is_ensemble = true;
                    ensemble_content = Some(content);
                }
            }
        }

        if !model_py.exists() && !is_ensemble {
            return Err(AppError::ModelNotFound(format!(
                "Neither model.py nor ensemble config found for {} version {}",
                model_name, version
            )));
        }

        // Register in registry. The caller owns config construction
        // (YAML load + model_defaults application) — re-reading the YAML
        // here would silently drop the caller's model_defaults overrides
        // for fields the per-model YAML doesn't set (B4).
        let model_config = config.clone();

        let model_type = if is_ensemble {
            ModelType::Ensemble
        } else {
            ModelType::LitAPI
        };

        // Reject duplicates before any eviction below: a doomed load must
        // never evict a healthy version. Same error `registry.register`
        // would return, but raised before `enforce_max_loaded_versions`.
        if self.registry.get(model_name, Some(version)).is_some() {
            return Err(AppError::VersionAlreadyLoaded(
                model_name.to_string(),
                version.to_string(),
            ));
        }

        // Evict LRU non-active versions if the model is at its
        // max_loaded_versions limit (§4.2). Placed after the on-disk checks
        // above so a doomed load never evicts a healthy version.
        self.enforce_max_loaded_versions(model_name).await?;

        self.registry.register(
            model_name,
            version,
            model_config.clone(),
            model_type,
            model_dir.clone(),
        )?;

        if is_ensemble {
            // P0/P6: parse the plan NOW — config validation surfaces at
            // load time (a bad DAG keeps the model unready, §4.4 note ③:
            // config errors never appear per-request), and the parsed plan
            // primes the P0 cache so the first request pays nothing.
            if let Some(plans) = &self.ensemble_plans {
                let content = ensemble_content.as_deref().unwrap_or_default();
                let config_path = config_yaml.clone();
                match crate::ensemble::parse_ensemble_plan(content, &config_path) {
                    Ok(plan) => {
                        plans.insert_ready(
                            crate::ensemble::PlanKey {
                                model: model_name.to_string(),
                                version: version.to_string(),
                            },
                            std::sync::Arc::new(plan),
                        );
                    }
                    Err(e) => {
                        // A plan-parse failure must not wedge the registry
                        // entry: unlike a warmup failure there is no runtime
                        // state to preserve for /health, and the only fix is
                        // "edit config.yaml and retry" — a wedged Pending
                        // entry makes every retry fail VersionAlreadyLoaded.
                        let _ = self.registry.remove(model_name, version);
                        return Err(e);
                    }
                }
            }
            // Ensemble: no workers, just mark ready
            self.registry.mark_ready(model_name, version)?;
            // P6 (batch 0): background warm — pre-check sub-model readiness
            // without blocking the load (sub-model preloading + the E4
            // resolved-version side-table land with batch 3).
            crate::ensemble::spawn_ensemble_warm(
                self.repo_path.clone(),
                self.ensemble_plans.clone(),
                self.registry.clone(),
                model_name.to_string(),
                version.to_string(),
            );
            crate::metrics::prometheus::record_model_load(model_name, version, true);
            self.sync_grpc_health().await;
            info!("Ensemble {} version {} loaded", model_name, version);
            return Ok(());
        }

        // Workers are about to spawn: Pending → Loading.
        self.registry
            .set_status(model_name, version, VersionStatus::Loading)?;

        // Launch workers
        let accelerator = model_config.accelerator.as_deref().unwrap_or("cpu");
        // worker_id → device-index table (device-placement plan); validated
        // already by ModelConfig::validate above, so this cannot fail.
        let device_plan = model_config
            .resolve_device_plan()
            .map_err(|e| AppError::Config(e.to_string()))?;
        let workers_per_device = model_config.workers_per_device.unwrap_or(1);

        if model_config.continuous_batching && workers_per_device != 1 {
            warn!("continuous_batching enabled; forcing workers_per_device=1");
        }
        if model_config.continuous_batching {
            if let Some(serde_json::Value::Object(m)) = &model_config.devices {
                if m.values().any(|v| v.as_u64() != Some(1)) {
                    warn!("continuous_batching enabled; forcing per-device worker counts to 1");
                }
            }
        }
        if model_config.continuous_batching && model_config.stream {
            // B5: CB workers answer stream opens with an explicit terminal
            // not_implemented error frame — surface the mismatch at load
            // time instead of letting clients discover it per request.
            warn!(
                "continuous_batching + streaming: CB workers do not serve streams; \
                 streaming endpoints reply with a not_implemented error"
            );
        }

        let total_workers = device_plan.len();

        // Create shared OutlierState — single instance for batch_collector,
        // health_checker, and streaming. Ejection thresholds come from
        // ModelConfig (§3); error_threshold == 0 disables ejection. Created
        // BEFORE the spawn loop so each worker's crash monitor can mark its
        // slot dead (crash-death routing gate).
        let ejection = crate::inference_queue::EjectionConfig {
            error_threshold: model_config.ejection_error_threshold,
            timeout: Duration::from_secs_f32(model_config.ejection_timeout),
            max_percent: model_config.ejection_max_percent,
            max_timeout: Duration::from_secs_f32(model_config.ejection_max_timeout),
        };
        let outlier = Arc::new(OutlierState::with_config(total_workers, &ejection));

        // Incremental registration: each worker enters the workers/zmq_clients
        // maps at the end of its loop iteration, so shutdown/unload can reach
        // a partially-started version. The loop runs inside an async block so
        // ANY error funnels to one mark_load_failed + teardown through the
        // single shared teardown path (§6.5) — a failed load never leaves map
        // entries or live worker processes behind (W1 + shutdown-window fix).
        let key = model_version_key(model_name, version);
        {
            let mut outliers = self.outlier_states.write().await;
            outliers.insert(key.clone(), outlier.clone());
        }
        let spawn_result: Result<(Vec<WorkerInfo>, Vec<Arc<WorkerZmqClient>>), AppError> = async {
            // Phase 1 — concurrent spawn + handshake (startup_concurrency;
            // 1 = legacy serial). Each future owns its child: on the first
            // error the `?` drops the stream (in-flight handshakes die via
            // kill_on_drop) and the completed-but-unregistered WorkerReadys
            // in `ready` die the same way.
            let concurrency = model_config.startup_concurrency.unwrap_or(1).max(1);
            let mut handshakes = futures::stream::iter((0..total_workers).map(|worker_id| {
                self.spawn_and_handshake(
                    model_name,
                    version,
                    &model_config,
                    &model_dir,
                    worker_id,
                    device_plan[worker_id],
                    accelerator,
                )
            }))
            .buffer_unordered(concurrency);
            let mut ready: Vec<Option<WorkerReady>> = (0..total_workers).map(|_| None).collect();
            while let Some(result) = handshakes.next().await {
                let r = result?;
                let worker_id = r.worker_id;
                ready[worker_id] = Some(r);
            }
            drop(handshakes);

            // Phase 2 — registration in worker_id order (worker_infos and
            // zmq_clients are index-aligned; routing pins by Vec index).
            // Each worker enters the maps right after its monitor is up, so
            // shutdown/unload can reach a partially-registered version.
            let mut worker_infos = Vec::new();
            let mut zmq_clients_for_model = Vec::new();
            for (worker_id, slot) in ready.iter_mut().enumerate() {
                // M1 (S3b): a concurrent unload/shutdown tears down through
                // the same maps this loop populates. Once the registry entry
                // is gone (or no longer Loading) or shutdown was requested,
                // bail out — continuing would re-create just-removed entries
                // and strand live workers nobody will ever stop. The error
                // funnel below reaps the partially-registered set.
                if !load_registration_alive(&self.registry, &self.shutdown_token, model_name, version)
                {
                    return Err(AppError::Internal(format!(
                        "load of {model_name} {version} aborted: torn down concurrently"
                    )));
                }
                let WorkerReady {
                    device,
                    endpoint,
                    child,
                    startup,
                    ..
                } = slot.take().expect("phase 1 filled every slot");

                // Register custom metrics from Python worker (gated by
                // features.custom_metrics; recording no-ops on unregistered ids).
                if self.custom_metrics {
                    if let Some(ref specs) = startup.metric_specs {
                        let spec_refs: Vec<(&str, &str)> = specs
                            .iter()
                            .map(|s| (s.name.as_str(), s.metric_type.as_str()))
                            .collect();
                        crate::metrics::prometheus::register_custom_metrics(
                            model_name,
                            version,
                            &spec_refs,
                        );
                    }
                }

                // Store per-model policies declared in config.yaml
                self.registry
                    .set_policies(model_name, version, policies_from_config(&model_config));

                // Register custom @route declarations (phase 2)
                self.upsert_routes(model_name, version, startup.custom_routes)
                    .await;

                // Create ZMQ client (binds the socket, worker connects). CB
                // workers get cb_remove notifications when a reply slot dies
                // (B2: stops wasted generation on client disconnect/timeout).
                let zmq_client = Arc::new(WorkerZmqClient::new_with_channel_size(
                    endpoint.clone(),
                    model_config.continuous_batching,
                    self.stream_channel_size,
                ));
                zmq_clients_for_model.push(zmq_client.clone());

                let pid = child.id();
                let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

                // Spawn monitor task — owns the Child, detects exits and handles
                // cleanup. The ZMQ client is reused across a worker's kill+respawn
                // (the bound PAIR socket outlives any single worker process), so
                // there is no `.sock` to clean on exit. `on_exit` fails in-flight
                // requests fast when the worker is gone: ZMQ PAIR has no
                // peer-disconnect event, so waiters would otherwise hang until
                // their caller-side timeouts (request_timeout, else 300s).
                let hooks_arc = Arc::new(model_config.hooks.clone());
                let done_rx = spawn_worker_monitor(
                    child,
                    model_name,
                    version,
                    worker_id as u32,
                    shutdown_rx,
                    crate::worker::process::crash_exit_handler(
                        Some(zmq_client.clone()),
                        Some(outlier.clone()),
                        self.registry.clone(),
                        model_name.to_string(),
                        version.to_string(),
                        worker_id as u32,
                        pid,
                    ),
                    Some(hooks_arc),
                    self.hook_tasks.clone(),
                    self.draining.clone(),
                );

                let info = WorkerInfo {
                    worker_id: worker_id as u32,
                    device,
                    endpoint: endpoint.clone(),
                    pid,
                    status: WorkerStatus::Ready,
                    capacity: None,
                };
                worker_infos.push(info);
                if let Some(pid) = pid {
                    crate::metrics::prometheus::set_worker_pid(
                        model_name,
                        version,
                        worker_id as u32,
                        pid,
                    );
                }

                // Incremental registration: the worker is visible to
                // shutdown/unload from here on; a later failure tears these
                // entries down through the shared teardown path.
                //
                // M1 (S3b): the aliveness check and both pushes run under the
                // SAME lock pair teardown_version_runtime takes (same order),
                // so a concurrent teardown either runs entirely before this
                // block (check fails → bail, nothing re-created) or entirely
                // after (it removes and reaps what we just pushed). A check
                // outside the locks would leave a check→push race, and
                // separate blocks would let teardown land between the two
                // pushes and orphan the client entry (bound socket).
                {
                    let mut workers = self.workers.write().await;
                    let mut clients = self.zmq_clients.write().await;
                    if !load_registration_alive(
                        &self.registry,
                        &self.shutdown_token,
                        model_name,
                        version,
                    ) {
                        return Err(AppError::Internal(format!(
                            "load of {model_name} {version} aborted: torn down concurrently"
                        )));
                    }
                    workers.entry(key.clone()).or_default().push(WorkerProcess {
                        worker_id: worker_id as u32,
                        endpoint,
                        shutdown_tx: Some(shutdown_tx),
                        done_rx: Some(done_rx),
                        kill_timeout: Duration::from_secs_f32(model_config.worker_kill_timeout),
                    });
                    clients.entry(key.clone()).or_default().push(zmq_client.clone());
                }
            }
            Ok((worker_infos, zmq_clients_for_model))
        }
        .await;

        let (worker_infos, zmq_clients_for_model) = match spawn_result {
            Ok(done) => done,
            Err(e) => {
                self.mark_load_failed(model_name, version);
                // Partially-started workers are in the maps (incremental
                // registration) — reap them through the single shared
                // teardown path (§6.5). The registry entry stays as Failed.
                self.teardown_version_runtime(model_name, version).await;
                // B6: a Failed version is never unloaded, so without this
                // its custom-metric families (registered in phase 2) would
                // leak forever. Built-in series stay — they are the live
                // observability of the Failed-kept version (D33).
                crate::metrics::prometheus::remove_custom_version_metrics(model_name, version);
                return Err(e);
            }
        };

        // M1 (S3a): these post-registration steps fail precisely when a
        // concurrent unload/shutdown removed the registry entry mid-load —
        // funnel them into the same cleanup so the phase-2-registered
        // workers are never stranded.
        //
        // P-WARM (§4.3): if warmup is enabled the version enters WarmingUp
        // (NOT_SERVING) here; the final Ready transition is deferred until the
        // dummy warmup below completes. Disabled → straight to Ready (legacy).
        let warmup = model_config.policies.warmup.clone();
        let finalize: Result<(), AppError> = async {
            self.registry
                .set_workers(model_name, version, worker_infos.clone())?;
            match warmup {
                Some(ref p) if p.enabled => {
                    self.registry.mark_warming_up(model_name, version)?;
                }
                _ => {
                    self.registry.mark_ready(model_name, version)?;
                }
            }
            Ok(())
        }
        .await;
        if let Err(e) = finalize {
            self.mark_load_failed(model_name, version);
            self.teardown_version_runtime(model_name, version).await;
            // B6: deregister custom families, same as the spawn-failure arm.
            crate::metrics::prometheus::remove_custom_version_metrics(model_name, version);
            return Err(e);
        }

        // Register inference queue for batching
        self.inference_queue.register_model(
            model_name,
            version,
            &model_config,
            worker_infos,
            zmq_clients_for_model,
            outlier.clone(),
            Some(self.respawn_tx.clone()),
        );

        // P-WARM (§4.3): with the queue wired, drive N dummy inferences to warm
        // the engine before the version becomes Ready. The version is still
        // WarmingUp (NOT_SERVING) — readiness/routing exclude it. A warmup
        // failure marks it Failed (D33: prefer delaying availability over
        // serving an unwarmed/broken model). Skipped entirely when disabled.
        if let Some(ref policy) = warmup {
            if policy.enabled {
                // Shutdown cancels the warmup the same way it cancels the
                // handshake; the teardown in the failure branch is idempotent
                // against shutdown()'s own unload of this version.
                let warmup_result = tokio::select! {
                    _ = self.shutdown_token.cancelled() => {
                        Err("load cancelled: server shutting down".to_string())
                    }
                    r = self.run_warmup_budgeted(model_name, version, &model_config, policy, None) => r,
                };
                match warmup_result {
                    Ok(()) => {
                        // S3a (warmup path): a concurrent unload may have
                        // removed the entry while the warmup ran — tear down
                        // the runtime state instead of stranding it.
                        if let Err(e) = self.registry.mark_ready(model_name, version) {
                            self.mark_load_failed(model_name, version);
                            self.teardown_version_runtime(model_name, version).await;
                            return Err(e);
                        }
                        info!(
                            model = %model_name, version = %version,
                            scope = ?policy.scope,
                            "warmup complete"
                        );
                    }
                    Err(reason) => {
                        error!(
                            model = %model_name, version = %version,
                            reason = %reason,
                            "warmup failed; marking version Failed"
                        );
                        // No `?`: the teardown below must run even when the
                        // entry is already gone (concurrent unload).
                        let _ = self.registry.mark_failed(model_name, version, &reason);
                        // L5: workers were spawned and the queue/client/
                        // routing state registered BEFORE warmup ran — tear
                        // that runtime state down through the single shared
                        // teardown path (§6.5). The registry entry stays as
                        // Failed (D33: /health must show why the version is
                        // not serving); only its runtime footprint is freed.
                        self.teardown_version_runtime(model_name, version).await;
                        // B6: deregister the version's custom families —
                        // a Failed version never sees an unload-time purge.
                        crate::metrics::prometheus::remove_custom_version_metrics(model_name, version);
                        crate::metrics::prometheus::set_active_workers(model_name, version, 0.0);
                        self.sync_grpc_health().await;
                        crate::metrics::prometheus::record_model_load(model_name, version, false);
                        return Err(AppError::WorkerCrashed(format!(
                            "warmup failed for {} {}: {}",
                            model_name, version, reason
                        )));
                    }
                }
            }
        }

        // Status coordinator: periodic Ready/Degraded reconciliation at the
        // configured cadence; 0 disables it (status stays event-driven).
        if model_config.health_check_interval > 0.0 {
            self.start_status_coordinator(
                model_name,
                version,
                Duration::from_secs_f32(model_config.health_check_interval),
            )
            .await;
        }
        self.sync_grpc_health().await;

        crate::metrics::prometheus::record_model_load(model_name, version, true);
        crate::metrics::prometheus::set_active_workers(model_name, version, total_workers as f64);

        info!(
            "Model {} version {} loaded with {} workers",
            model_name, version, total_workers
        );

        // Fire ModelLoad callback
        self.callback_runner
            .on_model_load(&ModelLifecycleContext {
                model_name: model_name.to_string(),
                version: version.to_string(),
                device: config
                    .devices
                    .as_ref()
                    .and_then(|d| d.as_str().map(|s| s.to_string())),
            })
            .await;

        Ok(())
    }

    /// P-WARM (§4.3): drive each warmup sample's dummy inferences through the
    /// inference queue to warm the engine (CUDA graph capture / torch.compile /
    /// allocator pools) before the version becomes `Ready`. Each sample is the
    /// raw `/predict` request body read from `<model_dir>/<input_ref>`, sent
    /// verbatim — multiple samples cover production input shapes/batches
    /// (Triton ModelWarmup 范式, M7). G1: units are pinned per worker via the
    /// existing `x-lite-worker-id` direct pin so EVERY worker process is warmed
    /// (see [`build_warmup_plan`]); under serial submission least-loaded would
    /// otherwise land every unit on worker 0. Returns `Err(reason)` on any
    /// failure (file unreadable, queue error, error response, timeout); the
    /// caller marks the version `Failed`. `pin` selects the plan shape:
    /// `None` (load) = [`build_warmup_plan`] over all workers; `Some(worker)`
    /// (G2 respawn re-warm) = the full sample set pinned to that one slot.
    ///
    /// G6: thin wrapper — records duration + terminal status exactly once per
    /// run (single record point); the body lives in `run_warmup_inner`.
    pub(super) async fn run_warmup(
        &self,
        model_name: &str,
        version: &str,
        model_config: &ModelConfig,
        policy: &crate::config::WarmupPolicy,
        pin: Option<u32>,
    ) -> Result<(), String> {
        let start = std::time::Instant::now();
        // G4: an optional budget over the WHOLE run, independent of the
        // per-iteration budget inside — whichever fires first fails the run.
        let inner = self.run_warmup_inner(model_name, version, model_config, policy, pin);
        let result = if policy.total_timeout_secs > 0.0 {
            match timeout(Duration::from_secs_f32(policy.total_timeout_secs), inner).await {
                Ok(r) => r,
                Err(_) => Err(WarmupFailure::timeout(format!(
                    "warmup: total timeout after {:.1}s",
                    policy.total_timeout_secs
                ))),
            }
        } else {
            inner.await
        };
        let status = match &result {
            Ok(()) => crate::metrics::prometheus::WarmupStatus::Success,
            Err(e) => e.kind,
        };
        crate::metrics::prometheus::record_model_warmup(
            model_name,
            version,
            start.elapsed().as_secs_f64(),
            status,
        );
        result.map_err(|e| e.reason)
    }

    /// G2/L3 + load: run_warmup plus the L3 fallback budget
    /// ([`warmup_fallback_budget`]) when the policy has no explicit
    /// `total_timeout_secs` (which already bounds the run inside run_warmup).
    /// `pin`: `Some(worker)` = respawn re-warm of one slot; `None` = the
    /// load-time warmup over all workers — an unbudgeted load warmup would
    /// otherwise park `load_model` forever on a hung worker.
    pub(super) async fn run_warmup_budgeted(
        &self,
        model_name: &str,
        version: &str,
        model_config: &ModelConfig,
        policy: &crate::config::WarmupPolicy,
        pin: Option<u32>,
    ) -> Result<(), String> {
        if policy.total_timeout_secs > 0.0 {
            return self
                .run_warmup(model_name, version, model_config, policy, pin)
                .await;
        }
        let budget = warmup_fallback_budget(model_config);
        self.run_warmup_with_budget(model_name, version, model_config, policy, pin, budget)
            .await
    }

    /// L3: run_warmup under an outer total budget; a budget expiry concludes
    /// as a warmup failure (the load path marks the version Failed; the
    /// respawn path force-ejects the slot) instead of parking forever. Split
    /// from [`run_warmup_budgeted`] so tests can inject a sub-second budget.
    async fn run_warmup_with_budget(
        &self,
        model_name: &str,
        version: &str,
        model_config: &ModelConfig,
        policy: &crate::config::WarmupPolicy,
        pin: Option<u32>,
        budget: Duration,
    ) -> Result<(), String> {
        match timeout(
            budget,
            self.run_warmup(model_name, version, model_config, policy, pin),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(format!(
                "warmup exceeded fallback budget of {:.0}s (total_timeout_secs unset)",
                budget.as_secs_f64()
            )),
        }
    }

    async fn run_warmup_inner(
        &self,
        model_name: &str,
        version: &str,
        model_config: &ModelConfig,
        policy: &crate::config::WarmupPolicy,
        pin: Option<u32>,
    ) -> Result<(), WarmupFailure> {
        let (model_dir, worker_count) = self
            .registry
            .get(model_name, Some(version))
            .map(|mv| (mv.model_dir.clone(), mv.workers.len()))
            .ok_or_else(|| {
                WarmupFailure::failure(format!(
                    "version {}/{} vanished during warmup",
                    model_name, version
                ))
            })?;
        let timeout_opt = policy.effective_timeout(model_config.request_timeout);

        // Read each sample's dummy body once — worker scope replays it per worker.
        let mut payloads: Vec<bytes::Bytes> = Vec::with_capacity(policy.samples.len());
        for sample in &policy.samples {
            let dummy_path = model_dir.join(&sample.input_ref);
            let body = tokio::fs::read(&dummy_path).await.map_err(|e| {
                WarmupFailure::failure(format!("read dummy input {}: {}", dummy_path.display(), e))
            })?;
            payloads.push(bytes::Bytes::from(body));
        }

        let plan = match pin {
            Some(w) => build_warmup_plan_for_worker(policy, w),
            None => build_warmup_plan(policy, worker_count),
        };
        info!(
            model = %model_name, version = %version,
            units = plan.len(),
            workers = worker_count,
            scope = ?policy.scope,
            "warming up model"
        );

        // G5: execute pin-groups SEQUENTIALLY (units of different pins never
        // share a collector window — a mixed batch would silently drop the
        // pin via batch_direct_pin's conflict fallback); units within a group
        // run up to `concurrency` in flight.
        let concurrency = policy.concurrency.max(1) as usize;
        for group in group_units_by_pin(&plan) {
            let mut units = futures::stream::iter(group.into_iter().map(|unit| {
                let sample = &policy.samples[unit.sample];
                self.run_warmup_unit(
                    model_name,
                    version,
                    policy,
                    unit,
                    sample,
                    &payloads,
                    timeout_opt,
                )
            }))
            .buffer_unordered(concurrency);
            while let Some(result) = units.next().await {
                // First failure aborts the run: the stream (and its in-flight
                // unit futures) drops here — response channels close and the
                // queued items clean up via the RAII inflight guard.
                result?;
            }
        }

        Ok(())
    }

    /// G7: run one warmup unit, retrying transient failures on the same
    /// worker (fixed 500ms interval, `policy.retries` extra attempts;
    /// 0 = fail-fast, D33).
    #[allow(clippy::too_many_arguments)] // execution-context bundle; splitting adds indirection only
    async fn run_warmup_unit(
        &self,
        model_name: &str,
        version: &str,
        policy: &crate::config::WarmupPolicy,
        unit: WarmupUnit,
        sample: &crate::config::WarmupSample,
        payloads: &[bytes::Bytes],
        timeout_opt: Option<Duration>,
    ) -> Result<(), WarmupFailure> {
        let mut attempt = 0u32;
        loop {
            match self
                .run_warmup_unit_once(model_name, version, &unit, sample, payloads, timeout_opt)
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    if attempt > policy.retries {
                        return Err(e);
                    }
                    info!(
                        model = %model_name, version = %version,
                        attempt,
                        reason = %e.reason,
                        "warmup unit failed; retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    /// One warmup attempt: submit the sample's dummy body (pinned when the
    /// unit says so) and validate the response.
    async fn run_warmup_unit_once(
        &self,
        model_name: &str,
        version: &str,
        unit: &WarmupUnit,
        sample: &crate::config::WarmupSample,
        payloads: &[bytes::Bytes],
        timeout_opt: Option<Duration>,
    ) -> Result<(), WarmupFailure> {
        // G3a: non-/predict samples ride the RouteCall channel (custom routes
        // bypass the inference queue), same as the HTTP custom-route handler.
        if sample.route != "/predict" {
            return self
                .run_route_warmup_once(
                    model_name,
                    version,
                    unit,
                    sample,
                    payloads[unit.sample].clone(),
                    timeout_opt,
                )
                .await;
        }
        // G3b: stream samples ride StreamOpen on the pinned worker's direct
        // client (validation forbids combining this with a custom route).
        if sample.mode == crate::config::WarmupSampleMode::Stream {
            return self
                .run_stream_warmup_once(
                    model_name,
                    version,
                    unit,
                    sample,
                    payloads[unit.sample].clone(),
                    timeout_opt,
                )
                .await;
        }
        let uid = format!("warmup_{}_{}_{}", model_name, version, uuid::Uuid::new_v4());
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        // User headers first, pin last — the control-plane pin must not be
        // overridable by a sample's headers.
        let mut headers = sample.headers.clone();
        if let Some(w) = unit.worker_pin {
            headers.insert("x-lite-worker-id".to_string(), w.to_string());
        }
        let meta = crate::proto::liteserver::RequestMeta {
            route: "/predict".to_string(),
            request_id: uid.clone(),
            client_ip: "127.0.0.1".to_string(),
            timestamp_ns,
            headers,
            payload: payloads[unit.sample].clone(),
            ..Default::default()
        };
        let (response_tx, response_rx) = oneshot::channel();
        let item = crate::inference_queue::QueueItem {
            uid: uid.clone(),
            data: payloads[unit.sample].clone(),
            meta: Some(Arc::new(meta)),
            response_tx,
            inflight_guard: None,
            enqueued_at: std::time::Instant::now(),
            is_warmup: true,
        };
        if let Err(e) = self.inference_queue.try_submit(model_name, version, item) {
            // Keep the QueueError detail (full / invalid pin / no live
            // workers) — a bare "queue not available" misdirects debugging.
            return Err(WarmupFailure::failure(format!(
                "warmup: queue submit failed: {e}"
            )));
        }

        // Bound the dummy inference by the per-iteration budget (None = unbounded).
        let response = match timeout_opt {
            Some(t) => match timeout(t, response_rx).await {
                Ok(Ok(r)) => r,
                Ok(Err(_)) => {
                    return Err(WarmupFailure::failure(
                        "warmup: response channel closed".to_string(),
                    ))
                }
                Err(_) => {
                    return Err(WarmupFailure::timeout(format!(
                        "warmup: timed out after {:.1}s",
                        t.as_secs_f32()
                    )))
                }
            },
            None => match response_rx.await {
                Ok(r) => r,
                Err(_) => {
                    return Err(WarmupFailure::failure(
                        "warmup: response channel closed".to_string(),
                    ))
                }
            },
        };

        // A non-Ok status (or a non-Single payload) fails the attempt.
        validate_warmup_response(response)
    }

    /// G3a/G3b: pick the direct client for a warmup unit's pinned worker
    /// (unpinned = a 0-worker version with no pin targets — fall back to the
    /// first client; the empty case errors out).
    async fn warmup_worker_client(
        &self,
        model_name: &str,
        version: &str,
        pin: Option<usize>,
    ) -> Result<Arc<WorkerZmqClient>, WarmupFailure> {
        let key = model_version_key(model_name, version);
        let clients = self.zmq_clients.read().await;
        let Some(clients) = clients.get(&key) else {
            return Err(WarmupFailure::failure(format!(
                "warmup: no worker clients for {model_name} {version}"
            )));
        };
        let idx = pin.unwrap_or(0);
        clients.get(idx).cloned().ok_or_else(|| {
            WarmupFailure::failure(format!(
                "warmup: no client for worker {idx} ({model_name} {version})"
            ))
        })
    }

    /// G3a: one RouteCall warmup attempt. Custom routes bypass the inference
    /// queue — they ride `send_route_or_stream` on the worker's direct
    /// client, mirroring the HTTP custom-route handler — so the pinned
    /// worker's client is picked here directly.
    async fn run_route_warmup_once(
        &self,
        model_name: &str,
        version: &str,
        unit: &WarmupUnit,
        sample: &crate::config::WarmupSample,
        payload: bytes::Bytes,
        timeout_opt: Option<Duration>,
    ) -> Result<(), WarmupFailure> {
        let client = self
            .warmup_worker_client(model_name, version, unit.worker_pin)
            .await?;

        let uid = format!(
            "warmup_route_{}_{}_{}",
            model_name,
            version,
            uuid::Uuid::new_v4()
        );
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        let meta = crate::proto::liteserver::RequestMeta {
            route: sample.route.clone(),
            method: "POST".to_string(),
            request_id: uid.clone(),
            client_ip: "127.0.0.1".to_string(),
            timestamp_ns,
            headers: sample.headers.clone(),
            ..Default::default()
        };
        let request = crate::proto::liteserver::Request {
            uid,
            meta: Some(meta),
            payload: Some(crate::proto::liteserver::request::Payload::RouteCall(
                crate::proto::liteserver::SingleRequest { data: payload },
            )),
        };
        let (resp_rx, mut chunk_rx) = client
            .send_route_or_stream(request)
            .await
            .map_err(|e| WarmupFailure::failure(format!("warmup route send: {e}")))?;

        // First-frame arbitration mirrors the HTTP custom-route handler: a
        // stream frame means the route is streaming, which warmup does not
        // support (G3b) — fail loudly rather than silently skip the sample.
        let wait = async move {
            let mut resp_rx = resp_rx;
            tokio::select! {
                biased;
                frame = chunk_rx.recv() => match frame {
                    Some(_) => Err(WarmupFailure::failure(format!(
                        "warmup: route {} returned a stream — streaming warmup is not supported (G3b)",
                        sample.route
                    ))),
                    None => (&mut resp_rx).await.map_err(|_| {
                        WarmupFailure::failure(
                            "warmup: route response channel closed".to_string(),
                        )
                    }),
                },
                unary = &mut resp_rx => unary.map_err(|_| {
                    WarmupFailure::failure("warmup: route response channel closed".to_string())
                }),
            }
        };
        let response = match timeout_opt {
            Some(t) => match timeout(t, wait).await {
                Ok(r) => r?,
                Err(_) => {
                    return Err(WarmupFailure::timeout(format!(
                        "warmup: timed out after {:.1}s",
                        t.as_secs_f32()
                    )))
                }
            },
            None => wait.await?,
        };
        validate_warmup_response(response)
    }

    /// G3b: one uni-stream warmup attempt — StreamOpen to the pinned worker's
    /// direct client (the SSE handler's wire path: `build_stream_open` +
    /// `send_stream`, minus HTTP), judge by the frames per `completion`, and
    /// in first_chunk mode CANCEL rather than drain so cost stays bounded.
    /// Bypassing HTTP means no streaming metrics are touched (they live in
    /// the handlers).
    async fn run_stream_warmup_once(
        &self,
        model_name: &str,
        version: &str,
        unit: &WarmupUnit,
        sample: &crate::config::WarmupSample,
        payload: bytes::Bytes,
        timeout_opt: Option<Duration>,
    ) -> Result<(), WarmupFailure> {
        let client = self
            .warmup_worker_client(model_name, version, unit.worker_pin)
            .await?;
        let stream_id = format!("warmup-stream-{}", uuid::Uuid::new_v4());
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        let meta = crate::proto::liteserver::RequestMeta {
            route: "/predict".to_string(),
            request_id: stream_id.clone(),
            client_ip: "127.0.0.1".to_string(),
            timestamp_ns,
            headers: sample.headers.clone(),
            ..Default::default()
        };
        let open =
            crate::streaming::build_stream_open(stream_id.clone(), payload, Some(meta), false);
        let mut chunk_rx = client
            .send_stream(open, stream_id.clone())
            .await
            .map_err(|e| WarmupFailure::failure(format!("warmup stream open: {e}")))?;

        use crate::proto::liteserver::stream_response::Payload as Frame;
        let completion = sample.completion.unwrap_or_default();
        // Cancel the worker-side stream on every non-terminal exit (see the
        // guard's doc). Terminal frames and a closed channel disarm.
        let mut guard = StreamCancelGuard::armed(client, stream_id);
        let wait = async {
            match completion {
                crate::config::WarmupStreamCompletion::FirstChunk => {
                    match chunk_rx.recv().await {
                        Some(frame) => match frame.payload {
                            // TTFT path warm — cancel instead of draining.
                            Some(Frame::Chunk(_)) => {
                                let _ = guard
                                    .client
                                    .send_raw(crate::streaming::build_stream_cancel(
                                        guard.stream_id.clone(),
                                    ))
                                    .await;
                                guard.disarm();
                                Ok(())
                            }
                            // Empty stream: the generator ran to Done (Q3 ruling).
                            Some(Frame::Done(_)) => {
                                guard.disarm();
                                Ok(())
                            }
                            Some(Frame::Error(e)) => {
                                guard.disarm();
                                Err(WarmupFailure::failure(format!(
                                    "warmup stream returned error: {}",
                                    e.message
                                )))
                            }
                            // Armed: dropping the guard cancels the stream.
                            other => Err(WarmupFailure::failure(format!(
                                "warmup: unexpected first stream frame ({other:?})"
                            ))),
                        },
                        None => {
                            guard.disarm();
                            Err(WarmupFailure::failure(
                                "warmup: stream closed before any frame".to_string(),
                            ))
                        }
                    }
                }
                crate::config::WarmupStreamCompletion::Drain => loop {
                    match chunk_rx.recv().await {
                        Some(frame) => match frame.payload {
                            Some(Frame::Chunk(_)) => continue,
                            Some(Frame::Done(_)) => {
                                guard.disarm();
                                break Ok(());
                            }
                            Some(Frame::Error(e)) => {
                                guard.disarm();
                                break Err(WarmupFailure::failure(format!(
                                    "warmup stream returned error: {}",
                                    e.message
                                )));
                            }
                            // Armed: dropping the guard cancels the stream.
                            other => {
                                break Err(WarmupFailure::failure(format!(
                                    "warmup: unexpected stream frame ({other:?})"
                                )))
                            }
                        },
                        None => {
                            guard.disarm();
                            break Err(WarmupFailure::failure(
                                "warmup: stream closed before Done".to_string(),
                            ));
                        }
                    }
                },
            }
        };
        match timeout_opt {
            Some(t) => match timeout(t, wait).await {
                Ok(r) => r,
                // Timed out with the guard armed — its Drop cancels the
                // worker-side stream on the way out.
                Err(_) => Err(WarmupFailure::timeout(format!(
                    "warmup: timed out after {:.1}s",
                    t.as_secs_f32()
                ))),
            },
            None => wait.await,
        }
    }

    pub async fn unload_model(
        &self,
        model_name: &str,
        version: Option<&str>,
    ) -> Result<bool, AppError> {
        if let Some(v) = version {
            self.unload_version(model_name, v).await?;
            Ok(true)
        } else {
            let versions = self.registry.list_versions(model_name);
            let version_strings: Vec<String> = versions.into_iter().map(|v| v.version).collect();
            let mut unloaded = false;
            for v in version_strings {
                self.unload_version(model_name, &v).await?;
                unloaded = true;
            }
            Ok(unloaded)
        }
    }

    /// Enforce the model's `max_loaded_versions` strategy (§4.2): evict
    /// least-recently-used non-active versions until there is room for one
    /// more. The active version is never evicted; if it is the only version
    /// left, the limit is exceeded with a warning rather than blocking the
    /// load (e.g. limit=1 must not break "load v2 → activate v2" cutover).
    async fn enforce_max_loaded_versions(&self, model_name: &str) -> Result<(), AppError> {
        let max = self
            .registry
            .get_strategy(model_name)
            .and_then(|s| s.max_loaded_versions);
        let Some(max) = max else { return Ok(()) };

        loop {
            let loaded = self.registry.list_versions(model_name).len();
            if loaded < max {
                return Ok(());
            }
            match self.registry.lru_eviction_candidate(model_name) {
                Some(victim) => {
                    info!(
                        model = %model_name, version = %victim,
                        max_loaded_versions = max,
                        "evicting LRU version to satisfy max_loaded_versions"
                    );
                    self.unload_version(model_name, &victim).await?;
                }
                None => {
                    warn!(
                        model = %model_name, max_loaded_versions = max,
                        "max_loaded_versions exceeded: only the active version remains"
                    );
                    return Ok(());
                }
            }
        }
    }

    pub(super) async fn unload_version(
        &self,
        model_name: &str,
        version: &str,
    ) -> Result<(), AppError> {
        info!("Unloading {} version {}", model_name, version);

        // P0 (D23): invalidate the ensemble plan cache BEFORE any registry
        // change — a fresh request must never hit a stale plan for a version
        // that is being unloaded. reload_model funnels through here, so this
        // single point covers both paths.
        if let Some(cache) = &self.ensemble_plans {
            cache.invalidate_model(model_name);
        }

        // Stop the status coordinator first so it can't tick against a
        // half-torn-down version.
        self.stop_status_coordinator(model_name, version).await;

        // Fire ModelUnload callback before unloading
        self.callback_runner
            .on_model_unload(&ModelLifecycleContext {
                model_name: model_name.to_string(),
                version: version.to_string(),
                device: None,
            })
            .await;

        // Status must flip to Unloading BEFORE the teardown kills workers —
        // a still-Ready version whose workers die invites respawn/routing
        // against a half-torn-down version.
        self.registry
            .set_status(model_name, version, VersionStatus::Unloading)?;

        self.teardown_version_runtime(model_name, version).await;

        self.registry.remove(model_name, version)?;
        self.sync_grpc_health().await;

        crate::metrics::prometheus::record_model_unload(model_name, version);
        crate::metrics::prometheus::set_active_workers(model_name, version, 0.0);
        crate::metrics::prometheus::remove_version_weight(model_name, version);
        crate::metrics::prometheus::remove_model_ready(model_name, version);
        // Round2 B2: drop the rest of the per-version series + timeline state
        // (after record_model_unload — MODEL_LOAD_TOTAL is excluded from the
        // purge, the event log survives). Worker memory registrations were
        // already dropped inside teardown_version_runtime (before this purge).
        crate::metrics::prometheus::remove_version_metrics(model_name, version);
        crate::metrics::aggregator::TIMELINE
            .remove(model_name, version)
            .await;

        info!("Model {} version {} unloaded", model_name, version);
        Ok(())
    }

    /// Shared runtime teardown for a version (§6.5 single teardown path):
    /// drain the inference queue, then stop the workers (graceful ZMQ stop →
    /// bounded await → client shutdown → socket-file cleanup) and drop the
    /// outlier/routing/client/worker-map state, finishing with the worker
    /// PID metric registrations (the kills above are awaited, so the pids
    /// are dead or dying — clearing here precedes any later metrics purge).
    ///
    /// Both `unload_version` and the load-time warmup-failure branch funnel
    /// through here; a partial-init failure must never hand-roll its own
    /// cleanup and drift from the unload sequence. The registry entry itself
    /// is NOT touched — the caller decides whether the version disappears
    /// (unload) or stays visible as `Failed` (warmup failure, D33).
    async fn teardown_version_runtime(&self, model_name: &str, version: &str) {
        // Unregister the inference queue first to stop accepting new
        // requests, then wait for in-flight requests to drain before killing
        // workers (§4.2). On grace timeout, proceed anyway — a stuck request
        // must not block unloading forever.
        let drain = self.inference_queue.begin_drain(model_name, version);
        if let Some(drain) = drain {
            if timeout(self.unload_grace, drain.wait_idle()).await.is_err() {
                warn!(
                    model = %model_name, version = %version,
                    grace_secs = self.unload_grace.as_secs(),
                    "drain timed out with in-flight requests; unloading anyway"
                );
            }
            drain.abort();
        }

        let key = model_version_key(model_name, version);
        let (procs, clients) = {
            let mut workers = self.workers.write().await;
            let mut zmq_clients = self.zmq_clients.write().await;
            let mut outliers = self.outlier_states.write().await;
            let mut routes = self.route_table.write().await;
            outliers.remove(&key);
            let clients = zmq_clients.remove(&key);
            routes.remove(&key);
            (workers.remove(&key), clients)
        };

        if let Some(mut procs) = procs {
            // Phase 1 — graceful: send each worker the ZMQ stop message.
            // Ordered after any in-flight request on the same PAIR socket, so
            // a worker still serving finishes first; the worker then breaks
            // its recv loop, runs the Python teardown (_run_teardown:
            // LitAPI.teardown + before/after_teardown callbacks) and exits
            // cleanly (observed by the monitor as a clean child exit).
            if let Some(ref clients) = clients {
                let stop_req = crate::streaming::build_stop_request();
                for client in clients {
                    let _ = client.send_raw(stop_req.clone()).await;
                }
            }

            // Phase 2 — await each worker's natural exit, bounded by
            // kill_timeout; escalate to the monitor's SIGKILL only when the
            // worker is hung (never read the stop message). Waiting for the
            // reap preserves the original no-orphan guarantee — without it,
            // unload/shutdown returns while a worker is still alive, an
            // orphan whose ZMQ auto-reconnect steals the re-bound socket on
            // reload/restart.
            for proc in procs.iter_mut() {
                super::process::stop_worker_gracefully(proc, model_name, version).await;
            }

            // RN-10: shut each client's actor down (bounded) BEFORE deleting
            // the socket files — a reload rebind must never race an actor
            // still inside its blocking send (double-socket window).
            if let Some(ref clients) = clients {
                for client in clients {
                    client.shutdown().await;
                }
            }

            // Phase 3 — clean up ZMQ socket files (Unix only).
            for proc in procs {
                #[cfg(unix)]
                {
                    let socket_str = proc
                        .endpoint
                        .strip_prefix("ipc://")
                        .unwrap_or(&proc.endpoint);
                    let socket_path = std::path::Path::new(socket_str);
                    let _ = tokio::fs::remove_file(socket_path).await;
                }
            }
        }

        crate::metrics::prometheus::clear_worker_pids(model_name, version);
    }

    /// H5 (delete escalation): terminate a version's workers forcibly,
    /// skipping the graceful phases (callbacks, drain, stop-request). Used
    /// when the graceful unload failed and the version directory is about
    /// to be deleted — a delete must never leave live workers pointing at
    /// a deleted directory. Idempotent: an already-removed version is a
    /// no-op (registry.remove returns Ok for unknown models).
    pub async fn force_unload_version(
        &self,
        model_name: &str,
        version: &str,
    ) -> Result<(), AppError> {
        info!(
            model = %model_name, version = %version,
            "force-unloading version after failed graceful unload"
        );

        // Same as the graceful path (P0/D23): invalidate the ensemble plan
        // cache BEFORE any registry change — a fresh request must never hit
        // a stale plan for a version that is being removed.
        if let Some(cache) = &self.ensemble_plans {
            cache.invalidate_model(model_name);
        }

        let key = model_version_key(model_name, version);
        let (procs, clients) = {
            let mut workers = self.workers.write().await;
            let mut zmq_clients = self.zmq_clients.write().await;
            let mut outliers = self.outlier_states.write().await;
            let mut routes = self.route_table.write().await;
            outliers.remove(&key);
            let clients = zmq_clients.remove(&key);
            routes.remove(&key);
            (workers.remove(&key), clients)
        };

        // RN-10: force-unload skips the graceful phases but the actor
        // teardown must still precede the socket-file delete.
        if let Some(clients) = clients {
            for client in &clients {
                client.shutdown().await;
            }
        }

        if let Some(mut procs) = procs {
            // Skip the stop request — trigger the monitor's SIGKILL path
            // directly (kill_on_drop reaps the process group on drop).
            for proc in procs.iter_mut() {
                if let Some(tx) = proc.shutdown_tx.take() {
                    let _ = tx.send(());
                }
            }
            // Clean up ZMQ socket files (Unix only).
            #[cfg(unix)]
            for proc in procs.iter() {
                let socket_str = proc
                    .endpoint
                    .strip_prefix("ipc://")
                    .unwrap_or(&proc.endpoint);
                let socket_path = std::path::Path::new(socket_str);
                let _ = tokio::fs::remove_file(socket_path).await;
            }
        }

        self.registry.remove(model_name, version)?;
        self.sync_grpc_health().await;
        crate::metrics::prometheus::record_model_unload(model_name, version);
        crate::metrics::prometheus::set_active_workers(model_name, version, 0.0);
        crate::metrics::prometheus::remove_version_weight(model_name, version);
        crate::metrics::prometheus::remove_model_ready(model_name, version);
        // Round2 B2: same per-version purge as the graceful unload path.
        // clear_worker_pids before the purge — same async-kill rationale as
        // the graceful path above.
        crate::metrics::prometheus::clear_worker_pids(model_name, version);
        crate::metrics::prometheus::remove_version_metrics(model_name, version);
        crate::metrics::aggregator::TIMELINE
            .remove(model_name, version)
            .await;
        Ok(())
    }

    pub async fn reload_model(
        &self,
        model_name: &str,
        version: Option<&str>,
    ) -> Result<bool, AppError> {
        let v = match version {
            Some(v) => v.to_string(),
            None => match self.registry.get_active_version(model_name) {
                Some(v) => v,
                None => return Ok(false),
            },
        };

        if self.registry.get(model_name, Some(&v)).is_none() {
            return Ok(false);
        }

        // Batch 0 (profile prerequisite): validate-then-swap — re-read
        // config.yaml from disk and apply model_defaults; any failure returns
        // early, old workers keep serving. Previously reload reused the
        // registry's stale config (§0.1), so "edit config.yaml → reload"
        // silently measured the old config 100% of the time.
        let model_dir = crate::validation::resolve_model_dir(&self.repo_path, model_name, &v)?;
        if !model_dir.exists() {
            return Err(AppError::ModelNotFound(format!(
                "{} version {} not found",
                model_name, v
            )));
        }
        let config_path = model_dir.join("config.yaml");
        let mut config = crate::config::load_model_config(&config_path)
            .map_err(|e| AppError::Config(format!("invalid config.yaml: {}", e)))?;
        self.model_defaults.apply_to(&mut config);
        config
            .validate()
            .map_err(|e| AppError::Config(e.to_string()))?;

        let was_active =
            self.registry.get_active_version(model_name).as_deref() == Some(v.as_str());

        info!("Reloading {} version {}", model_name, v);
        self.unload_version(model_name, &v).await?;

        // Small delay to ensure cleanup
        tokio::time::sleep(Duration::from_millis(500)).await;

        self.load_model(model_name, &v, &config).await?;
        // Restore the active pointer only if this version was active before
        // the reload — auto-recycling a standby version must not steal the
        // pointer from the serving version (§4.3).
        if was_active {
            self.registry.activate_version(model_name, &v)?;
        }

        // Fire ModelReload callback
        self.callback_runner
            .on_model_reload(&ModelLifecycleContext {
                model_name: model_name.to_string(),
                version: v.clone(),
                device: config
                    .devices
                    .as_ref()
                    .and_then(|d| d.as_str().map(|s| s.to_string())),
            })
            .await;

        info!("Model {} version {} reloaded", model_name, v);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callback::CallbackRunner;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;

    // ===== M1 guard tests (phase-2 registration leak) =====

    #[test]
    fn should_report_registration_alive_only_while_loading_and_not_cancelled() {
        let registry = ModelRegistry::new();
        registry
            .register(
                "m",
                "1",
                crate::config::ModelConfig::default(),
                crate::registry::types::ModelType::LitAPI,
                std::path::PathBuf::new(),
            )
            .unwrap();
        registry
            .set_status("m", "1", VersionStatus::Loading)
            .unwrap();
        let token = tokio_util::sync::CancellationToken::new();

        assert!(
            load_registration_alive(&registry, &token, "m", "1"),
            "Loading version + live token → registration may proceed"
        );

        token.cancel();
        assert!(
            !load_registration_alive(&registry, &token, "m", "1"),
            "cancelled shutdown token → stop registering"
        );

        let token2 = tokio_util::sync::CancellationToken::new();
        registry.mark_ready("m", "1").unwrap();
        assert!(
            !load_registration_alive(&registry, &token2, "m", "1"),
            "status flipped out of Loading → stop registering"
        );
        assert!(
            !load_registration_alive(&registry, &token2, "m", "2"),
            "registry entry gone (concurrent unload) → stop registering"
        );
    }

    /// M1: a version unloaded while its load is still mid-flight must not
    /// have its map entries re-created by the load's phase-2 registration
    /// (which runs AFTER the unload's teardown removed them), and no worker
    /// process may survive. Deterministic: the unload runs while phase 1 is
    /// blocked in a slow setup(), so the whole of phase 2 executes after the
    /// unload completed.
    #[cfg(unix)]
    #[tokio::test]
    async fn should_leave_no_residue_when_unload_lands_mid_load() {
        let tag = "midload";
        let repo =
            std::env::temp_dir().join(format!("lite-server-midload-{tag}-{}", std::process::id()));
        let model_dir = repo.join(tag).join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(
            model_dir.join("model.py"),
            r#"import os
import time
from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        with open(f"setup_pid_{os.getpid()}.txt", "w") as f:
            f.write(str(os.getpid()))
        time.sleep(3)

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#,
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 2\nstartup_concurrency: 2\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "debug".to_string(),
            Arc::new(CallbackRunner::new()),
        ));

        let config = ModelConfig {
            startup_timeout: 60.0,
            ..Default::default()
        };
        let wm_load = wm.clone();
        let tag_owned = tag.to_string();
        let mut load =
            tokio::spawn(async move { wm_load.load_model(&tag_owned, "1", &config).await });

        // Wait until at least one worker is alive in setup() (phase 1 well
        // under way), then unload the version out from under the load.
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let spawned = std::fs::read_dir(&model_dir)
                .unwrap()
                .any(|e| e.unwrap().file_name().into_string().unwrap().starts_with("setup_pid_"));
            if spawned {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "worker never reached setup()"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        wm.unload_version(tag, "1")
            .await
            .expect("unload of a mid-load version must succeed");

        let result = (&mut load).await.expect("load task panicked");
        assert!(
            result.is_err(),
            "the load must not report success for a version unloaded underneath it"
        );

        // The phase-2 registration runs after the unload's teardown — it
        // must NOT re-create the torn-down entries (M1).
        let key = model_version_key(tag, "1");
        assert!(
            wm.workers.read().await.get(&key).is_none(),
            "phase-2 re-created worker entries torn down by the concurrent unload"
        );
        assert!(
            wm.zmq_clients.read().await.get(&key).is_none(),
            "phase-2 re-created zmq client entries torn down by the concurrent unload"
        );

        // No python worker of this model may survive, registered or not.
        tokio::time::sleep(Duration::from_secs(1)).await;
        for entry in std::fs::read_dir(&model_dir).unwrap() {
            let name = entry.unwrap().file_name().into_string().unwrap();
            if let Some(pid_str) = name
                .strip_prefix("setup_pid_")
                .and_then(|s| s.strip_suffix(".txt"))
            {
                let pid: i32 = pid_str.parse().unwrap();
                let alive = unsafe { libc::kill(pid, 0) } == 0;
                if alive {
                    unsafe { libc::kill(pid, libc::SIGKILL) };
                }
                assert!(!alive, "worker pid {pid} survived the mid-load unload");
            }
        }

        let _ = std::fs::remove_dir_all(&repo);
    }

    /// H5: force_unload_version must clean the registry (and be a no-op
    /// for versions that never existed — the delete escalation path calls
    /// it blindly after a failed graceful unload).
    #[test]
    fn test_force_unload_version_cleans_registry_and_is_idempotent() {
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register(
                "m",
                "1",
                crate::config::ModelConfig::default(),
                crate::registry::types::ModelType::LitAPI,
                std::path::PathBuf::new(),
            )
            .unwrap();
        registry.mark_ready("m", "1").unwrap();
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::env::temp_dir(),
            Arc::new(InferenceQueue::new()),
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            wm.force_unload_version("m", "1").await.unwrap();
            assert!(
                registry.get("m", Some("1")).is_none(),
                "version must be removed"
            );
            // Idempotent: a second call on the deleted version is a no-op.
            wm.force_unload_version("m", "1").await.unwrap();
        });
    }

    /// Ensemble load-failure wedge (audit 2026-08-20): a version whose
    /// config.yaml has a top-level `ensemble` key but an invalid DAG fails
    /// load_model — but the registry entry (registered as Pending BEFORE the
    /// plan parse) is left behind: no mark_load_failed, no removal, in stark
    /// contrast to the non-ensemble funnel ("ANY error funnels to one
    /// mark_load_failed + teardown"). The wedged Pending entry makes every
    /// retry fail VersionAlreadyLoaded until a manual unload — fixing
    /// config.yaml and reloading can never heal on its own.
    #[tokio::test]
    async fn should_not_wedge_registry_when_ensemble_plan_is_invalid() {
        let tag = "enswedge";
        let repo =
            std::env::temp_dir().join(format!("lite-server-enswedge-{}", std::process::id()));
        let model_dir = repo.join(tag).join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        // Top-level `ensemble` key (detected structurally) but an invalid DAG.
        std::fs::write(model_dir.join("config.yaml"), "ensemble:\n  steps: []\n").unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        )
        .with_ensemble_plans(Arc::new(crate::ensemble::EnsemblePlanCache::new()));

        let result = wm.load_model(tag, "1", &ModelConfig::default()).await;
        assert!(result.is_err(), "an invalid ensemble DAG must fail the load");

        match registry.get(tag, Some("1")) {
            None => {} // removed — acceptable cleanup
            Some(mv) => assert_eq!(
                mv.status,
                VersionStatus::Failed,
                "a failed ensemble load must not wedge the entry in {:?}",
                mv.status
            ),
        }

        // The practical consequence: an immediate retry must not be rejected
        // as a duplicate.
        let retry = wm.load_model(tag, "1", &ModelConfig::default()).await;
        if let Err(e) = &retry {
            assert!(
                !matches!(e, AppError::VersionAlreadyLoaded(..)),
                "a retry after a failed ensemble load must not report VersionAlreadyLoaded"
            );
        }
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// Plan-cache invalidation gap (audit 2026-08-20): graceful unload
    /// invalidates the ensemble plan cache BEFORE any registry change
    /// (P0/D23 — a fresh request must never hit a stale plan for a version
    /// being unloaded). The H5 escalation force_unload_version has no
    /// invalidate_model call at all: any caller that reaches force-unload
    /// without a prior graceful attempt (it is `pub`) leaves the Ready plan
    /// cached for a version that no longer exists.
    #[tokio::test]
    async fn force_unload_must_invalidate_ensemble_plan_cache() {
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register(
                "m",
                "1",
                crate::config::ModelConfig::default(),
                crate::registry::types::ModelType::Ensemble,
                std::path::PathBuf::new(),
            )
            .unwrap();
        registry.mark_ready("m", "1").unwrap();
        let cache = Arc::new(crate::ensemble::EnsemblePlanCache::new());
        let wm = WorkerManager::new(
            registry.clone(),
            std::env::temp_dir(),
            Arc::new(InferenceQueue::new()),
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        )
        .with_ensemble_plans(cache.clone());

        let plan = crate::ensemble::parse_ensemble_plan(
            "ensemble:\n  output: \"$a.output\"\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      inputs:\n        x: \"$request\"\n",
            std::path::Path::new("/nonexistent/config.yaml"),
        )
        .expect("minimal ensemble config parses");
        let key = crate::ensemble::PlanKey {
            model: "m".to_string(),
            version: "1".to_string(),
        };
        cache.insert_ready(key.clone(), Arc::new(plan));
        assert!(cache.plans.get(&key).is_some(), "precondition: plan cached");

        wm.force_unload_version("m", "1")
            .await
            .expect("force unload succeeds");
        assert!(
            cache.plans.get(&key).is_none(),
            "force_unload_version must invalidate the ensemble plan cache, \
             same as the graceful unload path (P0/D23)"
        );
    }

    /// B6 (leak-gap-audit-0821): a FAILED load whose workers declared custom
    /// metrics must deregister those families — the only deregistration path
    /// was remove_version_metrics on UNLOAD, and a version that fails its
    /// load stays in the registry as Failed (D33) without ever being
    /// unloaded, so every failed version permanently accumulated one family
    /// per declared metric name.
    #[cfg(unix)]
    #[tokio::test]
    #[serial_test::serial(custom_metrics)]
    async fn failed_load_deregisters_custom_metric_families() {
        let key = "loadfail_gauge:gauge";
        assert!(
            !crate::metrics::prometheus::custom_family_registered_for_test(key),
            "precondition: loadfail_gauge must be unregistered"
        );
        let tag = "loadfail";
        let repo =
            std::env::temp_dir().join(format!("lite-server-loadfail-{}", std::process::id()));
        let model_dir = repo.join(tag).join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(
            model_dir.join("model.py"),
            r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        self.register_metric("loadfail_gauge", "gauge")

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#,
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = Arc::new(
            WorkerManager::new(
                registry.clone(),
                repo.clone(),
                Arc::new(InferenceQueue::new()),
                "error".to_string(),
                Arc::new(CallbackRunner::new()),
            )
            .with_custom_metrics(true),
        );
        // Warmup enabled with a sample file that does not exist — the warmup
        // fails AFTER phase-2 registered the worker's custom metrics.
        let config = ModelConfig {
            startup_timeout: 60.0,
            policies: crate::config::ModelPolicies {
                warmup: Some(crate::config::WarmupPolicy {
                    enabled: true,
                    samples: vec![crate::config::WarmupSample {
                        input_ref: "warmup/missing.json".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = wm.load_model(tag, "1", &config).await;
        assert!(result.is_err(), "a missing warmup sample must fail the load");
        assert_eq!(
            registry.get(tag, Some("1")).map(|mv| mv.status),
            Some(VersionStatus::Failed),
            "D33: the version stays visible as Failed"
        );

        assert!(
            !crate::metrics::prometheus::custom_family_registered_for_test(key),
            "B6: the failed version's custom family and phantom refs must be              deregistered"
        );
        assert!(
            !crate::metrics::prometheus::REGISTRY
                .gather()
                .iter()
                .any(|f| f.get_name() == "lite_server_loadfail_gauge"),
            "B6: the family must not be exported after the failed load"
        );
        let _ = std::fs::remove_dir_all(&repo);
    }

    // ===== L3: respawn re-warm fallback budget =====

    /// A PAIR worker that accepts requests and NEVER replies — the
    /// kill_threshold=0 "hung but not dead" shape from leak-gap-audit-0820.
    fn spawn_silent_worker(endpoint: String) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(5000);
            while s.recv_bytes(0).is_ok() {
                // swallow — never respond
            }
        })
    }

    /// WorkerManager over a real queue whose single worker is silent; the
    /// registered model dir carries one warmup sample file.
    async fn rewarm_harness(tag: &str) -> (Arc<WorkerManager>, std::path::PathBuf) {
        let model_dir = std::env::temp_dir().join(format!(
            "lite-server-rewarm-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&model_dir);
        std::fs::create_dir_all(model_dir.join("warmup")).unwrap();
        std::fs::write(model_dir.join("warmup").join("s1.json"), br#"{"x":1}"#).unwrap();

        let registry = Arc::new(ModelRegistry::new());
        registry
            .register(
                "m",
                "1",
                crate::config::ModelConfig::default(),
                crate::registry::types::ModelType::LitAPI,
                model_dir.clone(),
            )
            .unwrap();
        registry.mark_ready("m", "1").unwrap();
        registry
            .set_workers(
                "m",
                "1",
                vec![crate::registry::types::WorkerInfo {
                    worker_id: 0,
                    device: "cpu:0".to_string(),
                    endpoint: String::new(),
                    pid: None,
                    status: crate::registry::types::WorkerStatus::Ready,
                    capacity: None,
                }],
            )
            .unwrap();

        let queue = Arc::new(InferenceQueue::new());
        let endpoint = format!(
            "ipc://{}",
            std::env::temp_dir()
                .join(format!("rewarm-{tag}-{}.sock", std::process::id()))
                .display()
        );
        let _silent = spawn_silent_worker(endpoint.clone());
        let client = Arc::new(crate::transport::zmq::WorkerZmqClient::new(endpoint));
        let outlier = Arc::new(OutlierState::new(1));
        queue.register_model(
            "m",
            "1",
            &crate::config::ModelConfig::default(),
            registry.get("m", Some("1")).unwrap().workers.clone(),
            vec![client],
            outlier,
            None,
        );

        let wm = Arc::new(WorkerManager::new(
            registry,
            model_dir.clone(),
            queue,
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        (wm, model_dir)
    }

    fn rewarm_policy(total_timeout_secs: f32) -> crate::config::WarmupPolicy {
        crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup/s1.json".to_string(),
                ..Default::default()
            }],
            total_timeout_secs,
            ..Default::default()
        }
    }

    /// L3 precondition (documents the hazard, passes before and after the
    /// fix): with the default total_timeout_secs=0 the run parks on the
    /// silent worker — no conclusion within 2s.
    #[tokio::test]
    async fn rewarm_without_total_budget_hangs_on_silent_worker() {
        let (wm, model_dir) = rewarm_harness("hang").await;
        let config = crate::config::ModelConfig::default();
        let policy = rewarm_policy(0.0);
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            wm.run_warmup("m", "1", &config, &policy, Some(0)),
        )
        .await;
        assert!(
            result.is_err(),
            "documented hazard: an unbudgeted re-warm must park on a hung worker"
        );
        let _ = std::fs::remove_dir_all(&model_dir);
    }

    /// L3: a re-warm under the fallback budget concludes as a warmup failure
    /// (the respawn path force-ejects the slot) instead of parking forever.
    /// The budget is injected sub-second; rewarm_fallback_budget's own value
    /// is pinned by the pure test below.
    #[tokio::test]
    async fn rewarm_fallback_budget_terminates_on_silent_worker() {
        let (wm, model_dir) = rewarm_harness("budget").await;
        let config = crate::config::ModelConfig {
            startup_timeout: 1.0,
            ..Default::default()
        };
        let policy = rewarm_policy(0.0);
        let err = wm
            .run_warmup_with_budget(
                "m",
                "1",
                &config,
                &policy,
                Some(0),
                Duration::from_millis(200),
            )
            .await
            .unwrap_err();
        assert!(
            err.contains("fallback budget"),
            "the fallback budget must fail the re-warm, got: {err}"
        );
        let _ = std::fs::remove_dir_all(&model_dir);
    }

    /// Load-time warmup (pin=None) gets the same fallback total budget as
    /// the respawn re-warm when the policy has none — a load whose warmup
    /// hangs must conclude as a load failure, not park load_model forever.
    #[tokio::test]
    async fn load_warmup_fallback_budget_terminates_on_silent_worker() {
        let (wm, model_dir) = rewarm_harness("loadbudget").await;
        let config = crate::config::ModelConfig {
            startup_timeout: 1.0,
            ..Default::default()
        };
        let policy = rewarm_policy(0.0);
        let err = wm
            .run_warmup_with_budget("m", "1", &config, &policy, None, Duration::from_millis(200))
            .await
            .unwrap_err();
        assert!(
            err.contains("fallback budget"),
            "the fallback budget must fail the load-time warmup, got: {err}"
        );
        let _ = std::fs::remove_dir_all(&model_dir);
    }

    /// Load-time warmup with an explicit total_timeout_secs is governed by
    /// the policy's own budget, not the fallback.
    #[tokio::test]
    async fn load_warmup_explicit_total_budget_governs_over_fallback() {
        let (wm, model_dir) = rewarm_harness("loadexplicit").await;
        let config = crate::config::ModelConfig::default();
        let policy = rewarm_policy(0.3);
        let err = wm
            .run_warmup_budgeted("m", "1", &config, &policy, None)
            .await
            .unwrap_err();
        assert!(
            err.contains("total timeout after 0.3s"),
            "the policy's own budget must fire inside run_warmup, got: {err}"
        );
        let _ = std::fs::remove_dir_all(&model_dir);
    }

    /// L3: an explicit total_timeout_secs already bounds the run inside
    /// run_warmup — the fallback must not preempt it.
    #[tokio::test]
    async fn rewarm_explicit_total_budget_governs_over_fallback() {
        let (wm, model_dir) = rewarm_harness("explicit").await;
        let config = crate::config::ModelConfig::default();
        let policy = rewarm_policy(0.3);
        let err = wm
            .run_warmup_budgeted("m", "1", &config, &policy, Some(0))
            .await
            .unwrap_err();
        assert!(
            err.contains("total timeout after 0.3s"),
            "the policy's own budget must fire inside run_warmup, got: {err}"
        );
        let _ = std::fs::remove_dir_all(&model_dir);
    }

    #[test]
    fn rewarm_fallback_budget_is_max_of_startup_timeout_and_300s() {
        let small = crate::config::ModelConfig {
            startup_timeout: 10.0,
            ..Default::default()
        };
        assert_eq!(warmup_fallback_budget(&small), Duration::from_secs(300));
        let large = crate::config::ModelConfig {
            startup_timeout: 600.0,
            ..Default::default()
        };
        assert_eq!(warmup_fallback_budget(&large), Duration::from_secs(600));
    }

    // ===== LRU eviction tests (§4.2) =====

    fn lru_test_manager(registry: &Arc<ModelRegistry>) -> WorkerManager {
        WorkerManager::new(
            registry.clone(),
            std::path::PathBuf::new(),
            Arc::new(InferenceQueue::new()),
            "warn".to_string(),
            Arc::new(CallbackRunner::new()),
        )
    }

    fn lru_strategy(max: Option<usize>) -> crate::config::ModelStrategyConfig {
        crate::config::ModelStrategyConfig {
            name: "m".to_string(),
            load_policy: "explicit".to_string(),
            versions_to_load: vec![],
            default_version: None,
            max_loaded_versions: max,
            weights: None,
        }
    }

    #[tokio::test]
    async fn test_enforce_max_loaded_versions_evicts_lru_non_active() {
        let registry = Arc::new(ModelRegistry::new());
        registry.set_strategy("m", &lru_strategy(Some(2))).unwrap();
        for v in ["1", "2"] {
            registry
                .register(
                    "m",
                    v,
                    ModelConfig::default(),
                    ModelType::LitAPI,
                    std::path::PathBuf::new(),
                )
                .unwrap();
            registry.mark_ready("m", v).unwrap();
        }
        registry.activate_version("m", "2").unwrap();

        let wm = lru_test_manager(&registry);
        wm.enforce_max_loaded_versions("m").await.unwrap();

        assert!(
            registry.get("m", Some("1")).is_none(),
            "LRU non-active version evicted"
        );
        assert!(
            registry.get("m", Some("2")).is_some(),
            "active version preserved"
        );
    }

    #[tokio::test]
    async fn test_enforce_max_loaded_versions_keeps_active_when_no_candidate() {
        let registry = Arc::new(ModelRegistry::new());
        registry.set_strategy("m", &lru_strategy(Some(1))).unwrap();
        registry
            .register(
                "m",
                "1",
                ModelConfig::default(),
                ModelType::LitAPI,
                std::path::PathBuf::new(),
            )
            .unwrap();
        registry.mark_ready("m", "1").unwrap();
        registry.activate_version("m", "1").unwrap();

        // Only the active version is loaded: limit is exceeded with a
        // warning rather than evicting active or failing the load.
        let wm = lru_test_manager(&registry);
        wm.enforce_max_loaded_versions("m").await.unwrap();
        assert!(
            registry.get("m", Some("1")).is_some(),
            "active version must never be evicted"
        );
    }

    #[tokio::test]
    async fn test_enforce_max_loaded_versions_no_limit_is_noop() {
        let registry = Arc::new(ModelRegistry::new());
        registry.set_strategy("m", &lru_strategy(None)).unwrap();
        for v in ["1", "2", "3"] {
            registry
                .register(
                    "m",
                    v,
                    ModelConfig::default(),
                    ModelType::LitAPI,
                    std::path::PathBuf::new(),
                )
                .unwrap();
        }

        let wm = lru_test_manager(&registry);
        wm.enforce_max_loaded_versions("m").await.unwrap();
        assert_eq!(
            registry.list_versions("m").len(),
            3,
            "no limit → nothing evicted"
        );
    }

    /// Real-worker regression test: unload_model must not return before the
    /// Python worker process is actually dead. An orphaned worker's ZMQ
    /// socket auto-reconnect steals the re-bound socket on reload/restart
    /// (root cause of flaky worker log loss).
    #[cfg(unix)]
    #[tokio::test]
    async fn test_unload_model_leaves_no_orphan_worker() {
        let repo =
            std::env::temp_dir().join(format!("lite-server-orphan-test-{}", std::process::id()));
        let model_dir = repo.join("orphan_guard_model").join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(
            model_dir.join("model.py"),
            r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#,
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "debug".to_string(),
            Arc::new(CallbackRunner::new()),
        );

        wm.load_model("orphan_guard_model", "1", &ModelConfig::default())
            .await
            .unwrap();

        let pids: Vec<u32> = registry
            .get("orphan_guard_model", Some("1"))
            .unwrap()
            .workers
            .iter()
            .filter_map(|w| w.pid)
            .collect();
        assert!(!pids.is_empty(), "expected at least one worker pid");

        wm.unload_model("orphan_guard_model", None).await.unwrap();

        // When unload returns, every worker must already be reaped —
        // a live (or zombie) process here means the orphan race is back.
        for pid in pids {
            let alive = unsafe { libc::kill(pid as i32, 0) } == 0;
            assert!(
                !alive,
                "worker pid {} orphaned: still alive after unload_model returned",
                pid
            );
        }

        let _ = std::fs::remove_dir_all(&repo);
    }

    /// P1 W1 evidence (project-resource-leak-sweep-0815.md): `load_model`'s
    /// multi-worker loop spawns workers one at a time (:283-509); when ANY
    /// worker fails to start mid-loop the function `return Err`s and the local
    /// `worker_processes` vec (holding each started worker's shutdown_tx /
    /// done_rx) is dropped. The already-started workers were never inserted
    /// into the `workers` map (:542-550), so unload cannot reach them. If the
    /// process survives (no ownership that SIGKILLs it on that error path), it
    /// is an orphan until the server exits.
    ///
    /// Fixed code must reap every spawned-but-unregistered worker on the error
    /// path. Current code either leaves it alive (RED, defect confirmed) or the
    /// shutdown_tx drop happens to trigger the monitor's kill arm (GREEN, this
    /// particular path is already safe) — either way the test documents the
    /// behavior with evidence.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_load_model_worker_failure_mid_loop_leaves_no_orphan() {
        use std::io::Read;
        use std::time::Duration;

        let repo =
            std::env::temp_dir().join(format!("lite-server-w1-orphan-test-{}", std::process::id()));
        let model_dir = repo.join("w1_model").join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        // Worker 0 (device cpu:0) records its pid so the test can track it;
        // worker 1 (device cpu:1) refuses to start, failing the load after
        // worker 0 is already spawned and running.
        std::fs::write(
            model_dir.join("model.py"),
            r#"import os
from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        if device == "cpu:1":
            raise RuntimeError("device 1 refuses to start")
        with open("setup_pid.txt", "w") as f:
            f.write(str(os.getpid()))

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#,
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 2\nworkers_per_device: 1\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "debug".to_string(),
            Arc::new(CallbackRunner::new()),
        );

        // The caller owns config construction (B4: the per-model YAML is NOT
        // re-read for worker spawn decisions) — devices must be set here so the
        // spawn loop creates two workers (cpu:0 / cpu:1).
        let load_config = ModelConfig {
            devices: Some(serde_json::json!(2)),
            ..Default::default()
        };

        let err = wm
            .load_model("w1_model", "1", &load_config)
            .await
            .unwrap_err();
        assert!(
            matches!(err, AppError::WorkerCrashed(_)),
            "load_model must fail when worker 1 refuses to start, got {err:?}"
        );

        // Worker 0's pid (written in setup, before its ready line).
        let mut pid_str = String::new();
        let pid_path = model_dir.join("setup_pid.txt");
        std::fs::File::open(&pid_path)
            .expect("worker 0 must have written setup_pid.txt")
            .read_to_string(&mut pid_str)
            .expect("read pid file");
        let pid: i32 = pid_str.trim().parse().expect("pid is numeric");

        // The workers map / registry must not report any live worker for the
        // failed load — nothing but the orphan owns the process.
        if let Some(mv) = registry.get("w1_model", Some("1")) {
            assert!(
                mv.workers.is_empty(),
                "failed load must not register workers (found {})",
                mv.workers.len()
            );
        }

        // W1: the spawned-but-unregistered worker must be reaped on the error
        // path. Poll up to 5s for the pid to become unreachable (fully reaped).
        let mut reaped = false;
        for _ in 0..25 {
            if unsafe { libc::kill(pid, 0) } != 0 {
                reaped = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if !reaped {
            // Clean up the orphan so the test does not leak a rogue process.
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
        assert!(
            reaped,
            "W1: worker pid {pid} survived a failed load_model — spawned but \
             never inserted into the workers map, so unload cannot reap it \
             (orphan until server shutdown)"
        );

        let _ = std::fs::remove_dir_all(&repo);
    }

    // ===== Shutdown-during-startup window (worker-startup-shutdown-and-parallel-plan §1.2) =====

    /// Harness: a model whose worker writes its pid in setup() then sleeps
    /// 30s (simulating a slow weight load), with startup_timeout=60s so the
    /// handshake wait outlasts anything the test is willing to wait. Spawns
    /// load_model as a task and waits until the worker process is alive and
    /// mid-handshake. Returns (manager, repo, worker pid, load handle).
    #[cfg(unix)]
    async fn slow_setup_load_in_flight(
        tag: &str,
    ) -> (
        Arc<WorkerManager>,
        std::path::PathBuf,
        i32,
        tokio::task::JoinHandle<Result<(), AppError>>,
    ) {
        let repo =
            std::env::temp_dir().join(format!("lite-server-shutdown-{tag}-{}", std::process::id()));
        let model_dir = repo.join(tag).join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(
            model_dir.join("model.py"),
            r#"import os
import time
from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        with open("setup_pid.txt", "w") as f:
            f.write(str(os.getpid()))
        time.sleep(30)

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#,
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "debug".to_string(),
            Arc::new(CallbackRunner::new()),
        ));

        let config = ModelConfig {
            startup_timeout: 60.0,
            ..Default::default()
        };
        let wm_load = wm.clone();
        let tag_owned = tag.to_string();
        let load = tokio::spawn(async move { wm_load.load_model(&tag_owned, "1", &config).await });

        // Wait until the worker process is alive and stuck in setup() — i.e.
        // spawned but not yet handshaken, the exact window under test.
        let pid_path = model_dir.join("setup_pid.txt");
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let pid: i32 = loop {
            if let Ok(s) = std::fs::read_to_string(&pid_path) {
                if let Ok(pid) = s.trim().parse() {
                    break pid;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "worker never reached setup()"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        (wm, repo, pid, load)
    }

    /// Window A: a worker spawned but still mid-handshake is invisible to
    /// `WorkerManager::shutdown()` (it is not in the workers map yet), so
    /// without a cancellation path the process survives shutdown until its
    /// 30s sleep ends. After the fix, shutdown cancels the in-flight load and
    /// the spawned process is reaped promptly.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_shutdown_during_worker_startup_reaps_spawned_worker() {
        let (wm, repo, pid, _load) = slow_setup_load_in_flight("reap").await;

        wm.shutdown().await;

        let mut reaped = false;
        for _ in 0..50 {
            if unsafe { libc::kill(pid, 0) } != 0 {
                reaped = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if !reaped {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        assert!(
            reaped,
            "worker pid {pid} survived shutdown() — it was spawned but not yet \
             in the workers map, so shutdown could not reach it"
        );

        let _ = std::fs::remove_dir_all(&repo);
    }

    /// Window B: the ready-handshake wait is bounded only by startup_timeout
    /// (60s here) and ignores shutdown. After the fix the load task must
    /// return promptly (with an error) once shutdown cancels it.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_load_model_returns_promptly_when_shutdown_cancels_startup() {
        let (wm, repo, _pid, load) = slow_setup_load_in_flight("prompt").await;

        wm.shutdown().await;

        let result = timeout(Duration::from_secs(10), load).await;
        assert!(
            result.is_ok(),
            "load_model must return promptly after shutdown cancels it, not \
             wait out the 30s setup + handshake window"
        );
        assert!(
            result.unwrap().unwrap().is_err(),
            "a cancelled load must fail, not report success"
        );

        let _ = std::fs::remove_dir_all(&repo);
    }

    /// The error path of a cancelled/failed load must not leave partial
    /// entries in the zmq client map (incremental registration registers
    /// each worker as soon as its monitor is up; teardown must clean them).
    #[cfg(unix)]
    #[tokio::test]
    async fn test_cancelled_load_leaves_no_zmq_clients() {
        let (wm, repo, _pid, load) = slow_setup_load_in_flight("clean").await;

        wm.shutdown().await;
        let _ = timeout(Duration::from_secs(10), load).await;

        assert!(
            wm.get_zmq_clients("clean", "1").await.is_none(),
            "a cancelled load must not leave zmq clients registered"
        );

        let _ = std::fs::remove_dir_all(&repo);
    }

    // ===== Parallel worker startup (worker-startup-shutdown-and-parallel-plan §2.2) =====

    /// Harness: a two-worker model whose setup() sleeps `setup_secs`
    /// (simulating weight loading). Returns (manager, registry, repo).
    #[cfg(unix)]
    async fn parallel_harness(
        tag: &str,
        setup_secs: u32,
        startup_concurrency: Option<usize>,
    ) -> (
        Arc<WorkerManager>,
        Arc<ModelRegistry>,
        std::path::PathBuf,
    ) {
        let repo =
            std::env::temp_dir().join(format!("lite-server-par-{tag}-{}", std::process::id()));
        let model_dir = repo.join(tag).join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(
            model_dir.join("model.py"),
            format!(
                r#"import time
from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        time.sleep({setup_secs})

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {{"output": x}}

    def encode_response(self, output):
        return output
"#
            ),
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 2\nworkers_per_device: 1\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "debug".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        let config = ModelConfig {
            devices: Some(serde_json::json!(2)),
            startup_concurrency,
            ..Default::default()
        };
        wm.load_model(tag, "1", &config)
            .await
            .expect("load must succeed");
        (wm, registry, repo)
    }

    /// startup_concurrency=2 overlaps the two workers' 3s setups: total load
    /// time is ~max(setup) + spawn overhead, well under the serial floor of
    /// 2 x 3s.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_parallel_startup_overlaps_worker_setup() {
        let start = std::time::Instant::now();
        let (wm, _registry, repo) = parallel_harness("overlap", 3, Some(2)).await;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(5500),
            "parallel load took {elapsed:?}; serial would be >= ~6s (2 x 3s setups)"
        );

        let _ = wm.unload_model("overlap", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// Default (startup_concurrency unset) preserves the serial behavior:
    /// two 3s setups run back-to-back.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_default_startup_is_serial() {
        let start = std::time::Instant::now();
        let (wm, _registry, repo) = parallel_harness("serial", 3, None).await;
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(5500),
            "serial load finished in {elapsed:?} (< 2 x 3s setups) — default must stay serial"
        );

        let _ = wm.unload_model("serial", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// Concurrent handshakes complete out of order, but registration must
    /// stay index-aligned by worker_id (routing pins by Vec index).
    #[cfg(unix)]
    #[tokio::test]
    async fn test_parallel_startup_preserves_worker_order() {
        let (wm, registry, repo) = parallel_harness("order", 1, Some(2)).await;

        let workers = registry
            .get("order", Some("1"))
            .expect("version registered")
            .workers;
        assert_eq!(workers.len(), 2);
        assert_eq!(workers[0].worker_id, 0, "slot 0 must be worker 0");
        assert_eq!(workers[1].worker_id, 1, "slot 1 must be worker 1");
        assert_eq!(
            wm.get_zmq_clients("order", "1").await.map(|c| c.len()),
            Some(2),
            "one zmq client per worker, index-aligned"
        );

        let _ = wm.unload_model("order", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// A mid-load failure under concurrency must still reap every worker
    /// that already started (W1 shape, parallel driver).
    #[cfg(unix)]
    #[tokio::test]
    async fn test_parallel_startup_failure_reaps_started_workers() {
        use std::io::Read;

        let repo =
            std::env::temp_dir().join(format!("lite-server-par-fail-{}", std::process::id()));
        let model_dir = repo.join("par_fail").join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(
            model_dir.join("model.py"),
            r#"import os
import time
from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        if device == "cpu:1":
            # Fail AFTER worker 0 has started setup (its failure would
            # otherwise race worker 0's interpreter startup under
            # concurrency and the pid file might never appear).
            time.sleep(2)
            raise RuntimeError("device 1 refuses to start")
        with open("setup_pid.txt", "w") as f:
            f.write(str(os.getpid()))
        time.sleep(3)

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#,
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 2\nworkers_per_device: 1\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "debug".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        let config = ModelConfig {
            devices: Some(serde_json::json!(2)),
            startup_concurrency: Some(2),
            ..Default::default()
        };

        let err = wm.load_model("par_fail", "1", &config).await.unwrap_err();
        assert!(
            matches!(err, AppError::WorkerCrashed(_)),
            "load must fail when worker 1 refuses to start, got {err:?}"
        );

        let mut pid_str = String::new();
        std::fs::File::open(model_dir.join("setup_pid.txt"))
            .expect("worker 0 must have written setup_pid.txt")
            .read_to_string(&mut pid_str)
            .expect("read pid file");
        let pid: i32 = pid_str.trim().parse().expect("pid is numeric");

        let mut reaped = false;
        for _ in 0..25 {
            if unsafe { libc::kill(pid, 0) } != 0 {
                reaped = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if !reaped {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        assert!(
            reaped,
            "worker pid {pid} survived a failed parallel load — started workers \
             must be reaped on the error path"
        );

        let _ = std::fs::remove_dir_all(&repo);
    }

    /// Regression for B2: `load_model` used to run `enforce_max_loaded_versions`
    /// (which may evict a healthy version) before the duplicate check in
    /// `registry.register`. Re-loading an already-loaded version must fail
    /// with `VersionAlreadyLoaded` WITHOUT evicting anything.
    ///
    /// Scenario: max_loaded=2, v1 (active) + v2 loaded. Re-load v1 → must
    /// reject, and v2 must still be loaded.
    #[tokio::test]
    async fn test_load_model_duplicate_rejects_before_lru_eviction() {
        // On-disk fixture so load_model passes the model.py checks and
        // reaches the duplicate guard.
        let repo =
            std::env::temp_dir().join(format!("lite-server-b2-dup-test-{}", std::process::id()));
        for v in ["1", "2"] {
            let dir = repo.join("m").join(v);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("model.py"), "# test fixture\n").unwrap();
        }

        let registry = Arc::new(ModelRegistry::new());
        registry.set_strategy("m", &lru_strategy(Some(2))).unwrap();
        for v in ["1", "2"] {
            registry
                .register(
                    "m",
                    v,
                    ModelConfig::default(),
                    ModelType::LitAPI,
                    repo.join("m").join(v),
                )
                .unwrap();
            registry.mark_ready("m", v).unwrap();
        }
        registry.activate_version("m", "1").unwrap();

        let wm = WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "warn".to_string(),
            Arc::new(CallbackRunner::new()),
        );

        let err = wm
            .load_model("m", "1", &ModelConfig::default())
            .await
            .unwrap_err();
        assert!(
            matches!(err, AppError::VersionAlreadyLoaded(ref m, ref v) if m == "m" && v == "1"),
            "duplicate load must reject, got {err:?}"
        );
        assert!(
            registry.get("m", Some("2")).is_some(),
            "v2 must NOT be evicted by a doomed duplicate load"
        );
        assert_eq!(registry.list_versions("m").len(), 2);

        let _ = std::fs::remove_dir_all(&repo);
    }

    // ===== B1 regression guard: ensemble detection in load_model =====

    /// Regression guard (fixed in e5e45f4): `load_model` used to detect
    /// ensemble models via `content.contains("ensemble:")` rather than
    /// structural YAML parsing. A config.yaml with "ensemble:" appearing
    /// ONLY in a YAML comment or description string was misclassified as
    /// `ModelType::Ensemble` — no workers spawned, yet the model was
    /// silently marked Ready with nothing to serve. Detection is now
    /// structural; this test locks that in.
    #[tokio::test]
    async fn test_load_model_ensemble_detection_false_positive_on_comment() {
        let repo =
            std::env::temp_dir().join(format!("lite-server-wm-ensemble-fp-{}", std::process::id()));
        let model_dir = repo.join("test_model").join("1");
        std::fs::create_dir_all(&model_dir).unwrap();

        // Create a minimal but valid LitAPI model.py — after the fix the
        // model is classified as LitAPI and a real worker is spawned, so the
        // file must contain an actual LitAPI subclass.
        std::fs::write(
            model_dir.join("model.py"),
            r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#,
        )
        .unwrap();

        // config.yaml where "ensemble:" appears only in a YAML comment.
        // Structural detection (serde_yaml top-level key) classifies this
        // as LitAPI; the pre-e5e45f4 string-contains check misclassified it
        // as Ensemble.
        std::fs::write(
            model_dir.join("config.yaml"),
            "# ensemble: this is just a comment, not a real ensemble config\nmax_batch_size: 4\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        // The repo path must be the repo directory itself, not the model_dir —
        // resolve_model_dir joins repo_path / model_name / version.
        let wm = WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "warn".to_string(),
            Arc::new(CallbackRunner::new()),
        );

        let config = ModelConfig::default();

        // model.py EXISTS and "ensemble:" is only a comment, so load_model
        // must take the normal LitAPI path (spawn workers). Pre-e5e45f4 the
        // string-contains check took the ensemble fast path instead: register
        // as Ensemble, mark ready, return Ok — zero workers spawned.
        let result = wm.load_model("test_model", "1", &config).await;

        assert!(
            result.is_ok(),
            "load_model must succeed for a LitAPI model whose config only \
             mentions 'ensemble:' in a comment. Got: {:?}",
            result.err()
        );

        // The model must be registered as LitAPI, not Ensemble.
        let mv = registry.get("test_model", Some("1")).unwrap();
        assert_eq!(
            mv.model_type,
            ModelType::LitAPI,
            "model with model.py and 'ensemble:' only in a comment must be \
             classified as LitAPI, not {:?}.",
            mv.model_type
        );
        assert_eq!(
            mv.status,
            VersionStatus::Ready,
            "Model wrongly classified as Ensemble would be marked Ready \
             without any workers"
        );
        assert!(
            !mv.workers.is_empty(),
            "LitAPI-classified model must have spawned workers — an \
             Ensemble-classified model has none and would fail silently"
        );

        // Reap the spawned worker so it doesn't outlive the test repo dir.
        let _ = wm.unload_model("test_model", Some("1")).await;

        let _ = std::fs::remove_dir_all(&repo);
    }

    // ===== notify_file_changed (P3: hot reload without restart) =====

    fn fc_test_manager() -> Arc<WorkerManager> {
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(CallbackRunner::new());
        Arc::new(WorkerManager::new(
            registry,
            std::env::temp_dir(),
            inference_queue,
            "warn".to_string(),
            callback_runner,
        ))
    }

    #[cfg(unix)]
    fn fc_ipc_endpoint(tag: &str) -> String {
        let sock = std::env::temp_dir().join(format!(
            "lite-server-fc-{}-{}.sock",
            tag,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&sock);
        format!("ipc://{}", sock.display())
    }

    /// A fake worker on a ZMQ PAIR socket: expects one FILE_CHANGED request,
    /// asserts its paths, replies with the given SingleResponse data payload.
    #[cfg(unix)]
    fn spawn_fake_worker(
        endpoint: String,
        expect_paths: Vec<String>,
        reply_data: &'static [u8],
    ) -> std::thread::JoinHandle<()> {
        use prost::Message as _;
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let socket = ctx.socket(zmq::PAIR).unwrap();
            socket.set_rcvtimeo(5000).unwrap();
            socket.set_linger(0).unwrap();
            socket.connect(&endpoint).unwrap();
            let bytes = socket
                .recv_bytes(0)
                .expect("fake worker: no request received");
            let req = crate::proto::liteserver::Request::decode(bytes.as_slice()).unwrap();
            match req.payload {
                Some(crate::proto::liteserver::request::Payload::FileChanged(fc)) => {
                    assert_eq!(fc.paths, expect_paths);
                }
                _ => panic!("fake worker: expected FileChanged payload"),
            }
            let resp = crate::proto::liteserver::Response {
                uid: req.uid,
                metrics: None,
                payload: Some(crate::proto::liteserver::response::Payload::Single(
                    crate::proto::liteserver::SingleResponse {
                        data: bytes::Bytes::from_static(reply_data),
                        status: Some(crate::proto::liteserver::Status {
                            code: "Ok".to_string(),
                            message: String::new(),
                        }),
                        ..Default::default()
                    },
                )),
            };
            socket.send(resp.encode_to_vec(), 0).unwrap();
            // Keep the socket alive briefly so the reply actually flushes.
            std::thread::sleep(std::time::Duration::from_millis(300));
        })
    }

    #[cfg(unix)]
    async fn fc_manager_with_worker(tag: &str) -> (Arc<WorkerManager>, String) {
        let wm = fc_test_manager();
        let endpoint = fc_ipc_endpoint(tag);
        let client = Arc::new(WorkerZmqClient::new(endpoint.clone()));
        wm.zmq_clients
            .write()
            .await
            .insert(model_version_key("m", "1"), vec![client]);
        // Give the blocking bind a moment to come up before the peer connects.
        tokio::time::sleep(Duration::from_millis(200)).await;
        (wm, endpoint)
    }

    #[tokio::test]
    async fn notify_file_changed_no_workers_returns_false() {
        let wm = fc_test_manager();
        assert!(
            !wm.notify_file_changed("m", "1", &["/x.py".to_string()])
                .await,
            "no workers → nothing handled → caller must fall back to restart"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn notify_file_changed_handled_returns_true() {
        let (wm, endpoint) = fc_manager_with_worker("handled").await;
        let fake = spawn_fake_worker(
            endpoint,
            vec!["/a.py".to_string(), "/b.yaml".to_string()],
            b"{\"handled\":true}",
        );
        assert!(
            wm.notify_file_changed("m", "1", &["/a.py".to_string(), "/b.yaml".to_string()])
                .await
        );
        fake.join().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn notify_file_changed_unhandled_returns_false() {
        let (wm, endpoint) = fc_manager_with_worker("unhandled").await;
        let fake = spawn_fake_worker(endpoint, vec!["/a.py".to_string()], b"{\"handled\":false}");
        assert!(
            !wm.notify_file_changed("m", "1", &["/a.py".to_string()])
                .await
        );
        fake.join().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn notify_file_changed_malformed_reply_returns_false() {
        // Old workers predating FILE_CHANGED reply "Unsupported payload type"
        // (an error SingleResponse); any non-{"handled":true} body means the
        // caller must fall back to restart.
        let (wm, endpoint) = fc_manager_with_worker("malformed").await;
        let fake = spawn_fake_worker(
            endpoint,
            vec!["/a.py".to_string()],
            b"Unsupported payload type",
        );
        assert!(
            !wm.notify_file_changed("m", "1", &["/a.py".to_string()])
                .await
        );
        fake.join().unwrap();
    }

    // ===== B4: load_model silently drops model_defaults =====

    /// B4 (P1): `load_model` re-reads `config.yaml` and replaces the caller's
    /// `config` argument — which the caller (e.g. `reconcile_models`, CLI
    /// `serve`) has already applied `model_defaults` to.  The YAML-on-disk
    /// parse starts from serde defaults, so any field NOT explicitly in the
    /// per-model `config.yaml` reverts to the hardcoded serde default,
    /// silently ignoring the `model_defaults` override.
    ///
    /// Real example: `model_defaults: { max_queue_size: 500 }` has no effect
    /// on models with a `config.yaml` on disk — the per-model parse resets
    /// `max_queue_size` to 1000 (the serde default).  Only CLI overrides
    /// survived the drop before this test was written.
    #[tokio::test]
    async fn test_load_model_drops_model_defaults_when_config_yaml_exists() {
        let repo = std::env::temp_dir().join(format!("lite-server-b4-def-{}", std::process::id()));
        let model_dir = repo.join("m").join("1");
        std::fs::create_dir_all(&model_dir).unwrap();

        // Valid LitAPI model so the worker spawn path doesn't fail.
        std::fs::write(
            model_dir.join("model.py"),
            r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#,
        )
        .unwrap();

        // config.yaml WITHOUT max_queue_size — the caller applied a default
        // override (500) that should survive into the registry.
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "warn".to_string(),
            Arc::new(CallbackRunner::new()),
        );

        // Simulate what reconcile_models (and the startup path) does:
        // apply model_defaults, then hand the config to load_model.
        let mut config = ModelConfig::default();
        let defaults = crate::config::ModelTunables {
            max_queue_size: Some(500),
            ..Default::default()
        };
        defaults.apply_to(&mut config);
        assert_eq!(
            config.max_queue_size, 500,
            "defaults applied before load_model"
        );

        wm.load_model("m", "1", &config).await.unwrap();

        let mv = registry.get("m", Some("1")).unwrap();
        assert_eq!(
            mv.config.max_queue_size, 500,
            "B4 REGRESSION: model_defaults max_queue_size=500 was silently \
             dropped by load_model re-reading config.yaml from disk. The \
             YAML file doesn't set max_queue_size, so the serde default \
             (1000) wins instead of the caller's override (500). Expected \
             500, got {}.",
            mv.config.max_queue_size
        );

        let _ = wm.unload_model("m", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// B4 companion: `reload_model` re-enters `load_model` with the registry's
    /// config (which had model_defaults applied at the original load).  The
    /// reload must not lose those defaults.
    #[tokio::test]
    async fn test_reload_model_preserves_model_defaults() {
        let repo =
            std::env::temp_dir().join(format!("lite-server-b4-reload-{}", std::process::id()));
        let model_dir = repo.join("m").join("1");
        std::fs::create_dir_all(&model_dir).unwrap();

        std::fs::write(
            model_dir.join("model.py"),
            r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#,
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let defaults = crate::config::ModelTunables {
            max_queue_size: Some(500),
            ..Default::default()
        };
        // Batch 0: reload_model now re-reads config from disk and applies the
        // model_defaults held by WorkerManager (no longer the registry's stale
        // config). The defaults must live on the WorkerManager or the disk
        // path cannot see them.
        let wm = WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "warn".to_string(),
            Arc::new(CallbackRunner::new()),
        )
        .with_model_defaults(defaults.clone());

        let mut config = ModelConfig::default();
        defaults.apply_to(&mut config);
        wm.load_model("m", "1", &config).await.unwrap();

        assert!(wm.reload_model("m", Some("1")).await.unwrap());

        let mv = registry.get("m", Some("1")).unwrap();
        assert_eq!(
            mv.config.max_queue_size, 500,
            "reload_model must preserve model_defaults (got {})",
            mv.config.max_queue_size
        );

        let _ = wm.unload_model("m", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// Batch 0 (profile prerequisite): `reload_model` must re-read
    /// config.yaml from disk instead of reusing the registry's stale config —
    /// otherwise the "edit config.yaml → reload" swap mechanism silently
    /// measures the old config (plan §0.1).
    #[tokio::test]
    async fn test_reload_model_rereads_config_from_disk() {
        let repo =
            std::env::temp_dir().join(format!("lite-server-reload-disk-{}", std::process::id()));
        let model_dir = repo.join("m").join("1");
        std::fs::create_dir_all(&model_dir).unwrap();

        std::fs::write(
            model_dir.join("model.py"),
            r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#,
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "warn".to_string(),
            Arc::new(CallbackRunner::new()),
        );

        let config = crate::config::load_model_config(&model_dir.join("config.yaml")).unwrap();
        wm.load_model("m", "1", &config).await.unwrap();
        assert_eq!(
            registry.get("m", Some("1")).unwrap().config.max_batch_size,
            1
        );

        // Change config.yaml on disk, then reload — the new value must land.
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 4\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();

        assert!(wm.reload_model("m", Some("1")).await.unwrap());

        let mv = registry.get("m", Some("1")).unwrap();
        assert_eq!(
            mv.config.max_batch_size, 4,
            "reload_model must re-read config.yaml from disk (got {})",
            mv.config.max_batch_size
        );

        let _ = wm.unload_model("m", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// Batch 0 validate-then-swap: a bad config.yaml → reload returns Err
    /// early, **old workers keep serving** (no unload happens).
    #[tokio::test]
    async fn test_reload_model_bad_config_keeps_old_version() {
        let repo =
            std::env::temp_dir().join(format!("lite-server-reload-badcfg-{}", std::process::id()));
        let model_dir = repo.join("m").join("1");
        std::fs::create_dir_all(&model_dir).unwrap();

        std::fs::write(
            model_dir.join("model.py"),
            r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#,
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "warn".to_string(),
            Arc::new(CallbackRunner::new()),
        );

        let config = crate::config::load_model_config(&model_dir.join("config.yaml")).unwrap();
        wm.load_model("m", "1", &config).await.unwrap();

        // Corrupt the config: unclosed flow sequence is a YAML parse error.
        std::fs::write(model_dir.join("config.yaml"), "max_batch_size: [unclosed\n").unwrap();

        let err = wm.reload_model("m", Some("1")).await.unwrap_err();
        assert!(
            registry.get("m", Some("1")).is_some(),
            "bad config must be refused BEFORE unload (validate-then-swap); got: {}",
            err
        );

        let _ = wm.unload_model("m", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// G1: the warmup plan must spread dummy inferences across ALL workers —
    /// each worker process owns separate engine state (CUDA graphs, allocator
    /// pools), so warming only the least-loaded winner (worker 0 under serial
    /// submission) would leave N-1 workers cold.
    #[test]
    fn warmup_plan_worker_scope_covers_every_worker() {
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            scope: crate::config::WarmupScope::Worker,
            samples: vec![
                crate::config::WarmupSample {
                    input_ref: "warmup/a.json".to_string(),
                    iterations: 2,
                    ..Default::default()
                },
                crate::config::WarmupSample {
                    input_ref: "warmup/b.json".to_string(),
                    iterations: 1,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let plan = build_warmup_plan(&policy, 3);
        assert_eq!(
            plan.len(),
            3 * (2 + 1),
            "worker scope = workers x samples x iterations"
        );
        for w in 0..3 {
            let units: Vec<_> = plan.iter().filter(|u| u.worker_pin == Some(w)).collect();
            assert_eq!(
                units.len(),
                3,
                "worker {w} must receive the full sample set"
            );
            assert_eq!(
                units.iter().filter(|u| u.sample == 0).count(),
                2,
                "worker {w} must run sample 0 twice"
            );
        }
    }

    #[test]
    fn warmup_plan_version_scope_round_robins_across_workers() {
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            scope: crate::config::WarmupScope::Version,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup/a.json".to_string(),
                iterations: 7,
                ..Default::default()
            }],
            ..Default::default()
        };
        let plan = build_warmup_plan(&policy, 3);
        assert_eq!(plan.len(), 7, "version scope keeps the configured total");
        let pins: Vec<_> = plan.iter().map(|u| u.worker_pin).collect();
        assert_eq!(
            pins,
            vec![
                Some(0),
                Some(1),
                Some(2),
                Some(0),
                Some(1),
                Some(2),
                Some(0)
            ],
            "units must round-robin so no worker stays cold"
        );
    }

    #[test]
    fn warmup_plan_zero_workers_leaves_units_unpinned() {
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            scope: crate::config::WarmupScope::Worker,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup/a.json".to_string(),
                iterations: 2,
                ..Default::default()
            }],
            ..Default::default()
        };
        let plan = build_warmup_plan(&policy, 0);
        assert_eq!(plan.len(), 2, "no workers -> a single unpinned pass");
        assert!(
            plan.iter().all(|u| u.worker_pin.is_none()),
            "no pin target must not fabricate one"
        );
    }

    #[test]
    fn warmup_groups_have_uniform_pins_in_plan_order() {
        // G5: units sharing a collector window must never mix pins (a mixed
        // batch silently drops the pin via batch_direct_pin's conflict
        // fallback), so the executor groups by pin — each group uniform,
        // groups in first-occurrence order.
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            scope: crate::config::WarmupScope::Version,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup/a.json".to_string(),
                iterations: 7,
                ..Default::default()
            }],
            ..Default::default()
        };
        let plan = build_warmup_plan(&policy, 3);
        let groups = group_units_by_pin(&plan);
        assert_eq!(groups.len(), 3, "three pins -> three groups");
        let group_pins: Vec<_> = groups.iter().map(|g| g[0].worker_pin).collect();
        assert_eq!(group_pins, vec![Some(0), Some(1), Some(2)]);
        for g in &groups {
            assert!(
                g.iter().all(|u| u.worker_pin == g[0].worker_pin),
                "every group must be pin-unanimous"
            );
        }
        assert_eq!(
            groups.iter().map(|g| g.len()).sum::<usize>(),
            7,
            "grouping must not drop units"
        );
    }

    /// Batch-4 e2e harness: temp repo with the caller's model source and
    /// warmup policy; loads the model and returns manager/registry/repo plus
    /// the load outcome for assertions.
    #[cfg(unix)]
    async fn warmup_e2e_load(
        tag: &str,
        model_py: &str,
        policy: crate::config::WarmupPolicy,
    ) -> (
        WorkerManager,
        Arc<ModelRegistry>,
        std::path::PathBuf,
        Result<(), AppError>,
    ) {
        let repo =
            std::env::temp_dir().join(format!("lite-server-b4-{tag}-{}", std::process::id()));
        let model_dir = repo.join(tag).join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join("model.py"), model_py).unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();
        std::fs::write(model_dir.join("warmup_input.json"), "{\"input\": 7}").unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "debug".to_string(),
            Arc::new(CallbackRunner::new()),
        );
        let mut config = ModelConfig::default();
        config.policies.warmup = Some(policy);
        let result = wm.load_model(tag, "1", &config).await.map(|_| ());
        (wm, registry, repo, result)
    }

    /// 100ms per predict (ASYNC sleep — a sync sleep would block the worker's
    /// event loop and serialize everything, defeating the concurrency test);
    /// appends one marker line per call.
    #[cfg(unix)]
    fn slow_marker_model(marker: &std::path::Path) -> String {
        format!(
            r#"from lite_server import LitAPI
import asyncio


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    async def predict(self, x):
        with open({marker:?}, "a") as f:
            f.write("w\n")
        await asyncio.sleep(0.1)
        return {{"output": x}}

    def encode_response(self, output):
        return output
"#,
            marker = marker.display().to_string()
        )
    }

    /// G4: the per-iteration budget (30s here) must NOT be the only bound —
    /// total_timeout_secs caps the whole run (all units across all workers).
    #[cfg(unix)]
    #[tokio::test]
    async fn warmup_total_timeout_fails_the_whole_run() {
        let repo =
            std::env::temp_dir().join(format!("lite-server-b4-b4_total-{}", std::process::id()));
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup_input.json".to_string(),
                iterations: 4,
                ..Default::default()
            }],
            timeout_secs: 30.0,
            total_timeout_secs: 0.25,
            ..Default::default()
        };
        let (wm, _registry, repo, result) =
            warmup_e2e_load("b4_total", &slow_marker_model(&repo.join("m.txt")), policy).await;
        let err = result.expect_err("total timeout must fail the load");
        assert!(
            err.to_string().contains("total timeout"),
            "reason must name the total budget, got: {err}"
        );
        assert_eq!(
            crate::metrics::prometheus::MODEL_WARMUP_TOTAL
                .with_label_values(&["b4_total", "1", "timeout"])
                .get(),
            1.0,
            "a total-budget cut counts as status=timeout"
        );
        let _ = wm.unload_model("b4_total", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// G5: concurrency bounds the warmup wall time without dropping units.
    #[cfg(unix)]
    #[tokio::test]
    async fn warmup_concurrency_bounds_wall_time_and_runs_all_units() {
        let repo =
            std::env::temp_dir().join(format!("lite-server-b4-b4_conc-{}", std::process::id()));
        let marker = repo.join("marker.txt");
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup_input.json".to_string(),
                iterations: 6,
                ..Default::default()
            }],
            timeout_secs: 30.0,
            concurrency: 3,
            ..Default::default()
        };
        let (wm, _registry, repo, result) =
            warmup_e2e_load("b4_conc", &slow_marker_model(&marker), policy).await;
        result.expect("concurrent warmup must succeed");
        let lines = std::fs::read_to_string(&marker)
            .unwrap_or_default()
            .lines()
            .count();
        assert_eq!(lines, 6, "every unit must run exactly once");
        let wall = crate::metrics::prometheus::MODEL_WARMUP_DURATION
            .with_label_values(&["b4_conc", "1"])
            .get_sample_sum();
        assert!(
            wall < 0.5,
            "6 x 100ms at concurrency 3 should take ~0.2s (serial: >=0.6s), took {wall:.3}s"
        );
        let _ = wm.unload_model("b4_conc", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// G7: retries absorb transient inference failures; zero retries keeps
    /// the D33 fail-fast behavior.
    #[cfg(unix)]
    #[tokio::test]
    async fn warmup_retries_absorb_transient_failures() {
        let repo =
            std::env::temp_dir().join(format!("lite-server-b4-b4_retry-{}", std::process::id()));
        let counter = repo.join("calls.txt");
        let model_py = format!(
            r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        self.calls = 0

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        self.calls += 1
        with open({counter:?}, "w") as f:
            f.write(str(self.calls))
        if self.calls <= 2:
            raise RuntimeError("transient")
        return {{"output": x}}

    def encode_response(self, output):
        return output
"#,
            counter = counter.display().to_string()
        );
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup_input.json".to_string(),
                iterations: 1,
                ..Default::default()
            }],
            timeout_secs: 30.0,
            retries: 2,
            ..Default::default()
        };
        let (wm, _registry, repo, result) = warmup_e2e_load("b4_retry", &model_py, policy).await;
        result.expect("2 retries must absorb 2 transient failures");
        assert_eq!(
            std::fs::read_to_string(&counter).unwrap(),
            "3",
            "1 initial attempt + 2 retries"
        );
        let _ = wm.unload_model("b4_retry", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn warmup_zero_retries_keeps_fail_fast() {
        let repo =
            std::env::temp_dir().join(format!("lite-server-b4-b4_noretry-{}", std::process::id()));
        let counter = repo.join("calls.txt");
        let model_py = format!(
            r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        self.calls = 0

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        self.calls += 1
        with open({counter:?}, "w") as f:
            f.write(str(self.calls))
        raise RuntimeError("always")

    def encode_response(self, output):
        return output
"#,
            counter = counter.display().to_string()
        );
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup_input.json".to_string(),
                iterations: 1,
                ..Default::default()
            }],
            timeout_secs: 30.0,
            ..Default::default()
        };
        let (wm, _registry, repo, result) = warmup_e2e_load("b4_noretry", &model_py, policy).await;
        result.expect_err("no retries -> first failure fails the load (D33)");
        assert_eq!(
            std::fs::read_to_string(&counter).unwrap(),
            "1",
            "exactly one attempt without retries"
        );
        let _ = wm.unload_model("b4_noretry", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// G3a: a sample may target a custom @route — the handler runs (its lazy
    /// state initialized) BEFORE the version goes Ready.
    #[cfg(unix)]
    #[tokio::test]
    async fn warmup_custom_route_is_exercised() {
        let repo =
            std::env::temp_dir().join(format!("lite-server-b4-b5_route-{}", std::process::id()));
        let marker = repo.join("route_marker.txt");
        let model_py = format!(
            r#"from lite_server import LitAPI, route


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {{"output": x}}

    def encode_response(self, output):
        return output

    @route.post("/warm_route")
    def warm_route(self, ctx):
        with open({marker:?}, "a") as f:
            f.write("r\n")
        return {{"warmed": True}}
"#,
            marker = marker.display().to_string()
        );
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![
                crate::config::WarmupSample {
                    input_ref: "warmup_input.json".to_string(),
                    iterations: 1,
                    ..Default::default()
                },
                crate::config::WarmupSample {
                    input_ref: "warmup_input.json".to_string(),
                    iterations: 2,
                    route: "/warm_route".to_string(),
                    ..Default::default()
                },
            ],
            timeout_secs: 30.0,
            ..Default::default()
        };
        let (wm, _registry, repo, result) = warmup_e2e_load("b5_route", &model_py, policy).await;
        result.expect("route warmup must succeed");
        assert_eq!(
            std::fs::read_to_string(&marker)
                .unwrap_or_default()
                .lines()
                .count(),
            2,
            "the route sample must run its iterations through the @route handler"
        );
        let _ = wm.unload_model("b5_route", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// G3a: a sample targeting a route with no registered handler must fail
    /// the load — a silently skipped route sample is a silently cold handler.
    #[cfg(unix)]
    #[tokio::test]
    async fn warmup_unknown_route_fails_the_load() {
        let model_py = r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#;
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup_input.json".to_string(),
                iterations: 1,
                route: "/no_such_route".to_string(),
                ..Default::default()
            }],
            timeout_secs: 30.0,
            ..Default::default()
        };
        let (wm, _registry, repo, result) = warmup_e2e_load("b5_noroute", model_py, policy).await;
        let err = result.expect_err("unknown route must fail the load");
        assert!(
            err.to_string().contains("warmup inference returned error")
                || err.to_string().contains("no_such_route"),
            "reason must surface the route failure, got: {err}"
        );
        let _ = wm.unload_model("b5_noroute", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// G3b harness model: an async `stream_predict` generator that writes one
    /// marker line at generator ENTRY (deterministic — per-yield writes would
    /// race the cancel), then yields `chunks` chunks with `chunk_delay`
    /// between them.
    #[cfg(unix)]
    fn stream_marker_model(marker: &std::path::Path, chunks: u32, chunk_delay: f64) -> String {
        format!(
            r#"from lite_server import LitAPI
import asyncio


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {{"output": x}}

    def encode_response(self, output):
        return output

    async def stream_predict(self, x):
        with open({marker:?}, "a") as f:
            f.write("s\n")
        for i in range({chunks}):
            yield {{"chunk": i}}
            await asyncio.sleep({chunk_delay})
"#,
            marker = marker.display().to_string(),
        )
    }

    /// G3b: mode=stream + first_chunk (defaults) warms the streaming TTFT
    /// path — the generator runs — then CANCELS instead of draining: the
    /// whole run must finish well before the stream's natural end.
    #[cfg(unix)]
    #[tokio::test]
    async fn warmup_stream_first_chunk_cancels_instead_of_draining() {
        let repo = std::env::temp_dir()
            .join(format!("lite-server-b4-b6_stream-{}", std::process::id()));
        let marker = repo.join("stream_marker.txt");
        // 200 chunks x 10ms = 2s if drained; first_chunk + cancel ≈ instant.
        let model_py = stream_marker_model(&marker, 200, 0.01);
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup_input.json".to_string(),
                iterations: 1,
                mode: crate::config::WarmupSampleMode::Stream,
                ..Default::default()
            }],
            timeout_secs: 30.0,
            ..Default::default()
        };
        let (wm, _registry, repo, result) =
            warmup_e2e_load("b6_stream", &model_py, policy).await;
        result.expect("stream warmup must succeed");
        assert_eq!(
            std::fs::read_to_string(&marker)
                .unwrap_or_default()
                .lines()
                .count(),
            1,
            "the generator must have run exactly once"
        );
        let wall = crate::metrics::prometheus::MODEL_WARMUP_DURATION
            .with_label_values(&["b6_stream", "1"])
            .get_sample_sum();
        assert!(
            wall < 1.0,
            "first_chunk must cancel, not drain (drain = 2s); took {wall:.3}s"
        );
        let _ = wm.unload_model("b6_stream", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// G3b: an error frame from the stream fails the warmup (and thus the
    /// load, D33) — a broken stream_predict must never go Ready silent.
    #[cfg(unix)]
    #[tokio::test]
    async fn warmup_stream_error_frame_fails_the_load() {
        let model_py = r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output

    async def stream_predict(self, x):
        raise RuntimeError("stream boom")
        yield {"never": True}
"#;
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup_input.json".to_string(),
                iterations: 1,
                mode: crate::config::WarmupSampleMode::Stream,
                ..Default::default()
            }],
            timeout_secs: 30.0,
            ..Default::default()
        };
        let (wm, _registry, repo, result) =
            warmup_e2e_load("b6_stream_err", model_py, policy).await;
        let err = result.expect_err("an error frame must fail the load");
        assert!(
            err.to_string().contains("stream boom"),
            "the stream error message must surface, got: {err}"
        );
        let _ = wm.unload_model("b6_stream_err", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// G3b: completion=drain consumes the stream to Done — every chunk the
    /// model produces is pulled through the pipe.
    #[cfg(unix)]
    #[tokio::test]
    async fn warmup_stream_drain_consumes_to_done() {
        let repo = std::env::temp_dir()
            .join(format!("lite-server-b4-b6_drain-{}", std::process::id()));
        let marker = repo.join("stream_marker.txt");
        let model_py = stream_marker_model(&marker, 3, 0.0);
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup_input.json".to_string(),
                iterations: 2,
                mode: crate::config::WarmupSampleMode::Stream,
                completion: Some(crate::config::WarmupStreamCompletion::Drain),
                ..Default::default()
            }],
            timeout_secs: 30.0,
            ..Default::default()
        };
        let (wm, _registry, repo, result) =
            warmup_e2e_load("b6_drain", &model_py, policy).await;
        result.expect("drain warmup must succeed");
        // Generator-entry marker: 2 iterations. If drain stopped early the
        // stream would leak; success already proves Done was received, and
        // the marker proves both iterations ran.
        assert_eq!(
            std::fs::read_to_string(&marker)
                .unwrap_or_default()
                .lines()
                .count(),
            2,
            "both iterations must run their generator to Done"
        );
        let _ = wm.unload_model("b6_drain", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// F1 (warmup-gaps audit): dropping an ARMED StreamCancelGuard must put a
    /// StreamCancel on the wire — the timeout/unexpected-frame/outer-abort
    /// paths rely on it to stop the worker-side generator.
    #[cfg(unix)]
    #[tokio::test]
    async fn stream_cancel_guard_sends_cancel_on_armed_drop() {
        use prost::Message as _;
        let endpoint = fc_ipc_endpoint("cancel-guard");
        let client = Arc::new(WorkerZmqClient::new(endpoint.clone()));
        // Give the blocking bind a moment to come up before the peer connects.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let (tx, rx) = std::sync::mpsc::channel();
        let (connected_tx, connected_rx) = std::sync::mpsc::channel();
        let peer = std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let socket = ctx.socket(zmq::PAIR).unwrap();
            socket.set_rcvtimeo(5000).unwrap();
            socket.set_linger(0).unwrap();
            socket.connect(&endpoint).unwrap();
            connected_tx.send(()).unwrap();
            let bytes = socket.recv_bytes(0).expect("peer: no frame received");
            tx.send(bytes).unwrap();
            // Keep the socket alive briefly so the receive is not raced by drop.
            std::thread::sleep(std::time::Duration::from_millis(200));
        });

        // The actor's raw send is EAGAIN-dropped without a connected peer —
        // wait out the ZMQ handshake before arming/dropping the guard.
        connected_rx.recv().unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        {
            let _guard = StreamCancelGuard::armed(client.clone(), "stream-x".to_string());
        }
        // spawn_blocking: a bare blocking recv on the current-thread test
        // runtime would starve the spawned cancel task itself.
        let bytes = tokio::task::spawn_blocking(move || {
            rx.recv_timeout(std::time::Duration::from_secs(5))
                .expect("armed drop must send a cancel frame")
        })
        .await
        .unwrap();
        let req = crate::proto::liteserver::Request::decode(bytes.as_slice()).unwrap();
        match req.payload {
            Some(crate::proto::liteserver::request::Payload::Stream(sr)) => {
                assert_eq!(sr.stream_id, "stream-x");
                assert!(
                    matches!(
                        sr.action,
                        Some(crate::proto::liteserver::stream_request::Action::Cancel(_))
                    ),
                    "armed drop must send StreamCancel, got {:?}",
                    sr.action
                );
            }
            other => panic!("expected Stream payload, got {other:?}"),
        }
        peer.join().unwrap();

        // A disarmed guard sends nothing (no spawn, no panic).
        let mut guard = StreamCancelGuard::armed(client, "stream-y".to_string());
        guard.disarm();
        drop(guard);
    }

    /// F1 (warmup-gaps audit): a stream warmup that TIMES OUT must cancel the
    /// worker-side stream — previously the generator kept running to its
    /// natural end (here: 5s) because only the first_chunk SUCCESS path sent
    /// a cancel.
    #[cfg(unix)]
    #[tokio::test]
    async fn warmup_stream_timeout_cancels_worker_side_stream() {
        let repo =
            std::env::temp_dir().join(format!("lite-server-b4-b6_to_cancel-{}", std::process::id()));
        let marker = repo.join("cancel_marker.txt");
        let model_py = format!(
            r#"from lite_server import LitAPI
import asyncio


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {{"output": x}}

    def encode_response(self, output):
        return output

    async def stream_predict(self, x):
        try:
            await asyncio.sleep(5)
            yield {{"chunk": 0}}
        except asyncio.CancelledError:
            with open({marker:?}, "a") as f:
                f.write("cancelled\n")
            raise
"#,
            marker = marker.display().to_string()
        );
        // Load with warmup DISABLED so the version is Ready; the warmup under
        // test is then run manually (a load-time failure would tear the
        // workers down before the cancel could be observed).
        let disabled = crate::config::WarmupPolicy::default();
        let (wm, _registry, repo, result) =
            warmup_e2e_load("b6_to_cancel", &model_py, disabled).await;
        result.expect("load without warmup must succeed");

        let policy = crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup_input.json".to_string(),
                iterations: 1,
                mode: crate::config::WarmupSampleMode::Stream,
                ..Default::default()
            }],
            timeout_secs: 0.3,
            ..Default::default()
        };
        let config = crate::config::ModelConfig::default();
        let err = wm
            .run_warmup("b6_to_cancel", "1", &config, &policy, None)
            .await
            .expect_err("the 0.3s budget must cut the 5s stream");
        assert!(
            err.contains("timed out"),
            "expected a timeout failure, got: {err}"
        );

        // The cancel must land well before the stream's 5s natural end.
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let mut cancelled = false;
        while std::time::Instant::now() < deadline {
            if std::fs::read_to_string(&marker)
                .unwrap_or_default()
                .contains("cancelled")
            {
                cancelled = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            cancelled,
            "the timed-out stream must be cancelled worker-side (long before its 5s natural end)"
        );
        let _ = wm.unload_model("b6_to_cancel", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    #[test]
    fn warmup_plan_pinned_rewarm_covers_full_sample_set() {
        // G2: re-warming ONE replacement worker must run the full sample set
        // pinned to its slot — regardless of `scope` (a version-wide fraction
        // would leave the cold process mostly unwarmed).
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            scope: crate::config::WarmupScope::Version,
            samples: vec![
                crate::config::WarmupSample {
                    input_ref: "warmup/a.json".to_string(),
                    iterations: 2,
                    ..Default::default()
                },
                crate::config::WarmupSample {
                    input_ref: "warmup/b.json".to_string(),
                    iterations: 1,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let plan = build_warmup_plan_for_worker(&policy, 2);
        assert_eq!(plan.len(), 3, "full sample set: 2 + 1 units");
        assert!(
            plan.iter().all(|u| u.worker_pin == Some(2)),
            "every unit pinned to the replacement's slot"
        );
        assert_eq!(plan.iter().filter(|u| u.sample == 0).count(), 2);
        assert_eq!(plan.iter().filter(|u| u.sample == 1).count(), 1);
    }

    /// L5 reproduction (RED): `load_model` spawns workers and registers the
    /// queue/client/outlier state BEFORE running warmup (:320-590, then :597).
    /// When warmup fails the version is only `mark_failed` and `Err` returned —
    /// the spawned worker processes are never torn down. Each warmup-failed
    /// load therefore leaves live workers holding model weights until an
    /// explicit unload / LRU eviction / server shutdown.
    ///
    /// Fixed code must reap the workers on the warmup-failure path. Current
    /// code leaves them running (RED, defect confirmed).
    #[cfg(unix)]
    #[tokio::test]
    async fn test_warmup_failure_leaves_no_live_workers() {
        use crate::config::{WarmupPolicy, WarmupSample};

        let repo =
            std::env::temp_dir().join(format!("lite-server-l5-warmup-{}", std::process::id()));
        let model_dir = repo.join("l5_model").join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(
            model_dir.join("model.py"),
            r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#,
        )
        .unwrap();
        // A plain config: workers spawn and become Ready; warmup is driven
        // from the ModelConfig below (not the on-disk YAML).
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = WorkerManager::new(
            registry.clone(),
            repo.clone(),
            Arc::new(InferenceQueue::new()),
            "debug".to_string(),
            Arc::new(CallbackRunner::new()),
        );

        // Warmup enabled, sample points at a missing file → run_warmup fails
        // reading it AFTER the worker is already spawned and Ready.
        let mut config = ModelConfig::default();
        config.policies.warmup = Some(WarmupPolicy {
            enabled: true,
            samples: vec![WarmupSample {
                input_ref: "does_not_exist.json".to_string(),
                iterations: 1,
                ..Default::default()
            }],
            timeout_secs: 5.0,
            dummy_input_ref: None,
            iterations: None,
            scope: crate::config::WarmupScope::default(),
            respawn: true,
            total_timeout_secs: 0.0,
            concurrency: 1,
            retries: 0,
        });

        let err = wm.load_model("l5_model", "1", &config).await.unwrap_err();
        assert!(
            matches!(err, AppError::WorkerCrashed(_)),
            "warmup failure must fail the load, got {err:?}"
        );
        assert_eq!(
            crate::metrics::prometheus::MODEL_WARMUP_TOTAL
                .with_label_values(&["l5_model", "1", "failure"])
                .get(),
            1.0,
            "G6: the failed warmup must be counted with status=failure"
        );

        // The version is registered as Failed (load_model registered before
        // warmup) — collect its spawned worker pids.
        let pids: Vec<i32> = registry
            .get("l5_model", Some("1"))
            .map(|mv| {
                mv.workers
                    .iter()
                    .filter_map(|w| w.pid)
                    .map(|p| p as i32)
                    .collect()
            })
            .unwrap_or_default();
        assert!(
            !pids.is_empty(),
            "precondition: warmup-failed version must have spawned workers"
        );

        // Poll up to 5s for every worker to be dead (fully reaped).
        let mut alive: Vec<i32> = pids.clone();
        for _ in 0..25 {
            alive = pids
                .iter()
                .copied()
                .filter(|&p| unsafe { libc::kill(p, 0) } == 0)
                .collect();
            if alive.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Clean up any orphan so the test never leaks a rogue process.
        if !alive.is_empty() {
            for p in &alive {
                unsafe {
                    libc::kill(*p, libc::SIGKILL);
                }
            }
            let _ = wm.unload_model("l5_model", Some("1")).await;
        }
        let _ = std::fs::remove_dir_all(&repo);

        assert!(
            alive.is_empty(),
            "L5: warmup-failed workers still alive: {alive:?} — load_model \
             spawned them before warmup and never reaped them on failure"
        );
    }
}
