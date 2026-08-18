//! Model lifecycle: load/unload/reload, warmup, LRU version-cap eviction,
//! max_requests auto-recycle listener, and FILE_CHANGED hot-reload notify.

use super::hooks::{execute_hook, policies_from_config};
use futures::StreamExt;
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
use crate::transport::zmq::WorkerZmqClient;
use crate::worker::protocol::*;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{error, info, warn};

/// W1 hardening: explicit error-path reap for the multi-worker spawn loop in
/// `load_model`. On any error return the guard's Drop sends the shutdown
/// signal to every spawned-but-unregistered worker (triggering the monitor's
/// kill arm, same as the unload path at :943) instead of relying on the
/// incidental sender-drop → monitor-kill chain. On success the Vec is taken
/// out via `take()` and the Drop is a no-op.
struct SpawnedWorkersGuard(Vec<WorkerProcess>);

impl std::ops::Deref for SpawnedWorkersGuard {
    type Target = Vec<WorkerProcess>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for SpawnedWorkersGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl SpawnedWorkersGuard {
    fn take(&mut self) -> Vec<WorkerProcess> {
        std::mem::take(&mut self.0)
    }
}

impl Drop for SpawnedWorkersGuard {
    fn drop(&mut self) {
        for proc in self.0.iter_mut() {
            if let Some(tx) = proc.shutdown_tx.take() {
                let _ = tx.send(());
            }
        }
    }
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

/// G1: build the warmup execution plan. `worker` scope (default) replays the
/// full sample set on EVERY worker process (worker-major order) — each process
/// owns separate engine state, so a version-wide pass would leave N-1 of N
/// workers cold. `version` scope keeps the configured total
/// (Σ samples×iterations) and round-robins units across workers. Pins ride
/// the existing `x-lite-worker-id` direct-pin header (B3).
fn build_warmup_plan(
    policy: &crate::config::WarmupPolicy,
    worker_count: usize,
) -> Vec<WarmupUnit> {
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
    let mut order: Vec<Option<usize>> = Vec::new();
    let mut groups: std::collections::HashMap<Option<usize>, Vec<WarmupUnit>> =
        std::collections::HashMap::new();
    for u in plan {
        if !groups.contains_key(&u.worker_pin) {
            order.push(u.worker_pin);
        }
        groups.entry(u.worker_pin).or_default().push(u.clone());
    }
    order
        .into_iter()
        .map(|k| groups.remove(&k).expect("key inserted above"))
        .collect()
}

impl WorkerManager {
    /// Start the reload listener. Must be called once after construction.
    pub async fn start_reload_listener(self: &Arc<Self>) {
        let mut rx_guard = self.reload_rx.lock().await;
        if let Some(mut rx) = rx_guard.take() {
            let wm = Arc::downgrade(self);
            tokio::spawn(async move {
                let mut reloading = std::collections::HashSet::new();
                while let Some(signal) = rx.recv().await {
                    // Dedup per (model, version): two versions of the same
                    // model recycle independently.
                    let key = model_version_key(&signal.model_name, &signal.version);
                    if !reloading.insert(key.clone()) {
                        continue; // already reloading
                    }
                    if let Some(wm) = wm.upgrade() {
                        info!("Auto-recycling model {} version {} (max_requests reached)",
                            signal.model_name, signal.version);
                        let result = wm.reload_model(&signal.model_name, Some(&signal.version)).await;
                        match result {
                            Ok(true) => info!("Model {} version {} auto-recycled successfully",
                                signal.model_name, signal.version),
                            Ok(false) => warn!("Model {} version {} not found for auto-recycle",
                                signal.model_name, signal.version),
                            Err(e) => error!("Model {} version {} auto-recycle failed: {}",
                                signal.model_name, signal.version, e),
                        }
                        reloading.remove(&key);
                    } else {
                        break; // WorkerManager dropped
                    }
                }
            });
        }
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

        let clients = self.get_zmq_clients(model_name, version).await.unwrap_or_default();
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
            let handled = match client.send_with_timeout(request, file_changed_timeout).await {
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

        let model_dir = crate::validation::resolve_model_dir(
            &self.repo_path, model_name, version,
        )?;
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

        self.registry
            .register(model_name, version, model_config.clone(), model_type, model_dir.clone())?;

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
                        return Err(e);
                    }
                }
            }
            // Ensemble: no workers, just mark ready
            self.registry
                .mark_ready(model_name, version)?;
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
        let devices = match &model_config.devices {
            Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(1) as usize,
            Some(serde_json::Value::String(s)) if s == "auto" => 1,
            _ => 1,
        };
        let workers_per_device = model_config.workers_per_device.unwrap_or(1);

        if model_config.continuous_batching && workers_per_device != 1 {
            warn!("continuous_batching enabled; forcing workers_per_device=1");
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

        let computed = if model_config.continuous_batching {
            devices
        } else {
            devices * workers_per_device
        };
        let total_workers = computed;

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

        let mut worker_infos = Vec::new();
        let mut worker_processes = SpawnedWorkersGuard(Vec::new());
        let mut zmq_clients_for_model = Vec::new();

        for worker_id in 0..total_workers {
            let device = format!("{}:{}", accelerator, worker_id % devices);
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
            cmd.current_dir(&model_dir);

            let mut child = cmd
                .arg("-m")
                .arg("lite_server.worker.inference")
                .arg("--model-name")
                .arg(model_name)
                .arg("--version")
                .arg(version)
                .arg("--model-py")
                .arg(&model_py)
                .arg("--config")
                .arg(&config_yaml)
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
                })
                .inspect_err(|_| self.mark_load_failed(model_name, version))?;

            let stdout = child.stdout.take()
                .ok_or_else(|| AppError::Internal("worker stdout not piped".to_string()))
                .inspect_err(|_| self.mark_load_failed(model_name, version))?;
            let stderr = child.stderr.take()
                .ok_or_else(|| AppError::Internal("worker stderr not piped".to_string()))
                .inspect_err(|_| self.mark_load_failed(model_name, version))?;

            // Wait for "ready" signal
            let mut reader = BufReader::new(stdout);
            let mut ready_line = String::new();
            let n = timeout(Duration::from_secs_f32(model_config.startup_timeout), reader.read_line(&mut ready_line))
                .await
                .map_err(|_| AppError::InferenceTimeout("worker startup timeout".to_string()))
                .and_then(|r| r.map_err(AppError::Io))
                .inspect_err(|_| self.mark_load_failed(model_name, version))?;
            if n == 0 {
                self.mark_load_failed(model_name, version);
                let stderr_tail = drain_worker_stderr(stderr, &self.server_tunables).await;
                let msg = if stderr_tail.trim().is_empty() {
                    "worker exited before ready".to_string()
                } else {
                    format!("worker exited before ready: {stderr_tail}")
                };
                return Err(AppError::WorkerCrashed(msg));
            }
            let stdout = reader.into_inner();

            let startup: WorkerStartup = serde_json::from_str(ready_line.trim())
                .map_err(|e| AppError::Internal(format!("worker startup JSON parse error: {}", e)))
                .inspect_err(|_| self.mark_load_failed(model_name, version))?;

            if startup.status != "ready" {
                self.mark_load_failed(model_name, version);
                return Err(AppError::WorkerCrashed(format!(
                    "worker {} startup failed: {:?}",
                    worker_id, startup.message
                )));
            }

            // Register custom metrics from Python worker (gated by
            // features.custom_metrics; recording no-ops on unregistered ids).
            if self.custom_metrics {
                if let Some(ref specs) = startup.metric_specs {
                    let spec_refs: Vec<(&str, &str)> = specs
                        .iter()
                        .map(|s| (s.name.as_str(), s.metric_type.as_str()))
                        .collect();
                    crate::metrics::prometheus::register_custom_metrics(model_name, version, &spec_refs);
                }
            }

            // Store per-model policies declared in config.yaml
            self.registry
                .set_policies(model_name, version, policies_from_config(&model_config));

            // Register custom @route declarations (phase 2)
            self.upsert_routes(model_name, version, startup.custom_routes).await;

            info!("Worker {} for {} v{} ready (pid={:?})", worker_id, model_name, version, child.id());

            // Fire on_ready lifecycle hook
            execute_hook("ready", &model_config.hooks, vec![
                ("$MODEL".to_string(), model_name.to_string()),
                ("$VERSION".to_string(), version.to_string()),
                ("$WORKER_ID".to_string(), worker_id.to_string()),
            ], &self.hook_tasks);

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
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr);
                let mut buf = Vec::with_capacity(1024);
                loop {
                    buf.clear();
                    match reader.read_until(b'\n', &mut buf).await {
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
                            emit_stderr_line(level, msg, worker_id_clone, &model_name_clone, &version_clone);
                        }
                        Err(e) => {
                            tracing::error!(worker_id = worker_id_clone, "Worker stderr read error: {}", e);
                            break;
                        }
                    }
                }
            });

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
                child, model_name, version, worker_id as u32, shutdown_rx,
                crate::worker::process::crash_exit_handler(
                    Some(zmq_client.clone()),
                    Some(outlier.clone()),
                    self.registry.clone(),
                    model_name.to_string(),
                    version.to_string(),
                    worker_id as u32,
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
                crate::metrics::prometheus::set_worker_pid(model_name, version, worker_id as u32, pid);
            }

            worker_processes.push(WorkerProcess {
                worker_id: worker_id as u32,
                endpoint,
                shutdown_tx: Some(shutdown_tx),
                done_rx: Some(done_rx),
                kill_timeout: Duration::from_secs_f32(model_config.worker_kill_timeout),
            });
        }

        self.registry
            .set_workers(model_name, version, worker_infos.clone())?;

        // P-WARM (§4.3): if warmup is enabled the version enters WarmingUp
        // (NOT_SERVING) here; the final Ready transition is deferred until the
        // dummy warmup below completes. Disabled → straight to Ready (legacy).
        let warmup = model_config.policies.warmup.clone();
        if let Some(ref p) = warmup {
            if p.enabled {
                self.registry.mark_warming_up(model_name, version)?;
            } else {
                self.registry.mark_ready(model_name, version)?;
            }
        } else {
            self.registry.mark_ready(model_name, version)?;
        }

        // Register inference queue for batching
        self.inference_queue
            .register_model(model_name, version, &model_config, worker_infos, zmq_clients_for_model.clone(), self.reload_tx.clone(), outlier.clone(), Some(self.respawn_tx.clone()));

        {
            let mut workers = self.workers.write().await;
            let mut clients = self.zmq_clients.write().await;
            let mut outliers = self.outlier_states.write().await;
            let key = model_version_key(model_name, version);
            workers.insert(key.clone(), worker_processes.take());
            clients.insert(key.clone(), zmq_clients_for_model);
            outliers.insert(key, outlier);
        }

        // P-WARM (§4.3): with the queue wired, drive N dummy inferences to warm
        // the engine before the version becomes Ready. The version is still
        // WarmingUp (NOT_SERVING) — readiness/routing exclude it. A warmup
        // failure marks it Failed (D33: prefer delaying availability over
        // serving an unwarmed/broken model). Skipped entirely when disabled.
        if let Some(ref policy) = warmup {
            if policy.enabled {
                match self.run_warmup(model_name, version, &model_config, policy, None).await {
                    Ok(()) => {
                        self.registry.mark_ready(model_name, version)?;
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
                        self.registry
                            .mark_failed(model_name, version, &reason)?;
                        // L5: workers were spawned and the queue/client/
                        // routing state registered BEFORE warmup ran — tear
                        // that runtime state down through the single shared
                        // teardown path (§6.5). The registry entry stays as
                        // Failed (D33: /health must show why the version is
                        // not serving); only its runtime footprint is freed.
                        self.teardown_version_runtime(model_name, version).await;
                        crate::metrics::prometheus::set_active_workers(
                            model_name,
                            version,
                            0.0,
                        );
                        self.sync_grpc_health().await;
                        crate::metrics::prometheus::record_model_load(
                            model_name,
                            version,
                            false,
                        );
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

        info!("Model {} version {} loaded with {} workers", model_name, version, total_workers);

        // Fire ModelLoad callback
        self.callback_runner.on_model_load(&ModelLifecycleContext {
            model_name: model_name.to_string(),
            version: version.to_string(),
            device: config.devices.as_ref().and_then(|d| d.as_str().map(|s| s.to_string())),
        }).await;

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
            match timeout(
                Duration::from_secs_f32(policy.total_timeout_secs),
                inner,
            )
            .await
            {
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
            let body = tokio::fs::read(&dummy_path)
                .await
                .map_err(|e| {
                    WarmupFailure::failure(format!(
                        "read dummy input {}: {}",
                        dummy_path.display(),
                        e
                    ))
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
                self.run_warmup_unit(model_name, version, policy, unit, &payloads, timeout_opt)
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
    async fn run_warmup_unit(
        &self,
        model_name: &str,
        version: &str,
        policy: &crate::config::WarmupPolicy,
        unit: WarmupUnit,
        payloads: &[bytes::Bytes],
        timeout_opt: Option<Duration>,
    ) -> Result<(), WarmupFailure> {
        let mut attempt = 0u32;
        loop {
            match self
                .run_warmup_unit_once(model_name, version, &unit, payloads, timeout_opt)
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
        payloads: &[bytes::Bytes],
        timeout_opt: Option<Duration>,
    ) -> Result<(), WarmupFailure> {
        let uid = format!(
            "warmup_{}_{}_{}",
            model_name,
            version,
            uuid::Uuid::new_v4()
        );
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        let mut headers = std::collections::HashMap::new();
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
        match self.inference_queue.try_submit(model_name, version, item) {
            Ok(()) => {}
            Err(crate::inference_queue::QueueError::Full) => {
                return Err(WarmupFailure::failure("warmup queue full".to_string()));
            }
            Err(_) => {
                return Err(WarmupFailure::failure(
                    "warmup: inference queue not available".to_string(),
                ));
            }
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
        let ok = matches!(
            response.payload,
            Some(crate::proto::liteserver::response::Payload::Single(ref s))
                if s.status.as_ref().map(|st| st.code.as_str()).unwrap_or("Ok") != "Error"
        );
        if !ok {
            let detail = match response.payload {
                Some(crate::proto::liteserver::response::Payload::Single(ref s)) => s
                    .status
                    .as_ref()
                    .map(|st| st.message.clone())
                    .unwrap_or_default(),
                _ => "unexpected response payload".to_string(),
            };
            return Err(WarmupFailure::failure(format!(
                "warmup inference returned error: {}",
                detail
            )));
        }
        Ok(())
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
        self.callback_runner.on_model_unload(&ModelLifecycleContext {
            model_name: model_name.to_string(),
            version: version.to_string(),
            device: None,
        }).await;

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
        crate::metrics::aggregator::TIMELINE.remove(model_name, version).await;

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
                    let socket_str = proc.endpoint.strip_prefix("ipc://").unwrap_or(&proc.endpoint);
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
                let socket_str = proc.endpoint.strip_prefix("ipc://").unwrap_or(&proc.endpoint);
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
        crate::metrics::aggregator::TIMELINE.remove(model_name, version).await;
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

        let was_active = self.registry.get_active_version(model_name).as_deref() == Some(v.as_str());

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
        self.callback_runner.on_model_reload(&ModelLifecycleContext {
            model_name: model_name.to_string(),
            version: v.clone(),
            device: config.devices.as_ref().and_then(|d| d.as_str().map(|s| s.to_string())),
        }).await;

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
    use tokio::sync::mpsc;

    // ===== Reload Channel tests =====

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
            assert!(registry.get("m", Some("1")).is_none(), "version must be removed");
            // Idempotent: a second call on the deleted version is a no-op.
            wm.force_unload_version("m", "1").await.unwrap();
        });
    }

    #[test]
    fn test_reload_channel_creation() {
        // WorkerManager creates a reload channel in its constructor
        let (tx, mut rx) = mpsc::channel::<crate::inference_queue::ReloadSignal>(8);

        // Sender should be cloneable (for multiple batch collectors)
        let tx2 = tx.clone();

        // Send a signal
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            tx.send(crate::inference_queue::ReloadSignal {
                model_name: "test".to_string(),
                version: "1".to_string(),
            }).await.unwrap();

            let signal = rx.recv().await.unwrap();
            assert_eq!(signal.model_name, "test");
            assert_eq!(signal.version, "1");

            // Clone also works
            tx2.send(crate::inference_queue::ReloadSignal {
                model_name: "test2".to_string(),
                version: "1".to_string(),
            }).await.unwrap();
            let signal2 = rx.recv().await.unwrap();
            assert_eq!(signal2.model_name, "test2");
        });
    }

    #[test]
    fn test_reload_signal_dedup() {
        // Simulate the dedup logic from start_reload_listener: the key is
        // per (model, version) so two versions of the same model recycle
        // independently.
        let mut reloading = std::collections::HashSet::new();

        let a1 = model_version_key("model_a", "1");
        let a2 = model_version_key("model_a", "2");

        // First signal for model_a v1 → should process
        assert!(reloading.insert(a1.clone()));

        // Second signal for model_a v1 while reloading → should skip
        assert!(!reloading.insert(a1.clone()));

        // Same model, different version → independent, should process
        assert!(reloading.insert(a2.clone()));

        // After model_a v1 finishes reloading
        reloading.remove(&a1);

        // model_a v1 can be reloaded again
        assert!(reloading.insert(a1.clone()));
    }

    #[tokio::test]
    async fn test_reload_channel_try_send_non_blocking() {
        // try_send should not block even when channel is full
        let (tx, _rx) = mpsc::channel::<crate::inference_queue::ReloadSignal>(1);

        // First send succeeds
        assert!(tx.try_send(crate::inference_queue::ReloadSignal {
            model_name: "m1".to_string(),
            version: "1".to_string(),
        }).is_ok());

        // Second send should fail (channel full) — not block
        assert!(tx.try_send(crate::inference_queue::ReloadSignal {
            model_name: "m2".to_string(),
            version: "1".to_string(),
        }).is_err());
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
                .register("m", v, ModelConfig::default(), ModelType::LitAPI, std::path::PathBuf::new())
                .unwrap();
            registry.mark_ready("m", v).unwrap();
        }
        registry.activate_version("m", "2").unwrap();

        let wm = lru_test_manager(&registry);
        wm.enforce_max_loaded_versions("m").await.unwrap();

        assert!(registry.get("m", Some("1")).is_none(), "LRU non-active version evicted");
        assert!(registry.get("m", Some("2")).is_some(), "active version preserved");
    }

    #[tokio::test]
    async fn test_enforce_max_loaded_versions_keeps_active_when_no_candidate() {
        let registry = Arc::new(ModelRegistry::new());
        registry.set_strategy("m", &lru_strategy(Some(1))).unwrap();
        registry
            .register("m", "1", ModelConfig::default(), ModelType::LitAPI, std::path::PathBuf::new())
            .unwrap();
        registry.mark_ready("m", "1").unwrap();
        registry.activate_version("m", "1").unwrap();

        // Only the active version is loaded: limit is exceeded with a
        // warning rather than evicting active or failing the load.
        let wm = lru_test_manager(&registry);
        wm.enforce_max_loaded_versions("m").await.unwrap();
        assert!(registry.get("m", Some("1")).is_some(), "active version must never be evicted");
    }

    #[tokio::test]
    async fn test_enforce_max_loaded_versions_no_limit_is_noop() {
        let registry = Arc::new(ModelRegistry::new());
        registry.set_strategy("m", &lru_strategy(None)).unwrap();
        for v in ["1", "2", "3"] {
            registry
                .register("m", v, ModelConfig::default(), ModelType::LitAPI, std::path::PathBuf::new())
                .unwrap();
        }

        let wm = lru_test_manager(&registry);
        wm.enforce_max_loaded_versions("m").await.unwrap();
        assert_eq!(registry.list_versions("m").len(), 3, "no limit → nothing evicted");
    }

    /// Real-worker regression test: unload_model must not return before the
    /// Python worker process is actually dead. An orphaned worker's ZMQ
    /// socket auto-reconnect steals the re-bound socket on reload/restart
    /// (root cause of flaky worker log loss).
    #[cfg(unix)]
    #[tokio::test]
    async fn test_unload_model_leaves_no_orphan_worker() {
        let repo = std::env::temp_dir()
            .join(format!("lite-server-orphan-test-{}", std::process::id()));
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
            assert!(!alive, "worker pid {} orphaned: still alive after unload_model returned", pid);
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

        let repo = std::env::temp_dir()
            .join(format!("lite-server-w1-orphan-test-{}", std::process::id()));
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
            unsafe { libc::kill(pid, libc::SIGKILL); }
        }
        assert!(
            reaped,
            "W1: worker pid {pid} survived a failed load_model — spawned but \
             never inserted into the workers map, so unload cannot reap it \
             (orphan until server shutdown)"
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
        let repo = std::env::temp_dir()
            .join(format!("lite-server-b2-dup-test-{}", std::process::id()));
        for v in ["1", "2"] {
            let dir = repo.join("m").join(v);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("model.py"), "# test fixture\n").unwrap();
        }

        let registry = Arc::new(ModelRegistry::new());
        registry
            .set_strategy("m", &lru_strategy(Some(2)))
            .unwrap();
        for v in ["1", "2"] {
            registry
                .register(
                    "m", v,
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
        let repo = std::env::temp_dir().join(format!(
            "lite-server-wm-ensemble-fp-{}",
            std::process::id()
        ));
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
            let bytes = socket.recv_bytes(0).expect("fake worker: no request received");
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
            !wm.notify_file_changed("m", "1", &["/x.py".to_string()]).await,
            "no workers → nothing handled → caller must fall back to restart"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn notify_file_changed_handled_returns_true() {
        let (wm, endpoint) = fc_manager_with_worker("handled").await;
        let fake = spawn_fake_worker(endpoint, vec!["/a.py".to_string(), "/b.yaml".to_string()], b"{\"handled\":true}");
        assert!(wm
            .notify_file_changed("m", "1", &["/a.py".to_string(), "/b.yaml".to_string()])
            .await);
        fake.join().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn notify_file_changed_unhandled_returns_false() {
        let (wm, endpoint) = fc_manager_with_worker("unhandled").await;
        let fake = spawn_fake_worker(endpoint, vec!["/a.py".to_string()], b"{\"handled\":false}");
        assert!(!wm.notify_file_changed("m", "1", &["/a.py".to_string()]).await);
        fake.join().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn notify_file_changed_malformed_reply_returns_false() {
        // Old workers predating FILE_CHANGED reply "Unsupported payload type"
        // (an error SingleResponse); any non-{"handled":true} body means the
        // caller must fall back to restart.
        let (wm, endpoint) = fc_manager_with_worker("malformed").await;
        let fake = spawn_fake_worker(endpoint, vec!["/a.py".to_string()], b"Unsupported payload type");
        assert!(!wm.notify_file_changed("m", "1", &["/a.py".to_string()]).await);
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
        let repo = std::env::temp_dir()
            .join(format!("lite-server-b4-def-{}", std::process::id()));
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
        assert_eq!(config.max_queue_size, 500, "defaults applied before load_model");

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
        let repo = std::env::temp_dir()
            .join(format!("lite-server-b4-reload-{}", std::process::id()));
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
        let repo = std::env::temp_dir()
            .join(format!("lite-server-reload-disk-{}", std::process::id()));
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
        assert_eq!(registry.get("m", Some("1")).unwrap().config.max_batch_size, 1);

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
        let repo = std::env::temp_dir()
            .join(format!("lite-server-reload-badcfg-{}", std::process::id()));
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
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: [unclosed\n",
        )
        .unwrap();

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
                },
                crate::config::WarmupSample {
                    input_ref: "warmup/b.json".to_string(),
                    iterations: 1,
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
            assert_eq!(units.len(), 3, "worker {w} must receive the full sample set");
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
            }],
            ..Default::default()
        };
        let plan = build_warmup_plan(&policy, 3);
        assert_eq!(plan.len(), 7, "version scope keeps the configured total");
        let pins: Vec<_> = plan.iter().map(|u| u.worker_pin).collect();
        assert_eq!(
            pins,
            vec![Some(0), Some(1), Some(2), Some(0), Some(1), Some(2), Some(0)],
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
            }],
            ..Default::default()
        };
        let plan = build_warmup_plan(&policy, 3);
        let groups = group_units_by_pin(&plan);
        assert_eq!(groups.len(), 3, "three pins -> three groups");
        let group_pins: Vec<_> = groups
            .iter()
            .map(|g| g[0].worker_pin)
            .collect();
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
        let repo = std::env::temp_dir()
            .join(format!("lite-server-b4-b4_total-{}", std::process::id()));
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup_input.json".to_string(),
                iterations: 4,
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
        let repo = std::env::temp_dir()
            .join(format!("lite-server-b4-b4_conc-{}", std::process::id()));
        let marker = repo.join("marker.txt");
        let policy = crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup_input.json".to_string(),
                iterations: 6,
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
        let repo = std::env::temp_dir()
            .join(format!("lite-server-b4-b4_retry-{}", std::process::id()));
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
        let repo = std::env::temp_dir()
            .join(format!("lite-server-b4-b4_noretry-{}", std::process::id()));
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
            }],
            timeout_secs: 30.0,
            ..Default::default()
        };
        let (wm, _registry, repo, result) =
            warmup_e2e_load("b4_noretry", &model_py, policy).await;
        result.expect_err("no retries -> first failure fails the load (D33)");
        assert_eq!(
            std::fs::read_to_string(&counter).unwrap(),
            "1",
            "exactly one attempt without retries"
        );
        let _ = wm.unload_model("b4_noretry", Some("1")).await;
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
                },
                crate::config::WarmupSample {
                    input_ref: "warmup/b.json".to_string(),
                    iterations: 1,
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

        let repo = std::env::temp_dir()
            .join(format!("lite-server-l5-warmup-{}", std::process::id()));
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
                unsafe { libc::kill(*p, libc::SIGKILL); }
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
