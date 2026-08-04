//! Worker process management: child spawn/monitor/respawn, endpoint
//! construction, and stderr capture/classification for crash diagnostics.

use super::hooks::{execute_hook, policies_from_config};
use super::{HookTasks, WorkerManager, WorkerProcess};
use crate::error::AppError;
use crate::inference_queue::model_version_key;
use crate::registry::types::*;
use crate::worker::protocol::*;

/// Await a worker's natural exit, bounded by its `kill_timeout`; escalate to
/// the monitor's SIGKILL (via `shutdown_tx`) only when the worker is hung
/// (never read the graceful-stop message). Always waits for the reap — an
/// un-reaped worker is an orphan whose ZMQ auto-reconnect steals the
/// re-bound socket on reload/restart.
pub(super) async fn stop_worker_gracefully(
    proc: &mut super::WorkerProcess,
    model_name: &str,
    version: &str,
) {
    match proc.done_rx.take() {
        None => {
            if let Some(tx) = proc.shutdown_tx.take() {
                let _ = tx.send(());
            }
        }
        Some(rx) => {
            let rx = rx;
            tokio::pin!(rx);
            let mut escalated = false;
            loop {
                tokio::select! {
                    _ = &mut rx => break,
                    _ = tokio::time::sleep(proc.kill_timeout) => {
                        if escalated {
                            break;
                        }
                        escalated = true;
                        tracing::warn!(
                            model = %model_name, version = %version,
                            worker_id = proc.worker_id,
                            "Worker did not exit gracefully within kill_timeout; SIGKILLing"
                        );
                        if let Some(tx) = proc.shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                }
            }
        }
    }
}
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{error, info, warn};

// ===== stderr capture (crash diagnostics) =====

/// Trailing stderr lines folded into the crash message. Python tracebacks put
/// the actionable error on the last line.
const WORKER_STDERR_TAIL_LINES: usize = 5;

/// Return the last `n` lines of `text`, in order. `n == 0` or empty input
/// yields an empty string.
fn tail_lines(text: &str, n: usize) -> String {
    if n == 0 {
        return String::new();
    }
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join("\n")
}

/// Read up to `cap_bytes` bytes from `stderr` (or until EOF), bounded by
/// `deadline`, and return the last [`WORKER_STDERR_TAIL_LINES`]
/// lines. Surfaces a crashed worker's real traceback in the error message
/// instead of a bare "worker exited before ready". Generic over the reader so
/// it is unit-testable without spawning a real process.
async fn drain_worker_stderr_bounded<R>(stderr: R, deadline: Duration, cap_bytes: usize) -> String
where
    R: AsyncRead + Unpin,
{
    let mut reader = stderr;
    let mut cap: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut buf = [0u8; 4096];

    let read_loop = async {
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(k) => {
                    let remaining = cap_bytes.saturating_sub(cap.len());
                    if remaining == 0 {
                        break;
                    }
                    let take = k.min(remaining);
                    cap.extend_from_slice(&buf[..take]);
                    if cap.len() >= cap_bytes {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    };

    let _ = timeout(deadline, read_loop).await;
    tail_lines(&String::from_utf8_lossy(&cap), WORKER_STDERR_TAIL_LINES)
}

/// Production wrapper: drains worker stderr with the configured deadline and
/// capture bound (`tunables:` in server.yaml).
pub(super) async fn drain_worker_stderr<R>(stderr: R, tunables: &crate::config::ServerTunables) -> String
where
    R: AsyncRead + Unpin,
{
    drain_worker_stderr_bounded(
        stderr,
        Duration::from_secs_f32(tunables.worker_stderr_drain_secs),
        tunables.worker_stderr_tail_bytes,
    )
    .await
}

/// Classify a stderr line from the Python worker into a tracing level.
pub(super) fn classify_stderr_line(line: &str) -> tracing::Level {
    let trimmed = line.trim();
    if trimmed.starts_with("[ERROR]") {
        tracing::Level::ERROR
    } else if trimmed.starts_with("[WARN]") {
        tracing::Level::WARN
    } else if trimmed.starts_with("[INFO]") {
        tracing::Level::INFO
    } else {
        tracing::Level::DEBUG
    }
}

/// Strip the [LEVEL] prefix from a stderr line, returning the message content.
pub(super) fn strip_level_prefix(line: &str) -> &str {
    let trimmed = line.trim();
    trimmed
        .strip_prefix("[ERROR]")
        .or_else(|| trimmed.strip_prefix("[WARN]"))
        .or_else(|| trimmed.strip_prefix("[INFO]"))
        .or_else(|| trimmed.strip_prefix("[DEBUG]"))
        .unwrap_or(trimmed)
        .trim()
}

/// Emit a single stderr line at the given tracing level.
pub(super) fn emit_stderr_line(level: tracing::Level, msg: &str, worker_id: usize, model: &str, version: &str) {
    match level {
        tracing::Level::ERROR => {
            tracing::error!(worker_id = worker_id, model = %model, version = %version, "{}", msg);
        }
        tracing::Level::WARN => {
            tracing::warn!(worker_id = worker_id, model = %model, version = %version, "{}", msg);
        }
        tracing::Level::INFO => {
            tracing::info!(worker_id = worker_id, model = %model, version = %version, "{}", msg);
        }
        _ => {
            tracing::debug!(worker_id = worker_id, model = %model, version = %version, "{}", msg);
        }
    }
}

// ===== spawn / monitor helpers =====

/// Spawn a background task that monitors a worker child process.
/// - If the process exits on its own (crash, OOM kill), logs the event and runs cleanup.
/// - If a shutdown signal is sent via `shutdown_rx`, kills the process and runs cleanup.
/// - Fires lifecycle hooks (on_exit / on_error) if configured.
///
/// Health probing lives in the inference queue's health checker (single probe
/// loop with eject → kill escalation); this task owns only process lifecycle.
///
/// Returns a receiver that resolves once the child has been reaped (natural
/// exit or kill confirmed). Callers that need orphan-free shutdown must await
/// it; otherwise the process may outlive the server and steal the re-bound
/// ZMQ socket after a restart.
pub(super) fn spawn_worker_monitor(
    mut child: Child,
    model_name: &str,
    version: &str,
    worker_id: u32,
    mut shutdown_rx: oneshot::Receiver<()>,
    on_exit: impl FnOnce() + Send + 'static,
    hooks: Option<Arc<crate::config::WorkerHooksConfig>>,
    hook_tasks: HookTasks,
    draining: Arc<AtomicBool>,
) -> oneshot::Receiver<()> {
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let model = model_name.to_string();
    let ver = version.to_string();
    let hook_vars: Vec<(String, String)> = vec![
        ("$MODEL".to_string(), model_name.to_string()),
        ("$VERSION".to_string(), version.to_string()),
        ("$WORKER_ID".to_string(), worker_id.to_string()),
    ];
    tokio::spawn(async move {
        tokio::select! {
            result = child.wait() => {
                match result {
                    Ok(status) => {
                        if status.success() {
                            info!(
                                model = %model, version = %ver, worker_id,
                                "Worker process exited cleanly"
                            );
                            if let Some(ref h) = hooks {
                                execute_hook("exit", h, hook_vars.clone(), &hook_tasks);
                            }
                        } else {
                            let exit_code = status.code().unwrap_or(-1);
                            // B1: during draining/shutdown, a worker exit (including via
                            // signal) is expected — the server is shutting workers down.
                            // Log at WARN instead of ERROR and skip the error hook to
                            // avoid spurious alerts. Outside of draining this is a real
                            // worker crash and logged as ERROR.
                            if draining.load(Ordering::Relaxed) {
                                warn!(
                                    model = %model, version = %ver, worker_id,
                                    exit_code,
                                    "Worker process exited during drain (not unexpected)"
                                );
                            } else {
                                error!(
                                    model = %model, version = %ver, worker_id,
                                    exit_code,
                                    "Worker process exited unexpectedly"
                                );
                                if let Some(ref h) = hooks {
                                    let mut vars = hook_vars.clone();
                                    vars.push(("$EXIT_CODE".to_string(), exit_code.to_string()));
                                    vars.push(("$REASON".to_string(), "crash".to_string()));
                                    execute_hook("error", h, vars, &hook_tasks);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            model = %model, version = %ver, worker_id,
                            error = %e,
                            "Failed to wait on worker process"
                        );
                    }
                }
            }
            _ = &mut shutdown_rx => {
                info!(
                    model = %model, version = %ver, worker_id,
                    "Shutting down worker process"
                );
                let _ = child.kill().await;
            }
        }
        on_exit();

        // Signal that the child has been reaped (kill confirmed or natural
        // exit observed). Receivers that already dropped are fine.
        let _ = done_tx.send(());
    });
    done_rx
}

/// Build the Command for spawning a Python worker process, wiring PYTHONPATH
/// to the bundled lite_server module.
///
/// kill_on_drop(true) is the orphan safety net for paths that bypass graceful
/// shutdown (panic, runtime drop): the child is killed when the Child handle
/// is dropped instead of outliving the server and stealing the re-bound
/// ZMQ socket.
pub(super) fn new_worker_command(python_module_dir: &str) -> Command {
    let mut cmd = Command::new(crate::python::resolve_python_interpreter());
    if !python_module_dir.is_empty() {
        let current_pythonpath = std::env::var("PYTHONPATH").unwrap_or_default();
        let new_pythonpath = if current_pythonpath.is_empty() {
            python_module_dir.to_string()
        } else {
            #[cfg(windows)]
            { format!("{};{}", current_pythonpath, python_module_dir) }
            #[cfg(not(windows))]
            { format!("{}:{}", current_pythonpath, python_module_dir) }
        };
        cmd.env("PYTHONPATH", new_pythonpath);
    }
    #[cfg(unix)]
    {
        // Detach this worker from the terminal's process group so Ctrl+C (SIGINT)
        // only reaches the server — not the workers. Without this the workers
        // catch KeyboardInterrupt and start their own teardown, racing with the
        // server's graceful shutdown and producing spurious errors:
        //   • "exited unexpectedly" (worker already killed by signal)
        //   • "ZMQ raw send error" (socket closed before the server's stop msg).
        // SIGKILL (kill_on_drop) works across process groups, so orphan protection
        // is intact. tokio::process::Command exposes process_group() directly (no
        // need for std::os::unix::process::CommandExt).
        cmd.process_group(0);
    }
    cmd.kill_on_drop(true);
    cmd
}

/// Build a platform-appropriate ZMQ endpoint for a worker.
/// - Unix: IPC socket in the system temp directory
/// - Windows: TCP on localhost (IPC not supported)
/// Per-process-unique worker endpoint: `{model}_{version}_{worker_id}_{pid}`,
/// where `pid` is the SERVER process. The endpoint is therefore stable across
/// a worker's kill+respawn within one server run, so `respawn_worker` reuses
/// the already-bound PAIR socket (the replacement worker connects to the same
/// path) and never re-binds. The pid component only prevents cross-run
/// collisions (parallel server processes / orphaned `.sock` files); within a
/// run no second `bind` ever occurs, so the EEXIST-on-rebind hazard does not
/// apply.
pub(super) fn worker_endpoint(model_name: &str, version: &str, worker_id: usize) -> String {
    #[cfg(unix)]
    {
        let sock_path = std::env::temp_dir()
            .join("lite-server")
            .join(format!(
                "{}_{}_{}_{}.sock",
                model_name,
                version,
                worker_id,
                std::process::id()
            ));
        format!("ipc://{}", sock_path.display())
    }
    #[cfg(windows)]
    {
        let key = format!("{}_{}_{}_{}", model_name, version, worker_id, std::process::id());
        let port = crate::transport::derive_port_from_path(&key);
        format!("tcp://127.0.0.1:{}", port)
    }
}

impl WorkerManager {
    /// Start the respawn listener. Must be called once after construction.
    pub async fn start_respawn_listener(self: &Arc<Self>) {
        let mut rx_guard = self.respawn_rx.lock().await;
        if let Some(mut rx) = rx_guard.take() {
            let wm = Arc::downgrade(self);
            tokio::spawn(async move {
                while let Some(signal) = rx.recv().await {
                    if let Some(wm) = wm.upgrade() {
                        info!(
                            model = %signal.model_name, version = %signal.version,
                            worker_id = signal.worker_id, reason = signal.reason,
                            "Respawning worker"
                        );
                        if let Err(e) = wm.respawn_worker(
                            &signal.model_name, &signal.version, signal.worker_id, signal.reason,
                        ).await {
                            error!(
                                model = %signal.model_name, version = %signal.version,
                                worker_id = signal.worker_id,
                                "Worker respawn failed: {}", e
                            );
                        }
                    } else {
                        break; // WorkerManager dropped
                    }
                }
            });
        }
    }

    /// Respawn a single worker after it was killed (health-check kill escalation).
    /// Terminates the old worker first, then spawns a replacement and updates
    /// all registries.
    async fn respawn_worker(
        &self,
        model_name: &str,
        version: &str,
        worker_id: u32,
        reason: &'static str,
    ) -> Result<(), AppError> {
        let key = model_version_key(model_name, version);

        // Get config from registry
        let model_version = self.registry
            .get(model_name, Some(version))
            .ok_or_else(|| AppError::ModelNotFound(format!("{} v{}", model_name, version)))?;
        let model_config = model_version.config;

        let model_dir = crate::validation::resolve_model_dir(
            &self.repo_path, model_name, version,
        )?;
        let model_py = model_dir.join("model.py");
        let config_yaml = model_dir.join("config.yaml");

        // Terminate the old (possibly hung) worker BEFORE respawning: signal
        // its monitor to kill it, then await the reap so no orphaned process
        // survives to leak memory or steal the re-bound socket (mirrors the
        // unload sequence).
        let old_proc = {
            let mut workers = self.workers.write().await;
            workers.get_mut(&key).and_then(|procs| {
                procs.iter().position(|w| w.worker_id == worker_id)
                    .map(|pos| procs.remove(pos))
            })
        };
        if let Some(mut proc) = old_proc {
            // NO graceful stop here — kill escalation only. This path targets
            // a worker that failed health probes (likely hung), so teardown
            // would hang the respawn anyway; and a stop message sent now would
            // be replayed to the replacement worker when it reconnects to the
            // reused bound socket (ZMQ PAIR keeps queued messages across a
            // dead peer's reconnect), instantly killing the replacement in a
            // kill→respawn→kill loop. The graceful stop-then-wait sequence
            // (stop_worker_gracefully) belongs to the unload path only, where
            // the client socket is dropped and its queue dies with it.
            if let Some(tx) = proc.shutdown_tx.take() {
                let _ = tx.send(());
            }
            if let Some(done_rx) = proc.done_rx {
                if timeout(proc.kill_timeout, done_rx).await.is_err() {
                    error!(
                        model = %model_name, version = %version, worker_id,
                        "Timed out waiting for old worker to die; respawning anyway"
                    );
                }
            }
        }

        // The ZMQ client is REUSED across respawn: the endpoint embeds the
        // server pid (see `worker_endpoint`), so it is stable across this
        // worker's kill+respawn and the already-bound PAIR socket accepts the
        // replacement worker's reconnect. Do NOT create a new client or re-bind
        // (a second `bind` hits EEXIST), and do NOT unlink the `.sock` here —
        // the bound socket still owns it, and removing it breaks the new
        // worker's connect. The `.sock` is cleaned only at genuine teardown
        // (unload_version / load pre-bind), where the client is actually dropped.
        let endpoint = worker_endpoint(model_name, version, worker_id as usize);

        // Spawn new worker
        //
        // During respawn the version drops to Degraded (NOT Loading): with
        // multiple workers the surviving slots keep serving, and Degraded still
        // counts as serving (`routing_pick` / `has_serving`), so a single-slot
        // respawn doesn't take the whole version out of rotation for the startup
        // window. `Loading` would make `has_serving` false and stall all traffic.
        // A successful handshake marks it Ready again; a startup failure leaves
        // it Degraded (runtime worker loss, not a load failure — Failed is
        // load-phase only).
        self.registry
            .set_status(model_name, version, VersionStatus::Degraded)?;
        let accelerator = model_config.accelerator.as_deref().unwrap_or("cpu");
        let devices = match &model_config.devices {
            Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(1) as usize,
            Some(serde_json::Value::String(s)) if s == "auto" => 1,
            _ => 1,
        };
        let device = format!("{}:{}", accelerator, worker_id as usize % devices);

        let mut cmd = new_worker_command(&Self::find_python_module_path().unwrap_or_default());

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
            .map_err(|e| AppError::Python(format!("failed to spawn worker: {}", e)))
            .inspect_err(|_| self.mark_degraded(model_name, version))?;

        let stdout = child.stdout.take()
            .ok_or_else(|| AppError::Internal("worker stdout not piped".to_string()))
            .inspect_err(|_| self.mark_degraded(model_name, version))?;
        let stderr = child.stderr.take()
            .ok_or_else(|| AppError::Internal("worker stderr not piped".to_string()))
            .inspect_err(|_| self.mark_degraded(model_name, version))?;

        // Wait for "ready" signal
        let mut reader = BufReader::new(stdout);
        let mut ready_line = String::new();
        let n = timeout(Duration::from_secs_f32(model_config.startup_timeout), reader.read_line(&mut ready_line))
            .await
            .map_err(|_| AppError::InferenceTimeout("worker startup timeout".to_string()))
            .and_then(|r| r.map_err(AppError::Io))
            .inspect_err(|_| self.mark_degraded(model_name, version))?;
        if n == 0 {
            self.mark_degraded(model_name, version);
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
            .inspect_err(|_| self.mark_degraded(model_name, version))?;

        if startup.status != "ready" {
            self.mark_degraded(model_name, version);
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
                crate::metrics::prometheus::register_custom_metrics(&spec_refs);
            }
        }

        // Store per-model policies declared in config.yaml
        self.registry
            .set_policies(model_name, version, policies_from_config(&model_config));

        // Register custom @route declarations (phase 2)
        self.upsert_routes(model_name, version, startup.custom_routes).await;

        info!("Worker {} for {} v{} respawned (pid={:?})", worker_id, model_name, version, child.id());

        // Fire on_ready lifecycle hook
        execute_hook("ready", &model_config.hooks, vec![
            ("$MODEL".to_string(), model_name.to_string()),
            ("$VERSION".to_string(), version.to_string()),
            ("$WORKER_ID".to_string(), worker_id.to_string()),
        ], &self.hook_tasks);

        // Drain stdout
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
                    Ok(0) => break,
                    Ok(_) => {
                        while buf.last() == Some(&b'\n') || buf.last() == Some(&b'\r') {
                            buf.pop();
                        }
                        let line = String::from_utf8_lossy(&buf);
                        let level = classify_stderr_line(line.trim());
                        let msg = strip_level_prefix(line.trim());
                        emit_stderr_line(level, msg, worker_id_clone as usize, &model_name_clone, &version_clone);
                    }
                    Err(e) => {
                        tracing::error!(worker_id = worker_id_clone, "Worker stderr read error: {}", e);
                        break;
                    }
                }
            }
        });

        let pid = child.id();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // Spawn monitor for the new worker. The ZMQ client is reused (not
        // recreated) — the bound PAIR socket outlives this worker process and
        // accepts the replacement's reconnect — so there is no `.sock` to clean
        // on exit. `on_exit` stays a parameter: tests use it to observe that the
        // monitor ran.
        let hooks_arc = Arc::new(model_config.hooks.clone());
        let done_rx = spawn_worker_monitor(
            child, model_name, version, worker_id, shutdown_rx,
            || {},
            Some(hooks_arc),
            self.hook_tasks.clone(),
            self.draining.clone(),
        );

        // Update registry worker info
        let new_info = WorkerInfo {
            worker_id,
            device,
            endpoint: endpoint.clone(),
            pid,
            status: WorkerStatus::Ready,
            capacity: None,
        };
        self.registry.replace_worker(model_name, version, worker_id, new_info)?;

        // Update WorkerProcess entry
        {
            let mut workers = self.workers.write().await;
            if let Some(procs) = workers.get_mut(&key) {
                procs.push(WorkerProcess {
                    worker_id,
                    endpoint,
                    shutdown_tx: Some(shutdown_tx),
                    done_rx: Some(done_rx),
                    kill_timeout: Duration::from_secs_f32(model_config.worker_kill_timeout),
                });
            }
        }

        // Replacement worker is up: Loading → Ready (loaded_at preserved).
        self.registry.mark_ready(model_name, version)?;
        self.sync_grpc_health().await;

        // Clear the slot's ejection/error state so routing to the replacement
        // resumes immediately instead of waiting out the ejection timeout.
        {
            let outliers = self.outlier_states.read().await;
            if let Some(outlier) = outliers.get(&key) {
                outlier.reset(worker_id as usize);
            }
        }

        // Record metric
        crate::metrics::prometheus::WORKER_RESPAWNS_TOTAL
            .with_label_values(&[model_name, version, reason])
            .inc();

        Ok(())
    }

    pub(super) fn find_python_module_path() -> Option<String> {
        // 1. Check compile-time manifest dir (development / cargo run)
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let dev_path = PathBuf::from(manifest_dir).join("python");
        if dev_path.exists() {
            return Some(dev_path.to_string_lossy().to_string());
        }

        // 2. Check relative to executable
        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(exe_dir) = exe_path.parent() {
                let exe_python = exe_dir.join("python");
                if exe_python.exists() {
                    return Some(exe_python.to_string_lossy().to_string());
                }
                // For target/debug/lite-server, check ../../python
                if let Ok(project_python) = exe_dir.join("../../python").canonicalize() {
                    if project_python.exists() {
                        return Some(project_python.to_string_lossy().to_string());
                    }
                }
            }
        }

        // 3. Check current working directory
        if let Ok(cwd) = std::env::current_dir() {
            let cwd_python = cwd.join("python");
            if cwd_python.exists() {
                return Some(cwd_python.to_string_lossy().to_string());
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Parallel server processes must never collide on worker IPC paths: the
    /// endpoint embeds the server pid, and stays stable within one process
    /// across a worker's kill+respawn (the bound PAIR socket is reused, not
    /// re-bound).
    #[test]
    fn worker_endpoint_is_pid_scoped_and_stable() {
        let ep = worker_endpoint("m", "1", 0);
        assert!(
            ep.contains(&std::process::id().to_string()),
            "endpoint {ep} must embed the pid for cross-process uniqueness"
        );
        assert_eq!(ep, worker_endpoint("m", "1", 0), "same process → stable path");
        assert_ne!(ep, worker_endpoint("m", "1", 1), "worker_id still distinguishes");
    }

    /// When a child process exits unexpectedly, the monitor task must detect it
    /// and invoke the cleanup callback.
    #[tokio::test]
    async fn test_worker_monitor_detects_exit() {
        use std::sync::atomic::{AtomicBool, Ordering};

        // Spawn a process that exits immediately
        #[cfg(unix)]
        let child = tokio::process::Command::new("false")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        #[cfg(windows)]
        let child = tokio::process::Command::new("cmd")
            .args(["/c", "exit", "1"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let cleaned_up = Arc::new(AtomicBool::new(false));
        let cleaned_up_clone = cleaned_up.clone();

        let (_shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        spawn_worker_monitor(
            child, "test_model", "1", 0, shutdown_rx,
            move || { cleaned_up_clone.store(true, Ordering::SeqCst); },
            None,
            Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            Arc::new(AtomicBool::new(false)),
        );

        // Wait for the monitor to detect the exit and run cleanup
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !cleaned_up.load(Ordering::SeqCst) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(cleaned_up.load(Ordering::SeqCst), "monitor should have triggered cleanup");
    }

    /// B1: when draining is set, a worker exit (even non-zero) must NOT trigger
    /// the error hook. During normal operation (draining=false), the error hook
    /// fires on unexpected exit.
    #[tokio::test]
    async fn test_worker_monitor_draining_suppresses_error_hook() {
        // Spawn a process that exits with code 1 (simulates crash / signal kill)
        #[cfg(unix)]
        let child = tokio::process::Command::new("false")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        #[cfg(windows)]
        let child = tokio::process::Command::new("cmd")
            .args(["/c", "exit", "1"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let cleaned_up = Arc::new(AtomicBool::new(false));
        let cleaned_up_clone = cleaned_up.clone();

        let (_shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let hook_tasks = Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new()));
        let hook_tasks_clone = hook_tasks.clone();

        // Configure an on_error hook — it should NOT fire when draining=true
        let hooks = crate::config::WorkerHooksConfig {
            on_error: Some("echo 'should-not-run'".to_string()),
            ..Default::default()
        };

        let done_rx = spawn_worker_monitor(
            child, "test_model", "1", 0, shutdown_rx,
            move || { cleaned_up_clone.store(true, Ordering::SeqCst); },
            Some(Arc::new(hooks)),
            hook_tasks_clone,
            Arc::new(AtomicBool::new(true)), // draining=true
        );

        // Await the monitor's completion signal
        timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("monitor should signal completion even during drain")
            .expect("completion channel should not be dropped");

        assert!(cleaned_up.load(Ordering::SeqCst), "on_exit callback must fire during drain");

        // The error hook task should NOT have been spawned (draining suppresses it)
        let tasks = hook_tasks.lock().unwrap();
        assert!(tasks.is_empty(),
            "error hook should NOT fire when draining=true (found {} tasks)", tasks.len());
    }

    // ===== Respawn tests =====

    #[tokio::test]
    async fn test_respawn_channel_creation() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::inference_queue::RespawnSignal>(8);
        tx.send(crate::inference_queue::RespawnSignal {
            model_name: "m".to_string(),
            version: "1".to_string(),
            worker_id: 0,
            reason: "health_check",
        }).await.unwrap();
        let sig = rx.recv().await.unwrap();
        assert_eq!(sig.model_name, "m");
        assert_eq!(sig.worker_id, 0);
        assert_eq!(sig.reason, "health_check");
    }

    #[tokio::test]
    async fn test_monitor_exits_on_shutdown() {
        let mut cmd = Command::new("python");
        cmd.arg("-c").arg("import time; time.sleep(60)");
        let child = cmd.stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let done = Arc::new(AtomicBool::new(false));
        let done_c = done.clone();

        spawn_worker_monitor(
            child, "test", "1", 0, shutdown_rx,
            move || { done_c.store(true, Ordering::SeqCst); },
            None,
            Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            Arc::new(AtomicBool::new(false)),
        );

        // Send shutdown
        shutdown_tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(done.load(Ordering::SeqCst), "monitor should exit on shutdown signal");
    }

    // ===== Orphan prevention: kill completion signal =====

    /// Regression: shutdown/unload must not return before the worker process
    /// is actually killed+reaped. Otherwise the child is orphaned and its ZMQ
    /// socket auto-reconnect steals the re-bound socket after a restart.
    #[tokio::test]
    async fn test_monitor_signals_completion_after_shutdown_kill() {
        let mut cmd = Command::new("python");
        cmd.arg("-c").arg("import time; time.sleep(60)");
        let child = cmd.stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
        #[cfg(unix)]
        let pid = child.id().unwrap() as i32;

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let done_rx = spawn_worker_monitor(
            child, "test", "1", 0, shutdown_rx,
            || {},
            None,
            Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            Arc::new(AtomicBool::new(false)),
        );

        shutdown_tx.send(()).unwrap();

        // Completion must arrive promptly — it may only be sent after
        // kill().await has reaped the child.
        timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("monitor should signal completion promptly after kill")
            .expect("completion channel should not be dropped without signal");

        #[cfg(unix)]
        {
            let alive = unsafe { libc::kill(pid, 0) } == 0;
            assert!(!alive, "worker process {} should be dead once completion fires", pid);
        }
    }

    /// Natural exit must also fire the completion signal (callers treat both
    /// paths uniformly).
    #[tokio::test]
    async fn test_monitor_signals_completion_on_natural_exit() {
        #[cfg(unix)]
        let child = tokio::process::Command::new("false")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        #[cfg(windows)]
        let child = tokio::process::Command::new("cmd")
            .args(["/c", "exit", "1"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let (_shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let done_rx = spawn_worker_monitor(
            child, "test", "1", 0, shutdown_rx,
            || {},
            None,
            Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            Arc::new(AtomicBool::new(false)),
        );

        timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("monitor should signal completion after natural exit")
            .expect("completion channel should not be dropped without signal");
    }

    /// Guard: workers must be spawned with kill_on_drop so a server panic or
    /// runtime drop cannot orphan them — the Child handle is the last resort
    /// when graceful shutdown never runs.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_worker_command_kills_child_on_drop() {
        let child = new_worker_command("")
            .arg("-c")
            .arg("import time; time.sleep(60)")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as i32;

        drop(child);

        // kill_on_drop sends SIGKILL on drop and tokio reaps in the
        // background; poll briefly for the pid to disappear.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut alive = true;
        while std::time::Instant::now() < deadline {
            if unsafe { libc::kill(pid, 0) } != 0 {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(!alive, "dropped worker process {} should be killed via kill_on_drop", pid);
    }

    /// Worker processes MUST be in their own process group so terminal SIGINT
    /// (Ctrl+C) only reaches the server, not the workers. Without this, SIGINT
    /// kills workers before the server can run graceful shutdown, producing
    /// spurious "exited unexpectedly" errors and ZMQ send failures.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_worker_command_uses_own_process_group() {
        let child = new_worker_command("")
            .arg("-c")
            .arg("import time; time.sleep(10)")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let child_pid = child.id().unwrap() as i32;
        let server_pgid = unsafe { libc::getpgid(0) };
        let child_pgid = unsafe { libc::getpgid(child_pid) };

        // The child must be in its own process group (PGID == its own PID when
        // spawned with process_group(0)), different from the server's PGID.
        assert_eq!(child_pgid, child_pid,
            "worker should be process group leader (PGID == PID)");
        assert_ne!(child_pgid, server_pgid,
            "worker process group {} must differ from server process group {}",
            child_pgid, server_pgid);

        // Clean up
        unsafe { libc::kill(child_pid, libc::SIGKILL) };
    }

    // ===== P0: worker startup stderr drain (diagnostics on early exit) =====
    // When a worker exits before sending "ready" (n==0 on the stdout handshake),
    // its stderr — holding the real Python traceback — used to be dropped.
    // These cover the pure `tail_lines` and async `drain_worker_stderr` helpers
    // that surface the last few stderr lines in the crash message.

    #[test]
    fn tail_lines_returns_last_n_lines() {
        let text = "line0\nline1\nline2\nline3\nline4\nline5\nline6";
        assert_eq!(tail_lines(text, 3), "line4\nline5\nline6");
    }

    #[test]
    fn tail_lines_returns_all_when_fewer_than_n() {
        assert_eq!(tail_lines("only\none", 5), "only\none");
    }

    #[test]
    fn tail_lines_empty_input_returns_empty() {
        assert_eq!(tail_lines("", 5), "");
    }

    #[test]
    fn tail_lines_zero_n_returns_empty() {
        assert_eq!(tail_lines("a\nb\nc", 0), "");
    }

    #[test]
    fn tail_lines_keeps_real_traceback_error_line() {
        // A realistic traceback: the actionable line (ModuleNotFoundError) is
        // the LAST line — it must survive the tail truncation.
        let tb = "Traceback (most recent call last):\n\
  File \"model.py\", line 10, in <module>\n    import torch\n\
ModuleNotFoundError: No module named 'torch'";
        let tail = tail_lines(tb, 5);
        assert!(tail.contains("ModuleNotFoundError: No module named 'torch'"));
    }

    #[tokio::test]
    async fn drain_worker_stderr_captures_until_eof_then_tails() {
        // 7 lines; drain reads all (EOF) and returns the last 5.
        let stderr: &[u8] = b"l0\nl1\nl2\nl3\nl4\nl5\nl6\n";
        let got = drain_worker_stderr(stderr, &Default::default()).await;
        assert_eq!(got, "l2\nl3\nl4\nl5\nl6");
    }

    #[tokio::test]
    async fn drain_worker_stderr_empty_returns_empty() {
        let got = drain_worker_stderr(&b""[..], &Default::default()).await;
        assert_eq!(got, "");
    }

    #[tokio::test]
    async fn drain_worker_stderr_caps_at_64kb_before_eof() {
        // 64KB of filler followed by a marker line that only exists past the
        // cap. If the drain respects the 64KB cap it stops reading BEFORE the
        // marker; if it ignores the cap and reads to EOF, the marker leaks in.
        let mut bytes: Vec<u8> = Vec::with_capacity(64 * 1024 + 32);
        while bytes.len() < 64 * 1024 {
            bytes.extend_from_slice(b"x\n");
        }
        bytes.extend_from_slice(b"AFTER_CAP_MARKER\n");
        assert!(bytes.len() > 64 * 1024);

        let got = drain_worker_stderr(&bytes[..], &Default::default()).await;
        assert!(
            !got.contains("AFTER_CAP_MARKER"),
            "drain read past the 64KB cap: {got:?}"
        );
    }

    #[tokio::test]
    async fn drain_worker_stderr_does_not_hang_on_open_pipe() {
        // A pipe that never writes and never closes: the drain must give up at
        // the deadline and return whatever it captured (empty), not hang.
        let (_tx, rx) = tokio::io::duplex(64);
        // keep _tx alive so the pipe never sees EOF
        let got =
            drain_worker_stderr_bounded(rx, Duration::from_millis(50), 64 * 1024).await;
        assert_eq!(got, "");
        let _ = _tx;
    }

    // ===== Stderr line classification =====

    #[test]
    fn test_classify_stderr_line_info_not_dropped() {
        // BUG: [INFO] fell through to DEBUG, making Python info logs invisible
        // at the default Rust tracing level ("info").
        assert_eq!(
            classify_stderr_line("[INFO] hello world"),
            tracing::Level::INFO,
            "BUG: [INFO] was classified as DEBUG, making info logs invisible"
        );
        assert_eq!(classify_stderr_line("[ERROR] boom"), tracing::Level::ERROR);
        assert_eq!(classify_stderr_line("[WARN] hmm"), tracing::Level::WARN);
        assert_eq!(classify_stderr_line("plain text"), tracing::Level::DEBUG);
        assert_eq!(classify_stderr_line("[DEBUG] debug stuff"), tracing::Level::DEBUG);
        // Traceback lines (indented) should be DEBUG so they don't spam unless
        // the user wants full verbosity.
        assert_eq!(classify_stderr_line("  File \"x.py\", line 1"), tracing::Level::DEBUG);
        assert_eq!(classify_stderr_line("Traceback (most recent call last):"), tracing::Level::DEBUG);
    }

    #[test]
    fn test_strip_level_prefix_all_levels() {
        assert_eq!(strip_level_prefix("[ERROR] boom"), "boom");
        assert_eq!(strip_level_prefix("[WARN] hmm"), "hmm");
        assert_eq!(strip_level_prefix("[INFO] ok"), "ok");
        assert_eq!(strip_level_prefix("[DEBUG] dbg"), "dbg");
        assert_eq!(strip_level_prefix("plain text"), "plain text");
        // Indented [ERROR] is stripped because trim() is applied first
        assert_eq!(strip_level_prefix("  [ERROR] indented"), "indented");
    }
}
