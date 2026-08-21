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
/// re-bound socket on reload/restart. Returns true when SIGKILL escalation
/// fired (the worker never consumed the stop message).
pub(super) async fn stop_worker_gracefully(
    proc: &mut super::WorkerProcess,
    model_name: &str,
    version: &str,
) -> bool {
    match proc.done_rx.take() {
        None => {
            if let Some(tx) = proc.shutdown_tx.take() {
                let _ = tx.send(());
            }
            false
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
            escalated
        }
    }
}
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Has the worker's reap signal already fired (the process exited on its
/// own)? Resolved either by the monitor's send or by its end of the channel
/// dropping. The graceful recycle path uses this to avoid queueing a stop
/// for a dead peer — ZMQ PAIR replays it to the replacement on reconnect
/// (H2). On a resolved channel the receiver is TAKEN OUT: a completed
/// oneshot panics when polled again, so `stop_worker_gracefully` must find
/// `done_rx` already gone.
fn worker_already_reaped(proc: &mut WorkerProcess) -> bool {
    let reaped = proc
        .done_rx
        .as_mut()
        .map(|rx| !matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)))
        .unwrap_or(false);
    if reaped {
        proc.done_rx.take();
    }
    reaped
}
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
pub(super) async fn drain_worker_stderr<R>(
    stderr: R,
    tunables: &crate::config::ServerTunables,
) -> String
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
pub(super) fn emit_stderr_line(
    level: tracing::Level,
    msg: &str,
    worker_id: usize,
    model: &str,
    version: &str,
) {
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

/// How a monitored worker process terminated. Passed to the `on_exit`
/// callback of [`spawn_worker_monitor`]: `Clean` skips in-flight cleanup
/// (the unload path's client drop handles it), while `Crash`/`Killed` must
/// fail in-flight requests fast — ZMQ PAIR has no peer-disconnect event,
/// so waiters would otherwise hang until their caller-side timeouts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerExitKind {
    /// Zero exit status (graceful stop / intentional exit).
    Clean,
    /// Non-zero exit, or the wait itself failed. An exit during drain also
    /// maps here — its in-flight requests are dead either way.
    Crash,
    /// Supervisor-initiated kill via the shutdown channel (health-check
    /// kill escalation, or an unload's kill grace expiring).
    Killed,
}

/// Grace window after a respawn during which a Clean exit is treated as a
/// crash (H2 stale-stop replay): the replacement consumed a stop message
/// queued for its predecessor on the reused PAIR socket and exited 0.
/// Legitimate Clean exits never happen spontaneously — a worker exits 0 only
/// on a stop request, and by then its monitor is torn down (unload) or the
/// slot is mid-recycle (whose own respawn re-resets the flags it sets here).
/// 30s covers connect → consume → teardown of a replayed stop with margin.
const RESPAWN_CLEAN_GRACE: Duration = Duration::from_secs(30);

/// The single crash-bookkeeping closure for both monitor sites (initial
/// spawn + respawn): a non-clean exit fails in-flight requests fast, marks
/// the slot dead so routing stops sending new work to a process that can
/// never reply, and flips the registry entry to Stopped. Clean exits are
/// ignored (graceful unload has its own teardown path). The dead flag
/// outlives ejection timeouts; only a respawned replacement (`reset`)
/// revives the slot.
pub(crate) fn crash_exit_handler(
    fail_client: Option<Arc<crate::transport::zmq::WorkerZmqClient>>,
    outlier: Option<Arc<crate::inference_queue::OutlierState>>,
    registry: Arc<crate::registry::ModelRegistry>,
    model_name: String,
    version: String,
    worker_id: u32,
    worker_pid: Option<u32>,
) -> impl FnOnce(WorkerExitKind) + Send + 'static {
    crash_exit_handler_with_grace(
        fail_client,
        outlier,
        registry,
        model_name,
        version,
        worker_id,
        worker_pid,
        None,
    )
}

/// `crash_exit_handler` with an optional Clean-exit grace window: a Clean
/// exit observed within `clean_grace` of handler creation is reclassified as
/// a crash (fail_all + mark_dead + Stopped — deliberately NO auto-respawn,
/// bounding kill→respawn loops; health-check kill escalation, when enabled,
/// is the self-heal path). Used for respawned replacements, where a Clean
/// exit moments after startup means the worker consumed a stale stop
/// replayed on the reused PAIR socket (H2).
#[allow(clippy::too_many_arguments)]
pub(crate) fn crash_exit_handler_with_grace(
    fail_client: Option<Arc<crate::transport::zmq::WorkerZmqClient>>,
    outlier: Option<Arc<crate::inference_queue::OutlierState>>,
    registry: Arc<crate::registry::ModelRegistry>,
    model_name: String,
    version: String,
    worker_id: u32,
    worker_pid: Option<u32>,
    clean_grace: Option<Duration>,
) -> impl FnOnce(WorkerExitKind) + Send + 'static {
    let spawned_at = std::time::Instant::now();
    move |kind| {
        let kind = match kind {
            WorkerExitKind::Clean
                if clean_grace.is_some_and(|g| spawned_at.elapsed() < g) =>
            {
                error!(
                    model = %model_name, version = %version, worker_id,
                    "Replacement exited cleanly within the respawn grace window; \
                     likely consumed a stale stop replayed on the reused PAIR \
                     socket — treating as crash (no auto-respawn)"
                );
                WorkerExitKind::Crash
            }
            other => other,
        };
        let reason = match kind {
            WorkerExitKind::Crash => "worker process exited unexpectedly",
            WorkerExitKind::Killed => "worker terminated by supervisor",
            // Clean exit: the unload path's client drop handles in-flight
            // requests — no spurious fail on stops.
            WorkerExitKind::Clean => return,
        };
        // B5 (leak-gap-audit-0821): a LATE exit event from a previous
        // occupant of this slot must not poison the current one. After a
        // reap-timeout respawn the replacement owns the slot (shared ZMQ
        // client, shared OutlierState, registry entry with the new pid);
        // the old worker's delayed death would otherwise fail_all the
        // replacement's in-flight requests, mark the healthy slot dead
        // (no recovery except another respawn — permanent NoLiveWorkers for
        // single-worker versions), and flip the live registry entry to
        // Stopped. Act only when the registry still names this handler's
        // pid for the slot; a mismatch (replacement present, entry gone)
        // discards the event as stale. `None` pid = unknown, act as before.
        if let Some(pid) = worker_pid {
            let current_pid = registry
                .get(&model_name, Some(&version))
                .and_then(|mv| {
                    mv.workers
                        .iter()
                        .find(|w| w.worker_id == worker_id)
                        .and_then(|w| w.pid)
                });
            if current_pid != Some(pid) {
                info!(
                    model = %model_name, version = %version, worker_id,
                    "Discarding stale {kind:?} exit event for pid {pid} \
                     (slot's current occupant: {current_pid:?})"
                );
                return;
            }
        }
        if let Some(c) = &fail_client {
            c.fail_all(reason);
        }
        if let Some(o) = &outlier {
            o.mark_dead(worker_id as usize);
        }
        registry.mark_worker_stopped(&model_name, &version, worker_id);
    }
}

/// B14 (leak-gap-audit-0821): read ONE line from a perpetual stderr pipe,
/// capping the buffered bytes at `cap`. The startup-time drain has the
/// `worker_stderr_tail_bytes` cap, but the long-lived loggers used a bare
/// `read_until(b'\n')` — a worker printing a huge object without a newline
/// (tensor repr, minified JSON) made the host accumulate the entire line in
/// RAM per worker. An overlong line is truncated to `cap` bytes plus a
/// marker, and the remainder up to the newline is discarded so the next
/// call starts at the next line. Returns Ok(0) on EOF.
pub(super) async fn read_stderr_line_bounded<R>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    cap: usize,
) -> std::io::Result<usize>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    buf.clear();
    let n = (&mut *reader)
        .take(cap as u64 + 1)
        .read_until(b'\n', buf)
        .await?;
    if n == 0 {
        return Ok(0);
    }
    if !buf.ends_with(b"\n") {
        // Overlong line: truncate, then discard through the newline.
        buf.truncate(cap);
        buf.extend_from_slice(b"...[truncated]");
        let mut scratch = Vec::new();
        loop {
            scratch.clear();
            let m = reader.read_until(b'\n', &mut scratch).await?;
            if m == 0 || scratch.ends_with(b"\n") {
                break;
            }
        }
    }
    Ok(n)
}

/// B8 (leak-gap-audit-0821): does the registry's CURRENT entry for this
/// slot still name the process `pid`? A detached respawn re-warm completes
/// after a possible reload swapped every map under the same key — acting on
/// a map lookup at completion time would force-eject / mark Degraded the
/// NEW generation for the OLD one's failure. Same pid-guard shape as the
/// B5 late-on_exit guard.
pub(super) fn slot_occupied_by(
    registry: &crate::registry::ModelRegistry,
    model_name: &str,
    version: &str,
    worker_id: u32,
    pid: Option<u32>,
) -> bool {
    registry
        .get(model_name, Some(version))
        .and_then(|mv| {
            mv.workers
                .iter()
                .find(|w| w.worker_id == worker_id)
                .and_then(|w| w.pid)
        })
        == pid
}

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
// allow: worker 生命周期监控管道(child/关停通道/hooks/draining 等异构
// 部件),lifecycle 单点组装,无共享上下文可收。
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_worker_monitor(
    mut child: Child,
    model_name: &str,
    version: &str,
    worker_id: u32,
    mut shutdown_rx: oneshot::Receiver<()>,
    on_exit: impl FnOnce(WorkerExitKind) + Send + 'static,
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
        let kind = tokio::select! {
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
                            WorkerExitKind::Clean
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
                            WorkerExitKind::Crash
                        }
                    }
                    Err(e) => {
                        error!(
                            model = %model, version = %ver, worker_id,
                            error = %e,
                            "Failed to wait on worker process"
                        );
                        // The wait failed, not the child necessarily — but
                        // failing in-flight requests beats hanging them.
                        WorkerExitKind::Crash
                    }
                }
            }
            _ = &mut shutdown_rx => {
                info!(
                    model = %model, version = %ver, worker_id,
                    "Shutting down worker process"
                );
                let _ = child.kill().await;
                WorkerExitKind::Killed
            }
        };
        on_exit(kind);

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
            {
                format!("{};{}", current_pythonpath, python_module_dir)
            }
            #[cfg(not(windows))]
            {
                format!("{}:{}", current_pythonpath, python_module_dir)
            }
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
///
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
        let sock_path = std::env::temp_dir().join("lite-server").join(format!(
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
        let key = format!(
            "{}_{}_{}_{}",
            model_name,
            version,
            worker_id,
            std::process::id()
        );
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
            let shutdown = self.shutdown_token.clone();
            tokio::spawn(async move {
                // M5: exit on shutdown — a rolling-recycle signal can arrive
                // up to recycle_drain_timeout after its trigger, i.e. while
                // the unload sweep is running; spawning a worker then would
                // leak it past the sweep. The is_cancelled re-check covers a
                // signal that raced the token in the same select iteration.
                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        signal = rx.recv() => {
                            let Some(signal) = signal else { break };
                            if shutdown.is_cancelled() {
                                info!(
                                    model = %signal.model_name, version = %signal.version,
                                    worker_id = signal.worker_id,
                                    "Dropping respawn signal; server is shutting down"
                                );
                                continue;
                            }
                            if let Some(wm) = wm.upgrade() {
                                info!(
                                    model = %signal.model_name, version = %signal.version,
                                    worker_id = signal.worker_id, reason = signal.reason,
                                    graceful = signal.graceful,
                                    "Respawning worker"
                                );
                                if let Err(e) = wm
                                    .respawn_worker(
                                        &signal.model_name,
                                        &signal.version,
                                        signal.worker_id,
                                        signal.reason,
                                        signal.graceful,
                                    )
                                    .await
                                {
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
                    }
                }
            });
        }
    }

    /// Respawn a single worker after it was killed (health-check kill escalation).
    /// Terminates the old worker first, then spawns a replacement and updates
    /// all registries. Failures are counted (WORKER_RESPAWN_FAILURES_TOTAL) —
    /// a replacement that never comes up must be metric-visible, not log-only.
    async fn respawn_worker(
        self: &Arc<Self>,
        model_name: &str,
        version: &str,
        worker_id: u32,
        reason: &'static str,
        graceful: bool,
    ) -> Result<(), AppError> {
        // A non-graceful (health-kill) signal for a slot already claimed by a
        // rolling recycle is stale: the recycle is replacing that very
        // process, and the listener is serial — acting now would hard-kill
        // the draining worker, or worse its fresh replacement.
        if !graceful {
            let key = model_version_key(model_name, version);
            let recycling = {
                let outliers = self.outlier_states.read().await;
                outliers
                    .get(&key)
                    .map(|o| o.is_recycling(worker_id as usize))
                    .unwrap_or(false)
            };
            if recycling {
                info!(
                    model = %model_name, version = %version, worker_id,
                    "Dropping stale health-kill signal; slot is mid-rolling-recycle"
                );
                return Ok(());
            }
        }
        let result = self
            .respawn_worker_inner(model_name, version, worker_id, reason, graceful)
            .await;
        if result.is_err() {
            crate::metrics::prometheus::WORKER_RESPAWN_FAILURES_TOTAL
                .with_label_values(&[model_name, version, reason])
                .inc();
            // A failed rolling recycle must not strand the slot in the
            // recycling claim: fall back to a plain ejection so the
            // impairment stays visible (coordinator → Degraded) and the
            // circuit's half-open probe keeps a recovery path.
            if graceful {
                let key = model_version_key(model_name, version);
                let outliers = self.outlier_states.read().await;
                if let Some(outlier) = outliers.get(&key) {
                    outlier.release_recycle(worker_id as usize);
                }
            }
        }
        result
    }

    async fn respawn_worker_inner(
        self: &Arc<Self>,
        model_name: &str,
        version: &str,
        worker_id: u32,
        reason: &'static str,
        graceful: bool,
    ) -> Result<(), AppError> {
        let key = model_version_key(model_name, version);

        // Get config from registry
        let model_version = self
            .registry
            .get(model_name, Some(version))
            .ok_or_else(|| AppError::ModelNotFound(format!("{} v{}", model_name, version)))?;
        let model_config = model_version.config;

        // Stale-signal guard: a respawn signal's worker_id is valid only for
        // the generation that produced it. A reload may have shrunk the
        // worker count while the signal sat in the channel — an out-of-range
        // id must fail the respawn BEFORE any status flip or old-proc
        // removal; the unchecked device_plan index below would otherwise
        // panic and kill the serial respawn listener server-wide.
        let device_plan = model_config
            .resolve_device_plan()
            .map_err(|e| AppError::Config(e.to_string()))?;
        let Some(&device_index) = device_plan.get(worker_id as usize) else {
            return Err(AppError::Internal(format!(
                "stale respawn signal: worker_id {worker_id} out of range for \
                 {model_name} {version} (workers: {})",
                device_plan.len()
            )));
        };

        let model_dir = crate::validation::resolve_model_dir(&self.repo_path, model_name, version)?;
        let model_py = model_dir.join("model.py");
        let config_yaml = model_dir.join("config.yaml");

        // Terminate the old (possibly hung) worker BEFORE respawning: signal
        // its monitor to kill it, then await the reap so no orphaned process
        // survives to leak memory or steal the re-bound socket (mirrors the
        // unload sequence).
        let old_proc = {
            let mut workers = self.workers.write().await;
            workers.get_mut(&key).and_then(|procs| {
                procs
                    .iter()
                    .position(|w| w.worker_id == worker_id)
                    .map(|pos| procs.remove(pos))
            })
        };
        if let Some(mut proc) = old_proc {
            if graceful {
                // Rolling recycle of a healthy, drained worker: send the ZMQ
                // stop message (FIFO-ordered behind any in-flight batches on
                // the reused PAIR socket, so they are still answered first)
                // and wait out kill_timeout for a clean exit. Unlike the kill
                // path, this is safe against the stale-stop replay hazard
                // because the collector only signals a recycle after the slot
                // drained — a healthy worker consumes the stop before exiting
                // and is reaped before the replacement spawns.
                //
                // H2 path-a guard: if the old worker already exited on its
                // own (crash during the drain), a stop sent now would sit
                // unconsumed in the dead peer's PAIR buffer and be replayed
                // to the replacement on reconnect — skip it and just reap.
                if worker_already_reaped(&mut proc) {
                    info!(
                        model = %model_name, version = %version, worker_id,
                        "Old worker already exited before recycle; skipping stop message"
                    );
                } else {
                    let client = {
                        let clients = self.zmq_clients.read().await;
                        clients
                            .get(&key)
                            .and_then(|cs| cs.get(worker_id as usize).cloned())
                    };
                    if let Some(client) = client {
                        let _ = client.send_raw(crate::streaming::build_stop_request()).await;
                    }
                }
                let escalated = stop_worker_gracefully(&mut proc, model_name, version).await;
                if escalated {
                    warn!(
                        model = %model_name, version = %version, worker_id,
                        "Worker hung during graceful recycle; an unconsumed stop \
                         message may be replayed to the replacement on the reused socket"
                    );
                }
            } else {
                // NO graceful stop here — kill escalation only. This path targets
                // a worker that failed health probes (likely hung), so teardown
                // would hang the respawn anyway; and a stop message sent now would
                // be replayed to the replacement worker when it reconnects to the
                // reused bound socket (ZMQ PAIR keeps queued messages across a
                // dead peer's reconnect), instantly killing the replacement in a
                // kill→respawn→kill loop. The graceful stop-then-wait sequence
                // (stop_worker_gracefully) belongs to the unload path and the
                // rolling-recycle path, where the worker is known healthy.
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
        //
        // Graceful rolling recycle is the exception: the infer gate
        // (`registry.is_ready`) is strict-Ready, so degrading the version
        // would 503 ALL traffic for the whole stop+spawn window even though
        // the sibling slots are healthy. The recycling slot is already
        // ejected (routing avoids it), so the version stays Ready.
        if !graceful {
            self.registry
                .set_status(model_name, version, VersionStatus::Degraded)?;
        }
        let accelerator = model_config.accelerator.as_deref().unwrap_or("cpu");
        // Same resolver as the initial spawn (single source of truth), so a
        // respawned/recycled worker lands back on its original device.
        // Resolved and bounds-checked at the top of this function.
        let device = format!("{}:{}", accelerator, device_index);

        let mut cmd = new_worker_command(&Self::find_python_module_path().unwrap_or_default());
        // Parity with the load-time spawn: the worker's CWD is its model
        // dir — a model reading relative-path weights/configs must see the
        // same layout after a respawn (found via leak-gap-audit-0821's B7
        // test: the replacement silently ran with the server's CWD).
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
            .map_err(|e| AppError::Python(format!("failed to spawn worker: {}", e)))
            .inspect_err(|_| self.mark_degraded(model_name, version))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AppError::Internal("worker stdout not piped".to_string()))
            .inspect_err(|_| self.mark_degraded(model_name, version))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| AppError::Internal("worker stderr not piped".to_string()))
            .inspect_err(|_| self.mark_degraded(model_name, version))?;

        // Wait for "ready" signal
        let mut reader = BufReader::new(stdout);
        let mut ready_line = String::new();
        let n = timeout(
            Duration::from_secs_f32(model_config.startup_timeout),
            reader.read_line(&mut ready_line),
        )
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

        // B7 (leak-gap-audit-0821): an unload landing mid-respawn already
        // tore this version down (registry entry gone, metrics purged,
        // routes removed) — registering metrics/policies/routes now would
        // re-create just-purged state for a dead version (phantom family
        // refs pin the family forever; orphaned route entries linger until
        // a same-key reload). The load path has the M1 aliveness guard;
        // respawn needs the same. Bail — the child self-heals: shutdown_tx
        // drops with this scope and the monitor SIGKILLs it.
        if self.registry.get(model_name, Some(version)).is_none()
            || self.shutdown_token.is_cancelled()
        {
            return Err(AppError::Internal(format!(
                "respawn of {model_name} {version} aborted: torn down concurrently"
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
                crate::metrics::prometheus::register_custom_metrics(
                    model_name, version, &spec_refs,
                );
            }
        }

        // Store per-model policies declared in config.yaml
        self.registry
            .set_policies(model_name, version, policies_from_config(&model_config));

        // Register custom @route declarations (phase 2)
        self.upsert_routes(model_name, version, startup.custom_routes)
            .await;

        info!(
            "Worker {} for {} v{} respawned (pid={:?})",
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
        // B14: per-line buffer cap (host memory bound for newline-less prints).
        let stderr_line_cap = self.server_tunables.worker_stderr_tail_bytes;
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut buf = Vec::with_capacity(1024);
            loop {
                match read_stderr_line_bounded(&mut reader, &mut buf, stderr_line_cap).await {
                    Ok(0) => break,
                    Ok(_) => {
                        while buf.last() == Some(&b'\n') || buf.last() == Some(&b'\r') {
                            buf.pop();
                        }
                        let line = String::from_utf8_lossy(&buf);
                        let level = classify_stderr_line(line.trim());
                        let msg = strip_level_prefix(line.trim());
                        emit_stderr_line(
                            level,
                            msg,
                            worker_id_clone as usize,
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

        let pid = child.id();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // Spawn monitor for the new worker. The ZMQ client is reused (not
        // recreated) — the bound PAIR socket outlives this worker process and
        // accepts the replacement's reconnect — so there is no `.sock` to clean
        // on exit. `on_exit` fails in-flight requests fast when this worker
        // later dies: ZMQ PAIR has no peer-disconnect event, so waiters would
        // otherwise hang until their caller-side timeouts.
        let fail_client = {
            let clients = self.zmq_clients.read().await;
            clients
                .get(&key)
                .and_then(|cs| cs.get(worker_id as usize).cloned())
        };
        let outlier = {
            let outliers = self.outlier_states.read().await;
            outliers.get(&key).cloned()
        };
        let hooks_arc = Arc::new(model_config.hooks.clone());
        let worker_pid = child.id();
        let done_rx = spawn_worker_monitor(
            child,
            model_name,
            version,
            worker_id,
            shutdown_rx,
            crash_exit_handler_with_grace(
                fail_client,
                outlier.clone(),
                self.registry.clone(),
                model_name.to_string(),
                version.to_string(),
                worker_id,
                worker_pid,
                // H2: a Clean exit moments after a respawn means the
                // replacement consumed a stale stop replayed on the reused
                // PAIR socket — treat it as a crash, never ignore it.
                Some(RESPAWN_CLEAN_GRACE),
            ),
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
        self.registry
            .replace_worker(model_name, version, worker_id, new_info)?;
        // Respawn replaced the process: point memory sampling at the new PID
        // (same worker_id overwrites the stale entry).
        if let Some(pid) = pid {
            crate::metrics::prometheus::set_worker_pid(model_name, version, worker_id, pid);
        }

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

        // G2 (F4): the replacement is a fresh, cold process — with warmup
        // enabled, re-warm it (pinned to its own slot). The re-warm runs as a
        // BACKGROUND task: the respawn listener is a global serial loop and
        // the re-warm has no default total budget, so running it inline would
        // stall crash recovery for every other model behind one slow/hung
        // warmup. The slot rejoins rotation at the reset below (live traffic
        // may route here during the re-warm window); a failed re-warm
        // force-ejects the slot after the fact, and the circuit's half-open
        // probe re-warms it lazily on later traffic.
        let rewarm = model_config
            .policies
            .warmup
            .clone()
            .filter(|p| p.enabled && p.respawn);

        // Clear the slot's ejection/recycling/error state so routing to the
        // replacement resumes immediately instead of waiting out the ejection
        // timeout. This must run BEFORE the Ready restore below: a status
        // coordinator tick landing in between must not re-flip the version to
        // Degraded on stale ejection state.
        {
            let outliers = self.outlier_states.read().await;
            if let Some(outlier) = outliers.get(&key) {
                outlier.reset(worker_id as usize);
            }
        }

        // Replacement worker is up: Degraded → Ready (loaded_at preserved).
        // A graceful recycle never degraded the version, so restore Ready
        // only when it IS currently Degraded (kill mode, or the status
        // coordinator flipped it during a slow recycle) — never flip a
        // version that is Degraded for another slot's sake.
        let is_degraded = self
            .registry
            .get(model_name, Some(version))
            .map(|mv| mv.status == VersionStatus::Degraded)
            .unwrap_or(false);
        if !graceful || is_degraded {
            self.registry.mark_ready(model_name, version)?;
            self.sync_grpc_health().await;
        }

        // Record metric
        crate::metrics::prometheus::WORKER_RESPAWNS_TOTAL
            .with_label_values(&[model_name, version, reason])
            .inc();

        if let Some(p) = rewarm {
            let wm = Arc::clone(self);
            let model = model_name.to_string();
            let ver = version.to_string();
            // B8: pin the task to THIS generation — the outlier Arc captured
            // now (a reload replaces the map entry with a fresh Arc, so
            // ejecting the captured one can never poison the new
            // generation) and a pid check before any registry mutation.
            let outlier_for_slot = outlier.clone();
            tokio::spawn(async move {
                // L3: run_warmup_budgeted adds a fallback total budget
                // (max(startup_timeout, 300s)) when the policy has none — a
                // hung replacement must not park this detached task forever.
                //
                // B8: if a reload swapped the generation while this task was
                // queued, its warmup would run against the NEW workers with
                // the OLD generation's sample payloads — exit instead.
                if !slot_occupied_by(&wm.registry, &model, &ver, worker_id, worker_pid) {
                    info!(
                        model = %model, version = %ver, worker_id,
                        "Dropping re-warm for a superseded generation"
                    );
                    return;
                }
                if let Err(reason) = wm
                    .run_warmup_budgeted(&model, &ver, &model_config, &p, Some(worker_id))
                    .await
                {
                    error!(
                        model = %model, version = %ver, worker_id,
                        reason = %reason,
                        "respawn re-warm failed; force-ejecting the cold replacement"
                    );
                    let newly_ejected = outlier_for_slot
                        .as_ref()
                        .map(|o| o.force_eject(worker_id as usize))
                        .unwrap_or(false);
                    if newly_ejected {
                        crate::metrics::prometheus::inc_worker_ejection(&model, &ver);
                    }
                    crate::metrics::prometheus::WORKER_RESPAWN_FAILURES_TOTAL
                        .with_label_values(&[&model, &ver, "warmup"])
                        .inc();
                    // B8: only flip the version Degraded when the slot still
                    // belongs to this generation — a reload must not inherit
                    // the old generation's failure.
                    if slot_occupied_by(&wm.registry, &model, &ver, worker_id, worker_pid) {
                        // The slot left rotation — flip to Degraded
                        // event-driven (there is no periodic coordinator
                        // when health_check_interval == 0).
                        wm.mark_degraded(&model, &ver);
                        wm.sync_grpc_health().await;
                    }
                }
            });
        }

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

    // ===== H2 guard tests (stale-stop replay) =====

    fn proc_with_done_rx(done_rx: Option<oneshot::Receiver<()>>) -> WorkerProcess {
        WorkerProcess {
            worker_id: 0,
            endpoint: String::new(),
            shutdown_tx: None,
            done_rx,
            kill_timeout: Duration::from_secs(1),
        }
    }

    #[test]
    fn should_report_reaped_only_after_done_signal() {
        let (tx, rx) = oneshot::channel::<()>();
        let mut proc = proc_with_done_rx(Some(rx));
        assert!(
            !worker_already_reaped(&mut proc),
            "reap pending → the stop is still meaningful"
        );
        assert!(proc.done_rx.is_some(), "pending channel stays in place");
        let _ = tx.send(());
        assert!(
            worker_already_reaped(&mut proc),
            "reap observed → a stop would be replayed to the replacement"
        );
        assert!(
            proc.done_rx.is_none(),
            "a completed oneshot panics when re-polled — it is taken out"
        );
    }

    #[test]
    fn should_report_reaped_when_done_channel_dropped() {
        let (tx, rx) = oneshot::channel::<()>();
        drop(tx);
        let mut proc = proc_with_done_rx(Some(rx));
        assert!(
            worker_already_reaped(&mut proc),
            "a resolved-by-drop channel also means the process is gone"
        );
        assert!(proc.done_rx.is_none(), "dead channel is taken out");
    }

    #[test]
    fn should_report_not_reaped_without_done_rx() {
        let mut proc = proc_with_done_rx(None);
        assert!(
            !worker_already_reaped(&mut proc),
            "no reap channel → keep the legacy stop behavior"
        );
    }

    /// Spawn a child that exits 0 immediately and run its monitor with the
    /// given handler; returns once the monitor reaped it.
    async fn run_clean_exit_monitor(
        on_exit: impl FnOnce(WorkerExitKind) + Send + 'static,
    ) {
        #[cfg(unix)]
        let child = tokio::process::Command::new("sh")
            .args(["-c", "exit 0"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        #[cfg(windows)]
        let child = tokio::process::Command::new("cmd")
            .args(["/c", "exit", "0"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let (_shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let done_rx = spawn_worker_monitor(
            child,
            "test_model",
            "1",
            0,
            shutdown_rx,
            on_exit,
            None,
            Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            Arc::new(AtomicBool::new(false)),
        );
        timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("monitor did not observe the clean exit")
            .expect("completion channel dropped");
    }

    #[tokio::test]
    async fn should_treat_clean_exit_within_grace_as_crash() {
        let outlier = Arc::new(crate::inference_queue::OutlierState::new(1));
        let registry = Arc::new(crate::registry::ModelRegistry::new());
        let handler = crash_exit_handler_with_grace(
            None,
            Some(outlier.clone()),
            registry,
            "test_model".to_string(),
            "1".to_string(),
            0,
            None,
            Some(Duration::from_secs(60)),
        );
        run_clean_exit_monitor(handler).await;
        assert!(
            outlier.is_dead(0),
            "a Clean exit inside the grace window is a stale-stop replay — mark dead"
        );
    }

    #[tokio::test]
    async fn should_ignore_clean_exit_beyond_grace_window() {
        let outlier = Arc::new(crate::inference_queue::OutlierState::new(1));
        let registry = Arc::new(crate::registry::ModelRegistry::new());
        let handler = crash_exit_handler_with_grace(
            None,
            Some(outlier.clone()),
            registry,
            "test_model".to_string(),
            "1".to_string(),
            0,
            None,
            Some(Duration::ZERO), // window already elapsed at creation
        );
        run_clean_exit_monitor(handler).await;
        assert!(
            !outlier.is_dead(0),
            "a Clean exit outside the grace window keeps the ignore semantics"
        );
    }

    #[tokio::test]
    async fn should_ignore_clean_exit_without_grace() {
        let outlier = Arc::new(crate::inference_queue::OutlierState::new(1));
        let registry = Arc::new(crate::registry::ModelRegistry::new());
        let handler = crash_exit_handler(
            None,
            Some(outlier.clone()),
            registry,
            "test_model".to_string(),
            "1".to_string(),
            0,
            None,
        );
        run_clean_exit_monitor(handler).await;
        assert!(
            !outlier.is_dead(0),
            "initial-spawn monitors keep ignoring Clean exits (unload semantics)"
        );
    }

    #[tokio::test]
    async fn should_drop_respawn_signals_after_shutdown() {
        // M5: a rolling-recycle signal can arrive up to recycle_drain_timeout
        // after its trigger — i.e. mid-shutdown. The listener must exit on
        // the shutdown token instead of spawning a worker that races the
        // unload sweep. Observed via the failure metric: a processed signal
        // for a nonexistent model increments it, a dropped one cannot.
        let registry = Arc::new(crate::registry::ModelRegistry::new());
        let wm = Arc::new(WorkerManager::new(
            registry,
            std::env::temp_dir(),
            Arc::new(crate::inference_queue::InferenceQueue::new()),
            "debug".to_string(),
            Arc::new(crate::callback::CallbackRunner::new()),
        ));
        wm.start_respawn_listener().await;

        wm.shutdown().await;

        let metric = crate::metrics::prometheus::WORKER_RESPAWN_FAILURES_TOTAL
            .with_label_values(&["m-shutdown-drop", "1", "health_check"]);
        let before = metric.get();
        let _ = wm
            .respawn_tx
            .send(crate::inference_queue::RespawnSignal {
                model_name: "m-shutdown-drop".to_string(),
                version: "1".to_string(),
                worker_id: 0,
                reason: "health_check",
                graceful: false,
            })
            .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            metric.get(),
            before,
            "a respawn signal sent after shutdown must be dropped, not processed"
        );
    }

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
        assert_eq!(
            ep,
            worker_endpoint("m", "1", 0),
            "same process → stable path"
        );
        assert_ne!(
            ep,
            worker_endpoint("m", "1", 1),
            "worker_id still distinguishes"
        );
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
            child,
            "test_model",
            "1",
            0,
            shutdown_rx,
            move |_kind| {
                cleaned_up_clone.store(true, Ordering::SeqCst);
            },
            None,
            Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            Arc::new(AtomicBool::new(false)),
        );

        // Wait for the monitor to detect the exit and run cleanup
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !cleaned_up.load(Ordering::SeqCst) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            cleaned_up.load(Ordering::SeqCst),
            "monitor should have triggered cleanup"
        );
    }

    /// Audit 2026-08-10 #8: a crash must fail the worker's in-flight requests
    /// immediately (monitor → on_exit(Crash) → fail_all) — ZMQ PAIR has no
    /// peer-disconnect event, so without this they hang until their
    /// caller-side timeouts (up to the 300s backstop).
    #[tokio::test]
    async fn test_monitor_crash_fails_inflight_requests() {
        use crate::proto::liteserver as pb;
        use crate::transport::zmq::WorkerZmqClient;

        // Fake worker peer: connects and stays silent so in-flight entries
        // stay registered. rcvtimeo-bounded so the thread exits at teardown.
        #[cfg(unix)]
        let endpoint = {
            let sock = std::env::temp_dir()
                .join(format!("lite-monitor-failall-{}.sock", std::process::id()));
            let _ = std::fs::remove_file(&sock);
            format!("ipc://{}", sock.display())
        };
        #[cfg(windows)]
        let endpoint = format!("tcp://127.0.0.1:{}", 37000 + std::process::id() % 1000);
        let ep = endpoint.clone();
        let worker = std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).unwrap();
            s.connect(&ep).unwrap();
            let _ = s.set_rcvtimeo(5000);
            loop {
                match s.recv_bytes(0) {
                    Ok(_) => {}
                    Err(_) => return,
                }
            }
        });

        let client = std::sync::Arc::new(WorkerZmqClient::new(endpoint));
        tokio::time::sleep(Duration::from_millis(200)).await;

        // One in-flight unary with a 30s timeout — fail_all, not the
        // timeout, must resolve it.
        let req = pb::Request {
            uid: "crash-inflight".to_string(),
            meta: None,
            payload: Some(pb::request::Payload::Single(pb::SingleRequest {
                data: bytes::Bytes::from_static(b"{}"),
            })),
        };
        let inflight = tokio::spawn({
            let c = client.clone();
            async move { c.send_with_timeout(req, Duration::from_secs(30)).await }
        });
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Child that crashes (exit 1) right away.
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

        let observed = std::sync::Arc::new(std::sync::Mutex::new(None));
        let observed_c = observed.clone();
        // The production crash closure: fail_all + mark_dead + Stopped.
        let outlier = std::sync::Arc::new(crate::inference_queue::OutlierState::new(1));
        let registry = std::sync::Arc::new(crate::registry::ModelRegistry::new());
        registry
            .register(
                "test_model",
                "1",
                crate::config::ModelConfig::default(),
                crate::registry::types::ModelType::LitAPI,
                std::path::PathBuf::new(),
            )
            .unwrap();
        registry
            .set_workers(
                "test_model",
                "1",
                vec![crate::registry::types::WorkerInfo {
                    worker_id: 0,
                    device: "cpu:0".to_string(),
                    endpoint: String::new(),
                    pid: Some(12345),
                    status: crate::registry::types::WorkerStatus::Ready,
                    capacity: None,
                }],
            )
            .unwrap();
        let (_shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let done_rx = spawn_worker_monitor(
            child,
            "test_model",
            "1",
            0,
            shutdown_rx,
            {
                let handler = crash_exit_handler(
                    Some(client.clone()),
                    Some(outlier.clone()),
                    registry.clone(),
                    "test_model".to_string(),
                    "1".to_string(),
                    0,
                    None,
                );
                move |kind| {
                    *observed_c.lock().unwrap() = Some(kind);
                    handler(kind);
                }
            },
            None,
            Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            Arc::new(AtomicBool::new(false)),
        );

        timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("monitor did not observe the crash")
            .expect("completion channel dropped");

        let resp = timeout(Duration::from_secs(3), inflight)
            .await
            .expect("in-flight request was not released after the crash")
            .expect("inflight task panicked")
            .expect("send failed");
        match resp.payload {
            Some(pb::response::Payload::Single(single)) => {
                assert_eq!(single.status.unwrap().code, "Error");
            }
            other => panic!("expected Single error, got {:?}", other),
        }
        assert_eq!(*observed.lock().unwrap(), Some(WorkerExitKind::Crash));
        // Crash bookkeeping: the slot is dead for routing and the registry
        // entry no longer shows the dead process as Ready.
        assert!(
            outlier.is_dead(0),
            "crash must mark the worker slot dead for routing"
        );
        let w = &registry
            .get("test_model", Some("1"))
            .expect("model still registered")
            .workers[0];
        assert_eq!(w.status, crate::registry::types::WorkerStatus::Stopped);
        assert_eq!(w.pid, None, "the dead pid must be cleared");

        drop(client);
        let _ = worker.join();
    }

    /// A zero-status exit reports Clean (in-flight cleanup is left to the
    /// unload path's client drop — no spurious fail_all on graceful stops).
    #[tokio::test]
    async fn test_monitor_reports_clean_exit_kind() {
        #[cfg(unix)]
        let child = tokio::process::Command::new("true")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        #[cfg(windows)]
        let child = tokio::process::Command::new("cmd")
            .args(["/c", "exit", "0"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let observed = std::sync::Arc::new(std::sync::Mutex::new(None));
        let observed_c = observed.clone();
        let (_shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let done_rx = spawn_worker_monitor(
            child,
            "test_model",
            "1",
            0,
            shutdown_rx,
            move |kind| {
                *observed_c.lock().unwrap() = Some(kind);
            },
            None,
            Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            Arc::new(AtomicBool::new(false)),
        );

        timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("monitor should signal completion")
            .expect("completion channel dropped");
        assert_eq!(*observed.lock().unwrap(), Some(WorkerExitKind::Clean));
    }

    /// A supervisor-initiated kill (shutdown channel) reports Killed — the
    /// health-check kill escalation and the unload kill grace both take this
    /// path, and their in-flight requests must fail fast too.
    #[tokio::test]
    async fn test_monitor_reports_killed_kind() {
        let mut cmd = Command::new("python");
        cmd.arg("-c").arg("import time; time.sleep(60)");
        let child = cmd
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let observed = std::sync::Arc::new(std::sync::Mutex::new(None));
        let observed_c = observed.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let done_rx = spawn_worker_monitor(
            child,
            "test_model",
            "1",
            0,
            shutdown_rx,
            move |kind| {
                *observed_c.lock().unwrap() = Some(kind);
            },
            None,
            Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            Arc::new(AtomicBool::new(false)),
        );

        shutdown_tx.send(()).unwrap();
        timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("monitor should signal completion after kill")
            .expect("completion channel dropped");
        assert_eq!(*observed.lock().unwrap(), Some(WorkerExitKind::Killed));
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
            child,
            "test_model",
            "1",
            0,
            shutdown_rx,
            move |_kind| {
                cleaned_up_clone.store(true, Ordering::SeqCst);
            },
            Some(Arc::new(hooks)),
            hook_tasks_clone,
            Arc::new(AtomicBool::new(true)), // draining=true
        );

        // Await the monitor's completion signal
        timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("monitor should signal completion even during drain")
            .expect("completion channel should not be dropped");

        assert!(
            cleaned_up.load(Ordering::SeqCst),
            "on_exit callback must fire during drain"
        );

        // The error hook task should NOT have been spawned (draining suppresses it)
        let tasks = hook_tasks.lock().unwrap();
        assert!(
            tasks.is_empty(),
            "error hook should NOT fire when draining=true (found {} tasks)",
            tasks.len()
        );
    }

    /// B1 inverse (audit 2026-08-04): with draining=false a non-zero worker exit
    /// is a genuine crash — the error hook MUST fire. The committed B1 test only
    /// covers the draining=true direction, so an unconditional suppression would
    /// pass the suite; this pins the normal-operation contract.
    #[tokio::test]
    async fn test_worker_monitor_not_draining_fires_error_hook() {
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
        let hook_tasks = Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new()));
        let hook_tasks_clone = hook_tasks.clone();

        let hooks = crate::config::WorkerHooksConfig {
            on_error: Some("echo 'crash-hook'".to_string()),
            ..Default::default()
        };

        let done_rx = spawn_worker_monitor(
            child,
            "test_model",
            "1",
            0,
            shutdown_rx,
            |_kind| {},
            Some(Arc::new(hooks)),
            hook_tasks_clone,
            Arc::new(AtomicBool::new(false)), // draining=false — real crash
        );

        timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("monitor should signal completion")
            .expect("completion channel should not be dropped");

        let tasks = hook_tasks.lock().unwrap();
        assert!(
            !tasks.is_empty(),
            "error hook MUST fire on unexpected exit when draining=false"
        );
    }

    // ===== Respawn tests =====

    fn test_worker_manager() -> (
        WorkerManager,
        std::sync::Arc<crate::registry::ModelRegistry>,
    ) {
        let registry = std::sync::Arc::new(crate::registry::ModelRegistry::new());
        let queue = std::sync::Arc::new(crate::inference_queue::InferenceQueue::new());
        let cb = std::sync::Arc::new(crate::callback::CallbackRunner::new());
        let wm = WorkerManager::new(
            registry.clone(),
            std::path::PathBuf::new(),
            queue,
            "warn".to_string(),
            cb,
        );
        (wm, registry)
    }

    /// A spawn/handshake failure during load must count a load failure —
    /// otherwise the load failure rate is silently underestimated (only the
    /// warmup-failure path recorded it).
    #[tokio::test]
    async fn mark_load_failed_records_failure_metric() {
        let (wm, registry) = test_worker_manager();
        registry
            .register(
                "mfail",
                "1",
                crate::config::ModelConfig::default(),
                ModelType::LitAPI,
                std::path::PathBuf::new(),
            )
            .unwrap();
        let labels = &["mfail", "1", "load", "fail"];
        let before = crate::metrics::prometheus::MODEL_LOAD_TOTAL
            .with_label_values(labels)
            .get();
        wm.mark_load_failed("mfail", "1");
        let after = crate::metrics::prometheus::MODEL_LOAD_TOTAL
            .with_label_values(labels)
            .get();
        assert_eq!(
            after,
            before + 1.0,
            "mark_load_failed must count a model-load failure"
        );
        let status = registry.get("mfail", Some("1")).map(|mv| mv.status);
        assert_eq!(
            status,
            Some(crate::registry::types::VersionStatus::Failed),
            "the registry status flip must be preserved"
        );
    }

    /// A failed respawn (replacement never comes up) must be visible in
    /// metrics, not only in logs.
    #[tokio::test]
    async fn respawn_failure_records_metric() {
        let (wm, _registry) = test_worker_manager();
        let wm = std::sync::Arc::new(wm);
        let labels = &["ghost_m", "1", "health_check"];
        let before = crate::metrics::prometheus::WORKER_RESPAWN_FAILURES_TOTAL
            .with_label_values(labels)
            .get();
        wm.respawn_worker("ghost_m", "1", 0, "health_check", false)
            .await
            .expect_err("respawning an unknown model must fail");
        let after = crate::metrics::prometheus::WORKER_RESPAWN_FAILURES_TOTAL
            .with_label_values(labels)
            .get();
        assert_eq!(after, before + 1.0, "a failed respawn must be counted");
    }

    /// Stale-signal panic (audit 2026-08-20): a respawn signal carries a
    /// worker_id valid only for the generation that produced it. After a
    /// reload shrinks the worker count (config.yaml edited devices 2→1), the
    /// queued stale signal reaches respawn_worker_inner, whose
    /// `device_plan[worker_id as usize]` index is unchecked — an
    /// out-of-range worker_id PANICS the serial respawn listener, silently
    /// killing health-kill escalations and rolling recycles server-wide
    /// (the channel then backs up and wedges every model's health checker).
    /// An out-of-range worker_id must return Err, never panic.
    #[tokio::test]
    async fn respawn_with_out_of_range_worker_id_must_err_not_panic() {
        let (wm, registry) = test_worker_manager();
        let wm = std::sync::Arc::new(wm);
        // One-worker generation: device_plan.len() == 1.
        registry
            .register(
                "stale_m",
                "1",
                crate::config::ModelConfig::default(),
                ModelType::LitAPI,
                std::path::PathBuf::new(),
            )
            .unwrap();
        registry.mark_ready("stale_m", "1").unwrap();

        // worker_id 5 is valid for no generation of this model — the shape
        // of a signal queued before a reload shrank the worker count.
        let result = wm
            .respawn_worker("stale_m", "1", 5, "health_check", false)
            .await;
        assert!(
            result.is_err(),
            "an out-of-range worker_id must fail the respawn, not panic the listener"
        );
    }

    /// B14: an overlong single line is truncated at the cap (host memory
    /// stays bounded) and the stream resumes at the NEXT line.
    #[tokio::test]
    async fn read_stderr_line_bounded_caps_overlong_lines() {
        let cap = 64;
        let huge = "x".repeat(1024 * 1024);
        let input = format!("{huge}\nshort line\nnext\n");
        let mut cursor = std::io::Cursor::new(input.into_bytes());
        let mut buf = Vec::new();

        let n = read_stderr_line_bounded(&mut cursor, &mut buf, cap)
            .await
            .unwrap();
        assert!(n > 0);
        assert!(
            buf.len() <= cap + b"...[truncated]".len(),
            "the buffered line must stay bounded, got {} bytes",
            buf.len()
        );
        assert!(
            buf.ends_with(b"...[truncated]"),
            "an overlong line carries the truncation marker"
        );

        // The discard consumed through the newline: the next read is the
        // following line, not the remainder of the huge one.
        read_stderr_line_bounded(&mut cursor, &mut buf, cap)
            .await
            .unwrap();
        assert_eq!(buf, b"short line\n");
        read_stderr_line_bounded(&mut cursor, &mut buf, cap)
            .await
            .unwrap();
        assert_eq!(buf, b"next\n");
    }

    /// B14: a line exactly at the cap (newline inside the window) is not
    /// touched.
    #[tokio::test]
    async fn read_stderr_line_bounded_keeps_exact_fit() {
        let cap = 8;
        let input = b"12345678\n".to_vec(); // 8 bytes + newline
        let mut cursor = std::io::Cursor::new(input);
        let mut buf = Vec::new();
        read_stderr_line_bounded(&mut cursor, &mut buf, cap)
            .await
            .unwrap();
        assert_eq!(buf, b"12345678\n");
    }

    /// B8 (leak-gap-audit-0821): the re-warm generation check — true only
    /// when the registry's CURRENT slot entry names the same pid.
    #[test]
    fn slot_occupied_by_matches_only_the_same_generation_pid() {
        let registry = crate::registry::ModelRegistry::new();
        registry
            .register(
                "m",
                "1",
                crate::config::ModelConfig::default(),
                ModelType::LitAPI,
                std::path::PathBuf::new(),
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
                    pid: Some(2222),
                    status: crate::registry::types::WorkerStatus::Ready,
                    capacity: None,
                }],
            )
            .unwrap();

        assert!(
            slot_occupied_by(&registry, "m", "1", 0, Some(2222)),
            "same generation: the replacement still owns the slot"
        );
        assert!(
            !slot_occupied_by(&registry, "m", "1", 0, Some(1111)),
            "a stale re-warm (old pid) must not act on the new generation"
        );
        assert!(
            !slot_occupied_by(&registry, "m", "2", 0, Some(2222)),
            "entry gone (unload) → not ours"
        );
        assert!(
            !slot_occupied_by(&registry, "m", "1", 1, Some(2222)),
            "a different slot → not ours"
        );
    }

    /// B5 (leak-gap-audit-0821): after a reap-timeout respawn the OLD worker
    /// eventually dies and its load-time monitor fires a LATE exit event.
    /// The replacement already occupies the slot — same shared ZMQ client,
    /// same shared OutlierState, registry entry carrying the NEW pid. The
    /// stale event must be discarded: fail_all would kill the replacement's
    /// in-flight requests, mark_dead has no recovery path except another
    /// respawn (permanent 503 NoLiveWorkers for single-worker versions,
    /// since successful health probes never clear `dead`), and
    /// mark_worker_stopped would flip the live replacement's entry.
    #[tokio::test]
    async fn late_exit_from_previous_slot_occupant_does_not_poison_replacement() {
        let registry = Arc::new(crate::registry::ModelRegistry::new());
        registry
            .register(
                "m",
                "1",
                crate::config::ModelConfig::default(),
                ModelType::LitAPI,
                std::path::PathBuf::new(),
            )
            .unwrap();
        registry.mark_ready("m", "1").unwrap();
        // The replacement currently occupies slot 0 with pid 2222.
        registry
            .set_workers(
                "m",
                "1",
                vec![crate::registry::types::WorkerInfo {
                    worker_id: 0,
                    device: "cpu:0".to_string(),
                    endpoint: String::new(),
                    pid: Some(2222),
                    status: crate::registry::types::WorkerStatus::Ready,
                    capacity: None,
                }],
            )
            .unwrap();
        let outlier = Arc::new(crate::inference_queue::OutlierState::new(1));

        // The OLD worker's load-time handler, created for pid 1111, fires late.
        let handler = crash_exit_handler(
            None,
            Some(outlier.clone()),
            registry.clone(),
            "m".to_string(),
            "1".to_string(),
            0,
            Some(1111),
        );
        handler(WorkerExitKind::Killed);

        assert!(
            !outlier.is_dead(0),
            "a stale exit event must not mark the replacement's slot dead"
        );
        let workers = registry.get("m", Some("1")).unwrap().workers;
        assert_eq!(
            workers[0].status,
            crate::registry::types::WorkerStatus::Ready,
            "a stale exit event must not flip the replacement's registry entry"
        );
        assert_eq!(workers[0].pid, Some(2222), "the replacement's pid must stay");

        // Sanity: the CURRENT occupant's own exit event still acts.
        let outlier2 = Arc::new(crate::inference_queue::OutlierState::new(1));
        let handler2 = crash_exit_handler(
            None,
            Some(outlier2.clone()),
            registry.clone(),
            "m".to_string(),
            "1".to_string(),
            0,
            Some(2222),
        );
        handler2(WorkerExitKind::Crash);
        assert!(
            outlier2.is_dead(0),
            "the current occupant's crash must still mark the slot dead"
        );
    }

    /// B7 (leak-gap-audit-0821): an unload landing while a respawn's
    /// replacement is still in setup() must not let the respawn re-create
    /// torn-down state (custom-metric refs/families, route entries) for the
    /// dead version. Deterministic: the unload runs while the replacement
    /// is blocked in its slow setup(), so registration would happen AFTER
    /// the unload completed.
    #[cfg(unix)]
    #[tokio::test]
    #[serial_test::serial(custom_metrics)]
    async fn unload_landing_mid_respawn_leaves_no_metric_residue() {
        let key = "respawnrace_gauge:gauge";
        assert!(
            !crate::metrics::prometheus::custom_family_registered_for_test(key),
            "precondition: respawnrace_gauge must be unregistered"
        );
        let tag = "respawnrace";
        let repo =
            std::env::temp_dir().join(format!("lite-server-respawnrace-{}", std::process::id()));
        let model_dir = repo.join(tag).join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(
            model_dir.join("model.py"),
            r#"import os
import time
from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        self.register_metric("respawnrace_gauge", "gauge")
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
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\n",
        )
        .unwrap();

        let registry = std::sync::Arc::new(crate::registry::ModelRegistry::new());
        let wm = std::sync::Arc::new(
            WorkerManager::new(
                registry.clone(),
                repo.clone(),
                std::sync::Arc::new(crate::inference_queue::InferenceQueue::new()),
                "error".to_string(),
                std::sync::Arc::new(crate::callback::CallbackRunner::new()),
            )
            .with_custom_metrics(true),
        );
        let config = crate::config::ModelConfig {
            startup_timeout: 60.0,
            worker_kill_timeout: 1.0,
            ..Default::default()
        };
        wm.load_model(tag, "1", &config)
            .await
            .expect("load must succeed");
        // The load registered the worker's custom metric.
        assert!(
            crate::metrics::prometheus::custom_family_registered_for_test(key),
            "setup: load registered the custom metric"
        );
        let pids_seen = || {
            std::fs::read_dir(&model_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .into_string()
                        .unwrap()
                        .starts_with("setup_pid_")
                })
                .count()
        };

        // Start a kill-mode respawn; wait for the REPLACEMENT's setup marker
        // (a second pid file), then unload mid-setup.
        let wm_respawn = wm.clone();
        let mut respawn = tokio::spawn(async move {
            wm_respawn
                .respawn_worker(tag, "1", 0, "health_check", false)
                .await
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if pids_seen() >= 2 {
                break;
            }
            if respawn.is_finished() {
                let r = (&mut respawn).await.expect("respawn task panicked");
                let contents: Vec<_> = std::fs::read_dir(&model_dir)
                    .unwrap()
                    .filter_map(|e| e.ok().map(|x| x.file_name()))
                    .collect();
                panic!(
                    "respawn finished before the replacement reached setup: {r:?}; dir: {contents:?}"
                );
            }
            assert!(
                std::time::Instant::now() < deadline,
                "replacement never reached setup()"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        wm.unload_version(tag, "1")
            .await
            .expect("unload mid-respawn must succeed");

        let result = (&mut respawn).await.expect("respawn task panicked");
        assert!(
            result.is_err(),
            "the respawn must not report success for a version unloaded underneath it"
        );
        assert!(
            !crate::metrics::prometheus::custom_family_registered_for_test(key),
            "B7: the mid-respawn unload must not leave re-registered metric state"
        );
        assert!(
            wm.route_table.read().await.get(&model_version_key(tag, "1")).is_none(),
            "B7: no route-table residue for the torn-down version"
        );

        // No worker of this model may survive (registered or not).
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
                assert!(!alive, "worker pid {pid} survived the mid-respawn unload");
            }
        }
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// Device-placement harness: an echo model whose `setup` appends the
    /// device string it received to a marker file, so tests can assert which
    /// device each spawned worker (initial or replacement) landed on.
    /// Returns (manager, repo, marker).
    #[cfg(unix)]
    async fn device_marker_harness(
        tag: &str,
        devices: serde_json::Value,
        continuous_batching: bool,
    ) -> (WorkerManager, std::path::PathBuf, std::path::PathBuf) {
        let repo =
            std::env::temp_dir().join(format!("lite-server-devplan-{tag}-{}", std::process::id()));
        let model_dir = repo.join(tag).join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        let marker = repo.join("devices.txt");
        std::fs::write(
            model_dir.join("model.py"),
            format!(
                r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        with open({marker:?}, "a") as f:
            f.write(device + "\n")

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {{"output": x}}

    def encode_response(self, output):
        return output
"#,
                marker = marker.display().to_string()
            ),
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\n",
        )
        .unwrap();

        let registry = std::sync::Arc::new(crate::registry::ModelRegistry::new());
        let wm = WorkerManager::new(
            registry,
            repo.clone(),
            std::sync::Arc::new(crate::inference_queue::InferenceQueue::new()),
            "debug".to_string(),
            std::sync::Arc::new(crate::callback::CallbackRunner::new()),
        );
        let config = crate::config::ModelConfig {
            devices: Some(devices),
            continuous_batching,
            ..Default::default()
        };
        wm.load_model(tag, "1", &config)
            .await
            .expect("load must succeed");
        (wm, repo, marker)
    }

    /// Device placement (list form): a respawned worker must land back on
    /// the device its slot started on — the respawn path resolves through
    /// the same plan as the initial spawn.
    #[cfg(unix)]
    #[tokio::test]
    async fn respawn_keeps_slot_device_for_list_form() {
        let (wm, _repo, marker) =
            device_marker_harness("devlist", serde_json::json!([0, 1]), false).await;
        let wm = std::sync::Arc::new(wm);
        // Load spawned worker 0 → cpu:0, worker 1 → cpu:1.
        wm.respawn_worker("devlist", "1", 1, "health_check", false)
            .await
            .expect("respawn must succeed");
        let recorded = std::fs::read_to_string(&marker).unwrap();
        let lines: Vec<&str> = recorded.lines().collect();
        assert_eq!(
            lines,
            vec!["cpu:0", "cpu:1", "cpu:1"],
            "the replacement for worker 1 must be spawned on cpu:1 again"
        );
    }

    /// Device placement (map form): explicit per-device counts survive a
    /// respawn — the replacement keeps its slot's device.
    #[cfg(unix)]
    #[tokio::test]
    async fn respawn_keeps_slot_device_for_map_form() {
        let (wm, _repo, marker) =
            device_marker_harness("devmap", serde_json::json!({ "1": 2 }), false).await;
        let wm = std::sync::Arc::new(wm);
        // Load spawned workers 0 and 1, both on cpu:1.
        wm.respawn_worker("devmap", "1", 0, "health_check", false)
            .await
            .expect("respawn must succeed");
        let recorded = std::fs::read_to_string(&marker).unwrap();
        let lines: Vec<&str> = recorded.lines().collect();
        assert_eq!(
            lines,
            vec!["cpu:1", "cpu:1", "cpu:1"],
            "the replacement for worker 0 must be spawned on cpu:1 again"
        );
    }

    /// Device placement under continuous_batching: per-device counts are
    /// forced to one worker per unique device (mirroring the
    /// workers_per_device=1 force), so a map count of 2 yields ONE worker.
    #[cfg(unix)]
    #[tokio::test]
    async fn cb_load_forces_one_worker_per_unique_device() {
        let (_wm, _repo, marker) =
            device_marker_harness("devcb", serde_json::json!({ "0": 2 }), true).await;
        let recorded = std::fs::read_to_string(&marker).unwrap();
        let lines: Vec<&str> = recorded.lines().collect();
        assert_eq!(
            lines,
            vec!["cpu:0"],
            "continuous_batching must spawn exactly one worker for the unique device"
        );
    }

    /// G2 harness: a temp repo with an echo model whose `predict` appends one
    /// marker line per call, so tests can count how many warmup inferences a
    /// worker process actually ran. Returns (manager, registry, repo, marker).
    #[cfg(unix)]
    async fn rewarm_harness(
        tag: &str,
        iterations: u32,
    ) -> (
        WorkerManager,
        std::sync::Arc<crate::registry::ModelRegistry>,
        std::path::PathBuf,
        std::path::PathBuf,
    ) {
        let repo =
            std::env::temp_dir().join(format!("lite-server-g2-{tag}-{}", std::process::id()));
        let model_dir = repo.join(tag).join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        let marker = repo.join("marker.txt");
        std::fs::write(
            model_dir.join("model.py"),
            format!(
                r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        with open({marker:?}, "a") as f:
            f.write("w\n")
        return {{"output": x}}

    def encode_response(self, output):
        return output
"#,
                marker = marker.display().to_string()
            ),
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();
        std::fs::write(model_dir.join("warmup_input.json"), "{\"input\": 7}").unwrap();

        let registry = std::sync::Arc::new(crate::registry::ModelRegistry::new());
        let wm = WorkerManager::new(
            registry.clone(),
            repo.clone(),
            std::sync::Arc::new(crate::inference_queue::InferenceQueue::new()),
            "debug".to_string(),
            std::sync::Arc::new(crate::callback::CallbackRunner::new()),
        );
        let mut config = crate::config::ModelConfig::default();
        config.policies.warmup = Some(crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup_input.json".to_string(),
                iterations,
                ..Default::default()
            }],
            timeout_secs: 30.0,
            ..Default::default()
        });
        wm.load_model(tag, "1", &config)
            .await
            .expect("load with a valid warmup sample must succeed");
        (wm, registry, repo, marker)
    }

    /// G2: a respawned replacement is a cold process — with warmup enabled it
    /// must be re-warmed (pinned to its own slot) BEFORE the version returns
    /// to Ready, so post-recovery traffic never pays the cold-start cost.
    #[cfg(unix)]
    #[tokio::test]
    async fn respawn_rewarms_replacement_worker() {
        let warmups = || {
            crate::metrics::prometheus::MODEL_WARMUP_TOTAL
                .with_label_values(&["g2_rewarm", "1", "success"])
                .get()
        };
        let w0 = warmups();
        let (wm, registry, repo, marker) = rewarm_harness("g2_rewarm", 2).await;
        let wm = std::sync::Arc::new(wm);
        assert_eq!(
            warmups(),
            w0 + 1.0,
            "G6: the load-time warmup must be counted"
        );
        let lines = |p: &std::path::Path| {
            std::fs::read_to_string(p)
                .unwrap_or_default()
                .lines()
                .count()
        };
        assert_eq!(
            lines(&marker),
            2,
            "load warmup: 1 worker x 1 sample x 2 iterations"
        );

        wm.respawn_worker("g2_rewarm", "1", 0, "health_check", false)
            .await
            .expect("respawn must succeed");

        // F4: the re-warm runs as a background task (the respawn listener is
        // a global serial loop) — poll for its effects instead of expecting
        // them synchronously.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while (lines(&marker) < 4 || warmups() < w0 + 2.0)
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            warmups(),
            w0 + 2.0,
            "G6: the respawn re-warm must be counted"
        );
        assert_eq!(
            lines(&marker),
            4,
            "respawn must re-warm the cold replacement (2 more units)"
        );
        let status = registry.get("g2_rewarm", Some("1")).map(|mv| mv.status);
        assert_eq!(
            status,
            Some(crate::registry::types::VersionStatus::Ready),
            "a successful re-warm returns the version to Ready"
        );
        let outlier = wm
            .get_outlier_state("g2_rewarm", "1")
            .await
            .expect("outlier state");
        assert!(
            !outlier.is_ejected(0) && !outlier.is_dead(0),
            "re-warmed replacement must be back in rotation"
        );

        let _ = wm.unload_model("g2_rewarm", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// F4 (warmup-gaps audit): the respawn listener is a GLOBAL serial loop —
    /// a re-warm running inline would stall crash recovery for every other
    /// model (the re-warm has no default total budget). The respawn must
    /// return with the re-warm still running in the background; the cold
    /// slot is then force-ejected after the fact if the re-warm fails.
    #[cfg(unix)]
    #[tokio::test]
    async fn respawn_returns_before_rewarm_completes() {
        let repo =
            std::env::temp_dir().join(format!("lite-server-g2-async-{}", std::process::id()));
        let model_dir = repo.join("g2_async").join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        let marker = repo.join("marker.txt");
        // Sleep BEFORE the marker write: a completed re-warm is exactly 2 new
        // lines; an in-flight one is fewer.
        std::fs::write(
            model_dir.join("model.py"),
            format!(
                r#"from lite_server import LitAPI
import asyncio


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    async def predict(self, x):
        await asyncio.sleep(1.0)
        with open({marker:?}, "a") as f:
            f.write("w\n")
        return {{"output": x}}

    def encode_response(self, output):
        return output
"#,
                marker = marker.display().to_string()
            ),
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();
        std::fs::write(model_dir.join("warmup_input.json"), "{\"input\": 7}").unwrap();

        let registry = std::sync::Arc::new(crate::registry::ModelRegistry::new());
        let wm = std::sync::Arc::new(WorkerManager::new(
            registry.clone(),
            repo.clone(),
            std::sync::Arc::new(crate::inference_queue::InferenceQueue::new()),
            "debug".to_string(),
            std::sync::Arc::new(crate::callback::CallbackRunner::new()),
        ));
        let mut config = crate::config::ModelConfig::default();
        config.policies.warmup = Some(crate::config::WarmupPolicy {
            enabled: true,
            samples: vec![crate::config::WarmupSample {
                input_ref: "warmup_input.json".to_string(),
                iterations: 2,
                ..Default::default()
            }],
            timeout_secs: 30.0,
            ..Default::default()
        });
        wm.load_model("g2_async", "1", &config)
            .await
            .expect("load with a valid warmup sample must succeed");
        let lines = || {
            std::fs::read_to_string(&marker)
                .unwrap_or_default()
                .lines()
                .count()
        };
        assert_eq!(lines(), 2, "load warmup: 2 iterations");

        wm.respawn_worker("g2_async", "1", 0, "health_check", false)
            .await
            .expect("respawn must succeed");

        assert!(
            lines() < 4,
            "respawn must NOT block on the re-warm (2 x 1s units), got {} lines",
            lines()
        );
        // The background re-warm still completes.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while lines() < 4 && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(lines(), 4, "the background re-warm must finish");

        let _ = wm.unload_model("g2_async", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// G2: when the re-warm fails (sample file unreadable), the cold
    /// replacement must LEAVE rotation — force-ejected, so the coordinator
    /// keeps the version Degraded and the circuit's half-open probe can still
    /// re-warm it lazily on later real traffic. The failure is counted and
    /// the version is never marked Failed (runtime recovery ≠ load failure).
    #[cfg(unix)]
    #[tokio::test]
    async fn respawn_warmup_failure_force_ejects_replacement() {
        let (wm, registry, repo, _marker) = rewarm_harness("g2_rewarm_fail", 1).await;
        let wm = std::sync::Arc::new(wm);
        // Break the sample AFTER load so only the re-warm fails reading it.
        std::fs::remove_file(
            repo.join("g2_rewarm_fail")
                .join("1")
                .join("warmup_input.json"),
        )
        .unwrap();
        let labels = &["g2_rewarm_fail", "1", "warmup"];
        let before = crate::metrics::prometheus::WORKER_RESPAWN_FAILURES_TOTAL
            .with_label_values(labels)
            .get();

        wm.respawn_worker("g2_rewarm_fail", "1", 0, "health_check", false)
            .await
            .expect("the respawn itself succeeded — only the re-warm failed");

        // F4: the re-warm (and thus the force-eject) runs in a background
        // task — poll for the ejection.
        let outlier = wm
            .get_outlier_state("g2_rewarm_fail", "1")
            .await
            .expect("outlier state");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !outlier.is_ejected(0) && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            outlier.is_ejected(0),
            "cold replacement must be ejected from rotation"
        );
        assert!(
            !outlier.is_dead(0),
            "ejected, not dead: half-open recovery must stay possible"
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut after;
        let mut status;
        loop {
            after = crate::metrics::prometheus::WORKER_RESPAWN_FAILURES_TOTAL
                .with_label_values(labels)
                .get();
            status = registry
                .get("g2_rewarm_fail", Some("1"))
                .map(|mv| mv.status);
            if (after == before + 1.0
                && status == Some(crate::registry::types::VersionStatus::Degraded))
                || std::time::Instant::now() >= deadline
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(after, before + 1.0, "re-warm failure must be counted");
        assert_eq!(
            status,
            Some(crate::registry::types::VersionStatus::Degraded),
            "re-warm failure leaves the version Degraded, never Failed"
        );

        let _ = wm.unload_model("g2_rewarm_fail", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// Graceful-respawn harness: an echo model whose `teardown` runs
    /// `teardown_body`, so tests can tell a graceful stop (teardown ran)
    /// from a SIGKILL (no teardown). Returns (manager, registry, repo, marker).
    #[cfg(unix)]
    async fn teardown_marker_harness(
        tag: &str,
        teardown_body: &str,
        kill_timeout: f32,
    ) -> (
        std::sync::Arc<WorkerManager>,
        std::sync::Arc<crate::registry::ModelRegistry>,
        std::path::PathBuf,
        std::path::PathBuf,
    ) {
        let repo =
            std::env::temp_dir().join(format!("lite-server-grace-{tag}-{}", std::process::id()));
        let model_dir = repo.join(tag).join("1");
        std::fs::create_dir_all(&model_dir).unwrap();
        let marker = repo.join("teardown.txt");
        // `{marker}` in teardown_body expands to the absolute marker path.
        let teardown_body = teardown_body.replace("{marker}", &marker.display().to_string());
        std::fs::write(
            model_dir.join("model.py"),
            format!(
                r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {{"output": x}}

    def encode_response(self, output):
        return output

    def teardown(self):
        {teardown_body}
"#,
            ),
        )
        .unwrap();
        std::fs::write(
            model_dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();

        let registry = std::sync::Arc::new(crate::registry::ModelRegistry::new());
        let wm = std::sync::Arc::new(WorkerManager::new(
            registry.clone(),
            repo.clone(),
            std::sync::Arc::new(crate::inference_queue::InferenceQueue::new()),
            "error".to_string(),
            std::sync::Arc::new(crate::callback::CallbackRunner::new()),
        ));
        let config = crate::config::ModelConfig {
            worker_kill_timeout: kill_timeout,
            ..Default::default()
        };
        wm.load_model(tag, "1", &config)
            .await
            .expect("load must succeed");
        (wm, registry, repo, marker)
    }

    /// Rolling recycle: a graceful respawn must send the ZMQ stop message and
    /// wait for the old worker, so it runs its teardown hook and exits
    /// cleanly instead of being SIGKILLed.
    #[cfg(unix)]
    #[tokio::test]
    async fn should_run_teardown_hook_in_graceful_respawn() {
        let (wm, registry, repo, marker) = teardown_marker_harness(
            "grace_stop",
            "open(r'{marker}', 'a').write('t\\n')",
            10.0,
        )
        .await;

        wm.respawn_worker("grace_stop", "1", 0, "rolling_recycle", true)
            .await
            .expect("graceful respawn must succeed");

        assert!(
            marker.exists(),
            "graceful respawn must let the old worker run teardown"
        );
        let status = registry.get("grace_stop", Some("1")).map(|mv| mv.status);
        assert_eq!(
            status,
            Some(crate::registry::types::VersionStatus::Ready),
            "a successful graceful respawn returns the version to Ready"
        );

        let _ = wm.unload_model("grace_stop", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// Kill mode (health-check escalation) must keep its kill-only teardown:
    /// no stop message, no teardown hook — the worker is SIGKILLed.
    #[cfg(unix)]
    #[tokio::test]
    async fn should_not_run_teardown_hook_in_kill_respawn() {
        let (wm, _registry, repo, marker) = teardown_marker_harness(
            "kill_mode",
            "open(r'{marker}', 'a').write('t\\n')",
            10.0,
        )
        .await;

        wm.respawn_worker("kill_mode", "1", 0, "health_check", false)
            .await
            .expect("kill-mode respawn must succeed");

        assert!(
            !marker.exists(),
            "kill-mode respawn must not send a stop message (no teardown)"
        );

        let _ = wm.unload_model("kill_mode", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// A worker hung inside teardown must not stall a graceful respawn:
    /// after kill_timeout the monitor SIGKILLs it and the replacement still
    /// comes up (the stale-stop replay hazard is warn-logged, not fatal).
    #[cfg(unix)]
    #[tokio::test]
    async fn should_escalate_to_sigkill_when_graceful_stop_hangs() {
        let (wm, registry, repo, _marker) = teardown_marker_harness(
            "hang_stop",
            "import time; time.sleep(3600)",
            1.0,
        )
        .await;

        timeout(
            Duration::from_secs(30),
            wm.respawn_worker("hang_stop", "1", 0, "rolling_recycle", true),
        )
        .await
        .expect("graceful respawn must not stall on a hung worker")
        .expect("respawn must succeed after SIGKILL escalation");

        let status = registry.get("hang_stop", Some("1")).map(|mv| mv.status);
        assert_eq!(
            status,
            Some(crate::registry::types::VersionStatus::Ready),
            "the replacement must come up after the hung worker is killed"
        );

        let _ = wm.unload_model("hang_stop", Some("1")).await;
        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn test_respawn_channel_creation() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::inference_queue::RespawnSignal>(8);
        tx.send(crate::inference_queue::RespawnSignal {
            model_name: "m".to_string(),
            version: "1".to_string(),
            worker_id: 0,
            reason: "health_check",
            graceful: false,
        })
        .await
        .unwrap();
        let sig = rx.recv().await.unwrap();
        assert_eq!(sig.model_name, "m");
        assert_eq!(sig.worker_id, 0);
        assert_eq!(sig.reason, "health_check");
    }

    #[tokio::test]
    async fn test_monitor_exits_on_shutdown() {
        let mut cmd = Command::new("python");
        cmd.arg("-c").arg("import time; time.sleep(60)");
        let child = cmd
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let done = Arc::new(AtomicBool::new(false));
        let done_c = done.clone();

        spawn_worker_monitor(
            child,
            "test",
            "1",
            0,
            shutdown_rx,
            move |_kind| {
                done_c.store(true, Ordering::SeqCst);
            },
            None,
            Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            Arc::new(AtomicBool::new(false)),
        );

        // Send shutdown
        shutdown_tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            done.load(Ordering::SeqCst),
            "monitor should exit on shutdown signal"
        );
    }

    // ===== Orphan prevention: kill completion signal =====

    /// Regression: shutdown/unload must not return before the worker process
    /// is actually killed+reaped. Otherwise the child is orphaned and its ZMQ
    /// socket auto-reconnect steals the re-bound socket after a restart.
    #[tokio::test]
    async fn test_monitor_signals_completion_after_shutdown_kill() {
        let mut cmd = Command::new("python");
        cmd.arg("-c").arg("import time; time.sleep(60)");
        let child = cmd
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        #[cfg(unix)]
        let pid = child.id().unwrap() as i32;

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let done_rx = spawn_worker_monitor(
            child,
            "test",
            "1",
            0,
            shutdown_rx,
            |_kind| {},
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
            assert!(
                !alive,
                "worker process {} should be dead once completion fires",
                pid
            );
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
            child,
            "test",
            "1",
            0,
            shutdown_rx,
            |_kind| {},
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
        assert!(
            !alive,
            "dropped worker process {} should be killed via kill_on_drop",
            pid
        );
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
        assert_eq!(
            child_pgid, child_pid,
            "worker should be process group leader (PGID == PID)"
        );
        assert_ne!(
            child_pgid, server_pgid,
            "worker process group {} must differ from server process group {}",
            child_pgid, server_pgid
        );

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
        let got = drain_worker_stderr_bounded(rx, Duration::from_millis(50), 64 * 1024).await;
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
        assert_eq!(
            classify_stderr_line("[DEBUG] debug stuff"),
            tracing::Level::DEBUG
        );
        // Traceback lines (indented) should be DEBUG so they don't spam unless
        // the user wants full verbosity.
        assert_eq!(
            classify_stderr_line("  File \"x.py\", line 1"),
            tracing::Level::DEBUG
        );
        assert_eq!(
            classify_stderr_line("Traceback (most recent call last):"),
            tracing::Level::DEBUG
        );
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
