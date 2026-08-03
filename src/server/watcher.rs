use super::scanner::matches_patterns;
use crate::error::AppError;
use crate::registry::ModelRegistry;
use crate::worker::WorkerManager;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, Instant};
use tracing::{debug, error, info, warn};

/// File-event kind preserved through the watcher's debounce.
///
/// Only ``Create`` carries lifecycle signal — a directory appearing in the
/// repo announces a new version; ``Modify``/``Remove``/``Other`` inside an
/// existing directory are ordinary file churn. Renames count as
/// ``Create``: moving a directory into the repo (the atomic-deploy
/// pattern) is reported by notify as ``Modify(Name(..))`` — IN_MOVED_TO
/// on Linux, ItemRenamed under FSEvents — not as ``Create``.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(super) enum WatchEventKind {
    Create,
    Modify,
    Remove,
    Other,
}

impl From<notify::EventKind> for WatchEventKind {
    fn from(kind: notify::EventKind) -> Self {
        match kind {
            notify::EventKind::Create(_) => WatchEventKind::Create,
            notify::EventKind::Modify(notify::event::ModifyKind::Name(_)) => WatchEventKind::Create,
            notify::EventKind::Modify(_) => WatchEventKind::Modify,
            notify::EventKind::Remove(_) => WatchEventKind::Remove,
            _ => WatchEventKind::Other,
        }
    }
}

/// Process a debounced batch of file events.
///
/// Classification is registry-based, not directory-shape-based:
/// - events on a **live** version (loaded AND its dir still exists) are
///   file-level: the hot-reload path (gate + patterns + cooldown) restarts
///   the worker;
/// - everything else (unknown version, or the version dir is gone) is a
///   lifecycle event. In auto mode (`reconcile_trigger` = Some) these only
///   trigger a reconcile — the reconciler is the single authority on version
///   load/unload, applying load_policy and max_loaded_versions. In manual
///   mode the user is the lifecycle owner: only a **directory Create** event
///   announces a new version (warn log — never auto-loaded); file edits
///   inside an existing unloaded version dir are ordinary dev churn
///   (debug log). A version dir that disappeared is unloaded directly in
///   every mode — the files are gone, the worker cannot serve.
pub(super) async fn process_watch_events(
    events: Vec<(PathBuf, WatchEventKind)>,
    repo_path: PathBuf,
    worker_manager: Arc<WorkerManager>,
    registry: Arc<ModelRegistry>,
    last_reload: &mut std::collections::HashMap<(String, String), Instant>,
    server_tunables: &crate::config::ServerTunables,
    has_hot_reload: &AtomicBool,
    reconcile_trigger: &Option<mpsc::Sender<()>>,
) -> Result<(), AppError> {
    use std::collections::HashSet;

    // Map from (model, version) to the set of changed files for that model
    let mut models_to_reload: HashSet<(String, String)> = HashSet::new();
    let mut trigger_files: std::collections::HashMap<(String, String), Vec<PathBuf>> =
        std::collections::HashMap::new();
    let mut lifecycle_candidates: HashSet<(String, String)> = HashSet::new();
    // Keys whose event batch contains a directory Create — the signal that a
    // genuinely new version dir appeared (vs. churn inside an existing one).
    let mut new_version_candidates: HashSet<(String, String)> = HashSet::new();

    for (path, kind) in events {
        // Skip endpoint files and Python cache
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with("_endpoint.py") || name == "__pycache__" || name.ends_with(".pyc") || name.ends_with(".pyo") {
                continue;
            }
        }
        if path.components().any(|c| c.as_os_str() == "__pycache__") {
            continue;
        }

        // Try to extract model name and version from path
        let strip_result = path.strip_prefix(&repo_path);
        if let Ok(relative) = strip_result {
            let components: Vec<std::path::Component> = relative.components().collect();
            if components.len() >= 2 {
                // Expected: model_name/version/file
                if let (Some(model_name), Some(version)) = (
                    components.first().and_then(|c| match c {
                        std::path::Component::Normal(s) => s.to_str(),
                        _ => None,
                    }),
                    components.get(1).and_then(|c| match c {
                        std::path::Component::Normal(s) => s.to_str(),
                        _ => None,
                    }),
                ) {
                    if crate::validation::validate_identifier(model_name).is_err()
                        || crate::validation::validate_identifier(version).is_err()
                    {
                        continue;
                    }
                    let key = (model_name.to_string(), version.to_string());
                    let version_dir = repo_path.join(model_name).join(version);
                    let is_live = registry.get(model_name, Some(version)).is_some()
                        && version_dir.exists();
                    if path.is_dir() {
                        // Dir events on a live version (e.g. mtime changes
                        // when files are written inside) carry no signal.
                        if !is_live {
                            lifecycle_candidates.insert(key.clone());
                            // Only the creation of the version dir itself
                            // (components = [model, version]) announces a
                            // genuinely new version; Creates of nested
                            // subdirs and mtime/attr churn on an existing
                            // directory do not.
                            if kind == WatchEventKind::Create && components.len() == 2 {
                                new_version_candidates.insert(key);
                            }
                        }
                    } else if is_live {
                        models_to_reload.insert(key.clone());
                        trigger_files.entry(key).or_default().push(path.clone());
                    } else {
                        lifecycle_candidates.insert(key);
                    }
                }
            }
        }
    }

    // Reload changed models (with per-model/version cooldown)
    let cooldown = Duration::from_secs_f32(server_tunables.hot_reload_cooldown_secs);
    for (name, version) in models_to_reload {
        let key = (name.clone(), version.clone());
        if let Some(last) = last_reload.get(&key) {
            if Instant::now().duration_since(*last) < cooldown {
                info!("Hot reload: skipping {} version {} (cooldown)", name, version);
                continue;
            }
        }

        // P0: Check if hot_reload is enabled for this model version.
        // Classification guarantees the version is loaded; a concurrent
        // unload between classification and here is impossible on this task.
        if let Some(mv) = registry.get(&name, Some(&version)) {
            if !mv.config.hot_reload {
                debug!("Hot reload: skipping {} version {} (hot_reload=false)", name, version);
                continue;
            }

            // P1: Check hot_reload_patterns - if configured, only reload when matching files change
            if !mv.config.hot_reload_patterns.is_empty() {
                let empty: Vec<PathBuf> = Vec::new();
                let files = trigger_files.get(&key).unwrap_or(&empty);
                let any_match = files.iter().any(|f| {
                    let fname = f.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    matches_patterns(fname, &mv.config.hot_reload_patterns)
                });
                if !any_match {
                    debug!("Hot reload: skipping {} version {} (no pattern match)", name, version);
                    continue;
                }
            }

            last_reload.insert(key.clone(), Instant::now());
            // P3: give live workers a chance to refresh in-process via their
            // on_file_changed hook; fall back to a full worker restart
            // unless every worker of the version reports handled.
            let changed: Vec<String> = trigger_files
                .get(&key)
                .map(|fs| fs.iter().map(|f| f.to_string_lossy().into_owned()).collect())
                .unwrap_or_default();
            if worker_manager.notify_file_changed(&name, &version, &changed).await {
                info!(
                    "Hot reload: {} version {} refreshed in-process via on_file_changed (no restart)",
                    name, version
                );
            } else {
                info!("Hot reload: reloading {} version {}", name, version);
                if let Err(e) = worker_manager.reload_model(&name, Some(&version)).await {
                    warn!("Hot reload failed for {} version {}: {}", name, version, e);
                }
            }
        }
    }

    if lifecycle_candidates.is_empty() {
        return Ok(());
    }

    match reconcile_trigger {
        // Auto mode: the reconciler is the single authority on version
        // lifecycle. try_send coalesces naturally — a full channel means a
        // trigger is already pending.
        Some(tx) => {
            debug!(
                count = lifecycle_candidates.len(),
                "Lifecycle events detected, triggering reconcile"
            );
            let _ = tx.try_send(());
        }
        // Manual mode: the user is the lifecycle owner. New version dirs
        // are only reported — never auto-loaded (the legacy hot_reload
        // auto-load was removed in 0.7.7 after being deprecated in 0.7.6).
        None => {
            let mut had_unloads = false;
            for (name, version) in lifecycle_candidates {
                let version_dir = repo_path.join(&name).join(&version);
                if version_dir.exists() {
                    if new_version_candidates.contains(&(name.clone(), version.clone())) {
                        warn!(
                            "New version {} {} detected under manual control_mode — \
                             not loading; load explicitly via the Admin API or \
                             switch to control_mode: \"auto\"",
                            name, version
                        );
                    } else {
                        // Ordinary file churn inside a version dir that is
                        // not loaded (dev workflow) — traceable, not noisy.
                        debug!(
                            "File change in unloaded version {} {} — not loading \
                             (load via the Admin API or control_mode: \"auto\")",
                            name, version
                        );
                    }
                } else {
                    // Dir gone → unload. This invariant holds in every mode:
                    // the files no longer exist, the worker cannot serve.
                    had_unloads = true;
                    info!("Hot reload: auto-unloading removed model {} version {}", name, version);
                    let _ = worker_manager.unload_model(&name, Some(&version)).await;
                }
            }

            // P2: Re-check hot_reload flag after potential unloads
            if had_unloads {
                let any_hot_reload = registry.list_loaded().iter().any(|(_, _, mv)| mv.config.hot_reload);
                has_hot_reload.store(any_hot_reload, Ordering::Relaxed);
                if !any_hot_reload {
                    info!("Hot reload: no more enabled models, watcher will skip events");
                }
            }
        }
    }

    Ok(())
}
/// Start a file watcher on the model repository directory.
/// The `has_hot_reload` flag controls whether events are actually sent for processing.
/// When no models have hot_reload enabled, events are collected but not forwarded —
/// unless `always_forward` (control_mode = "auto"), where directory-level
/// lifecycle events feed the reconcile task regardless of hot_reload.
/// In manual mode events are also forwarded while the registry is non-empty:
/// a loaded model's version dir can disappear at any time and the
/// "dir gone → unload" invariant holds in every mode. Only an empty
/// registry (nothing to reload, nothing to unload) skips events.
pub(super) async fn start_file_watcher(
    repo_path: PathBuf,
    _worker_manager: Arc<WorkerManager>,
    tx: mpsc::Sender<Vec<(PathBuf, WatchEventKind)>>,
    has_hot_reload: Arc<AtomicBool>,
    always_forward: bool,
    debounce: Duration,
    registry: Arc<ModelRegistry>,
) {
    use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};

    let (notify_tx, mut notify_rx) = mpsc::channel::<notify::Result<Event>>(1024);

    let mut watcher: RecommendedWatcher = match Watcher::new(
        move |res: notify::Result<Event>| {
            let _ = notify_tx.blocking_send(res);
        },
        Config::default(),
    ) {
        Ok(w) => w,
        Err(e) => {
            error!("Failed to create file watcher: {}", e);
            return;
        }
    };

    if let Err(e) = watcher.watch(&repo_path, RecursiveMode::Recursive) {
        error!("Failed to watch repo path: {}", e);
        return;
    }

    info!("File watcher started on {}", repo_path.display());

    // Debounce: collect events over 1.5s windows
    let mut debounce_deadline: Option<Instant> = None;
    let mut pending_paths: Vec<(PathBuf, WatchEventKind)> = Vec::new();
    let mut tick = interval(Duration::from_millis(200));

    loop {
        tokio::select! {
            Some(res) = notify_rx.recv() => {
                match res {
                    Ok(event) => {
                        // P2: Skip collecting events if no hot_reload models
                        // (auto mode always forwards — lifecycle events feed
                        // the reconcile task). Manual mode must still forward
                        // while any model is loaded: a removed version dir is
                        // unloaded directly, in every mode. An empty registry
                        // means nothing can need reload or unload — skip.
                        if !always_forward
                            && !has_hot_reload.load(Ordering::Relaxed)
                            && registry.is_empty()
                        {
                            continue;
                        }

                        // The kind applies to every path in this event
                        // (notify delivers one kind per event batch).
                        let kind = WatchEventKind::from(event.kind);
                        for path in event.paths {
                            // Skip UDS sockets, temp files, hidden files, Python cache
                            if path.extension().map(|e| e == "sock").unwrap_or(false) {
                                continue;
                            }
                            if path.file_name()
                                .and_then(|n| n.to_str())
                                .map(|n| n.starts_with('.'))
                                .unwrap_or(false)
                            {
                                continue;
                            }
                            if path.file_name()
                                .and_then(|n| n.to_str())
                                .map(|n| n == "__pycache__" || n.ends_with(".pyc") || n.ends_with(".pyo"))
                                .unwrap_or(false)
                            {
                                continue;
                            }
                            pending_paths.push((path, kind));
                        }
                        debounce_deadline = Some(Instant::now() + debounce);
                    }
                    Err(e) => {
                        warn!("Watch error: {}", e);
                    }
                }
            }
            _ = tick.tick() => {
                if let Some(deadline) = debounce_deadline {
                    if Instant::now() >= deadline && !pending_paths.is_empty() {
                        // Deduplicate
                        pending_paths.sort();
                        pending_paths.dedup();
                        let paths: Vec<(PathBuf, WatchEventKind)> =
                            std::mem::take(&mut pending_paths);
                        let _ = tx.send(paths).await;
                        debounce_deadline = None;
                    }
                }
            }
            else => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callback::CallbackRunner;
    use crate::inference_queue::InferenceQueue;


    #[tokio::test]
    async fn test_file_watcher_aborts_cleanly() {
        let tmp_dir = std::env::temp_dir().join(format!("lite-server-fw-test-{}", std::process::id()));
        tokio::fs::create_dir_all(&tmp_dir).await.unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            tmp_dir.clone(),
            inference_queue,
            "warn".to_string(),
            callback_runner,
        ));

        let (tx, _rx) = mpsc::channel::<Vec<(PathBuf, WatchEventKind)>>(32);
        let has_hot_reload = Arc::new(AtomicBool::new(true));
        let handle = tokio::spawn(start_file_watcher(tmp_dir.clone(), worker_manager, tx, has_hot_reload, false, Duration::from_millis(2500), registry));

        // Give watcher time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        handle.abort();

        // Wait for abort to take effect
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert!(handle.is_finished());

        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    #[tokio::test]
    async fn test_file_watcher_detects_changes() {
        let tmp_dir_raw = std::env::temp_dir().join(format!("lite-server-fw-detect-{}", std::process::id()));
        tokio::fs::create_dir_all(&tmp_dir_raw).await.unwrap();
        let tmp_dir = tmp_dir_raw.canonicalize().unwrap();
        let sub_dir = tmp_dir.join("model").join("1");
        tokio::fs::create_dir_all(&sub_dir).await.unwrap();
        let model_py = sub_dir.join("model.py");
        tokio::fs::write(&model_py, "original").await.unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            tmp_dir.clone(),
            inference_queue,
            "warn".to_string(),
            callback_runner,
        ));

        let (tx, mut rx) = mpsc::channel::<Vec<(PathBuf, WatchEventKind)>>(32);
        let has_hot_reload = Arc::new(AtomicBool::new(true));
        let handle = tokio::spawn(start_file_watcher(tmp_dir.clone(), worker_manager, tx, has_hot_reload, false, Duration::from_millis(2500), registry));

        // Give watcher time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Modify the file
        tokio::fs::write(&model_py, "modified").await.unwrap();

        // Wait for event with timeout
        let paths = tokio::time::timeout(
            tokio::time::Duration::from_secs(10),
            rx.recv()
        ).await.expect("Timeout waiting for watcher event").expect("Channel closed");

        // Check if strip_prefix works
        for (p, _kind) in &paths {
            let _stripped = p.strip_prefix(&tmp_dir);
        }

        handle.abort();
        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    // ===== P1/P2: watcher lifecycle events → reconcile trigger (control_mode = "auto") =====

    use crate::config::ModelConfig;
    use crate::registry::types::ModelType;

    fn build_test_worker_manager(
        repo: PathBuf,
        registry: Arc<ModelRegistry>,
    ) -> Arc<WorkerManager> {
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(CallbackRunner::new());
        Arc::new(WorkerManager::new(
            registry,
            repo,
            inference_queue,
            "warn".to_string(),
            callback_runner,
        ))
    }

    fn test_repo_dir(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("lite-server-p12-{}-{}", tag, std::process::id()))
    }

    #[tokio::test]
    async fn auto_mode_new_version_triggers_reconcile_instead_of_loading() {
        let tmp = test_repo_dir("new");
        let version_dir = tmp.join("m").join("1");
        tokio::fs::create_dir_all(&version_dir).await.unwrap();
        tokio::fs::write(version_dir.join("config.yaml"), "hot_reload: true\n").await.unwrap();
        tokio::fs::write(version_dir.join("model.py"), "class M: pass").await.unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let wm = build_test_worker_manager(tmp.clone(), registry.clone());
        let (tx, mut rx) = mpsc::channel::<()>(8);
        let trigger = Some(tx);
        let mut last_reload = std::collections::HashMap::new();
        let flag = AtomicBool::new(true);

        process_watch_events(
            vec![(version_dir.join("config.yaml"), WatchEventKind::Create)],
            tmp.clone(),
            wm,
            registry.clone(),
            &mut last_reload,
            &crate::config::ServerTunables::default(),
            &flag,
            &trigger,
        ).await.unwrap();

        assert!(
            registry.get("m", Some("1")).is_none(),
            "auto mode: watcher must not load versions directly — reconcile decides"
        );
        assert!(
            rx.try_recv().is_ok(),
            "auto mode: lifecycle event must trigger reconcile"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn auto_mode_removed_version_triggers_reconcile_instead_of_unloading() {
        let tmp = test_repo_dir("removed");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        // Version dir does NOT exist (already deleted); registry still lists it.
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register("m", "1", ModelConfig::default(), ModelType::LitAPI, tmp.join("m").join("1"))
            .unwrap();
        let wm = build_test_worker_manager(tmp.clone(), registry.clone());
        let (tx, mut rx) = mpsc::channel::<()>(8);
        let trigger = Some(tx);
        let mut last_reload = std::collections::HashMap::new();
        let flag = AtomicBool::new(true);

        process_watch_events(
            vec![(tmp.join("m").join("1").join("model.py"), WatchEventKind::Remove)],
            tmp.clone(),
            wm,
            registry.clone(),
            &mut last_reload,
            &crate::config::ServerTunables::default(),
            &flag,
            &trigger,
        ).await.unwrap();

        assert!(
            registry.get("m", Some("1")).is_some(),
            "auto mode: watcher must not unload versions directly — reconcile decides"
        );
        assert!(
            rx.try_recv().is_ok(),
            "auto mode: removal must trigger reconcile"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn auto_mode_live_version_file_change_is_file_level_not_lifecycle() {
        let tmp = test_repo_dir("live");
        let version_dir = tmp.join("m").join("1");
        tokio::fs::create_dir_all(&version_dir).await.unwrap();
        let model_py = version_dir.join("model.py");
        tokio::fs::write(&model_py, "class M: pass").await.unwrap();

        // hot_reload=false: the file-level reload path is a no-op, so no
        // workers are spawned; the assertion targets the trigger channel.
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register("m", "1", ModelConfig::default(), ModelType::LitAPI, version_dir.clone())
            .unwrap();
        let wm = build_test_worker_manager(tmp.clone(), registry.clone());
        let (tx, mut rx) = mpsc::channel::<()>(8);
        let trigger = Some(tx);
        let mut last_reload = std::collections::HashMap::new();
        let flag = AtomicBool::new(true);

        process_watch_events(
            vec![(model_py, WatchEventKind::Modify)],
            tmp.clone(),
            wm,
            registry.clone(),
            &mut last_reload,
            &crate::config::ServerTunables::default(),
            &flag,
            &trigger,
        ).await.unwrap();

        assert!(
            rx.try_recv().is_err(),
            "file change on a live version must go down the hot-reload path, not trigger reconcile"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    #[tokio::test]
    async fn manual_mode_removed_version_unloads_directly() {
        // Regression: manual (non-auto) mode keeps the legacy direct-unload
        // behavior — the "dir gone → unload" invariant does not wait for a
        // reconciler that only runs at startup.
        let tmp = test_repo_dir("manual");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let registry = Arc::new(ModelRegistry::new());
        registry
            .register("m", "1", ModelConfig::default(), ModelType::LitAPI, tmp.join("m").join("1"))
            .unwrap();
        let wm = build_test_worker_manager(tmp.clone(), registry.clone());
        let trigger: Option<mpsc::Sender<()>> = None;
        let mut last_reload = std::collections::HashMap::new();
        let flag = AtomicBool::new(true);

        process_watch_events(
            vec![(tmp.join("m").join("1").join("model.py"), WatchEventKind::Remove)],
            tmp.clone(),
            wm,
            registry.clone(),
            &mut last_reload,
            &crate::config::ServerTunables::default(),
            &flag,
            &trigger,
        ).await.unwrap();

        assert!(
            registry.get("m", Some("1")).is_none(),
            "manual mode: removed version dir must be unloaded directly"
        );

        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // ===== P2: reconcile loop (tick resync + event trigger) =====

    #[tokio::test]
    async fn file_watcher_forwards_events_when_always_forward_without_hot_reload() {        // control_mode=auto: lifecycle events must reach the reconciler even
        // when no loaded model has hot_reload enabled.
        let tmp_dir_raw = std::env::temp_dir().join(format!("lite-server-fw-auto-{}", std::process::id()));
        tokio::fs::create_dir_all(&tmp_dir_raw).await.unwrap();
        let tmp_dir = tmp_dir_raw.canonicalize().unwrap();
        let sub_dir = tmp_dir.join("model").join("1");
        tokio::fs::create_dir_all(&sub_dir).await.unwrap();
        let model_py = sub_dir.join("model.py");
        tokio::fs::write(&model_py, "original").await.unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let worker_manager = build_test_worker_manager(tmp_dir.clone(), registry.clone());

        let (tx, mut rx) = mpsc::channel::<Vec<(PathBuf, WatchEventKind)>>(32);
        let has_hot_reload = Arc::new(AtomicBool::new(false));
        let handle = tokio::spawn(start_file_watcher(tmp_dir.clone(), worker_manager, tx, has_hot_reload, true, Duration::from_millis(2500), registry));

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        tokio::fs::write(&model_py, "modified").await.unwrap();

        let result = tokio::time::timeout(
            tokio::time::Duration::from_secs(10),
            rx.recv()
        ).await;
        assert!(
            result.is_ok(),
            "auto mode: watcher must forward events even with no hot_reload models"
        );

        handle.abort();
        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    /// B2 regression: manual mode (always_forward=false) with a loaded model
    /// whose hot_reload=false drops ALL file events at the watcher gate.
    /// This breaks the "dir gone → unload" invariant — if the user deletes a
    /// version directory, the watcher never forwards the event, so the model
    /// stays registered serving deleted files. The gate must forward whenever
    /// the registry is non-empty (something may need unloading); hot_reload
    /// gating stays per-model downstream.
    #[tokio::test]
    async fn manual_mode_watcher_forwards_events_when_models_loaded() {
        let tmp_dir_raw = std::env::temp_dir().join(format!(
            "lite-server-fw-manual-loaded-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp_dir_raw).await.unwrap();
        let tmp_dir = tmp_dir_raw.canonicalize().unwrap();
        let sub_dir = tmp_dir.join("model").join("1");
        tokio::fs::create_dir_all(&sub_dir).await.unwrap();
        let model_py = sub_dir.join("model.py");
        tokio::fs::write(&model_py, "original").await.unwrap();

        let registry = Arc::new(ModelRegistry::new());
        // Loaded model WITHOUT hot_reload — the gate must still forward so a
        // later dir removal can unload it.
        registry
            .register("model", "1", ModelConfig::default(), ModelType::LitAPI, sub_dir.clone())
            .unwrap();
        let worker_manager = build_test_worker_manager(tmp_dir.clone(), registry.clone());

        let (tx, mut rx) = mpsc::channel::<Vec<(PathBuf, WatchEventKind)>>(32);
        let has_hot_reload = Arc::new(AtomicBool::new(false));
        let handle = tokio::spawn(start_file_watcher(
            tmp_dir.clone(), worker_manager, tx, has_hot_reload,
            false, // always_forward = false (manual mode)
            Duration::from_millis(300),
            registry.clone(),
        ));

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        tokio::fs::write(&model_py, "modified").await.unwrap();

        let result = tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            rx.recv(),
        ).await;
        assert!(
            result.is_ok(),
            "manual mode with loaded models: watcher must forward events; \
             the 'dir gone → unload' invariant depends on this"
        );

        handle.abort();
        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    /// P2 counterpart: manual mode with an EMPTY registry keeps skipping
    /// events — nothing is loaded, so no model can need reload or unload.
    #[tokio::test]
    async fn manual_mode_watcher_skips_events_when_registry_empty() {
        let tmp_dir_raw = std::env::temp_dir().join(format!(
            "lite-server-fw-manual-empty-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&tmp_dir_raw).await.unwrap();
        let tmp_dir = tmp_dir_raw.canonicalize().unwrap();
        let sub_dir = tmp_dir.join("model").join("1");
        tokio::fs::create_dir_all(&sub_dir).await.unwrap();
        let model_py = sub_dir.join("model.py");
        tokio::fs::write(&model_py, "original").await.unwrap();

        let registry = Arc::new(ModelRegistry::new());
        let worker_manager = build_test_worker_manager(tmp_dir.clone(), registry.clone());

        let (tx, mut rx) = mpsc::channel::<Vec<(PathBuf, WatchEventKind)>>(32);
        let has_hot_reload = Arc::new(AtomicBool::new(false));
        let handle = tokio::spawn(start_file_watcher(
            tmp_dir.clone(), worker_manager, tx, has_hot_reload,
            false, // always_forward = false (manual mode)
            Duration::from_millis(300),
            registry.clone(),
        ));

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        tokio::fs::write(&model_py, "modified").await.unwrap();

        let result = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            rx.recv(),
        ).await;
        assert!(
            result.is_err(),
            "manual mode with empty registry: watcher must skip events (P2)"
        );

        handle.abort();
        let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    }

    // ===== Manual mode: new version dirs are NOT auto-loaded =====

    /// Capture the messages of all tracing events emitted while `f` runs on
    /// a fresh current-thread runtime (the subscriber is installed as the
    /// thread-local default, so no global-subscriber conflicts).
    fn run_capturing_logs<F, Fut>(f: F) -> Vec<String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        use tracing_subscriber::prelude::*;

        struct MessageVisitor<'a>(&'a mut String);
        impl tracing::field::Visit for MessageVisitor<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    *self.0 = format!("{:?}", value);
                }
            }
        }

        #[derive(Clone, Default)]
        struct Messages(std::sync::Arc<std::sync::Mutex<Vec<String>>>);
        struct CaptureLayer(Messages);
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureLayer {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let mut msg = String::new();
                event.record(&mut MessageVisitor(&mut msg));
                self.0 .0.lock().unwrap().push(msg);
            }
        }

        let messages = Messages::default();
        let subscriber = tracing_subscriber::registry().with(CaptureLayer(messages.clone()));
        tracing::subscriber::with_default(subscriber, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(f());
        });
        let out = messages.0.lock().unwrap().clone();
        out
    }

    #[test]
    fn manual_mode_new_version_warns_and_does_not_load() {
        let tmp = test_repo_dir("no-autoload");
        let version_dir = tmp.join("m").join("1");
        std::fs::create_dir_all(&version_dir).unwrap();
        // hot_reload=true would have opted into the removed auto-load path.
        std::fs::write(
            version_dir.join("config.yaml"),
            "hot_reload: true\nstartup_timeout: 5\n",
        ).unwrap();
        std::fs::write(version_dir.join("model.py"), "raise SystemExit(1)\n").unwrap();

        let messages = run_capturing_logs(|| {
            let tmp = tmp.clone();
            let version_dir = version_dir.clone();
            async move {
                let registry = Arc::new(ModelRegistry::new());
                let wm = build_test_worker_manager(tmp.clone(), registry.clone());
                let trigger: Option<mpsc::Sender<()>> = None;
                let mut last_reload = std::collections::HashMap::new();
                let flag = AtomicBool::new(true);
                // A directory Create event announces a genuinely new version.
                let _ = process_watch_events(
                    vec![(version_dir, WatchEventKind::Create)],
                    tmp.clone(),
                    wm,
                    registry.clone(),
                    &mut last_reload,
                    &crate::config::ServerTunables::default(),
                    &flag,
                    &trigger,
                ).await;
                assert!(
                    registry.get("m", Some("1")).is_none(),
                    "manual mode: new version dir must NOT be auto-loaded — \
                     the user is the lifecycle owner"
                );
            }
        });

        assert!(
            messages.iter().any(|m| m.contains("manual") && m.contains("Admin API")),
            "manual mode: a new version dir must log guidance to load explicitly; got: {:?}",
            messages
        );
        assert!(
            !messages.iter().any(|m| m.contains("auto-loading") || m.contains("Hot load failed")),
            "manual mode: no load attempt may happen; got: {:?}",
            messages
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn manual_mode_file_edit_in_unloaded_version_does_not_warn() {
        // Regression: editing files inside a version directory that exists
        // but is not loaded must NOT nag with "New version ... not loading"
        // on every save — only a directory Create event announces a new
        // version. The "dir gone → unload" invariant is unaffected (covered
        // by manual_mode_removed_version_unloads_directly).
        let tmp = test_repo_dir("no-nag");
        let version_dir = tmp.join("m").join("1");
        std::fs::create_dir_all(&version_dir).unwrap();
        std::fs::write(version_dir.join("model.py"), "x = 1\n").unwrap();

        let messages = run_capturing_logs(|| {
            let tmp = tmp.clone();
            let version_dir = version_dir.clone();
            async move {
                let registry = Arc::new(ModelRegistry::new());
                let wm = build_test_worker_manager(tmp.clone(), registry.clone());
                let trigger: Option<mpsc::Sender<()>> = None;
                let mut last_reload = std::collections::HashMap::new();
                let flag = AtomicBool::new(true);
                let _ = process_watch_events(
                    vec![(version_dir.join("model.py"), WatchEventKind::Modify)],
                    tmp.clone(),
                    wm,
                    registry.clone(),
                    &mut last_reload,
                    &crate::config::ServerTunables::default(),
                    &flag,
                    &trigger,
                ).await;
                assert!(
                    registry.get("m", Some("1")).is_none(),
                    "manual mode: a file edit must not load the version"
                );
            }
        });

        assert!(
            !messages.iter().any(|m| m.contains("manual") && m.contains("Admin API")),
            "file edit in an existing unloaded version must not warn; got: {:?}",
            messages
        );
        assert!(
            messages.iter().any(|m| m.contains("unloaded version")),
            "file edit in an existing unloaded version should still be traceable \
             at debug level; got: {:?}",
            messages
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    // ===== /audit 604c558: two confirmed defects (tests must FAIL on current code) =====

    #[test]
    fn audit_manual_mode_version_dir_moved_into_repo_warns_new_version() {
        // 数据/范围假设: a new version directory deployed by rename/move
        // (the standard atomic-deploy pattern, e.g. `mv staging repo/m/2`
        // on the same filesystem) is delivered by notify as
        // EventKind::Modify(ModifyKind::Name(RenameMode::To)) — IN_MOVED_TO
        // on Linux, ItemRenamed under FSEvents — NOT EventKind::Create.
        // Collapsing every Modify subkind into plain WatchEventKind::Modify
        // loses the "a directory appeared" signal: manual mode then never
        // warns that the new version is not loaded. Pre-604c558, ANY event
        // on an unknown existing version dir produced the WARN, so this is
        // a regression of the new-version notification for move-deploys.
        let tmp = test_repo_dir("audit-rename-in");
        let version_dir = tmp.join("m").join("2");
        std::fs::create_dir_all(&version_dir).unwrap();
        std::fs::write(version_dir.join("model.py"), "x = 1\n").unwrap();

        let messages = run_capturing_logs(|| {
            let tmp = tmp.clone();
            let version_dir = version_dir.clone();
            async move {
                let registry = Arc::new(ModelRegistry::new());
                let wm = build_test_worker_manager(tmp.clone(), registry.clone());
                let trigger: Option<mpsc::Sender<()>> = None;
                let mut last_reload = std::collections::HashMap::new();
                let flag = AtomicBool::new(true);
                // What notify actually delivers for a dir moved INTO the repo.
                let kind = WatchEventKind::from(notify::EventKind::Modify(
                    notify::event::ModifyKind::Name(notify::event::RenameMode::To),
                ));
                let _ = process_watch_events(
                    vec![(version_dir, kind)],
                    tmp.clone(),
                    wm,
                    registry.clone(),
                    &mut last_reload,
                    &crate::config::ServerTunables::default(),
                    &flag,
                    &trigger,
                ).await;
                assert!(
                    registry.get("m", Some("2")).is_none(),
                    "manual mode: a moved-in version must not be auto-loaded"
                );
            }
        });

        assert!(
            messages.iter().any(|m| m.contains("manual") && m.contains("Admin API")),
            "a version dir appearing via rename/move must warn like a dir Create; got: {:?}",
            messages
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn audit_manual_mode_subdir_create_inside_unloaded_version_does_not_warn() {
        // 范围假设: creating a SUBDIRECTORY inside an existing, unloaded
        // version dir (e.g. `mkdir repo/m/1/checkpoints`) delivers a Create
        // event whose path is the subdirectory — the version dir itself is
        // NOT new, so the "New version ... not loading" WARN must not fire.
        // Keying the new-version signal off any descendant Create mislabels
        // ordinary churn as a new version and brings back the WARN noise
        // this commit set out to remove (checkpoint/output dirs are created
        // routinely during development).
        let tmp = test_repo_dir("audit-subdir-create");
        let version_dir = tmp.join("m").join("1");
        let sub_dir = version_dir.join("checkpoints");
        std::fs::create_dir_all(&sub_dir).unwrap();

        let messages = run_capturing_logs(|| {
            let tmp = tmp.clone();
            let sub_dir = sub_dir.clone();
            async move {
                let registry = Arc::new(ModelRegistry::new());
                let wm = build_test_worker_manager(tmp.clone(), registry.clone());
                let trigger: Option<mpsc::Sender<()>> = None;
                let mut last_reload = std::collections::HashMap::new();
                let flag = AtomicBool::new(true);
                let _ = process_watch_events(
                    vec![(sub_dir, WatchEventKind::Create)],
                    tmp.clone(),
                    wm,
                    registry.clone(),
                    &mut last_reload,
                    &crate::config::ServerTunables::default(),
                    &flag,
                    &trigger,
                ).await;
                assert!(
                    registry.get("m", Some("1")).is_none(),
                    "manual mode: mkdir inside an unloaded version must not load it"
                );
            }
        });

        assert!(
            !messages.iter().any(|m| m.contains("manual") && m.contains("Admin API")),
            "mkdir inside an existing unloaded version must not warn 'New version'; got: {:?}",
            messages
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn audit_manual_mode_dir_mtime_churn_on_unloaded_version_does_not_warn() {
        // Guard for the From<notify::EventKind> mapping boundary: only
        // ModifyKind::Name (rename = appeared) may promote to Create.
        // Ordinary dir mtime churn — Modify(Any/Data/Metadata), delivered
        // whenever files are written inside an existing unloaded version
        // dir — must stay Modify and remain debug-level, or the WARN noise
        // this change removed comes back on every save.
        let tmp = test_repo_dir("audit-mtime-churn");
        let version_dir = tmp.join("m").join("1");
        std::fs::create_dir_all(&version_dir).unwrap();

        let messages = run_capturing_logs(|| {
            let tmp = tmp.clone();
            let version_dir = version_dir.clone();
            async move {
                let registry = Arc::new(ModelRegistry::new());
                let wm = build_test_worker_manager(tmp.clone(), registry.clone());
                let trigger: Option<mpsc::Sender<()>> = None;
                let mut last_reload = std::collections::HashMap::new();
                let flag = AtomicBool::new(true);
                // What notify delivers for dir mtime/attr churn.
                let kind = WatchEventKind::from(notify::EventKind::Modify(
                    notify::event::ModifyKind::Any,
                ));
                let _ = process_watch_events(
                    vec![(version_dir, kind)],
                    tmp.clone(),
                    wm,
                    registry.clone(),
                    &mut last_reload,
                    &crate::config::ServerTunables::default(),
                    &flag,
                    &trigger,
                ).await;
            }
        });

        assert!(
            !messages.iter().any(|m| m.contains("manual") && m.contains("Admin API")),
            "dir mtime churn on an existing unloaded version must stay debug; got: {:?}",
            messages
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
