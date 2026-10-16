use super::pick_latest_version;
use super::scanner::{auto_unpack_lma_files, group_by_model, scan_repo_models, RepoModel};
use crate::config::{ModelStrategyConfig, OrchestrationConfig};
use crate::registry::ModelRegistry;
use crate::worker::WorkerManager;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{error, info, warn, Instrument};

/// What [`activate_after_load`] did this round. Returned for tests and
/// observability; production callers log and discard.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum PostLoadActivation {
    /// The configured default (loaded this round) is now active.
    DefaultActivated,
    /// The configured default was loaded this round but could not be activated
    /// (not Ready, or the registry rejected it).
    DefaultFailed,
    /// No usable active version remained, and the configured default — already
    /// loaded and Ready — was re-activated. The fallback path that honours the
    /// default instead of arbitrary load order; logs no "falling back" warning
    /// because the default was respected after all.
    DefaultReactivated,
    /// No usable active version remained and the configured default could not
    /// be honoured; `versions_loaded[0]` was activated instead. The ONLY path
    /// that logs the "falling back" warning — steady-state ticks (nothing
    /// loaded this round, active version serving) return [`Unchanged`] and
    /// never warn.
    FallbackActivated(String),
    /// The fallback activation was attempted but failed (not Ready, or the
    /// registry rejected it).
    FallbackFailed,
    /// Nothing to do: no usable-active loss, and nothing loaded to activate.
    Unchanged,
}

/// Post-load activation policy, pure registry manipulation (no worker I/O) so
/// it is unit-testable without spawning anything.
///
/// Honour the configured `default_version` when this round loaded it;
/// otherwise, when no usable active version remains (a stale pin seeded by
/// cache_registry restore pointing at a version that failed to reload must
/// read as "no active version" — B1), fall back to a loaded version so the
/// model is never left Ready with no active version (every unversioned
/// request would 404 until an operator intervenes).
pub(super) fn activate_after_load(
    registry: &ModelRegistry,
    name: &str,
    default_version: Option<&str>,
    versions_loaded: &[String],
) -> PostLoadActivation {
    if let Some(dv) = default_version {
        if versions_loaded.iter().any(|v| v == dv) {
            return match registry.activate_version(name, dv) {
                Ok(true) => {
                    info!("Activated default version {} for {}", dv, name);
                    PostLoadActivation::DefaultActivated
                }
                Ok(false) => {
                    warn!(
                        "Failed to activate default version {} for {} (not ready)",
                        dv, name
                    );
                    PostLoadActivation::DefaultFailed
                }
                Err(e) => {
                    error!(
                        "Error activating default version {} for {}: {}",
                        dv, name, e
                    );
                    PostLoadActivation::DefaultFailed
                }
            };
        }
    }
    // Deliberately NOT gated on `default_version`: whenever the configured
    // default could not be activated (its config.yaml failed to load, it was
    // not in the load target, or it was not Ready), serving the first loaded
    // version is the strictly better failure mode.
    if versions_loaded.is_empty() || registry.active_version_is_ready(name) {
        return PostLoadActivation::Unchanged;
    }
    // Prefer the configured default when it is already loaded and Ready —
    // operator intent beats arbitrary load order. This is the one fallback
    // path that does NOT warn: the default is honoured after all.
    let default_ready = default_version.filter(|dv| {
        registry
            .get(name, Some(dv))
            .map(|mv| mv.status == crate::registry::types::VersionStatus::Ready)
            .unwrap_or(false)
    });
    let (pick, honours_default) = match default_ready {
        Some(dv) => (dv, true),
        None => (versions_loaded[0].as_str(), false),
    };
    if !honours_default {
        if let Some(dv) = default_version {
            warn!(
                "Configured default version {} for {} was not loaded this round; \
                 falling back to a loaded version",
                dv, name
            );
        }
    }
    match registry.activate_version(name, pick) {
        Ok(true) => {
            if honours_default {
                info!(
                    "Activated default version {} for {} (already loaded)",
                    pick, name
                );
                PostLoadActivation::DefaultReactivated
            } else {
                info!("Activated version {} for {}", pick, name);
                PostLoadActivation::FallbackActivated(pick.to_string())
            }
        }
        Ok(false) => {
            warn!(
                "Failed to activate version {} for {} (not ready)",
                pick, name
            );
            PostLoadActivation::FallbackFailed
        }
        Err(e) => {
            error!("Error activating version {} for {}: {}", pick, name, e);
            PostLoadActivation::FallbackFailed
        }
    }
}

/// Reconcile registry state with the model repository: load versions that
/// should be present per the orchestration config, unload managed versions
/// that should no longer be present (declarative semantics — the config is
/// the source of truth for models in scope). Runs once at startup (initial
/// load) and on every poll tick when control_mode = "auto".
///
/// `seen_lma` tracks (path, mtime) of .lma artifacts already unpacked so the
/// poller doesn't re-unpack — and thereby overwrite — the same file every
/// tick. A replaced artifact has a new mtime and is unpacked again.
pub(super) async fn reconcile_models(
    repo_path: &Path,
    orch: &OrchestrationConfig,
    worker_manager: &WorkerManager,
    registry: &ModelRegistry,
    model_defaults: &crate::config::ModelTunables,
    server_tunables: &crate::config::ServerTunables,
    seen_lma: &mut HashSet<(PathBuf, std::time::SystemTime)>,
) {
    auto_unpack_lma_files(
        repo_path,
        seen_lma,
        Duration::from_secs_f32(server_tunables.unpack_timeout_secs),
    )
    .await;

    let available = scan_repo_models(repo_path).await;
    let by_model = group_by_model(available);

    let names: Vec<String> = match orch.control_mode.as_str() {
        "all" => by_model.keys().cloned().collect(),
        _ => orch.load_models.clone(),
    };

    let strategy_map: HashMap<String, &ModelStrategyConfig> =
        orch.models.iter().map(|s| (s.name.clone(), s)).collect();

    for name in &names {
        let strategy = strategy_map.get(name).copied();
        let models = by_model.get(name).cloned().unwrap_or_default();
        let target = compute_target_versions(&models, strategy);

        let loaded: HashSet<String> = registry
            .list_versions(name)
            .into_iter()
            .map(|v| v.version)
            .collect();
        let target_set: HashSet<String> = target.iter().cloned().collect();

        // Unload managed versions that should no longer be present.
        for version in loaded.difference(&target_set) {
            info!(
                "Unloading {} version {} (no longer in target set)",
                name, version
            );
            if let Err(e) = worker_manager.unload_model(name, Some(version)).await {
                warn!("Failed to unload {} version {}: {}", name, version, e);
            }
        }

        // Load missing versions.
        let mut versions_loaded = Vec::new();
        for version in &target {
            if loaded.contains(version) {
                continue;
            }
            if let Err(e) = crate::validation::validate_identifier(name) {
                error!("Skipping model with invalid name '{}': {}", name, e);
                break;
            }
            if let Err(e) = crate::validation::validate_identifier(version) {
                error!(
                    "Skipping {} version with invalid version '{}': {}",
                    name, version, e
                );
                continue;
            }
            // Respect max_loaded_versions: loading beyond the cap would
            // trigger LRU eviction, which the next reconcile would see as
            // "missing" and reload — an evict/reload thrash loop.
            if let Some(max) = strategy.and_then(|s| s.max_loaded_versions) {
                let current = registry.list_versions(name).len();
                if current >= max {
                    warn!(
                        "Skipping load of {} version {}: max_loaded_versions={} already reached",
                        name, version, max
                    );
                    continue;
                }
            }
            let config_path = repo_path.join(name).join(version).join("config.yaml");
            // 配置解析/校验失败必须可见（此前 unwrap_or_default 静默回退默认
            // 配置——坏配置按默认值跑，比加载失败更难查；M7 迁移哨兵也依赖此
            // 错误上浮）。
            let mut config = match crate::config::load_model_config(&config_path) {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "Skipping {} version {}: invalid config.yaml: {}",
                        name, version, e
                    );
                    continue;
                }
            };
            model_defaults.apply_to(&mut config);

            if let Err(e) = worker_manager.load_model(name, version, &config).await {
                error!("Failed to load {} version {}: {}", name, version, e);
            } else {
                versions_loaded.push(version.clone());
            }
        }

        // Post-load activation: honour the configured default, else fall back
        // so the model never sits Ready with no active version (B1). See
        // `activate_after_load` for the warning policy (the "falling back"
        // warning fires only when the fallback genuinely engages).
        let default_version = strategy.and_then(|s| s.default_version.clone());
        activate_after_load(registry, name, default_version.as_deref(), &versions_loaded);
    }
}
/// Versions of a model that should be loaded per the strategy's load_policy:
/// "all" → every version found on disk; "latest" → the highest semver;
/// otherwise ("explicit") → versions_to_load ∩ disk versions. No strategy
/// (None) loads every disk version — the legacy default for load_models
/// entries without a strategy.
fn compute_target_versions(
    models: &[RepoModel],
    strategy: Option<&ModelStrategyConfig>,
) -> Vec<String> {
    let all: Vec<String> = models.iter().map(|m| m.version.clone()).collect();
    let load_policy = strategy
        .map(|s| s.load_policy.as_str())
        .unwrap_or("explicit");
    let mut target = match load_policy {
        "all" => all.clone(),
        "latest" => pick_latest_version(&all).into_iter().collect(),
        _ => match strategy {
            Some(s) => models
                .iter()
                .filter(|m| s.versions_to_load.contains(&m.version))
                .map(|m| m.version.clone())
                .collect(),
            None => all.clone(),
        },
    };

    // The configured default version must always be loaded: it is the version
    // the activation branch below is asked to make active, and a version that is
    // never loaded can never be activated. Without this, `load_policy:
    // explicit` with an empty `versions_to_load` (the default), or `latest` with
    // a default that is not the newest, left the model Ready with NO active
    // version — every request without an explicit version 404s, permanently,
    // because an already-loaded version is never re-activated on a later round.
    //
    // `configured_versions` (`http/handlers/admin.rs`) already counts
    // `default_version` as part of a model's configured set; this brings the
    // load target in line with it. Only versions present on disk are added, so a
    // default pointing at a version that was deleted does not turn into a
    // load failure.
    if let Some(dv) = strategy.and_then(|s| s.default_version.as_ref()) {
        if all.contains(dv) && !target.contains(dv) {
            target.push(dv.clone());
        }
    }
    target
}
// ===== Hot Reload File Watcher =====

/// Run the reconcile loop for control_mode = "auto": directory events
/// trigger a reconcile (near-real-time), the poll interval is the resync
/// backstop (watch events can be lost, e.g. on network filesystems).
/// `seen_lma` comes from the startup load so artifacts unpacked there are
/// not re-unpacked on the first tick.
// allow: server/mod.rs 单点 spawn 的 reconcile 任务,参数为 registry/
// worker_manager/tunables/触发通道等异构运行时部件。
#[allow(clippy::too_many_arguments)]
pub(super) async fn start_reconcile_task(
    repo_path: PathBuf,
    orch: OrchestrationConfig,
    worker_manager: Arc<WorkerManager>,
    registry: Arc<ModelRegistry>,
    model_defaults: crate::config::ModelTunables,
    server_tunables: crate::config::ServerTunables,
    trigger_rx: mpsc::Receiver<()>,
    seen_lma: HashSet<(PathBuf, std::time::SystemTime)>,
) {
    let poll_secs = orch.poll_interval.max(1);
    info!(
        "Reconcile task started (control_mode=auto, resync interval={}s)",
        poll_secs
    );
    let seen_lma = Arc::new(tokio::sync::Mutex::new(seen_lma));
    let coalesce = Duration::from_secs_f32(server_tunables.reconcile_coalesce_secs);
    reconcile_loop(
        Duration::from_secs(poll_secs),
        trigger_rx,
        coalesce,
        move || {
            let repo_path = repo_path.clone();
            let orch = orch.clone();
            let worker_manager = worker_manager.clone();
            let registry = registry.clone();
            let model_defaults = model_defaults.clone();
            let server_tunables = server_tunables.clone();
            let seen_lma = seen_lma.clone();
            async move {
                let mut seen_lma = seen_lma.lock().await;
                reconcile_models(
                    &repo_path,
                    &orch,
                    &worker_manager,
                    &registry,
                    &model_defaults,
                    &server_tunables,
                    &mut seen_lma,
                )
                .instrument(tracing::info_span!("reconcile"))
                .await;
            }
        },
    )
    .await;
}

/// Generic reconcile driver: run `reconcile` on every resync tick, and
/// near-real-time on trigger events (coalesced over `coalesce` so a burst
/// of filesystem events — e.g. unpacking a version dir — produces one run).
async fn reconcile_loop<F, Fut>(
    poll_interval: Duration,
    trigger_rx: mpsc::Receiver<()>,
    coalesce: Duration,
    mut reconcile: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut tick = interval(poll_interval);
    // The first interval tick fires immediately; skip it — the startup
    // reconcile just ran, a second one would be a redundant repo scan.
    tick.tick().await;
    let mut trigger_rx = Some(trigger_rx);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                reconcile().await;
            }
            msg = async {
                match trigger_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => futures::future::pending().await,
                }
            } => {
                match msg {
                    Some(()) => {
                        tokio::time::sleep(coalesce).await;
                        if let Some(rx) = trigger_rx.as_mut() {
                            while rx.try_recv().is_ok() {}
                        }
                        reconcile().await;
                    }
                    // Sender dropped (reload task gone): tick-only resync.
                    None => trigger_rx = None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // --- compute_target_versions ---

    fn rm(name: &str, version: &str) -> RepoModel {
        RepoModel {
            name: name.to_string(),
            version: version.to_string(),
        }
    }

    // --- activate_after_load ---

    /// Register `versions` (all Ready); the returned registry is the fixture.
    fn registry_with_ready(model: &str, versions: &[&str]) -> ModelRegistry {
        let registry = ModelRegistry::new();
        for v in versions {
            registry
                .register(
                    model,
                    v,
                    crate::config::ModelConfig::default(),
                    crate::registry::types::ModelType::LitAPI,
                    std::path::PathBuf::new(),
                )
                .unwrap();
            registry.mark_ready(model, v).unwrap();
        }
        registry
    }

    /// Steady-state tick (nothing loaded this round, active version serving)
    /// must be a no-op: the old inline code warned "was not loaded this round;
    /// falling back" on EVERY poll tick for every model with a configured
    /// default, with no fallback actually occurring — training operators to
    /// ignore the one log line that means a real misconfiguration.
    #[test]
    fn activate_after_load_steady_state_is_unchanged_and_warn_free() {
        let registry = registry_with_ready("m", &["1"]);
        registry.activate_version("m", "1").unwrap();

        let outcome = activate_after_load(&registry, "m", Some("1"), &[]);
        assert_eq!(outcome, PostLoadActivation::Unchanged);
        assert_eq!(registry.get_active_version("m").as_deref(), Some("1"));
    }

    #[test]
    fn activate_after_load_honours_default_loaded_this_round() {
        let registry = registry_with_ready("m", &["1"]);

        let outcome = activate_after_load(&registry, "m", Some("1"), &["1".to_string()]);
        assert_eq!(outcome, PostLoadActivation::DefaultActivated);
        assert_eq!(registry.get_active_version("m").as_deref(), Some("1"));
    }

    /// Genuine fallback (default not honourable, no usable active version)
    /// engages on the loaded version — and is the only path that logs the
    /// "falling back" warning.
    #[test]
    fn activate_after_load_falls_back_when_default_not_honourable() {
        let registry = registry_with_ready("m", &["1", "2"]);
        registry.activate_version("m", "1").unwrap();
        // The configured default "9" does not exist; version 1 stops serving
        // (pin points at a broken version — the B1 stale-pin shape).
        let _ = registry.set_status("m", "1", crate::registry::types::VersionStatus::Failed);

        let outcome = activate_after_load(&registry, "m", Some("9"), &["2".to_string()]);
        assert_eq!(
            outcome,
            PostLoadActivation::FallbackActivated("2".to_string())
        );
        assert_eq!(registry.get_active_version("m").as_deref(), Some("2"));
    }

    /// The fallback must PREFER the configured default when it is already
    /// loaded and Ready — operator intent beats arbitrary load order. The old
    /// fallback pinned `versions_loaded[0]` ignoring `default_version`
    /// entirely, permanently activating the wrong version (the reconcile
    /// fallback was the one activation path that did not prefer the default).
    #[test]
    fn activate_after_load_fallback_prefers_configured_default_when_ready() {
        let registry = registry_with_ready("m", &["1", "2"]);
        registry.activate_version("m", "2").unwrap();
        // Version 2 (the active pin) breaks; version 1 — the configured
        // default — is loaded and Ready. This round loaded version 3.
        let _ = registry.set_status("m", "2", crate::registry::types::VersionStatus::Failed);

        let outcome = activate_after_load(&registry, "m", Some("1"), &["3".to_string()]);
        assert_eq!(outcome, PostLoadActivation::DefaultReactivated);
        assert_eq!(
            registry.get_active_version("m").as_deref(),
            Some("1"),
            "the fallback must re-activate the configured default, not versions_loaded[0]"
        );
    }

    /// Nothing loaded this round and no usable active version → nothing the
    /// helper can do (the caller logs nothing either — the activation attempt
    /// below is gated on a non-empty loaded set).
    #[test]
    fn activate_after_load_no_loaded_versions_is_unchanged() {
        let registry = registry_with_ready("m", &["1"]);
        let _ = registry.set_status("m", "1", crate::registry::types::VersionStatus::Failed);

        let outcome = activate_after_load(&registry, "m", Some("1"), &[]);
        assert_eq!(outcome, PostLoadActivation::Unchanged);
    }

    #[test]
    fn test_compute_target_versions_all() {
        let models = vec![rm("m", "1"), rm("m", "2"), rm("m", "3")];
        let strategy = ModelStrategyConfig {
            name: "m".to_string(),
            load_policy: "all".to_string(),
            ..Default::default()
        };
        assert_eq!(
            compute_target_versions(&models, Some(&strategy)),
            vec!["1".to_string(), "2".to_string(), "3".to_string()]
        );
    }

    #[test]
    fn test_compute_target_versions_latest() {
        let models = vec![rm("m", "v2"), rm("m", "v10"), rm("m", "v1")];
        let strategy = ModelStrategyConfig {
            name: "m".to_string(),
            load_policy: "latest".to_string(),
            ..Default::default()
        };
        assert_eq!(
            compute_target_versions(&models, Some(&strategy)),
            vec!["v10".to_string()]
        );
    }

    #[test]
    fn test_compute_target_versions_explicit_intersection() {
        let models = vec![rm("m", "1"), rm("m", "2")];
        let strategy = ModelStrategyConfig {
            name: "m".to_string(),
            load_policy: "explicit".to_string(),
            versions_to_load: vec!["2".to_string(), "3".to_string()],
            ..Default::default()
        };
        assert_eq!(
            compute_target_versions(&models, Some(&strategy)),
            vec!["2".to_string()]
        );
    }

    /// Two functions answer "which versions does this model need", and they
    /// disagree. `compute_target_versions` (below) builds the load set from
    /// `load_policy` alone and never consults `strategy.default_version`, while
    /// `configured_versions` (`http/handlers/admin.rs:799`, "the explicit
    /// references (versions_to_load / default_version / weights keys)") DOES
    /// insert it. Only this one drives the load.
    ///
    /// Consequence at `reconcile.rs:126-141`: the activation branch is entered
    /// because `default_version` is `Some`, but its guard
    /// `versions_loaded.contains(dv)` is false (the version was never loaded),
    /// and the `else if` auto-activate fallback is unreachable because the
    /// `if let Some(ref dv)` arm was taken. The model loads and reports Ready
    /// with NO active version, so every request without an explicit version
    /// 404s — permanently, since already-loaded versions are never
    /// re-activated on later rounds.
    ///
    /// Reachable on the default `load_policy: "explicit"` with an empty
    /// `versions_to_load`, and on `"latest"` whenever the configured default is
    /// not the newest disk version.
    ///
    /// NOTE: this assertion encodes "a configured default version must be
    /// loadable". If the fix instead makes the activation branch tolerate a
    /// default that was never loaded (falling back to a loaded version), move
    /// this pin to that path rather than deleting it — the contract to keep is
    /// that a named default_version never strands the model without an active
    /// version.
    #[test]
    fn test_compute_target_versions_covers_the_configured_default_version() {
        let models = vec![rm("m", "1"), rm("m", "2"), rm("m", "3")];
        let strategy = ModelStrategyConfig {
            name: "m".to_string(),
            load_policy: "explicit".to_string(),
            versions_to_load: vec![],
            default_version: Some("3".to_string()),
            ..Default::default()
        };

        let target = compute_target_versions(&models, Some(&strategy));
        assert!(
            target.contains(&"3".to_string()),
            "the configured default_version must be in the load target — it is the version the \
             reconciler is asked to activate, so omitting it can only yield a Ready model with no \
             active version; got {target:?}"
        );
    }

    #[test]
    fn test_compute_target_versions_no_strategy_loads_all_disk_versions() {
        // A model in load_models without a strategy entry loads every
        // version found on disk (legacy default).
        let models = vec![rm("m", "1"), rm("m", "2")];
        assert_eq!(
            compute_target_versions(&models, None),
            vec!["1".to_string(), "2".to_string()]
        );
    }

    #[test]
    fn test_compute_target_versions_explicit_empty_versions_to_load() {
        // Strategy present with explicit policy but no versions_to_load:
        // nothing loads (empty intersection), unlike the no-strategy case.
        let models = vec![rm("m", "1"), rm("m", "2")];
        let strategy = ModelStrategyConfig {
            name: "m".to_string(),
            load_policy: "explicit".to_string(),
            ..Default::default()
        };
        assert!(compute_target_versions(&models, Some(&strategy)).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn reconcile_loop_trigger_runs_despite_long_poll_interval() {
        let (tx, rx) = mpsc::channel::<()>(8);
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let handle = tokio::spawn(reconcile_loop(
            Duration::from_secs(3600),
            rx,
            Duration::from_secs(2),
            move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                }
            },
        ));

        tx.send(()).await.unwrap();
        // Auto-advances past the coalesce window; the 3600s tick never fires.
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "event trigger must run reconcile even with a 3600s poll interval"
        );
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn reconcile_loop_coalesces_trigger_burst_into_one_run() {
        let (tx, rx) = mpsc::channel::<()>(8);
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let handle = tokio::spawn(reconcile_loop(
            Duration::from_secs(3600),
            rx,
            Duration::from_secs(2),
            move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                }
            },
        ));

        for _ in 0..5 {
            tx.send(()).await.unwrap();
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "a burst of triggers within the coalesce window must produce one reconcile run"
        );
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn reconcile_loop_ticks_without_triggers() {
        let (_tx, rx) = mpsc::channel::<()>(8);
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let handle = tokio::spawn(reconcile_loop(
            Duration::from_secs(1),
            rx,
            Duration::from_secs(2),
            move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                }
            },
        ));

        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            2,
            "resync ticks must keep firing without triggers"
        );
        handle.abort();
    }
}
