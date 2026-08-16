use crate::error::AppError;
use dashmap::DashMap;
use crate::http::state::AppState;
use indexmap::IndexMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::Duration;
use tracing::warn;

use super::*;

/// P6 (batch 0 scope): background plan warm, spawned from the lifecycle
/// load_model ensemble branch — prime the P0 cache so the first request
/// never pays the config parse, and pre-check sub-model readiness.
/// Non-blocking (load_model returns immediately; the warm runs detached).
/// The resolved-version side-table and sub-model preloading land with E4
/// (batch 3) — warm here only parses + checks.
pub fn spawn_ensemble_warm(
    repo_path: PathBuf,
    plans: Option<Arc<EnsemblePlanCache>>,
    registry: Arc<crate::registry::ModelRegistry>,
    model_name: String,
    version: String,
) {
    tokio::spawn(async move {
        let Some(plans) = plans else { return; };
        let key = PlanKey {
            model: model_name.clone(),
            version: version.clone(),
        };
        let plan = match plans
            .get_or_load(key, || load_ensemble_plan(&repo_path, &model_name, &version))
            .await
        {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    model = %model_name,
                    error = %e,
                    "ensemble warm: plan parse failed (first request retries)"
                );
                return;
            }
        };
        // E4 (batch 3): pre-resolve every sub-model step and preheat the
        // routing/LRU caches. D27: this warm pass is a TTFT hint ONLY — it is
        // NEVER a resolution source; the request-time D15 snapshot always
        // re-resolves against the registry, so an active drift between warm
        // and first request has no freeze window.
        let warm_snapshot = Arc::new(VersionSnapshot::default());
        // E8-1: a dags-form plan's outer steps are EMPTY — the steps live in
        // the sets. Warm every set's sub-models, or their routing/LRU caches
        // stay cold (and an unreferenced sub-model risks LRU eviction while
        // a live DAG depends on it).
        let warm_steps: Vec<&EnsembleStep> = match &plan.dag_sets {
            Some(sets) => sets.values().flat_map(|p| p.steps.iter()).collect(),
            None => plan.steps.iter().collect(),
        };
        for step in warm_steps {
            match warm_snapshot.resolve(&registry, step) {
                Ok(resolved) => {
                    // touch_last_used: an ensemble's sub-models must count as
                    // used, or LRU eviction can unload a live DAG dependency.
                    registry.touch_last_used(&step.model, &resolved);
                    // routing_pick exercises the weighted-routing cache the
                    // first request would otherwise pay cold.
                    registry.routing_pick(&step.model);
                    if !registry.is_ready(&step.model, Some(&resolved)) {
                        warn!(
                            model = %step.model,
                            version = %resolved,
                            "ensemble warm: sub-model not ready (first request autoloads)"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        model = %step.model,
                        error = %e,
                        "ensemble warm: sub-model version resolution failed"
                    );
                }
            }
        }
    });
}

/// P0 entry: cached plan lookup with single-flight + mtime re-check; falls
/// back to a direct load when no cache is installed (tests construct
/// WorkerManager without one).
pub async fn get_ensemble_plan(
    state: &AppState,
    model_name: &str,
    version: &str,
) -> Result<Arc<EnsemblePlan>, AppError> {
    let key = PlanKey {
        model: model_name.to_string(),
        version: version.to_string(),
    };
    match state.worker_manager.ensemble_plans() {
        Some(cache) => cache
            .get_or_load(key, || load_ensemble_plan(&state.repo_path, model_name, version))
            .await,
        None => load_ensemble_plan(&state.repo_path, model_name, version).await,
    }
}

// ===== P0: EnsemblePlan cache (D6 + review ①-④) =====

/// Parsed, validated, layer-sorted ensemble DAG — the object cached by
/// [`EnsemblePlanCache`] so a request never re-reads/parses/validates the
/// config.yaml. Batch 0 starts with the historical parse result; streaming
/// fields (stream/output semantics) land on this struct in batch 0 step 2
/// (D6: parse structured once, stream/MIMO validation stacks on top).
#[derive(Debug, Clone)]
pub struct EnsemblePlan {
    pub steps: Vec<EnsembleStep>,
    /// Topological layers as indices into `steps` (self-owning, no refs).
    pub layers: Vec<Vec<usize>>,
    /// DAG output step (E2 semantics unified at batch 0, §4.1 rule 4): with no
    /// explicit `output` this is `steps.last()`; validation ensures a
    /// streaming DAG's output step is its streaming step.
    pub output_step: usize,
    /// E2 (batch 3): explicit output field path (`output: "$s2.score"`).
    /// None = the output step's whole value.
    pub output_field: Option<String>,
    /// Pipeline chains (§4.2, batch 2): each chain is a linear consumer path
    /// of STREAMING steps (all nodes stream), tail = output step. Empty for
    /// non-pipeline DAGs (tail-streaming only).
    pub chains: Vec<Chain>,
    /// MIMO (D8/D9, batch 4①): the request-level named inputs declaration —
    /// None = legacy single anonymous input (`$request`, byte-identical).
    pub inputs_decl: Option<IndexMap<String, InputDecl>>,
    /// MIMO (R11/R12, batch 4①): per-step static input mode, index-aligned
    /// with `steps`. None = legacy dynamic dispatch (no declared type
    /// environment); Some(mode) = parse-decided assembly (no runtime type
    /// branches).
    pub input_modes: Vec<Option<InputMode>>,
    /// MIMO (R4, batch 4①): per-step optional-input names referenced —
    /// non-empty marks a CONDITIONAL step (absent input → step skipped,
    /// D13/E6-skip channel). Index-aligned with `steps`.
    pub conditional_refs: Vec<Vec<String>>,
    /// P1 (batch 6): per-step referenced CONTEXT keys, parse-computed from
    /// the step's input refs (derivation mirrors [`resolve_ref`]). Spawn-time
    /// clones select these keys instead of the whole table — the deep-copy
    /// of every sibling output per spawn is eliminated. Index-aligned with
    /// `steps`.
    pub step_dep_keys: Vec<Vec<String>>,
    /// P2/P8 (batch 6): per-step raw residency — an UNDECLARED step whose
    /// output is never field-projected stays unparsed Bytes in the context
    /// (whole refs splice the original bytes; a field access parses lazily
    /// once). Declared steps (step.outputs projections) and field-referenced
    /// steps always parse. Index-aligned with `steps`.
    pub step_raw_eligible: Vec<bool>,
    /// E7 (batch 4④): multi-sink aliases `{alias: $ref}` — the response is
    /// a KServe envelope (build_response). None = the historical single
    /// output.
    pub outputs: Option<IndexMap<String, String>>,
    /// E8-1 (batch 5): named DAG sets (the dags form) — the outer plan is a
    /// pure container; execution resolves the set via [`select_dag_set`].
    /// None = the historical single-set form.
    pub dag_sets: Option<IndexMap<String, Arc<EnsemblePlan>>>,
    /// Source config file — mtime re-check (review ②) stats this path.
    pub config_path: PathBuf,
    /// mtime of `config_path` captured BEFORE the file read (stat-before-read):
    /// a write landing mid-load leaves this mtime OLDER than the file's, so
    /// the interval re-check re-parses (safe) — the reverse order could pin a
    /// fresh mtime onto stale content and serve it indefinitely. Set by the
    /// production loader; None for the load-time direct-parse path
    /// (`insert_ready` stats on its own there).
    pub source_mtime: Option<std::time::SystemTime>,
}

impl EnsemblePlan {
    /// MIMO (R11/R12): the parse-decided input mode of step `idx`
    /// (None = legacy dynamic dispatch).
    pub fn input_mode(&self, idx: usize) -> Option<InputMode> {
        self.input_modes.get(idx).copied().flatten()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanKey {
    pub model: String,
    pub version: String,
}

pub(crate) enum PlanCell {
    /// Single-flight in progress: the holder parses; waiters park here. The
    /// generation id disambiguates ownership after an invalidation removed
    /// the holder's cell mid-load (D23 race) — without it the stale holder
    /// could clobber a NEW holder's cell or a load-time `insert_ready`.
    Loading {
        id: u64,
        waiters: Vec<oneshot::Sender<Result<Arc<EnsemblePlan>, AppError>>>,
    },
    Ready {
        plan: Arc<EnsemblePlan>,
        mtime: Option<std::time::SystemTime>,
        last_stat: tokio::time::Instant,
    },
}

/// M5: a holder whose `get_or_load` future is CANCELLED (dropped) mid-load
/// never reaches the Ok/Err write-back — `catch_unwind` guards panic, not
/// cancellation. Without this guard the `Loading` cell would stay in the map
/// forever and every later request for the key would park on `rx.await`.
/// On drop, if the cell is still OUR Loading placeholder, evict it: the key
/// returns to Vacant (next call re-parses) and the parked waiters' senders
/// drop with the cell, resolving their `rx.await` to Err instead of hanging.
/// The Ok path (cell already replaced by Ready) and the Err path (cell
/// already removed) make this a no-op via the id check.
struct LoadingGuard<'a> {
    plans: &'a DashMap<PlanKey, PlanCell>,
    key: PlanKey,
    id: u64,
}

impl Drop for LoadingGuard<'_> {
    fn drop(&mut self) {
        // remove_if: atomic per shard — an invalidate + new-holder insert
        // racing the drop must not evict the NEW holder's cell (id check).
        self.plans.remove_if(&self.key, |_, cell| {
            matches!(cell, PlanCell::Loading { id, .. } if *id == self.id)
        });
    }
}

/// P0: `DashMap<(model, version), Arc<EnsemblePlan>>` with single-flight
/// loads (review ④), mtime interval re-check (review ②, coarse tokio clock,
/// no syscall on the hot path) and model-prefix invalidation (review ③ —
/// `(m,"latest")` and `(m,"1")` are distinct keys, both must clear).
/// Invalidation hooks: lifecycle `unload_version` (via reload_model too) —
/// single collection point, fired BEFORE registry changes (D23).
pub struct EnsemblePlanCache {
    pub(crate) plans: DashMap<PlanKey, PlanCell>,
    stat_interval: Duration,
    /// Single-flight cell generation counter (ownership after invalidation).
    epoch: std::sync::atomic::AtomicU64,
}

impl Default for EnsemblePlanCache {
    fn default() -> Self {
        Self::new()
    }
}

impl EnsemblePlanCache {
    pub fn new() -> Self {
        Self {
            plans: DashMap::new(),
            stat_interval: Duration::from_secs(1),
            epoch: std::sync::atomic::AtomicU64::new(0),
        }
    }
    /// Get-or-load with single-flight: concurrent first requests share one
    /// parse; a failed load is NOT cached (next call re-parses — a fixed
    /// config heals without reload). Ready entries within `stat_interval`
    /// are returned without touching the filesystem.
    pub async fn get_or_load<F, Fut>(
        &self,
        key: PlanKey,
        load: F,
    ) -> Result<Arc<EnsemblePlan>, AppError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<Arc<EnsemblePlan>, AppError>> + Send,
    {
        use dashmap::mapref::entry::Entry as MapEntry;

        // Fast path: a fresh Ready entry returns without any filesystem work.
        if let Some(cell) = self.plans.get(&key) {
            if let PlanCell::Ready { plan, last_stat, .. } = &*cell {
                if last_stat.elapsed() < self.stat_interval {
                    return Ok(plan.clone());
                }
            }
        }

        loop {
            match self.plans.entry(key.clone()) {
                MapEntry::Occupied(mut occ) => match occ.get_mut() {
                    PlanCell::Loading { waiters, .. } => {
                        // Not the holder: park on the holder's result.
                        let (tx, rx) = oneshot::channel();
                        waiters.push(tx);
                        drop(occ);
                        return rx.await.map_err(|_| {
                            AppError::Internal("ensemble plan load task dropped".to_string())
                        })?;
                    }
                    PlanCell::Ready { plan, mtime, last_stat } => {
                        if last_stat.elapsed() < self.stat_interval {
                            return Ok(plan.clone());
                        }
                        // Interval elapsed (review ②): re-stat the source file.
                        // Coarse check amortised to once per interval, never on
                        // the hot path; a change evicts and re-loads in-flight.
                        let current_mtime = std::fs::metadata(&plan.config_path)
                            .and_then(|m| m.modified())
                            .ok();
                        if current_mtime == *mtime {
                            *last_stat = tokio::time::Instant::now();
                            return Ok(plan.clone());
                        }
                        occ.remove();
                        continue;
                    }
                },
                MapEntry::Vacant(vacant) => {
                    // We are the single-flight holder (review ④). Insert the
                    // Loading placeholder, drop the shard lock, then parse.
                    let my_id = self.epoch.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    vacant.insert(PlanCell::Loading { id: my_id, waiters: Vec::new() });
                    // M5: recycle the cell if THIS future is cancelled
                    // mid-load (dropped before the write-back below).
                    let _guard = LoadingGuard { plans: &self.plans, key: key.clone(), id: my_id };
                    // C2 (resource-leak-plan): a panicking loader must not
                    // wedge the Loading slot — the task's death would leave
                    // the placeholder in place and every waiter parked
                    // forever. Catch the unwind and funnel it into the normal
                    // Err path (evict + waiters notified), so the next call
                    // re-parses.
                    let loaded: Result<Arc<EnsemblePlan>, AppError> =
                        match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(load()))
                            .await
                        {
                            Ok(r) => r,
                            Err(panic) => {
                                let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                                    s.to_string()
                                } else if let Some(s) = panic.downcast_ref::<String>() {
                                    s.clone()
                                } else {
                                    "unknown panic".to_string()
                                };
                                Err(AppError::Internal(format!(
                                    "ensemble plan loader panicked: {msg}"
                                )))
                            }
                        };
                    match loaded {
                        Ok(plan) => {
                            // B2: the mtime comes from the loader's
                            // stat-before-read (a mid-load write can only make
                            // it older → safe re-parse), never a post-load
                            // stat (which could pin a fresh mtime onto stale
                            // content and serve it indefinitely).
                            let mtime = plan.source_mtime;
                            // The cell may be gone (invalidate raced the load,
                            // D23) or replaced (load-time insert_ready / a new
                            // holder): only write back while WE still own it.
                            // No panic paths — unload/reload racing a cold
                            // first load is normal operation.
                            let waiters = match self.plans.get_mut(&key) {
                                Some(mut cell) => match cell.value_mut() {
                                    PlanCell::Loading { id, waiters } if *id == my_id => {
                                        std::mem::take(waiters)
                                    }
                                    _ => Vec::new(),
                                },
                                None => Vec::new(),
                            };
                            for w in waiters {
                                let _ = w.send(Ok(plan.clone()));
                            }
                            if let Some(mut cell) = self.plans.get_mut(&key) {
                                if matches!(&*cell, PlanCell::Loading { id, .. } if *id == my_id) {
                                    *cell.value_mut() = PlanCell::Ready {
                                        plan: plan.clone(),
                                        mtime,
                                        last_stat: tokio::time::Instant::now(),
                                    };
                                }
                            }
                            return Ok(plan);
                        }
                        Err(e) => {
                            // Failed loads must NOT be cached: evict the
                            // placeholder so the next call re-parses (a fixed
                            // config heals without reload — behaviour parity
                            // with the uncached path). Waiters share the failure.
                            // Same ownership rule as the Ok path: only evict
                            // our own cell (an invalidate may have beaten us).
                            let waiters = match self.plans.get_mut(&key) {
                                Some(mut cell) => match cell.value_mut() {
                                    PlanCell::Loading { id, waiters } if *id == my_id => {
                                        Some(std::mem::take(waiters))
                                    }
                                    _ => None,
                                },
                                None => None,
                            };
                            if let Some(waiters) = waiters {
                                self.plans.remove(&key);
                                for w in waiters {
                                    let _ = w.send(Err(AppError::Internal(format!(
                                        "ensemble plan load failed: {e}"
                                    ))));
                                }
                            }
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    /// Direct insert from the lifecycle load path (P0/P6): the plan was
    /// parsed at load time, so the first request hits the cache without
    /// paying the parse. mtime is stat'ed here (once, at load).
    pub fn insert_ready(&self, key: PlanKey, plan: Arc<EnsemblePlan>) {
        let mtime = std::fs::metadata(&plan.config_path)
            .and_then(|m| m.modified())
            .ok();
        self.plans.insert(
            key,
            PlanCell::Ready {
                plan,
                mtime,
                last_stat: tokio::time::Instant::now(),
            },
        );
    }

    /// Invalidate every version of a model (review ③: latest + pinned
    /// versions are separate keys). Called on unload BEFORE registry
    /// changes (D23).
    pub fn invalidate_model(&self, model: &str) {
        self.plans.retain(|key, _| key.model != model);
    }

    pub fn invalidate_version(&self, model: &str, version: &str) {
        self.plans.remove(&PlanKey {
            model: model.to_string(),
            version: version.to_string(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M5 evidence (resource-leak sweep 2026-08-16): a holder that is
    /// CANCELLED (its `get_or_load` future is dropped) mid-load leaves the
    /// single-flight `Loading` cell in the DashMap with the waiters' oneshots
    /// unconsumed — `catch_unwind` guards panic but NOT future cancellation.
    /// Every later request for the same (model, version) parks on `rx.await`
    /// forever (no timeout), so the key is poisoned until an explicit
    /// unload/invalidate.
    ///
    /// Fixed code recycles the Loading cell when the holder is dropped;
    /// current code keeps the poisoned cell — this test FAILS (RED) until
    /// addressed.
    #[tokio::test]
    async fn m5_cancelled_holder_does_not_poison_loading_cell() {
        let cache = EnsemblePlanCache::new();
        let key = PlanKey {
            model: "m5_m".to_string(),
            version: "1".to_string(),
        };

        // Holder 1 starts a load that never completes (simulated slow parse).
        // Box::pin (not tokio::pin!): the future must be OWNED so the later
        // drop(first) really drops it — tokio::pin! rebinds a Pin<&mut F>
        // whose drop leaves the future alive until scope end.
        let mut first = Box::pin(cache.get_or_load(key.clone(), || async {
            std::future::pending::<Result<Arc<EnsemblePlan>, AppError>>().await
        }));
        // Drive holder 1 into the Loading cell (it parks on the pending load).
        tokio::select! {
            _ = &mut first => panic!("holder 1 must not complete"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        // Client disconnects: the future is dropped while holding the cell.
        drop(first);

        // A later request for the same key must return (re-parse), not park
        // forever on the abandoned Loading cell. The re-parse fails on
        // purpose — any terminal outcome proves the key is not poisoned.
        let second = cache.get_or_load(key.clone(), || async {
            Err::<Arc<EnsemblePlan>, _>(AppError::Internal("m5 re-parse".to_string()))
        });
        match tokio::time::timeout(Duration::from_millis(500), second)
            .await
            .expect("key must not stay poisoned after the holder cancels (500ms)")
        {
            Err(_) => {} // re-entered the Vacant path and re-parsed (failed) — correct.
            Ok(_) => panic!(
                "expected a re-parse outcome (Err), got Ok — the Loading cell \
                 is still parked"
            ),
        }
    }
}

