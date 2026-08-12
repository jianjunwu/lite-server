use crate::error::{AppError, ModelErrorData};
use crate::http::state::AppState;
use crate::proto::liteserver as pb;
use crate::registry::types::ModelType;
use bytes::Bytes;
use dashmap::DashMap;
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration};
use tracing::{info, warn};
use uuid::Uuid;

// ===== EnsembleValue: typed step input/output (B3, E6) =====

/// A value flowing through an ensemble DAG edge.
///
/// `Json` is the historical path — all steps participate in field-level
/// `$ref` resolution and merge into JSON objects. `Binary` is the
/// passthrough path — root input or final-step output can be opaque bytes,
/// but binary values MUST NOT flow between internal DAG steps (Option A
/// scope; internal binary flow is reserved for Option B).
#[derive(Debug, Clone)]
pub enum EnsembleValue {
    Json(serde_json::Value),
    Binary(Bytes, String /* content_type */),
}

// ===== Config parsing =====

#[derive(Debug, Clone, Deserialize)]
pub struct EnsembleConfig {
    pub ensemble: EnsembleBlock,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EnsembleBlock {
    pub steps: Vec<EnsembleStepRaw>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EnsembleStepRaw {
    pub name: String,
    pub model: String,
    pub version: String,
    pub inputs: HashMap<String, String>,
    /// §4.1: tail streaming. The streaming step must be the DAG output
    /// (config `steps.last()` — explicit `output` lands with E2, batch 3).
    #[serde(default)]
    pub stream: bool,
}

#[derive(Debug, Clone)]
pub struct EnsembleStep {
    pub name: String,
    pub model: String,
    pub version: String,
    pub inputs: HashMap<String, String>,
    pub stream: bool,
}

lazy_static::lazy_static! {
    static ref REF_RE: Regex = Regex::new(r"^\$(\w+)(?:\.(\w+))?$")
        .expect("invalid ensemble ref regex");
}

/// Parse + validate + topologically sort a config file into the cached
/// [`EnsemblePlan`] (P0, D6): the plan is structured once at load time,
/// streaming/MIMO validation stacks on top of it (no parse logic double
/// write). The caller owns the file read — the cache re-parses only on
/// miss/eviction.
pub fn parse_ensemble_plan(content: &str, config_path: &PathBuf) -> Result<EnsemblePlan, AppError> {
    let config: EnsembleConfig = serde_yaml::from_str(content)
        .map_err(|e| AppError::Config(format!("failed to parse ensemble config: {}", e)))?;

    let steps: Vec<EnsembleStep> = config.ensemble.steps.into_iter().map(|s| EnsembleStep {
        name: s.name,
        model: s.model,
        version: s.version,
        inputs: s.inputs,
        stream: s.stream,
    }).collect();

    validate_dag(&steps)?;
    validate_stream_rules(&steps)?;
    let index_of: HashMap<&str, usize> = steps
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.as_str(), i))
        .collect();
    let layers = topological_layers(&steps)
        .into_iter()
        .map(|layer| layer.into_iter().map(|s| index_of[s.name.as_str()]).collect())
        .collect();

    Ok(EnsemblePlan {
        output_step: steps.len() - 1,
        steps,
        layers,
        config_path: config_path.clone(),
    })
}

/// Production plan loader: resolve the model dir, read config.yaml and parse
/// into a cached plan (P0). Wrapped by [`get_ensemble_plan`].
async fn load_ensemble_plan(
    repo_path: &std::path::Path,
    model: &str,
    version: &str,
) -> Result<Arc<EnsemblePlan>, AppError> {
    let model_dir = crate::validation::resolve_model_dir(repo_path, model, version)?;
    let config_path = model_dir.join("config.yaml");
    let content = tokio::fs::read_to_string(&config_path)
        .await
        .map_err(|e| AppError::Config(format!("failed to read ensemble config: {}", e)))?;
    parse_ensemble_plan(&content, &config_path).map(Arc::new)
}

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
        for step in &plan.steps {
            if !registry.is_ready(&step.model, Some(&step.version)) {
                // P6: sub-model preload + resolved-version side-table land
                // with E4 (batch 3) — log-only for now.
                warn!(
                    model = %step.model,
                    version = %step.version,
                    "ensemble warm: sub-model not ready (preload deferred to batch 3)"
                );
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

fn validate_dag(steps: &[EnsembleStep]) -> Result<(), AppError> {
    let step_names: HashSet<&str> = steps.iter().map(|s| s.name.as_str()).collect();

    // Check for duplicate names
    if step_names.len() != steps.len() {
        return Err(AppError::Config("duplicate step names in ensemble".to_string()));
    }

    // Build dependency graph
    let mut dependencies: HashMap<&str, HashSet<&str>> = HashMap::new();
    for step in steps {
        let deps = dependencies.entry(&step.name).or_default();
        for ref_str in step.inputs.values() {
            let caps = REF_RE.captures(ref_str).ok_or_else(|| {
                AppError::Config(format!("invalid reference format: {}", ref_str))
            })?;
            let source = caps.get(1).unwrap().as_str();
            if source != "request" && !step_names.contains(source) {
                return Err(AppError::Config(format!(
                    "step '{}' references unknown step '{}'",
                    step.name, source
                )));
            }
            if source != "request" {
                deps.insert(source);
            }
        }
    }

    // Kahn's algorithm for cycle detection
    let mut in_degree: HashMap<&str, usize> = steps.iter()
        .map(|s| (s.name.as_str(), 0))
        .collect();
    for (step_name, deps) in &dependencies {
        for _dep in deps {
            *in_degree.get_mut(*step_name).unwrap() += 1;
        }
    }

    let mut queue: Vec<&str> = in_degree.iter()
        .filter(|(_, d)| **d == 0)
        .map(|(n, _)| *n)
        .collect();
    let mut visited = 0;

    while let Some(node) = queue.pop() {
        visited += 1;
        for step in steps {
            if dependencies.get(step.name.as_str()).map(|d| d.contains(node)).unwrap_or(false) {
                let deg = in_degree.get_mut(step.name.as_str()).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    queue.push(&step.name);
                }
            }
        }
    }

    if visited != steps.len() {
        return Err(AppError::Config("cycle detected in ensemble DAG".to_string()));
    }

    Ok(())
}

/// §4.0/D16 form-dispatching validation for `stream: true` steps (batch 0:
/// only the non-pipeline form is open). Rule 2 (streaming output not
/// referenced) is the definition of the form split — a referenced streaming
/// output IS the pipeline form, rejected here with an explicit
/// batch-2 message instead of being mis-killed by rules 1-3. The same
/// dispatch point opens pipeline form in batch 2 (no validator rework).
///
/// Rules (non-pipeline form):
/// 1. streaming step must be in the last topological layer — implied by
///    "output not referenced" (topological last layer = no dependents), so
///    the form split above covers it.
/// 3. at most one streaming step per DAG.
/// 4. output-step semantics unified (E2 base, batch 0): with `output`
///    omitted the DAG output is `steps.last()`, which MUST be the streaming
///    step — otherwise a streaming DAG would silently produce nothing
///    streamable (B-m4: explicit `output` lands with E2 in batch 3).
/// §4.0/D16 form-dispatching validation for `stream: true` steps (batch 0:
/// only the non-pipeline form is open). Rule 2 (streaming output not
/// referenced) is the definition of the form split — a referenced streaming
/// output IS the pipeline form, rejected here with an explicit batch-2
/// message instead of being mis-killed by rules 1-3. The same dispatch
/// point opens pipeline form in batch 2 (no validator rework).
///
/// Rules (non-pipeline form):
/// 1. streaming step must be in the last topological layer — implied by
///    "output not referenced" (topological last layer = no dependents), so
///    the form split above covers it.
/// 3. at most one streaming step per DAG.
/// 4. output-step semantics unified (E2 base, batch 0): with `output`
///    omitted the DAG output is `steps.last()`, which MUST be the streaming
///    step — otherwise a streaming DAG would silently produce nothing
///    streamable (B-m4: explicit `output` lands with E2 in batch 3).
fn validate_stream_rules(steps: &[EnsembleStep]) -> Result<(), AppError> {
    let stream_steps: Vec<&EnsembleStep> = steps.iter().filter(|s| s.stream).collect();
    if stream_steps.is_empty() {
        return Ok(());
    }
    if stream_steps.len() > 1 {
        return Err(AppError::Config(
            "at most one streaming step per ensemble DAG (rule 3)".to_string(),
        ));
    }
    let tail = stream_steps[0];

    // Form split (D16): streaming output referenced by a downstream step =
    // pipeline form. Not open before 0.9.0 batch 2 — explicit error rather
    // than a misleading rule-1/2 rejection.
    let consumed = steps.iter().any(|s| {
        s.inputs.values().any(|r| {
            REF_RE
                .captures(r)
                .map(|c| c.get(1).map(|m| m.as_str()) == Some(tail.name.as_str()))
                .unwrap_or(false)
        })
    });
    if consumed {
        return Err(AppError::Config(format!(
            "pipeline streaming (streaming step '{}' output consumed by a \
             downstream step) opens in 0.9.0 (batch 2); this server accepts \
             only tail streaming (streaming step = final step, output to client)",
            tail.name
        )));
    }

    // Rule 4 (B-m4): output-step semantics unified — with `output` omitted
    // the DAG output is `steps.last()`, which must be the streaming step.
    if steps.last().map(|s| s.name.as_str()) != Some(tail.name.as_str()) {
        return Err(AppError::Config(format!(
            "streaming step '{}' must be the DAG output step (config last \
             step), or the DAG output is ambiguous; explicit `output:` lands \
             in 0.9.0 (batch 3)",
            tail.name
        )));
    }

    Ok(())
}

fn topological_layers(steps: &[EnsembleStep]) -> Vec<Vec<&EnsembleStep>> {
    let mut dependencies: HashMap<&str, HashSet<&str>> = HashMap::new();
    for step in steps {
        let deps = dependencies.entry(&step.name).or_default();
        for ref_str in step.inputs.values() {
            if let Some(caps) = REF_RE.captures(ref_str) {
                let source = caps.get(1).unwrap().as_str();
                if source != "request" {
                    deps.insert(source);
                }
            }
        }
    }

    let step_map: HashMap<&str, &EnsembleStep> = steps.iter()
        .map(|s| (s.name.as_str(), s))
        .collect();

    let mut in_degree: HashMap<&str, usize> = steps.iter()
        .map(|s| (s.name.as_str(), 0))
        .collect();
    for (step_name, deps) in &dependencies {
        for _dep in deps {
            *in_degree.get_mut(*step_name).unwrap() += 1;
        }
    }

    let mut layers: Vec<Vec<&EnsembleStep>> = Vec::new();
    let mut remaining: HashSet<&str> = steps.iter().map(|s| s.name.as_str()).collect();

    while !remaining.is_empty() {
        let layer: Vec<&EnsembleStep> = remaining.iter()
            .filter(|n| in_degree.get(**n).copied().unwrap_or(0) == 0)
            .map(|n| *step_map.get(n).unwrap())
            .collect();

        if layer.is_empty() {
            break; // Should not happen if validated
        }

        for step in &layer {
            remaining.remove(step.name.as_str());
            for other in &remaining {
                if dependencies.get(*other).map(|d| d.contains(step.name.as_str())).unwrap_or(false) {
                    *in_degree.get_mut(*other).unwrap() -= 1;
                }
            }
        }

        layers.push(layer);
    }

    layers
}

fn resolve_ref(ref_str: &str, context: &HashMap<String, EnsembleValue>) -> Result<EnsembleValue, AppError> {
    let caps = REF_RE.captures(ref_str).ok_or_else(|| {
        AppError::Config(format!("invalid reference: {}", ref_str))
    })?;
    let source = caps.get(1).unwrap().as_str();
    let field = caps.get(2).map(|m| m.as_str());

    let source_data = context.get(source).ok_or_else(|| {
        AppError::Config(format!("reference source not found: {}", source))
    })?;

    match source_data {
        EnsembleValue::Binary(_, _) => match field {
            // E7: $request (whole) on Binary → passthrough (root binary to first layer)
            None => {
                if source == "request" {
                    Ok(source_data.clone())
                } else {
                    // E7: $stepN (whole) on Binary → 400 (Option A scope boundary:
                    // binary must not flow between internal DAG steps)
                    Err(AppError::InvalidRequestBody(format!(
                        "cannot reference binary step output '{}' as a whole; \
                         binary values may only flow from the root input to the first layer \
                         or from the final layer to the client",
                        ref_str
                    )))
                }
            }
            // E7: $request.field on Binary → 400 (no field semantics on bytes)
            Some(_) => Err(AppError::InvalidRequestBody(format!(
                "cannot extract field '{}' from binary data; \
                 binary values have no field-level semantics",
                ref_str
            ))),
        },
        EnsembleValue::Json(v) => match field {
            None => Ok(EnsembleValue::Json(v.clone())),
            Some(f) => {
                let field_val = v.get(f).cloned().ok_or_else(|| {
                    AppError::Config(format!(
                        "cannot resolve '{}' from {}",
                        ref_str, v
                    ))
                })?;
                Ok(EnsembleValue::Json(field_val))
            }
        },
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
    /// explicit `output` (batch 3) this is `steps.last()`; validation ensures
    /// a streaming DAG's output step is its streaming step.
    pub output_step: usize,
    /// Source config file — mtime re-check (review ②) stats this path.
    pub config_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanKey {
    pub model: String,
    pub version: String,
}

enum PlanCell {
    /// Single-flight in progress: the holder parses; waiters park here.
    Loading(Vec<oneshot::Sender<Result<Arc<EnsemblePlan>, AppError>>>),
    Ready {
        plan: Arc<EnsemblePlan>,
        mtime: Option<std::time::SystemTime>,
        last_stat: tokio::time::Instant,
    },
}

/// P0: `DashMap<(model, version), Arc<EnsemblePlan>>` with single-flight
/// loads (review ④), mtime interval re-check (review ②, coarse tokio clock,
/// no syscall on the hot path) and model-prefix invalidation (review ③ —
/// `(m,"latest")` and `(m,"1")` are distinct keys, both must clear).
/// Invalidation hooks: lifecycle `unload_version` (via reload_model too) —
/// single collection point, fired BEFORE registry changes (D23).
pub struct EnsemblePlanCache {
    plans: DashMap<PlanKey, PlanCell>,
    stat_interval: Duration,
}

impl EnsemblePlanCache {
    pub fn new() -> Self {
        Self {
            plans: DashMap::new(),
            stat_interval: Duration::from_secs(1),
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
                    PlanCell::Loading(waiters) => {
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
                    vacant.insert(PlanCell::Loading(Vec::new()));
                    match load().await {
                        Ok(plan) => {
                            let mtime = std::fs::metadata(&plan.config_path)
                                .and_then(|m| m.modified())
                                .ok();
                            let waiters = match self.plans.get_mut(&key) {
                                Some(mut cell) => match cell.value_mut() {
                                    PlanCell::Loading(w) => std::mem::take(w),
                                    PlanCell::Ready { .. } => {
                                        unreachable!("holder owns the Loading cell")
                                    }
                                },
                                None => unreachable!("holder owns the entry"),
                            };
                            for w in waiters {
                                let _ = w.send(Ok(plan.clone()));
                            }
                            let mut cell = self.plans.get_mut(&key).unwrap();
                            *cell.value_mut() = PlanCell::Ready {
                                plan: plan.clone(),
                                mtime,
                                last_stat: tokio::time::Instant::now(),
                            };
                            return Ok(plan);
                        }
                        Err(e) => {
                            // Failed loads must NOT be cached: evict the
                            // placeholder so the next call re-parses (a fixed
                            // config heals without reload — behaviour parity
                            // with the uncached path). Waiters share the failure.
                            let waiters = match self.plans.get_mut(&key) {
                                Some(mut cell) => match cell.value_mut() {
                                    PlanCell::Loading(w) => std::mem::take(w),
                                    PlanCell::Ready { .. } => {
                                        unreachable!("holder owns the Loading cell")
                                    }
                                },
                                None => unreachable!("holder owns the entry"),
                            };
                            self.plans.remove(&key);
                            for w in waiters {
                                let _ = w.send(Err(AppError::Internal(
                                    "ensemble plan load failed".to_string(),
                                )));
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

// ===== Execution =====

/// Execution-face parameters (D37, fixed once at batch 0 — later batches add
/// fields without touching the signature, e.g. `dag_selector` in batch 5;
/// D36's snapshot/depth ride the internal `execute_ensemble_inner`, not the
/// public opts).
#[derive(Debug, Clone)]
pub struct EnsembleExecOpts {
    pub client_ip: String,
    pub deadline_unix_ns: Option<i64>,
    /// StreamOpen.decoupled passthrough (batch 0).
    pub decoupled: bool,
}

/// D25: batch-0 result — Unary keeps the historical byte-identical path;
/// Stream carries the tail stream (and, from batch 2, the whole chain).
pub enum EnsembleOutcome {
    Unary(EnsembleValue),
    Stream(EnsembleStream),
}

/// Chain-handle element (D25): batch 0 always holds exactly the tail stream;
/// batch 2 pushes the chain as "tail first, then upstream". Adapter layers
/// read only the top-level quick-access fields — the seam stays fixed.
#[derive(Clone)]
pub struct StreamHandle {
    pub stream_id: String,
    pub cancel_client: Arc<crate::transport::zmq::WorkerZmqClient>,
    pub abort: tokio::task::AbortHandle,
}

/// The streaming result handed to the adapter layers. `chain` is the single
/// source of truth (tail included) once filled; top-level fields are the
/// tail quick-access the adapters already consume (same type as
/// `open_worker_stream`'s return — the SSE/WS/gRPC forward loops change
/// zero lines).
pub struct EnsembleStream {
    /// Tail stream — same type as `send_stream`'s receiver; the existing
    /// adapter forward loops consume it as-is.
    pub chunk_rx: mpsc::Receiver<pb::StreamResponse>,
    pub stream_id: String,
    pub cancel_client: Arc<crate::transport::zmq::WorkerZmqClient>,
    pub tail_model: String,
    pub tail_version: String,
    /// D35 (batch 3): E5 `timeout_secs` converted to a step wall-clock cap —
    /// the adapter's recv_chunk overall takes min(client overall, this).
    /// None = inactive.
    pub step_deadline: Option<std::time::Instant>,
    pub chain: Vec<StreamHandle>,
    /// D18: chain task tree unified teardown. Batch 0 = the tail stream
    /// itself (inert placeholder); batch 2 roots the whole chain here.
    pub abort: tokio::task::AbortHandle,
    /// P10 (D40): owned semaphore permit — released when the stream is
    /// dropped (terminal frame / adapter task end), the same single path as
    /// D18's teardown. None = capacity not configured (0/unlimited).
    pub permit: Option<StreamingPermit>,
}

/// P10 (D40): global streaming-DAG capacity — the semaphore that bounds
/// streaming residency (§6.3.1: streaming bypasses the queue, so a fan-out
/// has no other memory bound). Global scope (the derivation M = N × D_stream
/// is a global memory model; per-model caps make the bound unprovable).
/// Configuration: `server.max_concurrent_streaming_dags` (default 128;
/// 0 = unlimited, capacity not installed).
pub struct StreamingCapacityState {
    semaphore: Arc<tokio::sync::Semaphore>,
    in_use: Arc<std::sync::atomic::AtomicUsize>,
}

/// RAII permit handed to [`EnsembleStream`]. The Arc inner decouples the
/// release from the stream struct's own Drop (the adapter moves fields out
/// of EnsembleStream, which forbids a Drop impl on it): when the last
/// reference goes away, the semaphore permit is returned and the gauge is
/// synced (D40: owned permit, released via the D18 teardown path).
#[derive(Clone)]
pub struct StreamingPermit {
    // Held (not read) for RAII lifetime semantics; the inner's Drop returns
    // the semaphore slot and syncs the gauge.
    _inner: Arc<StreamingPermitInner>,
}

struct StreamingPermitInner {
    // Held (not read): owning the permit is the point — dropping it releases
    // the semaphore slot.
    _permit: tokio::sync::OwnedSemaphorePermit,
    in_use: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for StreamingPermitInner {
    fn drop(&mut self) {
        let prev = self.in_use.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        crate::metrics::prometheus::set_ensemble_streaming_active(prev.saturating_sub(1));
    }
}

impl StreamingCapacityState {
    pub fn new(limit: usize) -> Self {
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(limit)),
            in_use: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Acquire one streaming-DAG slot. Immediate rejection on exhaustion —
    /// no queueing (queueing turns an unbounded-stream memory problem into a
    /// latency one, and the pre-layers already waited in the queue once).
    pub fn try_acquire(&self) -> Result<StreamingPermit, AppError> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                let prev = self.in_use.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                crate::metrics::prometheus::set_ensemble_streaming_active(prev + 1);
                Ok(StreamingPermit {
                    _inner: Arc::new(StreamingPermitInner {
                        _permit: permit,
                        in_use: self.in_use.clone(),
                    }),
                })
            }
            Err(_) => Err(AppError::StreamingCapacityExceeded(
                "concurrent streaming ensemble DAG limit reached (429); \
                 reduce concurrency or raise server.max_concurrent_streaming_dags"
                    .to_string(),
            )),
        }
    }
}

pub async fn execute_ensemble(
    state: Arc<AppState>,
    model_name: &str,
    version: &str,
    payload: EnsembleValue,
    request_id: &str,
    opts: EnsembleExecOpts,
) -> Result<EnsembleOutcome, AppError> {
    // P0 (D6): plan comes from the cache — parse/validate/layers run once per
    // config version, not per request. In-flight requests hold their Arc and
    // finish on the old plan even across a reload (D23).
    let plan = get_ensemble_plan(&state, model_name, version).await?;
    let deadline_unix_ns = opts.deadline_unix_ns;
    let tail_idx = plan.output_step;

    let mut context: HashMap<String, EnsembleValue> = HashMap::new();
    context.insert("request".to_string(), payload);

    if !plan.steps[tail_idx].stream {
        // Historical unary path — byte-identical behaviour.
        run_layers(
            &state, &plan, &plan.layers, &mut context,
            model_name, version, request_id, &opts, deadline_unix_ns,
        ).await?;
        let value = plan.steps[tail_idx].name.as_str();
        let value = context.get(value).cloned()
            .ok_or_else(|| AppError::Internal("ensemble produced no output".to_string()))?;
        return Ok(EnsembleOutcome::Unary(value));
    }

    // Streaming DAG (D25: run_pre_layers / open_tail_stream split — batch 2
    // extends the split into a chain without rewriting the main flow).
    let tail_layer = plan.layers.iter().position(|l| l.contains(&tail_idx))
        .ok_or_else(|| AppError::Internal("output step missing from layers".to_string()))?;
    // P7: the stream-open preflight (route pick + meta build — nothing that
    // depends on the pre-layer results) runs IN PARALLEL with the pre-layers,
    // so the TTFT critical path after the layers is just assemble + send.
    // The autoload path stays serial (preflight only when already ready).
    let preflight_fut = async {
        tail_stream_preflight(
            &state, &plan.steps[tail_idx], request_id, &opts, deadline_unix_ns,
        )
        .await
    };
    let layers_fut = async {
        run_layers(
            &state, &plan, &plan.layers[..tail_layer], &mut context,
            model_name, version, request_id, &opts, deadline_unix_ns,
        )
        .await
    };
    let (preflight, layers_res) = tokio::join!(preflight_fut, layers_fut);
    layers_res?;

    // P10 (D40): acquire a streaming-DAG slot BEFORE opening the stream —
    // immediate 429 rejection on exhaustion (no queueing; the pre-layers
    // already waited in the queue once). The owned permit rides the stream
    // and releases when the adapter's forward task ends (D18 teardown path).
    let mut stream = open_tail_stream(
        &state, &plan, tail_idx, tail_layer, &context,
        request_id, &opts, deadline_unix_ns, preflight?,
    ).await?;
    if let Some(capacity) = state.worker_manager.streaming_capacity() {
        stream.permit = Some(capacity.try_acquire()?);
    }
    Ok(EnsembleOutcome::Stream(stream))
}

/// Layer-barrier executor (historical engine): runs `layers` serially, each
/// layer in a JoinSet (P-FLOW shared cancel), writing step outputs into
/// `context`. Step errors propagate directly (B3). The whole run is bounded
/// by a single shared deadline (P-DEADLINE §4.0.10): an N-layer ensemble can
/// never exceed the parent; the per-step timeout in execute_step is the
/// inner safety net, this outer deadline bounds the total.
async fn run_layers(
    state: &Arc<AppState>,
    plan: &EnsemblePlan,
    layers: &[Vec<usize>],
    context: &mut HashMap<String, EnsembleValue>,
    model_name: &str,
    version: &str,
    request_id: &str,
    opts: &EnsembleExecOpts,
    deadline_unix_ns: Option<i64>,
) -> Result<(), AppError> {
    let total_budget = crate::deadline::remaining(deadline_unix_ns);
    let plan_run = plan.clone();
    let ensemble_run = async {
        for layer in layers {
            // P-FLOW (§4.0.9): a JoinSet per layer is the ensemble's shared
            // cancel. On any early exit — a step error, the outer total-budget
            // timeout, or the parent request being dropped (client disconnect) —
            // the JoinSet is dropped and tokio ABORTS every in-flight step task
            // in the layer, so a cancelled ensemble does not leave sub-steps
            // running on workers (detached `tokio::spawn` would outlive the
            // parent). Completed tasks are no-ops to abort.
            let mut set: tokio::task::JoinSet<(String, Result<EnsembleValue, AppError>)> =
                tokio::task::JoinSet::new();
            for &step_idx in layer {
                let state = state.clone();
                let ctx = context.clone();
                let step = plan_run.steps[step_idx].clone();
                let ensemble_name = model_name.to_string();
                let request_id = request_id.to_string();
                let client_ip = opts.client_ip.clone();
                let deadline_unix_ns = deadline_unix_ns;
                set.spawn(async move {
                    let start = Instant::now();
                    let result =
                        execute_step(state, &step, &ctx, &request_id, &client_ip, deadline_unix_ns)
                            .await;
                    let latency = start.elapsed().as_secs_f64();
                    crate::metrics::prometheus::record_ensemble_step_latency(
                        &ensemble_name, &step.name, &step.model, &step.version, latency,
                    );
                    (step.name, result)
                });
            }

            while let Some(joined) = set.join_next().await {
                let (name, result) = joined.map_err(|e| {
                    AppError::Internal(format!("ensemble step join error: {}", e))
                })?;
                match result {
                    Ok(value) => {
                        context.insert(name, value);
                    }
                    Err(e) => {
                        // B3: propagate step errors directly — client errors
                        // (e.g. InvalidRequestBody from resolve_ref E7 rules)
                        // must reach the HTTP/gRPC layer with their correct
                        // status code, not be wrapped in Internal(500).
                        return Err(e);
                    }
                }
            }
        }
        Ok::<(), AppError>(())
    };

    match total_budget {
        Some(b) => {
            tokio::time::timeout(b, ensemble_run)
                .await
                .map_err(|_| AppError::InferenceTimeout(format!(
                    "ensemble {} {} exceeded total deadline of {:.1}s",
                    model_name, version, b.as_secs_f64()
                )))??;
        }
        // No deadline (no client spec AND server.timeout<=0): unbounded DAG run.
        None => {
            ensemble_run.await?;
        }
    }
    Ok(())
}

/// D25: open the tail stream — build/route the stream-open and return the
/// stream handle. Batch 0: the tail's same-layer sibling unary steps run in
/// parallel with the stream (rule 3: they produce no DAG output — results
/// dropped, failures warn only); batch 2 replaces this function's body with
/// the chain spawn without touching the caller seam.
async fn open_tail_stream(
    state: &Arc<AppState>,
    plan: &EnsemblePlan,
    tail_idx: usize,
    tail_layer: usize,
    context: &HashMap<String, EnsembleValue>,
    request_id: &str,
    opts: &EnsembleExecOpts,
    deadline_unix_ns: Option<i64>,
    preflight: Option<TailPreflight>,
) -> Result<EnsembleStream, AppError> {
    // §4.1 rule 3: same-layer sibling unary steps still run in parallel with
    // the streaming step. They never enter `context` (output semantics belong
    // to the streaming step) and cannot affect the stream contract, so their
    // results are dropped; the JoinSet gives them the same P-FLOW drop-abort
    // cancellation as every other layer.
    let siblings: Vec<usize> = plan.layers[tail_layer]
        .iter()
        .copied()
        .filter(|&i| i != tail_idx)
        .collect();

    let run_stream = execute_stream_step(state, plan, tail_idx, context, request_id, opts, deadline_unix_ns, preflight);
    tokio::pin!(run_stream);

    if siblings.is_empty() {
        return run_stream.await;
    }

    let run_siblings = async {
        let mut set: tokio::task::JoinSet<(String, Result<EnsembleValue, AppError>)> =
            tokio::task::JoinSet::new();
        for idx in siblings {
            let state = state.clone();
            let ctx = context.clone();
            let step = plan.steps[idx].clone();
            let request_id = request_id.to_string();
            let client_ip = opts.client_ip.clone();
            set.spawn(async move {
                let name = step.name.clone();
                execute_step(state, &step, &ctx, &request_id, &client_ip, deadline_unix_ns).await
                    .map(|v| (name.clone(), Ok(v)))
                    .unwrap_or_else(|e| (name.clone(), Err(e)))
            });
        }
        while let Some(joined) = set.join_next().await {
            if let Ok((name, Err(e))) = joined {
                warn!(step = %name, error = %e, "tail-layer sibling step failed (result dropped)");
            }
        }
    };
    tokio::pin!(run_siblings);

    // Whichever finishes first wins: if the stream opens first, the siblings
    // future is dropped — its JoinSet drop-aborts in-flight steps (P-FLOW),
    // exactly like a cancelled layer. If the siblings finish first, we await
    // the stream (their failures were already logged).
    let stream_result = tokio::select! {
        _ = &mut run_siblings => None,
        r = &mut run_stream => Some(r),
    };
    match stream_result {
        Some(r) => r,
        None => run_stream.await,
    }
}

/// P7: stream-open preflight — everything that does NOT depend on the
/// pre-layer results (route pick + meta build; autoload readiness check) is
/// done in parallel with the pre-layers, so the TTFT critical path only
/// waits for the assembled payload before send_stream. Returns None when the
/// sub-model was not ready at preflight time — the autoload path stays
/// serial after the pre-layers (P7 constraint: never preflight a stream for
/// a model still loading).
struct TailPreflight {
    meta: pb::RequestMeta,
    worker_id: usize,
    clients: Vec<Arc<crate::transport::zmq::WorkerZmqClient>>,
}

async fn tail_stream_preflight(
    state: &Arc<AppState>,
    step: &EnsembleStep,
    request_id: &str,
    opts: &EnsembleExecOpts,
    deadline_unix_ns: Option<i64>,
) -> Result<Option<TailPreflight>, AppError> {
    // P7 constraint: only preflight an already-ready sub-model. Not ready →
    // fall back to the serial path (which runs the autoload + poll).
    if !state.registry.is_ready(&step.model, Some(&step.version)) {
        return Ok(None);
    }
    if !streaming_worker_ready(state, &step.model, &step.version, &pb::RequestMeta::default()).await {
        return Ok(None);
    }
    let mv = state.registry.get(&step.model, Some(&step.version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", step.model, step.version)))?;
    let clients = state.worker_manager.get_zmq_clients(&step.model, &step.version).await
        .ok_or_else(|| AppError::WorkerCrashed(format!("{} {} has no ZMQ clients", step.model, step.version)))?;

    // P-TRACE: step-level span + trace injection (content-type is patched in
    // after assembly — it depends on the payload shape).
    let mut step_headers = HashMap::new();
    {
        let step_span = tracing::info_span!(
            "ensemble.step",
            step = %step.name,
            model = %step.model,
            trace_id = tracing::field::Empty,
            span_id = tracing::field::Empty,
        );
        crate::telemetry::link_parent(&step_span, &opentelemetry::Context::current());
        let _guard = step_span.enter();
        crate::telemetry::inject(&mut step_headers);
    }
    let meta = build_step_meta(
        step, request_id, &opts.client_ip, deadline_unix_ns, step_headers, bytes::Bytes::new(),
    );
    let outlier = state.worker_manager.get_outlier_state(&step.model, &step.version).await;
    let seq_registry = state.inference_queue.sequence_registry();
    let worker_id = crate::worker::pick_streaming_worker(
        &meta, mv.workers.len(), outlier.as_deref(), seq_registry, &step.model, &step.version,
    ).map_err(|e| AppError::Validation(e.0))?;
    if worker_id >= clients.len() {
        return Err(AppError::WorkerCrashed("invalid worker index".to_string()));
    }
    Ok(Some(TailPreflight { meta, worker_id, clients }))
}

/// D19: streaming readiness predicate — the sub-model must be reachable via
/// routing (pick_streaming_worker returns a valid worker), NOT merely
/// registry-ready: mark_ready and the streaming route table registration have
/// a window between them, so a pick failure is a retryable state, never an
/// instant 500.
async fn streaming_worker_ready(
    state: &Arc<AppState>,
    model: &str,
    version: &str,
    meta: &pb::RequestMeta,
) -> bool {
    let Some(mv) = state.registry.get(model, Some(version)) else { return false; };
    if mv.workers.is_empty() {
        return false;
    }
    let Some(clients) = state.worker_manager.get_zmq_clients(model, version).await else {
        return false;
    };
    if clients.is_empty() {
        return false;
    }
    let outlier = state.worker_manager.get_outlier_state(model, version).await;
    let seq = state.inference_queue.sequence_registry();
    match crate::worker::pick_streaming_worker(meta, mv.workers.len(), outlier.as_deref(), seq, model, version) {
        Ok(w) => w < clients.len(),
        Err(_) => false,
    }
}

/// Shared sub-model autoload (unary + streaming execution faces): config
/// parse/validation errors surface as ModelNotReady; load failures surface
/// directly. The caller polls its own readiness predicate afterwards (unary =
/// registry ready, streaming = `streaming_worker_ready`).
async fn ensure_sub_model_loaded(
    state: &Arc<AppState>,
    step: &EnsembleStep,
) -> Result<(), AppError> {
    if state.registry.is_ready(&step.model, Some(&step.version)) {
        return Ok(());
    }
    let load_start = Instant::now();
    info!("Auto-loading sub-model {} v{} for ensemble", step.model, step.version);
    let sub_model_dir = crate::validation::resolve_model_dir(
        &state.repo_path, &step.model, &step.version,
    )?;
    // 配置解析/校验失败必须可见(同 reconcile:不再 unwrap_or_default
    // 静默回退默认配置;M7 迁移哨兵依赖此错误上浮)。
    let mut config = match crate::config::load_model_config(
        &sub_model_dir.join("config.yaml")
    ) {
        Ok(c) => c,
        Err(e) => {
            return Err(AppError::ModelNotReady(format!(
                "sub-model {} v{} has invalid config.yaml: {}", step.model, step.version, e
            )));
        }
    };
    state.config.apply_model_defaults(&mut config);
    if let Err(e) = state.worker_manager.load_model(&step.model, &step.version, &config).await {
        warn!("Failed to auto-load sub-model {} v{}: {}", step.model, step.version, e);
        return Err(AppError::ModelNotReady(format!(
            "sub-model {} v{} not ready: {}", step.model, step.version, e
        )));
    }
    // m4: autoload wait (the dominant cold-TTFT term for ensemble DAGs).
    crate::metrics::prometheus::record_ensemble_autoload_wait_seconds(
        load_start.elapsed().as_secs_f64(),
    );
    Ok(())
}

/// Execute the streaming tail step: resolve inputs, assemble payload (same
/// rules as unary), D19 readiness poll (skipped when a P7 preflight is
/// provided), build meta, route and open the stream. Returns the stream
/// handle consumed by the adapter layers.
async fn execute_stream_step(
    state: &Arc<AppState>,
    plan: &EnsemblePlan,
    step_idx: usize,
    context: &HashMap<String, EnsembleValue>,
    request_id: &str,
    opts: &EnsembleExecOpts,
    deadline_unix_ns: Option<i64>,
    preflight: Option<TailPreflight>,
) -> Result<EnsembleStream, AppError> {
    let step = &plan.steps[step_idx];

    // Resolve inputs into EnsembleValues (identical to the unary path).
    let mut resolved: HashMap<String, EnsembleValue> = HashMap::new();
    for (key, ref_str) in &step.inputs {
        let value = resolve_ref(ref_str, context)?;
        resolved.insert(key.clone(), value);
    }
    let (payload_bytes, content_type_for_step) = assemble_step_payload(&step.name, &resolved)?;

    // P7: when the preflight already routed/built meta in parallel with the
    // pre-layers, skip the serial readiness/meta/pick block.
    let (mut meta, worker_id, clients) = match preflight {
        Some(pf) => (pf.meta, pf.worker_id, pf.clients),
        None => {
            // D19: ensure the sub-model is loaded, then poll STREAMING
            // readiness (pick non-empty) with backoff. A pick failure is a
            // retryable state.
            ensure_sub_model_loaded(state, step).await?;
            let mut retries = 0;
            let max_retries = 30;
            let mut delay = Duration::from_millis(50);
            while !streaming_worker_ready(state, &step.model, &step.version, &pb::RequestMeta::default()).await {
                if retries >= max_retries {
                    return Err(AppError::ModelNotReady(format!(
                        "sub-model {} v{} streaming not ready", step.model, step.version
                    )));
                }
                // D19: quick-fail when the remaining budget cannot cover one
                // more poll — never spin to timeout.
                if let Some(rem) = crate::deadline::remaining(deadline_unix_ns) {
                    if rem < delay {
                        return Err(AppError::InferenceTimeout(format!(
                            "ensemble step {}: deadline exhausted while waiting for sub-model {} v{}",
                            step.name, step.model, step.version
                        )));
                    }
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_millis(500));
                retries += 1;
            }

            let mv = state.registry.get(&step.model, Some(&step.version))
                .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", step.model, step.version)))?;
            let clients = state.worker_manager.get_zmq_clients(&step.model, &step.version).await
                .ok_or_else(|| AppError::WorkerCrashed(format!("{} {} has no ZMQ clients", step.model, step.version)))?;

            // P-TRACE (同 execute_step): step-level span links to the parent
            // trace and injects its context into the step headers so the
            // worker spans land as children of the step.
            let mut step_headers = HashMap::new();
            if let Some(ct) = &content_type_for_step {
                step_headers.insert("content-type".to_string(), ct.clone());
            }
            {
                let step_span = tracing::info_span!(
                    "ensemble.step",
                    step = %step.name,
                    model = %step.model,
                    trace_id = tracing::field::Empty,
                    span_id = tracing::field::Empty,
                );
                crate::telemetry::link_parent(&step_span, &opentelemetry::Context::current());
                let _guard = step_span.enter();
                crate::telemetry::inject(&mut step_headers);
            }

            // Streaming meta carries no payload — the body rides StreamOpen.data.
            let meta = build_step_meta(
                step, request_id, &opts.client_ip, deadline_unix_ns, step_headers, bytes::Bytes::new(),
            );

            let outlier = state.worker_manager.get_outlier_state(&step.model, &step.version).await;
            let seq_registry = state.inference_queue.sequence_registry();
            let worker_id = crate::worker::pick_streaming_worker(
                &meta, mv.workers.len(), outlier.as_deref(), seq_registry, &step.model, &step.version,
            ).map_err(|e| AppError::Validation(e.0))?;
            if worker_id >= clients.len() {
                return Err(AppError::WorkerCrashed("invalid worker index".to_string()));
            }
            (meta, worker_id, clients)
        }
    };

    // The preflight built meta before the payload was assembled — patch in
    // the content-type for binary passthrough (B3/E7).
    if let Some(ct) = &content_type_for_step {
        meta.headers
            .entry("content-type".to_string())
            .or_insert_with(|| ct.clone());
    }

    // D19: final deadline check before opening the stream — an expired
    // budget must not open one (deadline-exhausted mapping, §4.4).
    if let Some(rem) = crate::deadline::remaining(deadline_unix_ns) {
        if rem.is_zero() {
            return Err(AppError::InferenceTimeout(format!(
                "ensemble step {}: deadline exhausted before stream open", step.name
            )));
        }
    }
    crate::metrics::prometheus::record_worker_inference(&step.model, &step.version, worker_id, 1);

    let client = &clients[worker_id];
    let stream_id = format!("stream-{}", Uuid::new_v4());
    let open_req = crate::streaming::build_stream_open(
        stream_id.clone(), payload_bytes, Some(meta), opts.decoupled,
    );
    let chunk_rx = client.send_stream(open_req, stream_id.clone()).await?;

    // D25: chain handles — batch 0 = the tail stream itself as the single
    // element (chain[0] === the top-level fields; adapters never read chain).
    let abort = tokio::spawn(async {}).abort_handle();
    let chain = vec![StreamHandle {
        stream_id: stream_id.clone(),
        cancel_client: Arc::clone(client),
        abort: abort.clone(),
    }];

    Ok(EnsembleStream {
        chunk_rx,
        stream_id,
        cancel_client: Arc::clone(client),
        tail_model: step.model.clone(),
        tail_version: step.version.clone(),
        // D35 (batch 3): E5 timeout_secs → step wall-clock cap; adapter
        // recv_chunk overall takes min(client overall, this). Inactive until
        // E5 lands.
        step_deadline: None,
        chain,
        abort,
        // P10: filled by execute_ensemble after open_tail_stream returns.
        permit: None,
    })
}

/// Shared step RequestMeta (unary + streaming): request_id `{parent}:{step}`
/// suffix, trace-injected headers, deadline cascade, content-type passthrough
/// (B3/E7). `payload` is the serialized body — empty for streaming, where the
/// body rides StreamOpen.data.
fn build_step_meta(
    step: &EnsembleStep,
    request_id: &str,
    client_ip: &str,
    deadline_unix_ns: Option<i64>,
    step_headers: HashMap<String, String>,
    payload: bytes::Bytes,
) -> pb::RequestMeta {
    pb::RequestMeta {
        route: "/predict".to_string(),
        headers: step_headers,
        client_ip: client_ip.to_string(),
        // Correlate sub-step requests with the client-facing request ID;
        // the step-name suffix keeps each step uniquely identifiable.
        request_id: format!("{}:{}", request_id, step.name),
        timestamp_ns: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64,
        payload,
        // P-DEADLINE cascade: child step shares the parent deadline so a single
        // step cannot exceed it (budget = parent − already elapsed).
        deadline_unix_ns,
        ..Default::default()
    }
}

/// P9: cheap size estimate for with_capacity — recursive walk, no allocation
/// (numbers fall back to a fixed f64-bound; the estimate only needs to be
/// within a constant factor of the serialized size to eliminate the realloc
/// chain in assemble_step_payload).
fn json_value_len_estimate(v: &serde_json::Value) -> usize {
    match v {
        Value::Null => 4,
        Value::Bool(b) => {
            if *b {
                4
            } else {
                5
            }
        }
        Value::Number(_) => 16,
        Value::String(s) => s.len() + 2,
        Value::Array(a) => {
            a.iter().map(json_value_len_estimate).sum::<usize>() + a.len() + 2
        }
        Value::Object(o) => o
            .iter()
            .map(|(k, v)| k.len() + 3 + json_value_len_estimate(v))
            .sum::<usize>()
            + 2,
    }
}

/// B3 (E7): step input assembly — three branches.
///   all Json                                → build a JSON object (historical path)
///   exactly one input, a whole Binary       → raw bytes passthrough + content-type
///   any other combination involving Binary  → 400 (Option B scope)
///
/// P9: the Json branch pre-sizes its buffer from a cheap estimate (no
/// realloc chain) and hands the Vec straight to `Bytes` (ownership transfer,
/// zero copy — the queue payload stays Bytes end-to-end, §1.4); the Binary
/// branch passes the Bytes through by refcount (P2: the old `to_vec` copy is
/// gone).
fn assemble_step_payload(
    step_name: &str,
    resolved: &HashMap<String, EnsembleValue>,
) -> Result<(bytes::Bytes, Option<String>), AppError> {
    let binary_count = resolved
        .values()
        .filter(|v| matches!(v, EnsembleValue::Binary(_, _)))
        .count();

    if binary_count == 0 {
        // All Json → build JSON object (historical path).
        let mut obj = serde_json::Map::new();
        for (key, val) in resolved {
            match val {
                EnsembleValue::Json(v) => {
                    obj.insert(key.clone(), v.clone());
                }
                EnsembleValue::Binary(_, _) => unreachable!(),
            }
        }
        let estimate: usize = obj
            .iter()
            .map(|(k, v)| k.len() + 3 + json_value_len_estimate(v))
            .sum::<usize>()
            + 2;
        let mut buf = Vec::with_capacity(estimate);
        let mut ser = serde_json::Serializer::new(&mut buf);
        // Value serialization is infallible (E8 cleanup note).
        serde::Serialize::serialize(&Value::Object(obj), &mut ser)
            .expect("Value serialization is infallible");
        Ok((bytes::Bytes::from(buf), None))
    } else if resolved.len() == 1 && binary_count == 1 {
        // Exactly one input and it is Binary → raw bytes passthrough.
        let (data, ct) = match resolved.values().next().unwrap() {
            EnsembleValue::Binary(data, ct) => (data.clone(), ct.clone()),
            EnsembleValue::Json(_) => unreachable!(),
        };
        Ok((data, Some(ct)))
    } else {
        // Mixed or multiple inputs with any Binary → 400 (Option B scope).
        Err(AppError::InvalidRequestBody(format!(
            "step '{}' has {} input(s) with mixed JSON/Binary; \
             a binary input must be the step's sole whole input (Option B scope)",
            step_name,
            resolved.len()
        )))
    }
}

/// B3 (E8): typed step output — mirrors the unary media_type dispatch
/// (inference.rs:266-267). A non-JSON media_type declares the payload opaque
/// bytes; the JSON path is validated and errors are NOT swallowed (the old
/// code collapsed invalid JSON to `{}` — this is the regression pin).
fn parse_step_output(step_name: &str, single: pb::SingleResponse) -> Result<EnsembleValue, AppError> {
    let is_binary = !single.media_type.is_empty()
        && !single.media_type.starts_with("application/json");
    if is_binary {
        Ok(EnsembleValue::Binary(single.data, single.media_type))
    } else if single.data.is_empty() {
        Ok(EnsembleValue::Json(json!({})))
    } else {
        let v: Value = serde_json::from_slice(&single.data).map_err(|e| {
            AppError::Internal(format!(
                "ensemble step {} returned invalid JSON: {}",
                step_name, e
            ))
        })?;
        Ok(EnsembleValue::Json(v))
    }
}

async fn execute_step(
    state: Arc<AppState>,
    step: &EnsembleStep,
    context: &HashMap<String, EnsembleValue>,
    request_id: &str,
    client_ip: &str,
    deadline_unix_ns: Option<i64>,
) -> Result<EnsembleValue, AppError> {
    // Resolve inputs into EnsembleValues.
    let mut resolved: HashMap<String, EnsembleValue> = HashMap::new();
    for (key, ref_str) in &step.inputs {
        let value = resolve_ref(ref_str, context)?;
        resolved.insert(key.clone(), value);
    }

    // B3 (E7): input assembly — three branches (see assemble_step_payload).
    let (payload_bytes, content_type_for_step) = assemble_step_payload(&step.name, &resolved)?;

    // Ensure sub-model is ready (shared autoload; unary readiness predicate)
    ensure_sub_model_loaded(&state, step).await?;
    // Poll with exponential backoff for worker readiness
    let mut retries = 0;
    let max_retries = 30;
    let mut delay = Duration::from_millis(50);
    while !state.registry.is_ready(&step.model, Some(&step.version)) && retries < max_retries {
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_millis(500));
        retries += 1;
    }

    if !state.registry.is_ready(&step.model, Some(&step.version)) {
        return Err(AppError::ModelNotReady(format!(
            "sub-model {} v{} is not ready", step.model, step.version
        )));
    }

    // Get model version info
    let mv = state.registry.get(&step.model, Some(&step.version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", step.model, step.version)))?;

    if mv.model_type == ModelType::Ensemble {
        return Err(AppError::Internal("nested ensemble not supported".to_string()));
    }

    let num_workers = mv.workers.len();
    if num_workers == 0 {
        return Err(AppError::WorkerCrashed(format!("{} has no workers", step.model)));
    }

    // Send inference request through the unified queue
    let uid = format!("ensemble_{}_{}_{}", step.model, step.version, Uuid::new_v4());

    // P-TRACE (蓝图 §4.3 ensemble 接线，防 trace 断裂): the sub-step RequestMeta
    // would otherwise carry empty headers, orphaning every step span from the
    // parent request trace. Build a child `ensemble.step` span linked to the
    // current (parent) trace and inject its context into the step headers so the
    // worker spans land as children of the step (request_id is already
    // `{parent}:{step}`, trace follows the same shape).
    let mut step_headers = HashMap::new();
    // B3 (E7): when the step input is binary, tell the worker via content-type.
    if let Some(ref ct) = content_type_for_step {
        step_headers.insert("content-type".to_string(), ct.clone());
    }
    {
        let step_span = tracing::info_span!(
            "ensemble.step",
            step = %step.name,
            model = %step.model,
            trace_id = tracing::field::Empty,
            span_id = tracing::field::Empty,
        );
        crate::telemetry::link_parent(&step_span, &opentelemetry::Context::current());
        let _guard = step_span.enter();
        crate::telemetry::inject(&mut step_headers);
    }

    let meta = build_step_meta(
        step, request_id, client_ip, deadline_unix_ns, step_headers,
        payload_bytes.clone(),
    );

    let (response_tx, response_rx) = oneshot::channel();
    let item = crate::inference_queue::QueueItem {
        uid: uid.clone(),
        data: payload_bytes,
        meta: Some(std::sync::Arc::new(meta)),
        response_tx,
        inflight_guard: None,
        enqueued_at: std::time::Instant::now(),
    };

    match state.inference_queue.try_submit(&step.model, &step.version, item) {
        Ok(()) => {}
        Err(crate::inference_queue::QueueError::Full) => {
            return Err(AppError::QueueFull(format!(
                "Queue full for {} {}", step.model, step.version
            )));
        }
        Err(_) => {
            return Err(AppError::ModelNotReady(format!(
                "Queue not available for {} {}", step.model, step.version
            )));
        }
    }

    // P-DEADLINE cascade: bound this step by the parent deadline's remaining
    // budget (None = no deadline → unbounded inner wait, outer DAG bound still
    // applies via execute_ensemble's total_budget).
    let response = match crate::deadline::remaining(deadline_unix_ns) {
        Some(timeout_duration) => match timeout(timeout_duration, response_rx).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(_)) => {
                return Err(AppError::InferenceTimeout(format!(
                    "ensemble step {} response channel closed", step.name
                )));
            }
            Err(_) => {
                return Err(AppError::InferenceTimeout(format!(
                    "ensemble step {} timed out", step.name
                )));
            }
        },
        None => match response_rx.await {
            Ok(resp) => resp,
            Err(_) => {
                return Err(AppError::InferenceTimeout(format!(
                    "ensemble step {} response channel closed", step.name
                )));
            }
        },
    };

    match response.payload {
        Some(pb::response::Payload::Single(single)) => {
            let code = single.status.as_ref().map(|s| s.code.as_str()).unwrap_or("Ok");
            match code {
                "Ok" => {
                    // B3 (E8): typed output (see parse_step_output).
                    parse_step_output(&step.name, single)
                }
                "Error" => {
                    let msg = single.status.as_ref().and_then(|s| {
                        if s.message.is_empty() { None } else { Some(s.message.clone()) }
                    }).unwrap_or_else(|| "unknown worker error".to_string());
                    // §4.4: the worker carries the numeric HTTP status in
                    // Status.message (common._make_error_response). Parity
                    // with the unary queue path (inference.rs:425-465): a
                    // numeric status → ModelError (4xx passes through, 5xx
                    // maps 500 — B3, never sanitized to 503); a non-numeric
                    // message means the worker itself is broken →
                    // WorkerCrashed.
                    match msg.parse::<u16>() {
                        Ok(http_status) => {
                            let data: Value = if single.data.is_empty() {
                                json!({})
                            } else {
                                serde_json::from_slice(&single.data).unwrap_or(json!({}))
                            };
                            let err_obj = data.get("error");
                            Err(AppError::ModelError(Box::new(ModelErrorData {
                                status_code: http_status,
                                error_type: err_obj.and_then(|e| e.get("type"))
                                    .and_then(|t| t.as_str())
                                    .unwrap_or("model_error")
                                    .to_string(),
                                detail: err_obj.and_then(|e| e.get("message"))
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("model error")
                                    .to_string(),
                                code: err_obj.and_then(|e| e.get("code"))
                                    .and_then(|c| c.as_str())
                                    .map(|s| s.to_string()),
                                param: err_obj.and_then(|e| e.get("param"))
                                    .and_then(|p| p.as_str())
                                    .map(|s| s.to_string()),
                                headers: if single.headers.is_empty() {
                                    None
                                } else {
                                    Some(single.headers.clone())
                                },
                            })))
                        }
                        Err(_) => Err(AppError::WorkerCrashed(msg)),
                    }
                }
                _ => Err(AppError::WorkerCrashed(
                    single.status.as_ref().and_then(|s| {
                        if s.message.is_empty() { None } else { Some(s.message.clone()) }
                    }).unwrap_or_else(|| "ensemble step inference error".to_string())
                )),
            }
        }
        _ => Err(AppError::WorkerCrashed("unexpected response type".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_dag_ok() {
        let steps = vec![
            EnsembleStep {
                name: "step1".to_string(),
                model: "m1".to_string(),
                version: "1".to_string(),
                stream: false,
                inputs: [("input".to_string(), "$request".to_string())].into(),
            },
            EnsembleStep {
                name: "step2".to_string(),
                model: "m2".to_string(),
                version: "1".to_string(),
                stream: false,
                inputs: [("data".to_string(), "$step1".to_string())].into(),
            },
        ];
        assert!(validate_dag(&steps).is_ok());
    }

    #[test]
    fn test_validate_dag_cycle() {
        let steps = vec![
            EnsembleStep {
                name: "step1".to_string(),
                model: "m1".to_string(),
                version: "1".to_string(),
                stream: false,
                inputs: [("input".to_string(), "$step2".to_string())].into(),
            },
            EnsembleStep {
                name: "step2".to_string(),
                model: "m2".to_string(),
                version: "1".to_string(),
                stream: false,
                inputs: [("input".to_string(), "$step1".to_string())].into(),
            },
        ];
        assert!(validate_dag(&steps).is_err());
    }

    #[test]
    fn test_validate_dag_unknown_ref() {
        let steps = vec![
            EnsembleStep {
                name: "step1".to_string(),
                model: "m1".to_string(),
                version: "1".to_string(),
                stream: false,
                inputs: [("input".to_string(), "$unknown".to_string())].into(),
            },
        ];
        assert!(validate_dag(&steps).is_err());
    }

    #[test]
    fn test_topological_layers() {
        let steps = vec![
            EnsembleStep {
                name: "a".to_string(),
                model: "m1".to_string(),
                version: "1".to_string(),
                stream: false,
                inputs: [("x".to_string(), "$request".to_string())].into(),
            },
            EnsembleStep {
                name: "b".to_string(),
                model: "m2".to_string(),
                version: "1".to_string(),
                stream: false,
                inputs: [("x".to_string(), "$request".to_string())].into(),
            },
            EnsembleStep {
                name: "c".to_string(),
                model: "m3".to_string(),
                version: "1".to_string(),
                stream: false,
                inputs: [("x".to_string(), "$a".to_string())].into(),
            },
        ];
        let layers = topological_layers(&steps);
        assert_eq!(layers.len(), 2);
        assert_eq!(layers[0].len(), 2); // a and b (no deps)
        assert_eq!(layers[1].len(), 1); // c (depends on a)
    }

    #[test]
    fn test_resolve_ref() {
        let mut context = HashMap::new();
        context.insert("request".to_string(), EnsembleValue::Json(json!({"image": "cat.jpg"})));
        context.insert("step1".to_string(), EnsembleValue::Json(json!({"output": 42})));

        assert_eq!(
            match resolve_ref("$request", &context).unwrap() {
                EnsembleValue::Json(v) => v,
                _ => panic!("expected Json"),
            },
            json!({"image": "cat.jpg"})
        );
        assert_eq!(
            match resolve_ref("$request.image", &context).unwrap() {
                EnsembleValue::Json(v) => v,
                _ => panic!("expected Json"),
            },
            json!("cat.jpg")
        );
        assert_eq!(
            match resolve_ref("$step1.output", &context).unwrap() {
                EnsembleValue::Json(v) => v,
                _ => panic!("expected Json"),
            },
            json!(42)
        );
    }

    // === B3: resolve_ref Binary rules (E7) ===

    #[test]
    fn b3_resolve_ref_request_whole_binary_passthrough() {
        let mut context = HashMap::new();
        context.insert(
            "request".to_string(),
            EnsembleValue::Binary(Bytes::from_static(b"hello"), "text/plain".to_string()),
        );
        let result = resolve_ref("$request", &context).unwrap();
        match result {
            EnsembleValue::Binary(data, ct) => {
                assert_eq!(data.as_ref(), b"hello");
                assert_eq!(ct, "text/plain");
            }
            _ => panic!("expected Binary passthrough"),
        }
    }

    #[test]
    fn b3_resolve_ref_request_field_on_binary_is_400() {
        let mut context = HashMap::new();
        context.insert(
            "request".to_string(),
            EnsembleValue::Binary(Bytes::from_static(b"hello"), "text/plain".to_string()),
        );
        let err = resolve_ref("$request.field", &context).unwrap_err();
        assert!(
            matches!(err, AppError::InvalidRequestBody(_)),
            "field access on binary must be 400, got {err:?}"
        );
        assert!(
            err.to_string().contains("field"),
            "error must mention field extraction, got: {err}"
        );
    }

    #[test]
    fn b3_resolve_ref_step_binary_is_400() {
        let mut context = HashMap::new();
        context.insert(
            "step1".to_string(),
            EnsembleValue::Binary(Bytes::from_static(b"hello"), "text/plain".to_string()),
        );
        // Whole step reference on binary → 400 (Option A boundary).
        let err = resolve_ref("$step1", &context).unwrap_err();
        assert!(
            matches!(err, AppError::InvalidRequestBody(_)),
            "step binary reference must be 400, got {err:?}"
        );
        // Field access on step binary → same 400.
        let err = resolve_ref("$step1.field", &context).unwrap_err();
        assert!(
            matches!(err, AppError::InvalidRequestBody(_)),
            "step binary field access must be 400, got {err:?}"
        );
    }

    // ===== P-FLOW (§4.0.9): ensemble shared cancel =====

    #[tokio::test]
    async fn p_flow_ensemble_joinset_aborts_inflight_on_drop() {
        // execute_ensemble uses a per-layer JoinSet: dropping it (parent
        // disconnect, total-budget timeout, or a sibling step error) must
        // ABORT in-flight sub-step tasks so workers are not left computing
        // for a cancelled ensemble. This guards the invariant the executor
        // relies on.
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        let ran = Arc::new(AtomicBool::new(false));
        let ran_clone = ran.clone();
        let mut set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        set.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            ran_clone.store(true, Ordering::SeqCst);
        });
        // Simulate the ensemble future being dropped mid-layer.
        drop(set);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !ran.load(Ordering::SeqCst),
            "dropped JoinSet must abort its in-flight task (ensemble cancel)"
        );
    }

    // === B3: input assembly three branches (E7) ===

    fn bin(data: &'static [u8], ct: &str) -> EnsembleValue {
        EnsembleValue::Binary(Bytes::from_static(data), ct.to_string())
    }

    #[test]
    fn b3_assemble_all_json_builds_object() {
        let mut resolved = HashMap::new();
        resolved.insert("a".to_string(), EnsembleValue::Json(json!(1)));
        resolved.insert("b".to_string(), EnsembleValue::Json(json!("x")));
        let (bytes, ct) = assemble_step_payload("s", &resolved).unwrap();
        assert!(ct.is_none(), "all-Json assembly must not set a content-type");
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v, json!({"a": 1, "b": "x"}));
    }

    #[test]
    fn b3_assemble_single_binary_passthrough_with_ct() {
        let mut resolved = HashMap::new();
        resolved.insert("img".to_string(), bin(b"\x00\x01\x02", "image/png"));
        let (bytes, ct) = assemble_step_payload("s", &resolved).unwrap();
        assert_eq!(bytes.as_ref(), b"\x00\x01\x02", "binary payload must pass verbatim");
        assert_eq!(ct.as_deref(), Some("image/png"), "CT must be forwarded");
    }

    #[test]
    fn b3_assemble_mixed_binary_json_is_400() {
        let mut resolved = HashMap::new();
        resolved.insert("a".to_string(), bin(b"x", "application/octet-stream"));
        resolved.insert("b".to_string(), EnsembleValue::Json(json!(1)));
        let err = assemble_step_payload("s", &resolved).unwrap_err();
        assert!(
            matches!(err, AppError::InvalidRequestBody(_)),
            "mixed JSON/Binary inputs must be 400, got {err:?}"
        );
    }

    #[test]
    fn b3_assemble_two_binary_inputs_is_400() {
        // Even two whole-Binary inputs violate the "sole whole input" rule.
        let mut resolved = HashMap::new();
        resolved.insert("a".to_string(), bin(b"x", "application/octet-stream"));
        resolved.insert("b".to_string(), bin(b"y", "application/octet-stream"));
        let err = assemble_step_payload("s", &resolved).unwrap_err();
        assert!(
            matches!(err, AppError::InvalidRequestBody(_)),
            "two binary inputs must be 400, got {err:?}"
        );
    }

    // === B3: step output typed parse (E8) ===

    fn single(data: &'static [u8], media_type: &str) -> pb::SingleResponse {
        pb::SingleResponse {
            data: Bytes::from_static(data),
            media_type: media_type.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn b3_output_parse_json_media_type() {
        let out = parse_step_output("s", single(br#"{"a":1}"#, "application/json")).unwrap();
        match out {
            EnsembleValue::Json(v) => assert_eq!(v, json!({"a": 1})),
            _ => panic!("expected Json"),
        }
    }

    #[test]
    fn b3_output_parse_empty_media_type_defaults_json() {
        let out = parse_step_output("s", single(br#"{"a":1}"#, "")).unwrap();
        match out {
            EnsembleValue::Json(v) => assert_eq!(v, json!({"a": 1})),
            _ => panic!("expected Json"),
        }
    }

    #[test]
    fn b3_output_parse_invalid_json_not_swallowed() {
        // Regression pin for the old `:483` behaviour: invalid JSON from a
        // worker must surface as an error naming the step, never collapse
        // into a silent `{}` that the DAG keeps running on.
        let err = parse_step_output("mystep", single(b"{oops", "")).unwrap_err();
        assert!(
            matches!(err, AppError::Internal(_)),
            "invalid JSON must be an Internal error, got {err:?}"
        );
        assert!(
            err.to_string().contains("mystep"),
            "error must name the failing step, got: {err}"
        );
    }

    #[test]
    fn b3_output_parse_binary_media_type() {
        let out = parse_step_output("s", single(b"\x00\xff", "application/octet-stream")).unwrap();
        match out {
            EnsembleValue::Binary(d, ct) => {
                assert_eq!(d.as_ref(), b"\x00\xff");
                assert_eq!(ct, "application/octet-stream");
            }
            _ => panic!("expected Binary"),
        }
    }

    #[test]
    fn b3_output_parse_empty_data_is_empty_object() {
        let out = parse_step_output("s", single(b"", "")).unwrap();
        match out {
            EnsembleValue::Json(v) => assert_eq!(v, json!({})),
            _ => panic!("expected Json empty object"),
        }
    }

    // === §4.0/D16: streaming validation (form dispatch, batch 0) ===

    fn sstep(name: &str, inputs: &[(&str, &str)], stream: bool) -> EnsembleStep {
        EnsembleStep {
            name: name.to_string(),
            model: format!("m_{}", name),
            version: "1".to_string(),
            inputs: inputs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            stream,
        }
    }

    #[test]
    fn stream_rules_tail_stream_dag_valid() {
        // s1 (unary) → s2 (streaming tail): the only open form in batch 0.
        let steps = vec![
            sstep("s1", &[("input", "$request")], false),
            sstep("s2", &[("data", "$s1")], true),
        ];
        assert!(validate_stream_rules(&steps).is_ok(), "tail-streaming DAG must validate");
    }

    #[test]
    fn stream_rules_two_stream_steps_rejected() {
        // Rule 3: at most one streaming step per DAG.
        let steps = vec![
            sstep("s1", &[("input", "$request")], true),
            sstep("s2", &[("data", "$request")], true),
        ];
        let err = validate_stream_rules(&steps).unwrap_err();
        assert!(
            err.to_string().contains("one streaming step"),
            "must reject two streaming steps, got: {err}"
        );
    }

    #[test]
    fn stream_rules_tail_stream_not_config_last_rejected() {
        // Rule 4 (B-m4): with `output` omitted the DAG output is steps.last(),
        // which must be the streaming step — a streaming step that is not the
        // config last step would silently produce nothing streamable.
        let steps = vec![
            sstep("s1", &[("input", "$request")], true),
            sstep("s2", &[("data", "$request")], false),
        ];
        let err = validate_stream_rules(&steps).unwrap_err();
        assert!(
            err.to_string().contains("output") && err.to_string().contains("s1"),
            "must reject streaming step not at config tail, got: {err}"
        );
    }

    #[test]
    fn stream_rules_pipeline_form_rejected_with_batch2_message() {
        // D16: a streaming step whose output is consumed downstream IS the
        // pipeline form — batch 0 must reject it with an explicit batch-2
        // message, not a misleading rule-1/2 rejection (and not accept it).
        let steps = vec![
            sstep("s1", &[("input", "$request")], true),
            sstep("s2", &[("data", "$s1")], false),
        ];
        let err = validate_stream_rules(&steps).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.to_lowercase().contains("pipeline") && msg.contains("0.9.0"),
            "pipeline form must be rejected with an explicit batch-2 message, got: {msg}"
        );
    }

    #[test]
    fn stream_rules_plain_dag_unchanged() {
        // No stream: false — behaviour parity with the historical validator.
        let steps = vec![
            sstep("s1", &[("input", "$request")], false),
            sstep("s2", &[("data", "$s1")], false),
        ];
        assert!(validate_stream_rules(&steps).is_ok());
    }

    // === P10: streaming-DAG capacity (D40) ===

    #[test]
    fn p10_capacity_rejects_when_exhausted_and_releases_on_drop() {
        let cap = StreamingCapacityState::new(2);
        let p1 = cap.try_acquire().unwrap();
        let p2 = cap.try_acquire().unwrap();
        let err = cap.try_acquire().err().unwrap();
        assert!(
            matches!(err, AppError::StreamingCapacityExceeded(_)),
            "exhausted capacity must reject immediately (429), got {err:?}"
        );
        // Dropping a permit returns its slot — no queueing, no leak.
        drop(p1);
        let _p3 = cap.try_acquire().expect("slot must be released on permit drop");
        // Clones share the same permit: slot stays held until ALL clones drop.
        let p2_clone = p2.clone();
        drop(p2);
        let err = cap.try_acquire().err().unwrap();
        assert!(
            matches!(err, AppError::StreamingCapacityExceeded(_)),
            "cloned permit must hold the slot until the last reference drops"
        );
        drop(p2_clone);
        let _p4 = cap.try_acquire().expect("last clone drop must release the slot");
    }

    #[test]
    fn p10_capacity_zero_permits_is_immediately_exhausted() {
        // A 0-limit installation rejects everything (misconfiguration guard);
        // production never installs one for 0 (server/mod.rs skips it).
        let cap = StreamingCapacityState::new(0);
        let err = cap.try_acquire().err().unwrap();
        assert!(matches!(err, AppError::StreamingCapacityExceeded(_)));
    }

    // === P0: EnsemblePlan cache (D6 + review ①-④) ===

    fn test_plan(path: &str) -> Arc<EnsemblePlan> {
        Arc::new(EnsemblePlan {
            steps: Vec::new(),
            layers: Vec::new(),
            output_step: 0,
            config_path: PathBuf::from(path),
        })
    }

    #[tokio::test]
    async fn p0_cache_hit_returns_same_arc() {
        let cache = EnsemblePlanCache::new();
        let key = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let plan = test_plan("/nonexistent");
        let first = cache.get_or_load(key.clone(), || {
            let plan = plan.clone();
            async move { Ok::<_, AppError>(plan) }
        }).await.unwrap();
        assert!(Arc::ptr_eq(&plan, &first), "first load returns the parsed plan");
        let hit = cache.get_or_load(key, || async { panic!("cache hit must not parse again") })
            .await.unwrap();
        assert!(Arc::ptr_eq(&plan, &hit), "hit must return the cached Arc");
    }

    #[tokio::test]
    async fn p0_cache_single_flight_only_one_parse() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cache = Arc::new(EnsemblePlanCache::new());
        let key = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let parse_count = Arc::new(AtomicUsize::new(0));
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..10 {
            let cache = cache.clone();
            let key = key.clone();
            let parse_count = parse_count.clone();
            set.spawn(async move {
                cache.get_or_load(key, || {
                    let parse_count = parse_count.clone();
                    async move {
                        parse_count.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok::<_, AppError>(test_plan("/nonexistent"))
                    }
                }).await.unwrap()
            });
        }
        let mut plans = Vec::new();
        while let Some(r) = set.join_next().await {
            plans.push(r.unwrap());
        }
        assert_eq!(
            parse_count.load(Ordering::SeqCst),
            1,
            "concurrent first requests must parse exactly once (single-flight, review ④)"
        );
        for p in &plans {
            assert!(Arc::ptr_eq(&plans[0], p), "all waiters must receive the holder's plan");
        }
    }

    #[tokio::test]
    async fn p0_cache_failed_load_not_cached() {
        let cache = EnsemblePlanCache::new();
        let key = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let err = cache.get_or_load(key.clone(), || async {
            Err::<Arc<EnsemblePlan>, _>(AppError::Config("boom".to_string()))
        }).await.unwrap_err();
        assert!(err.to_string().contains("boom"));
        // A failed load must not be cached: the next call re-parses (a fixed
        // config heals without reload — behaviour parity with no cache).
        let ok = cache.get_or_load(key, || async {
            Ok::<_, AppError>(test_plan("/nonexistent"))
        }).await;
        assert!(ok.is_ok(), "failed load must not be cached; next call re-parses");
    }

    #[tokio::test]
    async fn p0_cache_invalidate_model_clears_all_versions() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cache = EnsemblePlanCache::new();
        let count = Arc::new(AtomicUsize::new(0));
        let k_latest = PlanKey { model: "m".to_string(), version: "latest".to_string() };
        let k_pinned = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let k_other = PlanKey { model: "other".to_string(), version: "1".to_string() };
        for key in [&k_latest, &k_pinned, &k_other] {
            cache.get_or_load(key.clone(), || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, AppError>(test_plan("/nonexistent"))
                }
            }).await.unwrap();
        }
        cache.invalidate_model("m");
        // Review ③: both "latest" and the pinned version are separate keys
        // and must ALL clear on a model-prefix invalidation.
        for key in [&k_latest, &k_pinned] {
            cache.get_or_load(key.clone(), || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, AppError>(test_plan("/nonexistent"))
                }
            }).await.unwrap();
        }
        assert_eq!(count.load(Ordering::SeqCst), 5, "invalidate_model must clear every version of the model");
    }

    #[tokio::test]
    async fn p0_cache_invalidate_version_clears_one() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cache = EnsemblePlanCache::new();
        let count = Arc::new(AtomicUsize::new(0));
        let k1 = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let k2 = PlanKey { model: "m".to_string(), version: "2".to_string() };
        for key in [&k1, &k2] {
            cache.get_or_load(key.clone(), || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, AppError>(test_plan("/nonexistent"))
                }
            }).await.unwrap();
        }
        cache.invalidate_version("m", "1");
        cache.get_or_load(k1, || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, AppError>(test_plan("/nonexistent"))
            }
        }).await.unwrap();
        // v2 untouched → no new parse.
        cache.get_or_load(k2, || async { panic!("v2 must stay cached") }).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 3, "only version 1 must re-parse");
    }

    #[tokio::test(start_paused = true)]
    async fn p0_cache_mtime_recheck_after_interval() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = std::env::temp_dir().join(format!("liteserver-ens-p0-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("config.yaml");
        std::fs::write(&config_path, b"v1").unwrap();
        let cache = EnsemblePlanCache::new();
        let key = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let count = Arc::new(AtomicUsize::new(0));
        let load = |count: Arc<AtomicUsize>| {
            let config_path = config_path.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, AppError>(Arc::new(EnsemblePlan {
                    steps: Vec::new(),
                    layers: Vec::new(),
                    output_step: 0,
                    config_path,
                }))
            }
        };
        cache.get_or_load(key.clone(), || load(count.clone())).await.unwrap();
        // Within the stat interval: no syscall, same Arc (review ②).
        let within = cache.get_or_load(key.clone(), || load(count.clone())).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1, "hot path must not stat within the interval");
        // Rewrite the file; still within interval → still served from cache.
        std::fs::write(&config_path, b"v2").unwrap();
        let stale = cache.get_or_load(key.clone(), || load(count.clone())).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1, "file change inside the interval is not seen");
        drop(within);
        drop(stale);
        // Advance past the interval: next get must stat, see the mtime change,
        // evict and re-parse (single-flight).
        tokio::time::advance(Duration::from_millis(1500)).await;
        cache.get_or_load(key.clone(), || load(count.clone())).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2, "mtime change after the interval must re-parse");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
