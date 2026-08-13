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
#[derive(Debug, Clone, PartialEq)]
pub enum EnsembleValue {
    Json(serde_json::Value),
    Binary(Bytes, String /* content_type */),
}

// ===== Config parsing =====

// D24: the ensemble schema denies unknown fields — keys from a newer release
// (params/when/outputs/dags/inputs/step.outputs, 0.9.0) or plain typos must
// fail fast at load, never be silently ignored (a swallowed `stream:` typo
// would silently disable streaming). NOTE: only the ensemble section denies —
// the top-level file shares space with model-config keys (max_batch_size …),
// so EnsembleConfig itself must stay open.
#[derive(Debug, Clone, Deserialize)]
pub struct EnsembleConfig {
    pub ensemble: EnsembleBlock,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnsembleBlock {
    pub steps: Vec<EnsembleStepRaw>,
    /// E2 (batch 3): explicit DAG output — `$stepN` or `$stepN.field`.
    /// Omitted = `steps.last()` (historical semantics).
    #[serde(default)]
    pub output: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnsembleStepRaw {
    pub name: String,
    pub model: String,
    /// E4 (batch 3): omitted or "latest" resolves at EXECUTION time via the
    /// D15 request-scoped snapshot (registry active); explicit versions keep
    /// the historical behavior. Stored unresolved in the plan — the P0 cache
    /// never invalidates on active-version drift.
    #[serde(default)]
    pub version: Option<String>,
    pub inputs: HashMap<String, String>,
    /// §4.1: tail streaming. The streaming step must be the DAG output
    /// (config `steps.last()`, or the explicit `output` — E2, batch 3).
    #[serde(default)]
    pub stream: bool,
    /// E3 (batch 3): constant step parameters merged into the assembled JSON
    /// payload (params win on key conflicts). Binary assembly has no params
    /// semantics — a non-empty params on a Binary step is rejected at
    /// assembly time.
    #[serde(default)]
    pub params: HashMap<String, Value>,
    /// E5 (batch 3): per-step wall-clock cap (seconds); None = parent
    /// deadline only.
    #[serde(default)]
    pub timeout_secs: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct EnsembleStep {
    pub name: String,
    pub model: String,
    /// E4: unresolved form — None = omitted/"latest" (execution-time
    /// resolution via the D15 snapshot).
    pub version: Option<String>,
    pub inputs: HashMap<String, String>,
    pub stream: bool,
    pub params: HashMap<String, Value>,
    pub timeout_secs: Option<f64>,
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
pub fn parse_ensemble_plan(content: &str, config_path: &std::path::Path) -> Result<EnsemblePlan, AppError> {
    let config: EnsembleConfig = serde_yaml::from_str(content)
        .map_err(|e| AppError::Config(format!("failed to parse ensemble config: {}", e)))?;

    let steps: Vec<EnsembleStep> = config.ensemble.steps.into_iter().map(|s| {
        // E5: per-step timeout must be a positive, finite duration.
        if let Some(t) = s.timeout_secs {
            if !t.is_finite() || t <= 0.0 {
                return Err(AppError::Config(format!(
                    "step '{}': timeout_secs must be a positive finite number, got {}",
                    s.name, t
                )));
            }
        }
        Ok(EnsembleStep {
            name: s.name,
            model: s.model,
            // E4: "latest" == omitted (execution-time active resolution).
            version: s.version.filter(|v| v != "latest"),
            inputs: s.inputs,
            stream: s.stream,
            params: s.params,
            timeout_secs: s.timeout_secs,
        })
    }).collect::<Result<_, AppError>>()?;

    validate_dag(&steps)?;
    // E2: resolve the explicit output BEFORE chain construction / streaming
    // validation — both anchor on the output step.
    let (output_step, output_field) = resolve_output(config.ensemble.output.as_deref(), &steps)?;
    // Pipeline-form validation + chain construction (P-R1..R5/D26, batch 2);
    // non-pipeline streaming rules apply only when no chain exists.
    let chains = build_chains(&steps, output_step)?;
    if chains.is_empty() {
        validate_stream_rules(&steps, output_step)?;
    }
    // E2 × D11: a streaming DAG's chunks have no field semantics — an
    // explicit output field on a streaming DAG is a parse-time rejection.
    if output_field.is_some() && steps.iter().any(|s| s.stream) {
        return Err(AppError::Config(
            "ensemble.output field projection is not supported on streaming DAGs \
             (chunks have no field semantics, D11)"
                .to_string(),
        ));
    }
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
        output_step,
        output_field,
        steps,
        layers,
        chains,
        config_path: config_path.to_path_buf(),
        // Set by the production loader (stat-before-read); None for the
        // load-time direct-parse path (insert_ready stats on its own).
        source_mtime: None,
    })
}

/// E2 (batch 3): resolve `ensemble.output` — `$stepN` or `$stepN.field` —
/// into the output step index + optional field path. Omitted = `steps.last()`
/// (historical semantics).
fn resolve_output(
    output: Option<&str>,
    steps: &[EnsembleStep],
) -> Result<(usize, Option<String>), AppError> {
    let Some(output) = output else {
        return Ok((steps.len() - 1, None));
    };
    let caps = REF_RE.captures(output).ok_or_else(|| {
        AppError::Config(format!(
            "invalid ensemble.output '{}': expected $stepN or $stepN.field",
            output
        ))
    })?;
    let source = caps.get(1).unwrap().as_str();
    if source == "request" {
        return Err(AppError::Config(
            "ensemble.output must reference a step, not $request".to_string(),
        ));
    }
    let idx = steps
        .iter()
        .position(|s| s.name == source)
        .ok_or_else(|| {
            AppError::Config(format!(
                "ensemble.output references unknown step '{}'",
                source
            ))
        })?;
    Ok((idx, caps.get(2).map(|m| m.as_str().to_string())))
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
    // Stat BEFORE the read: a write landing between stat and read leaves the
    // stored mtime OLDER than the file's, so the interval re-check re-parses
    // (safe). The reverse order could pin a fresh mtime onto stale content
    // and serve it indefinitely (the watcher-miss fallback's whole point).
    let source_mtime = std::fs::metadata(&config_path)
        .and_then(|m| m.modified())
        .ok();
    let content = tokio::fs::read_to_string(&config_path)
        .await
        .map_err(|e| AppError::Config(format!("failed to read ensemble config: {}", e)))?;
    let mut plan = parse_ensemble_plan(&content, &config_path)?;
    plan.source_mtime = source_mtime;
    Ok(Arc::new(plan))
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
        // E4 (batch 3): pre-resolve every sub-model step and preheat the
        // routing/LRU caches. D27: this warm pass is a TTFT hint ONLY — it is
        // NEVER a resolution source; the request-time D15 snapshot always
        // re-resolves against the registry, so an active drift between warm
        // and first request has no freeze window.
        let warm_snapshot = Arc::new(VersionSnapshot::default());
        for step in &plan.steps {
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

fn validate_dag(steps: &[EnsembleStep]) -> Result<(), AppError> {
    // An empty DAG has no output step — reject at parse instead of panicking
    // on `steps.len() - 1` (pre-batch-0 this degenerated into a request-time
    // 500; with the plan cache it must be a load-time config error).
    if steps.is_empty() {
        return Err(AppError::Config("ensemble declares no steps".to_string()));
    }
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
                // Single-level field refs only ($step.field). Name the
                // boundary for nested paths ($step.a.b) — they open with
                // MIMO multi-level mapping (R17), so a config author hitting
                // the limit sees the future path instead of a bare error.
                if ref_str.starts_with('$') && ref_str.contains('.') {
                    AppError::Config(format!(
                        "invalid reference '{}': nested field paths (a.b.c) are not \
                         supported yet — multi-level paths open with MIMO (R17)",
                        ref_str
                    ))
                } else {
                    AppError::Config(format!("invalid reference format: {}", ref_str))
                }
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
/// 1. streaming step must be in the last topological layer — checked
///    explicitly below: "output not referenced" does NOT imply it (Kahn
///    layering puts a no-dependency sink in layer 0 even when later layers
///    exist), and accepting such a DAG would truncate execution at the
///    tail's layer, silently never running the steps in later layers.
/// 3. at most one streaming step per DAG.
/// 4. output-step semantics unified (E2 base, batch 0): with `output`
///    omitted the DAG output is `steps.last()`, which MUST be the streaming
///    step — otherwise a streaming DAG would silently produce nothing
///    streamable (B-m4: explicit `output` lands with E2 in batch 3).
///
/// A linear pipeline chain (§4.2): consecutive STREAMING steps where each
/// step's output is consumed whole by the next (P-R1/P-R2), ending at the
/// DAG output step (P-R3/D26). Steps are indices into `EnsemblePlan.steps`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chain {
    pub nodes: Vec<usize>,
}

/// §4.0/D16 pipeline-form validation + chain construction (batch 2 opens the
/// form the batch-0 validator rejected). Rules P-R1..P-R5 + D26:
///   P-R1  a streaming output has exactly ONE consumer (0 → non-pipeline;
///         ≥2 → error)
///   P-R2  streaming outputs are referenced WHOLE only ($s0; $s0.field → error)
///   P-R3  the chain tail must be the DAG output step (and streaming — the
///         unary-tail shape is rejected here, D26's "pipeline chain tail
///         must be a streaming step")
///   P-R4  chains never share steps (Kahn covers cycles; the linear walk
///         below marks every node visited — a shared step fails)
///   P-R5  the DAG's streaming set is exactly one chain OR one tail streaming
///         step — a chain plus an orphan streaming step is an error
///   D26   a chain's unary consumer has no clean chunk→unary→chunk semantics
///         → the whole form is rejected at parse time
fn build_chains(steps: &[EnsembleStep], output_step: usize) -> Result<Vec<Chain>, AppError> {
    let is_stream: Vec<bool> = steps.iter().map(|s| s.stream).collect();
    let stream_count = is_stream.iter().filter(|b| **b).count();
    if stream_count == 0 {
        return Ok(Vec::new());
    }

    // Consumer edges: streaming step index → (consumer index, field-ref?).
    let mut consumers: HashMap<usize, Vec<(usize, bool)>> = HashMap::new();
    for (i, step) in steps.iter().enumerate() {
        for ref_str in step.inputs.values() {
            let Some(caps) = REF_RE.captures(ref_str) else { continue };
            let source = caps.get(1).unwrap().as_str();
            let Some(src_idx) = steps.iter().position(|s| s.name == source) else { continue };
            if is_stream[src_idx] {
                consumers
                    .entry(src_idx)
                    .or_default()
                    .push((i, caps.get(2).is_some()));
            }
        }
    }

    let mut chains: Vec<Chain> = Vec::new();
    let mut visited: HashSet<usize> = HashSet::new();
    for s in 0..steps.len() {
        if !is_stream[s] || visited.contains(&s) {
            continue;
        }
        match consumers.get(&s).map(|v| v.as_slice()) {
            None => {
                // Zero consumers = non-pipeline (tail-streaming) form. An
                // orphan that is NOT the output step breaks the output
                // semantics (§4.1-4 / P-R5).
                if s != output_step {
                    return Err(AppError::Config(format!(
                        "streaming step '{}' has no consumer and is not the DAG \
                         output step — the DAG mixes a pipeline chain with an \
                         orphan streaming step (P-R5)",
                        steps[s].name
                    )));
                }
            }
            Some([(c, field_ref)]) => {
                // Chain start (P-R1: exactly one consumer).
                if *field_ref {
                    return Err(AppError::Config(format!(
                        "streaming step '{}' output must be referenced whole \
                         ($step), not field-projected (P-R2)",
                        steps[s].name
                    )));
                }
                // Walk the consumer chain. Every node must be streaming —
                // a unary consumer is the D26-rejected chunk→unary→chunk form
                // (P-R3's "pipeline chain tail must be a streaming step").
                let mut nodes = vec![s];
                let mut cur = s;
                let mut next_consumer = *c;
                loop {
                    let c = next_consumer;
                    if !is_stream[c] {
                        return Err(AppError::Config(format!(
                            "pipeline chain step '{}' consumes streaming step \
                             '{}' but is not streaming — the pipeline chain \
                             tail must be a streaming step; unary consumers \
                             on a chain are not defined (D26)",
                            steps[c].name, steps[cur].name
                        )));
                    }
                    if visited.contains(&c) {
                        return Err(AppError::Config(format!(
                            "pipeline chains share step '{}' (P-R4)",
                            steps[c].name
                        )));
                    }
                    nodes.push(c);
                    visited.insert(c);
                    match consumers.get(&c).map(|v| v.as_slice()) {
                        None => break, // chain tail
                        Some([(c2, field_ref2)]) => {
                            if *field_ref2 {
                                return Err(AppError::Config(format!(
                                    "streaming step '{}' output must be referenced \
                                     whole ($step), not field-projected (P-R2)",
                                    steps[c].name
                                )));
                            }
                            cur = c;
                            next_consumer = *c2;
                        }
                        Some(_) => {
                            return Err(AppError::Config(format!(
                                "streaming step '{}' output has multiple \
                                 consumers (P-R1)",
                                steps[c].name
                            )));
                        }
                    }
                }
                let tail = *nodes.last().unwrap();
                if tail != output_step {
                    return Err(AppError::Config(format!(
                        "pipeline chain tail '{}' must be the DAG output step \
                         (config last step, or the explicit `output:`) (P-R3)",
                        steps[tail].name
                    )));
                }
                chains.push(Chain { nodes });
            }
            Some(_) => {
                return Err(AppError::Config(format!(
                    "streaming step '{}' output has multiple consumers (P-R1)",
                    steps[s].name
                )));
            }
        }
    }
    Ok(chains)
}

fn validate_stream_rules(steps: &[EnsembleStep], output_step: usize) -> Result<(), AppError> {
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

    // Rule 1: the streaming step must be in the LAST topological layer.
    // "Output not referenced" only proves it has no dependents — Kahn
    // layering puts a no-dependency sink in layer 0 even when later layers
    // exist, so without this check a streaming step in an early layer would
    // truncate execution (run_layers stops at the tail's layer) and steps in
    // later layers would silently never run. Load-time cost only (parse path).
    let layers = topological_layers(steps);
    let in_last_layer = layers
        .last()
        .map(|l| l.iter().any(|s| s.name == tail.name))
        .unwrap_or(false);
    if !in_last_layer {
        return Err(AppError::Config(format!(
            "streaming step '{}' must be in the last topological layer (rule 1); \
             steps in later layers would silently never execute",
            tail.name
        )));
    }

    // Rule 4 (B-m4, E2 batch 3): the DAG output step — config last step or
    // the explicit `output:` — must be the streaming step.
    if steps[output_step].name != tail.name {
        return Err(AppError::Config(format!(
            "streaming step '{}' must be the DAG output step (config last \
             step, or the explicit `output:`), or the DAG output is ambiguous",
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

/// E2 (batch 3): apply the explicit `output: "$stepN.field"` projection to
/// the DAG output value. None = whole value; Some = field extraction on a
/// Json output (Binary has no field semantics — D7's rule on the output
/// face). A missing field means the DAG's contract does not match the
/// model's output shape.
fn select_output_field(
    step_name: &str,
    value: EnsembleValue,
    field: Option<&str>,
) -> Result<EnsembleValue, AppError> {
    let Some(field) = field else {
        return Ok(value);
    };
    match value {
        EnsembleValue::Json(v) => {
            let field_val = v.get(field).cloned().ok_or_else(|| {
                AppError::Config(format!(
                    "ensemble.output field '{}' not found in step '{}' output",
                    field, step_name
                ))
            })?;
            Ok(EnsembleValue::Json(field_val))
        }
        EnsembleValue::Binary(_, _) => Err(AppError::InvalidRequestBody(format!(
            "ensemble.output field '{}' cannot be extracted from binary step \
             output '{}' (no field semantics on bytes)",
            field, step_name
        ))),
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanKey {
    pub model: String,
    pub version: String,
}

enum PlanCell {
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

/// P0: `DashMap<(model, version), Arc<EnsemblePlan>>` with single-flight
/// loads (review ④), mtime interval re-check (review ②, coarse tokio clock,
/// no syscall on the hot path) and model-prefix invalidation (review ③ —
/// `(m,"latest")` and `(m,"1")` are distinct keys, both must clear).
/// Invalidation hooks: lifecycle `unload_version` (via reload_model too) —
/// single collection point, fired BEFORE registry changes (D23).
pub struct EnsemblePlanCache {
    plans: DashMap<PlanKey, PlanCell>,
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
                    match load().await {
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

// ===== §4.3/D17: bidi upstream aggregation =====

/// Bidi upstream aggregation for ensemble DAGs (D17): all-Binary frames →
/// byte concat (content-type from the first frame); all-Json frames → JSON
/// array (a single frame = the value itself, historical single-frame
/// semantics); mixed kinds → 400 (no clean boundary/Content-Type semantics).
/// The cumulative byte cap is `max_request_body_bytes` (D3) — exceeded →
/// PayloadTooLarge (413/ResourceExhausted).
pub struct BidiAggregator {
    max_bytes: usize,
    total_bytes: usize,
    state: BidiAggState,
}

enum BidiAggState {
    Empty,
    Binary {
        parts: Vec<bytes::Bytes>,
        content_type: String,
    },
    Json {
        items: Vec<serde_json::Value>,
    },
}

impl BidiAggregator {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            total_bytes: 0,
            state: BidiAggState::Empty,
        }
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Push one data frame. `is_json` is the frame's declared kind (WS frame
    /// type / content-type header). The first frame fixes the kind; any
    /// later frame of the other kind → 400. Cap enforcement (D3) happens
    /// here so the aggregation can never exceed `max_request_body_bytes`.
    pub fn push(
        &mut self,
        data: bytes::Bytes,
        is_json: bool,
        content_type: Option<&str>,
    ) -> Result<(), AppError> {
        self.total_bytes += data.len();
        if self.total_bytes > self.max_bytes {
            return Err(AppError::PayloadTooLarge {
                max_size: self.max_bytes,
                actual_size: Some(self.total_bytes as u64),
            });
        }
        match &mut self.state {
            BidiAggState::Empty => {
                if is_json {
                    let v: Value = serde_json::from_slice(&data).map_err(|e| {
                        AppError::InvalidRequestBody(format!(
                            "bidi JSON frame is not valid JSON: {e}"
                        ))
                    })?;
                    self.state = BidiAggState::Json { items: vec![v] };
                } else {
                    self.state = BidiAggState::Binary {
                        parts: vec![data],
                        content_type: content_type
                            .unwrap_or("application/octet-stream")
                            .to_string(),
                    };
                }
            }
            BidiAggState::Json { items } => {
                if !is_json {
                    return Err(AppError::InvalidRequestBody(
                        "bidi stream mixes JSON and binary frames; \
                         aggregation requires a single kind (D17)"
                            .to_string(),
                    ));
                }
                let v: Value = serde_json::from_slice(&data).map_err(|e| {
                    AppError::InvalidRequestBody(format!("bidi JSON frame is not valid JSON: {e}"))
                })?;
                items.push(v);
            }
            BidiAggState::Binary { parts, .. } => {
                if is_json {
                    return Err(AppError::InvalidRequestBody(
                        "bidi stream mixes JSON and binary frames; \
                         aggregation requires a single kind (D17)"
                            .to_string(),
                    ));
                }
                parts.push(data);
            }
        }
        Ok(())
    }

    /// Trigger time (D33): produce the aggregated root input. Single Json
    /// frame → the value itself (not wrapped); multiple → `[f0, f1, ...]`;
    /// Binary → concatenated bytes with the first frame's content-type.
    pub fn finish(self) -> Result<EnsembleValue, AppError> {
        match self.state {
            BidiAggState::Empty => Ok(EnsembleValue::Json(json!({}))),
            BidiAggState::Json { items } => {
                if items.len() == 1 {
                    Ok(EnsembleValue::Json(items.into_iter().next().unwrap()))
                } else {
                    Ok(EnsembleValue::Json(Value::Array(items)))
                }
            }
            BidiAggState::Binary { parts, content_type } => {
                let total: usize = parts.iter().map(|p| p.len()).sum();
                let mut buf = Vec::with_capacity(total);
                for p in parts {
                    buf.extend_from_slice(&p);
                }
                Ok(EnsembleValue::Binary(bytes::Bytes::from(buf), content_type))
            }
        }
    }
}

// ===== Execution =====

/// Execution-face parameters (D37, fixed once at batch 0 — later batches add
/// fields without touching the signature, e.g. `dag_selector` in batch 5;
/// D36's snapshot/depth ride the internal `execute_ensemble_inner`, not the
/// public opts).
/// D15 (E4, batch 3): request-scoped version snapshot — `model → resolved
/// version`, lazily memoized. Every step of one request (across nesting,
/// D36) that references the same model with an unresolved version shares
/// the FIRST resolution, so an active-version drift mid-request can never
/// produce a DAG where step A hit v1 and step B hit v2. Explicit step
/// versions bypass the snapshot entirely.
///
/// E1 (batch 3): the NESTING CHAIN itself is NOT shared — each recursion
/// level extends its own immutable chain ([`extend_ancestor_chain`]) and
/// passes it down, so a concurrent sibling's in-flight child run can never
/// be misread as an ancestor of this branch (B1: the flat shared Vec
/// conflated sibling fan-out with recursion). Only the version table above
/// is request-shared (D36).
pub struct VersionSnapshot {
    resolved: std::sync::Mutex<HashMap<String, String>>,
}

impl Default for VersionSnapshot {
    fn default() -> Self {
        Self {
            resolved: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

impl VersionSnapshot {
    /// E4/D15: resolve a step's version — explicit versions as-is; the first
    /// unresolved resolution per model wins. Resolution source = registry
    /// active ONLY (D27: no canary pin / weighted routing at the step level;
    /// those precedences belong to the outer request's own version).
    pub fn resolve(
        &self,
        registry: &crate::registry::ModelRegistry,
        step: &EnsembleStep,
    ) -> Result<String, AppError> {
        if let Some(v) = step.version.as_deref() {
            return Ok(v.to_string());
        }
        let mut snapshot = self.resolved.lock().unwrap();
        if let Some(v) = snapshot.get(&step.model) {
            return Ok(v.clone());
        }
        let resolved = registry.get_active_version(&step.model).ok_or_else(|| {
            AppError::ModelNotFound(format!(
                "sub-model '{}' has no active version (step version omitted or \"latest\")",
                step.model
            ))
        })?;
        snapshot.insert(step.model.clone(), resolved.clone());
        Ok(resolved)
    }
}

/// E1: is (model, version) on THIS branch's nesting chain? Guards direct
/// self-loops — and, beyond the plan's m6 depth-only tradeoff, cross-model
/// mutual recursion (A→B→A) for free. The chain is immutable per branch:
/// a concurrent sibling's entries live on a DIFFERENT chain, so legal
/// same-layer fan-out to one child ensemble is never misread as recursion
/// (B1).
fn contains_ancestor(chain: &[(String, String)], model: &str, version: &str) -> bool {
    chain.iter().any(|(m, v)| m == model && v == version)
}

/// E1: extend this branch's nesting chain with the current ensemble for the
/// nested run. The parent chain is left untouched — sibling branches built
/// from the same parent stay independent (B1: per-branch chains replace the
/// flat request-global Vec; the version table alone remains shared, D36).
fn extend_ancestor_chain(
    chain: &[(String, String)],
    model: &str,
    version: &str,
) -> Vec<(String, String)> {
    let mut child = chain.to_vec();
    child.push((model.to_string(), version.to_string()));
    child
}

/// E1: runtime nesting depth limit (default 8, counted along the call tree
/// — cross-model nesting is invisible at parse time, G5). Depth 0 = the
/// top-level request.
const MAX_ENSEMBLE_NESTING_DEPTH: u32 = 8;

fn ensure_nesting_depth(depth: u32) -> Result<(), AppError> {
    if depth >= MAX_ENSEMBLE_NESTING_DEPTH {
        return Err(AppError::InvalidRequestBody(format!(
            "ensemble nesting depth limit ({MAX_ENSEMBLE_NESTING_DEPTH}) exceeded"
        )));
    }
    Ok(())
}

/// E5 (batch 3): a step's effective deadline — `min(parent deadline,
/// now + timeout_secs)`. None = no step timeout (parent passes through; a
/// parent of None stays None = unbounded). Both the unary submit/wait and
/// the streaming open (D35: recv_chunk overall takes min(client overall,
/// this cap)) use the result. The same formula applies per streaming step on
/// a pipeline chain (each hop computes its own from its open time).
fn step_effective_deadline(
    parent_deadline_unix_ns: Option<i64>,
    timeout_secs: Option<f64>,
) -> Option<i64> {
    let Some(timeout_secs) = timeout_secs else {
        return parent_deadline_unix_ns;
    };
    let now_unix_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    // Saturating: a parse-legal but absurd cap (e.g. 1e18 s) saturates the
    // f64→i64 cast, and a plain `now +` addition would then overflow — debug
    // builds panic, release wraps negative (every request on the step then
    // fails instantly as "expired"). Saturating keeps the config's intent
    // ("effectively no timeout") without either failure mode.
    let step_deadline = now_unix_ns.saturating_add((timeout_secs * 1e9) as i64);
    match parent_deadline_unix_ns {
        Some(parent) => Some(parent.min(step_deadline)),
        None => Some(step_deadline),
    }
}

/// E1: type-erased entry into the nested execution. `execute_step` awaits
/// this instead of `execute_ensemble_inner` directly — the `dyn Future`
/// erasure breaks the opaque-type cycle the direct call creates
/// (execute_step → execute_ensemble_inner → open_tail_stream → spawn
/// Send-check → execute_step).
#[allow(clippy::too_many_arguments)] // nested-execution plumbing rides together by design
fn execute_nested_ensemble_boxed(
    state: Arc<AppState>,
    model: String,
    version: String,
    payload: EnsembleValue,
    request_id: String,
    opts: EnsembleExecOpts,
    snapshot: Arc<VersionSnapshot>,
    depth: u32,
    ancestors: Vec<(String, String)>, // E1: this branch's chain (owned — moves into the boxed future)
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<EnsembleOutcome, AppError>> + Send>> {
    Box::pin(async move {
        execute_ensemble_inner(state, &model, &version, payload, &request_id, opts, &snapshot, depth, &ancestors)
            .await
    })
}

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
    /// Step-level metric label (§4.1): the adapters record
    /// `record_ensemble_step_latency(ensemble, tail_step, tail_model,
    /// tail_version, …)` at stream close.
    pub tail_step: String,
    /// D35 (batch 3): E5 `timeout_secs` converted to a step wall-clock cap —
    /// the adapter's recv_chunk overall takes min(client overall, this).
    /// None = inactive.
    pub step_deadline: Option<std::time::Instant>,
    /// D18/D25: chain handles — the single source of truth for the chain's
    /// streams (tail included). Batch 0 = the tail stream alone; batch 2 =
    /// every streaming step on the pipeline chain, pushed by the chain tasks
    /// as they open (tail first — chain[0] always mirrors the top-level
    /// fields, so adapters that only read the quick-access fields stay
    /// unchanged). Cancellation broadcasts over this list.
    pub chain: Arc<std::sync::Mutex<Vec<StreamHandle>>>,
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
        self.try_acquire_many(1)
    }

    /// Acquire `n` slots — pipeline chains weight by their streaming-step
    /// count (D40: 末步 = 1, k 链 = k; the residency derivation M = N × D_stream
    /// scales linearly with the chain length).
    pub fn try_acquire_many(&self, n: usize) -> Result<StreamingPermit, AppError> {
        match self.semaphore.clone().try_acquire_many_owned(n as u32) {
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
    // D15 (E4): request-scoped version snapshot — shared across every step
    // of this request and (D36) across nested ensemble calls.
    let snapshot = Arc::new(VersionSnapshot::default());
    execute_ensemble_inner(state, model_name, version, payload, request_id, opts, &snapshot, 0, &[])
        .await
}

/// D36 (E1/E4): the internal execution entry — the D15 version snapshot and
/// the nesting depth ride here, so the public signature stays fixed. Nested
/// ensembles recurse into this function with depth+1, the SAME snapshot (a
/// parent and child resolving the same model share one version) and a
/// branch-local ancestors chain (E1: recursion detection checks only THIS
/// branch's path — concurrent siblings never share it, B1).
#[allow(clippy::too_many_arguments)] // execution plumbing: state+ids+snapshot ride together by design
pub(crate) async fn execute_ensemble_inner(
    state: Arc<AppState>,
    model_name: &str,
    version: &str,
    payload: EnsembleValue,
    request_id: &str,
    opts: EnsembleExecOpts,
    snapshot: &Arc<VersionSnapshot>,
    depth: u32,
    ancestors: &[(String, String)], // E1: enclosing ensembles (self NOT included)
) -> Result<EnsembleOutcome, AppError> {
    // E1: depth limit before anything else (cheapest failure first).
    ensure_nesting_depth(depth)?;
    // E1: this branch's nesting chain — the current ensemble plus every
    // enclosing one. Each recursion level extends its OWN copy, so a
    // concurrent sibling branch never sees this entry (B1: the flat shared
    // Vec misread sibling fan-out as recursion).
    let ancestors = extend_ancestor_chain(ancestors, model_name, version);
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
            model_name, version, request_id, &opts, deadline_unix_ns, snapshot, depth, &ancestors,
        ).await?;
        let value = context.get(&plan.steps[tail_idx].name).cloned()
            .ok_or_else(|| AppError::Internal("ensemble produced no output".to_string()))?;
        // E2 (batch 3): explicit output field projection (whole-value pass
        // through when `output:` carried no `.field`).
        let value = select_output_field(&plan.steps[tail_idx].name, value, plan.output_field.as_deref())?;
        return Ok(EnsembleOutcome::Unary(value));
    }

    // §4.2 pipeline branch (batch 2): the DAG contains a chain — run the
    // pre-layers before the chain head, then spawn the whole chain and
    // return the tail stream immediately (D25's split, now the chain
    // branch; the tail-stream branch below remains for non-pipeline DAGs).
    if !plan.chains.is_empty() {
        let chain = &plan.chains[0];
        // §4.2: non-chain parts advance by layer — every non-chain step up
        // to the chain tail still runs (chain nodes excluded). The historical
        // executor runs whole layers, so pass layers filtered to non-chain
        // indices; they never depend on chain streaming outputs (P-R1/D26),
        // so running them before the chain is always valid.
        let mut pre_layers: Vec<Vec<usize>> = Vec::new();
        for layer in &plan.layers {
            let non_chain: Vec<usize> = layer
                .iter()
                .copied()
                .filter(|&i| !chain.nodes.contains(&i))
                .collect();
            if !non_chain.is_empty() {
                pre_layers.push(non_chain);
            }
        }
        run_layers(
            &state, &plan, &pre_layers, &mut context,
            model_name, version, request_id, &opts, deadline_unix_ns, snapshot, depth, &ancestors,
        )
        .await?;
        let mut stream = spawn_chain(
            &state, &plan, chain, &context, request_id, &opts, deadline_unix_ns, snapshot,
        )
        .await?;
        // P10 (D40): the permit is WEIGHTED by the chain's streaming-step
        // count (末步 = 1, k 链 = k — residency scales linearly with the
        // chain length).
        if let Some(capacity) = state.worker_manager.streaming_capacity() {
            stream.permit = Some(capacity.try_acquire_many(chain.nodes.len())?);
        }
        return Ok(EnsembleOutcome::Stream(stream));
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
            &state, &plan.steps[tail_idx], request_id, &opts, deadline_unix_ns, snapshot,
        )
        .await
    };
    let layers_fut = async {
        run_layers(
            &state, &plan, &plan.layers[..tail_layer], &mut context,
            model_name, version, request_id, &opts, deadline_unix_ns, snapshot, depth, &ancestors,
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
        request_id, &opts, deadline_unix_ns, preflight?, snapshot, depth, &ancestors,
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
#[allow(clippy::too_many_arguments)] // layer-engine plumbing: state+plan+ctx+ids ride together by design
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
    snapshot: &Arc<VersionSnapshot>,
    depth: u32, // E1: nesting depth — carried into execute_step for recursion
    ancestors: &[(String, String)], // E1: this branch's chain (B1: never shared across sibling branches)
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
                // E4/D15: resolve the metric label BEFORE spawning (a failed
                // resolution aborts the layer here — the same error execute_step
                // would surface for that step). The resolved label is the
                // truthful version the step will call.
                let resolved_label = snapshot.resolve(&state.registry, &step)?;
                // m4: the metric label records the actual version only for
                // EXPLICIT versions — unresolved ("latest"/omitted) normalizes
                // to "latest" so active drift cannot grow the label set
                // (model × step × version).
                let version_label = if step.version.is_some() {
                    resolved_label
                } else {
                    "latest".to_string()
                };
                let snapshot = snapshot.clone();
                let ancestors = ancestors.to_vec();
                set.spawn(async move {
                    let start = Instant::now();
                    let result =
                        execute_step(state, &step, &ctx, &request_id, &client_ip, deadline_unix_ns, &snapshot, depth, &ancestors)
                            .await;
                    let latency = start.elapsed().as_secs_f64();
                    crate::metrics::prometheus::record_ensemble_step_latency(
                        &ensemble_name, &step.name, &step.model, &version_label, depth, latency,
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
#[allow(clippy::too_many_arguments)] // layer-engine plumbing: state+plan+ctx+ids ride together by design
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
    snapshot: &Arc<VersionSnapshot>,
    depth: u32, // E1: carried into sibling unary steps (they can recurse)
    ancestors: &[(String, String)], // E1: this branch's chain (B1: never shared across sibling branches)
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

    let run_stream = execute_stream_step(state, plan, tail_idx, context, request_id, opts, deadline_unix_ns, preflight, snapshot);
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
            let snapshot = snapshot.clone();
            let ancestors = ancestors.to_vec();
            set.spawn(async move {
                let name = step.name.clone();
                execute_step(state, &step, &ctx, &request_id, &client_ip, deadline_unix_ns, &snapshot, depth, &ancestors).await
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
    snapshot: &Arc<VersionSnapshot>,
) -> Result<Option<TailPreflight>, AppError> {
    // E4/D15: execution-time version resolution (same source as execute_step).
    let resolved_version = snapshot.resolve(&state.registry, step)?;
    // P7 constraint: only preflight an already-ready sub-model. Not ready →
    // fall back to the serial path (which runs the autoload + poll).
    if !state.registry.is_ready(&step.model, Some(&resolved_version)) {
        return Ok(None);
    }
    if !streaming_worker_ready(state, &step.model, &resolved_version, &pb::RequestMeta::default()).await {
        return Ok(None);
    }
    let mv = state.registry.get(&step.model, Some(&resolved_version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", step.model, resolved_version)))?;
    let clients = state.worker_manager.get_zmq_clients(&step.model, &resolved_version).await
        .ok_or_else(|| AppError::WorkerCrashed(format!("{} {} has no ZMQ clients", step.model, resolved_version)))?;

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
    // E5 (batch 3): the preflight meta carries the STEP deadline — the same
    // min(parent, now + timeout_secs) formula the serial path would compute.
    let preflight_deadline = step_effective_deadline(deadline_unix_ns, step.timeout_secs);
    let meta = build_step_meta(
        &format!("{}:{}", request_id, step.name), &opts.client_ip, preflight_deadline, step_headers, bytes::Bytes::new(),
    );
    let outlier = state.worker_manager.get_outlier_state(&step.model, &resolved_version).await;
    let seq_registry = state.inference_queue.sequence_registry();
    let worker_id = crate::worker::pick_streaming_worker(
        &meta, mv.workers.len(), outlier.as_deref(), seq_registry, &step.model, &resolved_version,
    ).map_err(|e| AppError::Validation(e.0))?;
    if worker_id >= clients.len() {
        return Err(AppError::WorkerCrashed("invalid worker index".to_string()));
    }
    Ok(Some(TailPreflight { meta, worker_id, clients }))
}

/// §4.2: forward a worker stream into a downstream channel. Only Chunk
/// frames pass through — a per-chunk sub-stream's Done is an intermediate
/// milestone, NOT the chain's end (the tail hop synthesizes the final Done);
/// an Error frame passes through and terminates the whole chain (a node
/// failure cancels its worker and propagates upstream, D18). When the
/// downstream closes (adapter disconnect / next hop exiting), send
/// StreamCancel to THIS worker and end.
/// Returns `true` when the stream ended with an Error frame (chain stop).
async fn forward_stream(
    mut rx: mpsc::Receiver<pb::StreamResponse>,
    tx: mpsc::Sender<pb::StreamResponse>,
    cancel_client: Arc<crate::transport::zmq::WorkerZmqClient>,
    stream_id: String,
) -> bool {
    // m4: cumulative time this hop's downstream channel was FULL (the 64-slot
    // bound is backpressure, not a drop — saturation measures how long it held).
    let mut saturation_secs: f64 = 0.0;
    while let Some(chunk) = rx.recv().await {
        match chunk.payload {
            Some(pb::stream_response::Payload::Chunk(_)) => {
                let send_start = Instant::now();
                let send_res = tx.send(chunk).await;
                saturation_secs += send_start.elapsed().as_secs_f64();
                if send_res.is_err() {
                    let _ = cancel_client
                        .send_raw(crate::streaming::build_stream_cancel(stream_id))
                        .await;
                    crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
                        saturation_secs,
                    );
                    return false;
                }
            }
            Some(pb::stream_response::Payload::Error(_)) => {
                // Propagate the failure downstream, then stop the chain.
                let _ = tx.send(chunk).await;
                crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
                    saturation_secs,
                );
                return true;
            }
            Some(pb::stream_response::Payload::Done(_)) => {
                crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
                    saturation_secs,
                );
                return false;
            }
            _ => {}
        }
    }
    crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
        saturation_secs,
    );
    false
}

/// §4.2: a pipeline chain consumer — one nested send_stream sub-call PER
/// upstream chunk (chunk → 组包 → sub-stream → forward). D20: each sub-call
/// carries request_id `{parent}:{step}:{chunk_seq}`. D18: a failed
/// downstream send cancels this step's worker; the chain-handle list is
/// updated per sub-stream (tail inserted at index 0, others appended).
#[allow(clippy::too_many_arguments)] // chain-hop plumbing: state+plan+ctx+ids ride together by design
async fn consume_stream_consumer(
    state: &Arc<AppState>,
    plan: &EnsemblePlan,
    step_idx: usize,
    prev_step_name: &str,
    mut upstream: mpsc::Receiver<pb::StreamResponse>,
    downstream: mpsc::Sender<pb::StreamResponse>,
    context: &HashMap<String, EnsembleValue>,
    request_id: &str,
    opts: &EnsembleExecOpts,
    deadline_unix_ns: Option<i64>,
    chain_handles: &Arc<std::sync::Mutex<Vec<StreamHandle>>>,
    is_tail: bool,
    snapshot: &Arc<VersionSnapshot>,
) -> Result<(), AppError> {
    let step = &plan.steps[step_idx];
    let mut seq: u64 = 0;
    let mut error_terminated = false;
    while let Some(chunk) = upstream.recv().await {
        match &chunk.payload {
            Some(pb::stream_response::Payload::Chunk(c)) => {
                // Chunk → the previous step's value in a per-chunk context.
                let chunk_value: Value = serde_json::from_slice(&c.data).map_err(|e| {
                    AppError::Internal(format!(
                        "pipeline chunk from '{}' is not valid JSON: {e}",
                        prev_step_name
                    ))
                })?;
                let mut ctx = context.clone();
                ctx.insert(
                    prev_step_name.to_string(),
                    EnsembleValue::Json(chunk_value),
                );
                let sub_request_id = format!("{}:{}:{}", request_id, step.name, seq);
                let sub = execute_stream_step(
                    state, plan, step_idx, &ctx, &sub_request_id, opts, deadline_unix_ns, None,
                    snapshot,
                )
                .await?;
                {
                    let mut handles = chain_handles.lock().unwrap();
                    let handle = StreamHandle {
                        stream_id: sub.stream_id.clone(),
                        cancel_client: Arc::clone(&sub.cancel_client),
                        abort: tokio::spawn(async {}).abort_handle(),
                    };
                    if is_tail {
                        // D25: tail at index 0 — the top-level quick-access
                        // fields mirror it.
                        handles.insert(0, handle);
                    } else {
                        handles.push(handle);
                    }
                }
                let error_terminated = forward_stream(
                    sub.chunk_rx,
                    downstream.clone(),
                    sub.cancel_client.clone(),
                    sub.stream_id.clone(),
                )
                .await;
                if error_terminated {
                    // A node failed: cancel its worker and stop consuming
                    // upstream — the upstream sender drops with us (D18).
                    let _ = sub.cancel_client
                        .send_raw(crate::streaming::build_stream_cancel(sub.stream_id.clone()))
                        .await;
                    return Ok(());
                }
                seq += 1;
            }
            Some(pb::stream_response::Payload::Done(_)) => break,
            // An upstream Error frame is terminal (§4.4: every mid-stream
            // failure must reach the client as an Error frame). A hop's own
            // sub-stream Error is forwarded by forward_stream; a frame
            // arriving HERE comes from the head hop (which has no forwarding
            // hop) or an upstream hop's channel — forward it once, then stop.
            // Do not synthesize a Done after it.
            Some(pb::stream_response::Payload::Error(_)) => {
                let _ = downstream.send(chunk).await;
                error_terminated = true;
                break;
            }
            _ => {}
        }
    }
    // Tail hop: upstream exhausted = the chain finished normally — synthesize
    // the final Done so the adapter's close收口 fires with a clean reason
    // (the per-chunk sub-stream Dones were never forwarded).
    if is_tail && !error_terminated {
        let _ = downstream
            .send(pb::StreamResponse {
                stream_id: String::new(),
                payload: Some(pb::stream_response::Payload::Done(pb::StreamDone {
                    metrics: None,
                })),
            })
            .await;
    }
    Ok(())
}

/// §4.2: spawn the pipeline chain as one task tree and return the tail
/// stream immediately. The head opens its worker stream synchronously
/// (build failures surface as real error codes); every consumer hop then
/// runs as a task: per-chunk nested sub-streams, flows through bounded
/// mpsc channels (64, D2 — full = backpressure, never dropped). The chain
/// root's AbortHandle is the D18 teardown point.
#[allow(clippy::too_many_arguments)] // chain plumbing: state+plan+ctx+ids ride together by design
async fn spawn_chain(
    state: &Arc<AppState>,
    plan: &EnsemblePlan,
    chain: &Chain,
    context: &HashMap<String, EnsembleValue>,
    request_id: &str,
    opts: &EnsembleExecOpts,
    deadline_unix_ns: Option<i64>,
    snapshot: &Arc<VersionSnapshot>,
) -> Result<EnsembleStream, AppError> {
    let nodes: Vec<usize> = chain.nodes.clone();
    let tail_idx = *nodes.last().unwrap();
    let tail = &plan.steps[tail_idx];
    // E4/D15: the metric label carries the RESOLVED tail version (the chain
    // nodes resolve via the same snapshot when their streams open).
    let resolved_tail = snapshot.resolve(&state.registry, tail)?;
    // m4: label normalization — explicit versions only; unresolved
    // ("latest"/omitted) records "latest" (no cardinality growth on drift).
    let tail_version = if tail.version.is_some() {
        resolved_tail
    } else {
        "latest".to_string()
    };

    // Inter-hop channels (nodes.len() - 1) + the tail output channel.
    let mut hop_txs: Vec<mpsc::Sender<pb::StreamResponse>> = Vec::new();
    let mut hop_rxs: Vec<mpsc::Receiver<pb::StreamResponse>> = Vec::new();
    for _ in 1..nodes.len() {
        // D2: inter-hop channel capacity 64, aligned with STREAM_CHANNEL_SIZE.
        let (tx, rx) = mpsc::channel(64);
        hop_txs.push(tx);
        hop_rxs.push(rx);
    }
    let (tail_tx, tail_rx) = mpsc::channel(64);

    // D18: chain-handle collection shared with the consumer tasks.
    let chain_handles: Arc<std::sync::Mutex<Vec<StreamHandle>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    // Head: open the first worker stream from the pre-layer context
    // SYNCHRONOUSLY — build failures surface as real error codes before the
    // stream is returned (the chain consumers then run detached).
    let head_stream = execute_stream_step(
        state, plan, nodes[0], context, request_id, opts, deadline_unix_ns, None, snapshot,
    )
    .await?;
    {
        // D18: record the head handle (appended; the tail lands at index 0).
        let mut h = chain_handles.lock().unwrap();
        h.push(StreamHandle {
            stream_id: head_stream.stream_id.clone(),
            cancel_client: Arc::clone(&head_stream.cancel_client),
            abort: tokio::spawn(async {}).abort_handle(),
        });
    }
    // The top-level cancel_client is the head stream's client (synchronously
    // available) — adapters that only read the quick-access fields stay
    // unchanged; chain cancellation broadcasts over `chain` via
    // [`cancel_ensemble_stream`].
    let cancel_client = {
        let handles = chain_handles.lock().unwrap();
        Arc::clone(&handles[0].cancel_client)
    };

    let state_h = state.clone();
    let plan_h = plan.clone();
    let context_h = context.clone();
    let request_id_h = request_id.to_string();
    let opts_h = opts.clone();
    let handles_h = chain_handles.clone();
    let tail_tx_h = tail_tx.clone();
    let snapshot_h = snapshot.clone();
    let chain_depth = nodes.len();
    crate::metrics::prometheus::record_ensemble_pipeline_chain_depth(chain_depth);
    let root = tokio::spawn(async move {
        // Consumer hops: hop i consumes hop i-1's output and forwards into
        // hop i's channel (or the tail channel).
        let mut tasks: Vec<tokio::task::JoinHandle<Result<(), AppError>>> = Vec::new();
        let mut prev_rx = head_stream.chunk_rx;
        for (i, &node) in nodes.iter().enumerate().skip(1) {
            let is_tail = i == nodes.len() - 1;
            let down_tx = if is_tail {
                tail_tx_h.clone()
            } else {
                hop_txs[i - 1].clone()
            };
            let up_rx = std::mem::replace(&mut prev_rx, hop_rxs.remove(0));
            let state = state_h.clone();
            let plan = plan_h.clone();
            let ctx = context_h.clone();
            let req = request_id_h.clone();
            let opts = opts_h.clone();
            let handles = handles_h.clone();
            let snapshot = snapshot_h.clone();
            let prev_name = plan_h.steps[nodes[i - 1]].name.clone();
            tasks.push(tokio::spawn(async move {
                consume_stream_consumer(
                    &state, &plan, node, &prev_name, up_rx, down_tx, &ctx, &req, &opts,
                    deadline_unix_ns, &handles, is_tail, &snapshot,
                )
                .await
            }));
        }
        for t in tasks {
            t.await
                .map_err(|e| AppError::Internal(format!("chain task join error: {e}")))??;
        }
        Ok::<(), AppError>(())
    });

    let stream_id = format!("chain-{}", Uuid::new_v4());

    Ok(EnsembleStream {
        chunk_rx: tail_rx,
        stream_id,
        cancel_client,
        tail_model: tail.model.clone(),
        tail_version,
        tail_step: tail.name.clone(),
        step_deadline: None,
        chain: chain_handles,
        // D18: the chain task tree's root handle — the adapter aborts it on
        // disconnect as the teardown backstop.
        abort: root.abort_handle(),
        permit: None,
    })
}

/// D18: cancel an ensemble stream from the adapters. A pipeline chain
/// broadcasts over every chain handle (each streaming step's worker) and
/// aborts the chain task tree; the top-level client is the fallback (empty
/// handles = non-chain, or a chain whose consumers have not opened yet).
pub async fn cancel_chain(
    chain: Option<&Arc<std::sync::Mutex<Vec<StreamHandle>>>>,
    abort: Option<&tokio::task::AbortHandle>,
    fallback_stream_id: &str,
    fallback_client: &Arc<crate::transport::zmq::WorkerZmqClient>,
) {
    match chain {
        Some(chain) => {
            // Collect the cancel targets under the lock, then send outside it
            // (MutexGuard must not cross an await — the future stays Send).
            let targets: Vec<(String, Arc<crate::transport::zmq::WorkerZmqClient>)> = {
                let handles = chain.lock().unwrap();
                if handles.is_empty() {
                    vec![(fallback_stream_id.to_string(), fallback_client.clone())]
                } else {
                    handles
                        .iter()
                        .map(|h| (h.stream_id.clone(), h.cancel_client.clone()))
                        .collect()
                }
            };
            for (sid, client) in targets {
                let req = crate::streaming::build_stream_cancel(sid);
                let _ = client.send_raw(req).await;
            }
            if let Some(a) = abort {
                a.abort();
            }
        }
        None => {
            let req = crate::streaming::build_stream_cancel(fallback_stream_id.to_string());
            let _ = fallback_client.send_raw(req).await;
        }
    }
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
/// registry ready, streaming = `streaming_worker_ready`). Callers pass the
/// RESOLVED version (E4/D15 — resolution happens once per step).
async fn ensure_sub_model_loaded(
    state: &Arc<AppState>,
    model: &str,
    version: &str,
) -> Result<(), AppError> {
    if state.registry.is_ready(model, Some(version)) {
        return Ok(());
    }
    let load_start = Instant::now();
    info!("Auto-loading sub-model {} v{} for ensemble", model, version);
    let sub_model_dir = crate::validation::resolve_model_dir(
        &state.repo_path, model, version,
    )?;
    // 配置解析/校验失败必须可见(同 reconcile:不再 unwrap_or_default
    // 静默回退默认配置;M7 迁移哨兵依赖此错误上浮)。
    let mut config = match crate::config::load_model_config(
        &sub_model_dir.join("config.yaml")
    ) {
        Ok(c) => c,
        Err(e) => {
            return Err(AppError::ModelNotReady(format!(
                "sub-model {} v{} has invalid config.yaml: {}", model, version, e
            )));
        }
    };
    state.config.apply_model_defaults(&mut config);
    if let Err(e) = state.worker_manager.load_model(model, version, &config).await {
        // Concurrent autoload race (parallel steps / batch elements of the
        // same cold sub-model): the load error usually means a SIBLING's
        // load won — the registry flips ready when that load completes.
        // Poll briefly before surfacing the failure, so a benign conflict
        // never fails a whole step/element.
        for _ in 0..30 {
            if state.registry.is_ready(model, Some(version)) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        warn!("Failed to auto-load sub-model {} v{}: {}", model, version, e);
        return Err(AppError::ModelNotReady(format!(
            "sub-model {} v{} not ready: {}", model, version, e
        )));
    }
    // m4: autoload wait (the dominant cold-TTFT term for ensemble DAGs).
    crate::metrics::prometheus::record_ensemble_autoload_wait_seconds(
        load_start.elapsed().as_secs_f64(),
    );
    Ok(())
}

/// Execute the streaming tail step: resolve inputs, assemble payload (same
#[allow(clippy::too_many_arguments)] // layer-engine plumbing: state+plan+ctx+ids ride together by design
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
    snapshot: &Arc<VersionSnapshot>,
) -> Result<EnsembleStream, AppError> {
    let step = &plan.steps[step_idx];
    // E4/D15: execution-time version resolution (same source as execute_step;
    // the metric label tail_version below is the resolved version).
    let resolved_version = snapshot.resolve(&state.registry, step)?;
    // E5/D35 (batch 3): the step's wall-clock cap — the readiness poll, D19
    // checks and the meta cascade below are bounded by min(parent deadline,
    // now + timeout_secs). Each chain hop computes its own from its own
    // open time (the same formula per streaming step).
    let step_deadline = step_effective_deadline(deadline_unix_ns, step.timeout_secs);

    // Resolve inputs into EnsembleValues (identical to the unary path).
    let mut resolved: HashMap<String, EnsembleValue> = HashMap::new();
    for (key, ref_str) in &step.inputs {
        let value = resolve_ref(ref_str, context)?;
        resolved.insert(key.clone(), value);
    }
    let (payload_bytes, content_type_for_step) = assemble_step_payload(&step.name, &resolved, &step.params)?;

    // P7: when the preflight already routed/built meta in parallel with the
    // pre-layers, skip the serial readiness/meta/pick block.
    let (mut meta, worker_id, clients) = match preflight {
        Some(pf) => (pf.meta, pf.worker_id, pf.clients),
        None => {
            // D19: ensure the sub-model is loaded, then poll STREAMING
            // readiness (pick non-empty) with backoff. A pick failure is a
            // retryable state.
            ensure_sub_model_loaded(state, &step.model, &resolved_version).await?;
            let mut retries = 0;
            let max_retries = 30;
            let mut delay = Duration::from_millis(50);
            while !streaming_worker_ready(state, &step.model, &resolved_version, &pb::RequestMeta::default()).await {
                if retries >= max_retries {
                    return Err(AppError::ModelNotReady(format!(
                        "sub-model {} v{} streaming not ready", step.model, resolved_version
                    )));
                }
                // D19: quick-fail when the remaining budget cannot cover one
                // more poll — never spin to timeout (E5: the STEP budget).
                if let Some(rem) = crate::deadline::remaining(step_deadline) {
                    if rem < delay {
                        return Err(AppError::InferenceTimeout(format!(
                            "ensemble step {}: deadline exhausted while waiting for sub-model {} v{}",
                            step.name, step.model, resolved_version
                        )));
                    }
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_millis(500));
                retries += 1;
            }

            let mv = state.registry.get(&step.model, Some(&resolved_version))
                .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", step.model, resolved_version)))?;
            // E1/D4: ensembles have no streaming form (zero workers) — a
            // streaming step calling one is an unsupported combination, not a
            // readiness problem. Fail fast instead of exhausting the D19 poll.
            if mv.model_type == ModelType::Ensemble {
                return Err(AppError::InvalidRequestBody(format!(
                    "streaming step '{}' calls ensemble model '{}' — ensembles have no \
                     streaming form (D4)",
                    step.name, step.model
                )));
            }
            let clients = state.worker_manager.get_zmq_clients(&step.model, &resolved_version).await
                .ok_or_else(|| AppError::WorkerCrashed(format!("{} {} has no ZMQ clients", step.model, resolved_version)))?;

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
                &format!("{}:{}", request_id, step.name), &opts.client_ip, step_deadline, step_headers, bytes::Bytes::new(),
            );

            let outlier = state.worker_manager.get_outlier_state(&step.model, &resolved_version).await;
            let seq_registry = state.inference_queue.sequence_registry();
            let worker_id = crate::worker::pick_streaming_worker(
                &meta, mv.workers.len(), outlier.as_deref(), seq_registry, &step.model, &resolved_version,
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
    // budget must not open one (deadline-exhausted mapping, §4.4; E5: the
    // STEP budget).
    if let Some(rem) = crate::deadline::remaining(step_deadline) {
        if rem.is_zero() {
            return Err(AppError::InferenceTimeout(format!(
                "ensemble step {}: deadline exhausted before stream open", step.name
            )));
        }
    }
    crate::metrics::prometheus::record_worker_inference(&step.model, &resolved_version, worker_id, 1);

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
        // m4: label normalization — explicit versions only; unresolved
        // ("latest"/omitted) records "latest" (no cardinality growth on
        // active drift).
        tail_version: if step.version.is_some() {
            resolved_version.clone()
        } else {
            "latest".to_string()
        },
        tail_step: step.name.clone(),
        // D35 (batch 3): E5 timeout_secs → step wall-clock cap measured from
        // the open instant; the adapters' recv_chunk overall takes min(client
        // overall, this). None = no step timeout. Absurdly large caps are
        // parse-legal — an unrepresentable Duration degrades to "no cap"
        // (None) instead of a from_secs_f64 overflow panic.
        step_deadline: step.timeout_secs.and_then(|t| {
            Instant::now().checked_add(Duration::try_from_secs_f64(t).ok()?)
        }),
        chain: Arc::new(std::sync::Mutex::new(chain)),
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
        // The request_id passed in is ALREADY the full step-scoped id
        // (callers append `:{step}` or `:{step}:{chunk_seq}` for pipeline
        // sub-calls, D20) — no suffixing here so pipeline chunk calls can
        // carry their own seq.
        request_id: request_id.to_string(),
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
/// E3 (batch 3): `params` merge into the assembled JSON object AFTER the
/// inputs — params win on key conflicts. The merge only shapes the payload
/// layer (m3: it never touches the sub-model's own model_defaults/server
/// overrides). Binary assembly has no params semantics — a non-empty params
/// on a Binary step is rejected here (the input type is only decidable at
/// assembly; this is the earliest enforcement point).
///
/// P9: the Json branch pre-sizes its buffer from a cheap estimate (no
/// realloc chain) and hands the Vec straight to `Bytes` (ownership transfer,
/// zero copy — the queue payload stays Bytes end-to-end, §1.4); the Binary
/// branch passes the Bytes through by refcount (P2: the old `to_vec` copy is
/// gone).
fn assemble_step_payload(
    step_name: &str,
    resolved: &HashMap<String, EnsembleValue>,
    params: &HashMap<String, Value>,
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
        // E3: constant step params override the assembled inputs.
        for (key, val) in params {
            obj.insert(key.clone(), val.clone());
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
        // E3: Binary assembly has no params semantics.
        if !params.is_empty() {
            return Err(AppError::InvalidRequestBody(format!(
                "step '{}': params cannot be combined with a binary input (E3)",
                step_name
            )));
        }
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

#[allow(clippy::too_many_arguments)] // step plumbing: state+ctx+ids+snapshot ride together by design
async fn execute_step(
    state: Arc<AppState>,
    step: &EnsembleStep,
    context: &HashMap<String, EnsembleValue>,
    request_id: &str,
    client_ip: &str,
    deadline_unix_ns: Option<i64>,
    snapshot: &Arc<VersionSnapshot>,
    depth: u32, // E1: nesting depth — +1 on recursion into an ensemble step
    ancestors: &[(String, String)], // E1: THIS branch's nesting chain (B1: never shared across sibling branches)
) -> Result<EnsembleValue, AppError> {
    // E4/D15: resolve the sub-model version at execution time — explicit
    // versions as-is, unresolved ("latest"/omitted) via the request-scoped
    // snapshot (first resolution per model wins; registry active is the only
    // source, D27). Every readiness/queue/registry call below uses the
    // resolved version.
    let resolved_version = snapshot.resolve(&state.registry, step)?;

    // E5 (batch 3): the step's wall-clock cap — the meta cascade and the
    // response wait below are bounded by min(parent deadline, now +
    // timeout_secs).
    let step_deadline = step_effective_deadline(deadline_unix_ns, step.timeout_secs);
    // E5: a cap that expired BEFORE submit fails fast (§4.4 deadline row).
    if let Some(rem) = crate::deadline::remaining(step_deadline) {
        if rem.is_zero() {
            return Err(AppError::InferenceTimeout(format!(
                "ensemble step {}: step timeout exhausted before submit",
                step.name
            )));
        }
    }

    // Resolve inputs into EnsembleValues.
    let mut resolved: HashMap<String, EnsembleValue> = HashMap::new();
    for (key, ref_str) in &step.inputs {
        let value = resolve_ref(ref_str, context)?;
        resolved.insert(key.clone(), value);
    }

    // B3 (E7): input assembly — three branches (see assemble_step_payload).
    let (payload_bytes, content_type_for_step) = assemble_step_payload(&step.name, &resolved, &step.params)?;

    // Ensure sub-model is ready (shared autoload; unary readiness predicate)
    ensure_sub_model_loaded(&state, &step.model, &resolved_version).await?;
    // Poll with exponential backoff for worker readiness
    let mut retries = 0;
    let max_retries = 30;
    let mut delay = Duration::from_millis(50);
    while !state.registry.is_ready(&step.model, Some(&resolved_version)) && retries < max_retries {
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_millis(500));
        retries += 1;
    }

    if !state.registry.is_ready(&step.model, Some(&resolved_version)) {
        return Err(AppError::ModelNotReady(format!(
            "sub-model {} v{} is not ready", step.model, resolved_version
        )));
    }

    // Get model version info
    let mv = state.registry.get(&step.model, Some(&resolved_version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", step.model, resolved_version)))?;

    if mv.model_type == ModelType::Ensemble {
        // E1 (batch 3): nested ensemble — recurse with depth+1, the SAME
        // version snapshot (D36: a parent and child resolving the same model
        // share one resolution) and the shared parent deadline (E1: 递归共享
        // 父 deadline). The child request is the step's assembled payload
        // (params already merged, E3) re-materialized as a value.
        if contains_ancestor(ancestors, &step.model, &resolved_version) {
            return Err(AppError::InvalidRequestBody(format!(
                "ensemble recursion detected: {} v{} is already on the active nesting chain",
                step.model, resolved_version
            )));
        }
        // D4: a nested ensemble must not contain streaming steps (the
        // combination is out of scope — checked before recursing so the
        // error names the offending child DAG).
        let child_plan = get_ensemble_plan(&state, &step.model, &resolved_version).await?;
        if child_plan.steps[child_plan.output_step].stream || !child_plan.chains.is_empty() {
            return Err(AppError::InvalidRequestBody(format!(
                "nested ensemble '{}' contains a streaming step — nested ensembles \
                 must be unary-only (D4)",
                step.model
            )));
        }
        let child_payload = match &content_type_for_step {
            Some(ct) => EnsembleValue::Binary(payload_bytes.clone(), ct.clone()),
            None => EnsembleValue::Json(
                serde_json::from_slice(&payload_bytes).map_err(|e| {
                    AppError::Internal(format!(
                        "ensemble step {} payload reparse failed: {}",
                        step.name, e
                    ))
                })?,
            ),
        };
        let child_request_id = format!("{}:{}", request_id, step.name);
        let child_opts = EnsembleExecOpts {
            client_ip: client_ip.to_string(),
            deadline_unix_ns,
            decoupled: false, // the child is unary-only (D4)
        };
        let nested = execute_nested_ensemble_boxed(
            state.clone(),
            step.model.clone(),
            resolved_version.clone(),
            child_payload,
            child_request_id,
            child_opts,
            Arc::clone(snapshot),
            depth + 1,
            ancestors.to_vec(),
        );
        // E5 (batch 3): the step wall-clock cap bounds the nested run too —
        // the child execution IS this step's execution, same local timeout
        // wrap as the unary response wait below. E1's "recursion shares the
        // PARENT deadline" is preserved: child_opts still carries the parent
        // deadline; the cap is enforced here at the step boundary.
        let outcome = match crate::deadline::remaining(step_deadline) {
            Some(rem) => match tokio::time::timeout(rem, nested).await {
                Ok(r) => r?,
                Err(_) => {
                    return Err(AppError::InferenceTimeout(format!(
                        "ensemble step {} timed out", step.name
                    )))
                }
            },
            None => nested.await?,
        };
        return match outcome {
            EnsembleOutcome::Unary(v) => Ok(v),
            // Unreachable under D4 (child plans with streaming are rejected
            // above) — kept as the type-level backstop.
            EnsembleOutcome::Stream(_) => Err(AppError::Internal(
                "nested ensemble returned a stream (D4 violation)".to_string(),
            )),
        };
    }

    let num_workers = mv.workers.len();
    if num_workers == 0 {
        return Err(AppError::WorkerCrashed(format!("{} has no workers", step.model)));
    }

    // Send inference request through the unified queue
    let uid = format!("ensemble_{}_{}_{}", step.model, resolved_version, Uuid::new_v4());

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
            // m6 (E1): nesting depth so an abnormal fan-out is discoverable.
            depth = depth,
            trace_id = tracing::field::Empty,
            span_id = tracing::field::Empty,
        );
        crate::telemetry::link_parent(&step_span, &opentelemetry::Context::current());
        let _guard = step_span.enter();
        crate::telemetry::inject(&mut step_headers);
    }

    let meta = build_step_meta(
        &format!("{}:{}", request_id, step.name), client_ip, step_deadline, step_headers,
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

    match state.inference_queue.try_submit(&step.model, &resolved_version, item) {
        Ok(()) => {}
        Err(crate::inference_queue::QueueError::Full) => {
            return Err(AppError::QueueFull(format!(
                "Queue full for {} {}", step.model, resolved_version
            )));
        }
        Err(_) => {
            return Err(AppError::ModelNotReady(format!(
                "Queue not available for {} {}", step.model, resolved_version
            )));
        }
    }

    // P-DEADLINE cascade (E5): bound this step by the STEP deadline's
    // remaining budget — min(parent, now + timeout_secs). None = no deadline
    // → unbounded inner wait, outer DAG bound still applies via
    // execute_ensemble's total_budget.
    let response = match crate::deadline::remaining(step_deadline) {
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
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
                stream: false,
                inputs: [("input".to_string(), "$request".to_string())].into(),
            },
            EnsembleStep {
                name: "step2".to_string(),
                model: "m2".to_string(),
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
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
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
                stream: false,
                inputs: [("input".to_string(), "$step2".to_string())].into(),
            },
            EnsembleStep {
                name: "step2".to_string(),
                model: "m2".to_string(),
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
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
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
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
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
                stream: false,
                inputs: [("x".to_string(), "$request".to_string())].into(),
            },
            EnsembleStep {
                name: "b".to_string(),
                model: "m2".to_string(),
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
                stream: false,
                inputs: [("x".to_string(), "$request".to_string())].into(),
            },
            EnsembleStep {
                name: "c".to_string(),
                model: "m3".to_string(),
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
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
        let (bytes, ct) = assemble_step_payload("s", &resolved, &HashMap::new()).unwrap();
        assert!(ct.is_none(), "all-Json assembly must not set a content-type");
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v, json!({"a": 1, "b": "x"}));
    }

    #[test]
    fn b3_assemble_single_binary_passthrough_with_ct() {
        let mut resolved = HashMap::new();
        resolved.insert("img".to_string(), bin(b"\x00\x01\x02", "image/png"));
        let (bytes, ct) = assemble_step_payload("s", &resolved, &HashMap::new()).unwrap();
        assert_eq!(bytes.as_ref(), b"\x00\x01\x02", "binary payload must pass verbatim");
        assert_eq!(ct.as_deref(), Some("image/png"), "CT must be forwarded");
    }

    #[test]
    fn b3_assemble_mixed_binary_json_is_400() {
        let mut resolved = HashMap::new();
        resolved.insert("a".to_string(), bin(b"x", "application/octet-stream"));
        resolved.insert("b".to_string(), EnsembleValue::Json(json!(1)));
        let err = assemble_step_payload("s", &resolved, &HashMap::new()).unwrap_err();
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
        let err = assemble_step_payload("s", &resolved, &HashMap::new()).unwrap_err();
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

    // === §4.0/D16: pipeline-form validation (batch 2) ===

    fn pstep(name: &str, inputs: &[(&str, &str)], stream: bool) -> EnsembleStep {
        sstep(name, inputs, stream)
    }

    #[test]
    fn pipeline_chain_two_streaming_steps_valid() {
        // s0 (streaming) → s1 (streaming, consumes s0 whole) = a valid chain;
        // the chain tail is the config last step (output semantics, §4.1-4).
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("s1", &[("data", "$s0")], true),
        ];
        let chains = build_chains(&steps, 1).expect("valid chain must build");
        assert_eq!(chains.len(), 1, "one chain expected");
        assert_eq!(chains[0].nodes, vec![0, 1], "chain order: s0 → s1");
    }

    #[test]
    fn pipeline_r1_two_consumers_rejected() {
        // P-R1: a streaming step's output must have EXACTLY one consumer.
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("s1", &[("data", "$s0")], true),
            pstep("s2", &[("data", "$s0")], true),
        ];
        let err = build_chains(&steps, 2).unwrap_err();
        assert!(
            err.to_string().contains("consumer"),
            "P-R1 must name the consumer rule, got: {err}"
        );
    }

    #[test]
    fn pipeline_r2_whole_ref_only_rejected() {
        // P-R2: streaming outputs can only be referenced whole ($s0), never
        // field-projected ($s0.field).
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("s1", &[("data", "$s0.token")], true),
        ];
        let err = build_chains(&steps, 1).unwrap_err();
        assert!(
            err.to_string().contains("whole"),
            "P-R2 must reject field references, got: {err}"
        );
    }

    #[test]
    fn pipeline_d26_unary_consumer_rejected() {
        // D26: a chain's unary consumer has no clean chunk→unary→chunk
        // semantics — the form is rejected at parse time ("pipeline chain
        // tail must be a streaming step" covers the unary-tail shape).
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("u1", &[("data", "$s0")], false),
        ];
        let err = build_chains(&steps, 1).unwrap_err();
        assert!(
            err.to_string().contains("streaming") || err.to_string().contains("tail"),
            "D26 must reject the unary-consumer chain form, got: {err}"
        );
    }

    #[test]
    fn pipeline_r5_mixed_forms_rejected() {
        // P-R5: the DAG's streaming set is exactly ONE chain OR one tail
        // streaming step — a chain plus an orphan streaming step is an error.
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("s1", &[("data", "$s0")], true),
            pstep("s2", &[("input", "$request")], true),
        ];
        // Chain s0→s1 (tail = s1 = config last); s2 is an orphan streaming
        // step that is NOT the output step.
        let err = build_chains(&steps, 1).unwrap_err();
        assert!(
            err.to_string().contains("streaming step"),
            "P-R5 must reject chain + orphan streaming step, got: {err}"
        );
    }

    #[test]
    fn pipeline_chain_tail_must_be_output_step() {
        // P-R3: the chain tail must be the DAG output step. Chain s0→s1 but
        // the config last step is s2 (unary) → the chain tail is not the
        // output → rejected (§4.1-4 output semantics).
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("s1", &[("data", "$s0")], true),
            pstep("s2", &[("data", "$request")], false),
        ];
        let err = build_chains(&steps, 2).unwrap_err();
        assert!(
            err.to_string().contains("output step"),
            "chain tail must be the output step, got: {err}"
        );
    }

    #[test]
    fn pipeline_orphan_streaming_step_not_output_rejected() {
        // A zero-consumer streaming step that is NOT the output step is an
        // orphan (§4.1-4: the config last step must be the streaming step).
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("u1", &[("data", "$request")], false),
        ];
        let err = build_chains(&steps, 1).unwrap_err();
        assert!(
            err.to_string().contains("streaming"),
            "orphan streaming step must be rejected, got: {err}"
        );
    }

    // === §4.0/D16: streaming validation (form dispatch, batch 0) ===

    fn sstep(name: &str, inputs: &[(&str, &str)], stream: bool) -> EnsembleStep {
        EnsembleStep {
            name: name.to_string(),
            model: format!("m_{}", name),
            version: Some("1".to_string()),

            params: HashMap::new(),

            timeout_secs: None,
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
        assert!(validate_stream_rules(&steps, 1).is_ok(), "tail-streaming DAG must validate");
    }

    #[test]
    fn stream_rules_two_stream_steps_rejected() {
        // Rule 3: at most one streaming step per DAG.
        let steps = vec![
            sstep("s1", &[("input", "$request")], true),
            sstep("s2", &[("data", "$request")], true),
        ];
        let err = validate_stream_rules(&steps, 1).unwrap_err();
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
        let err = validate_stream_rules(&steps, 1).unwrap_err();
        assert!(
            err.to_string().contains("output") && err.to_string().contains("s1"),
            "must reject streaming step not at config tail, got: {err}"
        );
    }

    #[test]
    fn stream_rules_plain_dag_unchanged() {
        // No stream: false — behaviour parity with the historical validator.
        let steps = vec![
            sstep("s1", &[("input", "$request")], false),
            sstep("s2", &[("data", "$s1")], false),
        ];
        assert!(validate_stream_rules(&steps, 1).is_ok());
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
            output_field: None,
            chains: Vec::new(),
            config_path: PathBuf::from(path),
            source_mtime: None,
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
                    output_field: None,
                    chains: Vec::new(),
                    config_path,
                    source_mtime: None,
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

    // === /audit 2026-08-12: batch-0/1 defect repros (read-only; no impl changes) ===

    /// Concurrency assumption (D23): unload_version fires cache invalidation
    /// BEFORE registry changes, so an invalidate can race an in-flight
    /// single-flight load (cold cache + concurrent unload/reload). The holder
    /// must still resolve — not panic on its vanished Loading cell.
    #[tokio::test]
    async fn p0_cache_invalidate_during_inflight_load_must_not_panic() {
        let cache = Arc::new(EnsemblePlanCache::new());
        let key = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let c2 = cache.clone();
        let k2 = key.clone();
        let holder = tokio::spawn(async move {
            c2.get_or_load(k2, || async move {
                // Hold the single-flight load open (a slow disk read/parse).
                let _ = gate_rx.await;
                Ok::<_, AppError>(test_plan("/nonexistent"))
            })
            .await
        });
        // Wait until the holder has published the Loading cell.
        for _ in 0..1000 {
            if matches!(cache.plans.get(&key).as_deref(), Some(PlanCell::Loading { .. })) {
                break;
            }
            tokio::task::yield_now().await;
        }
        // D23: unload invalidates before registry changes — this races loads.
        cache.invalidate_model("m");
        let _ = gate_tx.send(());
        let joined = holder.await;
        assert!(
            joined.is_ok(),
            "invalidate racing an in-flight load panicked the holder: {joined:?}"
        );
        assert!(
            joined.unwrap().is_ok(),
            "the racing load must still resolve its plan"
        );
    }

    /// Order assumption (review ②): the loader stats BEFORE the read, so a
    /// write landing between stat and read leaves the stored mtime OLDER than
    /// the file's — the interval re-check must then re-parse (safe). The
    /// reverse order (stat after read) could pin a fresh mtime onto stale
    /// content and serve it indefinitely.
    #[tokio::test(start_paused = true)]
    async fn p0_cache_stat_after_read_must_not_pin_stale_plan() {
        let dir = std::env::temp_dir().join(format!("liteserver-ens-p0-toctou-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("config.yaml");
        std::fs::write(&config_path, b"v1").unwrap();
        // The production loader's stat-before-read mtime.
        let v1_mtime = std::fs::metadata(&config_path).and_then(|m| m.modified()).ok();
        // Real clock: force a distinct mtime for the interleaved write.
        std::thread::sleep(std::time::Duration::from_millis(10));
        let cache = EnsemblePlanCache::new();
        let key = PlanKey { model: "m".to_string(), version: "1".to_string() };
        // First load: returns the v1 plan (output_step=1) with the
        // stat-before-read mtime, but the file becomes v2 before the cache
        // stores the entry — exactly the stat→write→store interleaving the
        // mtime re-check must catch at the next interval.
        let first = cache
            .get_or_load(key.clone(), || {
                let cp = config_path.clone();
                async move {
                    std::fs::write(&cp, b"v2").unwrap(); // interleaved write
                    Ok::<_, AppError>(Arc::new(EnsemblePlan {
                        steps: Vec::new(),
                        layers: Vec::new(),
                        output_step: 1,
                        output_field: None,
                        chains: Vec::new(),
                        config_path: cp,
                        source_mtime: v1_mtime,
                    }))
                }
            })
            .await
            .unwrap();
        assert_eq!(first.output_step, 1);
        tokio::time::advance(Duration::from_millis(1500)).await;
        // Interval elapsed: the re-check must notice v2 (stored mtime is
        // v1's, older than the file's) and re-parse.
        let second = cache
            .get_or_load(key.clone(), || {
                let cp = config_path.clone();
                async move {
                    Ok::<_, AppError>(Arc::new(EnsemblePlan {
                        steps: Vec::new(),
                        layers: Vec::new(),
                        output_step: 2,
                        output_field: None,
                        chains: Vec::new(),
                        config_path: cp,
                        source_mtime: None,
                    }))
                }
            })
            .await
            .unwrap();
        assert_eq!(
            second.output_step, 2,
            "a mid-load write must be caught at the next interval re-check"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Control-flow assumption (§4.1 rule 1): a streaming step must be in the
    /// LAST topological layer. "Output not referenced" does NOT imply that —
    /// Kahn layering puts a no-dependency sink in layer 0 even when later
    /// layers exist. Accepting this config truncates execution at the tail's
    /// layer: steps in later layers silently never run.
    #[test]
    fn stream_rules_streaming_step_must_be_in_last_topological_layer() {
        // a (layer 0) ← c (layer 1); b (stream, no deps → layer 0, config-last).
        let yaml = r#"
ensemble:
  steps:
    - name: a
      model: m1
      version: "1"
      inputs: {x: "$request.x"}
    - name: c
      model: m2
      version: "1"
      inputs: {y: "$a"}
    - name: b
      model: m3
      version: "1"
      stream: true
      inputs: {z: "$request.z"}
"#;
        let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
        assert!(
            res.is_err(),
            "streaming step not in the last topological layer must be rejected (rule 1); \
             accepting it silently drops step c from execution"
        );
    }

    /// Data assumption: an empty steps list is malformed config. It must be a
    /// Config error at parse — not a `steps.len() - 1` underflow panic
    /// (regression: the pre-cache code returned a request-time 500).
    #[test]
    fn parse_ensemble_plan_empty_steps_is_config_error_not_panic() {
        let yaml = "ensemble:\n  steps: []\n";
        let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
        assert!(
            res.is_err(),
            "empty steps must be a config error, not an arithmetic panic"
        );
    }

    /// Config contract (D24): the ensemble schema must deny unknown fields so
    /// 0.9.0 keys (params/when/outputs/…) — or plain typos — fail fast at
    /// load instead of being silently ignored.
    #[test]
    fn ensemble_schema_rejects_unknown_fields_d24() {
        let yaml = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
      strem: true
"#;
        let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
        assert!(
            res.is_err(),
            "unknown step field `strem` must be rejected (D24 deny_unknown_fields); \
             silently ignoring it disables streaming without any error"
        );
    }

    // ===== Batch 3 (E2/E3/E4/E5) parse-layer tests =====

    /// E2: output omitted → steps.last() (historical semantics).
    #[test]
    fn e2_output_defaults_to_last_step() {
        let yaml = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      inputs: {x: "$s1"}
"#;
        let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
        assert_eq!(plan.output_step, 1);
        assert_eq!(plan.output_field, None);
    }

    /// E2: explicit output selects a named step.
    #[test]
    fn e2_output_selects_named_step() {
        let yaml = r#"
ensemble:
  output: "$s1"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      inputs: {x: "$s1"}
"#;
        let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
        assert_eq!(plan.output_step, 0);
        assert_eq!(plan.output_field, None);
    }

    /// E2: `$stepN.field` Json path.
    #[test]
    fn e2_output_field_path() {
        let yaml = r#"
ensemble:
  output: "$s2.score"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      inputs: {x: "$s1"}
"#;
        let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
        assert_eq!(plan.output_step, 1);
        assert_eq!(plan.output_field.as_deref(), Some("score"));
    }

    /// E2: unknown step / missing `$` prefix / `$request` → load-time config
    /// errors, never a silent fallback.
    #[test]
    fn e2_output_rejects_unknown_step_and_bad_format() {
        for output in ["$nope", "s1", "$request"] {
            let yaml = format!(
                r#"
ensemble:
  output: "{output}"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {{x: "$request"}}
"#
            );
            let res = parse_ensemble_plan(&yaml, &PathBuf::from("/nonexistent/config.yaml"));
            assert!(res.is_err(), "output '{output}' must be a config error");
        }
    }

    /// E2 × D11: with a streaming step, the explicit output must BE that
    /// step (validated at parse — the DAG output IS the stream).
    #[test]
    fn e2_streaming_output_must_point_at_streaming_step() {
        let yaml = r#"
ensemble:
  output: "$s1"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      stream: true
      inputs: {x: "$s1"}
"#;
        let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
        assert!(
            res.is_err(),
            "output pointing away from the streaming step must be rejected (D11)"
        );
    }

    /// E4: version omitted / "latest" → None (execution-time resolution);
    /// explicit versions are kept as-is.
    #[test]
    fn e4_version_optional_and_latest_normalized() {
        let yaml = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "latest"
      inputs: {x: "$s1"}
    - name: s3
      model: m3
      version: "1"
      inputs: {x: "$s2"}
"#;
        let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
        assert_eq!(plan.steps[0].version, None);
        assert_eq!(plan.steps[1].version, None, "\"latest\" == omitted (E4)");
        assert_eq!(plan.steps[2].version.as_deref(), Some("1"));
    }

    /// E3: params parse into the step (assembly applies them in Step 3).
    #[test]
    fn e3_params_parse_into_step() {
        let yaml = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
      params:
        temperature: 0.7
        top_p: 0.9
"#;
        let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
        let params = &plan.steps[0].params;
        assert_eq!(params.get("temperature").and_then(|v| v.as_f64()), Some(0.7));
        assert_eq!(params.get("top_p").and_then(|v| v.as_f64()), Some(0.9));
    }

    /// E5: timeout_secs parses; non-positive / non-finite rejected at load.
    #[test]
    fn e5_timeout_secs_parse_and_validation() {
        let ok = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
      timeout_secs: 2.5
"#;
        let plan = parse_ensemble_plan(ok, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
        assert_eq!(plan.steps[0].timeout_secs, Some(2.5));

        for bad in ["0", "-1", ".nan"] {
            let yaml = format!(
                r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {{x: "$request"}}
      timeout_secs: {bad}
"#
            );
            let res = parse_ensemble_plan(&yaml, &PathBuf::from("/nonexistent/config.yaml"));
            assert!(res.is_err(), "timeout_secs={bad} must be a config error");
        }
    }

    // ===== Batch 3 (E4/D15) snapshot resolution tests =====

    fn snapshot_step(model: &str, version: Option<&str>) -> EnsembleStep {
        EnsembleStep {
            name: "s".to_string(),
            model: model.to_string(),
            version: version.map(|v| v.to_string()),
            inputs: HashMap::new(),
            stream: false,
            params: HashMap::new(),
            timeout_secs: None,
        }
    }

    fn registry_with_active(model: &str, version: &str) -> crate::registry::ModelRegistry {
        let registry = crate::registry::ModelRegistry::new();
        registry.force_pin_active_version(model, version);
        registry
    }

    /// D15: the first resolution for a model wins — a later registry drift
    /// must not change it within the same request.
    #[test]
    fn e4_snapshot_memoizes_first_resolution() {
        let registry = registry_with_active("m", "1");
        let snapshot = VersionSnapshot::default();
        let step = snapshot_step("m", None);
        assert_eq!(snapshot.resolve(&registry, &step).unwrap(), "1");
        // Active drifts to v2 AFTER the first resolution — the snapshot still
        // serves v1 (same-request consistency, D15).
        registry.force_pin_active_version("m", "2");
        assert_eq!(snapshot.resolve(&registry, &step).unwrap(), "1");
    }

    /// E4: explicit versions bypass the snapshot entirely.
    #[test]
    fn e4_explicit_version_bypasses_snapshot() {
        let registry = registry_with_active("m", "1");
        let snapshot = VersionSnapshot::default();
        let step = snapshot_step("m", Some("2"));
        assert_eq!(snapshot.resolve(&registry, &step).unwrap(), "2");
        assert!(
            snapshot.resolved.lock().unwrap().is_empty(),
            "explicit versions must not touch the snapshot"
        );
    }

    /// E4: an unresolved step with no active version is an execution-time
    /// resolution error (ModelNotFound).
    #[test]
    fn e4_unresolved_without_active_version_is_error() {
        let registry = crate::registry::ModelRegistry::new();
        let snapshot = VersionSnapshot::default();
        let step = snapshot_step("m", None);
        assert!(snapshot.resolve(&registry, &step).is_err());
    }

    /// D15: the memoize key is the model — two unresolved steps for the same
    /// model share ONE snapshot entry.
    #[test]
    fn e4_snapshot_keyed_by_model() {
        let registry = registry_with_active("m", "1");
        let snapshot = VersionSnapshot::default();
        let a = snapshot_step("m", None);
        let b = snapshot_step("m", None);
        assert_eq!(snapshot.resolve(&registry, &a).unwrap(), "1");
        assert_eq!(snapshot.resolve(&registry, &b).unwrap(), "1");
        assert_eq!(
            snapshot.resolved.lock().unwrap().len(),
            1,
            "one snapshot entry per model"
        );
    }

    // ===== Batch 3 (E1) nesting tests =====

    /// E1: the ancestor chain detects a self-reference (same model+version
    /// on the active nesting path).
    #[test]
    fn e1_ancestor_chain_detects_self_loop() {
        let chain = vec![("a".to_string(), "1".to_string()), ("b".to_string(), "2".to_string())];
        assert!(contains_ancestor(&chain, "a", "1"));
        assert!(contains_ancestor(&chain, "b", "2"));
        assert!(!contains_ancestor(&chain, "a", "2"));
        assert!(!contains_ancestor(&chain, "other", "1"));
    }

    /// E1: a nested run extends the chain with the child ensemble; the
    /// PARENT chain stays untouched — each branch owns its copy, the chain
    /// is not request-global mutable state.
    #[test]
    fn e1_ancestor_chain_extends_per_branch() {
        let parent = vec![("a".to_string(), "1".to_string())];
        let child = extend_ancestor_chain(&parent, "b", "2");
        assert!(contains_ancestor(&child, "a", "1"));
        assert!(contains_ancestor(&child, "b", "2"));
        assert_eq!(parent.len(), 1, "the parent chain must stay unchanged");
        assert!(!contains_ancestor(&parent, "b", "2"));
    }

    /// E1: `contains_ancestor` answers for the CURRENT branch only — the
    /// chain is immutable per branch (each recursion level extends its own
    /// copy via [`extend_ancestor_chain`]), so a concurrent sibling's
    /// in-flight child run can never appear on this branch's chain. The old
    /// flat shared Vec conflated the two, turning legal same-layer fan-out
    /// (two steps calling one child ensemble; D30 batch elements) into a
    /// spurious "recursion detected" 400 (B1).
    #[test]
    fn e1_sibling_in_flight_child_is_not_recursion() {
        let parent = vec![("p".to_string(), "1".to_string())];
        // Sibling A recurses into the child ensemble and extends ITS branch...
        let _chain_a = extend_ancestor_chain(&parent, "child", "1");
        // ...sibling B checks ITS OWN chain — the sibling's in-flight child
        // run is not an ancestor of B's branch.
        assert!(
            !contains_ancestor(&parent, "child", "1"),
            "a concurrent sibling's in-flight child call is not recursion"
        );
    }

    /// E1: nesting depth limit — depth counts along the call tree; level 0
    /// is the top-level request, so depth 8+ is rejected.
    #[test]
    fn e1_nesting_depth_limit() {
        assert!(ensure_nesting_depth(0).is_ok());
        assert!(ensure_nesting_depth(7).is_ok());
        assert!(ensure_nesting_depth(8).is_err());
    }

    /// E5: `timeout_secs` is parse-legal whenever positive & finite, but the
    /// unix_ns conversion saturates the f64→i64 cast and the `now +` addition
    /// then overflows: debug builds panic (arithmetic overflow), release
    /// wraps negative — every request on that step then fails instantly as
    /// expired. The conversion must clamp instead of overflowing.
    #[test]
    fn e5_huge_timeout_secs_must_not_overflow() {
        // 1e18 s (~3×10^10 years) is absurd but parse-legal (positive, finite).
        let d = step_effective_deadline(None, Some(1e18));
        assert!(
            d.is_some() && d.unwrap() > 0,
            "a huge timeout must clamp to a sane deadline, got {d:?}"
        );
    }

    // ===== Batch 3 (E2/E3) output selection + params tests =====

    /// E2: output_field selection — None passes the whole value through;
    /// Some extracts the field from a Json output.
    #[test]
    fn e2_select_output_field_jsons() {
        let v = EnsembleValue::Json(json!({"score": 0.9, "label": "a"}));
        let whole = select_output_field("s1", v.clone(), None).unwrap();
        assert_eq!(whole, EnsembleValue::Json(json!({"score": 0.9, "label": "a"})));
        let field = select_output_field("s1", v, Some("score")).unwrap();
        assert_eq!(field, EnsembleValue::Json(json!(0.9)));
    }

    /// E2: a missing field is an error (the DAG's contract does not match the
    /// model's output shape).
    #[test]
    fn e2_select_output_field_missing_is_error() {
        let v = EnsembleValue::Json(json!({"score": 0.9}));
        let res = select_output_field("s1", v, Some("label"));
        assert!(res.is_err(), "missing field must error, not default");
    }

    /// E2: field projection on a Binary output is rejected (no field
    /// semantics on bytes — D7's rule applied to the output face).
    #[test]
    fn e2_select_output_field_on_binary_is_error() {
        let v = EnsembleValue::Binary(bytes::Bytes::from_static(b"raw"), "application/octet-stream".to_string());
        let res = select_output_field("s1", v, Some("score"));
        assert!(res.is_err());
    }

    /// E2 × D11: a streaming DAG cannot declare an output FIELD (chunks have
    /// no field semantics) — parse-time rejection.
    #[test]
    fn e2_streaming_output_field_rejected_at_parse() {
        let yaml = r#"
ensemble:
  output: "$s2.score"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      stream: true
      inputs: {x: "$s1"}
"#;
        let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
        assert!(res.is_err(), "field-projected streaming output must be rejected");
    }

    /// E3: params merge into the assembled Json payload AFTER inputs — params
    /// win on key conflicts.
    #[test]
    fn e3_params_merge_into_payload_params_win() {
        let resolved: HashMap<String, EnsembleValue> = [
            ("a".to_string(), EnsembleValue::Json(json!(1))),
            ("b".to_string(), EnsembleValue::Json(json!(2))),
        ]
        .into();
        let params: HashMap<String, Value> = [
            ("b".to_string(), json!(3)),
            ("c".to_string(), json!(4)),
        ]
        .into();
        let (bytes, ct) = assemble_step_payload("s", &resolved, &params).unwrap();
        assert_eq!(ct, None);
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v, json!({"a": 1, "b": 3, "c": 4}), "params override inputs");
    }

    /// E3: Binary assembly has no params semantics — a non-empty params on a
    /// Binary step is rejected at assembly (the earliest point the input type
    /// is decidable).
    #[test]
    fn e3_params_rejected_with_binary_input() {
        let resolved: HashMap<String, EnsembleValue> = [(
            "data".to_string(),
            EnsembleValue::Binary(bytes::Bytes::from_static(b"raw"), "application/octet-stream".to_string()),
        )]
        .into();
        let params: HashMap<String, Value> = [("temperature".to_string(), json!(0.7))].into();
        let res = assemble_step_payload("s", &resolved, &params);
        assert!(res.is_err(), "params × Binary input must be rejected (E3)");
    }

    // ===== Batch 3 (E5) step timeout tests =====

    fn unix_ns_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64
    }

    /// E5: no step timeout → the parent deadline passes through unchanged
    /// (historical behaviour).
    #[test]
    fn e5_step_effective_deadline_passthrough() {
        assert_eq!(step_effective_deadline(Some(1_000_000), None), Some(1_000_000));
        assert_eq!(step_effective_deadline(None, None), None);
    }

    /// E5: a step timeout produces a wall-clock cap of now + timeout_secs.
    #[test]
    fn e5_step_effective_deadline_timeout_cap() {
        let before = unix_ns_now();
        let deadline = step_effective_deadline(None, Some(2.0)).unwrap();
        let after = unix_ns_now() + 2_000_000_000;
        assert!(
            deadline >= before + 2_000_000_000 && deadline <= after,
            "step cap must be ~now + 2s, got {deadline}"
        );
    }

    /// E5: the tighter bound wins — an earlier parent deadline caps the step.
    #[test]
    fn e5_step_effective_deadline_min_with_parent() {
        let parent = unix_ns_now() + 1_000_000_000; // 1s from now
        let deadline = step_effective_deadline(Some(parent), Some(60.0));
        assert_eq!(deadline, Some(parent), "parent deadline must win when earlier");
    }
}
