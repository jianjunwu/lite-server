use crate::error::AppError;
use crate::http::state::AppState;
use crate::proto::liteserver as pb;
use crate::registry::types::ModelType;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

use super::*;

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
    pub(crate) resolved: std::sync::Mutex<HashMap<String, String>>,
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
pub(crate) fn contains_ancestor(chain: &[(String, String)], model: &str, version: &str) -> bool {
    chain.iter().any(|(m, v)| m == model && v == version)
}

/// E1: extend this branch's nesting chain with the current ensemble for the
/// nested run. The parent chain is left untouched — sibling branches built
/// from the same parent stay independent (B1: per-branch chains replace the
/// flat request-global Vec; the version table alone remains shared, D36).
pub(crate) fn extend_ancestor_chain(
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

pub(crate) fn ensure_nesting_depth(depth: u32) -> Result<(), AppError> {
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
pub(crate) fn step_effective_deadline(
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
pub(crate) fn execute_nested_ensemble_boxed(
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
    /// B1 (2026-08-28 audit, cluster A): true iff `deadline_unix_ns` came from
    /// the client (`x-lite-timeout` / `grpc-timeout`). The pre-layer DAG
    /// budget keeps using `deadline_unix_ns` unconditionally (unary fallback
    /// semantics, decided 2026-08-28); a STREAMING step's E5 base is gated on
    /// this flag so the server.timeout fallback never truncates a stream.
    pub deadline_client_specified: bool,
    /// StreamOpen.decoupled passthrough (batch 0).
    pub decoupled: bool,
    /// E8-1 (batch 5, D38): the request's DAG-set name — extracted by each
    /// endpoint from its transport metadata channel (`x-lite-dag`) and
    /// D22-validated; the orchestration layer sees a pure string, never
    /// transport headers.
    pub dag_selector: Option<String>,
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
    /// G1/G3: per-slot in-flight stream count for the tail worker — the
    /// adapter moves this into its forward task so the count releases
    /// exactly when the stream ends (any exit). Counted per DAG node at the
    /// worker-stream open (Q4: budget/drain semantics track worker
    /// invocations, not client streams).
    pub(crate) inflight_guard: Option<crate::streaming::StreamInflightGuard>,
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
    // P10 (§6.3.3): rejection observability needs a request-age baseline.
    let request_start = Instant::now();
    // P0 (D6): plan comes from the cache — parse/validate/layers run once per
    // config version, not per request. In-flight requests hold their Arc and
    // finish on the old plan even across a reload (D23).
    let plan = get_ensemble_plan(&state, model_name, version).await?;
    // §5.5.8 (R15): only the dags form tolerates superset envelopes.
    let is_dags_form = plan.dag_sets.is_some();
    // E8-2 (B6 fix): `$request.dag` must see the RESOLVED set name — with no
    // header the dags form selects "default", so when conditions on the
    // default set must compare against "default", not absence. Normalize
    // before selection so when-eval and select_dag_set agree.
    let mut opts = opts;
    if is_dags_form {
        opts.dag_selector = Some(
            opts.dag_selector
                .take()
                .unwrap_or_else(|| "default".to_string()),
        );
    }
    // E8-1 (D38/D22): resolve the request's DAG set (dags form) — single-
    // form plans pass through; unknown names 400 here.
    let plan = select_dag_set(&plan, opts.dag_selector.as_deref())?;
    let deadline_unix_ns = opts.deadline_unix_ns;
    // B1 (cluster A, decided 2026-08-28): the streaming step's E5 deadline
    // base is CLIENT-gated — the server.timeout fallback still bounds the
    // pre-layers (unary semantics unchanged), but it must never truncate a
    // stream. Every streaming path below (preflight / tail / chain, and the
    // D19 quick-fail predicate inside execute_stream_step) takes this value.
    let stream_deadline_unix_ns = if opts.deadline_client_specified {
        deadline_unix_ns
    } else {
        None
    };
    let tail_idx = plan.output_step;

    // MIMO (D31/R18): the single root-parsing point — the KServe envelope
    // (declared mode) or the legacy payload (byte-identical passthrough).
    let mut context: HashMap<String, EnsembleValue> = HashMap::new();
    let absent_inputs: HashSet<String> = match parse_root_inputs(payload, plan.inputs_decl.as_ref(), is_dags_form)? {
        RootInputs::Single(v) => {
            context.insert("request".to_string(), v);
            HashSet::new()
        }
        RootInputs::Named { values, absent } => {
            for (name, v) in values {
                context.insert(format!("inputs.{name}"), v);
            }
            absent.into_iter().collect()
        }
    };

    if !plan.steps[tail_idx].stream {
        // Historical unary path — byte-identical behaviour (E7: multi-sink
        // responses build from the same context via build_response).
        run_layers(
            &state, &plan, &plan.layers, &mut context, &absent_inputs,
            model_name, version, request_id, &opts, deadline_unix_ns, snapshot, depth, &ancestors,
        ).await?;
        return build_response(&plan, model_name, &context);
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
            &state, &plan, &pre_layers, &mut context, &absent_inputs,
            model_name, version, request_id, &opts, deadline_unix_ns, snapshot, depth, &ancestors,
        )
        .await?;
        // P10 (D40): the permit is WEIGHTED by the chain's streaming-step
        // count (末步 = 1, k 链 = k — residency scales linearly with the
        // chain length). Acquire BEFORE spawning the chain: a 429-rejected
        // request must be side-effect-free on sub-model workers (no worker
        // stream is opened without a permit).
        let permit = match state.worker_manager.streaming_capacity() {
            Some(capacity) => Some(capacity.try_acquire_many(chain.nodes.len()).inspect_err(|_| {
                // §6.3.3: every P10 rejection is observable.
                crate::metrics::prometheus::record_stream_rejected(
                    model_name, version, "4xx", request_start.elapsed().as_secs_f64(),
                    "concurrency_limit",
                );
            })?),
            None => None,
        };
        let mut stream = spawn_chain(
            &state, &plan, chain, &context, request_id, &opts, stream_deadline_unix_ns, snapshot,
        )
        .await?;
        stream.permit = permit;
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
        // B8 (cluster C): the preflight meta's request_id is completed at
        // the call site (`{parent}:{step}`, D20 non-chain format).
        tail_stream_preflight(
            &state, &plan.steps[tail_idx], &format!("{}:{}", request_id, plan.steps[tail_idx].name), &opts, stream_deadline_unix_ns, snapshot,
        )
        .await
    };
    let layers_fut = async {
        run_layers(
            &state, &plan, &plan.layers[..tail_layer], &mut context, &absent_inputs,
            model_name, version, request_id, &opts, deadline_unix_ns, snapshot, depth, &ancestors,
        )
        .await
    };
    let (preflight, layers_res) = tokio::join!(preflight_fut, layers_fut);
    layers_res?;

    // P10 (D40): acquire a streaming-DAG slot BEFORE opening the stream —
    // immediate 429 rejection on exhaustion (no queueing; the pre-layers
    // already waited in the queue once), and a rejected request never opens
    // a sub-model worker stream. The owned permit rides the stream and
    // releases when the adapter's forward task ends (D18 teardown path).
    let permit = match state.worker_manager.streaming_capacity() {
        Some(capacity) => Some(capacity.try_acquire().inspect_err(|_| {
            // §6.3.3: every P10 rejection is observable.
            crate::metrics::prometheus::record_stream_rejected(
                model_name, version, "4xx", request_start.elapsed().as_secs_f64(),
                "concurrency_limit",
            );
        })?),
        None => None,
    };
    let mut stream = open_tail_stream(
        &state, &plan, tail_idx, tail_layer, &context, &absent_inputs,
        request_id, &opts, stream_deadline_unix_ns, preflight?, snapshot, depth, &ancestors,
    ).await?;
    stream.permit = permit;
    Ok(EnsembleOutcome::Stream(stream))
}

/// P3/P11 (batch 6): a layer step future's completion — (step name, result).
pub(crate) type StepFutOutput = (String, Result<StepResults, AppError>);

/// P3/P11 (batch 6): build one step's execution future — the E8-2 when and
/// MIMO R4 conditional-absence checks run up front (a skipped step yields
/// None with a warn); the E4/D15 metric-label resolution runs here too, so
/// a failed resolution aborts the layer (the same error execute_step would
/// surface for that step). The future carries the step's latency recording.
#[allow(clippy::too_many_arguments)] // layer plumbing: state+plan+ctx+ids ride together by design
fn build_step_fut(
    state: &Arc<AppState>,
    plan_run: &Arc<EnsemblePlan>,
    step_idx: usize,
    context: &HashMap<String, EnsembleValue>,
    absent_inputs: &HashSet<String>,
    opts: &EnsembleExecOpts,
    model_name: &str,
    request_id: &str,
    snapshot: &Arc<VersionSnapshot>,
    depth: u32,
    ancestors: &[(String, String)],
    deadline_unix_ns: Option<i64>,
) -> Result<Option<impl Future<Output = StepFutOutput> + 'static>, AppError> {
    // E8-2 (batch 5): a when-false step is skipped (E6 channel).
    if !when_passes(step_idx, plan_run, opts, context)? {
        warn!(
            step = %plan_run.steps[step_idx].name,
            "ensemble step skipped (when condition false)"
        );
        return Ok(None);
    }
    // MIMO R4 (D13): a conditional step whose optional input is absent
    // this request is skipped — the E6-skip channel.
    if plan_run.conditional_refs[step_idx]
        .iter()
        .any(|n| absent_inputs.contains(n))
    {
        warn!(
            step = %plan_run.steps[step_idx].name,
            "ensemble step skipped (optional input absent)"
        );
        return Ok(None);
    }
    let state = state.clone();
    // P1 (batch 6): the step sees only the keys it references.
    let ctx = select_ctx_keys(context, &plan_run.step_dep_keys[step_idx]);
    let step = plan_run.steps[step_idx].clone();
    let plan_spawn = plan_run.clone();
    let ensemble_name = model_name.to_string();
    let request_id = request_id.to_string();
    let client_ip = opts.client_ip.clone();
    // E4/D15: resolve the metric label BEFORE the future runs (a failed
    // resolution aborts the layer here — the same error execute_step
    // would surface for that step). The resolved label is the truthful
    // version the step will call.
    let resolved_label = snapshot.resolve(&state.registry, &step)?;
    // m4: the metric label records the actual version only for EXPLICIT
    // versions — unresolved ("latest"/omitted) normalizes to "latest" so
    // active drift cannot grow the label set (model × step × version).
    let version_label = if step.version.is_some() {
        resolved_label
    } else {
        "latest".to_string()
    };
    let snapshot = snapshot.clone();
    let ancestors = ancestors.to_vec();
    Ok(Some(async move {
        let start = Instant::now();
        let result = execute_step(
            state, &plan_spawn, step_idx, &step, &ctx, &request_id, &client_ip,
            deadline_unix_ns, &snapshot, depth, &ancestors,
        )
        .await;
        let latency = start.elapsed().as_secs_f64();
        crate::metrics::prometheus::record_ensemble_step_latency(
            &ensemble_name, &step.name, &step.model, &version_label, depth, latency,
        );
        (step.name, result)
    }))
}

/// P3/P11 (batch 6): a completed step's result — named outputs land in the
/// context; a skip step's failure leaves it ABSENT and the layer continues
/// (parse rules guarantee nothing references it); any other error
/// propagates (B3 — client errors keep their status codes, no Internal
/// wrapping).
fn handle_step_result(
    name: &str,
    result: Result<StepResults, AppError>,
    context: &mut HashMap<String, EnsembleValue>,
    skip_set: &HashSet<&str>,
) -> Result<(), AppError> {
    match result {
        Ok(values) => {
            // MIMO: materialized named outputs land under their
            // `step.alias` keys (undeclared steps: the single `step` key).
            for (key, value) in values {
                context.insert(key, value);
            }
            Ok(())
        }
        Err(e) => {
            // E6 (batch 4): a skip step's failure leaves it ABSENT from the
            // context and the layer continues (parse rules guarantee
            // nothing references it).
            if skip_set.contains(name) {
                warn!(step = %name, error = %e, "ensemble step skipped (on_error: skip)");
                Ok(())
            } else {
                // B3: propagate step errors directly — client errors (e.g.
                // InvalidRequestBody from resolve_ref E7 rules) must reach
                // the HTTP/gRPC layer with their correct status code, not
                // be wrapped in Internal(500).
                Err(e)
            }
        }
    }
}

/// P11 (batch 6): drive one layer's step futures in-task (zero spawn) — the
/// first failing step propagates immediately and the remaining in-flight
/// futures are dropped with it (the historical JoinSet first-err +
/// drop-abort semantics, P-FLOW §4.0.9 preserved).
pub(crate) async fn drive_step_futs<F>(
    mut futs: FuturesUnordered<F>,
    context: &mut HashMap<String, EnsembleValue>,
    skip_set: &HashSet<&str>,
) -> Result<(), AppError>
where
    F: Future<Output = StepFutOutput>,
{
    while let Some((name, result)) = futs.next().await {
        handle_step_result(&name, result, context, skip_set)?;
    }
    Ok(())
}

/// Layer-barrier executor (historical engine): runs `layers` serially,
/// driving each layer IN-TASK (P3 + P11, batch 6 — zero spawn; the
/// historical per-step tokio::spawn is gone), writing step outputs into
/// `context`. Step errors propagate directly (B3). The whole run is bounded
/// by a single shared deadline (P-DEADLINE §4.0.10): an N-layer ensemble can
/// never exceed the parent; the per-step timeout in execute_step is the
#[allow(clippy::too_many_arguments)] // layer-engine plumbing: state+plan+ctx+ids ride together by design
/// inner safety net, this outer deadline bounds the total.
async fn run_layers(
    state: &Arc<AppState>,
    plan: &Arc<EnsemblePlan>,
    layers: &[Vec<usize>],
    context: &mut HashMap<String, EnsembleValue>,
    absent_inputs: &HashSet<String>,
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
    // Arc clone is a refcount bump — the cached plan is shared, never
    // deep-cloned per request.
    let plan_run = plan.clone();
    // E6 (batch 4): steps allowed to be absent on error (parse rules D5/D34
    // guarantee nothing references them — absence is safe by construction).
    let skip_set: HashSet<&str> = plan
        .steps
        .iter()
        .filter(|s| s.on_error == OnErrorKind::Skip)
        .map(|s| s.name.as_str())
        .collect();
    let ensemble_run = async {
        for layer in layers {
            // P3 + P11 (batch 6): zero-spawn layer driving. A single-step
            // layer is awaited directly (the historical JoinSet spawned a
            // task even here); a multi-step layer is driven in-task via
            // FuturesUnordered. P-FLOW cancel semantics are preserved: on
            // any early exit — a step error, the outer total-budget timeout,
            // or the parent request being dropped (client disconnect) — the
            // layer's in-flight futures are dropped with the executor, so a
            // cancelled ensemble never leaves sub-steps computing on workers.
            if layer.len() == 1 {
                let step_idx = layer[0];
                let Some(fut) = build_step_fut(
                    state, &plan_run, step_idx, context, absent_inputs, opts,
                    model_name, request_id, snapshot, depth, ancestors,
                    deadline_unix_ns,
                )?
                else {
                    continue;
                };
                let (name, result) = fut.await;
                handle_step_result(&name, result, context, &skip_set)?;
                continue;
            }
            let futs: FuturesUnordered<_> = FuturesUnordered::new();
            for &step_idx in layer {
                if let Some(fut) = build_step_fut(
                    state, &plan_run, step_idx, context, absent_inputs, opts,
                    model_name, request_id, snapshot, depth, ancestors,
                    deadline_unix_ns,
                )? {
                    futs.push(fut);
                }
            }
            drive_step_futs(futs, context, &skip_set).await?;
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
pub(crate) async fn open_tail_stream(
    state: &Arc<AppState>,
    plan: &Arc<EnsemblePlan>,
    tail_idx: usize,
    tail_layer: usize,
    context: &HashMap<String, EnsembleValue>,
    absent_inputs: &HashSet<String>,
    request_id: &str,
    opts: &EnsembleExecOpts,
    // B1 (cluster A): the CLIENT-gated deadline base for the streaming step
    // (server.timeout fallback excluded); unary siblings read
    // `opts.deadline_unix_ns` instead.
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

    // B8 (cluster C): the tail's complete request_id (`{parent}:{step}`,
    // D20 non-chain format) is built HERE — execute_stream_step uses it
    // verbatim (single suffix point per path).
    let tail_request_id = format!("{}:{}", request_id, plan.steps[tail_idx].name);
    let run_stream = execute_stream_step(state, plan, tail_idx, context, &tail_request_id, opts, deadline_unix_ns, preflight, snapshot);
    tokio::pin!(run_stream);

    if siblings.is_empty() {
        return run_stream.await;
    }

    let run_siblings = async {
        let mut set: tokio::task::JoinSet<(String, Result<StepResults, AppError>)> =
            tokio::task::JoinSet::new();
        for idx in siblings {
            // E8-2: a when-false sibling is skipped (E6 channel); an eval
            // error is warn + skip (sibling results are dropped anyway).
            match when_passes(idx, plan, opts, context) {
                Ok(true) => {}
                Ok(false) => {
                    warn!(
                        step = %plan.steps[idx].name,
                        "tail-layer sibling skipped (when condition false)"
                    );
                    continue;
                }
                Err(e) => {
                    warn!(step = %plan.steps[idx].name, error = %e, "tail-layer sibling when-eval failed (skipped)");
                    continue;
                }
            }
            // MIMO R4 (D13): conditional siblings with an absent optional
            // input are skipped (same channel as E6-skip).
            if plan.conditional_refs[idx]
                .iter()
                .any(|n| absent_inputs.contains(n))
            {
                warn!(
                    step = %plan.steps[idx].name,
                    "tail-layer sibling skipped (optional input absent)"
                );
                continue;
            }
            let state = state.clone();
            // P1 (batch 6): the sibling sees only its referenced keys.
            let ctx = select_ctx_keys(context, &plan.step_dep_keys[idx]);
            let step = plan.steps[idx].clone();
            let plan_spawn = plan.clone();
            let request_id = request_id.to_string();
            let client_ip = opts.client_ip.clone();
            let opts_deadline_unix_ns = opts.deadline_unix_ns;
            let snapshot = snapshot.clone();
            let ancestors = ancestors.to_vec();
            set.spawn(async move {
                let name = step.name.clone();
                // Siblings are UNARY steps — they keep the ungated deadline
                // (server.timeout fallback is established unary semantics,
                // B1/cluster A); the gated param belongs to the stream step.
                execute_step(state, &plan_spawn, idx, &step, &ctx, &request_id, &client_ip, opts_deadline_unix_ns, &snapshot, depth, &ancestors).await
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
pub(crate) struct TailPreflight {
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
    // B8 (cluster C): request_id arrives COMPLETE from the caller (the
    // execute_ensemble_inner preflight call site suffixes `{parent}:{step}`).
    let preflight_deadline = step_effective_deadline(deadline_unix_ns, step.timeout_secs);
    let meta = build_step_meta(
        request_id, &opts.client_ip, preflight_deadline, step_headers, bytes::Bytes::new(),
    );
    let outlier = state.worker_manager.get_outlier_state(&step.model, &resolved_version).await;
    let seq_registry = state.inference_queue.sequence_registry();
    let worker_id = crate::worker::pick_streaming_worker(
        &meta, mv.workers.len(), outlier.as_deref(), seq_registry, &step.model, &resolved_version,
    ).map_err(|e| match e {
        crate::worker::PickError::InvalidPin(msg) => AppError::Validation(msg),
        crate::worker::PickError::NoLiveWorkers(msg) => AppError::WorkerCrashed(msg),
        crate::worker::PickError::WorkerRecycling(msg) => AppError::WorkerRecycling(msg),
    })?;
    if worker_id >= clients.len() {
        return Err(AppError::WorkerCrashed("invalid worker index".to_string()));
    }
    Ok(Some(TailPreflight { meta, worker_id, clients }))
}

/// B7/O6 (cluster C): how a chain hop's forward ended — the consumer's next
/// action differs per outcome (stop the chain on error, continue with the
/// next chunk on done, unwind immediately on downstream close).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForwardOutcome {
    /// An Error frame terminated the chain (node failure or hop bound).
    Error,
    /// The sub-stream finished cleanly (Done frame / upstream channel close).
    Done,
    /// The downstream receiver is gone — the hop's worker was already
    /// cancelled inside forward_stream; the consumer unwinds immediately
    /// instead of opening a new sub-stream per remaining chunk (O6 churn).
    DownstreamClosed,
}

/// §4.2: forward a worker stream into a downstream channel. Only Chunk
/// frames pass through — a per-chunk sub-stream's Done is an intermediate
/// milestone, NOT the chain's end (the tail hop synthesizes the final Done);
/// an Error frame passes through and terminates the whole chain (a node
/// failure cancels its worker and propagates upstream, D18). When the
/// downstream closes (adapter disconnect / next hop exiting), send
/// StreamCancel to THIS worker and end.
/// D35 × B7 (cluster C): BOTH directions carry the two-stage bound — recv
/// via [`streaming::recv_chunk`], send via [`streaming::send_bounded`] —
/// with `deadline` = this hop's wall-clock cap (min(client overall, open +
/// timeout_secs), GAP-7) and `idle` = the always-on chunk idle budget. A
/// stalled hop (silent upstream OR backpressured downstream) is reclaimed by
/// THIS hop inside min(deadline, idle): an Error frame is try_send'd
/// downstream (best-effort — a full channel means the peer already stalled;
/// dropping matches the overflow-truncation philosophy) and the hop's worker
/// is cancelled. No send blocks unbounded (P10 permits / inflight counts /
/// the chain task tree all release along the error-terminated path).
async fn forward_stream(
    mut rx: mpsc::Receiver<pb::StreamResponse>,
    tx: mpsc::Sender<pb::StreamResponse>,
    cancel_client: Arc<crate::transport::zmq::WorkerZmqClient>,
    stream_id: String,
    deadline: Option<Instant>,
    idle: Option<Duration>,
    step_name: &str,
) -> ForwardOutcome {
    // m4: cumulative time this hop's downstream channel was FULL (the 64-slot
    // bound is backpressure, not a drop — saturation measures how long it held).
    let mut saturation_secs: f64 = 0.0;
    // B7: locally-generated terminal Error frame — best-effort try_send (a
    // full channel means the peer stalled; never block a dying hop on it).
    let try_send_hop_error = |tx: &mpsc::Sender<pb::StreamResponse>, msg: String| {
        let _ = tx.try_send(pb::StreamResponse {
            payload: Some(pb::stream_response::Payload::Error(pb::StreamError {
                message: msg,
            })),
            ..Default::default()
        });
    };
    loop {
        let next = match crate::streaming::recv_chunk(&mut rx, deadline, idle).await {
            Ok(v) => v,
            Err(elapsed) => {
                // D35: hop bound expired mid-stream — Error frame downstream
                // + cancel this hop's worker (§4.4: every mid-stream failure
                // reaches the client as an Error frame).
                let msg = match elapsed {
                    crate::streaming::RecvElapsed::Deadline => format!(
                        "pipeline step '{step_name}' exceeded its timeout (D35)"
                    ),
                    crate::streaming::RecvElapsed::Idle => format!(
                        "pipeline step '{step_name}' idle timeout"
                    ),
                };
                try_send_hop_error(&tx, msg);
                let _ = cancel_client
                    .send_raw(crate::streaming::build_stream_cancel(stream_id))
                    .await;
                crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
                    saturation_secs,
                );
                return ForwardOutcome::Error;
            }
        };
        let Some(chunk) = next else { break };
        match chunk.payload {
            Some(pb::stream_response::Payload::Chunk(_)) => {
                let send_start = Instant::now();
                let outcome = crate::streaming::send_bounded(deadline, idle, tx.send(chunk)).await;
                saturation_secs += send_start.elapsed().as_secs_f64();
                match outcome {
                    crate::streaming::SendOutcome::Sent(Ok(())) => {}
                    crate::streaming::SendOutcome::Sent(Err(_)) => {
                        let _ = cancel_client
                            .send_raw(crate::streaming::build_stream_cancel(stream_id))
                            .await;
                        crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
                            saturation_secs,
                        );
                        return ForwardOutcome::DownstreamClosed;
                    }
                    crate::streaming::SendOutcome::Deadline => {
                        // B7: the send must not escape the hop bound — a
                        // backpressured downstream previously parked the hop
                        // in an unbounded send, defeating the deadline.
                        try_send_hop_error(&tx, format!(
                            "pipeline step '{step_name}' exceeded its timeout (D35)"
                        ));
                        let _ = cancel_client
                            .send_raw(crate::streaming::build_stream_cancel(stream_id))
                            .await;
                        crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
                            saturation_secs,
                        );
                        return ForwardOutcome::Error;
                    }
                    crate::streaming::SendOutcome::Idle => {
                        try_send_hop_error(&tx, format!(
                            "pipeline step '{step_name}' idle timeout"
                        ));
                        let _ = cancel_client
                            .send_raw(crate::streaming::build_stream_cancel(stream_id))
                            .await;
                        crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
                            saturation_secs,
                        );
                        return ForwardOutcome::Error;
                    }
                }
            }
            Some(pb::stream_response::Payload::Error(_)) => {
                // Propagate the failure downstream, then stop the chain.
                // Bounded like the chunk path (B7): a stalled downstream
                // must not pin a dying hop either.
                let send_start = Instant::now();
                let _ = crate::streaming::send_bounded(deadline, idle, tx.send(chunk)).await;
                saturation_secs += send_start.elapsed().as_secs_f64();
                crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
                    saturation_secs,
                );
                return ForwardOutcome::Error;
            }
            Some(pb::stream_response::Payload::Done(_)) => {
                crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
                    saturation_secs,
                );
                return ForwardOutcome::Done;
            }
            _ => {}
        }
    }
    crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
        saturation_secs,
    );
    ForwardOutcome::Done
}

/// §4.2 / C5: a pipeline chunk is spliced RAW into the downstream step's
/// JSON payload (P2's zero-re-serialize path), so it must be well-formed
/// JSON — validate with a borrowed RawValue parse (no Value materialized).
/// A garbage/binary chunk fails the chain with the historical error.
pub(crate) fn validate_pipeline_chunk(prev_step_name: &str, data: &[u8]) -> Result<(), AppError> {
    serde_json::from_slice::<&serde_json::value::RawValue>(data)
        .map(|_| ())
        .map_err(|e| {
            AppError::Internal(format!(
                "pipeline chunk from '{prev_step_name}' is not valid JSON: {e}"
            ))
        })
}

/// §4.2: a pipeline chain consumer — one nested send_stream sub-call PER
/// upstream chunk (chunk → 组包 → sub-stream → forward). D20: each sub-call
/// carries request_id `{parent}:{step}:{chunk_seq}`. D18: a failed
/// downstream send cancels this step's worker; the chain-handle list is
/// updated per sub-stream (tail inserted at index 0, others appended) and
/// each sub-stream's handle is removed when it terminates (L1).
/// `upstream_handle_id` is the head stream's handle id — passed only to the
/// FIRST consumer, which drops it once the upstream head has terminated
/// (Done/terminal Error/channel close); a consumer-side bail leaves the
/// head live and its handle cancel-able via [`cancel_chain`].
#[allow(clippy::too_many_arguments)] // chain-hop plumbing: state+plan+ctx+ids ride together by design
pub(crate) async fn consume_stream_consumer(
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
    upstream_handle_id: Option<String>,
    is_tail: bool,
    snapshot: &Arc<VersionSnapshot>,
) -> Result<(), AppError> {
    let step = &plan.steps[step_idx];
    // P1 (batch 6): the per-chunk context clone selects only this step's
    // referenced keys (parse-computed) — the whole-table clone per chunk
    // is gone; the upstream chunk value is inserted per chunk below.
    let base_ctx = select_ctx_keys(context, &plan.step_dep_keys[step_idx]);
    let mut seq: u64 = 0;
    let mut error_terminated = false;
    // L1: set when the upstream head stream has TERMINATED in-band — a Done
    // or Error frame (both are terminal per §4.4). Only then may its handle
    // be dropped; a consumer-side bail (validation/open failure below, or
    // the early return on sub-stream error) leaves the head live and its
    // handle must stay cancel-able via cancel_chain.
    let mut upstream_terminal = false;
    while let Some(chunk) = upstream.recv().await {
        match &chunk.payload {
            Some(pb::stream_response::Payload::Chunk(c)) => {
                // O6 (cluster C): nobody consumes this chain anymore — unwind
                // instead of opening another sub-stream for this chunk (the
                // mid-sub-stream case is forward_stream's DownstreamClosed;
                // the head stays cancel-able via cancel_chain, same as any
                // consumer-side bail).
                if downstream.is_closed() {
                    return Ok(());
                }
                // Chunk → the previous step's value in a per-chunk context.
                // P2 (batch 6): the chunk stays unparsed raw bytes (P-R2
                // guarantees whole-reference-only consumption — the downstream
                // assembly splices the original bytes, no per-chunk parse).
                // C5: raw splicing requires well-formed JSON — validate.
                if let Err(e) = validate_pipeline_chunk(prev_step_name, &c.data) {
                    // §4.4: a mid-stream failure must reach the client as an
                    // Error frame. Propagating Err here would drop the
                    // downstream sender — a channel close reads as NORMAL
                    // completion to the next hop (which then synthesizes a
                    // Done), turning the failure into a silent clean EOF.
                    let _ = downstream
                        .send(pb::StreamResponse {
                            stream_id: String::new(),
                            payload: Some(pb::stream_response::Payload::Error(pb::StreamError {
                                message: e.to_string(),
                            })),
                        })
                        .await;
                    error_terminated = true;
                    break;
                }
                let mut ctx = base_ctx.clone();
                ctx.insert(
                    prev_step_name.to_string(),
                    EnsembleValue::RawJson(Arc::new(RawJsonValue::new(c.data.clone()))),
                );
                let sub_request_id = format!("{}:{}:{}", request_id, step.name, seq);
                let sub = match execute_stream_step(
                    state, plan, step_idx, &ctx, &sub_request_id, opts, deadline_unix_ns, None,
                    snapshot,
                )
                .await
                {
                    Ok(sub) => sub,
                    Err(e) => {
                        // §4.4: same contract as the validation arm above —
                        // the open failure becomes an Error frame, never a
                        // silent clean EOF.
                        let _ = downstream
                            .send(pb::StreamResponse {
                                stream_id: String::new(),
                                payload: Some(pb::stream_response::Payload::Error(pb::StreamError {
                                    message: e.to_string(),
                                })),
                            })
                            .await;
                        error_terminated = true;
                        break;
                    }
                };
                {
                    let mut handles = chain_handles.lock().unwrap_or_else(|e| e.into_inner());
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
                // D35 × GAP-7 (cluster C): the hop's wall-clock cap =
                // min(client overall [client-gated, cluster A], open +
                // timeout_secs) — mid hops no longer see only timeout_secs;
                // a tighter client overall deadline bounds every hop.
                let hop_deadline = crate::deadline::min_instant(
                    crate::deadline::to_instant(deadline_unix_ns),
                    step.timeout_secs.and_then(|t| {
                        Instant::now().checked_add(Duration::try_from_secs_f64(t).ok()?)
                    }),
                );
                // B7 (cluster C): the hop's chunk idle budget — the same
                // always-on two-stage reclaim the adapters run.
                let hop_idle = crate::deadline::idle_budget(
                    state.config.server.decoupled_idle_timeout_secs,
                );
                let outcome = forward_stream(
                    sub.chunk_rx,
                    downstream.clone(),
                    sub.cancel_client.clone(),
                    sub.stream_id.clone(),
                    hop_deadline,
                    hop_idle,
                    &step.name,
                )
                .await;
                // L1: the sub-stream has terminated (done/error/downstream
                // closed) — drop its handle so `chain_handles` tracks only
                // in-flight streams instead of accumulating O(chunks) over
                // the chain's lifetime.
                remove_chain_handle(chain_handles, &sub.stream_id);
                match outcome {
                    ForwardOutcome::Error => {
                        // A node failed: cancel its worker and stop consuming
                        // upstream — the upstream sender drops with us (D18).
                        let _ = sub.cancel_client
                            .send_raw(crate::streaming::build_stream_cancel(sub.stream_id.clone()))
                            .await;
                        return Ok(());
                    }
                    ForwardOutcome::DownstreamClosed => {
                        // O6 (cluster C): the downstream is gone — this hop's
                        // worker was already cancelled inside forward_stream
                        // and the upstream unwinds along the D18 channel-close
                        // chain. Do NOT open a new sub-stream per remaining
                        // chunk (the churn burned a worker open + G4 permit +
                        // inflight slot per chunk until the upstream ran dry).
                        return Ok(());
                    }
                    ForwardOutcome::Done => {}
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
                upstream_terminal = true;
                break;
            }
            _ => {}
        }
    }
    // L1: the upstream head terminated (Done/terminal Error above, or a
    // channel close — `!error_terminated` covers the while-let None exit)
    // → its handle is no longer a cancel target; drop it.
    if upstream_terminal || !error_terminated {
        if let Some(id) = &upstream_handle_id {
            remove_chain_handle(chain_handles, id);
        }
    }
    // Upstream exhausted (Done or channel close) = the chain finished
    // normally — synthesize the terminal Done downstream (P0-1). The tail's
    // Done fires the adapter's close收口 with a clean reason (the per-chunk
    // sub-stream Dones were never forwarded); a mid hop's Done terminates the
    // next hop promptly instead of hanging its recv on channel teardown.
    if !error_terminated {
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

/// L1: drop a terminated sub-stream's handle from the chain list (called at
/// every sub-stream termination exit: done/error/cancel/downstream close).
fn remove_chain_handle(
    chain_handles: &Arc<std::sync::Mutex<Vec<StreamHandle>>>,
    stream_id: &str,
) {
    chain_handles
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .retain(|h| h.stream_id != stream_id);
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
    plan: &Arc<EnsemblePlan>,
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
    // B8 (cluster C): the head's request_id (`{parent}:{step}`) is completed
    // at the call site — chain consumers then extend it per chunk
    // (`{parent}:{step}:{seq}`).
    let head_stream = execute_stream_step(
        state, plan, nodes[0], context, &format!("{}:{}", request_id, plan.steps[nodes[0]].name), opts, deadline_unix_ns, None, snapshot,
    )
    .await?;
    {
        // D18: record the head handle (appended; the tail lands at index 0).
        let mut h = chain_handles.lock().unwrap_or_else(|e| e.into_inner());
        h.push(StreamHandle {
            stream_id: head_stream.stream_id.clone(),
            cancel_client: Arc::clone(&head_stream.cancel_client),
            abort: tokio::spawn(async {}).abort_handle(),
        });
    }
    // L1: the first consumer drops the head's handle once the head stream
    // terminates (its chunk channel reports Done/Error/close).
    let head_handle_id = head_stream.stream_id.clone();
    // The top-level cancel_client is the head stream's client (synchronously
    // available) — adapters that only read the quick-access fields stay
    // unchanged; chain cancellation broadcasts over `chain` via
    // [`cancel_ensemble_stream`].
    let cancel_client = {
        let handles = chain_handles.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(&handles[0].cancel_client)
    };

    let state_h = state.clone();
    let plan_h = plan.clone();
    // P1 (batch 6): per-hop context subsets — each hop resolves only its
    // own step's refs; the whole-table clone per hop task is gone.
    let mut hop_ctxs: Vec<HashMap<String, EnsembleValue>> = nodes
        .iter()
        .skip(1)
        .map(|&node| select_ctx_keys(context, &plan.step_dep_keys[node]))
        .collect();
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
        // G1/G3: the head stream outlives spawn_chain (its chunks flow into
        // the first consumer) — move its in-flight count into that consumer,
        // or the count would release here while the stream is still running.
        let mut head_inflight = Some(head_stream.inflight_guard);
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
            let ctx = hop_ctxs.remove(0);
            let req = request_id_h.clone();
            let opts = opts_h.clone();
            let handles = handles_h.clone();
            let snapshot = snapshot_h.clone();
            let prev_name = plan_h.steps[nodes[i - 1]].name.clone();
            // L1: only the first consumer owns the head handle's cleanup.
            let upstream_handle_id = if i == 1 {
                Some(head_handle_id.clone())
            } else {
                None
            };
            // G1/G3: the first consumer also holds the head stream's count.
            let upstream_inflight = if i == 1 {
                head_inflight.take().flatten()
            } else {
                None
            };
            tasks.push(tokio::spawn(async move {
                let _upstream_inflight = upstream_inflight;
                consume_stream_consumer(
                    &state, &plan, node, &prev_name, up_rx, down_tx, &ctx, &req, &opts,
                    deadline_unix_ns, &handles, upstream_handle_id, is_tail, &snapshot,
                )
                .await
            }));
        }
        // P0-1: drop the original inter-hop senders before joining — each
        // consumer owns its clone; holding the originals until join keeps the
        // channels open after the mids exit and hangs the tail's recv.
        drop(hop_txs);
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
        // D35 × §4.2: the chain tail's E5 timeout_secs is a wall-clock cap
        // measured from the chain spawn (same formula as the single-tail
        // branch) — the adapters' recv_chunk overall takes min(client
        // overall, this). Mid-chain hops enforce their own caps in
        // forward_stream.
        step_deadline: tail.timeout_secs.and_then(|t| {
            Instant::now().checked_add(Duration::try_from_secs_f64(t).ok()?)
        }),
        chain: chain_handles,
        // D18: the chain task tree's root handle — the adapter aborts it on
        // disconnect as the teardown backstop.
        abort: root.abort_handle(),
        permit: None,
        // Synthetic chain wrapper — no worker stream of its own; each hop's
        // count lives in its consumer (head) or its `sub` stream (mid/tail).
        inflight_guard: None,
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
                let handles = chain.lock().unwrap_or_else(|e| e.into_inner());
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
pub(crate) async fn ensure_sub_model_loaded(
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
        match resolve_ref(plan, ref_str, context)? {
            ResolvedRef::Value(v) => {
                resolved.insert(key.clone(), v);
            }
            ResolvedRef::Absent(name) => {
                // Streaming steps cannot be conditional (D34) — reaching
                // here is an internal inconsistency.
                return Err(AppError::Internal(format!(
                    "streaming step '{}' resolved absent input '{}' (D34 violation)",
                    step.name, name
                )));
            }
        }
    }
    let (payload_bytes, content_type_for_step) = assemble_step_payload(
        &step.name, &resolved, &step.params, plan.input_mode(step_idx),
    )?;

    // P7: when the preflight already routed/built meta in parallel with the
    // pre-layers, skip the serial readiness/meta/pick block.
    let (mut meta, worker_id, clients) = match preflight {
        Some(pf) => (pf.meta, pf.worker_id, pf.clients),
        None => {
            // D19: ensure the sub-model is loaded, then poll STREAMING
            // readiness (pick non-empty) with backoff. A pick failure is a
            // retryable state.
            ensure_sub_model_loaded(state, &step.model, &resolved_version).await?;
            let mv = state.registry.get(&step.model, Some(&resolved_version))
                .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", step.model, resolved_version)))?;
            // E1/D4: ensembles have no streaming form (zero workers) — a
            // streaming step calling one is an unsupported combination, not a
            // readiness problem. Fail fast BEFORE the D19 poll (B5,
            // 2026-08-28 audit: the poll can never succeed for a workerless
            // ensemble, so a check behind it was unreachable — every such
            // request wasted a full backoff round and got the wrong status).
            if mv.model_type == ModelType::Ensemble {
                return Err(AppError::InvalidRequestBody(format!(
                    "streaming step '{}' calls ensemble model '{}' — ensembles have no \
                     streaming form (D4)",
                    step.name, step.model
                )));
            }
            let mut retries = 0;
            let max_retries = 30;
            let mut delay = Duration::from_millis(50);
            while !streaming_worker_ready(state, &step.model, &resolved_version, &pb::RequestMeta::default()).await {
                // Crash-death gate: a sub-model whose workers have all
                // exited can never become ready — fail fast instead of
                // spinning the D19 poll to exhaustion.
                if let Some(o) = state
                    .worker_manager
                    .get_outlier_state(&step.model, &resolved_version)
                    .await
                {
                    if o.all_dead() {
                        return Err(AppError::WorkerCrashed(format!(
                            "ensemble step {}: all workers for sub-model {} v{} have exited",
                            step.name, step.model, resolved_version
                        )));
                    }
                }
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
            // B8 (cluster C): the request_id arrives COMPLETE from the caller
            // (single suffix point — tail: `{parent}:{step}` at the
            // open_tail_stream call site; chain: `{parent}:{step}:{seq}` in
            // consume_stream_consumer) — this function never re-suffixes.
            let meta = build_step_meta(
                request_id, &opts.client_ip, step_deadline, step_headers, bytes::Bytes::new(),
            );

            let outlier = state.worker_manager.get_outlier_state(&step.model, &resolved_version).await;
            let seq_registry = state.inference_queue.sequence_registry();
            let worker_id = crate::worker::pick_streaming_worker(
                &meta, mv.workers.len(), outlier.as_deref(), seq_registry, &step.model, &resolved_version,
            ).map_err(|e| match e {
        crate::worker::PickError::InvalidPin(msg) => AppError::Validation(msg),
        crate::worker::PickError::NoLiveWorkers(msg) => AppError::WorkerCrashed(msg),
        crate::worker::PickError::WorkerRecycling(msg) => AppError::WorkerRecycling(msg),
    })?;
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

    // E6 (batch 4, D35): streaming retry is BUILD-WINDOW limited — the
    // window is send_stream → first non-Error frame; a first-frame Error /
    // build timeout / pre-frame close rebuilds with backoff and a re-pick
    // (can land on a healthy instance), each attempt a fresh stream_id.
    // Chunk/Start/Done commits the stream (no replay — P5/§6.3). retries == 0
    // keeps the historical no-peek path byte-for-byte.
    // G4: stream concurrency cap for this DAG node's model version — taken
    // BEFORE any worker stream opens so a capacity rejection has no side
    // effect to roll back (grpc/rpc/stream.rs:234 "rejected pre-open"
    // parity). One permit per logical stream, held across retry attempts.
    let permit = state
        .inference_queue
        .try_acquire_stream_permit(&step.model, &resolved_version)?;
    let (chunk_rx, stream_id, cancel_client, wrapper_abort, inflight_guard) = if step.retries == 0 {
        let client = &clients[worker_id];
        let stream_id = format!("stream-{}", Uuid::new_v4());
        let open_req = crate::streaming::build_stream_open(
            stream_id.clone(), payload_bytes.clone(), Some(meta.clone()), opts.decoupled,
        );
        let chunk_rx = client.send_stream(open_req, stream_id.clone()).await?;
        // G3: count the stream toward the slot's max_requests budget (per DAG node).
        state.inference_queue.record_stream_served(&step.model, &resolved_version, worker_id);
        // G1/G3: count the in-flight stream on its slot (per DAG node).
        let guard = state
            .worker_manager
            .get_outlier_state(&step.model, &resolved_version)
            .await
            .map(|o| crate::streaming::StreamInflightGuard::new(o, worker_id).with_permit(permit));
        (chunk_rx, stream_id, Arc::clone(client), None, guard)
    } else {
        open_stream_with_retry(
            state, step, &resolved_version, payload_bytes.clone(), &meta, opts,
            step_deadline, &clients, worker_id, step.retries, permit,
        )
        .await?
    };

    // D25: chain handles — batch 0 = the tail stream itself as the single
    // element (chain[0] === the top-level fields; adapters never read chain).
    // E6: with retries a first-frame forwarder runs — its AbortHandle is the
    // teardown point (aborting it drops the receiver, worker idle reclaim).
    let abort = wrapper_abort.unwrap_or_else(|| tokio::spawn(async {}).abort_handle());
    let chain = vec![StreamHandle {
        stream_id: stream_id.clone(),
        cancel_client: Arc::clone(&cancel_client),
        abort: abort.clone(),
    }];

    Ok(EnsembleStream {
        chunk_rx,
        stream_id,
        cancel_client,
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
        inflight_guard,
    })
}

/// E6 (batch 4, D35): open the step's worker stream under the build-window
/// retry contract. The window closes when a Chunk/Start/Done frame arrives
/// (committed) — a first-frame Error, a build timeout, a pre-frame channel
/// close, or a failing `send_stream` call rebuilds with exponential backoff
/// and a FRESH worker pick (a retry can land on a healthy instance; the old
/// stream is cancelled so it never leaks). On exhaustion the committed path
/// still runs — the first Error frame returns in-stream (the adapter emits
/// the Error frame + close, §4.4) and a build timeout hands the raw receiver
/// to the adapter (its recv_chunk bounds take over). Because the peek
/// consumes the first frame, a committed stream is wrapped by a zero-copy
/// forwarder that re-emits it first — the adapter forward loops stay
/// unchanged (D25 seam).
#[allow(clippy::too_many_arguments)] // stream-open plumbing: state+meta+clients ride together by design
async fn open_stream_with_retry(
    state: &Arc<AppState>,
    step: &EnsembleStep,
    resolved_version: &str,
    payload_bytes: bytes::Bytes,
    meta: &pb::RequestMeta,
    opts: &EnsembleExecOpts,
    step_deadline: Option<i64>,
    clients: &[Arc<crate::transport::zmq::WorkerZmqClient>],
    mut worker_id: usize,
    retries: u32,
    // G4: acquired by the caller BEFORE any open (capacity rejection must be
    // side-effect free); attached to the committed attempt's guard.
    permit: Option<crate::inference_queue::StreamPermit>,
) -> Result<
    (
        mpsc::Receiver<pb::StreamResponse>,
        String,
        Arc<crate::transport::zmq::WorkerZmqClient>,
        Option<tokio::task::AbortHandle>,
        Option<crate::streaming::StreamInflightGuard>,
    ),
    AppError,
> {
    let mut permit = permit;
    let mut attempt: u32 = 0;
    let mut backoff = Duration::from_millis(50);
    loop {
        // B6 (cluster C, D35): the first-frame peek bound is recomputed PER
        // ATTEMPT from the step deadline's CURRENT remaining budget — the
        // send/peek/backoff of earlier attempts already consumed part of the
        // window, so a per-entry bound would let the last attempt overshoot
        // the step wall-clock cap. With no deadline the idle budget is the
        // escape hatch (same as the adapters' recv_chunk: 0 = none).
        let peek_bound = crate::deadline::remaining(step_deadline).or_else(|| {
            crate::deadline::idle_budget(state.config.server.decoupled_idle_timeout_secs)
        });
        let stream_id = format!("stream-{}", Uuid::new_v4());
        let client = &clients[worker_id];
        let open_req = crate::streaming::build_stream_open(
            stream_id.clone(), payload_bytes.clone(), Some(meta.clone()), opts.decoupled,
        );
        // E6/D35: the window starts AT send_stream — a build failure is
        // retryable (worker crash race), no 4xx/5xx distinction on the
        // streaming wire (there are no status codes).
        let mut rx = match client.send_stream(open_req, stream_id.clone()).await {
            Ok(rx) => rx,
            Err(e) => {
                if attempt < retries && retry_sleep_budget(step_deadline, backoff).is_some() {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_millis(500));
                    attempt += 1;
                    worker_id = repick_streaming_worker(state, step, resolved_version, meta, clients).await?;
                    continue;
                }
                return Err(e);
            }
        };
        // G3: count this attempt toward the slot's max_requests budget.
        state.inference_queue.record_stream_served(&step.model, resolved_version, worker_id);
        // G1/G3: count this attempt's stream on its slot. Retry continues
        // cancel the stream and drop the guard with it; the committed
        // attempt's guard moves into the first-frame forwarder (or rides the
        // returned tuple when no forwarder runs). The G4 permit is attached
        // only at the commit points below.
        let attempt_guard = state
            .worker_manager
            .get_outlier_state(&step.model, resolved_version)
            .await
            .map(|o| crate::streaming::StreamInflightGuard::new(o, worker_id));

        // Peek the first frame (bounded). Committed frames: Chunk/Start/Done.
        let first = match peek_bound {
            Some(b) => match tokio::time::timeout(b, rx.recv()).await {
                Ok(f) => f,
                Err(_) => {
                    // Build timeout — retry; exhausted → hand the raw
                    // receiver to the adapter (its recv_chunk bounds rule).
                    if attempt < retries && retry_sleep_budget(step_deadline, backoff).is_some() {
                        let _ = client
                            .send_raw(crate::streaming::build_stream_cancel(stream_id))
                            .await;
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_millis(500));
                        attempt += 1;
                        worker_id = repick_streaming_worker(state, step, resolved_version, meta, clients).await?;
                        continue;
                    }
                    return Ok((rx, stream_id, Arc::clone(client), None, attempt_guard.map(|g| g.with_permit(permit.take()))));
                }
            },
            None => rx.recv().await,
        };

        let first_is_error = matches!(
            &first,
            Some(f) if matches!(&f.payload, Some(pb::stream_response::Payload::Error(_)))
        );
        let first_is_none = first.is_none();
        if (first_is_error || first_is_none) && attempt < retries {
            // First-frame Error / pre-frame close (worker died) — retryable.
            if retry_sleep_budget(step_deadline, backoff).is_some() {
                let _ = client
                    .send_raw(crate::streaming::build_stream_cancel(stream_id))
                    .await;
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_millis(500));
                attempt += 1;
                worker_id = repick_streaming_worker(state, step, resolved_version, meta, clients).await?;
                continue;
            }
        }

        // Committed (Chunk/Start/Done, or Error/close with retries
        // exhausted): preserve the consumed first frame behind a zero-copy
        // forwarder — capacity aligned with STREAM_CHANNEL_SIZE (D2).
        let (tx, out_rx) = mpsc::channel(64);
        let task = tokio::spawn(async move {
            // The in-flight count releases when this forwarder ends — the
            // stream is over by then (rx closed or the consumer gone).
            let _attempt_guard = attempt_guard.map(|g| g.with_permit(permit.take()));
            if let Some(f) = first {
                if tx.send(f).await.is_err() {
                    return;
                }
            }
            while let Some(f) = rx.recv().await {
                if tx.send(f).await.is_err() {
                    return;
                }
            }
        });
        return Ok((out_rx, stream_id, Arc::clone(client), Some(task.abort_handle()), None));
    }
}

/// E6 (batch 4): re-pick a streaming worker for a retry attempt — the same
/// pick path as the serial open (fresh pick can land on a healthy instance
/// after a crash race).
async fn repick_streaming_worker(
    state: &Arc<AppState>,
    step: &EnsembleStep,
    resolved_version: &str,
    meta: &pb::RequestMeta,
    clients: &[Arc<crate::transport::zmq::WorkerZmqClient>],
) -> Result<usize, AppError> {
    let mv = state.registry.get(&step.model, Some(resolved_version)).ok_or_else(|| {
        AppError::ModelNotFound(format!("{} version {}", step.model, resolved_version))
    })?;
    let outlier = state.worker_manager.get_outlier_state(&step.model, resolved_version).await;
    let seq_registry = state.inference_queue.sequence_registry();
    let worker_id = crate::worker::pick_streaming_worker(
        meta, mv.workers.len(), outlier.as_deref(), seq_registry, &step.model, resolved_version,
    )
    .map_err(|e| match e {
        crate::worker::PickError::InvalidPin(msg) => AppError::Validation(msg),
        crate::worker::PickError::NoLiveWorkers(msg) => AppError::WorkerCrashed(msg),
        crate::worker::PickError::WorkerRecycling(msg) => AppError::WorkerRecycling(msg),
    })?;
    if worker_id >= clients.len() {
        return Err(AppError::WorkerCrashed("invalid worker index".to_string()));
    }
    Ok(worker_id)
}

#[cfg(test)]
mod audit_0822_tests {
    //! Audit (2026-08-22, resource-leak/observability sweep) evidence tests.
    use super::*;
    use crate::callback::CallbackRunner;
    use crate::proto::liteserver as pb;
    use prost::Message;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::time::Duration;

    fn audit_state() -> Arc<AppState> {
        let cb = Arc::new(CallbackRunner::new());
        let registry = Arc::new(crate::registry::ModelRegistry::new());
        let queue = Arc::new(crate::inference_queue::InferenceQueue::new());
        let wm = Arc::new(crate::worker::WorkerManager::new(
            registry.clone(),
            PathBuf::new(),
            queue.clone(),
            "warn".to_string(),
            cb.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            wm,
            queue,
            crate::config::Config::default(),
            PathBuf::new(),
            cb,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    /// Register `model` v1 ready with one worker + ZMQ client (test hook) AND
    /// an inference-queue entry whose `max_concurrent_streams` is 1, so the
    /// G4 permit is enforced.
    async fn register_capped_streaming_model(
        state: &Arc<AppState>,
        model: &str,
        endpoint: String,
    ) {
        state
            .registry
            .register(
                model,
                "1",
                crate::config::ModelConfig::default(),
                crate::registry::types::ModelType::LitAPI,
                PathBuf::new(),
            )
            .unwrap();
        state.registry.mark_ready(model, "1").unwrap();
        state
            .registry
            .set_workers(
                model,
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
        let client = Arc::new(crate::transport::zmq::WorkerZmqClient::new(endpoint));
        state
            .worker_manager
            .insert_zmq_clients_for_test(model, "1", vec![client.clone()])
            .await;
        let config = crate::config::ModelConfig {
            max_concurrent_streams: 1,
            ..Default::default()
        };
        state.inference_queue.register_model(
            model,
            "1",
            &config,
            vec![],
            vec![client],
            Arc::new(crate::inference_queue::OutlierState::new(1)),
            None,
        );
    }

    /// Silent PAIR worker: records every received frame's stream action into
    /// `seen` (Open / Cancel), never replies — the opened stream stays
    /// in-flight until something cancels it.
    fn spawn_recording_worker(
        endpoint: String,
        seen: std::sync::mpsc::Sender<String>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(8000);
            while let Ok(bytes) = s.recv_bytes(0) {
                let Ok(req) = pb::Request::decode(bytes.as_slice()) else { continue };
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                let action = match st.action {
                    Some(pb::stream_request::Action::Open(_)) => "open",
                    Some(pb::stream_request::Action::Cancel(_)) => "cancel",
                    _ => "other",
                };
                let _ = seen.send(format!("{}:{action}", st.stream_id));
            }
        })
    }

    fn single_stream_step_plan(model: &str) -> EnsemblePlan {
        EnsemblePlan {
            steps: vec![EnsembleStep {
                name: "s".to_string(),
                model: model.to_string(),
                version: Some("1".to_string()),
                inputs: HashMap::new(),
                stream: true,
                params: HashMap::new(),
                timeout_secs: None,
                on_error: OnErrorKind::Fail,
                retries: 0,
                outputs_decl: None,
                when: None,
            }],
            layers: vec![vec![0]],
            output_step: 0,
            output_field: None,
            chains: Vec::new(),
            inputs_decl: None,
            input_modes: Vec::new(),
            conditional_refs: Vec::new(),
            step_dep_keys: vec![Vec::new()],
            step_raw_eligible: Vec::new(),
            outputs: None,
            dag_sets: None,
            config_path: PathBuf::from("/audit-0822"),
            source_mtime: None,
        }
    }

    /// execute_stream_step must take the G4 concurrency permit BEFORE opening
    /// the worker stream — the gRPC unary-stream path's contract
    /// ("rejected pre-open", grpc/rpc/stream.rs:234). The old order
    /// (send_stream → record → permit) abandoned an already-opened worker
    /// stream on capacity rejection with no cancel: the server-side route
    /// was reaped by the ZMQ actor's orphan sweep, but the worker-side
    /// generator never learned and ran to its natural end (an unbounded
    /// generator occupies the worker forever). The fix makes the rejection
    /// side-effect free: the worker must observe NO open frame at all.
    #[tokio::test]
    async fn capacity_reject_must_not_open_a_worker_stream() {
        let state = audit_state();
        let endpoint = {
            let sock = std::env::temp_dir().join(format!(
                "lite-server-audit0822-cap-{}.sock",
                std::process::id()
            ));
            format!("ipc://{}", sock.display())
        };
        let (seen_tx, seen_rx) = std::sync::mpsc::channel();
        let _worker = spawn_recording_worker(endpoint.clone(), seen_tx);
        register_capped_streaming_model(&state, "capm", endpoint).await;

        // Occupy the only stream-concurrency slot (cap = 1).
        let _held = state
            .inference_queue
            .try_acquire_stream_permit("capm", "1")
            .expect("permit acquisition must succeed")
            .expect("registered version must return a permit");

        let plan = single_stream_step_plan("capm");
        let ctx: HashMap<String, EnsembleValue> = HashMap::new();
        let opts = EnsembleExecOpts {
            client_ip: String::new(),
            deadline_unix_ns: None,
            deadline_client_specified: false,
            decoupled: false,
            dag_selector: None,
        };
        let snapshot = Arc::new(VersionSnapshot::default());
        let res = execute_stream_step(
            &state, &plan, 0, &ctx, "req-cap", &opts, None, None, &snapshot,
        )
        .await;
        assert!(
            matches!(res, Err(AppError::StreamingCapacityExceeded(_))),
            "the occupied cap must reject the second stream with StreamingCapacityExceeded"
        );

        // The rejection must be side-effect free: no open frame may reach
        // the worker for the rejected request.
        let mut frames = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            match seen_rx.recv_timeout(Duration::from_millis(200)) {
                Ok(frame) => frames.push(frame),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        assert!(
            frames.is_empty(),
            "capacity rejection leaked a worker stream open: {frames:?}"
        );
    }

    /// /audit 举证(流式面,控制流维度):E1/D4 —— `stream: true` 的 step 指向
    /// ensemble 子模型是「不支持的组合」,必须快速失败 400
    /// (`InvalidRequestBody`,§4.4「组合不支持」行)。exec.rs:1608-1614 的 D4
    /// 检查位于 D19 退避轮询(1567-1601)之后,而 ensemble 模型 workers 恒空
    /// → `streaming_worker_ready`(1436)永远为 false → 轮询只能以 30 次重试
    /// 耗尽(503 ModelNotReady)或 deadline 快速失败(504 InferenceTimeout)
    /// 退出,D4 检查对其目标场景是死代码。客户端白等一轮退避还拿到错误的
    /// 状态码归属。
    #[tokio::test]
    async fn test_audit_control_streaming_step_to_ensemble_must_fail_fast() {
        let state = audit_state();
        // Register the sub-model as an ENSEMBLE: registry-ready, zero workers.
        state
            .registry
            .register(
                "subens",
                "1",
                crate::config::ModelConfig::default(),
                crate::registry::types::ModelType::Ensemble,
                PathBuf::new(),
            )
            .unwrap();
        state.registry.mark_ready("subens", "1").unwrap();

        let plan = single_stream_step_plan("subens");
        let ctx: HashMap<String, EnsembleValue> = HashMap::new();
        // A short client deadline keeps the misrouted D19 poll quick (the
        // fast-fail arm fires in ~300ms instead of 30 retries ≈ 15s).
        let deadline_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
            + 300_000_000;
        let opts = EnsembleExecOpts {
            client_ip: String::new(),
            deadline_unix_ns: Some(deadline_ns),
            deadline_client_specified: true, // the test's short deadline stands in for a client one
            decoupled: false,
            dag_selector: None,
        };
        let snapshot = Arc::new(VersionSnapshot::default());
        let res = execute_stream_step(
            &state, &plan, 0, &ctx, "req-d4", &opts, Some(deadline_ns), None, &snapshot,
        )
        .await;
        let err = match res {
            Err(e) => e,
            Ok(_) => panic!(
                "D4: a streaming step calling an ensemble model must fail fast, not open a stream"
            ),
        };
        assert!(
            matches!(err, AppError::InvalidRequestBody(_)),
            "D4: a streaming step calling an ensemble model is an unsupported \
             combination and must fail fast with 400 InvalidRequestBody; got \
             {err:?} (the D19 readiness poll can never succeed for a workerless \
             ensemble, so the fail-fast check behind it is unreachable)"
        );
    }

    /// Register `model` v1 ready with one worker + ZMQ client and a DEFAULT
    /// (uncapped) queue entry.
    async fn register_uncapped_streaming_model(
        state: &Arc<AppState>,
        model: &str,
        endpoint: String,
    ) {
        state
            .registry
            .register(
                model,
                "1",
                crate::config::ModelConfig::default(),
                crate::registry::types::ModelType::LitAPI,
                PathBuf::new(),
            )
            .unwrap();
        state.registry.mark_ready(model, "1").unwrap();
        state
            .registry
            .set_workers(
                model,
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
        let client = Arc::new(crate::transport::zmq::WorkerZmqClient::new(endpoint));
        state
            .worker_manager
            .insert_zmq_clients_for_test(model, "1", vec![client.clone()])
            .await;
        state.inference_queue.register_model(
            model,
            "1",
            &crate::config::ModelConfig::default(),
            vec![],
            vec![client],
            Arc::new(crate::inference_queue::OutlierState::new(1)),
            None,
        );
    }

    /// Scripted PAIR worker: the first `error_opens` Open frames get an
    /// immediate StreamError reply; later Open frames stay silent (the stream
    /// never produces a first frame). Every observed Open is counted into
    /// `open_count`.
    fn spawn_error_then_silent_worker(
        endpoint: String,
        error_opens: u32,
        open_count: Arc<std::sync::atomic::AtomicU32>,
    ) -> std::thread::JoinHandle<()> {
        use prost::Message;
        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let s = ctx.socket(zmq::PAIR).expect("worker socket");
            s.connect(&endpoint).expect("worker connect");
            let _ = s.set_rcvtimeo(8000);
            let mut opens: u32 = 0;
            while let Ok(bytes) = s.recv_bytes(0) {
                let Ok(req) = pb::Request::decode(bytes.as_slice()) else { continue };
                let Some(pb::request::Payload::Stream(st)) = req.payload else { continue };
                if !matches!(st.action, Some(pb::stream_request::Action::Open(_))) {
                    continue;
                }
                opens += 1;
                open_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if opens <= error_opens {
                    let resp = pb::Response {
                        payload: Some(pb::response::Payload::Stream(pb::StreamResponse {
                            stream_id: st.stream_id.clone(),
                            payload: Some(pb::stream_response::Payload::Error(pb::StreamError {
                                message: "boom".to_string(),
                            })),
                        })),
                        ..Default::default()
                    };
                    let _ = s.send(resp.encode_to_vec(), 0);
                }
                // opens > error_opens: stay silent (hang before the first frame).
            }
        })
    }

    /// /audit 举证(流式面,顺序/数据维度):D35 建流期 retry 的首帧 peek 超时
    /// 预算 `peek_bound` 在 exec.rs:1797-1799 于循环外按「进入时的
    /// step_deadline 余量」计算一次,之后每次重试都用同一个陈旧值。前几次
    /// 尝试的 send/peek/退避已消耗预算,后续 peek 仍能等到「进入时刻 +
    /// 原始余量」,建流窗口可显著越过 step_deadline(D35:retry 窗口 =
    /// send_stream 至首个非 Error 帧,受 step 墙钟上限约束)。
    ///
    /// 场景:timeout_secs=1.5、retries=3;前 3 次建流立刻收到 Error 帧,
    /// 退避 50+100+200ms;第 4 次建流 worker 沉默。正确行为:第 4 次 peek
    /// 只剩「deadline − 已耗」的余量,函数在 ~1.5s 返回;当前行为:第 4 次
    /// peek 仍等满进入时的 1500ms,~2.1s 才返回。open_count 看守排除
    /// 「环境太慢导致第 4 次尝试根本没发生」的假通过。
    #[tokio::test]
    async fn test_audit_order_d35_retry_peek_must_recompute_remaining_budget() {
        let state = audit_state();
        let endpoint = format!(
            "ipc://{}",
            std::env::temp_dir()
                .join(format!("lite-server-audit0822-peek-{}.sock", std::process::id()))
                .display()
        );
        let open_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let _worker = spawn_error_then_silent_worker(endpoint.clone(), 3, open_count.clone());
        register_uncapped_streaming_model(&state, "retrym", endpoint).await;

        let mut plan = single_stream_step_plan("retrym");
        plan.steps[0].timeout_secs = Some(1.5);
        plan.steps[0].retries = 3;
        let ctx: HashMap<String, EnsembleValue> = HashMap::new();
        let opts = EnsembleExecOpts {
            client_ip: String::new(),
            deadline_unix_ns: None,
            deadline_client_specified: false,
            decoupled: false,
            dag_selector: None,
        };
        let snapshot = Arc::new(VersionSnapshot::default());
        let start = std::time::Instant::now();
        let _ = execute_stream_step(&state, &plan, 0, &ctx, "req-peek", &opts, None, None, &snapshot).await;
        let elapsed = start.elapsed();
        assert_eq!(
            open_count.load(std::sync::atomic::Ordering::Relaxed),
            4,
            "the scenario requires 3 fast-Error attempts + 1 silent attempt; \
             fewer opens means the environment was too slow, not the bug absent"
        );
        assert!(
            elapsed < Duration::from_millis(1800),
            "D35: the build window (open + retries + first-frame peeks) must stay \
             within the step wall-clock cap (1.5s); a stale per-entry peek bound \
             let the last attempt overshoot to {elapsed:?}"
        );
    }

    /// /audit 举证(流式面,资源/控制流维度):forward_stream 的跳级墙钟上限
    /// (D35,exec.rs:936-960)只包裹 `rx.recv()`;chunk 分支的
    /// `tx.send(chunk).await`(967)无界。下游 64 槽 channel 满且消费方停滞
    /// 时,hop 阻塞在 send 里,deadline 永远无法触发:无 Error 帧、无 cancel,
    /// 直到适配层 idle 回收兜底(`decoupled_idle_timeout_secs = 0` 时无限挂起,
    /// P10 许可/链路任务树一并泄漏)。
    ///
    /// 场景:上游瞬间供给 100 个 chunk(> 下游 64 槽),下游 receiver 永不
    /// 排空,hop deadline = 100ms。正确行为:deadline 触发,hop 以
    /// error-terminated 结束;当前行为:forward_stream 永久阻塞在第 65 个
    /// send 上。
    #[tokio::test]
    async fn test_audit_resource_forward_stream_send_must_not_escape_hop_deadline() {
        let (up_tx, up_rx) = mpsc::channel(128);
        let (down_tx, _down_rx) = mpsc::channel(64); // receiver never drained
        let endpoint = format!(
            "ipc://{}",
            std::env::temp_dir()
                .join(format!("lite-server-audit0822-fwd-{}.sock", std::process::id()))
                .display()
        );
        let (seen_tx, _seen_rx) = std::sync::mpsc::channel();
        let _worker = spawn_recording_worker(endpoint.clone(), seen_tx);
        let client = Arc::new(crate::transport::zmq::WorkerZmqClient::new(endpoint));

        let feeder = tokio::spawn(async move {
            for _ in 0..100 {
                let frame = pb::StreamResponse {
                    stream_id: "sid".to_string(),
                    payload: Some(pb::stream_response::Payload::Chunk(pb::StreamChunkResponse {
                        data: bytes::Bytes::from_static(b"{}"),
                        is_final: false,
                    })),
                };
                if up_tx.send(frame).await.is_err() {
                    break;
                }
            }
        });

        let hop_deadline = std::time::Instant::now() + Duration::from_millis(100);
        let res = tokio::time::timeout(
            Duration::from_secs(3),
            forward_stream(up_rx, down_tx, client, "sid".to_string(), Some(hop_deadline), None, "hop"),
        )
        .await;
        feeder.abort();
        let outcome = res.expect(
            "D35: the hop wall-clock cap must fire even while the downstream send \
             is backpressured; forward_stream hung past the 3s witness bound",
        );
        assert!(
            matches!(outcome, ForwardOutcome::Error),
            "a hop-deadline expiry must end the chain as error-terminated, got {outcome:?}"
        );
    }
}
