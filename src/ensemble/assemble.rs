use crate::error::{AppError, ModelErrorData};
use crate::http::state::AppState;
use crate::proto::liteserver as pb;
use crate::registry::types::ModelType;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};
use uuid::Uuid;

use super::*;


/// E6 (batch 4): the retry sleep — None when the step budget cannot cover
/// it (fast-fail instead of sleeping into the deadline, §4.4 deadline row).
pub(crate) fn retry_sleep_budget(step_deadline: Option<i64>, backoff: Duration) -> Option<Duration> {
    match crate::deadline::remaining(step_deadline) {
        Some(rem) if rem < backoff => None,
        _ => Some(backoff),
    }
}

/// Shared step RequestMeta (unary + streaming): request_id `{parent}:{step}`
/// suffix, trace-injected headers, deadline cascade, content-type passthrough
/// (B3/E7). `payload` is the serialized body — empty for streaming, where the
/// body rides StreamOpen.data.
pub(crate) fn build_step_meta(
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
pub(crate) fn assemble_step_payload(
    step_name: &str,
    resolved: &HashMap<String, EnsembleValue>,
    params: &HashMap<String, Value>,
    // MIMO (R11/R12): parse-decided mode — Some = static dispatch (no
    // runtime type branches); None = legacy dynamic dispatch (the historical
    // runtime checks stay byte-identical).
    mode: Option<InputMode>,
) -> Result<(bytes::Bytes, Option<String>), AppError> {
    let binary_count = resolved
        .values()
        .filter(|v| matches!(v, EnsembleValue::Binary(..)))
        .count();

    // MIMO: the static environment pre-decided the branch — the runtime
    // checks below are unreachable for Some(mode) (R12/R9 parse-guaranteed).
    match mode {
        Some(InputMode::GroupJson) => {
            return assemble_group_json(step_name, resolved, params);
        }
        Some(InputMode::BinaryPassThrough) => {
            return assemble_binary_passthrough(step_name, resolved, params);
        }
        None => {}
    }

    if binary_count == 0 {
        // All Json → build JSON object (historical path).
        return assemble_group_json(step_name, resolved, params);
    }
    if resolved.len() == 1 && binary_count == 1 {
        // Exactly one input and it is Binary → raw bytes passthrough.
        return assemble_binary_passthrough(step_name, resolved, params);
    }
    // Mixed or multiple inputs with any Binary → 400 (Option B scope).
    Err(AppError::InvalidRequestBody(format!(
        "step '{}' has {} input(s) with mixed JSON/Binary; \
         a binary input must be the step's sole whole input (Option B scope)",
        step_name,
        resolved.len()
    )))
}

/// MIMO (R12): GroupJson assembly — all-Json inputs build one object (E3
/// params override after the inputs). Shared by the static dispatch and the
/// legacy dynamic path (identical bytes).
///
/// P2/P8 (batch 6): raw-resident values splice their ORIGINAL bytes into the
/// assembled payload — no parse, no re-serialize on the hot path. Keys emit
/// in sorted order (the historical serde_json::Map ordering) so the bytes
/// are identical to the pre-P2 path.
pub(crate) fn assemble_group_json(
    _step_name: &str,
    resolved: &HashMap<String, EnsembleValue>,
    params: &HashMap<String, Value>,
) -> Result<(bytes::Bytes, Option<String>), AppError> {
    enum ValueSource<'a> {
        Input(&'a EnsembleValue),
        Param(&'a Value),
    }
    impl ValueSource<'_> {
        fn len_estimate(&self) -> usize {
            match self {
                ValueSource::Input(EnsembleValue::Json(v)) => json_value_len_estimate(v),
                ValueSource::Param(v) => json_value_len_estimate(v),
                ValueSource::Input(EnsembleValue::RawJson(raw)) => raw.bytes.len(),
                // Unreachable: legacy routes here only with binary_count == 0;
                // static GroupJson is parse-checked (R12).
                ValueSource::Input(EnsembleValue::Binary(..) | EnsembleValue::Envelope { .. }) => {
                    unreachable!()
                }
            }
        }
    }

    // E3: constant step params override the assembled inputs on key conflict.
    let mut merged: HashMap<&str, ValueSource> = resolved
        .iter()
        .map(|(k, v)| (k.as_str(), ValueSource::Input(v)))
        .collect();
    for (key, val) in params {
        merged.insert(key.as_str(), ValueSource::Param(val));
    }
    let mut entries: Vec<(&str, ValueSource)> = merged.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    let estimate: usize = entries
        .iter()
        .map(|(k, src)| k.len() + 3 + src.len_estimate())
        .sum::<usize>()
        + 2;
    let mut buf = Vec::with_capacity(estimate);
    buf.push(b'{');
    for (i, (key, src)) in entries.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        serde::Serialize::serialize(key, &mut serde_json::Serializer::new(&mut buf))
            .expect("key serialization is infallible");
        buf.push(b':');
        match src {
            ValueSource::Param(v) => {
                serde::Serialize::serialize(
                    v,
                    &mut serde_json::Serializer::new(&mut buf),
                )
                .expect("Value serialization is infallible");
            }
            ValueSource::Input(EnsembleValue::Json(v)) => {
                serde::Serialize::serialize(
                    v,
                    &mut serde_json::Serializer::new(&mut buf),
                )
                .expect("Value serialization is infallible");
            }
            ValueSource::Input(EnsembleValue::RawJson(raw)) => {
                buf.extend_from_slice(&raw.bytes);
            }
            ValueSource::Input(EnsembleValue::Binary(..) | EnsembleValue::Envelope { .. }) => {
                unreachable!()
            }
        }
    }
    buf.push(b'}');
    Ok((bytes::Bytes::from(buf), None))
}

/// MIMO (R12/D8): BinaryPassThrough assembly — the sole whole Binary input's
/// bytes pass through (D8's bounded internal binary flow).
fn assemble_binary_passthrough(
    step_name: &str,
    resolved: &HashMap<String, EnsembleValue>,
    params: &HashMap<String, Value>,
) -> Result<(bytes::Bytes, Option<String>), AppError> {
    // Unreachable under R9 in static mode — kept as the defensive mirror of
    // the legacy runtime check.
    if !params.is_empty() {
        return Err(AppError::InvalidRequestBody(format!(
            "step '{}': params cannot be combined with a binary input (E3/R9)",
            step_name
        )));
    }
    let (data, ct, ..) = match resolved.values().next().unwrap() {
        EnsembleValue::Binary(data, ct, ..) => (data.clone(), ct.clone()),
        // Unreachable: legacy routes here only with a sole binary value;
        // static BinaryPassThrough is parse-checked (R12).
        _ => unreachable!(),
    };
    Ok((data, Some(ct)))
}

/// B3 (E8): typed step output — mirrors the unary media_type dispatch
/// (inference.rs:266-267). A non-JSON media_type declares the payload opaque
/// bytes; the JSON path is validated and errors are NOT swallowed (the old
/// code collapsed invalid JSON to `{}` — this is the regression pin).
pub(crate) fn parse_step_output(step_name: &str, single: pb::SingleResponse) -> Result<EnsembleValue, AppError> {
    let is_binary = !single.media_type.is_empty()
        && !single.media_type.starts_with("application/json");
    if is_binary {
        Ok(EnsembleValue::Binary(single.data, single.media_type, None, None))
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

/// MIMO (D10, batch 4①: the binary half): materialize a step's raw worker
/// response into its declared named outputs — context keys are `step.alias`
/// (an undeclared step keeps the single `step` key). Binary alias semantics
/// (R6): path-less = the whole response (worker non-JSON media_type);
/// path-specified = a `$binary_b64` marker object at that JSON path. A
/// type mismatch (declared binary / worker JSON, or vice versa) is an
/// ordinary step error (I3's runtime failure #1 — on_error semantics apply).
/// Json aliases land with MIMO② — the declaration shape is already final.
pub(crate) fn materialize_step_outputs(
    step: &EnsembleStep,
    raw: EnsembleValue,
) -> Result<Vec<(String, EnsembleValue)>, AppError> {
    let Some(decl) = &step.outputs_decl else {
        return Ok(vec![(step.name.clone(), raw)]);
    };
    // P2 (batch 6): a declared step over a NESTED child's raw-resident
    // outcome parses before projection (unary workers never produce raw for
    // declared steps — unary_response_to_value forces the parse — but the
    // child's build_response may hand over raw bytes).
    let raw = match raw {
        EnsembleValue::RawJson(r) => {
            let v = r.parse().map_err(|e| {
                AppError::Internal(format!(
                    "step '{}' raw output is not valid JSON: {}",
                    step.name, e
                ))
            })?;
            EnsembleValue::Json((**v).clone())
        }
        other => other,
    };
    let mut out = Vec::with_capacity(decl.len());
    for (alias, d) in decl {
        let key = format!("{}.{}", step.name, alias);
        match (&raw, d.ty) {
            (EnsembleValue::Binary(data, ct, shape, dt), InputType::Binary) => {
                if d.path.is_some() {
                    return Err(AppError::Internal(format!(
                        "step '{}' alias '{}': a path-specified binary output needs a \
                         JSON response (the marker object lives in JSON)",
                        step.name, alias
                    )));
                }
                out.push((
                    key,
                    EnsembleValue::Binary(
                        data.clone(),
                        ct.clone(),
                        shape.clone(),
                        dt.clone(),
                    ),
                ));
            }
            (EnsembleValue::Binary(..), InputType::Json) => {
                return Err(AppError::InvalidRequestBody(format!(
                    "step '{}' alias '{}' is declared json but the worker returned \
                     binary (type mismatch, I3)",
                    step.name, alias
                )));
            }
            (EnsembleValue::Json(v), InputType::Binary) => {
                let path = d.path.as_deref().ok_or_else(|| {
                    AppError::InvalidRequestBody(format!(
                        "step '{}' alias '{}' is declared binary (whole response) but \
                         the worker returned JSON (type mismatch, I3)",
                        step.name, alias
                    ))
                })?;
                let marker = project_json_path(v, path).map_err(|_| {
                    AppError::InvalidRequestBody(format!(
                        "step '{}' alias '{}': path '{}' not found in the response",
                        step.name, alias, path
                    ))
                })?;
                let (bytes, ct) = decode_binary_marker(&marker)?;
                out.push((key, EnsembleValue::Binary(bytes, ct, None, None)));
            }
            (EnsembleValue::Json(v), InputType::Json) => {
                // MIMO② (D10): the default path is `$.<alias>`; an explicit
                // path projects the Json pointer subset (D29).
                let path = d.path.as_deref().unwrap_or(alias);
                let projected = project_json_path(v, path).map_err(|_| {
                    AppError::InvalidRequestBody(format!(
                        "step '{}' alias '{}': path '{}' not found in the response",
                        step.name, alias, path
                    ))
                })?;
                out.push((key, EnsembleValue::Json(projected)));
            }
            (EnsembleValue::Envelope { .. }, _) => {
                return Err(AppError::Internal(
                    "envelope reached step materialization".to_string(),
                ));
            }
            // P2 (batch 6): declared steps always parse (raw residency only
            // applies to undeclared steps).
            (EnsembleValue::RawJson(_), _) => {
                unreachable!("declared steps never receive raw-resident values")
            }
        }
    }
    Ok(out)
}

/// E6 (batch 4): retry classification — only transient worker-side failures
/// retry. 5xx model errors (upstream overload/crash race) and timeouts are
/// transient; 4xx is a deterministic client contract (retrying cannot fix
/// it), queue pressure retrying makes worse, and crashes/readiness belong to
/// the autoload path, not the retry path.
pub(crate) fn is_retryable_error(e: &AppError) -> bool {
    match e {
        AppError::InferenceTimeout(_) => true,
        AppError::ModelError(data) => data.status_code >= 500,
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)] // step plumbing: state+plan+ctx+ids+snapshot ride together by design
pub(crate) async fn execute_step(
    state: Arc<AppState>,
    plan: &EnsemblePlan,
    step_idx: usize,
    step: &EnsembleStep,
    context: &HashMap<String, EnsembleValue>,
    request_id: &str,
    client_ip: &str,
    deadline_unix_ns: Option<i64>,
    snapshot: &Arc<VersionSnapshot>,
    depth: u32, // E1: nesting depth — +1 on recursion into an ensemble step
    ancestors: &[(String, String)], // E1: THIS branch's nesting chain (B1: never shared across sibling branches)
) -> Result<Vec<(String, EnsembleValue)>, AppError> {
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
        match resolve_ref(plan, ref_str, context)? {
            ResolvedRef::Value(v) => {
                resolved.insert(key.clone(), v);
            }
            ResolvedRef::Absent(name) => {
                return Err(AppError::Internal(format!(
                    "step '{}' resolved absent input '{}' — conditional steps \
                     are skipped before execution (R4)",
                    step.name, name
                )));
            }
        }
    }

    // MIMO (R11/R12): static input-mode dispatch in declared configs; legacy
    // configs keep the dynamic runtime dispatch (byte-identical).
    let (payload_bytes, content_type_for_step) = assemble_step_payload(
        &step.name, &resolved, &step.params, plan.input_mode(step_idx),
    )?;

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
        // error names the offending child DAG). E8-1: a dags-form child is a
        // pure container whose outer plan has EMPTY steps — nested calls
        // carry no selector, so the check inspects the SELECTED default set
        // (indexing the outer plan would panic).
        let child_plan = get_ensemble_plan(&state, &step.model, &resolved_version).await?;
        let child_plan = select_dag_set(&child_plan, None)?;
        if child_plan.steps[child_plan.output_step].stream || !child_plan.chains.is_empty() {
            return Err(AppError::InvalidRequestBody(format!(
                "nested ensemble '{}' contains a streaming step — nested ensembles \
                 must be unary-only (D4)",
                step.model
            )));
        }
        let child_payload = match &content_type_for_step {
            Some(ct) => EnsembleValue::Binary(payload_bytes.clone(), ct.clone(), None, None),
            None => {
                let v: Value = serde_json::from_slice(&payload_bytes).map_err(|e| {
                    AppError::Internal(format!(
                        "ensemble step {} payload reparse failed: {}",
                        step.name, e
                    ))
                })?;
                // R17: a child declaring `inputs` consumes the parent's
                // assembled group object as its NAMED-INPUT map (keys =
                // input names, no $inputs wrapper) — reframe it as the
                // KServe envelope so the child's single parse point (R18)
                // validates it (unknown key / missing required → 400).
                if child_plan.inputs_decl.is_some() {
                    let Value::Object(map) = v else {
                        return Err(AppError::InvalidRequestBody(format!(
                            "nested ensemble '{}' declares inputs — the parent step's \
                             payload must be a group object of named inputs (R17)",
                            step.model
                        )));
                    };
                    let inputs: Vec<Value> = map
                        .into_iter()
                        .map(|(name, data)| json!({"name": name, "data": data}))
                        .collect();
                    EnsembleValue::Json(json!({"inputs": inputs}))
                } else {
                    EnsembleValue::Json(v)
                }
            }
        };
        let child_request_id = format!("{}:{}", request_id, step.name);
        let child_opts = EnsembleExecOpts {
            client_ip: client_ip.to_string(),
            deadline_unix_ns,
            decoupled: false, // the child is unary-only (D4)
            dag_selector: None, // E8-1: nested ensembles are unary-only, no selector passthrough
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
            EnsembleOutcome::Unary(v) => materialize_step_outputs(step, v),
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

    // E6 (batch 4): worker-inference retry loop — submit → bounded wait →
    // classify. Retryable errors (5xx/timeout, is_retryable_error) re-enter
    // with exponential backoff and a fresh uid (queue identity) while the
    // payload (Bytes refcount) and meta (Arc clone) are reused. 4xx, queue
    // pressure and crashes fall through immediately.
    let mut attempt: u32 = 0;
    let mut backoff = Duration::from_millis(50);
    loop {
        // Send inference request through the unified queue
        let uid = format!("ensemble_{}_{}_{}", step.model, resolved_version, Uuid::new_v4());

        let (response_tx, response_rx) = oneshot::channel();
        let item = crate::inference_queue::QueueItem {
            uid,
            data: payload_bytes.clone(),
            meta: Some(std::sync::Arc::new(meta.clone())),
            response_tx,
            inflight_guard: None,
            enqueued_at: std::time::Instant::now(),
        };

        match state.inference_queue.try_submit(&step.model, &resolved_version, item) {
            Ok(()) => {}
            Err(crate::inference_queue::QueueError::Full) => {
                // E6: queue pressure is NOT retryable (a retry adds pressure).
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

        let result = async {
            // P-DEADLINE cascade (E5): bound this attempt by the STEP
            // deadline's remaining budget — min(parent, now + timeout_secs).
            // None = no deadline → unbounded inner wait, outer DAG bound
            // still applies via execute_ensemble's total_budget.
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
            unary_response_to_value(&step.name, response, plan.step_raw_eligible[step_idx])
        }.await;

        match result {
            Ok(value) => return materialize_step_outputs(step, value),
            Err(e) => {
                if attempt < step.retries && is_retryable_error(&e) {
                    // E6: don't sleep past the step budget — the next attempt
                    // would fail immediately anyway (fast-fail, §4.4 deadline
                    // row semantics).
                    if let Some(rem) = crate::deadline::remaining(step_deadline) {
                        if rem < backoff {
                            return Err(e);
                        }
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_millis(500));
                    attempt += 1;
                    continue;
                }
                return Err(e);
            }
        }
    }
}

/// E6 (batch 4): map a unary queue response to the step value (extracted
/// from execute_step so the retry loop can classify the error before
/// deciding). B3: typed output; §4.4 status mapping — a numeric Status.message
/// → ModelError (4xx passes through, 5xx maps 500), a non-numeric message
/// means the worker itself is broken → WorkerCrashed.
pub(crate) fn unary_response_to_value(
    step_name: &str,
    response: pb::Response,
    raw_eligible: bool,
) -> Result<EnsembleValue, AppError> {
    match response.payload {
        Some(pb::response::Payload::Single(single)) => {
            let code = single.status.as_ref().map(|s| s.code.as_str()).unwrap_or("Ok");
            match code {
                "Ok" => {
                    // P2 (batch 6): a raw-eligible (pass-through schema)
                    // JSON response stays unparsed Bytes in the context —
                    // field access parses lazily. Whole refs splice the
                    // bytes, so raw residency REQUIRES well-formed JSON:
                    // a borrowed RawValue parse validates without
                    // materializing (C5 — invalid JSON keeps the historical
                    // parse error channel via parse_step_output; splicing it
                    // would produce malformed/injected downstream payloads).
                    let is_binary = !single.media_type.is_empty()
                        && !single.media_type.starts_with("application/json");
                    if raw_eligible
                        && !is_binary
                        && !single.data.is_empty()
                        && serde_json::from_slice::<&serde_json::value::RawValue>(&single.data).is_ok()
                    {
                        Ok(EnsembleValue::RawJson(Arc::new(RawJsonValue::new(
                            single.data,
                        ))))
                    } else {
                        // B3 (E8): typed output (see parse_step_output).
                        parse_step_output(step_name, single)
                    }
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

