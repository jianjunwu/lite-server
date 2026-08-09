use crate::error::AppError;
use crate::http::state::AppState;
use crate::proto::liteserver as pb;
use crate::registry::types::ModelType;
use bytes::Bytes;
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::oneshot;
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
}

#[derive(Debug, Clone)]
pub struct EnsembleStep {
    pub name: String,
    pub model: String,
    pub version: String,
    pub inputs: HashMap<String, String>,
}

lazy_static::lazy_static! {
    static ref REF_RE: Regex = Regex::new(r"^\$(\w+)(?:\.(\w+))?$")
        .expect("invalid ensemble ref regex");
}

pub async fn parse_ensemble_config(config_path: &PathBuf) -> Result<Vec<EnsembleStep>, AppError> {
    let content = tokio::fs::read_to_string(config_path)
        .await
        .map_err(|e| AppError::Config(format!("failed to read ensemble config: {}", e)))?;
    let config: EnsembleConfig = serde_yaml::from_str(&content)
        .map_err(|e| AppError::Config(format!("failed to parse ensemble config: {}", e)))?;

    let steps: Vec<EnsembleStep> = config.ensemble.steps.into_iter().map(|s| EnsembleStep {
        name: s.name,
        model: s.model,
        version: s.version,
        inputs: s.inputs,
    }).collect();

    validate_dag(&steps)?;
    Ok(steps)
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

// ===== Execution =====

pub async fn execute_ensemble(
    state: Arc<AppState>,
    model_name: &str,
    version: &str,
    payload: EnsembleValue,
    request_id: &str,
    client_ip: &str,
    deadline_unix_ns: Option<i64>,
) -> Result<EnsembleValue, AppError> {
    let model_dir = crate::validation::resolve_model_dir(
        &state.repo_path, model_name, version,
    )?;
    let config_path = model_dir.join("config.yaml");

    let steps = parse_ensemble_config(&config_path).await?;
    let layers = topological_layers(&steps);

    let mut context: HashMap<String, EnsembleValue> = HashMap::new();
    context.insert("request".to_string(), payload);

    // #3: bound the WHOLE ensemble by a single shared deadline (P-DEADLINE
    // §4.0.10): the parent request's deadline cascades across the whole DAG, so
    // an N-layer ensemble can never exceed the parent. Layers run serially, so
    // without this each layer could spend up to its own budget and amplify to
    // N×. The per-step timeout in execute_step (parent − elapsed) is the inner
    // safety net; this outer deadline is what actually bounds the total.
    let total_budget = crate::deadline::remaining(deadline_unix_ns);
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
            for step in layer {
                let state = state.clone();
                let ctx = context.clone();
                let step = step.clone();
                let ensemble_name = model_name.to_string();
                let request_id = request_id.to_string();
                let client_ip = client_ip.to_string();
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

    // Return last step's output
    steps.last()
        .and_then(|s| context.get(&s.name))
        .cloned()
        .ok_or_else(|| AppError::Internal("ensemble produced no output".to_string()))
}

/// B3 (E7): step input assembly — three branches.
///   all Json                                → build a JSON object (historical path)
///   exactly one input, a whole Binary       → raw bytes passthrough + content-type
///   any other combination involving Binary  → 400 (Option B scope)
fn assemble_step_payload(
    step_name: &str,
    resolved: &HashMap<String, EnsembleValue>,
) -> Result<(Vec<u8>, Option<String>), AppError> {
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
        let payload_value = Value::Object(obj);
        // serde_json::to_vec on a Value is infallible (E8 cleanup note).
        let bytes = serde_json::to_vec(&payload_value).expect("Value serialization is infallible");
        Ok((bytes, None))
    } else if resolved.len() == 1 && binary_count == 1 {
        // Exactly one input and it is Binary → raw bytes passthrough.
        let (data, ct) = match resolved.values().next().unwrap() {
            EnsembleValue::Binary(data, ct) => (data.clone(), ct.clone()),
            EnsembleValue::Json(_) => unreachable!(),
        };
        Ok((data.to_vec(), Some(ct)))
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

    // Ensure sub-model is ready
    if !state.registry.is_ready(&step.model, Some(&step.version)) {
        info!("Auto-loading sub-model {} v{} for ensemble", step.model, step.version);
        let sub_model_dir = crate::validation::resolve_model_dir(
            &state.repo_path, &step.model, &step.version,
        )?;
        // 配置解析/校验失败必须可见（同 reconcile：不再 unwrap_or_default
        // 静默回退默认配置；M7 迁移哨兵依赖此错误上浮）。
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
        // Poll with exponential backoff for worker readiness
        let mut retries = 0;
        let max_retries = 30;
        let mut delay = Duration::from_millis(50);
        while !state.registry.is_ready(&step.model, Some(&step.version)) && retries < max_retries {
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_millis(500));
            retries += 1;
        }
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

    let meta = pb::RequestMeta {
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
        payload: bytes::Bytes::from(payload_bytes.clone()),
        // P-DEADLINE cascade: child step shares the parent deadline so a single
        // step cannot exceed it (budget = parent − already elapsed).
        deadline_unix_ns,
        ..Default::default()
    };

    let (response_tx, response_rx) = oneshot::channel();
    let item = crate::inference_queue::QueueItem {
        uid: uid.clone(),
        data: bytes::Bytes::from(payload_bytes),
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
                inputs: [("input".to_string(), "$request".to_string())].into(),
            },
            EnsembleStep {
                name: "step2".to_string(),
                model: "m2".to_string(),
                version: "1".to_string(),
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
                inputs: [("input".to_string(), "$step2".to_string())].into(),
            },
            EnsembleStep {
                name: "step2".to_string(),
                model: "m2".to_string(),
                version: "1".to_string(),
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
                inputs: [("x".to_string(), "$request".to_string())].into(),
            },
            EnsembleStep {
                name: "b".to_string(),
                model: "m2".to_string(),
                version: "1".to_string(),
                inputs: [("x".to_string(), "$request".to_string())].into(),
            },
            EnsembleStep {
                name: "c".to_string(),
                model: "m3".to_string(),
                version: "1".to_string(),
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
        assert_eq!(bytes, b"\x00\x01\x02", "binary payload must pass verbatim");
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
}
