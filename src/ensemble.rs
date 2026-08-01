use crate::error::AppError;
use crate::http::state::AppState;
use crate::proto::liteserver as pb;
use crate::registry::types::ModelType;
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

fn resolve_ref(ref_str: &str, context: &HashMap<String, Value>) -> Result<Value, AppError> {
    let caps = REF_RE.captures(ref_str).ok_or_else(|| {
        AppError::Config(format!("invalid reference: {}", ref_str))
    })?;
    let source = caps.get(1).unwrap().as_str();
    let field = caps.get(2).map(|m| m.as_str());

    let source_data = context.get(source).ok_or_else(|| {
        AppError::Config(format!("reference source not found: {}", source))
    })?;

    match field {
        None => Ok(source_data.clone()),
        Some(f) => {
            source_data.get(f).cloned().ok_or_else(|| {
                AppError::Config(format!(
                    "cannot resolve '{}' from {}",
                    ref_str, source_data
                ))
            })
        }
    }
}

// ===== Execution =====

pub async fn execute_ensemble(
    state: Arc<AppState>,
    model_name: &str,
    version: &str,
    payload: Value,
    request_id: &str,
    client_ip: &str,
    deadline_unix_ns: Option<i64>,
) -> Result<Value, AppError> {
    let model_dir = crate::validation::resolve_model_dir(
        &state.repo_path, model_name, version,
    )?;
    let config_path = model_dir.join("config.yaml");

    let steps = parse_ensemble_config(&config_path).await?;
    let layers = topological_layers(&steps);

    let mut context: HashMap<String, Value> = HashMap::new();
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
            let mut set: tokio::task::JoinSet<(String, Result<Value, AppError>)> =
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
                        return Err(AppError::Internal(format!(
                            "ensemble step failed: {}", e
                        )));
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

async fn execute_step(
    state: Arc<AppState>,
    step: &EnsembleStep,
    context: &HashMap<String, Value>,
    request_id: &str,
    client_ip: &str,
    deadline_unix_ns: Option<i64>,
) -> Result<Value, AppError> {
    // Resolve inputs
    let mut payload = serde_json::Map::new();
    for (key, ref_str) in &step.inputs {
        let value = resolve_ref(ref_str, context)?;
        payload.insert(key.clone(), value);
    }

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
    let payload_value = Value::Object(payload);
    let payload_bytes = serde_json::to_vec(&payload_value).unwrap_or_default();

    // P-TRACE (蓝图 §4.3 ensemble 接线，防 trace 断裂): the sub-step RequestMeta
    // would otherwise carry empty headers, orphaning every step span from the
    // parent request trace. Build a child `ensemble.step` span linked to the
    // current (parent) trace and inject its context into the step headers so the
    // worker spans land as children of the step (request_id is already
    // `{parent}:{step}`, trace follows the same shape).
    let mut step_headers = HashMap::new();
    {
        let step_span = tracing::info_span!(
            "ensemble.step",
            step = %step.name,
            model = %step.model,
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
                    let data = if single.data.is_empty() {
                        json!({})
                    } else {
                        serde_json::from_slice(&single.data).unwrap_or(json!({}))
                    };
                    Ok(data)
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
        context.insert("request".to_string(), json!({"image": "cat.jpg"}));
        context.insert("step1".to_string(), json!({"output": 42}));

        assert_eq!(resolve_ref("$request", &context).unwrap(), json!({"image": "cat.jpg"}));
        assert_eq!(resolve_ref("$request.image", &context).unwrap(), json!("cat.jpg"));
        assert_eq!(resolve_ref("$step1.output", &context).unwrap(), json!(42));
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
}
