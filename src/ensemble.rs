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
) -> Result<Value, AppError> {
    let model_dir = crate::validation::resolve_model_dir(
        &state.repo_path, model_name, version,
    )?;
    let config_path = model_dir.join("config.yaml");

    let steps = parse_ensemble_config(&config_path).await?;
    let layers = topological_layers(&steps);

    let mut context: HashMap<String, Value> = HashMap::new();
    context.insert("request".to_string(), payload);

    // #3: bound the WHOLE ensemble by a single deadline equal to one request's
    // budget (server.timeout). Layers run serially, so without this an N-layer
    // ensemble could otherwise run up to N × server.timeout — far longer than
    // the parent request that triggered it (itself capped at server.timeout;
    // see http/handlers.rs). The per-step timeout in execute_step stays as an
    // inner safety net; this outer deadline is what actually bounds the total.
    let total_budget = Duration::from_secs_f64(state.config.server.timeout as f64);
    let ensemble_run = async {
        for layer in layers {
            let mut futures = Vec::new();
            for step in layer {
                let state = state.clone();
                let ctx = context.clone();
                let step = step.clone();
                let ensemble_name = model_name.to_string();
                let request_id = request_id.to_string();
                futures.push(tokio::spawn(async move {
                    let start = Instant::now();
                    let result = execute_step(state, &step, &ctx, &request_id).await;
                    let latency = start.elapsed().as_secs_f64();
                    crate::metrics::prometheus::record_ensemble_step_latency(
                        &ensemble_name, &step.name, &step.model, latency,
                    );
                    (step.name, result)
                }));
            }

            for handle in futures {
                let (name, result) = handle.await.map_err(|e| {
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

    tokio::time::timeout(total_budget, ensemble_run)
        .await
        .map_err(|_| AppError::InferenceTimeout(format!(
            "ensemble {} {} exceeded total timeout of {:.1}s",
            model_name, version, total_budget.as_secs_f64()
        )))??;

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
        let config = crate::config::load_model_config(
            &sub_model_dir.join("config.yaml")
        ).unwrap_or_default();
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

    let meta = pb::RequestMeta {
        route: "/predict".to_string(),
        headers: HashMap::new(),
        client_ip: "".to_string(),
        // Correlate sub-step requests with the client-facing request ID;
        // the step-name suffix keeps each step uniquely identifiable.
        request_id: format!("{}:{}", request_id, step.name),
        timestamp_ns: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64,
        payload: bytes::Bytes::from(payload_bytes.clone()),
        ..Default::default()
    };

    let (response_tx, response_rx) = oneshot::channel();
    let item = crate::inference_queue::QueueItem {
        uid: uid.clone(),
        data: bytes::Bytes::from(payload_bytes),
        meta: Some(std::sync::Arc::new(meta)),
        response_tx,
        inflight_guard: None,
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

    let timeout_duration = Duration::from_secs_f64(state.config.server.timeout as f64);
    let response = match timeout(timeout_duration, response_rx).await {
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
}
