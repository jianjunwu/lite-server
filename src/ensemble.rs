use crate::error::AppError;
use crate::http::state::AppState;
use crate::registry::types::{ModelType, VersionStatus};
use crate::transport::uds::send_to_worker;
use crate::worker::pick_worker_random;
use crate::worker::protocol::{InferenceRequest, RequestPayload, ResponseStatus};
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{timeout, Duration};
use tracing::{error, info, warn};

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

pub fn parse_ensemble_config(config_path: &PathBuf) -> Result<Vec<EnsembleStep>, AppError> {
    let content = std::fs::read_to_string(config_path)
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
        let deps = dependencies.entry(&step.name).or_insert_with(HashSet::new);
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
        let deps = dependencies.entry(&step.name).or_insert_with(HashSet::new);
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
) -> Result<Value, AppError> {
    let model_dir = state.repo_path.join(model_name).join(version);
    let config_path = model_dir.join("config.yaml");

    let steps = parse_ensemble_config(&config_path)?;
    let layers = topological_layers(&steps);

    let mut context: HashMap<String, Value> = HashMap::new();
    context.insert("request".to_string(), payload);

    for layer in layers {
        let mut futures = Vec::new();
        for step in layer {
            let state = state.clone();
            let ctx = context.clone();
            let step = step.clone();
            let ensemble_name = model_name.to_string();
            futures.push(tokio::spawn(async move {
                let start = Instant::now();
                let result = execute_step(state, &step, &ctx).await;
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
) -> Result<Value, AppError> {
    // Resolve inputs
    let mut payload = serde_json::Map::new();
    for (key, ref_str) in &step.inputs {
        let value = resolve_ref(ref_str, context)?;
        payload.insert(key.clone(), value);
    }

    // Ensure sub-model is ready
    if !state.registry.is_ready(&step.model, Some(&step.version)).await {
        info!("Auto-loading sub-model {} v{} for ensemble", step.model, step.version);
        let config = crate::config::load_model_config(
            &state.repo_path.join(&step.model).join(&step.version).join("config.yaml")
        ).unwrap_or_default();
        if let Err(e) = state.worker_manager.load_model(&step.model, &step.version, &config).await {
            warn!("Failed to auto-load sub-model {} v{}: {}", step.model, step.version, e);
            return Err(AppError::ModelNotReady(format!(
                "sub-model {} v{} not ready: {}", step.model, step.version, e
            )));
        }
        // Wait briefly for worker startup
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }

    if !state.registry.is_ready(&step.model, Some(&step.version)).await {
        return Err(AppError::ModelNotReady(format!(
            "sub-model {} v{} is not ready", step.model, step.version
        )));
    }

    // Get worker info
    let mv = state.registry.get(&step.model, Some(&step.version)).await
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", step.model, step.version)))?;

    let num_workers = mv.workers.len();
    if num_workers == 0 {
        return Err(AppError::WorkerCrashed(format!("{} has no workers", step.model)));
    }

    let worker_id = pick_worker_random(num_workers);
    let worker = &mv.workers[worker_id];
    let uds_path = worker.uds_path.clone();

    // Send inference request
    let uid = format!("ensemble_{}_{}_{}", step.model, step.version, uuid::Uuid::new_v4());
    let request = InferenceRequest {
        uid,
        payload: RequestPayload::Infer { data: Value::Object(payload) },
    };

    let timeout_duration = Duration::from_secs_f64(state.config.server.timeout as f64);
    let response = match timeout(timeout_duration, send_to_worker(&uds_path, request)).await {
        Ok(Ok(resp)) => resp,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(AppError::InferenceTimeout(format!(
            "ensemble step {} timed out", step.name
        ))),
    };

    match response.status.code.as_str() {
        "Ok" => Ok(response.data.unwrap_or(json!({}))),
        _ => Err(AppError::WorkerCrashed(
            response.status.message.unwrap_or_else(|| "ensemble step inference error".to_string())
        )),
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
