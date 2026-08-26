//! Model version config read endpoint (M1, .claude/admin-enhancement-plan.md
//! §3.3).
//!
//! Returns the version's config.yaml as a JSON tree of *file* values — keys
//! not written in the file do not appear (CLI/default overlays are a
//! load-time concern, not file content). Secret values are redacted, and an
//! etag over the file content supports optimistic concurrency for the M2
//! PATCH.

use crate::error::AppError;
use crate::http::state::AppState;
use axum::extract::{Path, State};
use axum::response::Json;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Placeholder substituted for redacted secret values.
const REDACTED: &str = "***";

/// Subtree paths (dot-joined) replaced wholesale. List length is preserved
/// so clients can still show "N keys configured" without seeing the keys.
const REDACTED_SUBTREES: &[&str] = &["policies.auth.keys"];

/// Leaf keys whose scalar value is redacted at any depth (*_key / *_secret,
/// case-insensitive).
fn is_secret_leaf(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    k.ends_with("_key") || k.ends_with("_secret")
}

fn redact_subtree(value: &Value) -> Value {
    match value {
        Value::Array(items) => json!(vec![REDACTED; items.len()]),
        _ => json!(REDACTED),
    }
}

/// Walk the tree in place, redacting secrets and recording each redacted
/// path (dot-joined; array elements as `path[i]`).
fn redact_tree(path: &str, value: &mut Value, redacted: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                if REDACTED_SUBTREES.contains(&child_path.as_str()) {
                    *child = redact_subtree(child);
                    redacted.push(child_path);
                    continue;
                }
                if is_secret_leaf(key) && !child.is_object() && !child.is_array() {
                    *child = json!(REDACTED);
                    redacted.push(child_path);
                    continue;
                }
                redact_tree(&child_path, child, redacted);
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter_mut().enumerate() {
                redact_tree(&format!("{path}[{i}]"), item, redacted);
            }
        }
        _ => {}
    }
}

/// First 16 hex chars of the file content SHA-256 — changes iff the file
/// content changes (mtime-only etags would miss same-size rewrites within
/// the same second).
fn content_etag(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// Shared core for the HTTP handler and (M1) the gRPC GetModelConfig RPC.
pub async fn model_version_config_json(
    state: &AppState,
    model_name: &str,
    version: &str,
) -> Result<Value, AppError> {
    let dir = state.repo_path.join(model_name).join(version);
    let loaded = state.registry.get(model_name, Some(version));
    if !dir.is_dir() && loaded.is_none() {
        return Err(AppError::ModelNotFound(format!("{model_name}/{version}")));
    }

    let (mut config, etag, has_file) = match tokio::fs::read(dir.join("config.yaml")).await {
        Ok(bytes) => {
            let yaml: serde_yaml::Value = serde_yaml::from_slice(&bytes).map_err(|e| {
                AppError::Config(format!("invalid config.yaml for {model_name}/{version}: {e}"))
            })?;
            let value = serde_json::to_value(yaml)?;
            // An empty file parses as null — present it as an empty object.
            let value = if value.is_null() { json!({}) } else { value };
            (value, Some(content_etag(&bytes)), true)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (json!({}), None, false),
        Err(e) => return Err(AppError::Io(e)),
    };

    let mut redacted = Vec::new();
    redact_tree("", &mut config, &mut redacted);

    Ok(json!({
        "model": model_name,
        "version": version,
        "config": config,
        "has_file": has_file,
        "redacted": redacted,
        "etag": etag,
        "loaded_at": loaded
            .and_then(|mv| mv.loaded_at)
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs()),
    }))
}

pub async fn model_version_config_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    Ok(Json(model_version_config_json(&state, &model_name, &version).await?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;
    use std::path::{Path as FsPath, PathBuf};
    use std::sync::atomic::AtomicBool;

    // ===== redaction unit tests =====

    #[test]
    fn redacts_auth_keys_subtree_preserving_count() {
        let mut v = json!({"policies": {"auth": {"keys": ["k1", "k2", "k3"]}}});
        let mut redacted = Vec::new();
        redact_tree("", &mut v, &mut redacted);
        assert_eq!(v["policies"]["auth"]["keys"], json!(["***", "***", "***"]));
        assert_eq!(redacted, vec!["policies.auth.keys"]);
    }

    #[test]
    fn redacts_secret_leaf_keys_at_any_depth() {
        let mut v = json!({
            "api_key": "plain",
            "nested": {"client_secret": "s3cr3t", "API_KEY": "upper"},
            "list": [{"token_key": "in-array"}],
            "normal_key_like": {"key_size": 4}
        });
        let mut redacted = Vec::new();
        redact_tree("", &mut v, &mut redacted);
        assert_eq!(v["api_key"], json!("***"));
        assert_eq!(v["nested"]["client_secret"], json!("***"));
        assert_eq!(v["nested"]["API_KEY"], json!("***"));
        assert_eq!(v["list"][0]["token_key"], json!("***"));
        // Non-secret leaf containing "key" but not matching the suffix rules.
        assert_eq!(v["normal_key_like"]["key_size"], json!(4));
        assert!(redacted.contains(&"api_key".to_string()));
        assert!(redacted.contains(&"nested.client_secret".to_string()));
        assert!(redacted.contains(&"list[0].token_key".to_string()));
    }

    #[test]
    fn leaves_normal_values_untouched() {
        let mut v = json!({"max_batch_size": 16, "accelerator": "cuda", "devices": [0, 1]});
        let original = v.clone();
        let mut redacted = Vec::new();
        redact_tree("", &mut v, &mut redacted);
        assert_eq!(v, original);
        assert!(redacted.is_empty());
    }

    // ===== handler tests with a temp repository =====

    fn test_state(repo_path: PathBuf) -> Arc<AppState> {
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(crate::callback::CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            repo_path.clone(),
            inference_queue.clone(),
            "warn".to_string(),
            callback_runner.clone(),
        ));
        Arc::new(AppState::new(
            registry,
            worker_manager,
            inference_queue,
            Config::default(),
            repo_path,
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        ))
    }

    fn temp_repo(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lite-m1-config-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_config(repo: &FsPath, model: &str, version: &str, content: &str) {
        let dir = repo.join(model).join(version);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("config.yaml"), content).unwrap();
    }

    #[tokio::test]
    async fn returns_file_tree_and_stable_etag() {
        let repo = temp_repo("tree");
        write_config(&repo, "m", "1", "max_batch_size: 16\naccelerator: cuda\n");
        let state = test_state(repo.clone());

        let first = model_version_config_json(&state, "m", "1").await.unwrap();
        assert_eq!(first["config"]["max_batch_size"], json!(16));
        assert_eq!(first["config"]["accelerator"], json!("cuda"));
        assert_eq!(first["has_file"], json!(true));
        let etag = first["etag"].as_str().unwrap().to_string();
        assert_eq!(etag.len(), 16);

        let second = model_version_config_json(&state, "m", "1").await.unwrap();
        assert_eq!(second["etag"], json!(etag));

        write_config(&repo, "m", "1", "max_batch_size: 32\n");
        let third = model_version_config_json(&state, "m", "1").await.unwrap();
        assert_ne!(third["etag"], json!(etag));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn missing_config_file_returns_empty_tree() {
        let repo = temp_repo("nofile");
        std::fs::create_dir_all(repo.join("m").join("1")).unwrap();
        let state = test_state(repo.clone());

        let out = model_version_config_json(&state, "m", "1").await.unwrap();
        assert_eq!(out["config"], json!({}));
        assert_eq!(out["has_file"], json!(false));
        assert_eq!(out["etag"], Value::Null);

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn unknown_version_is_not_found() {
        let repo = temp_repo("unknown");
        let state = test_state(repo.clone());
        let err = model_version_config_json(&state, "ghost", "9").await.unwrap_err();
        assert!(matches!(err, AppError::ModelNotFound(_)));
        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn secrets_are_redacted_end_to_end() {
        let repo = temp_repo("redact");
        write_config(
            &repo,
            "m",
            "1",
            "max_batch_size: 8\npolicies:\n  auth:\n    keys: [alpha, beta]\nwebhook_secret: top\n",
        );
        let state = test_state(repo.clone());

        let out = model_version_config_json(&state, "m", "1").await.unwrap();
        assert_eq!(out["config"]["policies"]["auth"]["keys"], json!(["***", "***"]));
        assert_eq!(out["config"]["webhook_secret"], json!("***"));
        assert_eq!(out["config"]["max_batch_size"], json!(8));
        let redacted = out["redacted"].as_array().unwrap();
        assert!(redacted.contains(&json!("policies.auth.keys")));
        assert!(redacted.contains(&json!("webhook_secret")));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn invalid_yaml_is_config_error() {
        let repo = temp_repo("bad-yaml");
        write_config(&repo, "m", "1", "a: [1, 2\n  b: {unclosed\n");
        let state = test_state(repo.clone());
        let err = model_version_config_json(&state, "m", "1").await.unwrap_err();
        assert!(matches!(err, AppError::Config(_)));
        let _ = std::fs::remove_dir_all(&repo);
    }
}
