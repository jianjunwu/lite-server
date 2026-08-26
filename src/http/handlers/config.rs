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

/// Server config secrets (M5): the literal secret leaf of every
/// `EndpointControl::Key` cell, plus telemetry OTLP auth headers (a
/// header→token map, redacted wholesale). The env-var NAME (`value_env`) and
/// file PATH (`value_file`) variants are not secrets and stay visible.
const SERVER_REDACTED_SUBTREES: &[&str] = &[
    "access_control.admin.http.value",
    "access_control.admin.grpc.value",
    "access_control.inference.http.value",
    "access_control.inference.grpc.value",
    "access_control.health.value",
    "openai_compact.auth.value",
    "telemetry.otlp_headers",
];

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

/// True for null / empty object / empty array — a subtree that carries no
/// secret. Redacting it to "***" would falsely imply a configured secret
/// (and flip its source label to "file").
fn is_empty_value(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Object(m) => m.is_empty(),
        Value::Array(a) => a.is_empty(),
        _ => false,
    }
}

/// Walk the tree in place, redacting secrets and recording each redacted
/// path (dot-joined; array elements as `path[i]`). `subtrees` lists
/// dot-joined paths replaced wholesale (see [`REDACTED_SUBTREES`] /
/// [`SERVER_REDACTED_SUBTREES`]); `*_key` / `*_secret` scalar leaves are
/// always redacted.
pub(crate) fn redact_tree(
    path: &str,
    value: &mut Value,
    redacted: &mut Vec<String>,
    subtrees: &[&str],
) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                if subtrees.contains(&child_path.as_str()) && !is_empty_value(child) {
                    *child = redact_subtree(child);
                    redacted.push(child_path);
                    continue;
                }
                if is_secret_leaf(key) && !child.is_object() && !child.is_array() {
                    *child = json!(REDACTED);
                    redacted.push(child_path);
                    continue;
                }
                redact_tree(&child_path, child, redacted, subtrees);
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter_mut().enumerate() {
                redact_tree(&format!("{path}[{i}]"), item, redacted, subtrees);
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
    redact_tree("", &mut config, &mut redacted, REDACTED_SUBTREES);

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

// ===== M5: server (instance) config read — plan §3.6 =====

/// Per-leaf source attribution. Deliberately approximate (plan §3.6): a leaf
/// whose path the CLI override structure sets is "cli"; of the rest, a leaf
/// equal to the built-in `Config::default()` reads as "default" and anything
/// else as "file". A value explicitly written into the file that happens to
/// equal the default is therefore labeled "default" — full provenance
/// tracking is not worth the framework for a read-only view.
fn attribute_sources(
    path: &str,
    effective: &Value,
    default: &Value,
    cli: &std::collections::BTreeSet<String>,
    out: &mut std::collections::BTreeMap<String, String>,
) {
    if let Value::Object(map) = effective {
        for (key, child) in map {
            let child_path = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            let fallback = Value::Null;
            let default_child = default.get(key).unwrap_or(&fallback);
            attribute_sources(&child_path, child, default_child, cli, out);
        }
        return;
    }
    // Leaves (scalars, arrays, nulls) compare wholesale.
    let source = if cli.contains(path) {
        "cli"
    } else if effective == default {
        "default"
    } else {
        "file"
    };
    out.insert(path.to_string(), source.to_string());
}

/// Shared core for the HTTP handler and the gRPC GetServerConfig RPC.
///
/// Sources are attributed on the REDACTED tree so every row the client
/// renders has an exact `sources` entry; a redacted secret never equals the
/// default tree's null/empty placeholder, so configured secrets read "file".
pub fn server_config_json(state: &AppState) -> Result<Value, AppError> {
    let mut config = serde_json::to_value(&state.config)
        .map_err(|e| AppError::Internal(format!("config serialize: {e}")))?;
    let mut redacted = Vec::new();
    redact_tree("", &mut config, &mut redacted, SERVER_REDACTED_SUBTREES);

    let default = serde_json::to_value(crate::config::Config::default())
        .map_err(|e| AppError::Internal(format!("default config serialize: {e}")))?;
    let cli = state.cli_overrides.overridden_paths();
    let mut sources = std::collections::BTreeMap::new();
    attribute_sources("", &config, &default, &cli, &mut sources);

    Ok(json!({
        "config": config,
        "sources": sources,
        "redacted": redacted,
    }))
}

pub async fn server_config_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(server_config_json(&state)?))
}

// ===== M2: PATCH (write) — plan §3.3 =====

/// Write mode for the config PATCH.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigPatchMode {
    /// Write, then run the lifecycle reload chain; roll the file back when
    /// the reload fails. The default.
    ApplyReload,
    /// Atomic write only; the in-memory config changes on the next reload.
    WriteOnly,
    /// Validate the merged result; nothing is written.
    DryRun,
}

impl Default for ConfigPatchMode {
    fn default() -> Self {
        Self::ApplyReload
    }
}

impl ConfigPatchMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::ApplyReload => "apply_reload",
            Self::WriteOnly => "write_only",
            Self::DryRun => "dry_run",
        }
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct ConfigPatchRequest {
    /// RFC 7386 JSON merge-patch against the on-disk config.yaml tree.
    pub patch: Value,
    /// Optimistic-concurrency precondition: the etag from the last GET.
    #[serde(default)]
    pub if_match: Option<String>,
    /// Bypass the etag precondition (same convention as delete ?force=true).
    #[serde(default)]
    pub force: bool,
    #[serde(default)]
    pub mode: ConfigPatchMode,
}

/// PATCH failures that carry machine-readable extras the plain AppError
/// envelope cannot express (current_etag / rolled_back / warnings).
#[derive(Debug)]
pub enum ConfigPatchError {
    /// Plain errors (not found, io, …) mapped by the usual machinery.
    App(AppError),
    /// etag precondition failed; carries the file's current etag.
    Conflict { current_etag: Option<String> },
    /// The merged config is unusable; nothing was written.
    Invalid { message: String, warnings: Vec<String> },
    /// The reload after a successful write failed; the file was rolled back
    /// to its pre-write bytes.
    ReloadFailed { message: String },
}

/// RFC 7386 JSON merge-patch: objects merge recursively, null deletes a key,
/// any non-object patch value replaces the target wholesale.
pub(crate) fn merge_patch(target: &mut Value, patch: &Value) {
    if !patch.is_object() {
        *target = patch.clone();
        return;
    }
    if !target.is_object() {
        *target = json!({});
    }
    let t = target.as_object_mut().expect("checked object above");
    for (key, value) in patch.as_object().expect("checked object above") {
        if value.is_null() {
            t.remove(key);
        } else {
            merge_patch(t.entry(key.clone()).or_insert(Value::Null), value);
        }
    }
}

/// Top-level keys ModelConfig actually deserializes — anything else in the
/// file is silently ignored on load, so the PATCH response warns about it.
/// Derived from serialization so future fields stay covered automatically.
fn known_model_config_keys() -> std::collections::BTreeSet<String> {
    match serde_yaml::to_value(crate::config::ModelConfig::default()) {
        Ok(serde_yaml::Value::Mapping(m)) => m
            .into_iter()
            .filter_map(|(k, _)| k.as_str().map(String::from))
            .collect(),
        _ => Default::default(),
    }
}

/// Write bytes to `path` atomically: temp file in the same directory, fsync,
/// then rename (mirrors python/lite_server/profile/config_writer.py).
async fn atomic_write(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("config.yaml");
    let tmp = path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()));
    let write_result = async {
        let mut f = tokio::fs::File::create(&tmp).await?;
        tokio::io::AsyncWriteExt::write_all(&mut f, bytes).await?;
        tokio::io::AsyncWriteExt::flush(&mut f).await?;
        f.sync_all().await?;
        drop(f);
        tokio::fs::rename(&tmp, path).await
    }
    .await;
    if write_result.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    write_result
}

/// A validated, not-yet-written patch.
pub(crate) struct PreparedPatch {
    path: std::path::PathBuf,
    /// Pre-write file bytes; None when the file did not exist.
    original: Option<Vec<u8>>,
    merged_yaml: String,
    new_etag: String,
    warnings: Vec<String>,
    model: String,
    version: String,
    mode: ConfigPatchMode,
}

impl PreparedPatch {
    fn response(&self, reloaded: bool, extra_warnings: &[&str]) -> Value {
        let mut warnings = self.warnings.clone();
        warnings.extend(extra_warnings.iter().map(|s| s.to_string()));
        json!({
            "model": self.model,
            "version": self.version,
            "mode": self.mode.as_str(),
            "valid": true,
            "written": true,
            "reloaded": reloaded,
            "etag": self.new_etag,
            "warnings": warnings,
        })
    }

    /// Restore the file to its pre-write state (byte-exact; a file the patch
    /// created is removed). The .bak backup is kept either way.
    async fn rollback(&self) {
        let result = match &self.original {
            Some(bytes) => atomic_write(&self.path, bytes).await,
            None => tokio::fs::remove_file(&self.path).await,
        };
        if let Err(e) = result {
            tracing::error!(
                "config rollback failed for {}/{}: {} — restore {} manually",
                self.model,
                self.version,
                e,
                self.path.with_file_name("config.yaml.bak").display(),
            );
        }
    }
}

pub(crate) enum PrepareOutcome {
    /// dry_run terminal report (no write).
    DryRun(Value),
    Ready(Box<PreparedPatch>),
}

/// Shared prepare phase for the PATCH: read the file tree, check the etag
/// precondition, merge the patch, serialize and validate the result.
pub(crate) async fn prepare_config_patch(
    state: &AppState,
    model_name: &str,
    version: &str,
    req: &ConfigPatchRequest,
) -> Result<PrepareOutcome, ConfigPatchError> {
    let dir = state.repo_path.join(model_name).join(version);
    if !dir.is_dir() && state.registry.get(model_name, Some(version)).is_none() {
        return Err(ConfigPatchError::App(AppError::ModelNotFound(format!(
            "{model_name}/{version}"
        ))));
    }
    if !req.patch.is_object() {
        return Err(ConfigPatchError::Invalid {
            message: "patch must be a JSON object (RFC 7386 merge-patch)".to_string(),
            warnings: vec![],
        });
    }

    let path = dir.join("config.yaml");
    let original: Option<Vec<u8>> = match tokio::fs::read(&path).await {
        Ok(bytes) => Some(bytes),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(ConfigPatchError::App(AppError::Io(e))),
    };
    let current_etag = original.as_deref().map(content_etag);

    if !req.force {
        if let Some(if_match) = &req.if_match {
            if Some(if_match.as_str()) != current_etag.as_deref() {
                return Err(ConfigPatchError::Conflict { current_etag });
            }
        }
    }

    // Merge against the on-disk tree — never the in-memory ModelConfig
    // (plan §3.3 invariant: memory state is produced by load/reload only).
    let mut tree: Value = match &original {
        Some(bytes) => {
            let yaml: serde_yaml::Value =
                serde_yaml::from_slice(bytes).map_err(|e| ConfigPatchError::Invalid {
                    message: format!("existing config.yaml is unparseable, refusing to patch: {e}"),
                    warnings: vec![],
                })?;
            let value = serde_json::to_value(yaml).map_err(AppError::from).map_err(ConfigPatchError::App)?;
            if value.is_null() { json!({}) } else { value }
        }
        None => json!({}),
    };
    merge_patch(&mut tree, &req.patch);

    let merged_yaml = serde_yaml::to_string(&tree)
        .map_err(|e| ConfigPatchError::App(AppError::Internal(format!("yaml serialize: {e}"))))?;

    // Warnings before validation so a dry-run of an invalid config still
    // reports unknown keys alongside the parse error.
    let mut warnings: Vec<String> = Vec::new();
    if let Value::Object(map) = &tree {
        let known = known_model_config_keys();
        for key in map.keys() {
            if !known.contains(key) {
                warnings.push(format!(
                    "unknown key `{key}` is not a ModelConfig field and is ignored on load"
                ));
            }
        }
    }

    if let Err(e) = crate::config::validate_model_config_yaml(&merged_yaml) {
        if req.mode == ConfigPatchMode::DryRun {
            let mut w = vec![format!("merged config is invalid: {e}")];
            w.extend(warnings);
            return Ok(PrepareOutcome::DryRun(json!({
                "model": model_name,
                "version": version,
                "mode": req.mode.as_str(),
                "valid": false,
                "written": false,
                "reloaded": false,
                "etag": current_etag,
                "warnings": w,
            })));
        }
        return Err(ConfigPatchError::Invalid {
            message: format!("merged config is invalid: {e}"),
            warnings,
        });
    }

    if req.mode == ConfigPatchMode::DryRun {
        return Ok(PrepareOutcome::DryRun(json!({
            "model": model_name,
            "version": version,
            "mode": req.mode.as_str(),
            "valid": true,
            "written": false,
            "reloaded": false,
            "etag": current_etag,
            "warnings": warnings,
        })));
    }

    let new_etag = content_etag(merged_yaml.as_bytes());
    Ok(PrepareOutcome::Ready(Box::new(PreparedPatch {
        path,
        original,
        merged_yaml,
        new_etag,
        warnings,
        model: model_name.to_string(),
        version: version.to_string(),
        mode: req.mode,
    })))
}

/// Persist a prepared patch: back up the current file, then atomically swap.
pub(crate) async fn write_prepared(prepared: &PreparedPatch) -> Result<(), AppError> {
    if let Some(bytes) = &prepared.original {
        let bak = prepared.path.with_file_name("config.yaml.bak");
        tokio::fs::write(&bak, bytes).await.map_err(AppError::Io)?;
    }
    atomic_write(&prepared.path, prepared.merged_yaml.as_bytes()).await.map_err(AppError::Io)
}

/// Fold the reload outcome into the PATCH response. The write already
/// happened; a failed reload rolls the file back to its pre-write bytes.
pub(crate) async fn finish_apply_reload(
    prepared: PreparedPatch,
    reload: Result<bool, AppError>,
) -> Result<Value, ConfigPatchError> {
    match reload {
        Ok(true) => Ok(prepared.response(true, &[])),
        Ok(false) => Ok(prepared.response(
            false,
            &["version not loaded; written to disk only, a later load picks it up"],
        )),
        Err(e) => {
            prepared.rollback().await;
            Err(ConfigPatchError::ReloadFailed {
                message: format!("{e}"),
            })
        }
    }
}

/// Shared core for the HTTP PATCH handler and the gRPC UpdateModelConfig RPC.
/// The response carries status/etag/warnings only — never the patch content,
/// so patched secrets are not echoed back.
pub async fn model_version_config_patch(
    state: &AppState,
    model_name: &str,
    version: &str,
    req: &ConfigPatchRequest,
) -> Result<Value, ConfigPatchError> {
    let prepared = match prepare_config_patch(state, model_name, version, req).await? {
        PrepareOutcome::DryRun(report) => return Ok(report),
        PrepareOutcome::Ready(p) => p,
    };
    write_prepared(&prepared).await.map_err(ConfigPatchError::App)?;
    if req.mode == ConfigPatchMode::WriteOnly {
        return Ok(prepared.response(
            false,
            &["written to disk; the in-memory config changes on the next reload"],
        ));
    }
    let reload = state
        .worker_manager
        .reload_model(model_name, Some(version))
        .await;
    finish_apply_reload(*prepared, reload).await
}

pub async fn model_version_config_patch_handler(
    State(state): State<Arc<AppState>>,
    Path((model_name, version)): Path<(String, String)>,
    cx: crate::request_context::RequestContext,
    Json(req): Json<ConfigPatchRequest>,
) -> Result<axum::response::Response, AppError> {
    use axum::response::IntoResponse;
    crate::validation::validate_identifier(&model_name)?;
    crate::validation::validate_version(&version)?;
    match model_version_config_patch(&state, &model_name, &version, &req).await {
        Ok(out) => {
            crate::audit::control_plane(
                Some(&cx),
                &state.access_control,
                crate::callback::Protocol::Http,
                "config_update",
                &model_name,
                Some(&version),
                &format!("mode={} reloaded={}", req.mode.as_str(), out["reloaded"]),
            );
            Ok(Json(out).into_response())
        }
        Err(ConfigPatchError::App(e)) => Err(e),
        Err(ConfigPatchError::Conflict { current_etag }) => Ok((
            axum::http::StatusCode::CONFLICT,
            Json(json!({
                "error": {
                    "type": "conflict_error",
                    "message": "config.yaml changed since the provided etag; re-read and retry, or set force",
                    "code": "conflict",
                    "param": Value::Null,
                },
                "current_etag": current_etag,
            })),
        )
            .into_response()),
        Err(ConfigPatchError::Invalid { message, warnings }) => Ok((
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({
                "error": {
                    "type": "invalid_request_error",
                    "message": message,
                    "code": "invalid_parameter_value",
                    "param": Value::Null,
                },
                "warnings": warnings,
            })),
        )
            .into_response()),
        Err(ConfigPatchError::ReloadFailed { message }) => Ok((
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({
                "error": {
                    "type": "invalid_request_error",
                    "message": format!("reload failed: {message}; config.yaml rolled back to the previous content"),
                    "code": "reload_failed",
                    "param": Value::Null,
                },
                "rolled_back": true,
            })),
        )
            .into_response()),
    }
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
        redact_tree("", &mut v, &mut redacted, REDACTED_SUBTREES);
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
        redact_tree("", &mut v, &mut redacted, REDACTED_SUBTREES);
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
    fn empty_redacted_subtrees_are_left_visible() {
        // An unset/empty secret container must not collapse to "***" — that
        // would falsely imply a configured secret.
        let mut v = json!({
            "telemetry": {"otlp_headers": {}},
            "access_control": {"health": null},
            "policies": {"auth": {"keys": []}}
        });
        let mut redacted = Vec::new();
        redact_tree("", &mut v, &mut redacted, SERVER_REDACTED_SUBTREES);
        assert_eq!(v["telemetry"]["otlp_headers"], json!({}));
        assert_eq!(v["access_control"]["health"], Value::Null);
        assert!(redacted.is_empty());

        let mut redacted = Vec::new();
        redact_tree("", &mut v, &mut redacted, REDACTED_SUBTREES);
        assert_eq!(v["policies"]["auth"]["keys"], json!([]));
        assert!(redacted.is_empty());
    }

    #[test]
    fn leaves_normal_values_untouched() {
        let mut v = json!({"max_batch_size": 16, "accelerator": "cuda", "devices": [0, 1]});
        let original = v.clone();
        let mut redacted = Vec::new();
        redact_tree("", &mut v, &mut redacted, REDACTED_SUBTREES);
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

    // ===== M2: merge-patch unit tests =====

    #[test]
    fn merge_patch_merges_nested_objects() {
        let mut target = json!({"a": {"b": 1, "c": 2}, "d": 3});
        merge_patch(&mut target, &json!({"a": {"b": 10}, "e": 4}));
        assert_eq!(target, json!({"a": {"b": 10, "c": 2}, "d": 3, "e": 4}));
    }

    #[test]
    fn merge_patch_null_deletes_keys() {
        let mut target = json!({"a": {"b": 1, "c": 2}, "d": 3});
        merge_patch(&mut target, &json!({"a": {"b": null}, "d": null}));
        assert_eq!(target, json!({"a": {"c": 2}}));
    }

    #[test]
    fn merge_patch_replaces_scalars_and_arrays_wholesale() {
        let mut target = json!({"devices": [0, 1], "n": 1});
        merge_patch(&mut target, &json!({"devices": [2], "n": {"nested": true}}));
        assert_eq!(target, json!({"devices": [2], "n": {"nested": true}}));
    }

    #[test]
    fn merge_patch_object_over_non_object_target() {
        // RFC 7386: a non-object target is treated as an empty object; null
        // members of the patch are dropped, not added.
        let mut target = json!(5);
        merge_patch(&mut target, &json!({"a": 1, "b": null}));
        assert_eq!(target, json!({"a": 1}));
    }

    // ===== M2: PATCH core tests =====

    fn patch_req(patch: Value, mode: ConfigPatchMode) -> ConfigPatchRequest {
        ConfigPatchRequest {
            patch,
            if_match: None,
            force: false,
            mode,
        }
    }

    fn read_file(repo: &FsPath, model: &str, version: &str, name: &str) -> Option<String> {
        std::fs::read_to_string(repo.join(model).join(version).join(name)).ok()
    }

    #[tokio::test]
    async fn dry_run_validates_without_writing() {
        let repo = temp_repo("dry-ok");
        write_config(&repo, "m", "1", "max_batch_size: 16\n");
        let state = test_state(repo.clone());

        let out = model_version_config_patch(
            &state,
            "m",
            "1",
            &patch_req(json!({"max_batch_size": 32}), ConfigPatchMode::DryRun),
        )
        .await
        .unwrap();
        assert_eq!(out["valid"], json!(true));
        assert_eq!(out["written"], json!(false));
        assert_eq!(out["reloaded"], json!(false));
        // Nothing written: disk and etag are untouched.
        assert_eq!(read_file(&repo, "m", "1", "config.yaml").as_deref(), Some("max_batch_size: 16\n"));
        assert!(read_file(&repo, "m", "1", "config.yaml.bak").is_none());

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn dry_run_reports_invalid_merged_config() {
        let repo = temp_repo("dry-bad");
        write_config(&repo, "m", "1", "max_batch_size: 16\n");
        let state = test_state(repo.clone());

        let out = model_version_config_patch(
            &state,
            "m",
            "1",
            &patch_req(json!({"max_batch_size": "not-a-number"}), ConfigPatchMode::DryRun),
        )
        .await
        .unwrap();
        assert_eq!(out["valid"], json!(false));
        assert!(!out["warnings"].as_array().unwrap().is_empty());
        assert_eq!(read_file(&repo, "m", "1", "config.yaml").as_deref(), Some("max_batch_size: 16\n"));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn write_only_writes_atomically_with_backup() {
        let repo = temp_repo("write-only");
        write_config(&repo, "m", "1", "max_batch_size: 16\nbatch_timeout: 0.1\n");
        let state = test_state(repo.clone());

        let out = model_version_config_patch(
            &state,
            "m",
            "1",
            &patch_req(json!({"max_batch_size": 32}), ConfigPatchMode::WriteOnly),
        )
        .await
        .unwrap();
        assert_eq!(out["written"], json!(true));
        assert_eq!(out["reloaded"], json!(false));
        let new_etag = out["etag"].as_str().unwrap().to_string();
        assert_eq!(new_etag.len(), 16);

        // The merged file carries the patch and preserves untouched keys.
        let merged = read_file(&repo, "m", "1", "config.yaml").unwrap();
        let merged_yaml: serde_yaml::Value = serde_yaml::from_str(&merged).unwrap();
        assert_eq!(merged_yaml["max_batch_size"], serde_yaml::Value::from(32));
        assert!(merged_yaml.get("batch_timeout").is_some());
        // The pre-write content is backed up.
        assert_eq!(
            read_file(&repo, "m", "1", "config.yaml.bak").as_deref(),
            Some("max_batch_size: 16\nbatch_timeout: 0.1\n")
        );
        // GET sees the new etag.
        let get = model_version_config_json(&state, "m", "1").await.unwrap();
        assert_eq!(get["etag"], json!(new_etag));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn write_only_creates_file_when_missing() {
        let repo = temp_repo("write-create");
        std::fs::create_dir_all(repo.join("m").join("1")).unwrap();
        let state = test_state(repo.clone());

        let out = model_version_config_patch(
            &state,
            "m",
            "1",
            &patch_req(json!({"max_batch_size": 8}), ConfigPatchMode::WriteOnly),
        )
        .await
        .unwrap();
        assert_eq!(out["written"], json!(true));
        let merged = read_file(&repo, "m", "1", "config.yaml").unwrap();
        let merged_yaml: serde_yaml::Value = serde_yaml::from_str(&merged).unwrap();
        assert_eq!(merged_yaml["max_batch_size"], serde_yaml::Value::from(8));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn write_only_rejects_invalid_merged_config_without_writing() {
        let repo = temp_repo("write-bad");
        write_config(&repo, "m", "1", "max_batch_size: 16\n");
        let state = test_state(repo.clone());

        let err = model_version_config_patch(
            &state,
            "m",
            "1",
            &patch_req(json!({"max_batch_size": "nope"}), ConfigPatchMode::WriteOnly),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ConfigPatchError::Invalid { .. }));
        assert_eq!(read_file(&repo, "m", "1", "config.yaml").as_deref(), Some("max_batch_size: 16\n"));
        assert!(read_file(&repo, "m", "1", "config.yaml.bak").is_none());

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn merge_patch_null_removes_key_from_file() {
        let repo = temp_repo("null-delete");
        write_config(&repo, "m", "1", "max_batch_size: 16\nhot_reload: true\n");
        let state = test_state(repo.clone());

        model_version_config_patch(
            &state,
            "m",
            "1",
            &patch_req(json!({"hot_reload": null}), ConfigPatchMode::WriteOnly),
        )
        .await
        .unwrap();
        let merged = read_file(&repo, "m", "1", "config.yaml").unwrap();
        let merged_yaml: serde_yaml::Value = serde_yaml::from_str(&merged).unwrap();
        assert!(merged_yaml.get("hot_reload").is_none());
        assert_eq!(merged_yaml["max_batch_size"], serde_yaml::Value::from(16));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn unknown_keys_produce_warnings() {
        let repo = temp_repo("unknown-keys");
        write_config(&repo, "m", "1", "max_batch_size: 16\n");
        let state = test_state(repo.clone());

        let out = model_version_config_patch(
            &state,
            "m",
            "1",
            &patch_req(json!({"totally_custom_key": 1}), ConfigPatchMode::DryRun),
        )
        .await
        .unwrap();
        assert_eq!(out["valid"], json!(true));
        let warnings = out["warnings"].as_array().unwrap();
        assert!(warnings.iter().any(|w| w.as_str().unwrap().contains("totally_custom_key")));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn stale_etag_is_conflict_and_force_bypasses() {
        let repo = temp_repo("etag");
        write_config(&repo, "m", "1", "max_batch_size: 16\n");
        let state = test_state(repo.clone());

        let req = ConfigPatchRequest {
            patch: json!({"max_batch_size": 32}),
            if_match: Some("0000000000000000".to_string()),
            force: false,
            mode: ConfigPatchMode::WriteOnly,
        };
        let err = model_version_config_patch(&state, "m", "1", &req).await.unwrap_err();
        match err {
            ConfigPatchError::Conflict { current_etag } => {
                assert_eq!(current_etag.as_deref().map(str::len), Some(16));
            }
            other => panic!("expected conflict, got {other:?}"),
        }
        // Nothing written on conflict.
        assert_eq!(read_file(&repo, "m", "1", "config.yaml").as_deref(), Some("max_batch_size: 16\n"));

        let forced = ConfigPatchRequest { force: true, ..req };
        let out = model_version_config_patch(&state, "m", "1", &forced).await.unwrap();
        assert_eq!(out["written"], json!(true));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn matching_etag_passes() {
        let repo = temp_repo("etag-ok");
        write_config(&repo, "m", "1", "max_batch_size: 16\n");
        let state = test_state(repo.clone());
        let etag = model_version_config_json(&state, "m", "1").await.unwrap()["etag"]
            .as_str()
            .unwrap()
            .to_string();

        let req = ConfigPatchRequest {
            patch: json!({"max_batch_size": 32}),
            if_match: Some(etag),
            force: false,
            mode: ConfigPatchMode::WriteOnly,
        };
        let out = model_version_config_patch(&state, "m", "1", &req).await.unwrap();
        assert_eq!(out["written"], json!(true));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn apply_reload_on_unloaded_version_writes_without_reload() {
        // The registry is empty, so the lifecycle reload is a no-op
        // (Ok(false)) — the write still lands and the response says so.
        let repo = temp_repo("reload-unloaded");
        write_config(&repo, "m", "1", "max_batch_size: 16\n");
        let state = test_state(repo.clone());

        let out = model_version_config_patch(
            &state,
            "m",
            "1",
            &patch_req(json!({"max_batch_size": 32}), ConfigPatchMode::ApplyReload),
        )
        .await
        .unwrap();
        assert_eq!(out["written"], json!(true));
        assert_eq!(out["reloaded"], json!(false));
        assert!(out["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w.as_str().unwrap().contains("not loaded")));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn reload_failure_rolls_file_back() {
        let repo = temp_repo("rollback");
        write_config(&repo, "m", "1", "max_batch_size: 16\n");
        let state = test_state(repo.clone());

        // Drive the write through prepare+write, then feed finish_apply_reload
        // a synthetic reload failure (a real reload spawns Python workers —
        // too heavy for a unit test; lifecycle tests cover the chain itself).
        let prepared = match prepare_config_patch(
            &state,
            "m",
            "1",
            &patch_req(json!({"max_batch_size": 32}), ConfigPatchMode::ApplyReload),
        )
        .await
        .unwrap()
        {
            PrepareOutcome::Ready(p) => p,
            PrepareOutcome::DryRun(_) => panic!("apply_reload never yields a dry-run report"),
        };
        write_prepared(&prepared).await.unwrap();
        let err = finish_apply_reload(*prepared, Err(AppError::Config("worker refused".to_string())))
            .await
            .unwrap_err();
        match err {
            ConfigPatchError::ReloadFailed { message, .. } => {
                assert!(message.contains("worker refused"));
            }
            other => panic!("expected reload failure, got {other:?}"),
        }
        // The file is back to the pre-write bytes.
        assert_eq!(read_file(&repo, "m", "1", "config.yaml").as_deref(), Some("max_batch_size: 16\n"));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn reload_success_marks_reloaded() {
        let repo = temp_repo("reload-ok");
        write_config(&repo, "m", "1", "max_batch_size: 16\n");
        let state = test_state(repo.clone());

        let prepared = match prepare_config_patch(
            &state,
            "m",
            "1",
            &patch_req(json!({"max_batch_size": 32}), ConfigPatchMode::ApplyReload),
        )
        .await
        .unwrap()
        {
            PrepareOutcome::Ready(p) => p,
            PrepareOutcome::DryRun(_) => panic!("apply_reload never yields a dry-run report"),
        };
        write_prepared(&prepared).await.unwrap();
        let out = finish_apply_reload(*prepared, Ok(true)).await.unwrap();
        assert_eq!(out["reloaded"], json!(true));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn rollback_removes_file_created_by_patch() {
        let repo = temp_repo("rollback-create");
        std::fs::create_dir_all(repo.join("m").join("1")).unwrap();
        let state = test_state(repo.clone());

        let prepared = match prepare_config_patch(
            &state,
            "m",
            "1",
            &patch_req(json!({"max_batch_size": 8}), ConfigPatchMode::ApplyReload),
        )
        .await
        .unwrap()
        {
            PrepareOutcome::Ready(p) => p,
            PrepareOutcome::DryRun(_) => panic!("apply_reload never yields a dry-run report"),
        };
        write_prepared(&prepared).await.unwrap();
        assert!(read_file(&repo, "m", "1", "config.yaml").is_some());
        let _ = finish_apply_reload(*prepared, Err(AppError::Internal("boom".to_string()))).await;
        assert!(read_file(&repo, "m", "1", "config.yaml").is_none());

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn response_never_echoes_patched_secrets() {
        let repo = temp_repo("no-echo");
        write_config(&repo, "m", "1", "max_batch_size: 16\n");
        let state = test_state(repo.clone());

        let out = model_version_config_patch(
            &state,
            "m",
            "1",
            &patch_req(
                json!({"policies": {"auth": {"keys": ["supersecretvalue"]}}}),
                ConfigPatchMode::WriteOnly,
            ),
        )
        .await
        .unwrap();
        assert!(!out.to_string().contains("supersecretvalue"));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn non_object_patch_is_rejected() {
        let repo = temp_repo("bad-patch");
        write_config(&repo, "m", "1", "max_batch_size: 16\n");
        let state = test_state(repo.clone());

        let err = model_version_config_patch(
            &state,
            "m",
            "1",
            &patch_req(json!([1, 2, 3]), ConfigPatchMode::WriteOnly),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ConfigPatchError::Invalid { .. }));

        let _ = std::fs::remove_dir_all(&repo);
    }

    #[tokio::test]
    async fn patch_unknown_version_is_not_found() {
        let repo = temp_repo("patch-404");
        let state = test_state(repo.clone());
        let err = model_version_config_patch(
            &state,
            "ghost",
            "9",
            &patch_req(json!({}), ConfigPatchMode::WriteOnly),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ConfigPatchError::App(AppError::ModelNotFound(_))));
        let _ = std::fs::remove_dir_all(&repo);
    }

    // ===== M5: server config read =====

    fn test_state_with(config: Config, cli: crate::config::CliOverrides) -> Arc<AppState> {
        // No disk access in the M5 core — a non-existent repo path is fine.
        let repo = PathBuf::from("/nonexistent-m5-repo");
        let registry = Arc::new(ModelRegistry::new());
        let inference_queue = Arc::new(InferenceQueue::new());
        let callback_runner = Arc::new(crate::callback::CallbackRunner::new());
        let worker_manager = Arc::new(WorkerManager::new(
            registry.clone(),
            repo.clone(),
            inference_queue.clone(),
            "warn".to_string(),
            callback_runner.clone(),
        ));
        let mut state = AppState::new(
            registry,
            worker_manager,
            inference_queue,
            config,
            repo,
            callback_runner,
            Arc::new(AtomicBool::new(false)),
            Arc::new(crate::rate_limit::RateLimiter::default()),
        );
        state.cli_overrides = cli;
        Arc::new(state)
    }

    #[test]
    fn server_config_marks_cli_file_and_default_sources() {
        let mut config = Config::default();
        // Differs from the built-in default with no CLI override → "file".
        config.metrics.timeline_max_points = 1440;
        let cli = crate::config::CliOverrides {
            // Equals the default value but forced via CLI → still "cli".
            port: Some(8000),
            no_grpc: true,
            ..Default::default()
        };
        let state = test_state_with(config, cli);

        let out = server_config_json(&state).unwrap();
        let sources = out["sources"].as_object().unwrap();
        let src = |p: &str| sources.get(p).and_then(|v| v.as_str());
        assert_eq!(src("server.http_port"), Some("cli"));
        assert_eq!(src("grpc.enabled"), Some("cli"));
        assert_eq!(src("metrics.timeline_max_points"), Some("file"));
        assert_eq!(src("server.host"), Some("default"));
        assert_eq!(src("logging.level"), Some("default"));
    }

    #[test]
    fn server_config_cli_beats_file_for_same_field() {
        // Both file and CLI set the port; the CLI value wins in the effective
        // config and the source must read "cli".
        let mut config = Config::default();
        config.server.http_port = 9000;
        let cli = crate::config::CliOverrides {
            port: Some(9000),
            ..Default::default()
        };
        let state = test_state_with(config, cli);
        let out = server_config_json(&state).unwrap();
        assert_eq!(out["config"]["server"]["http_port"], json!(9000));
        assert_eq!(
            out["sources"]["server.http_port"],
            json!("cli")
        );
    }

    #[test]
    fn server_config_redacts_access_control_and_otlp_secrets() {
        use crate::config::{EndpointControl, ProtocolControl};
        let mut config = Config::default();
        let key_cell = || EndpointControl::Key {
            key: "x-api-key".to_string(),
            value: Some("topsecret".to_string()),
            value_env: Some("ADMIN_KEY_ENV".to_string()),
            value_file: None,
        };
        config.access_control.admin = ProtocolControl {
            http: Some(key_cell()),
            grpc: None,
        };
        config.openai_compact.auth = Some(key_cell());
        config
            .telemetry
            .otlp_headers
            .insert("Authorization".to_string(), "Bearer tok".to_string());
        let state = test_state_with(config, crate::config::CliOverrides::default());

        let out = server_config_json(&state).unwrap();
        assert_eq!(out["config"]["access_control"]["admin"]["http"]["value"], json!("***"));
        assert_eq!(out["config"]["openai_compact"]["auth"]["value"], json!("***"));
        assert_eq!(out["config"]["telemetry"]["otlp_headers"], json!("***"));
        // Non-secret companions stay visible.
        assert_eq!(
            out["config"]["access_control"]["admin"]["http"]["value_env"],
            json!("ADMIN_KEY_ENV")
        );
        assert!(!out.to_string().contains("topsecret"));
        assert!(!out.to_string().contains("Bearer tok"));
        let redacted: Vec<&str> = out["redacted"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert!(redacted.contains(&"access_control.admin.http.value"));
        assert!(redacted.contains(&"openai_compact.auth.value"));
        assert!(redacted.contains(&"telemetry.otlp_headers"));
        // Redacted rows still carry a source entry (attributed post-redaction).
        assert_eq!(
            out["sources"]["access_control.admin.http.value"],
            json!("file")
        );
    }

    #[test]
    fn server_config_serializes_full_tree_with_sources_for_every_leaf() {
        let state = test_state_with(Config::default(), crate::config::CliOverrides::default());
        let out = server_config_json(&state).unwrap();
        // Sanity: the effective tree carries the known top-level sections and
        // every leaf path has a source label.
        for section in ["server", "grpc", "metrics", "alerts", "access_control", "features"] {
            assert!(out["config"].get(section).is_some(), "missing section {section}");
        }
        let sources = out["sources"].as_object().unwrap();
        assert!(sources.len() > 50, "expected per-leaf sources, got {}", sources.len());
        let non_default: Vec<_> = sources.iter().filter(|(_, s)| s.as_str() != Some("default")).collect();
        assert!(non_default.is_empty(), "non-default sources: {non_default:?}");
        assert!(out["redacted"].as_array().unwrap().is_empty());
    }
}
