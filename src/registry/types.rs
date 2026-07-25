use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::SystemTime;

/// Explicit lifecycle state machine for a model version.
///
/// ```text
/// Pending ──spawn──▶ Loading ──handshake──▶ Ready ◀──▶ Degraded
///                      │                     ▲            │
///                      └──load failure──▶ Failed     (coordinator
///                                                     reconciles)
/// ```
///
/// `Failed` is only set during load (startup crash / timeout); runtime worker
/// loss is `Degraded` (outlier ejection auto-recovers). The status coordinator
/// is the sole writer of `Ready`/`Degraded` transitions at runtime; `Loading`
/// →`Ready` stays event-driven at the worker handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionStatus {
    /// Registered, workers not yet spawned.
    Pending,
    /// Workers spawning / model weights loading.
    Loading,
    /// All workers healthy, serving traffic.
    Ready,
    /// Serving but impaired (some/all workers ejected).
    Degraded,
    /// Load failed; does not accept requests.
    Failed,
    /// Unload in progress.
    Unloading,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelType {
    LitAPI,
    Ensemble,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelEntry {
    pub name: String,
    pub versions: HashMap<String, ModelVersion>,
    pub load_policy: LoadPolicy,
    pub max_loaded_versions: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum LoadPolicy {
    #[default]
    Explicit,
    All,
    Latest,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelVersion {
    pub version: String,
    pub status: VersionStatus,
    pub config: crate::config::ModelConfig,
    pub model_type: ModelType,
    pub model_dir: PathBuf,
    pub workers: Vec<WorkerInfo>,
    /// When the version first entered `Ready` (handshake completed). `None`
    /// while still Pending/Loading and preserved across Ready↔Degraded.
    #[serde(default)]
    pub loaded_at: Option<SystemTime>,
    #[serde(default)]
    pub policies: crate::worker::protocol::ModelPolicies,
    /// Pre-built CORS header map, cached at policy ingest (B9) so responses
    /// avoid a per-request `String::join` + `HeaderValue::from_str` round.
    /// `#[serde(skip)]`: a Rust-side cache, never serialized over the wire.
    #[serde(skip)]
    pub cors_headers: Option<std::sync::Arc<axum::http::HeaderMap>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub worker_id: u32,
    pub device: String,
    pub endpoint: String,
    pub pid: Option<u32>,
    pub status: WorkerStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerStatus {
    Starting,
    Ready,
    Busy,
    Stopped,
}

impl ModelEntry {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            versions: HashMap::new(),
            load_policy: LoadPolicy::default(),
            max_loaded_versions: None,
        }
    }

    pub fn ready_versions(&self) -> Vec<&ModelVersion> {
        self.versions
            .values()
            .filter(|v| v.status == VersionStatus::Ready)
            .collect()
    }

    pub fn latest_version(&self) -> Option<&ModelVersion> {
        self.ready_versions()
            .into_iter()
            .max_by_key(|v| &v.version)
    }
}
