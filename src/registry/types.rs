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
    /// Desired traffic weights per version (§4.3). Survives version reload:
    /// `register` initializes `ModelVersion.weight` from this map.
    #[serde(default)]
    pub weights: HashMap<String, u32>,
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
    /// Last time a request was routed to this version (coarse, 1s granularity;
    /// see [`super::ModelRegistry::touch_last_used`]). Drives LRU eviction.
    #[serde(default)]
    pub last_used_at: Option<SystemTime>,
    /// Traffic weight for weighted/canary routing (§4.3). 0 = no weighted
    /// traffic (bare requests then fall back to the active version).
    #[serde(default)]
    pub weight: u32,
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
            weights: HashMap::new(),
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

/// One model version's health summary (server-wide status rollup, phase 3).
#[derive(Debug, Clone)]
pub struct ServerStatusEntry {
    pub name: String,
    pub version: String,
    pub status: VersionStatus,
    pub workers: usize,
    pub loaded_at: Option<SystemTime>,
}

/// Server-wide health rollup consumed by the /health, /readyz and /startupz
/// handlers and the gRPC Health sync. Built by
/// [`crate::registry::ModelRegistry::server_status`]; sorted by (name,
/// version) for deterministic output.
#[derive(Debug, Clone, Default)]
pub struct ServerStatus {
    pub entries: Vec<ServerStatusEntry>,
}

impl ServerStatus {
    /// Any version able to serve traffic (Ready or Degraded).
    pub fn has_serving(&self) -> bool {
        self.entries
            .iter()
            .any(|e| matches!(e.status, VersionStatus::Ready | VersionStatus::Degraded))
    }

    /// Versions still initializing (Pending or Loading), as (name, version).
    pub fn initializing(&self) -> Vec<(String, String)> {
        self.entries
            .iter()
            .filter(|e| matches!(e.status, VersionStatus::Pending | VersionStatus::Loading))
            .map(|e| (e.name.clone(), e.version.clone()))
            .collect()
    }

    /// Names of models with at least one serving version (deduped).
    pub fn serving_model_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .entries
            .iter()
            .filter(|e| matches!(e.status, VersionStatus::Ready | VersionStatus::Degraded))
            .map(|e| e.name.clone())
            .collect();
        names.dedup(); // entries sorted by name, so dedup suffices
        names
    }
}
