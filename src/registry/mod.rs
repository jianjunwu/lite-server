pub mod types;

use crate::config::{ModelConfig, ModelStrategyConfig};
use crate::registry::types::LoadPolicy;
use crate::error::AppError;
use crate::registry::types::*;
use dashmap::DashMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ModelRegistry {
    models: DashMap<String, ModelEntry>,
    active_versions: DashMap<String, String>,
}

impl ModelRegistry {
    pub fn new() -> Self {
        Self {
            models: DashMap::new(),
            active_versions: DashMap::new(),
        }
    }

    /// Return (name, version, ModelVersion) tuples for all loaded models.
    /// Clones ModelVersion — prefer list_loaded_keys() when only (name, version) needed.
    pub fn list_loaded(&self) -> Vec<(String, String, ModelVersion)> {
        let mut result = Vec::new();
        for entry in self.models.iter() {
            let name = entry.key();
            for (version, mv) in entry.versions.iter() {
                result.push((name.clone(), version.clone(), mv.clone()));
            }
        }
        result
    }

    /// Return (name, version) tuples without cloning ModelVersion.
    /// Intended for hot-path iteration (e.g. timeline sampling).
    pub fn list_loaded_keys(&self) -> Vec<(String, String)> {
        let mut result = Vec::new();
        for entry in self.models.iter() {
            let name = entry.key();
            for version in entry.versions.keys() {
                result.push((name.clone(), version.clone()));
            }
        }
        result
    }

    pub fn list_versions(&self, model_name: &str) -> Vec<ModelVersion> {
        self.models
            .get(model_name)
            .map(|e| e.versions.values().cloned().collect())
            .unwrap_or_default()
    }

    /// True when no model is registered. O(1); used by the file watcher
    /// gate to skip events only when nothing can need reload or unload.
    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }

    pub fn get(&self, model_name: &str, version: Option<&str>) -> Option<ModelVersion> {
        let entry = self.models.get(model_name)?;

        let version = match version {
            Some(v) => v.to_string(),
            None => self.active_versions.get(model_name)?.clone(),
        };

        entry.versions.get(&version).cloned()
    }

    pub fn is_ready(&self, model_name: &str, version: Option<&str>) -> bool {
        match self.get(model_name, version) {
            Some(mv) => mv.status == VersionStatus::Ready,
            None => false,
        }
    }

    /// Server-wide health rollup for the /health, /readyz and /startupz
    /// handlers and gRPC Health sync. Entries are sorted by (name, version)
    /// so responses are deterministic.
    pub fn server_status(&self) -> ServerStatus {
        let mut entries = Vec::new();
        for entry in self.models.iter() {
            for mv in entry.versions.values() {
                entries.push(ServerStatusEntry {
                    name: entry.name.clone(),
                    version: mv.version.clone(),
                    status: mv.status,
                    workers: mv.workers.len(),
                    loaded_at: mv.loaded_at,
                });
            }
        }
        entries.sort_by(|a, b| (&a.name, &a.version).cmp(&(&b.name, &b.version)));
        ServerStatus { entries }
    }

    pub fn get_active_version(&self, model_name: &str) -> Option<String> {
        self.active_versions.get(model_name).map(|r| r.clone())
    }

    /// Stamp `last_used_at` for LRU eviction (§4.2). Coarse by design: a touch
    /// within 1s of the previous stamp is a no-op, so the request hot path
    /// stays on a shared read and pays a write at most once per second.
    /// Missing model/version is a silent no-op.
    pub fn touch_last_used(&self, model_name: &str, version: &str) {
        const COALESCE: std::time::Duration = std::time::Duration::from_secs(1);
        let now = std::time::SystemTime::now();

        let fresh = self
            .models
            .get(model_name)
            .and_then(|entry| entry.versions.get(version).and_then(|v| v.last_used_at))
            .is_some_and(|t| now.duration_since(t).unwrap_or_default() < COALESCE);
        if fresh {
            return;
        }

        if let Some(mut entry) = self.models.get_mut(model_name) {
            if let Some(mv) = entry.versions.get_mut(version) {
                mv.last_used_at = Some(now);
            }
        }
    }

    /// Pick the least-recently-used non-active version for LRU eviction.
    /// Never-used versions (`last_used_at = None`) are the first candidates;
    /// the active version is never a candidate.
    pub fn lru_eviction_candidate(&self, model_name: &str) -> Option<String> {
        let entry = self.models.get(model_name)?;
        let active = self.active_versions.get(model_name).map(|a| a.clone());
        entry
            .versions
            .values()
            .filter(|v| active.as_ref() != Some(&v.version))
            .min_by_key(|v| v.last_used_at)
            .map(|v| v.version.clone())
    }

    pub fn register(
        &self,
        model_name: &str,
        version: &str,
        config: ModelConfig,
        model_type: ModelType,
        model_dir: PathBuf,
    ) -> Result<(), AppError> {
        let mut entry = self
            .models
            .entry(model_name.to_string())
            .or_insert_with(|| ModelEntry::new(model_name));

        // Refuse to silently overwrite: the old version's workers/queues would
        // be orphaned. Callers must unload first (§4.1).
        if entry.versions.contains_key(version) {
            return Err(AppError::VersionAlreadyLoaded(
                model_name.to_string(),
                version.to_string(),
            ));
        }

        let mv = ModelVersion {
            version: version.to_string(),
            status: VersionStatus::Pending,
            config,
            model_type,
            model_dir,
            workers: vec![],
            loaded_at: None,
            last_used_at: None,
            weight: entry.weights.get(version).copied().unwrap_or(0),
            policies: Default::default(),
            cors_headers: None,
        };
        entry.versions.insert(version.to_string(), mv);
        Ok(())
    }

    /// Atomically replace the model's traffic weights (§4.3). Versions not
    /// listed in the map get weight 0. Unknown versions are rejected before
    /// anything is mutated.
    pub fn set_weights(
        &self,
        model_name: &str,
        weights: &HashMap<String, u32>,
    ) -> Result<(), AppError> {
        let mut entry = self
            .models
            .get_mut(model_name)
            .ok_or_else(|| AppError::ModelNotFound(model_name.to_string()))?;
        for v in weights.keys() {
            if !entry.versions.contains_key(v) {
                return Err(AppError::Validation(format!(
                    "unknown version {} for model {}",
                    v, model_name
                )));
            }
        }
        entry.weights = weights.clone();
        for (v, mv) in entry.versions.iter_mut() {
            mv.weight = weights.get(v).copied().unwrap_or(0);
        }
        Ok(())
    }

    /// Weighted random pick among serving (`Ready`/`Degraded`) versions with
    /// weight > 0 (§4.3). Hot path: one shared read + a tiny candidate Vec.
    /// Returns `None` when no candidate exists — callers fall back to the
    /// active version.
    pub fn routing_pick(&self, model_name: &str) -> Option<String> {
        let entry = self.models.get(model_name)?;
        let candidates: Vec<(&String, u32)> = entry
            .versions
            .iter()
            .filter(|(_, mv)| {
                mv.weight > 0
                    && matches!(mv.status, VersionStatus::Ready | VersionStatus::Degraded)
            })
            .map(|(v, mv)| (v, mv.weight))
            .collect();
        let total: u32 = candidates.iter().map(|(_, w)| w).sum();
        if total == 0 {
            return None;
        }
        use rand::Rng;
        let mut roll = rand::thread_rng().gen_range(0..total);
        for (v, w) in candidates {
            if roll < w {
                return Some(v.clone());
            }
            roll -= w;
        }
        None // unreachable: roll < total guarantees a hit
    }

    pub fn set_status(
        &self,
        model_name: &str,
        version: &str,
        status: VersionStatus,
    ) -> Result<(), AppError> {
        let mut entry = self
            .models
            .get_mut(model_name)
            .ok_or_else(|| AppError::ModelNotFound(model_name.to_string()))?;
        let mv = entry
            .versions
            .get_mut(version)
            .ok_or_else(|| AppError::VersionNotFound(model_name.to_string(), version.to_string()))?;
        mv.status = status;
        Ok(())
    }

    /// Transition a version to `Ready`, stamping `loaded_at` on the first
    /// arrival only. `Ready`→`Ready` (e.g. worker respawn) preserves the
    /// original load timestamp.
    pub fn mark_ready(&self, model_name: &str, version: &str) -> Result<(), AppError> {
        let mut entry = self
            .models
            .get_mut(model_name)
            .ok_or_else(|| AppError::ModelNotFound(model_name.to_string()))?;
        let mv = entry
            .versions
            .get_mut(version)
            .ok_or_else(|| AppError::VersionNotFound(model_name.to_string(), version.to_string()))?;
        mv.status = VersionStatus::Ready;
        if mv.loaded_at.is_none() {
            mv.loaded_at = Some(std::time::SystemTime::now());
        }
        Ok(())
    }

    pub fn set_workers(
        &self,
        model_name: &str,
        version: &str,
        workers: Vec<WorkerInfo>,
    ) -> Result<(), AppError> {
        let mut entry = self
            .models
            .get_mut(model_name)
            .ok_or_else(|| AppError::ModelNotFound(model_name.to_string()))?;
        let mv = entry
            .versions
            .get_mut(version)
            .ok_or_else(|| AppError::VersionNotFound(model_name.to_string(), version.to_string()))?;
        mv.workers = workers;
        Ok(())
    }

    /// Replace a specific worker in the registry by worker_id.
    /// If the worker_id doesn't exist, appends the new info.
    pub fn replace_worker(
        &self,
        model_name: &str,
        version: &str,
        worker_id: u32,
        info: WorkerInfo,
    ) -> Result<(), AppError> {
        let mut entry = self
            .models
            .get_mut(model_name)
            .ok_or_else(|| AppError::ModelNotFound(model_name.to_string()))?;
        let mv = entry
            .versions
            .get_mut(version)
            .ok_or_else(|| AppError::VersionNotFound(model_name.to_string(), version.to_string()))?;
        if let Some(existing) = mv.workers.iter_mut().find(|w| w.worker_id == worker_id) {
            *existing = info;
        } else {
            mv.workers.push(info);
        }
        Ok(())
    }

    pub fn activate_version(&self, model_name: &str, version: &str) -> Result<bool, AppError> {
        let entry = self
            .models
            .get(model_name)
            .ok_or_else(|| AppError::ModelNotFound(model_name.to_string()))?;
        let mv = entry
            .versions
            .get(version)
            .ok_or_else(|| AppError::VersionNotFound(model_name.to_string(), version.to_string()))?;

        if mv.status != VersionStatus::Ready {
            return Ok(false);
        }
        drop(entry);

        self.active_versions.insert(model_name.to_string(), version.to_string());
        Ok(true)
    }

    pub fn deactivate(&self, model_name: &str) {
        self.active_versions.remove(model_name);
    }

    pub fn remove(&self, model_name: &str, version: &str) -> Result<(), AppError> {
        // Step 1: Remove the version from the model entry
        let mut should_remove_model = false;
        {
            let mut entry = match self.models.get_mut(model_name) {
                Some(e) => e,
                None => return Ok(()),
            };
            entry.versions.remove(version);
            if entry.versions.is_empty() {
                should_remove_model = true;
            }
        }

        // Step 2: Handle active version cleanup (no models lock held)
        let active_version = self.active_versions.get(model_name).map(|r| r.clone());
        if active_version.as_deref() == Some(version) {
            self.active_versions.remove(model_name);
            // Try auto-activate another ready version
            if let Some(entry) = self.models.get(model_name) {
                for (v, mv) in entry.versions.iter() {
                    if mv.status == VersionStatus::Ready {
                        self.active_versions.insert(model_name.to_string(), v.clone());
                        break;
                    }
                }
            }
        }

        // Step 3: Remove the model entry entirely if no versions left
        if should_remove_model {
            self.models.remove(model_name);
        }

        Ok(())
    }

    pub fn set_policies(
        &self,
        model_name: &str,
        version: &str,
        policies: Option<crate::config::ModelPolicies>,
    ) {
        if let Some(p) = policies {
            if let Some(mut entry) = self.models.get_mut(model_name) {
                if let Some(mv) = entry.versions.get_mut(version) {
                    // Pre-build the CORS HeaderMap once (B9) so the hot response
                    // path only Arc-clones instead of re-joining/parsing strings.
                    mv.cors_headers = p.cors.as_ref().map(|c| Arc::new(c.header_map()));
                    mv.policies = p;
                }
            }
        }
    }

    pub fn active_cors_headers(
        &self,
        model_name: &str,
    ) -> Option<Arc<axum::http::HeaderMap>> {
        let active_version = self.active_versions.get(model_name)?;
        let model = self.models.get(model_name)?;
        let mv = model.versions.get(active_version.value())?;
        mv.cors_headers.clone()
    }

    /// CORS headers for a specific version (§4.4): versioned routes answer
    /// OPTIONS with the hit version's policy, not the active one's.
    pub fn cors_headers_for(
        &self,
        model_name: &str,
        version: &str,
    ) -> Option<Arc<axum::http::HeaderMap>> {
        let model = self.models.get(model_name)?;
        let mv = model.versions.get(version)?;
        mv.cors_headers.clone()
    }

    pub fn set_strategy(
        &self,
        model_name: &str,
        strategy: &ModelStrategyConfig,
    ) -> Result<(), AppError> {
        let mut entry = self
            .models
            .entry(model_name.to_string())
            .or_insert_with(|| ModelEntry::new(model_name));

        entry.load_policy = match strategy.load_policy.as_str() {
            "all" => LoadPolicy::All,
            "latest" => LoadPolicy::Latest,
            _ => LoadPolicy::Explicit,
        };
        entry.max_loaded_versions = strategy.max_loaded_versions;
        if let Some(weights) = &strategy.weights {
            entry.weights = weights.clone();
        }
        Ok(())
    }

    pub fn get_strategy(&self, model_name: &str) -> Option<ModelStrategyConfig> {
        let entry = self.models.get(model_name)?;
        Some(ModelStrategyConfig {
            name: model_name.to_string(),
            load_policy: match entry.load_policy {
                LoadPolicy::All => "all".to_string(),
                LoadPolicy::Latest => "latest".to_string(),
                LoadPolicy::Explicit => "explicit".to_string(),
            },
            versions_to_load: entry.versions.keys().cloned().collect(),
            default_version: self.get_active_version(model_name),
            max_loaded_versions: entry.max_loaded_versions,
            weights: Some(entry.weights.clone()),
        })
    }
}

impl Default for ModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ModelConfig;
    

    fn test_config() -> ModelConfig {
        ModelConfig {
            max_batch_size: 1,
            ..Default::default()
        }
    }

    fn tmp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("lite-server-reg-test-{}", std::process::id()))
    }

    // --- Type assertions: DashMap enables sync (no .await) reads ---

    #[test]
    fn registry_models_is_dashmap() {
        fn assert_type<T>() {}
        assert_type::<DashMap<String, ModelEntry>>();
    }

    #[test]
    fn registry_active_versions_is_dashmap() {
        fn assert_type<T>() {}
        assert_type::<DashMap<String, String>>();
    }

    #[test]
    fn registry_get_is_sync_no_await() {
        let reg = ModelRegistry::new();
        // This must compile without .await — proves DashMap not RwLock
        let _ = reg.get("m1", Some("1"));
        let _ = reg.is_ready("m1", Some("1"));
        let _ = reg.get_active_version("m1");
        let _ = reg.list_loaded();
        let _ = reg.list_versions("m1");
    }

    // --- Basic lifecycle ---

    #[test]
    fn test_register_and_get() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();

        let mv = reg.get("m1", Some("1")).unwrap();
        assert_eq!(mv.version, "1");
        assert_eq!(mv.status, VersionStatus::Pending);
        assert_eq!(mv.loaded_at, None);
    }

    #[test]
    fn test_register_duplicate_version_rejected() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();

        // Silent overwrite would orphan the old version's workers/queues.
        let err = reg
            .register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap_err();
        assert!(
            matches!(err, AppError::VersionAlreadyLoaded(ref m, ref v) if m == "m1" && v == "1"),
            "duplicate register must be rejected, got {err:?}"
        );

        // A different version of the same model is fine.
        reg.register("m1", "2", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
    }

    // --- Explicit state machine ---

    #[test]
    fn version_status_serializes_snake_case() {
        let cases = [
            (VersionStatus::Pending, "pending"),
            (VersionStatus::Loading, "loading"),
            (VersionStatus::Ready, "ready"),
            (VersionStatus::Degraded, "degraded"),
            (VersionStatus::Failed, "failed"),
            (VersionStatus::Unloading, "unloading"),
        ];
        for (status, expected) in cases {
            let json = serde_json::to_string(&status).unwrap();
            assert_eq!(json, format!("\"{}\"", expected));
            let back: VersionStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(back, status);
        }
    }

    #[test]
    fn test_mark_ready_stamps_loaded_at_once() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();

        reg.mark_ready("m1", "1").unwrap();
        let first = reg.get("m1", Some("1")).unwrap();
        assert_eq!(first.status, VersionStatus::Ready);
        let stamped = first.loaded_at.expect("loaded_at stamped on first Ready");

        // Degraded → Ready (coordinator / respawn path) preserves the stamp.
        reg.set_status("m1", "1", VersionStatus::Degraded).unwrap();
        reg.mark_ready("m1", "1").unwrap();
        let second = reg.get("m1", Some("1")).unwrap();
        assert_eq!(second.loaded_at, Some(stamped));
    }

    #[test]
    fn test_mark_ready_nonexistent_errors() {
        let reg = ModelRegistry::new();
        assert!(reg.mark_ready("nope", "1").is_err());
    }

    // --- LRU last_used_at (§4.2) ---

    #[test]
    fn test_touch_last_used_sets_and_coalesces() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        assert!(reg.get("m1", Some("1")).unwrap().last_used_at.is_none());

        reg.touch_last_used("m1", "1");
        let first = reg
            .get("m1", Some("1"))
            .unwrap()
            .last_used_at
            .expect("touch must set last_used_at");

        // Coarse (1s): an immediate second touch is a no-op, keeping the hot
        // path on a shared read instead of a per-request write.
        reg.touch_last_used("m1", "1");
        let second = reg.get("m1", Some("1")).unwrap().last_used_at.unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn test_touch_last_used_missing_is_noop() {
        let reg = ModelRegistry::new();
        reg.touch_last_used("nope", "1"); // unknown model — must not panic
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.touch_last_used("m1", "2"); // unknown version — must not panic
        assert!(reg.get("m1", Some("1")).unwrap().last_used_at.is_none());
    }

    #[test]
    fn test_lru_eviction_candidate_skips_active_prefers_never_used() {
        let reg = ModelRegistry::new();
        for v in ["1", "2", "3"] {
            reg.register("m1", v, test_config(), ModelType::LitAPI, tmp_dir())
                .unwrap();
            reg.mark_ready("m1", v).unwrap();
        }
        reg.activate_version("m1", "3").unwrap();

        // "2" never used → first candidate; active "3" is never picked.
        reg.touch_last_used("m1", "1");
        assert_eq!(reg.lru_eviction_candidate("m1"), Some("2".to_string()));

        // All non-active used → candidate is one of them, never active "3".
        reg.touch_last_used("m1", "2");
        let c = reg.lru_eviction_candidate("m1").unwrap();
        assert!(c == "1" || c == "2", "active version must never be a candidate");
    }

    #[test]
    fn test_lru_eviction_candidate_none_when_only_active_loaded() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.mark_ready("m1", "1").unwrap();
        reg.activate_version("m1", "1").unwrap();
        assert_eq!(reg.lru_eviction_candidate("m1"), None);
    }

    // --- Weighted routing (§4.3) ---

    #[test]
    fn test_set_weights_atomic_and_unlisted_zeroed() {
        let reg = ModelRegistry::new();
        for v in ["1", "2"] {
            reg.register("m1", v, test_config(), ModelType::LitAPI, tmp_dir())
                .unwrap();
        }

        reg.set_weights("m1", &HashMap::from([("1".into(), 90u32), ("2".into(), 10)]))
            .unwrap();
        assert_eq!(reg.get("m1", Some("1")).unwrap().weight, 90);
        assert_eq!(reg.get("m1", Some("2")).unwrap().weight, 10);

        // Atomic full-set: versions not listed in the new map are zeroed.
        reg.set_weights("m1", &HashMap::from([("2".into(), 50u32)]))
            .unwrap();
        assert_eq!(reg.get("m1", Some("1")).unwrap().weight, 0);
        assert_eq!(reg.get("m1", Some("2")).unwrap().weight, 50);
    }

    #[test]
    fn test_set_weights_unknown_version_rejected() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        let err = reg
            .set_weights("m1", &HashMap::from([("nope".into(), 100u32)]))
            .unwrap_err();
        assert!(matches!(err, AppError::Validation(_)), "got {err:?}");
        // Rejected wholesale: the valid version's weight is untouched.
        assert_eq!(reg.get("m1", Some("1")).unwrap().weight, 0);

        assert!(reg.set_weights("nope", &HashMap::new()).is_err());
    }

    #[test]
    fn test_register_inherits_strategy_weights() {
        let reg = ModelRegistry::new();
        let strategy = ModelStrategyConfig {
            name: "m1".to_string(),
            weights: Some(HashMap::from([("1".to_string(), 80u32)])),
            ..Default::default()
        };
        reg.set_strategy("m1", &strategy).unwrap();

        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.register("m1", "2", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        assert_eq!(reg.get("m1", Some("1")).unwrap().weight, 80);
        assert_eq!(reg.get("m1", Some("2")).unwrap().weight, 0);
    }

    #[test]
    fn test_routing_pick_deterministic_single_candidate() {
        let reg = ModelRegistry::new();
        for v in ["1", "2"] {
            reg.register("m1", v, test_config(), ModelType::LitAPI, tmp_dir())
                .unwrap();
            reg.mark_ready("m1", v).unwrap();
        }
        reg.set_weights("m1", &HashMap::from([("1".into(), 100u32), ("2".into(), 0)]))
            .unwrap();
        for _ in 0..20 {
            assert_eq!(reg.routing_pick("m1"), Some("1".to_string()));
        }
    }

    #[test]
    fn test_routing_pick_excludes_not_serving_and_zero_weight() {
        let reg = ModelRegistry::new();
        for v in ["1", "2", "3"] {
            reg.register("m1", v, test_config(), ModelType::LitAPI, tmp_dir())
                .unwrap();
            reg.mark_ready("m1", v).unwrap();
        }
        reg.set_weights(
            "m1",
            &HashMap::from([("1".into(), 100u32), ("2".into(), 1u32), ("3".into(), 100u32)]),
        )
        .unwrap();
        // "1" Failed and "3" still Loading (reset below) → only "2" is eligible.
        reg.set_status("m1", "1", VersionStatus::Failed).unwrap();
        reg.set_status("m1", "3", VersionStatus::Loading).unwrap();
        for _ in 0..20 {
            assert_eq!(reg.routing_pick("m1"), Some("2".to_string()));
        }

        // Degraded still counts as serving.
        reg.set_status("m1", "2", VersionStatus::Degraded).unwrap();
        assert_eq!(reg.routing_pick("m1"), Some("2".to_string()));
    }

    #[test]
    fn test_routing_pick_none_when_no_candidate() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        // All weights zero (default) → None (caller falls back to active).
        assert_eq!(reg.routing_pick("m1"), None);
        // Weight > 0 but not serving → None.
        reg.set_weights("m1", &HashMap::from([("1".into(), 100u32)]))
            .unwrap();
        assert_eq!(reg.routing_pick("m1"), None, "Pending is not serving");
        // Unknown model → None.
        assert_eq!(reg.routing_pick("nope"), None);
    }

    #[test]
    fn test_activate_version_does_not_touch_weights() {
        // Registry-level activate moves the pointer only (§4.3): the weight
        // hard-switch lives in the HTTP handler, so internal re-activations
        // (e.g. auto-recycle reload) never clobber a canary split.
        let reg = ModelRegistry::new();
        for v in ["1", "2"] {
            reg.register("m1", v, test_config(), ModelType::LitAPI, tmp_dir())
                .unwrap();
            reg.mark_ready("m1", v).unwrap();
        }
        reg.set_weights("m1", &HashMap::from([("1".into(), 90u32), ("2".into(), 10)]))
            .unwrap();

        reg.activate_version("m1", "2").unwrap();
        assert_eq!(reg.get("m1", Some("1")).unwrap().weight, 90);
        assert_eq!(reg.get("m1", Some("2")).unwrap().weight, 10);
    }

    #[test]
    fn test_routing_pick_distribution_roughly_proportional() {
        let reg = ModelRegistry::new();
        for v in ["1", "2"] {
            reg.register("m1", v, test_config(), ModelType::LitAPI, tmp_dir())
                .unwrap();
            reg.mark_ready("m1", v).unwrap();
        }
        reg.set_weights("m1", &HashMap::from([("1".into(), 90u32), ("2".into(), 10)]))
            .unwrap();

        let mut v2 = 0usize;
        let n = 10_000;
        for _ in 0..n {
            if reg.routing_pick("m1").as_deref() == Some("2") {
                v2 += 1;
            }
        }
        // Expected 1000 ± wide tolerance (best-effort weights, no flake).
        assert!(
            (500..=1500).contains(&v2),
            "v2 picked {v2}/{n} times, expected ~10%"
        );
    }

    // --- Server-wide status rollup ---

    #[test]
    fn server_status_empty_registry_not_serving_nor_initializing() {
        let reg = ModelRegistry::new();
        let status = reg.server_status();
        assert!(!status.has_serving());
        assert!(status.initializing().is_empty());
        assert!(status.serving_model_names().is_empty());
    }

    #[test]
    fn server_status_degraded_counts_as_serving() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.set_status("m1", "1", VersionStatus::Degraded).unwrap();
        let status = reg.server_status();
        assert!(status.has_serving());
        assert_eq!(status.serving_model_names(), vec!["m1".to_string()]);
        assert!(status.initializing().is_empty());
    }

    #[test]
    fn server_status_pending_and_loading_are_initializing() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap(); // Pending
        reg.register("m1", "2", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.set_status("m1", "2", VersionStatus::Loading).unwrap();
        let status = reg.server_status();
        assert!(!status.has_serving());
        let mut init = status.initializing();
        init.sort();
        assert_eq!(
            init,
            vec![
                ("m1".to_string(), "1".to_string()),
                ("m1".to_string(), "2".to_string()),
            ]
        );
    }

    #[test]
    fn server_status_sorted_and_carries_workers_and_loaded_at() {
        let reg = ModelRegistry::new();
        reg.register("b", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.register("a", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.mark_ready("a", "1").unwrap();
        let status = reg.server_status();
        let names: Vec<&str> = status.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);
        let a = &status.entries[0];
        assert_eq!(a.status, VersionStatus::Ready);
        assert!(a.loaded_at.is_some());
        assert_eq!(a.workers, 0);
    }

    #[test]
    fn test_set_status() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();

        reg.set_status("m1", "1", VersionStatus::Ready).unwrap();

        let mv = reg.get("m1", Some("1")).unwrap();
        assert_eq!(mv.status, VersionStatus::Ready);
    }

    #[test]
    fn test_is_ready() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();

        assert!(!reg.is_ready("m1", Some("1")));

        reg.set_status("m1", "1", VersionStatus::Ready).unwrap();

        assert!(reg.is_ready("m1", Some("1")));
    }

    #[test]
    fn test_is_ready_nonexistent() {
        let reg = ModelRegistry::new();
        assert!(!reg.is_ready("nope", Some("1")));
    }

    // --- Activate version ---

    #[test]
    fn test_activate_version() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.set_status("m1", "1", VersionStatus::Ready).unwrap();

        let ok = reg.activate_version("m1", "1").unwrap();
        assert!(ok);
        assert_eq!(reg.get_active_version("m1"), Some("1".to_string()));
    }

    #[test]
    fn test_activate_not_ready_fails() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();

        let ok = reg.activate_version("m1", "1").unwrap();
        assert!(!ok);
        assert_eq!(reg.get_active_version("m1"), None);
    }

    #[test]
    fn test_activate_nonexistent_model_errors() {
        let reg = ModelRegistry::new();
        let result = reg.activate_version("nope", "1");
        assert!(result.is_err());
    }

    // --- Deactivate ---

    #[test]
    fn test_deactivate() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.set_status("m1", "1", VersionStatus::Ready).unwrap();
        reg.activate_version("m1", "1").unwrap();

        reg.deactivate("m1");
        assert_eq!(reg.get_active_version("m1"), None);
    }

    // --- Multiple versions ---

    #[test]
    fn test_multiple_versions() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.register("m1", "2", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();

        let versions = reg.list_versions("m1");
        assert_eq!(versions.len(), 2);
    }

    #[test]
    fn test_activate_switches_version() {
        let reg = ModelRegistry::new();
        for v in &["1", "2"] {
            reg.register("m1", v, test_config(), ModelType::LitAPI, tmp_dir())
                .unwrap();
            reg.set_status("m1", v, VersionStatus::Ready).unwrap();
        }

        reg.activate_version("m1", "1").unwrap();
        assert_eq!(reg.get_active_version("m1"), Some("1".to_string()));

        reg.activate_version("m1", "2").unwrap();
        assert_eq!(reg.get_active_version("m1"), Some("2".to_string()));
    }

    // --- Remove ---

    #[test]
    fn test_remove_version() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();

        reg.remove("m1", "1").unwrap();
        assert!(reg.get("m1", Some("1")).is_none());
    }

    #[test]
    fn test_remove_auto_activates_another_ready() {
        let reg = ModelRegistry::new();
        for v in &["1", "2"] {
            reg.register("m1", v, test_config(), ModelType::LitAPI, tmp_dir())
                .unwrap();
            reg.set_status("m1", v, VersionStatus::Ready).unwrap();
        }
        reg.activate_version("m1", "1").unwrap();

        reg.remove("m1", "1").unwrap();
        // Should auto-activate v2
        assert_eq!(reg.get_active_version("m1"), Some("2".to_string()));
    }

    #[test]
    fn test_remove_active_no_other_ready_clears_active() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.set_status("m1", "1", VersionStatus::Ready).unwrap();
        reg.activate_version("m1", "1").unwrap();

        reg.remove("m1", "1").unwrap();
        assert_eq!(reg.get_active_version("m1"), None);
    }

    #[test]
    fn test_remove_last_version_removes_model() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();

        reg.remove("m1", "1").unwrap();
        assert!(reg.list_loaded().is_empty());
    }

    // --- list_loaded ---

    #[test]
    fn test_list_loaded_empty() {
        let reg = ModelRegistry::new();
        assert!(reg.list_loaded().is_empty());
    }

    #[test]
    fn test_list_loaded_multiple_models() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.register("m2", "1", test_config(), ModelType::Ensemble, tmp_dir())
            .unwrap();

        let loaded = reg.list_loaded();
        assert_eq!(loaded.len(), 2);
    }

    // --- Set workers ---

    #[test]
    fn test_set_workers() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();

        let workers = vec![WorkerInfo {
            worker_id: 0,
            device: "cpu:0".to_string(),
            endpoint: "ipc:///tmp/w0.sock".to_string(),
            pid: Some(1234),
            status: WorkerStatus::Ready,
            capacity: None,
        }];
        reg.set_workers("m1", "1", workers).unwrap();

        let mv = reg.get("m1", Some("1")).unwrap();
        assert_eq!(mv.workers.len(), 1);
        assert_eq!(mv.workers[0].pid, Some(1234));
    }

    // --- get with active version ---

    #[test]
    fn test_get_uses_active_version_when_none_specified() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.set_status("m1", "1", VersionStatus::Ready).unwrap();
        reg.activate_version("m1", "1").unwrap();

        let mv = reg.get("m1", None).unwrap();
        assert_eq!(mv.version, "1");
    }

    #[test]
    fn test_get_returns_none_when_no_active() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();

        let mv = reg.get("m1", None);
        assert!(mv.is_none());
    }

    // --- Strategy ---

    #[test]
    fn test_set_and_get_strategy() {
        let reg = ModelRegistry::new();
        let strategy = ModelStrategyConfig {
            name: "m1".to_string(),
            load_policy: "latest".to_string(),
            max_loaded_versions: Some(2),
            ..Default::default()
        };

        reg.set_strategy("m1", &strategy).unwrap();
        let got = reg.get_strategy("m1").unwrap();
        assert_eq!(got.load_policy, "latest");
        assert_eq!(got.max_loaded_versions, Some(2));
    }

    // --- set_status on nonexistent ---

    #[test]
    fn test_set_status_nonexistent_model_errors() {
        let reg = ModelRegistry::new();
        let result = reg.set_status("nope", "1", VersionStatus::Ready);
        assert!(result.is_err());
    }

    #[test]
    fn test_set_status_nonexistent_version_errors() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();

        let result = reg.set_status("m1", "99", VersionStatus::Ready);
        assert!(result.is_err());
    }

    // --- Concurrent access ---

    #[test]
    fn test_list_loaded_keys() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir()).unwrap();
        reg.register("m1", "2", test_config(), ModelType::LitAPI, tmp_dir()).unwrap();
        reg.register("m2", "1", test_config(), ModelType::Ensemble, tmp_dir()).unwrap();

        let keys = reg.list_loaded_keys();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&("m1".to_string(), "1".to_string())));
        assert!(keys.contains(&("m1".to_string(), "2".to_string())));
        assert!(keys.contains(&("m2".to_string(), "1".to_string())));
    }

    #[test]
    fn test_list_loaded_keys_empty() {
        let reg = ModelRegistry::new();
        assert!(reg.list_loaded_keys().is_empty());
    }

    #[tokio::test]
    async fn test_concurrent_reads_and_writes() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .unwrap();
        reg.set_status("m1", "1", VersionStatus::Ready).unwrap();
        reg.activate_version("m1", "1").unwrap();

        let reg_clone = reg.clone();
        let mut handles = Vec::new();

        // Spawn concurrent readers
        for _ in 0..10 {
            let r = reg_clone.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..100 {
                    assert!(r.is_ready("m1", Some("1")));
                    let _ = r.get("m1", None);
                    let _ = r.get_active_version("m1");
                }
            }));
        }

        // Spawn concurrent writers
        for i in 0..5 {
            let r = reg_clone.clone();
            handles.push(tokio::spawn(async move {
                let v = format!("{}", i + 2);
                r.register("m1", &v, test_config(), ModelType::LitAPI, tmp_dir()).unwrap();
                r.set_status("m1", &v, VersionStatus::Ready).unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }
}
