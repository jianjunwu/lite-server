pub mod types;

use crate::config::{ModelConfig, ModelStrategyConfig};
use crate::registry::types::LoadPolicy;
use crate::error::AppError;
use crate::registry::types::*;
use dashmap::DashMap;
use std::path::PathBuf;

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

    pub fn get_active_version(&self, model_name: &str) -> Option<String> {
        self.active_versions.get(model_name).map(|r| r.clone())
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

        let mv = ModelVersion {
            version: version.to_string(),
            status: VersionStatus::Loading,
            config,
            model_type,
            model_dir,
            workers: vec![],
            policies: Default::default(),
        };
        entry.versions.insert(version.to_string(), mv);
        Ok(())
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
        policies: Option<crate::worker::protocol::ModelPolicies>,
    ) {
        if let Some(p) = policies {
            let _key = format!("{}/{}", model_name, version);
            if let Some(mut entry) = self.models.get_mut(model_name) {
                if let Some(mv) = entry.versions.get_mut(version) {
                    mv.policies = p;
                }
            }
        }
    }

    pub fn active_cors_policy(
        &self,
        model_name: &str,
    ) -> Option<crate::worker::protocol::CorsPolicy> {
        let active_version = self.active_versions.get(model_name)?;
        let model = self.models.get(model_name)?;
        let mv = model.versions.get(active_version.value())?;
        mv.policies.cors.clone()
    }

    pub fn active_rate_limit_policy(
        &self,
        model_name: &str,
    ) -> Option<crate::worker::protocol::RateLimitPolicy> {
        let active_version = self.active_versions.get(model_name)?;
        let model = self.models.get(model_name)?;
        let mv = model.versions.get(active_version.value())?;
        mv.policies.rate_limit.clone()
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
        assert_eq!(mv.status, VersionStatus::Loading);
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
