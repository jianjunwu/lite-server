pub mod types;

use crate::config::{ModelConfig, ModelStrategyConfig};
use crate::registry::types::LoadPolicy;
use crate::error::AppError;
use crate::registry::types::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct ModelRegistry {
    models: Arc<RwLock<HashMap<String, ModelEntry>>>,
    active_versions: Arc<RwLock<HashMap<String, String>>>,
}

impl ModelRegistry {
    pub fn new() -> Self {
        Self {
            models: Arc::new(RwLock::new(HashMap::new())),
            active_versions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn list_loaded(&self) -> Vec<(String, String, ModelVersion)> {
        let models = self.models.read().await;
        let mut result = Vec::new();
        for (name, entry) in models.iter() {
            for (version, mv) in entry.versions.iter() {
                result.push((name.clone(), version.clone(), mv.clone()));
            }
        }
        result
    }

    pub async fn list_versions(&self, model_name: &str) -> Vec<ModelVersion> {
        let models = self.models.read().await;
        models
            .get(model_name)
            .map(|e| e.versions.values().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn get(&self, model_name: &str, version: Option<&str>) -> Option<ModelVersion> {
        let models = self.models.read().await;
        let entry = models.get(model_name)?;

        let version = match version {
            Some(v) => v.to_string(),
            None => {
                let active = self.active_versions.read().await;
                active.get(model_name)?.clone()
            }
        };

        entry.versions.get(&version).cloned()
    }

    pub async fn is_ready(&self, model_name: &str, version: Option<&str>) -> bool {
        match self.get(model_name, version).await {
            Some(mv) => mv.status == VersionStatus::Ready,
            None => false,
        }
    }

    pub async fn get_active_version(&self, model_name: &str) -> Option<String> {
        let active = self.active_versions.read().await;
        active.get(model_name).cloned()
    }

    pub async fn register(
        &self,
        model_name: &str,
        version: &str,
        config: ModelConfig,
        model_type: ModelType,
        model_dir: PathBuf,
    ) -> Result<(), AppError> {
        let mut models = self.models.write().await;
        let entry = models
            .entry(model_name.to_string())
            .or_insert_with(|| ModelEntry::new(model_name));

        let mv = ModelVersion {
            version: version.to_string(),
            status: VersionStatus::Loading,
            config,
            model_type,
            model_dir,
            workers: vec![],
        };
        entry.versions.insert(version.to_string(), mv);
        Ok(())
    }

    pub async fn set_status(
        &self,
        model_name: &str,
        version: &str,
        status: VersionStatus,
    ) -> Result<(), AppError> {
        let mut models = self.models.write().await;
        let entry = models
            .get_mut(model_name)
            .ok_or_else(|| AppError::ModelNotFound(model_name.to_string()))?;
        let mv = entry
            .versions
            .get_mut(version)
            .ok_or_else(|| AppError::VersionNotFound(model_name.to_string(), version.to_string()))?;
        mv.status = status;
        Ok(())
    }

    pub async fn set_workers(
        &self,
        model_name: &str,
        version: &str,
        workers: Vec<WorkerInfo>,
    ) -> Result<(), AppError> {
        let mut models = self.models.write().await;
        let entry = models
            .get_mut(model_name)
            .ok_or_else(|| AppError::ModelNotFound(model_name.to_string()))?;
        let mv = entry
            .versions
            .get_mut(version)
            .ok_or_else(|| AppError::VersionNotFound(model_name.to_string(), version.to_string()))?;
        mv.workers = workers;
        Ok(())
    }

    pub async fn activate_version(&self, model_name: &str, version: &str) -> Result<bool, AppError> {
        let models = self.models.read().await;
        let entry = models
            .get(model_name)
            .ok_or_else(|| AppError::ModelNotFound(model_name.to_string()))?;
        let mv = entry
            .versions
            .get(version)
            .ok_or_else(|| AppError::VersionNotFound(model_name.to_string(), version.to_string()))?;

        if mv.status != VersionStatus::Ready {
            return Ok(false);
        }
        drop(models);

        let mut active = self.active_versions.write().await;
        active.insert(model_name.to_string(), version.to_string());
        Ok(true)
    }

    pub async fn deactivate(&self, model_name: &str) {
        let mut active = self.active_versions.write().await;
        active.remove(model_name);
    }

    pub async fn remove(&self, model_name: &str, version: &str) -> Result<(), AppError> {
        let mut models = self.models.write().await;
        if let Some(entry) = models.get_mut(model_name) {
            entry.versions.remove(version);
            if entry.versions.is_empty() {
                models.remove(model_name);
            }
        }

        let active_version = {
            let active = self.active_versions.read().await;
            active.get(model_name).cloned()
        };

        if active_version.as_deref() == Some(version) {
            let mut active = self.active_versions.write().await;
            active.remove(model_name);

            // Try auto-activate another ready version
            if let Some(entry) = models.get(model_name) {
                for (v, mv) in entry.versions.iter() {
                    if mv.status == VersionStatus::Ready {
                        active.insert(model_name.to_string(), v.clone());
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn set_strategy(
        &self,
        model_name: &str,
        strategy: &ModelStrategyConfig,
    ) -> Result<(), AppError> {
        let mut models = self.models.write().await;
        let entry = models
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

    pub async fn get_strategy(&self, model_name: &str) -> Option<ModelStrategyConfig> {
        let models = self.models.read().await;
        let entry = models.get(model_name)?;
        Some(ModelStrategyConfig {
            name: model_name.to_string(),
            load_policy: match entry.load_policy {
                LoadPolicy::All => "all".to_string(),
                LoadPolicy::Latest => "latest".to_string(),
                LoadPolicy::Explicit => "explicit".to_string(),
            },
            versions_to_load: entry.versions.keys().cloned().collect(),
            default_version: self.get_active_version(model_name).await,
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
    use crate::registry::types::*;

    fn test_config() -> ModelConfig {
        ModelConfig {
            max_batch_size: 1,
            ..Default::default()
        }
    }

    fn tmp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("lite-server-reg-test-{}", std::process::id()))
    }

    // --- Basic lifecycle ---

    #[tokio::test]
    async fn test_register_and_get() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();

        let mv = reg.get("m1", Some("1")).await.unwrap();
        assert_eq!(mv.version, "1");
        assert_eq!(mv.status, VersionStatus::Loading);
    }

    #[tokio::test]
    async fn test_set_status() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();

        reg.set_status("m1", "1", VersionStatus::Ready)
            .await
            .unwrap();

        let mv = reg.get("m1", Some("1")).await.unwrap();
        assert_eq!(mv.status, VersionStatus::Ready);
    }

    #[tokio::test]
    async fn test_is_ready() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();

        assert!(!reg.is_ready("m1", Some("1")).await);

        reg.set_status("m1", "1", VersionStatus::Ready)
            .await
            .unwrap();

        assert!(reg.is_ready("m1", Some("1")).await);
    }

    #[tokio::test]
    async fn test_is_ready_nonexistent() {
        let reg = ModelRegistry::new();
        assert!(!reg.is_ready("nope", Some("1")).await);
    }

    // --- Activate version ---

    #[tokio::test]
    async fn test_activate_version() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();
        reg.set_status("m1", "1", VersionStatus::Ready)
            .await
            .unwrap();

        let ok = reg.activate_version("m1", "1").await.unwrap();
        assert!(ok);
        assert_eq!(reg.get_active_version("m1").await, Some("1".to_string()));
    }

    #[tokio::test]
    async fn test_activate_not_ready_fails() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();
        // status is Loading, not Ready

        let ok = reg.activate_version("m1", "1").await.unwrap();
        assert!(!ok);
        assert_eq!(reg.get_active_version("m1").await, None);
    }

    #[tokio::test]
    async fn test_activate_nonexistent_model_errors() {
        let reg = ModelRegistry::new();
        let result = reg.activate_version("nope", "1").await;
        assert!(result.is_err());
    }

    // --- Deactivate ---

    #[tokio::test]
    async fn test_deactivate() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();
        reg.set_status("m1", "1", VersionStatus::Ready)
            .await
            .unwrap();
        reg.activate_version("m1", "1").await.unwrap();

        reg.deactivate("m1").await;
        assert_eq!(reg.get_active_version("m1").await, None);
    }

    // --- Multiple versions ---

    #[tokio::test]
    async fn test_multiple_versions() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();
        reg.register("m1", "2", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();

        let versions = reg.list_versions("m1").await;
        assert_eq!(versions.len(), 2);
    }

    #[tokio::test]
    async fn test_activate_switches_version() {
        let reg = ModelRegistry::new();
        for v in &["1", "2"] {
            reg.register("m1", v, test_config(), ModelType::LitAPI, tmp_dir())
                .await
                .unwrap();
            reg.set_status("m1", v, VersionStatus::Ready)
                .await
                .unwrap();
        }

        reg.activate_version("m1", "1").await.unwrap();
        assert_eq!(reg.get_active_version("m1").await, Some("1".to_string()));

        reg.activate_version("m1", "2").await.unwrap();
        assert_eq!(reg.get_active_version("m1").await, Some("2".to_string()));
    }

    // --- Remove ---

    #[tokio::test]
    async fn test_remove_version() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();

        reg.remove("m1", "1").await.unwrap();
        assert!(reg.get("m1", Some("1")).await.is_none());
    }

    #[tokio::test]
    async fn test_remove_auto_activates_another_ready() {
        let reg = ModelRegistry::new();
        for v in &["1", "2"] {
            reg.register("m1", v, test_config(), ModelType::LitAPI, tmp_dir())
                .await
                .unwrap();
            reg.set_status("m1", v, VersionStatus::Ready)
                .await
                .unwrap();
        }
        reg.activate_version("m1", "1").await.unwrap();

        reg.remove("m1", "1").await.unwrap();
        // Should auto-activate v2
        assert_eq!(reg.get_active_version("m1").await, Some("2".to_string()));
    }

    #[tokio::test]
    async fn test_remove_active_no_other_ready_clears_active() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();
        reg.set_status("m1", "1", VersionStatus::Ready)
            .await
            .unwrap();
        reg.activate_version("m1", "1").await.unwrap();

        reg.remove("m1", "1").await.unwrap();
        assert_eq!(reg.get_active_version("m1").await, None);
    }

    #[tokio::test]
    async fn test_remove_last_version_removes_model() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();

        reg.remove("m1", "1").await.unwrap();
        assert!(reg.list_loaded().await.is_empty());
    }

    // --- list_loaded ---

    #[tokio::test]
    async fn test_list_loaded_empty() {
        let reg = ModelRegistry::new();
        assert!(reg.list_loaded().await.is_empty());
    }

    #[tokio::test]
    async fn test_list_loaded_multiple_models() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();
        reg.register("m2", "1", test_config(), ModelType::Ensemble, tmp_dir())
            .await
            .unwrap();

        let loaded = reg.list_loaded().await;
        assert_eq!(loaded.len(), 2);
    }

    // --- Set workers ---

    #[tokio::test]
    async fn test_set_workers() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();

        let workers = vec![WorkerInfo {
            worker_id: 0,
            device: "cpu:0".to_string(),
            endpoint: "ipc:///tmp/w0.sock".to_string(),
            pid: Some(1234),
            status: WorkerStatus::Ready,
        }];
        reg.set_workers("m1", "1", workers).await.unwrap();

        let mv = reg.get("m1", Some("1")).await.unwrap();
        assert_eq!(mv.workers.len(), 1);
        assert_eq!(mv.workers[0].pid, Some(1234));
    }

    // --- get with active version ---

    #[tokio::test]
    async fn test_get_uses_active_version_when_none_specified() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();
        reg.set_status("m1", "1", VersionStatus::Ready)
            .await
            .unwrap();
        reg.activate_version("m1", "1").await.unwrap();

        let mv = reg.get("m1", None).await.unwrap();
        assert_eq!(mv.version, "1");
    }

    #[tokio::test]
    async fn test_get_returns_none_when_no_active() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();

        let mv = reg.get("m1", None).await;
        assert!(mv.is_none());
    }

    // --- Strategy ---

    #[tokio::test]
    async fn test_set_and_get_strategy() {
        let reg = ModelRegistry::new();
        let strategy = ModelStrategyConfig {
            name: "m1".to_string(),
            load_policy: "latest".to_string(),
            max_loaded_versions: Some(2),
            ..Default::default()
        };

        reg.set_strategy("m1", &strategy).await.unwrap();
        let got = reg.get_strategy("m1").await.unwrap();
        assert_eq!(got.load_policy, "latest");
        assert_eq!(got.max_loaded_versions, Some(2));
    }

    // --- set_status on nonexistent ---

    #[tokio::test]
    async fn test_set_status_nonexistent_model_errors() {
        let reg = ModelRegistry::new();
        let result = reg.set_status("nope", "1", VersionStatus::Ready).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_status_nonexistent_version_errors() {
        let reg = ModelRegistry::new();
        reg.register("m1", "1", test_config(), ModelType::LitAPI, tmp_dir())
            .await
            .unwrap();

        let result = reg.set_status("m1", "99", VersionStatus::Ready).await;
        assert!(result.is_err());
    }
}
