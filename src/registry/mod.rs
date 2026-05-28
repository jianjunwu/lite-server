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
