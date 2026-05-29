use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub grpc: GrpcConfig,
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
    pub model_repository: ModelRepositoryConfig,
    pub webui: WebUIConfig,
    pub features: FeaturesConfig,
    pub orchestration: OrchestrationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub http_port: u16,
    pub grpc_port: u16,
    pub metrics_port: u16,
    pub host: String,
    pub timeout: f32,
    pub log_level: String,
    pub http_workers: Option<usize>,
    pub transport: String,
    pub cache_registry: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_port: 8000,
            grpc_port: 8001,
            metrics_port: 8002,
            host: "0.0.0.0".to_string(),
            timeout: 30.0,
            log_level: "info".to_string(),
            http_workers: None,
            transport: "zmq".to_string(),
            cache_registry: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GrpcConfig {
    pub enabled: bool,
    pub max_workers: usize,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_workers: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub enabled: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub mode: String,
    pub level: String,
    pub format: String,
    pub output: Option<String>,
    pub info_output: Option<String>,
    pub error_output: Option<String>,
    pub rotation: String,
    pub rotate_by: String,
    pub max_size: usize,
    pub when: String,
    pub backup_count: usize,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            mode: "queue".to_string(),
            level: "info".to_string(),
            format: "text".to_string(),
            output: None,
            info_output: None,
            error_output: None,
            rotation: "none".to_string(),
            rotate_by: "none".to_string(),
            max_size: 100,
            when: "midnight".to_string(),
            backup_count: 7,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelRepositoryConfig {
    pub path: String,
}

impl Default for ModelRepositoryConfig {
    fn default() -> Self {
        Self {
            path: "./model_repo".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WebUIConfig {
    pub enabled: bool,
    pub report_retention_days: usize,
}

impl Default for WebUIConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            report_retention_days: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FeaturesConfig {
    pub timeline: bool,
    pub system_overview: bool,
    pub custom_metrics: bool,
    pub benchmarks: bool,
    pub playground: bool,
    pub alerts: bool,
    pub version_compare: bool,
    pub streaming: bool,
    pub grpc_streaming: bool,
    pub sse: bool,
    pub websocket_streaming: bool,
}

impl Default for FeaturesConfig {
    fn default() -> Self {
        Self {
            timeline: false,
            system_overview: true,
            custom_metrics: false,
            benchmarks: true,
            playground: false,
            alerts: true,
            version_compare: false,
            streaming: true,
            grpc_streaming: true,
            sse: true,
            websocket_streaming: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OrchestrationConfig {
    pub control_mode: String,
    pub poll_interval: u64,
    pub load_models: Vec<String>,
    pub models: Vec<ModelStrategyConfig>,
}

impl Default for OrchestrationConfig {
    fn default() -> Self {
        Self {
            control_mode: "explicit".to_string(),
            poll_interval: 5,
            load_models: vec![],
            models: vec![],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelStrategyConfig {
    pub name: String,
    pub load_policy: String,
    pub versions_to_load: Vec<String>,
    pub default_version: Option<String>,
    pub max_loaded_versions: Option<usize>,
}

impl Default for ModelStrategyConfig {
    fn default() -> Self {
        Self {
            name: "".to_string(),
            load_policy: "explicit".to_string(),
            versions_to_load: vec![],
            default_version: None,
            max_loaded_versions: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelConfig {
    pub name: String,
    pub api_path: String,
    pub max_batch_size: usize,
    pub batch_timeout: f32,
    pub stream: bool,
    pub bidirectional: bool,
    pub continuous_batching: bool,
    pub max_sequence_length: usize,
    pub accelerator: Option<String>,
    pub devices: Option<serde_json::Value>,
    pub workers_per_device: Option<usize>,
    pub max_queue_size: usize,
    pub queue_mode: String,
    pub hot_reload: bool,
    pub hot_reload_patterns: Vec<String>,
    pub hot_reload_interval: f32,
    pub adaptive_batching: bool,
    pub min_batch_timeout: f32,
    pub adaptive_queue_threshold: usize,
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            name: "".to_string(),
            api_path: "/predict".to_string(),
            max_batch_size: 1,
            batch_timeout: 0.0,
            stream: false,
            bidirectional: false,
            continuous_batching: false,
            max_sequence_length: 2048,
            accelerator: None,
            devices: None,
            workers_per_device: None,
            max_queue_size: 1000,
            queue_mode: "per_worker".to_string(),
            hot_reload: false,
            hot_reload_patterns: vec!["*.py".to_string()],
            hot_reload_interval: 1.0,
            adaptive_batching: false,
            min_batch_timeout: 0.001,
            adaptive_queue_threshold: 10,
        }
    }
}

pub fn load_config(path: &str) -> anyhow::Result<Config> {
    let content = std::fs::read_to_string(path)?;
    let config: Config = serde_yaml::from_str(&content)?;
    Ok(config)
}

pub fn load_orchestration(path: &Path) -> anyhow::Result<OrchestrationConfig> {
    if !path.exists() {
        return Ok(OrchestrationConfig::default());
    }
    let content = std::fs::read_to_string(path)?;
    let orch: OrchestrationConfig = serde_yaml::from_str(&content)?;
    Ok(orch)
}

pub fn load_model_config(path: &Path) -> anyhow::Result<ModelConfig> {
    if !path.exists() {
        return Ok(ModelConfig::default());
    }
    let content = std::fs::read_to_string(path)?;
    let config: ModelConfig = serde_yaml::from_str(&content)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // --- Default values ---

    #[test]
    fn test_server_config_defaults() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.http_port, 8000);
        assert_eq!(cfg.grpc_port, 8001);
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.timeout, 30.0);
        assert_eq!(cfg.transport, "zmq");
    }

    #[test]
    fn test_config_defaults() {
        let cfg = Config::default();
        assert_eq!(cfg.server.http_port, 8000);
        assert!(cfg.grpc.enabled);
        assert!(cfg.metrics.enabled);
        assert_eq!(cfg.model_repository.path, "./model_repo");
        assert!(cfg.features.streaming);
        assert!(cfg.features.websocket_streaming);
        assert_eq!(cfg.orchestration.control_mode, "explicit");
    }

    #[test]
    fn test_model_config_defaults() {
        let cfg = ModelConfig::default();
        assert_eq!(cfg.api_path, "/predict");
        assert_eq!(cfg.max_batch_size, 1);
        assert!(!cfg.stream);
        assert!(!cfg.continuous_batching);
        assert_eq!(cfg.max_queue_size, 1000);
    }

    // --- load_config ---

    #[test]
    fn test_load_config_valid_yaml() {
        let dir = std::env::temp_dir().join("lite-server-config-test-valid");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("server.yaml");
        fs::write(&path, "server:\n  http_port: 9090\n  host: 127.0.0.1\n").unwrap();

        let cfg = load_config(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.server.http_port, 9090);
        assert_eq!(cfg.server.host, "127.0.0.1");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_config_missing_file() {
        let result = load_config("/tmp/nonexistent-lite-server-config.yaml");
        assert!(result.is_err());
    }

    #[test]
    fn test_load_config_invalid_yaml() {
        let dir = std::env::temp_dir().join("lite-server-config-test-invalid");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("bad.yaml");
        fs::write(&path, "server: [invalid").unwrap();

        let result = load_config(path.to_str().unwrap());
        assert!(result.is_err());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_config_partial_fields() {
        let dir = std::env::temp_dir().join("lite-server-config-test-partial");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("partial.yaml");
        fs::write(&path, "server:\n  http_port: 7777\n").unwrap();

        let cfg = load_config(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.server.http_port, 7777);
        // Other fields should be defaults
        assert_eq!(cfg.server.host, "0.0.0.0");
        assert_eq!(cfg.server.grpc_port, 8001);

        let _ = fs::remove_dir_all(&dir);
    }

    // --- load_model_config ---

    #[test]
    fn test_load_model_config_valid() {
        let dir = std::env::temp_dir().join("lite-server-model-config-valid");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("config.yaml");
        fs::write(&path, "max_batch_size: 4\nstream: true\napi_path: /generate\n").unwrap();

        let cfg = load_model_config(&path).unwrap();
        assert_eq!(cfg.max_batch_size, 4);
        assert!(cfg.stream);
        assert_eq!(cfg.api_path, "/generate");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_model_config_missing_file_returns_default() {
        let path = Path::new("/tmp/nonexistent-lite-server-model-config.yaml");
        let cfg = load_model_config(path).unwrap();
        assert_eq!(cfg.max_batch_size, 1);
        assert!(!cfg.stream);
    }

    #[test]
    fn test_load_model_config_empty_file_returns_default() {
        let dir = std::env::temp_dir().join("lite-server-model-config-empty");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("config.yaml");
        fs::write(&path, "").unwrap();

        let cfg = load_model_config(&path).unwrap();
        assert_eq!(cfg.max_batch_size, 1);

        let _ = fs::remove_dir_all(&dir);
    }

    // --- load_orchestration ---

    #[test]
    fn test_load_orchestration_valid() {
        let dir = std::env::temp_dir().join("lite-server-orch-valid");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("orchestration.yaml");
        fs::write(&path, "control_mode: auto\npoll_interval: 10\n").unwrap();

        let orch = load_orchestration(&path).unwrap();
        assert_eq!(orch.control_mode, "auto");
        assert_eq!(orch.poll_interval, 10);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_orchestration_missing_file_returns_default() {
        let path = Path::new("/tmp/nonexistent-lite-server-orch.yaml");
        let orch = load_orchestration(path).unwrap();
        assert_eq!(orch.control_mode, "explicit");
    }

    // --- Serialization roundtrip ---

    #[test]
    fn test_config_yaml_roundtrip() {
        let cfg = Config::default();
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: Config = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.server.http_port, cfg.server.http_port);
        assert_eq!(parsed.server.host, cfg.server.host);
        assert_eq!(parsed.grpc.enabled, cfg.grpc.enabled);
    }

    #[test]
    fn test_model_config_yaml_roundtrip() {
        let cfg = ModelConfig {
            max_batch_size: 8,
            stream: true,
            api_path: "/v2/generate".to_string(),
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: ModelConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.max_batch_size, 8);
        assert!(parsed.stream);
        assert_eq!(parsed.api_path, "/v2/generate");
    }
}
