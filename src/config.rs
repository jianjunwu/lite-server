use serde::{Deserialize, Serialize};
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
    /// CLI-provided defaults that override per-model config when set.
    pub model_defaults: ModelDefaults,
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
    pub streaming_metrics: bool,
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
            streaming_metrics: true,
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

/// CLI-level defaults for model parameters.  When set (Some), these override
/// the per-model config.yaml values for every loaded model.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ModelDefaults {
    pub max_queue_size: Option<usize>,
    pub max_requests: Option<usize>,
    pub request_timeout: Option<f32>,
    pub health_check_interval: Option<f32>,
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
    /// Per-request hard timeout in seconds. 0 = disabled.
    pub request_timeout: f32,
    /// Auto-restart worker after this many requests. 0 = disabled.
    pub max_requests: usize,
    /// Active health check interval in seconds. 0 = disabled.
    pub health_check_interval: f32,
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
            request_timeout: 0.0,
            max_requests: 0,
            health_check_interval: 15.0,
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

/// CLI 参数覆盖集。None/bool 默认值 = 不覆盖（保持 YAML 或内置默认值）。
#[derive(Debug, Clone, Default)]
pub struct CliOverrides {
    pub port: Option<u16>,
    pub host: Option<String>,
    pub model_repo: Option<String>,
    pub http_workers: Option<usize>,
    pub timeout: Option<f32>,
    pub transport: Option<String>,
    pub log_level: Option<String>,
    pub grpc_port: Option<u16>,
    pub metrics_port: Option<u16>,
    pub no_grpc: bool,
    pub no_metrics: bool,
    pub no_streaming_metrics: bool,
    pub log_verbose: bool,
    pub max_queue_size: Option<usize>,
    pub max_requests: Option<usize>,
    pub request_timeout: Option<f32>,
    pub health_check_interval: Option<f32>,
}

impl Config {
    /// 用 CLI 参数覆盖当前配置，未指定的字段保持不变。
    pub fn apply_overrides(&mut self, cli: &CliOverrides) {
        if let Some(p) = cli.port {
            self.server.http_port = p;
        }
        if let Some(ref h) = cli.host {
            self.server.host = h.clone();
        }
        if let Some(ref r) = cli.model_repo {
            self.model_repository.path = r.clone();
        }
        if let Some(w) = cli.http_workers {
            self.server.http_workers = Some(w);
        }
        if let Some(t) = cli.timeout {
            self.server.timeout = t;
        }
        if let Some(ref t) = cli.transport {
            self.server.transport = t.clone();
        }
        if let Some(ref l) = cli.log_level {
            self.server.log_level = l.clone();
            self.logging.level = l.clone();
        }
        if let Some(gp) = cli.grpc_port {
            self.server.grpc_port = gp;
        }
        if let Some(mp) = cli.metrics_port {
            self.server.metrics_port = mp;
        }
        if cli.no_grpc {
            self.grpc.enabled = false;
        }
        if cli.no_metrics {
            self.metrics.enabled = false;
        }
        if cli.no_streaming_metrics {
            self.features.streaming_metrics = false;
        }
        if let Some(v) = cli.max_queue_size {
            self.model_defaults.max_queue_size = Some(v);
        }
        if let Some(v) = cli.max_requests {
            self.model_defaults.max_requests = Some(v);
        }
        if let Some(v) = cli.request_timeout {
            self.model_defaults.request_timeout = Some(v);
        }
        if let Some(v) = cli.health_check_interval {
            self.model_defaults.health_check_interval = Some(v);
        }
    }

    /// Apply CLI model defaults to a ModelConfig (called per-model at load time).
    pub fn apply_model_defaults(&self, model: &mut ModelConfig) {
        if let Some(v) = self.model_defaults.max_queue_size {
            model.max_queue_size = v;
        }
        if let Some(v) = self.model_defaults.max_requests {
            model.max_requests = v;
        }
        if let Some(v) = self.model_defaults.request_timeout {
            model.request_timeout = v;
        }
        if let Some(v) = self.model_defaults.health_check_interval {
            model.health_check_interval = v;
        }
    }
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
        assert!(cfg.features.streaming_metrics);
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

    // --- apply_overrides ---

    #[test]
    fn test_apply_overrides_partial() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            port: Some(9090),
            host: Some("127.0.0.1".to_string()),
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);

        assert_eq!(cfg.server.http_port, 9090);
        assert_eq!(cfg.server.host, "127.0.0.1");
        // Unchanged fields keep defaults
        assert_eq!(cfg.server.grpc_port, 8001);
        assert_eq!(cfg.server.transport, "zmq");
        assert!(cfg.grpc.enabled);
    }

    #[test]
    fn test_apply_overrides_none_keeps_defaults() {
        let mut cfg = Config::default();
        let overrides = CliOverrides::default();
        cfg.apply_overrides(&overrides);

        assert_eq!(cfg.server.http_port, 8000);
        assert_eq!(cfg.server.host, "0.0.0.0");
        assert!(cfg.grpc.enabled);
        assert!(cfg.metrics.enabled);
        assert!(cfg.features.streaming_metrics);
    }

    #[test]
    fn test_apply_overrides_bool_flags() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            no_grpc: true,
            no_metrics: true,
            no_streaming_metrics: true,
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);

        assert!(!cfg.grpc.enabled);
        assert!(!cfg.metrics.enabled);
        assert!(!cfg.features.streaming_metrics);
    }

    #[test]
    fn test_apply_overrides_log_level_propagates() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            log_level: Some("debug".to_string()),
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);

        assert_eq!(cfg.server.log_level, "debug");
        assert_eq!(cfg.logging.level, "debug");
    }

    #[test]
    fn test_apply_overrides_transport() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            transport: Some("uds".to_string()),
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);

        assert_eq!(cfg.server.transport, "uds");
    }

    // --- Model defaults ---

    #[test]
    fn test_model_defaults_none_by_default() {
        let cfg = Config::default();
        assert!(cfg.model_defaults.max_queue_size.is_none());
        assert!(cfg.model_defaults.max_requests.is_none());
        assert!(cfg.model_defaults.request_timeout.is_none());
        assert!(cfg.model_defaults.health_check_interval.is_none());
    }

    #[test]
    fn test_apply_overrides_model_defaults() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            max_queue_size: Some(500),
            max_requests: Some(10000),
            request_timeout: Some(60.0),
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);

        assert_eq!(cfg.model_defaults.max_queue_size, Some(500));
        assert_eq!(cfg.model_defaults.max_requests, Some(10000));
        assert_eq!(cfg.model_defaults.request_timeout, Some(60.0));
    }

    #[test]
    fn test_apply_model_defaults_overrides_model_config() {
        let cfg = Config {
            model_defaults: ModelDefaults {
                max_queue_size: Some(200),
                max_requests: Some(5000),
                request_timeout: Some(45.0),
                ..Default::default()
            },
            ..Default::default()
        };

        let mut model = ModelConfig::default();
        assert_eq!(model.max_queue_size, 1000); // default
        assert_eq!(model.max_requests, 0); // default
        assert_eq!(model.request_timeout, 0.0); // default

        cfg.apply_model_defaults(&mut model);
        assert_eq!(model.max_queue_size, 200);
        assert_eq!(model.max_requests, 5000);
        assert_eq!(model.request_timeout, 45.0);
    }

    #[test]
    fn test_apply_model_defaults_none_keeps_model_config() {
        let cfg = Config::default(); // all model_defaults are None

        let mut model = ModelConfig {
            max_queue_size: 500,
            max_requests: 100,
            request_timeout: 10.0,
            ..Default::default()
        };

        cfg.apply_model_defaults(&mut model);
        assert_eq!(model.max_queue_size, 500); // unchanged
        assert_eq!(model.max_requests, 100); // unchanged
        assert_eq!(model.request_timeout, 10.0); // unchanged
    }

    #[test]
    fn test_model_defaults_yaml_roundtrip() {
        let cfg = Config {
            model_defaults: ModelDefaults {
                max_queue_size: Some(300),
                max_requests: Some(8000),
                request_timeout: Some(120.0),
                ..Default::default()
            },
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: Config = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.model_defaults.max_queue_size, Some(300));
        assert_eq!(parsed.model_defaults.max_requests, Some(8000));
        assert_eq!(parsed.model_defaults.request_timeout, Some(120.0));
    }

    // --- health_check_interval ---

    #[test]
    fn test_health_check_interval_default() {
        let cfg = ModelConfig::default();
        assert_eq!(cfg.health_check_interval, 15.0);
    }

    #[test]
    fn test_health_check_interval_zero_disables() {
        let cfg = ModelConfig {
            health_check_interval: 0.0,
            ..Default::default()
        };
        assert_eq!(cfg.health_check_interval, 0.0);
    }

    #[test]
    fn test_apply_overrides_health_check_interval() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            health_check_interval: Some(30.0),
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);
        assert_eq!(cfg.model_defaults.health_check_interval, Some(30.0));
    }

    #[test]
    fn test_apply_model_defaults_health_check_interval() {
        let cfg = Config {
            model_defaults: ModelDefaults {
                health_check_interval: Some(10.0),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut model = ModelConfig::default();
        assert_eq!(model.health_check_interval, 15.0); // default
        cfg.apply_model_defaults(&mut model);
        assert_eq!(model.health_check_interval, 10.0);
    }

    #[test]
    fn test_health_check_interval_yaml_roundtrip() {
        let cfg = ModelConfig {
            health_check_interval: 20.0,
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: ModelConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.health_check_interval, 20.0);
    }
}
