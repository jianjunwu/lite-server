use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub grpc: GrpcConfig,
    pub metrics: MetricsConfig,
    pub rate_limit: RateLimitConfig,
    pub logging: LoggingConfig,
    pub model_repository: ModelRepositoryConfig,
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
    pub threads: Option<usize>,
    pub cache_registry: bool,
    /// Max seconds to wait for in-flight requests during graceful shutdown.
    pub graceful_timeout: f32,
    /// HTTP keep-alive timeout in seconds. 0 = disable keep-alive.
    pub keepalive_timeout: f32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_port: 8000,
            grpc_port: 8001,
            metrics_port: 8002,
            host: "0.0.0.0".to_string(),
            timeout: 30.0,
            threads: None,
            cache_registry: false,
            graceful_timeout: 30.0,
            keepalive_timeout: 5.0,
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

/// Server-wide rate-limiter tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimitConfig {
    /// Hard cap on the number of distinct rate-limit buckets (per IP / route
    /// key). Bounds memory under spoofed-source floods where every request
    /// carries a new source IP. 0 = unbounded.
    pub max_buckets: usize,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self { max_buckets: 65_536 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
    pub info_output: Option<String>,
    pub error_output: Option<String>,
    /// Rotation strategy: "none", "size", "daily", "hourly"
    pub rotation: String,
    /// Max log file size in MB (when rotation=size)
    pub max_size: usize,
    /// Number of rotated log files to keep
    pub backup_count: usize,
    /// Inject the system hostname into log filenames (server.log -> server-<host>.log)
    pub hostname_in_log_name: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            info_output: None,
            error_output: None,
            rotation: "none".to_string(),
            max_size: 100,
            backup_count: 7,
            hostname_in_log_name: false,
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
    /// Jitter range for max_requests to prevent thundering herd on worker recycle.
    pub max_requests_jitter: Option<usize>,
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
    /// Initial traffic weights per version (§4.3 canary). Applied to
    /// `ModelEntry.weights`; versions not listed get weight 0.
    pub weights: Option<std::collections::HashMap<String, u32>>,
}

impl Default for ModelStrategyConfig {
    fn default() -> Self {
        Self {
            name: "".to_string(),
            load_policy: "explicit".to_string(),
            versions_to_load: vec![],
            default_version: None,
            max_loaded_versions: None,
            weights: None,
        }
    }
}

/// HTTP hook configuration for worker lifecycle events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpHookConfig {
    pub url: String,
    #[serde(default = "default_http_method")]
    pub method: String,
    pub body_template: Option<String>,
}

fn default_http_method() -> String {
    "POST".to_string()
}

/// Worker lifecycle hook configuration.
/// Shell commands and HTTP hooks are both fire-and-forget, non-blocking.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct WorkerHooksConfig {
    pub on_ready: Option<String>,
    pub on_exit: Option<String>,
    pub on_error: Option<String>,
    pub on_ready_http: Option<HttpHookConfig>,
    pub on_exit_http: Option<HttpHookConfig>,
    pub on_error_http: Option<HttpHookConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelConfig {
    pub name: String,
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
    /// Random jitter added to max_requests to prevent thundering herd. 0 = disabled.
    pub max_requests_jitter: usize,
    /// Active health check interval in seconds. 0 = disabled.
    pub health_check_interval: f32,
    /// Worker lifecycle hooks (shell commands and HTTP callbacks).
    pub hooks: WorkerHooksConfig,
    /// Heartbeat probe interval in seconds. 0 = disabled.
    pub heartbeat_interval: f32,
    /// Heartbeat timeout in seconds — max time to wait for a probe response.
    pub heartbeat_timeout: f32,
    /// Consecutive heartbeat failures before killing the worker.
    pub heartbeat_max_failures: usize,
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            name: "".to_string(),
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
            max_requests_jitter: 0,
            health_check_interval: 15.0,
            hooks: WorkerHooksConfig::default(),
            heartbeat_interval: 0.0,
            heartbeat_timeout: 5.0,
            heartbeat_max_failures: 3,
        }
    }
}

/// Returns the Unix socket path if `host` starts with `unix:`, otherwise `None`.
pub fn unix_socket_path(host: &str) -> Option<&str> {
    host.strip_prefix("unix:")
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
    pub threads: Option<usize>,
    pub timeout: Option<f32>,
    pub log_level: Option<String>,
    pub log_info_output: Option<String>,
    pub log_error_output: Option<String>,
    pub log_rotation: Option<String>,
    pub grpc_port: Option<u16>,
    pub metrics_port: Option<u16>,
    pub no_grpc: bool,
    pub no_metrics: bool,
    pub no_streaming_metrics: bool,
    pub max_queue_size: Option<usize>,
    pub max_requests: Option<usize>,
    pub max_requests_jitter: Option<usize>,
    pub request_timeout: Option<f32>,
    pub health_check_interval: Option<f32>,
    pub graceful_timeout: Option<f32>,
    pub keepalive_timeout: Option<f32>,
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
        if let Some(t) = cli.threads {
            self.server.threads = Some(t);
        }
        if let Some(t) = cli.timeout {
            self.server.timeout = t;
        }
        if let Some(ref l) = cli.log_level {
            self.logging.level = l.clone();
        }
        if let Some(ref p) = cli.log_info_output {
            self.logging.info_output = Some(p.clone());
        }
        if let Some(ref p) = cli.log_error_output {
            self.logging.error_output = Some(p.clone());
        }
        if let Some(ref r) = cli.log_rotation {
            self.logging.rotation = r.clone();
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
        if let Some(v) = cli.max_requests_jitter {
            self.model_defaults.max_requests_jitter = Some(v);
        }
        if let Some(v) = cli.request_timeout {
            self.model_defaults.request_timeout = Some(v);
        }
        if let Some(v) = cli.health_check_interval {
            self.model_defaults.health_check_interval = Some(v);
        }
        if let Some(t) = cli.graceful_timeout {
            self.server.graceful_timeout = t;
        }
        if let Some(t) = cli.keepalive_timeout {
            self.server.keepalive_timeout = t;
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
        if let Some(v) = self.model_defaults.max_requests_jitter {
            model.max_requests_jitter = v;
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
        assert_eq!(cfg.graceful_timeout, 30.0);
        assert_eq!(cfg.keepalive_timeout, 5.0);
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
        fs::write(&path, "max_batch_size: 4\nstream: true\n").unwrap();

        let cfg = load_model_config(&path).unwrap();
        assert_eq!(cfg.max_batch_size, 4);
        assert!(cfg.stream);

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
    fn test_logging_hostname_in_log_name_from_yaml() {
        // Explicit `true` in server.yaml is honored.
        let yaml = "logging:\n  hostname_in_log_name: true\n";
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.logging.hostname_in_log_name);

        // Omitting the field falls back to the serde default (false).
        let yaml = "logging:\n  level: debug\n";
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(!cfg.logging.hostname_in_log_name);
    }

    #[test]
    fn test_examples_logging_yaml_loads_with_hostname() {
        // The 11_logging example ships hostname_in_log_name: true; ensure it loads.
        let cfg = load_config("examples/11_logging/server.yaml").unwrap();
        assert!(cfg.logging.hostname_in_log_name);
        assert!(cfg.logging.info_output.is_some());
    }

    #[test]
    fn test_model_config_yaml_roundtrip() {
        let cfg = ModelConfig {
            max_batch_size: 8,
            stream: true,
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: ModelConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.max_batch_size, 8);
        assert!(parsed.stream);
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

        assert_eq!(cfg.logging.level, "debug");
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

    // --- Unix socket path detection ---

    #[test]
    fn test_unix_socket_path_detects_unix_prefix() {
        assert_eq!(unix_socket_path("unix:/tmp/lite-server.sock"), Some("/tmp/lite-server.sock"));
        assert_eq!(unix_socket_path("unix:./lite-server.sock"), Some("./lite-server.sock"));
    }

    #[test]
    fn test_unix_socket_path_returns_none_for_tcp_host() {
        assert_eq!(unix_socket_path("0.0.0.0"), None);
        assert_eq!(unix_socket_path("127.0.0.1"), None);
        assert_eq!(unix_socket_path("[::1]"), None);
    }

    // --- Config YAML with new fields ---

    #[test]
    fn test_load_config_with_unix_host_and_timeouts() {
        let dir = std::env::temp_dir().join("lite-server-config-test-unix");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("server.yaml");
        fs::write(
            &path,
            "server:\n  host: unix:/tmp/test.sock\n  graceful_timeout: 60.0\n  keepalive_timeout: 10.0\n",
        )
        .unwrap();

        let cfg = load_config(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.server.host, "unix:/tmp/test.sock");
        assert_eq!(cfg.server.graceful_timeout, 60.0);
        assert_eq!(cfg.server.keepalive_timeout, 10.0);

        let _ = fs::remove_dir_all(&dir);
    }

    // --- CLI overrides for new fields ---

    #[test]
    fn test_apply_overrides_graceful_timeout() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            graceful_timeout: Some(60.0),
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);
        assert_eq!(cfg.server.graceful_timeout, 60.0);
    }

    #[test]
    fn test_apply_overrides_keepalive_timeout() {
        let mut cfg = Config::default();
        let overrides = CliOverrides {
            keepalive_timeout: Some(0.0),
            ..Default::default()
        };
        cfg.apply_overrides(&overrides);
        assert_eq!(cfg.server.keepalive_timeout, 0.0);
    }

    #[test]
    fn test_new_fields_yaml_roundtrip() {
        let cfg = Config {
            server: ServerConfig {
                host: "unix:/tmp/test.sock".to_string(),
                graceful_timeout: 60.0,
                keepalive_timeout: 10.0,
                ..Default::default()
            },
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: Config = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.server.host, "unix:/tmp/test.sock");
        assert_eq!(parsed.server.graceful_timeout, 60.0);
        assert_eq!(parsed.server.keepalive_timeout, 10.0);
    }

    // --- max_requests_jitter ---

    #[test]
    fn test_max_requests_jitter_default() {
        let cfg = ModelConfig::default();
        assert_eq!(cfg.max_requests_jitter, 0);
    }

    #[test]
    fn test_max_requests_jitter_yaml_roundtrip() {
        let cfg = ModelConfig {
            max_requests: 100,
            max_requests_jitter: 10,
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: ModelConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.max_requests, 100);
        assert_eq!(parsed.max_requests_jitter, 10);
    }

    // --- Worker Hooks ---

    #[test]
    fn test_worker_hooks_default_empty() {
        let cfg = ModelConfig::default();
        assert!(cfg.hooks.on_ready.is_none());
        assert!(cfg.hooks.on_exit.is_none());
        assert!(cfg.hooks.on_error.is_none());
    }

    #[test]
    fn test_worker_hooks_yaml_roundtrip() {
        let cfg = ModelConfig {
            hooks: WorkerHooksConfig {
                on_ready: Some("echo ready".to_string()),
                on_exit: Some("echo exit".to_string()),
                on_error: Some("echo error".to_string()),
                on_ready_http: Some(HttpHookConfig {
                    url: "http://localhost/ready".to_string(),
                    method: "POST".to_string(),
                    body_template: Some(r#"{"model":"$MODEL"}"#.to_string()),
                }),
                on_exit_http: None,
                on_error_http: None,
            },
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: ModelConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.hooks.on_ready, Some("echo ready".to_string()));
        assert_eq!(parsed.hooks.on_exit, Some("echo exit".to_string()));
        assert!(parsed.hooks.on_ready_http.is_some());
        assert_eq!(parsed.hooks.on_ready_http.as_ref().unwrap().method, "POST");
    }

    #[test]
    fn test_http_hook_config_default_method() {
        // When method is not specified, should default to POST
        let yaml = r#"url: "http://localhost/hook""#;
        let parsed: HttpHookConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed.method, "POST");
    }

    #[test]
    fn test_heartbeat_config_defaults() {
        let cfg = ModelConfig::default();
        assert_eq!(cfg.heartbeat_interval, 0.0);
        assert_eq!(cfg.heartbeat_timeout, 5.0);
        assert_eq!(cfg.heartbeat_max_failures, 3);
    }

    #[test]
    fn test_heartbeat_config_yaml_roundtrip() {
        let yaml = r#"
name: test
heartbeat_interval: 10.0
heartbeat_timeout: 3.0
heartbeat_max_failures: 5
"#;
        let cfg: ModelConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.heartbeat_interval, 10.0);
        assert_eq!(cfg.heartbeat_timeout, 3.0);
        assert_eq!(cfg.heartbeat_max_failures, 5);
    }

    // --- rate_limit config (#7) ---

    #[test]
    fn test_rate_limit_config_defaults() {
        let cfg = Config::default();
        assert_eq!(cfg.rate_limit.max_buckets, 65_536);
    }

    #[test]
    fn test_rate_limit_config_yaml_roundtrip() {
        let cfg = Config {
            rate_limit: RateLimitConfig { max_buckets: 4096 },
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed: Config = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.rate_limit.max_buckets, 4096);
    }
}
