pub mod callback;
pub mod config;
pub mod rate_limit;
pub mod ensemble;
pub mod error;
pub mod grpc;
pub mod http;
pub mod inference_queue;
pub mod logging;
pub mod metrics;
pub mod proto;
pub mod registry;
pub mod server;
pub mod streaming;
#[cfg(feature = "python")]
pub mod test_support;
pub mod transport;
pub mod validation;
pub mod worker;

#[cfg(feature = "python")]
use pyo3::prelude::*;
use tracing::info;

/// CLI overrides grouped to keep `run_server`'s signature readable
/// (clippy::too_many_arguments). Mirrors the `serve()` pyo3 kwargs.
pub struct ServerOptions {
    pub config: Option<String>,
    pub port: Option<u16>,
    pub host: Option<String>,
    pub model_repo: Option<String>,
    pub threads: Option<usize>,
    pub timeout: Option<f32>,
    pub log_level: Option<String>,
    pub log_info_output: Option<String>,
    pub log_error_output: Option<String>,
    pub log_rotation: Option<String>,
    pub metrics_port: Option<u16>,
    pub no_metrics: Option<bool>,
    pub grpc_port: Option<u16>,
    pub no_grpc: Option<bool>,
    pub no_streaming_metrics: Option<bool>,
    pub max_queue_size: Option<usize>,
    pub max_requests: Option<usize>,
    pub max_requests_jitter: Option<usize>,
    pub request_timeout: Option<f32>,
    pub health_check_interval: Option<f32>,
    pub graceful_timeout: Option<f32>,
    pub keepalive_timeout: Option<f32>,
    pub ejection_error_threshold: Option<usize>,
    pub ejection_timeout: Option<f32>,
    pub ejection_max_percent: Option<usize>,
    pub max_retries: Option<usize>,
    pub startup_timeout: Option<f32>,
    pub health_check_timeout: Option<f32>,
    pub worker_kill_timeout: Option<f32>,
    pub hook_http_timeout: Option<f32>,
}

pub fn run_server(
    opts: ServerOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ServerOptions {
        config,
        port,
        host,
        model_repo,
        threads,
        timeout,
        log_level,
        log_info_output,
        log_error_output,
        log_rotation,
        metrics_port,
        no_metrics,
        grpc_port,
        no_grpc,
        no_streaming_metrics,
        max_queue_size,
        max_requests,
        max_requests_jitter,
        request_timeout,
        health_check_interval,
        graceful_timeout,
        keepalive_timeout,
        ejection_error_threshold,
        ejection_timeout,
        ejection_max_percent,
        max_retries,
        startup_timeout,
        health_check_timeout,
        worker_kill_timeout,
        hook_http_timeout,
    } = opts;
    let mut cfg = if let Some(config_path) = config {
        config::load_config(&config_path)?
    } else {
        config::Config::default()
    };

    cfg.apply_overrides(&config::CliOverrides {
        port,
        host,
        model_repo,
        threads,
        timeout,
        log_level,
        log_info_output,
        log_error_output,
        log_rotation,
        grpc_port,
        metrics_port,
        no_grpc: no_grpc == Some(true),
        no_metrics: no_metrics == Some(true),
        no_streaming_metrics: no_streaming_metrics == Some(true),
        graceful_timeout,
        keepalive_timeout,
        tunables: config::ModelTunables {
            max_queue_size,
            max_requests,
            max_requests_jitter,
            request_timeout,
            health_check_interval,
            ejection_error_threshold,
            ejection_timeout,
            ejection_max_percent,
            max_retries,
            startup_timeout,
            health_check_timeout,
            worker_kill_timeout,
            hook_http_timeout,
            ..Default::default()
        },
    });

    let _log_guard = logging::init(
        &cfg.logging.level,
        cfg.logging.info_output.as_deref(),
        cfg.logging.error_output.as_deref(),
        &cfg.logging.rotation,
        cfg.logging.max_size,
        cfg.logging.backup_count,
        cfg.logging.hostname_in_log_name,
    );
    info!("Starting lite-server v{}", env!("CARGO_PKG_VERSION"));
    info!("HTTP port: {}", cfg.server.http_port);
    info!("Metrics port: {}", cfg.server.metrics_port);
    info!("Model repo: {}", cfg.model_repository.path);

    let threads = cfg.server.threads;
    let server = server::LiteServer::new(cfg);

    let rt = build_runtime(threads);
    rt.block_on(async {
        server.run().await.map_err(|e| {
            Box::<dyn std::error::Error + Send + Sync>::from(format!("Server error: {}", e))
        })
    })
}

fn build_runtime(threads: Option<usize>) -> tokio::runtime::Runtime {
    match threads {
        Some(1) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build single-threaded tokio runtime"),
        n => {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.enable_all();
            if let Some(t) = n {
                builder.worker_threads(t);
            }
            builder
                .build()
                .expect("failed to build multi-threaded tokio runtime")
        }
    }
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (
    config=None,
    port=None,
    host=None,
    model_repo=None,
    threads=None,
    timeout=None,
    log_level=None,
    log_info_output=None,
    log_error_output=None,
    log_rotation=None,
    metrics_port=None,
    no_metrics=None,
    grpc_port=None,
    no_grpc=None,
    no_streaming_metrics=None,
    max_queue_size=None,
    max_requests=None,
    max_requests_jitter=None,
    request_timeout=None,
    health_check_interval=None,
    graceful_timeout=None,
    keepalive_timeout=None,
    ejection_error_threshold=None,
    ejection_timeout=None,
    ejection_max_percent=None,
    max_retries=None,
    startup_timeout=None,
    health_check_timeout=None,
    worker_kill_timeout=None,
    hook_http_timeout=None,
))]
fn serve(
    config: Option<String>,
    port: Option<u16>,
    host: Option<String>,
    model_repo: Option<String>,
    threads: Option<usize>,
    timeout: Option<f32>,
    log_level: Option<String>,
    log_info_output: Option<String>,
    log_error_output: Option<String>,
    log_rotation: Option<String>,
    metrics_port: Option<u16>,
    no_metrics: Option<bool>,
    grpc_port: Option<u16>,
    no_grpc: Option<bool>,
    no_streaming_metrics: Option<bool>,
    max_queue_size: Option<usize>,
    max_requests: Option<usize>,
    max_requests_jitter: Option<usize>,
    request_timeout: Option<f32>,
    health_check_interval: Option<f32>,
    graceful_timeout: Option<f32>,
    keepalive_timeout: Option<f32>,
    ejection_error_threshold: Option<usize>,
    ejection_timeout: Option<f32>,
    ejection_max_percent: Option<usize>,
    max_retries: Option<usize>,
    startup_timeout: Option<f32>,
    health_check_timeout: Option<f32>,
    worker_kill_timeout: Option<f32>,
    hook_http_timeout: Option<f32>,
) -> PyResult<()> {
    pyo3::Python::with_gil(|py| {
        py.allow_threads(|| {
            run_server(ServerOptions {
                config, port, host, model_repo, threads, timeout, log_level,
                log_info_output, log_error_output, log_rotation,
                metrics_port, no_metrics, grpc_port, no_grpc, no_streaming_metrics,
                max_queue_size, max_requests, max_requests_jitter, request_timeout,
                health_check_interval, graceful_timeout, keepalive_timeout,
                ejection_error_threshold, ejection_timeout, ejection_max_percent,
                max_retries, startup_timeout, health_check_timeout,
                worker_kill_timeout, hook_http_timeout,
            })
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    })
}

/// Validate a server config file with the same serde path used at startup,
/// so `config-check` rejects anything `serve --config` would reject.
#[cfg(feature = "python")]
#[pyfunction]
fn validate_server_config(path: &str) -> PyResult<()> {
    crate::config::load_config(path)
        .map(|_| ())
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

/// Validate a per-model config file (model repo `config.yaml`) with the same
/// serde path used when the server loads a model.
#[cfg(feature = "python")]
#[pyfunction]
fn validate_model_config(path: &str) -> PyResult<()> {
    crate::config::load_model_config(std::path::Path::new(path))
        .map(|_| ())
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

#[cfg(feature = "python")]
#[pymodule]
fn _lite_server(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(serve, m)?)?;
    m.add_function(wrap_pyfunction!(validate_server_config, m)?)?;
    m.add_function(wrap_pyfunction!(validate_model_config, m)?)?;
    m.add_function(wrap_pyfunction!(test_support::validate_identifier, m)?)?;
    m.add_class::<test_support::PyModelRegistry>()?;
    Ok(())
}
