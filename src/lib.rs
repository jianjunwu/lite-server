pub mod config;
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

pub fn run_server(
    config: Option<String>,
    port: Option<u16>,
    host: Option<String>,
    model_repo: Option<String>,
    http_workers: Option<usize>,
    timeout: Option<f32>,
    log_level: Option<String>,
    metrics_port: Option<u16>,
    no_metrics: Option<bool>,
    transport: Option<String>,
    grpc_port: Option<u16>,
    no_grpc: Option<bool>,
    no_streaming_metrics: Option<bool>,
    log_verbose: Option<bool>,
    max_queue_size: Option<usize>,
    max_requests: Option<usize>,
    request_timeout: Option<f32>,
    health_check_interval: Option<f32>,
    graceful_timeout: Option<f32>,
    keepalive_timeout: Option<f32>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut cfg = if let Some(config_path) = config {
        config::load_config(&config_path)?
    } else {
        config::Config::default()
    };

    cfg.apply_overrides(&config::CliOverrides {
        port,
        host,
        model_repo,
        http_workers,
        timeout,
        transport,
        log_level,
        grpc_port,
        metrics_port,
        no_grpc: no_grpc == Some(true),
        no_metrics: no_metrics == Some(true),
        no_streaming_metrics: no_streaming_metrics == Some(true),
        log_verbose: false, // handled below by logging::init
        max_queue_size,
        max_requests,
        request_timeout,
        health_check_interval,
        graceful_timeout,
        keepalive_timeout,
    });

    let log_verbose = log_verbose.unwrap_or(false);
    let _log_guard = logging::init(
        &cfg.logging.level,
        cfg.logging.info_output.as_deref(),
        cfg.logging.error_output.as_deref(),
        &cfg.logging.rotation,
        cfg.logging.max_size,
        log_verbose,
    );
    info!("Starting lite-server v{}", env!("CARGO_PKG_VERSION"));
    info!("HTTP port: {}", cfg.server.http_port);
    info!("Metrics port: {}", cfg.server.metrics_port);
    info!("Model repo: {}", cfg.model_repository.path);

    let server = server::LiteServer::new(cfg);

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        server.run().await.map_err(|e| {
            Box::<dyn std::error::Error + Send + Sync>::from(format!("Server error: {}", e))
        })
    })
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (
    config=None,
    port=None,
    host=None,
    model_repo=None,
    http_workers=None,
    timeout=None,
    log_level=None,
    metrics_port=None,
    no_metrics=None,
    transport=None,
    grpc_port=None,
    no_grpc=None,
    no_streaming_metrics=None,
    log_verbose=None,
    max_queue_size=None,
    max_requests=None,
    request_timeout=None,
    graceful_timeout=None,
    keepalive_timeout=None,
))]
fn serve(
    config: Option<String>,
    port: Option<u16>,
    host: Option<String>,
    model_repo: Option<String>,
    http_workers: Option<usize>,
    timeout: Option<f32>,
    log_level: Option<String>,
    metrics_port: Option<u16>,
    no_metrics: Option<bool>,
    transport: Option<String>,
    grpc_port: Option<u16>,
    no_grpc: Option<bool>,
    no_streaming_metrics: Option<bool>,
    log_verbose: Option<bool>,
    max_queue_size: Option<usize>,
    max_requests: Option<usize>,
    request_timeout: Option<f32>,
    graceful_timeout: Option<f32>,
    keepalive_timeout: Option<f32>,
) -> PyResult<()> {
    pyo3::Python::with_gil(|py| {
        py.allow_threads(|| {
            run_server(
                config, port, host, model_repo, http_workers, timeout, log_level, metrics_port,
                no_metrics, transport, grpc_port, no_grpc, no_streaming_metrics, log_verbose,
                max_queue_size, max_requests, request_timeout, None, graceful_timeout,
                keepalive_timeout,
            )
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    })
}

#[cfg(feature = "python")]
#[pymodule]
fn _lite_server(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(serve, m)?)?;
    m.add_function(wrap_pyfunction!(test_support::validate_identifier, m)?)?;
    m.add_class::<test_support::PyModelRegistry>()?;
    m.add_class::<test_support::PyStreamingEngine>()?;
    Ok(())
}
