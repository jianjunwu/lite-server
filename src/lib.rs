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

pub fn run_server(
    config: Option<String>,
    port: Option<u16>,
    host: Option<String>,
    model_repo: Option<String>,
    endpoints_dir: Option<String>,
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
        endpoints_dir,
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
        max_queue_size,
        max_requests,
        max_requests_jitter,
        request_timeout,
        health_check_interval,
        graceful_timeout,
        keepalive_timeout,
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
    endpoints_dir=None,
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
))]
fn serve(
    config: Option<String>,
    port: Option<u16>,
    host: Option<String>,
    model_repo: Option<String>,
    endpoints_dir: Option<String>,
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
) -> PyResult<()> {
    pyo3::Python::with_gil(|py| {
        py.allow_threads(|| {
            run_server(
                config, port, host, model_repo, endpoints_dir, threads, timeout, log_level,
                log_info_output, log_error_output, log_rotation,
                metrics_port, no_metrics, grpc_port, no_grpc, no_streaming_metrics,
                max_queue_size, max_requests, max_requests_jitter, request_timeout,
                health_check_interval, graceful_timeout, keepalive_timeout,
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
    Ok(())
}
