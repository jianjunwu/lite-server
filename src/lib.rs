pub mod config;
pub mod ensemble;
pub mod error;
pub mod http;
pub mod inference_queue;
pub mod logging;
pub mod metrics;
pub mod proto;
pub mod registry;
pub mod server;
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut cfg = if let Some(config_path) = config {
        config::load_config(&config_path)?
    } else {
        config::Config::default()
    };

    if let Some(p) = port {
        cfg.server.http_port = p;
    }
    if let Some(h) = host {
        cfg.server.host = h;
    }
    if let Some(r) = model_repo {
        cfg.model_repository.path = r;
    }
    if let Some(w) = http_workers {
        cfg.server.http_workers = Some(w);
    }
    if let Some(t) = timeout {
        cfg.server.timeout = t;
    }
    if let Some(l) = log_level {
        cfg.server.log_level = l.clone();
        cfg.logging.level = l;
    }
    if let Some(mp) = metrics_port {
        cfg.server.metrics_port = mp;
    }
    if no_metrics == Some(true) {
        cfg.metrics.enabled = false;
    }

    let _log_guard = logging::init(
        &cfg.logging.level,
        cfg.logging.info_output.as_deref(),
        cfg.logging.error_output.as_deref(),
        &cfg.logging.rotation,
        cfg.logging.max_size,
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
) -> PyResult<()> {
    pyo3::Python::with_gil(|py| {
        py.allow_threads(|| {
            run_server(
                config, port, host, model_repo, http_workers, timeout, log_level, metrics_port,
                no_metrics,
            )
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    })
}

#[cfg(feature = "python")]
#[pymodule]
fn _lite_server(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(serve, m)?)?;
    Ok(())
}
