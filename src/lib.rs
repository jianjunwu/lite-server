pub mod callback;
pub mod client_ip;
pub mod config;
pub mod deadline;
pub mod access_control;
pub mod admission;
pub mod audit;
pub mod rate_limit;
pub mod ensemble;
pub mod error;
pub mod grpc;
pub mod http;
pub mod inference_queue;
pub mod logging;
pub mod metrics;
pub mod proto;
pub mod protocol;
pub mod preflight;
pub mod python;
pub mod registry;
pub mod request_context;
pub mod sequence;
pub mod server;
pub mod streaming;
pub mod telemetry;
#[cfg(feature = "python")]
pub mod test_support;
#[cfg(test)]
mod test_tracing;
pub mod tls;
pub mod transport;
pub mod validation;
pub mod worker;

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use std::sync::{Mutex, OnceLock};
use tokio::sync::oneshot;
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
    pub ejection_max_timeout: Option<f32>,
    pub max_retries: Option<usize>,
    pub startup_timeout: Option<f32>,
    pub health_check_timeout: Option<f32>,
    pub health_check_kill_threshold: Option<usize>,
    pub worker_kill_timeout: Option<f32>,
    pub hook_http_timeout: Option<f32>,
}

pub fn run_server(
    opts: ServerOptions,
    shutdown_rx: Option<oneshot::Receiver<()>>,
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
        ejection_max_timeout,
        max_retries,
        startup_timeout,
        health_check_timeout,
        health_check_kill_threshold,
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
            // No CLI flag: tunable via config file / model_defaults only.
            recycle_max_percent: None,
            request_timeout,
            health_check_interval,
            ejection_error_threshold,
            ejection_timeout,
            ejection_max_percent,
            ejection_max_timeout,
            max_retries,
            startup_timeout,
            health_check_timeout,
            health_check_kill_threshold,
            worker_kill_timeout,
            hook_http_timeout,
        },
    });

    // Validate after overrides so a bad float tunable fails fast here instead
    // of panicking at Duration::from_secs_f* during model load.
    cfg.validate()?;

    // P-TRACE: build the OTel layer (if enabled + feature) BEFORE logging::init so
    // it is attached in the same subscriber. No runtime context needed — the 0.30
    // BatchSpanProcessor runs on its own dedicated thread (opentelemetry-rust #2715
    // decoupled). Returns None (zero overhead) when telemetry is off.
    // 对账修复：OTLP exporter 构造经 tonic connect_lazy 向当前 reactor spawn
    // 后台任务——先建 runtime 并在其上下文中 init(enter 借用,非 block_on)。
    let rt = build_runtime(cfg.server.threads);
    let otel_layer = {
        let _rt_guard = rt.enter();
        telemetry::init(&cfg.telemetry)
    };
    let _log_guard = logging::init(
        &cfg.logging.level,
        cfg.logging.info_output.as_deref(),
        cfg.logging.error_output.as_deref(),
        &cfg.logging.rotation,
        cfg.logging.max_size,
        cfg.logging.backup_count,
        cfg.logging.hostname_in_log_name,
        otel_layer,
    );
    info!("Starting lite-server v{}", env!("CARGO_PKG_VERSION"));
    info!("HTTP port: {}", cfg.server.http_port);
    if cfg.metrics.enabled {
        info!("Metrics port: {}", cfg.server.metrics_port);
    } else {
        info!("Metrics: disabled");
    }
    info!("Model repo: {}", cfg.model_repository.path);

    let server = server::LiteServer::new(cfg);

    rt.block_on(async {
        server.run(shutdown_rx).await.map_err(|e| {
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

// ---------------------------------------------------------------------------
// Process-global slot tracking the running serve() instance.
//
// Holds the Sender half of a oneshot channel that `stop_server()` signals to
// trigger graceful shutdown, and doubles as the re-entrancy guard: Some(tx)
// means a serve() is running, so a second serve() fails fast (before it can
// race on the bind and wedge the tokio runtime drop on the Python CLI path).
// ---------------------------------------------------------------------------
#[cfg(feature = "python")]
static SERVE_SLOT: OnceLock<Mutex<Option<oneshot::Sender<()>>>> = OnceLock::new();

#[cfg(feature = "python")]
fn serve_slot() -> &'static Mutex<Option<oneshot::Sender<()>>> {
    SERVE_SLOT.get_or_init(|| Mutex::new(None))
}

/// Clears the serve slot when serve() unwinds through any path (Ok, `?`-Err, or
/// a server-thread panic propagating through join). Recovers from a poisoned
/// mutex (mirrors ShutdownState at server/mod.rs:42) so a panic does not wedge
/// the slot permanently and a later serve() can still run.
#[cfg(feature = "python")]
struct ServeSlotGuard;

#[cfg(feature = "python")]
impl Drop for ServeSlotGuard {
    fn drop(&mut self) {
        if let Ok(mut s) = serve_slot().lock() {
            *s = None;
        }
    }
}

/// Run the server, blocking the calling thread until shutdown.
///
/// The server runs on a dedicated OS thread (`lite-server-main`) with the GIL
/// released via `py.allow_threads`, so other Python threads progress while the
/// embedding application is blocked here. The tokio runtime lives entirely on
/// that thread, never on the CPython main thread; the inference hot path never
/// touches the GIL (inference runs in separate Python worker processes over
/// ZMQ).
///
/// Shutdown:
///   * A signal (SIGINT/SIGTERM) drives graceful shutdown, after which `serve`
///     returns and a `KeyboardInterrupt` propagates — embedders should wrap the
///     call in `try/except KeyboardInterrupt` (the CLI already does).
///   * For programmatic stop from another thread, call `stop_server()`.
///
/// Re-entrant calls (a second `serve` while one is running) fail fast with
/// `RuntimeError("...already running...")`.
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
    ejection_max_timeout=None,
    max_retries=None,
    startup_timeout=None,
    health_check_timeout=None,
    health_check_kill_threshold=None,
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
    ejection_max_timeout: Option<f32>,
    max_retries: Option<usize>,
    startup_timeout: Option<f32>,
    health_check_timeout: Option<f32>,
    health_check_kill_threshold: Option<usize>,
    worker_kill_timeout: Option<f32>,
    hook_http_timeout: Option<f32>,
) -> PyResult<()> {
    // Re-entrancy guard + stop trigger. Claim the process-global slot before
    // anything that could wedge: a second serve() racing on the bind deadlocks
    // the tokio runtime drop on this Python CLI path. The slot holds the Sender
    // stop_server() signals; ServeSlotGuard clears it on every exit path.
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    {
        let mut slot = serve_slot().lock().unwrap_or_else(|e| e.into_inner());
        if slot.is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "lite-server is already running; call stop_server() or wait for the running instance to finish",
            ));
        }
        *slot = Some(shutdown_tx);
    }
    let _guard = ServeSlotGuard;

    // The tokio runtime runs on a dedicated OS thread; the calling (Python)
    // thread blocks here in join() with the GIL released.
    pyo3::Python::with_gil(|py| {
        py.allow_threads(|| {
            std::thread::Builder::new()
                .name("lite-server-main".into())
                .spawn(move || {
                    run_server(ServerOptions {
                        config, port, host, model_repo, threads, timeout, log_level,
                        log_info_output, log_error_output, log_rotation,
                        metrics_port, no_metrics, grpc_port, no_grpc, no_streaming_metrics,
                        max_queue_size, max_requests, max_requests_jitter, request_timeout,
                        health_check_interval, graceful_timeout, keepalive_timeout,
                        ejection_error_threshold, ejection_timeout, ejection_max_percent,
                        ejection_max_timeout,
                        max_retries, startup_timeout, health_check_timeout,
                        health_check_kill_threshold, worker_kill_timeout, hook_http_timeout,
                    }, Some(shutdown_rx))
                })
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
                .join()
                .map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err("server thread panicked")
                })?
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    })
}

/// Trigger graceful shutdown of the running `serve()` and return immediately
/// (non-blocking). Returns `True` if a running server was signaled, `False` if
/// no `serve()` is currently running (idempotent — does not raise).
///
/// Must be called from a DIFFERENT thread than the one blocked in `serve()`;
/// the caller is responsible for joining the `serve` thread. Typical embed:
/// `t = threading.Thread(target=serve, ...); t.start(); ...; stop_server();
/// t.join()`.
#[cfg(feature = "python")]
#[pyfunction]
fn stop_server() -> bool {
    match serve_slot().lock().unwrap_or_else(|e| e.into_inner()).take() {
        Some(tx) => {
            let _ = tx.send(());
            true
        }
        None => false,
    }
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
    m.add_function(wrap_pyfunction!(stop_server, m)?)?;
    m.add_function(wrap_pyfunction!(validate_server_config, m)?)?;
    m.add_function(wrap_pyfunction!(validate_model_config, m)?)?;
    m.add_function(wrap_pyfunction!(test_support::validate_identifier, m)?)?;
    m.add_class::<test_support::PyModelRegistry>()?;
    Ok(())
}
