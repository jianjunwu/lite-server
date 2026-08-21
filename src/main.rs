use clap::{Parser, Subcommand};
use lite_server::config::{CliOverrides, Config};
use lite_server::server::LiteServer;
use tracing::{debug, error, info, warn};

#[derive(Parser)]
#[command(name = "lite-server")]
#[command(about = "High-performance inference server (Rust core + Python workers)")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)] // clap subcommand enums are intentionally flat arg structs
enum Commands {
    /// Start the inference server
    Serve {
        /// Path to YAML configuration file
        #[arg(short, long)]
        config: Option<String>,

        /// HTTP server port
        #[arg(long)]
        port: Option<u16>,

        /// Bind address
        #[arg(long)]
        host: Option<String>,

        /// Model repository path
        #[arg(long)]
        model_repo: Option<String>,

        /// Number of Tokio worker threads (null = auto = CPU cores)
        #[arg(long)]
        threads: Option<usize>,

        /// Request timeout in seconds
        #[arg(long)]
        timeout: Option<f32>,

        /// Log level
        #[arg(long)]
        log_level: Option<String>,

        /// Info log file path
        #[arg(long)]
        log_info_output: Option<String>,

        /// Error log file path
        #[arg(long)]
        log_error_output: Option<String>,

        /// Log rotation strategy: none, size, daily, hourly
        #[arg(long)]
        log_rotation: Option<String>,

        /// Metrics server port
        #[arg(long)]
        metrics_port: Option<u16>,

        /// Disable metrics
        #[arg(long)]
        no_metrics: bool,

        /// gRPC server port
        #[arg(long)]
        grpc_port: Option<u16>,

        /// Disable gRPC server
        #[arg(long)]
        no_grpc: bool,

        /// Disable streaming metrics collection
        #[arg(long)]
        no_streaming_metrics: bool,

        /// Max queue size per model (overrides model config)
        #[arg(long)]
        max_queue_size: Option<usize>,

        /// Auto-restart worker after N requests (0 = disabled, overrides model config)
        #[arg(long)]
        max_requests: Option<usize>,

        /// Jitter range for max_requests to prevent thundering herd (overrides model config)
        #[arg(long)]
        max_requests_jitter: Option<usize>,

        /// Per-request hard timeout in seconds (0 = disabled, overrides model config)
        #[arg(long)]
        request_timeout: Option<f32>,

        /// Active health check interval in seconds (0 = disabled, overrides model config)
        #[arg(long)]
        health_check_interval: Option<f32>,

        /// Consecutive errors before a worker is ejected (0 = disable, overrides model config)
        #[arg(long)]
        ejection_error_threshold: Option<usize>,

        /// Seconds a worker stays ejected before auto-recovery (overrides model config)
        #[arg(long)]
        ejection_timeout: Option<f32>,

        /// Max % of workers ejectable at once (1-100, overrides model config)
        #[arg(long)]
        ejection_max_percent: Option<usize>,

        /// Circuit-breaker backoff cap in seconds (overrides model config)
        #[arg(long)]
        ejection_max_timeout: Option<f32>,

        /// Retry a failed batch on a different worker up to N times (0 = disable, overrides model config)
        #[arg(long)]
        max_retries: Option<usize>,

        /// Max seconds to wait for a worker ready handshake (overrides model config)
        #[arg(long)]
        startup_timeout: Option<f32>,

        /// Seconds per health-check probe before timeout (overrides model config)
        #[arg(long)]
        health_check_timeout: Option<f32>,

        /// Consecutive health-probe failures before killing + respawning the worker (overrides model config)
        #[arg(long)]
        health_check_kill_threshold: Option<usize>,

        /// Graceful-stop budget: seconds a worker may take to finish teardown
        /// and exit after the stop message before SIGKILL; also the OS reap
        /// wait after the kill (overrides model config)
        #[arg(long)]
        worker_kill_timeout: Option<f32>,

        /// Seconds for a worker lifecycle HTTP hook request (overrides model config)
        #[arg(long)]
        hook_http_timeout: Option<f32>,

        /// Graceful shutdown timeout in seconds (max time to wait for in-flight requests)
        #[arg(long)]
        graceful_timeout: Option<f32>,

        /// HTTP keep-alive timeout in seconds. 0 = disable keep-alive
        #[arg(long)]
        keepalive_timeout: Option<f32>,
    },

    /// Validate configuration file
    ConfigCheck {
        /// Path to YAML configuration file
        config: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Serve {
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
            graceful_timeout,
            keepalive_timeout,
        } => {
            let mut cfg = if let Some(config_path) = config {
                match lite_server::config::load_config(&config_path) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("Failed to load config: {}", e);
                        std::process::exit(1);
                    }
                }
            } else {
                Config::default()
            };

            // CLI overrides
            cfg.apply_overrides(&CliOverrides {
                port,
                host,
                model_repo,
                threads,
                timeout,
                log_level: log_level.clone(),
                log_info_output,
                log_error_output,
                log_rotation,
                grpc_port,
                metrics_port,
                no_grpc,
                no_metrics,
                no_streaming_metrics,
                graceful_timeout,
                keepalive_timeout,
                tunables: lite_server::config::ModelTunables {
                    max_queue_size,
                    max_requests,
                    max_requests_jitter,
                    // No CLI flag: tunable via config file / model_defaults only.
                    recycle_max_percent: None,
                    count_streams_toward_max_requests: None,
                    recycle_stream_drain_timeout_secs: None,
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

            // Validate after CLI overrides so a negative --<tunable> fails fast
            // with a clear message instead of panicking at model load.
            if let Err(e) = cfg.validate() {
                eprintln!("Invalid configuration: {}", e);
                std::process::exit(1);
            }

            // Initialize logging (level from config, overridable via --log-level CLI).
            // P-TRACE: build the OTel layer first (no-op unless telemetry.enabled +
            // feature) so it rides the same subscriber; the 0.30 BatchSpanProcessor
            // uses its own dedicated thread (no runtime context required).
            // 对账修复：OTLP exporter 构造会经 tonic connect_lazy 向当前 reactor
            // spawn 后台任务——init 必须在 runtime 上下文中执行（enter 借用，
            // 不 block_on；spawn 的任务随后面 block_on 泵动）。
            let rt = build_runtime(cfg.server.threads);
            let otel_layer = {
                let _rt_guard = rt.enter();
                lite_server::telemetry::init(&cfg.telemetry)
            };
            let _log_guard = lite_server::logging::init(
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
            // B7（蓝图 §6.2，D30 配套）：旧配置形态迁移告警，点名 migration.md 条目。
            for w in lite_server::preflight::startup_preflight(&cfg) {
                warn!("config preflight: {w}");
            }
            debug!("Configuration loaded, log level: {}", cfg.logging.level);
            debug!("Server config: {:?}", cfg.server);
            info!("HTTP port: {}", cfg.server.http_port);
            if cfg.metrics.enabled {
                info!("Metrics port: {}", cfg.server.metrics_port);
            } else {
                info!("Metrics: disabled");
            }
            info!("Model repo: {}", cfg.model_repository.path);

            // Build tokio runtime with configured thread count（已在上方
            // telemetry init 前构建——OTLP exporter 构造需要 reactor 上下文）。
            let server = LiteServer::new(cfg);
            if let Err(e) = rt.block_on(server.run(None)) {
                error!("Server error: {}", e);
                std::process::exit(1);
            }
        }

        Commands::ConfigCheck { config } => {
            match lite_server::config::load_config(&config) {
                Ok(cfg) => {
                    println!("Configuration OK: {}", config);
                    println!("  HTTP port: {}", cfg.server.http_port);
                    println!("  gRPC port: {}", cfg.server.grpc_port);
                    println!("  Metrics port: {}", cfg.server.metrics_port);
                    println!("  Model repo: {}", cfg.model_repository.path);
                    // B7: 配置体检顺带输出迁移预检告警（点名 migration.md 条目）。
                    for w in lite_server::preflight::startup_preflight(&cfg) {
                        println!("  preflight warning: {w}");
                    }
                }
                Err(e) => {
                    eprintln!("Configuration error: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }
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
