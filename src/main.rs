use clap::{Parser, Subcommand};
use lite_server::config::{CliOverrides, Config};
use lite_server::server::LiteServer;
use tracing::{debug, error, info};

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
                max_queue_size,
                max_requests,
                max_requests_jitter,
                request_timeout,
                health_check_interval,
                graceful_timeout,
                keepalive_timeout,
                ..Default::default()
            });

            // Initialize logging (level from config, overridable via --log-level CLI)
            let _log_guard = lite_server::logging::init(
                &cfg.logging.level,
                cfg.logging.info_output.as_deref(),
                cfg.logging.error_output.as_deref(),
                &cfg.logging.rotation,
                cfg.logging.max_size,
                cfg.logging.backup_count,
                cfg.logging.hostname_in_log_name,
            );

            info!("Starting lite-server v{}", env!("CARGO_PKG_VERSION"));
            debug!("Configuration loaded, log level: {}", cfg.logging.level);
            debug!("Server config: {:?}", cfg.server);
            info!("HTTP port: {}", cfg.server.http_port);
            info!("Metrics port: {}", cfg.server.metrics_port);
            info!("Model repo: {}", cfg.model_repository.path);

            // Build tokio runtime with configured thread count
            let rt = build_runtime(cfg.server.threads);
            let server = LiteServer::new(cfg);
            if let Err(e) = rt.block_on(server.run()) {
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
