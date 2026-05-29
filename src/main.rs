use clap::{Parser, Subcommand};
use lite_server::config::Config;
use lite_server::server::LiteServer;
use tracing::{error, info};

#[derive(Parser)]
#[command(name = "lite-server")]
#[command(about = "High-performance inference server (Rust core + Python workers)")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
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

        /// Number of HTTP worker threads (tokio)
        #[arg(long)]
        http_workers: Option<usize>,

        /// Request timeout in seconds
        #[arg(long)]
        timeout: Option<f32>,

        /// Log level
        #[arg(long)]
        log_level: Option<String>,

        /// Metrics server port
        #[arg(long)]
        metrics_port: Option<u16>,

        /// Disable metrics
        #[arg(long)]
        no_metrics: bool,
    },

    /// Validate configuration file
    ConfigCheck {
        /// Path to YAML configuration file
        config: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Serve {
            config,
            port,
            host,
            model_repo,
            http_workers,
            timeout,
            log_level,
            metrics_port,
            no_metrics,
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
            if no_metrics {
                cfg.metrics.enabled = false;
            }

            // Initialize logging
            let _log_guard = lite_server::logging::init(
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

            let server = LiteServer::new(cfg);
            if let Err(e) = server.run().await {
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
