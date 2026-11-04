//! Entry point for the micro-tunnel daemon.
//! All comments and docstrings must be in English.

use clap::Parser;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use micro_tunnel::config::ConfigLoader;
use micro_tunnel::config::model::AppConfig;
use micro_tunnel::supervisor::TunnelSupervisor;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// High-performance lightweight HTTP/3 & WSS bidirectional TCP tunnel system.
#[derive(Parser, Debug)]
#[command(name = "micro-tunnel", version, about, long_about = None)]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,

    /// Override the number of Tokio worker threads.
    #[arg(short = 'w', long)]
    worker_threads: Option<usize>,

    /// DNS resolver to use (e.g. dns://1.1.1.1, dns+https://1.1.1.1?sni=cloudflare-dns.com, dns+tls://8.8.8.8?sni=dns.google, system://0.0.0.0). Can be specified multiple times.
    #[arg(long = "dns-resolver", action = clap::ArgAction::Append)]
    dns_resolver: Vec<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let mut app_config = match ConfigLoader::from_file(&cli.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!(
                "Failed to load configuration file '{:?}': {:?}",
                cli.config, e
            );
            std::process::exit(1);
        }
    };

    if !cli.dns_resolver.is_empty() {
        let dns = app_config.dns.get_or_insert_with(Default::default);
        dns.servers = Some(cli.dns_resolver);
    }

    // Initialize structured logging respecting LogConfig
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&app_config.log.level));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_ansi(!app_config.log.no_color)
        .init();

    info!(config = ?cli.config, "Loaded micro-tunnel configuration");

    let threads = cli.worker_threads.or(app_config.worker_threads);

    let mut builder = match threads {
        Some(1) => {
            tracing::info!("Initialized single-threaded (current_thread) Tokio runtime");
            tokio::runtime::Builder::new_current_thread()
        }
        Some(n) => {
            tracing::info!(
                "Initialized multi-threaded Tokio runtime with {} worker threads",
                n
            );
            let mut b = tokio::runtime::Builder::new_multi_thread();
            b.worker_threads(n);
            b
        }
        None => tokio::runtime::Builder::new_multi_thread(),
    };

    let runtime = builder.enable_all().build()?;
    runtime.block_on(async_main(app_config))
}

async fn async_main(app_config: AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let cancel_token = CancellationToken::new();
    let shutdown_token = cancel_token.clone();

    // Spawn signal listener for graceful shutdown (SIGINT / Ctrl+C and Unix SIGTERM)
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm = signal(SignalKind::terminate()).ok();
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Received termination signal (SIGINT/Ctrl+C). Initiating shutdown...");
                }
                _ = async {
                    if let Some(ref mut sig) = sigterm {
                        sig.recv().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    info!("Received termination signal (SIGTERM). Initiating shutdown...");
                }
            }
        }
        #[cfg(not(unix))]
        {
            if let Ok(()) = tokio::signal::ctrl_c().await {
                info!("Received termination signal (Ctrl+C). Initiating shutdown...");
            }
        }
        shutdown_token.cancel();
    });

    let supervisor = TunnelSupervisor::new(app_config, cancel_token);

    if let Err(e) = supervisor.run().await {
        error!("Tunnel supervisor terminated with error: {:?}", e);
        std::process::exit(1);
    }

    info!("Micro-tunnel daemon exited cleanly.");
    Ok(())
}
