//! Entry point for the micro-tunnel daemon.
//! All comments and docstrings must be in English.

use clap::Parser;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use micro_tunnel::config::ConfigLoader;
use micro_tunnel::supervisor::TunnelSupervisor;

/// High-performance lightweight HTTP/3 & WSS bidirectional TCP tunnel system.
#[derive(Parser, Debug)]
#[command(name = "micro-tunnel", version, about, long_about = None)]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    info!(config = ?cli.config, "Loading micro-tunnel configuration");
    let app_config = match ConfigLoader::from_file(&cli.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration file: {:?}", e);
            std::process::exit(1);
        }
    };

    let cancel_token = CancellationToken::new();
    let shutdown_token = cancel_token.clone();

    // Spawn signal listener for graceful shutdown (SIGINT / Ctrl+C)
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            info!("Received termination signal (Ctrl+C). Initiating shutdown...");
            shutdown_token.cancel();
        }
    });

    let supervisor = TunnelSupervisor::new(app_config, cancel_token);

    if let Err(e) = supervisor.run().await {
        error!("Tunnel supervisor terminated with error: {:?}", e);
        std::process::exit(1);
    }

    info!("Micro-tunnel daemon exited cleanly.");
    Ok(())
}
