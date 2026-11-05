// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Entry point for the micro-tunnel daemon.
//! All comments and docstrings must be in English.

use clap::Parser;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use micro_tunnel::config::ConfigLoader;
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

    /// Check and validate configuration syntax, schema, and semantics without starting the daemon.
    #[arg(long)]
    check: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let app_config = match ConfigLoader::from_file(&cli.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Configuration error in '{:?}': {}", cli.config, e);
            std::process::exit(1);
        }
    };

    if cli.check {
        println!(
            "Configuration check succeeded: '{:?}' is valid.",
            cli.config
        );
        return Ok(());
    }

    // Initialize structured logging respecting LogConfig
    let filter = match app_config.log.level.to_lowercase().as_str() {
        "trace" => tracing::Level::TRACE,
        "debug" => tracing::Level::DEBUG,
        "info" => tracing::Level::INFO,
        "warn" => tracing::Level::WARN,
        "error" => tracing::Level::ERROR,
        _ => tracing::Level::INFO,
    };
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter.as_str()));

    let fmt_builder = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_ansi(!app_config.log.no_color);

    if app_config.log.output.eq_ignore_ascii_case("stderr") {
        fmt_builder.with_writer(std::io::stderr).init();
    } else {
        fmt_builder.with_writer(std::io::stdout).init();
    }

    info!(config = ?cli.config, "Loaded micro-tunnel configuration");

    let threads = cli.worker_threads.or(app_config.worker_threads);

    let mut builder = match threads {
        Some(1) => {
            info!("Configuring single-thread Tokio current_thread runtime");
            tokio::runtime::Builder::new_current_thread()
        }
        Some(n) => {
            info!(
                threads = n,
                "Configuring multi-threaded Tokio runtime with custom worker threads"
            );
            let mut b = tokio::runtime::Builder::new_multi_thread();
            b.worker_threads(n);
            b
        }
        None => {
            info!("Configuring multi-threaded Tokio runtime auto-detecting CPU cores");
            tokio::runtime::Builder::new_multi_thread()
        }
    };

    builder.enable_all();

    let runtime = match builder.build() {
        Ok(rt) => rt,
        Err(e) => {
            error!("Failed to create Tokio runtime: {:?}", e);
            std::process::exit(1);
        }
    };

    runtime.block_on(run_daemon(app_config))
}

async fn run_daemon(
    app_config: micro_tunnel::config::model::AppConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let cancel_token = CancellationToken::new();

    // Listen for graceful shutdown signals (SIGINT / SIGTERM / Ctrl+C)
    let shutdown_token = cancel_token.clone();
    tokio::spawn(async move {
        micro_tunnel::platform::wait_for_shutdown_signal().await;
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
