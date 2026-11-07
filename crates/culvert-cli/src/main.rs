// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Command-line entry point for culvert.
//! All comments and docstrings must be in English.

use clap::Parser;
use mimalloc::MiMalloc;
use std::path::PathBuf;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use culvert::config::ConfigLoader;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// High-performance lightweight HTTP/3 & WSS bidirectional TCP tunnel system.
#[derive(Parser, Debug)]
#[command(name = "culvert", version, about, long_about = None)]
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

    // Initialize structured logging respecting LogConfig and POSIX service conventions.
    // Suppress noisy third-party libraries (Hickory DNS, Quinn, Rustls, Hyper, etc.).
    let level_str = match app_config.log.level.to_lowercase().as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => app_config.log.level.to_lowercase(),
        _ => "info".to_string(),
    };
    let default_filter = format!(
        "warn,culvert={lvl},culvert_cli={lvl},hickory_resolver=off,hickory_proto=off,trust_dns_resolver=off,trust_dns_proto=off,quinn=error,quinn_proto=error,rustls=error,h3=warn,hyper=warn,fastwebsockets=warn",
        lvl = level_str
    );
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    let fmt_builder = tracing_subscriber::fmt()
        .compact()
        .with_target(false)
        .with_env_filter(env_filter)
        .with_ansi(!app_config.log.no_color);

    if app_config.log.output.eq_ignore_ascii_case("stderr") {
        fmt_builder.with_writer(std::io::stderr).init();
    } else {
        fmt_builder.with_writer(std::io::stdout).init();
    }

    info!(config = ?cli.config, "Loaded configuration");

    let threads = cli.worker_threads.or(app_config.worker_threads);

    let mut builder = match threads {
        Some(1) => {
            info!("Configured single-thread runtime");
            tokio::runtime::Builder::new_current_thread()
        }
        Some(n) => {
            info!(threads = n, "Configured multi-thread runtime");
            let mut b = tokio::runtime::Builder::new_multi_thread();
            b.worker_threads(n);
            b
        }
        None => {
            info!("Configured multi-thread runtime (auto-detected cores)");
            tokio::runtime::Builder::new_multi_thread()
        }
    };

    builder.enable_all();

    let runtime = match builder.build() {
        Ok(rt) => rt,
        Err(e) => {
            error!("Failed to create Tokio runtime: {}", e);
            std::process::exit(1);
        }
    };

    if let Err(e) = runtime.block_on(culvert::run_daemon(app_config)) {
        error!("Culvert daemon terminated with error: {}", e);
        std::process::exit(1);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing() {
        let args = ["culvert", "-c", "custom.yaml", "-w", "4", "--check"];
        let cli = Cli::try_parse_from(args).expect("Failed to parse valid args");
        assert_eq!(cli.config, PathBuf::from("custom.yaml"));
        assert_eq!(cli.worker_threads, Some(4));
        assert!(cli.check);
    }

    #[test]
    fn test_cli_default_args() {
        let args = ["culvert"];
        let cli = Cli::try_parse_from(args).expect("Failed to parse default args");
        assert_eq!(cli.config, PathBuf::from("config.yaml"));
        assert_eq!(cli.worker_threads, None);
        assert!(!cli.check);
    }
}
