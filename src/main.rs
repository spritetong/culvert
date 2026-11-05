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
use micro_tunnel::config::model::AppConfig;
use micro_tunnel::core::error::TunnelError;
use micro_tunnel::dialer::endpoint::EndpointContext;
use micro_tunnel::dialer::tls::TlsConfigBuilder;
use micro_tunnel::supervisor::TunnelSupervisor;
use micro_tunnel::transport::TransportRegistry;
use std::sync::Arc;

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

    /// Check and validate configuration syntax and semantics without starting the daemon.
    #[arg(long)]
    check: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let app_config = match ConfigLoader::from_file(&cli.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!(
                "Configuration error in '{:?}': {}",
                cli.config, e
            );
            std::process::exit(1);
        }
    };

    if cli.check {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("Failed to initialize Tokio runtime for check: {}", e);
                std::process::exit(1);
            }
        };

        let check_res = runtime.block_on(async { preflight_check(&app_config) });
        if let Err(e) = check_res {
            eprintln!(
                "Configuration pre-flight check failed for '{:?}': {}",
                cli.config, e
            );
            std::process::exit(1);
        }
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
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(filter.as_str()));

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

fn preflight_check(config: &AppConfig) -> Result<(), TunnelError> {
    let registry = Arc::new(TransportRegistry::default());

    // 1. Dry-run tunnels: build EndpointContext and connectors
    for tunnel in &config.tunnels {
        let ctx = EndpointContext::build(&tunnel.endpoint, config)?;
        let auth_token = tunnel.token.as_deref();
        for mapping in &tunnel.mappings {
            let _connector = registry.create_connector(&ctx, auth_token, mapping.mode)?;
        }
    }

    // 2. Dry-run server: parse specs and verify TlsAcceptor if TLS scheme is present
    if let Some(ref server) = config.server {
        let specs = server.parse_specs()?;
        let effective_tls = server.effective_tls(config.tls.as_ref());
        for spec in &specs {
            let _driver = registry
                .get_driver(&spec.scheme)
                .ok_or_else(|| TunnelError::UnsupportedScheme(spec.scheme.clone()))?;

            if matches!(spec.scheme.as_str(), "ws" | "wss" | "h3" | "quic" | "https") {
                use std::net::ToSocketAddrs;
                let addrs: Vec<_> = spec.bind_addr.to_socket_addrs().map_err(|e| {
                    TunnelError::Config(format!(
                        "Server listen address '{}' cannot be resolved or is invalid: {}",
                        spec.bind_addr, e
                    ))
                })?.collect();

                if addrs.is_empty() {
                    return Err(TunnelError::Config(format!(
                        "Server listen address '{}' resolved to no socket addresses",
                        spec.bind_addr
                    )));
                }

                // Pre-flight test bind to detect AddrNotAvailable and AddrInUse early
                for addr in &addrs {
                    if matches!(spec.scheme.as_str(), "h3" | "quic") {
                        let socket = std::net::UdpSocket::bind(addr).map_err(|e| {
                            TunnelError::Config(format!(
                                "Server listen address '{}' ({}) cannot be bound: {}",
                                spec.bind_addr, addr, e
                            ))
                        })?;
                        drop(socket);
                    } else {
                        let listener = std::net::TcpListener::bind(addr).map_err(|e| {
                            TunnelError::Config(format!(
                                "Server listen address '{}' ({}) cannot be bound: {}",
                                spec.bind_addr, addr, e
                            ))
                        })?;
                        drop(listener);
                    }
                }
            }

            if matches!(spec.scheme.as_str(), "wss" | "https" | "h3" | "quic") {
                let cert_str = effective_tls
                    .as_ref()
                    .and_then(|t| t.certificate.as_ref())
                    .and_then(|p| p.to_str());
                let key_str = effective_tls
                    .as_ref()
                    .and_then(|t| t.key.as_ref())
                    .and_then(|p| p.to_str());
                let mut alpn = effective_tls
                    .as_ref()
                    .and_then(|t| t.alpn.as_ref())
                    .map(|list| list.iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>())
                    .unwrap_or_default();

                if matches!(spec.scheme.as_str(), "h3" | "quic") {
                    if !alpn.iter().any(|p| p == b"h3") {
                        alpn.push(b"h3".to_vec());
                    }
                    if !alpn.iter().any(|p| p == b"micro-tunnel-quic") {
                        alpn.push(b"micro-tunnel-quic".to_vec());
                    }
                }

                let alpn_opt = if alpn.is_empty() { None } else { Some(alpn) };
                let (min_v, max_v) = match effective_tls {
                    Some(ref t) => (t.min_version.as_deref(), t.max_version.as_deref()),
                    None => (None, None),
                };
                let _server_rustls = TlsConfigBuilder::build_server_config(
                    cert_str, key_str, alpn_opt, min_v, max_v,
                )?;
            }
        }
    }

    Ok(())
}
