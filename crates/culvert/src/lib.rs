// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! culvert core library.
//! All comments and docstrings must be in English.

pub mod client;
pub mod common;
pub mod config;
pub mod consts;
pub mod control;
pub mod core;
pub mod daemon;
pub mod dialer;
pub mod mux;
pub mod platform;
pub mod pool;
pub mod server;
pub mod session;
pub mod supervisor;
pub mod transport;
pub mod tunnel;

pub use client::ClientRunner;
pub use daemon::run_daemon;
pub use server::ServerRunner;
pub use supervisor::TunnelSupervisor;

/// Runs an ingress server until cancelled.
pub async fn run_server(
    config: config::model::ServerConfig,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Result<(), core::error::TunnelError> {
    ServerRunner::from_config(config).run(cancel_token).await
}

/// Runs client tunnels until cancelled.
pub async fn run_client(
    tunnel: config::model::TunnelConfig,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Result<(), core::error::TunnelError> {
    ClientRunner::new(tunnel)?.run(cancel_token).await
}

/// Runs the full supervisor (server and/or client tunnels) until cancelled.
pub async fn run(
    config: config::model::AppConfig,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Result<(), core::error::TunnelError> {
    TunnelSupervisor::new(config, cancel_token).run().await
}
