// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Multi-tunnel supervisor and lifecycle manager.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::common::TaskTracker;
use crate::config::model::{AppConfig, TunnelConfig};
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::server::ServerRunner;
use crate::session::SessionManager;
use crate::transport::TransportRegistry;

/// Multi-tunnel supervisor that concurrently runs multiple tunnels and ensures orderly shutdown.
pub struct TunnelSupervisor {
    config: AppConfig,
    cancel_token: CancellationToken,
    transport_registry: Arc<TransportRegistry>,
    session_manager: Arc<SessionManager>,
}

impl TunnelSupervisor {
    /// Creates a new `TunnelSupervisor` instance.
    pub fn new(config: AppConfig, cancel_token: CancellationToken) -> Self {
        Self {
            config,
            cancel_token,
            transport_registry: Arc::new(TransportRegistry::default()),
            session_manager: Arc::new(SessionManager::new()),
        }
    }

    /// Spawns and manages all configured tunnels until cancelled or interrupted.
    pub async fn run(&self) -> Result<(), TunnelError> {
        let has_server = self
            .config
            .server
            .as_ref()
            .is_some_and(|s| !s.listen.is_empty());
        let has_tunnels = !self.config.tunnels.is_empty();
        if !has_server && !has_tunnels {
            return Err(TunnelError::Config(
                "No server listeners and no tunnels are configured. Configuration must not be empty."
                    .to_string(),
            ));
        }

        let mut tracker: TaskTracker<Result<(), TunnelError>> =
            TaskTracker::new(self.cancel_token.clone());

        // Spawn server runner if ingress server is configured
        if let Some(ref server_cfg) = self.config.server {
            let server_runner = ServerRunner::with_global_tls(
                server_cfg.clone(),
                self.config.tls.clone(),
                Arc::clone(&self.transport_registry),
                Arc::clone(&self.session_manager),
            );
            tracker.spawn_with_cancel(move |token| async move { server_runner.run(token).await });
        }

        // Spawn client tunnel workers with retry supervision
        for tunnel_cfg in &self.config.tunnels {
            let cfg = tunnel_cfg.clone();
            let registry = Arc::clone(&self.transport_registry);
            let endpoint_ctx = EndpointContext::build_for_tunnel(&cfg, &self.config)?;

            tracker.spawn_with_cancel(move |token| async move {
                Self::run_tunnel_with_retry(cfg, endpoint_ctx, registry, token).await
            });
        }

        info!(count = tracker.len(), "Spawned tunnel workers");

        // Wait for cancellation signal or worker termination
        let mut exit_err: Option<TunnelError> = None;
        loop {
            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    debug!("Supervisor stopping on shutdown signal");
                    break;
                }
                res = tracker.join_next() => {
                    match res {
                        Some(Ok(Err(TunnelError::Shutdown))) => {
                            if tracker.is_empty() {
                                break;
                            }
                        }
                        Some(Ok(Err(e))) => {
                            if self.cancel_token.is_cancelled() {
                                if tracker.is_empty() {
                                    break;
                                }
                                continue;
                            }
                            error!("A tunnel worker terminated with an error: {:?}", e);
                            if exit_err.is_none() {
                                exit_err = Some(e);
                            }
                            self.cancel_token.cancel();
                            break;
                        }
                        Some(Err(join_err)) => {
                            if self.cancel_token.is_cancelled() {
                                if tracker.is_empty() {
                                    break;
                                }
                                continue;
                            }
                            error!("A tunnel worker panicked: {:?}", join_err);
                            if exit_err.is_none() {
                                exit_err = Some(TunnelError::Internal(format!("Worker panic: {:?}", join_err)));
                            }
                            self.cancel_token.cancel();
                            break;
                        }
                        Some(Ok(Ok(()))) => {
                            if tracker.is_empty() {
                                break;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
            }
        }

        // Graceful termination of all remaining tunnels (never aborts)
        tracker.shutdown_and_wait().await;
        if let Some(e) = exit_err
            && !matches!(e, TunnelError::Shutdown)
        {
            return Err(e);
        }
        Ok(())
    }

    /// Supervised retry loop for a tunnel instance.
    pub async fn run_tunnel_with_retry(
        config: TunnelConfig,
        endpoint_ctx: EndpointContext,
        registry: Arc<TransportRegistry>,
        token: CancellationToken,
    ) -> Result<(), TunnelError> {
        crate::client::ClientRunner::run_tunnel_with_retry(config, endpoint_ctx, registry, token)
            .await
    }
}
