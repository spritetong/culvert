//! Multi-tunnel supervisor and lifecycle manager.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::common::task::TaskTracker;
use crate::config::model::{AppConfig, TunnelConfig};
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::server::ServerRunner;
use crate::session::SessionManager;
use crate::transport::TransportRegistry;
use crate::transport::h3::H3TransportDriver;
use crate::transport::uds::UdsTransportDriver;
use crate::transport::ws::WsTransportDriver;
use crate::tunnel::run_tunnel_cluster;

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
        let mut registry = TransportRegistry::new();
        registry.register(WsTransportDriver);
        registry.register(H3TransportDriver);
        registry.register(UdsTransportDriver);

        Self {
            config,
            cancel_token,
            transport_registry: Arc::new(registry),
            session_manager: Arc::new(SessionManager::new()),
        }
    }

    /// Spawns and manages all configured tunnels until cancelled or interrupted.
    pub async fn run(&self) -> Result<(), TunnelError> {
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
            let endpoint_ctx = EndpointContext::build(&cfg.endpoint, &self.config)?;

            tracker.spawn_with_cancel(move |token| async move {
                Self::run_tunnel_with_retry(cfg, endpoint_ctx, registry, token).await
            });
        }

        info!(
            count = tracker.len(),
            "All configured tunnels successfully spawned"
        );

        // Wait for cancellation signal
        tokio::select! {
            biased;
            _ = self.cancel_token.cancelled() => {
                info!("Supervisor received shutdown signal.");
            }
            res = tracker.join_next() => {
                if let Some(Ok(Err(e))) = res {
                    error!("A tunnel worker terminated with an error: {:?}", e);
                }
            }
        }

        // Graceful termination of all remaining tunnels (never aborts)
        tracker.shutdown_and_wait().await;
        Ok(())
    }

    /// Supervised retry loop for a tunnel instance.
    async fn run_tunnel_with_retry(
        config: TunnelConfig,
        endpoint_ctx: EndpointContext,
        registry: Arc<TransportRegistry>,
        token: CancellationToken,
    ) -> Result<(), TunnelError> {
        let retry_cfg = config.retry.clone().unwrap_or_default();
        let retry_delay = retry_cfg.delay.unwrap_or_else(|| Duration::from_secs(3));
        let max_retries = retry_cfg.max_retries;

        let mut attempts = 0usize;
        loop {
            if token.is_cancelled() {
                break;
            }

            match run_tunnel_cluster(
                config.clone(),
                endpoint_ctx.clone(),
                Arc::clone(&registry),
                token.clone(),
            )
            .await
            {
                Ok(()) => {
                    info!(tag = %config.tag, "Tunnel exited cleanly");
                    break;
                }
                Err(e) => {
                    attempts += 1;
                    if let Some(max) = max_retries
                        && attempts >= max
                    {
                        error!(
                            tag = %config.tag,
                            attempts,
                            max,
                            "Tunnel exceeded maximum retry attempts, giving up: {:?}",
                            e
                        );
                        return Err(e);
                    }

                    warn!(
                        tag = %config.tag,
                        attempts,
                        retry_in = ?retry_delay,
                        "Tunnel execution error: {:?}. Retrying...",
                        e
                    );

                    tokio::select! {
                        biased;
                        _ = token.cancelled() => break,
                        _ = tokio::time::sleep(retry_delay) => {}
                    }
                }
            }
        }

        Ok(())
    }
}
