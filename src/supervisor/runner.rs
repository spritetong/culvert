//! Multi-tunnel supervisor and lifecycle manager.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::common::task::TaskTracker;
use crate::config::model::{AppConfig, TunnelConfig, TunnelMode};
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::transport::h3::H3TransportDriver;
use crate::transport::uds::UdsTransportDriver;
use crate::transport::ws::WsTransportDriver;
use crate::transport::TransportRegistry;
use crate::tunnel::{run_forward_tunnel, run_reverse_tunnel};

/// Multi-tunnel supervisor that concurrently runs multiple tunnels and ensures orderly shutdown.
pub struct TunnelSupervisor {
    config: AppConfig,
    cancel_token: CancellationToken,
    transport_registry: Arc<TransportRegistry>,
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
        }
    }

    /// Spawns and manages all configured tunnels until cancelled or interrupted.
    pub async fn run(&self) -> Result<(), TunnelError> {
        let mut tracker: TaskTracker<Result<(), TunnelError>> = TaskTracker::new(self.cancel_token.clone());

        for tunnel_cfg in &self.config.tunnels {
            let cfg = tunnel_cfg.clone();
            let registry = Arc::clone(&self.transport_registry);
            let endpoint_ctx = EndpointContext::build(&cfg.endpoint, &self.config)?;

            tracker.spawn_with_cancel(move |token| async move {
                Self::run_single_tunnel(cfg, endpoint_ctx, registry, token).await
            });
        }

        info!(count = tracker.len(), "All configured tunnels successfully spawned");

        // Wait for cancellation signal
        tokio::select! {
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

    async fn run_single_tunnel(
        config: TunnelConfig,
        endpoint_ctx: EndpointContext,
        registry: Arc<TransportRegistry>,
        token: CancellationToken,
    ) -> Result<(), TunnelError> {
        match config.mode {
            TunnelMode::Forward => run_forward_tunnel(config, endpoint_ctx, registry, token).await,
            TunnelMode::Reverse => run_reverse_tunnel(config, endpoint_ctx, registry, token).await,
        }
    }
}
