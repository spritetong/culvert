// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Multi-tunnel supervisor and lifecycle manager.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::common::{CancellationTokenExt, TaskTracker};
use crate::config::model::{AppConfig, TunnelConfig};
use crate::consts::DEFAULT_RETRY_DELAY;
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::server::ServerRunner;
use crate::session::SessionManager;
use crate::transport::TransportRegistry;
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
            let endpoint_ctx = EndpointContext::build(&cfg.endpoint, &self.config)?;

            tracker.spawn_with_cancel(move |token| async move {
                Self::run_tunnel_with_retry(cfg, endpoint_ctx, registry, token).await
            });
        }

        info!(
            count = tracker.len(),
            "All configured tunnels successfully spawned"
        );

        // Wait for cancellation signal or worker termination
        let mut exit_err: Option<TunnelError> = None;
        loop {
            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    info!("Supervisor received shutdown signal.");
                    break;
                }
                res = tracker.join_next() => {
                    match res {
                        Some(Ok(Err(e))) => {
                            error!("A tunnel worker terminated with an error: {:?}", e);
                            if exit_err.is_none() {
                                exit_err = Some(e);
                            }
                            self.cancel_token.cancel();
                            break;
                        }
                        Some(Err(join_err)) => {
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
        if let Some(e) = exit_err {
            return Err(e);
        }
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
        let retry_delay = retry_cfg.delay.unwrap_or(DEFAULT_RETRY_DELAY);
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
                    if e.is_permanent() {
                        error!(
                            tag = %config.tag,
                            "Permanent tunnel execution error encountered, aborting without retry: {:?}",
                            e
                        );
                        return Err(e);
                    }

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

                    if !token.sleep_or_cancel(retry_delay).await {
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}
