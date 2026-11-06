// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Client tunnel runner and lifecycle supervisor.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::common::{CancellationTokenExt, TaskTracker};
use crate::config::model::{AppConfig, TunnelConfig};
use crate::consts::DEFAULT_RETRY_DELAY;
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::transport::TransportRegistry;
use crate::tunnel::run_tunnel_cluster;

/// Client runner that manages one or more client tunnels with retry supervision.
pub struct ClientRunner {
    tunnels: Vec<(TunnelConfig, EndpointContext)>,
    transport_registry: Arc<TransportRegistry>,
}

impl ClientRunner {
    /// Creates a new `ClientRunner` for a single tunnel configuration with default settings.
    pub fn new(tunnel: TunnelConfig) -> Result<Self, TunnelError> {
        Self::from_tunnels(vec![tunnel])
    }

    /// Creates a new `ClientRunner` for multiple tunnel configurations with default settings.
    pub fn from_tunnels(tunnels: Vec<TunnelConfig>) -> Result<Self, TunnelError> {
        let app_config = AppConfig {
            tunnels,
            ..Default::default()
        };
        Self::from_app_config(&app_config)
    }

    /// Creates a new `ClientRunner` using the tunnels and global settings from an `AppConfig`.
    pub fn from_app_config(app_config: &AppConfig) -> Result<Self, TunnelError> {
        Self::with_registry(app_config, Arc::new(TransportRegistry::default()))
    }

    /// Creates a new `ClientRunner` with a custom transport registry.
    pub fn with_registry(
        app_config: &AppConfig,
        transport_registry: Arc<TransportRegistry>,
    ) -> Result<Self, TunnelError> {
        if app_config.tunnels.is_empty() {
            return Err(TunnelError::Config(
                "No client tunnels configured.".to_string(),
            ));
        }

        let mut tunnels = Vec::with_capacity(app_config.tunnels.len());
        for tunnel_cfg in &app_config.tunnels {
            let endpoint_ctx = EndpointContext::build(&tunnel_cfg.endpoint, app_config)?;
            tunnels.push((tunnel_cfg.clone(), endpoint_ctx));
        }

        Ok(Self {
            tunnels,
            transport_registry,
        })
    }

    /// Runs all configured client tunnels with retry supervision until cancelled.
    pub async fn run(&self, cancel_token: CancellationToken) -> Result<(), TunnelError> {
        let mut tracker: TaskTracker<Result<(), TunnelError>> =
            TaskTracker::new(cancel_token.clone());

        for (cfg, endpoint_ctx) in &self.tunnels {
            let config = cfg.clone();
            let ctx = endpoint_ctx.clone();
            let registry = Arc::clone(&self.transport_registry);

            tracker.spawn_with_cancel(move |token| async move {
                Self::run_tunnel_with_retry(config, ctx, registry, token).await
            });
        }

        info!(
            count = tracker.len(),
            "All configured client tunnels successfully spawned"
        );

        let mut exit_err: Option<TunnelError> = None;
        loop {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    info!("Client runner received shutdown signal.");
                    break;
                }
                res = tracker.join_next() => {
                    match res {
                        Some(Ok(Err(e))) => {
                            error!("A client tunnel worker terminated with an error: {:?}", e);
                            if exit_err.is_none() {
                                exit_err = Some(e);
                            }
                            cancel_token.cancel();
                            break;
                        }
                        Some(Err(join_err)) => {
                            error!("A client tunnel worker panicked: {:?}", join_err);
                            if exit_err.is_none() {
                                exit_err = Some(TunnelError::Internal(format!("Worker panic: {:?}", join_err)));
                            }
                            cancel_token.cancel();
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

        tracker.shutdown_and_wait().await;
        if let Some(e) = exit_err {
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
