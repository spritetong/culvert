// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Supervised Forward listener runner coordinating `Frontend`, `TransportPool`, and `Coordinator`.
//! All comments and docstrings must be in English.

use crate::core::error::TunnelError;
use crate::frontend::{Frontend, FrontendOutput};
use crate::io::addr::{L4Protocol, TargetAddr};
use crate::v2::coordinator::{Coordinator, CoordinatorSummary};
use crate::v2::forward::workflow::{ForwardInput, ForwardWorkflow};
use crate::v2::pool::{PoolHandle, PoolSummary};
use reusable_future::ReusableFutureRef;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Runs a single forward mapping using the v2 `Coordinator` + `IoTask` + `TransportPool` stack.
pub struct ForwardRunner {
    frontend: Box<dyn Frontend>,
    target: TargetAddr,
    protocol: L4Protocol,
    workflow: ForwardWorkflow,
    pool: Option<(PoolHandle, JoinHandle<PoolSummary>)>,
    prewarm_tasks: usize,
    drain_timeout: Duration,
}

impl ForwardRunner {
    /// Creates a new `ForwardRunner`.
    pub fn new(
        frontend: Box<dyn Frontend>,
        target: TargetAddr,
        protocol: L4Protocol,
        workflow: ForwardWorkflow,
        pool: Option<(PoolHandle, JoinHandle<PoolSummary>)>,
        drain_timeout: Duration,
    ) -> Self {
        Self {
            frontend,
            target,
            protocol,
            workflow,
            pool,
            prewarm_tasks: 0,
            drain_timeout,
        }
    }

    /// Pre-spawns `count` idle `Empty` tasks inside the `Coordinator` before accepting connections.
    pub fn with_prewarm_tasks(mut self, count: usize) -> Self {
        self.prewarm_tasks = count;
        self
    }

    /// Runs the accept loop until `shutdown_token` is cancelled, then drains deterministically.
    pub async fn run(
        mut self,
        shutdown_token: CancellationToken,
    ) -> Result<CoordinatorSummary, TunnelError> {
        let coord_force_token = CancellationToken::new();
        let (coord, coord_join) =
            Coordinator::start(self.workflow, self.drain_timeout, coord_force_token);

        if self.prewarm_tasks > 0 {
            let _ = coord.prewarm(self.prewarm_tasks).await;
        }

        let mut accept_slot: ReusableFutureRef<'static, FrontendOutput> =
            ReusableFutureRef::default();

        loop {
            if accept_slot.is_empty() {
                self.frontend.accept(&mut accept_slot);
            }

            tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => {
                    break;
                }
                accept_res = &mut accept_slot => {
                    match accept_res {
                        Ok(accepted) => {
                            let input = ForwardInput {
                                frontend: accepted.channel,
                                target: self.target.clone(),
                                protocol: self.protocol,
                            };
                            if let Err(err) = coord.assign(input).await {
                                tracing::debug!(error = %err, "Forward assignment rejected during shutdown");
                                break;
                            }
                        }
                        Err(err) => {
                            if !shutdown_token.is_cancelled() && !matches!(err, TunnelError::Shutdown) {
                                tracing::error!(error = %err, "Forward frontend accept failed");
                            }
                        }
                    }
                }
            }
        }

        accept_slot.reset();
        self.frontend.shutdown().await;

        if let Some((ref pool_handle, _)) = self.pool {
            let _ = pool_handle.stop_replenishing().await;
        }

        let _ = coord.graceful_stop().await;
        let summary = coord_join
            .await
            .map_err(|e| TunnelError::Internal(format!("v2 coordinator join error: {e}")))?;

        if let Some((pool_handle, pool_join)) = self.pool {
            pool_handle.shutdown();
            let _ = pool_join.await;
        }

        Ok(summary)
    }
}
