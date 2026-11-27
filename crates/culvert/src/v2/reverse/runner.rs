// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Supervised Reverse visitor listener runner integrating `Frontend`, `TransportPool`,
//! and `Coordinator`.
//! All comments and docstrings must be in English.

use crate::core::error::TunnelError;
use crate::frontend::{Frontend, FrontendOutput};
use crate::io::addr::{L4Protocol, TargetAddr};
use crate::v2::coordinator::{Coordinator, CoordinatorSummary};
use crate::v2::pool::{PoolHandle, PoolSummary};
use crate::v2::reverse::visitor::{ReverseVisitorInput, ReverseVisitorWorkflow};
use reusable_future::ReusableFutureRef;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Runs a reverse visitor listener using the v2 `Coordinator` + `IoTask` + `TransportPool` stack.
pub struct ReverseServerRunner {
    frontend: Box<dyn Frontend>,
    target: TargetAddr,
    protocol: L4Protocol,
    workflow: ReverseVisitorWorkflow,
    pool: Option<(PoolHandle, JoinHandle<PoolSummary>)>,
    prewarm_tasks: usize,
    drain_timeout: Duration,
}

impl ReverseServerRunner {
    /// Creates a new `ReverseServerRunner`.
    pub fn new(
        frontend: Box<dyn Frontend>,
        target: TargetAddr,
        protocol: L4Protocol,
        workflow: ReverseVisitorWorkflow,
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

    /// Pre-spawns `count` idle `Empty` tasks inside the `Coordinator`.
    pub fn with_prewarm_tasks(mut self, count: usize) -> Self {
        self.prewarm_tasks = count;
        self
    }

    /// Accepts external reverse visitors until `shutdown_token` cancels, then drains cleanly.
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
                _ = shutdown_token.cancelled() => break,
                accept_res = &mut accept_slot => {
                    match accept_res {
                        Ok(accepted) => {
                            let input = ReverseVisitorInput {
                                visitor: accepted.channel,
                                peer: accepted.peer,
                                target: self.target.clone(),
                                protocol: self.protocol,
                            };
                            if let Err(err) = coord.assign(input).await {
                                tracing::debug!(error = %err, "Reverse visitor rejected during shutdown");
                                break;
                            }
                        }
                        Err(err) => {
                            if !shutdown_token.is_cancelled() && !matches!(err, TunnelError::Shutdown) {
                                tracing::error!(error = %err, "Reverse visitor accept failed");
                            }
                        }
                    }
                }
            }
        }

        accept_slot.reset();
        self.frontend.shutdown().await;

        let _ = coord.graceful_stop().await;
        let summary = coord_join.await.map_err(|e| {
            TunnelError::Internal(format!("v2 reverse coordinator join error: {e}"))
        })?;

        if let Some((pool_handle, pool_join)) = self.pool {
            pool_handle.shutdown();
            let _ = pool_join.await;
        }

        Ok(summary)
    }
}
