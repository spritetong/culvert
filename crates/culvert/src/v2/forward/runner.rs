// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Supervised Forward listener runner coordinating `Frontend` and unified `Coordinator`.
//! All comments and docstrings must be in English.

use crate::core::error::TunnelError;
use crate::frontend::{Frontend, FrontendOutput};
use crate::io::addr::{L4Protocol, TargetAddr};
use crate::v2::coordinator::{Coordinator, CoordinatorSummary};
use crate::v2::forward::workflow::{ForwardInput, ForwardWorkflow};
use crate::v2::state::CoordinatorConfig;
use reusable_future::ReusableFutureRef;
use tokio_util::sync::CancellationToken;

/// Runs a single forward mapping using the unified v2 `Coordinator` + `IoTask` stack.
pub struct ForwardRunner {
    frontend: Box<dyn Frontend>,
    target: TargetAddr,
    protocol: L4Protocol,
    workflow: ForwardWorkflow,
    coord_config: CoordinatorConfig,
    prewarm_tasks: usize,
}

impl ForwardRunner {
    /// Creates a new `ForwardRunner`.
    pub fn new(
        frontend: Box<dyn Frontend>,
        target: TargetAddr,
        protocol: L4Protocol,
        workflow: ForwardWorkflow,
        coord_config: CoordinatorConfig,
    ) -> Self {
        Self {
            frontend,
            target,
            protocol,
            workflow,
            coord_config,
            prewarm_tasks: 0,
        }
    }

    /// Pre-spawns `count` empty worker tasks inside the `Coordinator` before accepting connections.
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
            Coordinator::start_with_config(self.workflow, self.coord_config, coord_force_token);

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
                            let input = ForwardInput::new(
                                accepted.channel,
                                self.target.clone(),
                                self.protocol,
                            );
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

        let _ = coord.graceful_stop().await;
        let summary = coord_join
            .await
            .map_err(|e| TunnelError::Internal(format!("v2 coordinator join error: {e}")))?;

        Ok(summary)
    }
}
