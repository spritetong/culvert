// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Supervised Reverse visitor listener runner integrating `Frontend` and unified `Coordinator`.
//! All comments and docstrings must be in English.

use crate::core::error::TunnelError;
use crate::frontend::{AcceptBackoff, Frontend, FrontendOutput, next_frontend_accept};
use crate::io::addr::{L4Protocol, TargetAddr};
use crate::v2::coordinator::{Coordinator, CoordinatorHandle, CoordinatorSummary};
use crate::v2::reverse::visitor::{ReverseSignaler, ReverseVisitorInput, ReverseVisitorWorkflow};
use crate::v2::state::CoordinatorConfig;
use reusable_future::ReusableFutureRef;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

enum CoordMode {
    Owned {
        workflow: ReverseVisitorWorkflow,
        config: Box<CoordinatorConfig>,
        prewarm_tasks: usize,
    },
    External {
        coord: CoordinatorHandle<ReverseVisitorWorkflow>,
        coord_join: Option<JoinHandle<CoordinatorSummary>>,
        signaler: Option<Arc<dyn ReverseSignaler>>,
        relay_idle_timeout: Option<Duration>,
    },
}

/// Runs a reverse visitor listener using the unified v2 `Coordinator` + `IoTask` stack.
pub struct ReverseServerRunner {
    frontend: Box<dyn Frontend>,
    target: TargetAddr,
    protocol: L4Protocol,
    mode: CoordMode,
}

impl ReverseServerRunner {
    /// Creates a new `ReverseServerRunner` that owns and supervises its own `Coordinator`.
    pub fn new(
        frontend: Box<dyn Frontend>,
        target: TargetAddr,
        protocol: L4Protocol,
        workflow: ReverseVisitorWorkflow,
        coord_config: CoordinatorConfig,
    ) -> Self {
        Self {
            frontend,
            target,
            protocol,
            mode: CoordMode::Owned {
                workflow,
                config: Box::new(coord_config),
                prewarm_tasks: 0,
            },
        }
    }

    /// Creates a `ReverseServerRunner` driving an existing `CoordinatorHandle` (for ingress
    /// `pool_idle` delivery, `0xA6` promotion, or shared `tunnel_id` clusters).
    ///
    /// When `coord_join` is `Some`, this runner owns the `Coordinator` lifetime and executes
    /// `graceful_stop()` + join on shutdown; when `None`, coordinator shutdown is coordinated
    /// externally by the shared cluster owner.
    pub fn with_coordinator(
        frontend: Box<dyn Frontend>,
        target: TargetAddr,
        protocol: L4Protocol,
        coord: CoordinatorHandle<ReverseVisitorWorkflow>,
        coord_join: Option<JoinHandle<CoordinatorSummary>>,
        signaler: Option<Arc<dyn ReverseSignaler>>,
        relay_idle_timeout: Option<Duration>,
    ) -> Self {
        Self {
            frontend,
            target,
            protocol,
            mode: CoordMode::External {
                coord,
                coord_join,
                signaler,
                relay_idle_timeout,
            },
        }
    }

    /// Pre-spawns `count` empty worker tasks inside an owned `Coordinator`.
    pub fn with_prewarm_tasks(mut self, count: usize) -> Self {
        if let CoordMode::Owned {
            ref mut prewarm_tasks,
            ..
        } = self.mode
        {
            *prewarm_tasks = count;
        }
        self
    }

    /// Accepts external reverse visitors until `shutdown_token` cancels, then drains cleanly.
    pub async fn run(
        mut self,
        shutdown_token: CancellationToken,
    ) -> Result<Option<CoordinatorSummary>, TunnelError> {
        let (coord, coord_join, signaler_override, relay_idle_timeout) = match self.mode {
            CoordMode::Owned {
                workflow,
                config,
                prewarm_tasks,
            } => {
                let coord_force_token = CancellationToken::new();
                let (coord, join) =
                    Coordinator::start_with_config(workflow, *config, coord_force_token);
                if prewarm_tasks > 0 {
                    let _ = coord.prewarm(prewarm_tasks).await;
                }
                (coord, Some(join), None, None)
            }
            CoordMode::External {
                coord,
                coord_join,
                signaler,
                relay_idle_timeout,
            } => (coord, coord_join, signaler, relay_idle_timeout),
        };

        let mut accept_slot: ReusableFutureRef<'static, FrontendOutput> =
            ReusableFutureRef::default();
        let mut accept_backoff = AcceptBackoff::default();

        let accept_res: Result<(), TunnelError> = loop {
            match next_frontend_accept(
                &mut *self.frontend,
                &mut accept_slot,
                &mut accept_backoff,
                &shutdown_token,
                "Reverse visitor",
            )
            .await
            {
                Ok(Some(accepted)) => {
                    let input = ReverseVisitorInput {
                        visitor: accepted.channel,
                        peer: accepted.peer,
                        target: self.target.clone(),
                        protocol: self.protocol,
                        relay_idle_timeout,
                        signaler: signaler_override.clone(),
                    };
                    if let Err(err) = coord.assign(input).await {
                        tracing::debug!(error = %err, "Reverse visitor rejected during shutdown");
                        break Ok(());
                    }
                }
                Ok(None) => break Ok(()),
                Err(err) => break Err(err),
            }
        };

        accept_slot.reset();
        self.frontend.shutdown().await;

        if let Some(join) = coord_join {
            let _ = coord.graceful_stop().await;
            let summary = join.await.map_err(|e| {
                TunnelError::Internal(format!("v2 reverse coordinator join error: {e}"))
            })?;
            accept_res?;
            Ok(Some(summary))
        } else {
            accept_res?;
            Ok(None)
        }
    }
}
