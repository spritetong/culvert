// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Multi-mapping tunnel cluster orchestrating a shared unified Forward `Coordinator`,
//! shared Reverse pre-warmed workers, and per-mapping Reverse Coordinators under one boundary.
//! All comments and docstrings must be in English.

use crate::config::model::{PortMappingConfig, TunnelConfig, TunnelMode};
use crate::consts::{
    DEFAULT_RELAY_DRAIN_TIMEOUT, POOL_BACKOFF_ACTIVE_WAITERS_MAX, POOL_BACKOFF_BASE,
    REVERSE_CONTROL_TARGET_HOST, clamp_reverse_pool_size,
};
use crate::core::coordinator::{Coordinator, CoordinatorHandle};
use crate::core::error::TunnelError;
use crate::core::state::CoordinatorConfig;
use crate::dialer::endpoint::EndpointContext;
use crate::frontend::{self, Frontend, FrontendOutput};
use crate::io::addr::{L4Protocol, MappingSpec, TargetAddr, parse_target_addr};
use crate::transport::{TransportConnector, TransportRegistry};
use crate::tunnel::forward::{ForwardInput, ForwardWorkflow};
use crate::tunnel::reverse::{
    PrewarmSignal, ReverseClientConfig, ReverseClientInput, ReverseClientRunner,
    ReverseClientWorkflow, ReverseReadyNotifier,
};
use reusable_future::ReusableFutureRef;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Shared reverse inbound pool runner feeding pre-warmed connections for all reverse mappings
/// under one `tunnel_id`.
struct SharedReversePoolRunner {
    tunnel_id: String,
    pool_size: usize,
    connector: Arc<dyn TransportConnector>,
    heartbeat_interval: Duration,
    idle_timeout: Duration,
    udp_idle_timeout: Duration,
    ready: Arc<ReverseReadyNotifier>,
}

impl SharedReversePoolRunner {
    async fn run(self, cancel_token: CancellationToken) -> Result<(), TunnelError> {
        let size = clamp_reverse_pool_size(Some(self.pool_size)).unwrap_or(0);
        if size == 0 {
            return Ok(());
        }
        if !self.ready.wait_ready(&cancel_token).await {
            return Ok(());
        }

        let relay_idle_timeout = self.idle_timeout.max(self.udp_idle_timeout);
        let workflow = ReverseClientWorkflow::new(
            Arc::clone(&self.connector),
            self.heartbeat_interval,
            self.idle_timeout,
            relay_idle_timeout,
            DEFAULT_RELAY_DRAIN_TIMEOUT,
        );
        let coord_force_token = CancellationToken::new();
        let (coord, coord_join) =
            Coordinator::start(workflow, DEFAULT_RELAY_DRAIN_TIMEOUT, coord_force_token);

        let _ = coord.prewarm(size).await;

        let listen_targets = vec![TargetAddr::Domain(
            REVERSE_CONTROL_TARGET_HOST.to_string(),
            0,
        )];
        let dummy_target = TargetAddr::Domain(REVERSE_CONTROL_TARGET_HOST.to_string(), 0);
        let active_generation = Arc::new(AtomicU64::new(1));
        let (replenish_tx, mut replenish_rx) = mpsc::unbounded_channel();
        let mut backoff = POOL_BACKOFF_BASE;

        for _ in 0..size {
            let _ = coord
                .assign(ReverseClientInput::Prewarmed {
                    listen_targets: listen_targets.clone(),
                    default_target: dummy_target.clone(),
                    default_protocol: L4Protocol::Tcp,
                    tunnel_id: Some(self.tunnel_id.clone()),
                    replenish_tx: replenish_tx.clone(),
                    generation: 1,
                    active_generation: Arc::clone(&active_generation),
                })
                .await;
        }

        loop {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                Some(sig) = replenish_rx.recv() => {
                    match sig {
                        PrewarmSignal::Consumed | PrewarmSignal::RetryImmediately => {
                            backoff = POOL_BACKOFF_BASE;
                        }
                        PrewarmSignal::FailedDial => {
                            tokio::select! {
                                _ = cancel_token.cancelled() => break,
                                _ = tokio::time::sleep(backoff) => {}
                            }
                            backoff = (backoff * 2).min(POOL_BACKOFF_ACTIVE_WAITERS_MAX);
                        }
                    }
                    if !cancel_token.is_cancelled() {
                        let _ = coord
                            .assign(ReverseClientInput::Prewarmed {
                                listen_targets: listen_targets.clone(),
                                default_target: dummy_target.clone(),
                                default_protocol: L4Protocol::Tcp,
                                tunnel_id: Some(self.tunnel_id.clone()),
                                replenish_tx: replenish_tx.clone(),
                                generation: 1,
                                active_generation: Arc::clone(&active_generation),
                            })
                            .await;
                    }
                }
            }
        }

        active_generation.fetch_add(1, Ordering::AcqRel);
        let _ = coord.graceful_stop().await;
        let _ = coord_join.await;
        Ok(())
    }
}

/// Runs all Forward and Reverse mappings for one `TunnelConfig` with unified supervision.
pub struct TunnelCluster;

impl TunnelCluster {
    /// Starts all configured mappings in `config` until `cancel_token` cancels or a fatal error occurs.
    pub async fn run(
        config: TunnelConfig,
        endpoint_ctx: EndpointContext,
        transport_registry: Arc<TransportRegistry>,
        cancel_token: CancellationToken,
        client_uuid: &str,
    ) -> Result<(), TunnelError> {
        let mappings = config.canonical_mappings()?;
        let hb_interval = endpoint_ctx.heartbeat_interval();
        let idle_timeout = endpoint_ctx.idle_timeout();
        let default_udp_idle = endpoint_ctx.udp_idle_timeout();

        let has_forward = mappings.iter().any(|m| m.mode == TunnelMode::Forward);
        let fwd_pool_size = config.forward_pool_size().filter(|&s| s > 0);
        let shared_fwd_coord = if has_forward {
            let conn: Arc<dyn TransportConnector> =
                Arc::from(transport_registry.create_connector(
                    &endpoint_ctx,
                    config.token.as_deref(),
                    TunnelMode::Forward,
                )?);
            let workflow = ForwardWorkflow::new(
                Some(Arc::clone(&conn)),
                hb_interval,
                idle_timeout,
                DEFAULT_RELAY_DRAIN_TIMEOUT,
            )
            .with_pooled(fwd_pool_size.is_some());
            let coord_cfg = match fwd_pool_size {
                Some(size) => CoordinatorConfig::forward_pooled(
                    Arc::clone(&conn),
                    size,
                    hb_interval,
                    idle_timeout,
                    DEFAULT_RELAY_DRAIN_TIMEOUT,
                ),
                None => CoordinatorConfig::plain(DEFAULT_RELAY_DRAIN_TIMEOUT),
            };
            Some(Coordinator::start_with_config(
                workflow,
                coord_cfg,
                CancellationToken::new(),
            ))
        } else {
            None
        };

        let has_reverse = mappings.iter().any(|m| m.mode == TunnelMode::Reverse);
        let rev_connector: Option<Arc<dyn TransportConnector>> = if has_reverse {
            Some(Arc::from(transport_registry.create_connector(
                &endpoint_ctx,
                config.token.as_deref(),
                TunnelMode::Reverse,
            )?))
        } else {
            None
        };

        let tunnel_id = config.effective_tunnel_id(client_uuid);
        let rev_pool_size = config.reverse_pool_size().filter(|&s| s > 0);
        let reverse_ready = if has_reverse && rev_pool_size.is_some() {
            Some(Arc::new(ReverseReadyNotifier::new()))
        } else {
            None
        };

        let cluster_cancel = cancel_token.child_token();
        let mut workers: JoinSet<Result<(), TunnelError>> = JoinSet::new();

        if let Some(ref conn) = rev_connector
            && let Some(pool_size) = rev_pool_size
            && let Some(ref ready) = reverse_ready
        {
            let pool_runner = SharedReversePoolRunner {
                tunnel_id: tunnel_id.clone(),
                pool_size,
                connector: Arc::clone(conn),
                heartbeat_interval: hb_interval,
                idle_timeout,
                udp_idle_timeout: default_udp_idle,
                ready: Arc::clone(ready),
            };
            let pool_tok = cluster_cancel.child_token();
            workers.spawn(async move { pool_runner.run(pool_tok).await });
        }

        let shared_fwd_handle = shared_fwd_coord.as_ref().map(|(h, _)| h.clone());

        for mapping in mappings {
            let PortMappingConfig {
                mode,
                protocol,
                listen,
                target,
                udp_idle_timeout,
            } = mapping;

            match mode {
                TunnelMode::Forward => {
                    let Some(ref coord) = shared_fwd_handle else {
                        return Err(TunnelError::Internal(
                            "missing shared forward coordinator".into(),
                        ));
                    };
                    let fe = frontend::bind(&MappingSpec::new(protocol, &listen, &target)).await?;
                    let target_parsed = parse_target_addr(&target)?;
                    let relay_idle = match protocol {
                        L4Protocol::Tcp => idle_timeout,
                        L4Protocol::Udp => udp_idle_timeout.unwrap_or(default_udp_idle),
                    };
                    let coord_clone = coord.clone();
                    let tok = cluster_cancel.child_token();
                    workers.spawn(async move {
                        Self::run_forward_frontend(
                            fe,
                            target_parsed,
                            protocol,
                            relay_idle,
                            coord_clone,
                            tok,
                        )
                        .await
                    });
                }
                TunnelMode::Reverse => {
                    let conn = rev_connector.as_ref().cloned().ok_or_else(|| {
                        TunnelError::Internal("missing reverse transport connector".into())
                    })?;
                    let relay_idle = match protocol {
                        L4Protocol::Tcp => idle_timeout,
                        L4Protocol::Udp => udp_idle_timeout.unwrap_or(default_udp_idle),
                    };
                    let mut runner = ReverseClientRunner::new(
                        ReverseClientConfig {
                            listen_addr: listen,
                            target_addr: target,
                            protocol,
                            pool_size: rev_pool_size,
                            tunnel_id: Some(tunnel_id.clone()),
                            heartbeat_interval: hb_interval,
                            idle_timeout,
                            relay_idle_timeout: relay_idle,
                            drain_timeout: DEFAULT_RELAY_DRAIN_TIMEOUT,
                        },
                        conn,
                    );
                    if let Some(ref ready) = reverse_ready {
                        runner = runner.with_shared_pool(Arc::clone(ready));
                    }
                    let tok = cluster_cancel.child_token();
                    workers.spawn(async move {
                        let _ = runner.run(tok).await?;
                        Ok(())
                    });
                }
            }
        }

        let mut exit_err: Option<TunnelError> = None;
        loop {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                res = workers.join_next(), if !workers.is_empty() => {
                    match res {
                        Some(Ok(Err(TunnelError::Shutdown))) => {
                            if workers.is_empty() {
                                break;
                            }
                        }
                        Some(Ok(Err(err))) => {
                            if !cancel_token.is_cancelled() {
                                if exit_err.is_none() {
                                    exit_err = Some(err);
                                }
                                break;
                            }
                        }
                        Some(Err(join_err)) => {
                            if !cancel_token.is_cancelled() && exit_err.is_none() {
                                exit_err = Some(TunnelError::Internal(format!(
                                    "cluster worker terminated: {join_err}"
                                )));
                                break;
                            }
                        }
                        Some(Ok(Ok(()))) => {
                            if workers.is_empty() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        cluster_cancel.cancel();
        while workers.join_next().await.is_some() {}

        if let Some((coord, join)) = shared_fwd_coord {
            let _ = coord.graceful_stop().await;
            let _ = join.await;
        }

        if let Some(err) = exit_err
            && !matches!(err, TunnelError::Shutdown)
        {
            return Err(err);
        }
        Ok(())
    }

    async fn run_forward_frontend(
        mut frontend: Box<dyn Frontend>,
        target: TargetAddr,
        protocol: L4Protocol,
        relay_idle_timeout: Duration,
        coord: CoordinatorHandle<ForwardWorkflow>,
        shutdown_token: CancellationToken,
    ) -> Result<(), TunnelError> {
        let mut accept_slot: ReusableFutureRef<'static, FrontendOutput> =
            ReusableFutureRef::default();
        let mut accept_backoff = crate::frontend::AcceptBackoff::default();

        let accept_res: Result<(), TunnelError> = loop {
            match crate::frontend::next_frontend_accept(
                &mut *frontend,
                &mut accept_slot,
                &mut accept_backoff,
                &shutdown_token,
                "Forward frontend",
            )
            .await
            {
                Ok(Some(accepted)) => {
                    let input = ForwardInput {
                        frontend: accepted.channel,
                        target: target.clone(),
                        protocol,
                        relay_idle_timeout: Some(relay_idle_timeout),
                    };
                    if let Err(err) = coord.assign(input).await {
                        tracing::debug!(error = %err, "Forward assignment rejected during shutdown");
                        break Ok(());
                    }
                }
                Ok(None) => break Ok(()),
                Err(err) => break Err(err),
            }
        };

        accept_slot.reset();
        frontend.shutdown().await;
        accept_res
    }
}
