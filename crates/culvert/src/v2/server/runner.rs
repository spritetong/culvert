// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Ingress server runner integrating `TransportListener`, `Coordinator<ServerForwardWorkflow>`,
//! `ReverseSessionHub`, and `ReverseControlSession`.
//! All comments and docstrings must be in English.

use crate::config::model::{QuicConfig, ServerConfig, ServerListenSpec, TlsConfig, TunnelMode};
use crate::consts::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT, DEFAULT_RELAY_DRAIN_TIMEOUT,
    DEFAULT_UDP_IDLE_TIMEOUT,
};
use crate::core::error::TunnelError;
use crate::core::stream::TunnelStream;
use crate::io::channel::IoChannel;
use crate::transport::{StreamMetadata, TransportRegistry};
use crate::v2::coordinator::{Coordinator, CoordinatorHandle};
use crate::v2::server::forward::{ServerForwardInput, ServerForwardWorkflow};
use crate::v2::server::reverse::{ReverseControlSession, ReverseSessionHub};
use crate::v2::state::PooledTransport;
use reusable_future::ReusableFutureRef;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// v2 Ingress server runner dispatching inbound Forward and Reverse streams onto
/// supervised v2 Coordinators and Pools.
pub struct ServerRunner {
    config: ServerConfig,
    global_tls: Option<TlsConfig>,
    quic_config: QuicConfig,
    transport_registry: Arc<TransportRegistry>,
    hub: Arc<ReverseSessionHub>,
    drain_timeout: Duration,
}

impl ServerRunner {
    /// Creates a `ServerRunner` with default registries and tuning.
    pub fn from_config(config: ServerConfig) -> Self {
        Self::with_quic_config(
            config,
            None,
            QuicConfig::default(),
            Arc::new(TransportRegistry::default()),
            Arc::new(ReverseSessionHub::new()),
        )
    }

    /// Creates a `ServerRunner` with custom transport registry and session hub.
    pub fn new(
        config: ServerConfig,
        transport_registry: Arc<TransportRegistry>,
        hub: Arc<ReverseSessionHub>,
    ) -> Self {
        Self::with_quic_config(config, None, QuicConfig::default(), transport_registry, hub)
    }

    /// Creates a `ServerRunner` with explicit TLS and QUIC transport tuning.
    pub fn with_quic_config(
        config: ServerConfig,
        global_tls: Option<TlsConfig>,
        quic_config: QuicConfig,
        transport_registry: Arc<TransportRegistry>,
        hub: Arc<ReverseSessionHub>,
    ) -> Self {
        Self {
            config,
            global_tls,
            quic_config,
            transport_registry,
            hub,
            drain_timeout: DEFAULT_RELAY_DRAIN_TIMEOUT,
        }
    }

    /// Runs all configured ingress listeners until `cancel_token` cancels, then drains cleanly.
    pub async fn run(&self, cancel_token: CancellationToken) -> Result<(), TunnelError> {
        let specs = self.config.parse_specs()?;
        if specs.is_empty() {
            cancel_token.cancelled().await;
            return Ok(());
        }

        let idle_timeout = self.config.idle_timeout.unwrap_or(DEFAULT_IDLE_TIMEOUT);
        let heartbeat_interval = self
            .config
            .heartbeat_interval
            .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL);
        let udp_idle_timeout = self
            .config
            .udp_idle_timeout
            .unwrap_or(DEFAULT_UDP_IDLE_TIMEOUT);

        let forward_workflow = ServerForwardWorkflow::new(
            self.config.default_target.clone(),
            heartbeat_interval,
            idle_timeout,
            udp_idle_timeout,
            self.drain_timeout,
        );
        let forward_force_token = CancellationToken::new();
        let (forward_coord, forward_coord_join) =
            Coordinator::start(forward_workflow, self.drain_timeout, forward_force_token);

        let server_tls = self.config.effective_tls(self.global_tls.as_ref());
        let mut listeners: JoinSet<Result<(), TunnelError>> = JoinSet::new();

        for spec in specs {
            let registry = Arc::clone(&self.transport_registry);
            let hub = Arc::clone(&self.hub);
            let forward_coord = forward_coord.clone();
            let token_opt = self.config.token.clone();
            let tls_cfg = server_tls.clone();
            let quic_cfg = self.quic_config.clone();
            let listener_cancel = cancel_token.child_token();

            listeners.spawn(async move {
                Self::run_listener(
                    spec,
                    registry,
                    token_opt,
                    tls_cfg,
                    quic_cfg,
                    idle_timeout,
                    heartbeat_interval,
                    udp_idle_timeout,
                    forward_coord,
                    hub,
                    listener_cancel,
                )
                .await
            });
        }

        let mut exit_err: Option<TunnelError> = None;
        loop {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                res = listeners.join_next(), if !listeners.is_empty() => {
                    match res {
                        Some(Ok(Err(TunnelError::Shutdown))) => {
                            if listeners.is_empty() {
                                break;
                            }
                        }
                        Some(Ok(Err(err))) => {
                            if cancel_token.is_cancelled() {
                                if listeners.is_empty() {
                                    break;
                                }
                                continue;
                            }
                            if exit_err.is_none() {
                                exit_err = Some(err);
                            }
                            cancel_token.cancel();
                            break;
                        }
                        Some(Err(join_err)) => {
                            if !cancel_token.is_cancelled() && exit_err.is_none() {
                                exit_err = Some(TunnelError::Internal(format!(
                                    "v2 server listener task terminated: {join_err}"
                                )));
                                cancel_token.cancel();
                            }
                            break;
                        }
                        Some(Ok(Ok(()))) => {
                            if listeners.is_empty() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        while let Some(res) = listeners.join_next().await {
            if let Ok(Err(err)) = res
                && !matches!(err, TunnelError::Shutdown)
                && !cancel_token.is_cancelled()
                && exit_err.is_none()
            {
                exit_err = Some(err);
            }
        }

        let _ = forward_coord.graceful_stop().await;
        let _ = forward_coord_join.await;

        if let Some(err) = exit_err
            && !matches!(err, TunnelError::Shutdown)
        {
            return Err(err);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_listener(
        spec: ServerListenSpec,
        registry: Arc<TransportRegistry>,
        token_opt: Option<String>,
        server_tls: Option<TlsConfig>,
        quic_config: QuicConfig,
        idle_timeout: Duration,
        heartbeat_interval: Duration,
        udp_idle_timeout: Duration,
        forward_coord: CoordinatorHandle<ServerForwardWorkflow>,
        hub: Arc<ReverseSessionHub>,
        cancel_token: CancellationToken,
    ) -> Result<(), TunnelError> {
        let mut listener = registry.create_listener(
            &spec.scheme,
            &spec.bind_addr,
            token_opt.as_deref(),
            spec.path.as_deref(),
            server_tls.as_ref(),
            &quic_config,
            cancel_token.clone(),
        )?;

        if let Some(mode) = spec.chmod
            && let Err(err) = crate::platform::set_socket_permissions(
                &crate::platform::normalize_uds_path(&spec.bind_addr),
                mode,
            )
        {
            tracing::warn!(
                bind = %spec.bind_addr,
                mode = format_args!("{:#o}", mode),
                error = %err,
                "Failed to set socket permissions"
            );
        }

        let mut reverse_tasks: JoinSet<()> = JoinSet::new();
        let mut accept_slot: ReusableFutureRef<
            'static,
            Result<(IoChannel, StreamMetadata), TunnelError>,
        > = ReusableFutureRef::default();
        let mut accept_err: Option<TunnelError> = None;

        loop {
            if accept_slot.is_empty() {
                listener.accept(&mut accept_slot);
            }

            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                Some(_) = reverse_tasks.join_next(), if !reverse_tasks.is_empty() => {}
                accept_res = &mut accept_slot => {
                    match accept_res {
                        Ok((channel, metadata)) => {
                            match metadata.mode {
                                TunnelMode::Forward => {
                                    let _ = forward_coord
                                        .assign(ServerForwardInput { channel, metadata })
                                        .await;
                                }
                                TunnelMode::Reverse => {
                                    let Ok(stream) = channel.into_stream() else {
                                        tracing::warn!("Reverse control plane requires a byte-stream channel");
                                        continue;
                                    };
                                    Self::dispatch_reverse_stream(
                                        stream,
                                        metadata,
                                        Arc::clone(&hub),
                                        idle_timeout,
                                        heartbeat_interval,
                                        udp_idle_timeout,
                                        &mut reverse_tasks,
                                        cancel_token.clone(),
                                    );
                                }
                            }
                        }
                        Err(TunnelError::Shutdown) => break,
                        Err(_) if cancel_token.is_cancelled() => break,
                        Err(err) => {
                            accept_err = Some(err);
                            break;
                        }
                    }
                }
            }
        }

        accept_slot.reset();
        while reverse_tasks.join_next().await.is_some() {}
        listener.shutdown().await;

        match accept_err {
            Some(TunnelError::Shutdown) => Ok(()),
            Some(_) if cancel_token.is_cancelled() => Ok(()),
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_reverse_stream(
        stream: Box<dyn TunnelStream>,
        metadata: StreamMetadata,
        hub: Arc<ReverseSessionHub>,
        idle_timeout: Duration,
        heartbeat_interval: Duration,
        udp_idle_timeout: Duration,
        reverse_tasks: &mut JoinSet<()>,
        cancel_token: CancellationToken,
    ) {
        if let Some(session_id) = metadata.session_id {
            if let Err(err) = hub.fulfill(&session_id, stream) {
                tracing::warn!(session = %session_id, error = ?err, "Failed to fulfill v2 reverse session");
            }
        } else if metadata.pool_idle {
            let coord_opt = hub.lookup_idle_coordinator(
                metadata.tunnel_id.as_deref(),
                metadata.protocol,
                metadata.target.as_ref(),
            );
            if let Some((coord, generation)) = coord_opt {
                let transport = PooledTransport::new(stream, generation);
                reverse_tasks.spawn(async move {
                    let _ = coord.supply_transport(transport).await;
                });
            }
        } else {
            reverse_tasks.spawn(async move {
                ReverseControlSession::run(
                    stream,
                    hub,
                    idle_timeout,
                    heartbeat_interval,
                    udp_idle_timeout,
                    cancel_token,
                )
                .await;
            });
        }
    }
}
