// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Reverse tunnel (`-R`) client control loop, on-demand pairing, and pre-warmed
//! worker execution built on `Coordinator`, `IoTask`, and `ChannelRelay`.
//! All comments and docstrings must be in English.

use crate::consts::{
    ACTIVATE_STREAM_ACK, DEFAULT_REVERSE_BACKOFF_INITIAL, DEFAULT_REVERSE_BACKOFF_MAX,
    DEFAULT_REVERSE_PROMOTE_WAIT, POOL_BACKOFF_ACTIVE_WAITERS_MAX, POOL_BACKOFF_BASE,
    REVERSE_BACKOFF_RESET_THRESHOLD, REVERSE_CONTROL_TARGET_HOST, clamp_reverse_pool_size,
    reverse_ack_timeout, reverse_activation_wait_timeout,
};
use crate::control::{
    ControlMessage, SignalingChannel, SignalingConfig, SignalingRouter, SignalingSink,
};
use crate::core::error::TunnelError;
use crate::io::addr::{L4Protocol, ListenAddr, TargetAddr, parse_listen_addr, parse_target_addr};
use crate::io::channel::IoChannel;
use crate::pool::activation::decode_activation;
use crate::transport::TransportConnector;
use crate::v2::coordinator::{Coordinator, CoordinatorHandle, CoordinatorSummary};
use crate::v2::error::TaskError;
use crate::v2::relay::{ChannelRelay, RelayCounters};
use crate::v2::stage::{ConnectingContext, TaskWorkflow};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLock, mpsc};
use tokio_util::sync::CancellationToken;

/// Internal replenishment notification from a pre-warmed worker back to `ReverseClientRunner`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrewarmSignal {
    Consumed,
    FailedDial,
    RetryImmediately,
}

struct SignalingAttachmentInner {
    generation: u64,
    router: SignalingRouter,
    sink: Option<SignalingSink>,
    sig_config: SignalingConfig,
    cancel: CancellationToken,
}

/// Shared signaling attachment target for `0xA6` emergency stream promotion.
#[derive(Clone, Default)]
pub struct SignalingAttachment {
    inner: Arc<RwLock<Option<SignalingAttachmentInner>>>,
}

impl SignalingAttachment {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set(
        &self,
        generation: u64,
        router: SignalingRouter,
        sink: Option<SignalingSink>,
        sig_config: SignalingConfig,
        cancel: CancellationToken,
    ) {
        *self.inner.write().await = Some(SignalingAttachmentInner {
            generation,
            router,
            sink,
            sig_config,
            cancel,
        });
    }

    pub async fn clear(&self) {
        *self.inner.write().await = None;
    }

    async fn try_promote(
        &self,
        generation: u64,
        stream: Box<dyn crate::core::stream::TunnelStream>,
    ) -> bool {
        let guard = self.inner.read().await;
        if let Some(ref att) = *guard
            && att.generation == generation
            && let Some(ref sink) = att.sink
        {
            let handle = SignalingChannel::spawn_with_event_sink(
                stream,
                att.sig_config,
                sink.clone(),
                att.cancel.child_token(),
            );
            att.router.attach(handle);
            true
        } else {
            false
        }
    }
}

/// Input payload dispatched by `ReverseClientRunner` into a `ReverseClientWorkflow` `IoTask`.
pub enum ReverseClientInput {
    /// On-demand visitor session signaled over `ControlMessage::NewConnection`.
    OnDemand {
        session_id: String,
        target: TargetAddr,
        protocol: L4Protocol,
    },
    /// Pre-warmed reverse stream worker awaiting server activation (`0xA5`/`0xA7`/`0xA6`).
    Prewarmed {
        listen_targets: Vec<TargetAddr>,
        default_target: TargetAddr,
        default_protocol: L4Protocol,
        tunnel_id: Option<String>,
        replenish_tx: mpsc::UnboundedSender<PrewarmSignal>,
        generation: u64,
        active_generation: Arc<std::sync::atomic::AtomicU64>,
    },
}

/// Reverse client `TaskWorkflow` handling both on-demand sessions and pre-warmed workers.
#[derive(Clone)]
pub struct ReverseClientWorkflow {
    connector: Arc<dyn TransportConnector>,
    attachment: SignalingAttachment,
    heartbeat_interval: Duration,
    idle_timeout: Duration,
    relay_idle_timeout: Duration,
    drain_timeout: Duration,
    counters: Option<Arc<RelayCounters>>,
}

impl ReverseClientWorkflow {
    /// Creates a `ReverseClientWorkflow`.
    pub fn new(
        connector: Arc<dyn TransportConnector>,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        relay_idle_timeout: Duration,
        drain_timeout: Duration,
    ) -> Self {
        Self {
            connector,
            attachment: SignalingAttachment::new(),
            heartbeat_interval,
            idle_timeout,
            relay_idle_timeout,
            drain_timeout,
            counters: None,
        }
    }

    /// Attaches optional shared relay byte counters.
    pub fn with_counters(mut self, counters: Arc<RelayCounters>) -> Self {
        self.counters = Some(counters);
        self
    }

    pub(crate) fn attachment(&self) -> &SignalingAttachment {
        &self.attachment
    }

    async fn connect_on_demand(
        &self,
        session_id: String,
        target: TargetAddr,
        protocol: L4Protocol,
    ) -> Result<ChannelRelay, TaskError> {
        let dummy_target = TargetAddr::Domain(REVERSE_CONTROL_TARGET_HOST.to_string(), 0);
        let (local_res, remote_res) = tokio::join!(
            crate::platform::connect_target_channel(&target, protocol),
            self.connector
                .connect_with_protocol(&dummy_target, Some(&session_id), protocol)
        );

        let local_channel = local_res.map_err(|e| TaskError::StageFailed(e.to_string()))?;
        let remote_channel = remote_res.map_err(|e| TaskError::StageFailed(e.to_string()))?;

        Ok(ChannelRelay::from_channels(
            local_channel,
            remote_channel,
            self.heartbeat_interval,
            self.relay_idle_timeout,
            self.drain_timeout,
            self.counters.clone(),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn connect_prewarmed(
        &self,
        listen_targets: Vec<TargetAddr>,
        default_target: TargetAddr,
        default_protocol: L4Protocol,
        tunnel_id: Option<String>,
        replenish_tx: mpsc::UnboundedSender<PrewarmSignal>,
        generation: u64,
        active_generation: Arc<std::sync::atomic::AtomicU64>,
    ) -> Result<ChannelRelay, TaskError> {
        let mut dialed = None;
        for cand in &listen_targets {
            match self
                .connector
                .connect_pooled_with_tunnel(cand, default_protocol, tunnel_id.as_deref())
                .await
            {
                Ok(ch) => {
                    if let Ok(s) = ch.into_stream() {
                        dialed = Some(s);
                        break;
                    }
                }
                Err(err) => {
                    tracing::debug!(error = %err, "v2 reverse pre-warm dial failed");
                }
            }
        }

        let Some(mut stream) = dialed else {
            if active_generation.load(std::sync::atomic::Ordering::Acquire) == generation {
                let _ = replenish_tx.send(PrewarmSignal::FailedDial);
            }
            return Err(TaskError::StageFailed(
                "reverse pre-warm dial failed".into(),
            ));
        };

        let no_local_cancel = CancellationToken::new();
        let wait_timeout = reverse_activation_wait_timeout(self.idle_timeout);
        let hdr = match decode_activation(&mut stream, &no_local_cancel, wait_timeout).await {
            Ok(Some(h)) => h,
            Ok(None) => {
                if active_generation.load(std::sync::atomic::Ordering::Acquire) == generation {
                    let _ = replenish_tx.send(PrewarmSignal::RetryImmediately);
                }
                return Err(TaskError::StageFailed(
                    "reverse idle stream closed before activation".into(),
                ));
            }
            Err(err) => {
                if active_generation.load(std::sync::atomic::Ordering::Acquire) == generation {
                    let _ = replenish_tx.send(PrewarmSignal::RetryImmediately);
                }
                return Err(TaskError::StageFailed(err.to_string()));
            }
        };

        if active_generation.load(std::sync::atomic::Ordering::Acquire) != generation {
            return Err(TaskError::StageFailed(
                "reverse pre-warmed generation superseded".into(),
            ));
        }

        if hdr.is_promotion {
            let _ = self.attachment.try_promote(generation, stream).await;
            if active_generation.load(std::sync::atomic::Ordering::Acquire) == generation {
                let _ = replenish_tx.send(PrewarmSignal::RetryImmediately);
            }
            return Err(TaskError::StageFailed(
                "reverse stream promoted to signaling".into(),
            ));
        }

        let ack_timeout = reverse_ack_timeout(self.idle_timeout);
        let ack_res = tokio::time::timeout(ack_timeout, async {
            stream.write_all(&[ACTIVATE_STREAM_ACK]).await?;
            stream.flush().await
        })
        .await;

        if !matches!(ack_res, Ok(Ok(()))) {
            if active_generation.load(std::sync::atomic::Ordering::Acquire) == generation {
                let _ = replenish_tx.send(PrewarmSignal::RetryImmediately);
            }
            return Err(TaskError::StageFailed(
                "failed to send reverse 0xAC ACK".into(),
            ));
        }

        // Stream is now committed to a live visitor: notify runner to replenish the slot in 1 RTT.
        if active_generation.load(std::sync::atomic::Ordering::Acquire) == generation {
            let _ = replenish_tx.send(PrewarmSignal::Consumed);
        }

        let target = hdr.target.unwrap_or(default_target);
        let proto = hdr.protocol;
        let local_channel = crate::platform::connect_target_channel(&target, proto)
            .await
            .map_err(|e| TaskError::StageFailed(e.to_string()))?;

        Ok(ChannelRelay::from_channels(
            local_channel,
            IoChannel::from_boxed_stream(stream),
            self.heartbeat_interval,
            self.relay_idle_timeout,
            self.drain_timeout,
            self.counters.clone(),
        ))
    }
}

impl TaskWorkflow for ReverseClientWorkflow {
    type Input = ReverseClientInput;
    type Active = ChannelRelay;

    fn connect(
        &self,
        _ctx: &mut ConnectingContext<'_>,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Active, TaskError>> + Send {
        let this = self.clone();
        async move {
            match input {
                ReverseClientInput::OnDemand {
                    session_id,
                    target,
                    protocol,
                } => this.connect_on_demand(session_id, target, protocol).await,
                ReverseClientInput::Prewarmed {
                    listen_targets,
                    default_target,
                    default_protocol,
                    tunnel_id,
                    replenish_tx,
                    generation,
                    active_generation,
                } => {
                    this.connect_prewarmed(
                        listen_targets,
                        default_target,
                        default_protocol,
                        tunnel_id,
                        replenish_tx,
                        generation,
                        active_generation,
                    )
                    .await
                }
            }
        }
    }
}

/// Configuration for `ReverseClientRunner`.
pub struct ReverseClientConfig {
    pub listen_addr: String,
    pub target_addr: String,
    pub protocol: L4Protocol,
    pub pool_size: Option<usize>,
    pub tunnel_id: Option<String>,
    pub heartbeat_interval: Duration,
    pub idle_timeout: Duration,
    pub relay_idle_timeout: Duration,
    pub drain_timeout: Duration,
}

/// Supervised Reverse tunnel client runner coordinating control signaling, pre-warmed
/// workers, and on-demand sessions under one `Coordinator`.
pub struct ReverseClientRunner {
    config: ReverseClientConfig,
    connector: Arc<dyn TransportConnector>,
    counters: Option<Arc<RelayCounters>>,
}

impl ReverseClientRunner {
    /// Creates a new `ReverseClientRunner`.
    pub fn new(config: ReverseClientConfig, connector: Arc<dyn TransportConnector>) -> Self {
        Self {
            config,
            connector,
            counters: None,
        }
    }

    /// Attaches shared relay byte counters.
    pub fn with_counters(mut self, counters: Arc<RelayCounters>) -> Self {
        self.counters = Some(counters);
        self
    }

    /// Runs the reverse control loop and supervised worker tasks until `shutdown_token` cancels.
    pub async fn run(
        self,
        shutdown_token: CancellationToken,
    ) -> Result<CoordinatorSummary, TunnelError> {
        let mut workflow = ReverseClientWorkflow::new(
            Arc::clone(&self.connector),
            self.config.heartbeat_interval,
            self.config.idle_timeout,
            self.config.relay_idle_timeout,
            self.config.drain_timeout,
        );
        if let Some(ref c) = self.counters {
            workflow = workflow.with_counters(Arc::clone(c));
        }
        let attachment = workflow.attachment().clone();

        let coord_force_token = CancellationToken::new();
        let (coord, coord_join) =
            Coordinator::start(workflow, self.config.drain_timeout, coord_force_token);

        let pool_workers = clamp_reverse_pool_size(self.config.pool_size).unwrap_or(0);
        if pool_workers > 0 {
            let _ = coord.prewarm(pool_workers).await;
        }

        let target_parsed = parse_target_addr(&self.config.target_addr)?;
        let dummy_target = TargetAddr::Domain(REVERSE_CONTROL_TARGET_HOST.to_string(), 0);
        let listen_targets = Self::build_listen_targets(&self.config.listen_addr, &dummy_target);
        let sig_config = SignalingConfig {
            heartbeat_interval: self.config.heartbeat_interval,
            idle_timeout: self.config.idle_timeout,
        };

        let mut backoff = DEFAULT_REVERSE_BACKOFF_INITIAL;
        let active_generation = Arc::new(std::sync::atomic::AtomicU64::new(0));

        while !shutdown_token.is_cancelled() {
            let ctrl_stream = tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => break,
                res = self.connector.connect(&dummy_target, None) => match res {
                    Ok(ch) => match ch.into_stream() {
                        Ok(s) => s,
                        Err(_) => break,
                    },
                    Err(err) => {
                        tracing::debug!(error = %err, "v2 reverse control dial failed; backing off");
                        tokio::select! {
                            _ = shutdown_token.cancelled() => break,
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                        continue;
                    }
                }
            };

            let ctrl_cancel = shutdown_token.child_token();
            let (signaling, mut sig_rx) =
                SignalingChannel::spawn(ctrl_stream, sig_config, ctrl_cancel.clone());
            let router = SignalingRouter::new(signaling);

            let reg_msg = ControlMessage::RegisterReverse {
                listen_addr: self.config.listen_addr.clone(),
                target_addr: self.config.target_addr.clone(),
                pool_size: clamp_reverse_pool_size(self.config.pool_size),
                protocol: self.config.protocol,
                tunnel_id: self.config.tunnel_id.clone(),
            };

            if router.send(reg_msg).await.is_err() {
                ctrl_cancel.cancel();
                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                continue;
            }

            let ack_ok = tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => break,
                res = tokio::time::timeout(self.config.idle_timeout, sig_rx.recv()) => {
                    matches!(
                        res,
                        Ok(Some(Ok(ControlMessage::RegisterAck { success: true, .. })))
                    )
                }
            };

            if !ack_ok {
                ctrl_cancel.cancel();
                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                continue;
            }

            let loop_start = Instant::now();
            let generation =
                active_generation.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1;
            attachment
                .set(
                    generation,
                    router.clone(),
                    sig_rx.event_sink(),
                    sig_config,
                    ctrl_cancel.clone(),
                )
                .await;
            let (replenish_tx, mut replenish_rx) = mpsc::unbounded_channel();
            let mut prewarm_backoff = POOL_BACKOFF_BASE;

            for _ in 0..pool_workers {
                let _ = Self::dispatch_prewarm(
                    &coord,
                    &listen_targets,
                    &target_parsed,
                    self.config.protocol,
                    self.config.tunnel_id.clone(),
                    &replenish_tx,
                    generation,
                    &active_generation,
                )
                .await;
            }

            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_token.cancelled() => break,
                    Some(sig) = replenish_rx.recv(), if pool_workers > 0 => {
                        match sig {
                            PrewarmSignal::Consumed | PrewarmSignal::RetryImmediately => {
                                prewarm_backoff = POOL_BACKOFF_BASE;
                            }
                            PrewarmSignal::FailedDial => {
                                tokio::select! {
                                    _ = shutdown_token.cancelled() => break,
                                    _ = tokio::time::sleep(prewarm_backoff) => {}
                                }
                                prewarm_backoff = (prewarm_backoff * 2).min(POOL_BACKOFF_ACTIVE_WAITERS_MAX);
                            }
                        }
                        if !shutdown_token.is_cancelled() {
                            let _ = Self::dispatch_prewarm(
                                &coord,
                                &listen_targets,
                                &target_parsed,
                                self.config.protocol,
                                self.config.tunnel_id.clone(),
                                &replenish_tx,
                                generation,
                                &active_generation,
                            )
                            .await;
                        }
                    }
                    msg_opt = sig_rx.recv() => {
                        match msg_opt {
                            Some(Ok(ControlMessage::NewConnection { session_id, .. })) => {
                                let _ = coord
                                    .assign(ReverseClientInput::OnDemand {
                                        session_id,
                                        target: target_parsed.clone(),
                                        protocol: self.config.protocol,
                                    })
                                    .await;
                            }
                            Some(Ok(_)) => {}
                            _ => {
                                if router.promote_standby_if_closed() || router.has_active_channel() {
                                    continue;
                                }
                                if pool_workers > 0 && !shutdown_token.is_cancelled() {
                                    let wait_window = DEFAULT_REVERSE_PROMOTE_WAIT.min(self.config.idle_timeout);
                                    let mut promoted = false;
                                    let deadline = tokio::time::sleep(wait_window);
                                    tokio::pin!(deadline);
                                    loop {
                                        let attached = router.notified();
                                        tokio::pin!(attached);
                                        if router.promote_standby_if_closed() || router.has_active_channel() {
                                            promoted = true;
                                            break;
                                        }
                                        tokio::select! {
                                            biased;
                                            _ = shutdown_token.cancelled() => break,
                                            _ = &mut deadline => break,
                                            _ = &mut attached => {
                                                if router.promote_standby_if_closed() || router.has_active_channel() {
                                                    promoted = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    if promoted {
                                        continue;
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }

            active_generation.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            attachment.clear().await;
            ctrl_cancel.cancel();

            if shutdown_token.is_cancelled() {
                break;
            }
            if loop_start.elapsed() > REVERSE_BACKOFF_RESET_THRESHOLD {
                backoff = DEFAULT_REVERSE_BACKOFF_INITIAL;
            } else {
                backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
            }
            tokio::select! {
                _ = shutdown_token.cancelled() => break,
                _ = tokio::time::sleep(backoff) => {}
            }
        }

        let _ = coord.graceful_stop().await;
        coord_join.await.map_err(|e| {
            TunnelError::Internal(format!("v2 reverse client coordinator join error: {e}"))
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_prewarm(
        coord: &CoordinatorHandle<ReverseClientWorkflow>,
        listen_targets: &[TargetAddr],
        default_target: &TargetAddr,
        default_protocol: L4Protocol,
        tunnel_id: Option<String>,
        replenish_tx: &mpsc::UnboundedSender<PrewarmSignal>,
        generation: u64,
        active_generation: &Arc<std::sync::atomic::AtomicU64>,
    ) -> Result<(), TaskError> {
        let _ = coord
            .assign(ReverseClientInput::Prewarmed {
                listen_targets: listen_targets.to_vec(),
                default_target: default_target.clone(),
                default_protocol,
                tunnel_id,
                replenish_tx: replenish_tx.clone(),
                generation,
                active_generation: Arc::clone(active_generation),
            })
            .await?;
        Ok(())
    }

    fn build_listen_targets(listen_addr: &str, fallback: &TargetAddr) -> Vec<TargetAddr> {
        match parse_listen_addr(listen_addr) {
            Ok(ListenAddr::DualStack(port)) => vec![
                TargetAddr::Ip(std::net::SocketAddr::new(
                    std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
                    port,
                )),
                TargetAddr::Ip(std::net::SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    port,
                )),
            ],
            Ok(ListenAddr::Ip(sa)) => vec![TargetAddr::Ip(sa)],
            Ok(ListenAddr::Domain(h, p)) => vec![TargetAddr::Domain(h, p)],
            Ok(ListenAddr::Path(p)) => vec![TargetAddr::Path(p)],
            Err(_) => vec![fallback.clone()],
        }
    }
}
