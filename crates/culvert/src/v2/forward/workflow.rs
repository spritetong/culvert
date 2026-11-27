// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Forward `TaskWorkflow`: consumes pre-warmed `Idle` transports or requests/dials
//! backend streams, activates pooled streams with `0xA5`/`0xAC`, and runs `ChannelRelay`.
//! All comments and docstrings must be in English.

use crate::consts::{ACTIVATE_STREAM_ACK, POOL_ACQUIRE_TIMEOUT, reverse_ack_timeout};
use crate::core::error::TunnelError;
use crate::io::addr::{L4Protocol, TargetAddr};
use crate::io::channel::IoChannel;
use crate::pool::activation::encode_activation_with_proto;
use crate::transport::TransportConnector;
use crate::v2::error::TaskError;
use crate::v2::relay::{ChannelRelay, RelayCounters};
use crate::v2::stage::{ConnectingContext, TaskWorkflow};
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Input payload delivered from `Frontend::accept` to a Forward `IoTask`.
pub struct ForwardInput {
    pub frontend: IoChannel,
    pub target: TargetAddr,
    pub protocol: L4Protocol,
    pub relay_idle_timeout: Option<Duration>,
}

impl ForwardInput {
    /// Creates a `ForwardInput` inheriting the workflow's default relay idle timeout.
    pub fn new(frontend: IoChannel, target: TargetAddr, protocol: L4Protocol) -> Self {
        Self {
            frontend,
            target,
            protocol,
            relay_idle_timeout: None,
        }
    }
}

/// Forward workflow handling direct dials and 0-RTT `Idle`-task stream activation with fallback.
#[derive(Clone)]
pub struct ForwardWorkflow {
    connector: Option<Arc<dyn TransportConnector>>,
    pooled: bool,
    acquire_timeout: Duration,
    heartbeat_interval: Duration,
    idle_timeout: Duration,
    relay_idle_timeout: Duration,
    drain_timeout: Duration,
    counters: Option<Arc<RelayCounters>>,
}

impl ForwardWorkflow {
    /// Creates a `ForwardWorkflow` backed by an optional direct `TransportConnector`.
    /// Call `.with_pooled(true)` when paired with a `CoordinatorConfig::forward_pooled(...)` pool.
    pub fn new(
        connector: Option<Arc<dyn TransportConnector>>,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        drain_timeout: Duration,
    ) -> Self {
        Self {
            connector,
            pooled: false,
            acquire_timeout: POOL_ACQUIRE_TIMEOUT,
            heartbeat_interval,
            idle_timeout,
            relay_idle_timeout: idle_timeout,
            drain_timeout,
            counters: None,
        }
    }

    /// Enables requesting/consuming pooled transports from `Coordinator` `Idle` tasks & `DialDriver`.
    pub fn with_pooled(mut self, pooled: bool) -> Self {
        self.pooled = pooled;
        self
    }

    /// Overrides the timeout for waiting on Coordinator transport dispatch (`request_transport`).
    pub fn with_acquire_timeout(mut self, acquire_timeout: Duration) -> Self {
        self.acquire_timeout = acquire_timeout;
        self
    }

    /// Overrides the relay idle timeout independently from pool/activation timeouts (e.g. for UDP).
    pub fn with_relay_idle_timeout(mut self, relay_idle_timeout: Duration) -> Self {
        self.relay_idle_timeout = relay_idle_timeout;
        self
    }

    /// Attaches optional shared relay byte counters (primarily for tests and telemetry).
    pub fn with_counters(mut self, counters: Arc<RelayCounters>) -> Self {
        self.counters = Some(counters);
        self
    }

    async fn acquire_backend(
        &self,
        ctx: &mut ConnectingContext<'_>,
        target: &TargetAddr,
        protocol: L4Protocol,
    ) -> Result<IoChannel, TaskError> {
        let ack_timeout = reverse_ack_timeout(self.idle_timeout);

        // 1. Zero-RTT transport handed directly via Idle -> Connecting transition.
        if let Some(mut transport) = ctx.take_transport() {
            match Self::activate_stream(&mut *transport.stream, target, protocol, ack_timeout).await
            {
                Ok(()) => return Ok(IoChannel::from_boxed_stream(transport.stream)),
                Err(err) => {
                    tracing::debug!(
                        target = %target,
                        error = %err,
                        "Idle transport activation failed; sweeping sibling streams and falling back"
                    );
                    if let Some(sid) = transport.session_id {
                        ctx.report_dead_session(sid);
                    }
                    drop(transport);
                    return self
                        .fallback_after_pool_failure(ctx, target, protocol, ack_timeout)
                        .await;
                }
            }
        }

        // 2. Pooled mode without an Idle stream on this slot: await Coordinator dial/inbound dispatch.
        if self.pooled {
            match ctx.request_transport(self.acquire_timeout).await {
                Ok(mut transport) => {
                    match Self::activate_stream(
                        &mut *transport.stream,
                        target,
                        protocol,
                        ack_timeout,
                    )
                    .await
                    {
                        Ok(()) => return Ok(IoChannel::from_boxed_stream(transport.stream)),
                        Err(err) => {
                            tracing::debug!(
                                target = %target,
                                error = %err,
                                "Dispatched pooled transport activation failed; falling back"
                            );
                            if let Some(sid) = transport.session_id {
                                ctx.report_dead_session(sid);
                            }
                            drop(transport);
                            return self
                                .fallback_after_pool_failure(ctx, target, protocol, ack_timeout)
                                .await;
                        }
                    }
                }
                Err(err) => {
                    tracing::debug!(
                        target = %target,
                        error = %err,
                        "Coordinator pooled transport request failed; checking direct connector"
                    );
                    if let Some(ref conn) = self.connector {
                        return Self::dial_direct(conn.as_ref(), target, protocol).await;
                    }
                    return Err(err);
                }
            }
        }

        // 3. Direct non-pooled dial.
        let Some(ref conn) = self.connector else {
            return Err(TaskError::StageFailed(
                "forward workflow has neither pooled transport nor connector".into(),
            ));
        };
        Self::dial_direct(conn.as_ref(), target, protocol).await
    }

    async fn fallback_after_pool_failure(
        &self,
        ctx: &ConnectingContext<'_>,
        target: &TargetAddr,
        protocol: L4Protocol,
        ack_timeout: Duration,
    ) -> Result<IoChannel, TaskError> {
        if let Some(ref conn) = self.connector {
            return Self::dial_direct(conn.as_ref(), target, protocol).await;
        }
        if self.pooled {
            let mut retry = ctx.request_transport(self.acquire_timeout).await?;
            Self::activate_stream(&mut *retry.stream, target, protocol, ack_timeout)
                .await
                .map_err(|e| {
                    if let Some(sid) = retry.session_id {
                        ctx.report_dead_session(sid);
                    }
                    TaskError::StageFailed(e.to_string())
                })?;
            return Ok(IoChannel::from_boxed_stream(retry.stream));
        }
        Err(TaskError::StageFailed(
            "forward activation failed with no fallback connector".into(),
        ))
    }

    async fn activate_stream<S>(
        stream: &mut S,
        target: &TargetAddr,
        protocol: L4Protocol,
        ack_timeout: Duration,
    ) -> io::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + ?Sized,
    {
        let pkt = encode_activation_with_proto(Some(target), protocol);
        stream.write_all(&pkt).await?;
        stream.flush().await?;

        let mut ack = [0u8; 1];
        tokio::time::timeout(ack_timeout, stream.read_exact(&mut ack))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for activation ACK",
                )
            })??;
        if ack[0] != ACTIVATE_STREAM_ACK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid activation ACK byte",
            ));
        }
        Ok(())
    }

    async fn dial_direct(
        connector: &dyn TransportConnector,
        target: &TargetAddr,
        protocol: L4Protocol,
    ) -> Result<IoChannel, TaskError> {
        connector
            .connect_with_protocol(target, None, protocol)
            .await
            .map_err(Self::tunnel_err_to_task)
    }

    fn tunnel_err_to_task(err: TunnelError) -> TaskError {
        TaskError::StageFailed(err.to_string())
    }
}

impl TaskWorkflow for ForwardWorkflow {
    type Input = ForwardInput;
    type Active = ChannelRelay;

    fn connect(
        &self,
        ctx: &mut ConnectingContext<'_>,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Active, TaskError>> + Send {
        let this = self.clone();
        async move {
            let backend = this
                .acquire_backend(ctx, &input.target, input.protocol)
                .await?;
            let relay_idle = input.relay_idle_timeout.unwrap_or(this.relay_idle_timeout);
            Ok(ChannelRelay::from_channels(
                input.frontend,
                backend,
                this.heartbeat_interval,
                relay_idle,
                this.drain_timeout,
                this.counters,
            ))
        }
    }
}
