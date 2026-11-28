// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Reverse visitor `TaskWorkflow`: 0-RTT `Idle`-task fast path with
//! `0xAC` ACK verification and seamless fallback to control-channel signaling.
//! All comments and docstrings must be in English.

use crate::consts::reverse_ack_timeout;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::io::addr::{L4Protocol, PeerAddr, TargetAddr};
use crate::io::channel::IoChannel;
use crate::pool::activation::activate_and_verify_stream;
use crate::v2::error::TaskError;
use crate::v2::relay::{ChannelRelay, RelayCounters};
use crate::v2::stage::{ConnectingContext, TaskWorkflow};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// Slow-path reverse signaling abstraction requested when no `Idle` task stream
/// is available or a popped idle stream fails activation ACK.
pub trait ReverseSignaler: Send + Sync + 'static {
    /// Signals the remote client to dial a dedicated reverse data stream for `visitor_peer`.
    ///
    /// Implementations should clean up any pending session registration if the returned
    /// future is dropped (e.g. due to `Drain` or `ForceStop` cancellation).
    fn request_pair(
        &self,
        visitor_peer: PeerAddr,
    ) -> BoxFuture<'static, Result<Box<dyn TunnelStream>, TaskError>>;
}

/// Input payload delivered from a reverse listening `Frontend` to an `IoTask`.
pub struct ReverseVisitorInput {
    pub visitor: IoChannel,
    pub peer: PeerAddr,
    pub target: TargetAddr,
    pub protocol: L4Protocol,
    pub relay_idle_timeout: Option<Duration>,
    pub signaler: Option<Arc<dyn ReverseSignaler>>,
}

impl ReverseVisitorInput {
    /// Creates a `ReverseVisitorInput` inheriting workflow defaults for idle timeout and signaler.
    pub fn new(
        visitor: IoChannel,
        peer: PeerAddr,
        target: TargetAddr,
        protocol: L4Protocol,
    ) -> Self {
        Self {
            visitor,
            peer,
            target,
            protocol,
            relay_idle_timeout: None,
            signaler: None,
        }
    }
}

/// Reverse visitor workflow unifying 0-RTT `Idle`-task fast-path and slow-path signaling.
#[derive(Clone)]
pub struct ReverseVisitorWorkflow {
    signaler: Option<Arc<dyn ReverseSignaler>>,
    heartbeat_interval: Duration,
    idle_timeout: Duration,
    drain_timeout: Duration,
    counters: Option<Arc<RelayCounters>>,
    #[cfg(test)]
    ack_timeout_override: Option<Duration>,
}

impl ReverseVisitorWorkflow {
    /// Creates a `ReverseVisitorWorkflow` backed by a default fallback `ReverseSignaler`.
    pub fn new(
        signaler: Arc<dyn ReverseSignaler>,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        drain_timeout: Duration,
    ) -> Self {
        Self {
            signaler: Some(signaler),
            heartbeat_interval,
            idle_timeout,
            drain_timeout,
            counters: None,
            #[cfg(test)]
            ack_timeout_override: None,
        }
    }

    /// Creates a `ReverseVisitorWorkflow` expecting each `ReverseVisitorInput` to supply its signaler
    /// (used for shared `tunnel_id` multi-listener Coordinators).
    pub fn shared(
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        drain_timeout: Duration,
    ) -> Self {
        Self {
            signaler: None,
            heartbeat_interval,
            idle_timeout,
            drain_timeout,
            counters: None,
            #[cfg(test)]
            ack_timeout_override: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout_override = Some(timeout);
        self
    }

    #[inline]
    fn effective_ack_timeout(&self) -> Duration {
        #[cfg(test)]
        if let Some(timeout) = self.ack_timeout_override {
            return timeout;
        }
        reverse_ack_timeout(self.idle_timeout)
    }

    /// Attaches shared atomic counters for telemetry and tests.
    pub fn with_counters(mut self, counters: Arc<RelayCounters>) -> Self {
        self.counters = Some(counters);
        self
    }

    async fn acquire_backend(
        &self,
        ctx: &mut ConnectingContext<'_>,
        peer: PeerAddr,
        target: &TargetAddr,
        protocol: L4Protocol,
        signaler_override: Option<Arc<dyn ReverseSignaler>>,
    ) -> Result<IoChannel, TaskError> {
        // 1. Fast Path (0-RTT Idle stream handed off on Idle -> Connecting + 0xAC verification)
        if let Some(mut transport) = ctx.take_transport() {
            let ack_timeout = self.effective_ack_timeout();
            match activate_and_verify_stream(
                &mut *transport.stream,
                Some(target),
                protocol,
                ack_timeout,
            )
            .await
            {
                Ok(()) => return Ok(IoChannel::from_boxed_stream(transport.stream)),
                Err(err) => {
                    // Activation handshake failure only proves this stream failed; keep
                    // sibling idle streams on the shared session intact.
                    tracing::debug!(
                        visitor = %peer,
                        error = %err,
                        "Reverse 0-RTT idle stream failed activation ACK; falling back to signaling"
                    );
                    drop(transport);
                }
            }
        }

        // 2. Slow Path (Control-channel NewConnection signaling)
        let signaler = signaler_override
            .or_else(|| self.signaler.clone())
            .ok_or_else(|| TaskError::StageFailed("reverse visitor missing signaler".into()))?;
        signaler
            .request_pair(peer)
            .await
            .map(IoChannel::from_boxed_stream)
    }
}

impl TaskWorkflow for ReverseVisitorWorkflow {
    type Input = ReverseVisitorInput;
    type Active = ChannelRelay;

    fn connect(
        &self,
        ctx: &mut ConnectingContext<'_>,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Active, TaskError>> + Send {
        let this = self.clone();
        async move {
            let backend = this
                .acquire_backend(
                    ctx,
                    input.peer,
                    &input.target,
                    input.protocol,
                    input.signaler,
                )
                .await?;
            let relay_idle = input.relay_idle_timeout.unwrap_or(this.idle_timeout);
            Ok(ChannelRelay::from_channels(
                input.visitor,
                backend,
                this.heartbeat_interval,
                relay_idle,
                this.drain_timeout,
                this.counters,
            ))
        }
    }
}
