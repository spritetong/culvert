// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Reverse visitor `TaskWorkflow`: 0-RTT `PassiveInbound` pool fast path with
//! `0xAC` ACK verification and seamless fallback to control-channel signaling.
//! All comments and docstrings must be in English.

use crate::consts::{ACTIVATE_STREAM_ACK, reverse_ack_timeout};
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::io::addr::{L4Protocol, PeerAddr, TargetAddr};
use crate::io::channel::IoChannel;
use crate::pool::activation::encode_activation_with_proto;
use crate::v2::error::TaskError;
use crate::v2::pool::{PoolHandle, StreamLease};
use crate::v2::relay::{DuplexRelay, RelayCounters};
use crate::v2::stage::{ConnectingContext, TaskWorkflow};
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Slow-path reverse signaling abstraction requested when the 0-RTT inbound pool
/// is empty or a popped idle stream fails activation ACK.
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
}

/// Reverse visitor workflow unifying 0-RTT pool fast-path and slow-path signaling.
#[derive(Clone)]
pub struct ReverseVisitorWorkflow {
    pool: Option<PoolHandle>,
    signaler: Arc<dyn ReverseSignaler>,
    heartbeat_interval: Duration,
    idle_timeout: Duration,
    drain_timeout: Duration,
    counters: Option<Arc<RelayCounters>>,
}

impl ReverseVisitorWorkflow {
    /// Creates a `ReverseVisitorWorkflow` backed by an optional `PassiveInbound` pool
    /// and a fallback `ReverseSignaler`.
    pub fn new(
        pool: Option<PoolHandle>,
        signaler: Arc<dyn ReverseSignaler>,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        drain_timeout: Duration,
    ) -> Self {
        Self {
            pool,
            signaler,
            heartbeat_interval,
            idle_timeout,
            drain_timeout,
            counters: None,
        }
    }

    /// Attaches shared atomic counters for telemetry and tests.
    pub fn with_counters(mut self, counters: Arc<RelayCounters>) -> Self {
        self.counters = Some(counters);
        self
    }

    async fn acquire_backend(
        &self,
        peer: PeerAddr,
        target: &TargetAddr,
        protocol: L4Protocol,
    ) -> Result<Box<dyn TunnelStream>, TaskError> {
        // 1. Fast Path (0-RTT PassiveInbound pool + 0xAC round-trip verification)
        if let Some(ref pool) = self.pool
            && let Ok(Some(mut lease)) = pool.try_acquire().await
        {
            let ack_timeout = reverse_ack_timeout(self.idle_timeout);
            match Self::activate_and_verify(&mut lease, target, protocol, ack_timeout).await {
                Ok(()) => return Ok(Box::new(lease)),
                Err(err) => {
                    tracing::debug!(
                        visitor = %peer,
                        error = %err,
                        "Reverse 0-RTT pooled stream failed activation ACK; falling back to signaling"
                    );
                    drop(lease);
                }
            }
        }

        // 2. Slow Path (Control-channel NewConnection signaling)
        self.signaler.request_pair(peer).await
    }

    async fn activate_and_verify(
        stream: &mut StreamLease,
        target: &TargetAddr,
        protocol: L4Protocol,
        ack_timeout: Duration,
    ) -> io::Result<()> {
        let pkt = encode_activation_with_proto(Some(target), protocol);
        stream.write_all(&pkt).await?;
        stream.flush().await?;

        let mut ack = [0u8; 1];
        tokio::time::timeout(ack_timeout, stream.read_exact(&mut ack))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for reverse 0xAC ACK",
                )
            })??;

        if ack[0] != ACTIVATE_STREAM_ACK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid reverse activation ACK byte",
            ));
        }
        Ok(())
    }
}

impl TaskWorkflow for ReverseVisitorWorkflow {
    type Input = ReverseVisitorInput;
    type Active = DuplexRelay;

    fn connect(
        &self,
        _ctx: &mut ConnectingContext<'_>,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Active, TaskError>> + Send {
        let this = self.clone();
        async move {
            let visitor = input
                .visitor
                .into_stream()
                .map_err(|e| TaskError::StageFailed(e.to_string()))?;
            let backend = this
                .acquire_backend(input.peer, &input.target, input.protocol)
                .await?;
            let mut relay = DuplexRelay::new(
                visitor,
                backend,
                this.heartbeat_interval,
                this.idle_timeout,
                this.drain_timeout,
            );
            if let Some(ref counters) = this.counters {
                relay = relay.with_counters(Arc::clone(counters));
            }
            Ok(relay)
        }
    }
}
