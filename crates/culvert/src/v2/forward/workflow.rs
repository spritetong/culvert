// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Forward `TaskWorkflow`: acquires or dials a backend stream, activates pooled
//! streams with `0xA5`/`0xAC`, and transitions into `DuplexRelay`.
//! All comments and docstrings must be in English.

use crate::consts::{ACTIVATE_STREAM_ACK, reverse_ack_timeout};
use crate::core::error::TunnelError;
use crate::io::addr::{L4Protocol, TargetAddr};
use crate::io::channel::IoChannel;
use crate::pool::activation::encode_activation_with_proto;
use crate::transport::TransportConnector;
use crate::v2::error::TaskError;
use crate::v2::pool::{PoolHandle, StreamLease};
use crate::v2::relay::{ChannelRelay, RelayCounters};
use crate::v2::stage::{ConnectingContext, TaskWorkflow};
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Input payload delivered from `Frontend::accept` to a Forward `IoTask`.
pub struct ForwardInput {
    pub frontend: IoChannel,
    pub target: TargetAddr,
    pub protocol: L4Protocol,
}

/// Forward workflow handling direct dials and 0-RTT pooled stream activation with fallback.
#[derive(Clone)]
pub struct ForwardWorkflow {
    pool: Option<PoolHandle>,
    connector: Option<Arc<dyn TransportConnector>>,
    heartbeat_interval: Duration,
    idle_timeout: Duration,
    relay_idle_timeout: Duration,
    drain_timeout: Duration,
    counters: Option<Arc<RelayCounters>>,
}

impl ForwardWorkflow {
    /// Creates a `ForwardWorkflow` backed by an optional `TransportPool` and/or direct connector.
    pub fn new(
        pool: Option<PoolHandle>,
        connector: Option<Arc<dyn TransportConnector>>,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        drain_timeout: Duration,
    ) -> Self {
        Self {
            pool,
            connector,
            heartbeat_interval,
            idle_timeout,
            relay_idle_timeout: idle_timeout,
            drain_timeout,
            counters: None,
        }
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
        target: &TargetAddr,
        protocol: L4Protocol,
    ) -> Result<IoChannel, TaskError> {
        if let Some(ref pool) = self.pool {
            match self
                .acquire_and_activate_pooled(pool, target, protocol)
                .await
            {
                Ok(lease) => return Ok(IoChannel::from_stream(lease)),
                Err(err) => {
                    tracing::debug!(
                        target = %target,
                        error = %err,
                        "Pooled stream activation failed; falling back"
                    );
                    if let Some(ref conn) = self.connector {
                        return Self::dial_direct(conn.as_ref(), target, protocol).await;
                    }
                    let retry_lease = self
                        .acquire_and_activate_pooled(pool, target, protocol)
                        .await?;
                    return Ok(IoChannel::from_stream(retry_lease));
                }
            }
        }

        let Some(ref conn) = self.connector else {
            return Err(TaskError::StageFailed(
                "forward workflow has neither pool nor connector".into(),
            ));
        };
        Self::dial_direct(conn.as_ref(), target, protocol).await
    }

    async fn acquire_and_activate_pooled(
        &self,
        pool: &PoolHandle,
        target: &TargetAddr,
        protocol: L4Protocol,
    ) -> Result<StreamLease, TaskError> {
        let mut lease = pool
            .acquire()
            .await
            .map_err(|e| TaskError::StageFailed(e.to_string()))?;
        let ack_timeout = reverse_ack_timeout(self.idle_timeout);
        if let Err(err) = Self::activate_stream(&mut lease, target, protocol, ack_timeout).await {
            drop(lease);
            return Err(TaskError::StageFailed(err.to_string()));
        }
        Ok(lease)
    }

    async fn activate_stream(
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
        _ctx: &mut ConnectingContext<'_>,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Active, TaskError>> + Send {
        let this = self.clone();
        async move {
            let backend = this.acquire_backend(&input.target, input.protocol).await?;
            Ok(ChannelRelay::from_channels(
                input.frontend,
                backend,
                this.heartbeat_interval,
                this.relay_idle_timeout,
                this.drain_timeout,
                this.counters,
            ))
        }
    }
}
