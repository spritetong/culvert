// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Server-side Forward `TaskWorkflow`: handles optional `0xA5`/`0xA7` pre-warmed
//! activation, target dialing, and `ChannelRelay` data forwarding.
//! All comments and docstrings must be in English.

use crate::consts::{ACTIVATE_STREAM_ACK, POOL_DUMMY_HOST, reverse_ack_timeout};
use crate::core::stream::TunnelStream;
use crate::io::addr::{L4Protocol, TargetAddr, parse_target_addr};
use crate::io::channel::IoChannel;
use crate::pool::activation::decode_activation_until_cancel;
use crate::transport::StreamMetadata;
use crate::v2::error::TaskError;
use crate::v2::relay::ChannelRelay;
use crate::v2::stage::{ConnectingContext, TaskWorkflow};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

/// Input payload accepted by a `v2::ServerRunner` listener for `TunnelMode::Forward`.
pub struct ServerForwardInput {
    pub channel: IoChannel,
    pub metadata: StreamMetadata,
}

/// Server-side `TaskWorkflow` processing one inbound `TunnelMode::Forward` stream.
#[derive(Clone)]
pub struct ServerForwardWorkflow {
    default_target: Option<String>,
    heartbeat_interval: Duration,
    idle_timeout: Duration,
    udp_idle_timeout: Duration,
    drain_timeout: Duration,
}

impl ServerForwardWorkflow {
    /// Creates a new `ServerForwardWorkflow`.
    pub fn new(
        default_target: Option<String>,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        udp_idle_timeout: Duration,
        drain_timeout: Duration,
    ) -> Self {
        Self {
            default_target,
            heartbeat_interval,
            idle_timeout,
            udp_idle_timeout,
            drain_timeout,
        }
    }

    async fn wait_pooled_activation(
        &self,
        stream: &mut Box<dyn TunnelStream>,
    ) -> Result<(Option<TargetAddr>, L4Protocol), TaskError> {
        let no_local_cancel = CancellationToken::new();
        let header = decode_activation_until_cancel(stream, &no_local_cancel)
            .await
            .map_err(|err| TaskError::StageFailed(err.to_string()))?
            .ok_or_else(|| {
                TaskError::StageFailed("pooled forward stream closed before activation".into())
            })?;

        let ack_timeout = reverse_ack_timeout(self.idle_timeout);
        tokio::time::timeout(ack_timeout, async {
            stream.write_all(&[ACTIVATE_STREAM_ACK]).await?;
            stream.flush().await
        })
        .await
        .map_err(|_| TaskError::StageFailed("timed out sending forward 0xAC ACK".into()))?
        .map_err(|err| TaskError::StageFailed(err.to_string()))?;

        Ok((header.target, header.protocol))
    }

    fn resolve_target(
        &self,
        dynamic_target: Option<TargetAddr>,
        meta_target: Option<TargetAddr>,
    ) -> Option<TargetAddr> {
        let valid_meta = match meta_target {
            Some(TargetAddr::Domain(ref host, _)) if host == POOL_DUMMY_HOST => None,
            other => other,
        };
        dynamic_target.or(valid_meta).or_else(|| {
            self.default_target
                .as_deref()
                .and_then(|s| parse_target_addr(s).ok())
        })
    }
}

impl TaskWorkflow for ServerForwardWorkflow {
    type Input = ServerForwardInput;
    type Active = ChannelRelay;

    async fn connect(
        &self,
        _ctx: &mut ConnectingContext<'_>,
        input: Self::Input,
    ) -> Result<Self::Active, TaskError> {
        let ServerForwardInput { channel, metadata } = input;

        let (inbound_channel, dynamic_target, activated_proto) = if metadata.pool_idle {
            let mut stream = channel
                .into_stream()
                .map_err(|err| TaskError::StageFailed(err.to_string()))?;
            let (dyn_target, proto) = self.wait_pooled_activation(&mut stream).await?;
            (
                IoChannel::from_boxed_stream(stream),
                dyn_target,
                Some(proto),
            )
        } else {
            (channel, None, None)
        };

        let effective_proto = activated_proto.unwrap_or(metadata.protocol);
        let relay_idle_timeout = match effective_proto {
            L4Protocol::Tcp => self.idle_timeout,
            L4Protocol::Udp => self.udp_idle_timeout,
        };

        let target = self
            .resolve_target(dynamic_target, metadata.target)
            .ok_or_else(|| {
                TaskError::StageFailed("forward stream arrived without target address".into())
            })?;

        let target_channel = crate::platform::connect_target_channel(&target, effective_proto)
            .await
            .map_err(|err| TaskError::StageFailed(err.to_string()))?;

        Ok(ChannelRelay::from_channels(
            inbound_channel,
            target_channel,
            self.heartbeat_interval,
            relay_idle_timeout,
            self.drain_timeout,
            None,
        ))
    }
}
