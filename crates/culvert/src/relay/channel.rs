// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Static-dispatch channel relay covering all `(Stream | Datagram) x (Stream | Datagram)` pairs.
//! All comments and docstrings must be in English.

use crate::core::error::TaskError;
use crate::core::stage::{ActiveStage, ClosingStage};
use crate::core::state::CloseReason;
use crate::io::channel::IoChannel;
use crate::relay::datagram_datagram::{DatagramDatagramClosing, DatagramDatagramRelay};
use crate::relay::datagram_stream::{DatagramStreamClosing, DatagramStreamRelay};
use crate::relay::duplex::{DuplexRelay, RelayClosing, RelayCounters};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

/// Unified hot-path `ActiveStage` dispatching across any `IoChannel` pair without heap boxing.
pub enum ChannelRelay {
    StreamStream(DuplexRelay),
    DatagramStream(DatagramStreamRelay),
    DatagramDatagram(DatagramDatagramRelay),
}

impl ChannelRelay {
    /// Builds the matching relay strategy for `(frontend, backend)`.
    pub fn from_channels(
        frontend: IoChannel,
        backend: IoChannel,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        drain_timeout: Duration,
        counters: Option<Arc<RelayCounters>>,
    ) -> Self {
        match (frontend, backend) {
            (IoChannel::Stream(a), IoChannel::Stream(b)) => {
                let mut relay =
                    DuplexRelay::new(a, b, heartbeat_interval, idle_timeout, drain_timeout);
                if let Some(c) = counters {
                    relay = relay.with_counters(c);
                }
                Self::StreamStream(relay)
            }
            (IoChannel::Datagram(d), IoChannel::Stream(s)) => {
                let mut relay = DatagramStreamRelay::new(
                    d,
                    s,
                    heartbeat_interval,
                    idle_timeout,
                    drain_timeout,
                    false,
                );
                if let Some(c) = counters {
                    relay = relay.with_counters(c);
                }
                Self::DatagramStream(relay)
            }
            (IoChannel::Stream(s), IoChannel::Datagram(d)) => {
                let mut relay = DatagramStreamRelay::new(
                    d,
                    s,
                    heartbeat_interval,
                    idle_timeout,
                    drain_timeout,
                    true,
                );
                if let Some(c) = counters {
                    relay = relay.with_counters(c);
                }
                Self::DatagramStream(relay)
            }
            (IoChannel::Datagram(a), IoChannel::Datagram(b)) => {
                let mut relay = DatagramDatagramRelay::new(
                    a,
                    b,
                    heartbeat_interval,
                    idle_timeout,
                    drain_timeout,
                );
                if let Some(c) = counters {
                    relay = relay.with_counters(c);
                }
                Self::DatagramDatagram(relay)
            }
        }
    }
}

impl ActiveStage for ChannelRelay {
    type Closing = ChannelClosing;

    fn poll_drive(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), TaskError>> {
        match self {
            Self::StreamStream(r) => r.poll_drive(cx),
            Self::DatagramStream(r) => r.poll_drive(cx),
            Self::DatagramDatagram(r) => r.poll_drive(cx),
        }
    }

    fn into_closing(self, reason: CloseReason) -> Self::Closing {
        match self {
            Self::StreamStream(r) => ChannelClosing::StreamStream(r.into_closing(reason)),
            Self::DatagramStream(r) => ChannelClosing::DatagramStream(r.into_closing(reason)),
            Self::DatagramDatagram(r) => ChannelClosing::DatagramDatagram(r.into_closing(reason)),
        }
    }
}

/// Corresponding bounded closing stage for `ChannelRelay`.
pub enum ChannelClosing {
    StreamStream(RelayClosing),
    DatagramStream(DatagramStreamClosing),
    DatagramDatagram(DatagramDatagramClosing),
}

impl ClosingStage for ChannelClosing {
    async fn run(&mut self) -> Result<(), TaskError> {
        match self {
            Self::StreamStream(c) => c.run().await,
            Self::DatagramStream(c) => c.run().await,
            Self::DatagramDatagram(c) => c.run().await,
        }
    }
}
