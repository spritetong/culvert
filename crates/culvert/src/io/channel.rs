// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! The one I/O product shared by frontend, core, and backend.
//! All comments and docstrings must be in English.

use crate::io::addr::{L4Protocol, PeerAddr, TargetAddr};
use crate::io::error::TunnelError;
use crate::io::stream::{HeartbeatStatus, StreamCaps, TunnelStream};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// The I/O shape of a channel. `PumpRegistry` is keyed by a pair of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChannelKind {
    Stream,
    Datagram,
}

/// Identifies one logical datagram flow. A datagram pump uses it to map
/// replies back to their originating peer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FlowKey {
    pub peer: PeerAddr,
    pub target: Option<TargetAddr>,
    pub protocol: L4Protocol,
}

/// One datagram envelope.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Datagram {
    pub flow: FlowKey,
    pub payload: Bytes,
}

/// Batched datagram I/O. The batch shape keeps high-rate UDP/QUIC datagram
/// paths allocation-free once a pump allocates its scratch array.
pub trait DatagramIo: Send + Unpin + 'static {
    /// Fills `batch` with received datagrams and returns how many were filled.
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        batch: &mut [Datagram],
    ) -> Poll<io::Result<usize>>;

    /// Sends as many leading datagrams from `batch` as the transport accepts.
    fn poll_send(&mut self, cx: &mut Context<'_>, batch: &[Datagram]) -> Poll<io::Result<usize>>;
}

/// A channel yielded by a `Frontend` or a `TransportConnector` / `TransportListener`.
pub enum IoChannel {
    Stream(Box<dyn TunnelStream>),
    Datagram(Box<dyn DatagramIo>),
}

impl fmt::Debug for IoChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Stream(_) => "IoChannel::Stream",
            Self::Datagram(_) => "IoChannel::Datagram",
        })
    }
}

impl IoChannel {
    /// Wraps any concrete stream implementation.
    pub fn from_stream<S: TunnelStream>(stream: S) -> Self {
        Self::Stream(Box::new(stream))
    }

    /// Wraps an already type-erased stream.
    pub fn from_boxed_stream(stream: Box<dyn TunnelStream>) -> Self {
        Self::Stream(stream)
    }

    /// Wraps any concrete datagram implementation.
    pub fn from_datagram<D: DatagramIo>(datagram: D) -> Self {
        Self::Datagram(Box::new(datagram))
    }

    /// Wraps an already type-erased datagram channel.
    pub fn from_boxed_datagram(datagram: Box<dyn DatagramIo>) -> Self {
        Self::Datagram(datagram)
    }

    pub fn kind(&self) -> ChannelKind {
        match self {
            Self::Stream(_) => ChannelKind::Stream,
            Self::Datagram(_) => ChannelKind::Datagram,
        }
    }

    /// Explicit by-value conversion for stream-only consumers (control framing,
    /// session pairing, idle pools). Fails on a datagram channel instead of
    /// silently discarding datagrams.
    pub fn into_stream(self) -> Result<Box<dyn TunnelStream>, TunnelError> {
        match self {
            Self::Stream(stream) => Ok(stream),
            Self::Datagram(_) => Err(TunnelError::UnsupportedChannel(
                "a datagram channel has no byte-stream view".into(),
            )),
        }
    }

    pub fn as_stream_mut(&mut self) -> Result<&mut Box<dyn TunnelStream>, TunnelError> {
        match self {
            Self::Stream(stream) => Ok(stream),
            Self::Datagram(_) => Err(TunnelError::UnsupportedChannel(
                "a datagram channel has no byte-stream view".into(),
            )),
        }
    }

    /// Explicit by-value conversion for datagram pumps.
    pub fn into_datagram(self) -> Result<Box<dyn DatagramIo>, TunnelError> {
        match self {
            Self::Datagram(datagram) => Ok(datagram),
            Self::Stream(_) => Err(TunnelError::UnsupportedChannel(
                "a stream channel has no datagram view".into(),
            )),
        }
    }

    pub fn as_datagram_mut(&mut self) -> Result<&mut Box<dyn DatagramIo>, TunnelError> {
        match self {
            Self::Datagram(datagram) => Ok(datagram),
            Self::Stream(_) => Err(TunnelError::UnsupportedChannel(
                "a stream channel has no datagram view".into(),
            )),
        }
    }
}

impl From<Box<dyn TunnelStream>> for IoChannel {
    fn from(stream: Box<dyn TunnelStream>) -> Self {
        Self::from_boxed_stream(stream)
    }
}

impl From<Box<dyn DatagramIo>> for IoChannel {
    fn from(datagram: Box<dyn DatagramIo>) -> Self {
        Self::from_boxed_datagram(datagram)
    }
}

fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "datagram channel has no byte-stream view",
    )
}

impl AsyncRead for IoChannel {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            IoChannel::Stream(stream) => Pin::new(&mut **stream).poll_read(cx, buf),
            IoChannel::Datagram(_) => Poll::Ready(Err(unsupported())),
        }
    }
}

impl AsyncWrite for IoChannel {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            IoChannel::Stream(stream) => Pin::new(&mut **stream).poll_write(cx, buf),
            IoChannel::Datagram(_) => Poll::Ready(Err(unsupported())),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            IoChannel::Stream(stream) => Pin::new(&mut **stream).poll_flush(cx),
            IoChannel::Datagram(_) => Poll::Ready(Err(unsupported())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            IoChannel::Stream(stream) => Pin::new(&mut **stream).poll_shutdown(cx),
            IoChannel::Datagram(_) => Poll::Ready(Err(unsupported())),
        }
    }
}

impl TunnelStream for IoChannel {
    fn caps(&self) -> StreamCaps {
        match self {
            IoChannel::Stream(stream) => stream.caps(),
            IoChannel::Datagram(_) => StreamCaps::default(),
        }
    }

    fn poll_write_owned(&mut self, cx: &mut Context<'_>, chunk: Bytes) -> Poll<io::Result<usize>> {
        match self {
            IoChannel::Stream(stream) => stream.poll_write_owned(cx, chunk),
            IoChannel::Datagram(_) => Poll::Ready(Err(unsupported())),
        }
    }

    fn poll_send_ping(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self {
            IoChannel::Stream(stream) => stream.poll_send_ping(cx),
            IoChannel::Datagram(_) => Poll::Ready(Ok(())),
        }
    }

    fn last_activity_ms(&self) -> Option<u64> {
        match self {
            IoChannel::Stream(stream) => stream.last_activity_ms(),
            IoChannel::Datagram(_) => None,
        }
    }

    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        match self {
            IoChannel::Stream(stream) => stream.poll_check_alive(cx),
            IoChannel::Datagram(_) => Poll::Ready(HeartbeatStatus::Alive),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flow_key_msgpack_roundtrip() {
        let key = FlowKey {
            peer: PeerAddr::Socket("10.0.0.1:5353".parse().unwrap()),
            target: Some(TargetAddr::Ip("8.8.8.8:53".parse().unwrap())),
            protocol: L4Protocol::Udp,
        };
        let bytes = rmp_serde::to_vec_named(&key).unwrap();
        let decoded: FlowKey = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(key, decoded);
    }
}
