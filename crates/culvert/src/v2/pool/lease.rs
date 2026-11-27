// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! RAII stream lease ensuring active stream counts never leak across errors or cancellations.
//! All comments and docstrings must be in English.

use crate::io::stream::{HeartbeatStatus, StreamCaps, TunnelStream};
use bytes::Bytes;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;

/// Wake-up signal emitted when a leased stream is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolReleaseSignal {
    Released,
}

/// RAII drop guard maintaining the pool's authoritative active stream count.
pub struct LeaseGuard {
    active_count: Arc<AtomicUsize>,
    release_tx: mpsc::UnboundedSender<PoolReleaseSignal>,
    disarmed: bool,
}

impl LeaseGuard {
    pub(crate) fn new(
        active_count: Arc<AtomicUsize>,
        release_tx: mpsc::UnboundedSender<PoolReleaseSignal>,
    ) -> Self {
        active_count.fetch_add(1, Ordering::AcqRel);
        Self {
            active_count,
            release_tx,
            disarmed: false,
        }
    }

    /// Reverses the active-count increment without emitting a wake-up signal.
    pub(crate) fn cancel_reclaim(mut self) {
        self.disarmed = true;
        let _ = self
            .active_count
            .try_update(Ordering::AcqRel, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        let _ = self
            .active_count
            .try_update(Ordering::AcqRel, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
        let _ = self.release_tx.send(PoolReleaseSignal::Released);
    }
}

/// Owned tunnel stream wrapped with an automatic `LeaseGuard`.
///
/// Implements `TunnelStream` directly so callers can pass it straight into pumps or
/// unwrap it via `into_parts()` when transferring ownership across stages.
pub struct StreamLease {
    stream: Option<Box<dyn TunnelStream>>,
    guard: Option<LeaseGuard>,
}

impl StreamLease {
    pub(crate) fn new(
        stream: Box<dyn TunnelStream>,
        active_count: Arc<AtomicUsize>,
        release_tx: mpsc::UnboundedSender<PoolReleaseSignal>,
    ) -> Self {
        Self {
            stream: Some(stream),
            guard: Some(LeaseGuard::new(active_count, release_tx)),
        }
    }

    /// Separates the inner stream from its `LeaseGuard`.
    pub fn into_parts(mut self) -> Option<(Box<dyn TunnelStream>, LeaseGuard)> {
        let stream = self.stream.take()?;
        let guard = self.guard.take()?;
        Some((stream, guard))
    }

    /// Consumes the lease and returns the raw stream when reclaiming an undelivered handoff.
    pub(crate) fn reclaim_stream(mut self) -> Option<Box<dyn TunnelStream>> {
        if let Some(guard) = self.guard.take() {
            guard.cancel_reclaim();
        }
        self.stream.take()
    }

    fn stream_mut(&mut self) -> io::Result<&mut Box<dyn TunnelStream>> {
        self.stream
            .as_mut()
            .ok_or_else(|| io::Error::other("stream lease already consumed"))
    }
}

impl AsyncRead for StreamLease {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.stream_mut() {
            Ok(stream) => Pin::new(&mut **stream).poll_read(cx, buf),
            Err(err) => Poll::Ready(Err(err)),
        }
    }
}

impl AsyncWrite for StreamLease {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.stream_mut() {
            Ok(stream) => Pin::new(&mut **stream).poll_write(cx, buf),
            Err(err) => Poll::Ready(Err(err)),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.stream_mut() {
            Ok(stream) => Pin::new(&mut **stream).poll_flush(cx),
            Err(err) => Poll::Ready(Err(err)),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.stream_mut() {
            Ok(stream) => Pin::new(&mut **stream).poll_shutdown(cx),
            Err(err) => Poll::Ready(Err(err)),
        }
    }
}

impl TunnelStream for StreamLease {
    fn caps(&self) -> StreamCaps {
        self.stream
            .as_ref()
            .map_or_else(StreamCaps::default, |s| s.caps())
    }

    fn poll_write_owned(&mut self, cx: &mut Context<'_>, chunk: Bytes) -> Poll<io::Result<usize>> {
        match self.stream_mut() {
            Ok(stream) => stream.poll_write_owned(cx, chunk),
            Err(err) => Poll::Ready(Err(err)),
        }
    }

    fn poll_send_ping(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.stream_mut() {
            Ok(stream) => stream.poll_send_ping(cx),
            Err(err) => Poll::Ready(Err(err)),
        }
    }

    fn last_activity_ms(&self) -> Option<u64> {
        self.stream.as_ref().and_then(|s| s.last_activity_ms())
    }

    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        match self.stream.as_mut() {
            Some(stream) => stream.poll_check_alive(cx),
            None => Poll::Ready(HeartbeatStatus::Dead),
        }
    }
}
