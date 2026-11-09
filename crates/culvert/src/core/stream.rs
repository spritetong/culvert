// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Core stream abstractions and type-erased dynamic trait objects.
//! All comments and docstrings must be in English.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Type alias for heap-pinned, sendable future without `async_trait` macro dependency.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

static ACTIVITY_ORIGIN: OnceLock<Instant> = OnceLock::new();

/// Monotonic millisecond clock shared by every stream activity tracker and heartbeat loop.
///
/// Uses `Instant` rather than wall-clock time so NTP adjustments cannot move a
/// connection's liveness deadline backwards.
#[inline]
pub fn activity_now_ms() -> u64 {
    ACTIVITY_ORIGIN
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis() as u64
}

/// Heartbeat decision for one stream at one tick.
///
/// Returns `(send_probe, expired)`. A transport without an activity clock keeps
/// its old behaviour: probes are still driven (for local liveness checks) but no
/// idle deadline is applied by the caller.
#[inline]
pub fn heartbeat_timing(
    activity: Option<u64>,
    now_ms: u64,
    heartbeat_interval: Duration,
    idle_timeout: Duration,
) -> (bool, bool) {
    match activity {
        None => (true, false),
        Some(last) => {
            let elapsed = now_ms.saturating_sub(last);
            let hb_ms = heartbeat_interval.as_millis().max(1) as u64;
            let idle_ms = idle_timeout.as_millis().max(1) as u64;
            (elapsed >= hb_ms, elapsed >= idle_ms)
        }
    }
}

/// Connection heartbeat and liveness status reported by idle streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatStatus {
    /// The stream is verified alive and healthy.
    Alive,
    /// The stream has detected EOF, RST, timeout, or underlying physical connection failure.
    Dead,
}

/// Unified dynamic tunnel stream trait combining non-blocking asynchronous reading,
/// writing, and protocol-level active liveness verification.
///
/// Implementations must be `Send + Unpin + 'static` to allow thread-safe dispatching
/// and storage across Tokio task boundaries as `Box<dyn TunnelStream>`.
pub trait TunnelStream: AsyncRead + AsyncWrite + Send + Unpin + 'static {
    /// Returns true if this transport stream benefits from owned `Bytes` buffers
    /// and implements true zero-copy processing internally (e.g. MuxStream).
    /// Defaults to false for standard byte-oriented streams (TCP, TLS, WS, QUIC, H3).
    fn consumes_owned_write(&self) -> bool {
        false
    }

    /// Writes an owned byte slice without copying if supported by the transport.
    /// The default implementation is a compatibility fallback only: it passes the
    /// borrowed slice to `poll_write`, which may copy. Implementations that can
    /// retain the refcounted buffer must override this.
    fn poll_write_bytes(
        &mut self,
        cx: &mut Context<'_>,
        buf: bytes::Bytes,
    ) -> Poll<io::Result<usize>> {
        Pin::new(self).poll_write(cx, &buf)
    }

    /// Non-blocking hook to transmit a protocol-level ping/heartbeat frame.
    ///
    /// Transports without protocol-level ping (e.g. raw TCP) default to `Poll::Ready(Ok(()))`.
    /// A probe is only a means of provoking inbound traffic: it must never be
    /// recorded as outbound activity.
    fn poll_send_ping(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// Timestamp of the last inbound frame or real outbound frame, in the
    /// [`activity_now_ms`] clock. Any RX counts, including Ping/Pong/control
    /// frames; keepalive probes must not count as TX.
    ///
    /// `None` means this transport has no application-level activity clock and
    /// callers must rely on [`TunnelStream::poll_check_alive`] alone.
    fn last_activity_ms(&self) -> Option<u64> {
        None
    }

    /// Non-blocking hook to inspect stream liveness (non-destructive read/peek).
    ///
    /// Returns `Poll::Ready(HeartbeatStatus::Dead)` if EOF, RST, or broken pipe is detected.
    /// Returns `Poll::Ready(HeartbeatStatus::Alive)` or `Poll::Pending` if still healthy.
    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus>;

    /// Indicates whether the transport stream supports half-close (closing the write
    /// direction while keeping the read direction open, e.g. TCP/QUIC FIN).
    ///
    /// Every in-tree transport implements a FIN: TCP/QUIC use the transport FIN,
    /// Mux uses a FIN frame, and WebSocket uses an in-band zero-length binary FIN
    /// understood by both culvert peers. A transport that can only tear down
    /// the whole connection must override this to return false.
    fn supports_half_close(&self) -> bool {
        true
    }
}

// Implement TunnelStream for Box<dyn TunnelStream> to allow trait-object wrapping.
impl TunnelStream for Box<dyn TunnelStream> {
    fn consumes_owned_write(&self) -> bool {
        (**self).consumes_owned_write()
    }

    fn poll_write_bytes(
        &mut self,
        cx: &mut Context<'_>,
        buf: bytes::Bytes,
    ) -> Poll<io::Result<usize>> {
        (**self).poll_write_bytes(cx, buf)
    }

    fn poll_send_ping(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (**self).poll_send_ping(cx)
    }

    fn last_activity_ms(&self) -> Option<u64> {
        (**self).last_activity_ms()
    }

    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        (**self).poll_check_alive(cx)
    }

    fn supports_half_close(&self) -> bool {
        (**self).supports_half_close()
    }
}

// Implement TunnelStream for tokio::net::TcpStream using poll_peek to detect EOF/RST without consuming bytes.
impl TunnelStream for tokio::net::TcpStream {
    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        // Catch OS-level asynchronous connection failures (e.g. RST) that a
        // later peek may not surface. Only terminal socket errors count: a
        // failed getsockopt or a transient status such as WouldBlock is not
        // evidence that the peer is gone, and a false Dead aborts a live relay.
        // Everything else falls through to `poll_peek`, which remains the
        // authoritative EOF/error check.
        if let Ok(Some(err)) = self.take_error()
            && matches!(
                err.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::ConnectionRefused
                    | io::ErrorKind::TimedOut
            )
        {
            return Poll::Ready(HeartbeatStatus::Dead);
        }

        let mut b = [0u8; 1];
        let mut buf = ReadBuf::new(&mut b);
        match self.poll_peek(cx, &mut buf) {
            Poll::Ready(Ok(0)) => Poll::Ready(HeartbeatStatus::Dead),
            Poll::Ready(Err(_)) => Poll::Ready(HeartbeatStatus::Dead),
            Poll::Ready(Ok(_)) => Poll::Ready(HeartbeatStatus::Alive),
            Poll::Pending => Poll::Ready(HeartbeatStatus::Alive),
        }
    }
}

/// Non-destructive buffered wrapper for TLS or arbitrary async I/O streams.
///
/// Buffers peeked single bytes during `poll_check_alive` liveness checks into
/// a pushback cache so that heartbeat inspections never destructively drop stream data.
pub struct BufferedTlsStream<S> {
    inner: S,
    pushback: Option<u8>,
}

impl<S> BufferedTlsStream<S> {
    /// Constructs a new `BufferedTlsStream` wrapping the inner stream.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            pushback: None,
        }
    }

    /// Returns a shared reference to the inner stream.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Returns a mutable reference to the inner stream.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consumes the wrapper and returns the underlying stream.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for BufferedTlsStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // A zero-capacity ReadBuf is legal (e.g. liveness probes). `put_slice`
        // would panic, and taking the byte here would silently drop payload.
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if let Some(b) = self.pushback.take() {
            buf.put_slice(&[b]);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for BufferedTlsStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<S> TunnelStream for BufferedTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    fn poll_write_bytes(
        &mut self,
        cx: &mut Context<'_>,
        buf: bytes::Bytes,
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, &buf)
    }

    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        if self.pushback.is_some() {
            return Poll::Ready(HeartbeatStatus::Alive);
        }
        let mut b = [0u8; 1];
        let mut buf = ReadBuf::new(&mut b);
        match Pin::new(&mut self.inner).poll_read(cx, &mut buf) {
            Poll::Ready(Ok(())) if buf.filled().is_empty() => Poll::Ready(HeartbeatStatus::Dead),
            Poll::Ready(Ok(())) => {
                // Buffer the read byte so subsequent poll_read re-emits it non-destructively
                self.pushback = Some(b[0]);
                Poll::Ready(HeartbeatStatus::Alive)
            }
            Poll::Ready(Err(_)) => Poll::Ready(HeartbeatStatus::Dead),
            Poll::Pending => Poll::Ready(HeartbeatStatus::Alive),
        }
    }
}

// Note: Direct `TunnelStream` implementation for raw `tokio_rustls::TlsStream` was removed
// to prevent destructive 1-byte read probes. Raw TLS streams must always be wrapped with
// `BufferedTlsStream` which supports non-destructive pushback.

// Implement TunnelStream for tokio::io::DuplexStream for in-memory unit tests
impl TunnelStream for tokio::io::DuplexStream {
    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        let mut empty = [0u8; 0];
        let mut buf = ReadBuf::new(&mut empty);
        match Pin::new(self).poll_read(cx, &mut buf) {
            Poll::Ready(Err(_)) => Poll::Ready(HeartbeatStatus::Dead),
            Poll::Ready(Ok(())) => Poll::Ready(HeartbeatStatus::Alive),
            Poll::Pending => Poll::Ready(HeartbeatStatus::Alive),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    #[test]
    fn test_heartbeat_timing_gates_probes_on_activity() {
        let now = 60_000u64;
        let hb = Duration::from_secs(5);
        let idle = Duration::from_secs(30);

        // Recent TX or RX: no probe, no deadline.
        assert_eq!(
            heartbeat_timing(Some(now - 1_000), now, hb, idle),
            (false, false)
        );
        // Genuinely idle: one probe, still alive.
        assert_eq!(
            heartbeat_timing(Some(now - 6_000), now, hb, idle),
            (true, false)
        );
        // Silent past the idle deadline: expire.
        assert_eq!(
            heartbeat_timing(Some(now - 31_000), now, hb, idle),
            (true, true)
        );
        // Transports without an activity clock keep driving local checks only.
        assert_eq!(heartbeat_timing(None, now, hb, idle), (true, false));
    }

    #[tokio::test]
    async fn test_buffered_tls_stream_non_destructive_check_alive() {
        let (mut client, server) = duplex(1024);
        let mut buffered = BufferedTlsStream::new(server);

        // Write "hello world" from client
        client.write_all(b"hello world").await.unwrap();

        // Perform heartbeat liveness check on buffered stream
        let status = poll_fn(|cx| buffered.poll_check_alive(cx)).await;
        assert_eq!(status, HeartbeatStatus::Alive);

        // Verify that the peeked first byte was not lost, and full message is read intact
        let mut buf = vec![0u8; 11];
        buffered.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello world");
    }

    #[tokio::test]
    async fn test_zero_capacity_read_preserves_pushback() {
        let (mut client, server) = duplex(64);
        let mut buffered = BufferedTlsStream::new(server);
        client.write_all(b"xy").await.unwrap();

        // Liveness check consumes one byte into the pushback cache.
        let status = poll_fn(|cx| buffered.poll_check_alive(cx)).await;
        assert_eq!(status, HeartbeatStatus::Alive);

        // A zero-capacity ReadBuf must be a no-op: no panic, no lost byte.
        let mut empty: [u8; 0] = [];
        let mut rb = ReadBuf::new(&mut empty);
        let res = poll_fn(|cx| Pin::new(&mut buffered).poll_read(cx, &mut rb)).await;
        assert!(res.is_ok());
        assert!(rb.filled().is_empty());

        let mut buf = [0u8; 2];
        buffered.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"xy");
    }
}
