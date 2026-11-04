//! Core stream abstractions and type-erased dynamic trait objects.
//! All comments and docstrings must be in English.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Type alias for heap-pinned, sendable future without `async_trait` macro dependency.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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
/// Implementations must be `Send + Sync + Unpin + 'static` to allow thread-safe dispatching
/// and storage across Tokio task boundaries as `Box<dyn TunnelStream>`.
pub trait TunnelStream: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {
    /// Non-blocking hook to transmit a protocol-level ping/heartbeat frame.
    ///
    /// Transports without protocol-level ping (e.g. raw TCP) default to `Poll::Ready(Ok(()))`.
    fn poll_send_ping(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// Non-blocking hook to inspect stream liveness (non-destructive read/peek).
    ///
    /// Returns `Poll::Ready(HeartbeatStatus::Dead)` if EOF, RST, or broken pipe is detected.
    /// Returns `Poll::Ready(HeartbeatStatus::Alive)` or `Poll::Pending` if still healthy.
    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus>;

    /// Indicates whether the transport stream supports true half-close (closing write direction
    /// while keeping read direction open, e.g. TCP FIN or QUIC stream FIN).
    /// Transports that do not support half-close (e.g. WebSocket RFC 6455) must return false.
    fn supports_half_close(&self) -> bool {
        true
    }
}

// Implement TunnelStream for Box<dyn TunnelStream> to allow trait-object wrapping.
impl TunnelStream for Box<dyn TunnelStream> {
    fn poll_send_ping(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (**self).poll_send_ping(cx)
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

// Implement TunnelStream for tokio_rustls client TLS stream
impl<S> TunnelStream for tokio_rustls::client::TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        // Read into an empty buffer to check if EOF or error is immediately pending
        let mut empty = [0u8; 0];
        let mut buf = ReadBuf::new(&mut empty);
        match Pin::new(self).poll_read(cx, &mut buf) {
            Poll::Ready(Err(_)) => Poll::Ready(HeartbeatStatus::Dead),
            Poll::Ready(Ok(())) => Poll::Ready(HeartbeatStatus::Alive),
            Poll::Pending => Poll::Ready(HeartbeatStatus::Alive),
        }
    }
}

// Implement TunnelStream for tokio_rustls server TLS stream
impl<S> TunnelStream for tokio_rustls::server::TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
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
