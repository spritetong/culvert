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
/// Implementations must be `Send + Unpin + 'static` to allow thread-safe dispatching
/// and storage across Tokio task boundaries as `Box<dyn TunnelStream>`.
pub trait TunnelStream: AsyncRead + AsyncWrite + Send + Unpin + 'static {
    /// Writes an owned byte slice without copying if supported by the transport.
    /// Default implementation falls back to `poll_write`.
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
    fn poll_write_bytes(&mut self, cx: &mut Context<'_>, buf: bytes::Bytes) -> Poll<io::Result<usize>> {
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

// Implement TunnelStream for tokio_rustls client TLS stream
impl<S> TunnelStream for tokio_rustls::client::TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        let mut b = [0u8; 1];
        let mut buf = ReadBuf::new(&mut b);
        match Pin::new(self).poll_read(cx, &mut buf) {
            Poll::Ready(Ok(())) if buf.filled().is_empty() => Poll::Ready(HeartbeatStatus::Dead),
            Poll::Ready(Ok(())) => Poll::Ready(HeartbeatStatus::Alive),
            Poll::Ready(Err(_)) => Poll::Ready(HeartbeatStatus::Dead),
            Poll::Pending => Poll::Ready(HeartbeatStatus::Alive),
        }
    }
}

// Implement TunnelStream for tokio_rustls server TLS stream
impl<S> TunnelStream for tokio_rustls::server::TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        let mut b = [0u8; 1];
        let mut buf = ReadBuf::new(&mut b);
        match Pin::new(self).poll_read(cx, &mut buf) {
            Poll::Ready(Ok(())) if buf.filled().is_empty() => Poll::Ready(HeartbeatStatus::Dead),
            Poll::Ready(Ok(())) => Poll::Ready(HeartbeatStatus::Alive),
            Poll::Ready(Err(_)) => Poll::Ready(HeartbeatStatus::Dead),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

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
}
