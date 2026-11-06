// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! HTTP/3 and QUIC tunnel stream adapters implementing `TunnelStream`.
//! All comments and docstrings must be in English.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::PollSender;
use tracing::debug;

use crate::consts::{
    DEFAULT_H3_FINISH_STREAM_TIMEOUT, DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD,
    H3_STREAM_CHUNK_QUEUE_SIZE,
};
use crate::core::stream::{HeartbeatStatus, TunnelStream};

/// Raw QUIC bidirectional stream adapter directly polling `quinn::SendStream` and `quinn::RecvStream`.
pub struct QuinnTunnelStream {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    conn: Option<quinn::Connection>,
    liveness: Option<QuicLivenessTracker>,
    shutdown_done: bool,
}

impl QuinnTunnelStream {
    /// Creates a new `QuinnTunnelStream` from split Quinn send and receive halves.
    pub fn new(send: quinn::SendStream, recv: quinn::RecvStream) -> Self {
        Self {
            send,
            recv,
            conn: None,
            liveness: None,
            shutdown_done: false,
        }
    }

    /// Associates the underlying Quinn connection for accurate liveness detection.
    pub fn with_connection(mut self, conn: quinn::Connection) -> Self {
        self.liveness = Some(QuicLivenessTracker::new(&conn));
        self.conn = Some(conn);
        self
    }
}

impl AsyncRead for QuinnTunnelStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuinnTunnelStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(Pin::new(&mut self.send), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(Pin::new(&mut self.send), cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.shutdown_done {
            return Poll::Ready(Ok(()));
        }
        match tokio::io::AsyncWrite::poll_shutdown(Pin::new(&mut self.send), cx) {
            Poll::Ready(Ok(())) => {
                self.shutdown_done = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::NotConnected => {
                self.shutdown_done = true;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

#[derive(Debug, Clone)]
struct QuicLivenessTracker {
    last_rx_dgrams: u64,
    last_tx_dgrams: u64,
    last_progress_at: std::time::Instant,
}

impl QuicLivenessTracker {
    fn new(conn: &quinn::Connection) -> Self {
        let stats = conn.stats();
        Self {
            last_rx_dgrams: stats.udp_rx.datagrams,
            last_tx_dgrams: stats.udp_tx.datagrams,
            last_progress_at: std::time::Instant::now(),
        }
    }

    /// Evaluates connection responsiveness based on UDP datagram progression.
    /// If UDP packets are being sent (e.g. keep-alive pings) but zero UDP packets are received
    /// for longer than unresponsive_threshold, the connection is deemed dead.
    fn check_liveness(
        &mut self,
        conn: &quinn::Connection,
        unresponsive_threshold: std::time::Duration,
    ) -> bool {
        let stats = conn.stats();
        let cur_rx = stats.udp_rx.datagrams;
        let cur_tx = stats.udp_tx.datagrams;

        if cur_rx > self.last_rx_dgrams {
            self.last_rx_dgrams = cur_rx;
            self.last_tx_dgrams = cur_tx;
            self.last_progress_at = std::time::Instant::now();
            return true;
        }

        // Transmitting packets while zero packets received for longer than threshold -> zombie!
        if cur_tx > self.last_tx_dgrams && self.last_progress_at.elapsed() >= unresponsive_threshold
        {
            return false;
        }

        true
    }
}

impl TunnelStream for QuinnTunnelStream {
    fn poll_send_ping(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(ref conn) = self.conn {
            if let Some(reason) = conn.close_reason() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    format!("Underlying QUIC connection closed: {:?}", reason),
                )));
            }
            if let Some(ref mut tracker) = self.liveness
                && !tracker.check_liveness(conn, DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD)
            {
                conn.close(quinn::VarInt::from_u32(0), b"liveness ping timeout");
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Underlying QUIC connection unresponsive to pings",
                )));
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_check_alive(&mut self, _cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        if let Some(ref conn) = self.conn {
            if conn.close_reason().is_some() {
                return Poll::Ready(HeartbeatStatus::Dead);
            }
            if let Some(ref mut tracker) = self.liveness
                && !tracker.check_liveness(conn, DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD)
            {
                conn.close(quinn::VarInt::from_u32(0), b"liveness check timeout");
                return Poll::Ready(HeartbeatStatus::Dead);
            }
        }
        Poll::Ready(HeartbeatStatus::Alive)
    }
}

/// Bidirectional stream adapter for HTTP/3 request streams (`h3::client` or `h3::server`).
pub struct H3TunnelStream {
    tx_outbound: Option<PollSender<Bytes>>,
    rx_inbound: mpsc::Receiver<io::Result<Bytes>>,
    read_buf: Option<Bytes>,
    shutdown_rx: Option<oneshot::Receiver<io::Result<()>>>,
    conn: Option<quinn::Connection>,
    liveness: Option<QuicLivenessTracker>,
    shutdown_done: bool,
    fin_sent: bool,
}

impl H3TunnelStream {
    /// Constructs an `H3TunnelStream` from a client-side HTTP/3 request stream.
    pub fn from_client(
        stream: h3::client::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    ) -> Self {
        let (send, recv) = stream.split();
        Self::spawn_split_pump(send, recv)
    }

    /// Constructs an `H3TunnelStream` from a server-side HTTP/3 request stream.
    pub fn from_server(
        stream: h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    ) -> Self {
        let (send, recv) = stream.split();
        Self::spawn_split_pump(send, recv)
    }

    /// Associates the underlying Quinn connection for accurate liveness detection.
    pub fn with_connection(mut self, conn: quinn::Connection) -> Self {
        self.liveness = Some(QuicLivenessTracker::new(&conn));
        self.conn = Some(conn);
        self
    }

    fn spawn_split_pump<S, R>(mut send_stream: S, mut recv_stream: R) -> Self
    where
        S: H3SendStream,
        R: H3RecvStream,
    {
        let (tx_outbound, mut rx_outbound) = mpsc::channel::<Bytes>(H3_STREAM_CHUNK_QUEUE_SIZE);
        let (tx_inbound, rx_inbound) =
            mpsc::channel::<io::Result<Bytes>>(H3_STREAM_CHUNK_QUEUE_SIZE);
        let (tx_shutdown, rx_shutdown) = oneshot::channel::<io::Result<()>>();

        // Outbound send task: continuously drains outgoing chunks without being blocked by incoming reading
        tokio::spawn(async move {
            let mut result = Ok(());
            while let Some(chunk) = rx_outbound.recv().await {
                if let Err(e) = send_stream.send_chunk(chunk).await {
                    debug!("H3 send_chunk failed: {:?}", e);
                    result = Err(e);
                    break;
                }
            }
            if result.is_ok() {
                match tokio::time::timeout(
                    DEFAULT_H3_FINISH_STREAM_TIMEOUT,
                    send_stream.finish_stream(),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        debug!("H3 finish_stream failed: {:?}", e);
                        result = Err(e);
                    }
                    Err(_) => {
                        debug!("H3 finish_stream timed out");
                        result = Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "H3 finish_stream timed out",
                        ));
                    }
                }
            }
            let _ = tx_shutdown.send(result);
        });

        // Inbound receive task: continuously receives incoming chunks without being blocked by outgoing writes
        tokio::spawn(async move {
            loop {
                match recv_stream.recv_chunk().await {
                    Ok(Some(data)) => {
                        if tx_inbound.send(Ok(data)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break, // EOF reached cleanly
                    Err(e) => {
                        debug!("H3 recv_chunk failed: {:?}", e);
                        let _ = tx_inbound.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        Self {
            tx_outbound: Some(PollSender::new(tx_outbound)),
            rx_inbound,
            read_buf: None,
            shutdown_rx: Some(rx_shutdown),
            conn: None,
            liveness: None,
            shutdown_done: false,
            fin_sent: false,
        }
    }
}

impl AsyncRead for H3TunnelStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(mut current) = self.read_buf.take() {
            let to_copy = std::cmp::min(buf.remaining(), current.len());
            buf.put_slice(&current[..to_copy]);
            current.advance(to_copy);
            if current.has_remaining() {
                self.read_buf = Some(current);
            }
            return Poll::Ready(Ok(()));
        }

        match self.rx_inbound.poll_recv(cx) {
            Poll::Ready(Some(Ok(mut bytes))) => {
                let to_copy = std::cmp::min(buf.remaining(), bytes.len());
                buf.put_slice(&bytes[..to_copy]);
                bytes.advance(to_copy);
                if bytes.has_remaining() {
                    self.read_buf = Some(bytes);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(None) => Poll::Ready(Ok(())), // EOF
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for H3TunnelStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.fin_sent {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "HTTP/3 stream write direction closed",
            )));
        }

        let this = self.get_mut();
        let tx = match this.tx_outbound.as_mut() {
            Some(t) => t,
            None => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "HTTP/3 stream write direction closed",
                )));
            }
        };

        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        // Use poll_reserve to properly register async waker instead of spinning in wake_by_ref
        match tx.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let chunk = Bytes::copy_from_slice(buf);
                let len = chunk.len();
                let _ = tx.send_item(chunk);
                Poll::Ready(Ok(len))
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "HTTP/3 background pump closed",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.shutdown_done {
            return Poll::Ready(Ok(()));
        }

        if let Some(ref conn) = self.conn
            && let Some(reason) = conn.close_reason()
        {
            self.shutdown_done = true;
            self.shutdown_rx.take();
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                format!("Underlying QUIC connection closed: {:?}", reason),
            )));
        }

        if self.tx_outbound.is_some() {
            self.fin_sent = true;
            // Dropping tx_outbound allows rx_outbound to drain completely before yielding None
            self.tx_outbound.take();
        }

        if let Some(ref mut rx) = self.shutdown_rx {
            match Future::poll(Pin::new(rx), cx) {
                Poll::Ready(Ok(Ok(()))) => {
                    self.shutdown_done = true;
                    self.shutdown_rx.take();
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Ok(Err(e))) => {
                    self.shutdown_done = true;
                    self.shutdown_rx.take();
                    Poll::Ready(Err(e))
                }
                Poll::Ready(Err(_)) => {
                    self.shutdown_done = true;
                    self.shutdown_rx.take();
                    Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "HTTP/3 background pump task terminated",
                    )))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            self.shutdown_done = true;
            Poll::Ready(Ok(()))
        }
    }
}

impl TunnelStream for H3TunnelStream {
    fn poll_send_ping(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(ref conn) = self.conn {
            if let Some(reason) = conn.close_reason() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    format!("Underlying QUIC connection closed: {:?}", reason),
                )));
            }
            if let Some(ref mut tracker) = self.liveness
                && !tracker.check_liveness(conn, std::time::Duration::from_secs(4))
            {
                conn.close(quinn::VarInt::from_u32(0), b"liveness ping timeout");
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Underlying QUIC connection unresponsive to pings",
                )));
            }
        }
        if let Poll::Ready(HeartbeatStatus::Dead) = self.poll_check_alive(cx) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "HTTP/3 stream closed or in error state",
            )));
        }
        Poll::Ready(Ok(()))
    }

    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        if let Some(ref conn) = self.conn {
            if conn.close_reason().is_some() {
                return Poll::Ready(HeartbeatStatus::Dead);
            }
            if let Some(ref mut tracker) = self.liveness
                && !tracker.check_liveness(conn, std::time::Duration::from_secs(4))
            {
                conn.close(quinn::VarInt::from_u32(0), b"liveness check timeout");
                return Poll::Ready(HeartbeatStatus::Dead);
            }
        }
        if self.read_buf.is_some() {
            return Poll::Ready(HeartbeatStatus::Alive);
        }
        match self.rx_inbound.poll_recv(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                self.read_buf = Some(bytes);
                Poll::Ready(HeartbeatStatus::Alive)
            }
            Poll::Ready(Some(Err(_))) | Poll::Ready(None) => Poll::Ready(HeartbeatStatus::Dead),
            Poll::Pending => Poll::Ready(HeartbeatStatus::Alive),
        }
    }
}

/// Trait for the independent send half of an HTTP/3 request stream.
pub trait H3SendStream: Send + 'static {
    fn send_chunk(
        &mut self,
        chunk: Bytes,
    ) -> impl std::future::Future<Output = Result<(), io::Error>> + Send;
    fn finish_stream(&mut self) -> impl std::future::Future<Output = Result<(), io::Error>> + Send;
}

/// Trait for the independent receive half of an HTTP/3 request stream.
pub trait H3RecvStream: Send + 'static {
    fn recv_chunk(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<Bytes>, io::Error>> + Send;
}

impl H3SendStream for h3::client::RequestStream<h3_quinn::SendStream<Bytes>, Bytes> {
    async fn send_chunk(&mut self, chunk: Bytes) -> Result<(), io::Error> {
        self.send_data(chunk)
            .await
            .map_err(|e| io::Error::other(e.to_string()))
    }

    async fn finish_stream(&mut self) -> Result<(), io::Error> {
        self.finish()
            .await
            .map_err(|e| io::Error::other(e.to_string()))
    }
}

impl H3RecvStream for h3::client::RequestStream<h3_quinn::RecvStream, Bytes> {
    async fn recv_chunk(&mut self) -> Result<Option<Bytes>, io::Error> {
        match self.recv_data().await {
            Ok(Some(mut buf)) => Ok(Some(buf.copy_to_bytes(buf.remaining()))),
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::other(e.to_string())),
        }
    }
}

impl H3SendStream for h3::server::RequestStream<h3_quinn::SendStream<Bytes>, Bytes> {
    async fn send_chunk(&mut self, chunk: Bytes) -> Result<(), io::Error> {
        self.send_data(chunk)
            .await
            .map_err(|e| io::Error::other(e.to_string()))
    }

    async fn finish_stream(&mut self) -> Result<(), io::Error> {
        self.finish()
            .await
            .map_err(|e| io::Error::other(e.to_string()))
    }
}

impl H3RecvStream for h3::server::RequestStream<h3_quinn::RecvStream, Bytes> {
    async fn recv_chunk(&mut self) -> Result<Option<Bytes>, io::Error> {
        match self.recv_data().await {
            Ok(Some(mut buf)) => Ok(Some(buf.copy_to_bytes(buf.remaining()))),
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::other(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    struct MockSendStream {
        #[allow(dead_code)]
        sent: Vec<Bytes>,
        #[allow(dead_code)]
        finished: bool,
    }

    impl H3SendStream for MockSendStream {
        async fn send_chunk(&mut self, chunk: Bytes) -> Result<(), io::Error> {
            self.sent.push(chunk);
            Ok(())
        }
        async fn finish_stream(&mut self) -> Result<(), io::Error> {
            self.finished = true;
            Ok(())
        }
    }

    struct MockFailingRecvStream {
        sent_item: bool,
    }

    impl H3RecvStream for MockFailingRecvStream {
        async fn recv_chunk(&mut self) -> Result<Option<Bytes>, io::Error> {
            if !self.sent_item {
                self.sent_item = true;
                Ok(Some(Bytes::from_static(b"hello before error")))
            } else {
                Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "simulated h3 reset",
                ))
            }
        }
    }

    #[tokio::test]
    async fn test_h3_stream_error_propagation_not_swallowed_as_eof() {
        let send = MockSendStream {
            sent: vec![],
            finished: false,
        };
        let recv = MockFailingRecvStream { sent_item: false };
        let mut stream = H3TunnelStream::spawn_split_pump(send, recv);

        let mut buf = vec![0u8; 18];
        let n = stream.read(&mut buf).await.expect("initial read succeeds");
        assert_eq!(&buf[..n], b"hello before error");

        // Next read MUST return Err(ConnectionReset), NOT Ok(0) (which would be masked EOF)
        let mut err_buf = [0u8; 10];
        let res = stream.read(&mut err_buf).await;
        assert!(
            res.is_err(),
            "Error must be propagated to caller, not masked as EOF"
        );
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::ConnectionReset);

        // Liveness check must also report Dead
        let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
        assert_eq!(
            stream.poll_check_alive(&mut cx),
            Poll::Ready(HeartbeatStatus::Dead)
        );
    }
}
