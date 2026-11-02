//! HTTP/3 and QUIC tunnel stream adapters implementing `TunnelStream`.
//! All comments and docstrings must be in English.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tracing::debug;

/// Raw QUIC bidirectional stream adapter directly polling `quinn::SendStream` and `quinn::RecvStream`.
pub struct QuinnTunnelStream {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

impl QuinnTunnelStream {
    /// Creates a new `QuinnTunnelStream` from split Quinn send and receive halves.
    pub fn new(send: quinn::SendStream, recv: quinn::RecvStream) -> Self {
        Self { send, recv }
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
        tokio::io::AsyncWrite::poll_shutdown(Pin::new(&mut self.send), cx)
    }
}

// Blanket impl provides TunnelStream for QuinnTunnelStream

/// Bidirectional stream adapter for HTTP/3 request streams (`h3::client` or `h3::server`).
pub struct H3TunnelStream {
    tx_outbound: Option<mpsc::Sender<Bytes>>,
    rx_inbound: mpsc::Receiver<Bytes>,
    read_buf: Option<Bytes>,
    fin_sent: bool,
}

impl H3TunnelStream {
    /// Constructs an `H3TunnelStream` from a client-side HTTP/3 request stream.
    pub fn from_client(
        stream: h3::client::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    ) -> Self {
        Self::spawn_pump(stream)
    }

    /// Constructs an `H3TunnelStream` from a server-side HTTP/3 request stream.
    pub fn from_server(
        stream: h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    ) -> Self {
        Self::spawn_pump(stream)
    }

    fn spawn_pump<S>(mut stream: S) -> Self
    where
        S: H3BidirectionalStream + Send + 'static,
    {
        let (tx_outbound, mut rx_outbound) = mpsc::channel::<Bytes>(64);
        let (tx_inbound, rx_inbound) = mpsc::channel::<Bytes>(64);
        let mut tx_inbound_opt = Some(tx_inbound);

        tokio::spawn(async move {
            let mut read_closed = false;
            let mut write_closed = false;

            while !read_closed || !write_closed {
                tokio::select! {
                    biased;

                    chunk_opt = rx_outbound.recv(), if !write_closed => {
                        match chunk_opt {
                            Some(chunk) => {
                                if let Err(e) = stream.send_chunk(chunk).await {
                                    debug!("H3 send_chunk failed: {:?}", e);
                                    break;
                                }
                            }
                            None => {
                                write_closed = true;
                                if let Err(e) = stream.finish_stream().await {
                                    debug!("H3 finish_stream failed: {:?}", e);
                                    break;
                                }
                            }
                        }
                    }

                    res = stream.recv_chunk(), if !read_closed => {
                        match res {
                            Ok(Some(data)) => {
                                if let Some(ref tx) = tx_inbound_opt {
                                    if tx.send(data).await.is_err() {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                            Ok(None) => {
                                // Peer closed write direction (FIN), half-close read and signal EOF
                                read_closed = true;
                                tx_inbound_opt.take();
                            }
                            Err(e) => {
                                debug!("H3 recv_chunk failed: {:?}", e);
                                tx_inbound_opt.take();
                                break;
                            }
                        }
                    }
                }
            }
        });

        Self {
            tx_outbound: Some(tx_outbound),
            rx_inbound,
            read_buf: None,
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
            Poll::Ready(Some(mut bytes)) => {
                let to_copy = std::cmp::min(buf.remaining(), bytes.len());
                buf.put_slice(&bytes[..to_copy]);
                bytes.advance(to_copy);
                if bytes.has_remaining() {
                    self.read_buf = Some(bytes);
                }
                Poll::Ready(Ok(()))
            }
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

        let tx = match self.tx_outbound.as_ref() {
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

        let chunk = Bytes::copy_from_slice(buf);
        let len = chunk.len();
        match tx.try_send(chunk) {
            Ok(()) => Poll::Ready(Ok(len)),
            Err(mpsc::error::TrySendError::Full(_)) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "HTTP/3 background pump closed",
            ))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.fin_sent = true;
        self.tx_outbound.take();
        Poll::Ready(Ok(()))
    }
}

// Blanket impl provides TunnelStream for H3TunnelStream

/// Trait unifying `h3::client::RequestStream` and `h3::server::RequestStream`.
pub trait H3BidirectionalStream {
    fn send_chunk(
        &mut self,
        chunk: Bytes,
    ) -> impl std::future::Future<Output = Result<(), io::Error>> + Send;
    fn recv_chunk(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<Bytes>, io::Error>> + Send;
    fn finish_stream(&mut self) -> impl std::future::Future<Output = Result<(), io::Error>> + Send;
}

impl H3BidirectionalStream for h3::client::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes> {
    async fn send_chunk(&mut self, chunk: Bytes) -> Result<(), io::Error> {
        self.send_data(chunk)
            .await
            .map_err(|e| io::Error::other(e.to_string()))
    }

    async fn recv_chunk(&mut self) -> Result<Option<Bytes>, io::Error> {
        match self.recv_data().await {
            Ok(Some(mut buf)) => Ok(Some(buf.copy_to_bytes(buf.remaining()))),
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::other(e.to_string())),
        }
    }

    async fn finish_stream(&mut self) -> Result<(), io::Error> {
        self.finish()
            .await
            .map_err(|e| io::Error::other(e.to_string()))
    }
}

impl H3BidirectionalStream for h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes> {
    async fn send_chunk(&mut self, chunk: Bytes) -> Result<(), io::Error> {
        self.send_data(chunk)
            .await
            .map_err(|e| io::Error::other(e.to_string()))
    }

    async fn recv_chunk(&mut self) -> Result<Option<Bytes>, io::Error> {
        match self.recv_data().await {
            Ok(Some(mut buf)) => Ok(Some(buf.copy_to_bytes(buf.remaining()))),
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::other(e.to_string())),
        }
    }

    async fn finish_stream(&mut self) -> Result<(), io::Error> {
        self.finish()
            .await
            .map_err(|e| io::Error::other(e.to_string()))
    }
}
