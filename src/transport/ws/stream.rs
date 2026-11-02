//! WebSocket adapter exposing `TunnelStream` (`AsyncRead + AsyncWrite`).
//! All comments and docstrings must be in English.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use fastwebsockets::{Frame, OpCode, Payload, Role, WebSocket};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio_util::sync::CancellationToken;

use crate::constants::RELAY_BUFFER_SIZE;

/// Dynamic tunnel stream adapter wrapping a `fastwebsockets::WebSocket`.
pub struct WsTunnelStream {
    inner: DuplexStream,
    cancel_token: CancellationToken,
}

impl WsTunnelStream {
    /// Constructs a `WsTunnelStream` from an upgraded underlying I/O stream.
    pub fn new<S>(stream: S, role: Role) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let mut ws = WebSocket::after_handshake(stream, role);
        ws.set_writev(true);
        ws.set_auto_close(true);
        ws.set_auto_pong(true);

        let (app_stream, bridge_stream) = tokio::io::duplex(RELAY_BUFFER_SIZE);
        let cancel_token = CancellationToken::new();
        let loop_token = cancel_token.clone();

        // Split websocket and bridge stream into independent halves
        let (mut ws_rx, mut ws_tx) = ws.split(tokio::io::split);
        let (mut bridge_rx, mut bridge_tx) = tokio::io::split(bridge_stream);

        // Spawn read pump: reads binary WebSocket frames and writes into duplex bridge
        let read_token = loop_token.clone();
        tokio::spawn(async move {
            let mut send_pong = |_| async { Ok::<(), std::io::Error>(()) };
            loop {
                tokio::select! {
                    _ = read_token.cancelled() => break,
                    frame_res = ws_rx.read_frame(&mut send_pong) => {
                        match frame_res {
                            Ok(frame) => match frame.opcode {
                                OpCode::Binary => {
                                    if bridge_tx.write_all(&frame.payload).await.is_err() {
                                        break;
                                    }
                                }
                                OpCode::Close => {
                                    let _ = bridge_tx.shutdown().await;
                                    break;
                                }
                                _ => {}
                            },
                            Err(_) => break,
                        }
                    }
                }
            }
            let _ = bridge_tx.shutdown().await;
        });

        // Spawn write pump: reads raw application bytes from duplex bridge and writes binary frames
        let write_token = loop_token.clone();
        tokio::spawn(async move {
            let mut buffer = vec![0u8; RELAY_BUFFER_SIZE];
            loop {
                tokio::select! {
                    _ = write_token.cancelled() => break,
                    read_res = bridge_rx.read(&mut buffer) => {
                        match read_res {
                            Ok(0) => {
                                // EOF from application
                                let _ = ws_tx.write_frame(Frame::close(1000, &[])).await;
                                break;
                            }
                            Ok(n) => {
                                let frame = Frame::binary(Payload::Borrowed(&buffer[..n]));
                                if ws_tx.write_frame(frame).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        Self {
            inner: app_stream,
            cancel_token,
        }
    }
}

impl Drop for WsTunnelStream {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

impl AsyncRead for WsTunnelStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for WsTunnelStream {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_ws_tunnel_stream_bidirectional() {
        let (raw_client, raw_server) = duplex(65536);

        let mut client = WsTunnelStream::new(raw_client, Role::Client);
        let mut server = WsTunnelStream::new(raw_server, Role::Server);

        let msg = b"hello micro-tunnel over fastwebsockets!";
        client.write_all(msg).await.unwrap();
        client.flush().await.unwrap();

        let mut buf = vec![0u8; msg.len()];
        server.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, msg);

        // Echo response in reverse direction
        let reply = b"pong reply from server";
        server.write_all(reply).await.unwrap();
        server.flush().await.unwrap();

        let mut reply_buf = vec![0u8; reply.len()];
        client.read_exact(&mut reply_buf).await.unwrap();
        assert_eq!(&reply_buf, reply);
    }
}
