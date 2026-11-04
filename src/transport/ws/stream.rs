//! WebSocket adapter exposing `TunnelStream` (`AsyncRead + AsyncWrite`).
//! All comments and docstrings must be in English.

use bytes::Bytes;
use fastwebsockets::{
    Frame, OpCode, Payload, Role, WebSocketError, WebSocketRead, WebSocketWrite,
    after_handshake_split,
};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::core::stream::{BoxFuture, HeartbeatStatus, TunnelStream};

type DynRead = Box<dyn AsyncRead + Send + Unpin>;
type DynWrite = Box<dyn AsyncWrite + Send + Unpin>;

enum ReadState {
    Idle(WebSocketRead<DynRead>),
    Reading(
        BoxFuture<
            'static,
            (
                WebSocketRead<DynRead>,
                Vec<Frame<'static>>,
                Result<(OpCode, Bytes), WebSocketError>,
            ),
        >,
    ),
    Buffered {
        rx: WebSocketRead<DynRead>,
        payload: Bytes,
        cursor: usize,
    },
    Eof,
    Failed,
}

enum WriteState {
    Idle(WebSocketWrite<DynWrite>),
    Writing {
        fut: BoxFuture<'static, (WebSocketWrite<DynWrite>, Result<(), WebSocketError>)>,
        written_len: usize,
    },
    WritingControl(BoxFuture<'static, (WebSocketWrite<DynWrite>, Result<(), WebSocketError>)>),
    Closing(BoxFuture<'static, (WebSocketWrite<DynWrite>, Result<(), WebSocketError>)>),
    Closed,
    Failed,
}

/// Dynamic tunnel stream adapter directly wrapping split `WebSocketRead` and `WebSocketWrite`.
///
/// Implements `AsyncRead` and `AsyncWrite` purely using state-machine transitions without
/// intermediate duplex pipes, background tasks, or multi-copy buffering.
pub struct WsTunnelStream {
    read_state: ReadState,
    write_state: WriteState,
    control_queue: std::collections::VecDeque<Frame<'static>>,
}

// Safety: WsTunnelStream exposes no methods taking `&self` that mutate state without
// synchronization. All reading and writing require `Pin<&mut Self>`.
unsafe impl Sync for WsTunnelStream {}

impl WsTunnelStream {
    /// Constructs a `WsTunnelStream` from an upgraded underlying I/O stream.
    pub fn new<S>(stream: S, role: Role) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (r, w) = tokio::io::split(stream);
        let (mut rx, mut tx) =
            after_handshake_split(Box::new(r) as DynRead, Box::new(w) as DynWrite, role);
        rx.set_auto_pong(true);
        rx.set_auto_close(false);
        tx.set_writev(true);

        Self {
            read_state: ReadState::Idle(rx),
            write_state: WriteState::Idle(tx),
            control_queue: std::collections::VecDeque::new(),
        }
    }

    /// Drives pending obligated control frames (Pong/Close) out to the underlying writer.
    fn poll_flush_control(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match std::mem::replace(&mut self.write_state, WriteState::Failed) {
                WriteState::Idle(mut tx) => {
                    if let Some(frame) = self.control_queue.pop_front() {
                        let fut = Box::pin(async move {
                            let res = tx.write_frame(frame).await;
                            (tx, res)
                        });
                        self.write_state = WriteState::WritingControl(fut);
                        continue;
                    } else {
                        self.write_state = WriteState::Idle(tx);
                        return Poll::Ready(Ok(()));
                    }
                }
                WriteState::WritingControl(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready((tx, Ok(()))) => {
                        self.write_state = WriteState::Idle(tx);
                        continue;
                    }
                    Poll::Ready((_tx, Err(e))) => {
                        self.write_state = WriteState::Failed;
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                    }
                    Poll::Pending => {
                        self.write_state = WriteState::WritingControl(fut);
                        return Poll::Pending;
                    }
                },
                other => {
                    self.write_state = other;
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

impl AsyncRead for WsTunnelStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Opportunistically flush any pending control frames
        let _ = self.poll_flush_control(cx);

        loop {
            match std::mem::replace(&mut self.read_state, ReadState::Failed) {
                ReadState::Buffered {
                    rx,
                    payload,
                    cursor,
                } => {
                    let remaining = payload.len() - cursor;
                    let to_read = remaining.min(buf.remaining());
                    buf.put_slice(&payload[cursor..cursor + to_read]);
                    let new_cursor = cursor + to_read;
                    if new_cursor < payload.len() {
                        self.read_state = ReadState::Buffered {
                            rx,
                            payload,
                            cursor: new_cursor,
                        };
                    } else {
                        self.read_state = ReadState::Idle(rx);
                    }
                    return Poll::Ready(Ok(()));
                }
                ReadState::Idle(mut rx) => {
                    let fut = Box::pin(async move {
                        let mut obligated = Vec::new();
                        let res = loop {
                            let mut send_fn = |f: Frame<'_>| {
                                let frame_to_queue = match f.opcode {
                                    OpCode::Pong => Frame::pong(Payload::Owned(f.payload.to_vec())),
                                    OpCode::Close => Frame::close_raw(Payload::Owned(f.payload.to_vec())),
                                    _ => Frame::new(f.fin, f.opcode, None, Payload::Owned(f.payload.to_vec())),
                                };
                                obligated.push(frame_to_queue);
                                async { Ok::<(), io::Error>(()) }
                            };
                            let frame = match rx.read_frame(&mut send_fn).await {
                                Ok(f) => f,
                                Err(e) => break Err(e),
                            };
                            match frame.opcode {
                                OpCode::Binary | OpCode::Text => {
                                    let payload = match frame.payload {
                                        Payload::Bytes(b) => b.freeze(),
                                        Payload::Owned(v) => Bytes::from(v),
                                        other => Bytes::copy_from_slice(&other),
                                    };
                                    break Ok((frame.opcode, payload));
                                }
                                OpCode::Close => {
                                    break Ok((OpCode::Close, Bytes::new()));
                                }
                                _ => {}
                            }
                        };
                        (rx, obligated, res)
                    });
                    self.read_state = ReadState::Reading(fut);
                }
                ReadState::Reading(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.read_state = ReadState::Reading(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready((_rx, obligated, Ok((OpCode::Close, _)))) => {
                        if !obligated.is_empty() {
                            self.control_queue.extend(obligated);
                            let _ = self.poll_flush_control(cx);
                        }
                        self.read_state = ReadState::Eof;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready((rx, obligated, Ok((_, payload)))) => {
                        if !obligated.is_empty() {
                            self.control_queue.extend(obligated);
                            let _ = self.poll_flush_control(cx);
                        }
                        if payload.is_empty() {
                            self.read_state = ReadState::Idle(rx);
                            continue;
                        }
                        self.read_state = ReadState::Buffered {
                            rx,
                            payload,
                            cursor: 0,
                        };
                        continue;
                    }
                    Poll::Ready((_rx, obligated, Err(e))) => {
                        if !obligated.is_empty() {
                            self.control_queue.extend(obligated);
                            let _ = self.poll_flush_control(cx);
                        }
                        self.read_state = ReadState::Failed;
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, e)));
                    }
                },
                ReadState::Eof => {
                    self.read_state = ReadState::Eof;
                    return Poll::Ready(Ok(()));
                }
                ReadState::Failed => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "WebSocket read half in failed state",
                    )));
                }
            }
        }
    }
}

impl AsyncWrite for WsTunnelStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Poll::Ready(Err(e)) = self.poll_flush_control(cx) {
            return Poll::Ready(Err(e));
        }

        loop {
            match std::mem::replace(&mut self.write_state, WriteState::Failed) {
                WriteState::Idle(mut tx) => {
                    let len = buf.len();
                    let payload = buf.to_vec();
                    let mut fut = Box::pin(async move {
                        let res = tx.write_frame(Frame::binary(Payload::Owned(payload))).await;
                        (tx, res)
                    });
                    match fut.as_mut().poll(cx) {
                        Poll::Ready((tx, Ok(()))) => {
                            self.write_state = WriteState::Idle(tx);
                            return Poll::Ready(Ok(len));
                        }
                        Poll::Ready((_tx, Err(e))) => {
                            self.write_state = WriteState::Failed;
                            return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                        }
                        Poll::Pending => {
                            self.write_state = WriteState::Writing { fut, written_len: len };
                            return Poll::Pending;
                        }
                    }
                }
                WriteState::Writing { mut fut, written_len } => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.write_state = WriteState::Writing { fut, written_len };
                        return Poll::Pending;
                    }
                    Poll::Ready((tx, Ok(()))) => {
                        self.write_state = WriteState::Idle(tx);
                        return Poll::Ready(Ok(written_len));
                    }
                    Poll::Ready((_tx, Err(e))) => {
                        self.write_state = WriteState::Failed;
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                    }
                },
                WriteState::WritingControl(fut) => {
                    self.write_state = WriteState::WritingControl(fut);
                    if let Poll::Ready(Err(e)) = self.poll_flush_control(cx) {
                        return Poll::Ready(Err(e));
                    }
                    return Poll::Pending;
                }
                WriteState::Closing(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.write_state = WriteState::Closing(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready((_tx, _)) => {
                        self.write_state = WriteState::Closed;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "WebSocket write half closed",
                        )));
                    }
                },
                WriteState::Closed => {
                    self.write_state = WriteState::Closed;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "WebSocket write half closed",
                    )));
                }
                WriteState::Failed => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "WebSocket write half in failed state",
                    )));
                }
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Poll::Ready(Err(e)) = self.poll_flush_control(cx) {
            return Poll::Ready(Err(e));
        }
        loop {
            match std::mem::replace(&mut self.write_state, WriteState::Failed) {
                WriteState::Idle(tx) => {
                    self.write_state = WriteState::Idle(tx);
                    return Poll::Ready(Ok(()));
                }
                WriteState::Writing { mut fut, written_len } => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.write_state = WriteState::Writing { fut, written_len };
                        return Poll::Pending;
                    }
                    Poll::Ready((tx, Ok(()))) => {
                        self.write_state = WriteState::Idle(tx);
                        continue;
                    }
                    Poll::Ready((_tx, Err(e))) => {
                        self.write_state = WriteState::Failed;
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                    }
                },
                WriteState::WritingControl(fut) => {
                    self.write_state = WriteState::WritingControl(fut);
                    if let Poll::Ready(Err(e)) = self.poll_flush_control(cx) {
                        return Poll::Ready(Err(e));
                    }
                    return Poll::Pending;
                }
                WriteState::Closing(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.write_state = WriteState::Closing(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready(_) => {
                        self.write_state = WriteState::Closed;
                        return Poll::Ready(Ok(()));
                    }
                },
                WriteState::Closed => {
                    self.write_state = WriteState::Closed;
                    return Poll::Ready(Ok(()));
                }
                WriteState::Failed => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "WebSocket write half in failed state",
                    )));
                }
            }
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Poll::Ready(Err(e)) = self.poll_flush_control(cx) {
            return Poll::Ready(Err(e));
        }
        loop {
            match std::mem::replace(&mut self.write_state, WriteState::Failed) {
                WriteState::Idle(mut tx) => {
                    let fut = Box::pin(async move {
                        let res = tx.write_frame(Frame::close(1000, &[])).await;
                        (tx, res)
                    });
                    self.write_state = WriteState::Closing(fut);
                    continue;
                }
                WriteState::Writing { mut fut, written_len } => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.write_state = WriteState::Writing { fut, written_len };
                        return Poll::Pending;
                    }
                    Poll::Ready((tx, Ok(()))) => {
                        self.write_state = WriteState::Idle(tx);
                        continue;
                    }
                    Poll::Ready((_tx, Err(e))) => {
                        self.write_state = WriteState::Failed;
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                    }
                },
                WriteState::WritingControl(fut) => {
                    self.write_state = WriteState::WritingControl(fut);
                    if let Poll::Ready(Err(e)) = self.poll_flush_control(cx) {
                        return Poll::Ready(Err(e));
                    }
                    return Poll::Pending;
                }
                WriteState::Closing(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.write_state = WriteState::Closing(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready((_tx, Ok(()))) => {
                        self.write_state = WriteState::Closed;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready((_tx, Err(e))) => {
                        self.write_state = WriteState::Failed;
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                    }
                },
                WriteState::Closed => {
                    self.write_state = WriteState::Closed;
                    return Poll::Ready(Ok(()));
                }
                WriteState::Failed => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "WebSocket write half in failed state",
                    )));
                }
            }
        }
    }
}

impl TunnelStream for WsTunnelStream {
    fn supports_half_close(&self) -> bool {
        // WebSocket RFC 6455 does not support half-close (closing one direction terminates session)
        false
    }

    fn poll_send_ping(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Poll::Ready(Err(e)) = self.poll_flush_control(cx) {
            return Poll::Ready(Err(e));
        }
        loop {
            match std::mem::replace(&mut self.write_state, WriteState::Failed) {
                WriteState::Idle(mut tx) => {
                    let mut fut = Box::pin(async move {
                        let res = tx.write_frame(Frame::new(true, OpCode::Ping, None, Payload::Borrowed(&[]))).await;
                        (tx, res)
                    });
                    match fut.as_mut().poll(cx) {
                        Poll::Ready((tx, Ok(()))) => {
                            self.write_state = WriteState::Idle(tx);
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Ready((_tx, Err(e))) => {
                            self.write_state = WriteState::Failed;
                            return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                        }
                        Poll::Pending => {
                            self.write_state = WriteState::WritingControl(fut);
                            return Poll::Pending;
                        }
                    }
                }
                WriteState::Writing { mut fut, written_len } => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.write_state = WriteState::Writing { fut, written_len };
                        return Poll::Pending;
                    }
                    Poll::Ready((tx, Ok(()))) => {
                        self.write_state = WriteState::Idle(tx);
                        continue;
                    }
                    Poll::Ready((_tx, Err(e))) => {
                        self.write_state = WriteState::Failed;
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                    }
                },
                WriteState::WritingControl(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.write_state = WriteState::WritingControl(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready((tx, Ok(()))) => {
                        self.write_state = WriteState::Idle(tx);
                        continue;
                    }
                    Poll::Ready((_tx, Err(e))) => {
                        self.write_state = WriteState::Failed;
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                    }
                },
                WriteState::Closing(fut) => {
                    self.write_state = WriteState::Closing(fut);
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "closing")));
                }
                WriteState::Closed => {
                    self.write_state = WriteState::Closed;
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")));
                }
                WriteState::Failed => {
                    self.write_state = WriteState::Failed;
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "failed")));
                }
            }
        }
    }

    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        match &mut self.read_state {
            ReadState::Eof | ReadState::Failed => Poll::Ready(HeartbeatStatus::Dead),
            ReadState::Buffered { .. } => Poll::Ready(HeartbeatStatus::Alive),
            ReadState::Idle(_) => {
                // Peek a read poll to check if underlying connection died or closed
                let mut empty = [0u8; 0];
                let mut buf = ReadBuf::new(&mut empty);
                match Pin::new(&mut *self).poll_read(cx, &mut buf) {
                    Poll::Ready(Err(_)) => Poll::Ready(HeartbeatStatus::Dead),
                    Poll::Pending => {
                        // Check if poll_read transitioned into Reading and inspect completion
                        if let ReadState::Reading(ref mut fut) = self.read_state {
                            match fut.as_mut().poll(cx) {
                                Poll::Ready((_rx, obligated, Ok((OpCode::Close, _)))) => {
                                    if !obligated.is_empty() {
                                        self.control_queue.extend(obligated);
                                        let _ = self.poll_flush_control(cx);
                                    }
                                    self.read_state = ReadState::Eof;
                                    Poll::Ready(HeartbeatStatus::Dead)
                                }
                                Poll::Ready((_rx, obligated, Err(_))) => {
                                    if !obligated.is_empty() {
                                        self.control_queue.extend(obligated);
                                        let _ = self.poll_flush_control(cx);
                                    }
                                    self.read_state = ReadState::Failed;
                                    Poll::Ready(HeartbeatStatus::Dead)
                                }
                                _ => Poll::Ready(HeartbeatStatus::Alive),
                            }
                        } else {
                            Poll::Ready(HeartbeatStatus::Alive)
                        }
                    }
                    _ => Poll::Ready(HeartbeatStatus::Alive),
                }
            }
            ReadState::Reading(fut) => {
                match fut.as_mut().poll(cx) {
                    Poll::Ready((_rx, obligated, Ok((OpCode::Close, _)))) => {
                        if !obligated.is_empty() {
                            self.control_queue.extend(obligated);
                            let _ = self.poll_flush_control(cx);
                        }
                        self.read_state = ReadState::Eof;
                        Poll::Ready(HeartbeatStatus::Dead)
                    }
                    Poll::Ready((_rx, obligated, Err(_))) => {
                        if !obligated.is_empty() {
                            self.control_queue.extend(obligated);
                            let _ = self.poll_flush_control(cx);
                        }
                        self.read_state = ReadState::Failed;
                        Poll::Ready(HeartbeatStatus::Dead)
                    }
                    Poll::Ready((rx, obligated, Ok((_, payload)))) => {
                        if !obligated.is_empty() {
                            self.control_queue.extend(obligated);
                            let _ = self.poll_flush_control(cx);
                        }
                        if payload.is_empty() {
                            self.read_state = ReadState::Idle(rx);
                        } else {
                            self.read_state = ReadState::Buffered { rx, payload, cursor: 0 };
                        }
                        Poll::Ready(HeartbeatStatus::Alive)
                    }
                    Poll::Pending => Poll::Ready(HeartbeatStatus::Alive),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

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
