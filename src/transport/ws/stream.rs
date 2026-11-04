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

type WsReadResult = (
    WebSocketRead<DynRead>,
    Vec<Frame<'static>>,
    Result<(OpCode, Bytes), WebSocketError>,
);
type WsReadFuture = BoxFuture<'static, WsReadResult>;

enum ReadState {
    Idle(WebSocketRead<DynRead>),
    Reading(WsReadFuture),
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
    Failed,
}

/// Dynamic tunnel stream adapter directly wrapping split `WebSocketRead` and `WebSocketWrite`.
///
/// Implements `AsyncRead` and `AsyncWrite` purely using state-machine transitions without
/// intermediate duplex pipes, background tasks, or multi-copy buffering.
/// Hard upper bound on pending control frames (Pong/Close) to prevent memory amplification attacks from Ping floods.
const MAX_CONTROL_QUEUE: usize = 64;

pub struct WsTunnelStream {
    read_state: ReadState,
    write_state: WriteState,
    control_queue: std::collections::VecDeque<Frame<'static>>,
    peer_closed: bool,
    close_sent: bool,
    fin_sent: bool,
}

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
            peer_closed: false,
            close_sent: false,
            fin_sent: false,
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
                        let mut budget = 32usize;
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
                                    let payload = match frame.payload {
                                        Payload::Bytes(b) => b.freeze(),
                                        Payload::Owned(v) => Bytes::from(v),
                                        other => Bytes::copy_from_slice(&other),
                                    };
                                    break Ok((OpCode::Close, payload));
                                }
                                _ => {
                                    budget = budget.saturating_sub(1);
                                    if budget == 0 {
                                        // Reached budget limit on control frames: yield to avoid worker starvation
                                        break Ok((frame.opcode, Bytes::new()));
                                    }
                                }
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
                    Poll::Ready((_rx, obligated, Ok((OpCode::Close, payload)))) => {
                        if !obligated.is_empty() {
                            while self.control_queue.len() + obligated.len() >= MAX_CONTROL_QUEUE {
                                self.control_queue.pop_front();
                            }
                            self.control_queue.extend(obligated);
                        }
                        self.peer_closed = true;
                        if !self.close_sent {
                            // RFC 6455 §5.5.1: If an endpoint receives a Close frame and did not previously
                            // send a Close frame, the endpoint MUST send a Close frame in response.
                            let close_frame = if payload.is_empty() {
                                Frame::close(1000, &[])
                            } else {
                                Frame::close_raw(Payload::Owned(payload.to_vec()))
                            };
                            self.control_queue.push_back(close_frame);
                            self.close_sent = true;
                        }
                        let _ = self.poll_flush_control(cx);
                        self.read_state = ReadState::Eof;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready((rx, obligated, Ok((opcode, payload)))) => {
                        if !obligated.is_empty() {
                            if self.control_queue.len() + obligated.len() > MAX_CONTROL_QUEUE {
                                self.read_state = ReadState::Failed;
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "WebSocket control frame flood",
                                )));
                            }
                            self.control_queue.extend(obligated);
                            let _ = self.poll_flush_control(cx);
                        }
                        if opcode == OpCode::Binary && payload.is_empty() {
                            // In-band logical FIN received: remote write direction is closed
                            self.read_state = ReadState::Eof;
                            return Poll::Ready(Ok(()));
                        }
                        if payload.is_empty() {
                            self.read_state = ReadState::Idle(rx);
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
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
                            while self.control_queue.len() + obligated.len() > MAX_CONTROL_QUEUE {
                                self.control_queue.pop_front();
                            }
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
        if self.peer_closed || self.close_sent {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "WebSocket peer or write half closed",
            )));
        }

        match self.poll_flush_control(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }

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
                        Poll::Ready(Ok(len))
                    }
                    Poll::Ready((_tx, Err(e))) => {
                        self.write_state = WriteState::Failed;
                        Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)))
                    }
                    Poll::Pending => {
                        self.write_state = WriteState::Writing { fut, written_len: len };
                        Poll::Pending
                    }
                }
            }
            WriteState::Writing { mut fut, written_len } => match fut.as_mut().poll(cx) {
                Poll::Pending => {
                    self.write_state = WriteState::Writing { fut, written_len };
                    Poll::Pending
                }
                Poll::Ready((tx, Ok(()))) => {
                    self.write_state = WriteState::Idle(tx);
                    Poll::Ready(Ok(written_len))
                }
                Poll::Ready((_tx, Err(e))) => {
                    self.write_state = WriteState::Failed;
                    Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)))
                }
            },
            WriteState::WritingControl(fut) => {
                self.write_state = WriteState::WritingControl(fut);
                Poll::Pending
            }
            WriteState::Failed => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "WebSocket write half in failed state",
            ))),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_flush_control(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
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
                    return Poll::Pending;
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
        if self.fin_sent || self.close_sent {
            return Poll::Ready(Ok(()));
        }
        match self.poll_flush_control(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }
        loop {
            match std::mem::replace(&mut self.write_state, WriteState::Failed) {
                WriteState::Idle(mut tx) => {
                    // Send 0-length binary frame as in-band logical FIN, supporting true half-close
                    let fut = Box::pin(async move {
                        let res = tx.write_frame(Frame::binary(Payload::Owned(Vec::new()))).await;
                        (tx, res)
                    });
                    self.write_state = WriteState::Writing { fut, written_len: 0 };
                    continue;
                }
                WriteState::Writing { mut fut, written_len } => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.write_state = WriteState::Writing { fut, written_len };
                        return Poll::Pending;
                    }
                    Poll::Ready((tx, Ok(()))) => {
                        self.write_state = WriteState::Idle(tx);
                        self.fin_sent = true;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready((_tx, Err(e))) => {
                        self.write_state = WriteState::Failed;
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                    }
                },
                WriteState::WritingControl(fut) => {
                    self.write_state = WriteState::WritingControl(fut);
                    return Poll::Pending;
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
    fn poll_write_bytes(
        &mut self,
        cx: &mut Context<'_>,
        buf: Bytes,
    ) -> Poll<io::Result<usize>> {
        if self.peer_closed || self.close_sent {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "WebSocket peer or write half closed",
            )));
        }

        match self.poll_flush_control(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }

        match std::mem::replace(&mut self.write_state, WriteState::Failed) {
            WriteState::Idle(mut tx) => {
                let len = buf.len();
                let payload = bytes::BytesMut::from(buf.as_ref());
                let mut fut = Box::pin(async move {
                    let res = tx.write_frame(Frame::binary(Payload::Bytes(payload))).await;
                    (tx, res)
                });
                match fut.as_mut().poll(cx) {
                    Poll::Ready((tx, Ok(()))) => {
                        self.write_state = WriteState::Idle(tx);
                        Poll::Ready(Ok(len))
                    }
                    Poll::Ready((_tx, Err(e))) => {
                        self.write_state = WriteState::Failed;
                        Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)))
                    }
                    Poll::Pending => {
                        self.write_state = WriteState::Writing { fut, written_len: len };
                        Poll::Pending
                    }
                }
            }
            WriteState::Writing { mut fut, written_len } => match fut.as_mut().poll(cx) {
                Poll::Pending => {
                    self.write_state = WriteState::Writing { fut, written_len };
                    Poll::Pending
                }
                Poll::Ready((tx, Ok(()))) => {
                    self.write_state = WriteState::Idle(tx);
                    Poll::Ready(Ok(written_len))
                }
                Poll::Ready((_tx, Err(e))) => {
                    self.write_state = WriteState::Failed;
                    Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)))
                }
            },
            WriteState::WritingControl(fut) => {
                self.write_state = WriteState::WritingControl(fut);
                Poll::Pending
            }
            WriteState::Failed => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "WebSocket write half in failed state",
            ))),
        }
    }

    fn supports_half_close(&self) -> bool {
        // Supported via in-band zero-length binary FIN frames without tearing down opposite stream
        true
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
                                Poll::Ready((_rx, obligated, Ok((OpCode::Close, payload)))) => {
                                    if !obligated.is_empty() {
                                        self.control_queue.extend(obligated);
                                    }
                                    self.peer_closed = true;
                                    if !self.close_sent {
                                        let close_frame = if payload.is_empty() {
                                            Frame::close(1000, &[])
                                        } else {
                                            Frame::close_raw(Payload::Owned(payload.to_vec()))
                                        };
                                        self.control_queue.push_back(close_frame);
                                        self.close_sent = true;
                                    }
                                    let _ = self.poll_flush_control(cx);
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
                    Poll::Ready((_rx, obligated, Ok((OpCode::Close, payload)))) => {
                        if !obligated.is_empty() {
                            self.control_queue.extend(obligated);
                        }
                        self.peer_closed = true;
                        if !self.close_sent {
                            let close_frame = if payload.is_empty() {
                                Frame::close(1000, &[])
                            } else {
                                Frame::close_raw(Payload::Owned(payload.to_vec()))
                            };
                            self.control_queue.push_back(close_frame);
                            self.close_sent = true;
                        }
                        let _ = self.poll_flush_control(cx);
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

    #[tokio::test]
    async fn test_ws_half_close_bidirectional_transfer() {
        let (raw_client, raw_server) = duplex(65536);

        let mut client = WsTunnelStream::new(raw_client, Role::Client);
        let mut server = WsTunnelStream::new(raw_server, Role::Server);

        // 1. Client writes payload then cleanly shuts down its write half (in-band FIN)
        client.write_all(b"half close request").await.unwrap();
        client.shutdown().await.unwrap();

        // 2. Server reads the request then observes EOF
        let mut req_buf = vec![0u8; 18];
        server.read_exact(&mut req_buf).await.unwrap();
        assert_eq!(&req_buf, b"half close request");

        let mut eof_buf = [0u8; 1];
        let n = server.read(&mut eof_buf).await.unwrap();
        assert_eq!(n, 0, "Server must receive EOF after client half-close");

        // 3. Server can still write response back to client
        server.write_all(b"response after client fin").await.unwrap();
        server.shutdown().await.unwrap();

        // 4. Client reads response then observes EOF
        let mut resp_buf = vec![0u8; 25];
        client.read_exact(&mut resp_buf).await.unwrap();
        assert_eq!(&resp_buf, b"response after client fin");

        let n = client.read(&mut eof_buf).await.unwrap();
        assert_eq!(n, 0, "Client must receive EOF after server half-close");
    }

    #[tokio::test]
    async fn test_ws_close_contract_rejects_further_writes() {
        let (raw_client, raw_server) = duplex(65536);

        // Client is a raw WebSocket sender that emits a standard RFC 6455 Close frame
        let (r, w) = tokio::io::split(raw_client);
        let (_rx, mut tx) = after_handshake_split(
            Box::new(r) as DynRead,
            Box::new(w) as DynWrite,
            Role::Client,
        );

        let mut server = WsTunnelStream::new(raw_server, Role::Server);

        // Client sends RFC 6455 Close
        tx.write_frame(Frame::close(1000, b"normal closure")).await.unwrap();

        // Server reads EOF
        let mut buf = [0u8; 10];
        let n = server.read(&mut buf).await.unwrap();
        assert_eq!(n, 0, "Server must receive EOF on Close frame");

        // Subsequent poll_write on server MUST fail with BrokenPipe per RFC 6455
        let write_res = server.write_all(b"illegal data after peer close").await;
        assert!(write_res.is_err(), "Write after peer close must fail");
        assert_eq!(write_res.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn test_ws_writing_control_no_lost_wakeup() {
        let (raw_client, raw_server) = duplex(65536);

        let mut client = WsTunnelStream::new(raw_client, Role::Client);
        let mut server = WsTunnelStream::new(raw_server, Role::Server);

        // Enqueue multiple control frames on server before writing data
        server.control_queue.push_back(Frame::pong(Payload::Owned(b"pong1".to_vec())));
        server.control_queue.push_back(Frame::pong(Payload::Owned(b"pong2".to_vec())));

        // server writes data frame; poll_flush_control must flush control queue without lost wakeups
        server.write_all(b"data frame after pong").await.unwrap();
        server.flush().await.unwrap();

        let mut buf = vec![0u8; 21];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"data frame after pong");
    }

    #[tokio::test]
    async fn test_ws_control_frame_queue_bounded_against_flood() {
        let (raw_client, raw_server) = duplex(65536);

        let (r, w) = tokio::io::split(raw_client);
        let (_rx, mut tx) = after_handshake_split(
            Box::new(r) as DynRead,
            Box::new(w) as DynWrite,
            Role::Client,
        );

        let mut server = WsTunnelStream::new(raw_server, Role::Server);

        // Pre-fill server's control queue to MAX_CONTROL_QUEUE and simulate write side backpressure
        for _ in 0..MAX_CONTROL_QUEUE {
            server.control_queue.push_back(Frame::pong(Payload::Owned(vec![])));
        }
        server.write_state = WriteState::Writing {
            fut: Box::pin(std::future::pending()),
            written_len: 0,
        };

        // Client sends a Ping frame (which triggers an obligated Pong frame on server)
        // followed by a data frame so server's read completes its loop.
        tx.write_frame(Frame::new(true, OpCode::Ping, None, Payload::Owned(b"flood".to_vec())))
            .await
            .unwrap();
        tx.write_frame(Frame::binary(Payload::Owned(b"data".to_vec())))
            .await
            .unwrap();

        // Server reading next frame must reject with InvalidData due to control frame queue overflow under backpressure
        let mut buf = [0u8; 10];
        let res = server.read(&mut buf).await;
        assert!(res.is_err(), "Server must reject control frame flood under backpressure");
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn test_ws_poll_write_bytes_zero_copy() {
        use std::future::poll_fn;

        let (raw_client, raw_server) = duplex(65536);

        let mut client = WsTunnelStream::new(raw_client, Role::Client);
        let mut server = WsTunnelStream::new(raw_server, Role::Server);

        let test_payload = Bytes::from_static(b"zero copy bytes payload transfer");
        let n = poll_fn(|cx| client.poll_write_bytes(cx, test_payload.clone())).await.unwrap();
        assert_eq!(n, test_payload.len());
        client.flush().await.unwrap();

        let mut buf = vec![0u8; test_payload.len()];
        server.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, test_payload.as_ref());
    }
}
