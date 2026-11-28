// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! HTTP/3 and QUIC tunnel stream adapters implementing `TunnelStream`.
//! All comments and docstrings must be in English.

use std::future::Future;
use std::io;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::PollSender;
use tracing::debug;

use crate::consts::{DEFAULT_H3_FINISH_STREAM_TIMEOUT, H3_STREAM_CHUNK_QUEUE_SIZE};
use crate::core::stream::{HeartbeatStatus, StreamCaps, TunnelStream, activity_now_ms};

use super::QuicSessionHandle;

/// Raw QUIC bidirectional stream adapter directly polling `quinn::SendStream` and `quinn::RecvStream`.
pub struct QuinnTunnelStream {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    session: Option<QuicSessionHandle>,
    shutdown_done: bool,
    read_buf: Option<u8>,
    last_activity_ms: AtomicU64,
}

impl QuinnTunnelStream {
    /// Creates a new `QuinnTunnelStream` from split Quinn send and receive halves.
    pub fn new(send: quinn::SendStream, recv: quinn::RecvStream) -> Self {
        Self {
            send,
            recv,
            session: None,
            shutdown_done: false,
            read_buf: None,
            last_activity_ms: AtomicU64::new(activity_now_ms()),
        }
    }

    /// Associates the shared QUIC session handle for accurate liveness detection.
    pub(crate) fn with_session(mut self, session: QuicSessionHandle) -> Self {
        self.session = Some(session);
        self
    }

    #[inline]
    fn touch_activity(&self) {
        self.last_activity_ms
            .store(activity_now_ms(), Ordering::Relaxed);
    }
}

impl AsyncRead for QuinnTunnelStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if let Some(ref session) = self.session {
            if !session.is_alive() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "Underlying QUIC connection closed",
                )));
            }
            if !session.check_liveness() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Underlying QUIC connection unresponsive",
                )));
            }
        }
        let mut filled_any = false;
        if let Some(b) = self.read_buf.take() {
            buf.put_slice(&[b]);
            filled_any = true;
            if buf.remaining() == 0 {
                self.touch_activity();
                return Poll::Ready(Ok(()));
            }
        }
        let before = buf.filled().len();
        match Pin::new(&mut self.recv).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if buf.filled().len() > before || filled_any {
                    self.touch_activity();
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                if filled_any {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Ready(Err(e))
                }
            }
            Poll::Pending => {
                if filled_any {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

impl AsyncWrite for QuinnTunnelStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(ref session) = self.session {
            if !session.is_alive() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "Underlying QUIC connection closed",
                )));
            }
            if !session.check_liveness() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Underlying QUIC connection unresponsive",
                )));
            }
        }
        match tokio::io::AsyncWrite::poll_write(Pin::new(&mut self.send), cx, buf) {
            Poll::Ready(Ok(n)) => {
                if n > 0 {
                    self.touch_activity();
                }
                Poll::Ready(Ok(n))
            }
            other => other,
        }
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

impl TunnelStream for QuinnTunnelStream {
    fn caps(&self) -> StreamCaps {
        StreamCaps {
            owned_write: true,
            ..StreamCaps::default()
        }
    }

    fn poll_write_owned(&mut self, cx: &mut Context<'_>, chunk: Bytes) -> Poll<io::Result<usize>> {
        if self.shutdown_done {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "QUIC stream write direction closed",
            )));
        }

        if let Some(ref session) = self.session {
            if !session.is_alive() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "Underlying QUIC connection closed",
                )));
            }
            if !session.check_liveness() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Underlying QUIC connection unresponsive",
                )));
            }
        }

        if chunk.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let this = self;
        let result = {
            let mut chunks = [chunk];
            let mut write = pin!(this.send.write_chunks(&mut chunks));
            write.as_mut().poll(cx)
        };
        match result {
            Poll::Ready(Ok(written)) => {
                if written.bytes > 0 {
                    this.touch_activity();
                }
                Poll::Ready(Ok(written.bytes))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn last_activity_ms(&self) -> Option<u64> {
        Some(self.last_activity_ms.load(Ordering::Relaxed))
    }

    fn poll_send_ping(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(ref session) = self.session {
            if let Some(reason) = session.conn().close_reason() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    format!("Underlying QUIC connection closed: {:?}", reason),
                )));
            }
            if !session.check_liveness() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Underlying QUIC connection unresponsive to pings",
                )));
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        if let Some(ref session) = self.session
            && !session.check_liveness()
        {
            return Poll::Ready(HeartbeatStatus::Dead);
        }
        if self.read_buf.is_some() {
            return Poll::Ready(HeartbeatStatus::Alive);
        }
        let mut b = [0u8; 1];
        let mut read_buf = ReadBuf::new(&mut b);
        match Pin::new(&mut self.recv).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) if read_buf.filled().is_empty() => {
                Poll::Ready(HeartbeatStatus::Dead)
            }
            Poll::Ready(Ok(())) => {
                self.read_buf = Some(b[0]);
                self.touch_activity();
                Poll::Ready(HeartbeatStatus::Alive)
            }
            Poll::Ready(Err(_)) => Poll::Ready(HeartbeatStatus::Dead),
            Poll::Pending => Poll::Ready(HeartbeatStatus::Alive),
        }
    }

    fn session_id(&self) -> Option<u64> {
        self.session.as_ref().map(QuicSessionHandle::session_id)
    }
}

enum OutboundItem {
    Chunk(Bytes),
    Flush(oneshot::Sender<io::Result<()>>),
}

fn check_session_health(session: Option<&QuicSessionHandle>) -> io::Result<()> {
    if let Some(session) = session {
        if !session.is_alive() {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "Underlying QUIC connection closed",
            ));
        }
        if !session.check_liveness() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Underlying QUIC connection unresponsive",
            ));
        }
    }
    Ok(())
}

/// Bidirectional stream adapter for HTTP/3 request streams (`h3::client` or `h3::server`).
pub struct H3TunnelStream {
    tx_outbound: Option<PollSender<OutboundItem>>,
    rx_inbound: mpsc::Receiver<io::Result<Bytes>>,
    read_buf: Option<Bytes>,
    flush_rx: Option<oneshot::Receiver<io::Result<()>>>,
    needs_flush: bool,
    shutdown_rx: Option<oneshot::Receiver<io::Result<()>>>,
    session: Option<QuicSessionHandle>,
    shutdown_done: bool,
    fin_sent: bool,
    last_activity_ms: AtomicU64,
    outbound_task: Option<tokio::task::JoinHandle<()>>,
    inbound_task: Option<tokio::task::JoinHandle<()>>,
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

    /// Associates the shared QUIC session handle for accurate liveness detection.
    pub(crate) fn with_session(mut self, session: QuicSessionHandle) -> Self {
        self.session = Some(session);
        self
    }

    #[inline]
    fn touch_activity(&self) {
        self.last_activity_ms
            .store(activity_now_ms(), Ordering::Relaxed);
    }

    fn take_outbound_close_error(&mut self) -> io::Error {
        if let Some(ref mut rx) = self.shutdown_rx
            && let Ok(Err(e)) = rx.try_recv()
        {
            self.shutdown_done = true;
            self.shutdown_rx.take();
            return e;
        }
        io::Error::new(
            io::ErrorKind::ConnectionReset,
            "HTTP/3 background pump closed",
        )
    }

    fn poll_outbound_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Err(e) = check_session_health(self.session.as_ref()) {
            return Poll::Ready(Err(e));
        }
        let Some(tx) = self.tx_outbound.as_mut() else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "HTTP/3 stream write direction closed",
            )));
        };
        match tx.poll_reserve(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(_)) => Poll::Ready(Err(self.take_outbound_close_error())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_write_chunk(
        &mut self,
        cx: &mut Context<'_>,
        len: usize,
        make_chunk: impl FnOnce() -> Bytes,
    ) -> Poll<io::Result<usize>> {
        if self.fin_sent {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "HTTP/3 stream write direction closed",
            )));
        }

        if len == 0 {
            if let Err(e) = check_session_health(self.session.as_ref()) {
                return Poll::Ready(Err(e));
            }
            if self.tx_outbound.is_none() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "HTTP/3 stream write direction closed",
                )));
            }
            return Poll::Ready(Ok(0));
        }

        match self.poll_outbound_ready(cx) {
            Poll::Ready(Ok(())) => {
                if let Some(ref mut tx) = self.tx_outbound {
                    let _ = tx.send_item(OutboundItem::Chunk(make_chunk()));
                }
                // Any newly queued chunk supersedes an incomplete flush barrier so the next
                // `poll_flush` must wait for this chunk too.
                self.flush_rx.take();
                self.needs_flush = true;
                self.touch_activity();
                Poll::Ready(Ok(len))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(ref mut rx) = self.shutdown_rx {
            match Future::poll(Pin::new(rx), cx) {
                Poll::Ready(Ok(res)) => {
                    self.shutdown_done = true;
                    self.shutdown_rx.take();
                    if res.is_ok() {
                        self.needs_flush = false;
                    }
                    Poll::Ready(res)
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

    fn spawn_split_pump<S, R>(mut send_stream: S, mut recv_stream: R) -> Self
    where
        S: H3SendStream,
        R: H3RecvStream,
    {
        let (tx_outbound, mut rx_outbound) =
            mpsc::channel::<OutboundItem>(H3_STREAM_CHUNK_QUEUE_SIZE);
        let (tx_inbound, rx_inbound) =
            mpsc::channel::<io::Result<Bytes>>(H3_STREAM_CHUNK_QUEUE_SIZE);
        let (tx_shutdown, rx_shutdown) = oneshot::channel::<io::Result<()>>();

        // Outbound send task: continuously drains outgoing chunks without being blocked by incoming reading
        let outbound_task = tokio::spawn(async move {
            let mut result = Ok(());
            while let Some(item) = rx_outbound.recv().await {
                match item {
                    OutboundItem::Chunk(chunk) => {
                        if let Err(e) = send_stream.send_chunk(chunk).await {
                            debug!("H3 send_chunk failed: {:?}", e);
                            rx_outbound.close();
                            while let Ok(pending) = rx_outbound.try_recv() {
                                if let OutboundItem::Flush(reply) = pending {
                                    let _ =
                                        reply.send(Err(io::Error::new(e.kind(), e.to_string())));
                                }
                            }
                            result = Err(e);
                            break;
                        }
                    }
                    OutboundItem::Flush(reply) => {
                        let _ = reply.send(Ok(()));
                    }
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
        let inbound_task = tokio::spawn(async move {
            loop {
                match recv_stream.recv_chunk().await {
                    Ok(Some(data)) => {
                        if data.is_empty() {
                            continue;
                        }
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
            flush_rx: None,
            needs_flush: false,
            shutdown_rx: Some(rx_shutdown),
            session: None,
            shutdown_done: false,
            fin_sent: false,
            last_activity_ms: AtomicU64::new(activity_now_ms()),
            outbound_task: Some(outbound_task),
            inbound_task: Some(inbound_task),
        }
    }
}

impl Drop for H3TunnelStream {
    fn drop(&mut self) {
        if let Some(task) = self.outbound_task.take() {
            task.abort();
        }
        if let Some(task) = self.inbound_task.take() {
            task.abort();
        }
    }
}

impl AsyncRead for H3TunnelStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if let Some(ref session) = self.session {
            if !session.is_alive() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "Underlying QUIC connection closed",
                )));
            }
            if !session.check_liveness() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Underlying QUIC connection unresponsive",
                )));
            }
        }
        while let Some(mut current) = self.read_buf.take() {
            if current.is_empty() {
                continue;
            }
            let to_copy = std::cmp::min(buf.remaining(), current.len());
            buf.put_slice(&current[..to_copy]);
            current.advance(to_copy);
            if current.has_remaining() {
                self.read_buf = Some(current);
            }
            self.touch_activity();
            return Poll::Ready(Ok(()));
        }

        loop {
            match self.rx_inbound.poll_recv(cx) {
                Poll::Ready(Some(Ok(bytes))) if bytes.is_empty() => continue,
                Poll::Ready(Some(Ok(mut bytes))) => {
                    let to_copy = std::cmp::min(buf.remaining(), bytes.len());
                    buf.put_slice(&bytes[..to_copy]);
                    bytes.advance(to_copy);
                    if bytes.has_remaining() {
                        self.read_buf = Some(bytes);
                    }
                    self.touch_activity();
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Ready(None) => return Poll::Ready(Ok(())), // EOF
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncWrite for H3TunnelStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_chunk(cx, buf.len(), || Bytes::copy_from_slice(buf))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(ref mut rx) = self.flush_rx {
            return match Future::poll(Pin::new(rx), cx) {
                Poll::Ready(Ok(res)) => {
                    self.flush_rx.take();
                    if res.is_ok() {
                        self.needs_flush = false;
                    }
                    Poll::Ready(res)
                }
                Poll::Ready(Err(_)) => {
                    self.flush_rx.take();
                    Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "HTTP/3 background pump terminated",
                    )))
                }
                Poll::Pending => Poll::Pending,
            };
        }

        if !self.needs_flush || self.shutdown_done {
            return Poll::Ready(Ok(()));
        }

        if self.tx_outbound.is_none() {
            return self.poll_shutdown_drain(cx);
        }

        match self.poll_outbound_ready(cx) {
            Poll::Ready(Ok(())) => {
                let (ftx, mut frx) = oneshot::channel();
                if let Some(ref mut tx) = self.tx_outbound {
                    let _ = tx.send_item(OutboundItem::Flush(ftx));
                }
                match Future::poll(Pin::new(&mut frx), cx) {
                    Poll::Ready(Ok(res)) => {
                        if res.is_ok() {
                            self.needs_flush = false;
                        }
                        Poll::Ready(res)
                    }
                    Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "HTTP/3 background pump terminated",
                    ))),
                    Poll::Pending => {
                        self.flush_rx = Some(frx);
                        Poll::Pending
                    }
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.shutdown_done {
            return Poll::Ready(Ok(()));
        }

        if let Some(ref session) = self.session
            && let Some(reason) = session.conn().close_reason()
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

        self.poll_shutdown_drain(cx)
    }
}

impl TunnelStream for H3TunnelStream {
    fn caps(&self) -> StreamCaps {
        StreamCaps {
            owned_write: true,
            ..StreamCaps::default()
        }
    }

    fn poll_write_owned(&mut self, cx: &mut Context<'_>, chunk: Bytes) -> Poll<io::Result<usize>> {
        self.poll_write_chunk(cx, chunk.len(), || chunk)
    }

    fn last_activity_ms(&self) -> Option<u64> {
        Some(self.last_activity_ms.load(Ordering::Relaxed))
    }

    fn poll_send_ping(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(ref session) = self.session {
            if let Some(reason) = session.conn().close_reason() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    format!("Underlying QUIC connection closed: {:?}", reason),
                )));
            }
            if !session.check_liveness() {
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
        if let Some(ref session) = self.session
            && !session.check_liveness()
        {
            return Poll::Ready(HeartbeatStatus::Dead);
        }
        if self.read_buf.as_ref().is_some_and(|b| !b.is_empty()) {
            return Poll::Ready(HeartbeatStatus::Alive);
        }
        self.read_buf.take();
        loop {
            match self.rx_inbound.poll_recv(cx) {
                Poll::Ready(Some(Ok(bytes))) if bytes.is_empty() => continue,
                Poll::Ready(Some(Ok(bytes))) => {
                    self.touch_activity();
                    self.read_buf = Some(bytes);
                    return Poll::Ready(HeartbeatStatus::Alive);
                }
                Poll::Ready(Some(Err(_))) | Poll::Ready(None) => {
                    return Poll::Ready(HeartbeatStatus::Dead);
                }
                Poll::Pending => return Poll::Ready(HeartbeatStatus::Alive),
            }
        }
    }

    fn session_id(&self) -> Option<u64> {
        self.session.as_ref().map(QuicSessionHandle::session_id)
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct MockRecvStream;
    impl H3RecvStream for MockRecvStream {
        async fn recv_chunk(&mut self) -> Result<Option<Bytes>, io::Error> {
            Ok(None)
        }
    }

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
        assert!(stream.caps().owned_write);

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

    #[tokio::test]
    async fn test_h3_stream_drop_aborts_background_tasks() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        let dropped_flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&dropped_flag);

        struct HangingRecvStream(Arc<AtomicBool>);
        impl H3RecvStream for HangingRecvStream {
            async fn recv_chunk(&mut self) -> Result<Option<Bytes>, io::Error> {
                let flag = Arc::clone(&self.0);
                scopeguard::defer! {
                    flag.store(true, Ordering::SeqCst);
                }
                std::future::pending::<()>().await;
                Ok(None)
            }
        }

        let send = MockSendStream {
            sent: vec![],
            finished: false,
        };
        let recv = HangingRecvStream(flag_clone);
        let stream = H3TunnelStream::spawn_split_pump(send, recv);

        // Give the spawned task a moment to enter recv_chunk
        tokio::task::yield_now().await;
        assert!(!dropped_flag.load(Ordering::SeqCst));

        // Dropping stream must abort the inbound task and drop the hanging future
        drop(stream);
        tokio::task::yield_now().await;
        assert!(dropped_flag.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_h3_stream_poll_write_owned_and_post_shutdown_fails() {
        use std::future::poll_fn;

        let send = MockSendStream {
            sent: vec![],
            finished: false,
        };
        let recv = MockRecvStream;
        let mut stream = H3TunnelStream::spawn_split_pump(send, recv);
        assert!(stream.caps().owned_write);

        let payload = Bytes::from_static(b"owned payload test");
        let n = poll_fn(|cx| stream.poll_write_owned(cx, payload.clone()))
            .await
            .expect("owned write succeeds");
        assert_eq!(n, payload.len());

        // Shutdown stream
        stream.shutdown().await.expect("shutdown succeeds");

        // Subsequent poll_write_owned must fail with BrokenPipe
        let err = poll_fn(|cx| stream.poll_write_owned(cx, payload.clone()))
            .await
            .expect_err("write after shutdown must fail");
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    struct PendingRecvStream;
    impl H3RecvStream for PendingRecvStream {
        async fn recv_chunk(&mut self) -> Result<Option<Bytes>, io::Error> {
            std::future::pending::<()>().await;
            Ok(None)
        }
    }

    #[tokio::test]
    async fn test_h3_stream_ping_does_not_refresh_activity_and_expires_idle_timeout() {
        use crate::core::error::TunnelError;
        use crate::io::pump::StreamStreamPump;
        use std::future::poll_fn;
        use std::time::Duration;
        use tokio_util::sync::CancellationToken;

        let (server_conn, client_conn) =
            super::super::connector::tests::create_test_quic_connection_pair().await;
        let session = QuicSessionHandle::new(client_conn);

        let send = MockSendStream {
            sent: vec![],
            finished: false,
        };
        let mut stream =
            H3TunnelStream::spawn_split_pump(send, PendingRecvStream).with_session(session);

        let before = stream.last_activity_ms().unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        for _ in 0..5 {
            poll_fn(|cx| stream.poll_send_ping(cx)).await.unwrap();
        }
        assert_eq!(
            stream.last_activity_ms().unwrap(),
            before,
            "H3 poll_send_ping must not refresh last_activity_ms"
        );

        let (peer_stream, _peer_keep) = tokio::io::duplex(1024);
        let cancel = CancellationToken::new();
        let res = tokio::time::timeout(
            Duration::from_secs(2),
            StreamStreamPump::relay_with_heartbeat(
                Box::new(stream),
                Box::new(peer_stream),
                cancel.child_token(),
                Duration::from_millis(20),
                Duration::from_millis(80),
            ),
        )
        .await
        .expect("relay must hit idle_timeout instead of hanging");

        match res {
            Err(TunnelError::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::TimedOut),
            other => panic!("expected idle TimedOut error, got {other:?}"),
        }
        cancel.cancel();
        drop(server_conn);
    }

    #[tokio::test]
    async fn test_quinn_stream_ping_does_not_refresh_activity_and_expires_idle_timeout() {
        use crate::core::error::TunnelError;
        use crate::io::pump::StreamStreamPump;
        use std::future::poll_fn;
        use std::time::Duration;
        use tokio_util::sync::CancellationToken;

        let (server_conn, client_conn) =
            super::super::connector::tests::create_test_quic_connection_pair().await;
        let (mut c_send, c_recv) = client_conn.open_bi().await.unwrap();
        // Write 1 byte so Quinn actually opens the stream on the server before we keep it idle.
        tokio::io::AsyncWriteExt::write_all(&mut c_send, b"x")
            .await
            .unwrap();
        let (s_send, mut s_recv) = server_conn.accept_bi().await.unwrap();
        let mut init = [0u8; 1];
        tokio::io::AsyncReadExt::read_exact(&mut s_recv, &mut init)
            .await
            .unwrap();

        let mut stream = QuinnTunnelStream::new(c_send, c_recv)
            .with_session(QuicSessionHandle::new(client_conn));

        let before = stream.last_activity_ms().unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        for _ in 0..5 {
            poll_fn(|cx| stream.poll_send_ping(cx)).await.unwrap();
        }
        assert_eq!(
            stream.last_activity_ms().unwrap(),
            before,
            "Quinn poll_send_ping must not refresh last_activity_ms"
        );

        let (peer_stream, _peer_keep) = tokio::io::duplex(1024);
        let cancel = CancellationToken::new();
        let res = tokio::time::timeout(
            Duration::from_secs(2),
            StreamStreamPump::relay_with_heartbeat(
                Box::new(stream),
                Box::new(peer_stream),
                cancel.child_token(),
                Duration::from_millis(20),
                Duration::from_millis(80),
            ),
        )
        .await
        .expect("relay must hit idle_timeout instead of hanging");

        match res {
            Err(TunnelError::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::TimedOut),
            other => panic!("expected idle TimedOut error, got {other:?}"),
        }
        cancel.cancel();
        drop((s_send, s_recv, server_conn));
    }

    #[tokio::test]
    async fn test_h3_stream_poll_flush_waits_for_outbound_send_chunk() {
        use std::sync::{Arc, Mutex};
        use std::time::Duration;
        use tokio::sync::Notify;

        struct GatedSendStream {
            sent: Arc<Mutex<Vec<Bytes>>>,
            entered: Arc<Notify>,
            release: Arc<Notify>,
            err_kind: Option<io::ErrorKind>,
        }

        impl H3SendStream for GatedSendStream {
            async fn send_chunk(&mut self, chunk: Bytes) -> Result<(), io::Error> {
                let wait = self.release.notified();
                tokio::pin!(wait);
                wait.as_mut().enable();
                self.entered.notify_waiters();
                wait.await;
                if let Some(kind) = self.err_kind {
                    return Err(io::Error::new(kind, "simulated send_chunk failure"));
                }
                self.sent.lock().unwrap().push(chunk);
                Ok(())
            }

            async fn finish_stream(&mut self) -> Result<(), io::Error> {
                Ok(())
            }
        }

        let sent = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let send = GatedSendStream {
            sent: Arc::clone(&sent),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            err_kind: None,
        };
        let mut stream = H3TunnelStream::spawn_split_pump(send, PendingRecvStream);

        // 1. write_all enqueues immediately, but flush must stay pending until send_chunk finishes.
        let entered_wait = entered.notified();
        tokio::pin!(entered_wait);
        entered_wait.as_mut().enable();
        stream.write_all(b"chunk-1").await.unwrap();
        entered_wait.await;

        let mut flush_fut = Box::pin(stream.flush());
        tokio::select! {
            _ = &mut flush_fut => panic!("flush must not complete before send_chunk finishes"),
            _ = tokio::time::sleep(Duration::from_millis(25)) => {}
        }
        assert!(sent.lock().unwrap().is_empty());

        release.notify_one();
        tokio::time::timeout(Duration::from_millis(500), flush_fut)
            .await
            .expect("flush should finish once send_chunk completes")
            .unwrap();
        assert_eq!(
            sent.lock().unwrap().as_slice(),
            &[Bytes::from_static(b"chunk-1")]
        );

        // 2. Writing a new chunk invalidates an incomplete flush barrier so the next flush waits for both.
        let entered_2 = entered.notified();
        tokio::pin!(entered_2);
        entered_2.as_mut().enable();
        stream.write_all(b"chunk-2").await.unwrap();
        entered_2.await;

        let mut stale_flush = Box::pin(stream.flush());
        tokio::select! {
            _ = &mut stale_flush => panic!("flush should block on chunk-2"),
            _ = tokio::time::sleep(Duration::from_millis(15)) => {}
        }
        drop(stale_flush);

        stream.write_all(b"chunk-3").await.unwrap();
        let mut final_flush = Box::pin(stream.flush());
        release.notify_one();
        tokio::select! {
            _ = &mut final_flush => panic!("flush must still wait for chunk-3 after chunk-2 finishes"),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        release.notify_one();
        tokio::time::timeout(Duration::from_millis(500), final_flush)
            .await
            .expect("final flush should complete after chunk-3")
            .unwrap();
        assert_eq!(
            sent.lock().unwrap().as_slice(),
            &[
                Bytes::from_static(b"chunk-1"),
                Bytes::from_static(b"chunk-2"),
                Bytes::from_static(b"chunk-3"),
            ]
        );

        // 3. send_chunk failure propagates through flush().
        let entered_err = Arc::new(Notify::new());
        let release_err = Arc::new(Notify::new());
        let mut err_stream = H3TunnelStream::spawn_split_pump(
            GatedSendStream {
                sent: Arc::new(Mutex::new(Vec::new())),
                entered: Arc::clone(&entered_err),
                release: Arc::clone(&release_err),
                err_kind: Some(io::ErrorKind::BrokenPipe),
            },
            PendingRecvStream,
        );
        err_stream.write_all(b"boom").await.unwrap();
        release_err.notify_one();
        let err = tokio::time::timeout(Duration::from_millis(500), err_stream.flush())
            .await
            .expect("flush must surface send_chunk failure")
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn test_h3_stream_empty_data_chunk_is_not_treated_as_eof() {
        use std::collections::VecDeque;

        struct SequenceRecvStream {
            chunks: VecDeque<Result<Option<Bytes>, io::Error>>,
        }

        impl H3RecvStream for SequenceRecvStream {
            async fn recv_chunk(&mut self) -> Result<Option<Bytes>, io::Error> {
                match self.chunks.pop_front() {
                    Some(res) => res,
                    None => Ok(None),
                }
            }
        }

        let send = MockSendStream {
            sent: vec![],
            finished: false,
        };
        let recv = SequenceRecvStream {
            chunks: VecDeque::from([
                Ok(Some(Bytes::new())),
                Ok(Some(Bytes::from_static(b"hello"))),
                Ok(Some(Bytes::new())),
                Ok(None),
            ]),
        };
        let mut stream = H3TunnelStream::spawn_split_pump(send, recv);

        let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
        assert_eq!(
            stream.poll_check_alive(&mut cx),
            Poll::Ready(HeartbeatStatus::Alive)
        );

        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        let mut eof_buf = [0u8; 8];
        let n = stream.read(&mut eof_buf).await.unwrap();
        assert_eq!(n, 0, "EOF must only occur after all real DATA chunks");
    }

    #[tokio::test]
    async fn test_quinn_stream_zero_capacity_read_buf_preserves_peeked_byte() {
        use std::time::Duration;

        let (server_conn, client_conn) =
            super::super::connector::tests::create_test_quic_connection_pair().await;
        let (mut c_send, c_recv) = client_conn.open_bi().await.unwrap();
        c_send.write_all(b"init").await.unwrap();
        let (mut s_send, mut s_recv) = server_conn.accept_bi().await.unwrap();
        let mut init_buf = [0u8; 4];
        s_recv.read_exact(&mut init_buf).await.unwrap();

        let mut stream = QuinnTunnelStream::new(c_send, c_recv)
            .with_session(QuicSessionHandle::new(client_conn));

        // Send data from peer and drive poll_check_alive() until QuinnTunnelStream peeks 1 byte.
        s_send.write_all(b"Zhello").await.unwrap();
        let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while stream.read_buf.is_none() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for poll_check_alive to peek first byte"
            );
            assert_eq!(
                stream.poll_check_alive(&mut cx),
                Poll::Ready(HeartbeatStatus::Alive)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(stream.read_buf, Some(b'Z'));

        let before = stream.last_activity_ms().unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Zero-capacity ReadBuf must return Ready(Ok(())) without panicking, without
        // consuming the peeked byte, and without refreshing activity.
        let mut empty = [];
        let mut zero_buf = ReadBuf::new(&mut empty);
        assert!(matches!(
            Pin::new(&mut stream).poll_read(&mut cx, &mut zero_buf),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(stream.read_buf, Some(b'Z'));
        assert_eq!(stream.last_activity_ms(), Some(before));

        // Normal read must still receive the peeked byte followed by the rest.
        let mut out = [0u8; 6];
        stream.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, b"Zhello");

        drop((s_send, s_recv, server_conn));
    }

    #[tokio::test]
    async fn test_h3_stream_zero_capacity_read_buf_preserves_data_and_activity() {
        use std::collections::VecDeque;
        use std::time::Duration;

        struct OnceRecvStream(VecDeque<Bytes>);
        impl H3RecvStream for OnceRecvStream {
            async fn recv_chunk(&mut self) -> Result<Option<Bytes>, io::Error> {
                match self.0.pop_front() {
                    Some(b) => Ok(Some(b)),
                    None => {
                        std::future::pending::<()>().await;
                        Ok(None)
                    }
                }
            }
        }

        let send = MockSendStream {
            sent: vec![],
            finished: false,
        };
        let recv = OnceRecvStream(VecDeque::from([Bytes::from_static(b"hello")]));
        let mut stream = H3TunnelStream::spawn_split_pump(send, recv);

        // Wait until inbound task delivers chunk so poll_check_alive buffers it.
        let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while stream.read_buf.is_none() {
            assert!(tokio::time::Instant::now() < deadline);
            assert_eq!(
                stream.poll_check_alive(&mut cx),
                Poll::Ready(HeartbeatStatus::Alive)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let before = stream.last_activity_ms().unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;

        let mut empty = [];
        let mut zero_buf = ReadBuf::new(&mut empty);
        assert!(matches!(
            Pin::new(&mut stream).poll_read(&mut cx, &mut zero_buf),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(stream.last_activity_ms(), Some(before));

        let mut out = [0u8; 5];
        stream.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, b"hello");
    }
}
