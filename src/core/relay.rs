// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! High-performance bidirectional byte relay engine.
//! All comments and docstrings must be in English.

use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::task::Poll;
use tokio::io::ReadBuf;
use tokio_util::sync::CancellationToken;

use crate::consts::{DEFAULT_RELAY_DRAIN_TIMEOUT, RELAY_BUFFER_SIZE};
use crate::core::error::TunnelError;
use crate::core::stream::TunnelStream;

/// Directional transfer state for one half of the duplex relay.
struct TransferState {
    buffer: Vec<u8>,
    read_pos: usize,
    write_pos: usize,
    in_flight_chunk: Option<bytes::Bytes>,
    chunk_pos: usize,
    read_done: bool,
    write_shutdown: bool,
    total_bytes: u64,
}

impl TransferState {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![0u8; capacity],
            read_pos: 0,
            write_pos: 0,
            in_flight_chunk: None,
            chunk_pos: 0,
            read_done: false,
            write_shutdown: false,
            total_bytes: 0,
        }
    }

    fn is_finished(&self) -> bool {
        self.read_done
            && self.write_pos == self.read_pos
            && self.in_flight_chunk.is_none()
            && self.write_shutdown
    }
}

/// Relays bytes bidirectionally between two dynamic stream trait objects.
///
/// Uses an optimized buffer size (64 KB) and a `poll_fn` state machine to
/// concurrently drive RX and TX while checking cancellation, completely avoiding
/// the overhead and cancellation drops of `tokio::select!`.
pub struct RelayEngine;

impl RelayEngine {
    /// Copies data bidirectionally between stream_a and stream_b until EOF or error.
    pub async fn relay(
        stream_a: Box<dyn TunnelStream>,
        stream_b: Box<dyn TunnelStream>,
    ) -> Result<(u64, u64), TunnelError> {
        Self::relay_with_cancel(stream_a, stream_b, CancellationToken::new()).await
    }

    /// Copies data bidirectionally while monitoring a cancellation token in the same event loop.
    pub async fn relay_with_cancel(
        mut stream_a: Box<dyn TunnelStream>,
        mut stream_b: Box<dyn TunnelStream>,
        cancel_token: CancellationToken,
    ) -> Result<(u64, u64), TunnelError> {
        let mut a_to_b = TransferState::new(RELAY_BUFFER_SIZE);
        let mut b_to_a = TransferState::new(RELAY_BUFFER_SIZE);

        // Pre-create the cancellation future once on the stack to register wakers
        // and achieve zero-allocation, purely event-driven cancellation without polling.
        let mut cancel_fut = pin!(cancel_token.cancelled_owned());
        let mut drain_sleep: Option<Pin<Box<tokio::time::Sleep>>> = None;

        poll_fn(|cx| {
            let is_cancelled = cancel_fut.as_mut().poll(cx).is_ready();
            if is_cancelled {
                if drain_sleep.is_none() {
                    // Give at most bounded drain period to flush in-flight tails
                    drain_sleep = Some(Box::pin(tokio::time::sleep(DEFAULT_RELAY_DRAIN_TIMEOUT)));
                }
                if let Some(sleep_fut) = drain_sleep.as_mut()
                    && sleep_fut.as_mut().poll(cx).is_ready()
                {
                    // Hard bounded deadline reached: terminate relay immediately without hanging
                    return Poll::Ready(Ok((a_to_b.total_bytes, b_to_a.total_bytes)));
                }
            }

            // Drive A -> B transfer (RX direction)
            let mut progress = false;
            let res_a_to_b = Self::poll_transfer_step(
                cx,
                &mut stream_a,
                &mut stream_b,
                &mut a_to_b,
                is_cancelled,
                &mut progress,
            );
            if let Poll::Ready(Err(e)) = res_a_to_b {
                return Poll::Ready(Err(TunnelError::Io(e)));
            }

            // Drive B -> A transfer (TX direction)
            let res_b_to_a = Self::poll_transfer_step(
                cx,
                &mut stream_b,
                &mut stream_a,
                &mut b_to_a,
                is_cancelled,
                &mut progress,
            );
            if let Poll::Ready(Err(e)) = res_b_to_a {
                return Poll::Ready(Err(TunnelError::Io(e)));
            }

            // If both directions have finished all writes and shutdowns
            if a_to_b.is_finished() && b_to_a.is_finished() {
                return Poll::Ready(Ok((a_to_b.total_bytes, b_to_a.total_bytes)));
            }

            if progress {
                // Yield control to allow runtime cooperation
                cx.waker().wake_by_ref();
            }

            Poll::Pending
        })
        .await
    }

    fn poll_transfer_step(
        cx: &mut std::task::Context<'_>,
        reader: &mut Box<dyn TunnelStream>,
        writer: &mut Box<dyn TunnelStream>,
        state: &mut TransferState,
        force_stop: bool,
        progress: &mut bool,
    ) -> Poll<Result<(), std::io::Error>> {
        let consumes_owned = writer.consumes_owned_write();

        if consumes_owned {
            // Path A: Writer benefits from owned Bytes buffers (e.g. MuxStream)
            while let Some(ref chunk) = state.in_flight_chunk {
                let to_write = if state.chunk_pos == 0 {
                    chunk.clone()
                } else {
                    chunk.slice(state.chunk_pos..)
                };

                match writer.poll_write_bytes(cx, to_write) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            "writer returned zero bytes",
                        )));
                    }
                    Poll::Ready(Ok(n)) => {
                        state.chunk_pos += n;
                        state.total_bytes += n as u64;
                        *progress = true;
                        if state.chunk_pos >= chunk.len() {
                            state.in_flight_chunk = None;
                            state.chunk_pos = 0;
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => break,
                }
            }

            if !state.read_done && !force_stop && state.in_flight_chunk.is_none() {
                let mut read_buf = ReadBuf::new(&mut state.buffer);
                match Pin::new(&mut **reader).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let n = read_buf.filled().len();
                        if n == 0 {
                            state.read_done = true;
                        } else {
                            state.in_flight_chunk =
                                Some(bytes::Bytes::copy_from_slice(&state.buffer[..n]));
                            state.chunk_pos = 0;
                        }
                        *progress = true;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {}
                }
            }
        } else {
            // Path B: Standard byte streams (TCP, TLS, WS, H3, QUIC) - zero heap allocations in hot path
            while state.write_pos < state.read_pos {
                match Pin::new(&mut **writer)
                    .poll_write(cx, &state.buffer[state.write_pos..state.read_pos])
                {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            "writer returned zero bytes",
                        )));
                    }
                    Poll::Ready(Ok(n)) => {
                        state.write_pos += n;
                        state.total_bytes += n as u64;
                        *progress = true;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => break,
                }
            }

            if state.write_pos == state.read_pos {
                state.read_pos = 0;
                state.write_pos = 0;
            }

            if !state.read_done && !force_stop && state.read_pos == 0 {
                let mut read_buf = ReadBuf::new(&mut state.buffer);
                match Pin::new(&mut **reader).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let n = read_buf.filled().len();
                        if n == 0 {
                            state.read_done = true;
                        } else {
                            state.read_pos = n;
                        }
                        *progress = true;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {}
                }
            }
        }

        if force_stop {
            state.read_done = true;
        }

        // Step 3: Flush and Shutdown writer if reader is done and all bytes written
        let all_written = if consumes_owned {
            state.in_flight_chunk.is_none()
        } else {
            state.write_pos == state.read_pos
        };

        if state.read_done && all_written && !state.write_shutdown {
            match Pin::new(&mut **writer).poll_flush(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => {
                    tracing::debug!("Relay writer flush error during shutdown: {}", e);
                }
                Poll::Pending => return Poll::Pending,
            }

            if writer.supports_half_close() {
                match Pin::new(&mut **writer).poll_shutdown(cx) {
                    Poll::Ready(Ok(())) => {
                        state.write_shutdown = true;
                        *progress = true;
                    }
                    Poll::Ready(Err(e)) => {
                        tracing::debug!("Relay writer shutdown error: {}", e);
                        state.write_shutdown = true;
                        *progress = true;
                    }
                    Poll::Pending => {}
                }
            } else {
                state.write_shutdown = true;
                *progress = true;
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    #[tokio::test]
    async fn test_poll_fn_relay_engine() {
        let (client_app, client_side) = duplex(65536);
        let (server_app, server_side) = duplex(65536);

        // Run relay in a background task
        let relay_handle = tokio::spawn(async move {
            RelayEngine::relay(Box::new(client_side), Box::new(server_side)).await
        });

        // Write from client
        let mut client_app = client_app;
        client_app.write_all(b"request from client").await.unwrap();
        client_app.shutdown().await.unwrap();

        // Read at server
        let mut server_app = server_app;
        let mut buf = [0u8; 19];
        server_app.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"request from client");

        // Write from server
        server_app.write_all(b"response from server").await.unwrap();
        server_app.shutdown().await.unwrap();

        // Read at client
        let mut reply = [0u8; 20];
        client_app.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"response from server");

        let (bytes_a, bytes_b) = relay_handle.await.unwrap().unwrap();
        assert_eq!(bytes_a, 19);
        assert_eq!(bytes_b, 20);
    }

    #[tokio::test]
    async fn test_poll_fn_relay_engine_event_driven_cancellation() {
        let (_client_app, client_side) = duplex(65536);
        let (_server_app, server_side) = duplex(65536);

        let cancel_token = CancellationToken::new();
        let trigger_token = cancel_token.clone();

        let relay_handle = tokio::spawn(async move {
            RelayEngine::relay_with_cancel(
                Box::new(client_side),
                Box::new(server_side),
                cancel_token,
            )
            .await
        });

        // Ensure relay task is actively waiting/sleeping in Poll::Pending
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Cancel token: should immediately wake up the sleeping poll_fn without I/O
        trigger_token.cancel();

        // Must finish promptly without hanging
        let res = tokio::time::timeout(std::time::Duration::from_millis(500), relay_handle)
            .await
            .expect("Relay task did not wake up promptly upon cancellation");

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_relay_boxed_consumes_owned_write_delegation() {
        use bytes::Bytes;
        use std::io;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::Context;

        struct MockOwnedWriter {
            write_bytes_called: Arc<AtomicUsize>,
            poll_write_called: Arc<AtomicUsize>,
        }

        impl tokio::io::AsyncRead for MockOwnedWriter {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        impl tokio::io::AsyncWrite for MockOwnedWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                self.poll_write_called.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Ok(buf.len()))
            }

            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        impl TunnelStream for MockOwnedWriter {
            fn consumes_owned_write(&self) -> bool {
                true
            }

            fn poll_write_bytes(
                &mut self,
                _cx: &mut Context<'_>,
                buf: Bytes,
            ) -> Poll<io::Result<usize>> {
                self.write_bytes_called.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Ok(buf.len()))
            }

            fn poll_check_alive(
                &mut self,
                _cx: &mut Context<'_>,
            ) -> Poll<crate::core::stream::HeartbeatStatus> {
                Poll::Ready(crate::core::stream::HeartbeatStatus::Alive)
            }
        }

        let write_bytes_called = Arc::new(AtomicUsize::new(0));
        let poll_write_called = Arc::new(AtomicUsize::new(0));

        let mock = MockOwnedWriter {
            write_bytes_called: Arc::clone(&write_bytes_called),
            poll_write_called: Arc::clone(&poll_write_called),
        };

        let boxed: Box<dyn TunnelStream> = Box::new(mock);
        assert!(
            boxed.consumes_owned_write(),
            "Box<dyn TunnelStream> must forward consumes_owned_write"
        );

        let (mut client_app, client_side) = duplex(65536);
        let relay_handle =
            tokio::spawn(async move { RelayEngine::relay(Box::new(client_side), boxed).await });

        client_app
            .write_all(b"payload for owned writer")
            .await
            .unwrap();
        client_app.shutdown().await.unwrap();

        relay_handle.await.unwrap().unwrap();

        assert_eq!(
            poll_write_called.load(Ordering::SeqCst),
            0,
            "Must not call raw poll_write for owned writer"
        );
        assert!(
            write_bytes_called.load(Ordering::SeqCst) > 0,
            "Must call poll_write_bytes via Path A"
        );
    }
}
