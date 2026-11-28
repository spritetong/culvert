// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Pure data-plane `(Datagram, Stream)` / `(Stream, Datagram)` relay stage.
//! All comments and docstrings must be in English.

use crate::consts::RELAY_BUFFER_SIZE;
use crate::io::channel::{Datagram, DatagramIo};
use crate::io::stream::{
    HeartbeatStatus, StreamCaps, TunnelStream, activity_now_ms, heartbeat_timing,
};
use crate::v2::error::TaskError;
use crate::v2::relay::datagram_queue::{DATAGRAM_BATCH_SIZE, DatagramQueue};
use crate::v2::relay::duplex::RelayCounters;
use crate::v2::stage::{ActiveStage, ClosingStage};
use crate::v2::state::CloseReason;
use bytes::{Bytes, BytesMut};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::ReadBuf;
use tokio::time::Sleep;

/// Length-prefixed datagram <-> byte-stream relay implementing `ActiveStage`.
pub struct DatagramStreamRelay {
    dgram: Box<dyn DatagramIo>,
    stream: Box<dyn TunnelStream>,
    caps: StreamCaps,
    recv_batch: Vec<Datagram>,
    tx_scratch: BytesMut,
    in_flight_chunk: Option<Bytes>,
    in_flight_payload_bytes: u64,
    chunk_pos: usize,
    need_flush: bool,
    dgram_read_done: bool,
    stream_write_shutdown: bool,
    rx_buf: BytesMut,
    read_scratch: Vec<u8>,
    pending_dgrams: DatagramQueue,
    stream_read_done: bool,
    dgram_to_stream_bytes: u64,
    heartbeat_interval: Duration,
    idle_timeout: Duration,
    drain_timeout: Duration,
    tick_interval: Duration,
    pump_start_ms: u64,
    last_dgram_activity: Instant,
    heartbeat_timer: Pin<Box<Sleep>>,
    stream_is_first: bool,
    counters: Option<Arc<RelayCounters>>,
}

impl DatagramStreamRelay {
    /// Creates a new relay between `dgram` and `stream`.
    ///
    /// `stream_is_first` preserves `(frontend, backend)` direction ordering in `RelayCounters`.
    pub fn new(
        dgram: Box<dyn DatagramIo>,
        stream: Box<dyn TunnelStream>,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        drain_timeout: Duration,
        stream_is_first: bool,
    ) -> Self {
        let caps = stream.caps();
        let tick_interval = heartbeat_interval
            .min(idle_timeout / 2)
            .max(Duration::from_millis(50));
        Self {
            dgram,
            stream,
            caps,
            recv_batch: vec![Datagram::default(); DATAGRAM_BATCH_SIZE],
            tx_scratch: BytesMut::with_capacity(RELAY_BUFFER_SIZE),
            in_flight_chunk: None,
            in_flight_payload_bytes: 0,
            chunk_pos: 0,
            need_flush: false,
            dgram_read_done: false,
            stream_write_shutdown: false,
            rx_buf: BytesMut::with_capacity(RELAY_BUFFER_SIZE),
            read_scratch: vec![0u8; RELAY_BUFFER_SIZE],
            pending_dgrams: DatagramQueue::new(),
            stream_read_done: false,
            dgram_to_stream_bytes: 0,
            heartbeat_interval,
            idle_timeout,
            drain_timeout,
            tick_interval,
            pump_start_ms: activity_now_ms(),
            last_dgram_activity: Instant::now(),
            heartbeat_timer: Box::pin(tokio::time::sleep(tick_interval)),
            stream_is_first,
            counters: None,
        }
    }

    /// Attaches shared atomic counters.
    pub fn with_counters(mut self, counters: Arc<RelayCounters>) -> Self {
        self.counters = Some(counters);
        self
    }

    fn sync_counters(&self) {
        if let Some(ref c) = self.counters {
            let s2d = self.pending_dgrams.total_bytes();
            let (a_to_b, b_to_a) = if self.stream_is_first {
                (s2d, self.dgram_to_stream_bytes)
            } else {
                (self.dgram_to_stream_bytes, s2d)
            };
            c.a_to_b.store(a_to_b, std::sync::atomic::Ordering::Release);
            c.b_to_a.store(b_to_a, std::sync::atomic::Ordering::Release);
        }
    }

    fn has_pending_data(&self) -> bool {
        self.in_flight_chunk.is_some()
            || self.need_flush
            || !self.tx_scratch.is_empty()
            || !self.pending_dgrams.is_empty()
            || !self.rx_buf.is_empty()
    }

    fn check_heartbeats(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), TaskError>> {
        let pending_data = self.has_pending_data();
        if !pending_data && self.last_dgram_activity.elapsed() >= self.idle_timeout {
            return Poll::Ready(Ok(()));
        }
        if !self.stream_read_done {
            let now_ms = activity_now_ms();
            let effective_activity = self
                .stream
                .last_activity_ms()
                .map(|ts| ts.max(self.pump_start_ms));
            let (probe, expired) = heartbeat_timing(
                effective_activity,
                now_ms,
                self.heartbeat_interval,
                self.idle_timeout.saturating_add(self.tick_interval),
            );
            if !pending_data && expired {
                return Poll::Ready(Err(TaskError::StageFailed(
                    "datagram relay stream idle timeout".into(),
                )));
            }
            if probe && let Poll::Ready(Err(err)) = self.stream.poll_send_ping(cx) {
                return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
            }
            if let Poll::Ready(HeartbeatStatus::Dead) = self.stream.poll_check_alive(cx) {
                return Poll::Ready(Err(TaskError::StageFailed(
                    "datagram relay stream unresponsive".into(),
                )));
            }
        }
        Poll::Pending
    }

    fn poll_step_inner(
        &mut self,
        cx: &mut Context<'_>,
        drain_only: bool,
    ) -> Poll<Result<(), TaskError>> {
        let mut progress = false;

        // Direction 1: Datagram -> Stream
        while let Some(ref chunk) = self.in_flight_chunk {
            let write_res = if self.caps.owned_write {
                let to_write = if self.chunk_pos == 0 {
                    chunk.clone()
                } else {
                    chunk.slice(self.chunk_pos..)
                };
                self.stream.poll_write_owned(cx, to_write)
            } else {
                Pin::new(&mut *self.stream).poll_write(cx, &chunk[self.chunk_pos..])
            };

            match write_res {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(TaskError::StageFailed(
                        "stream writer returned zero bytes".into(),
                    )));
                }
                Poll::Ready(Ok(n)) => {
                    self.chunk_pos += n;
                    self.last_dgram_activity = Instant::now();
                    progress = true;
                    if self.chunk_pos >= chunk.len() {
                        self.dgram_to_stream_bytes += self.in_flight_payload_bytes;
                        self.in_flight_payload_bytes = 0;
                        self.in_flight_chunk = None;
                        self.chunk_pos = 0;
                        self.need_flush = true;
                    }
                }
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
                }
                Poll::Pending => break,
            }
        }

        if !self.dgram_read_done && !drain_only && self.in_flight_chunk.is_none() {
            match self.dgram.poll_recv(cx, &mut self.recv_batch) {
                Poll::Ready(Ok(0)) => {
                    self.dgram_read_done = true;
                    progress = true;
                }
                Poll::Ready(Ok(n)) => {
                    let payload_bytes = match DatagramQueue::encode_batch(
                        &mut self.tx_scratch,
                        &self.recv_batch[..n],
                    ) {
                        Ok(bytes) => bytes,
                        Err(err) => {
                            return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
                        }
                    };
                    if !self.tx_scratch.is_empty() {
                        self.in_flight_chunk = Some(self.tx_scratch.split().freeze());
                        self.in_flight_payload_bytes = payload_bytes;
                        self.chunk_pos = 0;
                        self.last_dgram_activity = Instant::now();
                        progress = true;
                    }
                }
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
                }
                Poll::Pending => {}
            }
        }

        if self.need_flush && self.in_flight_chunk.is_none() {
            match Pin::new(&mut *self.stream).poll_flush(cx) {
                Poll::Ready(Ok(())) => {
                    self.need_flush = false;
                    progress = true;
                }
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
                }
                Poll::Pending => {}
            }
        }

        if drain_only {
            self.dgram_read_done = true;
        }

        if self.dgram_read_done && self.in_flight_chunk.is_none() && !self.stream_write_shutdown {
            match Pin::new(&mut *self.stream).poll_flush(cx) {
                Poll::Ready(Ok(())) => {
                    self.need_flush = false;
                    if self.caps.half_close {
                        match Pin::new(&mut *self.stream).poll_shutdown(cx) {
                            Poll::Ready(Ok(())) => {
                                self.stream_write_shutdown = true;
                                progress = true;
                            }
                            Poll::Ready(Err(err)) => {
                                return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
                            }
                            Poll::Pending => {}
                        }
                    } else {
                        self.stream_write_shutdown = true;
                        progress = true;
                    }
                }
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
                }
                Poll::Pending => {}
            }
        }

        // Direction 2: Stream -> Datagram
        if let Err(err) = self.pending_dgrams.decode_frames(&mut self.rx_buf) {
            return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
        }

        if !self.stream_read_done && !drain_only && self.pending_dgrams.can_accept_stream_frame() {
            let mut read_buf = ReadBuf::new(&mut self.read_scratch);
            match Pin::new(&mut *self.stream).poll_read(cx, &mut read_buf) {
                Poll::Ready(Ok(())) => {
                    let filled = read_buf.filled();
                    if filled.is_empty() {
                        self.stream_read_done = true;
                    } else {
                        self.rx_buf.extend_from_slice(filled);
                        if let Err(err) = self.pending_dgrams.decode_frames(&mut self.rx_buf) {
                            return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
                        }
                    }
                    progress = true;
                }
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
                }
                Poll::Pending => {}
            }
        }

        match self.pending_dgrams.poll_flush(cx, &mut *self.dgram) {
            Ok(true) => {
                self.last_dgram_activity = Instant::now();
                progress = true;
            }
            Ok(false) => {}
            Err(err) => return Poll::Ready(Err(err)),
        }

        if (self.stream_read_done || drain_only) && self.pending_dgrams.is_empty() {
            match self.pending_dgrams.decode_frames(&mut self.rx_buf) {
                Ok(true) => progress = true,
                Ok(false) => {}
                Err(err) => return Poll::Ready(Err(TaskError::StageFailed(err.to_string()))),
            }
            if self.stream_read_done && self.pending_dgrams.is_empty() && !self.rx_buf.is_empty() {
                return Poll::Ready(Err(TaskError::StageFailed(
                    "truncated UDP datagram frame at end of stream".into(),
                )));
            }
        }

        if self.dgram_read_done
            && self.stream_write_shutdown
            && self.pending_dgrams.is_empty()
            && (self.stream_read_done || drain_only)
        {
            if !self.rx_buf.is_empty() {
                return Poll::Ready(Err(TaskError::StageFailed(
                    "incomplete UDP datagram frame left in buffer during drain".into(),
                )));
            }
            return Poll::Ready(Ok(()));
        }

        if progress {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

impl ActiveStage for DatagramStreamRelay {
    type Closing = DatagramStreamClosing;

    fn poll_drive(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), TaskError>> {
        let res = self.poll_step_inner(cx, false);
        if res.is_ready() {
            self.sync_counters();
            return res;
        }

        if self.heartbeat_timer.as_mut().poll(cx).is_ready() {
            self.heartbeat_timer
                .as_mut()
                .reset(tokio::time::Instant::now() + self.tick_interval);
            let _ = self.heartbeat_timer.as_mut().poll(cx);
            if let Poll::Ready(hb_res) = self.check_heartbeats(cx) {
                self.sync_counters();
                return Poll::Ready(hb_res);
            }
        }

        Poll::Pending
    }

    fn into_closing(self, reason: CloseReason) -> Self::Closing {
        self.sync_counters();
        let drain_timeout = self.drain_timeout;
        DatagramStreamClosing {
            relay: Some(self),
            reason,
            drain_timeout,
        }
    }
}

/// Bounded graceful-drain stage for `DatagramStreamRelay`.
pub struct DatagramStreamClosing {
    relay: Option<DatagramStreamRelay>,
    reason: CloseReason,
    drain_timeout: Duration,
}

impl ClosingStage for DatagramStreamClosing {
    fn run(&mut self) -> impl Future<Output = Result<(), TaskError>> + Send {
        let relay = self.relay.take();
        let reason = self.reason;
        let drain_timeout = self.drain_timeout;

        super::run_bounded_closing(
            relay,
            reason,
            drain_timeout,
            |r, cx| r.poll_step_inner(cx, true),
            DatagramStreamRelay::sync_counters,
        )
    }
}
