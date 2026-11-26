// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! I/O strategy plugins. Core selects one by `(frontend.kind(), backend.kind())`
//! and never moves bytes itself.
//! All comments and docstrings must be in English.

use crate::io::channel::{ChannelKind, Datagram, DatagramIo, IoChannel};
use crate::io::error::TunnelError;
use crate::io::stream::{
    BoxFuture, HeartbeatStatus, StreamCaps, TunnelStream, activity_now_ms, heartbeat_timing,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::pin::{Pin, pin};
use std::sync::{Arc, OnceLock, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::ReadBuf;
use tokio_util::sync::CancellationToken;

use crate::consts::{DEFAULT_RELAY_DRAIN_TIMEOUT, RELAY_BUFFER_SIZE};

/// The two channels whose contents a `Pump` transfers.
pub struct PumpRequest {
    pub frontend: IoChannel,
    pub backend: IoChannel,
}

impl PumpRequest {
    pub fn new(frontend: impl Into<IoChannel>, backend: impl Into<IoChannel>) -> Self {
        Self {
            frontend: frontend.into(),
            backend: backend.into(),
        }
    }
}

/// Runtime context shared by every pump.
pub struct PumpCtx {
    pub cancel: CancellationToken,
    pub heartbeat_interval: Duration,
    pub idle_timeout: Duration,
}

/// An I/O strategy for one channel-kind pair.
pub trait Pump: Send + Sync + 'static {
    fn run<'a>(
        &'a self,
        req: PumpRequest,
        ctx: PumpCtx,
    ) -> BoxFuture<'a, Result<(u64, u64), TunnelError>>;
}

/// Process-wide pump table. `register` is the extension point for new frontend
/// capabilities: a datagram frontend adds its pumps here, not in `core::serve`.
pub struct PumpRegistry {
    pumps: RwLock<HashMap<(ChannelKind, ChannelKind), Arc<dyn Pump>>>,
}

impl PumpRegistry {
    pub fn new() -> Self {
        Self {
            pumps: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, pair: (ChannelKind, ChannelKind), pump: Arc<dyn Pump>) {
        self.pumps
            .write()
            .expect("pump registry lock poisoned")
            .insert(pair, pump);
    }

    pub fn lookup(&self, pair: (ChannelKind, ChannelKind)) -> Option<Arc<dyn Pump>> {
        self.pumps
            .read()
            .expect("pump registry lock poisoned")
            .get(&pair)
            .cloned()
    }
}

impl Default for PumpRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns the process-wide registry, seeded with stream/stream, datagram/stream,
/// stream/datagram, and datagram/datagram pumps.
pub fn pump_registry() -> &'static PumpRegistry {
    static REGISTRY: OnceLock<PumpRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let registry = PumpRegistry::new();
        registry.register(
            (ChannelKind::Stream, ChannelKind::Stream),
            Arc::new(StreamStreamPump),
        );
        registry.register(
            (ChannelKind::Datagram, ChannelKind::Stream),
            Arc::new(DatagramStreamPump),
        );
        registry.register(
            (ChannelKind::Stream, ChannelKind::Datagram),
            Arc::new(StreamDatagramPump),
        );
        registry.register(
            (ChannelKind::Datagram, ChannelKind::Datagram),
            Arc::new(DatagramDatagramPump),
        );
        registry
    })
}

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

/// The `(Stream, Stream)` pump. Its body is the hand-written `poll_fn` duplex
/// state machine; replacing it with `tokio::select!` is a measured regression.
pub struct StreamStreamPump;

impl Pump for StreamStreamPump {
    fn run<'a>(
        &'a self,
        req: PumpRequest,
        ctx: PumpCtx,
    ) -> BoxFuture<'a, Result<(u64, u64), TunnelError>> {
        Box::pin(async move {
            let frontend = req.frontend.into_stream()?;
            let backend = req.backend.into_stream()?;
            Self::relay_with_heartbeat(
                frontend,
                backend,
                ctx.cancel,
                ctx.heartbeat_interval,
                ctx.idle_timeout,
            )
            .await
        })
    }
}

impl StreamStreamPump {
    /// Copies data bidirectionally while driving activity-gated probes and monitoring cancellation.
    ///
    /// A PING is only emitted when neither direction has moved anything for one
    /// `heartbeat_interval`; any RX (including a PING) or real TX counts as activity.
    /// PONG is never awaited. A stream that stays silent for `idle_timeout` is dead.
    pub async fn relay_with_heartbeat(
        mut stream_a: Box<dyn TunnelStream>,
        mut stream_b: Box<dyn TunnelStream>,
        cancel_token: CancellationToken,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
    ) -> Result<(u64, u64), TunnelError> {
        // Capability queries happen once per direction, outside the hot loop.
        let caps_a = stream_a.caps();
        let caps_b = stream_b.caps();

        let mut a_to_b = TransferState::new(RELAY_BUFFER_SIZE);
        let mut b_to_a = TransferState::new(RELAY_BUFFER_SIZE);

        // Pre-create the cancellation future once on the stack to register wakers
        // and achieve zero-allocation, purely event-driven cancellation without polling.
        let mut cancel_fut = pin!(cancel_token.cancelled_owned());
        let mut drain_sleep: Option<Pin<Box<tokio::time::Sleep>>> = None;
        let tick_interval = std::cmp::min(heartbeat_interval / 2, Duration::from_millis(500))
            .max(Duration::from_millis(5));
        let mut heartbeat_timer = Box::pin(tokio::time::sleep(tick_interval));

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

            // Activity-gated probe & liveness check
            if heartbeat_timer.as_mut().poll(cx).is_ready() {
                heartbeat_timer
                    .as_mut()
                    .reset(tokio::time::Instant::now() + tick_interval);

                let now_ms = activity_now_ms();
                let (probe_a, _) = heartbeat_timing(
                    stream_a.last_activity_ms(),
                    now_ms,
                    heartbeat_interval,
                    idle_timeout,
                );
                let (probe_b, _) = heartbeat_timing(
                    stream_b.last_activity_ms(),
                    now_ms,
                    heartbeat_interval,
                    idle_timeout,
                );

                if !a_to_b.read_done
                    && probe_a
                    && let Poll::Ready(Err(e)) = stream_a.poll_send_ping(cx)
                {
                    return Poll::Ready(Err(TunnelError::Io(e)));
                }
                if !b_to_a.read_done
                    && probe_b
                    && let Poll::Ready(Err(e)) = stream_b.poll_send_ping(cx)
                {
                    return Poll::Ready(Err(TunnelError::Io(e)));
                }

                if !a_to_b.read_done
                    && let Poll::Ready(HeartbeatStatus::Dead) = stream_a.poll_check_alive(cx)
                {
                    return Poll::Ready(Err(TunnelError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Relay stream A unresponsive to heartbeats",
                    ))));
                }
                if !b_to_a.read_done
                    && let Poll::Ready(HeartbeatStatus::Dead) = stream_b.poll_check_alive(cx)
                {
                    return Poll::Ready(Err(TunnelError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Relay stream B unresponsive to heartbeats",
                    ))));
                }

                // Re-evaluate idle expiration after probes/pings have had an opportunity to touch activity clock
                let (_, expired_a) = heartbeat_timing(
                    stream_a.last_activity_ms(),
                    now_ms,
                    heartbeat_interval,
                    idle_timeout,
                );
                let (_, expired_b) = heartbeat_timing(
                    stream_b.last_activity_ms(),
                    now_ms,
                    heartbeat_interval,
                    idle_timeout,
                );

                if !a_to_b.read_done && expired_a {
                    return Poll::Ready(Err(TunnelError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Relay stream A idle timeout: no inbound or outbound activity",
                    ))));
                }
                if !b_to_a.read_done && expired_b {
                    return Poll::Ready(Err(TunnelError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Relay stream B idle timeout: no inbound or outbound activity",
                    ))));
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
                caps_b,
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
                caps_a,
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
        cx: &mut Context<'_>,
        reader: &mut Box<dyn TunnelStream>,
        writer: &mut Box<dyn TunnelStream>,
        state: &mut TransferState,
        force_stop: bool,
        progress: &mut bool,
        caps: StreamCaps,
    ) -> Poll<Result<(), std::io::Error>> {
        if caps.owned_write {
            // Path A: Writer retains owned Bytes buffers with no second copy.
            while let Some(ref chunk) = state.in_flight_chunk {
                let to_write = if state.chunk_pos == 0 {
                    chunk.clone()
                } else {
                    chunk.slice(state.chunk_pos..)
                };

                match writer.poll_write_owned(cx, to_write) {
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
            // Path B: Standard byte streams (TCP, TLS, QUIC) - zero heap allocations in hot path
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
        let all_written = if caps.owned_write {
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

            if caps.half_close {
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

const DATAGRAM_BATCH_SIZE: usize = 4;
const MAX_PENDING_DATAGRAMS: usize = 64;
const MAX_PENDING_DATAGRAM_BYTES: usize = 256 * 1024;


/// Decodes complete length-prefixed datagrams without exceeding the pending queue bound.
fn decode_pending_datagrams(
    rx_buf: &mut BytesMut,
    pending: &mut VecDeque<Datagram>,
    byte_counter: &mut u64,
    pending_bytes: &mut usize,
) -> std::io::Result<()> {
    while pending.len() < MAX_PENDING_DATAGRAMS && rx_buf.len() >= 2 {
        let frame_len = u16::from_be_bytes([rx_buf[0], rx_buf[1]]) as usize;
        if frame_len > crate::consts::MAX_UDP_DATAGRAM_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "UDP datagram exceeds maximum payload size",
            ));
        }
        if *pending_bytes + frame_len > MAX_PENDING_DATAGRAM_BYTES {
            break;
        }
        if rx_buf.len() < 2 + frame_len {
            break;
        }

        rx_buf.advance(2);
        let payload = rx_buf.split_to(frame_len).freeze();
        *byte_counter += frame_len as u64;
        *pending_bytes += frame_len;
        pending.push_back(Datagram {
            flow: Default::default(),
            payload,
        });
    }
    Ok(())
}

/// The `(Datagram, Stream)` pump framing datagrams over a length-prefixed byte stream.
///
/// Each datagram is encoded on the wire as `[u16 payload_len (big-endian) | payload bytes]`.
pub struct DatagramStreamPump;

impl Pump for DatagramStreamPump {
    fn run<'a>(
        &'a self,
        req: PumpRequest,
        ctx: PumpCtx,
    ) -> BoxFuture<'a, Result<(u64, u64), TunnelError>> {
        Box::pin(async move {
            let dgram = req.frontend.into_datagram()?;
            let stream = req.backend.into_stream()?;
            Self::relay_datagram_stream(
                dgram,
                stream,
                ctx.cancel,
                ctx.heartbeat_interval,
                ctx.idle_timeout,
            )
            .await
        })
    }
}

/// The `(Stream, Datagram)` pump delegating to `DatagramStreamPump`.
pub struct StreamDatagramPump;

impl Pump for StreamDatagramPump {
    fn run<'a>(
        &'a self,
        req: PumpRequest,
        ctx: PumpCtx,
    ) -> BoxFuture<'a, Result<(u64, u64), TunnelError>> {
        Box::pin(async move {
            let stream = req.frontend.into_stream()?;
            let dgram = req.backend.into_datagram()?;
            let (d2s, s2d) = DatagramStreamPump::relay_datagram_stream(
                dgram,
                stream,
                ctx.cancel,
                ctx.heartbeat_interval,
                ctx.idle_timeout,
            )
            .await?;
            Ok((s2d, d2s))
        })
    }
}

/// The `(Datagram, Datagram)` pump forwarding batched datagrams between two datagram channels.
pub struct DatagramDatagramPump;

impl Pump for DatagramDatagramPump {
    fn run<'a>(
        &'a self,
        req: PumpRequest,
        ctx: PumpCtx,
    ) -> BoxFuture<'a, Result<(u64, u64), TunnelError>> {
        Box::pin(async move {
            let mut a = req.frontend.into_datagram()?;
            let mut b = req.backend.into_datagram()?;
            let mut batch_a = vec![Datagram::default(); DATAGRAM_BATCH_SIZE];
            let mut batch_b = vec![Datagram::default(); DATAGRAM_BATCH_SIZE];
            let mut queue_a_to_b: VecDeque<Datagram> = VecDeque::with_capacity(DATAGRAM_BATCH_SIZE);
            let mut queue_b_to_a: VecDeque<Datagram> = VecDeque::with_capacity(DATAGRAM_BATCH_SIZE);
            let mut queue_a_to_b_bytes = 0usize;
            let mut queue_b_to_a_bytes = 0usize;
            let mut bytes_a_to_b = 0u64;
            let mut bytes_b_to_a = 0u64;
            let mut a_done = false;
            let mut b_done = false;

            let tick_interval = ctx
                .heartbeat_interval
                .min(ctx.idle_timeout / 2)
                .max(Duration::from_millis(50));
            let mut last_activity = Instant::now();
            let mut cancel_fut = pin!(ctx.cancel.cancelled_owned());
            let mut drain_sleep: Option<Pin<Box<tokio::time::Sleep>>> = None;
            let mut idle_timer = Box::pin(tokio::time::sleep(tick_interval));

            poll_fn(|cx| {
                let is_cancelled = cancel_fut.as_mut().poll(cx).is_ready();
                if is_cancelled {
                    if drain_sleep.is_none() {
                        drain_sleep =
                            Some(Box::pin(tokio::time::sleep(DEFAULT_RELAY_DRAIN_TIMEOUT)));
                    }
                    if let Some(sleep_fut) = drain_sleep.as_mut()
                        && sleep_fut.as_mut().poll(cx).is_ready()
                    {
                        return Poll::Ready(Ok((bytes_a_to_b, bytes_b_to_a)));
                    }
                }

                if idle_timer.as_mut().poll(cx).is_ready() {
                    if last_activity.elapsed() >= ctx.idle_timeout {
                        return Poll::Ready(Ok((bytes_a_to_b, bytes_b_to_a)));
                    }
                    idle_timer
                        .as_mut()
                        .reset(tokio::time::Instant::now() + tick_interval);
                }

                let mut progress = false;

                // Flush queues
                if !queue_a_to_b.is_empty() {
                    match b.poll_send(cx, queue_a_to_b.make_contiguous()) {
                        Poll::Ready(Ok(n)) if n > 0 => {
                            let sent_bytes: usize = queue_a_to_b.iter().take(n).map(|d| d.payload.len()).sum();
                            queue_a_to_b.drain(..n);
                            queue_a_to_b_bytes -= sent_bytes;
                            last_activity = Instant::now();
                            progress = true;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(TunnelError::Io(e))),
                        _ => {}
                    }
                }
                if !queue_b_to_a.is_empty() {
                    match a.poll_send(cx, queue_b_to_a.make_contiguous()) {
                        Poll::Ready(Ok(n)) if n > 0 => {
                            let sent_bytes: usize = queue_b_to_a.iter().take(n).map(|d| d.payload.len()).sum();
                            queue_b_to_a.drain(..n);
                            queue_b_to_a_bytes -= sent_bytes;
                            last_activity = Instant::now();
                            progress = true;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(TunnelError::Io(e))),
                        _ => {}
                    }
                }

                // Receive from A
                if !a_done
                    && !is_cancelled
                    && queue_a_to_b.len() < MAX_PENDING_DATAGRAMS
                    && queue_a_to_b_bytes + crate::consts::MAX_UDP_DATAGRAM_SIZE
                        <= MAX_PENDING_DATAGRAM_BYTES
                {
                    let remaining = MAX_PENDING_DATAGRAMS - queue_a_to_b.len();
                    let batch_len = remaining.min(batch_a.len());
                    match a.poll_recv(cx, &mut batch_a[..batch_len]) {
                        Poll::Ready(Ok(0)) => {
                            a_done = true;
                            progress = true;
                        }
                        Poll::Ready(Ok(n)) => {
                            for d in batch_a.iter().take(n) {
                                let payload_len = d.payload.len();
                                if queue_a_to_b_bytes + payload_len > MAX_PENDING_DATAGRAM_BYTES {
                                    continue;
                                }
                                bytes_a_to_b += payload_len as u64;
                                queue_a_to_b_bytes += payload_len;
                                queue_a_to_b.push_back(d.clone());
                            }
                            last_activity = Instant::now();
                            progress = true;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(TunnelError::Io(e))),
                        Poll::Pending => {}
                    }
                }

                // Receive from B
                if !b_done
                    && !is_cancelled
                    && queue_b_to_a.len() < MAX_PENDING_DATAGRAMS
                    && queue_b_to_a_bytes + crate::consts::MAX_UDP_DATAGRAM_SIZE
                        <= MAX_PENDING_DATAGRAM_BYTES
                {
                    let remaining = MAX_PENDING_DATAGRAMS - queue_b_to_a.len();
                    let batch_len = remaining.min(batch_b.len());
                    match b.poll_recv(cx, &mut batch_b[..batch_len]) {
                        Poll::Ready(Ok(0)) => {
                            b_done = true;
                            progress = true;
                        }
                        Poll::Ready(Ok(n)) => {
                            for d in batch_b.iter().take(n) {
                                let payload_len = d.payload.len();
                                if queue_b_to_a_bytes + payload_len > MAX_PENDING_DATAGRAM_BYTES {
                                    continue;
                                }
                                bytes_b_to_a += payload_len as u64;
                                queue_b_to_a_bytes += payload_len;
                                queue_b_to_a.push_back(d.clone());
                            }
                            last_activity = Instant::now();
                            progress = true;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(TunnelError::Io(e))),
                        Poll::Pending => {}
                    }
                }

                if ((a_done && queue_a_to_b.is_empty()) || (b_done && queue_b_to_a.is_empty()))
                    || (is_cancelled && queue_a_to_b.is_empty() && queue_b_to_a.is_empty())
                {
                    return Poll::Ready(Ok((bytes_a_to_b, bytes_b_to_a)));
                }

                if progress {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            })
            .await
        })
    }
}

impl DatagramStreamPump {
    /// Relays length-framed datagrams bidirectionally between a `DatagramIo` channel and a `TunnelStream`.
    pub async fn relay_datagram_stream(
        mut dgram: Box<dyn DatagramIo>,
        mut stream: Box<dyn TunnelStream>,
        cancel_token: CancellationToken,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
    ) -> Result<(u64, u64), TunnelError> {
        let caps = stream.caps();
        let mut recv_batch = vec![Datagram::default(); DATAGRAM_BATCH_SIZE];
        let mut tx_scratch = BytesMut::with_capacity(RELAY_BUFFER_SIZE);
        let mut in_flight_chunk: Option<Bytes> = None;
        let mut chunk_pos: usize = 0;
        let mut need_flush = false;
        let mut dgram_read_done = false;
        let mut stream_write_shutdown = false;

        let mut rx_buf = BytesMut::with_capacity(RELAY_BUFFER_SIZE);
        let mut read_scratch = vec![0u8; RELAY_BUFFER_SIZE];
        let mut pending_dgrams: VecDeque<Datagram> = VecDeque::with_capacity(DATAGRAM_BATCH_SIZE);
        let mut pending_dgram_bytes = 0usize;
        let mut stream_read_done = false;

        let mut dgram_to_stream_bytes: u64 = 0;
        let mut stream_to_dgram_bytes: u64 = 0;

        let tick_interval = heartbeat_interval
            .min(idle_timeout / 2)
            .max(Duration::from_millis(50));
        let pump_start_ms = activity_now_ms();
        let mut last_dgram_activity = Instant::now();

        let mut cancel_fut = pin!(cancel_token.cancelled_owned());
        let mut drain_sleep: Option<Pin<Box<tokio::time::Sleep>>> = None;
        let mut heartbeat_timer = Box::pin(tokio::time::sleep(tick_interval));

        poll_fn(|cx| {
            let is_cancelled = cancel_fut.as_mut().poll(cx).is_ready();
            if is_cancelled {
                if drain_sleep.is_none() {
                    drain_sleep = Some(Box::pin(tokio::time::sleep(DEFAULT_RELAY_DRAIN_TIMEOUT)));
                }
                if let Some(sleep_fut) = drain_sleep.as_mut()
                    && sleep_fut.as_mut().poll(cx).is_ready()
                {
                    return Poll::Ready(Ok((dgram_to_stream_bytes, stream_to_dgram_bytes)));
                }
            }

            if heartbeat_timer.as_mut().poll(cx).is_ready() {
                heartbeat_timer
                    .as_mut()
                    .reset(tokio::time::Instant::now() + tick_interval);

                if last_dgram_activity.elapsed() >= idle_timeout {
                    return Poll::Ready(Ok((dgram_to_stream_bytes, stream_to_dgram_bytes)));
                }

                let now_ms = activity_now_ms();
                let effective_activity = stream.last_activity_ms().map(|ts| ts.max(pump_start_ms));
                let (probe, expired) = heartbeat_timing(
                    effective_activity,
                    now_ms,
                    heartbeat_interval,
                    idle_timeout.saturating_add(tick_interval),
                );
                if expired {
                    return Poll::Ready(Err(TunnelError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Datagram relay tunnel stream idle timeout",
                    ))));
                }
                if probe && let Poll::Ready(Err(e)) = stream.poll_send_ping(cx) {
                    return Poll::Ready(Err(TunnelError::Io(e)));
                }
                if let Poll::Ready(HeartbeatStatus::Dead) = stream.poll_check_alive(cx) {
                    return Poll::Ready(Err(TunnelError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Datagram relay tunnel stream unresponsive to heartbeats",
                    ))));
                }
            }

            let mut progress = false;

            // --- Direction 1: Datagram -> Stream (write encoded frames) ---
            while let Some(ref chunk) = in_flight_chunk {
                let write_res = if caps.owned_write {
                    let to_write = if chunk_pos == 0 {
                        chunk.clone()
                    } else {
                        chunk.slice(chunk_pos..)
                    };
                    stream.poll_write_owned(cx, to_write)
                } else {
                    Pin::new(&mut *stream).poll_write(cx, &chunk[chunk_pos..])
                };

                match write_res {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(TunnelError::Io(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            "stream writer returned zero bytes",
                        ))));
                    }
                    Poll::Ready(Ok(n)) => {
                        chunk_pos += n;
                        progress = true;
                        if chunk_pos >= chunk.len() {
                            in_flight_chunk = None;
                            chunk_pos = 0;
                            need_flush = true;
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(TunnelError::Io(e))),
                    Poll::Pending => break,
                }
            }

            if !dgram_read_done && !is_cancelled && in_flight_chunk.is_none() {
                match dgram.poll_recv(cx, &mut recv_batch) {
                    Poll::Ready(Ok(0)) => {
                        dgram_read_done = true;
                        progress = true;
                    }
                    Poll::Ready(Ok(n)) => {
                        tx_scratch.clear();
                        for d in recv_batch.iter().take(n) {
                            let plen = d.payload.len();
                            if plen > crate::consts::MAX_UDP_DATAGRAM_SIZE {
                                return Poll::Ready(Err(TunnelError::Io(std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    "UDP datagram exceeds maximum payload size",
                                ))));
                            }
                            if tx_scratch.len() + 2 + plen > MAX_PENDING_DATAGRAM_BYTES {
                                continue;
                            }
                            tx_scratch.put_u16(plen as u16);
                            tx_scratch.extend_from_slice(&d.payload);
                            dgram_to_stream_bytes += plen as u64;
                        }
                        if !tx_scratch.is_empty() {
                            in_flight_chunk = Some(tx_scratch.split().freeze());
                            chunk_pos = 0;
                            last_dgram_activity = Instant::now();
                            progress = true;
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(TunnelError::Io(e))),
                    Poll::Pending => {}
                }
            }

            if need_flush && in_flight_chunk.is_none() {
                match Pin::new(&mut *stream).poll_flush(cx) {
                    Poll::Ready(Ok(())) => {
                        need_flush = false;
                        progress = true;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(TunnelError::Io(e))),
                    Poll::Pending => {}
                }
            }

            if is_cancelled {
                dgram_read_done = true;
            }

            if dgram_read_done && in_flight_chunk.is_none() && !stream_write_shutdown {
                match Pin::new(&mut *stream).poll_flush(cx) {
                    Poll::Ready(Ok(())) => {
                        need_flush = false;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(TunnelError::Io(e))),
                    Poll::Pending => return Poll::Pending,
                }
                if caps.half_close {
                    match Pin::new(&mut *stream).poll_shutdown(cx) {
                        Poll::Ready(Ok(())) => {
                            stream_write_shutdown = true;
                            progress = true;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(TunnelError::Io(e))),
                        Poll::Pending => {}
                    }
                } else {
                    stream_write_shutdown = true;
                    progress = true;
                }
            }

            // --- Direction 2: Stream -> Datagram (read & decode frames) ---
            if let Err(e) = decode_pending_datagrams(
                &mut rx_buf,
                &mut pending_dgrams,
                &mut stream_to_dgram_bytes,
                &mut pending_dgram_bytes,
            ) {
                return Poll::Ready(Err(TunnelError::Io(e)));
            }

            if !stream_read_done
                && !is_cancelled
                && pending_dgrams.len() < MAX_PENDING_DATAGRAMS
                && pending_dgram_bytes + crate::consts::MAX_UDP_DATAGRAM_SIZE
                    <= MAX_PENDING_DATAGRAM_BYTES
            {
                let mut read_buf = ReadBuf::new(&mut read_scratch);
                match Pin::new(&mut *stream).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let filled = read_buf.filled();
                        if filled.is_empty() {
                            stream_read_done = true;
                        } else {
                            rx_buf.extend_from_slice(filled);
                            if let Err(e) = decode_pending_datagrams(
                                &mut rx_buf,
                                &mut pending_dgrams,
                                &mut stream_to_dgram_bytes,
                                &mut pending_dgram_bytes,
                            ) {
                                return Poll::Ready(Err(TunnelError::Io(e)));
                            }
                        }
                        progress = true;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(TunnelError::Io(e))),
                    Poll::Pending => {}
                }
            }

            while !pending_dgrams.is_empty() {
                match dgram.poll_send(cx, pending_dgrams.make_contiguous()) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(TunnelError::Io(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            "datagram sender returned zero sent",
                        ))));
                    }
                    Poll::Ready(Ok(sent)) => {
                        let sent_bytes: usize = pending_dgrams.iter().take(sent).map(|d| d.payload.len()).sum();
                        pending_dgrams.drain(..sent);
                        pending_dgram_bytes -= sent_bytes;
                        last_dgram_activity = Instant::now();
                        progress = true;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(TunnelError::Io(e))),
                    Poll::Pending => break,
                }
            }

            if stream_read_done && pending_dgrams.is_empty() {
                // There may be complete frames left in rx_buf because the byte
                // budget prevented decoding them while the previous batch was queued.
                if let Err(e) = decode_pending_datagrams(
                    &mut rx_buf,
                    &mut pending_dgrams,
                    &mut stream_to_dgram_bytes,
                    &mut pending_dgram_bytes,
                ) {
                    return Poll::Ready(Err(TunnelError::Io(e)));
                }
                if pending_dgrams.is_empty() && !rx_buf.is_empty() {
                    return Poll::Ready(Err(TunnelError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "truncated UDP datagram frame at end of stream",
                    ))));
                }
            }

            // A stream read-side EOF is only a half-close: keep forwarding UDP
            // datagrams until the datagram side also finishes and the stream
            // write side has been flushed and shut down.
            if dgram_read_done
                && stream_write_shutdown
                && pending_dgrams.is_empty()
                && (stream_read_done || is_cancelled)
            {
                return Poll::Ready(Ok((dgram_to_stream_bytes, stream_to_dgram_bytes)));
            }

            if progress {
                cx.waker().wake_by_ref();
            }

            Poll::Pending
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::{DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT};
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Context;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    async fn relay(
        a: Box<dyn TunnelStream>,
        b: Box<dyn TunnelStream>,
    ) -> Result<(u64, u64), TunnelError> {
        StreamStreamPump::relay_with_heartbeat(
            a,
            b,
            CancellationToken::new(),
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
        )
        .await
    }

    #[tokio::test]
    async fn test_poll_fn_relay_engine() {
        let (client_app, client_side) = duplex(65536);
        let (server_app, server_side) = duplex(65536);

        let relay_handle =
            tokio::spawn(async move { relay(Box::new(client_side), Box::new(server_side)).await });

        let mut client_app = client_app;
        client_app.write_all(b"request from client").await.unwrap();
        client_app.shutdown().await.unwrap();

        let mut server_app = server_app;
        let mut buf = [0u8; 19];
        server_app.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"request from client");

        server_app.write_all(b"response from server").await.unwrap();
        server_app.shutdown().await.unwrap();

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
            StreamStreamPump::relay_with_heartbeat(
                Box::new(client_side),
                Box::new(server_side),
                cancel_token,
                DEFAULT_HEARTBEAT_INTERVAL,
                DEFAULT_IDLE_TIMEOUT,
            )
            .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        trigger_token.cancel();

        let res = tokio::time::timeout(std::time::Duration::from_millis(500), relay_handle)
            .await
            .expect("Relay task did not wake up promptly upon cancellation");

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_iochannel_forwards_owned_write_capability() {
        use bytes::Bytes;

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
            fn caps(&self) -> StreamCaps {
                StreamCaps {
                    owned_write: true,
                    half_close: true,
                }
            }

            fn poll_write_owned(
                &mut self,
                _cx: &mut Context<'_>,
                buf: Bytes,
            ) -> Poll<io::Result<usize>> {
                self.write_bytes_called.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Ok(buf.len()))
            }

            fn poll_check_alive(&mut self, _cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
                Poll::Ready(HeartbeatStatus::Alive)
            }
        }

        let write_bytes_called = Arc::new(AtomicUsize::new(0));
        let poll_write_called = Arc::new(AtomicUsize::new(0));

        let mock = MockOwnedWriter {
            write_bytes_called: Arc::clone(&write_bytes_called),
            poll_write_called: Arc::clone(&poll_write_called),
        };

        let boxed: Box<dyn TunnelStream> = Box::new(mock);
        assert!(boxed.caps().owned_write);
        let channel = IoChannel::from_boxed_stream(boxed);
        assert!(channel.caps().owned_write, "IoChannel must forward caps()");

        let (mut client_app, client_side) = duplex(65536);
        let relay_handle =
            tokio::spawn(async move { relay(Box::new(client_side), Box::new(channel)).await });

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
            "Must call poll_write_owned via Path A"
        );
    }

    #[tokio::test]
    async fn test_relay_heartbeat_terminates_on_dead_stream() {
        struct MockDeadOnPingStream {
            pings_received: Arc<AtomicUsize>,
        }

        impl tokio::io::AsyncRead for MockDeadOnPingStream {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                Poll::Pending
            }
        }

        impl tokio::io::AsyncWrite for MockDeadOnPingStream {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                Poll::Ready(Ok(buf.len()))
            }

            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        impl TunnelStream for MockDeadOnPingStream {
            fn poll_send_ping(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                self.pings_received.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Ok(()))
            }

            fn poll_check_alive(&mut self, _cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
                if self.pings_received.load(Ordering::SeqCst) > 0 {
                    Poll::Ready(HeartbeatStatus::Dead)
                } else {
                    Poll::Ready(HeartbeatStatus::Alive)
                }
            }
        }

        let pings = Arc::new(AtomicUsize::new(0));
        let dead_stream = MockDeadOnPingStream {
            pings_received: Arc::clone(&pings),
        };

        let (_client_app, client_side) = duplex(65536);

        let res = StreamStreamPump::relay_with_heartbeat(
            Box::new(client_side),
            Box::new(dead_stream),
            CancellationToken::new(),
            std::time::Duration::from_millis(20),
            std::time::Duration::from_millis(200),
        )
        .await;

        assert!(
            res.is_err(),
            "Relay must terminate with error on dead stream"
        );
        match res.unwrap_err() {
            TunnelError::Io(e) => {
                assert_eq!(e.kind(), io::ErrorKind::TimedOut);
            }
            other => panic!("Expected Io(TimedOut), got: {:?}", other),
        }
        assert!(pings.load(Ordering::SeqCst) > 0, "Ping must have been sent");
    }

    #[tokio::test]
    async fn test_relay_suppresses_probes_while_activity_is_recent() {
        struct AlwaysActive {
            pings: Arc<AtomicUsize>,
        }

        impl tokio::io::AsyncRead for AlwaysActive {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                Poll::Pending
            }
        }

        impl tokio::io::AsyncWrite for AlwaysActive {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                Poll::Ready(Ok(buf.len()))
            }

            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        impl TunnelStream for AlwaysActive {
            fn last_activity_ms(&self) -> Option<u64> {
                Some(activity_now_ms())
            }

            fn poll_send_ping(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                self.pings.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Ok(()))
            }

            fn poll_check_alive(&mut self, _cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
                Poll::Ready(HeartbeatStatus::Alive)
            }
        }

        let pings = Arc::new(AtomicUsize::new(0));
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        let pings_in = Arc::clone(&pings);
        let handle = tokio::spawn(async move {
            StreamStreamPump::relay_with_heartbeat(
                Box::new(AlwaysActive {
                    pings: Arc::clone(&pings_in),
                }),
                Box::new(AlwaysActive { pings: pings_in }),
                cancel,
                std::time::Duration::from_millis(10),
                std::time::Duration::from_millis(500),
            )
            .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        trigger.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(3), handle)
            .await
            .expect("relay did not shut down after cancellation")
            .unwrap()
            .unwrap();

        assert_eq!(
            pings.load(Ordering::SeqCst),
            0,
            "TX/RX activity must suppress keepalive probes"
        );
    }

    struct ChannelDatagramIo {
        rx: tokio::sync::mpsc::UnboundedReceiver<Datagram>,
        tx: tokio::sync::mpsc::UnboundedSender<Datagram>,
    }

    impl DatagramIo for ChannelDatagramIo {
        fn poll_recv(
            &mut self,
            cx: &mut Context<'_>,
            batch: &mut [Datagram],
        ) -> Poll<io::Result<usize>> {
            if batch.is_empty() {
                return Poll::Ready(Ok(0));
            }
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(d)) => {
                    batch[0] = d;
                    let mut count = 1;
                    while count < batch.len() {
                        match self.rx.try_recv() {
                            Ok(d) => {
                                batch[count] = d;
                                count += 1;
                            }
                            Err(_) => break,
                        }
                    }
                    Poll::Ready(Ok(count))
                }
                Poll::Ready(None) => Poll::Ready(Ok(0)),
                Poll::Pending => Poll::Pending,
            }
        }

        fn poll_send(
            &mut self,
            _cx: &mut Context<'_>,
            batch: &[Datagram],
        ) -> Poll<io::Result<usize>> {
            for d in batch {
                if self.tx.send(d.clone()).is_err() {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "datagram receiver closed",
                    )));
                }
            }
            Poll::Ready(Ok(batch.len()))
        }
    }

    #[tokio::test]
    async fn test_datagram_stream_pump_framing_and_empty_packet() {
        let (to_dgram_tx, from_pump_rx) = tokio::sync::mpsc::unbounded_channel();
        let (to_pump_tx, to_dgram_rx) = tokio::sync::mpsc::unbounded_channel();

        let (stream_a, mut stream_b) = duplex(65536);

        let dgram_io = ChannelDatagramIo {
            rx: to_dgram_rx,
            tx: to_dgram_tx,
        };

        let cancel = CancellationToken::new();
        let pump_handle = tokio::spawn({
            let cancel = cancel.clone();
            async move {
                DatagramStreamPump::relay_datagram_stream(
                    Box::new(dgram_io),
                    Box::new(stream_a),
                    cancel,
                    Duration::from_secs(5),
                    Duration::from_secs(10),
                )
                .await
            }
        });

        // 1. Send packet + empty packet from datagram side to stream side
        to_pump_tx
            .send(Datagram {
                flow: Default::default(),
                payload: Bytes::from_static(b"hello udp"),
            })
            .unwrap();
        to_pump_tx
            .send(Datagram {
                flow: Default::default(),
                payload: Bytes::new(),
            })
            .unwrap();

        let mut hdr = [0u8; 2];
        stream_b.read_exact(&mut hdr).await.unwrap();
        assert_eq!(u16::from_be_bytes(hdr), 9);
        let mut body = vec![0u8; 9];
        stream_b.read_exact(&mut body).await.unwrap();
        assert_eq!(&body, b"hello udp");

        stream_b.read_exact(&mut hdr).await.unwrap();
        assert_eq!(u16::from_be_bytes(hdr), 0);

        // 2. Write a fragmented frame from stream_b back to datagram side
        let reply = b"reply-from-stream";
        let frame_len = (reply.len() as u16).to_be_bytes();
        stream_b.write_all(&frame_len[..1]).await.unwrap();
        stream_b.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        stream_b.write_all(&frame_len[1..]).await.unwrap();
        stream_b.write_all(&reply[..5]).await.unwrap();
        stream_b.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        stream_b.write_all(&reply[5..]).await.unwrap();
        stream_b.flush().await.unwrap();

        let mut from_pump_rx = from_pump_rx;
        let recvd = from_pump_rx.recv().await.unwrap();
        assert_eq!(recvd.payload.as_ref(), reply);

        drop(to_pump_tx);
        let (d2s, s2d) = pump_handle.await.unwrap().unwrap();
        assert_eq!(d2s, 9);
        assert_eq!(s2d, 17);
    }

    #[tokio::test]
    async fn test_datagram_stream_pump_keeps_udp_to_stream_after_stream_read_eof() {
        let (udp_tx, udp_rx) = tokio::sync::mpsc::unbounded_channel();
        let (stream_to_udp_tx, _stream_to_udp_rx) = tokio::sync::mpsc::unbounded_channel();
        let dgram_io = ChannelDatagramIo {
            rx: udp_rx,
            tx: stream_to_udp_tx,
        };
        let (stream_a, mut stream_b) = duplex(4096);

        let pump = tokio::spawn(async move {
            DatagramStreamPump::relay_datagram_stream(
                Box::new(dgram_io),
                Box::new(stream_a),
                CancellationToken::new(),
                Duration::from_secs(5),
                Duration::from_secs(10),
            )
            .await
        });

        // Half-close only the stream read direction. UDP->stream must remain live.
        stream_b.shutdown().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        udp_tx.send(Datagram {
            flow: Default::default(),
            payload: Bytes::from_static(b"still-alive"),
        }).unwrap();

        let mut header = [0u8; 2];
        tokio::time::timeout(Duration::from_secs(1), stream_b.read_exact(&mut header))
            .await
            .expect("pump stopped after stream read-side EOF")
            .unwrap();
        assert_eq!(u16::from_be_bytes(header), 11);
        let mut payload = [0u8; 11];
        stream_b.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"still-alive");

        drop(udp_tx);
        tokio::time::timeout(Duration::from_secs(1), pump)
            .await
            .expect("pump did not finish after both directions closed")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_datagram_stream_pump_rejects_truncated_frame_at_eof() {
        let (_to_dgram_tx, _from_pump_rx) = tokio::sync::mpsc::unbounded_channel();
        let (_to_pump_tx, to_dgram_rx) = tokio::sync::mpsc::unbounded_channel();
        let dgram_io = ChannelDatagramIo {
            rx: to_dgram_rx,
            tx: _to_dgram_tx,
        };
        let (stream_a, mut stream_b) = duplex(4096);

        let pump = tokio::spawn(async move {
            DatagramStreamPump::relay_datagram_stream(
                Box::new(dgram_io),
                Box::new(stream_a),
                CancellationToken::new(),
                Duration::from_secs(5),
                Duration::from_secs(10),
            )
            .await
        });

        // A length of 3 followed by only one payload byte is a truncated frame.
        stream_b.write_all(&[0, 3, b'x']).await.unwrap();
        stream_b.shutdown().await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), pump)
            .await
            .expect("pump did not observe stream EOF")
            .unwrap();
        match result {
            Err(TunnelError::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof),
            other => panic!("expected UnexpectedEof, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_datagram_stream_pump_rejects_oversized_outbound_datagram() {
        let (out_tx, _out_rx) = tokio::sync::mpsc::unbounded_channel();
        let (packet_tx, packet_rx) = tokio::sync::mpsc::unbounded_channel();
        packet_tx.send(Datagram {
            flow: Default::default(),
            payload: Bytes::from(vec![0u8; crate::consts::MAX_UDP_DATAGRAM_SIZE + 1]),
        }).unwrap();
        drop(packet_tx);

        let dgram_io = ChannelDatagramIo {
            rx: packet_rx,
            tx: out_tx,
        };
        let (stream_a, _stream_b) = duplex(4096);

        let result = DatagramStreamPump::relay_datagram_stream(
            Box::new(dgram_io),
            Box::new(stream_a),
            CancellationToken::new(),
            Duration::from_secs(5),
            Duration::from_secs(10),
        ).await;
        match result {
            Err(TunnelError::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::InvalidInput),
            other => panic!("expected InvalidInput, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_datagram_stream_pump_idle_timeout() {
        let (_to_dgram_tx, _from_pump_rx) = tokio::sync::mpsc::unbounded_channel();
        let (_to_pump_tx, to_dgram_rx) = tokio::sync::mpsc::unbounded_channel();
        let (stream_a, _stream_b) = duplex(4096);

        let dgram_io = ChannelDatagramIo {
            rx: to_dgram_rx,
            tx: _to_dgram_tx,
        };

        let res = tokio::time::timeout(
            Duration::from_secs(2),
            DatagramStreamPump::relay_datagram_stream(
                Box::new(dgram_io),
                Box::new(stream_a),
                CancellationToken::new(),
                Duration::from_secs(15),
                Duration::from_millis(120),
            ),
        )
        .await;

        assert!(
            res.is_ok(),
            "Pump must terminate promptly on UDP idle timeout"
        );
        assert!(res.unwrap().is_ok(), "UDP idle expiration is clean Ok exit");
    }
}


#[cfg(test)]
mod udp_frame_tests {
    use super::*;

    fn frame(payload: &[u8]) -> BytesMut {
        let mut out = BytesMut::new();
        out.put_u16(payload.len() as u16);
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn decoder_never_exceeds_pending_limit_and_retains_remaining_frames() {
        let mut rx = BytesMut::new();
        for _ in 0..(MAX_PENDING_DATAGRAMS + 7) {
            rx.extend_from_slice(&frame(&[]));
        }
        let mut pending = VecDeque::new();
        let mut total = 0;
        let mut pending_bytes = 0;

        decode_pending_datagrams(&mut rx, &mut pending, &mut total, &mut pending_bytes).unwrap();
        assert_eq!(pending.len(), MAX_PENDING_DATAGRAMS);
        assert_eq!(rx.len(), 14);

        pending.clear();
        decode_pending_datagrams(&mut rx, &mut pending, &mut total, &mut pending_bytes).unwrap();
        assert_eq!(pending.len(), 7);
        assert!(rx.is_empty());
        assert_eq!(total, 0);
    }

    #[test]
    fn decoder_respects_pending_byte_budget_and_retains_frame() {
        let payload = vec![0x5a; crate::consts::MAX_UDP_DATAGRAM_SIZE];
        let mut rx = BytesMut::new();
        for _ in 0..5 {
            rx.extend_from_slice(&frame(&payload));
        }
        let mut pending = VecDeque::new();
        let mut total = 0;
        let mut pending_bytes = 0;

        decode_pending_datagrams(&mut rx, &mut pending, &mut total, &mut pending_bytes).unwrap();
        assert_eq!(pending.len(), 4);
        assert_eq!(pending_bytes, 4 * crate::consts::MAX_UDP_DATAGRAM_SIZE);
        assert_eq!(rx.len(), crate::consts::MAX_UDP_DATAGRAM_SIZE + 2);

        pending.clear();
        pending_bytes = 0;
        decode_pending_datagrams(&mut rx, &mut pending, &mut total, &mut pending_bytes).unwrap();
        assert_eq!(pending.len(), 1);
        assert!(rx.is_empty());
    }

    #[test]
    fn decoder_rejects_payload_length_above_udp_limit() {
        let mut rx = BytesMut::from(&((crate::consts::MAX_UDP_DATAGRAM_SIZE + 1) as u16).to_be_bytes()[..]);
        rx.resize(2 + crate::consts::MAX_UDP_DATAGRAM_SIZE + 1, 0);
        let mut pending = VecDeque::new();
        let mut total = 0;
        let mut pending_bytes = 0;
        let err = decode_pending_datagrams(&mut rx, &mut pending, &mut total, &mut pending_bytes).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
