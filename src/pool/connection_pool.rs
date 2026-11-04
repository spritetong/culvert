//! Protocol-aware connection pool supporting 0-RTT acquisition and session draining.
//! All comments and docstrings must be in English.

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::consts::ACTIVATE_STREAM_MAGIC;
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, HeartbeatStatus, TunnelStream};
use crate::transport::{TargetAddr, TransportConnector};

/// Statistics for monitoring connection pool performance.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    pub idle_count: usize,
    pub active_count: usize,
    pub total_created: u64,
}

/// Abstract stream pool trait.
pub trait StreamPool: Send + Sync {
    /// Acquires an idle, pre-warmed stream from the pool.
    fn acquire<'a>(&'a self) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>>;

    /// Returns a stream to the pool after draining any in-flight residue.
    fn release<'a>(&'a self, stream: Box<dyn TunnelStream>) -> BoxFuture<'a, ()>;

    /// Returns current pool capacity and usage statistics.
    fn stats(&self) -> PoolStats;
}

/// In-memory stream pool implementation using `parking_lot::Mutex`.
pub struct InMemoryStreamPool {
    idle_streams: Mutex<VecDeque<Box<dyn TunnelStream>>>,
    stats: Mutex<PoolStats>,
}

impl InMemoryStreamPool {
    /// Creates a new `InMemoryStreamPool`.
    pub fn new() -> Self {
        Self {
            idle_streams: Mutex::new(VecDeque::new()),
            stats: Mutex::new(PoolStats::default()),
        }
    }

    /// Seeds an idle stream directly into the pool.
    pub fn push_idle(&self, stream: Box<dyn TunnelStream>) {
        let mut guard = self.idle_streams.lock();
        guard.push_back(stream);
        let mut stats = self.stats.lock();
        stats.idle_count = guard.len();
        stats.total_created += 1;
    }
}

impl Default for InMemoryStreamPool {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamPool for InMemoryStreamPool {
    fn acquire<'a>(&'a self) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        Box::pin(async move {
            let mut guard = self.idle_streams.lock();
            if let Some(stream) = guard.pop_front() {
                let mut stats = self.stats.lock();
                stats.idle_count = guard.len();
                stats.active_count += 1;
                Ok(stream)
            } else {
                Err(TunnelError::PoolExhausted)
            }
        })
    }

    fn release<'a>(&'a self, stream: Box<dyn TunnelStream>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut guard = self.idle_streams.lock();
            guard.push_back(stream);
            let mut stats = self.stats.lock();
            stats.idle_count = guard.len();
            if stats.active_count > 0 {
                stats.active_count -= 1;
            }
        })
    }

    fn stats(&self) -> PoolStats {
        *self.stats.lock()
    }
}

/// State of an idle stream's heartbeat lifecycle within the pool.
#[derive(Debug, Clone, Copy)]
enum HeartbeatState {
    /// Waiting for the next scheduled heartbeat check.
    Idle(Instant),
    /// Attempting to send a ping frame (with start timestamp to detect hanging ping calls).
    SendingPing(Instant),
}

/// A wrapper around an idle `TunnelStream` keeping track of its heartbeat timing.
struct ManagedStream {
    stream: Box<dyn TunnelStream>,
    state: HeartbeatState,
}

impl ManagedStream {
    fn new(stream: Box<dyn TunnelStream>) -> Self {
        Self {
            stream,
            state: HeartbeatState::Idle(Instant::now()),
        }
    }
}

/// Request sent to the pool actor to acquire a stream.
struct AcquireRequest {
    target: Option<TargetAddr>,
    reply: oneshot::Sender<Result<Box<dyn TunnelStream>, TunnelError>>,
}

/// Control commands delivered to the pool actor.
enum PoolCommand {
    Acquire(AcquireRequest),
    PushIdle(Box<dyn TunnelStream>),
    StreamClosed,
}

/// Auto-replenishing stream pool managed by an actor loop with active heartbeat and self-healing.
pub struct ReplenishingStreamPool {
    tx_cmd: mpsc::Sender<PoolCommand>,
    stats: Arc<Mutex<PoolStats>>,
}

impl ReplenishingStreamPool {
    /// Creates and immediately activates a new `ReplenishingStreamPool` with a specific target.
    pub fn new(
        connector: Arc<dyn TransportConnector>,
        target: TargetAddr,
        target_size: usize,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::spawn_actor(connector, Some(target), target_size, cancel_token)
    }

    /// Creates and immediately activates a shared `ReplenishingStreamPool` that connects idle streams
    /// without fixing a target upfront. Target is specified dynamically during `acquire`.
    pub fn new_shared(
        connector: Arc<dyn TransportConnector>,
        target_size: usize,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::spawn_actor(connector, None, target_size, cancel_token)
    }

    fn spawn_actor(
        connector: Arc<dyn TransportConnector>,
        default_target: Option<TargetAddr>,
        target_size: usize,
        cancel_token: CancellationToken,
    ) -> Self {
        let (tx_cmd, mut rx_cmd) = mpsc::channel::<PoolCommand>(1024);
        let stats = Arc::new(Mutex::new(PoolStats::default()));
        let stats_clone = Arc::clone(&stats);

        tokio::spawn(async move {
            let mut idle_streams: VecDeque<ManagedStream> = VecDeque::new();
            let mut connecting_futures: FuturesUnordered<BoxFuture<'static, Result<Box<dyn TunnelStream>, TunnelError>>> =
                FuturesUnordered::new();
            let dummy_target = default_target
                .clone()
                .unwrap_or_else(|| TargetAddr::Domain("pool.idle".to_string(), 0));

            const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
            const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);
            const MAX_CONCURRENT_CONNECTS: usize = 4;

            loop {
                // 1. Replenish connecting futures if total (idle + in_flight) < target_size
                let in_flight = connecting_futures.len();
                let current_total = idle_streams.len() + in_flight;
                if current_total < target_size && in_flight < MAX_CONCURRENT_CONNECTS {
                    let c = Arc::clone(&connector);
                    let t = dummy_target.clone();
                    connecting_futures.push(Box::pin(async move {
                        c.connect_pooled(&t).await
                    }));
                }

                // 2. Perform non-blocking, multi-stream heartbeat sweep in a single poll
                let mut surviving = VecDeque::with_capacity(idle_streams.len());
                let now = Instant::now();

                while let Some(mut managed) = idle_streams.pop_front() {
                    let mut is_dead = false;

                    // Non-blocking single poll to check stream status and advance ping
                    let _ = std::future::poll_fn(|cx| {
                        // First: check liveness
                        if let Poll::Ready(HeartbeatStatus::Dead) = managed.stream.poll_check_alive(cx) {
                            is_dead = true;
                            return Poll::Ready(());
                        }

                        // Second: advance heartbeat state machine
                        match managed.state {
                            HeartbeatState::Idle(last_check) => {
                                if now.duration_since(last_check) >= HEARTBEAT_INTERVAL {
                                    match managed.stream.poll_send_ping(cx) {
                                        Poll::Ready(Ok(())) => {
                                            managed.state = HeartbeatState::Idle(now);
                                        }
                                        Poll::Ready(Err(_)) => {
                                            is_dead = true;
                                        }
                                        Poll::Pending => {
                                            managed.state = HeartbeatState::SendingPing(now);
                                        }
                                    }
                                }
                            }
                            HeartbeatState::SendingPing(started_at) => {
                                if now.duration_since(started_at) > HEARTBEAT_TIMEOUT {
                                    // Ping call hung in lower layer; evict defensively!
                                    is_dead = true;
                                } else {
                                    match managed.stream.poll_send_ping(cx) {
                                        Poll::Ready(Ok(())) => {
                                            managed.state = HeartbeatState::Idle(now);
                                        }
                                        Poll::Ready(Err(_)) => {
                                            is_dead = true;
                                        }
                                        Poll::Pending => {
                                            // Still pending, continue waiting without blocking other streams
                                        }
                                    }
                                }
                            }
                        }
                        Poll::Ready(())
                    }).await;

                    if !is_dead {
                        surviving.push_back(managed);
                    } else {
                        tracing::debug!("Evicted dead/hanging stream during non-blocking sweep");
                    }
                }
                idle_streams = surviving;
                stats_clone.lock().idle_count = idle_streams.len();

                // 3. Multi-branch non-blocking select
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    Some(conn_res) = connecting_futures.next(), if !connecting_futures.is_empty() => {
                        match conn_res {
                            Ok(stream) => {
                                idle_streams.push_back(ManagedStream::new(stream));
                                let mut st = stats_clone.lock();
                                st.idle_count = idle_streams.len();
                                st.total_created += 1;
                            }
                            Err(e) => {
                                tracing::debug!("Background connect_pooled failed: {:?}", e);
                            }
                        }
                    }
                    cmd_opt = rx_cmd.recv() => {
                        match cmd_opt {
                            Some(PoolCommand::Acquire(req)) => {
                                let mut acquired: Option<Box<dyn TunnelStream>> = None;

                                while let Some(mut managed) = idle_streams.pop_front() {
                                    let mut dead = false;
                                    let _ = std::future::poll_fn(|cx| {
                                        if let Poll::Ready(HeartbeatStatus::Dead) = managed.stream.poll_check_alive(cx) {
                                            dead = true;
                                        }
                                        Poll::Ready(())
                                    }).await;

                                    if !dead {
                                        acquired = Some(managed.stream);
                                        break;
                                    } else {
                                        tracing::debug!("Evicted dead stream during acquire inspection");
                                    }
                                }

                                if let Some(stream) = acquired {
                                    let mut st = stats_clone.lock();
                                    st.idle_count = idle_streams.len();
                                    st.active_count += 1;
                                    let _ = req.reply.send(Ok(stream));
                                } else {
                                    // Idle pool exhausted: dial on demand directly in a separate task
                                    let effective_target = req.target
                                        .or_else(|| default_target.clone())
                                        .unwrap_or_else(|| TargetAddr::Domain("pool.idle".to_string(), 0));
                                    let c = Arc::clone(&connector);
                                    let stats_inner = Arc::clone(&stats_clone);

                                    tokio::spawn(async move {
                                        let res = c.connect_pooled(&effective_target).await;
                                        match res {
                                            Ok(stream) => {
                                                let mut st = stats_inner.lock();
                                                st.active_count += 1;
                                                st.total_created += 1;
                                                let _ = req.reply.send(Ok(stream));
                                            }
                                            Err(e) => {
                                                let _ = req.reply.send(Err(e));
                                            }
                                        }
                                    });
                                }
                                stats_clone.lock().idle_count = idle_streams.len();
                            }
                            Some(PoolCommand::PushIdle(stream)) => {
                                idle_streams.push_back(ManagedStream::new(stream));
                                let mut st = stats_clone.lock();
                                st.idle_count = idle_streams.len();
                                st.total_created += 1;
                            }
                            Some(PoolCommand::StreamClosed) => {
                                let mut st = stats_clone.lock();
                                if st.active_count > 0 {
                                    st.active_count -= 1;
                                }
                            }
                            None => break,
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {
                        // Periodic timer to wake up and drive heartbeat sweep even when idle
                    }
                }
            }
        });

        Self { tx_cmd, stats }
    }

    /// Encodes target activation packet: [ACTIVATE_STREAM_MAGIC, len: u16, target_str]
    pub fn encode_activation(target: Option<&TargetAddr>) -> Vec<u8> {
        let mut pkt = Vec::new();
        pkt.push(ACTIVATE_STREAM_MAGIC);
        if let Some(t) = target {
            let s = t.to_string();
            let b = s.as_bytes();
            let len = b.len().min(u16::MAX as usize) as u16;
            pkt.extend_from_slice(&len.to_be_bytes());
            pkt.extend_from_slice(&b[..len as usize]);
        }
        pkt
    }

    /// Acquires a pre-warmed connection in 0-RTT, or falls back to dialing on demand.
    pub fn acquire<'a>(
        &'a self,
        target: Option<&'a TargetAddr>,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        let target_cloned = target.cloned();
        Box::pin(async move {
            let (reply, rx) = oneshot::channel();
            self.tx_cmd
                .send(PoolCommand::Acquire(AcquireRequest {
                    target: target_cloned,
                    reply,
                }))
                .await
                .map_err(|_| TunnelError::PoolExhausted)?;

            rx.await.map_err(|_| TunnelError::PoolExhausted)?
        })
    }

    /// Acquires using default target if configured.
    pub fn acquire_default<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        self.acquire(None)
    }

    /// Decrements the active connection count when a stream finishes relaying.
    pub fn on_stream_closed(&self) {
        let _ = self.tx_cmd.try_send(PoolCommand::StreamClosed);
    }

    /// Pushes an idle stream back into the pool.
    pub fn push_idle(&self, stream: Box<dyn TunnelStream>) {
        let _ = self.tx_cmd.try_send(PoolCommand::PushIdle(stream));
    }

    /// Returns pool statistics.
    pub fn stats(&self) -> PoolStats {
        *self.stats.lock()
    }
}
