//! Protocol-aware connection pool supporting 0-RTT acquisition and session draining.
//! All comments and docstrings must be in English.

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
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
            let mut idle_streams: VecDeque<Box<dyn TunnelStream>> = VecDeque::new();
            let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(5));
            heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let dummy_target = default_target
                .clone()
                .unwrap_or_else(|| TargetAddr::Domain("pool.idle".to_string(), 0));

            loop {
                let need_replenish = idle_streams.len() < target_size;

                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    dial_res = connector.connect_pooled(&dummy_target), if need_replenish => {
                        match dial_res {
                            Ok(stream) => {
                                idle_streams.push_back(stream);
                                let mut st = stats_clone.lock();
                                st.idle_count = idle_streams.len();
                                st.total_created += 1;
                            }
                            Err(e) => {
                                tracing::debug!("Failed to replenish idle pool stream: {:?}", e);
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                    _ = heartbeat_interval.tick() => {
                        // Periodic heartbeat inspection across all idle streams
                        let mut alive_streams = VecDeque::with_capacity(idle_streams.len());
                        while let Some(mut stream) = idle_streams.pop_front() {
                            let is_alive = std::future::poll_fn(|cx| {
                                let _ = stream.poll_send_ping(cx);
                                match stream.poll_check_alive(cx) {
                                    Poll::Ready(HeartbeatStatus::Dead) => Poll::Ready(false),
                                    _ => Poll::Ready(true),
                                }
                            }).await;

                            if is_alive {
                                alive_streams.push_back(stream);
                            } else {
                                tracing::debug!("Evicted dead stream during pool heartbeat sweep");
                            }
                        }
                        idle_streams = alive_streams;
                        stats_clone.lock().idle_count = idle_streams.len();
                    }
                    cmd_opt = rx_cmd.recv() => {
                        match cmd_opt {
                            Some(PoolCommand::Acquire(req)) => {
                                let mut acquired_stream: Option<Box<dyn TunnelStream>> = None;

                                while let Some(mut stream) = idle_streams.pop_front() {
                                    let is_alive = std::future::poll_fn(|cx| {
                                        match stream.poll_check_alive(cx) {
                                            Poll::Ready(HeartbeatStatus::Dead) => Poll::Ready(false),
                                            _ => Poll::Ready(true),
                                        }
                                    }).await;

                                    if is_alive {
                                        acquired_stream = Some(stream);
                                        break;
                                    } else {
                                        tracing::debug!("Evicted dead stream during acquire check");
                                    }
                                }

                                if let Some(stream) = acquired_stream {
                                    let mut st = stats_clone.lock();
                                    st.idle_count = idle_streams.len();
                                    st.active_count += 1;
                                    let _ = req.reply.send(Ok(stream));
                                } else {
                                    // Idle pool exhausted: dial on demand directly
                                    let effective_target = req.target
                                        .or_else(|| default_target.clone())
                                        .unwrap_or_else(|| TargetAddr::Domain("pool.idle".to_string(), 0));

                                    let dial_res = connector.connect_pooled(&effective_target).await;
                                    match dial_res {
                                        Ok(stream) => {
                                            let mut st = stats_clone.lock();
                                            st.active_count += 1;
                                            st.total_created += 1;
                                            let _ = req.reply.send(Ok(stream));
                                        }
                                        Err(e) => {
                                            let _ = req.reply.send(Err(e));
                                        }
                                    }
                                }
                                stats_clone.lock().idle_count = idle_streams.len();
                            }
                            Some(PoolCommand::PushIdle(stream)) => {
                                idle_streams.push_back(stream);
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
