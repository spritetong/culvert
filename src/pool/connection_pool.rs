//! Protocol-aware connection pool supporting 0-RTT acquisition and session draining.
//! All comments and docstrings must be in English.

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::consts::ACTIVATE_STREAM_MAGIC;
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
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

/// Auto-replenishing stream pool that maintains pre-warmed idle streams for 0-RTT acquisition.
pub struct ReplenishingStreamPool {
    connector: Arc<dyn TransportConnector>,
    default_target: Option<TargetAddr>,
    target_size: usize,
    idle_streams: Arc<Mutex<VecDeque<Box<dyn TunnelStream>>>>,
    stats: Arc<Mutex<PoolStats>>,
    notify: Arc<Notify>,
    cancel_token: CancellationToken,
}

impl ReplenishingStreamPool {
    /// Creates and immediately activates a new `ReplenishingStreamPool` with a specific target.
    pub fn new(
        connector: Arc<dyn TransportConnector>,
        target: TargetAddr,
        target_size: usize,
        cancel_token: CancellationToken,
    ) -> Self {
        let pool = Self {
            connector,
            default_target: Some(target),
            target_size,
            idle_streams: Arc::new(Mutex::new(VecDeque::new())),
            stats: Arc::new(Mutex::new(PoolStats::default())),
            notify: Arc::new(Notify::new()),
            cancel_token,
        };
        pool.spawn_replenish_worker();
        pool
    }

    /// Creates and immediately activates a shared `ReplenishingStreamPool` that connects idle streams
    /// without fixing a target upfront. Target is specified dynamically during `acquire`.
    pub fn new_shared(
        connector: Arc<dyn TransportConnector>,
        target_size: usize,
        cancel_token: CancellationToken,
    ) -> Self {
        let pool = Self {
            connector,
            default_target: None,
            target_size,
            idle_streams: Arc::new(Mutex::new(VecDeque::new())),
            stats: Arc::new(Mutex::new(PoolStats::default())),
            notify: Arc::new(Notify::new()),
            cancel_token,
        };
        pool.spawn_replenish_worker();
        pool
    }

    fn spawn_replenish_worker(&self) {
        let connector = Arc::clone(&self.connector);
        let default_target = self
            .default_target
            .clone()
            .unwrap_or_else(|| TargetAddr::Domain("pool.idle".to_string(), 0));
        let target_size = self.target_size;
        let idle_streams = Arc::clone(&self.idle_streams);
        let stats = Arc::clone(&self.stats);
        let notify = Arc::clone(&self.notify);
        let cancel_token = self.cancel_token.clone();

        tokio::spawn(async move {
            let mut consecutive_failures = 0u32;
            while !cancel_token.is_cancelled() {
                let current_len = idle_streams.lock().len();
                if current_len < target_size {
                    match connector.connect_pooled(&default_target).await {
                        Ok(stream) => {
                            consecutive_failures = 0;
                            let idle_len = {
                                let mut guard = idle_streams.lock();
                                guard.push_back(stream);
                                guard.len()
                            };
                            let mut st = stats.lock();
                            st.idle_count = idle_len;
                            st.total_created += 1;
                        }
                        Err(e) => {
                            consecutive_failures += 1;
                            let backoff_ms =
                                (100u64 * (1u64 << consecutive_failures.min(5))).min(5000);
                            tracing::warn!(
                                failures = consecutive_failures,
                                retry_in_ms = backoff_ms,
                                "Failed to replenish pool connection: {:?}",
                                e
                            );
                            tokio::select! {
                                _ = cancel_token.cancelled() => break,
                                _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                            }
                        }
                    }
                } else {
                    tokio::select! {
                        _ = cancel_token.cancelled() => break,
                        _ = notify.notified() => {}
                        _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                    }
                }
            }
        });
    }

    /// Encodes target activation packet: [ACTIVATE_STREAM_MAGIC, len: u16, target_str]
    fn encode_activation(target: Option<&TargetAddr>) -> Vec<u8> {
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

    /// Acquires a pre-warmed connection in 0-RTT and activates it for the target, or falls back to dialing on demand.
    pub fn acquire<'a>(
        &'a self,
        target: Option<&'a TargetAddr>,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        Box::pin(async move {
            let effective_target = target
                .or(self.default_target.as_ref())
                .cloned()
                .unwrap_or_else(|| TargetAddr::Domain("pool.idle".to_string(), 0));
            let activation_pkt = Self::encode_activation(target.or(self.default_target.as_ref()));

            loop {
                let stream_opt = {
                    let mut guard = self.idle_streams.lock();
                    guard.pop_front()
                };

                // Notify background worker to replenish the popped slot
                self.notify.notify_one();

                if let Some(mut stream) = stream_opt {
                    // Activate the idle stream by writing the activation packet.
                    // If the stream was disconnected while idle, writing/flushing fails immediately.
                    let pkt = activation_pkt.clone();
                    let activate_res = tokio::time::timeout(Duration::from_millis(500), async {
                        stream.write_all(&pkt).await?;
                        stream.flush().await?;
                        Ok::<(), std::io::Error>(())
                    })
                    .await;

                    match activate_res {
                        Ok(Ok(())) => {
                            let mut st = self.stats.lock();
                            st.idle_count = self.idle_streams.lock().len();
                            st.active_count += 1;
                            return Ok(stream);
                        }
                        _ => {
                            tracing::debug!(
                                "Discarded stale/dead idle connection from pool, trying next..."
                            );
                            continue;
                        }
                    }
                } else {
                    // Pool exhausted: fallback to on-demand direct connect and activate
                    let mut stream = self.connector.connect_pooled(&effective_target).await?;
                    stream
                        .write_all(&activation_pkt)
                        .await
                        .map_err(TunnelError::Io)?;
                    stream.flush().await.map_err(TunnelError::Io)?;

                    let mut st = self.stats.lock();
                    st.active_count += 1;
                    st.total_created += 1;
                    return Ok(stream);
                }
            }
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
        let mut st = self.stats.lock();
        if st.active_count > 0 {
            st.active_count -= 1;
        }
    }

    /// Pushes an idle stream back into the pool.
    pub fn push_idle(&self, stream: Box<dyn TunnelStream>) {
        let idle_len = {
            let mut guard = self.idle_streams.lock();
            guard.push_back(stream);
            guard.len()
        };
        let mut st = self.stats.lock();
        st.idle_count = idle_len;
    }

    /// Returns pool statistics.
    pub fn stats(&self) -> PoolStats {
        *self.stats.lock()
    }
}
