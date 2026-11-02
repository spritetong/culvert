//! Protocol-aware connection pool supporting 0-RTT acquisition and session draining.
//! All comments and docstrings must be in English.

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

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
    target: TargetAddr,
    target_size: usize,
    idle_streams: Arc<Mutex<VecDeque<Box<dyn TunnelStream>>>>,
    stats: Arc<Mutex<PoolStats>>,
    notify: Arc<Notify>,
    cancel_token: CancellationToken,
}

impl ReplenishingStreamPool {
    /// Creates and immediately activates a new `ReplenishingStreamPool`.
    pub fn new(
        connector: Arc<dyn TransportConnector>,
        target: TargetAddr,
        target_size: usize,
        cancel_token: CancellationToken,
    ) -> Self {
        let pool = Self {
            connector,
            target,
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
        let target = self.target.clone();
        let target_size = self.target_size;
        let idle_streams = Arc::clone(&self.idle_streams);
        let stats = Arc::clone(&self.stats);
        let notify = Arc::clone(&self.notify);
        let cancel_token = self.cancel_token.clone();

        tokio::spawn(async move {
            while !cancel_token.is_cancelled() {
                let current_len = idle_streams.lock().len();
                if current_len < target_size {
                    match connector.connect(&target, None).await {
                        Ok(stream) => {
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
                            tracing::warn!("Failed to pre-warm pool connection: {:?}", e);
                            tokio::select! {
                                _ = cancel_token.cancelled() => break,
                                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                            }
                        }
                    }
                } else {
                    tokio::select! {
                        _ = cancel_token.cancelled() => break,
                        _ = notify.notified() => {}
                    }
                }
            }
        });
    }

    /// Acquires a pre-warmed connection in 0-RTT, or falls back to dialing on demand.
    pub fn acquire<'a>(&'a self) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        Box::pin(async move {
            let (stream_opt, idle_len) = {
                let mut guard = self.idle_streams.lock();
                let stream = guard.pop_front();
                let len = guard.len();
                (stream, len)
            };

            self.notify.notify_one();

            if let Some(stream) = stream_opt {
                let mut st = self.stats.lock();
                st.idle_count = idle_len;
                st.active_count += 1;
                Ok(stream)
            } else {
                let stream = self.connector.connect(&self.target, None).await?;
                let mut st = self.stats.lock();
                st.active_count += 1;
                st.total_created += 1;
                Ok(stream)
            }
        })
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
