// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Protocol-aware connection pool supporting 0-RTT acquisition and session draining.
//! All comments and docstrings must be in English.

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};
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
    #[allow(dead_code)]
    target: Option<TargetAddr>,
    reply: oneshot::Sender<Result<Box<dyn TunnelStream>, TunnelError>>,
}

/// Control commands delivered to the pool actor.
enum PoolCommand {
    Acquire(AcquireRequest),
    PushIdle(Box<dyn TunnelStream>),
    StreamClosed,
}

/// Internal events driven by the unified reactor poll.
enum PoolEvent {
    Cancelled,
    ConnectionResult(Result<Box<dyn TunnelStream>, TunnelError>),
    Command(Option<PoolCommand>),
    RetryTimerExpired,
    HeartbeatTick,
    StreamsEvicted,
}

/// Auto-replenishing stream pool managed by an actor loop with active heartbeat and self-healing.
#[derive(Clone)]
pub struct ReplenishingStreamPool {
    tx_cmd: mpsc::Sender<PoolCommand>,
    stats: Arc<Mutex<PoolStats>>,
    active_count: Arc<AtomicUsize>,
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
        let active_count = Arc::new(AtomicUsize::new(0));

        tokio::spawn(async move {
            let mut idle_streams: VecDeque<ManagedStream> = VecDeque::new();
            let mut pending_requests: VecDeque<(AcquireRequest, Instant)> = VecDeque::new();
            let mut connecting_futures: FuturesUnordered<
                BoxFuture<'static, Result<Box<dyn TunnelStream>, TunnelError>>,
            > = FuturesUnordered::new();
            let dummy_target = default_target
                .clone()
                .unwrap_or_else(|| TargetAddr::Domain("pool.idle".to_string(), 0));

            const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
            const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);
            const HEARTBEAT_TICK_INTERVAL: Duration = Duration::from_secs(1);
            const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(15);
            const MAX_CONCURRENT_CONNECTS: usize = 4;
            const MAX_POOL_CAPACITY: usize = 16;
            const BACKOFF_BASE: Duration = Duration::from_millis(200);
            const BACKOFF_MAX: Duration = Duration::from_secs(10);

            let mut next_retry_at = Instant::now();
            let mut backoff = BACKOFF_BASE;
            let mut retry_timer: Option<Pin<Box<tokio::time::Sleep>>> = None;

            let mut heartbeat_ticker = tokio::time::interval_at(
                tokio::time::Instant::now() + HEARTBEAT_TICK_INTERVAL,
                HEARTBEAT_TICK_INTERVAL,
            );
            heartbeat_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let cancel_fut = cancel_token.cancelled();
            tokio::pin!(cancel_fut);

            loop {
                // 1. Replenish connecting futures if needed (idle streams < target_size OR waiters in pending_requests)
                let now = Instant::now();
                let in_flight = connecting_futures.len();
                let current_total = idle_streams.len() + in_flight;
                let active = stats_clone.lock().active_count;
                let total_allocated = active + current_total;

                let has_waiters = !pending_requests.is_empty();
                let needed = (current_total < target_size)
                    || (has_waiters && in_flight < pending_requests.len());
                let capacity_available =
                    total_allocated < MAX_POOL_CAPACITY && in_flight < MAX_CONCURRENT_CONNECTS;

                // When recovering from failures (backoff > BACKOFF_BASE), limit in_flight to 1 probe
                // to avoid slamming or serializing gate locks until a connection succeeds.
                let throttled_by_failure = backoff > BACKOFF_BASE && in_flight >= 1;

                if needed && capacity_available && !throttled_by_failure {
                    if now >= next_retry_at {
                        let c = Arc::clone(&connector);
                        let t = dummy_target.clone();
                        connecting_futures
                            .push(Box::pin(async move { c.connect_pooled(&t).await }));
                    } else if in_flight == 0 && retry_timer.is_none() {
                        retry_timer =
                            Some(Box::pin(tokio::time::sleep_until(next_retry_at.into())));
                    }
                }

                // 2. Drive single unified event reactor
                let event = std::future::poll_fn(|cx| {
                    // Branch A: cancellation check
                    if cancel_fut.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(PoolEvent::Cancelled);
                    }

                    // Branch B: command reception from callers
                    if let Poll::Ready(cmd_opt) = rx_cmd.poll_recv(cx) {
                        return Poll::Ready(PoolEvent::Command(cmd_opt));
                    }

                    // Branch C: background connecting futures completion
                    if !connecting_futures.is_empty()
                        && let Poll::Ready(Some(res)) = connecting_futures.poll_next_unpin(cx)
                    {
                        return Poll::Ready(PoolEvent::ConnectionResult(res));
                    }

                    // Branch D: drive idle stream liveness and in-flight heartbeat ping progress
                    let sweep_now = Instant::now();
                    let mut i = 0;
                    let mut evicted_any = false;
                    while i < idle_streams.len() {
                        let mut is_dead = false;
                        let managed = &mut idle_streams[i];

                        if let Poll::Ready(HeartbeatStatus::Dead) =
                            managed.stream.poll_check_alive(cx)
                        {
                            is_dead = true;
                        } else {
                            match managed.state {
                                HeartbeatState::Idle(last_check) => {
                                    if sweep_now.duration_since(last_check) >= HEARTBEAT_INTERVAL {
                                        match managed.stream.poll_send_ping(cx) {
                                            Poll::Ready(Ok(())) => {
                                                managed.state = HeartbeatState::Idle(sweep_now);
                                            }
                                            Poll::Ready(Err(_)) => {
                                                is_dead = true;
                                            }
                                            Poll::Pending => {
                                                managed.state =
                                                    HeartbeatState::SendingPing(sweep_now);
                                            }
                                        }
                                    }
                                }
                                HeartbeatState::SendingPing(started_at) => {
                                    if sweep_now.duration_since(started_at) > HEARTBEAT_TIMEOUT {
                                        is_dead = true;
                                    } else {
                                        match managed.stream.poll_send_ping(cx) {
                                            Poll::Ready(Ok(())) => {
                                                managed.state = HeartbeatState::Idle(sweep_now);
                                            }
                                            Poll::Ready(Err(_)) => {
                                                is_dead = true;
                                            }
                                            Poll::Pending => {
                                                // Socket in-flight: cx.waker() registered!
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if is_dead {
                            tracing::debug!("Evicted dead/hanging stream during reactor sweep");
                            idle_streams.swap_remove_back(i);
                            evicted_any = true;
                        } else {
                            i += 1;
                        }
                    }

                    if evicted_any {
                        return Poll::Ready(PoolEvent::StreamsEvicted);
                    }

                    // Branch E: retry backoff timer
                    if let Some(timer) = retry_timer.as_mut()
                        && timer.as_mut().poll(cx).is_ready()
                    {
                        return Poll::Ready(PoolEvent::RetryTimerExpired);
                    }

                    // Branch F: periodic heartbeat ticker
                    if heartbeat_ticker.poll_tick(cx).is_ready() {
                        return Poll::Ready(PoolEvent::HeartbeatTick);
                    }

                    Poll::Pending
                })
                .await;

                stats_clone.lock().idle_count = idle_streams.len();

                // 3. Dispatch and process pool events
                match event {
                    PoolEvent::Cancelled => break,
                    PoolEvent::ConnectionResult(conn_res) => {
                        match conn_res {
                            Ok(stream) => {
                                backoff = BACKOFF_BASE;
                                next_retry_at = Instant::now();
                                retry_timer = None;

                                // Try direct hand-off to pending requests before pushing to idle_streams
                                let mut cur_stream = Some(stream);
                                while let Some((pending_req, _)) = pending_requests.pop_front() {
                                    if let Some(s) = cur_stream.take() {
                                        match pending_req.reply.send(Ok(s)) {
                                            Ok(()) => {
                                                let mut st = stats_clone.lock();
                                                st.active_count += 1;
                                                st.total_created += 1;
                                                break;
                                            }
                                            Err(returned_res) => {
                                                // Caller dropped rx; reclaim the stream and offer to next waiter
                                                if let Ok(returned) = returned_res {
                                                    cur_stream = Some(returned);
                                                } else {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }

                                if let Some(s) = cur_stream {
                                    idle_streams.push_back(ManagedStream::new(s));
                                    let mut st = stats_clone.lock();
                                    st.idle_count = idle_streams.len();
                                    st.total_created += 1;
                                }
                            }
                            Err(e) => {
                                tracing::debug!("Background connect_pooled failed: {:?}", e);
                                let max_backoff = if !pending_requests.is_empty() {
                                    Duration::from_secs(1)
                                } else {
                                    BACKOFF_MAX
                                };
                                next_retry_at = Instant::now() + backoff;
                                backoff = (backoff * 2).min(max_backoff);
                                retry_timer =
                                    Some(Box::pin(tokio::time::sleep_until(next_retry_at.into())));
                            }
                        }
                    }
                    PoolEvent::Command(cmd_opt) => {
                        match cmd_opt {
                            Some(PoolCommand::Acquire(req)) => {
                                let mut acquired: Option<Box<dyn TunnelStream>> = None;

                                while let Some(mut managed) = idle_streams.pop_front() {
                                    let mut dead = false;
                                    let _ = std::future::poll_fn(|cx| {
                                        if let Poll::Ready(HeartbeatStatus::Dead) =
                                            managed.stream.poll_check_alive(cx)
                                        {
                                            dead = true;
                                        }
                                        Poll::Ready(())
                                    })
                                    .await;

                                    if !dead {
                                        acquired = Some(managed.stream);
                                        break;
                                    } else {
                                        tracing::debug!(
                                            "Evicted dead stream during acquire inspection"
                                        );
                                        connector.invalidate_broken_sessions();
                                    }
                                }

                                if let Some(stream) = acquired {
                                    let mut st = stats_clone.lock();
                                    st.idle_count = idle_streams.len();
                                    st.active_count += 1;
                                    let _ = req.reply.send(Ok(stream));
                                } else {
                                    // If pool has no idle streams and no connects currently in-flight,
                                    // reset backoff timer so this incoming demand is attempted promptly
                                    // rather than waiting for an idle replenishment backoff timer.
                                    if pending_requests.is_empty() && connecting_futures.is_empty()
                                    {
                                        next_retry_at = Instant::now();
                                        backoff = BACKOFF_BASE;
                                        retry_timer = None;
                                    }
                                    // Idle pool exhausted: enqueue into pending requests for hand-off
                                    pending_requests.push_back((req, Instant::now()));
                                }
                                stats_clone.lock().idle_count = idle_streams.len();
                            }
                            Some(PoolCommand::PushIdle(stream)) => {
                                let mut cur_stream = Some(stream);
                                while let Some((pending_req, _)) = pending_requests.pop_front() {
                                    if let Some(s) = cur_stream.take() {
                                        match pending_req.reply.send(Ok(s)) {
                                            Ok(()) => {
                                                let mut st = stats_clone.lock();
                                                st.active_count += 1;
                                                break;
                                            }
                                            Err(returned_res) => {
                                                if let Ok(returned) = returned_res {
                                                    cur_stream = Some(returned);
                                                } else {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }

                                if let Some(s) = cur_stream {
                                    idle_streams.push_back(ManagedStream::new(s));
                                    let mut st = stats_clone.lock();
                                    st.idle_count = idle_streams.len();
                                }
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
                    PoolEvent::RetryTimerExpired => {
                        retry_timer = None;
                    }
                    PoolEvent::StreamsEvicted => {
                        connector.invalidate_broken_sessions();
                        stats_clone.lock().idle_count = idle_streams.len();
                    }
                    PoolEvent::HeartbeatTick => {
                        let now = Instant::now();
                        // Prune closed and timed-out requests in FIFO order
                        while let Some((front, enqueued_at)) = pending_requests.front() {
                            if front.reply.is_closed() {
                                pending_requests.pop_front();
                            } else if now.duration_since(*enqueued_at) > ACQUIRE_TIMEOUT {
                                let (req, _) = pending_requests.pop_front().unwrap();
                                let _ = req.reply.send(Err(TunnelError::PoolExhausted));
                            } else {
                                break;
                            }
                        }
                        stats_clone.lock().idle_count = idle_streams.len();
                    }
                }
            }

            for (req, _) in pending_requests.drain(..) {
                let _ = req.reply.send(Err(TunnelError::PoolExhausted));
            }
        });

        Self {
            tx_cmd,
            stats,
            active_count,
        }
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
        } else {
            pkt.extend_from_slice(&0u16.to_be_bytes());
        }
        pkt
    }

    /// Acquires a pre-warmed connection in 0-RTT, or falls back to dialing on demand.
    pub async fn acquire(
        &self,
        target: Option<&TargetAddr>,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let target_cloned = target.cloned();

        let (reply, rx) = oneshot::channel();
        self.tx_cmd
            .send(PoolCommand::Acquire(AcquireRequest {
                target: target_cloned,
                reply,
            }))
            .await
            .map_err(|_| TunnelError::PoolExhausted)?;

        let stream = rx.await.map_err(|_| TunnelError::PoolExhausted)??;
        self.active_count.fetch_add(1, Ordering::Relaxed);
        Ok(stream)
    }

    /// Decrements the active connection count when a stream finishes relaying.
    pub fn on_stream_closed(&self) {
        // Duplicate/over-eager close notifications must not wrap the counter to
        // usize::MAX and corrupt `stats()`.
        let _ = self
            .active_count
            .try_update(Ordering::AcqRel, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
        let _ = self.tx_cmd.try_send(PoolCommand::StreamClosed);
    }

    /// Pushes an idle stream back into the pool.
    pub fn push_idle(&self, stream: Box<dyn TunnelStream>) {
        let _ = self.tx_cmd.try_send(PoolCommand::PushIdle(stream));
    }

    /// Returns pool statistics.
    pub fn stats(&self) -> PoolStats {
        let mut st = *self.stats.lock();
        st.active_count = self.active_count.load(Ordering::Relaxed);
        st
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    struct MockFailingConnector {
        call_count: Arc<AtomicUsize>,
    }

    impl TransportConnector for MockFailingConnector {
        fn protocol_scheme(&self) -> &'static str {
            "mock"
        }

        fn connect<'a>(
            &'a self,
            _target: &'a TargetAddr,
            _session_id: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Err(TunnelError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "refused",
                )))
            })
        }

        fn connect_pooled<'a>(
            &'a self,
            _target: &'a TargetAddr,
        ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Err(TunnelError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "refused",
                )))
            })
        }
    }

    #[tokio::test]
    async fn test_connection_pool_backoff_on_immediate_failures() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let connector = Arc::new(MockFailingConnector {
            call_count: Arc::clone(&call_count),
        });

        let cancel_token = CancellationToken::new();
        let _pool = ReplenishingStreamPool::new_shared(connector, 4, cancel_token.clone());

        // Sleep 400ms. In the buggy implementation without backoff, 400ms with instant failures
        // resulted in >100,000 reconnect attempts. With 200ms base backoff, call count must be <= 3.
        tokio::time::sleep(Duration::from_millis(400)).await;
        cancel_token.cancel();

        let count = call_count.load(Ordering::SeqCst);
        assert!(
            count <= 3,
            "Reconnect rate must be throttled by backoff gate, got {count}"
        );
        assert!(
            count >= 1,
            "At least one attempt should be performed, got {count}"
        );
    }

    struct MockSuccessfulConnector;

    impl TransportConnector for MockSuccessfulConnector {
        fn protocol_scheme(&self) -> &'static str {
            "mock"
        }

        fn connect<'a>(
            &'a self,
            _target: &'a TargetAddr,
            _session_id: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
            let (client, _server) = tokio::io::duplex(1024);
            Box::pin(async move { Ok(Box::new(client) as Box<dyn TunnelStream>) })
        }

        fn connect_pooled<'a>(
            &'a self,
            _target: &'a TargetAddr,
        ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
            let (client, _server) = tokio::io::duplex(1024);
            Box::pin(async move { Ok(Box::new(client) as Box<dyn TunnelStream>) })
        }
    }

    #[tokio::test]
    async fn test_connection_pool_reactor_replenish_and_acquire() {
        let connector = Arc::new(MockSuccessfulConnector);
        let cancel_token = CancellationToken::new();
        let pool = ReplenishingStreamPool::new_shared(connector, 2, cancel_token.clone());

        // Wait a short moment for background replenishment to reach target_size 2
        tokio::time::sleep(Duration::from_millis(50)).await;
        let stats = pool.stats();
        assert_eq!(stats.idle_count, 2, "Expected 2 idle connections in pool");

        // Acquire one connection
        let stream = pool.acquire(None).await;
        assert!(stream.is_ok(), "Acquire should succeed from idle pool");

        let stats_after = pool.stats();
        assert_eq!(stats_after.active_count, 1, "Expected 1 active stream");

        cancel_token.cancel();
    }

    #[tokio::test]
    async fn test_connection_pool_wait_queue_hand_off() {
        let connector = Arc::new(MockSuccessfulConnector);
        let cancel_token = CancellationToken::new();
        // target_size 0: no automatic background idle replenishment
        let pool = ReplenishingStreamPool::new_shared(connector, 0, cancel_token.clone());

        // Spawn a task waiting on acquire
        let pool_clone = pool.clone();
        let acquire_task = tokio::spawn(async move { pool_clone.acquire(None).await });

        // Give the task a moment to register into pending_requests
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Return a new stream via push_idle
        let (client, _server) = tokio::io::duplex(1024);
        pool.push_idle(Box::new(client));

        // The waiting acquire task should immediately receive the stream via direct hand-off
        let acquired = tokio::time::timeout(Duration::from_secs(1), acquire_task).await;
        assert!(
            acquired.is_ok(),
            "Acquire task should complete via hand-off"
        );
        assert!(
            acquired.unwrap().unwrap().is_ok(),
            "Stream acquisition should succeed"
        );

        // Idle count should still be 0 because it bypassed idle_streams directly to the waiting request
        assert_eq!(
            pool.stats().idle_count,
            0,
            "Handed-off stream must bypass idle_streams"
        );

        cancel_token.cancel();
    }

    #[tokio::test]
    async fn test_on_stream_closed_never_underflows_active_count() {
        let connector = Arc::new(MockSuccessfulConnector);
        let cancel_token = CancellationToken::new();
        let pool = ReplenishingStreamPool::new_shared(connector, 0, cancel_token.clone());

        // Duplicate close notifications used to wrap the counter to usize::MAX.
        pool.on_stream_closed();
        pool.on_stream_closed();
        assert_eq!(pool.stats().active_count, 0);

        cancel_token.cancel();
    }
}
