// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

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

use crate::consts::{
    ACTIVATE_STREAM_MAGIC, POOL_ACQUIRE_TIMEOUT, POOL_BACKOFF_ACTIVE_WAITERS_MAX,
    POOL_BACKOFF_BASE, POOL_BACKOFF_MAX, POOL_COMMAND_QUEUE_SIZE, POOL_DUMMY_HOST,
    POOL_HEARTBEAT_INTERVAL, POOL_HEARTBEAT_TICK_INTERVAL, POOL_HEARTBEAT_TIMEOUT,
    POOL_MAX_CAPACITY, POOL_MAX_CONCURRENT_CONNECTS,
};
use crate::core::error::TunnelError;
use crate::core::stream::{
    BoxFuture, HeartbeatStatus, TunnelStream, activity_now_ms, heartbeat_timing,
};
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
    /// Pure wake-up hint sent when a caller releases a stream. The authoritative
    /// active count is the shared atomic, so this command may be dropped safely.
    ActiveReleased,
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
        heartbeat_interval: Duration,
        idle_timeout: Duration,
    ) -> Self {
        Self::spawn_actor(
            connector,
            Some(target),
            target_size,
            cancel_token,
            heartbeat_interval,
            idle_timeout,
        )
    }

    /// Creates and immediately activates a shared `ReplenishingStreamPool` that connects idle streams
    /// without fixing a target upfront. Target is specified dynamically during `acquire`.
    pub fn new_shared(
        connector: Arc<dyn TransportConnector>,
        target_size: usize,
        cancel_token: CancellationToken,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
    ) -> Self {
        Self::spawn_actor(
            connector,
            None,
            target_size,
            cancel_token,
            heartbeat_interval,
            idle_timeout,
        )
    }

    fn spawn_actor(
        connector: Arc<dyn TransportConnector>,
        default_target: Option<TargetAddr>,
        target_size: usize,
        cancel_token: CancellationToken,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
    ) -> Self {
        let (tx_cmd, mut rx_cmd) = mpsc::channel::<PoolCommand>(POOL_COMMAND_QUEUE_SIZE);
        let stats = Arc::new(Mutex::new(PoolStats::default()));
        let stats_clone = Arc::clone(&stats);
        let active_count = Arc::new(AtomicUsize::new(0));
        // The actor reads the very same atomic that `acquire()` / `on_stream_closed()`
        // maintain. It must never keep a second, message-driven copy: a dropped
        // `StreamClosed` command used to leak that copy upward forever, and once
        // it reached POOL_MAX_CAPACITY replenishment stopped for the tunnel's
        // whole lifetime.
        let active_count_clone = Arc::clone(&active_count);

        tokio::spawn(async move {
            let mut idle_streams: VecDeque<ManagedStream> = VecDeque::new();
            let mut pending_requests: VecDeque<(AcquireRequest, Instant)> = VecDeque::new();
            let mut connecting_futures: FuturesUnordered<
                BoxFuture<'static, Result<Box<dyn TunnelStream>, TunnelError>>,
            > = FuturesUnordered::new();
            let dummy_target = default_target
                .clone()
                .unwrap_or_else(|| TargetAddr::Domain(POOL_DUMMY_HOST.to_string(), 0));

            let mut next_retry_at = Instant::now();
            let mut backoff = POOL_BACKOFF_BASE;
            let mut retry_timer: Option<Pin<Box<tokio::time::Sleep>>> = None;

            let mut heartbeat_ticker = tokio::time::interval_at(
                tokio::time::Instant::now() + POOL_HEARTBEAT_TICK_INTERVAL,
                POOL_HEARTBEAT_TICK_INTERVAL,
            );
            heartbeat_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let cancel_fut = cancel_token.cancelled();
            tokio::pin!(cancel_fut);

            loop {
                // 1. Replenish connecting futures up to every slot currently allowed.
                //    Adding only one connect per reactor turn serialized the entire
                //    refill behind one handshake RTT: after a burst drained the pool
                //    it took N round-trips (plus a 1 s ticker stall for slow dials)
                //    to get back to target_size. Fill the backlog in one turn while
                //    still respecting the capacity, in-flight and backoff gates.
                let now = Instant::now();
                loop {
                    let in_flight = connecting_futures.len();
                    let current_total = idle_streams.len() + in_flight;
                    let active = active_count_clone.load(Ordering::Relaxed);
                    let total_allocated = active + current_total;

                    let has_waiters = !pending_requests.is_empty();
                    let needed = (current_total < target_size)
                        || (has_waiters && in_flight < pending_requests.len());
                    let capacity_available = total_allocated < POOL_MAX_CAPACITY
                        && in_flight < POOL_MAX_CONCURRENT_CONNECTS;

                    // When recovering from failures (backoff > POOL_BACKOFF_BASE),
                    // keep at most one probe in flight so a broken endpoint is not
                    // slammed by a full parallel batch on every retry.
                    let throttled_by_failure = backoff > POOL_BACKOFF_BASE && in_flight >= 1;

                    if !(needed && capacity_available && !throttled_by_failure) {
                        break;
                    }
                    if now < next_retry_at {
                        if in_flight == 0 && retry_timer.is_none() {
                            retry_timer =
                                Some(Box::pin(tokio::time::sleep_until(next_retry_at.into())));
                        }
                        break;
                    }

                    let c = Arc::clone(&connector);
                    let t = dummy_target.clone();
                    connecting_futures.push(Box::pin(async move { c.connect_pooled(&t).await }));
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
                    let now_ms = activity_now_ms();
                    let mut i = 0;
                    let mut evicted_any = false;
                    while i < idle_streams.len() {
                        let mut is_dead = false;
                        let managed = &mut idle_streams[i];
                        let (probe, expired) = heartbeat_timing(
                            managed.stream.last_activity_ms(),
                            now_ms,
                            heartbeat_interval,
                            idle_timeout,
                        );

                        if expired {
                            is_dead = true;
                        } else if let Poll::Ready(HeartbeatStatus::Dead) =
                            managed.stream.poll_check_alive(cx)
                        {
                            is_dead = true;
                        } else {
                            match managed.state {
                                HeartbeatState::Idle(last_check) => {
                                    // Only a genuinely quiet stream needs a probe.
                                    if probe
                                        && sweep_now.duration_since(last_check)
                                            >= POOL_HEARTBEAT_INTERVAL
                                    {
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
                                    if sweep_now.duration_since(started_at) > POOL_HEARTBEAT_TIMEOUT
                                    {
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
                                backoff = POOL_BACKOFF_BASE;
                                next_retry_at = Instant::now();
                                retry_timer = None;

                                // Try direct hand-off to pending requests before pushing to idle_streams
                                let mut cur_stream = Some(stream);
                                while let Some((pending_req, _)) = pending_requests.pop_front() {
                                    if let Some(s) = cur_stream.take() {
                                        match pending_req.reply.send(Ok(s)) {
                                            Ok(()) => {
                                                // The caller increments the shared active
                                                // counter in `acquire()`; do not count here.
                                                stats_clone.lock().total_created += 1;
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
                                    POOL_BACKOFF_ACTIVE_WAITERS_MAX
                                } else {
                                    POOL_BACKOFF_MAX
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
                                    let (_, expired) = heartbeat_timing(
                                        managed.stream.last_activity_ms(),
                                        activity_now_ms(),
                                        heartbeat_interval,
                                        idle_timeout,
                                    );
                                    let mut dead = expired;
                                    if !dead {
                                        let _ = std::future::poll_fn(|cx| {
                                            if let Poll::Ready(HeartbeatStatus::Dead) =
                                                managed.stream.poll_check_alive(cx)
                                            {
                                                dead = true;
                                            }
                                            Poll::Ready(())
                                        })
                                        .await;
                                    }

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
                                    stats_clone.lock().idle_count = idle_streams.len();
                                    let _ = req.reply.send(Ok(stream));
                                } else {
                                    // If pool has no idle streams and no connects currently in-flight,
                                    // reset backoff timer so this incoming demand is attempted promptly
                                    // rather than waiting for an idle replenishment backoff timer.
                                    if pending_requests.is_empty() && connecting_futures.is_empty()
                                    {
                                        next_retry_at = Instant::now();
                                        backoff = POOL_BACKOFF_BASE;
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
                                    stats_clone.lock().idle_count = idle_streams.len();
                                }
                            }
                            Some(PoolCommand::ActiveReleased) => {
                                // Wake-up hint only. The shared atomic is authoritative,
                                // so losing this message under channel pressure is safe;
                                // the reactor simply re-evaluates the refill gates now
                                // instead of on the next 1 s heartbeat tick.
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
                            } else if now.duration_since(*enqueued_at) > POOL_ACQUIRE_TIMEOUT {
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
        // Best-effort wake-up only: the atomic decrement above is the real
        // accounting, so a full command channel cannot leak active count.
        let _ = self.tx_cmd.try_send(PoolCommand::ActiveReleased);
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
    use crate::consts::{DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT};
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

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
        let _pool = ReplenishingStreamPool::new_shared(
            connector,
            4,
            cancel_token.clone(),
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
        );

        // Sleep 400ms. Without backoff, instant failures produced >100,000
        // reconnect attempts. The first turn may now fill all allowed dial slots
        // in parallel (bounded by POOL_MAX_CONCURRENT_CONNECTS), but the
        // exponential backoff gate must still stop any further retry burst.
        tokio::time::sleep(Duration::from_millis(400)).await;
        cancel_token.cancel();

        let count = call_count.load(Ordering::SeqCst);
        assert!(
            count <= POOL_MAX_CONCURRENT_CONNECTS,
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
        let pool = ReplenishingStreamPool::new_shared(
            connector,
            2,
            cancel_token.clone(),
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
        );

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
        let pool = ReplenishingStreamPool::new_shared(
            connector,
            0,
            cancel_token.clone(),
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
        );

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

    /// Reproduces the permanent "one tunnel goes dark" stall. The actor used to
    /// keep a second active counter driven by `StreamClosed` commands sent with
    /// `try_send`; a single dropped command leaked that counter forever, and
    /// once it reached `POOL_MAX_CAPACITY`, `capacity_available` was false for
    /// the rest of the tunnel's life. Simulate the leaked value directly: the
    /// shared atomic is authoritative, so replenishment must still happen.
    #[tokio::test]
    async fn test_replenishment_survives_stale_actor_active_count() {
        let connector = Arc::new(MockSuccessfulConnector);
        let cancel_token = CancellationToken::new();
        let pool = ReplenishingStreamPool::new_shared(
            connector,
            2,
            cancel_token.clone(),
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
        );

        pool.stats.lock().active_count = POOL_MAX_CAPACITY;

        tokio::time::timeout(Duration::from_secs(2), async {
            while pool.stats().idle_count < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pool stayed starved by a stale active counter");
        assert_eq!(pool.stats().active_count, 0);

        cancel_token.cancel();
    }

    /// A burst that drains the pool must refill all allowed dial slots in one
    /// reactor turn. Launching one connect per turn serialized the refill behind
    /// a full handshake RTT per slot (or a 1 s ticker stall for slow dials).
    struct BarrierConnector {
        barrier: Arc<tokio::sync::Barrier>,
    }

    impl TransportConnector for BarrierConnector {
        fn protocol_scheme(&self) -> &'static str {
            "mock"
        }

        fn connect<'a>(
            &'a self,
            _target: &'a TargetAddr,
            _session_id: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
            self.connect_pooled(_target)
        }

        fn connect_pooled<'a>(
            &'a self,
            _target: &'a TargetAddr,
        ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
            let barrier = Arc::clone(&self.barrier);
            Box::pin(async move {
                // Only completes when every allowed slot is dialing concurrently.
                barrier.wait().await;
                let (client, _server) = tokio::io::duplex(1024);
                Ok(Box::new(client) as Box<dyn TunnelStream>)
            })
        }
    }

    #[tokio::test]
    async fn test_pool_refills_all_slots_in_one_reactor_turn() {
        let slots = POOL_MAX_CONCURRENT_CONNECTS;
        let connector = Arc::new(BarrierConnector {
            barrier: Arc::new(tokio::sync::Barrier::new(slots)),
        });
        let cancel_token = CancellationToken::new();
        let pool = ReplenishingStreamPool::new_shared(
            connector,
            slots,
            cancel_token.clone(),
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            while pool.stats().idle_count < slots {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("replenishment serialized dials behind one RTT per slot");

        cancel_token.cancel();
    }

    /// High-concurrency acquire/release must return the active counter to zero
    /// and let the idle population recover, even when release notifications are
    /// lost to channel pressure.
    #[tokio::test]
    async fn test_high_concurrency_release_recovers_active_count_and_idle_pool() {
        let connector = Arc::new(MockSuccessfulConnector);
        let cancel_token = CancellationToken::new();
        let pool = ReplenishingStreamPool::new_shared(
            connector,
            2,
            cancel_token.clone(),
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
        );
        // Wait for the initial warm population.
        tokio::time::timeout(Duration::from_secs(2), async {
            while pool.stats().idle_count < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("initial prewarm failed");

        let mut held = Vec::new();
        for _ in 0..12 {
            held.push(pool.acquire(None).await.expect("acquire must succeed"));
        }
        assert_eq!(pool.stats().active_count, 12);

        drop(held);
        for _ in 0..12 {
            pool.on_stream_closed();
        }
        assert_eq!(pool.stats().active_count, 0);

        tokio::time::timeout(Duration::from_secs(2), async {
            while pool.stats().idle_count < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pool did not replenish after the concurrency burst");

        cancel_token.cancel();
    }

    #[tokio::test]
    async fn test_on_stream_closed_never_underflows_active_count() {
        let connector = Arc::new(MockSuccessfulConnector);
        let cancel_token = CancellationToken::new();
        let pool = ReplenishingStreamPool::new_shared(
            connector,
            0,
            cancel_token.clone(),
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
        );

        // Duplicate close notifications used to wrap the counter to usize::MAX.
        pool.on_stream_closed();
        pool.on_stream_closed();
        assert_eq!(pool.stats().active_count, 0);

        cancel_token.cancel();
    }
}
