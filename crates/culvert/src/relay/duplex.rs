// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Pure data-plane duplex stream relay (`ActiveStage`) and bounded drain (`ClosingStage`).
//! All comments and docstrings must be in English.

use crate::consts::RELAY_BUFFER_SIZE;
use crate::core::error::TaskError;
use crate::core::stage::{ActiveStage, ClosingStage};
use crate::core::state::CloseReason;
use crate::io::stream::{
    HeartbeatStatus, StreamCaps, TunnelStream, activity_now_ms, heartbeat_timing,
};
use crate::relay::direction::TransferDirection;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Sleep;

/// Optional atomic counters for observing transferred byte totals across stages.
#[derive(Debug, Default)]
pub struct RelayCounters {
    pub a_to_b: AtomicU64,
    pub b_to_a: AtomicU64,
}

impl RelayCounters {
    /// Creates zero-initialized relay counters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `(a_to_b_bytes, b_to_a_bytes)`.
    #[inline]
    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.a_to_b.load(Ordering::Acquire),
            self.b_to_a.load(Ordering::Acquire),
        )
    }

    fn sync(&self, a_to_b: u64, b_to_a: u64) {
        self.a_to_b.store(a_to_b, Ordering::Release);
        self.b_to_a.store(b_to_a, Ordering::Release);
    }
}

/// Low-abstraction pure data-plane duplex relay implementing `ActiveStage`.
///
/// Does not own a `CancellationToken` or handle tunnel-level shutdown directly;
/// lifecycle preemption and graceful drain boundaries live in `IoTask`.
pub struct DuplexRelay {
    stream_a: Box<dyn TunnelStream>,
    stream_b: Box<dyn TunnelStream>,
    caps_a: StreamCaps,
    caps_b: StreamCaps,
    a_to_b: TransferDirection,
    b_to_a: TransferDirection,
    heartbeat_interval: Duration,
    idle_timeout: Duration,
    drain_timeout: Duration,
    tick_interval: Duration,
    heartbeat_timer: Pin<Box<Sleep>>,
    counters: Option<Arc<RelayCounters>>,
}

impl DuplexRelay {
    /// Constructs a new `DuplexRelay` pairing `stream_a` and `stream_b`.
    pub fn new(
        stream_a: Box<dyn TunnelStream>,
        stream_b: Box<dyn TunnelStream>,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        drain_timeout: Duration,
    ) -> Self {
        let caps_a = stream_a.caps();
        let caps_b = stream_b.caps();
        let tick_interval = (heartbeat_interval / 2)
            .min(idle_timeout / 2)
            .min(Duration::from_millis(500))
            .max(Duration::from_millis(5));
        Self {
            stream_a,
            stream_b,
            caps_a,
            caps_b,
            a_to_b: TransferDirection::new(RELAY_BUFFER_SIZE),
            b_to_a: TransferDirection::new(RELAY_BUFFER_SIZE),
            heartbeat_interval,
            idle_timeout,
            drain_timeout,
            tick_interval,
            heartbeat_timer: Box::pin(tokio::time::sleep(tick_interval)),
            counters: None,
        }
    }

    /// Attaches shared atomic counters updated when leaving `Active` / `Closing`.
    pub fn with_counters(mut self, counters: Arc<RelayCounters>) -> Self {
        self.counters = Some(counters);
        self
    }

    fn check_heartbeats(&mut self, cx: &mut Context<'_>) -> Result<(), TaskError> {
        let now_ms = activity_now_ms();
        let (probe_a, _) = heartbeat_timing(
            self.stream_a.last_activity_ms(),
            now_ms,
            self.heartbeat_interval,
            self.idle_timeout,
        );
        let (probe_b, _) = heartbeat_timing(
            self.stream_b.last_activity_ms(),
            now_ms,
            self.heartbeat_interval,
            self.idle_timeout,
        );

        if !self.a_to_b.is_read_done()
            && probe_a
            && let Poll::Ready(Err(err)) = self.stream_a.poll_send_ping(cx)
        {
            return Err(TaskError::StageFailed(format!(
                "stream A ping failed: {err}"
            )));
        }
        if !self.b_to_a.is_read_done()
            && probe_b
            && let Poll::Ready(Err(err)) = self.stream_b.poll_send_ping(cx)
        {
            return Err(TaskError::StageFailed(format!(
                "stream B ping failed: {err}"
            )));
        }

        if !self.a_to_b.is_read_done()
            && let Poll::Ready(HeartbeatStatus::Dead) = self.stream_a.poll_check_alive(cx)
        {
            return Err(TaskError::StageFailed(
                "stream A unresponsive to heartbeats".into(),
            ));
        }
        if !self.b_to_a.is_read_done()
            && let Poll::Ready(HeartbeatStatus::Dead) = self.stream_b.poll_check_alive(cx)
        {
            return Err(TaskError::StageFailed(
                "stream B unresponsive to heartbeats".into(),
            ));
        }

        let (_, expired_a) = heartbeat_timing(
            self.stream_a.last_activity_ms(),
            now_ms,
            self.heartbeat_interval,
            self.idle_timeout,
        );
        let (_, expired_b) = heartbeat_timing(
            self.stream_b.last_activity_ms(),
            now_ms,
            self.heartbeat_interval,
            self.idle_timeout,
        );

        if !self.a_to_b.is_read_done() && !self.a_to_b.has_buffered_data() && expired_a {
            return Err(TaskError::StageFailed("stream A idle timeout".into()));
        }
        if !self.b_to_a.is_read_done() && !self.b_to_a.has_buffered_data() && expired_b {
            return Err(TaskError::StageFailed("stream B idle timeout".into()));
        }

        Ok(())
    }

    fn poll_drain_step(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), TaskError>> {
        let mut progress = false;

        match self.a_to_b.poll_step(
            cx,
            &mut *self.stream_a,
            &mut *self.stream_b,
            true,
            self.caps_b,
        ) {
            Poll::Ready(Ok(p)) => progress |= p,
            Poll::Ready(Err(err)) => {
                return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
            }
            Poll::Pending => {}
        }

        match self.b_to_a.poll_step(
            cx,
            &mut *self.stream_b,
            &mut *self.stream_a,
            true,
            self.caps_a,
        ) {
            Poll::Ready(Ok(p)) => progress |= p,
            Poll::Ready(Err(err)) => {
                return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
            }
            Poll::Pending => {}
        }

        if self.a_to_b.is_finished() && self.b_to_a.is_finished() {
            return Poll::Ready(Ok(()));
        }

        if progress {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }

    fn sync_counters(&self) {
        if let Some(ref c) = self.counters {
            c.sync(self.a_to_b.total_bytes(), self.b_to_a.total_bytes());
        }
    }
}

impl ActiveStage for DuplexRelay {
    type Closing = RelayClosing;

    fn poll_drive(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), TaskError>> {
        let mut progress = false;

        match self.a_to_b.poll_step(
            cx,
            &mut *self.stream_a,
            &mut *self.stream_b,
            false,
            self.caps_b,
        ) {
            Poll::Ready(Ok(p)) => progress |= p,
            Poll::Ready(Err(err)) => {
                self.sync_counters();
                return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
            }
            Poll::Pending => {}
        }

        match self.b_to_a.poll_step(
            cx,
            &mut *self.stream_b,
            &mut *self.stream_a,
            false,
            self.caps_a,
        ) {
            Poll::Ready(Ok(p)) => progress |= p,
            Poll::Ready(Err(err)) => {
                self.sync_counters();
                return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
            }
            Poll::Pending => {}
        }

        if self.a_to_b.is_finished() && self.b_to_a.is_finished() {
            self.sync_counters();
            return Poll::Ready(Ok(()));
        }

        if self.heartbeat_timer.as_mut().poll(cx).is_ready() {
            self.heartbeat_timer
                .as_mut()
                .reset(tokio::time::Instant::now() + self.tick_interval);
            let _ = self.heartbeat_timer.as_mut().poll(cx);
            if let Err(err) = self.check_heartbeats(cx) {
                self.sync_counters();
                return Poll::Ready(Err(err));
            }
        }

        if progress {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }

    fn into_closing(self, reason: CloseReason) -> Self::Closing {
        self.sync_counters();
        let drain_timeout = self.drain_timeout;
        RelayClosing {
            relay: Some(self),
            reason,
            drain_timeout,
        }
    }
}

/// Bounded graceful-drain and cleanup stage paired with `DuplexRelay`.
pub struct RelayClosing {
    relay: Option<DuplexRelay>,
    reason: CloseReason,
    drain_timeout: Duration,
}

impl ClosingStage for RelayClosing {
    fn run(&mut self) -> impl Future<Output = Result<(), TaskError>> + Send {
        let relay = self.relay.take();
        let reason = self.reason;
        let drain_timeout = self.drain_timeout;

        super::run_bounded_closing(
            relay,
            reason,
            drain_timeout,
            DuplexRelay::poll_drain_step,
            DuplexRelay::sync_counters,
        )
    }
}
