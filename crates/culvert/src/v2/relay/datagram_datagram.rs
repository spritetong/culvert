// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Pure data-plane `(Datagram, Datagram)` relay stage.
//! All comments and docstrings must be in English.

use crate::io::channel::{Datagram, DatagramIo};
use crate::v2::error::TaskError;
use crate::v2::relay::datagram_queue::{DATAGRAM_BATCH_SIZE, DatagramQueue};
use crate::v2::relay::duplex::RelayCounters;
use crate::v2::stage::{ActiveStage, ClosingStage};
use crate::v2::state::CloseReason;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::time::Sleep;

/// Batched bidirectional datagram relay implementing `ActiveStage`.
pub struct DatagramDatagramRelay {
    a: Box<dyn DatagramIo>,
    b: Box<dyn DatagramIo>,
    batch_a: Vec<Datagram>,
    batch_b: Vec<Datagram>,
    queue_a_to_b: DatagramQueue,
    queue_b_to_a: DatagramQueue,
    a_done: bool,
    b_done: bool,
    idle_timeout: Duration,
    drain_timeout: Duration,
    tick_interval: Duration,
    last_activity: Instant,
    idle_timer: Pin<Box<Sleep>>,
    counters: Option<Arc<RelayCounters>>,
}

impl DatagramDatagramRelay {
    /// Creates a new batched datagram relay between `a` and `b`.
    pub fn new(
        a: Box<dyn DatagramIo>,
        b: Box<dyn DatagramIo>,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        drain_timeout: Duration,
    ) -> Self {
        let tick_interval = heartbeat_interval
            .min(idle_timeout / 2)
            .max(Duration::from_millis(50));
        Self {
            a,
            b,
            batch_a: vec![Datagram::default(); DATAGRAM_BATCH_SIZE],
            batch_b: vec![Datagram::default(); DATAGRAM_BATCH_SIZE],
            queue_a_to_b: DatagramQueue::new(),
            queue_b_to_a: DatagramQueue::new(),
            a_done: false,
            b_done: false,
            idle_timeout,
            drain_timeout,
            tick_interval,
            last_activity: Instant::now(),
            idle_timer: Box::pin(tokio::time::sleep(tick_interval)),
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
            c.a_to_b.store(
                self.queue_a_to_b.total_bytes(),
                std::sync::atomic::Ordering::Release,
            );
            c.b_to_a.store(
                self.queue_b_to_a.total_bytes(),
                std::sync::atomic::Ordering::Release,
            );
        }
    }

    fn has_pending_outbound(&self) -> bool {
        !self.queue_a_to_b.is_empty() || !self.queue_b_to_a.is_empty()
    }

    fn poll_step_inner(
        &mut self,
        cx: &mut Context<'_>,
        drain_only: bool,
    ) -> Poll<Result<(), TaskError>> {
        let mut progress = false;

        match self.queue_a_to_b.poll_flush(cx, &mut *self.b) {
            Ok(true) => {
                self.last_activity = Instant::now();
                progress = true;
            }
            Ok(false) => {}
            Err(err) => return Poll::Ready(Err(err)),
        }
        match self.queue_b_to_a.poll_flush(cx, &mut *self.a) {
            Ok(true) => {
                self.last_activity = Instant::now();
                progress = true;
            }
            Ok(false) => {}
            Err(err) => return Poll::Ready(Err(err)),
        }

        let batch_len_a = self.queue_a_to_b.available_batch_slots(self.batch_a.len());
        if !self.a_done && !drain_only && batch_len_a > 0 {
            match self.a.poll_recv(cx, &mut self.batch_a[..batch_len_a]) {
                Poll::Ready(Ok(0)) => {
                    self.a_done = true;
                    progress = true;
                }
                Poll::Ready(Ok(n)) => {
                    for d in self.batch_a[..batch_len_a].iter().take(n) {
                        if let Err(err) = self.queue_a_to_b.push(d.clone()) {
                            return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
                        }
                    }
                    self.last_activity = Instant::now();
                    progress = true;
                }
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
                }
                Poll::Pending => {}
            }
        }

        let batch_len_b = self.queue_b_to_a.available_batch_slots(self.batch_b.len());
        if !self.b_done && !drain_only && batch_len_b > 0 {
            match self.b.poll_recv(cx, &mut self.batch_b[..batch_len_b]) {
                Poll::Ready(Ok(0)) => {
                    self.b_done = true;
                    progress = true;
                }
                Poll::Ready(Ok(n)) => {
                    for d in self.batch_b[..batch_len_b].iter().take(n) {
                        if let Err(err) = self.queue_b_to_a.push(d.clone()) {
                            return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
                        }
                    }
                    self.last_activity = Instant::now();
                    progress = true;
                }
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(TaskError::StageFailed(err.to_string())));
                }
                Poll::Pending => {}
            }
        }

        let a_to_b_done = self.a_done && self.queue_a_to_b.is_empty();
        let b_to_a_done = self.b_done && self.queue_b_to_a.is_empty();
        if (a_to_b_done && b_to_a_done)
            || (drain_only && self.queue_a_to_b.is_empty() && self.queue_b_to_a.is_empty())
        {
            return Poll::Ready(Ok(()));
        }

        if progress {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

impl ActiveStage for DatagramDatagramRelay {
    type Closing = DatagramDatagramClosing;

    fn poll_drive(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), TaskError>> {
        let res = self.poll_step_inner(cx, false);
        if res.is_ready() {
            self.sync_counters();
            return res;
        }

        if self.idle_timer.as_mut().poll(cx).is_ready() {
            if !self.has_pending_outbound() && self.last_activity.elapsed() >= self.idle_timeout {
                self.sync_counters();
                return Poll::Ready(Ok(()));
            }
            self.idle_timer
                .as_mut()
                .reset(tokio::time::Instant::now() + self.tick_interval);
            let _ = self.idle_timer.as_mut().poll(cx);
        }

        Poll::Pending
    }

    fn into_closing(self, reason: CloseReason) -> Self::Closing {
        self.sync_counters();
        let drain_timeout = self.drain_timeout;
        DatagramDatagramClosing {
            relay: Some(self),
            reason,
            drain_timeout,
        }
    }
}

/// Bounded graceful-drain stage for `DatagramDatagramRelay`.
pub struct DatagramDatagramClosing {
    relay: Option<DatagramDatagramRelay>,
    reason: CloseReason,
    drain_timeout: Duration,
}

impl ClosingStage for DatagramDatagramClosing {
    fn run(&mut self) -> impl Future<Output = Result<(), TaskError>> + Send {
        let mut relay = self.relay.take();
        let reason = self.reason;
        let drain_timeout = self.drain_timeout;

        async move {
            if matches!(reason, CloseReason::GracefulDrain)
                && let Some(ref mut r) = relay
            {
                let _ =
                    tokio::time::timeout(drain_timeout, poll_fn(|cx| r.poll_step_inner(cx, true)))
                        .await;
                r.sync_counters();
            }
            drop(relay);
            Ok(())
        }
    }
}
