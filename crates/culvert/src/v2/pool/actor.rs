// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Supervised `TransportPool` Actor and `PoolHandle` supporting both Forward and Reverse modes.
//! All comments and docstrings must be in English.

use crate::consts::POOL_HEARTBEAT_TICK_INTERVAL;
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::v2::pool::config::{PoolConfig, PoolRole, PoolStats, PoolSummary};
use crate::v2::pool::inventory::PoolInventory;
use crate::v2::pool::lease::{PoolReleaseSignal, StreamLease};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Sleep;
use tokio_util::sync::CancellationToken;

const POOL_CMD_QUEUE_CAPACITY: usize = 1024;

struct PendingAcquire {
    reply: oneshot::Sender<Result<StreamLease, TunnelError>>,
    enqueued_at: Instant,
}

enum PoolCommand {
    Acquire {
        reply: oneshot::Sender<Result<StreamLease, TunnelError>>,
    },
    TryAcquire {
        reply: oneshot::Sender<Option<StreamLease>>,
    },
    PushIdle {
        stream: Box<dyn TunnelStream>,
        reply: oneshot::Sender<bool>,
    },
    ClearIdle {
        reply: oneshot::Sender<usize>,
    },
    Stats {
        reply: oneshot::Sender<PoolStats>,
    },
    StopReplenishing {
        reply: oneshot::Sender<()>,
    },
}

struct PoolHandleInner {
    cmd_tx: mpsc::Sender<PoolCommand>,
    cancel_token: CancellationToken,
}

impl Drop for PoolHandleInner {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

/// Cloneable handle to a supervised `TransportPool` Actor.
///
/// Dropping all clones cancels the pool's `CancellationToken` cleanly, ensuring
/// zero detached background pool tasks.
#[derive(Clone)]
pub struct PoolHandle {
    inner: Arc<PoolHandleInner>,
}

impl PoolHandle {
    /// Acquires a live idle stream or waits up to `acquire_timeout` for one to arrive.
    pub async fn acquire(&self) -> Result<StreamLease, TunnelError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(PoolCommand::Acquire { reply: reply_tx })
            .await
            .map_err(|_| TunnelError::PoolExhausted)?;
        reply_rx.await.map_err(|_| TunnelError::PoolExhausted)?
    }

    /// Non-blocking fast-path acquisition: returns `Ok(Some(lease))` immediately if a live
    /// idle stream is queued, or `Ok(None)` without waiting if the pool is empty.
    pub async fn try_acquire(&self) -> Result<Option<StreamLease>, TunnelError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(PoolCommand::TryAcquire { reply: reply_tx })
            .await
            .map_err(|_| TunnelError::PoolExhausted)?;
        reply_rx.await.map_err(|_| TunnelError::PoolExhausted)
    }

    /// Pushes an idle stream into the pool (hands off directly to any waiting `acquire`).
    pub async fn push_idle(&self, stream: Box<dyn TunnelStream>) -> Result<bool, TunnelError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(PoolCommand::PushIdle {
                stream,
                reply: reply_tx,
            })
            .await
            .map_err(|_| TunnelError::PoolExhausted)?;
        reply_rx.await.map_err(|_| TunnelError::PoolExhausted)
    }

    /// Clears all currently queued idle streams (e.g. superseded reverse generation).
    pub async fn clear_idle(&self) -> Result<usize, TunnelError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(PoolCommand::ClearIdle { reply: reply_tx })
            .await
            .map_err(|_| TunnelError::PoolExhausted)?;
        reply_rx.await.map_err(|_| TunnelError::PoolExhausted)
    }

    /// Returns a point-in-time `PoolStats` snapshot.
    pub async fn stats(&self) -> Result<PoolStats, TunnelError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(PoolCommand::Stats { reply: reply_tx })
            .await
            .map_err(|_| TunnelError::PoolExhausted)?;
        reply_rx.await.map_err(|_| TunnelError::PoolExhausted)
    }

    /// Halts background replenishment dialing while allowing existing leases to release during
    /// Coordinator graceful drain.
    pub async fn stop_replenishing(&self) -> Result<(), TunnelError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(PoolCommand::StopReplenishing { reply: reply_tx })
            .await
            .map_err(|_| TunnelError::PoolExhausted)?;
        reply_rx.await.map_err(|_| TunnelError::PoolExhausted)
    }

    /// Immediately stops the pool actor via its `CancellationToken`.
    #[inline]
    pub fn shutdown(&self) {
        self.inner.cancel_token.cancel();
    }

    /// Returns a clone of the pool's `CancellationToken`.
    #[inline]
    pub fn cancel_token(&self) -> CancellationToken {
        self.inner.cancel_token.clone()
    }

    /// Returns `true` if `self` and `other` point to the same underlying `TransportPool` instance.
    #[inline]
    pub fn same_pool(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

/// Unified single-writer stream pool supporting Forward active pre-warming and
/// Reverse passive inbound queueing.
pub struct TransportPool {
    role: PoolRole,
    acquire_timeout: Duration,
    draining: bool,
    inventory: PoolInventory,
    pending: VecDeque<PendingAcquire>,
    connecting: FuturesUnordered<BoxFuture<'static, Result<Box<dyn TunnelStream>, TunnelError>>>,
    next_retry_at: Instant,
    backoff: Duration,
    retry_sleep: Option<Pin<Box<Sleep>>>,
    cancel_token: CancellationToken,
    release_tx: mpsc::Sender<PoolReleaseSignal>,
    release_rx: mpsc::Receiver<PoolReleaseSignal>,
    _keep_cmd_tx: mpsc::Sender<PoolCommand>,
    cmd_rx: mpsc::Receiver<PoolCommand>,
}

impl TransportPool {
    /// Starts a supervised `TransportPool` Actor wired to `cancel_token`.
    pub fn start(
        config: PoolConfig,
        cancel_token: CancellationToken,
    ) -> (PoolHandle, JoinHandle<PoolSummary>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(POOL_CMD_QUEUE_CAPACITY);
        let (release_tx, release_rx) = mpsc::channel(1);

        let handle = PoolHandle {
            inner: Arc::new(PoolHandleInner {
                cmd_tx: cmd_tx.clone(),
                cancel_token: cancel_token.clone(),
            }),
        };

        let initial_backoff = match &config.role {
            PoolRole::ActiveDial { dial, .. } => dial.backoff_base,
            PoolRole::PassiveInbound => Duration::ZERO,
        };

        let inventory = PoolInventory::new(
            config.max_capacity,
            config.heartbeat_interval,
            config.idle_timeout,
            config.max_idle_age,
        );

        let actor = Self {
            role: config.role,
            acquire_timeout: config.acquire_timeout,
            draining: false,
            inventory,
            pending: VecDeque::new(),
            connecting: FuturesUnordered::new(),
            next_retry_at: Instant::now(),
            backoff: initial_backoff,
            retry_sleep: None,
            cancel_token,
            release_tx,
            release_rx,
            _keep_cmd_tx: cmd_tx,
            cmd_rx,
        };

        let join_handle = tokio::spawn(actor.run());
        (handle, join_handle)
    }

    async fn run(mut self) -> PoolSummary {
        let mut heartbeat_ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + POOL_HEARTBEAT_TICK_INTERVAL,
            POOL_HEARTBEAT_TICK_INTERVAL,
        );
        heartbeat_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            self.drain_release_signals();
            self.purge_closed_waiters();
            self.replenish_if_needed();

            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    break;
                }
                Some(_) = self.release_rx.recv() => {
                    self.drain_release_signals();
                }
                cmd_opt = self.cmd_rx.recv() => {
                    let Some(cmd) = cmd_opt else {
                        unreachable!("TransportPool retains _keep_cmd_tx so cmd_rx never closes");
                    };
                    self.handle_command(cmd).await;
                }
                Some(dial_res) = self.connecting.next(), if !self.connecting.is_empty() => {
                    self.handle_dial_result(dial_res);
                }
                _ = Self::poll_retry_sleep(&mut self.retry_sleep), if self.retry_sleep.is_some() => {
                    self.retry_sleep = None;
                }
                _ = heartbeat_ticker.tick() => {
                    self.handle_heartbeat_tick().await;
                }
            }
        }

        for req in self.pending.drain(..) {
            let _ = req.reply.send(Err(TunnelError::PoolExhausted));
        }
        self.connecting.clear();
        self.inventory.clear();

        PoolSummary {
            final_stats: self.inventory.stats(0, 0),
        }
    }

    async fn poll_retry_sleep(sleep_opt: &mut Option<Pin<Box<Sleep>>>) {
        if let Some(sleep) = sleep_opt.as_mut() {
            sleep.as_mut().await;
        }
    }

    fn drain_release_signals(&mut self) {
        while self.release_rx.try_recv().is_ok() {}
    }

    fn purge_closed_waiters(&mut self) {
        self.pending.retain(|req| !req.reply.is_closed());
    }

    fn replenish_if_needed(&mut self) {
        if self.draining {
            self.retry_sleep = None;
            return;
        }
        let PoolRole::ActiveDial { connector, dial } = &self.role else {
            return;
        };

        let now = Instant::now();

        loop {
            let in_flight = self.connecting.len();
            let idle_len = self.inventory.idle_len();
            let current_total = idle_len + in_flight;
            let has_waiters = !self.pending.is_empty();

            let needed = (current_total < dial.target_size)
                || (has_waiters && in_flight < self.pending.len());
            let dial_throttled = in_flight >= dial.max_concurrent_connects;
            let capacity_bound = dial.target_size.max(self.inventory.max_capacity());
            let capacity_available =
                !dial_throttled && (has_waiters || current_total < capacity_bound);
            let throttled_by_failure = self.backoff > dial.backoff_base && in_flight >= 1;

            if !(needed && capacity_available && !throttled_by_failure) {
                break;
            }

            if now < self.next_retry_at {
                if in_flight == 0 && self.retry_sleep.is_none() {
                    self.retry_sleep = Some(Box::pin(tokio::time::sleep_until(
                        self.next_retry_at.into(),
                    )));
                }
                break;
            }

            let conn = Arc::clone(connector);
            let target = dial.target.clone();
            let protocol = dial.protocol;
            let tunnel_id = dial.tunnel_id.clone();

            self.connecting.push(Box::pin(async move {
                conn.connect_pooled_with_tunnel(&target, protocol, tunnel_id.as_deref())
                    .await
                    .and_then(|ch| ch.into_stream())
            }));
        }
    }

    fn handle_dial_result(&mut self, res: Result<Box<dyn TunnelStream>, TunnelError>) {
        let PoolRole::ActiveDial { dial, .. } = &self.role else {
            return;
        };

        match res {
            Ok(stream) => {
                self.backoff = dial.backoff_base;
                self.next_retry_at = Instant::now();
                self.retry_sleep = None;

                if let Some(leftover) = self.deliver_to_pending(stream, true) {
                    self.inventory.push_back(leftover, true);
                }
            }
            Err(err) => {
                tracing::debug!(error = ?err, "TransportPool background dial failed");
                let max_backoff = if self.pending.is_empty() {
                    dial.backoff_max
                } else {
                    dial.backoff_active_waiters_max
                };
                self.next_retry_at = Instant::now() + self.backoff;
                self.backoff = (self.backoff * 2).min(max_backoff);
                self.retry_sleep = Some(Box::pin(tokio::time::sleep_until(
                    self.next_retry_at.into(),
                )));
            }
        }
    }

    async fn handle_command(&mut self, cmd: PoolCommand) {
        match cmd {
            PoolCommand::Acquire { reply } => {
                if reply.is_closed() {
                    return;
                }
                let acquired = poll_fn(|cx| Poll::Ready(self.inventory.take_live_idle(cx))).await;
                if let Some(stream) = acquired {
                    self.send_lease_or_reclaim(reply, stream);
                } else {
                    if let PoolRole::ActiveDial { dial, .. } = &self.role
                        && self.pending.is_empty()
                        && self.connecting.is_empty()
                    {
                        self.next_retry_at = Instant::now();
                        self.backoff = dial.backoff_base;
                        self.retry_sleep = None;
                    }
                    self.pending.push_back(PendingAcquire {
                        reply,
                        enqueued_at: Instant::now(),
                    });
                }
            }
            PoolCommand::TryAcquire { reply } => {
                if reply.is_closed() {
                    return;
                }
                let acquired = poll_fn(|cx| Poll::Ready(self.inventory.take_live_idle(cx))).await;
                let lease = acquired.map(|stream| self.new_lease(stream));
                if let Err(Some(returned)) = reply.send(lease)
                    && let Some(raw) = returned.reclaim_stream()
                {
                    self.inventory.push_front_reclaimed(raw);
                }
            }
            PoolCommand::PushIdle { stream, reply } => {
                if let Some(leftover) = self.deliver_to_pending(stream, false) {
                    let stored = self.inventory.push_back(leftover, false);
                    let _ = reply.send(stored);
                } else {
                    let _ = reply.send(true);
                }
            }
            PoolCommand::ClearIdle { reply } => {
                let cleared = self.inventory.clear();
                let _ = reply.send(cleared);
            }
            PoolCommand::Stats { reply } => {
                self.purge_closed_waiters();
                let _ = reply.send(
                    self.inventory
                        .stats(self.pending.len(), self.connecting.len()),
                );
            }
            PoolCommand::StopReplenishing { reply } => {
                self.draining = true;
                self.retry_sleep = None;
                let _ = reply.send(());
            }
        }
    }

    async fn handle_heartbeat_tick(&mut self) {
        if matches!(self.role, PoolRole::ActiveDial { .. }) {
            let _ = poll_fn(|cx| Poll::Ready(self.inventory.sweep_idle(cx))).await;
        }

        let now = Instant::now();
        let mut live = VecDeque::with_capacity(self.pending.len());
        while let Some(req) = self.pending.pop_front() {
            if req.reply.is_closed() {
                continue;
            }
            if now.duration_since(req.enqueued_at) > self.acquire_timeout {
                let _ = req.reply.send(Err(TunnelError::PoolExhausted));
            } else {
                live.push_back(req);
            }
        }
        self.pending = live;
    }

    fn new_lease(&self, stream: Box<dyn TunnelStream>) -> StreamLease {
        StreamLease::new(
            stream,
            self.inventory.active_counter(),
            self.release_tx.clone(),
        )
    }

    fn send_lease_or_reclaim(
        &mut self,
        reply: oneshot::Sender<Result<StreamLease, TunnelError>>,
        stream: Box<dyn TunnelStream>,
    ) {
        let lease = self.new_lease(stream);
        if let Err(Ok(returned)) = reply.send(Ok(lease))
            && let Some(raw) = returned.reclaim_stream()
            && let Some(leftover) = self.deliver_to_pending(raw, false)
        {
            self.inventory.push_front_reclaimed(leftover);
        }
    }

    fn deliver_to_pending(
        &mut self,
        stream: Box<dyn TunnelStream>,
        count_as_created: bool,
    ) -> Option<Box<dyn TunnelStream>> {
        let mut current = Some(stream);
        while let Some(req) = self.pending.pop_front() {
            if let Some(candidate) = current.take() {
                let lease = self.new_lease(candidate);
                match req.reply.send(Ok(lease)) {
                    Ok(()) => {
                        if count_as_created {
                            self.inventory.record_created();
                        }
                        return None;
                    }
                    Err(Ok(returned)) => {
                        current = returned.reclaim_stream();
                    }
                    Err(Err(_)) => break,
                }
            }
        }
        current
    }
}
