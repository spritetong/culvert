// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Single-writer idle stream inventory, liveness probing, and expiration management.
//! All comments and docstrings must be in English.

use crate::consts::{
    POOL_HEARTBEAT_INTERVAL, POOL_HEARTBEAT_TIMEOUT, reverse_pool_expiration_margin,
};
use crate::io::stream::{HeartbeatStatus, TunnelStream, activity_now_ms, heartbeat_timing};
use crate::v2::pool::config::PoolStats;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Heartbeat lifecycle of a single idle stream.
#[derive(Debug, Clone, Copy)]
enum HeartbeatState {
    Idle(Instant),
    SendingPing(Instant),
}

/// Idle stream entry tracked by `PoolInventory`.
struct ManagedStream {
    stream: Box<dyn TunnelStream>,
    enqueued_at: Instant,
    heartbeat: HeartbeatState,
}

impl ManagedStream {
    fn new(stream: Box<dyn TunnelStream>) -> Self {
        let now = Instant::now();
        Self {
            stream,
            enqueued_at: now,
            heartbeat: HeartbeatState::Idle(now),
        }
    }
}

/// Summary of one background heartbeat sweep over idle streams.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepOutcome {
    pub evicted_dead: usize,
    pub evicted_expired: usize,
}

/// Pure single-writer idle stream queue supporting both Forward and Reverse liveness rules.
pub struct PoolInventory {
    idle: VecDeque<ManagedStream>,
    max_capacity: usize,
    heartbeat_interval: Duration,
    idle_timeout: Duration,
    max_idle_age: Option<Duration>,
    active_count: Arc<AtomicUsize>,
    total_created: u64,
    total_evicted_dead: u64,
    total_evicted_expired: u64,
}

impl PoolInventory {
    /// Creates an empty `PoolInventory`.
    pub fn new(
        max_capacity: usize,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        max_idle_age: Option<Duration>,
    ) -> Self {
        Self {
            idle: VecDeque::new(),
            max_capacity,
            heartbeat_interval,
            idle_timeout,
            max_idle_age,
            active_count: Arc::new(AtomicUsize::new(0)),
            total_created: 0,
            total_evicted_dead: 0,
            total_evicted_expired: 0,
        }
    }

    /// Returns a shared handle to the authoritative active stream counter.
    #[inline]
    pub fn active_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.active_count)
    }

    /// Returns the configured maximum idle inventory capacity.
    #[inline]
    pub fn max_capacity(&self) -> usize {
        self.max_capacity
    }

    /// Returns the current number of idle streams.
    #[inline]
    pub fn idle_len(&self) -> usize {
        self.idle.len()
    }

    /// Records a newly created stream (used when handing off directly to a waiter).
    #[inline]
    pub fn record_created(&mut self) {
        self.total_created += 1;
    }

    /// Pushes a newly created or client-pushed idle stream to the back of the LIFO queue.
    ///
    /// Evicts the oldest entry (`pop_front`) if `max_capacity` is reached. Returns `false`
    /// only when `max_capacity == 0`.
    pub fn push_back(&mut self, stream: Box<dyn TunnelStream>, count_as_created: bool) -> bool {
        if self.max_capacity == 0 {
            return false;
        }
        if self.idle.len() >= self.max_capacity {
            self.idle.pop_front();
        }
        self.idle.push_back(ManagedStream::new(stream));
        if count_as_created {
            self.total_created += 1;
        }
        true
    }

    /// Reclaims a stream whose waiter dropped its receiver right during handoff.
    pub fn push_front_reclaimed(&mut self, stream: Box<dyn TunnelStream>) {
        if self.max_capacity == 0 {
            return;
        }
        if self.idle.len() >= self.max_capacity {
            self.idle.pop_front();
        }
        self.idle.push_front(ManagedStream::new(stream));
    }

    /// Pops the freshest (LIFO) idle stream that passes expiration and `poll_check_alive`.
    ///
    /// Streams currently mid-ping (`SendingPing`) are busy and skipped without eviction.
    /// Never closes a shared underlying transport session on single-stream retirement.
    pub fn take_live_idle(&mut self, cx: &mut Context<'_>) -> Option<Box<dyn TunnelStream>> {
        let now_ms = activity_now_ms();
        let mut idx = self.idle.len();

        while idx > 0 {
            idx -= 1;

            if matches!(self.idle[idx].heartbeat, HeartbeatState::SendingPing(_)) {
                continue;
            }

            if self.is_entry_expired(&self.idle[idx], now_ms) {
                self.idle.remove(idx);
                self.total_evicted_expired += 1;
                continue;
            }

            if matches!(
                self.idle[idx].stream.poll_check_alive(cx),
                Poll::Ready(HeartbeatStatus::Dead)
            ) {
                self.idle.remove(idx);
                self.total_evicted_dead += 1;
                continue;
            }

            if let Some(managed) = self.idle.remove(idx) {
                return Some(managed.stream);
            }
        }

        None
    }

    /// Runs a periodic sweep across idle streams, evicting expired/dead entries and
    /// driving activity-gated heartbeat probes.
    pub fn sweep_idle(&mut self, cx: &mut Context<'_>) -> SweepOutcome {
        let sweep_now = Instant::now();
        let now_ms = activity_now_ms();
        let mut outcome = SweepOutcome::default();
        let mut i = 0;

        while i < self.idle.len() {
            if self.is_entry_expired(&self.idle[i], now_ms) {
                self.idle.swap_remove_back(i);
                outcome.evicted_expired += 1;
                self.total_evicted_expired += 1;
                continue;
            }

            let managed = &mut self.idle[i];
            let (probe, _) = heartbeat_timing(
                managed.stream.last_activity_ms(),
                now_ms,
                self.heartbeat_interval,
                self.idle_timeout,
            );

            let mut is_dead = false;
            if let Poll::Ready(HeartbeatStatus::Dead) = managed.stream.poll_check_alive(cx) {
                is_dead = true;
            } else {
                match managed.heartbeat {
                    HeartbeatState::Idle(last_check) => {
                        if probe && sweep_now.duration_since(last_check) >= POOL_HEARTBEAT_INTERVAL
                        {
                            match managed.stream.poll_send_ping(cx) {
                                Poll::Ready(Ok(())) => {
                                    managed.heartbeat = HeartbeatState::Idle(sweep_now);
                                }
                                Poll::Ready(Err(_)) => {
                                    is_dead = true;
                                }
                                Poll::Pending => {
                                    managed.heartbeat = HeartbeatState::SendingPing(sweep_now);
                                }
                            }
                        }
                    }
                    HeartbeatState::SendingPing(started_at) => {
                        if sweep_now.duration_since(started_at) > POOL_HEARTBEAT_TIMEOUT {
                            is_dead = true;
                        } else {
                            match managed.stream.poll_send_ping(cx) {
                                Poll::Ready(Ok(())) => {
                                    managed.heartbeat = HeartbeatState::Idle(sweep_now);
                                }
                                Poll::Ready(Err(_)) => {
                                    is_dead = true;
                                }
                                Poll::Pending => {}
                            }
                        }
                    }
                }
            }

            if is_dead {
                self.idle.swap_remove_back(i);
                outcome.evicted_dead += 1;
                self.total_evicted_dead += 1;
            } else {
                i += 1;
            }
        }

        outcome
    }

    /// Drops every currently queued idle stream (e.g. superseded reverse generation).
    pub fn clear(&mut self) -> usize {
        let count = self.idle.len();
        self.idle.clear();
        count
    }

    /// Builds a consistent `PoolStats` snapshot.
    pub fn stats(&self, pending_waiters: usize, in_flight_dials: usize) -> PoolStats {
        PoolStats {
            idle_count: self.idle.len(),
            active_count: self.active_count.load(Ordering::Relaxed),
            pending_waiters,
            in_flight_dials,
            total_created: self.total_created,
            total_evicted_dead: self.total_evicted_dead,
            total_evicted_expired: self.total_evicted_expired,
        }
    }

    fn is_entry_expired(&self, entry: &ManagedStream, now_ms: u64) -> bool {
        if let Some(max_age) = self.max_idle_age {
            let safety_margin = reverse_pool_expiration_margin(max_age);
            if entry.enqueued_at.elapsed() + safety_margin >= max_age {
                return true;
            }
        }
        let (_, expired) = heartbeat_timing(
            entry.stream.last_activity_ms(),
            now_ms,
            self.heartbeat_interval,
            self.idle_timeout,
        );
        expired
    }
}
