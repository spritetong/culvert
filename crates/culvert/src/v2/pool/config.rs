// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Configuration, role selection, and statistics for `TransportPool`.
//! All comments and docstrings must be in English.

use crate::consts::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT, POOL_ACQUIRE_TIMEOUT,
    POOL_BACKOFF_ACTIVE_WAITERS_MAX, POOL_BACKOFF_BASE, POOL_BACKOFF_MAX, POOL_DUMMY_HOST,
    POOL_MAX_CAPACITY, POOL_MAX_CONCURRENT_CONNECTS,
};
use crate::io::addr::{L4Protocol, TargetAddr};
use crate::transport::TransportConnector;
use std::sync::Arc;
use std::time::Duration;

/// Dialing parameters used when `TransportPool` runs in active pre-warming mode.
#[derive(Clone)]
pub struct PoolDialConfig {
    pub target: TargetAddr,
    pub protocol: L4Protocol,
    pub tunnel_id: Option<String>,
    pub target_size: usize,
    pub max_concurrent_connects: usize,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    pub backoff_active_waiters_max: Duration,
}

impl PoolDialConfig {
    /// Creates default dialing parameters targeting `target_size` pre-warmed streams.
    pub fn new(target_size: usize) -> Self {
        Self {
            target: TargetAddr::Domain(POOL_DUMMY_HOST.to_string(), 0),
            protocol: L4Protocol::Tcp,
            tunnel_id: None,
            target_size,
            max_concurrent_connects: POOL_MAX_CONCURRENT_CONNECTS.max(1),
            backoff_base: POOL_BACKOFF_BASE,
            backoff_max: POOL_BACKOFF_MAX,
            backoff_active_waiters_max: POOL_BACKOFF_ACTIVE_WAITERS_MAX,
        }
    }
}

/// Operating mode of a `TransportPool` instance.
#[derive(Clone)]
pub enum PoolRole {
    /// Forward client mode: actively dials pre-warmed streams and probes idle activity.
    ActiveDial {
        connector: Arc<dyn TransportConnector>,
        dial: PoolDialConfig,
    },
    /// Reverse server inbound mode: passively holds streams pushed by remote clients.
    PassiveInbound,
}

/// Complete configuration for starting a `TransportPool`.
#[derive(Clone)]
pub struct PoolConfig {
    pub role: PoolRole,
    pub max_capacity: usize,
    pub acquire_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub idle_timeout: Duration,
    /// Optional wall-clock age cap (`enqueued_at + margin >= max_idle_age`), used in reverse mode.
    pub max_idle_age: Option<Duration>,
}

impl PoolConfig {
    /// Builds a forward client configuration with active dialing and activity-clock heartbeats.
    pub fn forward(
        connector: Arc<dyn TransportConnector>,
        target_size: usize,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            role: PoolRole::ActiveDial {
                connector,
                dial: PoolDialConfig::new(target_size),
            },
            max_capacity: POOL_MAX_CAPACITY.max(target_size),
            acquire_timeout: POOL_ACQUIRE_TIMEOUT,
            heartbeat_interval,
            idle_timeout,
            max_idle_age: None,
        }
    }

    /// Builds a reverse server inbound configuration holding client-pushed idle streams.
    pub fn reverse_inbound(capacity: usize, max_idle_age: Duration) -> Self {
        Self {
            role: PoolRole::PassiveInbound,
            max_capacity: capacity,
            acquire_timeout: POOL_ACQUIRE_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_idle_age: Some(max_idle_age),
        }
    }

    /// Overrides the dial target, L4 protocol, and optional tunnel ID (Forward active-dial mode).
    pub fn with_dial_routing(
        mut self,
        target: TargetAddr,
        protocol: L4Protocol,
        tunnel_id: Option<String>,
    ) -> Self {
        if let PoolRole::ActiveDial { ref mut dial, .. } = self.role {
            dial.target = target;
            dial.protocol = protocol;
            dial.tunnel_id = tunnel_id;
        }
        self
    }

    /// Overrides the maximum idle inventory capacity.
    pub fn with_max_capacity(mut self, max_capacity: usize) -> Self {
        self.max_capacity = max_capacity;
        self
    }

    /// Overrides the maximum wait duration for `acquire()`.
    pub fn with_acquire_timeout(mut self, acquire_timeout: Duration) -> Self {
        self.acquire_timeout = acquire_timeout;
        self
    }
}

/// Snapshot of pool inventory and lifecycle counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    pub idle_count: usize,
    pub active_count: usize,
    pub pending_waiters: usize,
    pub in_flight_dials: usize,
    pub total_created: u64,
    pub total_evicted_dead: u64,
    pub total_evicted_expired: u64,
}

/// Summary returned when the `TransportPool` actor finishes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolSummary {
    pub final_stats: PoolStats,
}
