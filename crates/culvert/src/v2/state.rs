// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Core state vocabulary for the unified v2 Coordinator and IO Task state machine.
//! All comments and docstrings must be in English.

use crate::consts::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT, POOL_ACQUIRE_TIMEOUT,
    POOL_BACKOFF_ACTIVE_WAITERS_MAX, POOL_BACKOFF_BASE, POOL_BACKOFF_MAX, POOL_DUMMY_HOST,
    POOL_MAX_CAPACITY, POOL_MAX_CONCURRENT_CONNECTS,
};
use crate::io::addr::{L4Protocol, TargetAddr};
use crate::io::stream::TunnelStream;
use crate::transport::TransportConnector;
use crate::v2::error::TaskError;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

/// Persistent identifier of a supervised, recyclable `IoTask` worker slot.
///
/// A `TaskId` remains constant across multiple work cycles (`Empty -> ... -> Empty`)
/// until the worker task exits to `Closed`. Individual work cycles are identified by [`JobId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(u64);

impl TaskId {
    /// Creates a `TaskId` from a raw sequence number.
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the underlying integer value.
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "task#{}", self.0)
    }
}

/// Monotonically increasing identifier of one job cycle executed on an `IoTask`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JobId(u64);

impl JobId {
    /// Creates a `JobId` from a raw sequence number.
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the underlying integer value.
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "job#{}", self.0)
    }
}

/// Monotonically increasing identifier for one idle transport residency on an `IoTask`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IdleId(u64);

impl IdleId {
    /// Creates an `IdleId` from a raw sequence number.
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the underlying integer value.
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for IdleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "idle#{}", self.0)
    }
}

/// External lifecycle states of an `IoTask` as tracked by the Coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExternalState {
    /// Task is idle with zero I/O resources, awaiting `Assign`, `SupplyIdle`, or `Shutdown`.
    Empty,
    /// Task holds a live pre-warmed/inbound transport stream and drives keepalive/eviction.
    Idle,
    /// Task received input and is acquiring, activating, or handshaking its resources.
    Connecting,
    /// Task exclusively owns paired resources and runs the hot-path loop.
    Active,
    /// Task is draining buffered work or releasing resources before recycling/exiting.
    Closing,
    /// Terminal state after the task event loop exits.
    Closed,
}

impl ExternalState {
    /// Returns `true` if `self` can advance monotonically forward to `target` within one cycle.
    #[inline]
    pub const fn can_advance_to(self, target: Self) -> bool {
        self.rank() <= target.rank()
    }

    /// Returns `true` if a live task in `self` can eventually reach `target`, accounting for
    /// forward progress within a cycle as well as returning to `Empty`/`Idle` after reuse.
    #[inline]
    pub const fn can_reach(self, target: Self) -> bool {
        self.can_advance_to(target) || (!self.is_terminal() && matches!(target, Self::Empty))
    }

    /// Returns `true` if this state is terminal.
    #[inline]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Closed)
    }

    #[inline]
    const fn rank(self) -> u8 {
        match self {
            Self::Empty => 0,
            Self::Idle => 1,
            Self::Connecting => 2,
            Self::Active => 3,
            Self::Closing => 4,
            Self::Closed => 5,
        }
    }
}

impl fmt::Display for ExternalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Empty => "Empty",
            Self::Idle => "Idle",
            Self::Connecting => "Connecting",
            Self::Active => "Active",
            Self::Closing => "Closing",
            Self::Closed => "Closed",
        })
    }
}

/// Owned pre-warmed/inbound transport stream together with its sweep metadata.
pub struct PooledTransport {
    pub stream: Box<dyn TunnelStream>,
    pub session_id: Option<u64>,
    pub generation: u64,
}

impl PooledTransport {
    /// Wraps `stream`, automatically extracting its shared `session_id()` if supported.
    pub fn new(stream: Box<dyn TunnelStream>, generation: u64) -> Self {
        let session_id = stream.session_id();
        Self {
            stream,
            session_id,
            generation,
        }
    }

    /// Explicitly overrides `session_id` (primarily for deterministic unit tests).
    pub fn with_session_id(mut self, session_id: Option<u64>) -> Self {
        self.session_id = session_id;
        self
    }
}

impl fmt::Debug for PooledTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PooledTransport")
            .field("session_id", &self.session_id)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

/// Keepalive and expiration rules executed locally by each `Idle` `IoTask`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdlePolicy {
    pub heartbeat_interval: Duration,
    pub idle_timeout: Duration,
    /// Optional wall-clock age cap (`enqueued_at + margin >= max_idle_age`), used in Reverse mode.
    pub max_idle_age: Option<Duration>,
    /// Whether this `Idle` task sends periodic probes (`true` in Forward, `false` in Reverse server).
    pub probe_enabled: bool,
}

impl IdlePolicy {
    /// Policy for actively dialed Forward idle tasks.
    pub fn forward(heartbeat_interval: Duration, idle_timeout: Duration) -> Self {
        Self {
            heartbeat_interval,
            idle_timeout,
            max_idle_age: None,
            probe_enabled: true,
        }
    }

    /// Policy for Reverse server passive inbound idle tasks (server owns wall-clock cap).
    pub fn reverse_inbound(max_idle_age: Duration) -> Self {
        Self {
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_idle_age: Some(max_idle_age),
            probe_enabled: false,
        }
    }
}

impl Default for IdlePolicy {
    fn default() -> Self {
        Self::forward(DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT)
    }
}

/// Dialing parameters used when Coordinator drives background stream pre-warming (Forward mode).
#[derive(Clone)]
pub struct DialConfig {
    pub connector: Arc<dyn TransportConnector>,
    pub target: TargetAddr,
    pub protocol: L4Protocol,
    pub tunnel_id: Option<String>,
    pub target_size: usize,
    pub max_concurrent_connects: usize,
    pub acquire_timeout: Duration,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    pub backoff_active_waiters_max: Duration,
}

impl DialConfig {
    /// Creates default dialing parameters targeting `target_size` `Idle` tasks.
    pub fn new(connector: Arc<dyn TransportConnector>, target_size: usize) -> Self {
        Self {
            connector,
            target: TargetAddr::Domain(POOL_DUMMY_HOST.to_string(), 0),
            protocol: L4Protocol::Tcp,
            tunnel_id: None,
            target_size,
            max_concurrent_connects: POOL_MAX_CONCURRENT_CONNECTS.max(1),
            acquire_timeout: POOL_ACQUIRE_TIMEOUT,
            backoff_base: POOL_BACKOFF_BASE,
            backoff_max: POOL_BACKOFF_MAX,
            backoff_active_waiters_max: POOL_BACKOFF_ACTIVE_WAITERS_MAX,
        }
    }

    /// Overrides dial target, L4 protocol, and optional tunnel ID.
    pub fn with_routing(
        mut self,
        target: TargetAddr,
        protocol: L4Protocol,
        tunnel_id: Option<String>,
    ) -> Self {
        self.target = target;
        self.protocol = protocol;
        self.tunnel_id = tunnel_id;
        self
    }

    /// Overrides the timeout for `ConnectingContext::request_transport`.
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }
}

/// Coordinator-wide configuration for idle capacity, policy, and optional active dialing.
#[derive(Clone)]
pub struct CoordinatorConfig {
    pub drain_timeout: Duration,
    pub max_idle_capacity: usize,
    pub idle_policy: IdlePolicy,
    pub dial: Option<DialConfig>,
}

impl CoordinatorConfig {
    /// Creates a plain Coordinator configuration with no background pool dialing.
    pub fn plain(drain_timeout: Duration) -> Self {
        Self {
            drain_timeout,
            max_idle_capacity: 0,
            idle_policy: IdlePolicy::default(),
            dial: None,
        }
    }

    /// Creates a Forward pooled Coordinator configuration with active background dialing.
    pub fn forward_pooled(
        connector: Arc<dyn TransportConnector>,
        target_size: usize,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        drain_timeout: Duration,
    ) -> Self {
        Self {
            drain_timeout,
            max_idle_capacity: POOL_MAX_CAPACITY.max(target_size),
            idle_policy: IdlePolicy::forward(heartbeat_interval, idle_timeout),
            dial: Some(DialConfig::new(connector, target_size)),
        }
    }

    /// Creates a Reverse server Coordinator configuration holding inbound `Idle` tasks.
    pub fn reverse_inbound(
        max_idle_capacity: usize,
        max_idle_age: Duration,
        drain_timeout: Duration,
    ) -> Self {
        Self {
            drain_timeout,
            max_idle_capacity,
            idle_policy: IdlePolicy::reverse_inbound(max_idle_age),
            dial: None,
        }
    }
}

/// Snapshot of idle/active/dialing metrics maintained by Coordinator & `StateEngine`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    pub empty_count: usize,
    pub idle_count: usize,
    pub active_count: usize,
    pub pending_waiters: usize,
    pub in_flight_dials: usize,
    pub total_idle_assigned: u64,
    pub total_idle_lost: u64,
}

/// Tunnel-level coordination phase owned exclusively by the Coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TunnelPhase {
    /// Accepting new work and recycling completed tasks back to `Empty`.
    Running,
    /// Rejecting new work, shutting down `Empty`/`Idle` tasks, and draining active tasks.
    Draining,
    /// Immediately terminating all tasks via `CancellationToken`.
    ForceStopping,
    /// All tasks have terminated and the Coordinator has exited.
    Stopped,
}

impl TunnelPhase {
    /// Returns `true` when new assignments, idle transports, and recycling are permitted.
    #[inline]
    pub const fn accepts_work(self) -> bool {
        matches!(self, Self::Running)
    }
}

impl fmt::Display for TunnelPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Running => "Running",
            Self::Draining => "Draining",
            Self::ForceStopping => "ForceStopping",
            Self::Stopped => "Stopped",
        })
    }
}

/// Asynchronous directive sent from Coordinator to a Task over its unbounded channel.
#[derive(Debug)]
pub enum TaskDirective<I> {
    /// Delivers input payload and `job_id` to transition `Empty`/`Idle -> Connecting`.
    Assign { job_id: JobId, input: I },
    /// Supplies an idle transport to an `Empty` task, transitioning it into `Idle`.
    SupplyIdle {
        idle_id: IdleId,
        transport: PooledTransport,
        policy: IdlePolicy,
    },
    /// Sweep directive closing a dead/superseded `Idle` transport without exiting the Worker.
    CloseTransport { expected_idle_id: Option<IdleId> },
    /// Requests bounded graceful drain/recycle; ignored when already `Empty`.
    Drain,
    /// Requests an `Idle` task to hand its stream directly to an external caller (e.g. `0xA6` promote).
    ExtractTransport {
        expected_idle_id: IdleId,
        reply: oneshot::Sender<Option<PooledTransport>>,
    },
    /// Requests the task loop to terminate cleanly and exit to `Closed`.
    Shutdown,
}

/// Reason an `ActiveStage` is transitioning into `ClosingStage`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// Normal EOF or completion returned by the active hot-path stage.
    NormalEnd,
    /// Active stage encountered a runtime error.
    StageError,
    /// Coordinator requested a graceful drain or proactive task recycle.
    GracefulDrain,
}

/// Outcome recorded for one completed job or terminated task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome {
    /// Job completed normally through `Active` -> `Closing`.
    Completed,
    /// Job completed graceful drain or proactive recycle.
    GracefullyDrained,
    /// Task was terminated immediately via `CancellationToken` (`ForceStop`).
    ForceStopped,
    /// Task exited cleanly while idle or after graceful drain.
    Shutdown,
    /// Job aborted due to a stage or coordination error.
    Failed(TaskError),
}

/// Result of waiting on another task's external state via the Coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitResult {
    /// Target task reached the desired external state.
    Reached,
    /// Target task entered a state that can no longer reach the desired state.
    TargetClosed(ExternalState),
    /// Tunnel began shutting down before the dependency was satisfied.
    Cancelled,
    /// Waiting would introduce a cycle in the Coordinator dependency graph.
    CycleDetected,
    /// Target task ID is not registered in the Coordinator.
    TargetNotFound,
}
