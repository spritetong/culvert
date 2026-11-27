// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Core state vocabulary for the v2 Coordinator and IO Task state machine.
//! All comments and docstrings must be in English.

use crate::v2::error::TaskError;
use std::fmt;

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
///
/// Allocated once per `Assign` and cleared when the worker recycles back to `Empty` or exits,
/// preventing late microtask updates from a previous job cycle from affecting a recycled worker.
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

/// External lifecycle states of an `IoTask` as tracked by the Coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExternalState {
    /// Task is idle with zero I/O resources, awaiting `Assign` or `Shutdown`.
    Empty,
    /// Task received input and is acquiring or handshaking its resources.
    Connecting,
    /// Task exclusively owns paired resources and runs the hot-path loop.
    Active,
    /// Task is draining buffered work or releasing resources before recycling/exiting.
    Closing,
    /// Terminal state after the task event loop exits.
    Closed,
}

impl ExternalState {
    /// Returns `true` if `self` can advance monotonically forward to `target` within one job.
    #[inline]
    pub const fn can_advance_to(self, target: Self) -> bool {
        self.rank() <= target.rank()
    }

    /// Returns `true` if a live task in `self` can eventually reach `target`, accounting for
    /// forward progress within the current job as well as recycling back to `Empty`.
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
            Self::Connecting => 1,
            Self::Active => 2,
            Self::Closing => 3,
            Self::Closed => 4,
        }
    }
}

impl fmt::Display for ExternalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Empty => "Empty",
            Self::Connecting => "Connecting",
            Self::Active => "Active",
            Self::Closing => "Closing",
            Self::Closed => "Closed",
        })
    }
}

/// Tunnel-level coordination phase owned exclusively by the Coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TunnelPhase {
    /// Accepting new work and recycling completed tasks back to `Empty`.
    Running,
    /// Rejecting new work, shutting down idle tasks, and draining active tasks.
    Draining,
    /// Immediately terminating all tasks via `CancellationToken`.
    ForceStopping,
    /// All tasks have terminated and the Coordinator has exited.
    Stopped,
}

impl TunnelPhase {
    /// Returns `true` when new assignments and task recycling are permitted.
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
    /// Delivers input payload and `job_id` to transition `Empty -> Connecting`.
    Assign { job_id: JobId, input: I },
    /// Requests bounded graceful drain/recycle; ignored when the task is already `Empty`.
    Drain,
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
    /// Target task entered a later state that can no longer reach the desired state.
    TargetClosed(ExternalState),
    /// Tunnel began shutting down before the dependency was satisfied.
    Cancelled,
    /// Waiting would introduce a cycle in the Coordinator dependency graph.
    CycleDetected,
    /// Target task ID is not registered in the Coordinator.
    TargetNotFound,
}
