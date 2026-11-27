// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Core state vocabulary for the v2 Coordinator and IO Task prototype.
//! All comments and docstrings must be in English.

use crate::v2::error::TaskError;
use std::fmt;

/// Monotonic unique task identifier assigned by the Coordinator.
///
/// Tasks are never pooled by slot index, so a monotonically increasing ID
/// eliminates stale-event contamination across task lifetimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(u64);

impl TaskId {
    /// Creates a `TaskId` from a raw monotonic sequence number.
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

/// External lifecycle states of an `IoTask` as tracked by the Coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExternalState {
    /// Task has been spawned and is acquiring or handshaking its resources.
    Connecting,
    /// Task exclusively owns its paired resources and runs the hot-path loop.
    Active,
    /// Task is draining buffered work or releasing resources before exiting.
    Closing,
    /// Terminal state after the task event loop exits.
    Closed,
}

impl ExternalState {
    /// Returns `true` if `self` can monotonically progress to `target` without rewinding.
    #[inline]
    pub const fn can_reach(self, target: Self) -> bool {
        self.rank() <= target.rank()
    }

    /// Returns `true` if this state is terminal.
    #[inline]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Closed)
    }

    #[inline]
    const fn rank(self) -> u8 {
        match self {
            Self::Connecting => 0,
            Self::Active => 1,
            Self::Closing => 2,
            Self::Closed => 3,
        }
    }
}

impl fmt::Display for ExternalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
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
    /// Accepting new tasks and allowing transitions into `Active`.
    Running,
    /// Rejecting new tasks and draining active tasks within a bounded window.
    Draining,
    /// Immediately terminating all tasks without waiting for drain completion.
    ForceStopping,
    /// All tasks have terminated and the Coordinator has finished.
    Stopped,
}

impl TunnelPhase {
    /// Returns `true` when new tasks and transitions into `Active` are permitted.
    #[inline]
    pub const fn accepts_active_work(self) -> bool {
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

/// Control directive broadcast from Coordinator to a Task via a non-blocking `watch` channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskDirective {
    /// Normal operation.
    Run,
    /// Stop accepting new work; enter bounded graceful drain if `Active`, or exit if `Connecting`.
    EnterDrain,
    /// Terminate immediately without awaiting drain or internal timeouts.
    ForceStop,
}

/// Reason an `ActiveStage` is transitioning into `ClosingStage`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// Normal EOF or completion returned by the active hot-path stage.
    NormalEnd,
    /// Active stage encountered a runtime error.
    StageError,
    /// Coordinator requested a graceful drain.
    GracefulDrain,
}

/// Outcome returned when a task exits its single event loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome {
    /// Completed normally through `Active` -> `Closing` -> `Closed`.
    Completed,
    /// Completed graceful drain after Coordinator entered `Draining`.
    GracefullyDrained,
    /// Terminated immediately on `ForceStop` (or cancelled during `Connecting`).
    ForceStopped,
    /// Transition into `Active` was rejected by Coordinator (e.g. shutting down).
    Rejected(TunnelPhase),
    /// Task aborted due to a stage or coordination failure.
    Failed(TaskError),
}

/// Result of waiting on another task's external state via the Coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitResult {
    /// Target task reached the desired external state.
    Reached,
    /// Target task closed or entered a later state that can no longer reach the desired state.
    TargetClosed(ExternalState),
    /// Tunnel began shutting down before the dependency was satisfied.
    Cancelled,
    /// Waiting would introduce a cycle in the Coordinator dependency graph.
    CycleDetected,
    /// Target task ID is not registered in the Coordinator.
    TargetNotFound,
}
