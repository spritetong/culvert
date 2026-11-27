// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Experimental v2 Coordinator + IO Task state-machine prototype.
//! Isolated from existing `core`, `tunnel`, `pool`, and `io/pump` implementations.
//! All comments and docstrings must be in English.

pub mod coordinator;
pub mod engine;
pub mod error;
pub mod pool;
pub mod stage;
pub mod state;
pub mod task;

#[cfg(test)]
mod tests;

pub use coordinator::{Coordinator, CoordinatorHandle, CoordinatorSummary, Microtask};
pub use engine::StateEngine;
pub use error::TaskError;
pub use pool::{
    LeaseGuard, PoolConfig, PoolDialConfig, PoolHandle, PoolInventory, PoolRole, PoolStats,
    PoolSummary, StreamLease, SweepOutcome, TransportPool,
};
pub use stage::{ActiveStage, ClosingStage, ConnectingContext, TaskWorkflow};
pub use state::{
    CloseReason, ExternalState, TaskDirective, TaskId, TaskOutcome, TunnelPhase, WaitResult,
};
pub use task::IoTask;
