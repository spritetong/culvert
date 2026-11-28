// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Core session lifecycle and task state. Core never moves bytes itself.
//! All comments and docstrings must be in English.

pub mod coordinator;
pub mod engine;
pub mod error;
pub mod failover;
pub mod stage;
pub mod state;
pub mod stream;
pub mod task;

pub use coordinator::{
    CompletedJob, Coordinator, CoordinatorHandle, CoordinatorSummary, Microtask,
};
pub use engine::StateEngine;
pub use error::{TaskError, TunnelError};
pub use stage::{ActiveStage, ClosingStage, ConnectingContext, TaskWorkflow};
pub use state::{
    CloseReason, CoordinatorConfig, DialConfig, ExternalState, IdleId, IdlePolicy, JobId,
    PoolStats, PooledTransport, TaskDirective, TaskId, TaskOutcome, TunnelPhase, WaitResult,
};
pub use stream::{BoxFuture, TunnelStream};
pub use task::IoTask;
