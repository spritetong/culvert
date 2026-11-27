// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Internal stage abstractions mapped 1-to-1 to external task states.
//! All comments and docstrings must be in English.

pub mod active;
pub mod closing;
pub mod connecting;

pub use active::ActiveStage;
pub use closing::ClosingStage;
pub use connecting::ConnectingStage;

use crate::v2::coordinator::CoordinatorHandle;
use crate::v2::error::TaskError;
use crate::v2::state::{ExternalState, TaskId, WaitResult};

/// Narrow Coordinator-facing capability object passed into `ConnectingStage`.
///
/// Keeps internal stages decoupled from external lifecycle internals while allowing
/// inter-task state coordination through the single-writer Coordinator.
pub struct StageContext<'a> {
    task_id: TaskId,
    coord: &'a CoordinatorHandle,
}

impl<'a> StageContext<'a> {
    /// Creates a new `StageContext` bound to `task_id` and `coord`.
    #[inline]
    pub fn new(task_id: TaskId, coord: &'a CoordinatorHandle) -> Self {
        Self { task_id, coord }
    }

    /// Returns the ID of the running task.
    #[inline]
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Waits for `target` to reach `desired` external state via the Coordinator.
    pub async fn await_task_state(
        &self,
        target: TaskId,
        desired: ExternalState,
    ) -> Result<WaitResult, TaskError> {
        self.coord
            .await_task_state(self.task_id, target, desired)
            .await
    }
}
