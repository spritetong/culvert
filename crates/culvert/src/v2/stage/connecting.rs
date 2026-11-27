// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! `Connecting` stage support: Coordinator microtask access for inter-task waits.
//! All comments and docstrings must be in English.

use crate::v2::coordinator::Microtask;
use crate::v2::error::TaskError;
use crate::v2::state::{ExternalState, TaskId, WaitResult};
use tokio::sync::{mpsc, oneshot};

/// Narrow capability object passed into `TaskWorkflow::connect`.
///
/// Permits registering inter-task dependencies through the Coordinator's synchronous
/// high-priority microtask channel without exposing Coordinator internals.
pub struct ConnectingContext<'a> {
    task_id: TaskId,
    micro_tx: &'a mpsc::UnboundedSender<Microtask>,
}

impl<'a> ConnectingContext<'a> {
    /// Creates a new `ConnectingContext` bound to `task_id` and `micro_tx`.
    #[inline]
    pub fn new(task_id: TaskId, micro_tx: &'a mpsc::UnboundedSender<Microtask>) -> Self {
        Self { task_id, micro_tx }
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
        let (reply_tx, reply_rx) = oneshot::channel();
        self.micro_tx
            .send(Microtask::AwaitState {
                waiter: self.task_id,
                target,
                desired,
                reply: reply_tx,
            })
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }
}
