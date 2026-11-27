// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! `Connecting` stage support: pre-attached idle transport handoff, synchronous
//! Coordinator transport acquisition, dead-session sweeping, and inter-task waits.
//! All comments and docstrings must be in English.

use crate::v2::coordinator::Microtask;
use crate::v2::error::TaskError;
use crate::v2::state::{ExternalState, JobId, PooledTransport, TaskId, WaitResult};
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Narrow capability object passed into `TaskWorkflow::connect`.
///
/// Permits consuming any `Idle`-carried transport stream, requesting a fresh pooled
/// transport from the Coordinator, reporting dead multiplexed sessions for batch sweep,
/// and registering inter-task state dependencies without exposing Coordinator internals.
pub struct ConnectingContext<'a> {
    task_id: TaskId,
    job_id: JobId,
    initial_transport: Mutex<Option<PooledTransport>>,
    micro_tx: &'a mpsc::UnboundedSender<Microtask>,
}

impl<'a> ConnectingContext<'a> {
    /// Creates a new `ConnectingContext` bound to `task_id`, `job_id`, optional idle
    /// `initial_transport`, and `micro_tx`.
    #[inline]
    pub fn new(
        task_id: TaskId,
        job_id: JobId,
        initial_transport: Option<PooledTransport>,
        micro_tx: &'a mpsc::UnboundedSender<Microtask>,
    ) -> Self {
        Self {
            task_id,
            job_id,
            initial_transport: Mutex::new(initial_transport),
            micro_tx,
        }
    }

    /// Returns the ID of the worker task executing this job.
    #[inline]
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns the ID of the current job cycle on this worker task.
    #[inline]
    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    /// Takes the pre-warmed/inbound transport carried over when this task transitioned
    /// directly from `Idle -> Connecting`.
    #[inline]
    pub fn take_transport(&mut self) -> Option<PooledTransport> {
        self.initial_transport.get_mut().ok().and_then(Option::take)
    }

    /// Requests a fresh transport from the Coordinator's dial/inbound pipeline up to `timeout`.
    ///
    /// Dropping the returned future before receipt drops `oneshot::Receiver`, allowing the
    /// Coordinator to immediately recover any in-flight `PooledTransport` without leaks.
    pub fn request_transport(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<PooledTransport, TaskError>> + Send + 'static {
        let (reply_tx, reply_rx) = oneshot::channel();
        let sent = self.micro_tx.send(Microtask::RequestTransport {
            waiter: self.task_id,
            job_id: self.job_id,
            reply: reply_tx,
        });
        async move {
            if sent.is_err() {
                return Err(TaskError::CoordinatorGone);
            }
            match tokio::time::timeout(timeout, reply_rx).await {
                Ok(Ok(res)) => res,
                Ok(Err(_)) => Err(TaskError::CoordinatorGone),
                Err(_) => Err(TaskError::StageFailed(
                    "timed out waiting for pooled transport".into(),
                )),
            }
        }
    }

    /// Reports a failed multiplexed underlying session (e.g. QUIC connection loss) so the
    /// Coordinator immediately sweeps sibling `Idle` tasks sharing `session_id`.
    #[inline]
    pub fn report_dead_session(&self, session_id: u64) {
        let _ = self.micro_tx.send(Microtask::SweepSession { session_id });
    }

    /// Waits for `target` to reach `desired` external state via the Coordinator.
    pub fn await_task_state(
        &self,
        target: TaskId,
        desired: ExternalState,
    ) -> impl Future<Output = Result<WaitResult, TaskError>> + Send + 'static {
        let (reply_tx, reply_rx) = oneshot::channel();
        let sent = self.micro_tx.send(Microtask::AwaitState {
            waiter: self.task_id,
            waiter_job: self.job_id,
            target,
            desired,
            reply: reply_tx,
        });
        async move {
            if sent.is_err() {
                return Err(TaskError::CoordinatorGone);
            }
            reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
        }
    }
}
