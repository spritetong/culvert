// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! IO Task prototype: single outer cancellation boundary and single event loop.
//! All comments and docstrings must be in English.

use crate::v2::coordinator::Microtask;
use crate::v2::error::TaskError;
use crate::v2::stage::{ActiveStage, ClosingStage, ConnectingContext, TaskWorkflow};
use crate::v2::state::{CloseReason, ExternalState, JobId, TaskDirective, TaskId, TaskOutcome};
use std::future::poll_fn;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Single source of truth binding external lifecycle phase to its exact resource payload.
///
/// Illegal combinations such as `(state = Active, payload = Empty)` are unrepresentable.
enum TaskPhase<W: TaskWorkflow> {
    Empty,
    Connecting {
        job_id: JobId,
        input: W::Input,
    },
    Active {
        job_id: JobId,
        stage: W::Active,
    },
    Closing {
        job_id: JobId,
        stage: <W::Active as ActiveStage>::Closing,
        outcome: TaskOutcome,
        shutdown_after_close: bool,
    },
    Closed {
        outcome: TaskOutcome,
    },
}

impl<W: TaskWorkflow> TaskPhase<W> {
    #[inline]
    fn external_state(&self) -> ExternalState {
        match self {
            Self::Empty => ExternalState::Empty,
            Self::Connecting { .. } => ExternalState::Connecting,
            Self::Active { .. } => ExternalState::Active,
            Self::Closing { .. } => ExternalState::Closing,
            Self::Closed { .. } => ExternalState::Closed,
        }
    }
}

/// Self-contained recyclable IO Task running a single event loop.
///
/// `CancellationToken` acts as the sole `ForceStop` boundary around `run_loop`,
/// eliminating scattered force-stop checks across internal state branches.
pub struct IoTask<W: TaskWorkflow> {
    id: TaskId,
    workflow: Arc<W>,
    micro_tx: mpsc::UnboundedSender<Microtask>,
    directive_rx: mpsc::UnboundedReceiver<TaskDirective<W::Input>>,
    cancel: CancellationToken,
    initial_phase: Option<TaskPhase<W>>,
}

impl<W: TaskWorkflow> IoTask<W> {
    /// Constructs a new `IoTask` starting either in `Empty` (`None`) or directly in
    /// `Connecting` (`Some((job_id, input))`).
    pub fn new(
        id: TaskId,
        initial_job: Option<(JobId, W::Input)>,
        workflow: Arc<W>,
        micro_tx: mpsc::UnboundedSender<Microtask>,
        directive_rx: mpsc::UnboundedReceiver<TaskDirective<W::Input>>,
        cancel: CancellationToken,
    ) -> Self {
        let initial_phase = match initial_job {
            Some((job_id, input)) => TaskPhase::Connecting { job_id, input },
            None => TaskPhase::Empty,
        };
        Self {
            id,
            workflow,
            micro_tx,
            directive_rx,
            cancel,
            initial_phase: Some(initial_phase),
        }
    }

    /// Runs the task until `Shutdown` or `ForceStop` cancellation.
    pub async fn run(mut self) -> TaskOutcome {
        let cancel = self.cancel.clone();
        tokio::select! {
            biased;
            _ = cancel.cancelled() => TaskOutcome::ForceStopped,
            outcome = self.run_loop() => outcome,
        }
    }

    async fn run_loop(&mut self) -> TaskOutcome {
        let mut phase = self.initial_phase.take().unwrap_or(TaskPhase::Empty);

        loop {
            phase = match phase {
                TaskPhase::Empty => match self.directive_rx.recv().await {
                    Some(TaskDirective::Assign { job_id, input }) => {
                        TaskPhase::Connecting { job_id, input }
                    }
                    // Ignore late Drain directives when already idle in Empty.
                    Some(TaskDirective::Drain) => TaskPhase::Empty,
                    Some(TaskDirective::Shutdown) | None => TaskPhase::Closed {
                        outcome: TaskOutcome::Shutdown,
                    },
                },
                TaskPhase::Connecting { job_id, input } => {
                    let mut ctx = ConnectingContext::new(self.id, job_id, &self.micro_tx);
                    enum ConnectBreak<A, I> {
                        Done(Result<A, TaskError>),
                        Directive(Option<TaskDirective<I>>),
                    }

                    let res = tokio::select! {
                        biased;
                        dir = self.directive_rx.recv() => ConnectBreak::Directive(dir),
                        res = self.workflow.connect(&mut ctx, input) => ConnectBreak::Done(res),
                    };

                    match res {
                        ConnectBreak::Directive(Some(TaskDirective::Shutdown) | None) => {
                            self.recycle_or_exit(job_id, TaskOutcome::Shutdown, true)
                        }
                        ConnectBreak::Directive(Some(TaskDirective::Drain)) => {
                            self.recycle_or_exit(job_id, TaskOutcome::GracefullyDrained, false)
                        }
                        ConnectBreak::Directive(Some(TaskDirective::Assign {
                            job_id: next_job,
                            input: next_input,
                        })) => {
                            let _ =
                                self.recycle_or_exit(job_id, TaskOutcome::GracefullyDrained, false);
                            TaskPhase::Connecting {
                                job_id: next_job,
                                input: next_input,
                            }
                        }
                        ConnectBreak::Done(Ok(stage)) => {
                            let next = TaskPhase::Active { job_id, stage };
                            self.report_state(job_id, next.external_state());
                            next
                        }
                        ConnectBreak::Done(Err(err)) => {
                            self.recycle_or_exit(job_id, TaskOutcome::Failed(err), false)
                        }
                    }
                }
                TaskPhase::Active { job_id, mut stage } => {
                    enum ActiveBreak<I> {
                        Finished(Result<(), TaskError>),
                        Directive(Option<TaskDirective<I>>),
                    }

                    let brk = tokio::select! {
                        biased;
                        dir = self.directive_rx.recv() => ActiveBreak::Directive(dir),
                        res = poll_fn(|cx| stage.poll_drive(cx)) => ActiveBreak::Finished(res),
                    };

                    let (reason, outcome, shutdown_after_close) = match brk {
                        ActiveBreak::Directive(Some(TaskDirective::Shutdown) | None) => (
                            CloseReason::GracefulDrain,
                            TaskOutcome::GracefullyDrained,
                            true,
                        ),
                        ActiveBreak::Directive(Some(TaskDirective::Drain)) => (
                            CloseReason::GracefulDrain,
                            TaskOutcome::GracefullyDrained,
                            false,
                        ),
                        ActiveBreak::Directive(Some(TaskDirective::Assign { .. })) => {
                            phase = TaskPhase::Active { job_id, stage };
                            continue;
                        }
                        ActiveBreak::Finished(Ok(())) => {
                            (CloseReason::NormalEnd, TaskOutcome::Completed, false)
                        }
                        ActiveBreak::Finished(Err(err)) => {
                            (CloseReason::StageError, TaskOutcome::Failed(err), false)
                        }
                    };

                    let next = TaskPhase::Closing {
                        job_id,
                        stage: stage.into_closing(reason),
                        outcome,
                        shutdown_after_close,
                    };
                    self.report_state(job_id, next.external_state());
                    next
                }
                TaskPhase::Closing {
                    job_id,
                    mut stage,
                    mut outcome,
                    shutdown_after_close,
                } => {
                    if let Err(err) = stage.run().await
                        && !matches!(outcome, TaskOutcome::Failed(_))
                    {
                        outcome = TaskOutcome::Failed(err);
                    }
                    drop(stage);
                    self.recycle_or_exit(job_id, outcome, shutdown_after_close)
                }
                TaskPhase::Closed { outcome } => break outcome,
            };
        }
    }

    fn report_state(&self, job_id: JobId, state: ExternalState) {
        let _ = self.micro_tx.send(Microtask::StateChanged {
            id: self.id,
            job_id,
            state,
        });
    }

    fn recycle_or_exit(
        &self,
        job_id: JobId,
        outcome: TaskOutcome,
        shutdown_after_close: bool,
    ) -> TaskPhase<W> {
        if shutdown_after_close {
            return TaskPhase::Closed { outcome };
        }
        // Send RecycledToEmpty AFTER resetting local payload & state (now returning Empty) so
        // no further job-scoped StateChanged messages for `job_id` can follow.
        if self
            .micro_tx
            .send(Microtask::RecycledToEmpty {
                id: self.id,
                job_id,
                outcome: outcome.clone(),
            })
            .is_err()
        {
            TaskPhase::Closed { outcome }
        } else {
            TaskPhase::Empty
        }
    }
}
