// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! IO Task prototype: single outer cancellation boundary and single event loop.
//! All comments and docstrings must be in English.

use crate::v2::coordinator::Microtask;
use crate::v2::stage::{ActiveStage, ClosingStage, ConnectingContext, TaskWorkflow};
use crate::v2::state::{CloseReason, ExternalState, TaskDirective, TaskId, TaskOutcome};
use std::future::poll_fn;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Internal state payload owned by the task, matched 1-to-1 with `ExternalState`.
enum TaskSlot<W: TaskWorkflow> {
    Empty,
    Connecting(W::Input),
    Active(W::Active),
    Closing(<W::Active as ActiveStage>::Closing),
}

impl<W: TaskWorkflow> TaskSlot<W> {
    fn take_input(&mut self) -> Option<W::Input> {
        match std::mem::replace(self, Self::Empty) {
            Self::Connecting(input) => Some(input),
            other => {
                *self = other;
                None
            }
        }
    }

    fn take_active(&mut self) -> Option<W::Active> {
        match std::mem::replace(self, Self::Empty) {
            Self::Active(active) => Some(active),
            other => {
                *self = other;
                None
            }
        }
    }
}

/// Self-contained recyclable IO Task running a single event loop.
///
/// `CancellationToken` acts as the sole `ForceStop` boundary around `run_loop`,
/// eliminating scattered force-stop checks across internal state branches.
pub struct IoTask<W: TaskWorkflow> {
    id: TaskId,
    state: ExternalState,
    workflow: Arc<W>,
    micro_tx: mpsc::UnboundedSender<Microtask>,
    directive_rx: mpsc::UnboundedReceiver<TaskDirective<W::Input>>,
    cancel: CancellationToken,
    slot: TaskSlot<W>,
    shutdown_after_close: bool,
}

impl<W: TaskWorkflow> IoTask<W> {
    /// Constructs a new `IoTask` in either `Empty` or `Connecting` initial state.
    pub fn new(
        id: TaskId,
        initial_state: ExternalState,
        workflow: Arc<W>,
        micro_tx: mpsc::UnboundedSender<Microtask>,
        directive_rx: mpsc::UnboundedReceiver<TaskDirective<W::Input>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            id,
            state: initial_state,
            workflow,
            micro_tx,
            directive_rx,
            cancel,
            slot: TaskSlot::Empty,
            shutdown_after_close: false,
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
        let mut last_outcome = TaskOutcome::Shutdown;

        loop {
            match self.state {
                ExternalState::Empty => match self.directive_rx.recv().await {
                    Some(TaskDirective::Assign(input)) => {
                        self.shutdown_after_close = false;
                        last_outcome = TaskOutcome::Completed;
                        self.slot = TaskSlot::Connecting(input);
                        self.state = ExternalState::Connecting;
                    }
                    // Ignore late Drain directives when already idle in Empty.
                    Some(TaskDirective::Drain) => {}
                    Some(TaskDirective::Shutdown) | None => {
                        self.state = ExternalState::Closed;
                        last_outcome = TaskOutcome::Shutdown;
                    }
                },
                ExternalState::Connecting => {
                    let Some(input) = self.slot.take_input() else {
                        match self.directive_rx.recv().await {
                            Some(TaskDirective::Assign(input)) => {
                                self.slot = TaskSlot::Connecting(input);
                            }
                            Some(TaskDirective::Drain) => {
                                last_outcome = TaskOutcome::GracefullyDrained;
                                self.recycle_or_exit(&last_outcome);
                            }
                            Some(TaskDirective::Shutdown) | None => {
                                self.shutdown_after_close = true;
                                last_outcome = TaskOutcome::Shutdown;
                                self.recycle_or_exit(&last_outcome);
                            }
                        }
                        continue;
                    };

                    let mut ctx = ConnectingContext::new(self.id, &self.micro_tx);
                    enum ConnectBreak<A, I> {
                        Done(Result<A, crate::v2::error::TaskError>),
                        Directive(Option<TaskDirective<I>>),
                    }

                    let res = tokio::select! {
                        biased;
                        dir = self.directive_rx.recv() => ConnectBreak::Directive(dir),
                        res = self.workflow.connect(&mut ctx, input) => ConnectBreak::Done(res),
                    };

                    match res {
                        ConnectBreak::Directive(Some(TaskDirective::Shutdown) | None) => {
                            self.shutdown_after_close = true;
                            last_outcome = TaskOutcome::Shutdown;
                            self.recycle_or_exit(&last_outcome);
                        }
                        ConnectBreak::Directive(Some(TaskDirective::Drain)) => {
                            last_outcome = TaskOutcome::GracefullyDrained;
                            self.recycle_or_exit(&last_outcome);
                        }
                        ConnectBreak::Directive(Some(TaskDirective::Assign(next_input))) => {
                            self.slot = TaskSlot::Connecting(next_input);
                        }
                        ConnectBreak::Done(Ok(active)) => {
                            self.report_state(ExternalState::Active);
                            self.slot = TaskSlot::Active(active);
                            self.state = ExternalState::Active;
                        }
                        ConnectBreak::Done(Err(err)) => {
                            last_outcome = TaskOutcome::Failed(err);
                            self.recycle_or_exit(&last_outcome);
                        }
                    }
                }
                ExternalState::Active => {
                    let Some(mut active) = self.slot.take_active() else {
                        self.state = ExternalState::Closed;
                        continue;
                    };

                    enum ActiveBreak<I> {
                        Finished(Result<(), crate::v2::error::TaskError>),
                        Directive(Option<TaskDirective<I>>),
                    }

                    let brk = tokio::select! {
                        biased;
                        dir = self.directive_rx.recv() => ActiveBreak::Directive(dir),
                        res = poll_fn(|cx| active.poll_drive(cx)) => ActiveBreak::Finished(res),
                    };

                    let reason = match brk {
                        ActiveBreak::Directive(Some(TaskDirective::Shutdown) | None) => {
                            self.shutdown_after_close = true;
                            last_outcome = TaskOutcome::GracefullyDrained;
                            CloseReason::GracefulDrain
                        }
                        ActiveBreak::Directive(Some(TaskDirective::Drain)) => {
                            last_outcome = TaskOutcome::GracefullyDrained;
                            CloseReason::GracefulDrain
                        }
                        ActiveBreak::Directive(Some(TaskDirective::Assign(_))) => {
                            self.slot = TaskSlot::Active(active);
                            continue;
                        }
                        ActiveBreak::Finished(Ok(())) => {
                            last_outcome = TaskOutcome::Completed;
                            CloseReason::NormalEnd
                        }
                        ActiveBreak::Finished(Err(err)) => {
                            last_outcome = TaskOutcome::Failed(err);
                            CloseReason::StageError
                        }
                    };

                    self.report_state(ExternalState::Closing);
                    self.slot = TaskSlot::Closing(active.into_closing(reason));
                    self.state = ExternalState::Closing;
                }
                ExternalState::Closing => {
                    if let TaskSlot::Closing(ref mut closing) = self.slot
                        && let Err(err) = closing.run().await
                        && !matches!(last_outcome, TaskOutcome::Failed(_))
                    {
                        last_outcome = TaskOutcome::Failed(err);
                    }
                    self.slot = TaskSlot::Empty;
                    self.recycle_or_exit(&last_outcome);
                }
                ExternalState::Closed => {
                    self.slot = TaskSlot::Empty;
                    break;
                }
            }
        }

        last_outcome
    }

    fn report_state(&self, state: ExternalState) {
        let _ = self
            .micro_tx
            .send(Microtask::StateChanged { id: self.id, state });
    }

    fn recycle_or_exit(&mut self, outcome: &TaskOutcome) {
        self.slot = TaskSlot::Empty;
        if self.shutdown_after_close {
            self.state = ExternalState::Closed;
            return;
        }
        // Reset local state to Empty BEFORE notifying Coordinator so FIFO ordering on
        // `micro_tx` guarantees no stale events from the previous run follow `RecycledToEmpty`.
        self.state = ExternalState::Empty;
        if self
            .micro_tx
            .send(Microtask::RecycledToEmpty {
                id: self.id,
                outcome: outcome.clone(),
            })
            .is_err()
        {
            self.state = ExternalState::Closed;
        }
    }
}
