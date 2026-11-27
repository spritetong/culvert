// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! IO Task prototype: single event loop dispatching external-state stage modules.
//! All comments and docstrings must be in English.

use crate::v2::coordinator::CoordinatorHandle;
use crate::v2::error::TaskError;
use crate::v2::stage::{ActiveStage, ClosingStage, ConnectingStage, StageContext};
use crate::v2::state::{CloseReason, ExternalState, TaskDirective, TaskId, TaskOutcome};
use std::future::poll_fn;
use tokio::sync::watch;

/// Non-blocking directive receiver wrapping `watch::Receiver<TaskDirective>`.
///
/// Tracks `observed` locally so `wait_change` never misses a directive update that
/// occurred while the task was executing another await point.
pub struct TaskControl {
    rx: watch::Receiver<TaskDirective>,
    observed: TaskDirective,
}

impl TaskControl {
    /// Wraps the Coordinator-issued directive receiver.
    pub fn new(rx: watch::Receiver<TaskDirective>) -> Self {
        let observed = *rx.borrow();
        Self { rx, observed }
    }

    /// Reads the latest directive without blocking.
    #[inline]
    pub fn current(&self) -> TaskDirective {
        *self.rx.borrow()
    }

    /// Waits until the directive differs from the last observed value, or until the
    /// Coordinator closes the channel (mapped to `TaskDirective::ForceStop`).
    pub async fn wait_change(&mut self) -> TaskDirective {
        let current = *self.rx.borrow();
        if current != self.observed {
            self.observed = current;
            return current;
        }
        if self.rx.changed().await.is_err() {
            self.observed = TaskDirective::ForceStop;
            return TaskDirective::ForceStop;
        }
        let next = *self.rx.borrow_and_update();
        self.observed = next;
        next
    }
}

/// Internal stage container ensuring ownership transfers cleanly across external states.
enum StageSlot<C: ConnectingStage> {
    Connecting(C),
    Active(C::Active),
    Closing(<C::Active as ActiveStage>::Closing),
    Empty,
}

impl<C: ConnectingStage> StageSlot<C> {
    fn take_active(&mut self) -> Option<C::Active> {
        match std::mem::replace(self, Self::Empty) {
            Self::Active(stage) => Some(stage),
            other => {
                *self = other;
                None
            }
        }
    }
}

/// Self-contained IO Task running a single event loop across `Connecting`, `Active`, and `Closing`.
pub struct IoTask<C: ConnectingStage> {
    id: TaskId,
    state: ExternalState,
    coord: CoordinatorHandle,
    ctrl: TaskControl,
    slot: StageSlot<C>,
}

impl<C: ConnectingStage> IoTask<C> {
    /// Constructs a new `IoTask` initialized in `ExternalState::Connecting`.
    pub fn new(
        id: TaskId,
        coord: CoordinatorHandle,
        directive_rx: watch::Receiver<TaskDirective>,
        connecting: C,
    ) -> Self {
        Self {
            id,
            state: ExternalState::Connecting,
            coord,
            ctrl: TaskControl::new(directive_rx),
            slot: StageSlot::Connecting(connecting),
        }
    }

    /// Runs the task's single logical event loop until terminal completion or stop.
    pub async fn run(mut self) -> TaskOutcome {
        let mut exit_outcome = TaskOutcome::Completed;

        loop {
            match self.state {
                ExternalState::Connecting => {
                    if !matches!(self.ctrl.current(), TaskDirective::Run) {
                        self.state = ExternalState::Closed;
                        exit_outcome = TaskOutcome::ForceStopped;
                        continue;
                    }

                    let StageSlot::Connecting(ref mut stage) = self.slot else {
                        self.state = ExternalState::Closed;
                        exit_outcome = TaskOutcome::Failed(TaskError::StageFailed(
                            "missing connecting stage".into(),
                        ));
                        continue;
                    };

                    let mut ctx = StageContext::new(self.id, &self.coord);
                    let connect_res = tokio::select! {
                        biased;
                        directive = self.ctrl.wait_change() => {
                            let _ = directive;
                            self.state = ExternalState::Closed;
                            exit_outcome = TaskOutcome::ForceStopped;
                            continue;
                        }
                        res = stage.run(&mut ctx) => res,
                    };

                    match connect_res {
                        Ok(active_stage) => {
                            match self.request_state_change(ExternalState::Active).await {
                                Ok(committed) => {
                                    self.slot = StageSlot::Active(active_stage);
                                    self.state = committed;
                                }
                                Err(TaskError::RejectedByCoordinator(phase)) => {
                                    self.slot = StageSlot::Empty;
                                    self.state = ExternalState::Closed;
                                    exit_outcome = TaskOutcome::Rejected(phase);
                                }
                                Err(err) => {
                                    self.slot = StageSlot::Empty;
                                    self.state = ExternalState::Closed;
                                    exit_outcome = TaskOutcome::Failed(err);
                                }
                            }
                        }
                        Err(err) => {
                            self.slot = StageSlot::Empty;
                            self.state = ExternalState::Closed;
                            exit_outcome = TaskOutcome::Failed(err);
                        }
                    }
                }
                ExternalState::Active => {
                    match self.ctrl.current() {
                        TaskDirective::ForceStop => {
                            self.slot = StageSlot::Empty;
                            self.state = ExternalState::Closed;
                            exit_outcome = TaskOutcome::ForceStopped;
                            continue;
                        }
                        TaskDirective::EnterDrain => {
                            self.enter_closing(CloseReason::GracefulDrain, &mut exit_outcome)
                                .await;
                            continue;
                        }
                        TaskDirective::Run => {}
                    }

                    let StageSlot::Active(ref mut active_stage) = self.slot else {
                        self.state = ExternalState::Closed;
                        exit_outcome = TaskOutcome::Failed(TaskError::StageFailed(
                            "missing active stage".into(),
                        ));
                        continue;
                    };

                    enum ActiveBreak {
                        Finished(Result<(), TaskError>),
                        Directive(TaskDirective),
                    }

                    let outcome = tokio::select! {
                        biased;
                        directive = self.ctrl.wait_change() => ActiveBreak::Directive(directive),
                        res = poll_fn(|cx| active_stage.poll_drive(cx)) => ActiveBreak::Finished(res),
                    };

                    match outcome {
                        ActiveBreak::Directive(TaskDirective::ForceStop) => {
                            self.slot = StageSlot::Empty;
                            self.state = ExternalState::Closed;
                            exit_outcome = TaskOutcome::ForceStopped;
                        }
                        ActiveBreak::Directive(TaskDirective::EnterDrain) => {
                            self.enter_closing(CloseReason::GracefulDrain, &mut exit_outcome)
                                .await;
                        }
                        ActiveBreak::Directive(TaskDirective::Run) => {}
                        ActiveBreak::Finished(Ok(())) => {
                            self.enter_closing(CloseReason::NormalEnd, &mut exit_outcome)
                                .await;
                        }
                        ActiveBreak::Finished(Err(err)) => {
                            exit_outcome = TaskOutcome::Failed(err);
                            self.enter_closing(CloseReason::StageError, &mut exit_outcome)
                                .await;
                        }
                    }
                }
                ExternalState::Closing => {
                    if matches!(self.ctrl.current(), TaskDirective::ForceStop) {
                        self.slot = StageSlot::Empty;
                        self.state = ExternalState::Closed;
                        exit_outcome = TaskOutcome::ForceStopped;
                        continue;
                    }

                    let StageSlot::Closing(ref mut closing_stage) = self.slot else {
                        self.state = ExternalState::Closed;
                        continue;
                    };

                    tokio::select! {
                        biased;
                        directive = self.ctrl.wait_change() => {
                            if matches!(directive, TaskDirective::ForceStop) {
                                self.slot = StageSlot::Empty;
                                self.state = ExternalState::Closed;
                                exit_outcome = TaskOutcome::ForceStopped;
                            }
                        }
                        res = closing_stage.run() => {
                            self.slot = StageSlot::Empty;
                            self.state = ExternalState::Closed;
                            if let Err(err) = res
                                && !matches!(exit_outcome, TaskOutcome::Failed(_))
                            {
                                exit_outcome = TaskOutcome::Failed(err);
                            }
                        }
                    }
                }
                ExternalState::Closed => {
                    self.slot = StageSlot::Empty;
                    break;
                }
            }
        }

        exit_outcome
    }

    async fn enter_closing(&mut self, reason: CloseReason, exit_outcome: &mut TaskOutcome) {
        if matches!(reason, CloseReason::GracefulDrain)
            && !matches!(exit_outcome, TaskOutcome::Failed(_))
        {
            *exit_outcome = TaskOutcome::GracefullyDrained;
        }

        let Some(active) = self.slot.take_active() else {
            self.state = ExternalState::Closed;
            return;
        };

        match self.request_state_change(ExternalState::Closing).await {
            Ok(committed) => {
                self.slot = StageSlot::Closing(active.into_closing(reason));
                self.state = committed;
            }
            Err(TaskError::RejectedByCoordinator(_)) => {
                // ForceStop raced with transition; drop stage immediately.
                self.slot = StageSlot::Empty;
                self.state = ExternalState::Closed;
                *exit_outcome = TaskOutcome::ForceStopped;
            }
            Err(err) => {
                self.slot = StageSlot::Empty;
                self.state = ExternalState::Closed;
                if !matches!(exit_outcome, TaskOutcome::Failed(_)) {
                    *exit_outcome = TaskOutcome::Failed(err);
                }
            }
        }
    }

    async fn request_state_change(
        &mut self,
        next: ExternalState,
    ) -> Result<ExternalState, TaskError> {
        let from = self.state;
        let fut = self.coord.transition_task(self.id, from, next);
        tokio::pin!(fut);

        loop {
            tokio::select! {
                biased;
                directive = self.ctrl.wait_change() => {
                    match directive {
                        TaskDirective::ForceStop => {
                            return Err(TaskError::RejectedByCoordinator(
                                crate::v2::state::TunnelPhase::ForceStopping,
                            ));
                        }
                        TaskDirective::EnterDrain if next == ExternalState::Active => {
                            return Err(TaskError::RejectedByCoordinator(
                                crate::v2::state::TunnelPhase::Draining,
                            ));
                        }
                        TaskDirective::EnterDrain | TaskDirective::Run => {}
                    }
                }
                res = &mut fut => return res,
            }
        }
    }
}
