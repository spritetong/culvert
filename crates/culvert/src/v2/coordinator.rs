// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Single-writer Coordinator Actor managing External State and task lifetimes.
//! All comments and docstrings must be in English.

use crate::v2::engine::StateEngine;
use crate::v2::error::TaskError;
use crate::v2::stage::ConnectingStage;
use crate::v2::state::{
    ExternalState, TaskDirective, TaskId, TaskOutcome, TunnelPhase, WaitResult,
};
use crate::v2::task::IoTask;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Sleep;

const COORDINATOR_QUEUE_CAPACITY: usize = 1024;

type TaskFuture = Pin<Box<dyn Future<Output = TaskOutcome> + Send>>;
type TaskSpawnFn =
    Box<dyn FnOnce(TaskId, CoordinatorHandle, watch::Receiver<TaskDirective>) -> TaskFuture + Send>;

/// Final report produced when an `IoTask` exits its `JoinSet` handle.
struct TaskReport {
    id: TaskId,
    outcome: TaskOutcome,
}

/// Command submitted to the Coordinator Actor.
enum CoordinatorMsg {
    SpawnTask {
        spawn_fn: TaskSpawnFn,
        reply: oneshot::Sender<Result<TaskId, TaskError>>,
    },
    Transition {
        task_id: TaskId,
        from: ExternalState,
        to: ExternalState,
        reply: oneshot::Sender<Result<ExternalState, TaskError>>,
    },
    AwaitState {
        waiter: TaskId,
        target: TaskId,
        desired: ExternalState,
        reply: oneshot::Sender<WaitResult>,
    },
    QueryTaskState {
        task_id: TaskId,
        reply: oneshot::Sender<Option<ExternalState>>,
    },
    QueryPhase {
        reply: oneshot::Sender<TunnelPhase>,
    },
    GracefulStop {
        reply: oneshot::Sender<TunnelPhase>,
    },
    ForceStop {
        reply: oneshot::Sender<TunnelPhase>,
    },
}

/// Cloneable handle used by callers and `IoTask` instances to synchronize with the Coordinator.
#[derive(Clone)]
pub struct CoordinatorHandle {
    tx: mpsc::Sender<CoordinatorMsg>,
}

impl CoordinatorHandle {
    /// Spawns a new `IoTask` under the Coordinator's supervision and returns its unique `TaskId`.
    pub async fn spawn_task<C: ConnectingStage>(&self, connecting: C) -> Result<TaskId, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let spawn_fn = Box::new(
            move |id,
                  handle,
                  directive_rx|
                  -> Pin<Box<dyn std::future::Future<Output = TaskOutcome> + Send>> {
                let task = IoTask::new(id, handle, directive_rx, connecting);
                Box::pin(task.run())
            },
        );
        self.tx
            .send(CoordinatorMsg::SpawnTask {
                spawn_fn,
                reply: reply_tx,
            })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)?
    }

    /// Requests the Coordinator to commit an external state transition for `task_id`.
    pub async fn transition_task(
        &self,
        task_id: TaskId,
        from: ExternalState,
        to: ExternalState,
    ) -> Result<ExternalState, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(CoordinatorMsg::Transition {
                task_id,
                from,
                to,
                reply: reply_tx,
            })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)?
    }

    /// Waits via the Coordinator until `target` reaches `desired` external state.
    pub async fn await_task_state(
        &self,
        waiter: TaskId,
        target: TaskId,
        desired: ExternalState,
    ) -> Result<WaitResult, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(CoordinatorMsg::AwaitState {
                waiter,
                target,
                desired,
                reply: reply_tx,
            })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }

    /// Queries the current external state of `task_id`.
    pub async fn query_task_state(
        &self,
        task_id: TaskId,
    ) -> Result<Option<ExternalState>, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(CoordinatorMsg::QueryTaskState {
                task_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }

    /// Queries the current tunnel phase.
    pub async fn query_phase(&self) -> Result<TunnelPhase, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(CoordinatorMsg::QueryPhase { reply: reply_tx })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }

    /// Initiates a bounded graceful stop across all supervised tasks.
    pub async fn graceful_stop(&self) -> Result<TunnelPhase, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(CoordinatorMsg::GracefulStop { reply: reply_tx })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }

    /// Initiates an immediate force stop across all supervised tasks.
    pub async fn force_stop(&self) -> Result<TunnelPhase, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(CoordinatorMsg::ForceStop { reply: reply_tx })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }
}

/// Summary produced when the Coordinator Actor exits cleanly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorSummary {
    pub final_phase: TunnelPhase,
    pub completed_tasks: Vec<(TaskId, TaskOutcome)>,
}

/// Single-writer Coordinator Actor supervising all `IoTask` lifecycles.
pub struct Coordinator {
    engine: StateEngine,
    join_set: JoinSet<TaskReport>,
    drain_timeout: Duration,
    drain_sleep: Option<Pin<Box<Sleep>>>,
    self_handle: CoordinatorHandle,
    cmd_rx: mpsc::Receiver<CoordinatorMsg>,
    completed_tasks: Vec<(TaskId, TaskOutcome)>,
}

impl Coordinator {
    /// Starts a new Coordinator Actor on the current Tokio runtime, returning its
    /// `CoordinatorHandle` and supervised `JoinHandle`.
    pub fn start(drain_timeout: Duration) -> (CoordinatorHandle, JoinHandle<CoordinatorSummary>) {
        let (tx, cmd_rx) = mpsc::channel(COORDINATOR_QUEUE_CAPACITY);
        let handle = CoordinatorHandle { tx };
        let actor = Self {
            engine: StateEngine::new(),
            join_set: JoinSet::new(),
            drain_timeout,
            drain_sleep: None,
            self_handle: handle.clone(),
            cmd_rx,
            completed_tasks: Vec::new(),
        };
        let join_handle = tokio::spawn(actor.run());
        (handle, join_handle)
    }

    async fn run(mut self) -> CoordinatorSummary {
        loop {
            if matches!(self.engine.phase(), TunnelPhase::Stopped) && self.join_set.is_empty() {
                break;
            }

            tokio::select! {
                biased;
                Some(join_res) = self.join_set.join_next(), if !self.join_set.is_empty() => {
                    self.handle_join_result(join_res);
                }
                _ = Self::poll_drain_sleep(&mut self.drain_sleep), if self.drain_sleep.is_some() => {
                    self.drain_sleep = None;
                    self.engine.begin_force_stop();
                }
                cmd_opt = self.cmd_rx.recv() => {
                    match cmd_opt {
                        Some(msg) => self.handle_message(msg),
                        None => {
                            // All external handles dropped; escalate stop to exit cleanly.
                            self.engine.begin_force_stop();
                            if self.join_set.is_empty() {
                                self.engine.finish_stop();
                            }
                        }
                    }
                }
            }
        }

        self.engine.finish_stop();
        CoordinatorSummary {
            final_phase: self.engine.phase(),
            completed_tasks: self.completed_tasks,
        }
    }

    async fn poll_drain_sleep(sleep_opt: &mut Option<Pin<Box<Sleep>>>) {
        if let Some(sleep) = sleep_opt.as_mut() {
            sleep.as_mut().await;
        }
    }

    fn handle_join_result(&mut self, join_res: Result<TaskReport, tokio::task::JoinError>) {
        match join_res {
            Ok(report) => {
                self.engine.complete_task(report.id);
                self.completed_tasks.push((report.id, report.outcome));
            }
            Err(join_err) => {
                tracing::error!(error = ?join_err, "Supervised IO task panicked or aborted unexpectedly");
                if self.join_set.is_empty()
                    && matches!(
                        self.engine.phase(),
                        TunnelPhase::Draining | TunnelPhase::ForceStopping
                    )
                {
                    self.engine.finish_stop();
                }
            }
        }
        if self.join_set.is_empty() {
            self.drain_sleep = None;
        }
    }

    fn handle_message(&mut self, msg: CoordinatorMsg) {
        match msg {
            CoordinatorMsg::SpawnTask { spawn_fn, reply } => {
                let res = self.engine.register_task().map(|(id, directive_rx)| {
                    let fut = spawn_fn(id, self.self_handle.clone(), directive_rx);
                    self.join_set.spawn(async move {
                        let outcome = fut.await;
                        TaskReport { id, outcome }
                    });
                    id
                });
                let _ = reply.send(res);
            }
            CoordinatorMsg::Transition {
                task_id,
                from,
                to,
                reply,
            } => {
                let res = self.engine.transition_task(task_id, from, to);
                let _ = reply.send(res);
            }
            CoordinatorMsg::AwaitState {
                waiter,
                target,
                desired,
                reply,
            } => {
                self.engine.register_waiter(waiter, target, desired, reply);
            }
            CoordinatorMsg::QueryTaskState { task_id, reply } => {
                let _ = reply.send(self.engine.task_state(task_id));
            }
            CoordinatorMsg::QueryPhase { reply } => {
                let _ = reply.send(self.engine.phase());
            }
            CoordinatorMsg::GracefulStop { reply } => {
                let phase = self.engine.begin_graceful_stop();
                if matches!(phase, TunnelPhase::Draining) && self.drain_sleep.is_none() {
                    self.drain_sleep = Some(Box::pin(tokio::time::sleep(self.drain_timeout)));
                }
                let _ = reply.send(phase);
            }
            CoordinatorMsg::ForceStop { reply } => {
                self.drain_sleep = None;
                let phase = self.engine.begin_force_stop();
                let _ = reply.send(phase);
            }
        }
    }
}
