// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Two-channel (`Microtask` + `Event`) Coordinator Actor with `CancellationToken` force-stop.
//! All comments and docstrings must be in English.

use crate::v2::engine::StateEngine;
use crate::v2::error::TaskError;
use crate::v2::stage::TaskWorkflow;
use crate::v2::state::{
    ExternalState, TaskDirective, TaskId, TaskOutcome, TunnelPhase, WaitResult,
};
use crate::v2::task::IoTask;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Sleep;
use tokio_util::sync::CancellationToken;

const COORDINATOR_EVENT_QUEUE_CAPACITY: usize = 1024;

/// Final report returned when an `IoTask` exits its `JoinSet` handle.
struct TaskReport {
    id: TaskId,
    outcome: TaskOutcome,
}

/// High-priority synchronous state synchronization messages sent from Tasks to Coordinator.
///
/// Contract: the Coordinator handler for `Microtask` MUST remain 100% synchronous (never `.await`),
/// mutating only `StateEngine` and replying via non-blocking `oneshot`/`unbounded` channels.
pub enum Microtask {
    StateChanged {
        id: TaskId,
        state: ExternalState,
    },
    RecycledToEmpty {
        id: TaskId,
        outcome: TaskOutcome,
    },
    AwaitState {
        waiter: TaskId,
        target: TaskId,
        desired: ExternalState,
        reply: oneshot::Sender<WaitResult>,
    },
}

/// Normal-priority external events handled by the Coordinator.
enum CoordinatorEvent<I> {
    Prewarm {
        count: usize,
        reply: oneshot::Sender<Result<Vec<TaskId>, TaskError>>,
    },
    AssignWork {
        input: I,
        reply: oneshot::Sender<Result<TaskId, TaskError>>,
    },
    RecycleTask {
        task_id: TaskId,
        reply: oneshot::Sender<bool>,
    },
    QueryTaskState {
        task_id: TaskId,
        reply: oneshot::Sender<Option<ExternalState>>,
    },
    QueryPhase {
        reply: oneshot::Sender<TunnelPhase>,
    },
    QueryIdleCount {
        reply: oneshot::Sender<usize>,
    },
    GracefulStop {
        reply: oneshot::Sender<TunnelPhase>,
    },
}

struct CoordinatorHandleInner<W: TaskWorkflow> {
    event_tx: mpsc::Sender<CoordinatorEvent<W::Input>>,
    cancel_token: CancellationToken,
}

impl<W: TaskWorkflow> Drop for CoordinatorHandleInner<W> {
    fn drop(&mut self) {
        // Dropping the last external handle cancels the Coordinator and all child tasks cleanly.
        self.cancel_token.cancel();
    }
}

/// Cloneable external handle to the Coordinator.
///
/// Dropping all clones triggers `CancellationToken::cancel()`, unifying handle drop
/// and explicit `force_stop()` into a single termination path.
pub struct CoordinatorHandle<W: TaskWorkflow> {
    inner: Arc<CoordinatorHandleInner<W>>,
}

impl<W: TaskWorkflow> Clone for CoordinatorHandle<W> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<W: TaskWorkflow> CoordinatorHandle<W> {
    /// Pre-spawns `count` idle `Empty` tasks ready for fast assignment.
    pub async fn prewarm(&self, count: usize) -> Result<Vec<TaskId>, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(CoordinatorEvent::Prewarm {
                count,
                reply: reply_tx,
            })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)?
    }

    /// Assigns `input` to an idle `Empty` task or spawns a new task if none are idle.
    pub async fn assign(&self, input: W::Input) -> Result<TaskId, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(CoordinatorEvent::AssignWork {
                input,
                reply: reply_tx,
            })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)?
    }

    /// Asynchronously asks a running task to drain and recycle back to `Empty`.
    pub async fn request_recycle(&self, task_id: TaskId) -> Result<bool, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(CoordinatorEvent::RecycleTask {
                task_id,
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
        self.inner
            .event_tx
            .send(CoordinatorEvent::QueryTaskState {
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
        self.inner
            .event_tx
            .send(CoordinatorEvent::QueryPhase { reply: reply_tx })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }

    /// Queries the number of idle `Empty` tasks currently cached for reuse.
    pub async fn query_idle_count(&self) -> Result<usize, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(CoordinatorEvent::QueryIdleCount { reply: reply_tx })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }

    /// Initiates a bounded graceful stop across all supervised tasks.
    pub async fn graceful_stop(&self) -> Result<TunnelPhase, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(CoordinatorEvent::GracefulStop { reply: reply_tx })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }

    /// Immediately forces all tasks and the Coordinator to stop via `CancellationToken`.
    #[inline]
    pub fn force_stop(&self) {
        self.inner.cancel_token.cancel();
    }

    /// Returns a clone of the Coordinator's root cancellation token.
    #[inline]
    pub fn cancel_token(&self) -> CancellationToken {
        self.inner.cancel_token.clone()
    }
}

/// Summary produced when the Coordinator Actor exits cleanly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorSummary {
    pub final_phase: TunnelPhase,
    pub completed_jobs: Vec<(TaskId, TaskOutcome)>,
}

/// Single-writer Coordinator Actor supervising all `IoTask` lifecycles.
pub struct Coordinator<W: TaskWorkflow> {
    workflow: Arc<W>,
    engine: StateEngine<W::Input>,
    join_set: JoinSet<TaskReport>,
    drain_timeout: Duration,
    drain_sleep: Option<Pin<Box<Sleep>>>,
    cancel_token: CancellationToken,
    micro_tx: mpsc::UnboundedSender<Microtask>,
    micro_rx: mpsc::UnboundedReceiver<Microtask>,
    _keep_event_tx: mpsc::Sender<CoordinatorEvent<W::Input>>,
    event_rx: mpsc::Receiver<CoordinatorEvent<W::Input>>,
    completed_jobs: Vec<(TaskId, TaskOutcome)>,
}

impl<W: TaskWorkflow> Coordinator<W> {
    /// Starts a new Coordinator Actor wired to `cancel_token`.
    pub fn start(
        workflow: W,
        drain_timeout: Duration,
        cancel_token: CancellationToken,
    ) -> (CoordinatorHandle<W>, JoinHandle<CoordinatorSummary>) {
        let (event_tx, event_rx) = mpsc::channel(COORDINATOR_EVENT_QUEUE_CAPACITY);
        let (micro_tx, micro_rx) = mpsc::unbounded_channel();

        let handle = CoordinatorHandle {
            inner: Arc::new(CoordinatorHandleInner {
                event_tx: event_tx.clone(),
                cancel_token: cancel_token.clone(),
            }),
        };

        let actor = Self {
            workflow: Arc::new(workflow),
            engine: StateEngine::new(),
            join_set: JoinSet::new(),
            drain_timeout,
            drain_sleep: None,
            cancel_token,
            micro_tx,
            micro_rx,
            _keep_event_tx: event_tx,
            event_rx,
            completed_jobs: Vec::new(),
        };

        let join_handle = tokio::spawn(actor.run());
        (handle, join_handle)
    }

    async fn run(mut self) -> CoordinatorSummary {
        loop {
            self.drain_microtasks();

            if matches!(self.engine.phase(), TunnelPhase::Stopped) && self.join_set.is_empty() {
                break;
            }

            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled(), if !matches!(self.engine.phase(), TunnelPhase::ForceStopping | TunnelPhase::Stopped) => {
                    self.drain_sleep = None;
                    self.engine.begin_force_stop();
                }
                micro_opt = self.micro_rx.recv() => {
                    let Some(micro) = micro_opt else {
                        unreachable!("Coordinator retains micro_tx so micro_rx never closes");
                    };
                    self.handle_microtask(micro);
                    self.drain_microtasks();
                }
                Some(join_res) = self.join_set.join_next(), if !self.join_set.is_empty() => {
                    self.handle_join_result(join_res);
                }
                _ = Self::poll_drain_sleep(&mut self.drain_sleep), if self.drain_sleep.is_some() => {
                    self.drain_sleep = None;
                    self.engine.begin_force_stop();
                    self.cancel_token.cancel();
                }
                evt_opt = self.event_rx.recv() => {
                    let Some(evt) = evt_opt else {
                        unreachable!("Coordinator retains event_tx so event_rx never closes");
                    };
                    self.handle_event(evt);
                }
            }
        }

        self.engine.finish_stop();
        CoordinatorSummary {
            final_phase: self.engine.phase(),
            completed_jobs: self.completed_jobs,
        }
    }

    async fn poll_drain_sleep(sleep_opt: &mut Option<Pin<Box<Sleep>>>) {
        if let Some(sleep) = sleep_opt.as_mut() {
            sleep.as_mut().await;
        }
    }

    fn drain_microtasks(&mut self) {
        while let Ok(micro) = self.micro_rx.try_recv() {
            self.handle_microtask(micro);
        }
    }

    fn handle_microtask(&mut self, micro: Microtask) {
        match micro {
            Microtask::StateChanged { id, state } => {
                self.engine.apply_task_state(id, state);
            }
            Microtask::RecycledToEmpty { id, outcome } => {
                self.completed_jobs.push((id, outcome));
                self.engine.on_task_recycled(id);
            }
            Microtask::AwaitState {
                waiter,
                target,
                desired,
                reply,
            } => {
                self.engine.register_waiter(waiter, target, desired, reply);
            }
        }
    }

    fn handle_join_result(&mut self, join_res: Result<TaskReport, tokio::task::JoinError>) {
        match join_res {
            Ok(report) => {
                self.engine.complete_task(report.id);
                if !matches!(report.outcome, TaskOutcome::Shutdown) {
                    self.completed_jobs.push((report.id, report.outcome));
                }
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

    fn spawn_task_in(&mut self, initial_state: ExternalState) -> Result<TaskId, TaskError> {
        let (id, directive_rx) = self.engine.register_task(initial_state)?;
        let task = IoTask::new(
            id,
            initial_state,
            Arc::clone(&self.workflow),
            self.micro_tx.clone(),
            directive_rx,
            self.cancel_token.child_token(),
        );
        self.join_set.spawn(async move {
            let outcome = task.run().await;
            TaskReport { id, outcome }
        });
        Ok(id)
    }

    fn handle_event(&mut self, evt: CoordinatorEvent<W::Input>) {
        match evt {
            CoordinatorEvent::Prewarm { count, reply } => {
                let mut ids = Vec::with_capacity(count);
                let mut res = Ok(());
                for _ in 0..count {
                    match self.spawn_task_in(ExternalState::Empty) {
                        Ok(id) => ids.push(id),
                        Err(err) => {
                            res = Err(err);
                            break;
                        }
                    }
                }
                let _ = reply.send(res.map(|()| ids));
            }
            CoordinatorEvent::AssignWork { input, reply } => {
                let res = match self.engine.pop_idle_for_connect() {
                    Ok(Some(id)) => {
                        self.engine.dispatch_to_task(id, input);
                        Ok(id)
                    }
                    Ok(None) => {
                        let spawned = self.spawn_task_in(ExternalState::Connecting);
                        if let Ok(id) = spawned {
                            self.engine.dispatch_to_task(id, input);
                        }
                        spawned
                    }
                    Err(err) => Err(err),
                };
                let _ = reply.send(res);
            }
            CoordinatorEvent::RecycleTask { task_id, reply } => {
                let sent = self.engine.send_directive(task_id, TaskDirective::Drain);
                let _ = reply.send(sent);
            }
            CoordinatorEvent::QueryTaskState { task_id, reply } => {
                let _ = reply.send(self.engine.task_state(task_id));
            }
            CoordinatorEvent::QueryPhase { reply } => {
                let _ = reply.send(self.engine.phase());
            }
            CoordinatorEvent::QueryIdleCount { reply } => {
                let _ = reply.send(self.engine.idle_task_count());
            }
            CoordinatorEvent::GracefulStop { reply } => {
                let phase = self.engine.begin_graceful_stop();
                if matches!(phase, TunnelPhase::Draining) && self.drain_sleep.is_none() {
                    self.drain_sleep = Some(Box::pin(tokio::time::sleep(self.drain_timeout)));
                }
                let _ = reply.send(phase);
            }
        }
    }
}
