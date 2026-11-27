// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! IO Task: single outer cancellation boundary and unified event loop owning
//! `Empty`, `Idle` (local keepalive/eviction), `Connecting`, `Active`, and `Closing`.
//! All comments and docstrings must be in English.

use crate::consts::{
    POOL_HEARTBEAT_TICK_INTERVAL, POOL_HEARTBEAT_TIMEOUT, reverse_pool_expiration_margin,
};
use crate::io::stream::{HeartbeatStatus, activity_now_ms, heartbeat_timing};
use crate::v2::coordinator::Microtask;
use crate::v2::error::TaskError;
use crate::v2::stage::{ActiveStage, ClosingStage, ConnectingContext, TaskWorkflow};
use crate::v2::state::{
    CloseReason, ExternalState, IdleId, IdlePolicy, JobId, PooledTransport, TaskDirective, TaskId,
    TaskOutcome,
};
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Reason a local `Idle` keepalive loop retired its `PooledTransport`.
enum IdleExitReason {
    Dead(Option<u64>),
    Expired,
}

/// Initial state passed when spawning an `IoTask` (`Empty`, `Idle`, or `Connecting`).
pub enum InitialTaskPhase<I> {
    Empty,
    Idle {
        idle_id: IdleId,
        transport: PooledTransport,
        policy: IdlePolicy,
    },
    Connecting {
        job_id: JobId,
        input: I,
    },
}

/// Single source of truth binding external lifecycle phase to its exact resource payload.
///
/// Illegal combinations such as `(state = Idle, transport = None)` are unrepresentable.
enum TaskPhase<W: TaskWorkflow> {
    Empty,
    Idle {
        idle_id: IdleId,
        transport: PooledTransport,
        policy: IdlePolicy,
        enqueued_at: Instant,
    },
    Connecting {
        job_id: JobId,
        input: W::Input,
        transport: Option<PooledTransport>,
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
            Self::Idle { .. } => ExternalState::Idle,
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
/// while idle transport keepalive, ping probing, and dead/expiry cleanup run directly
/// inside `TaskPhase::Idle`.
pub struct IoTask<W: TaskWorkflow> {
    id: TaskId,
    workflow: Arc<W>,
    micro_tx: mpsc::UnboundedSender<Microtask>,
    directive_rx: mpsc::UnboundedReceiver<TaskDirective<W::Input>>,
    cancel: CancellationToken,
    initial_phase: Option<TaskPhase<W>>,
}

impl<W: TaskWorkflow> IoTask<W> {
    /// Constructs a new `IoTask` in `Empty`, `Idle`, or `Connecting`.
    pub fn new(
        id: TaskId,
        initial: InitialTaskPhase<W::Input>,
        workflow: Arc<W>,
        micro_tx: mpsc::UnboundedSender<Microtask>,
        directive_rx: mpsc::UnboundedReceiver<TaskDirective<W::Input>>,
        cancel: CancellationToken,
    ) -> Self {
        let initial_phase = match initial {
            InitialTaskPhase::Empty => TaskPhase::Empty,
            InitialTaskPhase::Idle {
                idle_id,
                transport,
                policy,
            } => TaskPhase::Idle {
                idle_id,
                transport,
                policy,
                enqueued_at: Instant::now(),
            },
            InitialTaskPhase::Connecting { job_id, input } => TaskPhase::Connecting {
                job_id,
                input,
                transport: None,
            },
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
                    // Race recovery: if Coordinator dispatched Assign expecting Idle right after
                    // local transport loss, proceed seamlessly into Connecting with `transport: None`.
                    Some(TaskDirective::Assign { job_id, input }) => TaskPhase::Connecting {
                        job_id,
                        input,
                        transport: None,
                    },
                    Some(TaskDirective::SupplyIdle {
                        idle_id,
                        transport,
                        policy,
                    }) => TaskPhase::Idle {
                        idle_id,
                        transport,
                        policy,
                        enqueued_at: Instant::now(),
                    },
                    Some(TaskDirective::ExtractTransport { reply, .. }) => {
                        let _ = reply.send(None);
                        TaskPhase::Empty
                    }
                    // Ignore late CloseTransport / Drain directives when already Empty.
                    Some(TaskDirective::CloseTransport { .. } | TaskDirective::Drain) => {
                        TaskPhase::Empty
                    }
                    Some(TaskDirective::Shutdown) | None => TaskPhase::Closed {
                        outcome: TaskOutcome::Shutdown,
                    },
                },
                TaskPhase::Idle {
                    idle_id,
                    mut transport,
                    policy,
                    enqueued_at,
                } => {
                    enum IdleBreak<I> {
                        KeepaliveExit(IdleExitReason),
                        Directive(Option<TaskDirective<I>>),
                    }

                    let brk = tokio::select! {
                        biased;
                        dir = self.directive_rx.recv() => IdleBreak::Directive(dir),
                        reason = Self::drive_idle_keepalive(&mut transport, enqueued_at, policy) => {
                            IdleBreak::KeepaliveExit(reason)
                        }
                    };

                    match brk {
                        IdleBreak::Directive(Some(TaskDirective::Assign { job_id, input })) => {
                            TaskPhase::Connecting {
                                job_id,
                                input,
                                transport: Some(transport),
                            }
                        }
                        IdleBreak::Directive(Some(TaskDirective::CloseTransport {
                            expected_idle_id,
                        })) => {
                            if expected_idle_id.is_none_or(|exp| exp == idle_id) {
                                drop(transport);
                                self.clear_idle_or_exit(idle_id, None)
                            } else {
                                TaskPhase::Idle {
                                    idle_id,
                                    transport,
                                    policy,
                                    enqueued_at,
                                }
                            }
                        }
                        IdleBreak::Directive(Some(TaskDirective::ExtractTransport {
                            expected_idle_id,
                            reply,
                        })) => {
                            if expected_idle_id == idle_id {
                                match reply.send(Some(transport)) {
                                    Ok(()) => self.clear_idle_or_exit(idle_id, None),
                                    Err(Some(returned)) => {
                                        // Caller dropped reply receiver; return transport to Coordinator
                                        // so tracking stays synchronized without losing the stream.
                                        self.return_transport(returned);
                                        self.clear_idle_or_exit(idle_id, None)
                                    }
                                    Err(None) => self.clear_idle_or_exit(idle_id, None),
                                }
                            } else {
                                let _ = reply.send(None);
                                TaskPhase::Idle {
                                    idle_id,
                                    transport,
                                    policy,
                                    enqueued_at,
                                }
                            }
                        }
                        IdleBreak::Directive(Some(TaskDirective::Drain)) => {
                            drop(transport);
                            self.clear_idle_or_exit(idle_id, None)
                        }
                        IdleBreak::Directive(Some(TaskDirective::Shutdown) | None) => {
                            drop(transport);
                            TaskPhase::Closed {
                                outcome: TaskOutcome::Shutdown,
                            }
                        }
                        IdleBreak::Directive(Some(TaskDirective::SupplyIdle {
                            transport: extra,
                            ..
                        })) => {
                            // Unexpected SupplyIdle while already Idle: return synchronously to Coordinator.
                            self.return_transport(extra);
                            TaskPhase::Idle {
                                idle_id,
                                transport,
                                policy,
                                enqueued_at,
                            }
                        }
                        IdleBreak::KeepaliveExit(reason) => {
                            let dead_session = match reason {
                                IdleExitReason::Dead(sid) => sid,
                                IdleExitReason::Expired => None,
                            };
                            drop(transport);
                            self.clear_idle_or_exit(idle_id, dead_session)
                        }
                    }
                }
                TaskPhase::Connecting {
                    job_id,
                    input,
                    transport,
                } => {
                    let mut ctx =
                        ConnectingContext::new(self.id, job_id, transport, &self.micro_tx);
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
                        ConnectBreak::Directive(Some(TaskDirective::SupplyIdle {
                            transport: unwanted,
                            ..
                        })) => {
                            // Non-blocking auto-recovery: wrong-state transport goes straight back to Coordinator.
                            self.return_transport(unwanted);
                            self.recycle_or_exit(job_id, TaskOutcome::GracefullyDrained, false)
                        }
                        ConnectBreak::Directive(Some(TaskDirective::ExtractTransport {
                            reply,
                            ..
                        })) => {
                            let _ = reply.send(None);
                            self.recycle_or_exit(job_id, TaskOutcome::GracefullyDrained, false)
                        }
                        ConnectBreak::Directive(Some(TaskDirective::CloseTransport { .. })) => {
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
                                transport: None,
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
                        ActiveBreak::Directive(Some(TaskDirective::SupplyIdle {
                            transport,
                            ..
                        })) => {
                            self.return_transport(transport);
                            phase = TaskPhase::Active { job_id, stage };
                            continue;
                        }
                        ActiveBreak::Directive(Some(TaskDirective::ExtractTransport {
                            reply,
                            ..
                        })) => {
                            let _ = reply.send(None);
                            phase = TaskPhase::Active { job_id, stage };
                            continue;
                        }
                        ActiveBreak::Directive(Some(
                            TaskDirective::Assign { .. } | TaskDirective::CloseTransport { .. },
                        )) => {
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

    async fn drive_idle_keepalive(
        transport: &mut PooledTransport,
        enqueued_at: Instant,
        policy: IdlePolicy,
    ) -> IdleExitReason {
        let tick = (policy.heartbeat_interval / 2)
            .min(policy.idle_timeout / 2)
            .min(POOL_HEARTBEAT_TICK_INTERVAL)
            .max(Duration::from_millis(10));
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + tick, tick);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let max_age_sleep = policy.max_idle_age.map(|max_age| {
            let margin = reverse_pool_expiration_margin(max_age);
            let effective = max_age.saturating_sub(margin);
            Box::pin(tokio::time::sleep_until((enqueued_at + effective).into()))
        });
        tokio::pin!(max_age_sleep);

        loop {
            tokio::select! {
                biased;
                _ = async {
                    if let Some(s) = max_age_sleep.as_mut().as_pin_mut() {
                        s.await;
                    }
                }, if max_age_sleep.is_some() => {
                    return IdleExitReason::Expired;
                }
                _ = interval.tick() => {
                    let alive = poll_fn(|cx| match transport.stream.poll_check_alive(cx) {
                        Poll::Ready(HeartbeatStatus::Dead) => Poll::Ready(false),
                        _ => Poll::Ready(true),
                    })
                    .await;
                    if !alive {
                        return IdleExitReason::Dead(transport.session_id);
                    }

                    let (probe, expired) = heartbeat_timing(
                        transport.stream.last_activity_ms(),
                        activity_now_ms(),
                        policy.heartbeat_interval,
                        policy.idle_timeout,
                    );
                    if expired {
                        return IdleExitReason::Expired;
                    }

                    if policy.probe_enabled && probe {
                        let ping_res = tokio::time::timeout(
                            POOL_HEARTBEAT_TIMEOUT,
                            poll_fn(|cx| transport.stream.poll_send_ping(cx)),
                        )
                        .await;
                        if !matches!(ping_res, Ok(Ok(()))) {
                            return IdleExitReason::Dead(transport.session_id);
                        }
                    }
                }
            }
        }
    }

    fn report_state(&self, job_id: JobId, state: ExternalState) {
        let _ = self.micro_tx.send(Microtask::StateChanged {
            id: self.id,
            job_id,
            state,
        });
    }

    fn return_transport(&self, transport: PooledTransport) {
        let _ = self.micro_tx.send(Microtask::ReturnTransport {
            id: self.id,
            transport,
        });
    }

    fn clear_idle_or_exit(&self, idle_id: IdleId, dead_session: Option<u64>) -> TaskPhase<W> {
        if self
            .micro_tx
            .send(Microtask::IdleCleared {
                id: self.id,
                idle_id,
                dead_session,
            })
            .is_err()
        {
            TaskPhase::Closed {
                outcome: TaskOutcome::Shutdown,
            }
        } else {
            TaskPhase::Empty
        }
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
