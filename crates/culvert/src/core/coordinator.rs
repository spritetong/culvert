// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Two-channel (`Microtask` + `Event`) Coordinator Actor with unified `Idle` `IoTask`
//! management, embedded asynchronous dialing (`DialConfig`), session/generation sweep,
//! and `CancellationToken` force-stop.
//! All comments and docstrings must be in English.

use crate::core::engine::{StateEngine, TransportDispatch};
use crate::core::error::TaskError;
use crate::core::error::TunnelError;
use crate::core::stage::TaskWorkflow;
use crate::core::state::{
    CoordinatorConfig, DialConfig, ExternalState, IdleId, JobId, PoolStats, PooledTransport,
    TaskDirective, TaskId, TaskOutcome, TunnelPhase, WaitResult,
};
use crate::core::stream::BoxFuture;
use crate::core::task::{InitialTaskPhase, IoTask};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Sleep;
use tokio_util::sync::CancellationToken;

const COORDINATOR_EVENT_QUEUE_CAPACITY: usize = 1024;

/// Final report returned when an `IoTask` exits its `JoinSet` handle.
struct TaskReport {
    id: TaskId,
    join_id: tokio::task::Id,
    outcome: TaskOutcome,
}

/// High-priority synchronous state synchronization messages sent from Tasks to Coordinator.
///
/// Contract: the Coordinator handler for `Microtask` MUST remain 100% synchronous (never `.await`),
/// mutating only `StateEngine` and replying via non-blocking `oneshot`/`unbounded` channels.
pub enum Microtask {
    StateChanged {
        id: TaskId,
        job_id: JobId,
        state: ExternalState,
    },
    RecycledToEmpty {
        id: TaskId,
        job_id: JobId,
        outcome: TaskOutcome,
    },
    IdleCleared {
        id: TaskId,
        idle_id: IdleId,
        dead_session: Option<u64>,
    },
    ReturnTransport {
        id: TaskId,
        transport: PooledTransport,
    },
    RequestTransport {
        waiter: TaskId,
        job_id: JobId,
        reply: oneshot::Sender<Result<PooledTransport, TaskError>>,
    },
    SweepSession {
        session_id: u64,
    },
    AwaitState {
        waiter: TaskId,
        waiter_job: JobId,
        target: TaskId,
        desired: ExternalState,
        reply: oneshot::Sender<WaitResult>,
    },
}

enum DialEvent {
    Dialed(Result<PooledTransport, TunnelError>),
    RetryReady,
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
    SupplyTransport {
        transport: PooledTransport,
        reply: oneshot::Sender<bool>,
    },
    TakeIdleTransport {
        reply: oneshot::Sender<Option<PooledTransport>>,
    },
    SweepIdle {
        reply: oneshot::Sender<usize>,
    },
    SweepSession {
        session_id: u64,
        reply: oneshot::Sender<usize>,
    },
    SweepBeforeGeneration {
        min_generation: u64,
        reply: oneshot::Sender<usize>,
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
    QueryEmptyCount {
        reply: oneshot::Sender<usize>,
    },
    QueryIdleCount {
        reply: oneshot::Sender<usize>,
    },
    QueryPoolStats {
        reply: oneshot::Sender<PoolStats>,
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
    /// Pre-spawns `count` idle `Empty` worker slots ready for instant transport or job assignment.
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

    /// Assigns `input` to an `Idle` task (LIFO) if present, else an `Empty` worker,
    /// spawning a new worker if none are available.
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

    /// Supplies an inbound pre-warmed `PooledTransport` to a waiting `Connecting` task or an
    /// `Empty -> Idle` worker. Returns `false` if dropped due to capacity or shutdown.
    pub async fn supply_transport(&self, transport: PooledTransport) -> Result<bool, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(CoordinatorEvent::SupplyTransport {
                transport,
                reply: reply_tx,
            })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }

    /// Extracts one live transport from the freshest `Idle` task (used for `0xA6` signaling promotion).
    pub async fn take_idle_transport(&self) -> Result<Option<PooledTransport>, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(CoordinatorEvent::TakeIdleTransport { reply: reply_tx })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }

    /// Asynchronously sweeps all currently `Idle` tasks by sending `CloseTransport`.
    pub async fn sweep_idle(&self) -> Result<usize, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(CoordinatorEvent::SweepIdle { reply: reply_tx })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }

    /// Asynchronously sweeps all `Idle` tasks sharing `session_id`.
    pub async fn sweep_session(&self, session_id: u64) -> Result<usize, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(CoordinatorEvent::SweepSession {
                session_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }

    /// Asynchronously sweeps all `Idle` tasks belonging to a generation older than `min_generation`.
    pub async fn sweep_before_generation(&self, min_generation: u64) -> Result<usize, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(CoordinatorEvent::SweepBeforeGeneration {
                min_generation,
                reply: reply_tx,
            })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
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

    /// Queries the number of empty worker slots (`ExternalState::Empty`).
    pub async fn query_empty_count(&self) -> Result<usize, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(CoordinatorEvent::QueryEmptyCount { reply: reply_tx })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }

    /// Queries the number of `Idle` tasks currently holding live transports.
    pub async fn query_idle_count(&self) -> Result<usize, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(CoordinatorEvent::QueryIdleCount { reply: reply_tx })
            .await
            .map_err(|_| TaskError::CoordinatorGone)?;
        reply_rx.await.map_err(|_| TaskError::CoordinatorGone)
    }

    /// Returns a point-in-time `PoolStats` snapshot.
    pub async fn query_pool_stats(&self) -> Result<PoolStats, TaskError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .event_tx
            .send(CoordinatorEvent::QueryPoolStats { reply: reply_tx })
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

    /// Returns `true` if `self` and `other` point to the same Coordinator instance.
    #[inline]
    pub fn same_coordinator(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

/// Completion record for one job cycle executed on a supervised `IoTask`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedJob {
    pub task_id: TaskId,
    pub job_id: JobId,
    pub outcome: TaskOutcome,
}

/// Summary produced when the Coordinator Actor exits cleanly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorSummary {
    pub final_phase: TunnelPhase,
    pub completed_jobs: Vec<CompletedJob>,
}

struct DialDriver {
    cfg: DialConfig,
    in_flight: FuturesUnordered<BoxFuture<'static, Result<PooledTransport, TunnelError>>>,
    next_retry_at: Instant,
    backoff: Duration,
    retry_sleep: Option<Pin<Box<Sleep>>>,
}

impl DialDriver {
    fn new(cfg: DialConfig) -> Self {
        let backoff = cfg.backoff_base;
        Self {
            cfg,
            in_flight: FuturesUnordered::new(),
            next_retry_at: Instant::now(),
            backoff,
            retry_sleep: None,
        }
    }

    fn reset_backoff_for_demand(&mut self, pending_waiters: usize) {
        if pending_waiters == 0 && self.in_flight.is_empty() {
            self.next_retry_at = Instant::now();
            self.backoff = self.cfg.backoff_base;
            self.retry_sleep = None;
        }
    }

    fn replenish(&mut self, idle_len: usize, pending_waiters: usize, max_idle_capacity: usize) {
        let now = Instant::now();
        loop {
            let in_flight = self.in_flight.len();
            let current_total = idle_len + in_flight;
            let has_waiters = pending_waiters > 0;
            let needed = (current_total < self.cfg.target_size)
                || (has_waiters && in_flight < pending_waiters);
            let dial_throttled = in_flight >= self.cfg.max_concurrent_connects;
            let capacity_bound = self.cfg.target_size.max(max_idle_capacity);
            let capacity_available =
                !dial_throttled && (has_waiters || current_total < capacity_bound);
            let throttled_by_failure = self.backoff > self.cfg.backoff_base && in_flight >= 1;

            if !(needed && capacity_available && !throttled_by_failure) {
                break;
            }
            if now < self.next_retry_at {
                if in_flight == 0 && self.retry_sleep.is_none() {
                    self.retry_sleep = Some(Box::pin(tokio::time::sleep_until(
                        self.next_retry_at.into(),
                    )));
                }
                break;
            }

            let conn = Arc::clone(&self.cfg.connector);
            let target = self.cfg.target.clone();
            let protocol = self.cfg.protocol;
            let tunnel_id = self.cfg.tunnel_id.clone();

            self.in_flight.push(Box::pin(async move {
                let ch = conn
                    .connect_pooled_with_tunnel(&target, protocol, tunnel_id.as_deref())
                    .await?;
                let stream = ch.into_stream()?;
                Ok(PooledTransport::new(stream, 0))
            }));
        }
    }

    fn clear_on_stop(&mut self) {
        self.in_flight.clear();
        self.retry_sleep = None;
    }
}

/// Single-writer Coordinator Actor supervising all `IoTask` lifecycles, idle workers,
/// optional dial pre-warming, and session/generation sweeps.
pub struct Coordinator<W: TaskWorkflow> {
    workflow: Arc<W>,
    engine: StateEngine<W::Input>,
    join_set: JoinSet<TaskReport>,
    join_tasks: HashMap<tokio::task::Id, TaskId>,
    config: CoordinatorConfig,
    dial: Option<DialDriver>,
    drain_sleep: Option<Pin<Box<Sleep>>>,
    cancel_token: CancellationToken,
    micro_tx: mpsc::UnboundedSender<Microtask>,
    micro_rx: mpsc::UnboundedReceiver<Microtask>,
    _keep_event_tx: mpsc::Sender<CoordinatorEvent<W::Input>>,
    event_rx: mpsc::Receiver<CoordinatorEvent<W::Input>>,
    completed_jobs: Vec<CompletedJob>,
}

impl<W: TaskWorkflow> Coordinator<W> {
    /// Starts a plain Coordinator Actor with `drain_timeout` and `cancel_token`.
    pub fn start(
        workflow: W,
        drain_timeout: Duration,
        cancel_token: CancellationToken,
    ) -> (CoordinatorHandle<W>, JoinHandle<CoordinatorSummary>) {
        Self::start_with_config(
            workflow,
            CoordinatorConfig::plain(drain_timeout),
            cancel_token,
        )
    }

    /// Starts a Coordinator Actor configured with idle capacity, policy, and optional dialing.
    pub fn start_with_config(
        workflow: W,
        config: CoordinatorConfig,
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

        let dial = config.dial.clone().map(DialDriver::new);
        let actor = Self {
            workflow: Arc::new(workflow),
            engine: StateEngine::new(),
            join_set: JoinSet::new(),
            join_tasks: HashMap::new(),
            config,
            dial,
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
            self.replenish_dials_if_needed();

            if matches!(self.engine.phase(), TunnelPhase::Stopped) && self.join_set.is_empty() {
                break;
            }

            let has_dial_work = self
                .dial
                .as_ref()
                .is_some_and(|d| !d.in_flight.is_empty() || d.retry_sleep.is_some());

            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled(), if !matches!(self.engine.phase(), TunnelPhase::ForceStopping | TunnelPhase::Stopped) => {
                    self.drain_sleep = None;
                    if let Some(ref mut d) = self.dial {
                        d.clear_on_stop();
                    }
                    self.engine.begin_force_stop();
                }
                micro_opt = self.micro_rx.recv() => {
                    let Some(micro) = micro_opt else {
                        unreachable!("Coordinator retains micro_tx so micro_rx never closes");
                    };
                    self.handle_microtask(micro);
                    self.drain_microtasks();
                }
                Some(dial_evt) = Self::poll_dial(&mut self.dial), if has_dial_work => {
                    match dial_evt {
                        DialEvent::Dialed(dial_res) => self.handle_dial_result(dial_res),
                        DialEvent::RetryReady => {
                            if let Some(ref mut d) = self.dial {
                                d.retry_sleep = None;
                            }
                        }
                    }
                }
                Some(join_res) = self.join_set.join_next(), if !self.join_set.is_empty() => {
                    self.handle_join_result(join_res);
                }
                _ = Self::poll_drain_sleep(&mut self.drain_sleep), if self.drain_sleep.is_some() => {
                    self.drain_sleep = None;
                    if let Some(ref mut d) = self.dial {
                        d.clear_on_stop();
                    }
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

    async fn poll_dial(dial: &mut Option<DialDriver>) -> Option<DialEvent> {
        let d = dial.as_mut()?;
        let has_in_flight = !d.in_flight.is_empty();
        let has_sleep = d.retry_sleep.is_some();
        tokio::select! {
            Some(res) = d.in_flight.next(), if has_in_flight => Some(DialEvent::Dialed(res)),
            () = Self::poll_drain_sleep(&mut d.retry_sleep), if has_sleep => Some(DialEvent::RetryReady),
            else => None,
        }
    }

    async fn poll_drain_sleep(sleep_opt: &mut Option<Pin<Box<Sleep>>>) {
        if let Some(sleep) = sleep_opt.as_mut() {
            sleep.as_mut().await;
        }
    }

    fn replenish_dials_if_needed(&mut self) {
        if !self.engine.phase().accepts_work() {
            return;
        }
        let idle_len = self.engine.idle_task_count();
        let pending_waiters = self.engine.waiting_transport_count();
        let max_idle = self.config.max_idle_capacity;
        if let Some(ref mut d) = self.dial {
            d.replenish(idle_len, pending_waiters, max_idle);
        }
    }

    fn handle_dial_result(&mut self, res: Result<PooledTransport, TunnelError>) {
        let Some(ref mut d) = self.dial else {
            return;
        };
        match res {
            Ok(transport) => {
                d.backoff = d.cfg.backoff_base;
                d.next_retry_at = Instant::now();
                d.retry_sleep = None;
                let _ = self.dispatch_transport(transport);
            }
            Err(err) => {
                tracing::debug!(error = ?err, "Coordinator background dial failed");
                let has_waiters = self.engine.waiting_transport_count() > 0;
                let max_backoff = if has_waiters {
                    d.cfg.backoff_active_waiters_max
                } else {
                    d.cfg.backoff_max
                };
                d.next_retry_at = Instant::now() + d.backoff;
                d.backoff = (d.backoff * 2).min(max_backoff);
                d.retry_sleep = Some(Box::pin(tokio::time::sleep_until(d.next_retry_at.into())));
            }
        }
    }

    fn drain_microtasks(&mut self) {
        while let Ok(micro) = self.micro_rx.try_recv() {
            self.handle_microtask(micro);
        }
    }

    fn handle_microtask(&mut self, micro: Microtask) {
        match micro {
            Microtask::StateChanged { id, job_id, state } => {
                self.engine.apply_task_state(id, job_id, state);
            }
            Microtask::RecycledToEmpty {
                id,
                job_id,
                outcome,
            } => {
                if self.engine.on_task_recycled(id, job_id)
                    || !matches!(outcome, TaskOutcome::Shutdown)
                {
                    self.completed_jobs.push(CompletedJob {
                        task_id: id,
                        job_id,
                        outcome,
                    });
                }
            }
            Microtask::IdleCleared {
                id,
                idle_id,
                dead_session,
            } => {
                self.engine.on_idle_cleared(id, idle_id, dead_session);
            }
            Microtask::ReturnTransport { id: _, transport } => {
                let _ = self.dispatch_transport(transport);
            }
            Microtask::RequestTransport {
                waiter,
                job_id,
                reply,
            } => {
                let pending = self.engine.waiting_transport_count();
                if let Some(ref mut d) = self.dial {
                    d.reset_backoff_for_demand(pending);
                }
                self.engine.register_transport_waiter(waiter, job_id, reply);
            }
            Microtask::SweepSession { session_id } => {
                self.engine.sweep_idle_by_session(session_id);
            }
            Microtask::AwaitState {
                waiter,
                waiter_job,
                target,
                desired,
                reply,
            } => {
                self.engine
                    .register_waiter(waiter, waiter_job, target, desired, reply);
            }
        }
    }

    fn handle_join_result(&mut self, join_res: Result<TaskReport, tokio::task::JoinError>) {
        match join_res {
            Ok(report) => {
                self.join_tasks.remove(&report.join_id);
                if let Some((_, Some(job_id))) = self.engine.complete_task(report.id)
                    && !matches!(report.outcome, TaskOutcome::Shutdown)
                {
                    self.completed_jobs.push(CompletedJob {
                        task_id: report.id,
                        job_id,
                        outcome: report.outcome,
                    });
                }
            }
            Err(join_err) => {
                tracing::error!(
                    error = ?join_err,
                    "Supervised IO task terminated unexpectedly"
                );
                if let Some(task_id) = self.join_tasks.remove(&join_err.id())
                    && let Some((_, Some(job_id))) = self.engine.complete_task(task_id)
                {
                    self.completed_jobs.push(CompletedJob {
                        task_id,
                        job_id,
                        outcome: TaskOutcome::Failed(TaskError::StageFailed(format!(
                            "supervised task terminated unexpectedly: {join_err}"
                        ))),
                    });
                }
            }
        }
        if self.join_set.is_empty() {
            self.drain_sleep = None;
        }
    }

    fn dispatch_transport(&mut self, transport: PooledTransport) -> bool {
        let session_id = transport.session_id;
        let generation = transport.generation;
        match self.engine.try_dispatch_transport(
            transport,
            self.config.idle_policy,
            self.config.max_idle_capacity,
        ) {
            TransportDispatch::DeliveredToWaiter(_) | TransportDispatch::AssignedIdle(_) => true,
            TransportDispatch::Dropped => false,
            TransportDispatch::NeedsSpawnIdle(transport) => {
                let Ok(reg) = self.engine.register_task_with_meta(
                    ExternalState::Idle,
                    session_id,
                    generation,
                ) else {
                    return false;
                };
                let id = reg.id;
                let Some(idle_id) = reg.idle_id else {
                    return false;
                };
                let task = IoTask::new(
                    id,
                    InitialTaskPhase::Idle {
                        idle_id,
                        transport,
                        policy: self.config.idle_policy,
                    },
                    Arc::clone(&self.workflow),
                    self.micro_tx.clone(),
                    reg.directive_rx,
                    self.cancel_token.child_token(),
                );
                let abort_handle = self.join_set.spawn(async move {
                    let join_id = tokio::task::id();
                    let outcome = task.run().await;
                    TaskReport {
                        id,
                        join_id,
                        outcome,
                    }
                });
                self.join_tasks.insert(abort_handle.id(), id);
                true
            }
        }
    }

    fn spawn_task_with(&mut self, initial_input: Option<W::Input>) -> Result<TaskId, TaskError> {
        let initial_state = if initial_input.is_some() {
            ExternalState::Connecting
        } else {
            ExternalState::Empty
        };
        let reg = self.engine.register_task(initial_state)?;
        let id = reg.id;
        let initial_phase = match (reg.job_id, initial_input) {
            (Some(job_id), Some(input)) => InitialTaskPhase::Connecting { job_id, input },
            _ => InitialTaskPhase::Empty,
        };
        let task = IoTask::new(
            id,
            initial_phase,
            Arc::clone(&self.workflow),
            self.micro_tx.clone(),
            reg.directive_rx,
            self.cancel_token.child_token(),
        );
        let abort_handle = self.join_set.spawn(async move {
            let join_id = tokio::task::id();
            let outcome = task.run().await;
            TaskReport {
                id,
                join_id,
                outcome,
            }
        });
        self.join_tasks.insert(abort_handle.id(), id);
        Ok(id)
    }

    fn handle_event(&mut self, evt: CoordinatorEvent<W::Input>) {
        match evt {
            CoordinatorEvent::Prewarm { count, reply } => {
                let mut ids = Vec::with_capacity(count);
                let mut res = Ok(());
                for _ in 0..count {
                    match self.spawn_task_with(None) {
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
                let res = match self.engine.try_assign_idle(input) {
                    Ok(Ok((id, _job_id))) => Ok(id),
                    Ok(Err(recovered_input)) => self.spawn_task_with(Some(recovered_input)),
                    Err(err) => Err(err),
                };
                let _ = reply.send(res);
            }
            CoordinatorEvent::SupplyTransport { transport, reply } => {
                let accepted = self.dispatch_transport(transport);
                let _ = reply.send(accepted);
            }
            CoordinatorEvent::TakeIdleTransport { reply } => {
                let _ = self.engine.extract_idle_transport(reply);
            }
            CoordinatorEvent::SweepIdle { reply } => {
                let _ = reply.send(self.engine.sweep_all_idle());
            }
            CoordinatorEvent::SweepSession { session_id, reply } => {
                let _ = reply.send(self.engine.sweep_idle_by_session(session_id));
            }
            CoordinatorEvent::SweepBeforeGeneration {
                min_generation,
                reply,
            } => {
                let _ = reply.send(self.engine.sweep_idle_before_generation(min_generation));
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
            CoordinatorEvent::QueryEmptyCount { reply } => {
                let _ = reply.send(self.engine.empty_task_count());
            }
            CoordinatorEvent::QueryIdleCount { reply } => {
                let _ = reply.send(self.engine.idle_task_count());
            }
            CoordinatorEvent::QueryPoolStats { reply } => {
                let in_flight = self.dial.as_ref().map_or(0, |d| d.in_flight.len());
                let _ = reply.send(self.engine.pool_stats(in_flight));
            }
            CoordinatorEvent::GracefulStop { reply } => {
                if let Some(ref mut d) = self.dial {
                    d.clear_on_stop();
                }
                let phase = self.engine.begin_graceful_stop();
                if matches!(phase, TunnelPhase::Draining) && self.drain_sleep.is_none() {
                    self.drain_sleep =
                        Some(Box::pin(tokio::time::sleep(self.config.drain_timeout)));
                }
                let _ = reply.send(phase);
            }
        }
    }
}
