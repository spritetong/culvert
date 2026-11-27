// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Pure single-writer state machine engine owned by the Coordinator.
//! All comments and docstrings must be in English.

use crate::v2::error::TaskError;
use crate::v2::state::{
    ExternalState, IdleId, IdlePolicy, JobId, PoolStats, PooledTransport, TaskDirective, TaskId,
    TunnelPhase, WaitResult,
};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::{mpsc, oneshot};

/// Maximum number of concurrent inter-task state waiters tracked by `StateEngine`.
const MAX_PENDING_WAITERS: usize = 4096;

#[derive(Debug, Clone, Copy)]
struct IdleRecord {
    idle_id: IdleId,
    session_id: Option<u64>,
    generation: u64,
}

/// Coordinator-owned record for one live `IoTask`.
struct TaskEntry<I> {
    state: ExternalState,
    current_job: Option<JobId>,
    current_idle: Option<IdleRecord>,
    directive_tx: mpsc::UnboundedSender<TaskDirective<I>>,
}

impl<I> TaskEntry<I> {
    fn new(
        state: ExternalState,
        current_job: Option<JobId>,
        current_idle: Option<IdleRecord>,
        directive_tx: mpsc::UnboundedSender<TaskDirective<I>>,
    ) -> Self {
        Self {
            state,
            current_job,
            current_idle,
            directive_tx,
        }
    }

    fn send_directive(&self, directive: TaskDirective<I>) -> Result<(), TaskDirective<I>> {
        self.directive_tx.send(directive).map_err(|err| err.0)
    }
}

/// Pending inter-task state dependency mediated by the Coordinator.
struct StateWaiter {
    waiter: TaskId,
    waiter_job: JobId,
    target: TaskId,
    desired: ExternalState,
    reply: oneshot::Sender<WaitResult>,
}

/// Pending `Connecting` task awaiting a `PooledTransport` from Coordinator.
struct TransportWaiter {
    task_id: TaskId,
    job_id: JobId,
    reply: oneshot::Sender<Result<PooledTransport, TaskError>>,
}

/// Registration handle returned when a new `IoTask` is inserted into `StateEngine`.
pub struct RegisteredTask<I> {
    pub id: TaskId,
    pub job_id: Option<JobId>,
    pub idle_id: Option<IdleId>,
    pub directive_rx: mpsc::UnboundedReceiver<TaskDirective<I>>,
}

/// Result of dispatching an inbound `PooledTransport` synchronously inside `StateEngine`.
pub enum TransportDispatch {
    DeliveredToWaiter(TaskId),
    AssignedIdle(TaskId),
    NeedsSpawnIdle(PooledTransport),
    Dropped,
}

/// Pure synchronous state engine for Tunnel phases, Task states (`Empty`/`Idle`/`Connecting`/
/// `Active`/`Closing`/`Closed`), idle recycling, session/generation sweep, and waits.
pub struct StateEngine<I> {
    phase: TunnelPhase,
    next_task_seq: u64,
    next_job_seq: u64,
    next_idle_seq: u64,
    tasks: HashMap<TaskId, TaskEntry<I>>,
    empty_tasks: VecDeque<TaskId>,
    idle_tasks: VecDeque<TaskId>,
    waiting_transports: VecDeque<TransportWaiter>,
    waiters: Vec<StateWaiter>,
    total_idle_assigned: u64,
    total_idle_lost: u64,
    min_idle_generation: u64,
}

impl<I> StateEngine<I> {
    /// Creates a new `StateEngine` in the `Running` phase.
    pub fn new() -> Self {
        Self {
            phase: TunnelPhase::Running,
            next_task_seq: 1,
            next_job_seq: 1,
            next_idle_seq: 1,
            tasks: HashMap::new(),
            empty_tasks: VecDeque::new(),
            idle_tasks: VecDeque::new(),
            waiting_transports: VecDeque::new(),
            waiters: Vec::new(),
            total_idle_assigned: 0,
            total_idle_lost: 0,
            min_idle_generation: 0,
        }
    }

    fn alloc_job_id(&mut self) -> JobId {
        let id = JobId::new(self.next_job_seq);
        self.next_job_seq = self.next_job_seq.saturating_add(1);
        id
    }

    fn alloc_idle_id(&mut self) -> IdleId {
        let id = IdleId::new(self.next_idle_seq);
        self.next_idle_seq = self.next_idle_seq.saturating_add(1);
        id
    }

    /// Returns the current tunnel phase.
    #[inline]
    pub fn phase(&self) -> TunnelPhase {
        self.phase
    }

    /// Returns the number of tracked live tasks (including `Empty` and `Idle` tasks).
    #[inline]
    pub fn live_task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Returns the number of currently `Empty` worker slots (holding no transport).
    #[inline]
    pub fn empty_task_count(&self) -> usize {
        self.empty_tasks.len()
    }

    /// Returns the number of currently `Idle` tasks holding live transports.
    #[inline]
    pub fn idle_task_count(&self) -> usize {
        self.idle_tasks.len()
    }

    /// Returns the number of `Connecting` tasks waiting for a pooled transport.
    #[inline]
    pub fn waiting_transport_count(&mut self) -> usize {
        self.purge_closed_transport_waiters();
        self.waiting_transports.len()
    }

    /// Returns a point-in-time `PoolStats` snapshot.
    pub fn pool_stats(&mut self, in_flight_dials: usize) -> PoolStats {
        self.purge_closed_transport_waiters();
        let active_count = self
            .tasks
            .values()
            .filter(|e| matches!(e.state, ExternalState::Connecting | ExternalState::Active))
            .count();
        PoolStats {
            empty_count: self.empty_tasks.len(),
            idle_count: self.idle_tasks.len(),
            active_count,
            pending_waiters: self.waiting_transports.len(),
            in_flight_dials,
            total_idle_assigned: self.total_idle_assigned,
            total_idle_lost: self.total_idle_lost,
        }
    }

    /// Queries the current external state of `task_id`, if live.
    #[inline]
    pub fn task_state(&self, task_id: TaskId) -> Option<ExternalState> {
        self.tasks.get(&task_id).map(|entry| entry.state)
    }

    /// Queries the active `JobId` currently assigned to `task_id`, if any.
    #[inline]
    pub fn task_job(&self, task_id: TaskId) -> Option<JobId> {
        self.tasks.get(&task_id).and_then(|entry| entry.current_job)
    }

    /// Pops an `Idle` (LIFO, preferred) or `Empty` task and atomically dispatches `input`.
    ///
    /// Returns `Ok(Ok((id, job_id)))` when accepted, or `Ok(Err(input))` when a new `IoTask`
    /// should be spawned directly in `Connecting`.
    pub fn try_assign_idle(
        &mut self,
        mut input: I,
    ) -> Result<Result<(TaskId, JobId), I>, TaskError> {
        if !self.phase.accepts_work() {
            return Err(TaskError::RejectedByCoordinator(self.phase));
        }

        // 1. Fast path: pop freshest `Idle` task holding a warm transport (LIFO).
        while let Some(id) = self.idle_tasks.pop_back() {
            let is_idle = self
                .tasks
                .get(&id)
                .is_some_and(|entry| entry.state == ExternalState::Idle);
            if !is_idle {
                continue;
            }
            let job_id = self.alloc_job_id();
            let Some(entry) = self.tasks.get_mut(&id) else {
                continue;
            };
            match entry.send_directive(TaskDirective::Assign { job_id, input }) {
                Ok(()) => {
                    entry.state = ExternalState::Connecting;
                    entry.current_idle = None;
                    entry.current_job = Some(job_id);
                    self.total_idle_assigned += 1;
                    self.resolve_waiters_for(id, ExternalState::Connecting);
                    return Ok(Ok((id, job_id)));
                }
                Err(TaskDirective::Assign {
                    input: recovered, ..
                }) => {
                    input = recovered;
                }
                Err(_) => unreachable!("only Assign is sent here"),
            }
        }

        // 2. Fallback: pop an `Empty` worker slot.
        while let Some(id) = self.empty_tasks.pop_front() {
            let is_empty = self
                .tasks
                .get(&id)
                .is_some_and(|entry| entry.state == ExternalState::Empty);
            if !is_empty {
                continue;
            }
            let job_id = self.alloc_job_id();
            let Some(entry) = self.tasks.get_mut(&id) else {
                continue;
            };
            match entry.send_directive(TaskDirective::Assign { job_id, input }) {
                Ok(()) => {
                    entry.state = ExternalState::Connecting;
                    entry.current_job = Some(job_id);
                    self.resolve_waiters_for(id, ExternalState::Connecting);
                    return Ok(Ok((id, job_id)));
                }
                Err(TaskDirective::Assign {
                    input: recovered, ..
                }) => {
                    input = recovered;
                }
                Err(_) => unreachable!("only Assign is sent here"),
            }
        }

        Ok(Err(input))
    }

    /// Dispatches an inbound `PooledTransport` to waiting `Connecting` tasks or an `Empty` -> `Idle` worker.
    pub fn try_dispatch_transport(
        &mut self,
        transport: PooledTransport,
        policy: IdlePolicy,
        max_idle_capacity: usize,
    ) -> TransportDispatch {
        if transport.generation < self.min_idle_generation {
            return TransportDispatch::Dropped;
        }
        let session_id = transport.session_id;
        let generation = transport.generation;
        // 1. Hand off directly to any `Connecting` task waiting for a transport.
        let mut pending_value = Ok(transport);
        while let Some(waiter) = self.waiting_transports.pop_front() {
            let valid = self
                .tasks
                .get(&waiter.task_id)
                .is_some_and(|e| e.current_job == Some(waiter.job_id));
            if !valid || waiter.reply.is_closed() {
                continue;
            }
            match waiter.reply.send(pending_value) {
                Ok(()) => return TransportDispatch::DeliveredToWaiter(waiter.task_id),
                Err(Ok(recovered)) => {
                    pending_value = Ok(recovered);
                }
                Err(Err(_)) => return TransportDispatch::Dropped,
            }
        }
        let Ok(mut transport) = pending_value else {
            return TransportDispatch::Dropped;
        };

        if !self.phase.accepts_work() || max_idle_capacity == 0 {
            return TransportDispatch::Dropped;
        }

        // 2. Enforce soft capacity by closing the oldest `Idle` transport first.
        while self.idle_tasks.len() >= max_idle_capacity {
            let Some(oldest_id) = self.idle_tasks.pop_front() else {
                break;
            };
            if let Some(entry) = self.tasks.get(&oldest_id)
                && let Some(rec) = entry.current_idle
            {
                let _ = entry.send_directive(TaskDirective::CloseTransport {
                    expected_idle_id: Some(rec.idle_id),
                });
            }
        }

        // 3. Assign transport to an `Empty` worker so it becomes `Idle`.
        while let Some(id) = self.empty_tasks.pop_front() {
            let is_empty = self
                .tasks
                .get(&id)
                .is_some_and(|entry| entry.state == ExternalState::Empty);
            if !is_empty {
                continue;
            }
            let idle_id = self.alloc_idle_id();
            let rec = IdleRecord {
                idle_id,
                session_id,
                generation,
            };
            let Some(entry) = self.tasks.get_mut(&id) else {
                continue;
            };
            match entry.send_directive(TaskDirective::SupplyIdle {
                idle_id,
                transport,
                policy,
            }) {
                Ok(()) => {
                    entry.state = ExternalState::Idle;
                    entry.current_idle = Some(rec);
                    self.idle_tasks.push_back(id);
                    self.resolve_waiters_for(id, ExternalState::Idle);
                    return TransportDispatch::AssignedIdle(id);
                }
                Err(TaskDirective::SupplyIdle {
                    transport: recovered,
                    ..
                }) => {
                    transport = recovered;
                }
                Err(_) => unreachable!("only SupplyIdle is sent here"),
            }
        }

        // 4. Caller (Coordinator) will spawn a fresh worker directly in `Idle`.
        TransportDispatch::NeedsSpawnIdle(transport)
    }

    /// Registers a newly spawned `IoTask` in `Empty`, `Idle`, or `Connecting` state.
    pub fn register_task(
        &mut self,
        initial_state: ExternalState,
    ) -> Result<RegisteredTask<I>, TaskError> {
        self.register_task_with_meta(initial_state, None, 0)
    }

    /// Registers a newly spawned `IoTask` with optional `Idle` sweep metadata.
    pub fn register_task_with_meta(
        &mut self,
        initial_state: ExternalState,
        session_id: Option<u64>,
        generation: u64,
    ) -> Result<RegisteredTask<I>, TaskError> {
        if !self.phase.accepts_work() {
            return Err(TaskError::RejectedByCoordinator(self.phase));
        }
        let id = TaskId::new(self.next_task_seq);
        self.next_task_seq = self.next_task_seq.saturating_add(1);
        let initial_job =
            matches!(initial_state, ExternalState::Connecting).then(|| self.alloc_job_id());
        let initial_idle = matches!(initial_state, ExternalState::Idle).then(|| IdleRecord {
            idle_id: self.alloc_idle_id(),
            session_id,
            generation,
        });
        let (directive_tx, directive_rx) = mpsc::unbounded_channel();
        self.tasks.insert(
            id,
            TaskEntry::new(initial_state, initial_job, initial_idle, directive_tx),
        );
        match initial_state {
            ExternalState::Empty => self.empty_tasks.push_back(id),
            ExternalState::Idle => self.idle_tasks.push_back(id),
            _ => {}
        }
        Ok(RegisteredTask {
            id,
            job_id: initial_job,
            idle_id: initial_idle.map(|r| r.idle_id),
            directive_rx,
        })
    }

    /// Registers a `Connecting` task waiting for a `PooledTransport` via `oneshot`.
    pub fn register_transport_waiter(
        &mut self,
        waiter: TaskId,
        job_id: JobId,
        reply: oneshot::Sender<Result<PooledTransport, TaskError>>,
    ) {
        if !self.phase.accepts_work() {
            let _ = reply.send(Err(TaskError::RejectedByCoordinator(self.phase)));
            return;
        }
        if self.tasks.get(&waiter).and_then(|e| e.current_job) != Some(job_id) {
            let _ = reply.send(Err(TaskError::StageFailed(
                "stale transport request".into(),
            )));
            return;
        }
        self.waiting_transports.push_back(TransportWaiter {
            task_id: waiter,
            job_id,
            reply,
        });
    }

    /// Sends an asynchronous directive to `task_id`. Returns `true` only if queued onto a live channel.
    pub fn send_directive(&self, task_id: TaskId, directive: TaskDirective<I>) -> bool {
        self.tasks
            .get(&task_id)
            .is_some_and(|entry| entry.send_directive(directive).is_ok())
    }

    /// Pops the freshest `Idle` task and sends `ExtractTransport` directly to it (zero blocking).
    pub fn extract_idle_transport(
        &mut self,
        mut reply: oneshot::Sender<Option<PooledTransport>>,
    ) -> bool {
        while let Some(id) = self.idle_tasks.pop_back() {
            let Some(entry) = self.tasks.get(&id) else {
                continue;
            };
            if entry.state != ExternalState::Idle {
                continue;
            }
            let Some(rec) = entry.current_idle else {
                continue;
            };
            match entry.send_directive(TaskDirective::ExtractTransport {
                expected_idle_id: rec.idle_id,
                reply,
            }) {
                Ok(()) => return true,
                Err(TaskDirective::ExtractTransport {
                    reply: recovered, ..
                }) => {
                    reply = recovered;
                }
                Err(_) => unreachable!("only ExtractTransport is sent here"),
            }
        }
        let _ = reply.send(None);
        false
    }

    /// Handles an `Idle` task clearing/losing its transport and returning to `Empty`.
    ///
    /// If `dead_session` is `Some(sid)`, automatically sweeps all remaining `Idle` sibling tasks
    /// sharing that dead underlying transport session.
    pub fn on_idle_cleared(
        &mut self,
        task_id: TaskId,
        idle_id: IdleId,
        dead_session: Option<u64>,
    ) -> bool {
        let Some(entry) = self.tasks.get_mut(&task_id) else {
            return false;
        };
        if entry.current_idle.map(|r| r.idle_id) != Some(idle_id) {
            return false;
        }
        entry.current_idle = None;
        self.idle_tasks.retain(|&id| id != task_id);
        if dead_session.is_some() {
            self.total_idle_lost += 1;
        }

        let recycled = if self.phase.accepts_work() {
            entry.state = ExternalState::Empty;
            self.empty_tasks.push_back(task_id);
            self.resolve_waiters_on_recycle(task_id);
            true
        } else {
            entry.state = ExternalState::Closing;
            let _ = entry.send_directive(TaskDirective::Shutdown);
            self.resolve_waiters_for(task_id, ExternalState::Closing);
            false
        };

        if let Some(sid) = dead_session {
            self.sweep_idle_by_session(sid);
        }
        recycled
    }

    /// Sends async `CloseTransport` sweep directives to all `Idle` tasks matching `session_id`.
    pub fn sweep_idle_by_session(&mut self, session_id: u64) -> usize {
        self.sweep_idle_matching(|rec| rec.session_id == Some(session_id))
    }

    /// Sends async `CloseTransport` sweep directives to all `Idle` tasks with `generation < min_gen`
    /// and raises the generation floor so older in-flight deliveries are dropped.
    pub fn sweep_idle_before_generation(&mut self, min_generation: u64) -> usize {
        self.min_idle_generation = self.min_idle_generation.max(min_generation);
        self.sweep_idle_matching(|rec| rec.generation < min_generation)
    }

    /// Sends async `CloseTransport` sweep directives to all currently `Idle` tasks.
    pub fn sweep_all_idle(&mut self) -> usize {
        self.sweep_idle_matching(|_| true)
    }

    fn sweep_idle_matching<F>(&mut self, mut predicate: F) -> usize
    where
        F: FnMut(IdleRecord) -> bool,
    {
        let mut swept = 0usize;
        let mut remaining = VecDeque::with_capacity(self.idle_tasks.len());
        while let Some(id) = self.idle_tasks.pop_front() {
            if let Some(entry) = self.tasks.get(&id)
                && entry.state == ExternalState::Idle
                && let Some(rec) = entry.current_idle
                && predicate(rec)
            {
                let _ = entry.send_directive(TaskDirective::CloseTransport {
                    expected_idle_id: Some(rec.idle_id),
                });
                swept += 1;
            } else {
                remaining.push_back(id);
            }
        }
        self.idle_tasks = remaining;
        swept
    }

    /// Applies an asynchronous state notification reported by `task_id` for `job_id`.
    pub fn apply_task_state(&mut self, task_id: TaskId, job_id: JobId, next: ExternalState) {
        let Some(entry) = self.tasks.get_mut(&task_id) else {
            return;
        };
        if entry.current_job != Some(job_id) {
            return;
        }
        if !entry.state.can_advance_to(next) || entry.state == next {
            return;
        }
        entry.state = next;
        self.resolve_waiters_for(task_id, next);
    }

    /// Handles a task's synchronous self-recycle notification (`Closing`/`Connecting` -> `Empty`).
    pub fn on_task_recycled(&mut self, task_id: TaskId, job_id: JobId) -> bool {
        let Some(entry) = self.tasks.get_mut(&task_id) else {
            return false;
        };
        if entry.current_job != Some(job_id) {
            return false;
        }
        entry.current_job = None;

        let accepted_idle = if self.phase.accepts_work() {
            entry.state = ExternalState::Empty;
            self.empty_tasks.push_back(task_id);
            true
        } else {
            entry.state = ExternalState::Closing;
            let _ = entry.send_directive(TaskDirective::Shutdown);
            false
        };

        self.drop_waiters_owned_by_job(task_id, job_id);
        self.waiting_transports
            .retain(|w| !(w.task_id == task_id && w.job_id == job_id));

        if accepted_idle {
            self.resolve_waiters_on_recycle(task_id);
        } else {
            self.resolve_waiters_for(task_id, ExternalState::Closing);
        }
        accepted_idle
    }

    /// Marks `task_id` fully exited (`Closed`), prunes queues, clears tracking,
    /// and returns its last external state and unfinished `JobId` (if any).
    pub fn complete_task(&mut self, task_id: TaskId) -> Option<(ExternalState, Option<JobId>)> {
        let removed = self.tasks.remove(&task_id)?;
        self.empty_tasks.retain(|&id| id != task_id);
        self.idle_tasks.retain(|&id| id != task_id);
        self.waiting_transports.retain(|w| w.task_id != task_id);
        self.resolve_waiters_for(task_id, ExternalState::Closed);
        self.drop_waiters_owned_by_task(task_id);
        if self.tasks.is_empty()
            && matches!(
                self.phase,
                TunnelPhase::Draining | TunnelPhase::ForceStopping
            )
        {
            self.phase = TunnelPhase::Stopped;
        }
        Some((removed.state, removed.current_job))
    }

    /// Registers an inter-task state dependency (`waiter`/`waiter_job` waits for `target`
    /// to reach `desired`).
    pub fn register_waiter(
        &mut self,
        waiter: TaskId,
        waiter_job: JobId,
        target: TaskId,
        desired: ExternalState,
        reply: oneshot::Sender<WaitResult>,
    ) {
        if !self.phase.accepts_work() || self.waiters.len() >= MAX_PENDING_WAITERS {
            let _ = reply.send(WaitResult::Cancelled);
            return;
        }
        if self.tasks.get(&waiter).and_then(|e| e.current_job) != Some(waiter_job) {
            let _ = reply.send(WaitResult::Cancelled);
            return;
        }
        if waiter == target {
            let _ = reply.send(WaitResult::CycleDetected);
            return;
        }
        let Some(target_entry) = self.tasks.get(&target) else {
            let _ = reply.send(WaitResult::TargetNotFound);
            return;
        };
        if target_entry.state == desired {
            let _ = reply.send(WaitResult::Reached);
            return;
        }
        if !target_entry.state.can_reach(desired) {
            let _ = reply.send(WaitResult::TargetClosed(target_entry.state));
            return;
        }
        if self.would_create_cycle(waiter, target) {
            let _ = reply.send(WaitResult::CycleDetected);
            return;
        }
        self.waiters.push(StateWaiter {
            waiter,
            waiter_job,
            target,
            desired,
            reply,
        });
    }

    /// Transitions the tunnel into `Draining` (or `Stopped` immediately if no tasks remain):
    /// `Empty` and `Idle` tasks receive `Shutdown` while `Connecting`/`Active` tasks receive `Drain`.
    pub fn begin_graceful_stop(&mut self) -> TunnelPhase {
        if self.phase == TunnelPhase::Running {
            self.cancel_all_waiters();
            self.empty_tasks.clear();
            self.idle_tasks.clear();
            if self.tasks.is_empty() {
                self.phase = TunnelPhase::Stopped;
            } else {
                self.phase = TunnelPhase::Draining;
                for entry in self.tasks.values_mut() {
                    if matches!(entry.state, ExternalState::Empty | ExternalState::Idle) {
                        entry.state = ExternalState::Closing;
                        entry.current_idle = None;
                        let _ = entry.send_directive(TaskDirective::Shutdown);
                    } else {
                        let _ = entry.send_directive(TaskDirective::Drain);
                    }
                }
            }
        }
        self.phase
    }

    /// Transitions the tunnel into `ForceStopping` (or `Stopped` immediately if no tasks remain).
    pub fn begin_force_stop(&mut self) -> TunnelPhase {
        if matches!(self.phase, TunnelPhase::Running | TunnelPhase::Draining) {
            self.cancel_all_waiters();
            self.empty_tasks.clear();
            self.idle_tasks.clear();
            self.phase = if self.tasks.is_empty() {
                TunnelPhase::Stopped
            } else {
                TunnelPhase::ForceStopping
            };
        }
        self.phase
    }

    /// Marks the engine fully stopped once all task handles have drained.
    pub fn finish_stop(&mut self) {
        self.cancel_all_waiters();
        self.empty_tasks.clear();
        self.idle_tasks.clear();
        self.phase = TunnelPhase::Stopped;
    }

    fn purge_closed_transport_waiters(&mut self) {
        let tasks = &self.tasks;
        self.waiting_transports.retain(|w| {
            !w.reply.is_closed()
                && tasks.get(&w.task_id).and_then(|e| e.current_job) == Some(w.job_id)
        });
    }

    fn would_create_cycle(&self, waiter: TaskId, target: TaskId) -> bool {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(target);
        while let Some(node) = queue.pop_front() {
            if node == waiter {
                return true;
            }
            if !visited.insert(node) {
                continue;
            }
            for edge in &self.waiters {
                if edge.waiter == node {
                    queue.push_back(edge.target);
                }
            }
        }
        false
    }

    fn resolve_waiters_for(&mut self, target: TaskId, new_state: ExternalState) {
        let mut i = 0;
        while i < self.waiters.len() {
            if self.waiters[i].target != target {
                i += 1;
                continue;
            }
            let desired = self.waiters[i].desired;
            if new_state == desired {
                let w = self.waiters.swap_remove(i);
                let _ = w.reply.send(WaitResult::Reached);
            } else if !new_state.can_reach(desired) {
                let w = self.waiters.swap_remove(i);
                let _ = w.reply.send(WaitResult::TargetClosed(new_state));
            } else {
                i += 1;
            }
        }
    }

    fn resolve_waiters_on_recycle(&mut self, target: TaskId) {
        let mut i = 0;
        while i < self.waiters.len() {
            if self.waiters[i].target != target {
                i += 1;
                continue;
            }
            match self.waiters[i].desired {
                ExternalState::Empty => {
                    let w = self.waiters.swap_remove(i);
                    let _ = w.reply.send(WaitResult::Reached);
                }
                ExternalState::Closed => {
                    i += 1;
                }
                ExternalState::Idle
                | ExternalState::Connecting
                | ExternalState::Active
                | ExternalState::Closing => {
                    let w = self.waiters.swap_remove(i);
                    let _ = w.reply.send(WaitResult::TargetClosed(ExternalState::Empty));
                }
            }
        }
    }

    fn drop_waiters_owned_by_task(&mut self, waiter: TaskId) {
        let mut i = 0;
        while i < self.waiters.len() {
            if self.waiters[i].waiter == waiter {
                let w = self.waiters.swap_remove(i);
                let _ = w.reply.send(WaitResult::Cancelled);
            } else {
                i += 1;
            }
        }
    }

    fn drop_waiters_owned_by_job(&mut self, waiter: TaskId, waiter_job: JobId) {
        let mut i = 0;
        while i < self.waiters.len() {
            if self.waiters[i].waiter == waiter && self.waiters[i].waiter_job == waiter_job {
                let w = self.waiters.swap_remove(i);
                let _ = w.reply.send(WaitResult::Cancelled);
            } else {
                i += 1;
            }
        }
    }

    fn cancel_all_waiters(&mut self) {
        for w in self.waiters.drain(..) {
            let _ = w.reply.send(WaitResult::Cancelled);
        }
        for w in self.waiting_transports.drain(..) {
            let _ = w
                .reply
                .send(Err(TaskError::RejectedByCoordinator(self.phase)));
        }
    }
}

impl<I> Default for StateEngine<I> {
    fn default() -> Self {
        Self::new()
    }
}
