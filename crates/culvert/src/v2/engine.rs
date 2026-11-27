// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Pure single-writer state machine engine owned by the Coordinator.
//! All comments and docstrings must be in English.

use crate::v2::error::TaskError;
use crate::v2::state::{ExternalState, JobId, TaskDirective, TaskId, TunnelPhase, WaitResult};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::{mpsc, oneshot};

/// Maximum number of concurrent inter-task state waiters tracked by `StateEngine`.
const MAX_PENDING_WAITERS: usize = 4096;

/// Coordinator-owned record for one live `IoTask`.
struct TaskEntry<I> {
    state: ExternalState,
    current_job: Option<JobId>,
    directive_tx: mpsc::UnboundedSender<TaskDirective<I>>,
}

impl<I> TaskEntry<I> {
    fn new(
        state: ExternalState,
        current_job: Option<JobId>,
        directive_tx: mpsc::UnboundedSender<TaskDirective<I>>,
    ) -> Self {
        Self {
            state,
            current_job,
            directive_tx,
        }
    }

    fn send_directive(&self, directive: TaskDirective<I>) {
        let _ = self.directive_tx.send(directive);
    }
}

/// Pending inter-task state dependency mediated by the Coordinator.
struct StateWaiter {
    waiter: TaskId,
    target: TaskId,
    desired: ExternalState,
    reply: oneshot::Sender<WaitResult>,
}

/// Registration handle returned when a new `IoTask` is inserted into `StateEngine`.
pub struct RegisteredTask<I> {
    pub id: TaskId,
    pub job_id: Option<JobId>,
    pub directive_rx: mpsc::UnboundedReceiver<TaskDirective<I>>,
}

/// Pure synchronous state engine for Tunnel phases, Task states, idle recycling, and waits.
///
/// Designed for zero-blocking single-writer execution inside the Coordinator Microtask
/// handler and deterministic unit testing without Tokio or network I/O.
pub struct StateEngine<I> {
    phase: TunnelPhase,
    next_task_seq: u64,
    next_job_seq: u64,
    tasks: HashMap<TaskId, TaskEntry<I>>,
    idle_tasks: VecDeque<TaskId>,
    waiters: Vec<StateWaiter>,
}

impl<I> StateEngine<I> {
    /// Creates a new `StateEngine` in the `Running` phase.
    pub fn new() -> Self {
        Self {
            phase: TunnelPhase::Running,
            next_task_seq: 1,
            next_job_seq: 1,
            tasks: HashMap::new(),
            idle_tasks: VecDeque::new(),
            waiters: Vec::new(),
        }
    }

    fn alloc_job_id(&mut self) -> JobId {
        let id = JobId::new(self.next_job_seq);
        self.next_job_seq = self.next_job_seq.saturating_add(1);
        id
    }

    /// Returns the current tunnel phase.
    #[inline]
    pub fn phase(&self) -> TunnelPhase {
        self.phase
    }

    /// Returns the number of tracked live tasks (including `Empty` tasks).
    #[inline]
    pub fn live_task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Returns the number of currently idle `Empty` tasks available for reuse.
    #[inline]
    pub fn idle_task_count(&self) -> usize {
        self.idle_tasks.len()
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

    /// Pops an idle `Empty` task if available, transitions it to `Connecting`, and
    /// allocates a fresh `JobId`.
    pub fn pop_idle_for_connect(&mut self) -> Result<Option<(TaskId, JobId)>, TaskError> {
        if !self.phase.accepts_work() {
            return Err(TaskError::RejectedByCoordinator(self.phase));
        }
        while let Some(id) = self.idle_tasks.pop_front() {
            let is_idle = self
                .tasks
                .get(&id)
                .is_some_and(|entry| entry.state == ExternalState::Empty);
            if is_idle {
                let job_id = self.alloc_job_id();
                if let Some(entry) = self.tasks.get_mut(&id) {
                    entry.state = ExternalState::Connecting;
                    entry.current_job = Some(job_id);
                }
                self.resolve_waiters_for(id, ExternalState::Connecting);
                return Ok(Some((id, job_id)));
            }
        }
        Ok(None)
    }

    /// Registers a newly spawned `IoTask` in either `Empty` (`job_id == None`) or
    /// `Connecting` (`job_id == Some(JobId)`) initial state.
    pub fn register_task(
        &mut self,
        initial_state: ExternalState,
    ) -> Result<RegisteredTask<I>, TaskError> {
        if !self.phase.accepts_work() {
            return Err(TaskError::RejectedByCoordinator(self.phase));
        }
        let id = TaskId::new(self.next_task_seq);
        self.next_task_seq = self.next_task_seq.saturating_add(1);
        let initial_job = if initial_state == ExternalState::Empty {
            None
        } else {
            Some(self.alloc_job_id())
        };
        let (directive_tx, directive_rx) = mpsc::unbounded_channel();
        self.tasks
            .insert(id, TaskEntry::new(initial_state, initial_job, directive_tx));
        if initial_state == ExternalState::Empty {
            self.idle_tasks.push_back(id);
        }
        Ok(RegisteredTask {
            id,
            job_id: initial_job,
            directive_rx,
        })
    }

    /// Dispatches `Assign { job_id, input }` to an idle task popped via `pop_idle_for_connect`.
    pub fn dispatch_to_task(&self, task_id: TaskId, job_id: JobId, input: I) {
        if let Some(entry) = self.tasks.get(&task_id) {
            entry.send_directive(TaskDirective::Assign { job_id, input });
        }
    }

    /// Sends an asynchronous directive (`Drain` or `Shutdown`) to a specific task.
    pub fn send_directive(&self, task_id: TaskId, directive: TaskDirective<I>) -> bool {
        if let Some(entry) = self.tasks.get(&task_id) {
            entry.send_directive(directive);
            true
        } else {
            false
        }
    }

    /// Applies an asynchronous state notification reported by `task_id` for `job_id`.
    ///
    /// Stale notifications whose `job_id` does not match the entry's active job are ignored.
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
    ///
    /// Strictly distinguishes task recycling (`* -> Empty`) from task exit (`* -> Closed`):
    /// - Waiters targeting `Empty` receive `WaitResult::Reached`.
    /// - Waiters targeting `Closed` stay pending until `complete_task` runs.
    /// - Waiters targeting earlier job stages (`Connecting`/`Active`/`Closing`) receive
    ///   `WaitResult::TargetClosed(Empty)` so they never stall across job generations.
    pub fn on_task_recycled(&mut self, task_id: TaskId, job_id: JobId) -> bool {
        self.drop_waiters_owned_by(task_id);

        let Some(entry) = self.tasks.get_mut(&task_id) else {
            return false;
        };
        if entry.current_job != Some(job_id) {
            return false;
        }
        entry.current_job = None;

        if self.phase.accepts_work() {
            entry.state = ExternalState::Empty;
            self.idle_tasks.push_back(task_id);
            self.resolve_waiters_on_recycle(task_id);
            true
        } else {
            entry.state = ExternalState::Closing;
            entry.send_directive(TaskDirective::Shutdown);
            self.resolve_waiters_for(task_id, ExternalState::Closing);
            false
        }
    }

    /// Marks `task_id` fully exited (`Closed`), clears tracking, and returns its last
    /// external state and unfinished `JobId` (if any).
    pub fn complete_task(&mut self, task_id: TaskId) -> Option<(ExternalState, Option<JobId>)> {
        let removed = self.tasks.remove(&task_id)?;
        self.resolve_waiters_for(task_id, ExternalState::Closed);
        self.drop_waiters_owned_by(task_id);
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

    /// Registers an inter-task state dependency (`waiter` waits for `target` to reach `desired`).
    ///
    /// Performs synchronous cycle detection on the waiter graph so task-to-task waits
    /// can never deadlock.
    pub fn register_waiter(
        &mut self,
        waiter: TaskId,
        target: TaskId,
        desired: ExternalState,
        reply: oneshot::Sender<WaitResult>,
    ) {
        if !self.phase.accepts_work() || self.waiters.len() >= MAX_PENDING_WAITERS {
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
            target,
            desired,
            reply,
        });
    }

    /// Transitions the tunnel into `Draining` (or `Stopped` immediately if no tasks remain):
    /// `Empty` tasks receive `Shutdown` while active/connecting tasks receive `Drain`.
    pub fn begin_graceful_stop(&mut self) -> TunnelPhase {
        if self.phase == TunnelPhase::Running {
            self.cancel_all_waiters();
            self.idle_tasks.clear();
            if self.tasks.is_empty() {
                self.phase = TunnelPhase::Stopped;
            } else {
                self.phase = TunnelPhase::Draining;
                for entry in self.tasks.values_mut() {
                    if entry.state == ExternalState::Empty {
                        entry.state = ExternalState::Closing;
                        entry.send_directive(TaskDirective::Shutdown);
                    } else {
                        entry.send_directive(TaskDirective::Drain);
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
        self.idle_tasks.clear();
        self.phase = TunnelPhase::Stopped;
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
                ExternalState::Connecting | ExternalState::Active | ExternalState::Closing => {
                    let w = self.waiters.swap_remove(i);
                    let _ = w.reply.send(WaitResult::TargetClosed(ExternalState::Empty));
                }
            }
        }
    }

    fn drop_waiters_owned_by(&mut self, waiter: TaskId) {
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

    fn cancel_all_waiters(&mut self) {
        for w in self.waiters.drain(..) {
            let _ = w.reply.send(WaitResult::Cancelled);
        }
    }
}

impl<I> Default for StateEngine<I> {
    fn default() -> Self {
        Self::new()
    }
}
