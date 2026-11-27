// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Pure single-writer state machine engine owned by the Coordinator.
//! All comments and docstrings must be in English.

use crate::v2::error::TaskError;
use crate::v2::state::{ExternalState, TaskDirective, TaskId, TunnelPhase, WaitResult};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::{mpsc, oneshot};

/// Coordinator-owned record for one live `IoTask`.
struct TaskEntry<I> {
    state: ExternalState,
    directive_tx: mpsc::UnboundedSender<TaskDirective<I>>,
}

impl<I> TaskEntry<I> {
    fn new(state: ExternalState, directive_tx: mpsc::UnboundedSender<TaskDirective<I>>) -> Self {
        Self {
            state,
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

/// Pure synchronous state engine for Tunnel phases, Task states, idle recycling, and waits.
///
/// Designed for zero-blocking single-writer execution inside the Coordinator Microtask
/// handler and deterministic unit testing without Tokio or network I/O.
pub struct StateEngine<I> {
    phase: TunnelPhase,
    next_task_seq: u64,
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
            tasks: HashMap::new(),
            idle_tasks: VecDeque::new(),
            waiters: Vec::new(),
        }
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

    /// Pops an idle `Empty` task if available and transitions it to `Connecting`.
    pub fn pop_idle_for_connect(&mut self) -> Result<Option<TaskId>, TaskError> {
        if !self.phase.accepts_work() {
            return Err(TaskError::RejectedByCoordinator(self.phase));
        }
        while let Some(id) = self.idle_tasks.pop_front() {
            if let Some(entry) = self.tasks.get_mut(&id)
                && entry.state == ExternalState::Empty
            {
                entry.state = ExternalState::Connecting;
                return Ok(Some(id));
            }
        }
        Ok(None)
    }

    /// Registers a newly spawned `IoTask` in the requested initial state (`Empty` or `Connecting`).
    pub fn register_task(
        &mut self,
        initial_state: ExternalState,
    ) -> Result<(TaskId, mpsc::UnboundedReceiver<TaskDirective<I>>), TaskError> {
        if !self.phase.accepts_work() {
            return Err(TaskError::RejectedByCoordinator(self.phase));
        }
        let id = TaskId::new(self.next_task_seq);
        self.next_task_seq = self.next_task_seq.saturating_add(1);
        let (directive_tx, directive_rx) = mpsc::unbounded_channel();
        self.tasks
            .insert(id, TaskEntry::new(initial_state, directive_tx));
        if initial_state == ExternalState::Empty {
            self.idle_tasks.push_back(id);
        }
        Ok((id, directive_rx))
    }

    /// Assigns `input` to a newly registered `Connecting` task.
    pub fn dispatch_to_task(&self, task_id: TaskId, input: I) {
        if let Some(entry) = self.tasks.get(&task_id) {
            entry.send_directive(TaskDirective::Assign(input));
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

    /// Applies an asynchronous state notification reported by a task (`Active` or `Closing`).
    pub fn apply_task_state(&mut self, task_id: TaskId, next: ExternalState) {
        let Some(entry) = self.tasks.get_mut(&task_id) else {
            return;
        };
        if !entry.state.can_reach(next) || entry.state == next {
            return;
        }
        entry.state = next;
        self.resolve_waiters_for(task_id, next);
    }

    /// Handles a task's synchronous self-recycle notification (`Closing`/`Connecting` -> `Empty`).
    ///
    /// If the tunnel is `Running`, returns the task to `idle_tasks` for reuse. Otherwise
    /// instructs the task to `Shutdown` cleanly so the `JoinSet` can drain.
    pub fn on_task_recycled(&mut self, task_id: TaskId) -> bool {
        self.resolve_waiters_for(task_id, ExternalState::Closed);
        self.drop_waiters_owned_by(task_id);

        let Some(entry) = self.tasks.get_mut(&task_id) else {
            return false;
        };

        if self.phase.accepts_work() {
            entry.state = ExternalState::Empty;
            self.idle_tasks.push_back(task_id);
            true
        } else {
            entry.state = ExternalState::Closing;
            entry.send_directive(TaskDirective::Shutdown);
            false
        }
    }

    /// Marks `task_id` fully exited (`Closed`) and removes it from tracking.
    pub fn complete_task(&mut self, task_id: TaskId) -> Option<ExternalState> {
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
        Some(removed.state)
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
        if !self.phase.accepts_work() {
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
