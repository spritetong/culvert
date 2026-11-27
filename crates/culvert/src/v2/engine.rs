// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Pure single-writer state machine engine owned by the Coordinator.
//! All comments and docstrings must be in English.

use crate::v2::error::TaskError;
use crate::v2::state::{ExternalState, TaskDirective, TaskId, TunnelPhase, WaitResult};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::{oneshot, watch};

/// Coordinator-owned record for one live task.
struct TaskEntry {
    state: ExternalState,
    directive_tx: watch::Sender<TaskDirective>,
}

impl TaskEntry {
    fn new(state: ExternalState, directive_tx: watch::Sender<TaskDirective>) -> Self {
        Self {
            state,
            directive_tx,
        }
    }

    fn send_directive(&self, directive: TaskDirective) {
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

/// Pure, synchronous state engine for Tunnel phases, Task external states, and inter-task waits.
///
/// Designed so all state-transition invariants and cycle checks can be verified
/// deterministically without network I/O or a Tokio runtime.
pub struct StateEngine {
    phase: TunnelPhase,
    next_task_seq: u64,
    tasks: HashMap<TaskId, TaskEntry>,
    waiters: Vec<StateWaiter>,
}

impl StateEngine {
    /// Creates a new `StateEngine` in the `Running` phase.
    pub fn new() -> Self {
        Self {
            phase: TunnelPhase::Running,
            next_task_seq: 1,
            tasks: HashMap::new(),
            waiters: Vec::new(),
        }
    }

    /// Returns the current tunnel phase.
    #[inline]
    pub fn phase(&self) -> TunnelPhase {
        self.phase
    }

    /// Returns the number of tracked active (non-closed) tasks.
    #[inline]
    pub fn active_task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Queries the current external state of `task_id`, if still active.
    #[inline]
    pub fn task_state(&self, task_id: TaskId) -> Option<ExternalState> {
        self.tasks.get(&task_id).map(|entry| entry.state)
    }

    /// Allocates a new `TaskId` and registers the task in `ExternalState::Connecting`.
    ///
    /// Fails if the tunnel is no longer in `TunnelPhase::Running`.
    pub fn register_task(&mut self) -> Result<(TaskId, watch::Receiver<TaskDirective>), TaskError> {
        if !self.phase.accepts_active_work() {
            return Err(TaskError::RejectedByCoordinator(self.phase));
        }
        let id = TaskId::new(self.next_task_seq);
        self.next_task_seq = self.next_task_seq.saturating_add(1);
        let (directive_tx, directive_rx) = watch::channel(TaskDirective::Run);
        self.tasks
            .insert(id, TaskEntry::new(ExternalState::Connecting, directive_tx));
        Ok((id, directive_rx))
    }

    /// Attempts to transition `task_id` from `expected_from` to `next`.
    ///
    /// Returns the committed `ExternalState` on success, or an error if the guard fails.
    pub fn transition_task(
        &mut self,
        task_id: TaskId,
        expected_from: ExternalState,
        next: ExternalState,
    ) -> Result<ExternalState, TaskError> {
        let Some(entry) = self.tasks.get_mut(&task_id) else {
            return Err(TaskError::InvalidTransition {
                task_id,
                from: expected_from,
                to: next,
            });
        };

        let current = entry.state;
        match next {
            ExternalState::Active => {
                if !self.phase.accepts_active_work() {
                    return Err(TaskError::RejectedByCoordinator(self.phase));
                }
                if current != ExternalState::Connecting
                    || expected_from != ExternalState::Connecting
                {
                    return Err(TaskError::InvalidTransition {
                        task_id,
                        from: current,
                        to: next,
                    });
                }
                entry.state = ExternalState::Active;
                self.resolve_waiters_for(task_id, ExternalState::Active);
                Ok(ExternalState::Active)
            }
            ExternalState::Closing => {
                // Convergent idempotency: if Coordinator already initiated Draining while an
                // Active task was simultaneously finishing, keep Closing without error.
                if current == ExternalState::Closing && expected_from == ExternalState::Active {
                    return Ok(ExternalState::Closing);
                }
                if current != expected_from
                    || !matches!(current, ExternalState::Connecting | ExternalState::Active)
                {
                    return Err(TaskError::InvalidTransition {
                        task_id,
                        from: current,
                        to: next,
                    });
                }
                entry.state = ExternalState::Closing;
                self.resolve_waiters_for(task_id, ExternalState::Closing);
                Ok(ExternalState::Closing)
            }
            ExternalState::Connecting | ExternalState::Closed => {
                Err(TaskError::InvalidTransition {
                    task_id,
                    from: current,
                    to: next,
                })
            }
        }
    }

    /// Marks `task_id` as `Closed`, removes it from active tracking, and resolves all
    /// waiters targeting or owned by this task.
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
        if !self.phase.accepts_active_work() {
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

    /// Transitions the tunnel into `Draining` (or `Stopped` immediately if no tasks remain)
    /// and broadcasts `TaskDirective::EnterDrain` to all live tasks.
    pub fn begin_graceful_stop(&mut self) -> TunnelPhase {
        match self.phase {
            TunnelPhase::Running => {
                self.cancel_all_waiters();
                if self.tasks.is_empty() {
                    self.phase = TunnelPhase::Stopped;
                } else {
                    self.phase = TunnelPhase::Draining;
                    for entry in self.tasks.values() {
                        entry.send_directive(TaskDirective::EnterDrain);
                    }
                }
            }
            TunnelPhase::Draining | TunnelPhase::ForceStopping | TunnelPhase::Stopped => {}
        }
        self.phase
    }

    /// Transitions the tunnel into `ForceStopping` (or `Stopped` immediately if no tasks remain)
    /// and broadcasts `TaskDirective::ForceStop` to all live tasks.
    pub fn begin_force_stop(&mut self) -> TunnelPhase {
        match self.phase {
            TunnelPhase::Running | TunnelPhase::Draining => {
                self.cancel_all_waiters();
                if self.tasks.is_empty() {
                    self.phase = TunnelPhase::Stopped;
                } else {
                    self.phase = TunnelPhase::ForceStopping;
                    for entry in self.tasks.values() {
                        entry.send_directive(TaskDirective::ForceStop);
                    }
                }
            }
            TunnelPhase::ForceStopping | TunnelPhase::Stopped => {}
        }
        self.phase
    }

    /// Marks the engine fully stopped once all task handles have been drained.
    pub fn finish_stop(&mut self) {
        self.cancel_all_waiters();
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
            let matches_target = self.waiters[i].target == target;
            if !matches_target {
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

impl Default for StateEngine {
    fn default() -> Self {
        Self::new()
    }
}
