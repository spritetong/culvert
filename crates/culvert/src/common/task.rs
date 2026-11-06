// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Asynchronous task tracker and graceful lifecycle coordinator.
//! All comments and docstrings must be in English.

use std::future::Future;
use std::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::consts::DEFAULT_GRACE_PERIOD;

/// Process-wide absolute deadline for the current graceful shutdown.
///
/// Nested trackers each call `shutdown_and_wait()`; deriving a fresh
/// `Instant::now() + 5s` in each one compounded into a 20s+ shutdown. The first
/// tracker to shut down fixes the instant and every other tracker reuses it.
/// An already-expired deadline opens a new window, so unit tests that create
/// several independent runtimes in one process remain independent.
static SHUTDOWN_DEADLINE: Mutex<Option<Instant>> = Mutex::new(None);

/// Resets the shared deadline at daemon startup.
pub fn reset_shutdown_deadline() {
    *SHUTDOWN_DEADLINE.lock().unwrap_or_else(|e| e.into_inner()) = None;
}

/// Returns the single absolute deadline shared by all trackers in this process.
fn shared_shutdown_deadline() -> Instant {
    let mut guard = SHUTDOWN_DEADLINE.lock().unwrap_or_else(|e| e.into_inner());
    let now = Instant::now();
    match *guard {
        Some(deadline) if deadline > now => deadline,
        _ => {
            let deadline = now + DEFAULT_GRACE_PERIOD;
            *guard = Some(deadline);
            deadline
        }
    }
}

/// Manages a set of async tasks to ensure full lifecycle supervision and safe shutdown.
///
/// Termination contract (the behaviour boundary, defined here once):
/// every tracked task observes the tracker's cancellation token, and every
/// await inside it is either cancellation-aware or bounded by a timeout.
/// Shutdown is therefore simply `cancel` followed by awaiting the JoinSet to
/// empty. `abort_all()` is only the deadline fallback, and even then every
/// handle is still awaited. A task must never be detached: a task that cannot
/// terminate has broken this contract and must be fixed at its source.
pub struct TaskTracker<T: 'static> {
    join_set: JoinSet<T>,
    cancel_token: CancellationToken,
}

impl<T: 'static> TaskTracker<T> {
    /// Creates a new `TaskTracker` bound to a root cancellation token.
    pub fn new(cancel_token: CancellationToken) -> Self {
        Self {
            join_set: JoinSet::new(),
            cancel_token,
        }
    }

    /// Spawns a new task and tracks its `JoinHandle`.
    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = T> + Send + 'static,
        T: Send,
    {
        self.join_set.spawn(future);
    }

    /// Spawns a task that observes a child cancellation token.
    pub fn spawn_with_cancel<F, Fut>(&mut self, factory: F)
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
        T: Send,
    {
        let child_token = self.cancel_token.child_token();
        self.join_set.spawn(factory(child_token));
    }

    /// Returns the cancellation token associated with this tracker.
    pub fn token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Returns the current number of tracked running tasks.
    pub fn len(&self) -> usize {
        self.join_set.len()
    }

    /// Checks if all tasks have finished.
    pub fn is_empty(&self) -> bool {
        self.join_set.is_empty()
    }

    /// Waits for the next task to complete.
    pub async fn join_next(&mut self) -> Option<Result<T, tokio::task::JoinError>> {
        self.join_set.join_next().await
    }

    /// Signals all tasks to stop via the cancellation token and awaits their orderly completion.
    /// If graceful shutdown exceeds the 5-second deadline, remaining tasks are aborted.
    pub async fn shutdown_and_wait(&mut self) {
        self.shutdown_and_wait_until(shared_shutdown_deadline())
            .await;
    }

    /// Signals cancellation and awaits completion until a specified instant deadline.
    /// This prevents nested trackers from compounding multiple 5s delays.
    pub async fn shutdown_and_wait_until(&mut self, deadline: tokio::time::Instant) {
        info!("Initiating graceful task shutdown via cancellation token...");
        self.cancel_token.cancel();

        let sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                res = self.join_set.join_next() => match res {
                    Some(Ok(_)) => {},
                    Some(Err(e)) => {
                        error!("Task panicked during graceful shutdown: {:?}", e);
                    }
                    None => break,
                },
                _ = &mut sleep => {
                    tracing::warn!("Graceful shutdown deadline reached, aborting remaining tasks");
                    self.join_set.abort_all();
                    // Abort is a forced termination, not permission to leak the
                    // handles. Every aborted task is still awaited here.
                    while self.join_set.join_next().await.is_some() {}
                    break;
                }
            }
        }
        info!("All tracked tasks completed cleanly.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn nested_trackers_share_one_absolute_deadline() {
        reset_shutdown_deadline();
        let first = shared_shutdown_deadline();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let second = shared_shutdown_deadline();
        assert_eq!(
            first, second,
            "nested trackers must not restart the grace window"
        );
        reset_shutdown_deadline();
    }

    #[tokio::test]
    async fn shutdown_awaits_aborted_tasks_after_deadline() {
        let cancel = CancellationToken::new();
        let mut tracker = TaskTracker::<()>::new(cancel);
        tracker.spawn(async {
            std::future::pending::<()>().await;
        });

        let deadline = tokio::time::Instant::now() + Duration::from_millis(10);
        let finished = tokio::time::timeout(
            Duration::from_secs(1),
            tracker.shutdown_and_wait_until(deadline),
        )
        .await;
        assert!(
            finished.is_ok(),
            "shutdown chain must not block on a stuck task after the deadline"
        );
        assert!(
            tracker.is_empty(),
            "every aborted handle must be awaited, never detached"
        );
    }
}
