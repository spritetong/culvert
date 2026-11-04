//! Asynchronous task tracker and graceful lifecycle coordinator.
//! All comments and docstrings must be in English.

use std::future::Future;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Manages a set of async tasks to ensure full lifecycle supervision and safe shutdown.
/// Tasks should always observe cooperative cancellation. Calling `abort()` directly on
/// individual task handles during normal lifecycle is prohibited; `abort_all()` is reserved
/// strictly as an emergency last-resort fallback when graceful cancellation exceeds the deadline.
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
        self.shutdown_and_wait_until(
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
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
                    while self.join_set.join_next().await.is_some() {}
                    break;
                }
            }
        }
        info!("All tracked tasks completed cleanly.");
    }
}
