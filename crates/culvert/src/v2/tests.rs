// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Minimal tests for two-channel Coordinator, task recycling, and CancellationToken force-stop.
//! All comments and docstrings must be in English.

use crate::v2::{
    ActiveStage, CloseReason, ClosingStage, ConnectingContext, Coordinator, ExternalState,
    StateEngine, TaskDirective, TaskError, TaskId, TaskOutcome, TaskWorkflow, TunnelPhase,
    WaitResult,
};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;

// =============================================================================
// 1. Pure StateEngine Tests (No Tokio / No Network)
// =============================================================================

#[test]
fn state_engine_recycle_and_draining_shutdown() {
    let mut engine = StateEngine::<()>::new();
    let (id, mut rx) = engine.register_task(ExternalState::Empty).unwrap();
    assert_eq!(engine.task_state(id), Some(ExternalState::Empty));
    assert_eq!(engine.idle_task_count(), 1);

    // Pop idle for connect: Empty -> Connecting
    assert_eq!(engine.pop_idle_for_connect().unwrap(), Some(id));
    assert_eq!(engine.idle_task_count(), 0);
    assert_eq!(engine.task_state(id), Some(ExternalState::Connecting));
    engine.dispatch_to_task(id, ());
    assert!(matches!(rx.try_recv(), Ok(TaskDirective::Assign(()))));

    // Connecting -> Active -> Closing -> Recycled back to Empty
    engine.apply_task_state(id, ExternalState::Active);
    assert_eq!(engine.task_state(id), Some(ExternalState::Active));
    engine.apply_task_state(id, ExternalState::Closing);
    assert_eq!(engine.task_state(id), Some(ExternalState::Closing));

    assert!(engine.on_task_recycled(id));
    assert_eq!(engine.task_state(id), Some(ExternalState::Empty));
    assert_eq!(engine.idle_task_count(), 1);

    // Graceful stop sends Shutdown to Empty task
    assert_eq!(engine.begin_graceful_stop(), TunnelPhase::Draining);
    assert!(matches!(rx.try_recv(), Ok(TaskDirective::Shutdown)));
    assert_eq!(engine.idle_task_count(), 0);

    engine.complete_task(id);
    assert_eq!(engine.phase(), TunnelPhase::Stopped);
}

#[test]
fn state_engine_detects_wait_cycles_and_resolves_dependencies() {
    let mut engine = StateEngine::<()>::new();
    let (a, _rx_a) = engine.register_task(ExternalState::Connecting).unwrap();
    let (b, _rx_b) = engine.register_task(ExternalState::Connecting).unwrap();

    // Self-wait is an immediate cycle
    let (tx_self, mut rx_self) = oneshot::channel();
    engine.register_waiter(a, a, ExternalState::Active, tx_self);
    assert_eq!(rx_self.try_recv().unwrap(), WaitResult::CycleDetected);

    // B waits for A -> Active
    let (tx_ba, mut rx_ba) = oneshot::channel();
    engine.register_waiter(b, a, ExternalState::Active, tx_ba);
    assert!(rx_ba.try_recv().is_err());

    // A waiting for B -> Active would create cycle A -> B -> A
    let (tx_ab, mut rx_ab) = oneshot::channel();
    engine.register_waiter(a, b, ExternalState::Active, tx_ab);
    assert_eq!(rx_ab.try_recv().unwrap(), WaitResult::CycleDetected);

    // Transition A -> Active wakes B
    engine.apply_task_state(a, ExternalState::Active);
    assert_eq!(rx_ba.try_recv().unwrap(), WaitResult::Reached);
}

// =============================================================================
// 2. Minimal Mock Workflow for Coordinator + Recyclable IoTask Integration
// =============================================================================

#[derive(Clone)]
struct MockWorkflow {
    closed_jobs: Arc<AtomicUsize>,
}

enum MockInput {
    Immediate {
        wait_for: Option<(TaskId, ExternalState)>,
        on_ready: Option<Arc<Notify>>,
    },
    ForeverPending {
        on_active: Option<Arc<Notify>>,
    },
}

struct MockActive {
    forever_pending: bool,
    on_active: Option<Arc<Notify>>,
    closed_jobs: Arc<AtomicUsize>,
}

struct MockClosing {
    _reason: CloseReason,
    closed_jobs: Arc<AtomicUsize>,
}

impl TaskWorkflow for MockWorkflow {
    type Input = MockInput;
    type Active = MockActive;

    fn connect(
        &self,
        ctx: &mut ConnectingContext<'_>,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Active, TaskError>> + Send {
        let closed_jobs = Arc::clone(&self.closed_jobs);
        async move {
            match input {
                MockInput::Immediate { wait_for, on_ready } => {
                    if let Some((target, desired)) = wait_for {
                        let res = ctx.await_task_state(target, desired).await?;
                        if res != WaitResult::Reached {
                            return Err(TaskError::DependencyFailed(format!("{res:?}")));
                        }
                        if let Some(n) = on_ready {
                            n.notify_one();
                        }
                    }
                    Ok(MockActive {
                        forever_pending: false,
                        on_active: None,
                        closed_jobs,
                    })
                }
                MockInput::ForeverPending { on_active } => Ok(MockActive {
                    forever_pending: true,
                    on_active,
                    closed_jobs,
                }),
            }
        }
    }
}

impl ActiveStage for MockActive {
    type Closing = MockClosing;

    fn poll_drive(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), TaskError>> {
        if let Some(n) = self.on_active.take() {
            n.notify_one();
        }
        if self.forever_pending {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn into_closing(self, reason: CloseReason) -> Self::Closing {
        MockClosing {
            _reason: reason,
            closed_jobs: self.closed_jobs,
        }
    }
}

impl ClosingStage for MockClosing {
    fn run(&mut self) -> impl Future<Output = Result<(), TaskError>> + Send {
        let closed_jobs = Arc::clone(&self.closed_jobs);
        async move {
            closed_jobs.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }
}

#[tokio::test]
async fn task_recycles_to_empty_and_reuses_same_id() {
    let cancel = CancellationToken::new();
    let closed_jobs = Arc::new(AtomicUsize::new(0));
    let (handle, join) = Coordinator::start(
        MockWorkflow {
            closed_jobs: Arc::clone(&closed_jobs),
        },
        Duration::from_millis(200),
        cancel,
    );

    // Prewarm 1 Empty task, assign job 1 -> completes & recycles -> assign job 2 reuses same TaskId
    let prewarmed = handle.prewarm(1).await.unwrap();
    let task_id = prewarmed[0];
    assert_eq!(
        handle.query_task_state(task_id).await.unwrap(),
        Some(ExternalState::Empty)
    );

    let assigned_1 = handle
        .assign(MockInput::Immediate {
            wait_for: None,
            on_ready: None,
        })
        .await
        .unwrap();
    assert_eq!(assigned_1, task_id);

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(
        handle.query_task_state(task_id).await.unwrap(),
        Some(ExternalState::Empty)
    );
    assert_eq!(handle.query_idle_count().await.unwrap(), 1);

    // Late Drain delivered to Empty task must be ignored and keep the task reusable
    assert!(handle.request_recycle(task_id).await.unwrap());
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(
        handle.query_task_state(task_id).await.unwrap(),
        Some(ExternalState::Empty)
    );

    let assigned_2 = handle
        .assign(MockInput::Immediate {
            wait_for: None,
            on_ready: None,
        })
        .await
        .unwrap();
    assert_eq!(assigned_2, task_id);

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(closed_jobs.load(Ordering::Acquire), 2);

    let phase = handle.graceful_stop().await.unwrap();
    assert!(matches!(
        phase,
        TunnelPhase::Draining | TunnelPhase::Stopped
    ));

    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .expect("coordinator should exit after graceful stop")
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(
        summary.completed_jobs,
        vec![
            (task_id, TaskOutcome::Completed),
            (task_id, TaskOutcome::Completed),
        ]
    );
}

#[tokio::test]
async fn inter_task_coordination_and_proactive_recycle() {
    let cancel = CancellationToken::new();
    let closed_jobs = Arc::new(AtomicUsize::new(0));
    let (handle, join) = Coordinator::start(
        MockWorkflow {
            closed_jobs: Arc::clone(&closed_jobs),
        },
        Duration::from_millis(200),
        cancel,
    );

    let active_notify = Arc::new(Notify::new());
    let dep_notify = Arc::new(Notify::new());

    let task_a = handle
        .assign(MockInput::ForeverPending {
            on_active: Some(Arc::clone(&active_notify)),
        })
        .await
        .unwrap();
    active_notify.notified().await;

    let task_b = handle
        .assign(MockInput::Immediate {
            wait_for: Some((task_a, ExternalState::Active)),
            on_ready: Some(Arc::clone(&dep_notify)),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), dep_notify.notified())
        .await
        .expect("task B should observe task A in Active");

    // Coordinator proactively recycles forever-pending task_a back to Empty via async Drain
    assert!(handle.request_recycle(task_a).await.unwrap());
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(
        handle.query_task_state(task_a).await.unwrap(),
        Some(ExternalState::Empty)
    );

    let phase = handle.graceful_stop().await.unwrap();
    assert!(matches!(
        phase,
        TunnelPhase::Draining | TunnelPhase::Stopped
    ));

    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .expect("coordinator must drain cleanly")
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert!(
        summary
            .completed_jobs
            .contains(&(task_a, TaskOutcome::GracefullyDrained))
    );
    assert!(
        summary
            .completed_jobs
            .contains(&(task_b, TaskOutcome::Completed))
    );
}

#[tokio::test]
async fn dropping_all_handles_triggers_force_stop_immediately() {
    let cancel = CancellationToken::new();
    let closed_jobs = Arc::new(AtomicUsize::new(0));
    let (handle, join) = Coordinator::start(
        MockWorkflow {
            closed_jobs: Arc::clone(&closed_jobs),
        },
        Duration::from_secs(30),
        cancel,
    );

    let active_notify = Arc::new(Notify::new());
    let task_id = handle
        .assign(MockInput::ForeverPending {
            on_active: Some(Arc::clone(&active_notify)),
        })
        .await
        .unwrap();
    active_notify.notified().await;

    // Dropping the sole handle must cancel via Inner::drop and terminate promptly without waiting 30s.
    drop(handle);

    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .expect("coordinator must exit promptly when all handles drop")
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(
        summary.completed_jobs,
        vec![(task_id, TaskOutcome::ForceStopped)]
    );
    assert_eq!(closed_jobs.load(Ordering::Acquire), 0);
}
