// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Minimal tests for two-channel Coordinator, task recycling, and CancellationToken force-stop.
//! All comments and docstrings must be in English.

use crate::v2::{
    ActiveStage, CloseReason, ClosingStage, CompletedJob, ConnectingContext, Coordinator,
    ExternalState, JobId, StateEngine, TaskDirective, TaskError, TaskId, TaskOutcome, TaskWorkflow,
    TunnelPhase, WaitResult,
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
    let reg = engine.register_task(ExternalState::Empty).unwrap();
    let id = reg.id;
    let mut rx = reg.directive_rx;
    assert_eq!(reg.job_id, None);
    assert_eq!(engine.task_state(id), Some(ExternalState::Empty));
    assert_eq!(engine.idle_task_count(), 1);

    // Pop idle for connect: Empty -> Connecting with fresh JobId via atomic try_assign_idle
    let (popped_id, job_1) = engine.try_assign_idle(()).unwrap().unwrap();
    assert_eq!(popped_id, id);
    assert_eq!(engine.task_job(id), Some(job_1));
    assert_eq!(engine.idle_task_count(), 0);
    assert_eq!(engine.task_state(id), Some(ExternalState::Connecting));
    assert!(matches!(
        rx.try_recv(),
        Ok(TaskDirective::Assign { job_id, .. }) if job_id == job_1
    ));

    // Connecting -> Active -> Closing -> Recycled back to Empty
    engine.apply_task_state(id, job_1, ExternalState::Active);
    assert_eq!(engine.task_state(id), Some(ExternalState::Active));
    engine.apply_task_state(id, job_1, ExternalState::Closing);
    assert_eq!(engine.task_state(id), Some(ExternalState::Closing));

    assert!(engine.on_task_recycled(id, job_1));
    assert_eq!(engine.task_state(id), Some(ExternalState::Empty));
    assert_eq!(engine.task_job(id), None);
    assert_eq!(engine.idle_task_count(), 1);

    // Stale job notification after recycle must be ignored
    engine.apply_task_state(id, job_1, ExternalState::Active);
    assert_eq!(engine.task_state(id), Some(ExternalState::Empty));

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
    let reg_a = engine.register_task(ExternalState::Connecting).unwrap();
    let reg_b = engine.register_task(ExternalState::Connecting).unwrap();
    let a = reg_a.id;
    let job_a = reg_a.job_id.unwrap();
    let b = reg_b.id;
    let job_b = reg_b.job_id.unwrap();

    // Self-wait is an immediate cycle
    let (tx_self, mut rx_self) = oneshot::channel();
    engine.register_waiter(a, job_a, a, ExternalState::Active, tx_self);
    assert_eq!(rx_self.try_recv().unwrap(), WaitResult::CycleDetected);

    // B waits for A -> Active
    let (tx_ba, mut rx_ba) = oneshot::channel();
    engine.register_waiter(b, job_b, a, ExternalState::Active, tx_ba);
    assert!(rx_ba.try_recv().is_err());

    // A waiting for B -> Active would create cycle A -> B -> A
    let (tx_ab, mut rx_ab) = oneshot::channel();
    engine.register_waiter(a, job_a, b, ExternalState::Active, tx_ab);
    assert_eq!(rx_ab.try_recv().unwrap(), WaitResult::CycleDetected);

    // Transition A -> Active wakes B
    engine.apply_task_state(a, job_a, ExternalState::Active);
    assert_eq!(rx_ba.try_recv().unwrap(), WaitResult::Reached);
}

#[test]
fn state_engine_waiter_recycle_semantics_distinguishes_empty_and_closed() {
    let mut engine = StateEngine::<()>::new();
    let reg_t = engine.register_task(ExternalState::Connecting).unwrap();
    let reg_e = engine.register_task(ExternalState::Connecting).unwrap();
    let reg_c = engine.register_task(ExternalState::Connecting).unwrap();
    let reg_a = engine.register_task(ExternalState::Connecting).unwrap();
    let target = reg_t.id;
    let job_1 = reg_t.job_id.unwrap();
    let waiter_empty = reg_e.id;
    let waiter_closed = reg_c.id;
    let waiter_active = reg_a.id;

    let (tx_empty, mut rx_empty) = oneshot::channel();
    let (tx_closed, mut rx_closed) = oneshot::channel();
    let (tx_active, mut rx_active) = oneshot::channel();
    engine.register_waiter(
        waiter_empty,
        reg_e.job_id.unwrap(),
        target,
        ExternalState::Empty,
        tx_empty,
    );
    engine.register_waiter(
        waiter_closed,
        reg_c.job_id.unwrap(),
        target,
        ExternalState::Closed,
        tx_closed,
    );
    engine.register_waiter(
        waiter_active,
        reg_a.job_id.unwrap(),
        target,
        ExternalState::Active,
        tx_active,
    );

    // Target recycles back to Empty after failing Connecting (without reaching Active or Closed):
    // - Waiter for Empty -> Reached
    // - Waiter for Closed -> Still pending until complete_task
    // - Waiter for Active -> TargetClosed(Empty), never leaks into Job 2
    assert!(engine.on_task_recycled(target, job_1));
    assert_eq!(rx_empty.try_recv().unwrap(), WaitResult::Reached);
    assert!(rx_closed.try_recv().is_err());
    assert_eq!(
        rx_active.try_recv().unwrap(),
        WaitResult::TargetClosed(ExternalState::Empty)
    );

    // Only when target actually completes to Closed does waiter_closed receive Reached
    engine.complete_task(target);
    assert_eq!(rx_closed.try_recv().unwrap(), WaitResult::Reached);
}

#[test]
fn closed_directive_channel_rejected_and_recovers_input() {
    let mut engine = StateEngine::<u32>::new();
    let dead_reg = engine.register_task(ExternalState::Empty).unwrap();
    let live_reg = engine.register_task(ExternalState::Empty).unwrap();
    let dead_id = dead_reg.id;
    let live_id = live_reg.id;
    let mut live_rx = live_reg.directive_rx;

    // Drop the dead worker's directive receiver before any directive is sent.
    drop(dead_reg.directive_rx);

    assert!(!engine.send_directive(dead_id, TaskDirective::Drain));
    assert!(engine.send_directive(live_id, TaskDirective::Drain));
    assert!(matches!(live_rx.try_recv(), Ok(TaskDirective::Drain)));

    // try_assign_idle skips the dead worker without losing `42` or corrupting dead_id to Connecting,
    // and delivers `42` to the live idle worker instead.
    let assigned = engine.try_assign_idle(42).unwrap().unwrap();
    assert_eq!(assigned.0, live_id);
    assert_eq!(engine.task_state(dead_id), Some(ExternalState::Empty));
    assert!(matches!(
        live_rx.try_recv(),
        Ok(TaskDirective::Assign { input: 42, .. })
    ));

    // If all idle channels are closed, try_assign_idle returns Ok(Err(input)) intact.
    let dead2 = engine.register_task(ExternalState::Empty).unwrap();
    drop(dead2.directive_rx);
    let recovered = engine.try_assign_idle(99).unwrap().unwrap_err();
    assert_eq!(recovered, 99);
}

#[test]
fn complete_task_prunes_idle_queue_and_stale_job_waiters_remain_isolated() {
    let mut engine = StateEngine::<()>::new();
    let worker_a = engine.register_task(ExternalState::Connecting).unwrap();
    let target_t = engine.register_task(ExternalState::Connecting).unwrap();
    let dead_idle = engine.register_task(ExternalState::Empty).unwrap();

    // Removing an Empty task via complete_task immediately prunes idle_tasks.
    assert_eq!(engine.idle_task_count(), 1);
    engine.complete_task(dead_idle.id);
    assert_eq!(engine.idle_task_count(), 0);
    assert_eq!(engine.live_task_count(), 2);

    // Worker A finishes Job 1 and receives Job 2.
    let job_1 = worker_a.job_id.unwrap();
    assert!(engine.on_task_recycled(worker_a.id, job_1));
    let (_, job_2) = engine.try_assign_idle(()).unwrap().unwrap();
    assert_ne!(job_1, job_2);

    // Job 2 registers a waiter on Target T.
    let (tx_job2, mut rx_job2) = oneshot::channel();
    engine.register_waiter(
        worker_a.id,
        job_2,
        target_t.id,
        ExternalState::Active,
        tx_job2,
    );

    // Stale events carrying job_1 MUST NOT cancel job_2's waiter or recycle worker_a.
    assert!(!engine.on_task_recycled(worker_a.id, job_1));
    let (tx_stale, mut rx_stale) = oneshot::channel();
    engine.register_waiter(
        worker_a.id,
        job_1,
        target_t.id,
        ExternalState::Active,
        tx_stale,
    );
    assert_eq!(rx_stale.try_recv().unwrap(), WaitResult::Cancelled);
    assert!(rx_job2.try_recv().is_err());

    // Target T reaching Active wakes Job 2's waiter cleanly.
    engine.apply_task_state(target_t.id, target_t.job_id.unwrap(), ExternalState::Active);
    assert_eq!(rx_job2.try_recv().unwrap(), WaitResult::Reached);
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
    PanicInConnect,
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
                MockInput::PanicInConnect => panic!("intentional test panic inside IoTask"),
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
            CompletedJob {
                task_id,
                job_id: JobId::new(1),
                outcome: TaskOutcome::Completed,
            },
            CompletedJob {
                task_id,
                job_id: JobId::new(2),
                outcome: TaskOutcome::Completed,
            },
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
            .iter()
            .any(|c| { c.task_id == task_a && c.outcome == TaskOutcome::GracefullyDrained })
    );
    assert!(
        summary
            .completed_jobs
            .iter()
            .any(|c| c.task_id == task_b && c.outcome == TaskOutcome::Completed)
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
        vec![CompletedJob {
            task_id,
            job_id: JobId::new(1),
            outcome: TaskOutcome::ForceStopped,
        }]
    );
    assert_eq!(closed_jobs.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn coordinator_cleans_up_panicked_task_and_exits_gracefully() {
    let cancel = CancellationToken::new();
    let (handle, join) = Coordinator::start(
        MockWorkflow {
            closed_jobs: Arc::new(AtomicUsize::new(0)),
        },
        Duration::from_millis(500),
        cancel,
    );

    let panicked_task = handle.assign(MockInput::PanicInConnect).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Task entry must have been removed upon JoinError so query returns None.
    assert_eq!(handle.query_task_state(panicked_task).await.unwrap(), None);

    // Coordinator must reach Stopped immediately instead of hanging in Draining.
    let phase = handle.graceful_stop().await.unwrap();
    assert_eq!(phase, TunnelPhase::Stopped);

    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .expect("coordinator must not stall after a task panic")
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(summary.completed_jobs.len(), 1);
    assert_eq!(summary.completed_jobs[0].task_id, panicked_task);
    assert!(matches!(
        summary.completed_jobs[0].outcome,
        TaskOutcome::Failed(_)
    ));
}

#[tokio::test]
async fn coordinator_assign_join_and_drain_progress_under_microtask_burst() {
    let cancel = CancellationToken::new();
    let closed_jobs = Arc::new(AtomicUsize::new(0));
    let (handle, join) = Coordinator::start(
        MockWorkflow {
            closed_jobs: Arc::clone(&closed_jobs),
        },
        Duration::from_millis(80),
        cancel,
    );

    // Prewarm a worker pool and flood 400 rapid jobs generating heavy microtask traffic
    // while concurrently keeping a long-lived task active until bounded graceful_stop.
    handle.prewarm(16).await.unwrap();
    let active_notify = Arc::new(Notify::new());
    let sticky = handle
        .assign(MockInput::ForeverPending {
            on_active: Some(Arc::clone(&active_notify)),
        })
        .await
        .unwrap();
    active_notify.notified().await;

    for _ in 0..400 {
        handle
            .assign(MockInput::Immediate {
                wait_for: Some((sticky, ExternalState::Active)),
                on_ready: None,
            })
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(handle.graceful_stop().await.unwrap(), TunnelPhase::Draining);

    let summary = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("coordinator must drain & join promptly after microtask burst")
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(summary.completed_jobs.len(), 401);
}
