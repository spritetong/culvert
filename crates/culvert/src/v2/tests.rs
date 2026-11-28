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
    assert_eq!(engine.empty_task_count(), 1);

    // Pop empty for connect: Empty -> Connecting with fresh JobId via atomic try_assign_idle
    let (popped_id, job_1) = engine.try_assign_idle(()).unwrap().unwrap();
    assert_eq!(popped_id, id);
    assert_eq!(engine.task_job(id), Some(job_1));
    assert_eq!(engine.empty_task_count(), 0);
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
    assert_eq!(engine.empty_task_count(), 1);

    // Stale job notification after recycle must be ignored
    engine.apply_task_state(id, job_1, ExternalState::Active);
    assert_eq!(engine.task_state(id), Some(ExternalState::Empty));

    // Graceful stop sends Shutdown to Empty task
    assert_eq!(engine.begin_graceful_stop(), TunnelPhase::Draining);
    assert!(matches!(rx.try_recv(), Ok(TaskDirective::Shutdown)));
    assert_eq!(engine.empty_task_count(), 0);

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

    // Removing an Empty task via complete_task immediately prunes empty_tasks.
    assert_eq!(engine.empty_task_count(), 1);
    engine.complete_task(dead_idle.id);
    assert_eq!(engine.empty_task_count(), 0);
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
    assert_eq!(handle.query_empty_count().await.unwrap(), 1);

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

    // Submit 400 concurrent assign calls simultaneously while all tasks generate heavy
    // microtask traffic against the Coordinator.
    let assign_futs = (0..400).map(|_| {
        handle.assign(MockInput::Immediate {
            wait_for: Some((sticky, ExternalState::Active)),
            on_ready: None,
        })
    });
    for res in futures_util::future::join_all(assign_futs).await {
        res.unwrap();
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

// =============================================================================
// 3. Unified IoTask::Idle, Keepalive, Sweep, Extraction, and Race Recovery Tests
// =============================================================================

struct MockIdleStream {
    inner: tokio::io::DuplexStream,
    sid: Option<u64>,
    alive: Arc<std::sync::atomic::AtomicBool>,
    dropped: Arc<AtomicUsize>,
}

impl MockIdleStream {
    fn new(
        sid: Option<u64>,
        alive: Arc<std::sync::atomic::AtomicBool>,
        dropped: Arc<AtomicUsize>,
    ) -> (Self, tokio::io::DuplexStream) {
        let (a, b) = tokio::io::duplex(256);
        (
            Self {
                inner: a,
                sid,
                alive,
                dropped,
            },
            b,
        )
    }
}

impl Drop for MockIdleStream {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::AcqRel);
    }
}

impl tokio::io::AsyncRead for MockIdleStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for MockIdleStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl crate::io::stream::TunnelStream for MockIdleStream {
    fn poll_check_alive(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<crate::io::stream::HeartbeatStatus> {
        if self.alive.load(Ordering::Acquire) {
            Poll::Ready(crate::io::stream::HeartbeatStatus::Alive)
        } else {
            Poll::Ready(crate::io::stream::HeartbeatStatus::Dead)
        }
    }

    fn session_id(&self) -> Option<u64> {
        self.sid
    }
}

#[derive(Clone)]
struct TransportCheckingWorkflow {
    saw_transport: Arc<AtomicUsize>,
    saw_missing: Arc<AtomicUsize>,
    report_dead_sid: Option<u64>,
}

impl TaskWorkflow for TransportCheckingWorkflow {
    type Input = ();
    type Active = MockActive;

    fn connect(
        &self,
        ctx: &mut ConnectingContext<'_>,
        _input: Self::Input,
    ) -> impl Future<Output = Result<Self::Active, TaskError>> + Send {
        let saw_transport = Arc::clone(&self.saw_transport);
        let saw_missing = Arc::clone(&self.saw_missing);
        let report_dead_sid = self.report_dead_sid;
        let has_t = ctx.take_transport().is_some();
        if let Some(sid) = report_dead_sid {
            ctx.report_dead_session(sid);
        }
        async move {
            if has_t {
                saw_transport.fetch_add(1, Ordering::AcqRel);
            } else {
                saw_missing.fetch_add(1, Ordering::AcqRel);
            }
            Ok(MockActive {
                closed_jobs: Arc::new(AtomicUsize::new(0)),
                forever_pending: false,
                on_active: None,
            })
        }
    }
}

#[tokio::test]
async fn io_task_idle_keepalive_dead_self_evicts_and_counts_loss() {
    let cfg = crate::v2::CoordinatorConfig {
        drain_timeout: Duration::from_millis(80),
        max_idle_capacity: 4,
        idle_policy: crate::v2::IdlePolicy::forward(
            Duration::from_millis(15),
            Duration::from_millis(200),
        ),
        dial: None,
    };
    let wf = TransportCheckingWorkflow {
        saw_transport: Arc::new(AtomicUsize::new(0)),
        saw_missing: Arc::new(AtomicUsize::new(0)),
        report_dead_sid: None,
    };
    let (handle, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());

    let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let dropped = Arc::new(AtomicUsize::new(0));
    let (stream, _peer) = MockIdleStream::new(Some(7), Arc::clone(&alive), Arc::clone(&dropped));

    assert!(
        handle
            .supply_transport(crate::v2::PooledTransport::new(Box::new(stream), 1))
            .await
            .unwrap()
    );
    assert_eq!(handle.query_idle_count().await.unwrap(), 1);
    assert_eq!(handle.query_empty_count().await.unwrap(), 0);

    // Flip heartbeat probe status to Dead; next interval tick must evict stream back to Empty.
    alive.store(false, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(dropped.load(Ordering::Acquire), 1);
    assert_eq!(handle.query_idle_count().await.unwrap(), 0);
    assert_eq!(handle.query_empty_count().await.unwrap(), 1);
    let stats = handle.query_pool_stats().await.unwrap();
    assert_eq!(stats.total_idle_lost, 1);

    handle.graceful_stop().await.unwrap();
    let summary = join.await.unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
}

#[tokio::test]
async fn io_task_idle_max_age_expiry_self_evicts_in_reverse_policy() {
    let cfg = crate::v2::CoordinatorConfig::reverse_inbound(
        4,
        Duration::from_millis(45),
        Duration::from_millis(80),
    );
    let wf = TransportCheckingWorkflow {
        saw_transport: Arc::new(AtomicUsize::new(0)),
        saw_missing: Arc::new(AtomicUsize::new(0)),
        report_dead_sid: None,
    };
    let (handle, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());

    let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let dropped = Arc::new(AtomicUsize::new(0));
    let (stream, _peer) = MockIdleStream::new(None, alive, Arc::clone(&dropped));

    assert!(
        handle
            .supply_transport(crate::v2::PooledTransport::new(Box::new(stream), 1))
            .await
            .unwrap()
    );
    assert_eq!(handle.query_idle_count().await.unwrap(), 1);

    tokio::time::sleep(Duration::from_millis(90)).await;

    assert_eq!(dropped.load(Ordering::Acquire), 1);
    assert_eq!(handle.query_idle_count().await.unwrap(), 0);
    assert_eq!(handle.query_empty_count().await.unwrap(), 1);

    handle.graceful_stop().await.unwrap();
    let _ = join.await.unwrap();
}

#[tokio::test]
async fn coordinator_session_id_sweep_evicts_only_matching_siblings() {
    let cfg = crate::v2::CoordinatorConfig::reverse_inbound(
        8,
        Duration::from_secs(30),
        Duration::from_millis(80),
    );
    let wf = TransportCheckingWorkflow {
        saw_transport: Arc::new(AtomicUsize::new(0)),
        saw_missing: Arc::new(AtomicUsize::new(0)),
        report_dead_sid: None,
    };
    let (handle, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());

    let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let dropped_42 = Arc::new(AtomicUsize::new(0));
    let dropped_99 = Arc::new(AtomicUsize::new(0));

    let (s1, _p1) = MockIdleStream::new(Some(42), Arc::clone(&alive), Arc::clone(&dropped_42));
    let (s2, _p2) = MockIdleStream::new(Some(42), Arc::clone(&alive), Arc::clone(&dropped_42));
    let (s3, _p3) = MockIdleStream::new(Some(99), Arc::clone(&alive), Arc::clone(&dropped_99));

    for s in [s1, s2, s3] {
        assert!(
            handle
                .supply_transport(crate::v2::PooledTransport::new(Box::new(s), 1))
                .await
                .unwrap()
        );
    }
    assert_eq!(handle.query_idle_count().await.unwrap(), 3);

    let swept = handle.sweep_session(42).await.unwrap();
    assert_eq!(swept, 2);
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert_eq!(dropped_42.load(Ordering::Acquire), 2);
    assert_eq!(dropped_99.load(Ordering::Acquire), 0);
    assert_eq!(handle.query_idle_count().await.unwrap(), 1);
    assert_eq!(handle.query_empty_count().await.unwrap(), 2);

    handle.graceful_stop().await.unwrap();
    let _ = join.await.unwrap();
}

#[tokio::test]
async fn coordinator_generation_sweep_evicts_old_and_rejects_stale_supplies() {
    let cfg = crate::v2::CoordinatorConfig::reverse_inbound(
        4,
        Duration::from_secs(30),
        Duration::from_millis(80),
    );
    let wf = TransportCheckingWorkflow {
        saw_transport: Arc::new(AtomicUsize::new(0)),
        saw_missing: Arc::new(AtomicUsize::new(0)),
        report_dead_sid: None,
    };
    let (handle, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());

    let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let dropped = Arc::new(AtomicUsize::new(0));
    let (s1, _p1) = MockIdleStream::new(None, Arc::clone(&alive), Arc::clone(&dropped));
    assert!(
        handle
            .supply_transport(crate::v2::PooledTransport::new(Box::new(s1), 1))
            .await
            .unwrap()
    );

    assert_eq!(handle.sweep_before_generation(2).await.unwrap(), 1);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(dropped.load(Ordering::Acquire), 1);
    assert_eq!(handle.query_idle_count().await.unwrap(), 0);

    // Late arrival tagged with generation 1 (< 2) must be rejected cleanly.
    let (s_stale, _p_stale) = MockIdleStream::new(None, Arc::clone(&alive), Arc::clone(&dropped));
    assert!(
        !handle
            .supply_transport(crate::v2::PooledTransport::new(Box::new(s_stale), 1))
            .await
            .unwrap()
    );
    assert_eq!(handle.query_idle_count().await.unwrap(), 0);

    // Arrival tagged with generation 2 is accepted.
    let (s_fresh, _p_fresh) = MockIdleStream::new(None, alive, dropped);
    assert!(
        handle
            .supply_transport(crate::v2::PooledTransport::new(Box::new(s_fresh), 2))
            .await
            .unwrap()
    );
    assert_eq!(handle.query_idle_count().await.unwrap(), 1);

    handle.graceful_stop().await.unwrap();
    let _ = join.await.unwrap();
}

#[tokio::test]
async fn coordinator_take_idle_transport_extracts_and_frees_slot() {
    let cfg = crate::v2::CoordinatorConfig::reverse_inbound(
        4,
        Duration::from_secs(30),
        Duration::from_millis(80),
    );
    let wf = TransportCheckingWorkflow {
        saw_transport: Arc::new(AtomicUsize::new(0)),
        saw_missing: Arc::new(AtomicUsize::new(0)),
        report_dead_sid: None,
    };
    let (handle, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());

    assert!(handle.take_idle_transport().await.unwrap().is_none());

    let (a, _b) = tokio::io::duplex(256);
    assert!(
        handle
            .supply_transport(crate::v2::PooledTransport::new(Box::new(a), 1))
            .await
            .unwrap()
    );
    assert_eq!(handle.query_idle_count().await.unwrap(), 1);

    let extracted = handle.take_idle_transport().await.unwrap();
    assert!(extracted.is_some());

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(handle.query_idle_count().await.unwrap(), 0);
    assert_eq!(handle.query_empty_count().await.unwrap(), 1);
    assert!(handle.take_idle_transport().await.unwrap().is_none());

    handle.graceful_stop().await.unwrap();
    let _ = join.await.unwrap();
}

#[tokio::test]
async fn coordinator_connecting_dead_session_report_sweeps_idle_siblings() {
    let cfg = crate::v2::CoordinatorConfig::reverse_inbound(
        4,
        Duration::from_secs(30),
        Duration::from_millis(80),
    );
    let saw_transport = Arc::new(AtomicUsize::new(0));
    let saw_missing = Arc::new(AtomicUsize::new(0));
    let wf = TransportCheckingWorkflow {
        saw_transport: Arc::clone(&saw_transport),
        saw_missing: Arc::clone(&saw_missing),
        report_dead_sid: Some(555),
    };
    let (handle, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());

    let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let dropped = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let (s, _p) = MockIdleStream::new(Some(555), Arc::clone(&alive), Arc::clone(&dropped));
        assert!(
            handle
                .supply_transport(crate::v2::PooledTransport::new(Box::new(s), 1))
                .await
                .unwrap()
        );
    }
    assert_eq!(handle.query_idle_count().await.unwrap(), 2);

    // Assign picks one Idle task, which reports dead session 555 -> sweeps remaining sibling.
    handle.assign(()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert_eq!(saw_transport.load(Ordering::Acquire), 1);
    assert_eq!(saw_missing.load(Ordering::Acquire), 0);
    assert_eq!(handle.query_idle_count().await.unwrap(), 0);
    assert_eq!(handle.query_empty_count().await.unwrap(), 2);

    handle.graceful_stop().await.unwrap();
    let _ = join.await.unwrap();
}

#[test]
fn state_engine_dispatch_assign_and_extract_lifo_invariants() {
    let mut engine = StateEngine::<()>::new();
    let r1 = engine.register_task(ExternalState::Empty).unwrap();
    let r2 = engine.register_task(ExternalState::Empty).unwrap();
    let mut rx1 = r1.directive_rx;
    let mut rx2 = r2.directive_rx;

    // Supply two transports -> Empty -> Idle (r1 then r2)
    let (s1, _p1) = tokio::io::duplex(64);
    let (s2, _p2) = tokio::io::duplex(64);
    assert!(matches!(
        engine.try_dispatch_transport(
            crate::v2::PooledTransport::new(Box::new(s1), 1),
            crate::v2::IdlePolicy::default(),
            4
        ),
        crate::v2::engine::TransportDispatch::AssignedIdle(id) if id == r1.id
    ));
    assert!(matches!(
        engine.try_dispatch_transport(
            crate::v2::PooledTransport::new(Box::new(s2), 1),
            crate::v2::IdlePolicy::default(),
            4
        ),
        crate::v2::engine::TransportDispatch::AssignedIdle(id) if id == r2.id
    ));
    assert_eq!(engine.idle_task_count(), 2);
    assert_eq!(engine.empty_task_count(), 0);

    assert!(matches!(
        rx1.try_recv(),
        Ok(TaskDirective::SupplyIdle { .. })
    ));
    assert!(matches!(
        rx2.try_recv(),
        Ok(TaskDirective::SupplyIdle { .. })
    ));

    // Assign pops freshest Idle (LIFO -> r2) first
    let (assigned_id, _) = engine.try_assign_idle(()).unwrap().unwrap();
    assert_eq!(assigned_id, r2.id);
    assert_eq!(engine.task_state(r2.id), Some(ExternalState::Connecting));
    assert_eq!(engine.idle_task_count(), 1);
    assert!(matches!(rx2.try_recv(), Ok(TaskDirective::Assign { .. })));

    // ExtractIdle pops remaining r1 with ExtractTransport
    let (tx, _rx) = oneshot::channel();
    assert!(engine.extract_idle_transport(tx));
    assert_eq!(engine.idle_task_count(), 0);
    assert!(matches!(
        rx1.try_recv(),
        Ok(TaskDirective::ExtractTransport { .. })
    ));
}

#[test]
fn cancelled_state_waiter_does_not_consume_capacity_or_create_false_cycle() {
    let mut engine = StateEngine::<()>::new();
    let reg_a = engine.register_task(ExternalState::Connecting).unwrap();
    let reg_b = engine.register_task(ExternalState::Connecting).unwrap();
    let reg_c = engine.register_task(ExternalState::Connecting).unwrap();
    let a = reg_a.id;
    let job_a = reg_a.job_id.unwrap();
    let b = reg_b.id;
    let job_b = reg_b.job_id.unwrap();
    let c = reg_c.id;
    let job_c = reg_c.job_id.unwrap();

    // 1. Register a waiter then drop its receiver; subsequent registration purges the closed
    //    waiter, and an already-closed incoming reply is rejected immediately without insertion.
    let (tx_stale, rx_stale) = oneshot::channel();
    engine.register_waiter(b, job_b, a, ExternalState::Active, tx_stale);
    assert_eq!(engine.pending_state_waiter_count(), 1);
    drop(rx_stale);

    let (tx_closed, rx_closed) = oneshot::channel();
    drop(rx_closed);
    engine.register_waiter(c, job_c, a, ExternalState::Active, tx_closed);
    assert_eq!(engine.pending_state_waiter_count(), 0);

    // 2. Repeatedly creating and cancelling waiters beyond MAX_PENDING_WAITERS (4096)
    //    must not exhaust capacity for healthy waiters.
    for _ in 0..5000 {
        let (tx, rx) = oneshot::channel();
        engine.register_waiter(c, job_c, a, ExternalState::Active, tx);
        drop(rx);
    }
    let (tx_ca, mut rx_ca) = oneshot::channel();
    engine.register_waiter(c, job_c, a, ExternalState::Active, tx_ca);
    assert!(rx_ca.try_recv().is_err());
    assert_eq!(engine.pending_state_waiter_count(), 1);

    // 3. Cancelled dependency edge A -> B must NOT trigger CycleDetected when B waits on A.
    let (tx_ab, rx_ab) = oneshot::channel();
    engine.register_waiter(a, job_a, b, ExternalState::Active, tx_ab);
    assert_eq!(engine.pending_state_waiter_count(), 2);
    drop(rx_ab);

    let (tx_ba, mut rx_ba) = oneshot::channel();
    engine.register_waiter(b, job_b, a, ExternalState::Active, tx_ba);
    assert!(rx_ba.try_recv().is_err());
    assert_eq!(engine.pending_state_waiter_count(), 2);

    // Transitioning A -> Active wakes both remaining healthy waiters (C and B).
    engine.apply_task_state(a, job_a, ExternalState::Active);
    assert_eq!(rx_ca.try_recv().unwrap(), WaitResult::Reached);
    assert_eq!(rx_ba.try_recv().unwrap(), WaitResult::Reached);
    assert_eq!(engine.pending_state_waiter_count(), 0);
}
