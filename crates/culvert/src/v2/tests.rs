// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Minimal state-machine and Coordinator/Task coordination prototype tests.
//! All comments and docstrings must be in English.

use crate::v2::{
    ActiveStage, CloseReason, ClosingStage, ConnectingStage, Coordinator, ExternalState,
    StageContext, StateEngine, TaskError, TaskId, TaskOutcome, TunnelPhase, WaitResult,
};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{Notify, oneshot};

// =============================================================================
// 1. Pure StateEngine Tests (No Tokio / No Network)
// =============================================================================

#[test]
fn state_engine_valid_lifecycle_and_invalid_transitions() {
    let mut engine = StateEngine::new();
    let (id, _rx) = engine.register_task().unwrap();
    assert_eq!(engine.task_state(id), Some(ExternalState::Connecting));

    // Cannot skip Connecting -> Active guard with mismatched `from`
    assert!(matches!(
        engine.transition_task(id, ExternalState::Active, ExternalState::Active),
        Err(TaskError::InvalidTransition { .. })
    ));

    // Valid: Connecting -> Active -> Closing -> Closed
    assert_eq!(
        engine
            .transition_task(id, ExternalState::Connecting, ExternalState::Active)
            .unwrap(),
        ExternalState::Active
    );
    assert_eq!(engine.task_state(id), Some(ExternalState::Active));

    // Cannot transition backwards to Connecting
    assert!(matches!(
        engine.transition_task(id, ExternalState::Active, ExternalState::Connecting),
        Err(TaskError::InvalidTransition { .. })
    ));

    assert_eq!(
        engine
            .transition_task(id, ExternalState::Active, ExternalState::Closing)
            .unwrap(),
        ExternalState::Closing
    );
    assert_eq!(engine.complete_task(id), Some(ExternalState::Closing));
    assert_eq!(engine.task_state(id), None);
}

#[test]
fn state_engine_detects_wait_cycles_and_resolves_dependencies() {
    let mut engine = StateEngine::new();
    let (a, _rx_a) = engine.register_task().unwrap();
    let (b, _rx_b) = engine.register_task().unwrap();

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
    engine
        .transition_task(a, ExternalState::Connecting, ExternalState::Active)
        .unwrap();
    assert_eq!(rx_ba.try_recv().unwrap(), WaitResult::Reached);
}

#[test]
fn state_engine_rejects_active_transition_during_draining() {
    let mut engine = StateEngine::new();
    let (a, _rx_a) = engine.register_task().unwrap();
    assert_eq!(engine.begin_graceful_stop(), TunnelPhase::Draining);

    let res = engine.transition_task(a, ExternalState::Connecting, ExternalState::Active);
    assert_eq!(
        res,
        Err(TaskError::RejectedByCoordinator(TunnelPhase::Draining))
    );
}

// =============================================================================
// 2. Minimal Mock Stages for Coordinator + IoTask Integration
// =============================================================================

struct MockConnecting {
    wait_for: Option<(TaskId, ExternalState)>,
    observed_wait: Option<Arc<Notify>>,
    active_mode: MockActiveMode,
    drained_flag: Arc<AtomicBool>,
}

#[derive(Clone, Copy)]
enum MockActiveMode {
    ImmediateComplete,
    ForeverPending,
}

struct MockActive {
    mode: MockActiveMode,
    drained_flag: Arc<AtomicBool>,
    state_gate: Option<oneshot::Sender<()>>,
}

struct MockClosing {
    reason: CloseReason,
    drained_flag: Arc<AtomicBool>,
    hang_forever: bool,
}

impl ConnectingStage for MockConnecting {
    type Active = MockActive;

    fn run(
        &mut self,
        ctx: &mut StageContext<'_>,
    ) -> impl Future<Output = Result<Self::Active, TaskError>> + Send {
        let wait_for = self.wait_for;
        let observed_wait = self.observed_wait.clone();
        let mode = self.active_mode;
        let drained_flag = Arc::clone(&self.drained_flag);

        async move {
            if let Some((target, desired)) = wait_for {
                let res = ctx.await_task_state(target, desired).await?;
                if res != WaitResult::Reached {
                    return Err(TaskError::DependencyFailed(format!("{res:?}")));
                }
                if let Some(n) = observed_wait {
                    n.notify_one();
                }
            }
            Ok(MockActive {
                mode,
                drained_flag,
                state_gate: None,
            })
        }
    }
}

impl ActiveStage for MockActive {
    type Closing = MockClosing;

    fn poll_drive(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), TaskError>> {
        if let Some(tx) = self.state_gate.take() {
            let _ = tx.send(());
        }
        match self.mode {
            MockActiveMode::ImmediateComplete => Poll::Ready(Ok(())),
            MockActiveMode::ForeverPending => Poll::Pending,
        }
    }

    fn into_closing(self, reason: CloseReason) -> Self::Closing {
        MockClosing {
            reason,
            drained_flag: self.drained_flag,
            hang_forever: false,
        }
    }
}

impl ClosingStage for MockClosing {
    fn run(&mut self) -> impl Future<Output = Result<(), TaskError>> + Send {
        let flag = Arc::clone(&self.drained_flag);
        let hang = self.hang_forever;
        let _reason = self.reason;
        async move {
            if hang {
                std::future::pending::<()>().await;
            }
            flag.store(true, Ordering::Release);
            Ok(())
        }
    }
}

#[tokio::test]
async fn task_and_coordinator_complete_full_lifecycle() {
    let (handle, join) = Coordinator::start(Duration::from_millis(200));
    let drained = Arc::new(AtomicBool::new(false));

    let id = handle
        .spawn_task(MockConnecting {
            wait_for: None,
            observed_wait: None,
            active_mode: MockActiveMode::ImmediateComplete,
            drained_flag: Arc::clone(&drained),
        })
        .await
        .unwrap();

    // Wait briefly for task completion and stop coordinator.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(handle.query_task_state(id).await.unwrap(), None);

    let phase = handle.graceful_stop().await.unwrap();
    assert_eq!(phase, TunnelPhase::Stopped);

    let summary = join.await.unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(summary.completed_tasks, vec![(id, TaskOutcome::Completed)]);
    assert!(drained.load(Ordering::Acquire));
}

#[tokio::test]
async fn inter_task_coordination_awaits_target_active_state() {
    let (handle, join) = Coordinator::start(Duration::from_millis(200));
    let drained_a = Arc::new(AtomicBool::new(false));
    let drained_b = Arc::new(AtomicBool::new(false));
    let dep_reached = Arc::new(Notify::new());

    // Task A holds Active forever until force_stop.
    let task_a = handle
        .spawn_task(MockConnecting {
            wait_for: None,
            observed_wait: None,
            active_mode: MockActiveMode::ForeverPending,
            drained_flag: Arc::clone(&drained_a),
        })
        .await
        .unwrap();

    // Task B waits in Connecting until Task A reaches Active.
    let task_b = handle
        .spawn_task(MockConnecting {
            wait_for: Some((task_a, ExternalState::Active)),
            observed_wait: Some(Arc::clone(&dep_reached)),
            active_mode: MockActiveMode::ImmediateComplete,
            drained_flag: Arc::clone(&drained_b),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), dep_reached.notified())
        .await
        .expect("task B should observe task A reaching Active");

    let phase = handle.force_stop().await.unwrap();
    assert!(matches!(
        phase,
        TunnelPhase::ForceStopping | TunnelPhase::Stopped
    ));

    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .expect("coordinator must exit promptly on force stop")
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert!(
        summary
            .completed_tasks
            .iter()
            .any(|(id, out)| *id == task_a && *out == TaskOutcome::ForceStopped)
    );
    assert!(
        summary
            .completed_tasks
            .iter()
            .any(|(id, out)| *id == task_b && *out == TaskOutcome::Completed)
    );
}

#[tokio::test]
async fn graceful_stop_drains_active_task_without_waiting_on_forever_pending_deadline() {
    let (handle, join) = Coordinator::start(Duration::from_millis(50));
    let drained = Arc::new(AtomicBool::new(false));

    let id = handle
        .spawn_task(MockConnecting {
            wait_for: None,
            observed_wait: None,
            active_mode: MockActiveMode::ForeverPending,
            drained_flag: Arc::clone(&drained),
        })
        .await
        .unwrap();

    // Ensure task reached Active before graceful_stop.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        handle.query_task_state(id).await.unwrap(),
        Some(ExternalState::Active)
    );

    let phase = handle.graceful_stop().await.unwrap();
    assert!(matches!(
        phase,
        TunnelPhase::Draining | TunnelPhase::Stopped
    ));

    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .expect("graceful drain must finish cleanly")
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(
        summary.completed_tasks,
        vec![(id, TaskOutcome::GracefullyDrained)]
    );
    assert!(drained.load(Ordering::Acquire));
}
