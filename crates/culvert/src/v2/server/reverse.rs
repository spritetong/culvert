// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Server-side Reverse session registry, real `RouterReverseSignaler`, and
//! supervised `ReverseControlSession` managing registration, 0-RTT `Idle`-task
//! coordination, `0xA6` emergency promotion, and bounded grace-period reattachment.
//! All comments and docstrings must be in English.

use crate::consts::{
    ACTIVATE_STREAM_PROMOTE, DEFAULT_RELAY_DRAIN_TIMEOUT, DEFAULT_SESSION_PAIR_TIMEOUT,
    reverse_frame_timeout, reverse_grace_period, reverse_pool_soft_cap,
};
use crate::control::{
    ControlMessage, FramedControlStream, SendCommitGate, SignalingChannel, SignalingConfig,
    SignalingReceiver, SignalingRouter,
};
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::frontend;
use crate::io::addr::{L4Protocol, MappingSpec, PeerAddr, TargetAddr, canonical_reverse_key};
use crate::v2::coordinator::{Coordinator, CoordinatorHandle, CoordinatorSummary};
use crate::v2::error::TaskError;
use crate::v2::reverse::{ReverseServerRunner, ReverseSignaler, ReverseVisitorWorkflow};
use crate::v2::state::CoordinatorConfig;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use futures_util::{SinkExt, StreamExt};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Sleep;
use tokio_util::sync::CancellationToken;

static NEXT_SESSION_SEQ: AtomicU64 = AtomicU64::new(1);

/// Reconnected control stream handed over to a grace-period listener.
pub(super) struct ReattachPayload {
    pub(super) ctrl_stream: Box<FramedControlStream<Box<dyn TunnelStream>>>,
}

struct GraceSlot {
    id: u64,
    tx: mpsc::Sender<ReattachPayload>,
    closed_flag: Arc<AtomicBool>,
    closed_notify: Arc<Notify>,
}

pub(super) struct ReverseIdleTarget {
    pub(super) coord: CoordinatorHandle<ReverseVisitorWorkflow>,
    generation: AtomicU64,
    listeners: AtomicUsize,
    join: Mutex<Option<JoinHandle<CoordinatorSummary>>>,
}

impl ReverseIdleTarget {
    fn new(
        coord: CoordinatorHandle<ReverseVisitorWorkflow>,
        join: JoinHandle<CoordinatorSummary>,
    ) -> Self {
        Self {
            coord,
            generation: AtomicU64::new(1),
            listeners: AtomicUsize::new(0),
            join: Mutex::new(Some(join)),
        }
    }

    fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn bump_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn take_join(&self) -> Option<JoinHandle<CoordinatorSummary>> {
        self.join.lock().ok().and_then(|mut guard| guard.take())
    }
}

/// Active grace-period registration preserving a bound reverse listener across brief disconnects.
pub(super) struct GraceHandle {
    key: String,
    slot_id: u64,
    rx: mpsc::Receiver<ReattachPayload>,
    closed_flag: Arc<AtomicBool>,
    closed_notify: Arc<Notify>,
    registry: Arc<DashMap<String, GraceSlot>>,
    finished: bool,
}

impl GraceHandle {
    pub(super) async fn recv(&mut self) -> Option<ReattachPayload> {
        self.rx.recv().await
    }

    #[cfg(test)]
    pub(super) fn close_rx_only(&mut self) {
        self.rx.close();
    }

    fn close_and_try_recv(&mut self) -> Option<ReattachPayload> {
        self.rx.close();
        self.rx.try_recv().ok()
    }

    pub(super) fn finish(mut self) {
        self.cleanup();
    }

    fn cleanup(&mut self) {
        if !self.finished {
            self.finished = true;
            self.rx.close();
            self.registry
                .remove_if(&self.key, |_, slot| slot.id == self.slot_id);
            self.closed_flag.store(true, Ordering::Release);
            self.closed_notify.notify_waiters();
        }
    }
}

impl Drop for GraceHandle {
    fn drop(&mut self) {
        self.cleanup();
    }
}

struct PendingSessionGuard {
    hub: Arc<ReverseSessionHub>,
    session_id: String,
    gate: Arc<SendCommitGate>,
    rx: Option<oneshot::Receiver<Box<dyn TunnelStream>>>,
    linger_timeout: Duration,
    active: bool,
}

impl PendingSessionGuard {
    fn rx_mut(&mut self) -> Option<&mut oneshot::Receiver<Box<dyn TunnelStream>>> {
        self.rx.as_mut()
    }

    fn defuse(mut self) {
        self.active = false;
    }
}

impl Drop for PendingSessionGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if self.gate.try_cancel() {
            // Cancelled before wire commit: clean up immediately.
            self.hub.remove_session(&self.session_id);
        } else if let Some(rx) = self.rx.take() {
            // Already committed on the wire (`start_send`): keep pending session alive
            // until either paired via `fulfill()` or `linger_timeout` expires.
            let hub = Arc::clone(&self.hub);
            let session_id = self.session_id.clone();
            let timeout = self.linger_timeout;
            tokio::spawn(async move {
                let _ = tokio::time::timeout(timeout, rx).await;
                hub.remove_session(&session_id);
            });
        }
    }
}

pub(super) struct RegisteredCoordinatorGuard {
    hub: Arc<ReverseSessionHub>,
    protocol: L4Protocol,
    listen_addr: String,
    tunnel_id: Option<String>,
    target: Arc<ReverseIdleTarget>,
    released: bool,
}

impl RegisteredCoordinatorGuard {
    pub(super) async fn shutdown(mut self) {
        self.released = true;
        let is_last = self.hub.unregister_coordinator(
            self.protocol,
            &self.listen_addr,
            self.tunnel_id.as_deref(),
            &self.target,
        );
        if is_last || self.tunnel_id.is_none() {
            let _ = self.target.coord.graceful_stop().await;
            if let Some(join) = self.target.take_join() {
                let _ = join.await;
            }
        }
    }
}

impl Drop for RegisteredCoordinatorGuard {
    fn drop(&mut self) {
        if !self.released {
            let is_last = self.hub.unregister_coordinator(
                self.protocol,
                &self.listen_addr,
                self.tunnel_id.as_deref(),
                &self.target,
            );
            if is_last || self.tunnel_id.is_none() {
                self.target.coord.force_stop();
            }
        }
    }
}

/// Thread-safe v2 server registry pairing inbound reverse data streams, routing
/// pre-warmed `PoolIdle` streams into unified `Coordinator` `Idle` tasks, and
/// coordinating grace-period control channel reattachment.
#[derive(Default)]
pub struct ReverseSessionHub {
    pending_sessions: DashMap<String, oneshot::Sender<Box<dyn TunnelStream>>>,
    reverse_coordinators: DashMap<String, Arc<ReverseIdleTarget>>,
    tunnel_coordinators: DashMap<String, Arc<ReverseIdleTarget>>,
    grace_listeners: Arc<DashMap<String, GraceSlot>>,
    next_grace_id: AtomicU64,
}

impl ReverseSessionHub {
    /// Creates an empty `ReverseSessionHub`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a pending session ID with an atomic commit-guarded cleanup drop handler.
    fn register_guarded(
        self: &Arc<Self>,
        session_id: String,
        gate: Arc<SendCommitGate>,
        linger_timeout: Duration,
    ) -> PendingSessionGuard {
        let (tx, rx) = oneshot::channel();
        self.pending_sessions.insert(session_id.clone(), tx);
        PendingSessionGuard {
            hub: Arc::clone(self),
            session_id,
            gate,
            rx: Some(rx),
            linger_timeout,
            active: true,
        }
    }

    #[cfg(test)]
    pub(super) fn pending_session_count(&self) -> usize {
        self.pending_sessions.len()
    }

    /// Returns the number of reverse listeners currently held in their reconnection grace period.
    pub fn grace_listener_count(&self) -> usize {
        self.grace_listeners.len()
    }

    fn remove_session(&self, session_id: &str) {
        self.pending_sessions.remove(session_id);
    }

    /// Delivers an inbound `session_id` reverse data stream to its awaiting visitor task.
    pub fn fulfill(
        &self,
        session_id: &str,
        stream: Box<dyn TunnelStream>,
    ) -> Result<(), TunnelError> {
        let Some((_, tx)) = self.pending_sessions.remove(session_id) else {
            return Err(TunnelError::SessionNotFound(session_id.to_string()));
        };
        tx.send(stream).map_err(|_| TunnelError::SessionClosed)
    }

    /// Resolves the destination `CoordinatorHandle` and current generation for an inbound
    /// `pool_idle` reverse stream without holding any map lock across `.await`.
    pub fn lookup_idle_coordinator(
        &self,
        tunnel_id: Option<&str>,
        protocol: L4Protocol,
        target: Option<&TargetAddr>,
    ) -> Option<(CoordinatorHandle<ReverseVisitorWorkflow>, u64)> {
        if let Some(tid) = tunnel_id
            && let Some(slot) = self.tunnel_coordinators.get(tid)
        {
            return Some((slot.coord.clone(), slot.current_generation()));
        }
        let target_addr = target?;
        let key = canonical_reverse_key(protocol, &target_addr.to_string());
        self.reverse_coordinators
            .get(&key)
            .map(|entry| (entry.coord.clone(), entry.current_generation()))
    }

    /// Registers an exclusive or shared `tunnel_id` `Coordinator` for a bound reverse listener.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn register_or_join_coordinator(
        self: &Arc<Self>,
        protocol: L4Protocol,
        listen_addr: &str,
        tunnel_id: Option<String>,
        fallback_signaler: Arc<dyn ReverseSignaler>,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
        relay_idle_timeout: Duration,
        soft_cap: usize,
    ) -> (Arc<ReverseIdleTarget>, RegisteredCoordinatorGuard) {
        if let Some(ref tid) = tunnel_id {
            // Mutate `listeners` under the DashMap Entry shard write lock so registration can
            // never read an Arc that an interleaving last-unregister is about to remove and stop.
            let slot = match self.tunnel_coordinators.entry(tid.clone()) {
                Entry::Occupied(entry) => {
                    let existing = Arc::clone(entry.get());
                    existing.listeners.fetch_add(1, Ordering::AcqRel);
                    existing
                }
                Entry::Vacant(entry) => {
                    let workflow = ReverseVisitorWorkflow::shared(
                        heartbeat_interval,
                        idle_timeout,
                        DEFAULT_RELAY_DRAIN_TIMEOUT,
                    );
                    let cfg = CoordinatorConfig::reverse_inbound(
                        soft_cap,
                        idle_timeout,
                        DEFAULT_RELAY_DRAIN_TIMEOUT,
                    );
                    let (coord, join) =
                        Coordinator::start_with_config(workflow, cfg, CancellationToken::new());
                    let created = Arc::new(ReverseIdleTarget::new(coord, join));
                    created.listeners.store(1, Ordering::Release);
                    entry.insert(Arc::clone(&created));
                    created
                }
            };
            let guard = RegisteredCoordinatorGuard {
                hub: Arc::clone(self),
                protocol,
                listen_addr: listen_addr.to_string(),
                tunnel_id: Some(tid.clone()),
                target: Arc::clone(&slot),
                released: false,
            };
            (slot, guard)
        } else {
            let key = canonical_reverse_key(protocol, listen_addr);
            let workflow = ReverseVisitorWorkflow::new(
                fallback_signaler,
                heartbeat_interval,
                relay_idle_timeout,
                DEFAULT_RELAY_DRAIN_TIMEOUT,
            );
            let cfg = CoordinatorConfig::reverse_inbound(
                soft_cap,
                idle_timeout,
                DEFAULT_RELAY_DRAIN_TIMEOUT,
            );
            let (coord, join) =
                Coordinator::start_with_config(workflow, cfg, CancellationToken::new());
            let target = Arc::new(ReverseIdleTarget::new(coord, join));
            if let Some(old) = self.reverse_coordinators.insert(key, Arc::clone(&target)) {
                old.coord.force_stop();
            }
            let guard = RegisteredCoordinatorGuard {
                hub: Arc::clone(self),
                protocol,
                listen_addr: listen_addr.to_string(),
                tunnel_id: None,
                target: Arc::clone(&target),
                released: false,
            };
            (target, guard)
        }
    }

    fn unregister_coordinator(
        &self,
        protocol: L4Protocol,
        listen_addr: &str,
        tunnel_id: Option<&str>,
        target: &Arc<ReverseIdleTarget>,
    ) -> bool {
        if let Some(tid) = tunnel_id {
            // Decrement and decide removal atomically inside the DashMap shard write lock.
            self.tunnel_coordinators
                .remove_if(tid, |_, slot| {
                    Arc::ptr_eq(slot, target) && slot.listeners.fetch_sub(1, Ordering::AcqRel) == 1
                })
                .is_some()
        } else {
            let key = canonical_reverse_key(protocol, listen_addr);
            self.reverse_coordinators
                .remove_if(&key, |_, current| {
                    current.coord.same_coordinator(&target.coord)
                })
                .is_some()
        }
    }

    pub(super) fn register_grace_listener(
        &self,
        protocol: L4Protocol,
        listen_addr: &str,
    ) -> GraceHandle {
        let key = canonical_reverse_key(protocol, listen_addr);
        let slot_id = self.next_grace_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(4);
        let closed_flag = Arc::new(AtomicBool::new(false));
        let closed_notify = Arc::new(Notify::new());
        self.grace_listeners.insert(
            key.clone(),
            GraceSlot {
                id: slot_id,
                tx,
                closed_flag: Arc::clone(&closed_flag),
                closed_notify: Arc::clone(&closed_notify),
            },
        );
        GraceHandle {
            key,
            slot_id,
            rx,
            closed_flag,
            closed_notify,
            registry: Arc::clone(&self.grace_listeners),
            finished: false,
        }
    }

    pub(super) async fn try_reattach_grace_listener(
        &self,
        protocol: L4Protocol,
        listen_addr: &str,
        mut payload: ReattachPayload,
        cancel_token: &CancellationToken,
    ) -> Result<(), ReattachPayload> {
        let key = canonical_reverse_key(protocol, listen_addr);
        loop {
            let (tx, closed_flag, closed_notify) = match self.grace_listeners.get(&key) {
                Some(entry) => (
                    entry.tx.clone(),
                    Arc::clone(&entry.closed_flag),
                    Arc::clone(&entry.closed_notify),
                ),
                None => return Err(payload),
            };
            // Pin and enable the Notified waiter BEFORE reading `closed_flag` or calling `tx.send()`
            // so concurrent cleanup cannot fire `notify_waiters()` before we start awaiting.
            let mut notified = std::pin::pin!(closed_notify.notified());
            notified.as_mut().enable();
            if closed_flag.load(Ordering::Acquire) {
                continue;
            }
            match tx.send(payload).await {
                Ok(()) => return Ok(()),
                Err(send_err) => {
                    payload = send_err.0;
                    if closed_flag.load(Ordering::Acquire) {
                        continue;
                    }
                    tokio::select! {
                        biased;
                        _ = cancel_token.cancelled() => return Err(payload),
                        () = &mut notified => {}
                    }
                }
            }
        }
    }
}

/// Real `ReverseSignaler` sending `ControlMessage::NewConnection` over `SignalingRouter`
/// and awaiting a paired reverse data stream in `ReverseSessionHub`.
pub struct RouterReverseSignaler {
    router: SignalingRouter,
    hub: Arc<ReverseSessionHub>,
    pair_timeout: Duration,
    disconnect_notify: Arc<Notify>,
}

impl RouterReverseSignaler {
    /// Creates a new `RouterReverseSignaler`.
    pub fn new(
        router: SignalingRouter,
        hub: Arc<ReverseSessionHub>,
        pair_timeout: Duration,
        disconnect_notify: Arc<Notify>,
    ) -> Self {
        Self {
            router,
            hub,
            pair_timeout,
            disconnect_notify,
        }
    }
}

impl ReverseSignaler for RouterReverseSignaler {
    fn request_pair(
        &self,
        visitor_peer: PeerAddr,
    ) -> BoxFuture<'static, Result<Box<dyn TunnelStream>, TaskError>> {
        let router = self.router.clone();
        let hub = Arc::clone(&self.hub);
        let pair_timeout = self.pair_timeout;
        let disconnect_notify = Arc::clone(&self.disconnect_notify);

        Box::pin(async move {
            let seq = NEXT_SESSION_SEQ.fetch_add(1, Ordering::Relaxed);
            let session_id = format!("v2s-{seq}");
            let gate = Arc::new(SendCommitGate::new());
            let mut guard =
                hub.register_guarded(session_id.clone(), Arc::clone(&gate), pair_timeout);

            let msg = ControlMessage::NewConnection {
                session_id,
                client_addr: visitor_peer.to_string(),
            };
            if let Err(err) = router.send_with_gate(msg, gate).await {
                disconnect_notify.notify_one();
                return Err(TaskError::StageFailed(format!(
                    "failed to send NewConnection signal: {err}"
                )));
            }

            let rx = guard
                .rx_mut()
                .ok_or_else(|| TaskError::StageFailed("reverse receiver missing".to_string()))?;
            let stream = tokio::time::timeout(pair_timeout, rx)
                .await
                .map_err(|_| TaskError::StageFailed("reverse pair timeout".to_string()))?
                .map_err(|_| TaskError::StageFailed("reverse session dropped".to_string()))?;

            guard.defuse();
            Ok(stream)
        })
    }
}

/// Supervises one reverse control session end-to-end on the v2 server stack.
pub struct ReverseControlSession;

impl ReverseControlSession {
    /// Drives initial `RegisterReverse` handshake, spawns `ReverseServerRunner`, and
    /// supervises signaling failover (`0xA6`) and grace-period control reattachment.
    pub async fn run(
        stream: Box<dyn TunnelStream>,
        hub: Arc<ReverseSessionHub>,
        idle_timeout: Duration,
        heartbeat_interval: Duration,
        udp_idle_timeout: Duration,
        cancel_token: CancellationToken,
    ) {
        let mut ctrl_stream = FramedControlStream::new(stream);
        let Some((listen_addr, target_addr, pool_size, protocol, tunnel_id)) =
            Self::read_initial_register(&mut ctrl_stream, idle_timeout, &cancel_token).await
        else {
            return;
        };

        let reattach = ReattachPayload {
            ctrl_stream: Box::new(ctrl_stream),
        };
        let mut ctrl_stream = match hub
            .try_reattach_grace_listener(protocol, &listen_addr, reattach, &cancel_token)
            .await
        {
            Ok(()) => return,
            Err(returned) => *returned.ctrl_stream,
        };

        if cancel_token.is_cancelled() {
            return;
        }

        let frame_timeout = reverse_frame_timeout(idle_timeout);
        let mut fe = match frontend::bind(&MappingSpec::new(protocol, &listen_addr, &target_addr))
            .await
        {
            Ok(l) => l,
            Err(err) => {
                tracing::warn!(addr = %listen_addr, protocol = %protocol, error = %err, "v2 reverse frontend bind failed");
                let _ = tokio::time::timeout(
                    frame_timeout,
                    ctrl_stream.send(ControlMessage::RegisterAck {
                        listen_addr,
                        success: false,
                        error_msg: Some(err.to_string()),
                    }),
                )
                .await;
                return;
            }
        };

        let sig_config = SignalingConfig {
            heartbeat_interval,
            idle_timeout,
        };
        let (signaling, mut sig_rx) =
            SignalingChannel::spawn_framed(ctrl_stream, sig_config, cancel_token.child_token());
        let router = SignalingRouter::new(signaling);

        let relay_idle_timeout = match protocol {
            L4Protocol::Tcp => idle_timeout,
            L4Protocol::Udp => udp_idle_timeout,
        };
        let disconnect_notify = Arc::new(Notify::new());
        let signaler: Arc<dyn ReverseSignaler> = Arc::new(RouterReverseSignaler::new(
            router.clone(),
            Arc::clone(&hub),
            DEFAULT_SESSION_PAIR_TIMEOUT,
            Arc::clone(&disconnect_notify),
        ));

        let soft_cap = reverse_pool_soft_cap(pool_size);
        let (coord_target, coord_guard) = hub.register_or_join_coordinator(
            protocol,
            &listen_addr,
            tunnel_id.clone(),
            Arc::clone(&signaler),
            heartbeat_interval,
            idle_timeout,
            relay_idle_timeout,
            soft_cap,
        );

        let ack_sent = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => false,
            res = router.send(ControlMessage::RegisterAck {
                listen_addr: listen_addr.clone(),
                success: true,
                error_msg: None,
            }) => res.is_ok(),
        };
        if !ack_sent {
            fe.shutdown().await;
            drop(fe);
            coord_guard.shutdown().await;
            return;
        }

        let Ok(target_parsed) = crate::io::addr::parse_target_addr(&target_addr) else {
            fe.shutdown().await;
            drop(fe);
            coord_guard.shutdown().await;
            return;
        };

        let exclusive_join = if tunnel_id.is_none() {
            coord_target.take_join()
        } else {
            None
        };
        let runner = ReverseServerRunner::with_coordinator(
            fe,
            target_parsed,
            protocol,
            coord_target.coord.clone(),
            exclusive_join,
            tunnel_id.as_ref().map(|_| Arc::clone(&signaler)),
            tunnel_id.as_ref().map(|_| relay_idle_timeout),
        );

        let runner_cancel = cancel_token.child_token();
        let mut runner_handle = tokio::spawn(runner.run(runner_cancel.clone()));

        let grace_period = reverse_grace_period(idle_timeout);
        let mut grace_handle: Option<GraceHandle> = None;
        let mut grace_deadline: Option<Pin<Box<Sleep>>> = None;

        loop {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                _ = &mut runner_handle => {
                    coord_guard.shutdown().await;
                    return;
                }
                Some(reattach) = Self::poll_grace_reattach(&mut grace_handle), if grace_handle.is_some() => {
                    if let Some(g) = grace_handle.take() {
                        g.finish();
                    }
                    grace_deadline = None;
                    let new_gen = coord_target.bump_generation();
                    let _ = coord_target.coord.sweep_before_generation(new_gen).await;
                    if let Some(sink) = sig_rx.event_sink() {
                        let new_handle = SignalingChannel::spawn_framed_with_event_sink(
                            *reattach.ctrl_stream,
                            sig_config,
                            sink,
                            cancel_token.child_token(),
                        );
                        router.attach(new_handle);
                        let _ = router
                            .send(ControlMessage::RegisterAck {
                                listen_addr: listen_addr.clone(),
                                success: true,
                                error_msg: None,
                            })
                            .await;
                    }
                }
                _ = Self::poll_sleep(&mut grace_deadline), if grace_deadline.is_some() => {
                    if let Some(reattach) = grace_handle.as_mut().and_then(|g| g.close_and_try_recv()) {
                        if let Some(g) = grace_handle.take() {
                            g.finish();
                        }
                        grace_deadline = None;
                        let new_gen = coord_target.bump_generation();
                        let _ = coord_target.coord.sweep_before_generation(new_gen).await;
                        if let Some(sink) = sig_rx.event_sink() {
                            let new_handle = SignalingChannel::spawn_framed_with_event_sink(
                                *reattach.ctrl_stream,
                                sig_config,
                                sink,
                                cancel_token.child_token(),
                            );
                            router.attach(new_handle);
                            let _ = router
                                .send(ControlMessage::RegisterAck {
                                    listen_addr: listen_addr.clone(),
                                    success: true,
                                    error_msg: None,
                                })
                                .await;
                            continue;
                        }
                    }
                    break;
                }
                _ = disconnect_notify.notified(), if grace_handle.is_none() => {
                    if !router.has_active_channel().await
                        && !Self::recover_or_enter_grace(
                            &listen_addr,
                            protocol,
                            tunnel_id.as_deref(),
                            idle_timeout,
                            grace_period,
                            sig_config,
                            &cancel_token,
                            &hub,
                            &coord_target,
                            &router,
                            &sig_rx,
                            &mut grace_handle,
                            &mut grace_deadline,
                        ).await
                    {
                        break;
                    }
                }
                msg_opt = sig_rx.recv(), if grace_handle.is_none() => {
                    match msg_opt {
                        Some(Ok(ControlMessage::Ping | ControlMessage::Pong | ControlMessage::RegisterReverse { .. } | ControlMessage::RegisterAck { .. } | ControlMessage::NewConnection { .. })) => {}
                        _ => {
                            if !Self::recover_or_enter_grace(
                                &listen_addr,
                                protocol,
                                tunnel_id.as_deref(),
                                idle_timeout,
                                grace_period,
                                sig_config,
                                &cancel_token,
                                &hub,
                                &coord_target,
                                &router,
                                &sig_rx,
                                &mut grace_handle,
                                &mut grace_deadline,
                            ).await {
                                break;
                            }
                        }
                    }
                }
            }
        }

        if let Some(g) = grace_handle.take() {
            g.finish();
        }
        runner_cancel.cancel();
        let _ = runner_handle.await;
        coord_guard.shutdown().await;
    }

    async fn poll_grace_reattach(grace: &mut Option<GraceHandle>) -> Option<ReattachPayload> {
        match grace.as_mut() {
            Some(g) => g.recv().await,
            None => None,
        }
    }

    async fn poll_sleep(sleep_opt: &mut Option<Pin<Box<Sleep>>>) {
        if let Some(sleep) = sleep_opt.as_mut() {
            sleep.as_mut().await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_or_enter_grace(
        listen_addr: &str,
        protocol: L4Protocol,
        tunnel_id: Option<&str>,
        idle_timeout: Duration,
        grace_period: Duration,
        sig_config: SignalingConfig,
        cancel_token: &CancellationToken,
        hub: &ReverseSessionHub,
        coord_target: &ReverseIdleTarget,
        router: &SignalingRouter,
        sig_rx: &SignalingReceiver,
        grace_handle: &mut Option<GraceHandle>,
        grace_deadline: &mut Option<Pin<Box<Sleep>>>,
    ) -> bool {
        if cancel_token.is_cancelled() {
            return false;
        }
        if router.promote_standby_if_closed().await {
            return true;
        }
        if grace_handle.is_none() {
            *grace_handle = Some(hub.register_grace_listener(protocol, listen_addr));
        }

        // Generalized 0xA6 promotion extracting an Idle transport straight from Coordinator
        // when no dedicated standby channel exists (single-listener mode without shared tunnel_id).
        if tunnel_id.is_none()
            && let Ok(Some(mut transport)) = coord_target.coord.take_idle_transport().await
        {
            let write_fut = async {
                transport
                    .stream
                    .write_all(&[ACTIVATE_STREAM_PROMOTE])
                    .await?;
                transport.stream.flush().await
            };
            if tokio::time::timeout(reverse_frame_timeout(idle_timeout), write_fut)
                .await
                .is_ok_and(|r| r.is_ok())
                && let Some(sink) = sig_rx.event_sink()
            {
                let promoted_handle = SignalingChannel::spawn_with_event_sink(
                    transport.stream,
                    sig_config,
                    sink,
                    cancel_token.child_token(),
                );
                router.attach(promoted_handle);
                if let Some(g) = grace_handle.take() {
                    g.finish();
                }
                *grace_deadline = None;
                return true;
            }
        }

        if grace_deadline.is_none() {
            *grace_deadline = Some(Box::pin(tokio::time::sleep(grace_period)));
        }
        true
    }

    async fn read_initial_register(
        ctrl_stream: &mut FramedControlStream<Box<dyn TunnelStream>>,
        idle_timeout: Duration,
        cancel_token: &CancellationToken,
    ) -> Option<(String, String, Option<usize>, L4Protocol, Option<String>)> {
        let deadline = Instant::now() + idle_timeout;
        let frame_timeout = reverse_frame_timeout(idle_timeout);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return None,
                frame = tokio::time::timeout(remaining, ctrl_stream.next()) => {
                    match frame {
                        Ok(Some(Ok(ControlMessage::RegisterReverse {
                            listen_addr,
                            target_addr,
                            pool_size,
                            protocol,
                            tunnel_id,
                        }))) => {
                            return Some((listen_addr, target_addr, pool_size, protocol, tunnel_id));
                        }
                        Ok(Some(Ok(ControlMessage::Ping))) => {
                            if tokio::time::timeout(frame_timeout, ctrl_stream.send(ControlMessage::Pong))
                                .await
                                .map(|r| r.is_ok())
                                != Ok(true)
                            {
                                return None;
                            }
                        }
                        _ => return None,
                    }
                }
            }
        }
    }
}
