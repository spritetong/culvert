// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Server-side Reverse session registry, real `RouterReverseSignaler`, and
//! supervised `ReverseControlSession` managing registration, 0-RTT inbound pool,
//! `0xA6` emergency promotion, and bounded grace-period reattachment.
//! All comments and docstrings must be in English.

use crate::consts::{
    ACTIVATE_STREAM_PROMOTE, DEFAULT_RELAY_DRAIN_TIMEOUT, DEFAULT_SESSION_PAIR_TIMEOUT,
    reverse_frame_timeout, reverse_grace_period, reverse_pool_soft_cap,
};
use crate::control::{
    ControlMessage, FramedControlStream, SignalingChannel, SignalingConfig, SignalingReceiver,
    SignalingRouter,
};
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::frontend;
use crate::io::addr::{L4Protocol, MappingSpec, PeerAddr, TargetAddr};
use crate::session::manager::canonical_reverse_key;
use crate::v2::error::TaskError;
use crate::v2::pool::{PoolConfig, PoolHandle, TransportPool};
use crate::v2::reverse::{ReverseServerRunner, ReverseSignaler, ReverseVisitorWorkflow};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::Sleep;
use tokio_util::sync::CancellationToken;

static NEXT_SESSION_SEQ: AtomicU64 = AtomicU64::new(1);

/// Reconnected control stream handed over to a grace-period listener.
struct ReattachPayload {
    ctrl_stream: Box<FramedControlStream<Box<dyn TunnelStream>>>,
}

struct GraceSlot {
    id: u64,
    tx: mpsc::Sender<ReattachPayload>,
    closed: Arc<Notify>,
}

struct TunnelPoolSlot {
    pool: PoolHandle,
    listeners: AtomicUsize,
}

/// Active grace-period registration preserving a bound reverse listener across brief disconnects.
struct GraceHandle {
    key: String,
    slot_id: u64,
    rx: mpsc::Receiver<ReattachPayload>,
    closed: Arc<Notify>,
    registry: Arc<DashMap<String, GraceSlot>>,
    finished: bool,
}

impl GraceHandle {
    async fn recv(&mut self) -> Option<ReattachPayload> {
        self.rx.recv().await
    }

    fn close_and_try_recv(&mut self) -> Option<ReattachPayload> {
        self.rx.close();
        self.rx.try_recv().ok()
    }

    fn finish(mut self) {
        self.cleanup();
    }

    fn cleanup(&mut self) {
        if !self.finished {
            self.finished = true;
            self.rx.close();
            self.registry
                .remove_if(&self.key, |_, slot| slot.id == self.slot_id);
            self.closed.notify_waiters();
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
    active: bool,
}

impl PendingSessionGuard {
    fn defuse(mut self) {
        self.active = false;
    }
}

impl Drop for PendingSessionGuard {
    fn drop(&mut self) {
        if self.active {
            self.hub.remove_session(&self.session_id);
        }
    }
}

struct RegisteredPoolGuard {
    hub: Arc<ReverseSessionHub>,
    protocol: L4Protocol,
    listen_addr: String,
    tunnel_id: Option<String>,
    pool: PoolHandle,
}

impl Drop for RegisteredPoolGuard {
    fn drop(&mut self) {
        self.hub.unregister_pool(
            self.protocol,
            &self.listen_addr,
            self.tunnel_id.as_deref(),
            &self.pool,
        );
    }
}

/// Thread-safe v2 server registry pairing inbound reverse data streams, routing
/// pre-warmed `PoolIdle` streams into `TransportPool` handles, and coordinating
/// grace-period control channel reattachment.
#[derive(Default)]
pub struct ReverseSessionHub {
    pending_sessions: DashMap<String, oneshot::Sender<Box<dyn TunnelStream>>>,
    reverse_pools: DashMap<String, PoolHandle>,
    tunnel_pools: DashMap<String, Arc<TunnelPoolSlot>>,
    grace_listeners: Arc<DashMap<String, GraceSlot>>,
    next_grace_id: AtomicU64,
}

impl ReverseSessionHub {
    /// Creates an empty `ReverseSessionHub`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a pending session ID with an RAII drop guard that cleans up on cancellation.
    fn register_guarded(
        self: &Arc<Self>,
        session_id: String,
    ) -> (
        oneshot::Receiver<Box<dyn TunnelStream>>,
        PendingSessionGuard,
    ) {
        let (tx, rx) = oneshot::channel();
        self.pending_sessions.insert(session_id.clone(), tx);
        let guard = PendingSessionGuard {
            hub: Arc::clone(self),
            session_id,
            active: true,
        };
        (rx, guard)
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

    /// Resolves the destination `PoolHandle` for an inbound `pool_idle` reverse stream
    /// without holding any map lock across `.await`.
    pub fn lookup_idle_pool(
        &self,
        tunnel_id: Option<&str>,
        protocol: L4Protocol,
        target: Option<&TargetAddr>,
    ) -> Option<PoolHandle> {
        if let Some(tid) = tunnel_id
            && let Some(slot) = self.tunnel_pools.get(tid)
        {
            return Some(slot.pool.clone());
        }
        let target_addr = target?;
        let key = canonical_reverse_key(protocol, &target_addr.to_string());
        self.reverse_pools
            .get(&key)
            .map(|entry| entry.value().clone())
    }

    /// Registers an active `PoolHandle` (or attaches to a shared `tunnel_id` pool slot)
    /// and returns it along with an RAII guard that unregisters on exit.
    fn register_or_join_pool(
        self: &Arc<Self>,
        protocol: L4Protocol,
        listen_addr: &str,
        tunnel_id: Option<String>,
        idle_timeout: Duration,
        soft_cap: usize,
        cancel_token: &CancellationToken,
    ) -> (
        PoolHandle,
        Option<tokio::task::JoinHandle<crate::v2::pool::PoolSummary>>,
        RegisteredPoolGuard,
    ) {
        if let Some(ref tid) = tunnel_id {
            let mut spawned_join = None;
            let slot = Arc::clone(&*self.tunnel_pools.entry(tid.clone()).or_insert_with(|| {
                let (pool, join) = TransportPool::start(
                    PoolConfig::reverse_inbound(soft_cap, idle_timeout),
                    cancel_token.child_token(),
                );
                spawned_join = Some(join);
                Arc::new(TunnelPoolSlot {
                    pool,
                    listeners: AtomicUsize::new(0),
                })
            }));
            slot.listeners.fetch_add(1, Ordering::AcqRel);
            let pool = slot.pool.clone();
            let guard = RegisteredPoolGuard {
                hub: Arc::clone(self),
                protocol,
                listen_addr: listen_addr.to_string(),
                tunnel_id: Some(tid.clone()),
                pool: pool.clone(),
            };
            (pool, spawned_join, guard)
        } else {
            let key = canonical_reverse_key(protocol, listen_addr);
            let (pool, join) = TransportPool::start(
                PoolConfig::reverse_inbound(soft_cap, idle_timeout),
                cancel_token.child_token(),
            );
            if let Some(old) = self.reverse_pools.insert(key, pool.clone()) {
                old.shutdown();
            }
            let guard = RegisteredPoolGuard {
                hub: Arc::clone(self),
                protocol,
                listen_addr: listen_addr.to_string(),
                tunnel_id: None,
                pool: pool.clone(),
            };
            (pool, Some(join), guard)
        }
    }

    fn unregister_pool(
        &self,
        protocol: L4Protocol,
        listen_addr: &str,
        tunnel_id: Option<&str>,
        pool: &PoolHandle,
    ) {
        if let Some(tid) = tunnel_id {
            let mut should_remove = false;
            if let Some(slot) = self.tunnel_pools.get(tid)
                && slot.listeners.fetch_sub(1, Ordering::AcqRel) == 1
            {
                should_remove = true;
            }
            if should_remove {
                self.tunnel_pools
                    .remove_if(tid, |_, slot| slot.listeners.load(Ordering::Acquire) == 0);
            }
        } else {
            let key = canonical_reverse_key(protocol, listen_addr);
            self.reverse_pools
                .remove_if(&key, |_, current| current.same_pool(pool));
        }
    }

    fn register_grace_listener(&self, protocol: L4Protocol, listen_addr: &str) -> GraceHandle {
        let key = canonical_reverse_key(protocol, listen_addr);
        let slot_id = self.next_grace_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(4);
        let closed = Arc::new(Notify::new());
        self.grace_listeners.insert(
            key.clone(),
            GraceSlot {
                id: slot_id,
                tx,
                closed: Arc::clone(&closed),
            },
        );
        GraceHandle {
            key,
            slot_id,
            rx,
            closed,
            registry: Arc::clone(&self.grace_listeners),
            finished: false,
        }
    }

    async fn try_reattach_grace_listener(
        &self,
        protocol: L4Protocol,
        listen_addr: &str,
        mut payload: ReattachPayload,
        cancel_token: &CancellationToken,
    ) -> Result<(), ReattachPayload> {
        let key = canonical_reverse_key(protocol, listen_addr);
        loop {
            let (tx, closed) = match self.grace_listeners.get(&key) {
                Some(entry) => (entry.tx.clone(), Arc::clone(&entry.closed)),
                None => return Err(payload),
            };
            match tx.send(payload).await {
                Ok(()) => return Ok(()),
                Err(send_err) => {
                    payload = send_err.0;
                    tokio::select! {
                        biased;
                        _ = cancel_token.cancelled() => return Err(payload),
                        _ = closed.notified() => {}
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
            if !router.has_active_channel() {
                disconnect_notify.notify_one();
                return Err(TaskError::StageFailed(
                    "reverse signaling channel unavailable".into(),
                ));
            }

            let session_id = format!(
                "v2_sess_{}_{}",
                std::process::id(),
                NEXT_SESSION_SEQ.fetch_add(1, Ordering::Relaxed)
            );
            let (pair_rx, guard) = hub.register_guarded(session_id.clone());

            let msg = ControlMessage::NewConnection {
                session_id: session_id.clone(),
                client_addr: visitor_peer.to_string(),
            };
            if let Err(err) = router.send(msg).await {
                if !err.to_string().contains("timed out") {
                    disconnect_notify.notify_one();
                }
                return Err(TaskError::StageFailed(format!(
                    "failed signaling NewConnection for {session_id}: {err}"
                )));
            }

            match tokio::time::timeout(pair_timeout, pair_rx).await {
                Ok(Ok(stream)) => {
                    guard.defuse();
                    Ok(stream)
                }
                Ok(Err(_)) => Err(TaskError::StageFailed(format!(
                    "reverse session {session_id} sender dropped"
                ))),
                Err(_) => Err(TaskError::StageFailed(format!(
                    "timed out waiting for reverse session {session_id}"
                ))),
            }
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

        let soft_cap = reverse_pool_soft_cap(pool_size);
        let (pool_handle, pool_join, _pool_guard) = hub.register_or_join_pool(
            protocol,
            &listen_addr,
            tunnel_id.clone(),
            idle_timeout,
            soft_cap,
            &cancel_token,
        );

        let sig_config = SignalingConfig {
            heartbeat_interval,
            idle_timeout,
        };
        let (signaling, mut sig_rx) =
            SignalingChannel::spawn_framed(ctrl_stream, sig_config, cancel_token.child_token());
        let router = SignalingRouter::new(signaling);

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
            if let Some(join) = pool_join {
                pool_handle.shutdown();
                let _ = join.await;
            }
            return;
        }

        let relay_idle_timeout = match protocol {
            L4Protocol::Tcp => idle_timeout,
            L4Protocol::Udp => udp_idle_timeout,
        };
        let disconnect_notify = Arc::new(Notify::new());
        let signaler = Arc::new(RouterReverseSignaler::new(
            router.clone(),
            Arc::clone(&hub),
            DEFAULT_SESSION_PAIR_TIMEOUT,
            Arc::clone(&disconnect_notify),
        ));
        let Ok(target_parsed) = crate::io::addr::parse_target_addr(&target_addr) else {
            fe.shutdown().await;
            drop(fe);
            if let Some(join) = pool_join {
                pool_handle.shutdown();
                let _ = join.await;
            }
            return;
        };

        let runner = ReverseServerRunner::new(
            fe,
            target_parsed,
            protocol,
            ReverseVisitorWorkflow::new(
                (soft_cap > 0).then_some(pool_handle.clone()),
                signaler,
                heartbeat_interval,
                relay_idle_timeout,
                DEFAULT_RELAY_DRAIN_TIMEOUT,
            ),
            pool_join.map(|j| (pool_handle.clone(), j)),
            DEFAULT_RELAY_DRAIN_TIMEOUT,
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
                    // ReverseServerRunner exited prematurely; tear down control session cleanly.
                    return;
                }
                Some(reattach) = Self::poll_grace_reattach(&mut grace_handle), if grace_handle.is_some() => {
                    if let Some(g) = grace_handle.take() {
                        g.finish();
                    }
                    grace_deadline = None;
                    let _ = pool_handle.clear_idle().await;
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
                        let _ = pool_handle.clear_idle().await;
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
                    if !router.has_active_channel()
                        && !Self::recover_or_enter_grace(
                            &listen_addr,
                            protocol,
                            tunnel_id.as_deref(),
                            idle_timeout,
                            grace_period,
                            sig_config,
                            &cancel_token,
                            &hub,
                            &pool_handle,
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
                                &pool_handle,
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
        pool_handle: &PoolHandle,
        router: &SignalingRouter,
        sig_rx: &SignalingReceiver,
        grace_handle: &mut Option<GraceHandle>,
        grace_deadline: &mut Option<Pin<Box<Sleep>>>,
    ) -> bool {
        if cancel_token.is_cancelled() {
            return false;
        }
        if router.promote_standby_if_closed() {
            return true;
        }
        if grace_handle.is_none() {
            *grace_handle = Some(hub.register_grace_listener(protocol, listen_addr));
        }

        // Generalized 0xA6 promotion directly from the Tunnel's PassiveInbound PoolHandle
        // when no dedicated standby channel exists (single-listener mode without shared tunnel_id).
        if tunnel_id.is_none()
            && let Ok(Some(mut lease)) = pool_handle.try_acquire().await
        {
            let write_fut = async {
                lease.write_all(&[ACTIVATE_STREAM_PROMOTE]).await?;
                lease.flush().await
            };
            if tokio::time::timeout(reverse_frame_timeout(idle_timeout), write_fut)
                .await
                .is_ok_and(|r| r.is_ok())
                && let Some(sink) = sig_rx.event_sink()
            {
                let promoted_handle = SignalingChannel::spawn_with_event_sink(
                    Box::new(lease),
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
