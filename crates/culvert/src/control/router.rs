// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Lock-free Actor-based `SignalingRouter` with single-owner state, non-blocking send
//! dispatch, strict FIFO ordering, channel-instance failure isolation, and single failover.
//! All comments and docstrings must be in English.

use std::collections::VecDeque;
use std::future::pending;
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::control::channel::SignalingHandle;
use crate::control::stream::ControlMessage;
use crate::core::error::TunnelError;

enum InFlightEvent {
    CallerCancelled,
    Finished(Result<Result<(), TunnelError>, tokio::task::JoinError>),
}

enum RouterCommand {
    Send {
        message: ControlMessage,
        timeout: Option<Duration>,
        reply: oneshot::Sender<Result<(), TunnelError>>,
    },
    Attach {
        handle: SignalingHandle,
    },
    Detach,
    HasActiveChannel {
        reply: oneshot::Sender<bool>,
    },
    PromoteIfClosed {
        reply: oneshot::Sender<bool>,
    },
}

#[derive(Clone)]
struct ChannelSlot {
    id: u64,
    handle: SignalingHandle,
}

struct PendingSend {
    message: ControlMessage,
    timeout: Option<Duration>,
    reply: oneshot::Sender<Result<(), TunnelError>>,
    retried_on_failover: bool,
}

struct InFlightSend {
    channel_id: u64,
    pending: PendingSend,
    wire_cancel: CancellationToken,
    cancelled_by_caller: bool,
    task: JoinHandle<Result<(), TunnelError>>,
}

struct RouterState {
    primary: Option<ChannelSlot>,
    standby: Option<ChannelSlot>,
    next_channel_id: u64,
    epoch: u64,
}

impl RouterState {
    fn new(primary: SignalingHandle) -> Self {
        Self {
            primary: Some(ChannelSlot {
                id: 1,
                handle: primary,
            }),
            standby: None,
            next_channel_id: 2,
            epoch: 1,
        }
    }

    fn alloc_channel_id(&mut self) -> u64 {
        let id = self.next_channel_id;
        self.next_channel_id = self.next_channel_id.saturating_add(1);
        id
    }

    fn prune_closed_handles(&mut self) {
        if self.primary.as_ref().is_some_and(|c| c.handle.is_closed()) {
            self.primary = None;
        }
        if self.standby.as_ref().is_some_and(|c| c.handle.is_closed()) {
            self.standby = None;
        }
    }

    fn attach(&mut self, handle: SignalingHandle) {
        self.prune_closed_handles();
        let id = self.alloc_channel_id();
        if self.primary.is_none() {
            self.primary = Some(ChannelSlot { id, handle });
            self.epoch = self.epoch.saturating_add(1);
        } else {
            self.standby = Some(ChannelSlot { id, handle });
        }
    }

    fn detach(&mut self) {
        self.primary = None;
        self.standby = None;
    }

    fn promote_standby_if_closed(&mut self) -> bool {
        if self.primary.as_ref().is_some_and(|c| !c.handle.is_closed()) {
            return true;
        }
        self.primary = None;
        if let Some(sec) = self.standby.take().filter(|c| !c.handle.is_closed()) {
            self.primary = Some(sec);
            self.epoch = self.epoch.saturating_add(1);
            true
        } else {
            false
        }
    }

    fn on_channel_failed(&mut self, failed_channel_id: u64) -> Option<u64> {
        if self
            .primary
            .as_ref()
            .is_some_and(|c| c.id == failed_channel_id)
        {
            self.primary = None;
            if self.promote_standby_if_closed() {
                return self.primary.as_ref().map(|c| c.id);
            }
        } else if self
            .standby
            .as_ref()
            .is_some_and(|c| c.id == failed_channel_id)
        {
            self.standby = None;
        }
        self.primary
            .as_ref()
            .filter(|c| !c.handle.is_closed() && c.id != failed_channel_id)
            .map(|c| c.id)
    }

    fn has_active_channel(&mut self) -> bool {
        self.prune_closed_handles();
        self.primary.is_some() || self.standby.is_some()
    }
}

struct SignalingRouterActor {
    state: RouterState,
    cmd_rx: mpsc::UnboundedReceiver<RouterCommand>,
    pending_sends: VecDeque<PendingSend>,
    in_flight: Option<InFlightSend>,
    epoch_tx: watch::Sender<u64>,
    attach_notify: ArcNotify,
}

type ArcNotify = std::sync::Arc<Notify>;

impl SignalingRouterActor {
    fn publish_epoch(&self) {
        self.epoch_tx.send_if_modified(|cur| {
            if *cur == self.state.epoch {
                false
            } else {
                *cur = self.state.epoch;
                true
            }
        });
    }

    async fn poll_slot_closed(slot: &Option<ChannelSlot>) {
        match slot {
            Some(c) => c.handle.closed().await,
            None => pending::<()>().await,
        }
    }

    async fn poll_in_flight_event(in_flight: &mut Option<InFlightSend>) -> InFlightEvent {
        let Some(active) = in_flight.as_mut() else {
            return pending().await;
        };
        let watch_cancel = !active.cancelled_by_caller;
        let reply = &mut active.pending.reply;
        let task = &mut active.task;
        tokio::select! {
            biased;
            () = reply.closed(), if watch_cancel => InFlightEvent::CallerCancelled,
            res = task => InFlightEvent::Finished(res),
        }
    }

    fn dispatch_next_if_idle(&mut self) {
        if self.in_flight.is_some() {
            return;
        }
        while let Some(req) = self.pending_sends.pop_front() {
            if req.reply.is_closed() {
                continue;
            }
            if !self.state.promote_standby_if_closed() {
                self.publish_epoch();
                let _ = req.reply.send(Err(TunnelError::ControlProtocol(
                    "No signaling channels available".into(),
                )));
                continue;
            }
            let Some(ref slot) = self.state.primary else {
                continue;
            };
            let channel_id = slot.id;
            let handle = slot.handle.clone();
            let msg = req.message.clone();
            let explicit_timeout = req.timeout;
            let wire_cancel = CancellationToken::new();
            let task_cancel = wire_cancel.clone();

            let task = tokio::spawn(async move {
                handle
                    .send_cancellable(msg, explicit_timeout, task_cancel)
                    .await
            });
            self.publish_epoch();
            self.in_flight = Some(InFlightSend {
                channel_id,
                pending: req,
                wire_cancel,
                cancelled_by_caller: false,
                task,
            });
            break;
        }
    }

    fn handle_in_flight_completion(
        &mut self,
        res: Result<Result<(), TunnelError>, tokio::task::JoinError>,
    ) {
        let Some(mut completed) = self.in_flight.take() else {
            return;
        };
        let outcome = match res {
            Ok(inner) => inner,
            Err(join_err) => Err(TunnelError::ControlProtocol(format!(
                "Signaling dispatch task failed: {join_err}"
            ))),
        };

        match outcome {
            Ok(()) => {
                let _ = completed.pending.reply.send(Ok(()));
            }
            Err(TunnelError::Shutdown) if completed.cancelled_by_caller => {
                // Pre-commit cancellation avoided wire transmission; channel remains healthy.
            }
            Err(err) => {
                let had_standby_before = self.state.standby.is_some();
                let replacement_id = self.state.on_channel_failed(completed.channel_id);
                if had_standby_before && replacement_id.is_some() {
                    warn!("Primary channel send failed: {}; promoting standby", err);
                }
                self.publish_epoch();

                if !completed.pending.retried_on_failover
                    && replacement_id.is_some()
                    && !completed.pending.reply.is_closed()
                {
                    completed.pending.retried_on_failover = true;
                    self.pending_sends.push_front(completed.pending);
                } else {
                    let reply_err =
                        if replacement_id.is_none() && !completed.pending.retried_on_failover {
                            TunnelError::ControlProtocol("No signaling channels available".into())
                        } else {
                            err
                        };
                    let _ = completed.pending.reply.send(Err(reply_err));
                }
            }
        }
        self.dispatch_next_if_idle();
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                () = Self::poll_slot_closed(&self.state.primary), if self.state.primary.is_some() => {
                    self.state.promote_standby_if_closed();
                    self.publish_epoch();
                    self.dispatch_next_if_idle();
                }
                () = Self::poll_slot_closed(&self.state.standby), if self.state.standby.is_some() => {
                    self.state.prune_closed_handles();
                }
                evt = Self::poll_in_flight_event(&mut self.in_flight), if self.in_flight.is_some() => {
                    match evt {
                        InFlightEvent::CallerCancelled => {
                            if let Some(ref mut active) = self.in_flight {
                                active.cancelled_by_caller = true;
                                active.wire_cancel.cancel();
                            }
                        }
                        InFlightEvent::Finished(res) => {
                            self.handle_in_flight_completion(res);
                        }
                    }
                }
                cmd_opt = self.cmd_rx.recv() => {
                    let Some(cmd) = cmd_opt else {
                        break;
                    };
                    match cmd {
                        RouterCommand::Send { message, timeout, reply } => {
                            if !reply.is_closed() {
                                self.pending_sends.push_back(PendingSend {
                                    message,
                                    timeout,
                                    reply,
                                    retried_on_failover: false,
                                });
                                self.dispatch_next_if_idle();
                            }
                        }
                        RouterCommand::Attach { handle } => {
                            self.state.attach(handle);
                            self.publish_epoch();
                            self.attach_notify.notify_waiters();
                            self.dispatch_next_if_idle();
                        }
                        RouterCommand::Detach => {
                            self.state.detach();
                        }
                        RouterCommand::HasActiveChannel { reply } => {
                            let active = self.state.has_active_channel();
                            let _ = reply.send(active);
                        }
                        RouterCommand::PromoteIfClosed { reply } => {
                            let promoted = self.state.promote_standby_if_closed();
                            self.publish_epoch();
                            let _ = reply.send(promoted);
                            self.dispatch_next_if_idle();
                        }
                    }
                }
            }
        }

        if let Some(in_flight) = self.in_flight.take() {
            in_flight.task.abort();
        }
    }
}

/// Multi-channel signaling router Actor providing active-standby failover and single-flight dispatch.
///
/// Routing state (`primary`, `standby`, `epoch`) is owned exclusively by a single background
/// Actor task with zero internal locks. Concurrent `send` calls preserve FIFO order without
/// blocking `attach` or `detach` commands, and stale failures from superseded channels cannot
/// clear a newly promoted primary.
#[derive(Clone)]
pub struct SignalingRouter {
    cmd_tx: mpsc::UnboundedSender<RouterCommand>,
    epoch_rx: watch::Receiver<u64>,
    attach_notify: ArcNotify,
}

impl SignalingRouter {
    /// Creates a router Actor with an initial primary channel.
    pub fn new(primary: SignalingHandle) -> Self {
        let initial_state = RouterState::new(primary);
        let initial_epoch = initial_state.epoch;
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (epoch_tx, epoch_rx) = watch::channel(initial_epoch);
        let attach_notify = std::sync::Arc::new(Notify::new());

        let actor = SignalingRouterActor {
            state: initial_state,
            cmd_rx,
            pending_sends: VecDeque::new(),
            in_flight: None,
            epoch_tx,
            attach_notify: std::sync::Arc::clone(&attach_notify),
        };
        tokio::spawn(actor.run());

        Self {
            cmd_tx,
            epoch_rx,
            attach_notify,
        }
    }

    /// Attaches an additional channel as standby (or as primary if none exists or primary is closed).
    pub fn attach(&self, handle: SignalingHandle) {
        let _ = self.cmd_tx.send(RouterCommand::Attach { handle });
    }

    /// Detaches any currently held primary and standby channels from active routing.
    pub fn detach(&self) {
        let _ = self.cmd_tx.send(RouterCommand::Detach);
    }

    /// Returns a future resolved when a new channel is attached and visible in router state.
    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.attach_notify.notified()
    }

    /// Returns the active epoch generation counter.
    pub fn epoch(&self) -> u64 {
        *self.epoch_rx.borrow()
    }

    /// Checks if at least one live (non-closed) channel is attached in the Actor.
    pub async fn has_active_channel(&self) -> bool {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(RouterCommand::HasActiveChannel { reply: reply_tx })
            .is_err()
        {
            return false;
        }
        reply_rx.await.unwrap_or(false)
    }

    /// Promotes a live standby channel to primary if primary is missing or closed.
    ///
    /// Returns `true` if a live primary channel is available after promotion.
    pub async fn promote_standby_if_closed(&self) -> bool {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(RouterCommand::PromoteIfClosed { reply: reply_tx })
            .is_err()
        {
            return false;
        }
        reply_rx.await.unwrap_or(false)
    }

    /// Sends a message using the active primary channel, with automatic single failover to standby.
    pub async fn send(&self, msg: ControlMessage) -> Result<(), TunnelError> {
        self.send_inner(msg, None).await
    }

    /// Sends a message with an explicit timeout.
    pub async fn send_timeout(
        &self,
        msg: ControlMessage,
        timeout: Duration,
    ) -> Result<(), TunnelError> {
        self.send_inner(msg, Some(timeout)).await
    }

    async fn send_inner(
        &self,
        msg: ControlMessage,
        explicit_timeout: Option<Duration>,
    ) -> Result<(), TunnelError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(RouterCommand::Send {
                message: msg,
                timeout: explicit_timeout,
                reply: reply_tx,
            })
            .map_err(|_| TunnelError::ControlProtocol("Signaling router closed".into()))?;

        reply_rx
            .await
            .map_err(|_| TunnelError::ControlProtocol("Signaling router response dropped".into()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::SignalingConfig;
    use crate::control::channel::SignalingChannel;
    use crate::control::stream::FramedControlStream;
    use futures_util::StreamExt;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, duplex};
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    struct StallWriteStream {
        allow_flush: std::sync::Arc<std::sync::atomic::AtomicBool>,
        drop_on_flush: bool,
        waker: std::sync::Arc<std::sync::Mutex<Option<std::task::Waker>>>,
    }

    impl AsyncRead for StallWriteStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    impl AsyncWrite for StallWriteStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if self.allow_flush.load(std::sync::atomic::Ordering::Acquire) {
                if self.drop_on_flush {
                    std::task::Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "late flush failure",
                    )))
                } else {
                    std::task::Poll::Ready(Ok(()))
                }
            } else {
                if let Ok(mut slot) = self.waker.lock() {
                    *slot = Some(cx.waker().clone());
                }
                std::task::Poll::Pending
            }
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn router_actor_concurrent_sends_preserve_fifo_order_and_single_failover() {
        let (ch1_tx, ch1_rx) = duplex(4096);
        let (ch2_tx, ch2_rx) = duplex(4096);
        let cancel = CancellationToken::new();
        let cfg = SignalingConfig {
            heartbeat_interval: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
        };

        let (h1, _rx1) = SignalingChannel::spawn(ch1_tx, cfg, cancel.child_token());
        let (h2, _rx2) = SignalingChannel::spawn(ch2_tx, cfg, cancel.child_token());
        let router = SignalingRouter::new(h1);
        router.attach(h2);

        drop(ch1_rx);
        let mut peer2 = FramedControlStream::new(ch2_rx);

        let f1 = router.send(ControlMessage::NewConnection {
            session_id: "ord-1".into(),
            client_addr: "10.0.0.1:1".into(),
        });
        let f2 = router.send(ControlMessage::NewConnection {
            session_id: "ord-2".into(),
            client_addr: "10.0.0.1:2".into(),
        });
        let f3 = router.send(ControlMessage::NewConnection {
            session_id: "ord-3".into(),
            client_addr: "10.0.0.1:3".into(),
        });

        let (r1, r2, r3) = tokio::join!(f1, f2, f3);
        r1.unwrap();
        r2.unwrap();
        r3.unwrap();
        assert_eq!(router.epoch(), 2);

        for expected in ["ord-1", "ord-2", "ord-3"] {
            let msg = timeout(Duration::from_millis(500), peer2.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(
                msg,
                ControlMessage::NewConnection {
                    session_id: expected.into(),
                    client_addr: format!("10.0.0.1:{}", expected.chars().last().unwrap()),
                }
            );
        }

        cancel.cancel();
    }

    #[tokio::test]
    async fn router_actor_attach_does_not_block_behind_stalled_send() {
        let cancel = CancellationToken::new();
        let cfg = SignalingConfig {
            heartbeat_interval: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
        };

        let stalled = StallWriteStream {
            allow_flush: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            drop_on_flush: false,
            waker: std::sync::Arc::new(std::sync::Mutex::new(None)),
        };
        let (h1, _rx1) = SignalingChannel::spawn(stalled, cfg, cancel.child_token());
        let router = SignalingRouter::new(h1);

        // Start a send with a long timeout that stalls in SignalingChannel flush.
        let r_send = router.clone();
        let stalled_send = tokio::spawn(async move {
            r_send
                .send_timeout(
                    ControlMessage::NewConnection {
                        session_id: "stalled".into(),
                        client_addr: "127.0.0.1:1".into(),
                    },
                    Duration::from_secs(5),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Attaching a fresh standby while M1 is stalled must complete and notify immediately.
        let (ch2_tx, _ch2_rx) = duplex(1024);
        let (h2, _rx2) = SignalingChannel::spawn(ch2_tx, cfg, cancel.child_token());
        let mut notified = std::pin::pin!(router.notified());
        notified.as_mut().enable();
        router.attach(h2);

        timeout(Duration::from_millis(200), notified)
            .await
            .expect("attach must not block behind an in-flight stalled send");
        assert!(router.has_active_channel().await);

        stalled_send.abort();
        cancel.cancel();
    }

    #[tokio::test]
    async fn router_actor_late_send_failure_from_old_channel_does_not_clear_new_primary() {
        let cancel = CancellationToken::new();
        let cfg = SignalingConfig {
            heartbeat_interval: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
        };

        let allow_flush = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let waker = std::sync::Arc::new(std::sync::Mutex::new(None::<std::task::Waker>));
        let stalled = StallWriteStream {
            allow_flush: std::sync::Arc::clone(&allow_flush),
            drop_on_flush: true,
            waker: std::sync::Arc::clone(&waker),
        };
        let (h1, _rx1) = SignalingChannel::spawn(stalled, cfg, cancel.child_token());
        let router = SignalingRouter::new(h1);

        // Start a slow send on ch1 that stalls mid-flush.
        let r_send = router.clone();
        let slow_send = tokio::spawn(async move {
            r_send
                .send_timeout(
                    ControlMessage::NewConnection {
                        session_id: "old-chan-fail".into(),
                        client_addr: "127.0.0.1:9".into(),
                    },
                    Duration::from_secs(5),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Replace primary with healthy h3 BEFORE ch1 fails.
        let (ch3_tx, ch3_rx) = duplex(4096);
        let (h3, _rx3) = SignalingChannel::spawn(ch3_tx, cfg, cancel.child_token());
        router.detach();
        router.attach(h3);
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Unblock ch1 flush so it fails late with BrokenPipe AFTER h3 is already primary.
        allow_flush.store(true, std::sync::atomic::Ordering::Release);
        if let Some(w) = waker.lock().ok().and_then(|mut g| g.take()) {
            w.wake();
        }
        let _ = slow_send.await;

        // Verify h3 remains installed as primary and serves new sends cleanly.
        let mut peer3 = FramedControlStream::new(ch3_rx);
        router
            .send(ControlMessage::NewConnection {
                session_id: "new-chan-ok".into(),
                client_addr: "127.0.0.1:10".into(),
            })
            .await
            .expect("late failure from old channel must not clear newly attached primary");
        loop {
            let got = timeout(Duration::from_millis(500), peer3.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if matches!(&got, ControlMessage::NewConnection { session_id, .. } if session_id == "new-chan-ok")
            {
                break;
            }
        }
        cancel.cancel();
    }

    #[tokio::test]
    async fn router_actor_cancelled_send_future_keeps_state_and_order_consistent() {
        let cancel = CancellationToken::new();
        let cfg = SignalingConfig {
            heartbeat_interval: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
        };
        let (ch_tx, ch_rx) = duplex(4096);
        let (h1, _rx1) = SignalingChannel::spawn(ch_tx, cfg, cancel.child_token());
        let router = SignalingRouter::new(h1);
        let mut peer = FramedControlStream::new(ch_rx);

        // Queue M1 and drop its future immediately before it completes or gets awaited.
        {
            let fut_m2 = router.send(ControlMessage::NewConnection {
                session_id: "dropped-send".into(),
                client_addr: "127.0.0.1:20".into(),
            });
            drop(fut_m2);
        }

        router
            .send(ControlMessage::NewConnection {
                session_id: "kept-send".into(),
                client_addr: "127.0.0.1:21".into(),
            })
            .await
            .unwrap();

        let first = timeout(Duration::from_millis(500), peer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            first,
            ControlMessage::NewConnection {
                session_id: "kept-send".into(),
                client_addr: "127.0.0.1:21".into(),
            }
        );
        cancel.cancel();
    }
}
