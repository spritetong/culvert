// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Decoupled signaling channel actor and handles for reverse tunnel control.
//! All comments and docstrings must be in English.

use futures_util::{SinkExt, StreamExt};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::{Instant, interval, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::common::CancelFutureExt;
use crate::consts::{
    DEFAULT_DEDUPLICATION_WINDOW_CAPACITY, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT,
    reverse_frame_timeout,
};
use crate::control::stream::{ControlMessage, FramedControlStream};
use crate::core::error::TunnelError;
use crate::io::stream::{activity_now_ms, heartbeat_timing};

/// Configuration parameters for a signaling session.
#[derive(Debug, Clone, Copy)]
pub struct SignalingConfig {
    pub heartbeat_interval: Duration,
    pub idle_timeout: Duration,
}

impl Default for SignalingConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
        }
    }
}

struct OutboundCommand {
    msg: ControlMessage,
    tx: oneshot::Sender<Result<(), TunnelError>>,
    timeout: Duration,
}

/// Cloned handle used by business logic to transmit control commands asynchronously.
#[derive(Clone)]
pub struct SignalingHandle {
    cmd_tx: mpsc::Sender<OutboundCommand>,
    default_frame_timeout: Duration,
}

impl SignalingHandle {
    /// Returns `true` when the backing signaling actor has exited.
    pub fn is_closed(&self) -> bool {
        self.cmd_tx.is_closed()
    }

    /// Transmits a control message using the session's derived frame timeout.
    pub async fn send(&self, msg: ControlMessage) -> Result<(), TunnelError> {
        self.send_timeout(msg, self.default_frame_timeout).await
    }

    /// Transmits a control message with an explicit bounded timeout.
    pub async fn send_timeout(
        &self,
        msg: ControlMessage,
        send_timeout: Duration,
    ) -> Result<(), TunnelError> {
        let (tx, rx) = oneshot::channel();
        let cmd = OutboundCommand {
            msg,
            tx,
            timeout: send_timeout,
        };
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| TunnelError::ControlProtocol("Signaling channel closed".into()))?;

        rx.await
            .map_err(|_| TunnelError::ControlProtocol("Signaling response dropped".into()))?
    }
}

/// Bounded sliding-window deduplication filter ensuring idempotent command execution across dual channels.
#[derive(Debug, Clone)]
pub struct CommandDeduplicator {
    seen: HashSet<String>,
    queue: VecDeque<String>,
    capacity: usize,
}

impl CommandDeduplicator {
    /// Creates a deduplicator with the specified capacity limit.
    pub fn new(capacity: usize) -> Self {
        Self {
            seen: HashSet::with_capacity(capacity),
            queue: VecDeque::with_capacity(capacity),
            capacity: capacity.max(1),
        }
    }

    /// Checks if an identifier has been processed. Returns `true` if new (unique), or `false` if duplicate.
    pub fn check_and_record(&mut self, id: &str) -> bool {
        if self.seen.contains(id) {
            return false;
        }
        if self.queue.len() >= self.capacity
            && let Some(oldest) = self.queue.pop_front()
        {
            self.seen.remove(&oldest);
        }
        self.seen.insert(id.to_string());
        self.queue.push_back(id.to_string());
        true
    }

    /// Clears the history buffer.
    pub fn clear(&mut self) {
        self.seen.clear();
        self.queue.clear();
    }

    /// Number of tracked IDs currently in the sliding window.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Whether the sliding window is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

impl Default for CommandDeduplicator {
    fn default() -> Self {
        Self::new(DEFAULT_DEDUPLICATION_WINDOW_CAPACITY)
    }
}

/// Multi-channel signaling router providing active-standby failover and single-flight dispatch.
///
/// Guarantees that at any given moment, commands are transmitted strictly over ONE channel,
/// eliminating duplicate message dispatch. If the primary channel drops, it atomically
/// promotes the standby channel and increments the generation epoch.
#[derive(Clone)]
pub struct SignalingRouter {
    primary: Arc<parking_lot::Mutex<Option<SignalingHandle>>>,
    standby: Arc<parking_lot::Mutex<Option<SignalingHandle>>>,
    epoch: Arc<AtomicU64>,
    attach_notify: Arc<Notify>,
}

impl SignalingRouter {
    /// Creates a router with an initial primary channel.
    pub fn new(primary: SignalingHandle) -> Self {
        Self {
            primary: Arc::new(parking_lot::Mutex::new(Some(primary))),
            standby: Arc::new(parking_lot::Mutex::new(None)),
            epoch: Arc::new(AtomicU64::new(1)),
            attach_notify: Arc::new(Notify::new()),
        }
    }

    /// Attaches an additional channel as standby (or as primary if none exists or primary is closed).
    pub fn attach(&self, handle: SignalingHandle) {
        {
            let mut prim = self.primary.lock();
            if prim.as_ref().is_none_or(|h| h.is_closed()) {
                *prim = Some(handle);
                self.epoch.fetch_add(1, Ordering::SeqCst);
            } else {
                let mut sec = self.standby.lock();
                *sec = Some(handle);
            }
        }
        self.attach_notify.notify_waiters();
    }

    /// Returns a future resolved when a new channel is attached to the router.
    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.attach_notify.notified()
    }

    /// Returns the active epoch generation counter.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Checks if at least one live (non-closed) channel is attached.
    pub fn has_active_channel(&self) -> bool {
        self.primary.lock().as_ref().is_some_and(|h| !h.is_closed())
            || self.standby.lock().as_ref().is_some_and(|h| !h.is_closed())
    }

    /// Promotes a live standby channel to primary if primary is missing or closed.
    ///
    /// Returns `true` if a live primary channel is available after promotion.
    pub fn promote_standby_if_closed(&self) -> bool {
        let mut prim = self.primary.lock();
        if prim.as_ref().is_some_and(|h| !h.is_closed()) {
            return true;
        }
        let next = {
            let mut sec = self.standby.lock();
            sec.take().filter(|h| !h.is_closed())
        };
        if let Some(handle) = next {
            *prim = Some(handle);
            self.epoch.fetch_add(1, Ordering::SeqCst);
            true
        } else {
            *prim = None;
            false
        }
    }

    /// Sends a message using the active primary channel, with automatic failover to standby.
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
        let primary = { self.primary.lock().clone() };
        if let Some(ref handle) = primary {
            if !handle.is_closed() {
                let res = match explicit_timeout {
                    Some(t) => handle.send_timeout(msg.clone(), t).await,
                    None => handle.send(msg.clone()).await,
                };
                match res {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        warn!("Primary channel send failed: {}; promoting standby", e);
                        let mut p = self.primary.lock();
                        if p.as_ref()
                            .is_some_and(|cur| cur.cmd_tx.same_channel(&handle.cmd_tx))
                        {
                            *p = None;
                        }
                    }
                }
            } else {
                let mut p = self.primary.lock();
                if p.as_ref()
                    .is_some_and(|cur| cur.cmd_tx.same_channel(&handle.cmd_tx))
                {
                    *p = None;
                }
            }
        }

        // Failover: promote standby to primary
        let standby = {
            let mut sec = self.standby.lock();
            sec.take().filter(|h| !h.is_closed())
        };

        if let Some(new_primary) = standby {
            self.epoch.fetch_add(1, Ordering::SeqCst);
            let res = match explicit_timeout {
                Some(t) => new_primary.send_timeout(msg, t).await,
                None => new_primary.send(msg).await,
            };
            if res.is_ok() {
                let mut p = self.primary.lock();
                if p.as_ref().is_none_or(|h| h.is_closed()) {
                    *p = Some(new_primary);
                }
            }
            res
        } else {
            Err(TunnelError::ControlProtocol(
                "No signaling channels available".into(),
            ))
        }
    }
}

/// Clonable sink used to attach additional physical channels to an existing `SignalingReceiver`.
#[derive(Clone)]
pub struct SignalingSink {
    tx: mpsc::Sender<Result<ControlMessage, TunnelError>>,
    active_actors: Arc<AtomicUsize>,
    actor_exit_notify: Arc<Notify>,
}

/// Receiver yielding incoming application-level control messages with automatic deduplication.
pub struct SignalingReceiver {
    rx: mpsc::Receiver<Result<ControlMessage, TunnelError>>,
    event_tx: mpsc::Sender<Result<ControlMessage, TunnelError>>,
    active_actors: Arc<AtomicUsize>,
    actor_exit_notify: Arc<Notify>,
    dedup: CommandDeduplicator,
}

impl SignalingReceiver {
    /// Creates an event sink to attach additional physical channels into this receiver.
    ///
    /// Keeping `event_tx` owned by `SignalingReceiver` allows new promoted or reattached
    /// signaling actors to attach even across temporary gaps where zero actors are active.
    pub fn event_sink(&self) -> Option<SignalingSink> {
        Some(SignalingSink {
            tx: self.event_tx.clone(),
            active_actors: Arc::clone(&self.active_actors),
            actor_exit_notify: Arc::clone(&self.actor_exit_notify),
        })
    }

    /// Sets the deduplication history window capacity.
    pub fn with_dedup_capacity(mut self, capacity: usize) -> Self {
        self.dedup = CommandDeduplicator::new(capacity);
        self
    }

    async fn recv_raw(&mut self) -> Option<Result<ControlMessage, TunnelError>> {
        loop {
            if let Ok(res) = self.rx.try_recv() {
                return Some(res);
            }

            if self.active_actors.load(Ordering::SeqCst) == 0 {
                return None;
            }

            let notified = self.actor_exit_notify.notified();
            tokio::pin!(notified);

            if self.active_actors.load(Ordering::SeqCst) == 0 {
                return self.rx.try_recv().ok();
            }

            tokio::select! {
                biased;
                maybe_res = self.rx.recv() => {
                    return maybe_res;
                }
                _ = &mut notified => {
                    continue;
                }
            }
        }
    }

    /// Awaits the next application-level control message, automatically discarding duplicate commands.
    ///
    /// Heartbeat frames (`Ping` and `Pong`) are answered and accounted for
    /// automatically by the signaling actor and are never delivered here.
    pub async fn recv(&mut self) -> Option<Result<ControlMessage, TunnelError>> {
        loop {
            let res = self.recv_raw().await?;
            match res {
                Ok(ControlMessage::NewConnection { ref session_id, .. }) => {
                    if !self.dedup.check_and_record(session_id) {
                        debug!(session = %session_id, "Filtered duplicate NewConnection across channels");
                        continue;
                    }
                    return Some(res);
                }
                other => return Some(other),
            }
        }
    }

    /// Yields raw incoming events without applying deduplication filtering.
    pub async fn recv_unfiltered(&mut self) -> Option<Result<ControlMessage, TunnelError>> {
        self.recv_raw().await
    }

    /// Convenience helper yielding `Ok(Some(msg))`, `Ok(None)` on EOF/clean close, or `Err(e)`.
    pub async fn recv_msg(&mut self) -> Result<Option<ControlMessage>, TunnelError> {
        match self.recv().await {
            Some(Ok(msg)) => Ok(Some(msg)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

struct ActorDropGuard {
    active_actors: Arc<AtomicUsize>,
    actor_exit_notify: Arc<Notify>,
}

impl Drop for ActorDropGuard {
    fn drop(&mut self) {
        self.active_actors.fetch_sub(1, Ordering::SeqCst);
        self.actor_exit_notify.notify_waiters();
    }
}

/// Spawns an autonomous signaling actor managing transport framing, keepalive, and timeouts.
pub struct SignalingChannel;

impl SignalingChannel {
    /// Spawns the signaling actor over `stream` and returns its command handle and event receiver.
    pub fn spawn<S>(
        stream: S,
        config: SignalingConfig,
        cancel_token: CancellationToken,
    ) -> (SignalingHandle, SignalingReceiver)
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::spawn_framed(FramedControlStream::new(stream), config, cancel_token)
    }

    /// Spawns the signaling actor over an already-framed control stream.
    pub fn spawn_framed<S>(
        ctrl_stream: FramedControlStream<S>,
        config: SignalingConfig,
        cancel_token: CancellationToken,
    ) -> (SignalingHandle, SignalingReceiver)
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (event_tx, event_rx) = mpsc::channel::<Result<ControlMessage, TunnelError>>(32);
        let active_actors = Arc::new(AtomicUsize::new(0));
        let actor_exit_notify = Arc::new(Notify::new());
        let sink = SignalingSink {
            tx: event_tx.clone(),
            active_actors: Arc::clone(&active_actors),
            actor_exit_notify: Arc::clone(&actor_exit_notify),
        };
        let handle = Self::spawn_framed_with_event_sink(ctrl_stream, config, sink, cancel_token);
        let receiver = SignalingReceiver {
            rx: event_rx,
            event_tx,
            active_actors,
            actor_exit_notify,
            dedup: CommandDeduplicator::default(),
        };
        (handle, receiver)
    }

    /// Spawns a signaling actor attached to an existing shared event sink.
    pub fn spawn_with_event_sink<S>(
        stream: S,
        config: SignalingConfig,
        event_sink: SignalingSink,
        cancel_token: CancellationToken,
    ) -> SignalingHandle
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::spawn_framed_with_event_sink(
            FramedControlStream::new(stream),
            config,
            event_sink,
            cancel_token,
        )
    }

    /// Spawns a signaling actor from an already-framed control stream attached to an existing event sink.
    pub fn spawn_framed_with_event_sink<S>(
        ctrl_stream: FramedControlStream<S>,
        config: SignalingConfig,
        event_sink: SignalingSink,
        cancel_token: CancellationToken,
    ) -> SignalingHandle
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        event_sink.active_actors.fetch_add(1, Ordering::SeqCst);
        let guard = ActorDropGuard {
            active_actors: event_sink.active_actors,
            actor_exit_notify: event_sink.actor_exit_notify,
        };

        let frame_timeout = reverse_frame_timeout(config.idle_timeout);
        let (cmd_tx, cmd_rx) = mpsc::channel::<OutboundCommand>(32);

        let handle = SignalingHandle {
            cmd_tx,
            default_frame_timeout: frame_timeout,
        };

        tokio::spawn(Self::run_actor(
            ctrl_stream,
            config,
            frame_timeout,
            cmd_rx,
            event_sink.tx,
            guard,
            cancel_token,
        ));

        handle
    }

    async fn run_actor<S>(
        mut ctrl_stream: FramedControlStream<S>,
        config: SignalingConfig,
        frame_timeout: Duration,
        mut cmd_rx: mpsc::Receiver<OutboundCommand>,
        event_tx: mpsc::Sender<Result<ControlMessage, TunnelError>>,
        guard: ActorDropGuard,
        cancel_token: CancellationToken,
    ) where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let _guard = guard;
        let mut heartbeat = interval(config.heartbeat_interval);
        let mut last_activity_ms = activity_now_ms();
        let idle_deadline = sleep(config.idle_timeout);
        tokio::pin!(idle_deadline);

        loop {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    debug!("Signaling actor stopping on cancellation signal");
                    break;
                }
                _ = &mut idle_deadline => {
                    warn!(
                        idle_timeout = ?config.idle_timeout,
                        "Signaling channel idle timeout: no activity. Closing channel."
                    );
                    break;
                }
                cmd_opt = cmd_rx.recv() => {
                    match cmd_opt {
                        Some(cmd) => {
                            let send_res = timeout(cmd.timeout, ctrl_stream.send(cmd.msg))
                                .cancel_with(&cancel_token)
                                .await;
                            match send_res {
                                Some(Ok(Ok(()))) => {
                                    last_activity_ms = activity_now_ms();
                                    idle_deadline.as_mut().reset(Instant::now() + config.idle_timeout);
                                    let _ = cmd.tx.send(Ok(()));
                                }
                                Some(Ok(Err(e))) => {
                                    let _ = cmd.tx.send(Err(e));
                                    break;
                                }
                                Some(Err(_)) => {
                                    let err_msg = format!("Send timed out after {:?}", cmd.timeout);
                                    let _ = cmd.tx.send(Err(TunnelError::ControlProtocol(err_msg)));
                                    break;
                                }
                                None => break,
                            }
                        }
                        None => {
                            debug!("Signaling handle dropped; closing signaling actor");
                            break;
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    let (send_probe, _) = heartbeat_timing(
                        Some(last_activity_ms),
                        activity_now_ms(),
                        config.heartbeat_interval,
                        config.idle_timeout,
                    );
                    if send_probe {
                        let ping = timeout(frame_timeout, ctrl_stream.send(ControlMessage::Ping))
                            .cancel_with(&cancel_token)
                            .await;
                        match ping {
                            Some(Ok(Ok(()))) => {}
                            Some(Ok(Err(e))) => {
                                debug!("Signaling actor failed to send heartbeat ping: {}", e);
                                break;
                            }
                            Some(Err(_)) => {
                                warn!("Signaling actor heartbeat ping stalled; closing channel");
                                break;
                            }
                            None => break,
                        }
                    }
                }
                msg_opt = ctrl_stream.next() => {
                    match msg_opt {
                        Some(Ok(msg)) => {
                            last_activity_ms = activity_now_ms();
                            idle_deadline.as_mut().reset(Instant::now() + config.idle_timeout);
                            match msg {
                                ControlMessage::Ping => {
                                    let pong = timeout(frame_timeout, ctrl_stream.send(ControlMessage::Pong))
                                        .cancel_with(&cancel_token)
                                        .await;
                                    if !matches!(pong, Some(Ok(Ok(())))) {
                                        debug!("Signaling actor Pong stalled; closing channel");
                                        break;
                                    }
                                }
                                ControlMessage::Pong => {
                                    // Activity clock already refreshed above
                                }
                                app_msg => {
                                    if event_tx.send(Ok(app_msg)).await.is_err() {
                                        debug!("Signaling event receiver dropped; closing actor");
                                        break;
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            let is_last_actor = _guard.active_actors.load(Ordering::SeqCst) <= 1;
                            if is_last_actor {
                                let _ = event_tx.send(Err(e)).await;
                            } else {
                                warn!(error = %e, "Signaling transport failed while sibling channel remains active");
                            }
                            break;
                        }
                        None => {
                            debug!("Signaling transport disconnected by peer");
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_signaling_channel_autonomous_ping_pong() {
        let (client_raw, server_raw) = duplex(4096);
        let cancel = CancellationToken::new();

        let cfg = SignalingConfig {
            heartbeat_interval: Duration::from_millis(50),
            idle_timeout: Duration::from_millis(500),
        };

        // Spawn server as autonomous signaling channel
        let (_srv_handle, mut srv_rx) =
            SignalingChannel::spawn(server_raw, cfg, cancel.child_token());

        // Client sends Ping manually
        let mut client_framed = FramedControlStream::new(client_raw);
        client_framed.send(ControlMessage::Ping).await.unwrap();

        // Client should receive automatic Pong from server actor without server rx waking up
        let reply = client_framed.next().await.unwrap().unwrap();
        assert_eq!(reply, ControlMessage::Pong);

        // Application message should pass through to srv_rx
        client_framed
            .send(ControlMessage::NewConnection {
                session_id: "test_sess".into(),
                client_addr: "1.2.3.4:5678".into(),
            })
            .await
            .unwrap();

        let event = srv_rx.recv_msg().await.unwrap().unwrap();
        assert!(matches!(event, ControlMessage::NewConnection { .. }));

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_signaling_channel_idle_timeout() {
        let (_client_raw, server_raw) = duplex(4096);
        let cancel = CancellationToken::new();

        let cfg = SignalingConfig {
            heartbeat_interval: Duration::from_millis(100),
            idle_timeout: Duration::from_millis(100),
        };

        let (_srv_handle, mut srv_rx) =
            SignalingChannel::spawn(server_raw, cfg, cancel.child_token());

        // No activity for 100ms should trigger idle timeout and close receiver
        let res = timeout(Duration::from_millis(500), srv_rx.recv_msg()).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap().unwrap(), None);
    }

    #[test]
    fn test_command_deduplicator_sliding_window_and_eviction() {
        let mut dedup = CommandDeduplicator::new(3);
        assert_eq!(dedup.len(), 0);

        // Unique entries return true
        assert!(dedup.check_and_record("cmd_1"));
        assert!(dedup.check_and_record("cmd_2"));
        assert!(dedup.check_and_record("cmd_3"));
        assert_eq!(dedup.len(), 3);

        // Duplicate entries within window return false
        assert!(!dedup.check_and_record("cmd_1"));
        assert!(!dedup.check_and_record("cmd_2"));
        assert!(!dedup.check_and_record("cmd_3"));
        assert_eq!(dedup.len(), 3);

        // Fourth entry pushes out cmd_1
        assert!(dedup.check_and_record("cmd_4"));
        assert_eq!(dedup.len(), 3);

        // cmd_1 is now evicted and can be accepted again
        assert!(dedup.check_and_record("cmd_1"));
        // But cmd_4 and cmd_1 are present, cmd_2 was evicted
        assert!(!dedup.check_and_record("cmd_4"));
        assert!(!dedup.check_and_record("cmd_1"));
    }

    #[tokio::test]
    async fn test_dual_channel_router_single_flight_no_duplicate_dispatch() {
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
        assert_eq!(router.epoch(), 1);

        let mut peer1 = FramedControlStream::new(ch1_rx);
        let mut peer2 = FramedControlStream::new(ch2_rx);

        // Send NewConnection via router
        let msg = ControlMessage::NewConnection {
            session_id: "uniq_1".into(),
            client_addr: "10.0.0.1:1234".into(),
        };
        router.send(msg.clone()).await.unwrap();

        // Primary (ch1) receives the message
        let r1 = timeout(Duration::from_millis(500), peer1.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(r1, msg);

        // Standby (ch2) MUST NOT receive any message (strictly single-flight dispatch, no duplicates)
        let r2 = timeout(Duration::from_millis(100), peer2.next()).await;
        assert!(
            r2.is_err(),
            "Standby channel must not receive duplicate command dispatch"
        );

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_dual_channel_router_automatic_failover() {
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

        let peer1 = FramedControlStream::new(ch1_rx);
        let mut peer2 = FramedControlStream::new(ch2_rx);

        // Drop peer1 raw stream to cause primary channel send failure
        drop(peer1);

        // Send via router - should detect primary failure, failover to standby, and succeed
        let msg = ControlMessage::NewConnection {
            session_id: "failover_1".into(),
            client_addr: "10.0.0.2:2345".into(),
        };
        router.send(msg.clone()).await.unwrap();
        assert_eq!(router.epoch(), 2);

        // Standby (ch2) receives the command
        let r2 = timeout(Duration::from_millis(500), peer2.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(r2, msg);

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_dual_channel_receiver_deduplication_filters_identical_commands() {
        let (ch1_tx, ch1_rx) = duplex(4096);
        let (ch2_tx, ch2_rx) = duplex(4096);
        let cancel = CancellationToken::new();

        let cfg = SignalingConfig {
            heartbeat_interval: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
        };

        // Create receiver on Channel 1
        let (_h1, mut unified_rx) = SignalingChannel::spawn(ch1_rx, cfg, cancel.child_token());

        // Attach Channel 2 to the same unified receiver event sink
        let _h2 = SignalingChannel::spawn_with_event_sink(
            ch2_rx,
            cfg,
            unified_rx.event_sink().expect("event sink should be open"),
            cancel.child_token(),
        );

        let mut client1 = FramedControlStream::new(ch1_tx);
        let mut client2 = FramedControlStream::new(ch2_tx);

        // Send duplicate NewConnection frames across Channel 1 and Channel 2
        let dup_msg = ControlMessage::NewConnection {
            session_id: "dup_sess_999".into(),
            client_addr: "192.168.1.1:1111".into(),
        };
        let distinct_msg = ControlMessage::NewConnection {
            session_id: "uniq_sess_1000".into(),
            client_addr: "192.168.1.2:2222".into(),
        };

        // Channel 1 transmits dup_msg
        client1.send(dup_msg.clone()).await.unwrap();
        // Channel 2 ALSO transmits the exact same dup_msg (simulating retransmission / multi-channel race)
        client2.send(dup_msg.clone()).await.unwrap();
        // Channel 2 transmits distinct_msg
        client2.send(distinct_msg.clone()).await.unwrap();

        // 1st recv MUST be dup_msg
        let first = timeout(Duration::from_millis(500), unified_rx.recv_msg())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first, dup_msg);

        // 2nd recv MUST be distinct_msg (the duplicate dup_msg was completely filtered out!)
        let second = timeout(Duration::from_millis(500), unified_rx.recv_msg())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(second, distinct_msg);

        // No more messages in queue
        let third = timeout(Duration::from_millis(100), unified_rx.recv_msg()).await;
        assert!(third.is_err(), "Duplicate command must have been discarded");

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_signaling_receiver_survives_zero_actor_gap_and_primary_error() {
        let cancel = CancellationToken::new();
        let (ch1_tx, ch1_rx) = duplex(4096);
        let (ch2_tx, ch2_rx) = duplex(4096);

        let cfg = SignalingConfig {
            heartbeat_interval: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(30),
        };

        let (h1, mut unified_rx) = SignalingChannel::spawn(ch1_rx, cfg, cancel.child_token());
        let router = SignalingRouter::new(h1);
        assert!(router.has_active_channel());

        // Drop ch1_tx so primary exits with clean EOF -> zero active actors
        drop(ch1_tx);
        assert_eq!(
            timeout(Duration::from_millis(500), unified_rx.recv_msg())
                .await
                .unwrap()
                .unwrap(),
            None
        );

        // Even after dropping to zero actors, event_sink remains usable for recovery
        let sink = unified_rx
            .event_sink()
            .expect("event sink must remain valid across zero-actor gap");
        let h2 = SignalingChannel::spawn_with_event_sink(ch2_rx, cfg, sink, cancel.child_token());
        router.attach(h2);
        assert!(router.has_active_channel());

        // Attach a third channel carrying corrupted bytes; its decode error must not poison h2
        let (mut bad_tx, bad_rx) = duplex(4096);
        let bad_sink = unified_rx.event_sink().expect("sink");
        let _h3 =
            SignalingChannel::spawn_with_event_sink(bad_rx, cfg, bad_sink, cancel.child_token());

        tokio::io::AsyncWriteExt::write_all(&mut bad_tx, &[0, 0, 0, 2, 0xFF, 0xFF])
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::flush(&mut bad_tx).await.unwrap();

        let mut peer2 = FramedControlStream::new(ch2_tx);
        let ok_msg = ControlMessage::NewConnection {
            session_id: "post_recovery_1".into(),
            client_addr: "127.0.0.1:4444".into(),
        };
        peer2.send(ok_msg.clone()).await.unwrap();

        let delivered = timeout(Duration::from_millis(500), unified_rx.recv_msg())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(delivered, ok_msg);

        cancel.cancel();
    }
}
