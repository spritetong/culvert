// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Session manager for pairing inbound reverse tunnel data streams and managing inbound pools.
//! All comments and docstrings must be in English.

use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::common::CancelFutureExt;
use crate::control::FramedControlStream;
use crate::core::error::TunnelError;
use crate::core::stream::{HeartbeatStatus, TunnelStream};
use crate::io::addr::L4Protocol;
use crate::transport::TargetAddr;

/// Derives a canonical key for matching reverse listener inbound pools.
///
/// The client puts its `listen` address into the pre-warm target header while
/// the server registers the pool under `(protocol, listen_addr)` from
/// `RegisterReverse`. Normalizing both through `TargetAddr` keeps IPv4/IPv6/UDS
/// spellings aligned, and prefixing by protocol lets TCP and UDP reverse
/// listeners coexist on the same port without cross-talk.
pub fn canonical_reverse_key(protocol: L4Protocol, addr: &str) -> String {
    let s = addr.trim();
    let normalized = match crate::io::addr::parse_target_addr(s) {
        Ok(target) => target.to_string(),
        Err(_) => s.to_string(),
    };
    format!("{}:{}", protocol, normalized)
}

/// One queued idle stream together with the time it arrived at the server.
struct IdleEntry {
    stream: Box<dyn TunnelStream>,
    enqueued_at: Instant,
}

/// Pre-warmed stream queue owned by one reverse listener.
///
/// The server is the *only* owner of an idle stream's lifetime: the client
/// worker holds the other end but never closes it on its own timer. `push`
/// evicts the oldest entry on overflow and `pop_live` lazily discards entries
/// older than the effective idle window. Both operations only ever touch
/// entries still in this queue, so a popped (committed) stream can never be
/// closed by the peer while it is about to be used.
pub struct ReverseInboundPool {
    idle: Mutex<VecDeque<IdleEntry>>,
    capacity: usize,
    notify: Arc<Notify>,
}

impl ReverseInboundPool {
    /// Creates a pool with a soft upper bound on queued idle streams.
    pub fn new(capacity: usize) -> Self {
        Self {
            idle: Mutex::new(VecDeque::new()),
            capacity,
            notify: Arc::new(Notify::new()),
        }
    }

    /// Returns an owned future that resolves when a new idle stream is pushed into the pool.
    pub fn notified_owned(&self) -> tokio::sync::futures::OwnedNotified {
        Arc::clone(&self.notify).notified_owned()
    }

    /// Enqueues an idle stream, evicting the oldest entry when at capacity.
    pub fn push(&self, stream: Box<dyn TunnelStream>) -> bool {
        if self.capacity == 0 {
            return false;
        }
        {
            let mut idle = self.idle.lock();
            if idle.len() >= self.capacity {
                // Dropping the handle closes the transport; the client worker sees
                // EOF and re-dials. No visitor has been paired with this stream.
                idle.pop_front();
            }
            idle.push_back(IdleEntry {
                stream,
                enqueued_at: Instant::now(),
            });
        }
        self.notify.notify_one();
        true
    }

    /// Pops the newest idle stream that is both fresh enough and still alive.
    ///
    /// LIFO favours fresh streams. An entry older than `max_age` is closed
    /// rather than paired: a silently expired NAT/QUIC pinhole cannot be
    /// detected by `poll_check_alive`, so age is the second liveness signal.
    /// Note that `poll_check_alive` reports `Pending` as healthy for transports
    /// without a detectable EOF; the age bound is what covers that half-open
    /// case without an extra RTT on the visitor's fast path.
    pub async fn pop_live(&self, max_age: Duration) -> Option<Box<dyn TunnelStream>> {
        loop {
            let entry = self.idle.lock().pop_back()?;
            if entry.enqueued_at.elapsed() >= max_age {
                drop(entry);
                continue;
            }
            let mut candidate = entry.stream;
            let alive = poll_fn(|cx| match candidate.poll_check_alive(cx) {
                Poll::Ready(HeartbeatStatus::Dead) => Poll::Ready(false),
                _ => Poll::Ready(true),
            })
            .await;
            if alive {
                return Some(candidate);
            }
        }
    }

    /// Pops an idle stream and writes the emergency promotion magic byte (`ACTIVATE_STREAM_PROMOTE = 0xA6`).
    ///
    /// If successful, returns the promoted stream ready to serve as a replacement signaling channel.
    pub async fn promote_idle(&self, max_age: Duration) -> Option<Box<dyn TunnelStream>> {
        use tokio::io::AsyncWriteExt;
        let mut stream = self.pop_live(max_age).await?;
        let write_fut = async {
            stream
                .write_all(&[crate::consts::ACTIVATE_STREAM_PROMOTE])
                .await?;
            stream.flush().await
        };
        // Bounded write: an unread or dead stream buffer must never hang promote_idle indefinitely.
        if tokio::time::timeout(Duration::from_secs(1), write_fut)
            .await
            .is_ok_and(|r| r.is_ok())
        {
            Some(stream)
        } else {
            None
        }
    }

    /// Closes every idle stream of a superseded control-channel generation.
    pub fn drain(&self) {
        self.idle.lock().clear();
    }

    /// Returns the number of currently queued idle streams.
    pub fn len(&self) -> usize {
        self.idle.lock().len()
    }

    /// Returns true when no idle streams are queued.
    pub fn is_empty(&self) -> bool {
        self.idle.lock().is_empty()
    }
}

/// Reconnected reverse control stream handed over to a listener currently in its grace period.
pub struct ReverseGraceReattach {
    pub ctrl_stream: FramedControlStream<Box<dyn TunnelStream>>,
    pub pool_size: Option<usize>,
}

struct GraceSlot {
    id: u64,
    tx: mpsc::Sender<ReverseGraceReattach>,
    closed: Arc<Notify>,
}

/// Active grace-period registration keeping a bound reverse listener discoverable
/// for incoming `RegisterReverse` reattachment without re-binding the OS socket.
pub struct ReverseGraceHandle {
    key: String,
    slot_id: u64,
    rx: mpsc::Receiver<ReverseGraceReattach>,
    closed: Arc<Notify>,
    registry: Arc<DashMap<String, GraceSlot>>,
    finished: bool,
}

impl ReverseGraceHandle {
    /// Awaits a reconnected control stream targeting this grace-period listener.
    pub async fn recv(&mut self) -> Option<ReverseGraceReattach> {
        self.rx.recv().await
    }

    /// Closes the reattach channel and drains any stream already queued right before expiry.
    pub fn close_and_try_recv(&mut self) -> Option<ReverseGraceReattach> {
        self.rx.close();
        self.rx.try_recv().ok()
    }

    /// Unregisters the grace slot and notifies any concurrent reconnector after socket teardown.
    pub fn finish(mut self) {
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

impl Drop for ReverseGraceHandle {
    fn drop(&mut self) {
        self.cleanup();
    }
}

/// RAII drop guard that unregisters a pending reverse session from [`SessionManager`]
/// if the pairing future is dropped, cancelled, or aborted before completion.
pub type PendingSessionGuard =
    scopeguard::ScopeGuard<(Arc<SessionManager>, String), fn((Arc<SessionManager>, String))>;

/// Internal state for a guarded inbound reverse stream pool.
pub struct ReversePoolEntry {
    pub manager: Arc<SessionManager>,
    pub protocol: L4Protocol,
    pub listen_addr: String,
    pub pool: Arc<ReverseInboundPool>,
    pub tunnel_id: Option<String>,
}

impl ReversePoolEntry {
    /// Replaces the active pool with a new generation, unregistering the superseded generation first.
    pub fn replace_pool(&mut self, new_pool: Arc<ReverseInboundPool>) {
        if let Some(ref tid) = self.tunnel_id {
            self.manager.unregister_tunnel_pool(tid, &self.pool);
        } else {
            self.manager
                .unregister_pool(self.protocol, &self.listen_addr, &self.pool);
        }
        self.pool = new_pool;
    }
}

impl std::ops::Deref for ReversePoolEntry {
    type Target = Arc<ReverseInboundPool>;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

/// RAII drop guard that ensures an inbound reverse stream pool is cleanly unregistered
/// from [`SessionManager`] when the owning listener task completes, errors, or is aborted.
pub type ReversePoolGuard = scopeguard::ScopeGuard<ReversePoolEntry, fn(ReversePoolEntry)>;

fn unregister_pending_session((mgr, id): (Arc<SessionManager>, String)) {
    mgr.remove(&id);
}

fn unregister_pool_entry(e: ReversePoolEntry) {
    if let Some(ref tid) = e.tunnel_id {
        e.manager.unregister_tunnel_pool(tid, &e.pool);
    } else {
        e.manager
            .unregister_pool(e.protocol, &e.listen_addr, &e.pool);
    }
}

/// Shared pool slot for a tunnel instance, tracking its shared inbound pool and active listener count.
pub struct TunnelPoolSlot {
    pub pool: Arc<ReverseInboundPool>,
    pub listener_count: std::sync::atomic::AtomicUsize,
}

/// Thread-safe in-memory routing table for matching incoming reverse data streams
/// and managing pre-warmed inbound stream pools.
#[derive(Clone, Default)]
pub struct SessionManager {
    pending_sessions: Arc<DashMap<String, oneshot::Sender<Box<dyn TunnelStream>>>>,
    reverse_pools: Arc<DashMap<String, Arc<ReverseInboundPool>>>,
    tunnel_pools: Arc<DashMap<String, Arc<TunnelPoolSlot>>>,
    grace_listeners: Arc<DashMap<String, GraceSlot>>,
    next_grace_id: Arc<AtomicU64>,
}

impl SessionManager {
    /// Creates a new, empty `SessionManager`.
    pub fn new() -> Self {
        Self {
            pending_sessions: Arc::new(DashMap::new()),
            reverse_pools: Arc::new(DashMap::new()),
            tunnel_pools: Arc::new(DashMap::new()),
            grace_listeners: Arc::new(DashMap::new()),
            next_grace_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Registers a pending session expectation and returns a receiver for the stream.
    pub fn register(&self, session_id: String) -> oneshot::Receiver<Box<dyn TunnelStream>> {
        let (tx, rx) = oneshot::channel();
        self.pending_sessions.insert(session_id, tx);
        rx
    }

    /// Registers a pending session and returns both the stream receiver and an RAII [`PendingSessionGuard`].
    ///
    /// If the awaiting task is cancelled, dropped, or aborted before pairing finishes,
    /// [`PendingSessionGuard`]'s `Drop` implementation automatically removes the session.
    pub fn register_guarded(
        self: &Arc<Self>,
        session_id: String,
    ) -> (
        oneshot::Receiver<Box<dyn TunnelStream>>,
        PendingSessionGuard,
    ) {
        let rx = self.register(session_id.clone());
        let guard = scopeguard::guard(
            (Arc::clone(self), session_id),
            unregister_pending_session as fn(_),
        );
        (rx, guard)
    }

    /// Delivers an incoming reverse data stream to the waiting task.
    pub fn fulfill(
        &self,
        session_id: &str,
        stream: Box<dyn TunnelStream>,
    ) -> Result<(), TunnelError> {
        if let Some((_, tx)) = self.pending_sessions.remove(session_id) {
            tx.send(stream).map_err(|_| TunnelError::SessionClosed)
        } else {
            Err(TunnelError::SessionNotFound(session_id.to_string()))
        }
    }

    /// Removes a pending session, for example when a wait times out.
    pub fn remove(&self, session_id: &str) {
        self.pending_sessions.remove(session_id);
    }

    /// Returns the current number of pending reverse sessions waiting for streams.
    pub fn len(&self) -> usize {
        self.pending_sessions.len()
    }

    /// Checks if there are no pending sessions.
    pub fn is_empty(&self) -> bool {
        self.pending_sessions.is_empty()
    }

    /// Registers an inbound pool for a `(protocol, listen_addr)` reverse listener.
    ///
    /// A reconnect replaces the pool and closes everything queued by the old
    /// control-channel generation, so late-arriving stale streams cannot be
    /// inherited and consumed LIFO ahead of the new generation.
    pub fn register_pool(
        &self,
        protocol: L4Protocol,
        listen_addr: &str,
        capacity: usize,
    ) -> Arc<ReverseInboundPool> {
        let key = canonical_reverse_key(protocol, listen_addr);
        let pool = Arc::new(ReverseInboundPool::new(capacity));
        if let Some(previous) = self.reverse_pools.insert(key, Arc::clone(&pool)) {
            previous.drain();
        }
        pool
    }

    /// Removes a pool only if it is still the one this listener registered.
    ///
    /// A reconnecting control channel can overlap with the old one; without the
    /// identity check the old listener's teardown would delete the new pool.
    pub fn unregister_pool(
        &self,
        protocol: L4Protocol,
        listen_addr: &str,
        pool: &Arc<ReverseInboundPool>,
    ) {
        let key = canonical_reverse_key(protocol, listen_addr);
        self.reverse_pools
            .remove_if(&key, |_, current| Arc::ptr_eq(current, pool));
    }

    /// Registers or joins a shared inbound pool for a `tunnel_id`.
    ///
    /// Multiple reverse listeners with the same `tunnel_id` share this pool.
    pub fn register_tunnel_pool(
        &self,
        tunnel_id: &str,
        capacity: usize,
    ) -> Arc<ReverseInboundPool> {
        let slot = self
            .tunnel_pools
            .entry(tunnel_id.to_string())
            .or_insert_with(|| {
                Arc::new(TunnelPoolSlot {
                    pool: Arc::new(ReverseInboundPool::new(capacity)),
                    listener_count: std::sync::atomic::AtomicUsize::new(0),
                })
            });
        slot.listener_count.fetch_add(1, Ordering::Relaxed);
        Arc::clone(&slot.pool)
    }

    /// Unregisters one listener from the shared tunnel pool, draining and removing it when count reaches zero.
    pub fn unregister_tunnel_pool(
        &self,
        tunnel_id: &str,
        pool: &Arc<ReverseInboundPool>,
    ) {
        if let Some(slot) = self.tunnel_pools.get(tunnel_id)
            && Arc::ptr_eq(&slot.pool, pool)
        {
            let prev = slot.listener_count.fetch_sub(1, Ordering::Relaxed);
            if prev <= 1 {
                drop(slot);
                if let Some((_, removed)) = self.tunnel_pools.remove(tunnel_id) {
                    removed.pool.drain();
                }
            }
        }
    }

    /// Registers an inbound pool for a reverse listener, joining the tunnel-scoped shared pool
    /// when `tunnel_id` is present, or falling back to a per-`(protocol, listen_addr)` pool.
    pub fn register_pool_guarded_with_tunnel(
        self: &Arc<Self>,
        tunnel_id: Option<String>,
        protocol: L4Protocol,
        listen_addr: &str,
        capacity: usize,
    ) -> ReversePoolGuard {
        let pool = if let Some(ref tid) = tunnel_id {
            self.register_tunnel_pool(tid, capacity)
        } else {
            self.register_pool(protocol, listen_addr, capacity)
        };
        let entry = ReversePoolEntry {
            manager: Arc::clone(self),
            protocol,
            listen_addr: listen_addr.to_string(),
            pool,
            tunnel_id,
        };
        scopeguard::guard(entry, unregister_pool_entry as fn(_))
    }

    /// Registers an inbound pool for a `(protocol, listen_addr)` reverse listener,
    /// returning an RAII [`ReversePoolGuard`] that automatically unregisters it on abort or exit.
    pub fn register_pool_guarded(
        self: &Arc<Self>,
        protocol: L4Protocol,
        listen_addr: &str,
        capacity: usize,
    ) -> ReversePoolGuard {
        self.register_pool_guarded_with_tunnel(None, protocol, listen_addr, capacity)
    }

    /// Pushes a pre-warmed idle reverse stream into the matching listener's pool.
    pub fn push_idle_stream(
        &self,
        protocol: L4Protocol,
        target_opt: Option<&TargetAddr>,
        stream: Box<dyn TunnelStream>,
    ) -> bool {
        if let Some(target) = target_opt {
            let key = canonical_reverse_key(protocol, &target.to_string());
            return match self.reverse_pools.get(&key) {
                Some(entry) => entry.value().push(stream),
                // Never guess: routing a stream meant for another listener would
                // pair it with the wrong local target.
                None => false,
            };
        }
        // Target-less transports are ambiguous; only a single active pool for
        // the given protocol is safe.
        let prefix = format!("{}:", protocol);
        let mut matches = self
            .reverse_pools
            .iter()
            .filter(|entry| entry.key().starts_with(&prefix));
        if let Some(first) = matches.next()
            && matches.next().is_none()
        {
            return first.value().push(stream);
        }
        false
    }

    /// Pushes a pre-warmed idle reverse stream into the matching pool,
    /// routing by `tunnel_id` when present, or falling back to target routing.
    pub fn push_idle_stream_with_tunnel(
        &self,
        tunnel_id: Option<&str>,
        protocol: L4Protocol,
        target_opt: Option<&TargetAddr>,
        stream: Box<dyn TunnelStream>,
    ) -> bool {
        if let Some(tid) = tunnel_id
            && let Some(slot) = self.tunnel_pools.get(tid)
        {
            return slot.pool.push(stream);
        }
        self.push_idle_stream(protocol, target_opt, stream)
    }

    /// Returns the number of registered reverse inbound pools.
    pub fn pool_count(&self) -> usize {
        self.reverse_pools.len() + self.tunnel_pools.len()
    }

    /// Registers a reverse listener entering its grace period so incoming
    /// `RegisterReverse` reconnects can reattach without re-binding the port.
    pub fn register_grace_listener(
        &self,
        protocol: L4Protocol,
        listen_addr: &str,
    ) -> ReverseGraceHandle {
        let key = canonical_reverse_key(protocol, listen_addr);
        let slot_id = self.next_grace_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(1);
        let closed = Arc::new(Notify::new());
        if let Some(prev) = self.grace_listeners.insert(
            key.clone(),
            GraceSlot {
                id: slot_id,
                tx,
                closed: Arc::clone(&closed),
            },
        ) {
            prev.closed.notify_waiters();
        }
        ReverseGraceHandle {
            key,
            slot_id,
            rx,
            closed,
            registry: Arc::clone(&self.grace_listeners),
            finished: false,
        }
    }

    /// Attempts to hand over a newly connected `RegisterReverse` control stream
    /// to an existing listener currently in its grace period.
    ///
    /// Returns `Ok(())` on successful handoff, or `Err(Box<ReverseGraceReattach>)`
    /// if no grace listener exists (or once an expiring grace listener has finished
    /// closing its socket).
    pub async fn try_reattach_grace_listener(
        &self,
        protocol: L4Protocol,
        listen_addr: &str,
        mut reattach: ReverseGraceReattach,
        cancel_token: &CancellationToken,
    ) -> Result<(), Box<ReverseGraceReattach>> {
        let key = canonical_reverse_key(protocol, listen_addr);
        loop {
            let slot = self
                .grace_listeners
                .get(&key)
                .map(|entry| (entry.tx.clone(), Arc::clone(&entry.closed)));
            let Some((tx, closed)) = slot else {
                return Err(Box::new(reattach));
            };
            let notified = closed.notified();
            tokio::pin!(notified);

            match tx.send(reattach).await {
                Ok(()) => return Ok(()),
                Err(mpsc::error::SendError(returned)) => {
                    reattach = returned;
                    // Wait until the expiring grace listener finishes shutting
                    // down and dropping its bound `Frontend` before retrying or
                    // binding fresh, avoiding EADDRINUSE races.
                    if notified.cancel_with(cancel_token).await.is_none() {
                        return Err(Box::new(reattach));
                    }
                }
            }
        }
    }

    /// Returns the number of reverse listeners currently in their grace period.
    pub fn grace_listener_count(&self) -> usize {
        self.grace_listeners.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::pin::Pin;
    use std::task::Context;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, duplex};

    /// Stream whose liveness hook always reports EOF/RST.
    struct DeadStream;

    impl AsyncRead for DeadStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for DeadStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl TunnelStream for DeadStream {
        fn poll_check_alive(&mut self, _cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
            Poll::Ready(HeartbeatStatus::Dead)
        }
    }

    #[tokio::test]
    async fn test_session_register_and_fulfill() {
        let manager = SessionManager::new();
        let session_id = "test_sess_001".to_string();
        let rx = manager.register(session_id.clone());
        assert_eq!(manager.len(), 1);
        let (client, _server) = duplex(1024);
        manager.fulfill(&session_id, Box::new(client)).unwrap();
        assert_eq!(manager.len(), 0);
        let stream = rx.await.unwrap();
        drop(stream);
    }

    #[tokio::test]
    async fn test_inbound_pool_register_and_push() {
        let manager = SessionManager::new();
        assert_eq!(manager.pool_count(), 0);

        let pool = manager.register_pool(L4Protocol::Tcp, "0.0.0.0:8080", 8);
        assert_eq!(manager.pool_count(), 1);

        let (client, _server) = duplex(1024);
        let target = TargetAddr::Domain("0.0.0.0".into(), 8080);
        assert!(manager.push_idle_stream(L4Protocol::Tcp, Some(&target), Box::new(client)));
        assert_eq!(pool.len(), 1);
        assert!(pool.pop_live(Duration::from_secs(60)).await.is_some());
        assert!(pool.is_empty());

        manager.unregister_pool(L4Protocol::Tcp, "0.0.0.0:8080", &pool);
        assert_eq!(manager.pool_count(), 0);
    }

    #[tokio::test]
    async fn test_pool_soft_cap_evicts_oldest() {
        let pool = ReverseInboundPool::new(3);
        for _ in 0..5 {
            let (client, _server) = duplex(64);
            assert!(pool.push(Box::new(client)));
            assert!(pool.len() <= 3);
        }
        assert_eq!(pool.len(), 3, "soft cap must bound queued streams");
        assert!(!ReverseInboundPool::new(0).push(Box::new(DeadStream)));
    }

    #[tokio::test]
    async fn test_pop_live_skips_dead_streams() {
        let pool = ReverseInboundPool::new(8);
        let (client, _server) = duplex(64);
        pool.push(Box::new(client));
        pool.push(Box::new(DeadStream));
        assert!(
            pool.pop_live(Duration::from_secs(60)).await.is_some(),
            "live stream must be used"
        );
        assert!(
            pool.pop_live(Duration::from_secs(60)).await.is_none(),
            "dead stream must be discarded, not paired"
        );
        assert!(pool.is_empty());
    }

    #[tokio::test]
    async fn test_pop_live_discards_over_age_streams() {
        let pool = ReverseInboundPool::new(4);
        let (old, _old_peer) = duplex(64);
        pool.push(Box::new(old));
        tokio::time::sleep(Duration::from_millis(120)).await;

        let (fresh, _fresh_peer) = duplex(64);
        pool.push(Box::new(fresh));

        assert!(
            pool.pop_live(Duration::from_millis(60)).await.is_some(),
            "the fresh stream must be paired"
        );
        assert!(
            pool.pop_live(Duration::from_millis(60)).await.is_none(),
            "an over-age stream must be closed, never paired"
        );
        assert!(pool.is_empty());
    }

    #[tokio::test]
    async fn test_register_pool_drains_previous_generation() {
        let manager = SessionManager::new();
        let old_pool = manager.register_pool(L4Protocol::Tcp, "127.0.0.1:8080", 8);
        let (client, _peer) = duplex(64);
        assert!(old_pool.push(Box::new(client)));
        assert_eq!(old_pool.len(), 1);

        let new_pool = manager.register_pool(L4Protocol::Tcp, "127.0.0.1:8080", 8);
        assert!(
            old_pool.is_empty(),
            "a reconnect must close the stale generation's idle streams"
        );
        assert!(new_pool.is_empty());
        assert_eq!(manager.pool_count(), 1);
    }

    #[tokio::test]
    async fn test_push_does_not_route_to_wrong_listener() {
        let manager = SessionManager::new();
        let pool_a = manager.register_pool(L4Protocol::Tcp, "127.0.0.1:8080", 8);
        let (client, _server) = duplex(64);
        let wrong = TargetAddr::Domain("127.0.0.1".into(), 9090);
        assert!(
            !manager.push_idle_stream(L4Protocol::Tcp, Some(&wrong), Box::new(client)),
            "a single registered pool must not capture another listener's streams"
        );
        assert!(pool_a.is_empty());

        // Two listeners on the same port but different hosts stay distinct.
        let pool_b = manager.register_pool(L4Protocol::Tcp, "127.0.0.2:8080", 8);
        assert_eq!(manager.pool_count(), 2);
        assert!(!Arc::ptr_eq(&pool_a, &pool_b));
    }

    #[tokio::test]
    async fn test_unregister_pool_is_identity_aware() {
        let manager = SessionManager::new();
        let old_pool = manager.register_pool(L4Protocol::Tcp, "127.0.0.1:8080", 8);
        let new_pool = manager.register_pool(L4Protocol::Tcp, "127.0.0.1:8080", 8);
        manager.unregister_pool(L4Protocol::Tcp, "127.0.0.1:8080", &old_pool);
        assert_eq!(
            manager.pool_count(),
            1,
            "stale teardown must not delete new pool"
        );
        let (client, _server) = duplex(64);
        let target = TargetAddr::Domain("127.0.0.1".into(), 8080);
        assert!(manager.push_idle_stream(L4Protocol::Tcp, Some(&target), Box::new(client)));
        assert_eq!(new_pool.len(), 1);
    }

    #[tokio::test]
    async fn test_tcp_and_udp_pools_coexist_on_same_port() {
        let manager = SessionManager::new();
        let tcp_pool = manager.register_pool(L4Protocol::Tcp, "0.0.0.0:5353", 4);
        let udp_pool = manager.register_pool(L4Protocol::Udp, "0.0.0.0:5353", 4);
        assert_eq!(manager.pool_count(), 2);

        let target = TargetAddr::Domain("0.0.0.0".into(), 5353);
        let (c_tcp, _) = duplex(64);
        let (c_udp, _) = duplex(64);
        assert!(manager.push_idle_stream(L4Protocol::Tcp, Some(&target), Box::new(c_tcp)));
        assert!(manager.push_idle_stream(L4Protocol::Udp, Some(&target), Box::new(c_udp)));
        assert_eq!(tcp_pool.len(), 1);
        assert_eq!(udp_pool.len(), 1);

        manager.unregister_pool(L4Protocol::Tcp, "0.0.0.0:5353", &tcp_pool);
        assert_eq!(manager.pool_count(), 1);
        assert_eq!(udp_pool.len(), 1);
    }

    #[tokio::test]
    async fn test_promote_idle_writes_magic_byte() {
        let pool = ReverseInboundPool::new(4);
        let (client, mut server) = duplex(64);
        assert!(pool.push(Box::new(client)));
        assert_eq!(pool.len(), 1);

        let promoted = pool.promote_idle(Duration::from_secs(5)).await;
        assert!(promoted.is_some());
        assert_eq!(pool.len(), 0);

        // Server end must read ACTIVATE_STREAM_PROMOTE (0xA6)
        let mut magic = [0u8; 1];
        tokio::io::AsyncReadExt::read_exact(&mut server, &mut magic)
            .await
            .unwrap();
        assert_eq!(magic[0], crate::consts::ACTIVATE_STREAM_PROMOTE);
    }

    #[test]
    fn test_canonical_reverse_key() {
        assert_eq!(
            canonical_reverse_key(L4Protocol::Tcp, "0.0.0.0:9090"),
            "tcp:0.0.0.0:9090"
        );
        assert_eq!(
            canonical_reverse_key(L4Protocol::Udp, "0.0.0.0:9090"),
            "udp:0.0.0.0:9090"
        );
        assert_ne!(
            canonical_reverse_key(L4Protocol::Tcp, "127.0.0.1:9090"),
            canonical_reverse_key(L4Protocol::Udp, "127.0.0.1:9090")
        );
        assert_eq!(
            canonical_reverse_key(L4Protocol::Tcp, "127.0.0.1:9090"),
            "tcp:127.0.0.1:9090"
        );
        assert_ne!(
            canonical_reverse_key(L4Protocol::Tcp, "127.0.0.1:9090"),
            canonical_reverse_key(L4Protocol::Tcp, "127.0.0.2:9090")
        );
        assert_eq!(
            canonical_reverse_key(L4Protocol::Tcp, "[::1]:9090"),
            "tcp:[::1]:9090"
        );
        // UDS paths are normalized by the platform, so assert stability rather
        // than a POSIX-specific literal.
        let uds_a = canonical_reverse_key(L4Protocol::Tcp, "unix:///tmp/sock");
        assert!(uds_a.starts_with("tcp:unix://"));
        let uds_path = uds_a.strip_prefix("tcp:").unwrap();
        let uds_b = canonical_reverse_key(L4Protocol::Tcp, uds_path);
        assert_eq!(uds_a, uds_b);
    }

    #[tokio::test]
    async fn test_grace_listener_reattach_and_expiry_coordination() {
        let manager = SessionManager::new();
        let cancel = CancellationToken::new();

        let mut grace = manager.register_grace_listener(L4Protocol::Udp, "127.0.0.1:5353");
        assert_eq!(manager.grace_listener_count(), 1);

        let (c1, _s1) = duplex(1024);
        let reattach = ReverseGraceReattach {
            ctrl_stream: FramedControlStream::new(Box::new(c1) as Box<dyn TunnelStream>),
            pool_size: Some(4),
        };
        assert!(
            manager
                .try_reattach_grace_listener(L4Protocol::Udp, "127.0.0.1:5353", reattach, &cancel)
                .await
                .is_ok()
        );
        let received = grace
            .recv()
            .await
            .expect("should receive reattached stream");
        assert_eq!(received.pool_size, Some(4));
        grace.finish();
        assert_eq!(manager.grace_listener_count(), 0);

        // Concurrent expiry: close_and_try_recv closes rx; reconnector waits until finish()
        let mut expiring = manager.register_grace_listener(L4Protocol::Udp, "127.0.0.1:5354");
        assert!(expiring.close_and_try_recv().is_none());

        let mgr_clone = manager.clone();
        let cancel_clone = cancel.clone();
        let waiter = tokio::spawn(async move {
            let (c2, _s2) = duplex(1024);
            let req = ReverseGraceReattach {
                ctrl_stream: FramedControlStream::new(Box::new(c2) as Box<dyn TunnelStream>),
                pool_size: None,
            };
            mgr_clone
                .try_reattach_grace_listener(L4Protocol::Udp, "127.0.0.1:5354", req, &cancel_clone)
                .await
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !waiter.is_finished(),
            "must wait until finish() releases socket"
        );
        expiring.finish();

        let res = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter must unblock on finish")
            .unwrap();
        assert!(
            res.is_err(),
            "expired grace must hand stream back for fresh bind"
        );
        assert_eq!(manager.grace_listener_count(), 0);
    }

    #[tokio::test]
    async fn test_inbound_pool_notifies_on_push() {
        let pool = ReverseInboundPool::new(4);
        let notified = pool.notified_owned();
        let (c, _s) = duplex(64);
        assert!(pool.push(Box::new(c)));
        tokio::time::timeout(Duration::from_millis(200), notified)
            .await
            .expect("push must wake owned notifier");
    }

    #[tokio::test]
    async fn test_pending_session_guard_cleans_up_on_drop() {
        let manager = Arc::new(SessionManager::new());
        let session_id = "sess_abort_test_01".to_string();
        let (_rx, guard) = manager.register_guarded(session_id.clone());
        assert_eq!(manager.len(), 1);

        // Aborting/dropping the task or guard drops the guard without disarming:
        drop(guard);
        assert_eq!(manager.len(), 0);
    }

    #[tokio::test]
    async fn test_pending_session_guard_disarm_retains_session() {
        let manager = Arc::new(SessionManager::new());
        let session_id = "sess_disarm_test_02".to_string();
        let (rx, guard) = manager.register_guarded(session_id.clone());
        assert_eq!(manager.len(), 1);

        let (client, _server) = duplex(64);
        manager.fulfill(&session_id, Box::new(client)).unwrap();
        let _ = scopeguard::ScopeGuard::into_inner(guard);

        let stream = rx.await.unwrap();
        drop(stream);
        assert_eq!(manager.len(), 0);
    }

    #[tokio::test]
    async fn test_reverse_pool_guard_unregisters_on_drop() {
        let manager = Arc::new(SessionManager::new());
        assert_eq!(manager.pool_count(), 0);

        let guard = manager.register_pool_guarded(L4Protocol::Tcp, "127.0.0.1:9999", 4);
        assert_eq!(manager.pool_count(), 1);

        drop(guard);
        assert_eq!(manager.pool_count(), 0);
    }

    #[tokio::test]
    async fn test_reverse_pool_guard_replace_pool() {
        let manager = Arc::new(SessionManager::new());
        let mut guard = manager.register_pool_guarded(L4Protocol::Tcp, "127.0.0.1:9998", 4);
        let (c1, _) = duplex(64);
        assert!(guard.push(Box::new(c1)));
        assert_eq!(guard.len(), 1);

        let new_pool = manager.register_pool(L4Protocol::Tcp, "127.0.0.1:9998", 8);
        guard.replace_pool(new_pool);
        assert_eq!(manager.pool_count(), 1);
        assert_eq!(guard.len(), 0);

        drop(guard);
        assert_eq!(manager.pool_count(), 0);
    }
}
