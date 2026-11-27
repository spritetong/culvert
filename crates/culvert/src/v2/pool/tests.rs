// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Unit tests for the unified v2 `TransportPool` (Forward + Reverse modes).
//! All comments and docstrings must be in English.

use crate::consts::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT, POOL_MAX_CONCURRENT_CONNECTS,
};
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, HeartbeatStatus, TunnelStream};
use crate::io::addr::{L4Protocol, TargetAddr};
use crate::io::channel::IoChannel;
use crate::transport::TransportConnector;
use crate::v2::pool::{PoolConfig, PoolInventory, TransportPool};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio_util::sync::CancellationToken;

struct ProbeStream {
    id: usize,
    dead: bool,
    activity_ms: Option<u64>,
    ping_count: Arc<AtomicUsize>,
    dropped_flag: Option<Arc<AtomicBool>>,
}

impl ProbeStream {
    fn new(id: usize) -> Self {
        Self {
            id,
            dead: false,
            activity_ms: None,
            ping_count: Arc::new(AtomicUsize::new(0)),
            dropped_flag: None,
        }
    }
}

impl Drop for ProbeStream {
    fn drop(&mut self) {
        if let Some(ref flag) = self.dropped_flag {
            flag.store(true, Ordering::Release);
        }
    }
}

impl AsyncRead for ProbeStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for ProbeStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.dead {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream is dead",
            )))
        } else {
            Poll::Ready(Ok(buf.len()))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl TunnelStream for ProbeStream {
    fn poll_send_ping(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.ping_count.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(()))
    }

    fn last_activity_ms(&self) -> Option<u64> {
        self.activity_ms
    }

    fn poll_check_alive(&mut self, _cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        let _ = self.id;
        if self.dead {
            Poll::Ready(HeartbeatStatus::Dead)
        } else {
            Poll::Ready(HeartbeatStatus::Alive)
        }
    }
}

struct MockDialConnector {
    dial_count: Arc<AtomicUsize>,
    fail_dials: Arc<AtomicBool>,
    last_tunnel_id: Arc<parking_lot::Mutex<Option<String>>>,
}

impl TransportConnector for MockDialConnector {
    fn connect<'a>(
        &'a self,
        _target: &'a TargetAddr,
        _session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        Box::pin(async { Err(TunnelError::Internal("unused".into())) })
    }

    fn connect_pooled<'a>(
        &'a self,
        _target: &'a TargetAddr,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.connect_pooled_with_tunnel(_target, L4Protocol::Tcp, None)
    }

    fn connect_pooled_with_tunnel<'a>(
        &'a self,
        _target: &'a TargetAddr,
        _protocol: L4Protocol,
        tunnel_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        let id = self.dial_count.fetch_add(1, Ordering::AcqRel);
        *self.last_tunnel_id.lock() = tunnel_id.map(str::to_owned);
        let should_fail = self.fail_dials.load(Ordering::Acquire);
        Box::pin(async move {
            if should_fail {
                Err(TunnelError::Io(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "mock dial refused",
                )))
            } else {
                Ok(IoChannel::from_stream(ProbeStream::new(id)))
            }
        })
    }
}

#[tokio::test]
async fn inventory_lifo_evicts_dead_and_expired_streams() {
    let mut inv = PoolInventory::new(
        2,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        Some(Duration::from_millis(50)),
    );

    let oldest_dropped = Arc::new(AtomicBool::new(false));
    let mut s1 = ProbeStream::new(1);
    s1.dropped_flag = Some(Arc::clone(&oldest_dropped));
    let s2 = ProbeStream::new(2);
    let mut s3_dead = ProbeStream::new(3);
    s3_dead.dead = true;

    assert!(inv.push_back(Box::new(s1), false));
    assert!(inv.push_back(Box::new(s2), false));
    // Capacity is 2: pushing s3_dead must evict oldest (s1).
    assert!(inv.push_back(Box::new(s3_dead), false));
    assert!(oldest_dropped.load(Ordering::Acquire));
    assert_eq!(inv.idle_len(), 2);

    // LIFO take skips/evicts s3_dead and returns healthy s2.
    let popped = std::future::poll_fn(|cx| Poll::Ready(inv.take_live_idle(cx))).await;
    assert!(popped.is_some());
    assert_eq!(inv.idle_len(), 0);
    assert_eq!(inv.stats(0, 0).total_evicted_dead, 1);
}

#[tokio::test]
async fn forward_pool_replenishes_tracks_leases_and_backs_off() {
    let dial_count = Arc::new(AtomicUsize::new(0));
    let fail_dials = Arc::new(AtomicBool::new(false));
    let last_tunnel_id = Arc::new(parking_lot::Mutex::new(None));
    let connector = Arc::new(MockDialConnector {
        dial_count: Arc::clone(&dial_count),
        fail_dials: Arc::clone(&fail_dials),
        last_tunnel_id: Arc::clone(&last_tunnel_id),
    });

    let cfg = PoolConfig::forward(
        connector,
        2,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
    )
    .with_dial_routing(
        TargetAddr::Domain("example.com".into(), 443),
        L4Protocol::Tcp,
        Some("tun-v2".into()),
    );
    let cancel = CancellationToken::new();
    let (pool, join) = TransportPool::start(cfg, cancel);

    tokio::time::sleep(Duration::from_millis(40)).await;
    let stats = pool.stats().await.unwrap();
    assert_eq!(stats.idle_count, 2);
    assert_eq!(last_tunnel_id.lock().as_deref(), Some("tun-v2"));

    // Acquire one stream -> active_count == 1, dropping lease auto-decrements active_count to 0.
    let mut lease = pool.acquire().await.unwrap();
    lease.write_all(b"ping").await.unwrap();
    assert_eq!(pool.stats().await.unwrap().active_count, 1);

    drop(lease);
    tokio::time::sleep(Duration::from_millis(40)).await;
    let stats_after = pool.stats().await.unwrap();
    assert_eq!(stats_after.active_count, 0);
    assert_eq!(stats_after.idle_count, 2);

    // Dropping all PoolHandle clones terminates the pool actor promptly.
    drop(pool);
    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .expect("pool actor must exit when handle drops")
        .unwrap();
    assert_eq!(summary.final_stats.idle_count, 0);

    // Verify backoff throttles failing connector bursts.
    let fail_count = Arc::new(AtomicUsize::new(0));
    let fail_conn = Arc::new(MockDialConnector {
        dial_count: Arc::clone(&fail_count),
        fail_dials: Arc::new(AtomicBool::new(true)),
        last_tunnel_id: Arc::new(parking_lot::Mutex::new(None)),
    });
    let (fail_pool, fail_join) = TransportPool::start(
        PoolConfig::forward(
            fail_conn,
            4,
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
        ),
        CancellationToken::new(),
    );
    tokio::time::sleep(Duration::from_millis(350)).await;
    drop(fail_pool);
    fail_join.await.unwrap();
    let attempts = fail_count.load(Ordering::Acquire);
    assert!(
        (1..=POOL_MAX_CONCURRENT_CONNECTS).contains(&attempts),
        "expected throttled dials within 1..={POOL_MAX_CONCURRENT_CONNECTS}, got {attempts}"
    );
}

#[tokio::test]
async fn reverse_inbound_pool_supports_push_try_acquire_wait_and_clear() {
    let cfg = PoolConfig::reverse_inbound(2, Duration::from_secs(30));
    let (pool, join) = TransportPool::start(cfg, CancellationToken::new());

    // Empty reverse pool returns None immediately on try_acquire (fast-path miss -> signaling).
    assert!(pool.try_acquire().await.unwrap().is_none());

    assert!(
        pool.push_idle(Box::new(ProbeStream::new(10)))
            .await
            .unwrap()
    );
    assert!(
        pool.push_idle(Box::new(ProbeStream::new(11)))
            .await
            .unwrap()
    );
    assert_eq!(pool.stats().await.unwrap().idle_count, 2);

    let lease = pool
        .try_acquire()
        .await
        .unwrap()
        .expect("must pop idle stream");
    assert_eq!(pool.stats().await.unwrap().active_count, 1);
    assert_eq!(pool.stats().await.unwrap().idle_count, 1);
    drop(lease);

    // Clear remaining idle stream when a reverse generation rotates.
    assert_eq!(pool.clear_idle().await.unwrap(), 1);
    assert_eq!(pool.stats().await.unwrap().idle_count, 0);

    // Waiting acquire resolves via direct handoff on push_idle.
    let waiter_pool = pool.clone();
    let wait_task = tokio::spawn(async move { waiter_pool.acquire().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        pool.push_idle(Box::new(ProbeStream::new(12)))
            .await
            .unwrap()
    );

    let handed_off = tokio::time::timeout(Duration::from_secs(1), wait_task)
        .await
        .expect("acquire should receive pushed stream")
        .unwrap()
        .unwrap();
    drop(handed_off);

    pool.shutdown();
    tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .expect("pool actor must exit on shutdown")
        .unwrap();
}

#[tokio::test]
async fn cancelled_acquire_waiter_is_purged_and_stop_replenishing_halts_dials() {
    let dial_count = Arc::new(AtomicUsize::new(0));
    let conn = Arc::new(MockDialConnector {
        dial_count: Arc::clone(&dial_count),
        fail_dials: Arc::new(AtomicBool::new(false)),
        last_tunnel_id: Arc::new(parking_lot::Mutex::new(None)),
    });

    // Reverse inbound pool starts empty; cancelling acquire() leaves zero pending waiters.
    let (rev_pool, rev_join) = TransportPool::start(
        PoolConfig::reverse_inbound(2, Duration::from_secs(30)),
        CancellationToken::new(),
    );
    let waiter = {
        let p = rev_pool.clone();
        tokio::spawn(async move { p.acquire().await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(rev_pool.stats().await.unwrap().pending_waiters, 1);
    waiter.abort();
    let _ = waiter.await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(rev_pool.stats().await.unwrap().pending_waiters, 0);
    assert_eq!(rev_pool.stats().await.unwrap().active_count, 0);
    rev_pool.shutdown();
    rev_join.await.unwrap();

    // Forward pool: stop_replenishing() halts new dials while existing leases stay valid until dropped.
    let cfg = PoolConfig::forward(conn, 1, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT);
    let (fwd_pool, fwd_join) = TransportPool::start(cfg, CancellationToken::new());
    tokio::time::sleep(Duration::from_millis(30)).await;
    let lease = fwd_pool.acquire().await.unwrap();
    let dials_before_drain = dial_count.load(Ordering::Acquire);

    fwd_pool.stop_replenishing().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(dial_count.load(Ordering::Acquire), dials_before_drain);
    assert_eq!(fwd_pool.stats().await.unwrap().active_count, 1);

    drop(lease);
    fwd_pool.shutdown();
    let summary = fwd_join.await.unwrap();
    assert_eq!(summary.final_stats.active_count, 0);
}
