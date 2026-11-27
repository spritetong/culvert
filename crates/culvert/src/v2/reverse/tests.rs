// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Deterministic unit tests for `ReverseVisitorWorkflow` and `ReverseServerRunner`.
//! All comments and docstrings must be in English.

use crate::consts::{
    ACTIVATE_STREAM_ACK, ACTIVATE_STREAM_MAGIC, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT,
    DEFAULT_RELAY_DRAIN_TIMEOUT,
};
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::frontend::{Frontend, FrontendAccept, FrontendOutput};
use crate::io::addr::{L4Protocol, PeerAddr, TargetAddr};
use crate::io::channel::IoChannel;
use crate::v2::pool::{PoolConfig, TransportPool};
use crate::v2::reverse::{ReverseServerRunner, ReverseSignaler, ReverseVisitorWorkflow};
use crate::v2::{TaskError, TaskOutcome, TunnelPhase};
use reusable_future::ReusableFutureRef;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct ChannelFrontend {
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<FrontendOutput>>>,
}

impl ChannelFrontend {
    fn new() -> (Self, mpsc::Sender<FrontendOutput>) {
        let (tx, rx) = mpsc::channel(32);
        (
            Self {
                rx: Arc::new(tokio::sync::Mutex::new(rx)),
            },
            tx,
        )
    }
}

impl Frontend for ChannelFrontend {
    fn accept(&mut self, slot: &mut ReusableFutureRef<'static, FrontendOutput>) {
        let rx = Arc::clone(&self.rx);
        slot.set(|| async move {
            let mut guard = rx.lock().await;
            guard.recv().await.unwrap_or(Err(TunnelError::Shutdown))
        });
    }

    fn shutdown<'a>(&'a mut self) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}

struct MockSignaler {
    calls: Arc<AtomicUsize>,
    hang_forever: bool,
}

impl ReverseSignaler for MockSignaler {
    fn request_pair(
        &self,
        _visitor_peer: PeerAddr,
    ) -> BoxFuture<'static, Result<Box<dyn TunnelStream>, TaskError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let hang = self.hang_forever;
        Box::pin(async move {
            if hang {
                std::future::pending::<()>().await;
            }
            let (client, mut server) = duplex(4096);
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                loop {
                    match server.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if server.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                let _ = server.shutdown().await;
            });
            Ok(Box::new(client) as Box<dyn TunnelStream>)
        })
    }
}

fn spawn_reverse_worker_echo(mut stream: DuplexStream, send_ack: bool) {
    tokio::spawn(async move {
        let mut hdr = [0u8; 3];
        if stream.read_exact(&mut hdr).await.is_err() {
            return;
        }
        assert_eq!(hdr[0], ACTIVATE_STREAM_MAGIC);
        let len = u16::from_be_bytes([hdr[1], hdr[2]]) as usize;
        let mut target = vec![0u8; len];
        if stream.read_exact(&mut target).await.is_err() {
            return;
        }
        if !send_ack {
            // Simulate half-open silent drop before 0xAC ACK.
            return;
        }
        if stream.write_all(&[ACTIVATE_STREAM_ACK]).await.is_err() {
            return;
        }
        let _ = stream.flush().await;

        let mut buf = [0u8; 1024];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if stream.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = stream.shutdown().await;
    });
}

#[tokio::test]
async fn reverse_runner_zero_rtt_fast_path_skips_signaling() {
    let (frontend, accept_tx) = ChannelFrontend::new();
    let signal_calls = Arc::new(AtomicUsize::new(0));
    let signaler = Arc::new(MockSignaler {
        calls: Arc::clone(&signal_calls),
        hang_forever: false,
    });

    let (pool_handle, pool_join) = TransportPool::start(
        PoolConfig::reverse_inbound(4, DEFAULT_IDLE_TIMEOUT),
        CancellationToken::new(),
    );

    let (client_stream, worker_half) = duplex(4096);
    spawn_reverse_worker_echo(worker_half, true);
    assert!(
        pool_handle
            .push_idle(Box::new(client_stream))
            .await
            .unwrap()
    );

    let workflow = ReverseVisitorWorkflow::new(
        Some(pool_handle.clone()),
        signaler,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let runner = ReverseServerRunner::new(
        Box::new(frontend),
        TargetAddr::Ip("127.0.0.1:8080".parse().unwrap()),
        L4Protocol::Tcp,
        workflow,
        Some((pool_handle, pool_join)),
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let cancel = CancellationToken::new();
    let runner_handle = tokio::spawn(runner.run(cancel.clone()));

    let (mut visitor, stream) = duplex(4096);
    accept_tx
        .send(Ok(FrontendAccept {
            channel: IoChannel::from_stream(stream),
            peer: PeerAddr::Socket("127.0.0.1:41000".parse().unwrap()),
        }))
        .await
        .unwrap();

    visitor.write_all(b"reverse-0rtt").await.unwrap();
    visitor.shutdown().await.unwrap();
    let mut echoed = [0u8; 12];
    visitor.read_exact(&mut echoed).await.unwrap();
    assert_eq!(&echoed, b"reverse-0rtt");
    assert_eq!(signal_calls.load(Ordering::Acquire), 0);

    cancel.cancel();
    let summary = tokio::time::timeout(Duration::from_secs(1), runner_handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(summary.completed_jobs.len(), 1);
    assert_eq!(summary.completed_jobs[0].1, TaskOutcome::Completed);
}

#[tokio::test]
async fn reverse_runner_half_open_pool_stream_fails_over_to_signaling() {
    let (frontend, accept_tx) = ChannelFrontend::new();
    let signal_calls = Arc::new(AtomicUsize::new(0));
    let signaler = Arc::new(MockSignaler {
        calls: Arc::clone(&signal_calls),
        hang_forever: false,
    });

    let (pool_handle, pool_join) = TransportPool::start(
        PoolConfig::reverse_inbound(4, DEFAULT_IDLE_TIMEOUT),
        CancellationToken::new(),
    );

    // Seed a half-open stream that drops without echoing 0xAC ACK.
    let (stale_stream, stale_worker) = duplex(4096);
    spawn_reverse_worker_echo(stale_worker, false);
    assert!(pool_handle.push_idle(Box::new(stale_stream)).await.unwrap());

    let workflow = ReverseVisitorWorkflow::new(
        Some(pool_handle.clone()),
        signaler,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let runner = ReverseServerRunner::new(
        Box::new(frontend),
        TargetAddr::Ip("127.0.0.1:8080".parse().unwrap()),
        L4Protocol::Tcp,
        workflow,
        Some((pool_handle.clone(), pool_join)),
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let cancel = CancellationToken::new();
    let runner_handle = tokio::spawn(runner.run(cancel.clone()));

    let (mut visitor, stream) = duplex(4096);
    accept_tx
        .send(Ok(FrontendAccept {
            channel: IoChannel::from_stream(stream),
            peer: PeerAddr::Socket("127.0.0.1:41001".parse().unwrap()),
        }))
        .await
        .unwrap();

    visitor.write_all(b"failover-ok").await.unwrap();
    visitor.shutdown().await.unwrap();
    let mut echoed = [0u8; 11];
    visitor.read_exact(&mut echoed).await.unwrap();
    assert_eq!(&echoed, b"failover-ok");
    assert_eq!(signal_calls.load(Ordering::Acquire), 1);

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(pool_handle.stats().await.unwrap().active_count, 0);

    cancel.cancel();
    let summary = tokio::time::timeout(Duration::from_secs(1), runner_handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
}

#[tokio::test]
async fn reverse_runner_cancels_hanging_signaling_immediately_on_shutdown() {
    let (frontend, accept_tx) = ChannelFrontend::new();
    let signal_calls = Arc::new(AtomicUsize::new(0));
    let signaler = Arc::new(MockSignaler {
        calls: Arc::clone(&signal_calls),
        hang_forever: true,
    });

    let workflow = ReverseVisitorWorkflow::new(
        None,
        signaler,
        DEFAULT_HEARTBEAT_INTERVAL,
        Duration::from_secs(30),
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let runner = ReverseServerRunner::new(
        Box::new(frontend),
        TargetAddr::Ip("127.0.0.1:8080".parse().unwrap()),
        L4Protocol::Tcp,
        workflow,
        None,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let cancel = CancellationToken::new();
    let runner_handle = tokio::spawn(runner.run(cancel.clone()));

    let (_visitor, stream) = duplex(1024);
    accept_tx
        .send(Ok(FrontendAccept {
            channel: IoChannel::from_stream(stream),
            peer: PeerAddr::Socket("127.0.0.1:41002".parse().unwrap()),
        }))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(signal_calls.load(Ordering::Acquire), 1);

    let started = std::time::Instant::now();
    cancel.cancel();
    let summary = tokio::time::timeout(Duration::from_secs(1), runner_handle)
        .await
        .expect("hanging reverse pairing must cancel immediately on shutdown")
        .unwrap()
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(250));
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
}
