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
    assert_eq!(summary.completed_jobs[0].outcome, TaskOutcome::Completed);
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

#[tokio::test]
async fn reverse_client_real_ws_server_e2e_on_demand_and_prewarmed() {
    use crate::config::model::{AppConfig, EndpointConfig, ServerConfig, TunnelMode};
    use crate::dialer::endpoint::EndpointContext;
    use crate::server::ServerRunner;
    use crate::session::SessionManager;
    use crate::transport::ws::WsTransportDriver;
    use crate::transport::{TransportConnector, TransportRegistry};
    use crate::v2::reverse::{ReverseClientConfig, ReverseClientRunner};

    let probe_echo = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let echo_port = probe_echo.local_addr().unwrap().port();
    probe_echo.set_nonblocking(true).unwrap();
    let echo_listener = tokio::net::TcpListener::from_std(probe_echo).unwrap();

    let probe_ws = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let ws_port = probe_ws.local_addr().unwrap().port();
    drop(probe_ws);

    let probe_rev = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rev_port = probe_rev.local_addr().unwrap().port();
    drop(probe_rev);

    let server_cancel = CancellationToken::new();
    let echo_cancel = server_cancel.clone();
    let echo_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = echo_cancel.cancelled() => break,
                res = echo_listener.accept() => {
                    let Ok((mut sock, _)) = res else { break; };
                    tokio::spawn(async move {
                        let (mut rd, mut wr) = sock.split();
                        let _ = tokio::io::copy(&mut rd, &mut wr).await;
                    });
                }
            }
        }
    });

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);

    let auth_token = "v2-rev-ws-e2e";
    let ws_listen = format!("127.0.0.1:{ws_port}");
    let server_runner = ServerRunner::new(
        ServerConfig {
            listen: vec![format!("ws://{ws_listen}/tunnel")],
            token: Some(auth_token.to_string()),
            default_target: None,
            ..Default::default()
        },
        Arc::clone(&registry),
        Arc::new(SessionManager::new()),
    );
    let server_tok = server_cancel.clone();
    let server_handle = tokio::spawn(async move { server_runner.run(server_tok).await });
    tokio::time::sleep(Duration::from_millis(80)).await;

    let endpoint_cfg = EndpointConfig::Simple(format!("ws://{ws_listen}/tunnel"));
    let endpoint_ctx = EndpointContext::build(&endpoint_cfg, &AppConfig::default()).unwrap();
    let connector: Arc<dyn TransportConnector> = Arc::from(
        registry
            .create_connector(&endpoint_ctx, Some(auth_token), TunnelMode::Reverse)
            .unwrap(),
    );

    let rev_listen = format!("127.0.0.1:{rev_port}");
    let target_addr = format!("127.0.0.1:{echo_port}");
    let client = ReverseClientRunner::new(
        ReverseClientConfig {
            listen_addr: rev_listen.clone(),
            target_addr,
            protocol: L4Protocol::Tcp,
            pool_size: Some(2),
            tunnel_id: None,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            relay_idle_timeout: DEFAULT_IDLE_TIMEOUT,
            drain_timeout: DEFAULT_RELAY_DRAIN_TIMEOUT,
        },
        connector,
    );

    let client_cancel = CancellationToken::new();
    let client_handle = tokio::spawn(client.run(client_cancel.clone()));

    // Wait for server reverse port to bind and prewarmed pool streams to settle.
    let mut visitor = None;
    for _ in 0..40 {
        if let Ok(s) = tokio::net::TcpStream::connect(&rev_listen).await {
            visitor = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let mut visitor = visitor.expect("reverse listener must bind on server");
    tokio::time::sleep(Duration::from_millis(80)).await;

    visitor.write_all(b"v2-reverse-e2e-ok").await.unwrap();
    visitor.shutdown().await.unwrap();
    let mut buf = [0u8; 17];
    tokio::time::timeout(Duration::from_secs(5), visitor.read_exact(&mut buf))
        .await
        .expect("reverse echo timeout")
        .unwrap();
    assert_eq!(&buf, b"v2-reverse-e2e-ok");

    // Second visitor exercises replenished or 0-RTT pre-warmed worker.
    let mut visitor2 = tokio::net::TcpStream::connect(&rev_listen).await.unwrap();
    visitor2.write_all(b"v2-reverse-second").await.unwrap();
    visitor2.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), visitor2.read_exact(&mut buf))
        .await
        .expect("second reverse echo timeout")
        .unwrap();
    assert_eq!(&buf, b"v2-reverse-second");

    client_cancel.cancel();
    let summary = tokio::time::timeout(Duration::from_secs(3), client_handle)
        .await
        .expect("v2 ReverseClientRunner must exit cleanly")
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert!(summary.completed_jobs.len() >= 2);

    server_cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        let _ = tokio::join!(echo_handle, server_handle);
    })
    .await;
}

#[tokio::test]
async fn reverse_old_generation_cannot_promote_new_signaling_attachment() {
    use crate::consts::ACTIVATE_STREAM_PROMOTE;
    use crate::control::{SignalingChannel, SignalingConfig, SignalingRouter};
    use crate::v2::Coordinator;
    use crate::v2::reverse::{ReverseClientInput, ReverseClientWorkflow};
    use std::sync::atomic::AtomicU64;

    struct PromotingConnector {
        stream_holder: Arc<tokio::sync::Mutex<Option<Box<dyn TunnelStream>>>>,
    }
    impl crate::transport::TransportConnector for PromotingConnector {
        fn connect<'a>(
            &'a self,
            _target: &'a TargetAddr,
            _session_id: Option<&'a str>,
        ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
            Box::pin(async { Err(TunnelError::Shutdown) })
        }

        fn connect_pooled<'a>(
            &'a self,
            _target: &'a TargetAddr,
        ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
            let holder = Arc::clone(&self.stream_holder);
            Box::pin(async move {
                let s = holder.lock().await.take().ok_or(TunnelError::Shutdown)?;
                Ok(IoChannel::from_boxed_stream(s))
            })
        }
    }

    let (client_prewarm, mut server_prewarm) = duplex(1024);
    let (ctrl_client, ctrl_server) = duplex(1024);
    let sig_cfg = SignalingConfig::default();
    let router_cancel = CancellationToken::new();
    let (sig_handle, sig_rx) =
        SignalingChannel::spawn(Box::new(ctrl_client), sig_cfg, router_cancel.child_token());
    let router = SignalingRouter::new(sig_handle);

    let workflow = ReverseClientWorkflow::new(
        Arc::new(PromotingConnector {
            stream_holder: Arc::new(tokio::sync::Mutex::new(Some(Box::new(client_prewarm)))),
        }),
        DEFAULT_HEARTBEAT_INTERVAL,
        Duration::from_secs(5),
        Duration::from_secs(5),
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );
    // Active generation is already 2 and router is attached to generation 2.
    let active_generation = Arc::new(AtomicU64::new(2));
    workflow
        .attachment()
        .set(
            2,
            router.clone(),
            sig_rx.event_sink(),
            sig_cfg,
            router_cancel.clone(),
        )
        .await;

    let (replenish_tx, mut replenish_rx) = mpsc::unbounded_channel();
    let (coord, coord_join) = Coordinator::start(
        workflow,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
        CancellationToken::new(),
    );

    // Dispatch an old generation-1 Prewarmed worker; server sends 0xA6 promotion.
    coord
        .assign(ReverseClientInput::Prewarmed {
            listen_targets: vec![TargetAddr::Ip("127.0.0.1:8080".parse().unwrap())],
            default_target: TargetAddr::Ip("127.0.0.1:8080".parse().unwrap()),
            default_protocol: L4Protocol::Tcp,
            tunnel_id: None,
            replenish_tx,
            generation: 1,
            active_generation,
        })
        .await
        .unwrap();

    server_prewarm
        .write_all(&[ACTIVATE_STREAM_PROMOTE])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    // Superseded generation-1 worker must neither emit replenish signals nor attach onto gen-2 router.
    assert!(replenish_rx.try_recv().is_err());
    drop(ctrl_server);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!router.promote_standby_if_closed());
    assert!(!router.has_active_channel());

    router_cancel.cancel();
    coord.graceful_stop().await.unwrap();
    let summary = coord_join.await.unwrap();
    assert_eq!(summary.completed_jobs.len(), 1);
    assert!(matches!(
        summary.completed_jobs[0].outcome,
        TaskOutcome::Failed(_)
    ));
}
