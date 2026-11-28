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
use crate::v2::reverse::{ReverseServerRunner, ReverseSignaler, ReverseVisitorWorkflow};
use crate::v2::{Coordinator, CoordinatorConfig, PooledTransport};
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

    let workflow = ReverseVisitorWorkflow::new(
        signaler,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );
    let (coord, coord_join) = Coordinator::start_with_config(
        workflow,
        CoordinatorConfig::reverse_inbound(4, DEFAULT_IDLE_TIMEOUT, DEFAULT_RELAY_DRAIN_TIMEOUT),
        CancellationToken::new(),
    );

    let (client_stream, worker_half) = duplex(4096);
    spawn_reverse_worker_echo(worker_half, true);
    assert!(
        coord
            .supply_transport(PooledTransport::new(Box::new(client_stream), 1))
            .await
            .unwrap()
    );

    let runner = ReverseServerRunner::with_coordinator(
        Box::new(frontend),
        TargetAddr::Ip("127.0.0.1:8080".parse().unwrap()),
        L4Protocol::Tcp,
        coord,
        Some(coord_join),
        None,
        None,
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

    let workflow = ReverseVisitorWorkflow::new(
        signaler,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );
    let (coord, coord_join) = Coordinator::start_with_config(
        workflow,
        CoordinatorConfig::reverse_inbound(4, DEFAULT_IDLE_TIMEOUT, DEFAULT_RELAY_DRAIN_TIMEOUT),
        CancellationToken::new(),
    );

    // Seed a half-open stream that drops without echoing 0xAC ACK.
    let (stale_stream, stale_worker) = duplex(4096);
    spawn_reverse_worker_echo(stale_worker, false);
    assert!(
        coord
            .supply_transport(PooledTransport::new(Box::new(stale_stream), 1))
            .await
            .unwrap()
    );

    let coord_observer = coord.clone();
    let runner = ReverseServerRunner::with_coordinator(
        Box::new(frontend),
        TargetAddr::Ip("127.0.0.1:8080".parse().unwrap()),
        L4Protocol::Tcp,
        coord,
        Some(coord_join),
        None,
        None,
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
    assert_eq!(
        coord_observer
            .query_pool_stats()
            .await
            .unwrap()
            .active_count,
        0
    );

    cancel.cancel();
    let summary = tokio::time::timeout(Duration::from_secs(1), runner_handle)
        .await
        .unwrap()
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
        CoordinatorConfig::plain(DEFAULT_RELAY_DRAIN_TIMEOUT),
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
    assert!(!router.promote_standby_if_closed().await);
    assert!(!router.has_active_channel().await);

    router_cancel.cancel();
    coord.graceful_stop().await.unwrap();
    let summary = coord_join.await.unwrap();
    assert_eq!(summary.completed_jobs.len(), 1);
    assert!(matches!(
        summary.completed_jobs[0].outcome,
        TaskOutcome::Failed(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reverse_ready_notifier_does_not_lose_ready_notification() {
    use crate::v2::reverse::ReverseReadyNotifier;

    for _ in 0..80 {
        let notifier = Arc::new(ReverseReadyNotifier::new());
        let cancel = CancellationToken::new();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));

        let n_mark = Arc::clone(&notifier);
        let b_mark = Arc::clone(&barrier);
        let marker = tokio::spawn(async move {
            b_mark.wait().await;
            n_mark.mark_ready();
        });

        let n_wait = Arc::clone(&notifier);
        let b_wait = Arc::clone(&barrier);
        let c_wait = cancel.clone();
        let waiter = tokio::spawn(async move {
            b_wait.wait().await;
            n_wait.wait_ready(&c_wait).await
        });

        marker.await.unwrap();
        let ready = tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .expect("ReverseReadyNotifier::wait_ready must not stall on lost notify")
            .unwrap();
        assert!(ready);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn router_standby_attach_during_promotion_wait_is_observed() {
    use crate::control::{SignalingChannel, SignalingConfig, SignalingRouter};
    use crate::v2::reverse::ReverseClientRunner;

    for _ in 0..50 {
        let cancel = CancellationToken::new();
        let cfg = SignalingConfig {
            heartbeat_interval: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
        };
        let (ch1_a, ch1_b) = duplex(1024);
        let (h1, _rx1) = SignalingChannel::spawn(ch1_a, cfg, cancel.child_token());
        drop(ch1_b);
        let router = SignalingRouter::new(h1);

        // Ensure primary channel is closed before starting the standby recovery wait.
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(!router.has_active_channel().await);

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let b_attach = Arc::clone(&barrier);
        let b_wait = Arc::clone(&barrier);

        let router_attach = router.clone();
        let cancel_attach = cancel.child_token();
        let (_ch2_a, ch2_b) = duplex(1024);
        let attacher = tokio::spawn(async move {
            b_attach.wait().await;
            let h2 = SignalingChannel::spawn(ch2_b, cfg, cancel_attach).0;
            router_attach.attach(h2);
        });

        let router_wait = router.clone();
        let cancel_wait = cancel.clone();
        let waiter = tokio::spawn(async move {
            b_wait.wait().await;
            // Use a long promotion window so a lost wakeup would blow past the 500ms timeout.
            ReverseClientRunner::wait_for_router_recovery(
                &router_wait,
                Duration::from_secs(10),
                &cancel_wait,
            )
            .await
        });

        attacher.await.unwrap();
        let promoted = tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .expect("wait_for_router_recovery must observe attach without waiting for deadline")
            .unwrap();
        assert!(promoted);

        cancel.cancel();
    }
}

#[tokio::test]
async fn reverse_activation_failure_preserves_sibling_idle_and_session_close_sweeps_siblings() {
    use crate::core::stream::HeartbeatStatus;
    use crate::v2::IdlePolicy;
    use crate::v2::reverse::ReverseVisitorInput;
    use std::pin::Pin;
    use std::sync::atomic::AtomicBool;
    use std::task::{Context, Poll};

    struct SessionIdleStream {
        inner: DuplexStream,
        session_id: u64,
        alive: Arc<AtomicBool>,
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for SessionIdleStream {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl tokio::io::AsyncRead for SessionIdleStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl tokio::io::AsyncWrite for SessionIdleStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    impl TunnelStream for SessionIdleStream {
        fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
            if !self.alive.load(Ordering::Acquire) {
                return Poll::Ready(HeartbeatStatus::Dead);
            }
            Pin::new(&mut self.inner).poll_check_alive(cx)
        }

        fn session_id(&self) -> Option<u64> {
            Some(self.session_id)
        }
    }

    let spawn_ack_peer = |mut srv: DuplexStream, ack_byte: u8| {
        tokio::spawn(async move {
            let mut hdr = [0u8; 3];
            if srv.read_exact(&mut hdr).await.is_err() {
                return;
            }
            let len = u16::from_be_bytes([hdr[1], hdr[2]]) as usize;
            let mut target_buf = vec![0u8; len];
            if srv.read_exact(&mut target_buf).await.is_err() {
                return;
            }
            let _ = srv.write_all(&[ack_byte]).await;
            let _ = srv.flush().await;
            if ack_byte != ACTIVATE_STREAM_ACK {
                return;
            }
            let mut buf = [0u8; 256];
            loop {
                match srv.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if srv.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
            let _ = srv.shutdown().await;
        })
    };

    let signal_calls = Arc::new(AtomicUsize::new(0));
    let signaler = Arc::new(MockSignaler {
        calls: Arc::clone(&signal_calls),
        hang_forever: false,
    });
    let workflow = ReverseVisitorWorkflow::new(
        signaler,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let cfg = CoordinatorConfig {
        drain_timeout: DEFAULT_RELAY_DRAIN_TIMEOUT,
        max_idle_capacity: 8,
        idle_policy: IdlePolicy::reverse_inbound(Duration::from_secs(30)),
        dial: None,
    };
    let (handle, join) = Coordinator::start_with_config(workflow, cfg, CancellationToken::new());

    let shared_alive = Arc::new(AtomicBool::new(true));
    let good_dropped = Arc::new(AtomicUsize::new(0));
    let bad_dropped = Arc::new(AtomicUsize::new(0));

    let (c_good, s_good) = duplex(4096);
    let (c_bad, s_bad) = duplex(4096);
    spawn_ack_peer(s_good, ACTIVATE_STREAM_ACK);
    spawn_ack_peer(s_bad, 0xFF);

    assert!(
        handle
            .supply_transport(PooledTransport::new(
                Box::new(SessionIdleStream {
                    inner: c_good,
                    session_id: 777,
                    alive: Arc::clone(&shared_alive),
                    dropped: Arc::clone(&good_dropped),
                }),
                1,
            ))
            .await
            .unwrap()
    );
    assert!(
        handle
            .supply_transport(PooledTransport::new(
                Box::new(SessionIdleStream {
                    inner: c_bad,
                    session_id: 777,
                    alive: Arc::clone(&shared_alive),
                    dropped: Arc::clone(&bad_dropped),
                }),
                1,
            ))
            .await
            .unwrap()
    );
    assert_eq!(handle.query_idle_count().await.unwrap(), 2);

    // 1. Visitor 1 hits bad-ACK stream A, fails 0xAC handshake, and falls back to signaling.
    let (mut v1, v1_stream) = duplex(4096);
    handle
        .assign(ReverseVisitorInput {
            visitor: IoChannel::from_stream(v1_stream),
            peer: PeerAddr::Socket("127.0.0.1:41010".parse().unwrap()),
            target: TargetAddr::Ip("127.0.0.1:8080".parse().unwrap()),
            protocol: L4Protocol::Tcp,
            relay_idle_timeout: None,
            signaler: None,
        })
        .await
        .unwrap();
    v1.write_all(b"sig-fallbk").await.unwrap();
    v1.shutdown().await.unwrap();
    let mut got1 = [0u8; 10];
    v1.read_exact(&mut got1).await.unwrap();
    assert_eq!(&got1, b"sig-fallbk");
    assert_eq!(signal_calls.load(Ordering::Acquire), 1);

    // Healthy sibling B on session 777 MUST stay Idle and NOT be swept!
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(handle.query_idle_count().await.unwrap(), 1);
    assert_eq!(good_dropped.load(Ordering::Acquire), 0);

    // 2. Visitor 2 activates healthy sibling B cleanly via 0-RTT without calling signaling.
    let (mut v2, v2_stream) = duplex(4096);
    handle
        .assign(ReverseVisitorInput {
            visitor: IoChannel::from_stream(v2_stream),
            peer: PeerAddr::Socket("127.0.0.1:41011".parse().unwrap()),
            target: TargetAddr::Ip("127.0.0.1:8080".parse().unwrap()),
            protocol: L4Protocol::Tcp,
            relay_idle_timeout: None,
            signaler: None,
        })
        .await
        .unwrap();
    v2.write_all(b"rev-0rtt!!").await.unwrap();
    v2.shutdown().await.unwrap();
    let mut got2 = [0u8; 10];
    v2.read_exact(&mut got2).await.unwrap();
    assert_eq!(&got2, b"rev-0rtt!!");
    assert_eq!(signal_calls.load(Ordering::Acquire), 1);

    // 3. Explicit session sweep still evicts all matching siblings on a dead session.
    let sweep_alive = Arc::new(AtomicBool::new(true));
    let sweep_dropped = Arc::new(AtomicUsize::new(0));
    let mut _sweep_peers = Vec::new();
    for _ in 0..2 {
        let (c_sweep, s_sweep) = duplex(256);
        _sweep_peers.push(s_sweep);
        assert!(
            handle
                .supply_transport(PooledTransport::new(
                    Box::new(SessionIdleStream {
                        inner: c_sweep,
                        session_id: 888,
                        alive: Arc::clone(&sweep_alive),
                        dropped: Arc::clone(&sweep_dropped),
                    }),
                    1,
                ))
                .await
                .unwrap()
        );
    }
    assert_eq!(handle.sweep_session(888).await.unwrap(), 2);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(sweep_dropped.load(Ordering::Acquire), 2);
    assert_eq!(handle.query_idle_count().await.unwrap(), 0);

    handle.graceful_stop().await.unwrap();
    let _ = join.await.unwrap();
}
