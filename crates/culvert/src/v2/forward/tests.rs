// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! End-to-end prototype tests for `ForwardRunner` and `ForwardWorkflow`.
//! All comments and docstrings must be in English.

use crate::consts::{
    ACTIVATE_STREAM_ACK, ACTIVATE_STREAM_MAGIC, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT,
    DEFAULT_RELAY_DRAIN_TIMEOUT,
};
use crate::core::error::TunnelError;
use crate::core::stream::BoxFuture;
use crate::frontend::{Frontend, FrontendAccept, FrontendOutput};
use crate::io::addr::{L4Protocol, PeerAddr, TargetAddr};
use crate::io::channel::IoChannel;
use crate::transport::TransportConnector;
use crate::v2::forward::{ForwardRunner, ForwardWorkflow};
use crate::v2::relay::RelayCounters;
use crate::v2::{CoordinatorConfig, TaskOutcome, TunnelPhase};
use reusable_future::ReusableFutureRef;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

struct EchoConnector {
    direct_dials: Arc<AtomicUsize>,
    pooled_dials: Arc<AtomicUsize>,
    fail_first_pooled_ack: Arc<AtomicBool>,
}

impl EchoConnector {
    fn spawn_echo_server(mut server: DuplexStream, expect_activation: bool, send_ack: bool) {
        tokio::spawn(async move {
            if expect_activation {
                let mut hdr = [0u8; 3];
                if server.read_exact(&mut hdr).await.is_err() {
                    return;
                }
                assert_eq!(hdr[0], ACTIVATE_STREAM_MAGIC);
                let len = u16::from_be_bytes([hdr[1], hdr[2]]) as usize;
                let mut target_buf = vec![0u8; len];
                if server.read_exact(&mut target_buf).await.is_err() {
                    return;
                }
                if !send_ack {
                    // Simulate stale/half-open pooled stream dropping without 0xAC ACK.
                    return;
                }
                if server.write_all(&[ACTIVATE_STREAM_ACK]).await.is_err() {
                    return;
                }
                let _ = server.flush().await;
            }

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
    }
}

impl TransportConnector for EchoConnector {
    fn connect<'a>(
        &'a self,
        _target: &'a TargetAddr,
        _session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.direct_dials.fetch_add(1, Ordering::AcqRel);
        let (client, server) = duplex(4096);
        Self::spawn_echo_server(server, false, false);
        Box::pin(async move { Ok(IoChannel::from_stream(client)) })
    }

    fn connect_pooled<'a>(
        &'a self,
        _target: &'a TargetAddr,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.pooled_dials.fetch_add(1, Ordering::AcqRel);
        let send_ack = !self.fail_first_pooled_ack.swap(false, Ordering::AcqRel);
        let (client, server) = duplex(4096);
        Self::spawn_echo_server(server, true, send_ack);
        Box::pin(async move { Ok(IoChannel::from_stream(client)) })
    }
}

#[tokio::test]
async fn forward_runner_direct_echo_recycles_tasks_and_shuts_down() {
    let (frontend, accept_tx) = ChannelFrontend::new();
    let direct_dials = Arc::new(AtomicUsize::new(0));
    let connector = Arc::new(EchoConnector {
        direct_dials: Arc::clone(&direct_dials),
        pooled_dials: Arc::new(AtomicUsize::new(0)),
        fail_first_pooled_ack: Arc::new(AtomicBool::new(false)),
    });
    let counters = Arc::new(RelayCounters::new());

    let workflow = ForwardWorkflow::new(
        Some(connector),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    )
    .with_counters(Arc::clone(&counters));

    let runner = ForwardRunner::new(
        Box::new(frontend),
        TargetAddr::Ip("127.0.0.1:9000".parse().unwrap()),
        L4Protocol::Tcp,
        workflow,
        CoordinatorConfig::plain(DEFAULT_RELAY_DRAIN_TIMEOUT),
    )
    .with_prewarm_tasks(1);

    let cancel = CancellationToken::new();
    let runner_handle = tokio::spawn(runner.run(cancel.clone()));

    for msg in [b"first-req".as_slice(), b"second-req".as_slice()] {
        let (mut visitor, stream) = duplex(4096);
        accept_tx
            .send(Ok(FrontendAccept {
                channel: IoChannel::from_stream(stream),
                peer: PeerAddr::Socket("127.0.0.1:12345".parse().unwrap()),
            }))
            .await
            .unwrap();

        visitor.write_all(msg).await.unwrap();
        visitor.shutdown().await.unwrap();

        let mut echoed = vec![0u8; msg.len()];
        visitor.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, msg);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(direct_dials.load(Ordering::Acquire), 2);
    cancel.cancel();

    let summary = tokio::time::timeout(Duration::from_secs(1), runner_handle)
        .await
        .expect("runner must shut down promptly")
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(summary.completed_jobs.len(), 2);
    // Both jobs ran on the single prewarmed TaskId via Empty recycling with distinct JobIds.
    assert_eq!(
        summary.completed_jobs[0].task_id,
        summary.completed_jobs[1].task_id
    );
    assert_ne!(
        summary.completed_jobs[0].job_id,
        summary.completed_jobs[1].job_id
    );
    assert!(
        summary
            .completed_jobs
            .iter()
            .all(|job| job.outcome == TaskOutcome::Completed)
    );
}

#[tokio::test]
async fn forward_runner_pooled_activation_falls_back_to_direct_on_missing_ack() {
    let (frontend, accept_tx) = ChannelFrontend::new();
    let direct_dials = Arc::new(AtomicUsize::new(0));
    let pooled_dials = Arc::new(AtomicUsize::new(0));
    let connector = Arc::new(EchoConnector {
        direct_dials: Arc::clone(&direct_dials),
        pooled_dials: Arc::clone(&pooled_dials),
        fail_first_pooled_ack: Arc::new(AtomicBool::new(true)),
    });

    let coord_cfg = CoordinatorConfig::forward_pooled(
        Arc::clone(&connector) as Arc<dyn TransportConnector>,
        1,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let workflow = ForwardWorkflow::new(
        Some(connector),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    )
    .with_pooled(true);

    let runner = ForwardRunner::new(
        Box::new(frontend),
        TargetAddr::Domain("app.internal".into(), 8080),
        L4Protocol::Tcp,
        workflow,
        coord_cfg,
    );

    let cancel = CancellationToken::new();
    let runner_handle = tokio::spawn(runner.run(cancel.clone()));

    // Wait for DialDriver to prewarm 1 Idle task (whose server half drops without 0xAC ACK).
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(pooled_dials.load(Ordering::Acquire) >= 1);

    let (mut visitor, stream) = duplex(4096);
    accept_tx
        .send(Ok(FrontendAccept {
            channel: IoChannel::from_stream(stream),
            peer: PeerAddr::Socket("127.0.0.1:22222".parse().unwrap()),
        }))
        .await
        .unwrap();

    visitor.write_all(b"fallback-ok").await.unwrap();
    visitor.shutdown().await.unwrap();
    let mut echoed = [0u8; 11];
    visitor.read_exact(&mut echoed).await.unwrap();
    assert_eq!(&echoed, b"fallback-ok");

    // First pooled attempt failed ACK -> fell back to 1 direct dial, and IoTask recycled cleanly.
    assert_eq!(direct_dials.load(Ordering::Acquire), 1);
    tokio::time::sleep(Duration::from_millis(30)).await;

    cancel.cancel();
    let summary = tokio::time::timeout(Duration::from_secs(1), runner_handle)
        .await
        .expect("runner must shut down cleanly")
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(summary.completed_jobs.len(), 1);
    assert_eq!(summary.completed_jobs[0].outcome, TaskOutcome::Completed);
}

#[tokio::test]
async fn forward_runner_real_ws_server_e2e_pooled_and_recycled() {
    use crate::config::model::{AppConfig, EndpointConfig, ServerConfig, TunnelMode};
    use crate::dialer::endpoint::EndpointContext;
    use crate::frontend;
    use crate::io::addr::MappingSpec;
    use crate::server::ServerRunner;
    use crate::session::SessionManager;
    use crate::transport::TransportRegistry;
    use crate::transport::ws::WsTransportDriver;

    let probe_echo = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let echo_port = probe_echo.local_addr().unwrap().port();
    probe_echo.set_nonblocking(true).unwrap();
    let echo_listener = tokio::net::TcpListener::from_std(probe_echo).unwrap();

    let probe_ws = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let ws_port = probe_ws.local_addr().unwrap().port();
    drop(probe_ws);

    let probe_client = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let client_port = probe_client.local_addr().unwrap().port();
    drop(probe_client);

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

    let auth_token = "v2-ws-e2e-token";
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
            .create_connector(&endpoint_ctx, Some(auth_token), TunnelMode::Forward)
            .unwrap(),
    );

    let coord_cfg = CoordinatorConfig::forward_pooled(
        Arc::clone(&connector),
        2,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let client_listen = format!("127.0.0.1:{client_port}");
    let echo_target = format!("127.0.0.1:{echo_port}");
    let tcp_frontend = frontend::bind(&MappingSpec::new(
        L4Protocol::Tcp,
        &client_listen,
        &echo_target,
    ))
    .await
    .unwrap();

    let workflow = ForwardWorkflow::new(
        Some(connector),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    )
    .with_pooled(true);

    let runner = ForwardRunner::new(
        tcp_frontend,
        TargetAddr::Ip(echo_target.parse().unwrap()),
        L4Protocol::Tcp,
        workflow,
        coord_cfg,
    )
    .with_prewarm_tasks(1);

    let client_cancel = CancellationToken::new();
    let runner_handle = tokio::spawn(runner.run(client_cancel.clone()));

    // Wait briefly for prewarmed pooled streams, then run 3 sequential TCP connections.
    tokio::time::sleep(Duration::from_millis(120)).await;

    for idx in 0..3 {
        let mut tcp = tokio::net::TcpStream::connect(&client_listen)
            .await
            .unwrap();
        let msg = format!("v2-real-ws-echo-{idx}");
        tcp.write_all(msg.as_bytes()).await.unwrap();
        tcp.shutdown().await.unwrap();

        let mut buf = vec![0u8; msg.len()];
        tokio::time::timeout(Duration::from_secs(5), tcp.read_exact(&mut buf))
            .await
            .expect("echo timeout")
            .unwrap();
        assert_eq!(buf, msg.as_bytes());
    }

    tokio::time::sleep(Duration::from_millis(40)).await;
    client_cancel.cancel();
    let summary = tokio::time::timeout(Duration::from_secs(3), runner_handle)
        .await
        .expect("v2 ForwardRunner must shut down cleanly")
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(summary.completed_jobs.len(), 3);
    assert!(
        summary
            .completed_jobs
            .iter()
            .all(|job| job.outcome == TaskOutcome::Completed)
    );

    server_cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        let _ = tokio::join!(echo_handle, server_handle);
    })
    .await;
}

#[tokio::test]
async fn forward_runner_real_ws_udp_e2e() {
    use crate::config::model::{AppConfig, EndpointConfig, ServerConfig, TunnelMode};
    use crate::dialer::endpoint::EndpointContext;
    use crate::frontend;
    use crate::io::addr::MappingSpec;
    use crate::server::ServerRunner;
    use crate::session::SessionManager;
    use crate::transport::TransportRegistry;
    use crate::transport::ws::WsTransportDriver;

    let udp_echo = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let echo_port = udp_echo.local_addr().unwrap().port();

    let probe_ws = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let ws_port = probe_ws.local_addr().unwrap().port();
    drop(probe_ws);

    let probe_client_udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let client_udp_port = probe_client_udp.local_addr().unwrap().port();
    drop(probe_client_udp);

    let server_cancel = CancellationToken::new();
    let echo_cancel = server_cancel.clone();
    let echo_handle = tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        loop {
            tokio::select! {
                _ = echo_cancel.cancelled() => break,
                res = udp_echo.recv_from(&mut buf) => {
                    if let Ok((n, peer)) = res {
                        let _ = udp_echo.send_to(&buf[..n], peer).await;
                    }
                }
            }
        }
    });

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);

    let auth_token = "v2-ws-udp-token";
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
            .create_connector(&endpoint_ctx, Some(auth_token), TunnelMode::Forward)
            .unwrap(),
    );

    let client_listen = format!("127.0.0.1:{client_udp_port}");
    let echo_target = format!("127.0.0.1:{echo_port}");
    let udp_frontend = frontend::bind(&MappingSpec::new(
        L4Protocol::Udp,
        &client_listen,
        &echo_target,
    ))
    .await
    .unwrap();

    let workflow = ForwardWorkflow::new(
        Some(connector),
        DEFAULT_HEARTBEAT_INTERVAL,
        Duration::from_secs(5),
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let runner = ForwardRunner::new(
        udp_frontend,
        TargetAddr::Ip(echo_target.parse().unwrap()),
        L4Protocol::Udp,
        workflow,
        CoordinatorConfig::plain(DEFAULT_RELAY_DRAIN_TIMEOUT),
    );

    let client_cancel = CancellationToken::new();
    let runner_handle = tokio::spawn(runner.run(client_cancel.clone()));

    let client_sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client_sock.connect(&client_listen).await.unwrap();
    client_sock.send(b"v2-udp-over-ws").await.unwrap();

    let mut resp = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(5), client_sock.recv(&mut resp))
        .await
        .expect("UDP echo timeout")
        .unwrap();
    assert_eq!(&resp[..n], b"v2-udp-over-ws");

    client_cancel.cancel();
    let summary = tokio::time::timeout(Duration::from_secs(3), runner_handle)
        .await
        .expect("v2 UDP ForwardRunner must shut down cleanly")
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(summary.completed_jobs.len(), 1);

    server_cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        let _ = tokio::join!(echo_handle, server_handle);
    })
    .await;
}

#[tokio::test]
async fn forward_runner_real_h3_concurrent_streams_and_quic_shutdown() {
    use crate::config::model::{AppConfig, EndpointConfig, ServerConfig, TlsConfig, TunnelMode};
    use crate::dialer::endpoint::EndpointContext;
    use crate::frontend;
    use crate::io::addr::MappingSpec;
    use crate::server::ServerRunner;
    use crate::session::SessionManager;
    use crate::transport::TransportRegistry;
    use crate::transport::h3::H3TransportDriver;

    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let cert_path = temp_dir.path().join("cert.pem");
    let key_path = temp_dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();

    let probe_echo = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let echo_port = probe_echo.local_addr().unwrap().port();
    probe_echo.set_nonblocking(true).unwrap();
    let echo_listener = tokio::net::TcpListener::from_std(probe_echo).unwrap();

    let h3_port = {
        let mut found = 0u16;
        for _ in 0..50 {
            let t = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let p = t.local_addr().unwrap().port();
            if let Ok(u) = std::net::UdpSocket::bind(("127.0.0.1", p)) {
                drop(u);
                drop(t);
                found = p;
                break;
            }
        }
        assert_ne!(found, 0);
        found
    };

    let probe_client = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let client_port = probe_client.local_addr().unwrap().port();
    drop(probe_client);

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
    registry.register(H3TransportDriver);
    let registry = Arc::new(registry);

    let auth_token = "v2-h3-concurrent-token";
    let h3_listen = format!("127.0.0.1:{h3_port}");
    let server_runner = ServerRunner::new(
        ServerConfig {
            listen: vec![format!("h3://{h3_listen}/tunnel")],
            token: Some(auth_token.to_string()),
            default_target: None,
            tls: Some(TlsConfig {
                certificate: Some(cert_path),
                key: Some(key_path),
                ..Default::default()
            }),
            ..Default::default()
        },
        Arc::clone(&registry),
        Arc::new(SessionManager::new()),
    );
    let server_tok = server_cancel.clone();
    let server_handle = tokio::spawn(async move { server_runner.run(server_tok).await });
    tokio::time::sleep(Duration::from_millis(120)).await;

    let endpoint_cfg = EndpointConfig::Detailed {
        url: format!("h3://{h3_listen}/tunnel"),
        dns: None,
        dialer: None,
        tls: Some(TlsConfig {
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        http_headers: None,
    };
    let endpoint_ctx = EndpointContext::build(&endpoint_cfg, &AppConfig::default()).unwrap();
    let connector: Arc<dyn TransportConnector> = Arc::from(
        registry
            .create_connector(&endpoint_ctx, Some(auth_token), TunnelMode::Forward)
            .unwrap(),
    );

    let coord_cfg = CoordinatorConfig::forward_pooled(
        Arc::clone(&connector),
        4,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let client_listen = format!("127.0.0.1:{client_port}");
    let echo_target = format!("127.0.0.1:{echo_port}");
    let tcp_frontend = frontend::bind(&MappingSpec::new(
        L4Protocol::Tcp,
        &client_listen,
        &echo_target,
    ))
    .await
    .unwrap();

    let workflow = ForwardWorkflow::new(
        Some(connector),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    )
    .with_pooled(true);

    let runner = ForwardRunner::new(
        tcp_frontend,
        TargetAddr::Ip(echo_target.parse().unwrap()),
        L4Protocol::Tcp,
        workflow,
        coord_cfg,
    )
    .with_prewarm_tasks(4);

    let client_cancel = CancellationToken::new();
    let runner_handle = tokio::spawn(runner.run(client_cancel.clone()));
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Burst 24 concurrent TCP connections over multiplexed HTTP/3 + unified Coordinator Idle tasks.
    let mut join_set = tokio::task::JoinSet::new();
    for idx in 0..24usize {
        let addr = client_listen.clone();
        join_set.spawn(async move {
            let mut tcp = tokio::net::TcpStream::connect(&addr).await.unwrap();
            let msg = format!("h3-v2-concurrent-payload-{idx:02}");
            tcp.write_all(msg.as_bytes()).await.unwrap();
            tcp.shutdown().await.unwrap();
            let mut buf = vec![0u8; msg.len()];
            tokio::time::timeout(Duration::from_secs(10), tcp.read_exact(&mut buf))
                .await
                .expect("H3 concurrent echo timeout")
                .unwrap();
            assert_eq!(buf, msg.as_bytes());
        });
    }

    while let Some(res) = join_set.join_next().await {
        res.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(60)).await;

    client_cancel.cancel();
    let summary = tokio::time::timeout(Duration::from_secs(3), runner_handle)
        .await
        .expect("v2 H3 ForwardRunner must exit promptly")
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(summary.completed_jobs.len(), 24);
    assert!(
        summary
            .completed_jobs
            .iter()
            .all(|job| job.outcome == TaskOutcome::Completed)
    );

    server_cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        let _ = tokio::join!(echo_handle, server_handle);
    })
    .await;
}

#[tokio::test]
async fn forward_runner_coordinated_pool_drain_halts_replenish_and_zeroes_leases() {
    let (frontend, accept_tx) = ChannelFrontend::new();
    let direct_dials = Arc::new(AtomicUsize::new(0));
    let pooled_dials = Arc::new(AtomicUsize::new(0));
    let connector = Arc::new(EchoConnector {
        direct_dials,
        pooled_dials: Arc::clone(&pooled_dials),
        fail_first_pooled_ack: Arc::new(AtomicBool::new(false)),
    });

    let drain_timeout = Duration::from_millis(120);
    let coord_cfg = CoordinatorConfig::forward_pooled(
        Arc::clone(&connector) as Arc<dyn TransportConnector>,
        2,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        drain_timeout,
    );

    let workflow = ForwardWorkflow::new(
        Some(connector),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        Duration::from_millis(80),
    )
    .with_pooled(true);
    let runner = ForwardRunner::new(
        Box::new(frontend),
        TargetAddr::Domain("app.internal".into(), 8080),
        L4Protocol::Tcp,
        workflow,
        coord_cfg,
    );

    let cancel = CancellationToken::new();
    let runner_handle = tokio::spawn(runner.run(cancel.clone()));
    tokio::time::sleep(Duration::from_millis(40)).await;
    let initial_dials = pooled_dials.load(Ordering::Acquire);
    assert!(initial_dials >= 2);

    // Keep an active relay open across shutdown trigger so Coordinator enters Draining while
    // DialDriver stops replenishing before draining active jobs.
    let (mut visitor, stream) = duplex(4096);
    accept_tx
        .send(Ok(FrontendAccept {
            channel: IoChannel::from_stream(stream),
            peer: PeerAddr::Socket("127.0.0.1:22223".parse().unwrap()),
        }))
        .await
        .unwrap();

    visitor.write_all(b"ping").await.unwrap();
    let mut pong = [0u8; 4];
    visitor.read_exact(&mut pong).await.unwrap();
    assert_eq!(&pong, b"ping");

    let dials_at_shutdown = pooled_dials.load(Ordering::Acquire);
    cancel.cancel();

    let summary = tokio::time::timeout(Duration::from_secs(2), runner_handle)
        .await
        .expect("coordinated forward + idle shutdown must finish promptly")
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(summary.completed_jobs.len(), 1);
    assert_eq!(
        summary.completed_jobs[0].outcome,
        TaskOutcome::GracefullyDrained
    );
    // Once Coordinator enters Draining, no extra background dials are launched during drain.
    assert_eq!(pooled_dials.load(Ordering::Acquire), dials_at_shutdown);
}

#[tokio::test]
async fn forward_activation_failure_preserves_sibling_idle_and_session_close_sweeps_siblings() {
    use crate::core::stream::{HeartbeatStatus, TunnelStream};
    use crate::v2::forward::ForwardInput;
    use crate::v2::{Coordinator, IdlePolicy, PooledTransport};
    use std::pin::Pin;
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

    let spawn_activation_peer = |mut srv: DuplexStream, ack_byte: u8| {
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

    let direct_dials = Arc::new(AtomicUsize::new(0));
    let connector = Arc::new(EchoConnector {
        direct_dials: Arc::clone(&direct_dials),
        pooled_dials: Arc::new(AtomicUsize::new(0)),
        fail_first_pooled_ack: Arc::new(AtomicBool::new(false)),
    });
    let workflow = ForwardWorkflow::new(
        Some(connector),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    )
    .with_pooled(true);

    let cfg = CoordinatorConfig {
        drain_timeout: DEFAULT_RELAY_DRAIN_TIMEOUT,
        max_idle_capacity: 8,
        idle_policy: IdlePolicy::forward(Duration::from_millis(15), Duration::from_secs(30)),
        dial: None,
    };
    let (handle, join) = Coordinator::start_with_config(workflow, cfg, CancellationToken::new());

    // Push healthy sibling B first, then bad-ACK stream A second (LIFO pops A first).
    let shared_alive = Arc::new(AtomicBool::new(true));
    let good_dropped = Arc::new(AtomicUsize::new(0));
    let bad_dropped = Arc::new(AtomicUsize::new(0));

    let (c_good, s_good) = duplex(4096);
    let (c_bad, s_bad) = duplex(4096);
    spawn_activation_peer(s_good, ACTIVATE_STREAM_ACK);
    spawn_activation_peer(s_bad, 0xFF);

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

    // 1. Job 1 picks bad-ACK stream A, fails activation, and falls back to direct dial.
    let (mut v1, v1_stream) = duplex(4096);
    handle
        .assign(ForwardInput::new(
            IoChannel::from_stream(v1_stream),
            TargetAddr::Domain("app.internal".into(), 8080),
            L4Protocol::Tcp,
        ))
        .await
        .unwrap();
    v1.write_all(b"fallback-1").await.unwrap();
    v1.shutdown().await.unwrap();
    let mut got1 = [0u8; 10];
    v1.read_exact(&mut got1).await.unwrap();
    assert_eq!(&got1, b"fallback-1");
    assert_eq!(direct_dials.load(Ordering::Acquire), 1);

    // Healthy sibling B on session 777 MUST still be Idle and NOT swept!
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(handle.query_idle_count().await.unwrap(), 1);
    assert_eq!(good_dropped.load(Ordering::Acquire), 0);

    // 2. Job 2 consumes healthy sibling B directly without falling back to direct dial.
    let (mut v2, v2_stream) = duplex(4096);
    handle
        .assign(ForwardInput::new(
            IoChannel::from_stream(v2_stream),
            TargetAddr::Domain("app.internal".into(), 8080),
            L4Protocol::Tcp,
        ))
        .await
        .unwrap();
    v2.write_all(b"sibling-ok").await.unwrap();
    v2.shutdown().await.unwrap();
    let mut got2 = [0u8; 10];
    v2.read_exact(&mut got2).await.unwrap();
    assert_eq!(&got2, b"sibling-ok");
    assert_eq!(direct_dials.load(Ordering::Acquire), 1);

    // 3. True underlying session closure / sweep still evicts all matching siblings.
    let dead_session_alive = Arc::new(AtomicBool::new(false));
    let dead_session_dropped = Arc::new(AtomicUsize::new(0));
    let sweep_alive = Arc::new(AtomicBool::new(true));
    let sweep_dropped = Arc::new(AtomicUsize::new(0));

    let mut _dead_peers = Vec::new();
    for _ in 0..2 {
        let (c_dead, s_dead) = duplex(256);
        _dead_peers.push(s_dead);
        assert!(
            handle
                .supply_transport(PooledTransport::new(
                    Box::new(SessionIdleStream {
                        inner: c_dead,
                        session_id: 888,
                        alive: Arc::clone(&dead_session_alive),
                        dropped: Arc::clone(&dead_session_dropped),
                    }),
                    1,
                ))
                .await
                .unwrap()
        );
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(dead_session_dropped.load(Ordering::Acquire), 2);

    let mut _sweep_peers = Vec::new();
    for _ in 0..2 {
        let (c_sweep, s_sweep) = duplex(256);
        _sweep_peers.push(s_sweep);
        assert!(
            handle
                .supply_transport(PooledTransport::new(
                    Box::new(SessionIdleStream {
                        inner: c_sweep,
                        session_id: 999,
                        alive: Arc::clone(&sweep_alive),
                        dropped: Arc::clone(&sweep_dropped),
                    }),
                    1,
                ))
                .await
                .unwrap()
        );
    }
    assert_eq!(handle.sweep_session(999).await.unwrap(), 2);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(sweep_dropped.load(Ordering::Acquire), 2);
    assert_eq!(handle.query_idle_count().await.unwrap(), 0);

    handle.graceful_stop().await.unwrap();
    let _ = join.await.unwrap();
}

#[tokio::test]
async fn forward_activation_write_and_flush_backpressure_timeout_and_cancel() {
    use crate::core::stream::{HeartbeatStatus, TunnelStream};
    use crate::v2::forward::ForwardInput;
    use crate::v2::{Coordinator, IdlePolicy, PooledTransport};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::sync::Notify;

    #[derive(Clone, Copy)]
    enum StallStage {
        None,
        Write,
        Flush,
    }

    struct StallWriteStream {
        inner: DuplexStream,
        session_id: u64,
        stall: StallStage,
        entered: Option<Arc<Notify>>,
    }

    impl tokio::io::AsyncRead for StallWriteStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl tokio::io::AsyncWrite for StallWriteStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if matches!(self.stall, StallStage::Write) {
                if let Some(ref notify) = self.entered {
                    notify.notify_waiters();
                }
                return Poll::Pending;
            }
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            if matches!(self.stall, StallStage::Flush) {
                if let Some(ref notify) = self.entered {
                    notify.notify_waiters();
                }
                return Poll::Pending;
            }
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    impl TunnelStream for StallWriteStream {
        fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
            Pin::new(&mut self.inner).poll_check_alive(cx)
        }

        fn session_id(&self) -> Option<u64> {
            Some(self.session_id)
        }
    }

    // 1. poll_write() returning Pending forever hits ack_timeout and falls back cleanly,
    //    preserving healthy sibling idle tasks on the same session_id.
    {
        let direct_dials = Arc::new(AtomicUsize::new(0));
        let connector = Arc::new(EchoConnector {
            direct_dials: Arc::clone(&direct_dials),
            pooled_dials: Arc::new(AtomicUsize::new(0)),
            fail_first_pooled_ack: Arc::new(AtomicBool::new(false)),
        });
        let workflow = ForwardWorkflow::new(
            Some(connector),
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
            DEFAULT_RELAY_DRAIN_TIMEOUT,
        )
        .with_pooled(true)
        .with_ack_timeout(Duration::from_millis(60));

        let cfg = CoordinatorConfig {
            drain_timeout: DEFAULT_RELAY_DRAIN_TIMEOUT,
            max_idle_capacity: 4,
            idle_policy: IdlePolicy::forward(Duration::from_secs(30), Duration::from_secs(30)),
            dial: None,
        };
        let (handle, join) =
            Coordinator::start_with_config(workflow, cfg, CancellationToken::new());

        let (c_sibling, _s_sibling) = duplex(256);
        let (c_stall, _s_stall) = duplex(256);
        // Push healthy sibling first, stalled-write stream second (LIFO pops stalled first).
        assert!(
            handle
                .supply_transport(PooledTransport::new(
                    Box::new(StallWriteStream {
                        inner: c_sibling,
                        session_id: 501,
                        stall: StallStage::None,
                        entered: None,
                    }),
                    1,
                ))
                .await
                .unwrap()
        );
        assert!(
            handle
                .supply_transport(PooledTransport::new(
                    Box::new(StallWriteStream {
                        inner: c_stall,
                        session_id: 501,
                        stall: StallStage::Write,
                        entered: None,
                    }),
                    1,
                ))
                .await
                .unwrap()
        );

        let (mut visitor, stream) = duplex(4096);
        handle
            .assign(ForwardInput::new(
                IoChannel::from_stream(stream),
                TargetAddr::Domain("app.internal".into(), 8080),
                L4Protocol::Tcp,
            ))
            .await
            .unwrap();

        visitor.write_all(b"write-stall-fb").await.unwrap();
        visitor.shutdown().await.unwrap();
        let mut got = [0u8; 14];
        tokio::time::timeout(Duration::from_secs(1), visitor.read_exact(&mut got))
            .await
            .expect("write-stalled activation must time out and fall back promptly")
            .unwrap();
        assert_eq!(&got, b"write-stall-fb");
        assert_eq!(direct_dials.load(Ordering::Acquire), 1);
        assert_eq!(handle.query_idle_count().await.unwrap(), 1);

        handle.graceful_stop().await.unwrap();
        let _ = join.await.unwrap();
    }

    // 2. poll_flush() returning Pending forever with a long 5s ack_timeout is cancelled
    //    immediately on Drain without waiting for the timeout.
    {
        let workflow = ForwardWorkflow::new(
            None,
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
            Duration::from_millis(80),
        )
        .with_pooled(true);
        let cfg = CoordinatorConfig {
            drain_timeout: Duration::from_millis(80),
            max_idle_capacity: 4,
            idle_policy: IdlePolicy::forward(Duration::from_secs(30), Duration::from_secs(30)),
            dial: None,
        };
        let (handle, join) =
            Coordinator::start_with_config(workflow, cfg, CancellationToken::new());

        let entered = Arc::new(Notify::new());
        let (c_flush, _s_flush) = duplex(256);
        assert!(
            handle
                .supply_transport(PooledTransport::new(
                    Box::new(StallWriteStream {
                        inner: c_flush,
                        session_id: 502,
                        stall: StallStage::Flush,
                        entered: Some(Arc::clone(&entered)),
                    }),
                    1,
                ))
                .await
                .unwrap()
        );

        let entered_wait = entered.notified();
        tokio::pin!(entered_wait);
        entered_wait.as_mut().enable();

        let (_visitor, stream) = duplex(1024);
        handle
            .assign(ForwardInput::new(
                IoChannel::from_stream(stream),
                TargetAddr::Domain("app.internal".into(), 8080),
                L4Protocol::Tcp,
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("activation must enter stalled poll_flush");

        let started = std::time::Instant::now();
        handle.graceful_stop().await.unwrap();
        let summary = tokio::time::timeout(Duration::from_secs(1), join)
            .await
            .expect("Drain must cancel stalled activation before 5s ack_timeout")
            .unwrap();
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    }
}
