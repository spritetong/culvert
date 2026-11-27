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
use crate::v2::pool::{PoolConfig, TransportPool};
use crate::v2::relay::RelayCounters;
use crate::v2::{TaskOutcome, TunnelPhase};
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
        None,
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
        None,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
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

    let pool_cfg = PoolConfig::forward(
        Arc::clone(&connector) as Arc<dyn TransportConnector>,
        1,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
    );
    let (pool_handle, pool_join) = TransportPool::start(pool_cfg, CancellationToken::new());

    // Wait for 1 prewarmed stream (whose server half will drop without 0xAC ACK).
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(pool_handle.stats().await.unwrap().idle_count, 1);

    let workflow = ForwardWorkflow::new(
        Some(pool_handle.clone()),
        Some(connector),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let runner = ForwardRunner::new(
        Box::new(frontend),
        TargetAddr::Domain("app.internal".into(), 8080),
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
            peer: PeerAddr::Socket("127.0.0.1:22222".parse().unwrap()),
        }))
        .await
        .unwrap();

    visitor.write_all(b"fallback-ok").await.unwrap();
    visitor.shutdown().await.unwrap();
    let mut echoed = [0u8; 11];
    visitor.read_exact(&mut echoed).await.unwrap();
    assert_eq!(&echoed, b"fallback-ok");

    // First pooled attempt failed ACK -> fell back to 1 direct dial, and LeaseGuard released active_count.
    assert_eq!(direct_dials.load(Ordering::Acquire), 1);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(pool_handle.stats().await.unwrap().active_count, 0);

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

    let pool_cfg = PoolConfig::forward(
        Arc::clone(&connector),
        2,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
    );
    let (pool_handle, pool_join) = TransportPool::start(pool_cfg, CancellationToken::new());

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
        Some(pool_handle.clone()),
        Some(connector),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let runner = ForwardRunner::new(
        tcp_frontend,
        TargetAddr::Ip(echo_target.parse().unwrap()),
        L4Protocol::Tcp,
        workflow,
        Some((pool_handle, pool_join)),
        DEFAULT_RELAY_DRAIN_TIMEOUT,
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
        None,
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
        None,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
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

    let pool_cfg = PoolConfig::forward(
        Arc::clone(&connector),
        4,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
    );
    let (pool_handle, pool_join) = TransportPool::start(pool_cfg, CancellationToken::new());

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
        Some(pool_handle.clone()),
        Some(connector),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );

    let runner = ForwardRunner::new(
        tcp_frontend,
        TargetAddr::Ip(echo_target.parse().unwrap()),
        L4Protocol::Tcp,
        workflow,
        Some((pool_handle.clone(), pool_join)),
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    )
    .with_prewarm_tasks(4);

    let client_cancel = CancellationToken::new();
    let runner_handle = tokio::spawn(runner.run(client_cancel.clone()));
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Burst 24 concurrent TCP connections over multiplexed HTTP/3 + TransportPool.
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
    assert_eq!(pool_handle.stats().await.unwrap().active_count, 0);

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

    let pool_cfg = PoolConfig::forward(
        Arc::clone(&connector) as Arc<dyn TransportConnector>,
        2,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
    );
    let (pool_handle, pool_join) = TransportPool::start(pool_cfg, CancellationToken::new());
    let pool_observer = pool_handle.clone();
    tokio::time::sleep(Duration::from_millis(40)).await;
    let initial_dials = pooled_dials.load(Ordering::Acquire);
    assert!(initial_dials >= 2);

    let workflow = ForwardWorkflow::new(
        Some(pool_handle.clone()),
        Some(connector),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        Duration::from_millis(80),
    );
    let runner = ForwardRunner::new(
        Box::new(frontend),
        TargetAddr::Domain("app.internal".into(), 8080),
        L4Protocol::Tcp,
        workflow,
        Some((pool_handle, pool_join)),
        Duration::from_millis(120),
    );

    let cancel = CancellationToken::new();
    let runner_handle = tokio::spawn(runner.run(cancel.clone()));

    // Keep an active relay open across shutdown trigger so Coordinator enters Draining while
    // TransportPool must stop replenishing and wait for LeaseGuard drop before exiting.
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
        .expect("coordinated forward + pool shutdown must finish promptly")
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(summary.completed_jobs.len(), 1);
    assert_eq!(
        summary.completed_jobs[0].outcome,
        TaskOutcome::GracefullyDrained
    );
    // Once stop_replenishing() ran, no extra background dials were launched during drain.
    assert_eq!(pooled_dials.load(Ordering::Acquire), dials_at_shutdown);
    assert!(pool_observer.stats().await.is_err());
}
