// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Rigorous tests verifying dual-channel emergency promotion and command deduplication.
//! All comments and docstrings must be in English.

use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::io::duplex;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use culvert::consts::ACTIVATE_STREAM_PROMOTE;
use culvert::control::{
    ControlMessage, FramedControlStream, SignalingChannel, SignalingConfig, SignalingRouter,
};
use culvert::session::manager::ReverseInboundPool;

#[tokio::test]
async fn test_dual_channel_strictly_no_duplicate_dispatch() {
    let cancel = CancellationToken::new();
    let (ch1_tx, ch1_rx) = duplex(4096);
    let (ch2_tx, ch2_rx) = duplex(4096);

    let cfg = SignalingConfig {
        heartbeat_interval: Duration::from_secs(15),
        idle_timeout: Duration::from_secs(30),
    };

    let (h1, _rx1) = SignalingChannel::spawn(ch1_tx, cfg, cancel.child_token());
    let (h2, _rx2) = SignalingChannel::spawn(ch2_tx, cfg, cancel.child_token());

    let router = SignalingRouter::new(h1);
    router.attach(h2);

    let mut peer1 = FramedControlStream::new(ch1_rx);
    let mut peer2 = FramedControlStream::new(ch2_rx);

    let msg = ControlMessage::NewConnection {
        session_id: "strict_sess_1".into(),
        client_addr: "10.0.0.1:80".into(),
    };

    // Dispatch message through router
    router.send(msg.clone()).await.expect("send must succeed");

    // Primary channel must receive the message
    let received = timeout(Duration::from_millis(500), peer1.next())
        .await
        .expect("peer1 timeout")
        .expect("stream ended")
        .expect("control decode error");
    assert_eq!(received, msg);

    // Standby channel must NOT receive duplicate command dispatch
    let standby_res = timeout(Duration::from_millis(150), peer2.next()).await;
    assert!(
        standby_res.is_err(),
        "Standby channel must not receive duplicate command"
    );

    cancel.cancel();
}

#[tokio::test]
async fn test_dual_channel_burst_deduplication() {
    let cancel = CancellationToken::new();
    let (ch1_tx, ch1_rx) = duplex(16384);
    let (ch2_tx, ch2_rx) = duplex(16384);

    let cfg = SignalingConfig {
        heartbeat_interval: Duration::from_secs(15),
        idle_timeout: Duration::from_secs(30),
    };

    let (_h1, mut unified_rx) = SignalingChannel::spawn(ch1_rx, cfg, cancel.child_token());
    let _h2 = SignalingChannel::spawn_with_event_sink(
        ch2_rx,
        cfg,
        unified_rx.event_sink().expect("sink"),
        cancel.child_token(),
    );

    let mut client1 = FramedControlStream::new(ch1_tx);
    let mut client2 = FramedControlStream::new(ch2_tx);

    let session_id = "burst_shared_session_42".to_string();

    // Keep client streams alive so channels do not close prematurely
    for _ in 0..50 {
        let msg = ControlMessage::NewConnection {
            session_id: session_id.clone(),
            client_addr: "1.1.1.1:100".into(),
        };
        client1.send(msg).await.unwrap();
    }
    for _ in 0..50 {
        let msg = ControlMessage::NewConnection {
            session_id: session_id.clone(),
            client_addr: "2.2.2.2:200".into(),
        };
        client2.send(msg).await.unwrap();
    }

    // Application reader must receive EXACTLY ONE NewConnection for this session_id
    let first = timeout(Duration::from_millis(500), unified_rx.recv_msg())
        .await
        .expect("timeout waiting for first")
        .expect("recv error")
        .expect("no message");

    match first {
        ControlMessage::NewConnection {
            session_id: ref s, ..
        } => {
            assert_eq!(s, &session_id);
        }
        other => panic!("Unexpected message: {:?}", other),
    }

    // All 99 other duplicate frames must be completely suppressed by the deduplication filter
    let second = timeout(Duration::from_millis(150), unified_rx.recv_msg()).await;
    match second {
        Err(_) => {}       // Timed out as expected: zero duplicate frames were delivered!
        Ok(Ok(None)) => {} // Clean EOF
        Ok(Ok(Some(dup))) => {
            panic!(
                "Erroneous duplicate command delivered across channels: {:?}",
                dup
            );
        }
        Ok(Err(e)) => panic!("Unexpected channel error: {:?}", e),
    }

    drop(client1);
    drop(client2);
    cancel.cancel();
}

#[tokio::test]
async fn test_inbound_pool_stream_emergency_promotion_e2e() {
    let pool = ReverseInboundPool::new(8);
    let (client, mut server) = duplex(1024);

    assert!(pool.push(Box::new(client)));
    assert_eq!(pool.len(), 1);

    // Promote idle connection to signaling stream
    let promoted = pool
        .promote_idle(Duration::from_secs(5))
        .await
        .expect("promotion should succeed");
    assert_eq!(pool.len(), 0);

    // Peer reading the stream must detect the 0xA6 promotion byte
    let mut magic = [0u8; 1];
    tokio::io::AsyncReadExt::read_exact(&mut server, &mut magic)
        .await
        .expect("read magic");
    assert_eq!(magic[0], ACTIVATE_STREAM_PROMOTE);

    // Both ends can now safely establish a new SignalingChannel over the promoted stream
    let cancel = CancellationToken::new();
    let cfg = SignalingConfig {
        heartbeat_interval: Duration::from_millis(50),
        idle_timeout: Duration::from_millis(500),
    };

    let (srv_handle, _srv_rx) = SignalingChannel::spawn(promoted, cfg, cancel.child_token());
    let mut client_framed = FramedControlStream::new(server);

    // Verify control communication over promoted stream
    let test_msg = ControlMessage::NewConnection {
        session_id: "promoted_stream_test".into(),
        client_addr: "127.0.0.1:9999".into(),
    };
    srv_handle
        .send(test_msg.clone())
        .await
        .expect("send over promoted");

    let received = client_framed.next().await.unwrap().unwrap();
    assert_eq!(received, test_msg);

    cancel.cancel();
}

#[tokio::test]
async fn test_dual_channel_failover_maintains_deduplication_integrity() {
    let cancel = CancellationToken::new();
    let (ch1_tx, ch1_rx) = duplex(4096);
    let (ch2_tx, ch2_rx) = duplex(4096);

    let cfg = SignalingConfig {
        heartbeat_interval: Duration::from_secs(15),
        idle_timeout: Duration::from_secs(30),
    };

    let (h1, mut rx) = SignalingChannel::spawn(ch1_rx, cfg, cancel.child_token());
    let h2 = SignalingChannel::spawn_with_event_sink(
        ch2_rx,
        cfg,
        rx.event_sink().expect("sink"),
        cancel.child_token(),
    );

    let router = SignalingRouter::new(h1);
    router.attach(h2);

    let mut peer1 = FramedControlStream::new(ch1_tx);
    let mut peer2 = FramedControlStream::new(ch2_tx);

    // 1. Send command via Primary
    let msg1 = ControlMessage::NewConnection {
        session_id: "sess_pre_failover".into(),
        client_addr: "10.0.0.1:1".into(),
    };
    router.send(msg1.clone()).await.expect("send msg1");
    let r1 = peer1.next().await.unwrap().unwrap();
    assert_eq!(r1, msg1);

    // 2. Simulate Primary connection break
    drop(peer1);

    // 3. Send command via router after failover
    let msg2 = ControlMessage::NewConnection {
        session_id: "sess_post_failover".into(),
        client_addr: "10.0.0.2:2".into(),
    };
    router.send(msg2.clone()).await.expect("send msg2");
    let r2 = peer2.next().await.unwrap().unwrap();
    assert_eq!(r2, msg2);

    // 4. If a stale/retransmitted msg1 arrives on Channel 2, receiver must reject it
    peer2
        .send(msg1.clone())
        .await
        .expect("send stale msg1 on ch2");
    peer2.send(msg2.clone()).await.expect("send msg2 on ch2");

    // Receiver should deliver msg1 once, msg2 once, and no duplicates!
    let delivered_1 = rx.recv_msg().await.unwrap().unwrap();
    assert_eq!(delivered_1, msg1);

    let delivered_2 = rx.recv_msg().await.unwrap().unwrap();
    assert_eq!(delivered_2, msg2);

    // Nothing further should be delivered
    let trailing = timeout(Duration::from_millis(150), rx.recv_msg()).await;
    assert!(
        trailing.is_err(),
        "Duplicate commands must not be delivered"
    );

    cancel.cancel();
}

mod e2e_reverse_promotion_tests {
    use parking_lot::Mutex;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream, UdpSocket};
    use tokio_util::sync::CancellationToken;

    use culvert::backend::registry::TransportRegistry;
    use culvert::config::model::{
        AppConfig, EndpointConfig, PortMappingConfig, ServerConfig, TunnelConfig, TunnelMode,
        TunnelRetryConfig,
    };
    use culvert::dialer::endpoint::EndpointContext;
    use culvert::io::addr::L4Protocol;
    use culvert::server::ServerRunner;
    use culvert::session::SessionManager;
    use culvert::supervisor::TunnelSupervisor;
    use culvert::transport::ws::WsTransportDriver;
    use culvert::tunnel::run_reverse_tunnel;

    struct DroppingTcpProxy {
        proxy_port: u16,
        reject_new: Arc<AtomicBool>,
        conns: Arc<Mutex<Vec<CancellationToken>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl DroppingTcpProxy {
        fn start(target_port: u16, cancel: CancellationToken) -> Self {
            let std_listener = std::net::TcpListener::bind("127.0.0.1:0")
                .expect("Failed to bind DroppingTcpProxy");
            let proxy_port = std_listener.local_addr().unwrap().port();
            std_listener.set_nonblocking(true).unwrap();
            let listener = TcpListener::from_std(std_listener).unwrap();

            let reject_new = Arc::new(AtomicBool::new(false));
            let conns: Arc<Mutex<Vec<CancellationToken>>> = Arc::new(Mutex::new(Vec::new()));

            let reject_clone = Arc::clone(&reject_new);
            let conns_clone = Arc::clone(&conns);
            let task = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        res = listener.accept() => {
                            let (mut inbound, _) = match res {
                                Ok(pair) => pair,
                                Err(_) => break,
                            };
                            if reject_clone.load(Ordering::SeqCst) {
                                drop(inbound);
                                continue;
                            }
                            let conn_cancel = CancellationToken::new();
                            conns_clone.lock().push(conn_cancel.clone());
                            let parent_cancel = cancel.clone();
                            tokio::spawn(async move {
                                let Ok(mut outbound) =
                                    TcpStream::connect(("127.0.0.1", target_port)).await
                                else {
                                    return;
                                };
                                tokio::select! {
                                    _ = conn_cancel.cancelled() => {}
                                    _ = parent_cancel.cancelled() => {}
                                    _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                                }
                            });
                        }
                    }
                }
            });

            Self {
                proxy_port,
                reject_new,
                conns,
                task,
            }
        }

        fn conn_count(&self) -> usize {
            self.conns.lock().len()
        }

        fn kill_conn(&self, idx: usize) {
            if let Some(tok) = self.conns.lock().get(idx) {
                tok.cancel();
            }
        }

        fn set_reject_new(&self, reject: bool) {
            self.reject_new.store(reject, Ordering::SeqCst);
        }
    }

    fn get_dual_ephemeral_port() -> u16 {
        for _ in 0..100 {
            let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = tcp.local_addr().unwrap().port();
            if let Ok(udp) = std::net::UdpSocket::bind(("127.0.0.1", port)) {
                drop(udp);
                drop(tcp);
                return port;
            }
            drop(tcp);
        }
        panic!("Failed to allocate ephemeral port");
    }

    fn spawn_udp_echo_server(cancel: CancellationToken) -> (tokio::task::JoinHandle<()>, u16) {
        let std_udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = std_udp.local_addr().unwrap().port();
        std_udp.set_nonblocking(true).unwrap();
        let sock = UdpSocket::from_std(std_udp).unwrap();
        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    res = sock.recv_from(&mut buf) => {
                        match res {
                            Ok((n, peer)) => {
                                let _ = sock.send_to(&buf[..n], peer).await;
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });
        (handle, port)
    }

    async fn udp_roundtrip(sock: &UdpSocket, addr: &str, payload: &[u8]) -> Vec<u8> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut buf = vec![0u8; 4096];
        loop {
            sock.send_to(payload, addr)
                .await
                .expect("UDP send_to failed");
            match tokio::time::timeout(Duration::from_millis(250), sock.recv_from(&mut buf)).await {
                Ok(Ok((n, _))) => return buf[..n].to_vec(),
                Ok(Err(e)) => panic!("UDP recv_from error: {}", e),
                Err(_) if tokio::time::Instant::now() < deadline => continue,
                Err(_) => panic!(
                    "UDP roundtrip to {} timed out for {:?}",
                    addr,
                    String::from_utf8_lossy(payload)
                ),
            }
        }
    }

    #[tokio::test]
    async fn test_e2e_server_initiated_0xa6_promotion_keeps_udp_reverse_alive() {
        let cancel = CancellationToken::new();
        let (echo_handle, echo_port) = spawn_udp_echo_server(cancel.clone());

        let server_ws_port = get_dual_ephemeral_port();
        let reverse_udp_port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let reverse_listen = format!("127.0.0.1:{}", reverse_udp_port);
        let auth_token = "e2e_0xa6_token";

        let mut registry = TransportRegistry::new();
        registry.register(WsTransportDriver);
        let registry = Arc::new(registry);
        let session_mgr = Arc::new(SessionManager::new());

        let server_runner = ServerRunner::new(
            ServerConfig {
                listen: vec![format!("ws://127.0.0.1:{}/tunnel", server_ws_port)],
                token: Some(auth_token.to_string()),
                ..Default::default()
            },
            Arc::clone(&registry),
            Arc::clone(&session_mgr),
        );
        let s_cancel = cancel.clone();
        let server_handle = tokio::spawn(async move {
            let _ = server_runner.run(s_cancel).await;
        });

        tokio::time::sleep(Duration::from_millis(80)).await;
        let proxy = DroppingTcpProxy::start(server_ws_port, cancel.clone());

        let tunnel_cfg = TunnelConfig {
            tag: "e2e-0xa6-udp".to_string(),
        uuid: None,
        pool: None,
            endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/tunnel", proxy.proxy_port)),
            token: Some(auth_token.to_string()),
            pool_size: Some(3),
            failover: None,
            multiplex: None,
            retry: None,
            http_headers: None,
            mappings: vec![PortMappingConfig {
                mode: TunnelMode::Reverse,
                protocol: L4Protocol::Udp,
                listen: reverse_listen.clone(),
                target: format!("127.0.0.1:{}", echo_port),
                udp_idle_timeout: None,
            }],
        };
        let ep_ctx = EndpointContext::build(&tunnel_cfg.endpoint, &AppConfig::default()).unwrap();
        let c_cancel = cancel.clone();
        let c_reg = Arc::clone(&registry);
        let client_handle = tokio::spawn(async move {
            let _ = run_reverse_tunnel(tunnel_cfg, ep_ctx, c_reg, c_cancel).await;
        });

        // Wait until primary control (conn 0) + 3 idle pool streams have connected.
        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while proxy.conn_count() < 4 && tokio::time::Instant::now() < ready_deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            proxy.conn_count() >= 4,
            "Expected control + 3 pool connections, got {}",
            proxy.conn_count()
        );
        tokio::time::sleep(Duration::from_millis(150)).await;

        let udp_sock_1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        assert_eq!(
            udp_roundtrip(&udp_sock_1, &reverse_listen, b"before-0xa6").await,
            b"before-0xa6"
        );

        // Block any new TCP connections so only 0xA6 promotion over an existing pool stream can recover.
        proxy.set_reject_new(true);
        proxy.kill_conn(0);
        tokio::time::sleep(Duration::from_millis(150)).await;

        assert!(
            !client_handle.is_finished(),
            "run_reverse_tunnel must stay alive via server-initiated 0xA6 promotion"
        );
        assert_eq!(
            session_mgr.grace_listener_count(),
            0,
            "0xA6 promotion should avoid entering grace period"
        );

        assert_eq!(
            udp_roundtrip(&udp_sock_1, &reverse_listen, b"after-0xa6-existing").await,
            b"after-0xa6-existing"
        );

        let udp_sock_2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        assert_eq!(
            udp_roundtrip(&udp_sock_2, &reverse_listen, b"after-0xa6-new-visitor").await,
            b"after-0xa6-new-visitor"
        );

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            let _ = tokio::join!(echo_handle, server_handle, client_handle, proxy.task);
        })
        .await;
    }

    #[tokio::test]
    async fn test_e2e_udp_reverse_grace_period_empty_pool_no_deadlock_and_reconnect() {
        let cancel = CancellationToken::new();
        let (echo_handle, echo_port) = spawn_udp_echo_server(cancel.clone());

        let server_ws_port = get_dual_ephemeral_port();
        let reverse_udp_port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let reverse_listen = format!("127.0.0.1:{}", reverse_udp_port);
        let auth_token = "e2e_grace_token";

        let mut registry = TransportRegistry::new();
        registry.register(WsTransportDriver);
        let registry = Arc::new(registry);
        let session_mgr = Arc::new(SessionManager::new());

        let server_runner = ServerRunner::new(
            ServerConfig {
                listen: vec![format!("ws://127.0.0.1:{}/tunnel", server_ws_port)],
                token: Some(auth_token.to_string()),
                ..Default::default()
            },
            Arc::clone(&registry),
            Arc::clone(&session_mgr),
        );
        let s_cancel = cancel.clone();
        let server_handle = tokio::spawn(async move {
            let _ = server_runner.run(s_cancel).await;
        });

        tokio::time::sleep(Duration::from_millis(80)).await;
        let proxy = DroppingTcpProxy::start(server_ws_port, cancel.clone());

        let app_cfg = AppConfig {
            tunnels: vec![TunnelConfig {
                tag: "e2e-udp-grace".to_string(),
        uuid: None,
        pool: None,
                endpoint: EndpointConfig::Simple(format!(
                    "ws://127.0.0.1:{}/tunnel",
                    proxy.proxy_port
                )),
                token: Some(auth_token.to_string()),
                pool_size: Some(1),
                failover: None,
                multiplex: None,
                retry: Some(TunnelRetryConfig {
                    delay: Some(Duration::from_millis(50)),
                    max_retries: None,
                }),
                http_headers: None,
                mappings: vec![PortMappingConfig {
                    mode: TunnelMode::Reverse,
                    protocol: L4Protocol::Udp,
                    listen: reverse_listen.clone(),
                    target: format!("127.0.0.1:{}", echo_port),
                    udp_idle_timeout: None,
                }],
            }],
            ..Default::default()
        };
        let supervisor = TunnelSupervisor::new(app_cfg, cancel.clone());
        let sup_handle = tokio::spawn(async move {
            let _ = supervisor.run().await;
        });

        // Wait for primary control (0) + single pool stream (1).
        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while proxy.conn_count() < 2 && tokio::time::Instant::now() < ready_deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(proxy.conn_count() >= 2);
        tokio::time::sleep(Duration::from_millis(120)).await;

        // First UDP flow consumes conn 1 so its data relay remains independent of control.
        let udp_sock_1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        assert_eq!(
            udp_roundtrip(&udp_sock_1, &reverse_listen, b"before-grace").await,
            b"before-grace"
        );

        // Wait for pool replenishment (conn 2), then block new connects and kill control + idle pool
        // while keeping conn 1 (the active UDP data stream) alive.
        let replenish_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while proxy.conn_count() < 3 && tokio::time::Instant::now() < replenish_deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        proxy.set_reject_new(true);
        proxy.kill_conn(0);
        for idx in 2..proxy.conn_count() {
            proxy.kill_conn(idx);
        }

        // Wait until server enters the 10s grace period with zero control channels.
        let grace_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while session_mgr.grace_listener_count() == 0
            && tokio::time::Instant::now() < grace_deadline
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            session_mgr.grace_listener_count(),
            1,
            "Server must retain UDP listener inside grace period on empty-pool disconnect"
        );

        // Existing UDP flow must continue relaying while no control channel exists.
        assert_eq!(
            udp_roundtrip(&udp_sock_1, &reverse_listen, b"during-grace-no-control").await,
            b"during-grace-no-control"
        );

        // Re-allow connections; TunnelSupervisor must exit cleanly (no deadlock) and reattach.
        proxy.set_reject_new(false);
        let reattach_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while session_mgr.grace_listener_count() > 0
            && tokio::time::Instant::now() < reattach_deadline
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            session_mgr.grace_listener_count(),
            0,
            "TunnelSupervisor must reconnect and clear grace slot without EADDRINUSE"
        );

        tokio::time::sleep(Duration::from_millis(120)).await;
        let udp_sock_2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        assert_eq!(
            udp_roundtrip(&udp_sock_2, &reverse_listen, b"after-reattach-new-visitor").await,
            b"after-reattach-new-visitor"
        );
        assert_eq!(
            udp_roundtrip(&udp_sock_1, &reverse_listen, b"after-reattach-existing").await,
            b"after-reattach-existing"
        );

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            let _ = tokio::join!(echo_handle, server_handle, sup_handle, proxy.task);
        })
        .await;
    }
}
