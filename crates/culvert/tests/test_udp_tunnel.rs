// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! End-to-end integration tests for UDP port mapping across forward/reverse modes
//! and WS, HTTP/3, and raw QUIC transports.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio_util::sync::CancellationToken;

use culvert::backend::registry::TransportRegistry;
use culvert::backend::{TransportConnector, TransportDriver, TransportListener};
use culvert::config::model::{
    AppConfig, DialerConfig, EndpointConfig, MappingRuleConfig, ServerConfig, TlsConfig,
    TunnelConfig, TunnelMappingsConfig, TunnelMode,
};
use culvert::dialer::endpoint::EndpointContext;
use culvert::io::addr::{L4Protocol, TargetAddr};
use culvert::io::channel::IoChannel;
use culvert::io::error::TunnelError;
use culvert::io::stream::BoxFuture;
use culvert::server::ServerRunner;
use culvert::session::SessionManager;
use culvert::supervisor::TunnelSupervisor;
use culvert::transport::h3::H3TransportDriver;
use culvert::transport::ws::WsTransportDriver;
use culvert::tunnel::{run_forward_tunnel, run_reverse_tunnel};

/// Finds an available ephemeral port free for both TCP and UDP.
fn get_dual_ephemeral_port() -> u16 {
    for _ in 0..100 {
        let tcp = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("Failed to bind ephemeral TCP socket");
        let port = tcp
            .local_addr()
            .expect("Failed to read TCP local_addr")
            .port();
        if let Ok(udp) = std::net::UdpSocket::bind(("127.0.0.1", port)) {
            drop(udp);
            drop(tcp);
            return port;
        }
        drop(tcp);
    }
    panic!("Failed to allocate dual TCP/UDP ephemeral port");
}

/// Finds an available ephemeral UDP port.
fn get_udp_ephemeral_port() -> u16 {
    let udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("Failed to bind ephemeral UDP port");
    udp.local_addr()
        .expect("Failed to read UDP local_addr")
        .port()
}

/// Spawns a TCP echo server bound to `127.0.0.1:0`.
fn spawn_tcp_echo_server(cancel_token: CancellationToken) -> (tokio::task::JoinHandle<()>, u16) {
    let std_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind TCP echo server");
    let port = std_listener
        .local_addr()
        .expect("Failed to read TCP echo addr")
        .port();
    std_listener
        .set_nonblocking(true)
        .expect("Failed to set nonblocking TCP echo listener");
    let listener = TcpListener::from_std(std_listener).expect("Failed to convert TCP listener");
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                res = listener.accept() => {
                    let (mut socket, _) = match res {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    tokio::spawn(async move {
                        let (mut rd, mut wr) = socket.split();
                        let _ = tokio::io::copy(&mut rd, &mut wr).await;
                    });
                }
            }
        }
    });
    (handle, port)
}

/// Spawns a UDP echo server bound to `127.0.0.1:0` (supports 0-byte and large datagrams).
fn spawn_udp_echo_server(cancel_token: CancellationToken) -> (tokio::task::JoinHandle<()>, u16) {
    let std_udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("Failed to bind UDP echo server");
    let port = std_udp
        .local_addr()
        .expect("Failed to read UDP echo addr")
        .port();
    std_udp
        .set_nonblocking(true)
        .expect("Failed to set nonblocking UDP echo socket");
    let socket = UdpSocket::from_std(std_udp).expect("Failed to convert UDP socket");
    let handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 65536];
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                res = socket.recv_from(&mut buf) => {
                    match res {
                        Ok((n, peer)) => {
                            let _ = socket.send_to(&buf[..n], peer).await;
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });
    (handle, port)
}

/// Sends one UDP datagram and waits up to 5 seconds for an echoed packet matching `expected`.
async fn udp_roundtrip(client: &UdpSocket, dest: &str, payload: &[u8]) -> Vec<u8> {
    client
        .send_to(payload, dest)
        .await
        .expect("Failed to send UDP test packet");
    let mut buf = vec![0u8; payload.len().max(64) + 1024];
    let (n, _) = tokio::time::timeout(Duration::from_secs(5), client.recv_from(&mut buf))
        .await
        .expect("Timed out waiting for UDP echo response")
        .expect("Failed to recv UDP echo response");
    buf.truncate(n);
    buf
}

#[tokio::test]
async fn test_forward_udp_ws_unpooled_and_pooled() {
    let cancel_token = CancellationToken::new();
    let (echo_handle, echo_port) = spawn_udp_echo_server(cancel_token.clone());

    let server_ws_port = get_dual_ephemeral_port();
    let unpooled_udp_port = get_udp_ephemeral_port();
    let pooled_udp_port = get_udp_ephemeral_port();
    let auth_token = "udp_ws_secret_token";

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://127.0.0.1:{}/tunnel", server_ws_port)],
        token: Some(auth_token.to_string()),
        ..Default::default()
    };
    let server_runner = ServerRunner::new(
        server_config,
        Arc::clone(&registry),
        Arc::clone(&session_mgr),
    );
    let server_cancel = cancel_token.clone();
    let server_handle = tokio::spawn(async move {
        let _ = server_runner.run(server_cancel).await;
    });

    // Unpooled UDP forward tunnel
    let unpooled_listen = format!("127.0.0.1:{}", unpooled_udp_port);
    let unpooled_cfg = TunnelConfig {
        tag: "udp-fwd-unpooled".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/tunnel", server_ws_port)),
        token: Some(auth_token.to_string()),
        failover: None,
        multiplex: None,
        retry: None,
        http_headers: None,
        mappings: TunnelMappingsConfig::forward(vec![MappingRuleConfig::udp(
            unpooled_listen.clone(),
            format!("127.0.0.1:{}", echo_port),
        )]),
    };
    let app_cfg = AppConfig::default();
    let ep_ctx1 = EndpointContext::build(&unpooled_cfg.endpoint, &app_cfg).expect("ep_ctx1");
    let c1 = cancel_token.clone();
    let reg1 = Arc::clone(&registry);
    let client_unpooled = tokio::spawn(async move {
        let _ = run_forward_tunnel(unpooled_cfg, ep_ctx1, reg1, c1).await;
    });

    // Pooled 0-RTT UDP forward tunnel
    let pooled_listen = format!("127.0.0.1:{}", pooled_udp_port);
    let pooled_cfg = TunnelConfig {
        tag: "udp-fwd-pooled".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/tunnel", server_ws_port)),
        token: Some(auth_token.to_string()),
        failover: None,
        multiplex: None,
        retry: None,
        http_headers: None,
        mappings: TunnelMappingsConfig::forward_pool(
            4,
            vec![MappingRuleConfig::udp(
                pooled_listen.clone(),
                format!("127.0.0.1:{}", echo_port),
            )],
        ),
    };
    let ep_ctx2 = EndpointContext::build(&pooled_cfg.endpoint, &app_cfg).expect("ep_ctx2");
    let c2 = cancel_token.clone();
    let reg2 = Arc::clone(&registry);
    let client_pooled = tokio::spawn(async move {
        let _ = run_forward_tunnel(pooled_cfg, ep_ctx2, reg2, c2).await;
    });

    // Allow listeners and pool pre-warming to become ready
    tokio::time::sleep(Duration::from_millis(250)).await;

    // 1. Exercise unpooled flow with normal, large, and 0-byte datagrams
    let client1 = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind UDP test client 1");
    assert_eq!(
        udp_roundtrip(&client1, &unpooled_listen, b"hello udp unpooled").await,
        b"hello udp unpooled"
    );
    assert_eq!(udp_roundtrip(&client1, &unpooled_listen, b"").await, b"");
    let large_payload = vec![0x5Cu8; 2048];
    assert_eq!(
        udp_roundtrip(&client1, &unpooled_listen, &large_payload).await,
        large_payload
    );

    // 2. Exercise pooled 0-RTT (ACTIVATE_STREAM_UDP 0xA7) across multiple distinct UDP clients
    for i in 0..5 {
        let cli = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind UDP pooled client");
        let msg = format!("pooled-udp-packet-{}", i);
        assert_eq!(
            udp_roundtrip(&cli, &pooled_listen, msg.as_bytes()).await,
            msg.as_bytes()
        );
    }

    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3, r4) =
            tokio::join!(echo_handle, server_handle, client_unpooled, client_pooled);
        r1.expect("echo handle panicked");
        r2.expect("server handle panicked");
        r3.expect("unpooled client panicked");
        r4.expect("pooled client panicked");
    })
    .await
    .expect("Timed out shutting down test_forward_udp_ws_unpooled_and_pooled");
}

#[tokio::test]
async fn test_forward_udp_h3_and_raw_quic() {
    let cancel_token = CancellationToken::new();
    let (echo_handle, echo_port) = spawn_udp_echo_server(cancel_token.clone());

    let h3_server_port = get_dual_ephemeral_port();
    let quic_server_port = get_dual_ephemeral_port();
    let h3_client_udp_port = get_udp_ephemeral_port();
    let quic_client_udp_port = get_udp_ephemeral_port();

    let mut registry = TransportRegistry::new();
    registry.register(H3TransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![
            format!("h3://127.0.0.1:{}/tunnel", h3_server_port),
            format!("quic://127.0.0.1:{}/raw", quic_server_port),
        ],
        token: Some("udp_quic_token".to_string()),
        ..Default::default()
    };
    let server_runner = ServerRunner::new(
        server_config,
        Arc::clone(&registry),
        Arc::clone(&session_mgr),
    );
    let server_cancel = cancel_token.clone();
    let server_handle = tokio::spawn(async move {
        let _ = server_runner.run(server_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    let h3_listen = format!("127.0.0.1:{}", h3_client_udp_port);
    let quic_listen = format!("127.0.0.1:{}", quic_client_udp_port);
    let app_config = AppConfig {
        tls: Some(TlsConfig {
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        tunnels: vec![
            TunnelConfig {
                tag: "udp-over-h3".to_string(),
                uuid: None,
                endpoint: EndpointConfig::Simple(format!(
                    "h3://127.0.0.1:{}/tunnel",
                    h3_server_port
                )),
                token: Some("udp_quic_token".to_string()),
                failover: None,
                multiplex: None,
                retry: None,
                http_headers: None,
                mappings: TunnelMappingsConfig::forward(vec![MappingRuleConfig::udp(
                    h3_listen.clone(),
                    format!("127.0.0.1:{}", echo_port),
                )]),
            },
            TunnelConfig {
                tag: "udp-over-quic-pooled".to_string(),
                uuid: None,
                endpoint: EndpointConfig::Simple(format!(
                    "quic://127.0.0.1:{}/raw",
                    quic_server_port
                )),
                token: Some("udp_quic_token".to_string()),
                failover: None,
                multiplex: None,
                retry: None,
                http_headers: None,
                mappings: TunnelMappingsConfig::forward_pool(
                    2,
                    vec![MappingRuleConfig::udp(
                        quic_listen.clone(),
                        format!("127.0.0.1:{}", echo_port),
                    )],
                ),
            },
        ],
        ..Default::default()
    };

    let sup = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = sup.run().await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let h3_sock = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind H3 UDP test socket");
    assert_eq!(
        udp_roundtrip(&h3_sock, &h3_listen, b"h3-udp-packet-1").await,
        b"h3-udp-packet-1"
    );
    assert_eq!(
        udp_roundtrip(&h3_sock, &h3_listen, b"h3-udp-packet-2").await,
        b"h3-udp-packet-2"
    );

    let quic_sock = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind QUIC UDP test socket");
    assert_eq!(
        udp_roundtrip(&quic_sock, &quic_listen, b"quic-udp-packet-1").await,
        b"quic-udp-packet-1"
    );
    assert_eq!(
        udp_roundtrip(&quic_sock, &quic_listen, b"quic-udp-packet-2").await,
        b"quic-udp-packet-2"
    );

    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(echo_handle, server_handle, sup_handle);
        r1.expect("echo handle panicked");
        r2.expect("server handle panicked");
        r3.expect("supervisor handle panicked");
    })
    .await
    .expect("Timed out shutting down test_forward_udp_h3_and_raw_quic");
}

#[tokio::test]
async fn test_forward_udp_concurrent_multi_client_nat_demux() {
    let cancel_token = CancellationToken::new();
    let (echo_handle, echo_port) = spawn_udp_echo_server(cancel_token.clone());

    let server_ws_port = get_dual_ephemeral_port();
    let fwd_udp_port = get_udp_ephemeral_port();
    let fwd_listen = format!("127.0.0.1:{}", fwd_udp_port);

    let app_config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("ws://127.0.0.1:{}/tunnel", server_ws_port)],
            token: Some("nat_demux_token".to_string()),
            ..Default::default()
        }),
        tunnels: vec![TunnelConfig {
            tag: "udp-nat-demux".to_string(),
            uuid: None,
            endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/tunnel", server_ws_port)),
            token: Some("nat_demux_token".to_string()),
            failover: None,
            multiplex: None,
            retry: None,
            http_headers: None,
            mappings: TunnelMappingsConfig::forward_pool(
                3,
                vec![MappingRuleConfig::udp(
                    fwd_listen.clone(),
                    format!("127.0.0.1:{}", echo_port),
                )],
            ),
        }],
        ..Default::default()
    };

    let supervisor = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    tokio::time::sleep(Duration::from_millis(350)).await;

    let mut clients = Vec::new();
    for client_id in 0..4 {
        let addr = fwd_listen.clone();
        clients.push(tokio::spawn(async move {
            let sock = UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("Failed to bind concurrent UDP client");
            for seq in 0..5 {
                let msg = format!("client-{}-seq-{}", client_id, seq);
                let reply = udp_roundtrip(&sock, &addr, msg.as_bytes()).await;
                assert_eq!(reply, msg.as_bytes(), "NAT demux mismatch across clients");
            }
        }));
    }

    for h in clients {
        h.await.expect("Concurrent UDP client task panicked");
    }

    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2) = tokio::join!(echo_handle, sup_handle);
        r1.expect("echo handle panicked");
        r2.expect("supervisor handle panicked");
    })
    .await
    .expect("Timed out shutting down test_forward_udp_concurrent_multi_client_nat_demux");
}

#[tokio::test]
async fn test_same_port_tcp_and_udp_forward_coexistence_and_empty_datagram() {
    let cancel_token = CancellationToken::new();
    let (tcp_echo_handle, tcp_echo_port) = spawn_tcp_echo_server(cancel_token.clone());
    let (udp_echo_handle, udp_echo_port) = spawn_udp_echo_server(cancel_token.clone());

    let server_ws_port = get_dual_ephemeral_port();
    let shared_port = get_dual_ephemeral_port();
    let shared_listen = format!("127.0.0.1:{}", shared_port);

    let app_config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("ws://127.0.0.1:{}/tunnel", server_ws_port)],
            token: Some("dual_proto_token".to_string()),
            ..Default::default()
        }),
        tunnels: vec![TunnelConfig {
            tag: "same-port-tcp-udp".to_string(),
            uuid: None,
            endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/tunnel", server_ws_port)),
            token: Some("dual_proto_token".to_string()),
            failover: None,
            multiplex: None,
            retry: None,
            http_headers: None,
            mappings: TunnelMappingsConfig::forward_pool(
                4,
                vec![
                    MappingRuleConfig::tcp(
                        shared_listen.clone(),
                        format!("127.0.0.1:{}", tcp_echo_port),
                    ),
                    MappingRuleConfig::udp(
                        shared_listen.clone(),
                        format!("127.0.0.1:{}", udp_echo_port),
                    ),
                ],
            ),
        }],
        ..Default::default()
    };

    let supervisor = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    tokio::time::sleep(Duration::from_millis(350)).await;

    // 1. Verify TCP forwarding on shared_port
    let mut tcp_client = TcpStream::connect(&shared_listen)
        .await
        .expect("Failed to connect TCP on shared port");
    let tcp_msg = b"tcp payload on dual-bound port";
    tcp_client
        .write_all(tcp_msg)
        .await
        .expect("Failed to write TCP payload");
    let mut tcp_buf = vec![0u8; tcp_msg.len()];
    tokio::time::timeout(Duration::from_secs(5), tcp_client.read_exact(&mut tcp_buf))
        .await
        .expect("Timeout reading TCP echo")
        .expect("Failed reading TCP echo");
    assert_eq!(&tcp_buf, tcp_msg);

    // 2. Verify UDP forwarding (including 0-byte empty datagram) on the exact same shared_port
    let udp_client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind UDP client");
    assert_eq!(udp_roundtrip(&udp_client, &shared_listen, b"").await, b"");
    assert_eq!(
        udp_roundtrip(
            &udp_client,
            &shared_listen,
            b"udp payload on dual-bound port"
        )
        .await,
        b"udp payload on dual-bound port"
    );

    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(tcp_echo_handle, udp_echo_handle, sup_handle);
        r1.expect("tcp_echo handle panicked");
        r2.expect("udp_echo handle panicked");
        r3.expect("supervisor handle panicked");
    })
    .await
    .expect(
        "Timed out shutting down test_same_port_tcp_and_udp_forward_coexistence_and_empty_datagram",
    );
}

#[tokio::test]
async fn test_reverse_udp_ws_pooled_and_signaling_fallback() {
    let cancel_token = CancellationToken::new();
    let (echo_handle, local_echo_port) = spawn_udp_echo_server(cancel_token.clone());

    let server_ws_port = get_dual_ephemeral_port();
    let reverse_udp_port = get_udp_ephemeral_port();
    let reverse_listen = format!("127.0.0.1:{}", reverse_udp_port);
    let auth_token = "rev_udp_ws_token";

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://127.0.0.1:{}/tunnel", server_ws_port)],
        token: Some(auth_token.to_string()),
        ..Default::default()
    };
    let server_runner = ServerRunner::new(
        server_config,
        Arc::clone(&registry),
        Arc::clone(&session_mgr),
    );
    let s_cancel = cancel_token.clone();
    let server_handle = tokio::spawn(async move {
        let _ = server_runner.run(s_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let tunnel_config = TunnelConfig {
        tag: "rev-udp-pool-and-fallback".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/tunnel", server_ws_port)),
        token: Some(auth_token.to_string()),
        failover: None,
        multiplex: None,
        retry: None,
        http_headers: None,
        mappings: TunnelMappingsConfig::reverse_pool(
            2,
            vec![MappingRuleConfig::udp(
                reverse_listen.clone(),
                format!("127.0.0.1:{}", local_echo_port),
            )],
        ),
    };
    let app_cfg = AppConfig::default();
    let ep_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_cfg).expect("ep_ctx");
    let c_cancel = cancel_token.clone();
    let c_reg = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ = run_reverse_tunnel(tunnel_config, ep_ctx, c_reg, c_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(350)).await;

    // 5 concurrent UDP visitors exceed pool_size=2, exercising both 0-RTT pool (0xA7)
    // and on-demand NewConnection signaling fallback.
    let mut visitors = Vec::new();
    for id in 0..5 {
        let addr = reverse_listen.clone();
        visitors.push(tokio::spawn(async move {
            let cli = UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("Failed to bind reverse UDP visitor");
            let msg1 = format!("rev-udp-visitor-{}-msg-1", id);
            assert_eq!(
                udp_roundtrip(&cli, &addr, msg1.as_bytes()).await,
                msg1.as_bytes()
            );
            let msg2 = format!("rev-udp-visitor-{}-msg-2", id);
            assert_eq!(
                udp_roundtrip(&cli, &addr, msg2.as_bytes()).await,
                msg2.as_bytes()
            );
        }));
    }

    for v in visitors {
        v.await.expect("Reverse UDP visitor panicked");
    }

    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(echo_handle, server_handle, client_handle);
        r1.expect("echo handle panicked");
        r2.expect("server handle panicked");
        r3.expect("client handle panicked");
    })
    .await
    .expect("Timed out shutting down test_reverse_udp_ws_pooled_and_signaling_fallback");
}

#[tokio::test]
async fn test_reverse_udp_h3() {
    let cancel_token = CancellationToken::new();
    let (echo_handle, local_echo_port) = spawn_udp_echo_server(cancel_token.clone());

    let h3_server_port = get_dual_ephemeral_port();
    let reverse_udp_port = get_udp_ephemeral_port();
    let reverse_listen = format!("127.0.0.1:{}", reverse_udp_port);

    let app_config = AppConfig {
        tls: Some(TlsConfig {
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        server: Some(ServerConfig {
            listen: vec![format!("h3://127.0.0.1:{}/tunnel", h3_server_port)],
            token: Some("rev_h3_udp_token".to_string()),
            ..Default::default()
        }),
        tunnels: vec![TunnelConfig {
            tag: "rev-udp-h3".to_string(),
            uuid: None,
            endpoint: EndpointConfig::Simple(format!("h3://127.0.0.1:{}/tunnel", h3_server_port)),
            token: Some("rev_h3_udp_token".to_string()),
            failover: None,
            multiplex: None,
            retry: None,
            http_headers: None,
            mappings: TunnelMappingsConfig::reverse_pool(
                2,
                vec![MappingRuleConfig::udp(
                    reverse_listen.clone(),
                    format!("127.0.0.1:{}", local_echo_port),
                )],
            ),
        }],
        ..Default::default()
    };

    let sup = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = sup.run().await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let visitor = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind H3 reverse visitor socket");
    assert_eq!(
        udp_roundtrip(&visitor, &reverse_listen, b"reverse-h3-udp-1").await,
        b"reverse-h3-udp-1"
    );
    assert_eq!(udp_roundtrip(&visitor, &reverse_listen, b"").await, b"");
    assert_eq!(
        udp_roundtrip(&visitor, &reverse_listen, b"reverse-h3-udp-2").await,
        b"reverse-h3-udp-2"
    );

    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2) = tokio::join!(echo_handle, sup_handle);
        r1.expect("echo handle panicked");
        r2.expect("supervisor handle panicked");
    })
    .await
    .expect("Timed out shutting down test_reverse_udp_h3");
}

struct CountingConnector {
    inner: Box<dyn TransportConnector>,
    connect_count: Arc<AtomicUsize>,
}

impl TransportConnector for CountingConnector {
    fn connect<'a>(
        &'a self,
        target: &'a TargetAddr,
        session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.connect_count.fetch_add(1, Ordering::SeqCst);
        self.inner.connect(target, session_id)
    }

    fn connect_pooled<'a>(
        &'a self,
        target: &'a TargetAddr,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.inner.connect_pooled(target)
    }

    fn connect_with_protocol<'a>(
        &'a self,
        target: &'a TargetAddr,
        session_id: Option<&'a str>,
        protocol: L4Protocol,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.connect_count.fetch_add(1, Ordering::SeqCst);
        self.inner
            .connect_with_protocol(target, session_id, protocol)
    }

    fn connect_pooled_with_protocol<'a>(
        &'a self,
        target: &'a TargetAddr,
        protocol: L4Protocol,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.inner.connect_pooled_with_protocol(target, protocol)
    }

    fn invalidate_broken_sessions(&self) {
        self.inner.invalidate_broken_sessions();
    }
}

struct CountingWsDriver {
    connect_count: Arc<AtomicUsize>,
}

impl TransportDriver for CountingWsDriver {
    fn schemes(&self) -> &[&'static str] {
        WsTransportDriver.schemes()
    }

    fn build_connector(
        &self,
        endpoint: &EndpointContext,
        auth_token: &str,
        mode: TunnelMode,
    ) -> Result<Box<dyn TransportConnector>, TunnelError> {
        let inner = WsTransportDriver.build_connector(endpoint, auth_token, mode)?;
        Ok(Box::new(CountingConnector {
            inner,
            connect_count: Arc::clone(&self.connect_count),
        }))
    }

    fn build_listener(
        &self,
        listen_addr: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        tls: Option<&TlsConfig>,
        cancel: CancellationToken,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        WsTransportDriver.build_listener(listen_addr, auth_token, path, tls, cancel)
    }
}

#[tokio::test]
async fn test_udp_nat_idle_timeout_expiration_and_recreation() {
    let cancel_token = CancellationToken::new();
    let (echo_handle, echo_port) = spawn_udp_echo_server(cancel_token.clone());

    let server_ws_port = get_dual_ephemeral_port();
    let fwd_udp_port = get_udp_ephemeral_port();
    let fwd_listen = format!("127.0.0.1:{}", fwd_udp_port);

    let connect_count = Arc::new(AtomicUsize::new(0));
    let mut registry = TransportRegistry::new();
    registry.register(CountingWsDriver {
        connect_count: Arc::clone(&connect_count),
    });
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://127.0.0.1:{}/tunnel", server_ws_port)],
        token: Some("udp_idle_tok".to_string()),
        ..Default::default()
    };
    let server_runner = ServerRunner::new(
        server_config,
        Arc::clone(&registry),
        Arc::clone(&session_mgr),
    );
    let s_cancel = cancel_token.clone();
    let server_handle = tokio::spawn(async move {
        let _ = server_runner.run(s_cancel).await;
    });

    // Long heartbeat_interval (15s) and idle_timeout (30s) paired with short udp_idle_timeout (200ms).
    let app_cfg = AppConfig {
        dialer: Some(DialerConfig {
            heartbeat_interval: Some(Duration::from_secs(15)),
            idle_timeout: Some(Duration::from_secs(30)),
            ..Default::default()
        }),
        ..Default::default()
    };
    let tunnel_cfg = TunnelConfig {
        tag: "udp-idle-recreate".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/tunnel", server_ws_port)),
        token: Some("udp_idle_tok".to_string()),
        failover: None,
        multiplex: None,
        retry: None,
        http_headers: None,
        mappings: TunnelMappingsConfig::forward(vec![MappingRuleConfig::udp_with_timeout(
            fwd_listen.clone(),
            format!("127.0.0.1:{}", echo_port),
            Duration::from_millis(200),
        )]),
    };
    let ep_ctx = EndpointContext::build(&tunnel_cfg.endpoint, &app_cfg).expect("ep_ctx");
    let c_cancel = cancel_token.clone();
    let c_reg = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ = run_forward_tunnel(tunnel_cfg, ep_ctx, c_reg, c_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Keep the exact same local UDP client socket across both flows.
    let client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind client socket");

    // Flow 1: multiple datagrams share 1 tunnel stream.
    assert_eq!(
        udp_roundtrip(&client, &fwd_listen, b"flow1-a").await,
        b"flow1-a"
    );
    assert_eq!(
        udp_roundtrip(&client, &fwd_listen, b"flow1-b").await,
        b"flow1-b"
    );
    assert_eq!(connect_count.load(Ordering::SeqCst), 1);

    // Wait longer than udp_idle_timeout (200ms) so the NAT flow expires cleanly.
    tokio::time::sleep(Duration::from_millis(450)).await;

    // Flow 2 from the same client source port transparently opens a fresh tunnel stream.
    assert_eq!(
        udp_roundtrip(&client, &fwd_listen, b"flow2-a").await,
        b"flow2-a"
    );
    assert_eq!(
        connect_count.load(Ordering::SeqCst),
        2,
        "Expired UDP NAT session must be recreated after udp_idle_timeout"
    );

    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(echo_handle, server_handle, client_handle);
        r1.expect("echo handle panicked");
        r2.expect("server handle panicked");
        r3.expect("client handle panicked");
    })
    .await
    .expect("Timed out shutting down test_udp_nat_idle_timeout_expiration_and_recreation");
}
