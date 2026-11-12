// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Tests for multi-mapping tunnels, shared connection pools, path matching, optional tokens, and retry supervision.
//! All comments and docstrings must be in English.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use culvert::config::model::{
    AppConfig, EndpointConfig, PortMappingConfig, ServerConfig, TunnelConfig, TunnelMode,
    TunnelPoolConfig, TunnelRetryConfig,
};
use culvert::supervisor::TunnelSupervisor;

/// Helper to get an available random ephemeral port assigned by the OS.
fn get_available_port() -> u16 {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn spawn_echo_server(cancel_token: CancellationToken) -> (tokio::task::JoinHandle<()>, u16) {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = std_listener.local_addr().unwrap().port();
    std_listener.set_nonblocking(true).unwrap();
    let listener = TcpListener::from_std(std_listener).unwrap();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                res = listener.accept() => {
                    if let Ok((mut socket, _)) = res {
                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            while let Ok(n) = socket.read(&mut buf).await {
                                if n == 0 { break; }
                                if socket.write_all(&buf[..n]).await.is_err() { break; }
                            }
                        });
                    }
                }
            }
        }
    });
    (handle, port)
}

#[tokio::test]
async fn test_multi_mapping_shared_pool_and_path_matching_e2e() {
    let cancel_token = CancellationToken::new();

    let (echo1_handle, echo_target1_port) = spawn_echo_server(cancel_token.clone());
    let (echo2_handle, echo_target2_port) = spawn_echo_server(cancel_token.clone());
    let server_ws_port = get_available_port();
    let map1_listen_port = get_available_port();
    let map2_listen_port = get_available_port();

    // Single unified AppConfig with Server + Tunnel containing multiple mappings
    let mut app_config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("ws://127.0.0.1:{}/secret_path", server_ws_port)],
            token: None, // No-auth mode
            default_target: None,
            ..Default::default()
        }),
        ..Default::default()
    };

    app_config.tunnels.push(TunnelConfig {
        tag: "shared-pool-cluster".to_string(),
        uuid: None,
        pool: None,
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/secret_path", server_ws_port)),
        token: None,
        pool_size: Some(4), // 4 idle pre-warmed streams shared across all mappings
        failover: None,
        multiplex: None,
        retry: Some(TunnelRetryConfig {
            delay: Some(Duration::from_millis(500)),
            max_retries: Some(5),
        }),
        http_headers: None,
        mappings: vec![
            PortMappingConfig {
                mode: TunnelMode::Forward,
                listen: format!("127.0.0.1:{}", map1_listen_port),
                target: format!("127.0.0.1:{}", echo_target1_port),
                ..Default::default()
            },
            PortMappingConfig {
                mode: TunnelMode::Forward,
                listen: format!("127.0.0.1:{}", map2_listen_port),
                target: format!("127.0.0.1:{}", echo_target2_port),
                ..Default::default()
            },
        ],
    });

    let supervisor = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    // Wait for server and pool replenishment
    tokio::time::sleep(Duration::from_millis(350)).await;

    // Test Mapping 1 (Target 1)
    let mut client1 = TcpStream::connect(format!("127.0.0.1:{}", map1_listen_port))
        .await
        .expect("Should connect to map1");
    let payload1 = b"Payload for target 1";
    client1.write_all(payload1).await.unwrap();
    let mut buf1 = vec![0u8; payload1.len()];
    tokio::time::timeout(Duration::from_secs(10), client1.read_exact(&mut buf1))
        .await
        .expect("Timeout reading client1 response")
        .unwrap();
    assert_eq!(&buf1, payload1);

    // Test Mapping 2 (Target 2)
    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", map2_listen_port))
        .await
        .expect("Should connect to map2");
    let payload2 = b"Payload for target 2 via shared pool";
    client2.write_all(payload2).await.unwrap();
    let mut buf2 = vec![0u8; payload2.len()];
    tokio::time::timeout(Duration::from_secs(10), client2.read_exact(&mut buf2))
        .await
        .expect("Timeout reading client2 response")
        .unwrap();
    assert_eq!(&buf2, payload2);

    cancel_token.cancel();
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            let (r1, r2, r3) = tokio::join!(echo1_handle, echo2_handle, sup_handle);
            r1.expect("echo1 handle panicked");
            r2.expect("echo2 handle panicked");
            r3.expect("sup handle panicked");
        },
    )
    .await
    .expect("Timeout waiting for handles to exit after cancellation in test_multi_mapping_tunnel_shared_pool");
}

#[tokio::test]
async fn test_path_mismatch_rejection() {
    let cancel_token = CancellationToken::new();
    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());
    let server_ws_port = get_available_port();
    let client_listen_port = get_available_port();

    let mut app_config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("ws://127.0.0.1:{}/valid_path", server_ws_port)],
            token: None,
            default_target: None,
            ..Default::default()
        }),
        ..Default::default()
    };

    // Client connects to the WRONG path: /wrong_path
    app_config.tunnels.push(TunnelConfig {
        tag: "wrong-path-tunnel".to_string(),
        uuid: None,
        pool: None,
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/wrong_path", server_ws_port)),
        token: None,
        pool_size: None,
        failover: None,
        multiplex: None,
        retry: None,
        http_headers: None,
        mappings: vec![PortMappingConfig {
            mode: TunnelMode::Forward,
            listen: format!("127.0.0.1:{}", client_listen_port),
            target: format!("127.0.0.1:{}", echo_port),
            ..Default::default()
        }],
    });

    let supervisor = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    tokio::time::sleep(Duration::from_millis(250)).await;

    // Connect to client listener - should connect to local port but forwarding should fail
    // because server returns 404 Not Found on path mismatch
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", client_listen_port))
        .await
        .unwrap();
    let payload = b"Hello";
    let _ = stream.write_all(payload).await;

    let mut buf = [0u8; 10];
    let n = stream.read(&mut buf).await.unwrap_or(0);
    assert_eq!(n, 0, "Server must close stream on path mismatch (404)");

    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2) = tokio::join!(echo_handle, sup_handle);
        r1.expect("echo handle panicked");
        r2.expect("sup handle panicked");
    })
    .await
    .expect(
        "Timeout waiting for handles to exit after cancellation in test_path_mismatch_rejection",
    );
}

#[tokio::test]
async fn test_cluster_mixed_forward_and_reverse_with_pool_e2e() {
    let cancel_token = CancellationToken::new();

    let (echo_fwd_handle, echo_fwd_port) = spawn_echo_server(cancel_token.clone());
    let (echo_rev_handle, echo_rev_port) = spawn_echo_server(cancel_token.clone());
    let server_ws_port = get_available_port();
    let fwd_listen_port = get_available_port();
    let rev_listen_port = get_available_port();

    let mut app_config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("ws://127.0.0.1:{}/cluster_ws", server_ws_port)],
            token: Some("cluster_mixed_pool_secret".to_string()),
            default_target: None,
            ..Default::default()
        }),
        ..Default::default()
    };

    app_config.tunnels.push(TunnelConfig {
        tag: "mixed-cluster".to_string(),
        uuid: None,
        pool: None,
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/cluster_ws", server_ws_port)),
        token: Some("cluster_mixed_pool_secret".to_string()),
        pool_size: Some(4), // Outbound pool for forward, Inbound pool for reverse
        failover: None,
        multiplex: None,
        retry: None,
        http_headers: None,
        mappings: vec![
            PortMappingConfig {
                mode: TunnelMode::Forward,
                listen: format!("127.0.0.1:{}", fwd_listen_port),
                target: format!("127.0.0.1:{}", echo_fwd_port),
                ..Default::default()
            },
            PortMappingConfig {
                mode: TunnelMode::Reverse,
                listen: format!("127.0.0.1:{}", rev_listen_port),
                target: format!("127.0.0.1:{}", echo_rev_port),
                ..Default::default()
            },
        ],
    });

    let supervisor = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // 1. Test Forward mapping (client listen -> remote echo)
    for i in 1..=3 {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", fwd_listen_port))
            .await
            .expect("Failed to connect forward listen port");
        let msg = format!("FWD TEST {}", i);
        stream.write_all(msg.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; msg.len()];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, msg.as_bytes());
    }

    // 2. Test Reverse mapping (server listen -> local echo)
    for i in 1..=3 {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", rev_listen_port))
            .await
            .expect("Failed to connect reverse listen port on server");
        let msg = format!("REV TEST {}", i);
        stream.write_all(msg.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; msg.len()];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, msg.as_bytes());
    }

    // Clean shutdown
    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(echo_fwd_handle, echo_rev_handle, sup_handle);
        r1.expect("echo_fwd handle panicked");
        r2.expect("echo_rev handle panicked");
        r3.expect("sup handle panicked");
    })
    .await
    .expect("Timeout waiting for handles to exit in test_cluster_mixed_forward_and_reverse_with_pool_e2e");
}

#[tokio::test]
async fn test_multi_reverse_mapping_shared_pool_with_uuid_e2e() {
    let cancel_token = CancellationToken::new();

    let (echo1_handle, echo1_port) = spawn_echo_server(cancel_token.clone());
    let (echo2_handle, echo2_port) = spawn_echo_server(cancel_token.clone());
    let server_ws_port = get_available_port();
    let rev1_listen_port = get_available_port();
    let rev2_listen_port = get_available_port();

    let mut app_config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("ws://127.0.0.1:{}/rev_cluster", server_ws_port)],
            token: Some("shared_rev_secret".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };

    app_config.tunnels.push(TunnelConfig {
        tag: "shared-rev-cluster".to_string(),
        uuid: Some("550e8400-e29b-41d4-a716-446655440000".to_string()),
        pool: Some(TunnelPoolConfig {
            forward: None,
            reverse: Some(3),
        }),
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/rev_cluster", server_ws_port)),
        token: Some("shared_rev_secret".to_string()),
        pool_size: None,
        failover: None,
        multiplex: None,
        retry: None,
        http_headers: None,
        mappings: vec![
            PortMappingConfig {
                mode: TunnelMode::Reverse,
                listen: format!("127.0.0.1:{}", rev1_listen_port),
                target: format!("127.0.0.1:{}", echo1_port),
                ..Default::default()
            },
            PortMappingConfig {
                mode: TunnelMode::Reverse,
                listen: format!("127.0.0.1:{}", rev2_listen_port),
                target: format!("127.0.0.1:{}", echo2_port),
                ..Default::default()
            },
        ],
    });

    let supervisor = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    // Wait for server, listener binding, and shared pool replenishment
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Test Reverse mapping 1 (server listen 1 -> local echo 1)
    for i in 1..=3 {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", rev1_listen_port))
            .await
            .expect("Failed to connect reverse mapping 1");
        let msg = format!("MAPPING 1 TEST {}", i);
        stream.write_all(msg.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; msg.len()];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, msg.as_bytes());
    }

    // Test Reverse mapping 2 (server listen 2 -> local echo 2)
    for i in 1..=3 {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", rev2_listen_port))
            .await
            .expect("Failed to connect reverse mapping 2");
        let msg = format!("MAPPING 2 TEST {}", i);
        stream.write_all(msg.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; msg.len()];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, msg.as_bytes());
    }

    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(echo1_handle, echo2_handle, sup_handle);
        r1.expect("echo1 handle panicked");
        r2.expect("echo2 handle panicked");
        r3.expect("sup handle panicked");
    })
    .await
    .expect("Timeout waiting for handles to exit in test_multi_reverse_mapping_shared_pool_with_uuid_e2e");
}

#[tokio::test]
async fn test_asymmetric_pool_sizing_e2e() {
    let cancel_token = CancellationToken::new();

    let (echo_fwd_handle, echo_fwd_port) = spawn_echo_server(cancel_token.clone());
    let (echo_rev_handle, echo_rev_port) = spawn_echo_server(cancel_token.clone());
    let server_ws_port = get_available_port();
    let fwd_listen_port = get_available_port();
    let rev_listen_port = get_available_port();

    let mut app_config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("ws://127.0.0.1:{}/asym_cluster", server_ws_port)],
            token: Some("asym_pool_secret".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };

    app_config.tunnels.push(TunnelConfig {
        tag: "asym-pool-tunnel".to_string(),
        uuid: None,
        pool: Some(TunnelPoolConfig {
            forward: Some(2),
            reverse: Some(4),
        }),
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/asym_cluster", server_ws_port)),
        token: Some("asym_pool_secret".to_string()),
        pool_size: None,
        failover: None,
        multiplex: None,
        retry: None,
        http_headers: None,
        mappings: vec![
            PortMappingConfig {
                mode: TunnelMode::Forward,
                listen: format!("127.0.0.1:{}", fwd_listen_port),
                target: format!("127.0.0.1:{}", echo_fwd_port),
                ..Default::default()
            },
            PortMappingConfig {
                mode: TunnelMode::Reverse,
                listen: format!("127.0.0.1:{}", rev_listen_port),
                target: format!("127.0.0.1:{}", echo_rev_port),
                ..Default::default()
            },
        ],
    });

    let supervisor = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Test Forward mapping with pool
    let mut fwd_client = TcpStream::connect(format!("127.0.0.1:{}", fwd_listen_port))
        .await
        .expect("Failed to connect forward port");
    fwd_client.write_all(b"ASYM FORWARD PING").await.unwrap();
    let mut fwd_buf = [0u8; 17];
    fwd_client.read_exact(&mut fwd_buf).await.unwrap();
    assert_eq!(&fwd_buf, b"ASYM FORWARD PING");

    // Test Reverse mapping with pool
    let mut rev_client = TcpStream::connect(format!("127.0.0.1:{}", rev_listen_port))
        .await
        .expect("Failed to connect reverse port");
    rev_client.write_all(b"ASYM REVERSE PING").await.unwrap();
    let mut rev_buf = [0u8; 17];
    rev_client.read_exact(&mut rev_buf).await.unwrap();
    assert_eq!(&rev_buf, b"ASYM REVERSE PING");

    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(echo_fwd_handle, echo_rev_handle, sup_handle);
        r1.expect("echo_fwd handle panicked");
        r2.expect("echo_rev handle panicked");
        r3.expect("sup handle panicked");
    })
    .await
    .expect("Timeout waiting for handles to exit in test_asymmetric_pool_sizing_e2e");
}
