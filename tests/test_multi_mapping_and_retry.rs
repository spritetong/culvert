// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Tests for multi-mapping tunnels, shared connection pools, path matching, optional tokens, and retry supervision.
//! All comments and docstrings must be in English.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use micro_tunnel::config::model::{
    AppConfig, EndpointConfig, PortMappingConfig, ServerConfig, TunnelConfig, TunnelMode,
    TunnelRetryConfig,
};
use micro_tunnel::supervisor::TunnelSupervisor;

/// Helper to get an available random ephemeral port assigned by the OS.
fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind ephemeral port");
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
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/secret_path", server_ws_port)),
        token: None,
        pool_size: Some(4), // 4 idle pre-warmed streams shared across all mappings
        failover: None,
        multiplex: None,
        retry: Some(TunnelRetryConfig {
            delay: Some(Duration::from_millis(500)),
            max_retries: Some(5),
        }),
        mappings: vec![
            PortMappingConfig {
                mode: TunnelMode::Forward,
                listen: format!("127.0.0.1:{}", map1_listen_port),
                target: format!("127.0.0.1:{}", echo_target1_port),
            },
            PortMappingConfig {
                mode: TunnelMode::Forward,
                listen: format!("127.0.0.1:{}", map2_listen_port),
                target: format!("127.0.0.1:{}", echo_target2_port),
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
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/wrong_path", server_ws_port)),
        token: None,
        pool_size: None,
        failover: None,
        multiplex: None,
        retry: None,
        mappings: vec![PortMappingConfig {
            mode: TunnelMode::Forward,
            listen: format!("127.0.0.1:{}", client_listen_port),
            target: format!("127.0.0.1:{}", echo_port),
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
