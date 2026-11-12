// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! End-to-end integration tests for WebSocket tunnels.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use culvert::config::model::{
    AppConfig, EndpointConfig, MappingRuleConfig, ServerConfig, TunnelConfig, TunnelMappingsConfig,
};
use culvert::dialer::endpoint::EndpointContext;
use culvert::server::ServerRunner;
use culvert::session::SessionManager;
use culvert::supervisor::TunnelSupervisor;
use culvert::transport::TransportRegistry;
use culvert::transport::ws::WsTransportDriver;
use culvert::tunnel::{run_forward_tunnel, run_reverse_tunnel};

/// Helper to get an available random ephemeral port assigned by the OS.
fn get_available_port() -> u16 {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Spawns a simple TCP echo server using held-listener handoff.
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
                    let (mut socket, _) = match res {
                        Ok(conn) => conn,
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

#[tokio::test]
async fn test_forward_tunnel_ws_e2e() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("trace")
        .try_init();
    let cancel_token = CancellationToken::new();

    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());
    let server_port = get_available_port();
    let client_port = get_available_port();

    let server_listen = format!("127.0.0.1:{}", server_port);
    let client_listen = format!("127.0.0.1:{}", client_port);
    let auth_token = "ws_test_token_secret_123";

    // 1. Start ServerRunner (echo server already started via handoff)

    // 2. Setup transport registry and start ServerRunner
    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://{}/tunnel", server_listen)],
        token: Some(auth_token.to_string()),
        default_target: None,
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

    // 3. Setup client forward tunnel
    let tunnel_config = TunnelConfig {
        tag: "test-forward".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Simple(format!("ws://{}/tunnel", server_listen)),
        token: Some(auth_token.to_string()),
        failover: None,
        multiplex: None,
        mappings: TunnelMappingsConfig::forward(vec![MappingRuleConfig::tcp(
            client_listen.clone(),
            format!("127.0.0.1:{}", echo_port),
        )]),
        retry: None,
        http_headers: None,
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ =
            run_forward_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });

    // Give servers a brief moment to bind and listen
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 4. Connect visitor to client listen port and verify bidirectional echo
    let mut stream = TcpStream::connect(&client_listen)
        .await
        .expect("Failed to connect to client forward listen port");

    let payload = b"Hello culvert forward WebSocket!";
    stream
        .write_all(payload)
        .await
        .expect("Failed to write to client");

    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut buf))
        .await
        .expect("Timeout reading response in forward tunnel")
        .expect("Failed to read response");
    assert_eq!(&buf, payload, "Echoed payload mismatch in forward tunnel");

    // 5. Graceful shutdown
    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(echo_handle, server_handle, client_handle);
        r1.expect("echo handle panicked");
        r2.expect("server handle panicked");
        r3.expect("client handle panicked");
    })
    .await
    .expect("Timeout waiting for handles to exit after cancellation in test_forward_tunnel_ws_e2e");
}

#[tokio::test]
async fn test_reverse_tunnel_ws_e2e() {
    let cancel_token = CancellationToken::new();

    let (echo_handle, local_echo_port) = spawn_echo_server(cancel_token.clone());
    let server_ws_port = get_available_port();
    let reverse_listen_port = get_available_port();

    let server_ws_listen = format!("127.0.0.1:{}", server_ws_port);
    let reverse_listen = format!("127.0.0.1:{}", reverse_listen_port);
    let auth_token = "ws_reverse_test_token_456";

    // 1. Setup transport registry and start ServerRunner (echo server started via handoff)
    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://{}/tunnel", server_ws_listen)],
        token: Some(auth_token.to_string()),
        default_target: None,
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

    // Give server time to listen
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 3. Setup client reverse tunnel
    let tunnel_config = TunnelConfig {
        tag: "test-reverse".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Simple(format!("ws://{}/tunnel", server_ws_listen)),
        token: Some(auth_token.to_string()),
        failover: None,
        multiplex: None,
        mappings: TunnelMappingsConfig::reverse(vec![MappingRuleConfig::tcp(
            reverse_listen.clone(),
            format!("127.0.0.1:{}", local_echo_port),
        )]),
        retry: None,
        http_headers: None,
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ =
            run_reverse_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });

    // Give reverse registration time to connect and bind reverse_listen port on server
    tokio::time::sleep(Duration::from_millis(250)).await;

    // 4. Connect external visitor to reverse_listen port on server
    let mut visitor_stream = TcpStream::connect(&reverse_listen)
        .await
        .expect("Failed to connect to reverse listener port on server");

    let payload = b"Hello culvert reverse WebSocket!";
    visitor_stream
        .write_all(payload)
        .await
        .expect("Failed to write to reverse port");

    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(Duration::from_secs(10), visitor_stream.read_exact(&mut buf))
        .await
        .expect("Timeout reading response from reverse port")
        .expect("Failed to read response from reverse port");
    assert_eq!(&buf, payload, "Echoed payload mismatch in reverse tunnel");

    // 5. Graceful shutdown
    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(echo_handle, server_handle, client_handle);
        r1.expect("echo handle panicked");
        r2.expect("server handle panicked");
        r3.expect("client handle panicked");
    })
    .await
    .expect("Timeout waiting for handles to exit after cancellation in test_reverse_tunnel_ws_e2e");
}

#[tokio::test]
async fn test_reverse_tunnel_with_inbound_pool_e2e() {
    let cancel_token = CancellationToken::new();

    let (echo_handle, local_echo_port) = spawn_echo_server(cancel_token.clone());
    let server_ws_port = get_available_port();
    let reverse_listen_port = get_available_port();

    let server_ws_listen = format!("127.0.0.1:{}", server_ws_port);
    let reverse_listen = format!("127.0.0.1:{}", reverse_listen_port);
    let auth_token = "ws_reverse_pool_test_token_789";

    // 1. Setup transport registry and start ServerRunner
    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://{}/tunnel", server_ws_listen)],
        token: Some(auth_token.to_string()),
        default_target: None,
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

    tokio::time::sleep(Duration::from_millis(100)).await;

    // 2. Setup client reverse tunnel with pool_size: 4
    let tunnel_config = TunnelConfig {
        tag: "test-reverse-pool".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Simple(format!("ws://{}/tunnel", server_ws_listen)),
        token: Some(auth_token.to_string()),
        failover: None,
        multiplex: None,
        mappings: TunnelMappingsConfig::reverse_pool(
            4,
            vec![MappingRuleConfig::tcp(
                reverse_listen.clone(),
                format!("127.0.0.1:{}", local_echo_port),
            )],
        ),
        retry: None,
        http_headers: None,
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ =
            run_reverse_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });

    // Give time for reverse listener to bind and pool connections to pre-warm
    tokio::time::sleep(Duration::from_millis(400)).await;

    // 3. Connect 5 consecutive external visitors (exceeding initial pool size 4 to verify replenishment)
    for i in 1..=5 {
        let mut visitor_stream = match TcpStream::connect(&reverse_listen).await {
            Ok(s) => s,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                TcpStream::connect(&reverse_listen)
                    .await
                    .expect("Failed to connect reverse listener port")
            }
        };

        let payload = format!("Hello reverse pool echo packet #{}", i);
        visitor_stream
            .write_all(payload.as_bytes())
            .await
            .expect("Failed to write to reverse port");

        let mut buf = vec![0u8; payload.len()];
        tokio::time::timeout(Duration::from_secs(5), visitor_stream.read_exact(&mut buf))
            .await
            .expect("Timeout reading reverse pool reply")
            .expect("Failed to read reverse pool reply");
        assert_eq!(
            &buf,
            payload.as_bytes(),
            "Echo mismatch in reverse pool test"
        );
    }

    // 4. Graceful shutdown
    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(echo_handle, server_handle, client_handle);
        r1.expect("echo handle panicked");
        r2.expect("server handle panicked");
        r3.expect("client handle panicked");
    })
    .await
    .expect("Timeout waiting for handles to exit in test_reverse_tunnel_with_inbound_pool_e2e");
}

/// A burst larger than the pre-warmed pool must be served by on-demand
/// `NewConnection` signaling instead of stalling or dropping visitors.
#[tokio::test]
async fn test_reverse_pool_burst_falls_back_to_signaling_e2e() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    let cancel_token = CancellationToken::new();
    let (echo_handle, local_echo_port) = spawn_echo_server(cancel_token.clone());
    let server_ws_port = get_available_port();
    let reverse_listen_port = get_available_port();
    let server_ws_listen = format!("127.0.0.1:{}", server_ws_port);
    let reverse_listen = format!("127.0.0.1:{}", reverse_listen_port);
    let auth_token = "ws_reverse_burst_token";

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://{}/tunnel", server_ws_listen)],
        token: Some(auth_token.to_string()),
        default_target: None,
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
    tokio::time::sleep(Duration::from_millis(100)).await;

    let tunnel_config = TunnelConfig {
        tag: "test-reverse-burst".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Simple(format!("ws://{}/tunnel", server_ws_listen)),
        token: Some(auth_token.to_string()),
        failover: None,
        multiplex: None,
        mappings: TunnelMappingsConfig::reverse_pool(
            2,
            vec![MappingRuleConfig::tcp(
                reverse_listen.clone(),
                format!("127.0.0.1:{}", local_echo_port),
            )],
        ),
        retry: None,
        http_headers: None,
    };
    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ =
            run_reverse_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut visitors = Vec::new();
    for i in 0..8 {
        let addr = reverse_listen.clone();
        visitors.push(tokio::spawn(async move {
            let payload = format!("burst-packet-{}", i);
            tokio::time::timeout(Duration::from_secs(8), async {
                let mut stream = loop {
                    match TcpStream::connect(&addr).await {
                        Ok(s) => break s,
                        Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                    }
                };
                stream.write_all(payload.as_bytes()).await.unwrap();
                let mut buf = vec![0u8; payload.len()];
                stream.read_exact(&mut buf).await.unwrap();
                assert_eq!(&buf, payload.as_bytes());
            })
            .await
            .expect("burst visitor timed out (empty pool did not fall back to signaling)");
        }));
    }
    for visitor in visitors {
        visitor.await.expect("burst visitor task panicked");
    }

    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(echo_handle, server_handle, client_handle);
        r1.expect("echo handle panicked");
        r2.expect("server handle panicked");
        r3.expect("client handle panicked");
    })
    .await
    .expect("Timeout waiting for handles to exit in test_reverse_pool_burst_falls_back_to_signaling_e2e");
}

#[tokio::test]
async fn test_supervisor_multi_tunnel_concurrency_e2e() {
    let cancel_token = CancellationToken::new();

    let (echo_fwd_handle, echo_fwd_port) = spawn_echo_server(cancel_token.clone());
    let (echo_rev_handle, echo_rev_port) = spawn_echo_server(cancel_token.clone());
    let server_ws_port = get_available_port();
    let fwd_listen_port = get_available_port();
    let rev_listen_port = get_available_port();

    let auth_token = "supervisor_multi_test_token";

    // 1. Configure AppConfig with Server + Forward Tunnel + Reverse Tunnel concurrently
    let mut app_config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("ws://127.0.0.1:{}/ws", server_ws_port)],
            token: Some(auth_token.to_string()),
            default_target: None,
            ..Default::default()
        }),
        ..Default::default()
    };

    app_config.tunnels.push(TunnelConfig {
        tag: "fwd-tunnel".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/ws", server_ws_port)),
        token: Some(auth_token.to_string()),
        failover: None,
        multiplex: None,
        mappings: TunnelMappingsConfig::forward(vec![MappingRuleConfig::tcp(
            format!("127.0.0.1:{}", fwd_listen_port),
            format!("127.0.0.1:{}", echo_fwd_port),
        )]),
        retry: None,
        http_headers: None,
    });

    app_config.tunnels.push(TunnelConfig {
        tag: "rev-tunnel".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/ws", server_ws_port)),
        token: Some(auth_token.to_string()),
        failover: None,
        multiplex: None,
        mappings: TunnelMappingsConfig::reverse(vec![MappingRuleConfig::tcp(
            format!("127.0.0.1:{}", rev_listen_port),
            format!("127.0.0.1:{}", echo_rev_port),
        )]),
        retry: None,
        http_headers: None,
    });

    // 3. Launch supervisor
    let supervisor = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // 4. Test Forward Tunnel concurrently
    let fwd_fut = async move {
        let mut fwd_stream = TcpStream::connect(format!("127.0.0.1:{}", fwd_listen_port))
            .await
            .expect("Failed to connect forward listen port");
        let payload = b"Multi-Tunnel Concurrency Forward Check";
        fwd_stream.write_all(payload).await.unwrap();
        let mut buf = vec![0u8; payload.len()];
        tokio::time::timeout(Duration::from_secs(10), fwd_stream.read_exact(&mut buf))
            .await
            .expect("Timeout reading fwd_stream in concurrency test")
            .unwrap();
        assert_eq!(&buf, payload);
    };

    // 5. Test Reverse Tunnel concurrently
    let rev_fut = async move {
        let mut rev_stream = TcpStream::connect(format!("127.0.0.1:{}", rev_listen_port))
            .await
            .expect("Failed to connect reverse listen port");
        let payload = b"Multi-Tunnel Concurrency Reverse Check";
        rev_stream.write_all(payload).await.unwrap();
        let mut buf = vec![0u8; payload.len()];
        tokio::time::timeout(Duration::from_secs(10), rev_stream.read_exact(&mut buf))
            .await
            .expect("Timeout reading rev_stream in concurrency test")
            .unwrap();
        assert_eq!(&buf, payload);
    };

    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::try_join!(tokio::spawn(fwd_fut), tokio::spawn(rev_fut)).unwrap();
    })
    .await
    .expect("Timeout waiting for forward and reverse futures");

    // 6. Graceful shutdown
    cancel_token.cancel();
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            let (r1, r2, r3) = tokio::join!(echo_fwd_handle, echo_rev_handle, sup_handle);
            r1.expect("echo_fwd handle panicked");
            r2.expect("echo_rev handle panicked");
            r3.expect("sup handle panicked");
        },
    )
    .await
    .expect("Timeout waiting for handles to exit after cancellation in test_supervisor_multi_tunnel_concurrency_e2e");
}
