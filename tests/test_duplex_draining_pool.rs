//! Integration tests for duplex data transfer, graceful half-close draining,
//! and connection pool pre-warming / reuse.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use micro_tunnel::config::model::{
    AppConfig, EndpointConfig, ServerConfig, TunnelConfig, TunnelMode,
};
use micro_tunnel::dialer::endpoint::EndpointContext;
use micro_tunnel::server::ServerRunner;
use micro_tunnel::session::SessionManager;
use micro_tunnel::transport::TransportRegistry;
use micro_tunnel::transport::ws::WsTransportDriver;
use micro_tunnel::tunnel::run_forward_tunnel;

/// Helper to get an available random ephemeral port from Suite 2 partition [27000, 32500).
fn get_available_port() -> u16 {
    static PORT_COUNTER: AtomicU16 = AtomicU16::new(0);
    let base = 27000 + (std::process::id() as u16 % 100) * 50;
    for _ in 0..200 {
        let offset = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let port = base.wrapping_add(offset);
        if (1024..65535).contains(&port)
            && let Ok(listener) = std::net::TcpListener::bind(("127.0.0.1", port))
        {
            drop(listener);
            return port;
        }
    }
    panic!("Failed to find an available port in Suite 2 partition [27000, 32500)");
}

/// Spawns a full-duplex echo server using held-listener handoff to eliminate TOCTOU.
fn spawn_echo_server(cancel_token: CancellationToken) -> (tokio::task::JoinHandle<()>, u16) {
    let std_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind echo server");
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
async fn test_high_volume_duplex_stream_integrity() {
    let cancel_token = CancellationToken::new();

    let server_ws_port = get_available_port();
    let client_listen_port = get_available_port();

    let server_ws_listen = format!("127.0.0.1:{}", server_ws_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);
    let auth_token = "high_volume_test_token";

    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://{}/ws", server_ws_listen)],
        token: Some(auth_token.to_string()),
        default_target: None,
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

    let tunnel_config = TunnelConfig {
        tag: "test-volume".to_string(),
        mode: Some(TunnelMode::Forward),
        listen: Some(client_listen.clone()),
        target: Some(format!("127.0.0.1:{}", echo_port)),
        token: Some(auth_token.to_string()),
        pool_size: None,
        failover: None,
        multiplex: None,
        mappings: vec![],
        retry: None,
        endpoint: EndpointConfig::Simple(format!("ws://{}/ws", server_ws_listen)),
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ =
            run_forward_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Connect client
    let stream = TcpStream::connect(&client_listen)
        .await
        .expect("Failed to connect client forward listener");

    // Send 4MB of pseudo-random pattern data
    const DATA_SIZE: usize = 4 * 1024 * 1024; // 4 MB
    let mut send_data = vec![0u8; DATA_SIZE];
    for (i, byte) in send_data.iter_mut().enumerate() {
        *byte = ((i * 31 + 17) & 0xFF) as u8;
    }

    let send_clone = send_data.clone();
    let (mut rd, mut wr) = stream.into_split();

    let writer_handle = tokio::spawn(async move {
        wr.write_all(&send_clone).await.unwrap();
        wr.flush().await.unwrap();
    });

    let reader_handle = tokio::spawn(async move {
        let mut recv_data = vec![0u8; DATA_SIZE];
        tokio::time::timeout(Duration::from_secs(10), rd.read_exact(&mut recv_data))
            .await
            .expect("Timeout reading duplex data")
            .unwrap();
        recv_data
    });

    let (writer_res, reader_res) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(writer_handle, reader_handle)
    })
    .await
    .expect("Timeout waiting for writer/reader handles");
    writer_res.expect("writer_handle panicked");
    let recv_data = reader_res.expect("reader_handle panicked");

    assert_eq!(recv_data.len(), DATA_SIZE);
    assert_eq!(
        &recv_data[..],
        &send_data[..],
        "High-volume duplex data mismatch!"
    );

    cancel_token.cancel();
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            let (r1, r2, r3) = tokio::join!(echo_handle, server_handle, client_handle);
            r1.expect("echo_handle panicked");
            r2.expect("server_handle panicked");
            r3.expect("client_handle panicked");
        },
    )
    .await
    .expect("Timeout waiting for handles to exit after cancellation in test_full_duplex_concurrent_traffic");
}

#[tokio::test]
async fn test_stream_half_close_and_draining() {
    let cancel_token = CancellationToken::new();

    let backend_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind backend listener");
    let backend_service_port = backend_listener.local_addr().unwrap().port();
    backend_listener.set_nonblocking(true).unwrap();
    let tokio_backend_listener = TcpListener::from_std(backend_listener).unwrap();

    let server_ws_port = get_available_port();
    let client_listen_port = get_available_port();

    let server_ws_listen = format!("127.0.0.1:{}", server_ws_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);
    let auth_token = "draining_test_token";

    // Backend service that reads until EOF (half-close draining), then responds
    let service_cancel = cancel_token.clone();
    let backend_handle = tokio::spawn(async move {
        tokio::select! {
            _ = service_cancel.cancelled() => {}
            res = tokio_backend_listener.accept() => {
                let (mut socket, _) = res.unwrap();
                let mut buf = Vec::new();
                // Read all data until client sends FIN (half-close) with bounded timeout
                tokio::time::timeout(Duration::from_secs(10), socket.read_to_end(&mut buf))
                    .await
                    .expect("Timeout reading client FIN in draining backend")
                    .unwrap();

                // Respond with confirmation and received size
                let reply = format!("DRAINED_TOTAL_{}", buf.len());
                socket.write_all(reply.as_bytes()).await.unwrap();
                socket.flush().await.unwrap();
            }
        }
    });

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://{}/ws", server_ws_listen)],
        token: Some(auth_token.to_string()),
        default_target: None,
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

    let tunnel_config = TunnelConfig {
        tag: "test-draining".to_string(),
        mode: Some(TunnelMode::Forward),
        listen: Some(client_listen.clone()),
        target: Some(format!("127.0.0.1:{}", backend_service_port)),
        token: Some(auth_token.to_string()),
        pool_size: None,
        failover: None,
        multiplex: None,
        mappings: vec![],
        retry: None,
        endpoint: EndpointConfig::Simple(format!("ws://{}/ws", server_ws_listen)),
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ =
            run_forward_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Connect client
    let mut stream = TcpStream::connect(&client_listen)
        .await
        .expect("Failed to connect client listen port");

    // Send 512KB and call shutdown() to half-close write direction
    const UPLOAD_SIZE: usize = 512 * 1024;
    let upload_data = vec![0xEEu8; UPLOAD_SIZE];
    stream.write_all(&upload_data).await.unwrap();
    stream.shutdown().await.unwrap(); // Half-close: client will send no more data

    // Read response from backend service
    let mut reply = String::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_string(&mut reply))
        .await
        .expect("Timeout reading reply in test_stream_half_close_and_draining")
        .unwrap();
    assert_eq!(reply, format!("DRAINED_TOTAL_{}", UPLOAD_SIZE));

    cancel_token.cancel();
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            let (r1, r2, r3) = tokio::join!(backend_handle, server_handle, client_handle);
            r1.expect("backend_handle panicked");
            r2.expect("server_handle panicked");
            r3.expect("client_handle panicked");
        },
    )
    .await
    .expect("Timeout waiting for handles to exit after cancellation in test_stream_half_close_and_draining");
}

#[tokio::test]
async fn test_pool_prewarm_and_reuse() {
    let cancel_token = CancellationToken::new();

    let server_ws_port = get_available_port();
    let client_listen_port = get_available_port();

    let server_ws_listen = format!("127.0.0.1:{}", server_ws_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);
    let auth_token = "pool_reuse_test_token";

    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://{}/ws", server_ws_listen)],
        token: Some(auth_token.to_string()),
        default_target: None,
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

    // Configure tunnel with pool_size: 4
    let tunnel_config = TunnelConfig {
        tag: "test-pool".to_string(),
        mode: Some(TunnelMode::Forward),
        listen: Some(client_listen.clone()),
        target: Some(format!("127.0.0.1:{}", echo_port)),
        token: Some(auth_token.to_string()),
        pool_size: Some(4), // Pre-warm 4 connections
        failover: None,
        multiplex: None,
        mappings: vec![],
        retry: None,
        endpoint: EndpointConfig::Simple(format!("ws://{}/ws", server_ws_listen)),
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ =
            run_forward_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });

    // Give pool time to pre-warm the 4 idle connections
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Send 10 sequential connections with disconnects; all should succeed and recover cleanly
    for i in 0..10 {
        let mut stream = TcpStream::connect(&client_listen)
            .await
            .expect("Failed to connect client listen port");

        let msg = format!("Pooled connection request #{}", i);
        stream.write_all(msg.as_bytes()).await.unwrap();

        let mut buf = vec![0u8; msg.len()];
        tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut buf))
            .await
            .expect("Timeout reading pooled connection reply")
            .unwrap();
        assert_eq!(&buf, msg.as_bytes());

        // Explicitly close connection
        drop(stream);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(echo_handle, server_handle, client_handle);
        r1.expect("echo_handle panicked");
        r2.expect("server_handle panicked");
        r3.expect("client_handle panicked");
    })
    .await
    .expect(
        "Timeout waiting for handles to exit after cancellation in test_pool_prewarm_and_reuse",
    );
}

#[tokio::test]
async fn test_pool_resilience_after_disconnect_and_high_churn() {
    let cancel_token = CancellationToken::new();

    let server_ws_port = get_available_port();
    let client_listen_port = get_available_port();

    let server_ws_listen = format!("127.0.0.1:{}", server_ws_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);
    let auth_token = "pool_churn_test_token";

    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://{}/ws", server_ws_listen)],
        token: Some(auth_token.to_string()),
        default_target: None,
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

    // Configure tunnel with pool_size: 4
    let tunnel_config = TunnelConfig {
        tag: "test-pool-churn".to_string(),
        mode: Some(TunnelMode::Forward),
        listen: Some(client_listen.clone()),
        target: Some(format!("127.0.0.1:{}", echo_port)),
        token: Some(auth_token.to_string()),
        pool_size: Some(4),
        failover: None,
        multiplex: None,
        mappings: vec![],
        retry: None,
        endpoint: EndpointConfig::Simple(format!("ws://{}/ws", server_ws_listen)),
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ =
            run_forward_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Concurrently open 20 connections in parallel, exceeding pool capacity (4)
    let mut handles = Vec::new();
    for i in 0..20 {
        let listen_addr = client_listen.clone();
        handles.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(&listen_addr)
                .await
                .expect("Failed to connect client listen port");

            let msg = format!("Concurrent request #{}", i);
            stream.write_all(msg.as_bytes()).await.unwrap();

            let mut buf = vec![0u8; msg.len()];
            tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut buf))
                .await
                .expect("Timeout reading concurrent churn reply")
                .unwrap();
            assert_eq!(&buf, msg.as_bytes());
            drop(stream);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Wait a brief moment for pool replenishment after churn
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Subsequent connection after burst MUST succeed and be stable
    for i in 0..5 {
        let mut stream = TcpStream::connect(&client_listen)
            .await
            .expect("Failed to connect after high churn");

        let msg = format!("Post churn request #{}", i);
        stream.write_all(msg.as_bytes()).await.unwrap();

        let mut buf = vec![0u8; msg.len()];
        tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut buf))
            .await
            .expect("Timeout reading post-churn reply")
            .unwrap();
        assert_eq!(&buf, msg.as_bytes());
    }

    cancel_token.cancel();
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            let (r1, r2, r3) = tokio::join!(echo_handle, server_handle, client_handle);
            r1.expect("echo_handle panicked");
            r2.expect("server_handle panicked");
            r3.expect("client_handle panicked");
        },
    )
    .await
    .expect("Timeout waiting for handles to exit after cancellation in test_pool_resilience_after_disconnect_and_high_churn");
}
