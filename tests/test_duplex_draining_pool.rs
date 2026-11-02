//! Integration tests for duplex data transfer, graceful half-close draining,
//! and connection pool pre-warming / reuse.
//! All comments and docstrings must be in English.

use std::net::SocketAddr;
use std::sync::Arc;
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
use micro_tunnel::transport::ws::WsTransportDriver;
use micro_tunnel::transport::TransportRegistry;
use micro_tunnel::tunnel::run_forward_tunnel;

fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind ephemeral port");
    listener.local_addr().unwrap().port()
}

/// Spawns a full-duplex echo server that echoes all received data back.
fn spawn_echo_server(addr: SocketAddr, cancel_token: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.expect("Failed to bind echo server");
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
    })
}

#[tokio::test]
async fn test_high_volume_duplex_stream_integrity() {
    let cancel_token = CancellationToken::new();

    let echo_port = get_available_port();
    let server_ws_port = get_available_port();
    let client_listen_port = get_available_port();

    let echo_addr: SocketAddr = format!("127.0.0.1:{}", echo_port).parse().unwrap();
    let server_ws_listen = format!("127.0.0.1:{}", server_ws_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);
    let auth_token = "high_volume_test_token";

    let echo_handle = spawn_echo_server(echo_addr, cancel_token.clone());

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: server_ws_listen.clone(),
        token: auth_token.to_string(),
        default_target: None,
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
        mode: TunnelMode::Forward,
        listen: client_listen.clone(),
        target: format!("127.0.0.1:{}", echo_port),
        token: auth_token.to_string(),
        pool_size: None,
        failover: None,
        endpoint: EndpointConfig::Simple(format!("ws://{}/ws", server_ws_listen)),
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ = run_forward_tunnel(
            tunnel_config,
            endpoint_ctx,
            client_registry,
            client_cancel,
        ).await;
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
        rd.read_exact(&mut recv_data).await.unwrap();
        recv_data
    });

    let (_, recv_data) = tokio::join!(writer_handle, reader_handle);
    let recv_data = recv_data.unwrap();

    assert_eq!(recv_data.len(), DATA_SIZE);
    assert_eq!(&recv_data[..], &send_data[..], "High-volume duplex data mismatch!");

    cancel_token.cancel();
    let _ = tokio::join!(echo_handle, server_handle, client_handle);
}

#[tokio::test]
async fn test_stream_half_close_and_draining() {
    let cancel_token = CancellationToken::new();

    let backend_service_port = get_available_port();
    let server_ws_port = get_available_port();
    let client_listen_port = get_available_port();

    let backend_addr: SocketAddr = format!("127.0.0.1:{}", backend_service_port).parse().unwrap();
    let server_ws_listen = format!("127.0.0.1:{}", server_ws_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);
    let auth_token = "draining_test_token";

    // Backend service that reads until EOF (half-close draining), then responds
    let service_cancel = cancel_token.clone();
    let backend_handle = tokio::spawn(async move {
        let listener = TcpListener::bind(backend_addr).await.unwrap();
        tokio::select! {
            _ = service_cancel.cancelled() => {}
            res = listener.accept() => {
                let (mut socket, _) = res.unwrap();
                let mut buf = Vec::new();
                // Read all data until client sends FIN (half-close)
                socket.read_to_end(&mut buf).await.unwrap();

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
        listen: server_ws_listen.clone(),
        token: auth_token.to_string(),
        default_target: None,
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
        mode: TunnelMode::Forward,
        listen: client_listen.clone(),
        target: format!("127.0.0.1:{}", backend_service_port),
        token: auth_token.to_string(),
        pool_size: None,
        failover: None,
        endpoint: EndpointConfig::Simple(format!("ws://{}/ws", server_ws_listen)),
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ = run_forward_tunnel(
            tunnel_config,
            endpoint_ctx,
            client_registry,
            client_cancel,
        ).await;
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
    stream.read_to_string(&mut reply).await.unwrap();
    assert_eq!(reply, format!("DRAINED_TOTAL_{}", UPLOAD_SIZE));

    cancel_token.cancel();
    let _ = tokio::join!(backend_handle, server_handle, client_handle);
}

#[tokio::test]
async fn test_pool_prewarm_and_reuse() {
    let cancel_token = CancellationToken::new();

    let echo_port = get_available_port();
    let server_ws_port = get_available_port();
    let client_listen_port = get_available_port();

    let echo_addr: SocketAddr = format!("127.0.0.1:{}", echo_port).parse().unwrap();
    let server_ws_listen = format!("127.0.0.1:{}", server_ws_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);
    let auth_token = "pool_reuse_test_token";

    let echo_handle = spawn_echo_server(echo_addr, cancel_token.clone());

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: server_ws_listen.clone(),
        token: auth_token.to_string(),
        default_target: None,
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
        mode: TunnelMode::Forward,
        listen: client_listen.clone(),
        target: format!("127.0.0.1:{}", echo_port),
        token: auth_token.to_string(),
        pool_size: Some(4), // Pre-warm 4 connections
        failover: None,
        endpoint: EndpointConfig::Simple(format!("ws://{}/ws", server_ws_listen)),
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ = run_forward_tunnel(
            tunnel_config,
            endpoint_ctx,
            client_registry,
            client_cancel,
        ).await;
    });

    // Give pool time to pre-warm the 4 idle connections
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Send 6 sequential connections; all should acquire from pool in 0-RTT
    for i in 0..6 {
        let mut stream = TcpStream::connect(&client_listen)
            .await
            .expect("Failed to connect client listen port");

        let msg = format!("Pooled connection request #{}", i);
        stream.write_all(msg.as_bytes()).await.unwrap();

        let mut buf = vec![0u8; msg.len()];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, msg.as_bytes());
    }

    cancel_token.cancel();
    let _ = tokio::join!(echo_handle, server_handle, client_handle);
}
