//! Comprehensive integration test suite for HTTP/3 and raw QUIC transport.
//! All comments and docstrings must be in English.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use micro_tunnel::config::model::{AppConfig, EndpointConfig, TlsConfig, TunnelMode};
use micro_tunnel::core::error::TunnelError;
use micro_tunnel::dialer::endpoint::EndpointContext;
use micro_tunnel::transport::h3::H3Connector;
use micro_tunnel::transport::h3::listener::H3Listener;
use micro_tunnel::transport::{TargetAddr, TransportConnector, TransportListener};

fn get_ephemeral_port() -> u16 {
    let listener = std::net::UdpSocket::bind("127.0.0.1:0").expect("Failed to bind UDP ephemeral port");
    listener.local_addr().unwrap().port()
}

/// Helper to generate a test PKI certificate and ServerConfig for QUIC.
fn generate_test_quic_pki() -> (
    rustls::ServerConfig,
    rustls::pki_types::CertificateDer<'static>,
) {
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let cert_key = rcgen::generate_simple_self_signed(subject_alt_names).unwrap();

    let cert_der = cert_key.cert.der().to_vec();
    let key_der = cert_key.signing_key.serialize_der();

    let cert_chain = vec![rustls::pki_types::CertificateDer::from(cert_der.clone())];
    let private_key = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
    );

    let server_rustls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .unwrap();

    (
        server_rustls,
        rustls::pki_types::CertificateDer::from(cert_der),
    )
}

#[tokio::test]
async fn test_h3_transport_handshake_and_bidirectional_transfer() {
    let port = get_ephemeral_port();
    let listen_addr = format!("127.0.0.1:{}", port);
    let auth_token = "secret_h3_token_12345";

    let (server_rustls, _cert) = generate_test_quic_pki();
    let mut listener =
        H3Listener::bind(&listen_addr, Some(auth_token), None, server_rustls).unwrap();
    let (tx_done, rx_done) = tokio::sync::oneshot::channel::<()>();

    // Spawn server accept task
    let server_task = tokio::spawn(async move {
        let (mut stream, metadata) = listener.accept().await.unwrap();
        assert_eq!(metadata.mode, TunnelMode::Forward);
        assert_eq!(metadata.session_id.as_deref(), Some("sess_h3_test_1"));
        assert_eq!(
            metadata.target,
            Some(TargetAddr::Domain("backend.local".to_string(), 8080))
        );

        // Read 100KB test data from client
        let mut buf = vec![0u8; 100 * 1024];
        stream.read_exact(&mut buf).await.unwrap();

        // Echo reversed data back
        buf.reverse();
        stream.write_all(&buf).await.unwrap();
        stream.shutdown().await.unwrap();

        // Hold listener until client has finished reading
        let _ = rx_done.await;
    });

    // Client endpoint configuration
    let endpoint_cfg = EndpointConfig::Detailed {
        url: format!("h3://{}/tunnel", listen_addr),
        dns: None,
        dialer: None,
        tls: Some(TlsConfig {
            enabled: true,
            insecure: true, // Accept self-signed test cert
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
    };

    let app_cfg = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&endpoint_cfg, &app_cfg).unwrap();
    let connector = H3Connector::new(&endpoint_ctx, auth_token, TunnelMode::Forward).unwrap();

    let target = TargetAddr::Domain("backend.local".to_string(), 8080);
    let mut client_stream = connector
        .connect(&target, Some("sess_h3_test_1"))
        .await
        .unwrap();

    // Generate 100KB test data
    let test_data: Vec<u8> = (0..100 * 1024).map(|i| (i % 251) as u8).collect();
    client_stream.write_all(&test_data).await.unwrap();
    client_stream.shutdown().await.unwrap();

    // Read echoed response
    let mut response = vec![0u8; 100 * 1024];
    client_stream.read_exact(&mut response).await.unwrap();

    let mut expected = test_data.clone();
    expected.reverse();
    assert_eq!(
        response, expected,
        "HTTP/3 echoed data must match reversed original"
    );

    let _ = tx_done.send(());
    server_task.await.unwrap();
}

#[tokio::test]
async fn test_h3_transport_auth_failure() {
    let port = get_ephemeral_port();
    let listen_addr = format!("127.0.0.1:{}", port);
    let valid_token = "valid_secret_token";
    let invalid_token = "invalid_wrong_token";

    let (server_rustls, _) = generate_test_quic_pki();
    let _listener = H3Listener::bind(&listen_addr, Some(valid_token), None, server_rustls).unwrap();

    let endpoint_cfg = EndpointConfig::Detailed {
        url: format!("h3://{}/tunnel", listen_addr),
        dns: None,
        dialer: None,
        tls: Some(TlsConfig {
            enabled: true,
            insecure: true,
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
    };

    let app_cfg = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&endpoint_cfg, &app_cfg).unwrap();
    let connector = H3Connector::new(&endpoint_ctx, invalid_token, TunnelMode::Forward).unwrap();

    let target = TargetAddr::Domain("backend.local".to_string(), 8080);
    let res = connector.connect(&target, None).await;

    match res {
        Err(TunnelError::AuthFailed) => {}
        Err(e) => panic!("Expected AuthFailed, got error: {:?}", e),
        Ok(_) => panic!("Expected AuthFailed, got Ok stream"),
    }
}

#[tokio::test]
async fn test_quic_raw_stream_bidirectional_transfer() {
    let port = get_ephemeral_port();
    let listen_addr = format!("127.0.0.1:{}", port);
    let auth_token = "secret_quic_raw_token";

    let (server_rustls, _) = generate_test_quic_pki();
    let mut listener =
        H3Listener::bind(&listen_addr, Some(auth_token), None, server_rustls).unwrap();
    let (tx_done, rx_done) = tokio::sync::oneshot::channel::<()>();

    let server_task = tokio::spawn(async move {
        let (mut stream, metadata) = listener.accept().await.unwrap();
        assert_eq!(metadata.mode, TunnelMode::Forward);
        assert_eq!(metadata.session_id.as_deref(), Some("sess_quic_raw_1"));

        let mut buf = vec![0u8; 64 * 1024];
        stream.read_exact(&mut buf).await.unwrap();
        buf.reverse();
        stream.write_all(&buf).await.unwrap();
        stream.shutdown().await.unwrap();

        // Hold listener until client finishes reading response
        let _ = rx_done.await;
    });

    let endpoint_cfg = EndpointConfig::Detailed {
        url: format!("quic://{}/raw", listen_addr),
        dns: None,
        dialer: None,
        tls: Some(TlsConfig {
            enabled: true,
            insecure: true,
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
    };

    let app_cfg = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&endpoint_cfg, &app_cfg).unwrap();
    let connector = H3Connector::new(&endpoint_ctx, auth_token, TunnelMode::Forward).unwrap();

    let target = TargetAddr::Ip("10.0.0.1:443".parse().unwrap());
    let mut client_stream = connector
        .connect(&target, Some("sess_quic_raw_1"))
        .await
        .unwrap();

    let test_data: Vec<u8> = (0..64 * 1024).map(|i| (i % 239) as u8).collect();
    client_stream.write_all(&test_data).await.unwrap();
    client_stream.shutdown().await.unwrap();

    let mut response = vec![0u8; 64 * 1024];
    client_stream.read_exact(&mut response).await.unwrap();

    let mut expected = test_data.clone();
    expected.reverse();
    assert_eq!(
        response, expected,
        "Raw QUIC echoed data must match reversed original"
    );

    let _ = tx_done.send(());
    server_task.await.unwrap();
}

#[tokio::test]
async fn test_h3_stream_multiplexing_over_single_quic_connection() {
    let port = get_ephemeral_port();
    let listen_addr = format!("127.0.0.1:{}", port);
    let auth_token = "multiplex_token_789";

    let (server_rustls, _) = generate_test_quic_pki();
    let mut listener =
        H3Listener::bind(&listen_addr, Some(auth_token), None, server_rustls).unwrap();
    let (tx_done, rx_done) = tokio::sync::oneshot::channel::<()>();

    const STREAMS: usize = 3;

    // Spawn server accepting 3 streams
    let server_task = tokio::spawn(async move {
        for _ in 0..STREAMS {
            let (mut stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut data = Vec::new();
                stream.read_to_end(&mut data).await.unwrap();
                data.reverse();
                stream.write_all(&data).await.unwrap();
                stream.shutdown().await.unwrap();
            });
        }
        let _ = rx_done.await;
    });

    let endpoint_cfg = EndpointConfig::Detailed {
        url: format!("h3://{}/tunnel", listen_addr),
        dns: None,
        dialer: None,
        tls: Some(TlsConfig {
            enabled: true,
            insecure: true,
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
    };

    let app_cfg = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&endpoint_cfg, &app_cfg).unwrap();
    let connector =
        Arc::new(H3Connector::new(&endpoint_ctx, auth_token, TunnelMode::Forward).unwrap());

    let mut client_handles = Vec::new();
    for i in 0..STREAMS {
        let connector_clone = Arc::clone(&connector);
        let h = tokio::spawn(async move {
            let target = TargetAddr::Domain(format!("backend{}.local", i), 80);
            let mut stream = connector_clone.connect(&target, None).await.unwrap();

            let payload = format!("stream {} payload data", i).into_bytes();
            stream.write_all(&payload).await.unwrap();
            stream.shutdown().await.unwrap();

            let mut resp = Vec::new();
            stream.read_to_end(&mut resp).await.unwrap();

            let mut expected = payload.clone();
            expected.reverse();
            assert_eq!(resp, expected);
        });
        client_handles.push(h);
    }

    for h in client_handles {
        h.await.unwrap();
    }
    let _ = tx_done.send(());
    server_task.await.unwrap();
}

#[tokio::test]
async fn test_h3_tunnel_pool_prewarm_and_activation_end_to_end() {
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let echo_port = get_ephemeral_port();
    let server_h3_port = get_ephemeral_port();
    let client_listen_port = get_ephemeral_port();

    let echo_addr: std::net::SocketAddr = format!("127.0.0.1:{}", echo_port).parse().unwrap();
    let server_h3_listen = format!("127.0.0.1:{}", server_h3_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);

    // 1. Spawn TCP echo service backend
    let echo_cancel = cancel_token.clone();
    let echo_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(echo_addr).await.unwrap();
        loop {
            tokio::select! {
                _ = echo_cancel.cancelled() => break,
                res = listener.accept() => {
                    let (mut socket, _) = match res {
                        Ok(c) => c,
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

    // 2. Setup server with H3 listener and self-signed cert, no token, path "/tunnel"
    let mut server_registry = micro_tunnel::transport::TransportRegistry::new();
    server_registry.register(micro_tunnel::transport::h3::H3TransportDriver);
    let server_registry = Arc::new(server_registry);
    let session_mgr = Arc::new(micro_tunnel::session::SessionManager::new());

    let server_config = micro_tunnel::config::model::ServerConfig {
        listen: vec![format!("h3://{}/tunnel", server_h3_listen)],
        token: None, // Optional token / no token
        default_target: Some(format!("127.0.0.1:{}", echo_port)),
        ..Default::default()
    };
    let server_runner = micro_tunnel::server::ServerRunner::new(
        server_config,
        Arc::clone(&server_registry),
        Arc::clone(&session_mgr),
    );
    let s_cancel = cancel_token.clone();
    let server_handle = tokio::spawn(async move {
        let _ = server_runner.run(s_cancel).await;
    });

    // Give server time to bind UDP port
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 3. Setup client AppConfig with forward tunnel using pool_size: 4
    let app_config = micro_tunnel::config::model::AppConfig {
        tls: Some(TlsConfig {
            enabled: true,
            insecure: true,
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        tunnels: vec![micro_tunnel::config::model::TunnelConfig {
            tag: "bench-h3-pool".to_string(),
            mode: Some(TunnelMode::Forward),
            listen: Some(client_listen.clone()),
            target: Some(format!("127.0.0.1:{}", echo_port)),
            token: None,
            endpoint: EndpointConfig::Simple(format!("h3://{}/tunnel", server_h3_listen)),
            pool_size: Some(4), // Pre-warm 4 connections in pool!
            failover: None,
            multiplex: None,
            mappings: vec![],
            retry: None,
        }],
        ..Default::default()
    };

    let c_cancel = cancel_token.clone();
    let supervisor = micro_tunnel::supervisor::TunnelSupervisor::new(app_config, c_cancel);
    let client_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    // Wait for connection pool to pre-warm streams
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // 4. Connect 3 consecutive clients to client_listen port to consume pooled streams
    for i in 1..=3 {
        let mut client_tcp = tokio::net::TcpStream::connect(&client_listen)
            .await
            .expect("Failed to connect to local client tunnel port");

        let test_message = format!("H3 POOL ECHO TEST PACKET {}", i);
        client_tcp.write_all(test_message.as_bytes()).await.unwrap();

        let mut read_buf = vec![0u8; test_message.len()];
        client_tcp.read_exact(&mut read_buf).await.unwrap();
        assert_eq!(String::from_utf8_lossy(&read_buf), test_message);
    }

    cancel_token.cancel();
    let _ = tokio::join!(echo_handle, server_handle, client_handle);
}

#[tokio::test]
async fn test_quic_raw_tunnel_pool_prewarm_and_activation_end_to_end() {
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let echo_port = get_ephemeral_port();
    let server_quic_port = get_ephemeral_port();
    let client_listen_port = get_ephemeral_port();

    let echo_addr: std::net::SocketAddr = format!("127.0.0.1:{}", echo_port).parse().unwrap();
    let server_quic_listen = format!("127.0.0.1:{}", server_quic_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);

    // 1. Spawn TCP echo service backend
    let echo_cancel = cancel_token.clone();
    let echo_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(echo_addr).await.unwrap();
        loop {
            tokio::select! {
                _ = echo_cancel.cancelled() => break,
                res = listener.accept() => {
                    let (mut socket, _) = match res {
                        Ok(c) => c,
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

    // 2. Setup server with raw QUIC listener and self-signed cert
    let mut server_registry = micro_tunnel::transport::TransportRegistry::new();
    server_registry.register(micro_tunnel::transport::h3::H3TransportDriver);
    let server_registry = Arc::new(server_registry);
    let session_mgr = Arc::new(micro_tunnel::session::SessionManager::new());

    let server_config = micro_tunnel::config::model::ServerConfig {
        listen: vec![format!("quic://{}/raw", server_quic_listen)],
        token: None,
        default_target: Some(format!("127.0.0.1:{}", echo_port)),
        ..Default::default()
    };
    let server_runner = micro_tunnel::server::ServerRunner::new(
        server_config,
        Arc::clone(&server_registry),
        Arc::clone(&session_mgr),
    );
    let s_cancel = cancel_token.clone();
    let server_handle = tokio::spawn(async move {
        let _ = server_runner.run(s_cancel).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 3. Setup client AppConfig with forward tunnel using raw QUIC and pool_size: 4
    let app_config = micro_tunnel::config::model::AppConfig {
        tls: Some(TlsConfig {
            enabled: true,
            insecure: true,
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        tunnels: vec![micro_tunnel::config::model::TunnelConfig {
            tag: "bench-quic-pool".to_string(),
            mode: Some(TunnelMode::Forward),
            listen: Some(client_listen.clone()),
            target: Some(format!("127.0.0.1:{}", echo_port)),
            token: None,
            endpoint: EndpointConfig::Simple(format!("quic://{}/raw", server_quic_listen)),
            pool_size: Some(4),
            failover: None,
            multiplex: None,
            mappings: vec![],
            retry: None,
        }],
        ..Default::default()
    };

    let c_cancel = cancel_token.clone();
    let supervisor = micro_tunnel::supervisor::TunnelSupervisor::new(app_config, c_cancel);
    let client_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // 4. Connect 3 consecutive clients to client_listen port to consume pooled streams
    for i in 1..=3 {
        let mut client_tcp = tokio::net::TcpStream::connect(&client_listen)
            .await
            .expect("Failed to connect to local client tunnel port");

        let test_message = format!("QUIC RAW POOL ECHO TEST PACKET {}", i);
        client_tcp.write_all(test_message.as_bytes()).await.unwrap();

        let mut read_buf = vec![0u8; test_message.len()];
        client_tcp.read_exact(&mut read_buf).await.unwrap();
        assert_eq!(String::from_utf8_lossy(&read_buf), test_message);
    }

    cancel_token.cancel();
    let _ = tokio::join!(echo_handle, server_handle, client_handle);
}

#[tokio::test]
async fn test_quic_raw_forward_stress_throughput() {
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let sink_port = get_ephemeral_port();
    let server_quic_port = get_ephemeral_port();
    let client_listen_port = get_ephemeral_port();

    let sink_addr: std::net::SocketAddr = format!("127.0.0.1:{}", sink_port).parse().unwrap();
    let server_quic_listen = format!("127.0.0.1:{}", server_quic_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);

    // 1. Spawn TCP sink service backend (reads all bytes until EOF and counts)
    let sink_cancel = cancel_token.clone();
    let (tx_total, rx_total) = tokio::sync::oneshot::channel::<usize>();
    let sink_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(sink_addr).await.unwrap();
        tokio::select! {
            _ = sink_cancel.cancelled() => {}
            res = listener.accept() => {
                let (mut socket, _) = res.unwrap();
                let mut total = 0;
                let mut buf = vec![0u8; 64 * 1024];
                loop {
                    match socket.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => total += n,
                        Err(e) => {
                            eprintln!("Sink read error: {:?}", e);
                            break;
                        }
                    }
                }
                let _ = tx_total.send(total);
            }
        }
    });

    // 2. Setup server with raw QUIC listener
    let mut server_registry = micro_tunnel::transport::TransportRegistry::new();
    server_registry.register(micro_tunnel::transport::h3::H3TransportDriver);
    let server_registry = Arc::new(server_registry);
    let session_mgr = Arc::new(micro_tunnel::session::SessionManager::new());

    let server_config = micro_tunnel::config::model::ServerConfig {
        listen: vec![format!("quic://{}/raw", server_quic_listen)],
        token: None,
        default_target: Some(format!("127.0.0.1:{}", sink_port)),
        ..Default::default()
    };
    let server_runner = micro_tunnel::server::ServerRunner::new(
        server_config,
        Arc::clone(&server_registry),
        Arc::clone(&session_mgr),
    );
    let s_cancel = cancel_token.clone();
    let server_handle = tokio::spawn(async move {
        let _ = server_runner.run(s_cancel).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 3. Setup client AppConfig with forward tunnel
    let app_config = micro_tunnel::config::model::AppConfig {
        tls: Some(TlsConfig {
            enabled: true,
            insecure: true,
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        tunnels: vec![micro_tunnel::config::model::TunnelConfig {
            tag: "bench-quic-stress".to_string(),
            mode: Some(TunnelMode::Forward),
            listen: Some(client_listen.clone()),
            target: Some(format!("127.0.0.1:{}", sink_port)),
            token: None,
            endpoint: EndpointConfig::Simple(format!("quic://{}/raw", server_quic_listen)),
            pool_size: Some(4),
            failover: None,
            multiplex: None,
            mappings: vec![],
            retry: None,
        }],
        ..Default::default()
    };

    let c_cancel = cancel_token.clone();
    let supervisor = micro_tunnel::supervisor::TunnelSupervisor::new(app_config, c_cancel);
    let client_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // 4. Connect client and blast 20MB of data
    let mut client_tcp = tokio::net::TcpStream::connect(&client_listen)
        .await
        .expect("Failed to connect to local client tunnel port");

    const CHUNK_SIZE: usize = 64 * 1024;
    const CHUNKS: usize = 1600; // 100 MB
    let chunk = vec![0x5au8; CHUNK_SIZE];

    for _ in 0..CHUNKS {
        client_tcp.write_all(&chunk).await.unwrap();
    }
    client_tcp.shutdown().await.unwrap();

    let total_received = rx_total.await.unwrap();
    assert_eq!(total_received, CHUNK_SIZE * CHUNKS);

    cancel_token.cancel();
    let _ = tokio::join!(sink_handle, server_handle, client_handle);
}

#[tokio::test]
async fn test_h3_concurrent_forward_stress() {
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let echo_port = get_ephemeral_port();
    let server_h3_port = get_ephemeral_port();
    let client_listen_port = get_ephemeral_port();

    let echo_addr: std::net::SocketAddr = format!("127.0.0.1:{}", echo_port).parse().unwrap();
    let server_h3_listen = format!("127.0.0.1:{}", server_h3_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);

    // 1. Spawn TCP echo service backend
    let echo_cancel = cancel_token.clone();
    let echo_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(echo_addr).await.unwrap();
        loop {
            tokio::select! {
                _ = echo_cancel.cancelled() => break,
                res = listener.accept() => {
                    let (mut socket, _) = match res {
                        Ok(c) => c,
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

    // 2. Setup server with H3 listener
    let mut server_registry = micro_tunnel::transport::TransportRegistry::new();
    server_registry.register(micro_tunnel::transport::h3::H3TransportDriver);
    let server_registry = Arc::new(server_registry);
    let session_mgr = Arc::new(micro_tunnel::session::SessionManager::new());

    let server_config = micro_tunnel::config::model::ServerConfig {
        listen: vec![format!("h3://{}/tunnel", server_h3_listen)],
        token: None,
        default_target: Some(format!("127.0.0.1:{}", echo_port)),
        ..Default::default()
    };
    let server_runner = micro_tunnel::server::ServerRunner::new(
        server_config,
        Arc::clone(&server_registry),
        Arc::clone(&session_mgr),
    );
    let s_cancel = cancel_token.clone();
    let server_handle = tokio::spawn(async move {
        let _ = server_runner.run(s_cancel).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 3. Setup client AppConfig with forward tunnel using pool_size: 4
    let app_config = micro_tunnel::config::model::AppConfig {
        tls: Some(TlsConfig {
            enabled: true,
            insecure: true,
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        tunnels: vec![micro_tunnel::config::model::TunnelConfig {
            tag: "bench-h3-conc".to_string(),
            mode: Some(TunnelMode::Forward),
            listen: Some(client_listen.clone()),
            target: Some(format!("127.0.0.1:{}", echo_port)),
            token: None,
            endpoint: EndpointConfig::Simple(format!("h3://{}/tunnel", server_h3_listen)),
            pool_size: Some(4),
            failover: None,
            multiplex: None,
            mappings: vec![],
            retry: None,
        }],
        ..Default::default()
    };

    let c_cancel = cancel_token.clone();
    let supervisor = micro_tunnel::supervisor::TunnelSupervisor::new(app_config, c_cancel);
    let client_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // 4. Run 10 concurrent streams pushing 2MB each and verifying echo
    const CONCURRENCY: usize = 10;
    const CHUNK_SIZE: usize = 32 * 1024;
    const CHUNKS_PER_STREAM: usize = 64; // 2MB

    let mut handles = Vec::new();
    for id in 0..CONCURRENCY {
        let addr = client_listen.clone();
        handles.push(tokio::spawn(async move {
            let client = tokio::net::TcpStream::connect(&addr).await.unwrap();
            let (mut rd, mut wr) = client.into_split();

            let payload = vec![(id as u8).wrapping_add(1); CHUNK_SIZE];

            let wr_task = tokio::spawn(async move {
                for _ in 0..CHUNKS_PER_STREAM {
                    wr.write_all(&payload).await.unwrap();
                }
                wr.shutdown().await.unwrap();
            });

            let mut received = 0;
            let mut buf = vec![0u8; CHUNK_SIZE];
            loop {
                let n = rd.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                received += n;
            }

            wr_task.await.unwrap();
            assert_eq!(received, CHUNK_SIZE * CHUNKS_PER_STREAM);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    cancel_token.cancel();
    let _ = tokio::join!(echo_handle, server_handle, client_handle);
}







