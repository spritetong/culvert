// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Comprehensive integration test suite for HTTP/3 and raw QUIC transport.
//! All comments and docstrings must be in English.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use micro_tunnel::config::model::{
    AppConfig, EndpointConfig, PortMappingConfig, TlsConfig, TunnelMode,
};
use micro_tunnel::core::error::TunnelError;
use micro_tunnel::dialer::endpoint::EndpointContext;
use micro_tunnel::transport::h3::H3Connector;
use micro_tunnel::transport::h3::listener::H3Listener;
use micro_tunnel::transport::{TargetAddr, TransportConnector, TransportListener};


/// Helper to get an available ephemeral port from the OS that is free for both TCP and UDP.
fn get_ephemeral_port() -> u16 {
    for _ in 0..100 {
        let t = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind ephemeral TCP port");
        let port = t.local_addr().unwrap().port();
        if let Ok(u) = std::net::UdpSocket::bind(("127.0.0.1", port)) {
            drop(u);
            drop(t);
            return port;
        }
        drop(t);
    }
    panic!("Failed to find an available dual TCP/UDP ephemeral port");
}

/// Spawns a simple TCP echo server using held-listener handoff.
fn spawn_echo_server(
    cancel_token: tokio_util::sync::CancellationToken,
) -> (tokio::task::JoinHandle<()>, u16) {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = std_listener.local_addr().unwrap().port();
    std_listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
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
    (handle, port)
}

async fn connect_with_retry(addr: &str) -> tokio::net::TcpStream {
    for _ in 0..100 {
        if let Ok(s) = tokio::net::TcpStream::connect(addr).await {
            return s;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    tokio::net::TcpStream::connect(addr)
        .await
        .expect("Failed to connect to local tunnel port after retries")
}

async fn read_exact_timed<T: tokio::io::AsyncRead + Unpin>(
    s: &mut T,
    buf: &mut [u8],
) -> std::io::Result<usize> {
    tokio::time::timeout(std::time::Duration::from_secs(10), s.read_exact(buf))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read_exact timed out"))?
}

async fn read_to_end_timed<T: tokio::io::AsyncRead + Unpin>(
    s: &mut T,
    buf: &mut Vec<u8>,
) -> std::io::Result<usize> {
    tokio::time::timeout(std::time::Duration::from_secs(10), s.read_to_end(buf))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read_to_end timed out"))?
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
        read_exact_timed(&mut stream, &mut buf).await.unwrap();

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
            insecure: Some(true), // Accept self-signed test cert
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
    read_exact_timed(&mut client_stream, &mut response)
        .await
        .unwrap();

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
            insecure: Some(true),
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
        read_exact_timed(&mut stream, &mut buf).await.unwrap();
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
            insecure: Some(true),
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
    read_exact_timed(&mut client_stream, &mut response)
        .await
        .unwrap();

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
                read_to_end_timed(&mut stream, &mut data).await.unwrap();
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
            insecure: Some(true),
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
            read_to_end_timed(&mut stream, &mut resp).await.unwrap();

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

    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());
    let server_h3_port = get_ephemeral_port();
    let client_listen_port = get_ephemeral_port();

    let server_h3_listen = format!("127.0.0.1:{}", server_h3_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);

    // 1. Setup server with H3 listener and self-signed cert, no token, path "/tunnel" (echo server started via handoff)
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
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        tunnels: vec![micro_tunnel::config::model::TunnelConfig {
            tag: "bench-h3-pool".to_string(),
            token: None,
            endpoint: EndpointConfig::Simple(format!("h3://{}/tunnel", server_h3_listen)),
            pool_size: Some(4), // Pre-warm 4 connections in pool!
            failover: None,
            multiplex: None,
            mappings: vec![PortMappingConfig {
                mode: TunnelMode::Forward,
                listen: client_listen.clone(),
                target: format!("127.0.0.1:{}", echo_port),
            }],
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
        let mut client_tcp = connect_with_retry(&client_listen).await;

        let test_message = format!("H3 POOL ECHO TEST PACKET {}", i);
        client_tcp.write_all(test_message.as_bytes()).await.unwrap();

        let mut read_buf = vec![0u8; test_message.len()];
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client_tcp.read_exact(&mut read_buf),
        )
        .await
        .expect("Timed out reading echo response in H3 pool test")
        .unwrap();
        assert_eq!(String::from_utf8_lossy(&read_buf), test_message);
    }

    cancel_token.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(echo_handle, server_handle, client_handle);
        r1.expect("echo_handle panicked");
        r2.expect("server_handle panicked");
        r3.expect("client_handle panicked");
    })
    .await
    .expect(
        "Timeout waiting for tasks to exit after cancellation in test_h3_pool_prewarm_and_reuse",
    );
}

#[tokio::test]
async fn test_quic_raw_tunnel_pool_prewarm_and_activation_end_to_end() {
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());
    let server_quic_port = get_ephemeral_port();
    let client_listen_port = get_ephemeral_port();

    let server_quic_listen = format!("127.0.0.1:{}", server_quic_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);

    // 1. Setup server with raw QUIC listener and self-signed cert (echo server started via handoff)
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
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        tunnels: vec![micro_tunnel::config::model::TunnelConfig {
            tag: "bench-quic-pool".to_string(),
            token: None,
            endpoint: EndpointConfig::Simple(format!("quic://{}/raw", server_quic_listen)),
            pool_size: Some(4),
            failover: None,
            multiplex: None,
            mappings: vec![PortMappingConfig {
                mode: TunnelMode::Forward,
                listen: client_listen.clone(),
                target: format!("127.0.0.1:{}", echo_port),
            }],
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
        let mut client_tcp = connect_with_retry(&client_listen).await;

        let test_message = format!("QUIC RAW POOL ECHO TEST PACKET {}", i);
        client_tcp.write_all(test_message.as_bytes()).await.unwrap();

        let mut read_buf = vec![0u8; test_message.len()];
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client_tcp.read_exact(&mut read_buf),
        )
        .await
        .expect("Timed out reading echo response in QUIC pool test")
        .unwrap();
        assert_eq!(String::from_utf8_lossy(&read_buf), test_message);
    }

    cancel_token.cancel();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        async {
            let (r1, r2, r3) = tokio::join!(echo_handle, server_handle, client_handle);
            r1.expect("echo_handle panicked");
            r2.expect("server_handle panicked");
            r3.expect("client_handle panicked");
        },
    )
    .await
    .expect("Timeout waiting for tasks to exit after cancellation in test_quic_raw_tunnel_pool_prewarm_and_activation_end_to_end");
}

#[tokio::test]
async fn test_quic_raw_forward_stress_throughput() {
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let server_quic_port = get_ephemeral_port();
    let client_listen_port = get_ephemeral_port();

    let server_quic_listen = format!("127.0.0.1:{}", server_quic_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);

    // 1. Spawn TCP sink service backend (reads all bytes until EOF and counts)
    let sink_cancel = cancel_token.clone();
    let (tx_total, rx_total) = tokio::sync::oneshot::channel::<usize>();
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let sink_port = std_listener.local_addr().unwrap().port();
    std_listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
    let sink_handle = tokio::spawn(async move {
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
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        tunnels: vec![micro_tunnel::config::model::TunnelConfig {
            tag: "bench-quic-stress".to_string(),
            token: None,
            endpoint: EndpointConfig::Simple(format!("quic://{}/raw", server_quic_listen)),
            pool_size: Some(4),
            failover: None,
            multiplex: None,
            mappings: vec![PortMappingConfig {
                mode: TunnelMode::Forward,
                listen: client_listen.clone(),
                target: format!("127.0.0.1:{}", sink_port),
            }],
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
    let mut client_tcp = connect_with_retry(&client_listen).await;

    const CHUNK_SIZE: usize = 64 * 1024;
    const CHUNKS: usize = 1600; // 100 MB
    let chunk = vec![0x5au8; CHUNK_SIZE];

    for _ in 0..CHUNKS {
        client_tcp.write_all(&chunk).await.unwrap();
    }
    client_tcp.shutdown().await.unwrap();

    let total_received = tokio::time::timeout(std::time::Duration::from_secs(10), rx_total)
        .await
        .expect("Timed out waiting for rx_total in stress throughput test")
        .unwrap();
    assert_eq!(total_received, CHUNK_SIZE * CHUNKS);

    cancel_token.cancel();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        async {
            let (r1, r2, r3) = tokio::join!(sink_handle, server_handle, client_handle);
            r1.expect("sink_handle panicked");
            r2.expect("server_handle panicked");
            r3.expect("client_handle panicked");
        },
    )
    .await
    .expect("Timeout waiting for tasks to exit after cancellation in test_quic_raw_forward_stress_throughput");
}

#[tokio::test]
async fn test_h3_concurrent_forward_stress() {
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());
    let server_h3_port = get_ephemeral_port();
    let client_listen_port = get_ephemeral_port();

    let server_h3_listen = format!("127.0.0.1:{}", server_h3_port);
    let client_listen = format!("127.0.0.1:{}", client_listen_port);

    // 1. Setup server with H3 listener (echo server started via handoff)
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
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        tunnels: vec![micro_tunnel::config::model::TunnelConfig {
            tag: "bench-h3-conc".to_string(),
            token: None,
            endpoint: EndpointConfig::Simple(format!("h3://{}/tunnel", server_h3_listen)),
            pool_size: Some(4),
            failover: None,
            multiplex: None,
            mappings: vec![PortMappingConfig {
                mode: TunnelMode::Forward,
                listen: client_listen.clone(),
                target: format!("127.0.0.1:{}", echo_port),
            }],
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
            let client = connect_with_retry(&addr).await;
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
                let n = tokio::time::timeout(std::time::Duration::from_secs(10), rd.read(&mut buf))
                    .await
                    .expect("read timed out in concurrent forward stress")
                    .unwrap();
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
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let (r1, r2, r3) = tokio::join!(echo_handle, server_handle, client_handle);
        r1.expect("echo_handle panicked");
        r2.expect("server_handle panicked");
        r3.expect("client_handle panicked");
    })
    .await
    .expect(
        "Timeout waiting for tasks to exit after cancellation in test_h3_concurrent_forward_stress",
    );
}

#[tokio::test]
async fn test_h3_sibling_streams_survive_sibling_abort() {
    let port = get_ephemeral_port();
    let listen_addr = format!("127.0.0.1:{}", port);
    let auth_token = "sibling_survive_token";

    let (server_rustls, _) = generate_test_quic_pki();
    let mut listener =
        H3Listener::bind(&listen_addr, Some(auth_token), None, server_rustls).unwrap();
    let (tx_done, rx_done) = tokio::sync::oneshot::channel::<()>();

    // Server accepts multiple streams
    let server_task = tokio::spawn(async move {
        // Stream 1 handler: echo
        let (mut stream1, _) = listener.accept().await.unwrap();
        let s1_handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 16 * 1024];
            read_exact_timed(&mut stream1, &mut buf).await.unwrap();
            buf.reverse();
            stream1.write_all(&buf).await.unwrap();
            stream1.shutdown().await.unwrap();
        });

        // Stream 2 handler: just accepted, then client will abruptly drop
        let (mut stream2, _) = listener.accept().await.unwrap();
        let s2_handle = tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let _ = stream2.read(&mut buf).await;
        });

        let _ = rx_done.await;
        let _ = s1_handle.await;
        let _ = s2_handle.await;
    });

    let endpoint_cfg = EndpointConfig::Detailed {
        url: format!("h3://{}/tunnel", listen_addr),
        dns: None,
        dialer: None,
        tls: Some(TlsConfig {
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
    };

    let app_cfg = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&endpoint_cfg, &app_cfg).unwrap();
    let connector =
        Arc::new(H3Connector::new(&endpoint_ctx, auth_token, TunnelMode::Forward).unwrap());

    let target = TargetAddr::Domain("backend.local".to_string(), 80);

    // 1. Open stream 1
    let mut stream1 = connector.connect(&target, None).await.unwrap();

    // 2. Open stream 2 and abruptly drop it (simulating error/abort)
    let stream2 = connector.connect(&target, None).await.unwrap();
    drop(stream2);

    // 3. Verify stream 1 over the shared connection is completely unaffected
    let test_data: Vec<u8> = (0..16 * 1024).map(|i| (i % 251) as u8).collect();
    stream1.write_all(&test_data).await.unwrap();
    stream1.shutdown().await.unwrap();

    let mut response = vec![0u8; 16 * 1024];
    read_exact_timed(&mut stream1, &mut response).await.unwrap();

    let mut expected = test_data.clone();
    expected.reverse();
    assert_eq!(
        response, expected,
        "Stream 1 must complete echoed transfer unaffected by sibling stream drop"
    );

    let _ = tx_done.send(());
    server_task.await.unwrap();
}

#[tokio::test]
async fn test_h3_stream_liveness_detects_closed_connection() {
    let port = get_ephemeral_port();
    let listen_addr = format!("127.0.0.1:{}", port);
    let auth_token = "liveness_test_token";

    let (server_rustls, _) = generate_test_quic_pki();
    let mut listener =
        H3Listener::bind(&listen_addr, Some(auth_token), None, server_rustls).unwrap();

    let server_task = tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            // Keep stream open until client reads or terminates
            let mut buf = [0u8; 16];
            let _ = stream.read(&mut buf).await;
        }
    });

    let endpoint_cfg = EndpointConfig::Detailed {
        url: format!("h3://{}/tunnel", listen_addr),
        dns: None,
        dialer: None,
        tls: Some(TlsConfig {
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
    };

    let app_cfg = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&endpoint_cfg, &app_cfg).unwrap();
    let connector =
        Arc::new(H3Connector::new(&endpoint_ctx, auth_token, TunnelMode::Forward).unwrap());

    let target = TargetAddr::Domain("backend.local".to_string(), 80);
    let mut stream = connector.connect(&target, None).await.unwrap();

    // While connection is open, check alive
    let waker = futures_util::task::noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert_eq!(
        stream.poll_check_alive(&mut cx),
        std::task::Poll::Ready(micro_tunnel::core::stream::HeartbeatStatus::Alive)
    );
    assert!(stream.poll_send_ping(&mut cx).is_ready());

    // Abort server task, dropping listener and triggering cancellation
    server_task.abort();
    let _ = server_task.await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Stream must now detect dead/closed connection
    let alive_status = stream.poll_check_alive(&mut cx);
    assert_eq!(
        alive_status,
        std::task::Poll::Ready(micro_tunnel::core::stream::HeartbeatStatus::Dead)
    );
}

#[tokio::test]
async fn test_h3_connector_recovers_after_server_restart() {
    let port = get_ephemeral_port();
    let listen_addr = format!("127.0.0.1:{}", port);
    let auth_token = "restart_test_token";

    // 1. Start Server 1
    let (server_rustls_1, _) = generate_test_quic_pki();
    let mut listener_1 =
        H3Listener::bind(&listen_addr, Some(auth_token), None, server_rustls_1).unwrap();

    let server_1_cancel = tokio_util::sync::CancellationToken::new();
    let s1_cancel = server_1_cancel.clone();
    let server_1_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = s1_cancel.cancelled() => break,
                res = listener_1.accept() => {
                    let (mut stream, _) = match res {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    tokio::spawn(async move {
                        let mut buf = [0u8; 1024];
                        if let Ok(n) = stream.read(&mut buf).await {
                            let _ = stream.write_all(&buf[..n]).await;
                            let _ = stream.shutdown().await;
                        }
                    });
                }
            }
        }
    });

    let endpoint_cfg = EndpointConfig::Detailed {
        url: format!("h3://{}/tunnel", listen_addr),
        dns: None,
        dialer: None,
        tls: Some(TlsConfig {
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
    };

    let app_cfg = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&endpoint_cfg, &app_cfg).unwrap();
    let connector =
        Arc::new(H3Connector::new(&endpoint_ctx, auth_token, TunnelMode::Forward).unwrap());

    let target = TargetAddr::Domain("backend.local".to_string(), 80);

    // 2. Connect to Server 1 and verify data transfer
    let mut stream1 = connector.connect(&target, None).await.unwrap();
    stream1.write_all(b"hello_server_1").await.unwrap();
    stream1.shutdown().await.unwrap();
    let mut resp1 = Vec::new();
    read_to_end_timed(&mut stream1, &mut resp1).await.unwrap();
    assert_eq!(resp1, b"hello_server_1");

    // 3. Drop Server 1 (simulating process restart)
    server_1_cancel.cancel();
    let _ = server_1_task.await;
    // Allow UDP socket to release
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 4. Start Server 2 on the exact same port with fresh TLS config
    let (server_rustls_2, _) = generate_test_quic_pki();
    let mut listener_2 =
        H3Listener::bind(&listen_addr, Some(auth_token), None, server_rustls_2).unwrap();

    let server_2_cancel = tokio_util::sync::CancellationToken::new();
    let s2_cancel = server_2_cancel.clone();
    let server_2_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = s2_cancel.cancelled() => break,
                res = listener_2.accept() => {
                    let (mut stream, _) = match res {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    tokio::spawn(async move {
                        let mut buf = [0u8; 1024];
                        if let Ok(n) = stream.read(&mut buf).await {
                            let _ = stream.write_all(&buf[..n]).await;
                            let _ = stream.shutdown().await;
                        }
                    });
                }
            }
        }
    });

    // 5. Connect again. When the first attempt detects or times out against the dead session,
    // the zombie session is invalidated and subsequent reconnect dials Server 2.
    let start = std::time::Instant::now();
    let mut stream2 = None;
    for _ in 0..5 {
        match connector.connect(&target, None).await {
            Ok(s) => {
                stream2 = Some(s);
                break;
            }
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }

    let mut stream2 = stream2.expect("Client should successfully reconnect to Server 2");
    stream2.write_all(b"hello_server_2").await.unwrap();
    stream2.shutdown().await.unwrap();
    let mut resp2 = Vec::new();
    read_to_end_timed(&mut stream2, &mut resp2).await.unwrap();
    assert_eq!(resp2, b"hello_server_2");

    let elapsed = start.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(15),
        "Recovery must complete well within ACQUIRE_TIMEOUT (15s), took {:?}",
        elapsed
    );

    server_2_cancel.cancel();
    let _ = server_2_task.await;
}
