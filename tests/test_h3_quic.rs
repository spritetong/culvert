//! Comprehensive integration test suite for HTTP/3 and raw QUIC transport.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use micro_tunnel::config::model::{AppConfig, EndpointConfig, TlsConfig, TunnelMode};
use micro_tunnel::core::error::TunnelError;
use micro_tunnel::dialer::endpoint::EndpointContext;
use micro_tunnel::transport::h3::H3Connector;
use micro_tunnel::transport::h3::listener::H3Listener;
use micro_tunnel::transport::{TargetAddr, TransportConnector, TransportListener};

static PORT_COUNTER: AtomicUsize = AtomicUsize::new(51200);

fn get_ephemeral_port() -> u16 {
    let pid = std::process::id() as u16;
    let offset = PORT_COUNTER.fetch_add(1, Ordering::SeqCst) as u16;
    40000 + ((pid ^ offset) % 15000)
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
