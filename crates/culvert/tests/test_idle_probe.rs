// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Targeted probe tests for `QuinnTunnelStream` liveness and dead-connection behavior.
//! All comments and docstrings must be in English.

use culvert::backend::TransportConnector;
use culvert::core::stream::HeartbeatStatus;
use culvert::transport::TargetAddr;
use std::time::Duration;
use tokio::io::AsyncReadExt;

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
async fn probe_quinn_tunnel_stream_poll_check_alive_blind_to_closed_stream() {
    use culvert::backend::accept_one;
    use culvert::config::model::{AppConfig, EndpointConfig, TlsConfig, TunnelMode};
    use culvert::dialer::endpoint::EndpointContext;
    use culvert::transport::h3::H3Connector;
    use culvert::transport::h3::listener::H3Listener;

    let port = {
        let t = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        t.local_addr().unwrap().port()
    };
    let listen_addr = format!("127.0.0.1:{}", port);
    let auth_token = "probe_quic_token";

    let (server_rustls, _) = generate_test_quic_pki();
    let mut listener =
        H3Listener::bind(&listen_addr, Some(auth_token), None, server_rustls).unwrap();

    let server_task = tokio::spawn(async move {
        // Accept the raw QUIC stream
        let (stream, metadata) = accept_one(&mut listener).await.unwrap();
        assert!(metadata.pool_idle);
        // Server immediately closes/drops the stream!
        drop(stream);
        // Keep listener alive so connection doesn't terminate
        tokio::time::sleep(Duration::from_millis(500)).await;
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
        http_headers: None,
    };

    let app_cfg = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&endpoint_cfg, &app_cfg).unwrap();
    let connector = H3Connector::new(&endpoint_ctx, auth_token, TunnelMode::Forward).unwrap();

    let dummy_target = TargetAddr::Domain("pool.idle".to_string(), 0);
    let mut client_stream = connector
        .connect_pooled(&dummy_target)
        .await
        .unwrap()
        .into_stream()
        .unwrap();

    // Wait for the server drop and QUIC RESET_STREAM / FIN packets to arrive at client:
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Now test: what does poll_check_alive report on client_stream?
    let alive_status = std::future::poll_fn(|cx| client_stream.poll_check_alive(cx)).await;
    println!("client_stream.poll_check_alive: {:?}", alive_status);

    // QuinnTunnelStream::poll_check_alive must detect the closed/reset stream and report Dead!
    assert_eq!(
        alive_status,
        HeartbeatStatus::Dead,
        "poll_check_alive must detect closed stream as Dead, not Alive!"
    );

    let _ = server_task.await;
}

#[tokio::test]
async fn probe_quinn_tunnel_stream_read_aborts_immediately_when_session_marked_dead() {
    use culvert::backend::accept_one;
    use culvert::config::model::{AppConfig, EndpointConfig, TlsConfig, TunnelMode};
    use culvert::dialer::endpoint::EndpointContext;
    use culvert::transport::h3::H3Connector;
    use culvert::transport::h3::listener::H3Listener;

    let port = {
        let t = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        t.local_addr().unwrap().port()
    };
    let listen_addr = format!("127.0.0.1:{}", port);
    let auth_token = "probe_quic_dead_session";

    let (server_rustls, _) = generate_test_quic_pki();
    let mut listener =
        H3Listener::bind(&listen_addr, Some(auth_token), None, server_rustls).unwrap();

    let server_task = tokio::spawn(async move {
        let (stream, metadata) = accept_one(&mut listener).await.unwrap();
        assert!(metadata.pool_idle);
        // Server stays alive and silent without writing to stream
        tokio::time::sleep(Duration::from_secs(2)).await;
        drop(stream);
        drop(listener);
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
        http_headers: None,
    };

    let app_cfg = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&endpoint_cfg, &app_cfg).unwrap();
    let connector = H3Connector::new(&endpoint_ctx, auth_token, TunnelMode::Forward).unwrap();

    let dummy_target = TargetAddr::Domain("pool.idle".to_string(), 0);
    let mut client_stream = connector
        .connect_pooled(&dummy_target)
        .await
        .unwrap()
        .into_stream()
        .unwrap();

    // Abruptly abort server to simulate sudden failure
    server_task.abort();

    // Wait briefly for connection drop to register
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Reading on client_stream must not hang waiting for data; it must fail fast with error!
    let mut buf = [0u8; 1];
    let res = tokio::time::timeout(Duration::from_millis(500), client_stream.read(&mut buf)).await;
    assert!(res.is_ok(), "read hung waiting on dead connection!");
    assert!(
        res.unwrap().is_err(),
        "read should return error when connection drops!"
    );
}
