// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Test verifying that large unread streams do not cause connection-level
//! head-of-line (HOL) blocking for healthy sibling streams under QUIC flow control.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

use micro_tunnel::config::model::{AppConfig, EndpointConfig, TlsConfig, TunnelMode};
use micro_tunnel::dialer::endpoint::EndpointContext;
use micro_tunnel::transport::h3::H3Connector;
use micro_tunnel::transport::h3::listener::H3Listener;
use micro_tunnel::transport::{TargetAddr, TransportConnector, TransportListener};

use std::sync::atomic::{AtomicU16, Ordering};

/// Helper to get an available ephemeral port from Suite 7 partition [60500, 65000).
fn get_ephemeral_port() -> u16 {
    static PORT_COUNTER: AtomicU16 = AtomicU16::new(0);
    let base = 60500 + (std::process::id() as u16 % 80) * 50;
    for _ in 0..200 {
        let offset = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let port = base.wrapping_add(offset);
        if (1024..65535).contains(&port)
            && let Ok(u) = std::net::UdpSocket::bind(("127.0.0.1", port))
            && let Ok(t) = std::net::TcpListener::bind(("127.0.0.1", port))
        {
            drop(u);
            drop(t);
            return port;
        }
    }
    panic!("Failed to find an available port in Suite 7 partition [60500, 65000)");
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
async fn test_quic_flow_control_hol_blocking_prevention() {
    let port = get_ephemeral_port();
    let listen_addr = format!("127.0.0.1:{}", port);
    let auth_token = "flow_control_test_token";

    let (server_rustls, _) = generate_test_quic_pki();
    let mut listener =
        H3Listener::bind(&listen_addr, Some(auth_token), None, server_rustls).unwrap();

    let (probe_tx, probe_rx) = oneshot::channel::<Vec<u8>>();

    // Server accepts streams:
    // - Stream 1 & 2 are accepted and held WITHOUT reading (simulating slow consumers/backpressure)
    // - Stream 3 is accepted and reads the probe byte
    let server_task = tokio::spawn(async move {
        let mut held_streams = Vec::new();
        let mut probe_tx_opt = Some(probe_tx);

        for stream_idx in 0..3 {
            let (mut stream, _meta) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    panic!("Server accept error at stream {}: {:?}", stream_idx, e);
                }
            };

            if stream_idx < 2 {
                // Keep stream open, but do NOT read from it!
                held_streams.push(stream);
            } else {
                // Stream 3: read probe bytes
                let mut buf = vec![0u8; 5];
                if stream.read_exact(&mut buf).await.is_ok()
                    && let Some(tx) = probe_tx_opt.take()
                {
                    let _ = tx.send(buf);
                }
            }
        }

        // Keep streams alive until test concludes
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        drop(held_streams);
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

    let target = TargetAddr::Domain("backend.local".to_string(), 80);

    // 1. Open Stream 1 and pour 20MiB of unread data
    let mut stream1 = connector.connect(&target, None).await.unwrap();
    let write_task1 = tokio::spawn(async move {
        let chunk = vec![0xAA; 64 * 1024];
        for _ in 0..320 {
            // 320 * 64KB = 20MiB
            if stream1.write_all(&chunk).await.is_err() {
                break;
            }
        }
    });

    // 2. Open Stream 2 and pour 20MiB of unread data
    let mut stream2 = connector.connect(&target, None).await.unwrap();
    let write_task2 = tokio::spawn(async move {
        let chunk = vec![0xBB; 64 * 1024];
        for _ in 0..320 {
            // 320 * 64KB = 20MiB
            if stream2.write_all(&chunk).await.is_err() {
                break;
            }
        }
    });

    // Brief yield to allow client to fill stream buffers up to flow-control limits
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 3. Open healthy Stream 3 on the same connection and send probe bytes
    let mut stream3 = connector.connect(&target, None).await.unwrap();
    stream3.write_all(b"PROBE").await.unwrap();
    stream3.flush().await.unwrap();

    // 4. Assert probe byte reaches the server within bounded time
    // Under flawed flow control (16MB stream / 32MB conn), connection window is exhausted,
    // causing stream 3 to block and timing out.
    // Under fixed flow control (4MB stream / 128MB conn), connection window has ample space,
    // so probe arrives promptly.
    let probe_res = tokio::time::timeout(std::time::Duration::from_secs(8), probe_rx).await;

    let probe_data = probe_res
        .expect("Probe timed out due to connection-level HOL flow control blocking")
        .expect("Server failed to send probe notification");

    assert_eq!(&probe_data, b"PROBE");

    write_task1.abort();
    write_task2.abort();
    server_task.abort();
}
