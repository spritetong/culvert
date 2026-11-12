// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Comprehensive integration tests for WSS (WebSocket over TLS 1.3 / Rustls).
//! Covers SNI override, self-signed/insecure mode, custom CA verification, and mTLS.
//! All comments and docstrings must be in English.

use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig as RustlsServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

use culvert::config::model::{
    AppConfig, EndpointConfig, PortMappingConfig, ServerConfig, TlsConfig, TunnelConfig, TunnelMode,
};
use culvert::dialer::endpoint::EndpointContext;
use culvert::server::ServerRunner;
use culvert::session::SessionManager;
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

/// In-memory or temporary PKI credentials bundle generated via rcgen.
#[allow(dead_code)]
struct TestPki {
    _temp_dir: tempfile_dummy::TempDir,
    ca_cert_path: PathBuf,
    server_cert_path: PathBuf,
    server_key_path: PathBuf,
    client_cert_path: PathBuf,
    client_key_path: PathBuf,
    server_rustls_config: Arc<RustlsServerConfig>,
    mtls_server_rustls_config: Arc<RustlsServerConfig>,
}

mod tempfile_dummy {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static PKI_COUNTER: AtomicU64 = AtomicU64::new(1);

    pub struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        pub fn new() -> Self {
            let id = PKI_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("culvert_pki_{}_{}", std::process::id(), id));
            let _ = std::fs::create_dir_all(&path);
            Self { path }
        }

        pub fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

/// Generates a test CA, a server certificate signed by CA, and a client certificate for mTLS.
fn generate_test_pki() -> TestPki {
    let temp_dir = tempfile_dummy::TempDir::new();
    let dir = temp_dir.path();

    // 1. Generate Root CA
    let mut ca_params = CertificateParams::default();
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, "Culvert Root CA");
    ca_params.distinguished_name = ca_dn;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca = rcgen::CertifiedIssuer::self_signed(ca_params, &ca_key).unwrap();

    let ca_cert_pem = ca.as_ref().pem();
    let ca_cert_path = dir.join("ca.crt");
    fs::write(&ca_cert_path, &ca_cert_pem).unwrap();

    // 2. Generate Server Certificate signed by Root CA
    let mut server_params = CertificateParams::default();
    let mut server_dn = DistinguishedName::new();
    server_dn.push(DnType::CommonName, "tunnel.secure.io");
    server_params.distinguished_name = server_dn;
    server_params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into().unwrap()),
        SanType::DnsName("tunnel.secure.io".try_into().unwrap()),
        SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))),
    ];
    let server_key = KeyPair::generate().unwrap();
    let server_cert = server_params.signed_by(&server_key, &ca).unwrap();

    let server_cert_pem = server_cert.pem();
    let server_key_pem = server_key.serialize_pem();
    let server_cert_path = dir.join("server.crt");
    let server_key_path = dir.join("server.key");
    fs::write(&server_cert_path, &server_cert_pem).unwrap();
    fs::write(&server_key_path, &server_key_pem).unwrap();

    // 3. Generate Client Certificate for mTLS
    let mut client_params = CertificateParams::default();
    let mut client_dn = DistinguishedName::new();
    client_dn.push(DnType::CommonName, "test-client");
    client_params.distinguished_name = client_dn;
    let client_key = KeyPair::generate().unwrap();
    let client_cert = client_params.signed_by(&client_key, &ca).unwrap();

    let client_cert_pem = client_cert.pem();
    let client_key_pem = client_key.serialize_pem();
    let client_cert_path = dir.join("client.crt");
    let client_key_path = dir.join("client.key");
    fs::write(&client_cert_path, &client_cert_pem).unwrap();
    fs::write(&client_key_path, &client_key_pem).unwrap();

    // Server rustls config without client auth
    let server_cert_der = CertificateDer::from(server_cert.der().to_vec());
    let server_key_der = PrivateKeyDer::try_from(server_key.serialize_der()).unwrap();

    let server_rustls_config = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![server_cert_der.clone()], server_key_der.clone_key())
        .unwrap();

    // Server rustls config with mTLS required
    let mut client_roots = RootCertStore::empty();
    let ca_cert_der = CertificateDer::from(ca.as_ref().der().to_vec());
    client_roots.add(ca_cert_der).unwrap();

    let client_verifier = WebPkiClientVerifier::builder(Arc::new(client_roots))
        .build()
        .unwrap();
    let mtls_server_rustls_config = RustlsServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(vec![server_cert_der], server_key_der)
        .unwrap();

    TestPki {
        _temp_dir: temp_dir,
        ca_cert_path,
        server_cert_path,
        server_key_path,
        client_cert_path,
        client_key_path,
        server_rustls_config: Arc::new(server_rustls_config),
        mtls_server_rustls_config: Arc::new(mtls_server_rustls_config),
    }
}

/// Spawns a TLS-terminating server ingress proxy (simulating Caddy or direct TLS terminator)
/// using held-listener handoff to eliminate TOCTOU.
fn spawn_tls_terminator(
    backend_plain_addr: SocketAddr,
    tls_config: Arc<RustlsServerConfig>,
    cancel_token: CancellationToken,
) -> (tokio::task::JoinHandle<()>, u16) {
    let acceptor = TlsAcceptor::from(tls_config);
    let std_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind TLS terminator");
    let port = std_listener.local_addr().unwrap().port();
    std_listener.set_nonblocking(true).unwrap();
    let listener = TcpListener::from_std(std_listener).unwrap();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                res = listener.accept() => {
                    let (tcp_stream, _) = match res {
                        Ok(conn) => conn,
                        Err(_) => break,
                    };
                    let acceptor = acceptor.clone();
                    let backend_plain_addr = backend_plain_addr;
                    tokio::spawn(async move {
                        let tls_stream = match acceptor.accept(tcp_stream).await {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::warn!("TLS handshake rejected by server: {:?}", e);
                                return;
                            }
                        };
                        let backend_conn = match TcpStream::connect(backend_plain_addr).await {
                            Ok(s) => s,
                            Err(_) => return,
                        };
                        let (mut tls_rd, mut tls_wr) = tokio::io::split(tls_stream);
                        let (mut be_rd, mut be_wr) = backend_conn.into_split();
                        let (res1, res2) = tokio::join!(
                            tokio::io::copy(&mut tls_rd, &mut be_wr),
                            tokio::io::copy(&mut be_rd, &mut tls_wr)
                        );
                        if let Err(e) = res1 {
                            tracing::debug!("TLS terminator to backend copy finished: {:?}", e);
                        }
                        if let Err(e) = res2 {
                            tracing::debug!("Backend to TLS terminator copy finished: {:?}", e);
                        }
                    });
                }
            }
        }
    });
    (handle, port)
}

/// Spawns a simple TCP echo server using held-listener handoff to eliminate TOCTOU.
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
async fn test_wss_forward_tunnel_with_custom_ca_and_sni() {
    let cancel_token = CancellationToken::new();
    let pki = generate_test_pki();

    let backend_server_port = get_available_port();
    let client_listen_port = get_available_port();

    let backend_addr: SocketAddr = format!("127.0.0.1:{}", backend_server_port)
        .parse()
        .unwrap();
    let auth_token = "secure_wss_test_token";

    // 1. Echo server with held-listener handoff
    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());

    // 2. Culvert ServerRunner (backend plain listener)
    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://127.0.0.1:{}/tunnel", backend_server_port)],
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

    // 3. TLS Terminator with held-listener handoff
    let (tls_handle, tls_ingress_port) = spawn_tls_terminator(
        backend_addr,
        pki.server_rustls_config.clone(),
        cancel_token.clone(),
    );

    tokio::time::sleep(Duration::from_millis(150)).await;

    // 4. Client forward tunnel connecting via wss:// with custom CA and SNI override
    let tunnel_config = TunnelConfig {
        tag: "test-wss-ca-sni".to_string(),
        uuid: None,
        pool: None,
        token: Some(auth_token.to_string()),
        pool_size: None,
        failover: None,
        multiplex: None,
        mappings: vec![PortMappingConfig {
            mode: TunnelMode::Forward,
            listen: format!("127.0.0.1:{}", client_listen_port),
            target: format!("127.0.0.1:{}", echo_port),
            ..Default::default()
        }],
        retry: None,
        http_headers: None,
        endpoint: EndpointConfig::Detailed {
            url: format!("wss://127.0.0.1:{}/tunnel", tls_ingress_port),
            dns: None,
            dialer: None,
            tls: Some(TlsConfig {
                server_name: Some("tunnel.secure.io".to_string()),
                insecure: Some(false), // Strict CA validation!
                ca_certificate: Some(pki.ca_cert_path.clone()),
                ..Default::default()
            }),
            http_headers: None,
        },
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ =
            run_forward_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });

    // 5. Connect to client listen port with retry loop and verify bidirectional echo
    let mut stream = None;
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(format!("127.0.0.1:{}", client_listen_port)).await {
            stream = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut stream = stream.expect("Failed to connect client listen port after retries");

    let payload = b"Hello encrypted WSS tunnel with strict CA and SNI validation!";
    stream.write_all(payload).await.unwrap();

    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut buf))
        .await
        .expect("Timeout reading WSS echo")
        .unwrap();
    assert_eq!(&buf, payload, "Echo mismatch over TLS 1.3 WSS");

    // Cleanup
    cancel_token.cancel();
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            let (r1, r2, r3, r4) = tokio::join!(echo_handle, server_handle, tls_handle, client_handle);
            r1.expect("echo_handle panicked");
            r2.expect("server_handle panicked");
            r3.expect("tls_handle panicked");
            r4.expect("client_handle panicked");
        },
    )
    .await
    .expect("Timeout waiting for tasks to exit after cancellation in test_wss_forward_tunnel_with_custom_ca_and_sni");
}

#[tokio::test]
async fn test_wss_forward_tunnel_insecure_mode() {
    let cancel_token = CancellationToken::new();
    let pki = generate_test_pki();

    let backend_server_port = get_available_port();
    let client_listen_port = get_available_port();

    let backend_addr: SocketAddr = format!("127.0.0.1:{}", backend_server_port)
        .parse()
        .unwrap();
    let auth_token = "insecure_wss_test_token";

    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://127.0.0.1:{}/tunnel", backend_server_port)],
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

    let (tls_handle, tls_ingress_port) = spawn_tls_terminator(
        backend_addr,
        pki.server_rustls_config.clone(),
        cancel_token.clone(),
    );

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Client connects with insecure: true (without CA cert)
    let tunnel_config = TunnelConfig {
        tag: "test-wss-insecure".to_string(),
        uuid: None,
        pool: None,
        token: Some(auth_token.to_string()),
        pool_size: None,
        failover: None,
        multiplex: None,
        mappings: vec![PortMappingConfig {
            mode: TunnelMode::Forward,
            listen: format!("127.0.0.1:{}", client_listen_port),
            target: format!("127.0.0.1:{}", echo_port),
            ..Default::default()
        }],
        retry: None,
        http_headers: None,
        endpoint: EndpointConfig::Detailed {
            url: format!("wss://127.0.0.1:{}/tunnel", tls_ingress_port),
            dns: None,
            dialer: None,
            tls: Some(TlsConfig {
                server_name: Some("untrusted.host".to_string()),
                insecure: Some(true), // Should skip all cert validation!
                ..Default::default()
            }),
            http_headers: None,
        },
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ =
            run_forward_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });

    let mut stream = None;
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(format!("127.0.0.1:{}", client_listen_port)).await {
            stream = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut stream = stream.expect("Failed to connect client listen port after retries");

    let payload = b"Testing insecure mode TLS bypass!";
    stream.write_all(payload).await.unwrap();

    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut buf))
        .await
        .expect("Timeout reading insecure WSS echo")
        .unwrap();
    assert_eq!(&buf, payload);

    cancel_token.cancel();
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            let (r1, r2, r3, r4) = tokio::join!(echo_handle, server_handle, tls_handle, client_handle);
            r1.expect("echo_handle panicked");
            r2.expect("server_handle panicked");
            r3.expect("tls_handle panicked");
            r4.expect("client_handle panicked");
        },
    )
    .await
    .expect("Timeout waiting for tasks to exit after cancellation in test_wss_forward_tunnel_insecure_mode");
}

#[tokio::test]
async fn test_wss_mtls_mutual_authentication() {
    let cancel_token = CancellationToken::new();
    let pki = generate_test_pki();

    let backend_server_port = get_available_port();
    let client_listen_port = get_available_port();

    let backend_addr: SocketAddr = format!("127.0.0.1:{}", backend_server_port)
        .parse()
        .unwrap();
    let auth_token = "mtls_wss_test_token";

    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://127.0.0.1:{}/tunnel", backend_server_port)],
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

    // Server requires mTLS with Root CA validation
    let (tls_handle, tls_ingress_port) = spawn_tls_terminator(
        backend_addr,
        pki.mtls_server_rustls_config.clone(),
        cancel_token.clone(),
    );

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Client configures client_certificate and client_key
    let tunnel_config = TunnelConfig {
        tag: "test-wss-mtls".to_string(),
        uuid: None,
        pool: None,
        token: Some(auth_token.to_string()),
        pool_size: None,
        failover: None,
        multiplex: None,
        mappings: vec![PortMappingConfig {
            mode: TunnelMode::Forward,
            listen: format!("127.0.0.1:{}", client_listen_port),
            target: format!("127.0.0.1:{}", echo_port),
            ..Default::default()
        }],
        retry: None,
        http_headers: None,
        endpoint: EndpointConfig::Detailed {
            url: format!("wss://127.0.0.1:{}/tunnel", tls_ingress_port),
            dns: None,
            dialer: None,
            tls: Some(TlsConfig {
                server_name: Some("localhost".to_string()),
                insecure: Some(false),
                ca_certificate: Some(pki.ca_cert_path.clone()),
                client_certificate: Some(pki.client_cert_path.clone()),
                client_key: Some(pki.client_key_path.clone()),
                ..Default::default()
            }),
            http_headers: None,
        },
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ =
            run_forward_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });

    let mut stream = None;
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(format!("127.0.0.1:{}", client_listen_port)).await {
            stream = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut stream = stream.expect("Failed to connect client listen port after retries");

    let payload = b"mTLS bidirectional authentication succeeded!";
    stream.write_all(payload).await.unwrap();

    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut buf))
        .await
        .expect("Timeout reading mTLS echo")
        .unwrap();
    assert_eq!(&buf, payload);

    cancel_token.cancel();
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            let (r1, r2, r3, r4) = tokio::join!(echo_handle, server_handle, tls_handle, client_handle);
            r1.expect("echo_handle panicked");
            r2.expect("server_handle panicked");
            r3.expect("tls_handle panicked");
            r4.expect("client_handle panicked");
        },
    )
    .await
    .expect("Timeout waiting for tasks to exit after cancellation in test_wss_mtls_mutual_authentication");
}

#[tokio::test]
async fn test_wss_reverse_tunnel_e2e() {
    let cancel_token = CancellationToken::new();
    let pki = generate_test_pki();

    let backend_server_port = get_available_port();
    let reverse_listen_port = get_available_port();

    let backend_addr: SocketAddr = format!("127.0.0.1:{}", backend_server_port)
        .parse()
        .unwrap();
    let auth_token = "wss_reverse_secret_token";

    // 1. Local Echo Server with held-listener handoff
    let (echo_handle, local_echo_port) = spawn_echo_server(cancel_token.clone());

    // 2. Culvert ServerRunner
    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://127.0.0.1:{}/tunnel", backend_server_port)],
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

    // 3. TLS Terminator with held-listener handoff
    let (tls_handle, tls_ingress_port) = spawn_tls_terminator(
        backend_addr,
        pki.server_rustls_config.clone(),
        cancel_token.clone(),
    );

    tokio::time::sleep(Duration::from_millis(150)).await;

    // 4. Reverse tunnel client over WSS with CA cert
    let tunnel_config = TunnelConfig {
        tag: "test-wss-reverse".to_string(),
        uuid: None,
        pool: None,
        token: Some(auth_token.to_string()),
        pool_size: None,
        failover: None,
        multiplex: None,
        mappings: vec![PortMappingConfig {
            mode: TunnelMode::Reverse,
            listen: format!("127.0.0.1:{}", reverse_listen_port),
            target: format!("127.0.0.1:{}", local_echo_port),
            ..Default::default()
        }],
        retry: None,
        http_headers: None,
        endpoint: EndpointConfig::Detailed {
            url: format!("wss://127.0.0.1:{}/tunnel", tls_ingress_port),
            dns: None,
            dialer: None,
            tls: Some(TlsConfig {
                server_name: Some("localhost".to_string()),
                insecure: Some(false),
                ca_certificate: Some(pki.ca_cert_path.clone()),
                ..Default::default()
            }),
            http_headers: None,
        },
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    let client_handle = tokio::spawn(async move {
        let _ =
            run_reverse_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // 5. External visitor connects to reverse_listen_port on server with retry loop
    let mut visitor = None;
    for _ in 0..50 {
        if let Ok(stream) = TcpStream::connect(format!("127.0.0.1:{}", reverse_listen_port)).await {
            visitor = Some(stream);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut visitor =
        visitor.expect("Failed to connect reverse listener port on server after retries");

    let payload = b"Reverse tunnel over encrypted TLS 1.3 WebSocket!";
    visitor.write_all(payload).await.unwrap();

    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(Duration::from_secs(5), visitor.read_exact(&mut buf))
        .await
        .expect("Timeout reading reverse tunnel echo")
        .unwrap();
    assert_eq!(&buf, payload, "Echo mismatch on reverse WSS tunnel");

    cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2, r3, r4) = tokio::join!(echo_handle, server_handle, tls_handle, client_handle);
        r1.expect("echo_handle panicked");
        r2.expect("server_handle panicked");
        r3.expect("tls_handle panicked");
        r4.expect("client_handle panicked");
    })
    .await
    .expect("Timeout waiting for tasks to exit after cancellation in test_wss_reverse_tunnel_e2e");
}

#[tokio::test]
async fn test_wss_server_tls_version_enforcement() {
    let cancel_token = CancellationToken::new();
    let pki = generate_test_pki();
    let server_port = get_available_port();

    let mut reg = TransportRegistry::new();
    reg.register(WsTransportDriver);
    let registry = Arc::new(reg);
    let session_mgr = Arc::new(SessionManager::new());

    // 1. Configure Server to strictly enforce TLS 1.3
    let server_config = ServerConfig {
        listen: vec![format!("wss://127.0.0.1:{}/tunnel", server_port)],
        tls: Some(TlsConfig {
            certificate: Some(pki.server_cert_path.clone()),
            key: Some(pki.server_key_path.clone()),
            min_version: Some("1.3".to_string()),
            ..Default::default()
        }),
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

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 2. Client restricted to TLS 1.2 only must fail to handshake
    let client_tls_12 = TlsConfig {
        server_name: Some("localhost".to_string()),
        ca_certificate: Some(pki.ca_cert_path.clone()),
        max_version: Some("1.2".to_string()),
        ..Default::default()
    };
    let app_cfg = AppConfig::default();
    let ep_cfg = EndpointConfig::Detailed {
        url: format!("wss://127.0.0.1:{}/tunnel", server_port),
        dns: None,
        dialer: None,
        tls: Some(client_tls_12),
        http_headers: None,
    };
    let ep_ctx = EndpointContext::build(&ep_cfg, &app_cfg).unwrap();
    let connector_12 = registry
        .create_connector(&ep_ctx, None, TunnelMode::Forward)
        .unwrap();

    let target = culvert::transport::TargetAddr::Domain("127.0.0.1".into(), 80);
    let connect_res_12 = connector_12.connect(&target, None).await;
    assert!(
        connect_res_12.is_err(),
        "Server enforcing TLS 1.3 must reject client capped at TLS 1.2"
    );

    // 3. Client allowing TLS 1.3 must succeed
    let client_tls_13 = TlsConfig {
        server_name: Some("localhost".to_string()),
        ca_certificate: Some(pki.ca_cert_path.clone()),
        min_version: Some("1.3".to_string()),
        ..Default::default()
    };
    let ep_cfg_13 = EndpointConfig::Detailed {
        url: format!("wss://127.0.0.1:{}/tunnel", server_port),
        dns: None,
        dialer: None,
        tls: Some(client_tls_13),
        http_headers: None,
    };
    let ep_ctx_13 = EndpointContext::build(&ep_cfg_13, &app_cfg).unwrap();
    let connector_13 = registry
        .create_connector(&ep_ctx_13, None, TunnelMode::Forward)
        .unwrap();

    let connect_res_13 = connector_13.connect(&target, None).await;
    assert!(
        connect_res_13.is_ok(),
        "Client connecting with TLS 1.3 must succeed"
    );

    cancel_token.cancel();
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            let (r,) = tokio::join!(server_handle);
            r.expect("server_handle panicked");
        },
    )
    .await
    .expect("Timeout waiting for server handle to exit after cancellation in test_wss_server_tls_version_enforcement");
}

#[tokio::test]
async fn test_wss_server_inherits_global_tls_config() {
    let cancel_token = CancellationToken::new();
    let pki = generate_test_pki();
    let server_port = get_available_port();

    let mut reg = TransportRegistry::new();
    reg.register(WsTransportDriver);
    let registry = Arc::new(reg);
    let session_mgr = Arc::new(SessionManager::new());

    // 1. Configure Server with server.tls = None, but supply top-level global TLS
    let server_config = ServerConfig {
        listen: vec![format!("wss://127.0.0.1:{}/tunnel", server_port)],
        tls: None, // server.tls is None
        ..Default::default()
    };

    let global_tls = TlsConfig {
        certificate: Some(pki.server_cert_path.clone()),
        key: Some(pki.server_key_path.clone()),
        min_version: Some("1.3".to_string()),
        ..Default::default()
    };

    let server_runner = ServerRunner::with_global_tls(
        server_config,
        Some(global_tls),
        Arc::clone(&registry),
        Arc::clone(&session_mgr),
    );
    let s_cancel = cancel_token.clone();
    let server_handle = tokio::spawn(async move {
        let _ = server_runner.run(s_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 2. Client restricted to TLS 1.2 only must fail because server inherited min_version: 1.3
    let client_tls_12 = TlsConfig {
        server_name: Some("localhost".to_string()),
        ca_certificate: Some(pki.ca_cert_path.clone()),
        max_version: Some("1.2".to_string()),
        ..Default::default()
    };
    let app_cfg = AppConfig::default();
    let ep_cfg = EndpointConfig::Detailed {
        url: format!("wss://127.0.0.1:{}/tunnel", server_port),
        dns: None,
        dialer: None,
        tls: Some(client_tls_12),
        http_headers: None,
    };
    let ep_ctx = EndpointContext::build(&ep_cfg, &app_cfg).unwrap();
    let connector_12 = registry
        .create_connector(&ep_ctx, None, TunnelMode::Forward)
        .unwrap();

    let target = culvert::transport::TargetAddr::Domain("127.0.0.1".into(), 80);
    let connect_res_12 = connector_12.connect(&target, None).await;
    assert!(
        connect_res_12.is_err(),
        "Server inheriting global TLS must reject client capped at TLS 1.2"
    );

    // 3. Client using TLS 1.3 must succeed
    let client_tls_13 = TlsConfig {
        server_name: Some("localhost".to_string()),
        ca_certificate: Some(pki.ca_cert_path.clone()),
        min_version: Some("1.3".to_string()),
        ..Default::default()
    };
    let ep_cfg_13 = EndpointConfig::Detailed {
        url: format!("wss://127.0.0.1:{}/tunnel", server_port),
        dns: None,
        dialer: None,
        tls: Some(client_tls_13),
        http_headers: None,
    };
    let ep_ctx_13 = EndpointContext::build(&ep_cfg_13, &app_cfg).unwrap();
    let connector_13 = registry
        .create_connector(&ep_ctx_13, None, TunnelMode::Forward)
        .unwrap();

    let connect_res_13 = connector_13.connect(&target, None).await;
    assert!(
        connect_res_13.is_ok(),
        "Server inheriting global TLS must accept client with TLS 1.3"
    );

    cancel_token.cancel();
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            let (r,) = tokio::join!(server_handle);
            r.expect("server_handle panicked");
        },
    )
    .await
    .expect("Timeout waiting for server handle to exit after cancellation in test_wss_server_inherits_global_tls_config");
}
