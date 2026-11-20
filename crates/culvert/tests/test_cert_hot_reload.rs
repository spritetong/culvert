// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Integration tests for dynamic TLS certificate monitoring and hot-reloading.
//! Verifies zero-downtime certificate rotation on disk, fallback on invalid files,
//! and mTLS client CA hot-reloading.
//! All comments and docstrings must be in English.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, SanType};
use tokio_util::sync::CancellationToken;

use culvert::config::model::{
    AppConfig, ClientAuthMode, EndpointConfig, ServerConfig, TlsConfig, TunnelMode,
};
use culvert::dialer::endpoint::EndpointContext;
use culvert::server::ServerRunner;
use culvert::session::SessionManager;
use culvert::transport::ws::WsTransportDriver;
use culvert::transport::TransportRegistry;

fn get_available_port() -> u16 {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

struct TestPki {
    _dir: tempfile::TempDir,
    ca_cert_path: PathBuf,
    server_cert_path: PathBuf,
    server_key_path: PathBuf,
    ca_key_pem: String,
}

fn generate_pki() -> TestPki {
    let dir = tempfile::tempdir().unwrap();

    // 1. Root CA
    let mut ca_params = CertificateParams::default();
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, "Culvert HotReload Root CA");
    ca_params.distinguished_name = ca_dn;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca = rcgen::CertifiedIssuer::self_signed(ca_params, &ca_key).unwrap();

    let ca_cert_pem = ca.as_ref().pem();
    let ca_cert_path = dir.path().join("ca.crt");
    let ca_key_pem = ca_key.serialize_pem();
    fs::write(&ca_cert_path, ca_cert_pem).unwrap();

    // 2. Initial Server Certificate (Cert 1)
    let mut srv_params = CertificateParams::default();
    let mut srv_dn = DistinguishedName::new();
    srv_dn.push(DnType::CommonName, "localhost");
    srv_params.distinguished_name = srv_dn;
    srv_params
        .subject_alt_names
        .push(SanType::DnsName("localhost".try_into().unwrap()));
    srv_params
        .subject_alt_names
        .push(SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::new(127, 0, 0, 1),
        )));

    let srv_key = KeyPair::generate().unwrap();
    let srv_cert = srv_params.signed_by(&srv_key, &ca).unwrap();

    let server_cert_path = dir.path().join("server.crt");
    let server_key_path = dir.path().join("server.key");

    fs::write(&server_cert_path, srv_cert.pem()).unwrap();
    fs::write(&server_key_path, srv_key.serialize_pem()).unwrap();

    TestPki {
        _dir: dir,
        ca_cert_path,
        server_cert_path,
        server_key_path,
        ca_key_pem,
    }
}

fn issue_new_server_cert(pki: &TestPki) -> (String, String) {
    let mut ca_params = CertificateParams::default();
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, "Culvert HotReload Root CA");
    ca_params.distinguished_name = ca_dn;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = KeyPair::from_pem(&pki.ca_key_pem).unwrap();
    let ca = rcgen::CertifiedIssuer::self_signed(ca_params, &ca_key).unwrap();

    let mut srv_params = CertificateParams::default();
    let mut srv_dn = DistinguishedName::new();
    srv_dn.push(DnType::CommonName, "localhost-v2");
    srv_params.distinguished_name = srv_dn;
    srv_params
        .subject_alt_names
        .push(SanType::DnsName("localhost".try_into().unwrap()));
    srv_params
        .subject_alt_names
        .push(SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::new(127, 0, 0, 1),
        )));

    let srv_key = KeyPair::generate().unwrap();
    let srv_cert = srv_params.signed_by(&srv_key, &ca).unwrap();
    (srv_cert.pem(), srv_key.serialize_pem())
}

#[tokio::test]
async fn test_server_dynamic_cert_hot_reload() {
    let cancel_token = CancellationToken::new();
    let pki = generate_pki();
    let server_port = get_available_port();
    let auth_token = "hot_reload_token";

    // Start Culvert Server with native TLS
    let mut server_registry = TransportRegistry::new();
    server_registry.register(WsTransportDriver);
    let server_registry = Arc::new(server_registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("wss://127.0.0.1:{}/tunnel", server_port)],
        token: Some(auth_token.to_string()),
        default_target: None,
        certificate: Some(pki.server_cert_path.to_str().unwrap().to_string()),
        key: Some(pki.server_key_path.to_str().unwrap().to_string()),
        ..Default::default()
    };

    let server_runner = ServerRunner::new(
        server_config,
        Arc::clone(&server_registry),
        Arc::clone(&session_mgr),
    );

    let cancel_server = cancel_token.child_token();
    let server_handle = tokio::spawn(async move {
        server_runner.run(cancel_server).await.unwrap();
    });

    // Wait for server to bind
    tokio::time::sleep(Duration::from_millis(200)).await;

    let client_tls = TlsConfig {
        server_name: Some("localhost".to_string()),
        ca_certificate: Some(pki.ca_cert_path.clone()),
        ..Default::default()
    };
    let ep_cfg = EndpointConfig::Detailed {
        url: format!("wss://127.0.0.1:{}/tunnel", server_port),
        dns: None,
        dialer: None,
        tls: Some(client_tls.clone()),
        http_headers: None,
    };
    let app_cfg = AppConfig::default();
    let ep_ctx = EndpointContext::build(&ep_cfg, &app_cfg).unwrap();
    let connector = server_registry
        .create_connector(&ep_ctx, Some(auth_token), TunnelMode::Forward)
        .unwrap();

    let target = culvert::transport::TargetAddr::Domain("127.0.0.1".into(), 80);

    // 1. Initial connection with Cert 1
    let conn1 = connector.connect(&target, None).await;
    assert!(
        conn1.is_ok(),
        "Client 1 should successfully connect with initial certificate"
    );

    // 2. Overwrite certificate and key with Cert 2 on disk
    let (cert2_pem, key2_pem) = issue_new_server_cert(&pki);
    fs::write(&pki.server_cert_path, &cert2_pem).unwrap();
    fs::write(&pki.server_key_path, &key2_pem).unwrap();

    // Wait for debounce window (default 1000ms) to elapse and trigger reload
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // 3. Connect Client 2 after reload
    let conn2 = connector.connect(&target, None).await;
    assert!(
        conn2.is_ok(),
        "Client 2 should successfully connect after certificate hot-reload"
    );

    // 4. Overwrite key with invalid data to simulate partial write or corrupt file
    fs::write(&pki.server_key_path, "INVALID CORRUPT PRIVATE KEY").unwrap();

    // Wait for debounce window
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // 5. Connect Client 3: server must still be functioning using the active valid certificate!
    let conn3 = connector.connect(&target, None).await;
    assert!(
        conn3.is_ok(),
        "Client 3 must still successfully connect: corrupt disk files must not break the active certificate"
    );

    cancel_token.cancel();
    let _ = server_handle.await;
}

fn issue_client_cert(ca_key_pem: &str, ca_cn: &str, client_cn: &str) -> (String, String) {
    let mut ca_params = CertificateParams::default();
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, ca_cn);
    ca_params.distinguished_name = ca_dn;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = KeyPair::from_pem(ca_key_pem).unwrap();
    let ca = rcgen::CertifiedIssuer::self_signed(ca_params, &ca_key).unwrap();

    let mut client_params = CertificateParams::default();
    let mut client_dn = DistinguishedName::new();
    client_dn.push(DnType::CommonName, client_cn);
    client_params.distinguished_name = client_dn;

    let client_key = KeyPair::generate().unwrap();
    let client_cert = client_params.signed_by(&client_key, &ca).unwrap();
    (client_cert.pem(), client_key.serialize_pem())
}

fn generate_second_ca() -> (String, String) {
    let mut ca_params = CertificateParams::default();
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, "Culvert Second CA");
    ca_params.distinguished_name = ca_dn;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca = rcgen::CertifiedIssuer::self_signed(ca_params, &ca_key).unwrap();
    (ca.as_ref().pem(), ca_key.serialize_pem())
}

#[tokio::test]
async fn test_server_dynamic_client_ca_hot_reload() {
    let cancel_token = CancellationToken::new();
    let pki = generate_pki();
    let server_port = get_available_port();
    let auth_token = "mtls_hot_reload_token";

    // Client A cert signed by CA 1
    let (client_a_cert, client_a_key) =
        issue_client_cert(&pki.ca_key_pem, "Culvert HotReload Root CA", "client-a");
    let client_a_cert_path = pki._dir.path().join("client_a.crt");
    let client_a_key_path = pki._dir.path().join("client_a.key");
    fs::write(&client_a_cert_path, &client_a_cert).unwrap();
    fs::write(&client_a_key_path, &client_a_key).unwrap();

    // Client B cert signed by CA 2
    let (ca2_cert, ca2_key) = generate_second_ca();
    let (client_b_cert, client_b_key) =
        issue_client_cert(&ca2_key, "Culvert Second CA", "client-b");
    let client_b_cert_path = pki._dir.path().join("client_b.crt");
    let client_b_key_path = pki._dir.path().join("client_b.key");
    fs::write(&client_b_cert_path, &client_b_cert).unwrap();
    fs::write(&client_b_key_path, &client_b_key).unwrap();

    // Server CA file initially only trusts CA 1
    let client_ca_path = pki._dir.path().join("trusted_clients_ca.crt");
    let ca1_cert_pem = fs::read_to_string(&pki.ca_cert_path).unwrap();
    fs::write(&client_ca_path, &ca1_cert_pem).unwrap();

    // Start Culvert Server with native TLS & Mandatory mTLS
    let mut server_registry = TransportRegistry::new();
    server_registry.register(WsTransportDriver);
    let server_registry = Arc::new(server_registry);
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("wss://127.0.0.1:{}/tunnel", server_port)],
        token: Some(auth_token.to_string()),
        default_target: None,
        certificate: Some(pki.server_cert_path.to_str().unwrap().to_string()),
        key: Some(pki.server_key_path.to_str().unwrap().to_string()),
        client_ca_certificate: Some(client_ca_path.to_str().unwrap().to_string()),
        client_auth: Some(culvert::config::model::ClientAuthMode::Required),
        ..Default::default()
    };

    let server_runner = ServerRunner::new(
        server_config,
        Arc::clone(&server_registry),
        Arc::clone(&session_mgr),
    );

    let cancel_server = cancel_token.child_token();
    let server_handle = tokio::spawn(async move {
        server_runner.run(cancel_server).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    let target = culvert::transport::TargetAddr::Domain("127.0.0.1".into(), 80);
    let app_cfg = AppConfig::default();

    // Connector for Client A (signed by CA 1)
    let client_a_tls = TlsConfig {
        server_name: Some("localhost".to_string()),
        ca_certificate: Some(pki.ca_cert_path.clone()),
        client_certificate: Some(client_a_cert_path),
        client_key: Some(client_a_key_path),
        ..Default::default()
    };
    let ep_a = EndpointConfig::Detailed {
        url: format!("wss://127.0.0.1:{}/tunnel", server_port),
        dns: None,
        dialer: None,
        tls: Some(client_a_tls),
        http_headers: None,
    };
    let ep_ctx_a = EndpointContext::build(&ep_a, &app_cfg).unwrap();
    let connector_a = server_registry
        .create_connector(&ep_ctx_a, Some(auth_token), TunnelMode::Forward)
        .unwrap();

    // Connector for Client B (signed by CA 2)
    let client_b_tls = TlsConfig {
        server_name: Some("localhost".to_string()),
        ca_certificate: Some(pki.ca_cert_path.clone()),
        client_certificate: Some(client_b_cert_path),
        client_key: Some(client_b_key_path),
        ..Default::default()
    };
    let ep_b = EndpointConfig::Detailed {
        url: format!("wss://127.0.0.1:{}/tunnel", server_port),
        dns: None,
        dialer: None,
        tls: Some(client_b_tls),
        http_headers: None,
    };
    let ep_ctx_b = EndpointContext::build(&ep_b, &app_cfg).unwrap();
    let connector_b = server_registry
        .create_connector(&ep_ctx_b, Some(auth_token), TunnelMode::Forward)
        .unwrap();

    // 1. Client A connects -> Must succeed
    let conn_a = connector_a.connect(&target, None).await;
    if let Err(ref e) = conn_a {
        eprintln!("[DEBUG client A error] {:?}", e);
    }
    assert!(conn_a.is_ok(), "Client A (signed by CA 1) must succeed initially");

    // 2. Client B connects -> Must fail (CA 2 not trusted yet)
    let conn_b = connector_b.connect(&target, None).await;
    assert!(conn_b.is_err(), "Client B (signed by CA 2) must fail initially");

    // 3. Update client_ca_path to include both CA 1 and CA 2
    let combined_ca = format!("{}\n{}", ca1_cert_pem, ca2_cert);
    fs::write(&client_ca_path, combined_ca).unwrap();

    // Wait for debounce window (default 1000ms) to reload
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // 4. Client B connects again -> Must now succeed!
    let conn_b2 = connector_b.connect(&target, None).await;
    if let Err(ref e) = conn_b2 {
        eprintln!("[DEBUG client B2 error] {:?}", e);
    }
    assert!(conn_b2.is_ok(), "Client B must succeed after CA 2 is added to trusted CAs");

    // 5. Client A connects again -> Must still succeed!
    let conn_a2 = connector_a.connect(&target, None).await;
    assert!(conn_a2.is_ok(), "Client A must still succeed after CA list update");

    cancel_token.cancel();
    let _ = server_handle.await;
}

#[tokio::test]
async fn test_optional_client_auth_allows_anonymous_client() {
    let cancel_token = CancellationToken::new();
    let pki = generate_pki();
    let server_port = get_available_port();
    let auth_token = "optional_mtls_token";

    let mut server_registry = TransportRegistry::new();
    server_registry.register(WsTransportDriver);
    let server_registry = Arc::new(server_registry);

    let server_config = ServerConfig {
        listen: vec![format!("wss://127.0.0.1:{}/tunnel", server_port)],
        token: Some(auth_token.to_string()),
        certificate: Some(pki.server_cert_path.to_str().unwrap().to_string()),
        key: Some(pki.server_key_path.to_str().unwrap().to_string()),
        client_ca_certificate: Some(pki.ca_cert_path.to_str().unwrap().to_string()),
        client_auth: Some(ClientAuthMode::Optional),
        ..Default::default()
    };

    let server_runner = ServerRunner::new(
        server_config,
        Arc::clone(&server_registry),
        Arc::new(SessionManager::new()),
    );
    let cancel_server = cancel_token.child_token();
    let server_handle = tokio::spawn(async move {
        server_runner.run(cancel_server).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Anonymous client: trusts the server CA but presents no client certificate.
    let tls = TlsConfig {
        server_name: Some("localhost".to_string()),
        ca_certificate: Some(pki.ca_cert_path.clone()),
        ..Default::default()
    };
    let ep = EndpointConfig::Detailed {
        url: format!("wss://127.0.0.1:{}/tunnel", server_port),
        dns: None,
        dialer: None,
        tls: Some(tls),
        http_headers: None,
    };
    let ep_ctx = EndpointContext::build(&ep, &AppConfig::default()).unwrap();
    let anonymous = server_registry
        .create_connector(&ep_ctx, Some(auth_token), TunnelMode::Forward)
        .unwrap();
    let target = culvert::transport::TargetAddr::Domain("127.0.0.1".into(), 80);

    assert!(
        anonymous.connect(&target, None).await.is_ok(),
        "Optional mTLS must accept a client that presents no certificate"
    );

    cancel_token.cancel();
    let _ = server_handle.await;
}
