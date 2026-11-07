// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Integration tests verifying custom `http_headers` and `Host` override handling across transports.
//! All comments and docstrings must be in English.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use culvert::client::ClientRunner;
use culvert::config::model::{
    AppConfig, EndpointConfig, PortMappingConfig, ServerConfig, TunnelConfig, TunnelMode,
};
use culvert::dialer::endpoint::EndpointContext;
use culvert::server::ServerRunner;
use culvert::session::SessionManager;
use culvert::transport::ws::WsConnector;
use culvert::transport::{TargetAddr, TransportConnector, TransportRegistry};

#[tokio::test]
async fn test_ws_connector_sends_custom_host_and_http_headers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen_port = listener.local_addr().unwrap().port();

    // Spawn server to inspect incoming HTTP Upgrade Request
    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = socket.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).to_string()
    });

    let mut custom_headers = HashMap::new();
    custom_headers.insert("Host".to_string(), "vhost.corporate.internal:8080".to_string());
    custom_headers.insert("User-Agent".to_string(), "culvert-test/2.0".to_string());
    custom_headers.insert("X-Custom-Auth".to_string(), "secret-auth-999".to_string());

    let endpoint_cfg = EndpointConfig::Simple(format!("ws://127.0.0.1:{}/tunnel", listen_port));
    let app_cfg = AppConfig::default();
    let mut endpoint_ctx = EndpointContext::build(&endpoint_cfg, &app_cfg).unwrap();
    endpoint_ctx.merge_http_headers(&custom_headers);

    let connector = WsConnector::new(endpoint_ctx, "", TunnelMode::Forward).unwrap();
    let target = TargetAddr::Ip("127.0.0.1:5201".parse().unwrap());

    // Connect will send the HTTP handshake request. The server won't respond with 101, so connect will timeout/fail,
    // but the request was captured by server_task.
    let _ = connector.connect(&target, None).await;

    let captured_request = server_task.await.unwrap();
    let lower = captured_request.to_lowercase();

    assert!(
        lower.contains("host: vhost.corporate.internal:8080\r\n"),
        "Request must contain overridden Host header. Got:\n{}",
        captured_request
    );
    assert!(
        lower.contains("user-agent: culvert-test/2.0\r\n"),
        "Request must contain custom User-Agent header. Got:\n{}",
        captured_request
    );
    assert!(
        lower.contains("x-custom-auth: secret-auth-999\r\n"),
        "Request must contain custom X-Custom-Auth header. Got:\n{}",
        captured_request
    );
}

#[tokio::test]
async fn test_e2e_ws_tunnel_with_custom_host_and_headers() {
    let cancel_token = CancellationToken::new();

    // 1. Setup echo backend server
    let echo_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_port = echo_listener.local_addr().unwrap().port();
    let echo_cancel = cancel_token.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = echo_cancel.cancelled() => break,
                res = echo_listener.accept() => {
                    if let Ok((mut socket, _)) = res {
                        tokio::spawn(async move {
                            let (mut reader, mut writer) = socket.split();
                            let _ = tokio::io::copy(&mut reader, &mut writer).await;
                        });
                    }
                }
            }
        }
    });

    // 2. Setup culvert server
    let server_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_port = server_listener.local_addr().unwrap().port();
    drop(server_listener);

    let auth_token = "secure_token_abc";
    let registry = Arc::new(TransportRegistry::default());
    let session_mgr = Arc::new(SessionManager::new());

    let server_config = ServerConfig {
        listen: vec![format!("ws://127.0.0.1:{}/tunnel", server_port)],
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
    tokio::spawn(async move {
        let _ = server_runner.run(server_cancel).await;
    });

    // 3. Setup client forward tunnel with custom http_headers
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_port = client_listener.local_addr().unwrap().port();
    drop(client_listener);

    let mut custom_headers = HashMap::new();
    custom_headers.insert("Host".to_string(), "vhost.proxy.internal".to_string());
    custom_headers.insert("X-Custom-Client".to_string(), "my-client-app".to_string());

    let tunnel_config = TunnelConfig {
        tag: "custom-headers-e2e".to_string(),
        endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/tunnel", server_port)),
        token: Some(auth_token.to_string()),
        pool_size: None,
        failover: None,
        multiplex: None,
        retry: None,
        http_headers: Some(custom_headers),
        mappings: vec![PortMappingConfig {
            mode: TunnelMode::Forward,
            listen: format!("127.0.0.1:{}", client_port),
            target: format!("127.0.0.1:{}", echo_port),
        }],
    };

    let client_runner = ClientRunner::new(tunnel_config).unwrap();
    let client_cancel = cancel_token.clone();
    tokio::spawn(async move {
        let _ = client_runner.run(client_cancel).await;
    });

    // Wait for bindings to stabilize
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 4. Connect visitor to client listen port and verify bidirectional echo
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", client_port))
        .await
        .expect("Failed to connect to client forward port");

    let payload = b"Testing custom http_headers end-to-end echo payload!";
    stream.write_all(payload).await.expect("Failed to write to client");

    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut buf))
        .await
        .expect("Timeout waiting for echo response")
        .expect("Failed to read from stream");

    assert_eq!(&buf, payload, "Echo response must match payload exactly");

    cancel_token.cancel();
}
