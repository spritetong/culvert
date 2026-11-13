// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! End-to-end integration tests for client tunnels operating through HTTP CONNECT and SOCKS5 proxies.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use culvert::config::model::{
    AppConfig, DialerConfig, EndpointConfig, MappingRuleConfig, ServerConfig, TunnelConfig,
    TunnelMappingsConfig,
};
use culvert::dialer::endpoint::EndpointContext;
use culvert::server::ServerRunner;
use culvert::session::SessionManager;
use culvert::transport::TransportRegistry;
use culvert::tunnel::run_forward_tunnel;

#[tokio::test]
async fn test_forward_tunnel_over_http_proxy() {
    let cancel_token = CancellationToken::new();

    // 1. Setup backend TCP echo server
    let echo_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    let echo_cancel = cancel_token.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = echo_cancel.cancelled() => {},
            res = echo_listener.accept() => {
                if let Ok((mut socket, _)) = res {
                    let mut buf = [0u8; 1024];
                    while let Ok(n) = socket.read(&mut buf).await {
                        if n == 0 { break; }
                        if socket.write_all(&buf[..n]).await.is_err() { break; }
                    }
                }
            }
        }
    });

    // 2. Setup culvert server
    let server_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server_listener.local_addr().unwrap();
    drop(server_listener); // release port for culvert server bind

    let registry = Arc::new(TransportRegistry::default());
    let session_mgr = Arc::new(SessionManager::new());
    let server_config = ServerConfig {
        listen: vec![format!("ws://{}/tunnel", server_addr)],
        token: Some("secret-token".to_string()),
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

    // 3. Setup mock HTTP CONNECT proxy
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();
    let proxy_invoked = Arc::new(AtomicBool::new(false));
    let proxy_invoked_clone = Arc::clone(&proxy_invoked);

    let proxy_cancel = cancel_token.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = proxy_cancel.cancelled() => {},
            res = proxy_listener.accept() => {
                if let Ok((mut client_sock, _)) = res {
                    let mut req = Vec::new();
                    let mut byte = [0u8; 1];
                    while req.len() < 2048 {
                        if client_sock.read_exact(&mut byte).await.is_err() { return; }
                        req.push(byte[0]);
                        if req.ends_with(b"\r\n\r\n") { break; }
                    }
                    let req_str = String::from_utf8_lossy(&req);
                    if req_str.starts_with(&format!("CONNECT 127.0.0.1:{} HTTP/1.1", server_addr.port())) {
                        proxy_invoked_clone.store(true, Ordering::SeqCst);
                    }
                    let _ = client_sock.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n").await;
                    if let Ok(mut target_sock) = TcpStream::connect(server_addr).await {
                        let _ = tokio::io::copy_bidirectional(&mut client_sock, &mut target_sock).await;
                    }
                }
            }
        }
    });

    // 4. Setup client forward tunnel through proxy
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = client_listener.local_addr().unwrap();
    drop(client_listener);

    let tunnel_config = TunnelConfig {
        tag: "proxy-forward-tunnel".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Detailed {
            url: format!("ws://127.0.0.1:{}/tunnel", server_addr.port()),
            dns: None,
            dialer: Some(DialerConfig {
                proxy: Some(format!("http://{}", proxy_addr)),
                ..Default::default()
            }),
            tls: None,
            http_headers: None,
        },
        token: Some("secret-token".to_string()),
        failover: None,
        multiplex: None,
        mappings: TunnelMappingsConfig::forward(vec![MappingRuleConfig::tcp(
            format!("127.0.0.1:{}", client_addr.port()),
            format!("127.0.0.1:{}", echo_addr.port()),
        )]),
        retry: None,
        http_headers: None,
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    tokio::spawn(async move {
        let _ =
            run_forward_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 5. Connect visitor to client listen port and verify traffic roundtrip
    let mut visitor = TcpStream::connect(client_addr).await.unwrap();
    let msg = b"ping via http proxy";
    visitor.write_all(msg).await.unwrap();

    let mut buf = vec![0u8; msg.len()];
    tokio::time::timeout(Duration::from_secs(5), visitor.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&buf, msg);
    assert!(
        proxy_invoked.load(Ordering::SeqCst),
        "Proxy was not invoked during dial!"
    );

    cancel_token.cancel();
}

#[tokio::test]
async fn test_forward_tunnel_over_socks5_proxy() {
    let cancel_token = CancellationToken::new();

    // 1. Setup backend TCP echo server
    let echo_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    let echo_cancel = cancel_token.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = echo_cancel.cancelled() => {},
            res = echo_listener.accept() => {
                if let Ok((mut socket, _)) = res {
                    let mut buf = [0u8; 1024];
                    while let Ok(n) = socket.read(&mut buf).await {
                        if n == 0 { break; }
                        if socket.write_all(&buf[..n]).await.is_err() { break; }
                    }
                }
            }
        }
    });

    // 2. Setup culvert server
    let server_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server_listener.local_addr().unwrap();
    drop(server_listener);

    let registry = Arc::new(TransportRegistry::default());
    let session_mgr = Arc::new(SessionManager::new());
    let server_config = ServerConfig {
        listen: vec![format!("ws://{}/tunnel", server_addr)],
        token: Some("secret-token".to_string()),
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

    // 3. Setup mock SOCKS5 proxy
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();
    let proxy_invoked = Arc::new(AtomicBool::new(false));
    let proxy_invoked_clone = Arc::clone(&proxy_invoked);

    let proxy_cancel = cancel_token.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = proxy_cancel.cancelled() => {},
            res = proxy_listener.accept() => {
                if let Ok((mut client_sock, _)) = res {
                    // Method greeting: [0x05, 0x01, 0x00]
                    let mut greeting = [0u8; 3];
                    if client_sock.read_exact(&mut greeting).await.is_err() { return; }
                    let _ = client_sock.write_all(&[0x05, 0x00]).await;

                    // Connect request: [0x05, 0x01, 0x00, ATYP, ...]
                    let mut req_header = [0u8; 4];
                    if client_sock.read_exact(&mut req_header).await.is_err() { return; }
                    if req_header[0] == 0x05 && req_header[1] == 0x01 {
                        proxy_invoked_clone.store(true, Ordering::SeqCst);
                    }
                    if req_header[3] == 0x01 {
                        // IPv4: 4 bytes IP + 2 bytes port
                        let mut buf = [0u8; 6];
                        let _ = client_sock.read_exact(&mut buf).await;
                    } else if req_header[3] == 0x03 {
                        // Domain
                        let mut len = [0u8; 1];
                        let _ = client_sock.read_exact(&mut len).await;
                        let mut domain = vec![0u8; len[0] as usize + 2];
                        let _ = client_sock.read_exact(&mut domain).await;
                    }

                    // Success response: [0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0]
                    let _ = client_sock.write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0]).await;

                    if let Ok(mut target_sock) = TcpStream::connect(server_addr).await {
                        let _ = tokio::io::copy_bidirectional(&mut client_sock, &mut target_sock).await;
                    }
                }
            }
        }
    });

    // 4. Setup client forward tunnel through SOCKS5 proxy
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = client_listener.local_addr().unwrap();
    drop(client_listener);

    let tunnel_config = TunnelConfig {
        tag: "socks5-forward-tunnel".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Detailed {
            url: format!("ws://127.0.0.1:{}/tunnel", server_addr.port()),
            dns: None,
            dialer: Some(DialerConfig {
                proxy: Some(format!("socks5://{}", proxy_addr)),
                ..Default::default()
            }),
            tls: None,
            http_headers: None,
        },
        token: Some("secret-token".to_string()),
        failover: None,
        multiplex: None,
        mappings: TunnelMappingsConfig::forward(vec![MappingRuleConfig::tcp(
            format!("127.0.0.1:{}", client_addr.port()),
            format!("127.0.0.1:{}", echo_addr.port()),
        )]),
        retry: None,
        http_headers: None,
    };

    let app_config = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&tunnel_config.endpoint, &app_config).unwrap();
    let client_cancel = cancel_token.clone();
    let client_registry = Arc::clone(&registry);
    tokio::spawn(async move {
        let _ =
            run_forward_tunnel(tunnel_config, endpoint_ctx, client_registry, client_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 5. Connect visitor to client listen port and verify traffic roundtrip
    let mut visitor = TcpStream::connect(client_addr).await.unwrap();
    let msg = b"ping via socks5 proxy";
    visitor.write_all(msg).await.unwrap();

    let mut buf = vec![0u8; msg.len()];
    tokio::time::timeout(Duration::from_secs(5), visitor.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&buf, msg);
    assert!(
        proxy_invoked.load(Ordering::SeqCst),
        "SOCKS5 Proxy was not invoked during dial!"
    );

    cancel_token.cancel();
}
