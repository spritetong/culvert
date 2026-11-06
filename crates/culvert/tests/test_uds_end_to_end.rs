// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! End-to-end integration tests for full-featured UDS support across server, endpoints, and mappings.
//! All comments and docstrings must be in English.

use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use culvert::config::model::*;
use culvert::platform::*;
use culvert::supervisor::TunnelSupervisor;

#[tokio::test]
async fn test_e2e_server_uds_url_and_mappings() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("trace")
        .try_init();
    let tmp = tempdir().unwrap();
    let server_sock = tmp.path().join("server.sock");
    let target_backend_sock = tmp.path().join("backend.sock");
    let forward_listen_sock = tmp.path().join("fwd_listen.sock");

    let server_sock_url = format!("ws+unix://{}", server_sock.display());
    let target_sock_path = format!("unix://{}", target_backend_sock.display());
    let forward_sock_path = format!("unix://{}", forward_listen_sock.display());

    // 1. Start a mock local backend listening on target_backend_sock
    let mut backend_listener = PlatformUdsListener::bind(&target_backend_sock).unwrap();
    let backend_task = tokio::spawn(async move {
        let (mut stream, _) = backend_listener.accept().await.unwrap();
        let mut buf = [0u8; 13];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello backend");
        stream.write_all(b"reply backend").await.unwrap();
        stream.flush().await.unwrap();
    });

    // 2. Configure culvert: server listens on UDS, forward mapping listens on UDS and forwards to backend UDS
    let config = AppConfig {
        log: LogConfig {
            level: "debug".to_string(),
            output: "stdout".to_string(),
            no_color: true,
        },
        worker_threads: Some(2),
        dns: None,
        dialer: None,
        tls: None,
        server: Some(ServerConfig {
            listen: vec![server_sock_url.clone()],
            token: Some("secret123".to_string()),
            default_target: None,
            tls: None,
            certificate: None,
            key: None,
            ..Default::default()
        }),
        tunnels: vec![TunnelConfig {
            tag: "uds-to-uds-forward".to_string(),
            endpoint: EndpointConfig::Simple(server_sock_url),
            token: Some("secret123".to_string()),
            pool_size: Some(2),
            retry: None,
            failover: None,
            multiplex: None,
            mappings: vec![PortMappingConfig {
                mode: TunnelMode::Forward,
                listen: forward_sock_path.clone(),
                target: target_sock_path.clone(),
            }],
        }],
    };

    let cancel_token = CancellationToken::new();
    let supervisor = TunnelSupervisor::new(config, cancel_token.clone());

    let sup_task = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    // Wait for the forward listener socket file to be created
    let mut connected = false;
    for _ in 0..50 {
        if forward_listen_sock.exists()
            && let Ok(mut client) = connect_uds(&forward_listen_sock).await
        {
            client.write_all(b"hello backend").await.unwrap();
            client.flush().await.unwrap();
            let mut reply = [0u8; 13];
            client.read_exact(&mut reply).await.unwrap();
            assert_eq!(&reply, b"reply backend");
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(connected, "Failed to connect and relay through UDS tunnel");

    backend_task.await.unwrap();
    cancel_token.cancel();
    let _ = sup_task.await;
}

#[tokio::test]
async fn test_e2e_reverse_mapping_uds() {
    let tmp = tempdir().unwrap();
    let server_sock = tmp.path().join("server_rev.sock");
    let rev_external_sock = tmp.path().join("rev_external.sock");
    let rev_target_sock = tmp.path().join("rev_target.sock");

    let server_sock_url = format!("ws+unix://{}", server_sock.display());
    let rev_external_str = format!("unix://{}", rev_external_sock.display());
    let rev_target_str = format!("unix://{}", rev_target_sock.display());

    // 1. Start local target backend service for reverse tunnel
    let mut backend_listener = PlatformUdsListener::bind(&rev_target_sock).unwrap();
    let backend_task = tokio::spawn(async move {
        let (mut stream, _) = backend_listener.accept().await.unwrap();
        let mut buf = [0u8; 12];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping reverse");
        stream.write_all(b"pong reverse").await.unwrap();
        stream.flush().await.unwrap();
    });

    // 2. Configure culvert with reverse mapping:
    // Server listens on `rev_external_str` (UDS) and client connects to `rev_target_str` (UDS)
    let config = AppConfig {
        log: LogConfig {
            level: "debug".to_string(),
            output: "stdout".to_string(),
            no_color: true,
        },
        worker_threads: Some(2),
        dns: None,
        dialer: None,
        tls: None,
        server: Some(ServerConfig {
            listen: vec![server_sock_url.clone()],
            token: Some("rev_secret".to_string()),
            default_target: None,
            tls: None,
            certificate: None,
            key: None,
            ..Default::default()
        }),
        tunnels: vec![TunnelConfig {
            tag: "uds-reverse-tunnel".to_string(),
            endpoint: EndpointConfig::Simple(server_sock_url),
            token: Some("rev_secret".to_string()),
            pool_size: None,
            retry: None,
            failover: None,
            multiplex: None,
            mappings: vec![PortMappingConfig {
                mode: TunnelMode::Reverse,
                listen: rev_external_str.clone(),
                target: rev_target_str.clone(),
            }],
        }],
    };

    let cancel_token = CancellationToken::new();
    let supervisor = TunnelSupervisor::new(config, cancel_token.clone());

    let sup_task = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    // 3. Connect to the reverse listen socket on the server
    let mut connected = false;
    for _ in 0..50 {
        if rev_external_sock.exists()
            && let Ok(mut visitor) = connect_uds(&rev_external_sock).await
        {
            visitor.write_all(b"ping reverse").await.unwrap();
            visitor.flush().await.unwrap();
            let mut reply = [0u8; 12];
            visitor.read_exact(&mut reply).await.unwrap();
            assert_eq!(&reply, b"pong reverse");
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        connected,
        "Failed to connect and relay through reverse UDS tunnel"
    );

    backend_task.await.unwrap();
    cancel_token.cancel();
    let _ = sup_task.await;
}

#[tokio::test]
async fn test_e2e_server_ws_unix_with_query_path() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("trace")
        .try_init();
    let temp_dir = tempfile::tempdir().unwrap();
    let server_sock = temp_dir.path().join("server_custom.sock");
    let target_sock = temp_dir.path().join("target_backend.sock");
    let fwd_listen_sock = temp_dir.path().join("fwd_listen.sock");

    let server_sock_str = server_sock.to_string_lossy().to_string();
    let target_sock_str = format!("unix://{}", target_sock.display());
    let fwd_listen_str = format!("unix://{}", fwd_listen_sock.display());

    // 1. Target backend UDS service
    let mut target_listener = PlatformUdsListener::bind(&target_sock).unwrap();
    let backend_task = tokio::spawn(async move {
        if let Ok((mut stream, _)) = target_listener.accept().await {
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
            stream.write_all(b"pong").await.unwrap();
            stream.flush().await.unwrap();
        }
    });

    // 2. Client connects with ws+unix scheme and query path: ws+unix:///path?path=/custom-path
    let server_listen_url = format!(
        "ws+unix://{}?path=/custom-path&chmod=0o660",
        server_sock_str
    );
    let client_endpoint = format!("ws+unix://{}?path=/custom-path", server_sock_str);

    let config = AppConfig {
        log: LogConfig {
            level: "debug".to_string(),
            output: "stdout".to_string(),
            no_color: true,
        },
        worker_threads: Some(2),
        dns: None,
        dialer: None,
        tls: None,
        server: Some(ServerConfig {
            listen: vec![server_listen_url],
            token: Some("token123".to_string()),
            default_target: None,
            tls: None,
            certificate: None,
            key: None,
            ..Default::default()
        }),
        tunnels: vec![TunnelConfig {
            tag: "ws-unix-tunnel".to_string(),
            endpoint: EndpointConfig::Simple(client_endpoint),
            token: Some("token123".to_string()),
            pool_size: Some(2),
            retry: None,
            failover: None,
            multiplex: None,
            mappings: vec![PortMappingConfig {
                mode: TunnelMode::Forward,
                listen: fwd_listen_str.clone(),
                target: target_sock_str.clone(),
            }],
        }],
    };

    let cancel_token = CancellationToken::new();
    let supervisor = TunnelSupervisor::new(config, cancel_token.clone());

    let sup_task = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    // Connect to local forward UDS listener which dials through UDS tunnel to UDS target
    let mut connected = false;
    for _ in 0..50 {
        if fwd_listen_sock.exists()
            && let Ok(mut visitor) = connect_uds(&fwd_listen_sock).await
            && visitor.write_all(b"ping").await.is_ok()
            && visitor.flush().await.is_ok()
        {
            let mut reply = [0u8; 4];
            if visitor.read_exact(&mut reply).await.is_ok() && &reply == b"pong" {
                connected = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        connected,
        "Failed to connect and relay through UDS tunnel with query path"
    );

    backend_task.await.unwrap();
    cancel_token.cancel();
    let _ = sup_task.await;
}
