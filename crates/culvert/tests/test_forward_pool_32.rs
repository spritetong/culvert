// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Regression test for forward pool idle-stream lifetime.
//! All comments and docstrings must be in English.

use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use culvert::TunnelSupervisor;
use culvert::config::model::{
    AppConfig, DialerConfig, EndpointConfig, MappingRuleConfig, ServerConfig, TlsConfig,
    TunnelConfig, TunnelMappingsConfig,
};

const IDLE_TIMEOUT: Duration = Duration::from_secs(12);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const TEST_WINDOW: Duration = Duration::from_secs(20);

fn get_available_port() -> u16 {
    for _ in 0..100 {
        let t = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
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

fn spawn_echo_server(cancel: CancellationToken) -> (tokio::task::JoinHandle<()>, u16) {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = std_listener.local_addr().unwrap().port();
    std_listener.set_nonblocking(true).unwrap();
    let listener = TcpListener::from_std(std_listener).unwrap();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                Ok((mut socket, _)) = listener.accept() => {
                    tokio::spawn(async move {
                        let mut buf = [0u8; 4096];
                        while let Ok(n) = socket.read(&mut buf).await {
                            if n == 0 { break; }
                            if socket.write_all(&buf[..n]).await.is_err() { break; }
                        }
                    });
                }
            }
        }
    });
    (handle, port)
}

#[tokio::test]
async fn test_forward_pool_idle_streams_survive_asymmetric_timeout() {
    let cancel = CancellationToken::new();
    let server_port = get_available_port();
    let client_listen_port = get_available_port();
    let (echo, echo_port) = spawn_echo_server(cancel.clone());

    let url = format!("quic://127.0.0.1:{server_port}/raw");
    let mut config = AppConfig {
        tls: Some(TlsConfig {
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        server: Some(ServerConfig {
            listen: vec![url.clone()],
            token: Some("secret_token".to_string()),
            // Deliberately shorter than the client window: the server must not
            // close a waiting pooled stream on its own idle deadline.
            idle_timeout: Some(Duration::from_secs(4)),
            ..Default::default()
        }),
        ..Default::default()
    };
    config.tunnels.push(TunnelConfig {
        tag: "pool32-quic".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Detailed {
            url,
            dns: None,
            dialer: Some(DialerConfig {
                idle_timeout: Some(IDLE_TIMEOUT),
                heartbeat_interval: Some(HEARTBEAT_INTERVAL),
                ..Default::default()
            }),
            tls: None,
            http_headers: None,
        },
        token: Some("secret_token".to_string()),
        failover: None,
        multiplex: None,
        retry: None,
        http_headers: None,
        mappings: TunnelMappingsConfig::forward_pool(
            32,
            vec![MappingRuleConfig::tcp(
                format!("127.0.0.1:{client_listen_port}"),
                format!("127.0.0.1:{echo_port}"),
            )],
        ),
    });

    let supervisor = TunnelSupervisor::new(config, cancel.clone());
    let sup = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });
    let started = Instant::now();

    tokio::time::sleep(Duration::from_secs(2)).await;
    let tcp = TcpStream::connect(format!("127.0.0.1:{client_listen_port}"))
        .await
        .expect("Client should connect");

    let chunk = vec![0x42u8; 64 * 1024];
    let chunks = 20;
    let (mut reader, mut writer) = tcp.into_split();
    let write_task = tokio::spawn(async move {
        for _ in 0..chunks {
            writer.write_all(&chunk).await.unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        writer.shutdown().await.unwrap();
    });

    let mut received = 0usize;
    let mut buf = vec![0u8; 64 * 1024];
    while let Ok(n) = reader.read(&mut buf).await {
        if n == 0 {
            break;
        }
        received += n;
    }
    write_task.await.unwrap();
    assert_eq!(received, chunks * 64 * 1024);

    if let Some(remaining) = TEST_WINDOW.checked_sub(started.elapsed()) {
        tokio::time::sleep(remaining).await;
    }

    // Subsequent connection after asymmetric idle window must still succeed cleanly.
    let mut tcp2 = TcpStream::connect(format!("127.0.0.1:{client_listen_port}"))
        .await
        .expect("Post-window client should connect");
    tcp2.write_all(b"post-window-check").await.unwrap();
    tcp2.shutdown().await.unwrap();
    let mut tail = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), tcp2.read_to_end(&mut tail))
        .await
        .expect("Post-window read timed out")
        .unwrap();
    assert_eq!(&tail, b"post-window-check");

    cancel.cancel();
    let _ = tokio::join!(echo, sup);
}
