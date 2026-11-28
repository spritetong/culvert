// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Concurrent reverse clients against one server.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use culvert::TunnelSupervisor;
use culvert::config::model::{
    AppConfig, EndpointConfig, MappingRuleConfig, ServerConfig, TunnelConfig, TunnelMappingsConfig,
};

fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn spawn_tagged_echo_server(tag: &'static str, cancel: CancellationToken) -> (JoinHandle<()>, u16) {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("echo bind");
    let port = std_listener.local_addr().unwrap().port();
    std_listener.set_nonblocking(true).unwrap();
    let listener = TcpListener::from_std(std_listener).unwrap();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                res = listener.accept() => {
                    let Ok((mut socket, _)) = res else { continue };
                    tokio::spawn(async move {
                        let mut buf = [0u8; 4096];
                        let Ok(n) = socket.read(&mut buf).await else { return };
                        if n == 0 { return }
                        let mut response = tag.as_bytes().to_vec();
                        response.extend_from_slice(&buf[..n]);
                        if socket.write_all(&response).await.is_ok() {
                            let _ = socket.shutdown().await;
                        }
                    });
                }
            }
        }
    });
    (handle, port)
}

fn client_config(uuid: &str, server_port: u16, listen_port: u16, target_port: u16) -> AppConfig {
    AppConfig {
        tunnels: vec![TunnelConfig {
            tag: format!("reverse-client-{}", uuid),
            uuid: Some(uuid.to_string()),
            endpoint: EndpointConfig::Simple(format!("ws://127.0.0.1:{}/reverse", server_port)),
            token: Some("reverse-secret".to_string()),
            failover: None,
            multiplex: None,
            retry: None,
            http_headers: None,
            mappings: TunnelMappingsConfig::reverse_pool(
                3,
                vec![MappingRuleConfig::tcp(
                    format!("127.0.0.1:{}", listen_port),
                    format!("127.0.0.1:{}", target_port),
                )],
            ),
        }],
        ..Default::default()
    }
}

fn spawn_supervisor(config: AppConfig, cancel: CancellationToken) -> JoinHandle<()> {
    let supervisor = TunnelSupervisor::new(config, cancel);
    tokio::spawn(async move {
        let _ = supervisor.run().await;
    })
}

async fn tagged_echo(port: u16, payload: &[u8]) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect reverse listener");
    stream.write_all(payload).await.unwrap();
    stream.shutdown().await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .expect("reverse echo timeout")
        .expect("reverse echo read");
    String::from_utf8(response).expect("utf8 echo")
}

async fn start_server(root: &CancellationToken) -> (u16, JoinHandle<()>) {
    let port = get_available_port();
    let config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("ws://127.0.0.1:{}/reverse", port)],
            token: Some("reverse-secret".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let handle = spawn_supervisor(config, root.child_token());
    tokio::time::sleep(Duration::from_millis(200)).await;
    (port, handle)
}

#[tokio::test]
async fn test_multiple_reverse_clients_with_distinct_uuids_are_stable() {
    let root = CancellationToken::new();
    let (echo_a, echo_a_port) = spawn_tagged_echo_server("A:", root.child_token());
    let (echo_b, echo_b_port) = spawn_tagged_echo_server("B:", root.child_token());
    let (server_port, server) = start_server(&root).await;

    let rev_a_port = get_available_port();
    let rev_b_port = get_available_port();
    let client_a_token = root.child_token();
    let client_b_token = root.child_token();
    let client_a = spawn_supervisor(
        client_config("client-a", server_port, rev_a_port, echo_a_port),
        client_a_token.clone(),
    );
    let client_b = spawn_supervisor(
        client_config("client-b", server_port, rev_b_port, echo_b_port),
        client_b_token.clone(),
    );
    tokio::time::sleep(Duration::from_millis(800)).await;

    for i in 0..10 {
        let payload = format!("A{}", i);
        assert_eq!(
            tagged_echo(rev_a_port, payload.as_bytes()).await,
            format!("A:{}", payload)
        );
        let payload = format!("B{}", i);
        assert_eq!(
            tagged_echo(rev_b_port, payload.as_bytes()).await,
            format!("B:{}", payload)
        );
    }

    client_a_token.cancel();
    tokio::time::timeout(Duration::from_secs(10), client_a)
        .await
        .expect("client A shutdown timeout")
        .expect("client A task panicked");

    for i in 0..5 {
        let payload = format!("B-after-A-stopped-{}", i);
        assert_eq!(
            tagged_echo(rev_b_port, payload.as_bytes()).await,
            format!("B:{}", payload)
        );
    }

    root.cancel();
    let (r_echo_a, r_echo_b, r_client_b, r_server) =
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(echo_a, echo_b, client_b, server)
        })
        .await
        .expect("shutdown timeout");
    r_echo_a.expect("echo A task panicked");
    r_echo_b.expect("echo B task panicked");
    r_client_b.expect("client B task panicked");
    r_server.expect("server task panicked");
}

#[tokio::test]
async fn test_reverse_clients_sharing_uuid_keep_mapping_targets() {
    let root = CancellationToken::new();
    let (echo_a, echo_a_port) = spawn_tagged_echo_server("A:", root.child_token());
    let (echo_b, echo_b_port) = spawn_tagged_echo_server("B:", root.child_token());
    let (server_port, server) = start_server(&root).await;

    let rev_a_port = get_available_port();
    let rev_b_port = get_available_port();
    let client_a = spawn_supervisor(
        client_config("shared-uuid", server_port, rev_a_port, echo_a_port),
        root.child_token(),
    );
    let client_b = spawn_supervisor(
        client_config("shared-uuid", server_port, rev_b_port, echo_b_port),
        root.child_token(),
    );
    tokio::time::sleep(Duration::from_millis(800)).await;

    for i in 0..10 {
        let payload = format!("A{}", i);
        assert_eq!(
            tagged_echo(rev_a_port, payload.as_bytes()).await,
            format!("A:{}", payload)
        );
        let payload = format!("B{}", i);
        assert_eq!(
            tagged_echo(rev_b_port, payload.as_bytes()).await,
            format!("B:{}", payload)
        );
    }

    root.cancel();
    let (r_echo_a, r_echo_b, r_client_a, r_client_b, r_server) =
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(echo_a, echo_b, client_a, client_b, server)
        })
        .await
        .expect("shutdown timeout");
    r_echo_a.expect("echo A task panicked");
    r_echo_b.expect("echo B task panicked");
    r_client_a.expect("client A task panicked");
    r_client_b.expect("client B task panicked");
    r_server.expect("server task panicked");
}
