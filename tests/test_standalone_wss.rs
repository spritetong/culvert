// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Test standalone direct WSS termination with self-signed certificate fallback.
//! All comments and docstrings must be in English.

use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use micro_tunnel::config::model::{
    AppConfig, EndpointConfig, PortMappingConfig, ServerConfig, TlsConfig, TunnelConfig, TunnelMode,
};
use micro_tunnel::supervisor::TunnelSupervisor;

/// Helper to get an available random ephemeral port from Suite 3 partition [34000, 39500).
fn get_available_port() -> u16 {
    static PORT_COUNTER: AtomicU16 = AtomicU16::new(0);
    for _ in 0..1000 {
        let offset = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let port = 34000 + (offset % 5500);
        if let Ok(listener) = std::net::TcpListener::bind(("127.0.0.1", port)) {
            drop(listener);
            return port;
        }
    }
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn spawn_echo_server(cancel_token: CancellationToken) -> (tokio::task::JoinHandle<()>, u16) {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = std_listener.local_addr().unwrap().port();
    std_listener.set_nonblocking(true).unwrap();
    let listener = TcpListener::from_std(std_listener).unwrap();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                res = listener.accept() => {
                    if let Ok((mut socket, _)) = res {
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
        }
    });
    (handle, port)
}

#[tokio::test]
async fn test_standalone_wss_server_self_signed_direct_termination() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("trace")
        .try_init();
    let cancel_token = CancellationToken::new();

    let server_wss_port = get_available_port();
    let client_listen_port = get_available_port();

    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());

    // 1. Configure Server with standalone wss:// without providing cert/key files -> auto self-signed
    let mut app_config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("wss://127.0.0.1:{}/tunnel", server_wss_port)],
            token: Some("secret_wss_token".to_string()),
            default_target: None,
            certificate: None, // No file provided: triggers self-signed fallback
            key: None,
            tls: None,
        }),
        ..Default::default()
    };

    // 2. Configure Client with wss:// and insecure: true to connect directly to the standalone WSS server
    app_config.tunnels.push(TunnelConfig {
        tag: "direct-wss-tunnel".to_string(),
        endpoint: EndpointConfig::Detailed {
            url: format!("wss://127.0.0.1:{}/tunnel", server_wss_port),
            dns: None,
            dialer: None,
            tls: Some(TlsConfig {
                insecure: true, // Accepts self-signed cert
                server_name: Some("localhost".to_string()),
                ..Default::default()
            }),
        },
        token: Some("secret_wss_token".to_string()),
        pool_size: None,
        failover: None,
        multiplex: None,
        retry: None,
        mappings: vec![PortMappingConfig {
            mode: TunnelMode::Forward,
            listen: format!("127.0.0.1:{}", client_listen_port),
            target: format!("127.0.0.1:{}", echo_port),
        }],
    });

    // 3. Launch supervisor
    let supervisor = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    tokio::time::sleep(Duration::from_millis(350)).await;

    // 4. Verify end-to-end traffic through standalone TLS WSS termination
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", client_listen_port))
        .await
        .expect("Should connect to client forward port");

    let payload = b"Hello Standalone WSS Direct TLS Termination!";
    stream.write_all(payload).await.unwrap();

    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut buf))
        .await
        .expect("Timeout reading standalone WSS echo")
        .unwrap();
    assert_eq!(&buf, payload);

    // 5. Clean shutdown
    cancel_token.cancel();
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            let (r1, r2) = tokio::join!(echo_handle, sup_handle);
            r1.expect("echo_handle panicked");
            r2.expect("sup_handle panicked");
        },
    )
    .await
    .expect("Timeout waiting for echo and supervisor handles to terminate in test_standalone_wss_server_self_signed_direct_termination");
}
