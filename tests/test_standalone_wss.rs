//! Test standalone direct WSS termination with self-signed certificate fallback.
//! All comments and docstrings must be in English.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use micro_tunnel::config::model::{
    AppConfig, EndpointConfig, PortMappingConfig, ServerConfig, TlsConfig, TunnelConfig, TunnelMode,
};
use micro_tunnel::supervisor::TunnelSupervisor;

fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn spawn_echo_server(
    addr: SocketAddr,
    cancel_token: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();
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
    })
}

#[tokio::test]
async fn test_standalone_wss_server_self_signed_direct_termination() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("trace")
        .try_init();
    let cancel_token = CancellationToken::new();

    let echo_port = get_available_port();
    let server_wss_port = get_available_port();
    let client_listen_port = get_available_port();

    let echo_addr: SocketAddr = format!("127.0.0.1:{}", echo_port).parse().unwrap();
    let echo_handle = spawn_echo_server(echo_addr, cancel_token.clone());

    // 1. Configure Server with standalone wss:// without providing cert/key files -> auto self-signed
    let mut app_config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("wss://127.0.0.1:{}/tunnel", server_wss_port)],
            token: Some("secret_wss_token".to_string()),
            default_target: None,
            certificate: None, // No file provided: triggers self-signed fallback
            key: None,
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
                enabled: true,
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
        mode: None,
        listen: None,
        target: None,
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
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, payload);

    // 5. Clean shutdown
    cancel_token.cancel();
    let _ = tokio::join!(echo_handle, sup_handle);
}
