// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Dedicated test reproducing forward pool_size 32 idle stream lifecycle and eviction diagnosis.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use culvert::config::model::{
    AppConfig, EndpointConfig, MappingRuleConfig, ServerConfig, TlsConfig, TunnelConfig,
    TunnelMappingsConfig,
};
use culvert::supervisor::TunnelSupervisor;

fn get_available_port() -> u16 {
    for _ in 0..100 {
        let t =
            std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind ephemeral TCP port");
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

struct LogCounterLayer {
    evicted_dead_count: Arc<AtomicUsize>,
    evicted_expired_count: Arc<AtomicUsize>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LogCounterLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct MessageVisitor<'a> {
            message: &'a mut String,
            count: &'a mut usize,
        }
        impl tracing::field::Visit for MessageVisitor<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    use std::fmt::Write;
                    let _ = write!(self.message, "{:?}", value);
                }
            }
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                if field.name() == "count" {
                    *self.count = value as usize;
                }
            }
        }

        let mut msg = String::new();
        let mut count = 1usize;
        event.record(&mut MessageVisitor {
            message: &mut msg,
            count: &mut count,
        });
        if msg.contains("Evicted dead/hanging stream") {
            self.evicted_dead_count.fetch_add(count, Ordering::SeqCst);
            eprintln!("[INTERCEPTED EVIC_DEAD count={}] {}", count, msg);
        } else if msg.contains("Evicted expired idle stream") {
            self.evicted_expired_count
                .fetch_add(count, Ordering::SeqCst);
            eprintln!("[INTERCEPTED EVIC_EXPIRED count={}] {}", count, msg);
        }
    }
}

#[tokio::test]
async fn test_forward_pool_32_wss_idle_eviction_diagnosis() {
    let evicted_dead = Arc::new(AtomicUsize::new(0));
    let evicted_expired = Arc::new(AtomicUsize::new(0));

    let counter_layer = LogCounterLayer {
        evicted_dead_count: Arc::clone(&evicted_dead),
        evicted_expired_count: Arc::clone(&evicted_expired),
    };

    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(counter_layer)
        .with(tracing_subscriber::EnvFilter::new("culvert=debug"))
        .try_init();

    let cancel_token = CancellationToken::new();
    let server_port = get_available_port();
    let client_listen_port = get_available_port();
    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());

    let mut app_config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("wss://127.0.0.1:{}/tunnel", server_port)],
            token: Some("secret_token".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };

    app_config.tunnels.push(TunnelConfig {
        tag: "pool32-wss".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Detailed {
            url: format!("wss://127.0.0.1:{}/tunnel", server_port),
            dns: None,
            dialer: None,
            tls: Some(TlsConfig {
                insecure: Some(true),
                server_name: Some("localhost".to_string()),
                ..Default::default()
            }),
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
                format!("127.0.0.1:{}", client_listen_port),
                format!("127.0.0.1:{}", echo_port),
            )],
        ),
    });

    let supervisor = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    // Wait 3 seconds for initial pool replenishment to reach 32
    tokio::time::sleep(Duration::from_secs(3)).await;

    let initial_dead = evicted_dead.load(Ordering::SeqCst);
    let initial_expired = evicted_expired.load(Ordering::SeqCst);
    eprintln!(
        "=== AFTER 3 SECONDS: initial_dead={}, initial_expired={} ===",
        initial_dead, initial_expired
    );

    // Keep running for 10 more seconds to observe if evictions happen
    for sec in 1..=10 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let cur_dead = evicted_dead.load(Ordering::SeqCst);
        let cur_expired = evicted_expired.load(Ordering::SeqCst);
        eprintln!(
            "=== SEC {}: dead={}, expired={} ===",
            sec + 3,
            cur_dead,
            cur_expired
        );
    }

    cancel_token.cancel();
    let _ = tokio::join!(echo_handle, sup_handle);
}

#[tokio::test]
async fn test_forward_pool_32_quic_idle_eviction_diagnosis() {
    let evicted_dead = Arc::new(AtomicUsize::new(0));
    let evicted_expired = Arc::new(AtomicUsize::new(0));

    let counter_layer = LogCounterLayer {
        evicted_dead_count: Arc::clone(&evicted_dead),
        evicted_expired_count: Arc::clone(&evicted_expired),
    };

    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(counter_layer)
        .with(tracing_subscriber::EnvFilter::new("culvert=debug"))
        .try_init();

    let cancel_token = CancellationToken::new();
    let server_port = get_available_port();
    let client_listen_port = get_available_port();
    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());

    let mut app_config = AppConfig {
        tls: Some(TlsConfig {
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        server: Some(ServerConfig {
            listen: vec![format!("quic://127.0.0.1:{}/raw", server_port)],
            token: Some("secret_token".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };

    app_config.tunnels.push(TunnelConfig {
        tag: "pool32-quic".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Simple(format!("quic://127.0.0.1:{}/raw", server_port)),
        token: Some("secret_token".to_string()),
        failover: None,
        multiplex: None,
        retry: None,
        http_headers: None,
        mappings: TunnelMappingsConfig::forward_pool(
            32,
            vec![MappingRuleConfig::tcp(
                format!("127.0.0.1:{}", client_listen_port),
                format!("127.0.0.1:{}", echo_port),
            )],
        ),
    });

    let supervisor = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    // Wait 3 seconds for initial pool replenishment to reach 32
    tokio::time::sleep(Duration::from_secs(3)).await;

    let initial_dead = evicted_dead.load(Ordering::SeqCst);
    let initial_expired = evicted_expired.load(Ordering::SeqCst);
    eprintln!(
        "=== [QUIC] AFTER 3 SECONDS: initial_dead={}, initial_expired={} ===",
        initial_dead, initial_expired
    );

    // Keep running for 10 more seconds to observe if evictions happen
    for sec in 1..=10 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let cur_dead = evicted_dead.load(Ordering::SeqCst);
        let cur_expired = evicted_expired.load(Ordering::SeqCst);
        eprintln!(
            "=== [QUIC] SEC {}: dead={}, expired={} ===",
            sec + 3,
            cur_dead,
            cur_expired
        );
    }

    cancel_token.cancel();
    let _ = tokio::join!(echo_handle, sup_handle);
}

#[tokio::test]
async fn test_forward_pool_32_quic_with_large_transfer_and_no_spurious_dead_evictions() {
    let evicted_dead = Arc::new(AtomicUsize::new(0));

    let counter_layer = LogCounterLayer {
        evicted_dead_count: Arc::clone(&evicted_dead),
        evicted_expired_count: Arc::new(AtomicUsize::new(0)),
    };

    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(counter_layer)
        .with(tracing_subscriber::EnvFilter::new("culvert=debug"))
        .try_init();

    let cancel_token = CancellationToken::new();
    let server_port = get_available_port();
    let client_listen_port = get_available_port();
    let (echo_handle, echo_port) = spawn_echo_server(cancel_token.clone());

    let mut app_config = AppConfig {
        tls: Some(TlsConfig {
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        server: Some(ServerConfig {
            listen: vec![format!("quic://127.0.0.1:{}/raw", server_port)],
            token: Some("secret_token".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };

    app_config.tunnels.push(TunnelConfig {
        tag: "pool32-quic-active".to_string(),
        uuid: None,
        endpoint: EndpointConfig::Simple(format!("quic://127.0.0.1:{}/raw", server_port)),
        token: Some("secret_token".to_string()),
        failover: None,
        multiplex: None,
        retry: None,
        http_headers: None,
        mappings: TunnelMappingsConfig::forward_pool(
            32,
            vec![MappingRuleConfig::tcp(
                format!("127.0.0.1:{}", client_listen_port),
                format!("127.0.0.1:{}", echo_port),
            )],
        ),
    });

    let supervisor = TunnelSupervisor::new(app_config, cancel_token.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect a client and perform a 5MB transfer spanning 12 seconds
    let tcp = TcpStream::connect(format!("127.0.0.1:{}", client_listen_port))
        .await
        .expect("Client should connect");

    let chunk = vec![0x42u8; 64 * 1024]; // 64 KB
    let total_chunks = 40; // 2.5 MB total

    let (mut rd, mut wr) = tcp.into_split();
    let write_task = tokio::spawn(async move {
        for _ in 0..total_chunks {
            wr.write_all(&chunk).await.unwrap();
            tokio::time::sleep(Duration::from_millis(250)).await; // 40 * 250ms = 10s
        }
        wr.shutdown().await.unwrap();
    });

    let mut total_received = 0usize;
    let mut read_buf = vec![0u8; 64 * 1024];
    while let Ok(n) = rd.read(&mut read_buf).await {
        if n == 0 {
            break;
        }
        total_received += n;
    }

    write_task.await.unwrap();
    assert_eq!(total_received, total_chunks * 64 * 1024);

    // During this 10-12s continuous transfer, the 31 remaining idle streams in the pool
    // must NOT be evicted as Dead!
    let dead_count = evicted_dead.load(Ordering::SeqCst);
    eprintln!("=== TOTAL DEAD STREAMS EVICTED: {} ===", dead_count);
    assert_eq!(
        dead_count, 0,
        "Expected zero dead stream evictions during continuous transfer!"
    );

    cancel_token.cancel();
    let _ = tokio::join!(echo_handle, sup_handle);
}
