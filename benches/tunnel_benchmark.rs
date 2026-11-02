//! High-performance benchmarks for micro-tunnel components.
//! All comments and docstrings must be in English.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fastwebsockets::Role;
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

use micro_tunnel::control::stream::ControlMessage;
use micro_tunnel::core::relay::RelayEngine;
use micro_tunnel::pool::ReplenishingStreamPool;
use micro_tunnel::transport::ws::stream::WsTunnelStream;
use micro_tunnel::transport::{TargetAddr, TransportConnector};
use micro_tunnel::core::stream::{BoxFuture, TunnelStream};
use micro_tunnel::core::error::TunnelError;

/// Benchmark MessagePack control serialization performance.
fn bench_messagepack_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("control_message_msgpack");

    let sample_msg = ControlMessage::NewConnection {
        session_id: "sess_benchmark_test_session_id_123456789".to_string(),
        client_addr: "192.168.1.100:54321".to_string(),
    };

    group.bench_function("serialize_named", |b| {
        b.iter(|| {
            let vec = rmp_serde::to_vec_named(&sample_msg).unwrap();
            black_box(vec);
        });
    });

    let serialized = rmp_serde::to_vec_named(&sample_msg).unwrap();
    group.bench_function("deserialize_named", |b| {
        b.iter(|| {
            let msg: ControlMessage = rmp_serde::from_slice(&serialized).unwrap();
            black_box(msg);
        });
    });

    group.finish();
}

/// Benchmark RelayEngine raw throughput across stream adapters.
fn bench_relay_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("relay_throughput");
    let rt = Runtime::new().unwrap();

    for &size_kb in &[64, 512, 2048] {
        let total_bytes = (size_kb as usize) * 1024;
        group.throughput(Throughput::Bytes(total_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("ws_tunnel_stream", format!("{}KB", size_kb)),
            &total_bytes,
            |b, &size| {
                b.to_async(&rt).iter(|| async move {
                    let (raw_a, raw_b) = duplex(128 * 1024);
                    let ws_client = Box::new(WsTunnelStream::new(raw_a, Role::Client));
                    let ws_server = Box::new(WsTunnelStream::new(raw_b, Role::Server));

                    let (mut app_a, client_side) = duplex(128 * 1024);
                    let (mut app_b, server_side) = duplex(128 * 1024);

                    let cancel_token = CancellationToken::new();

                    // Relay client_side <-> ws_client
                    let h1 = tokio::spawn(RelayEngine::relay_with_cancel(
                        Box::new(client_side),
                        ws_client,
                        cancel_token.clone(),
                    ));

                    // Relay server_side <-> ws_server
                    let h2 = tokio::spawn(RelayEngine::relay_with_cancel(
                        Box::new(server_side),
                        ws_server,
                        cancel_token.clone(),
                    ));

                    // Pump test data through
                    let writer = tokio::spawn(async move {
                        let payload = vec![0x42u8; size];
                        app_a.write_all(&payload).await.unwrap();
                        app_a.flush().await.unwrap();
                    });

                    let reader = tokio::spawn(async move {
                        let mut buf = vec![0u8; size];
                        app_b.read_exact(&mut buf).await.unwrap();
                    });

                    let _ = tokio::join!(writer, reader);
                    cancel_token.cancel();
                    let _ = tokio::join!(h1, h2);
                });
            },
        );
    }

    group.finish();
}

/// Dummy connector for pool benchmark
struct DummyConnector;

impl TransportConnector for DummyConnector {
    fn connect<'a>(
        &'a self,
        _target: &'a TargetAddr,
        _session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        Box::pin(async move {
            let (a, _) = duplex(65536);
            Ok(Box::new(a) as Box<dyn TunnelStream>)
        })
    }

    fn protocol_scheme(&self) -> &'static str {
        "dummy"
    }
}

/// Benchmark 0-RTT pool acquisition vs on-demand creation.
fn bench_pool_acquisition(c: &mut Criterion) {
    let mut group = c.benchmark_group("pool_acquisition");
    let rt = Runtime::new().unwrap();
    let _guard = rt.enter();

    let connector: Arc<dyn TransportConnector> = Arc::new(DummyConnector);
    let target = TargetAddr::Domain("127.0.0.1".to_string(), 80);
    let cancel_token = CancellationToken::new();

    let pool = Arc::new(ReplenishingStreamPool::new(
        connector,
        target,
        16,
        cancel_token.clone(),
    ));

    // Warm up pool
    rt.block_on(tokio::time::sleep(Duration::from_millis(50)));

    group.bench_function("0_rtt_prewarmed_acquire", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let pool = Arc::clone(&pool);
            async move {
                for _ in 0..iters {
                    let (a, _) = duplex(1024);
                    pool.push_idle(Box::new(a));
                }
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    let stream = pool.acquire().await.unwrap();
                    black_box(stream);
                }
                start.elapsed()
            }
        });
    });

    cancel_token.cancel();
    group.finish();
}

criterion_group!(
    benches,
    bench_messagepack_serialization,
    bench_relay_throughput,
    bench_pool_acquisition
);
criterion_main!(benches);
