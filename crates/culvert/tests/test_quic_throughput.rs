// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Throughput probes over a real QUIC loopback link.
//!
//! Each case opens a fresh quinn connection pair with an explicit MTU and pushes
//! a fixed payload in one direction. Cases are deliberately split into layers so
//! a slow path can be attributed to one specific cause:
//!
//! - `raw borrowed`  : quinn `write_all(&[u8])`, which copies every write into an
//!   owned `Bytes` inside quinn-proto (`ByteSlice::pop_chunk`).
//! - `raw owned`     : quinn `write_chunk(Bytes)`, the zero-copy `BytesArray` path.
//! - `quinn adapter` : `QuinnTunnelStream`, whose `poll_write` is the borrowed path.
//! - `h3 adapter`    : `H3TunnelStream`, adding two mpsc channels and two tasks.
//!
//! Running `cargo test --release --test test_quic_throughput -- --nocapture
//! --test-threads=1` prints one line per case. Release mode and serial
//! execution matter: debug crypto and parallel cases would both distort the
//! numbers. This is a throughput probe, not a byte-level correctness fuzz test:
//! it checks the exact byte count and a sentinel byte per read, which is enough
//! to detect truncation/zeroed buffers without distorting the timing.
//! All comments and docstrings must be in English.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use quinn::{Connection, Endpoint, TransportConfig, VarInt};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::oneshot;

use culvert::config::model::{
    AppConfig, EndpointConfig, MappingRuleConfig, TlsConfig, TunnelConfig, TunnelMappingsConfig,
};
use culvert::io::stream::TunnelStream;
use culvert::transport::h3::{H3TunnelStream, QuinnTunnelStream};

/// 32 MiB keeps each case short enough for local iteration while still long
/// enough for the congestion controller to reach a steady state.
const TOTAL_BYTES: usize = 32 * 1024 * 1024;
const CHUNK_SIZE: usize = 64 * 1024;
const PATTERN: u8 = 0x5A;

/// Conservative Internet baseline: 1200-byte datagrams, PLPMTUD disabled so the
/// path stays fixed for the whole measurement.
const SMALL_MTU: u16 = 1200;
/// Loopback-safe jumbo datagrams. Windows loopback MTU is 65536, so this never
/// fragments; it cuts the datagram count (and thus AEAD + syscall count) ~7x.
const JUMBO_MTU: u16 = 8192;

const PROBE_TIMEOUT: Duration = Duration::from_secs(60);

/// A connected QUIC pair. Both endpoints are kept alive: dropping an endpoint
/// tears down every connection it owns, which would invalidate the measurement.
struct QuicPair {
    client: Connection,
    server: Connection,
    _client_ep: Endpoint,
    _server_ep: Endpoint,
}

struct Sample {
    layer: &'static str,
    /// Zero means the production QUIC defaults were left untouched.
    mtu: u16,
    bytes: usize,
    elapsed: Duration,
}

impl Sample {
    fn new(layer: &'static str, mtu: u16, bytes: usize, elapsed: Duration) -> Self {
        assert_eq!(bytes, TOTAL_BYTES, "{layer} transferred {bytes} bytes");
        Self {
            layer,
            mtu,
            bytes,
            elapsed,
        }
    }

    fn report(&self) {
        let mb_s = self.bytes as f64 / self.elapsed.as_secs_f64() / 1_000_000.0;
        let mtu = if self.mtu == 0 {
            "default".to_string()
        } else {
            self.mtu.to_string()
        };
        println!(
            "{:<16} mtu={:<5} {:>7.1} MB/s  ({:.3}s)",
            self.layer,
            mtu,
            mb_s,
            self.elapsed.as_secs_f64()
        );
    }
}

/// Mirrors the production flow-control windows so adapter comparisons are
/// apples-to-apples with the deployed relay path.
fn transport_config(mtu: u16) -> Arc<TransportConfig> {
    let mut cfg = TransportConfig::default();
    cfg.stream_receive_window(VarInt::from_u32(4 * 1024 * 1024));
    cfg.receive_window(VarInt::from_u32(128 * 1024 * 1024));
    cfg.send_window(128 * 1024 * 1024);
    cfg.max_idle_timeout(Some(Duration::from_secs(30).try_into().unwrap()));
    if mtu <= SMALL_MTU {
        cfg.mtu_discovery_config(None);
    } else {
        cfg.initial_mtu(mtu);
        let mut discovery = quinn::MtuDiscoveryConfig::default();
        discovery.upper_bound(mtu);
        cfg.mtu_discovery_config(Some(discovery));
    }
    Arc::new(cfg)
}

/// `EndpointConfig::max_udp_payload_size` is advertised to the peer as the QUIC
/// transport parameter and sizes the receive buffer, so it must be raised on
/// both ends for jumbo datagrams to actually be used.
fn endpoint_config(mtu: u16) -> quinn::EndpointConfig {
    let mut cfg = quinn::EndpointConfig::default();
    cfg.max_udp_payload_size(mtu).expect("MTU out of range");
    cfg
}

fn make_cert() -> (
    Vec<CertificateDer<'static>>,
    PrivateKeyDer<'static>,
    CertificateDer<'static>,
) {
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap();
    let cert_der = cert.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
    (vec![cert_der.clone()], key, cert_der)
}

async fn quic_pair(mtu: u16) -> QuicPair {
    let (cert_chain, key, root_cert) = make_cert();
    let transport = transport_config(mtu);

    let mut server_tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .unwrap();
    server_tls.alpn_protocols = vec![b"h3".to_vec()];
    let server_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(server_tls).unwrap();
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(server_crypto));
    server_config.transport_config(Arc::clone(&transport));

    let mut roots = rustls::RootCertStore::empty();
    roots.add(root_cert).unwrap();
    let mut client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_tls.alpn_protocols = vec![b"h3".to_vec()];
    let client_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(client_tls).unwrap();
    let mut client_config = quinn::ClientConfig::new(Arc::new(client_crypto));
    client_config.transport_config(Arc::clone(&transport));

    let runtime = quinn::default_runtime().expect("quinn needs a Tokio runtime");
    let server_socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let server_addr = server_socket.local_addr().unwrap();
    let server_ep = Endpoint::new(
        endpoint_config(mtu),
        Some(server_config),
        server_socket,
        Arc::clone(&runtime),
    )
    .unwrap();

    let client_socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut client_ep = Endpoint::new(endpoint_config(mtu), None, client_socket, runtime).unwrap();
    client_ep.set_default_client_config(client_config);

    let connecting = client_ep.connect(server_addr, "localhost").unwrap();
    let incoming = server_ep.accept().await.expect("server endpoint closed");
    let (server, client) = tokio::join!(
        async { incoming.await.expect("server handshake failed") },
        async { connecting.await.expect("client handshake failed") },
    );

    QuicPair {
        client,
        server,
        _client_ep: client_ep,
        _server_ep: server_ep,
    }
}

/// Writes the whole payload as borrowed chunks and closes the write half.
async fn write_payload<W: AsyncWrite + Unpin>(writer: &mut W) {
    let chunk = vec![PATTERN; CHUNK_SIZE];
    for _ in 0..(TOTAL_BYTES / CHUNK_SIZE) {
        writer.write_all(&chunk).await.unwrap();
    }
    writer.shutdown().await.unwrap();
}

/// Reads to EOF, checking the byte count and one sentinel byte per read.
async fn drain_payload<R: AsyncRead + Unpin>(reader: &mut R) -> (usize, bool) {
    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut total = 0usize;
    let mut intact = true;
    loop {
        let n = reader.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        if buf[0] != PATTERN {
            intact = false;
        }
        total += n;
    }
    (total, intact)
}

/// Times only the payload transfer, not connection/handshake setup. The whole
/// probe is still bounded by `PROBE_TIMEOUT` so a stalled adapter fails fast.
async fn timed<F>(label: &'static str, mtu: u16, probe: F) -> Sample
where
    F: Future<Output = (usize, bool)>,
{
    let start = Instant::now();
    let (total, intact) = tokio::time::timeout(PROBE_TIMEOUT, probe)
        .await
        .unwrap_or_else(|_| panic!("{label} @ MTU {mtu} exceeded {PROBE_TIMEOUT:?}"));
    let elapsed = start.elapsed();
    assert!(intact, "{label} @ MTU {mtu} saw a corrupted buffer");
    let sample = Sample::new(label, mtu, total, elapsed);
    sample.report();
    sample
}

/// Raw quinn baseline. `owned == false` goes through `write_all`, triggering
/// quinn-proto's `ByteSlice::pop_chunk` copy; `owned == true` uses `write_chunk`
/// and its zero-copy `BytesArray` path.
async fn raw_quinn(mtu: u16, owned: bool) -> Sample {
    let pair = quic_pair(mtu).await;
    let label = if owned { "raw owned" } else { "raw borrowed" };

    timed(label, mtu, async {
        let (mut send, _recv) = pair.client.open_bi().await.unwrap();
        // quinn only emits a stream frame on first write, so the writer must be
        // running before the server's accept_bi can complete.
        let writer = tokio::spawn(async move {
            if owned {
                let chunk = Bytes::from(vec![PATTERN; CHUNK_SIZE]);
                for _ in 0..(TOTAL_BYTES / CHUNK_SIZE) {
                    send.write_chunk(chunk.clone()).await.unwrap();
                }
            } else {
                let chunk = vec![PATTERN; CHUNK_SIZE];
                for _ in 0..(TOTAL_BYTES / CHUNK_SIZE) {
                    send.write_all(&chunk).await.unwrap();
                }
            }
            send.shutdown().await.unwrap();
        });
        let (_server_send, mut server_recv) = pair.server.accept_bi().await.unwrap();
        let reader = tokio::spawn(async move { drain_payload(&mut server_recv).await });
        writer.await.unwrap();
        reader.await.unwrap()
    })
    .await
}

/// `QuinnTunnelStream` is a thin wrapper whose `poll_write` delegates to quinn's
/// borrowed `AsyncWrite`, so it should track `raw borrowed` closely. Any gap is
/// the wrapper's per-write activity-clock update.
async fn quinn_adapter(mtu: u16) -> Sample {
    let pair = quic_pair(mtu).await;
    timed("quinn adapter", mtu, async {
        let (send, recv) = pair.client.open_bi().await.unwrap();
        let mut client_stream = QuinnTunnelStream::new(send, recv);
        assert!(
            client_stream.caps().owned_write,
            "QUIC adapter lost owned writes"
        );
        // Start the writer before accept_bi: quinn emits the stream frame on
        // first write, otherwise the accept waits forever.
        let writer = tokio::spawn(async move {
            write_payload(&mut client_stream).await;
        });
        let (server_send, server_recv) = pair.server.accept_bi().await.unwrap();
        let mut server_stream = QuinnTunnelStream::new(server_send, server_recv);
        let reader = tokio::spawn(async move { drain_payload(&mut server_stream).await });
        writer.await.unwrap();
        reader.await.unwrap()
    })
    .await
}

/// Full H3 adapter path: two mpsc channels and two spawned tasks per direction
/// on top of quinn. Compared with `quinn adapter`, the delta is the H3 adapter's
/// scheduling and extra copies.
async fn h3_adapter(mtu: u16) -> Sample {
    let pair = quic_pair(mtu).await;
    let server_conn = pair.server.clone();
    let (ready_tx, ready_rx) = oneshot::channel::<()>();

    let reader = tokio::spawn(async move {
        let mut h3_conn = h3::server::builder()
            .build::<_, Bytes>(h3_quinn::Connection::new(server_conn))
            .await
            .unwrap();
        let resolver = h3_conn.accept().await.unwrap().expect("no H3 request");
        let (_req, mut stream) = resolver.resolve_request().await.unwrap();
        let response = Response::builder().status(StatusCode::OK).body(()).unwrap();
        stream.send_response(response).await.unwrap();
        let mut stream = H3TunnelStream::from_server(stream);
        ready_tx.send(()).unwrap();

        let mut buf = vec![0u8; CHUNK_SIZE];
        let mut total = 0usize;
        let mut intact = true;
        loop {
            tokio::select! {
                read = stream.read(&mut buf) => {
                    let n = read.unwrap();
                    if n == 0 { break; }
                    if buf[0] != PATTERN { intact = false; }
                    total += n;
                }
                // Keep polling the H3 connection so control frames and flow
                // control updates keep flowing while the stream is drained.
                accepted = h3_conn.accept() => match accepted {
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                },
            }
        }
        (total, intact)
    });

    let (mut driver, mut send_request) = h3::client::builder()
        .build::<_, _, Bytes>(h3_quinn::Connection::new(pair.client.clone()))
        .await
        .unwrap();
    let driver_task = tokio::spawn(async move {
        let _ = driver.wait_idle().await;
    });

    let request = Request::builder()
        .method("CONNECT")
        .uri("/tunnel")
        .header("host", "localhost")
        .body(())
        .unwrap();
    let mut raw_stream = send_request.send_request(request).await.unwrap();
    let response = raw_stream.recv_response().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut client_stream = H3TunnelStream::from_client(raw_stream);
    assert!(
        client_stream.caps().owned_write,
        "H3 adapter lost owned writes"
    );

    // The server signals readiness after send_response, by which point the
    // client's recv_response has already completed.
    ready_rx.await.unwrap();

    let sample = timed("h3 adapter", mtu, async {
        write_payload(&mut client_stream).await;
        reader.await.unwrap()
    })
    .await;
    driver_task.abort();
    sample
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_quic_throughput_raw_quinn() {
    println!("=== raw quinn: borrowed vs owned write ===");
    raw_quinn(SMALL_MTU, false).await;
    raw_quinn(SMALL_MTU, true).await;
    raw_quinn(JUMBO_MTU, false).await;
    raw_quinn(JUMBO_MTU, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_quic_throughput_quinn_adapter() {
    println!("=== QuinnTunnelStream vs raw borrowed ===");
    quinn_adapter(SMALL_MTU).await;
    quinn_adapter(JUMBO_MTU).await;
}

/// Production can run with `worker_threads: 1`. The H3 adapter spawns two tasks
/// per stream that then share the worker with quinn's endpoint driver and the
/// relay pump; this case checks whether that scheduling contention is what the
/// iperf3 experiments observed.
#[tokio::test(flavor = "current_thread")]
async fn test_quic_throughput_h3_adapter_single_thread() {
    println!("=== H3TunnelStream on one worker thread ===");
    h3_adapter(SMALL_MTU).await;
    h3_adapter(JUMBO_MTU).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_quic_throughput_h3_adapter() {
    println!("=== H3TunnelStream (mpsc + split pumps) ===");
    h3_adapter(SMALL_MTU).await;
    h3_adapter(JUMBO_MTU).await;
}

fn free_udp_port() -> u16 {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap().port()
}

fn free_tcp_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Backend that drains `connections` TCP connections concurrently, reporting
/// the aggregated byte count. This is the iperf3 `-s` role plus iperf3's `-P`.
fn spawn_sink(
    cancel: tokio_util::sync::CancellationToken,
    connections: usize,
) -> (tokio::task::JoinHandle<usize>, u16) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    let handle = tokio::spawn(async move {
        let mut total = 0usize;
        for _ in 0..connections {
            let (mut socket, _) = tokio::select! {
                _ = cancel.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok(pair) => pair,
                    Err(_) => break,
                },
            };
            let mut buf = vec![0u8; CHUNK_SIZE];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => total += n,
                }
            }
        }
        total
    });
    (handle, port)
}

/// Full production path (TCP frontend -> relay pump -> transport -> relay pump
/// -> TCP backend) parameterized by transport scheme, pool size and concurrent
/// streams. This is what an iperf3 `-P n` run through culvert actually measures.
async fn full_relay(label: &'static str, scheme: &str, pool_size: usize, streams: usize) {
    use culvert::config::model::ServerConfig;
    use culvert::transport::TransportRegistry;

    let cancel = tokio_util::sync::CancellationToken::new();
    let (sink_handle, sink_port) = spawn_sink(cancel.clone(), streams);
    let server_port = free_udp_port();
    let client_port = free_tcp_port();
    let server_addr = format!("127.0.0.1:{server_port}");
    let client_addr = format!("127.0.0.1:{client_port}");

    let mut registry = TransportRegistry::new();
    registry.register(culvert::transport::h3::H3TransportDriver);
    let server_config = ServerConfig {
        // One H3 listener handles both ALPNs (`ALPN_H3` and `ALPN_QUIC`), matching
        // benchmarks/server-bench.yaml. Binding quic:// on the same UDP port too
        // would conflict.
        listen: vec![format!("h3://{server_addr}/tunnel")],
        default_target: Some(format!("127.0.0.1:{sink_port}")),
        ..Default::default()
    };
    let session_mgr = Arc::new(culvert::session::SessionManager::new());
    let server_runner =
        culvert::server::ServerRunner::new(server_config, Arc::new(registry), session_mgr);
    let server_cancel = cancel.clone();
    let server_handle = tokio::spawn(async move {
        let _ = server_runner.run(server_cancel).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let endpoint = if scheme == "quic" {
        format!("quic://{server_addr}/raw")
    } else {
        format!("{scheme}://{server_addr}/tunnel")
    };
    let app_config = AppConfig {
        tls: Some(TlsConfig {
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        tunnels: vec![TunnelConfig {
            tag: format!("throughput-{scheme}"),
            uuid: None,
            token: None,
            endpoint: EndpointConfig::Simple(endpoint),
            failover: None,
            multiplex: None,
            mappings: TunnelMappingsConfig::forward_pool(
                pool_size,
                vec![MappingRuleConfig::tcp(
                    client_addr.clone(),
                    format!("127.0.0.1:{sink_port}"),
                )],
            ),
            retry: None,
            http_headers: None,
        }],
        ..Default::default()
    };
    let client_cancel = cancel.clone();
    let supervisor = culvert::supervisor::TunnelSupervisor::new(app_config, client_cancel);
    let client_handle = tokio::spawn(async move {
        let _ = supervisor.run().await;
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut frontends = Vec::new();
    for _ in 0..streams {
        let mut connected = None;
        for _ in 0..100 {
            if let Ok(stream) = tokio::net::TcpStream::connect(&client_addr).await {
                connected = Some(stream);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        frontends.push(connected.expect("tunnel frontend never came up"));
    }
    let start = Instant::now();
    let chunk = Bytes::from(vec![PATTERN; CHUNK_SIZE]);
    let mut writers = Vec::new();
    for mut frontend in frontends {
        let chunk = chunk.clone();
        writers.push(tokio::spawn(async move {
            for _ in 0..(TOTAL_BYTES / CHUNK_SIZE / streams) {
                frontend.write_all(&chunk).await.unwrap();
            }
            frontend.shutdown().await.unwrap();
        }));
    }
    for writer in writers {
        writer.await.unwrap();
    }
    let total = sink_handle.await.unwrap();
    let elapsed = start.elapsed();
    let sample = Sample::new(label, 0, total, elapsed);
    sample.report();

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), async {
        let _ = tokio::join!(server_handle, client_handle);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_quic_throughput_full_relay_paths() {
    println!("=== full relay: single H3 stream ===");
    full_relay("relay h3", "h3", 0, 1).await;
    println!("=== full relay: raw QUIC, pool 16, 4 streams (benchmarks/client-bench.yaml) ===");
    full_relay("relay quic x4", "quic", 16, 4).await;
}
