// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Multi-mapping shared `Idle` transport contract tests auditing resource sharing,
//! isolation, capacity accounting, concurrent assignment, cancellation safety, and cleanup.
//! All comments and docstrings must be in English.

use bytes::Bytes;
use culvert::config::model::{
    AppConfig, EndpointConfig, MappingGroupConfig, MappingRuleConfig, ServerConfig, TunnelConfig,
    TunnelMappingsConfig,
};
use culvert::consts::{
    ACTIVATE_STREAM_ACK, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT,
    DEFAULT_RELAY_DRAIN_TIMEOUT, POOL_DUMMY_HOST,
};
use culvert::core::coordinator::{Coordinator, CoordinatorHandle};
use culvert::core::error::TaskError;
use culvert::core::error::TunnelError;
use culvert::core::state::{CoordinatorConfig, PooledTransport, TunnelPhase};
use culvert::core::stream::BoxFuture;
use culvert::io::addr::{L4Protocol, PeerAddr, TargetAddr};
use culvert::io::channel::{Datagram, DatagramIo, FlowKey, IoChannel};
use culvert::io::stream::{HeartbeatStatus, TunnelStream};
use culvert::pool::activation::decode_activation_until_cancel;
use culvert::supervisor::TunnelSupervisor;
use culvert::transport::TransportConnector;
use culvert::transport::ws::WsTransportDriver;
use culvert::tunnel::forward::{ForwardInput, ForwardWorkflow};
use culvert::tunnel::reverse::{
    ReverseServerRunner, ReverseSignaler, ReverseVisitorInput, ReverseVisitorWorkflow,
};
use futures_util::future::join_all;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf, duplex,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedActivation {
    stream_id: usize,
    target: Option<TargetAddr>,
    protocol: L4Protocol,
}

/// Instrumented `TunnelStream` tracking live count via `Drop` for leak assertions.
struct TrackedStream {
    inner: DuplexStream,
    live_counter: Arc<AtomicUsize>,
}

impl TrackedStream {
    fn new(inner: DuplexStream, live_counter: Arc<AtomicUsize>) -> Self {
        live_counter.fetch_add(1, Ordering::AcqRel);
        Self {
            inner,
            live_counter,
        }
    }
}

impl Drop for TrackedStream {
    fn drop(&mut self) {
        self.live_counter.fetch_sub(1, Ordering::AcqRel);
    }
}

impl AsyncRead for TrackedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TrackedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl TunnelStream for TrackedStream {
    fn poll_check_alive(&mut self, _cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        Poll::Ready(HeartbeatStatus::Alive)
    }
}

/// Minimal `DatagramIo` channel backed by async `mpsc` queues for UDP mapping tests.
struct MockDatagramFrontend {
    rx: mpsc::UnboundedReceiver<Datagram>,
    tx: mpsc::UnboundedSender<Datagram>,
}

impl MockDatagramFrontend {
    fn pair() -> (
        IoChannel,
        mpsc::UnboundedSender<Datagram>,
        mpsc::UnboundedReceiver<Datagram>,
    ) {
        let (in_tx, in_rx) = mpsc::unbounded_channel();
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        (
            IoChannel::from_datagram(Self {
                rx: in_rx,
                tx: out_tx,
            }),
            in_tx,
            out_rx,
        )
    }
}

impl DatagramIo for MockDatagramFrontend {
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        batch: &mut [Datagram],
    ) -> Poll<io::Result<usize>> {
        if batch.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(d)) => {
                batch[0] = d;
                Poll::Ready(Ok(1))
            }
            Poll::Ready(None) => Poll::Ready(Ok(0)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_send(&mut self, _cx: &mut Context<'_>, batch: &[Datagram]) -> Poll<io::Result<usize>> {
        for d in batch {
            self.tx
                .send(d.clone())
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "udp test sink closed"))?;
        }
        Poll::Ready(Ok(batch.len()))
    }
}

/// Mock connector routing activated pooled streams by `(protocol, target)` and recording
/// every activation count & destination for contract verification.
struct RoutingMockConnector {
    pooled_dials: Arc<AtomicUsize>,
    direct_dials: Arc<AtomicUsize>,
    live_streams: Arc<AtomicUsize>,
    activations: Arc<Mutex<Vec<RecordedActivation>>>,
    per_stream_activations: Arc<Mutex<HashMap<usize, AtomicUsize>>>,
    dial_gate: Option<Arc<Notify>>,
    gate_open: Arc<AtomicBool>,
    fail_first_activation: Arc<AtomicBool>,
}

impl RoutingMockConnector {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            pooled_dials: Arc::new(AtomicUsize::new(0)),
            direct_dials: Arc::new(AtomicUsize::new(0)),
            live_streams: Arc::new(AtomicUsize::new(0)),
            activations: Arc::new(Mutex::new(Vec::new())),
            per_stream_activations: Arc::new(Mutex::new(HashMap::new())),
            dial_gate: None,
            gate_open: Arc::new(AtomicBool::new(true)),
            fail_first_activation: Arc::new(AtomicBool::new(false)),
        })
    }

    fn with_gate() -> (Arc<Self>, Arc<Notify>, Arc<AtomicBool>) {
        let notify = Arc::new(Notify::new());
        let gate_open = Arc::new(AtomicBool::new(false));
        let conn = Arc::new(Self {
            pooled_dials: Arc::new(AtomicUsize::new(0)),
            direct_dials: Arc::new(AtomicUsize::new(0)),
            live_streams: Arc::new(AtomicUsize::new(0)),
            activations: Arc::new(Mutex::new(Vec::new())),
            per_stream_activations: Arc::new(Mutex::new(HashMap::new())),
            dial_gate: Some(Arc::clone(&notify)),
            gate_open: Arc::clone(&gate_open),
            fail_first_activation: Arc::new(AtomicBool::new(false)),
        });
        (conn, notify, gate_open)
    }

    fn spawn_stream_router(
        mut server: DuplexStream,
        stream_id: usize,
        pooled: bool,
        activations: Arc<Mutex<Vec<RecordedActivation>>>,
        per_stream_activations: Arc<Mutex<HashMap<usize, AtomicUsize>>>,
        fail_first: bool,
        direct_target: Option<(TargetAddr, L4Protocol)>,
    ) {
        tokio::spawn(async move {
            let (target, proto) = if pooled {
                let cancel = CancellationToken::new();
                let Ok(Some(hdr)) = decode_activation_until_cancel(&mut server, &cancel).await
                else {
                    return;
                };
                {
                    let map = per_stream_activations.lock();
                    if let Some(cnt) = map.get(&stream_id) {
                        cnt.fetch_add(1, Ordering::AcqRel);
                    }
                }
                activations.lock().push(RecordedActivation {
                    stream_id,
                    target: hdr.target.clone(),
                    protocol: hdr.protocol,
                });
                if fail_first {
                    return;
                }
                if server.write_all(&[ACTIVATE_STREAM_ACK]).await.is_err() {
                    return;
                }
                let _ = server.flush().await;
                (hdr.target, hdr.protocol)
            } else if let Some((t, p)) = direct_target {
                (Some(t), p)
            } else {
                (None, L4Protocol::Tcp)
            };

            let route_tag = format!(
                "{}|{}",
                target
                    .as_ref()
                    .map_or_else(|| "none".to_string(), ToString::to_string),
                proto
            );

            match proto {
                L4Protocol::Tcp => {
                    let mut buf = [0u8; 1024];
                    loop {
                        match server.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                let reply =
                                    format!("[{route_tag}]{}", String::from_utf8_lossy(&buf[..n]));
                                if server.write_all(reply.as_bytes()).await.is_err() {
                                    break;
                                }
                                let _ = server.flush().await;
                            }
                        }
                    }
                    let _ = server.shutdown().await;
                }
                L4Protocol::Udp => {
                    // Read `[u16 len | payload]` frames emitted by DatagramStreamRelay and reply similarly.
                    loop {
                        let mut len_buf = [0u8; 2];
                        if server.read_exact(&mut len_buf).await.is_err() {
                            break;
                        }
                        let len = u16::from_be_bytes(len_buf) as usize;
                        let mut payload = vec![0u8; len];
                        if server.read_exact(&mut payload).await.is_err() {
                            break;
                        }
                        let reply = format!("[{route_tag}]{}", String::from_utf8_lossy(&payload));
                        let rb = reply.as_bytes();
                        let hdr = (rb.len() as u16).to_be_bytes();
                        if server.write_all(&hdr).await.is_err()
                            || server.write_all(rb).await.is_err()
                        {
                            break;
                        }
                        let _ = server.flush().await;
                    }
                }
            }
        });
    }

    fn spawn_seeded_stream(&self, stream_id: usize) -> PooledTransport {
        self.per_stream_activations
            .lock()
            .insert(stream_id, AtomicUsize::new(0));
        let (client, server) = duplex(4096);
        Self::spawn_stream_router(
            server,
            stream_id,
            true,
            Arc::clone(&self.activations),
            Arc::clone(&self.per_stream_activations),
            false,
            None,
        );
        PooledTransport::new(
            Box::new(TrackedStream::new(client, Arc::clone(&self.live_streams))),
            1,
        )
    }
}

impl TransportConnector for RoutingMockConnector {
    fn connect<'a>(
        &'a self,
        target: &'a TargetAddr,
        session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.connect_with_protocol(target, session_id, L4Protocol::Tcp)
    }

    fn connect_with_protocol<'a>(
        &'a self,
        target: &'a TargetAddr,
        _session_id: Option<&'a str>,
        protocol: L4Protocol,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        let id = 10_000 + self.direct_dials.fetch_add(1, Ordering::AcqRel) + 1;
        let target_clone = target.clone();
        let (client, server) = duplex(4096);
        Self::spawn_stream_router(
            server,
            id,
            false,
            Arc::clone(&self.activations),
            Arc::clone(&self.per_stream_activations),
            false,
            Some((target_clone, protocol)),
        );
        let live = Arc::clone(&self.live_streams);
        Box::pin(async move {
            Ok(IoChannel::from_boxed_stream(Box::new(TrackedStream::new(
                client, live,
            ))))
        })
    }

    fn connect_pooled<'a>(
        &'a self,
        target: &'a TargetAddr,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.connect_pooled_with_tunnel(target, L4Protocol::Tcp, None)
    }

    fn connect_pooled_with_tunnel<'a>(
        &'a self,
        target: &'a TargetAddr,
        _protocol: L4Protocol,
        _tunnel_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        assert_eq!(
            *target,
            TargetAddr::Domain(POOL_DUMMY_HOST.to_string(), 0),
            "Idle transport dial must use generic POOL_DUMMY_HOST without binding any mapping target"
        );
        let gate = self.dial_gate.clone();
        let gate_open = Arc::clone(&self.gate_open);
        let pooled_dials = Arc::clone(&self.pooled_dials);
        let live_streams = Arc::clone(&self.live_streams);
        let activations = Arc::clone(&self.activations);
        let per_stream_activations = Arc::clone(&self.per_stream_activations);
        let fail_first = self.fail_first_activation.swap(false, Ordering::AcqRel);

        Box::pin(async move {
            if let Some(notify) = gate {
                while !gate_open.load(Ordering::Acquire) {
                    notify.notified().await;
                }
            }
            let stream_id = pooled_dials.fetch_add(1, Ordering::AcqRel) + 1;
            per_stream_activations
                .lock()
                .insert(stream_id, AtomicUsize::new(0));
            let (client, server) = duplex(4096);
            Self::spawn_stream_router(
                server,
                stream_id,
                true,
                activations,
                per_stream_activations,
                fail_first,
                None,
            );
            Ok(IoChannel::from_boxed_stream(Box::new(TrackedStream::new(
                client,
                live_streams,
            ))))
        })
    }
}

async fn send_tcp_via_shared(
    coord: &CoordinatorHandle<ForwardWorkflow>,
    target: TargetAddr,
    payload: &[u8],
) -> String {
    let (mut visitor, stream) = duplex(4096);
    coord
        .assign(ForwardInput::new(
            IoChannel::from_stream(stream),
            target,
            L4Protocol::Tcp,
        ))
        .await
        .unwrap();

    visitor.write_all(payload).await.unwrap();
    visitor.shutdown().await.unwrap();

    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), visitor.read_to_end(&mut out))
        .await
        .expect("TCP shared relay echo timed out")
        .unwrap();
    String::from_utf8(out).unwrap()
}

async fn send_udp_via_shared(
    coord: &CoordinatorHandle<ForwardWorkflow>,
    target: TargetAddr,
    payload: &'static [u8],
) -> String {
    let (fe_channel, tx, mut rx) = MockDatagramFrontend::pair();
    coord
        .assign(ForwardInput::new(fe_channel, target, L4Protocol::Udp))
        .await
        .unwrap();

    tx.send(Datagram {
        flow: FlowKey {
            peer: PeerAddr::Socket("127.0.0.1:45001".parse().unwrap()),
            target: None,
            protocol: L4Protocol::Udp,
        },
        payload: Bytes::from_static(payload),
    })
    .unwrap();

    let dgram = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("UDP shared relay echo timed out")
        .unwrap();
    String::from_utf8(dgram.payload.to_vec()).unwrap()
}

async fn bind_tagged_tcp_echo(
    tag: &'static str,
    cancel: CancellationToken,
    observed: Arc<Mutex<Vec<String>>>,
) -> String {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = probe.local_addr().unwrap();
    probe.set_nonblocking(true).unwrap();
    let listener = TcpListener::from_std(probe).unwrap();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                res = listener.accept() => {
                    let Ok((mut sock, _)) = res else { break; };
                    let observed = Arc::clone(&observed);
                    tokio::spawn(async move {
                        let mut buf = Vec::new();
                        let _ = sock.read_to_end(&mut buf).await;
                        let body = String::from_utf8_lossy(&buf).into_owned();
                        if body.is_empty() {
                            return;
                        }
                        let reply = format!("[{tag}]{body}");
                        observed.lock().push(reply.clone());
                        let _ = sock.write_all(reply.as_bytes()).await;
                        let _ = sock.shutdown().await;
                    });
                }
            }
        }
    });

    addr.to_string()
}

struct CountedNoopSignaler {
    calls: Arc<AtomicUsize>,
}

impl ReverseSignaler for CountedNoopSignaler {
    fn request_pair(
        &self,
        _visitor_peer: PeerAddr,
    ) -> BoxFuture<'static, Result<Box<dyn TunnelStream>, TaskError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async {
            Err(TaskError::StageFailed(
                "slow-path signaling must not be used when Idle pool serves 0-RTT".into(),
            ))
        })
    }
}

#[tokio::test]
async fn forward_mappings_share_one_idle_coordinator() {
    let conn = RoutingMockConnector::new();
    let wf = ForwardWorkflow::new(
        Some(Arc::clone(&conn) as Arc<dyn TransportConnector>),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    )
    .with_pooled(true);
    let cfg = CoordinatorConfig::forward_pooled(
        Arc::clone(&conn) as Arc<dyn TransportConnector>,
        3,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );
    let (coord, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());

    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(coord.query_idle_count().await.unwrap(), 3);

    let target_a = TargetAddr::Domain("svc-a.internal".into(), 8001);
    let target_b = TargetAddr::Domain("svc-b.internal".into(), 8002);
    let target_c = TargetAddr::Domain("svc-c.internal".into(), 9003);

    let (resp_a, resp_b, resp_c) = tokio::join!(
        send_tcp_via_shared(&coord, target_a, b"payload-A"),
        send_tcp_via_shared(&coord, target_b, b"payload-B"),
        send_udp_via_shared(&coord, target_c, b"payload-C"),
    );

    assert_eq!(resp_a, "[svc-a.internal:8001|tcp]payload-A");
    assert_eq!(resp_b, "[svc-b.internal:8002|tcp]payload-B");
    assert_eq!(resp_c, "[svc-c.internal:9003|udp]payload-C");
    assert_eq!(conn.direct_dials.load(Ordering::Acquire), 0);

    coord.graceful_stop().await.unwrap();
    let summary = join.await.unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
}

#[tokio::test]
async fn reverse_mappings_share_one_prewarm_coordinator() {
    let cancel = CancellationToken::new();
    let obs_x = Arc::new(Mutex::new(Vec::new()));
    let obs_y = Arc::new(Mutex::new(Vec::new()));
    let echo_x = bind_tagged_tcp_echo("REV-X", cancel.clone(), Arc::clone(&obs_x)).await;
    let echo_y = bind_tagged_tcp_echo("REV-Y", cancel.clone(), Arc::clone(&obs_y)).await;

    let ws_probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let ws_port = ws_probe.local_addr().unwrap().port();
    drop(ws_probe);
    let rev_x_probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rev_x_port = rev_x_probe.local_addr().unwrap().port();
    drop(rev_x_probe);
    let rev_y_probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rev_y_port = rev_y_probe.local_addr().unwrap().port();
    drop(rev_y_probe);

    let ws_addr = format!("127.0.0.1:{ws_port}");
    let rev_x_listen = format!("127.0.0.1:{rev_x_port}");
    let rev_y_listen = format!("127.0.0.1:{rev_y_port}");

    let app_config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("ws://{ws_addr}/tunnel")],
            token: Some("shared-rev-tok".into()),
            ..Default::default()
        }),
        tunnels: vec![TunnelConfig {
            tag: "shared-rev-cluster".into(),
            endpoint: EndpointConfig::Simple(format!("ws://{ws_addr}/tunnel")),
            token: Some("shared-rev-tok".into()),
            uuid: Some("rev-cluster-uuid".into()),
            retry: None,
            failover: None,
            http_headers: None,
            multiplex: None,
            mappings: TunnelMappingsConfig {
                forward: None,
                reverse: Some(MappingGroupConfig {
                    pool_size: Some(2),
                    rules: vec![
                        MappingRuleConfig {
                            protocol: L4Protocol::Tcp,
                            listen: rev_x_listen.clone(),
                            target: echo_x,
                            udp_idle_timeout: None,
                        },
                        MappingRuleConfig {
                            protocol: L4Protocol::Tcp,
                            listen: rev_y_listen.clone(),
                            target: echo_y,
                            udp_idle_timeout: None,
                        },
                    ],
                }),
            },
        }],
        ..Default::default()
    };

    let mut reg = culvert::transport::TransportRegistry::new();
    reg.register(WsTransportDriver);
    let sup = TunnelSupervisor::with_registry(app_config, cancel.clone(), Arc::new(reg));
    let sup_handle = tokio::spawn(async move { sup.run().await });

    let mut s_x = None;
    for _ in 0..40 {
        if let Ok(s) = TcpStream::connect(&rev_x_listen).await {
            s_x = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let mut s_x = s_x.expect("reverse X must bind");

    let mut s_y = None;
    for _ in 0..40 {
        if let Ok(s) = TcpStream::connect(&rev_y_listen).await {
            s_y = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let mut s_y = s_y.expect("reverse Y must bind");

    // Allow SharedReversePoolRunner time to prewarm its shared capacity.
    tokio::time::sleep(Duration::from_millis(100)).await;

    s_x.write_all(b"msg-for-x").await.unwrap();
    s_x.shutdown().await.unwrap();
    s_y.write_all(b"msg-for-y").await.unwrap();
    s_y.shutdown().await.unwrap();

    let mut rx_x = Vec::new();
    let mut rx_y = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), s_x.read_to_end(&mut rx_x))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), s_y.read_to_end(&mut rx_y))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(String::from_utf8(rx_x).unwrap(), "[REV-X]msg-for-x");
    assert_eq!(String::from_utf8(rx_y).unwrap(), "[REV-Y]msg-for-y");
    assert_eq!(&*obs_x.lock(), &vec!["[REV-X]msg-for-x".to_string()]);
    assert_eq!(&*obs_y.lock(), &vec!["[REV-Y]msg-for-y".to_string()]);

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), sup_handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn forward_and_reverse_idle_are_isolated() {
    let cancel = CancellationToken::new();
    let fwd_obs = Arc::new(Mutex::new(Vec::new()));
    let rev_obs = Arc::new(Mutex::new(Vec::new()));
    let fwd_echo = bind_tagged_tcp_echo("FWD-ISO", cancel.clone(), Arc::clone(&fwd_obs)).await;
    let rev_echo = bind_tagged_tcp_echo("REV-ISO", cancel.clone(), Arc::clone(&rev_obs)).await;

    let ws_probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let ws_port = ws_probe.local_addr().unwrap().port();
    drop(ws_probe);
    let fwd_probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let fwd_port = fwd_probe.local_addr().unwrap().port();
    drop(fwd_probe);
    let rev_probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rev_port = rev_probe.local_addr().unwrap().port();
    drop(rev_probe);

    let ws_addr = format!("127.0.0.1:{ws_port}");
    let fwd_listen = format!("127.0.0.1:{fwd_port}");
    let rev_listen = format!("127.0.0.1:{rev_port}");

    let app_config = AppConfig {
        server: Some(ServerConfig {
            listen: vec![format!("ws://{ws_addr}/tunnel")],
            token: Some("iso-tok".into()),
            ..Default::default()
        }),
        tunnels: vec![TunnelConfig {
            tag: "iso-tunnel".into(),
            endpoint: EndpointConfig::Simple(format!("ws://{ws_addr}/tunnel")),
            token: Some("iso-tok".into()),
            uuid: Some("iso-uuid".into()),
            retry: None,
            failover: None,
            http_headers: None,
            multiplex: None,
            mappings: TunnelMappingsConfig {
                forward: Some(MappingGroupConfig {
                    pool_size: Some(2),
                    rules: vec![MappingRuleConfig {
                        protocol: L4Protocol::Tcp,
                        listen: fwd_listen.clone(),
                        target: fwd_echo,
                        udp_idle_timeout: None,
                    }],
                }),
                reverse: Some(MappingGroupConfig {
                    pool_size: Some(2),
                    rules: vec![MappingRuleConfig {
                        protocol: L4Protocol::Tcp,
                        listen: rev_listen.clone(),
                        target: rev_echo,
                        udp_idle_timeout: None,
                    }],
                }),
            },
        }],
        ..Default::default()
    };

    let mut reg = culvert::transport::TransportRegistry::new();
    reg.register(WsTransportDriver);
    let sup = TunnelSupervisor::with_registry(app_config, cancel.clone(), Arc::new(reg));
    let sup_handle = tokio::spawn(async move { sup.run().await });

    // Wait for both endpoints to bind.
    for addr in [&fwd_listen, &rev_listen] {
        let mut ready = false;
        for _ in 0..40 {
            if let Ok(stream) = TcpStream::connect(addr).await {
                drop(stream);
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        assert!(ready, "endpoint {addr} failed to bind");
    }

    // Burst 6 concurrent Forward connections to exhaust Forward idle pool while running Reverse traffic.
    let fwd_tasks = (0..6).map(|idx| {
        let addr = fwd_listen.clone();
        async move {
            let mut tcp = TcpStream::connect(&addr).await.unwrap();
            let msg = format!("fwd-burst-{idx}");
            tcp.write_all(msg.as_bytes()).await.unwrap();
            tcp.shutdown().await.unwrap();
            let mut out = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), tcp.read_to_end(&mut out))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(String::from_utf8(out).unwrap(), format!("[FWD-ISO]{msg}"));
        }
    });

    let rev_fut = async {
        let mut tcp = TcpStream::connect(&rev_listen).await.unwrap();
        tcp.write_all(b"rev-isolated").await.unwrap();
        tcp.shutdown().await.unwrap();
        let mut out = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), tcp.read_to_end(&mut out))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "[REV-ISO]rev-isolated");
    };

    tokio::join!(join_all(fwd_tasks), rev_fut);
    assert_eq!(fwd_obs.lock().len(), 6);
    assert_eq!(&*rev_obs.lock(), &vec!["[REV-ISO]rev-isolated".to_string()]);

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), sup_handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn concurrent_mappings_never_double_assign_idle() {
    let conn = RoutingMockConnector::new();
    // No fallback direct connector and no DialDriver -> only the 3 seeded Idle streams exist.
    let wf = ForwardWorkflow::new(
        None,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    )
    .with_pooled(true)
    .with_acquire_timeout(Duration::from_millis(500));
    let cfg = CoordinatorConfig {
        drain_timeout: DEFAULT_RELAY_DRAIN_TIMEOUT,
        max_idle_capacity: 8,
        idle_policy: Default::default(),
        dial: None,
    };
    let (coord, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());

    for id in 1..=3 {
        let t = conn.spawn_seeded_stream(id);
        assert!(coord.supply_transport(t).await.unwrap());
    }
    assert_eq!(coord.query_idle_count().await.unwrap(), 3);

    let (r1, r2, r3) = tokio::join!(
        send_tcp_via_shared(&coord, TargetAddr::Domain("m1.test".into(), 101), b"one"),
        send_tcp_via_shared(&coord, TargetAddr::Domain("m2.test".into(), 102), b"two"),
        send_udp_via_shared(&coord, TargetAddr::Domain("m3.test".into(), 103), b"three"),
    );

    assert_eq!(r1, "[m1.test:101|tcp]one");
    assert_eq!(r2, "[m2.test:102|tcp]two");
    assert_eq!(r3, "[m3.test:103|udp]three");

    {
        let act_counts = conn.per_stream_activations.lock();
        for id in 1..=3 {
            assert_eq!(
                act_counts.get(&id).unwrap().load(Ordering::Acquire),
                1,
                "seeded Idle stream #{id} must be assigned and activated at most once"
            );
        }
    }

    coord.graceful_stop().await.unwrap();
    let _ = join.await.unwrap();
}

#[tokio::test]
async fn mapping_shutdown_preserves_shared_idle() {
    // Part A: Shared Forward Coordinator survives single-mapping exit and keeps unallocated Idle streams.
    let conn = RoutingMockConnector::new();
    let wf = ForwardWorkflow::new(
        Some(Arc::clone(&conn) as Arc<dyn TransportConnector>),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    )
    .with_pooled(true);
    let cfg = CoordinatorConfig::forward_pooled(
        Arc::clone(&conn) as Arc<dyn TransportConnector>,
        3,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );
    let (coord, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(coord.query_idle_count().await.unwrap(), 3);

    // Mapping A holds 1 stream active, leaving 2 unallocated shared Idle streams.
    let (mut vis_a, fe_a) = duplex(4096);
    coord
        .assign(ForwardInput::new(
            IoChannel::from_stream(fe_a),
            TargetAddr::Domain("map-a.local".into(), 7001),
            L4Protocol::Tcp,
        ))
        .await
        .unwrap();
    vis_a.write_all(b"from-A").await.unwrap();
    let mut ack_a = [0u8; 28];
    vis_a.read_exact(&mut ack_a).await.unwrap();
    assert_eq!(&ack_a, b"[map-a.local:7001|tcp]from-A");

    // Shut down Mapping A's frontend connection without stopping shared Coordinator.
    vis_a.shutdown().await.unwrap();
    drop(vis_a);
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert_eq!(coord.query_phase().await.unwrap(), TunnelPhase::Running);
    assert!(coord.query_idle_count().await.unwrap() >= 2);

    // Mappings B and C continue claiming preserved Idle streams cleanly.
    let resp_b = send_tcp_via_shared(
        &coord,
        TargetAddr::Domain("map-b.local".into(), 7002),
        b"B-ok",
    )
    .await;
    let resp_c = send_udp_via_shared(
        &coord,
        TargetAddr::Domain("map-c.local".into(), 7003),
        b"C-ok",
    )
    .await;
    assert_eq!(resp_b, "[map-b.local:7002|tcp]B-ok");
    assert_eq!(resp_c, "[map-c.local:7003|udp]C-ok");
    assert_eq!(conn.direct_dials.load(Ordering::Acquire), 0);

    coord.graceful_stop().await.unwrap();
    let _ = join.await.unwrap();

    // Part B: Reverse shared tunnel_id Coordinator stays live when 1 of 2 listeners exits.
    let signal_calls = Arc::new(AtomicUsize::new(0));
    let signaler: Arc<dyn ReverseSignaler> = Arc::new(CountedNoopSignaler {
        calls: Arc::clone(&signal_calls),
    });
    let rev_wf = ReverseVisitorWorkflow::shared(
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );
    let rev_cfg =
        CoordinatorConfig::reverse_inbound(4, DEFAULT_IDLE_TIMEOUT, DEFAULT_RELAY_DRAIN_TIMEOUT);
    let (rev_coord, rev_join) =
        Coordinator::start_with_config(rev_wf, rev_cfg, CancellationToken::new());

    // Seed 1 inbound 0xAC echo stream into shared reverse Coordinator.
    let (inbound_client, mut inbound_worker) = duplex(4096);
    tokio::spawn(async move {
        let cancel = CancellationToken::new();
        let Ok(Some(_)) = decode_activation_until_cancel(&mut inbound_worker, &cancel).await else {
            return;
        };
        let _ = inbound_worker.write_all(&[ACTIVATE_STREAM_ACK]).await;
        let _ = inbound_worker.flush().await;
        let mut buf = Vec::new();
        let _ = inbound_worker.read_to_end(&mut buf).await;
        let _ = inbound_worker.write_all(&buf).await;
        let _ = inbound_worker.shutdown().await;
    });
    assert!(
        rev_coord
            .supply_transport(PooledTransport::new(Box::new(inbound_client), 1))
            .await
            .unwrap()
    );

    // Listener X runs & exits cleanly (`coord_join = None` -> does NOT stop shared Coordinator).
    let hub_fe_x = culvert::frontend::FrontendOutput::Err(TunnelError::Shutdown);
    struct OneShotShutdownFrontend(Option<culvert::frontend::FrontendOutput>);
    impl culvert::frontend::Frontend for OneShotShutdownFrontend {
        fn accept(
            &mut self,
            slot: &mut reusable_future::ReusableFutureRef<
                'static,
                culvert::frontend::FrontendOutput,
            >,
        ) {
            let item = self.0.take();
            slot.set(|| async move { item.unwrap_or(Err(TunnelError::Shutdown)) });
        }
        fn shutdown<'a>(&'a mut self) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }
    }

    let runner_x = ReverseServerRunner::with_coordinator(
        Box::new(OneShotShutdownFrontend(Some(hub_fe_x))),
        TargetAddr::Domain("rev-x.local".into(), 80),
        L4Protocol::Tcp,
        rev_coord.clone(),
        None,
        Some(Arc::clone(&signaler)),
        None,
    );
    let cancel_x = CancellationToken::new();
    cancel_x.cancel();
    assert!(runner_x.run(cancel_x).await.unwrap().is_none());

    // Shared reverse Coordinator & its Idle stream must still be intact for Listener Y.
    assert_eq!(rev_coord.query_phase().await.unwrap(), TunnelPhase::Running);
    assert_eq!(rev_coord.query_idle_count().await.unwrap(), 1);

    let (mut vis_y, fe_y) = duplex(4096);
    rev_coord
        .assign(ReverseVisitorInput {
            visitor: IoChannel::from_stream(fe_y),
            peer: PeerAddr::Socket("127.0.0.1:50001".parse().unwrap()),
            target: TargetAddr::Domain("rev-y.local".into(), 80),
            protocol: L4Protocol::Tcp,
            relay_idle_timeout: None,
            signaler: Some(signaler),
        })
        .await
        .unwrap();
    vis_y.write_all(b"preserved-rev").await.unwrap();
    vis_y.shutdown().await.unwrap();
    let mut out_y = Vec::new();
    vis_y.read_to_end(&mut out_y).await.unwrap();
    assert_eq!(&out_y, b"preserved-rev");
    assert_eq!(signal_calls.load(Ordering::Acquire), 0);

    rev_coord.graceful_stop().await.unwrap();
    let _ = rev_join.await.unwrap();
}

#[tokio::test]
async fn mapping_load_does_not_duplicate_pool_capacity() {
    let conn = RoutingMockConnector::new();
    let wf = ForwardWorkflow::new(
        Some(Arc::clone(&conn) as Arc<dyn TransportConnector>),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    )
    .with_pooled(true);
    // Tunnel-level pool_size = 3 shared across 5 mappings.
    let cfg = CoordinatorConfig::forward_pooled(
        Arc::clone(&conn) as Arc<dyn TransportConnector>,
        3,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );
    let (coord, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(coord.query_idle_count().await.unwrap(), 3);
    assert_eq!(conn.pooled_dials.load(Ordering::Acquire), 3);

    // Issue traffic across 5 logical mappings sequentially -> replenished pool size stays 3 (not 15).
    for idx in 0..5 {
        let target = TargetAddr::Domain(format!("svc-{idx}.internal"), 8000 + idx);
        let msg = format!("m{idx}");
        let resp = send_tcp_via_shared(&coord, target.clone(), msg.as_bytes()).await;
        assert_eq!(resp, format!("[{target}|tcp]{msg}"));
    }

    tokio::time::sleep(Duration::from_millis(60)).await;
    let stats = coord.query_pool_stats().await.unwrap();
    assert_eq!(stats.idle_count, 3);
    assert_eq!(conn.pooled_dials.load(Ordering::Acquire), 8);

    coord.graceful_stop().await.unwrap();
    let _ = join.await.unwrap();
}

#[tokio::test]
async fn waiter_cancellation_does_not_affect_other_mappings() {
    let (conn, notify, gate_open) = RoutingMockConnector::with_gate();
    let wf = ForwardWorkflow::new(
        None,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    )
    .with_pooled(true)
    .with_acquire_timeout(Duration::from_secs(2));
    let cfg = CoordinatorConfig::forward_pooled(
        Arc::clone(&conn) as Arc<dyn TransportConnector>,
        1,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );
    let (coord, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());

    // Ensure idle pool is empty (dial is blocked behind closed gate_open).
    assert_eq!(coord.query_idle_count().await.unwrap(), 0);

    // Mapping A enters Connecting -> Waiter A queued first.
    let (_vis_a, fe_a) = duplex(1024);
    let task_a = coord
        .assign(ForwardInput::new(
            IoChannel::from_stream(fe_a),
            TargetAddr::Domain("map-a.cancel".into(), 1),
            L4Protocol::Tcp,
        ))
        .await
        .unwrap();

    // Mapping B enters Connecting -> Waiter B queued second.
    let (mut vis_b, fe_b) = duplex(4096);
    let _task_b = coord
        .assign(ForwardInput::new(
            IoChannel::from_stream(fe_b),
            TargetAddr::Domain("map-b.live".into(), 2),
            L4Protocol::Tcp,
        ))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(25)).await;
    // Cancel ONLY Mapping A via Drain (its RequestTransport future & oneshot receiver drop).
    assert!(coord.request_recycle(task_a).await.unwrap());
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Now unblock DialDriver: Waiter A must be skipped safely and Waiter B receives the transport.
    gate_open.store(true, Ordering::Release);
    notify.notify_waiters();

    vis_b.write_all(b"survivor-B").await.unwrap();
    vis_b.shutdown().await.unwrap();
    let mut out_b = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), vis_b.read_to_end(&mut out_b))
        .await
        .expect("Mapping B must receive transport after Mapping A cancellation")
        .unwrap();
    assert_eq!(
        String::from_utf8(out_b).unwrap(),
        "[map-b.live:2|tcp]survivor-B"
    );

    coord.graceful_stop().await.unwrap();
    let _ = join.await.unwrap();
}

#[tokio::test]
async fn activation_uses_request_mapping_parameters() {
    let conn = RoutingMockConnector::new();
    let wf = ForwardWorkflow::new(
        Some(Arc::clone(&conn) as Arc<dyn TransportConnector>),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    )
    .with_pooled(true);
    let cfg = CoordinatorConfig::forward_pooled(
        Arc::clone(&conn) as Arc<dyn TransportConnector>,
        3,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );
    let (coord, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());
    tokio::time::sleep(Duration::from_millis(40)).await;

    let t_tcp_1 = TargetAddr::Ip("10.1.0.1:443".parse().unwrap());
    let t_tcp_2 = TargetAddr::Domain("api.service.local".into(), 8443);
    let t_udp_3 = TargetAddr::Ip("10.1.0.3:5353".parse().unwrap());

    assert_eq!(
        send_tcp_via_shared(&coord, t_tcp_1.clone(), b"p1").await,
        "[10.1.0.1:443|tcp]p1"
    );
    assert_eq!(
        send_tcp_via_shared(&coord, t_tcp_2.clone(), b"p2").await,
        "[api.service.local:8443|tcp]p2"
    );
    assert_eq!(
        send_udp_via_shared(&coord, t_udp_3.clone(), b"p3").await,
        "[10.1.0.3:5353|udp]p3"
    );

    let recorded = conn.activations.lock().clone();
    assert_eq!(recorded.len(), 3);
    assert!(
        recorded
            .iter()
            .any(|r| r.target == Some(t_tcp_1.clone()) && r.protocol == L4Protocol::Tcp)
    );
    assert!(
        recorded
            .iter()
            .any(|r| r.target == Some(t_tcp_2.clone()) && r.protocol == L4Protocol::Tcp)
    );
    assert!(
        recorded
            .iter()
            .any(|r| r.target == Some(t_udp_3.clone()) && r.protocol == L4Protocol::Udp)
    );

    coord.graceful_stop().await.unwrap();
    let _ = join.await.unwrap();
}

#[tokio::test]
async fn replenishment_under_concurrent_consumption() {
    let conn = RoutingMockConnector::new();
    let wf = ForwardWorkflow::new(
        Some(Arc::clone(&conn) as Arc<dyn TransportConnector>),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    )
    .with_pooled(true);
    let cfg = CoordinatorConfig::forward_pooled(
        Arc::clone(&conn) as Arc<dyn TransportConnector>,
        3,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );
    let (coord, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(coord.query_idle_count().await.unwrap(), 3);

    // Concurrently consume all 3 Idle streams across 3 different targets.
    let (r1, r2, r3) = tokio::join!(
        send_tcp_via_shared(&coord, TargetAddr::Domain("c1".into(), 1), b"a"),
        send_tcp_via_shared(&coord, TargetAddr::Domain("c2".into(), 2), b"b"),
        send_udp_via_shared(&coord, TargetAddr::Domain("c3".into(), 3), b"c"),
    );
    assert_eq!(r1, "[c1:1|tcp]a");
    assert_eq!(r2, "[c2:2|tcp]b");
    assert_eq!(r3, "[c3:3|udp]c");

    // Verify DialDriver replenishes back cleanly to target_size (3) without duplicate over-dialing.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let stats = coord.query_pool_stats().await.unwrap();
    assert_eq!(stats.idle_count, 3);
    assert_eq!(stats.in_flight_dials, 0);
    assert_eq!(conn.pooled_dials.load(Ordering::Acquire), 6);

    coord.graceful_stop().await.unwrap();
    let _ = join.await.unwrap();
}

#[tokio::test]
async fn tunnel_shutdown_reclaims_all_shared_resources() {
    let conn = RoutingMockConnector::new();
    let wf = ForwardWorkflow::new(
        Some(Arc::clone(&conn) as Arc<dyn TransportConnector>),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        Duration::from_millis(80),
    )
    .with_pooled(true);
    let cfg = CoordinatorConfig::forward_pooled(
        Arc::clone(&conn) as Arc<dyn TransportConnector>,
        4,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        Duration::from_millis(80),
    );
    let (coord, join) = Coordinator::start_with_config(wf, cfg, CancellationToken::new());

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(coord.query_idle_count().await.unwrap(), 4);
    assert!(conn.live_streams.load(Ordering::Acquire) >= 4);

    // Keep 2 active jobs (1 TCP, 1 UDP) running while 2 Idle streams remain unallocated.
    let (mut vis_tcp, fe_tcp) = duplex(4096);
    coord
        .assign(ForwardInput::new(
            IoChannel::from_stream(fe_tcp),
            TargetAddr::Domain("drain-tcp".into(), 1),
            L4Protocol::Tcp,
        ))
        .await
        .unwrap();
    let (fe_udp, udp_tx, _udp_rx) = MockDatagramFrontend::pair();
    coord
        .assign(ForwardInput::new(
            fe_udp,
            TargetAddr::Domain("drain-udp".into(), 2),
            L4Protocol::Udp,
        ))
        .await
        .unwrap();

    vis_tcp.write_all(b"keepalive").await.unwrap();
    let mut tmp = [0u8; 26];
    vis_tcp.read_exact(&mut tmp).await.unwrap();
    assert_eq!(&tmp, b"[drain-tcp:1|tcp]keepalive");

    udp_tx
        .send(Datagram {
            flow: FlowKey::default(),
            payload: Bytes::from_static(b"keepalive-udp"),
        })
        .unwrap();

    // Shutdown Tunnel Coordinator: must close Idle streams, drain active streams, and leak zero transports.
    coord.graceful_stop().await.unwrap();
    let summary = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("tunnel shutdown must complete within drain timeout")
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(summary.completed_jobs.len(), 2);

    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        conn.live_streams.load(Ordering::Acquire),
        0,
        "all shared Idle and Active streams must be reclaimed on tunnel shutdown"
    );
}
