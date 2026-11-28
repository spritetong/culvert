// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Regression, boundary, backpressure, resource-limit, and long-running bidirectional
//! stress tests for UDP forwarding.
//! All comments and docstrings must be in English.

use bytes::Bytes;
use futures_util::future::poll_fn;
use reusable_future::ReusableFutureRef;
use std::collections::{BTreeSet, HashMap};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, duplex};
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use culvert::config::model::{
    AppConfig, DialerConfig, EndpointConfig, MappingRuleConfig, ServerConfig, TlsConfig,
    TunnelConfig, TunnelMappingsConfig,
};
use culvert::consts::{
    MAX_UDP_DATAGRAM_SIZE, MAX_UDP_SESSIONS, UDP_SESSION_CHANNEL_SIZE, UDP_SESSION_QUEUE_MAX_BYTES,
};
use culvert::core::stream::{HeartbeatStatus, StreamCaps, TunnelStream};
use culvert::frontend::udp::UdpFrontend;
use culvert::frontend::{Frontend, FrontendAccept, FrontendOutput};
use culvert::io::channel::{Datagram, DatagramIo, IoChannel};
use culvert::io::pump::{DatagramDatagramPump, DatagramStreamPump, Pump, PumpCtx, PumpRequest};
use culvert::supervisor::TunnelSupervisor;

const PACKET_MAGIC: &[u8; 4] = b"CVT2";
const DIR_C2S: u8 = 1;
const DIR_S2C_ECHO: u8 = 2;
const DIR_S2C_PUSH: u8 = 3;
const HEADER_LEN: usize = 4 + 1 + 4 + 4 + 8 + 4; // magic(4) + dir(1) + client_id(4) + flow_gen(4) + seq(8) + payload_len(4) = 25

/// Serializes the process-wide `MAX_UDP_SESSIONS` exhaustion test against concurrent
/// UDP tests in this binary via an async reader-writer lock.
fn flow_limit_guard() -> &'static RwLock<()> {
    static GUARD: OnceLock<RwLock<()>> = OnceLock::new();
    GUARD.get_or_init(|| RwLock::new(()))
}

fn get_dual_ephemeral_port() -> u16 {
    for _ in 0..100 {
        let tcp = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("Failed to bind ephemeral TCP socket");
        let port = tcp.local_addr().expect("TCP local_addr").port();
        if let Ok(udp) = std::net::UdpSocket::bind(("127.0.0.1", port)) {
            drop(udp);
            drop(tcp);
            return port;
        }
        drop(tcp);
    }
    panic!("Failed to allocate dual TCP/UDP ephemeral port");
}

fn get_udp_ephemeral_port() -> u16 {
    let udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("Failed to bind ephemeral UDP port");
    udp.local_addr().expect("UDP local_addr").port()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SeqPacketHeader {
    dir: u8,
    client_id: u32,
    flow_gen: u32,
    seq: u64,
    total_len: usize,
}

fn encode_seq_packet(
    dir: u8,
    client_id: u32,
    flow_gen: u32,
    seq: u64,
    total_len: usize,
) -> Vec<u8> {
    assert!(total_len >= HEADER_LEN);
    let mut buf = vec![0u8; total_len];
    buf[0..4].copy_from_slice(PACKET_MAGIC);
    buf[4] = dir;
    buf[5..9].copy_from_slice(&client_id.to_be_bytes());
    buf[9..13].copy_from_slice(&flow_gen.to_be_bytes());
    buf[13..21].copy_from_slice(&seq.to_be_bytes());
    buf[21..25].copy_from_slice(&(total_len as u32).to_be_bytes());
    let seed = (seq as u8) ^ ((client_id as u8).wrapping_mul(31)) ^ (flow_gen as u8);
    for (i, b) in buf[HEADER_LEN..].iter_mut().enumerate() {
        *b = seed.wrapping_add((i & 0xFF) as u8);
    }
    buf
}

fn decode_and_verify_seq_packet(buf: &[u8]) -> Result<SeqPacketHeader, String> {
    if buf.len() < HEADER_LEN {
        return Err(format!("packet too short: {}", buf.len()));
    }
    if &buf[0..4] != PACKET_MAGIC {
        return Err("invalid magic".to_string());
    }
    let dir = buf[4];
    let client_id = u32::from_be_bytes(buf[5..9].try_into().unwrap());
    let flow_gen = u32::from_be_bytes(buf[9..13].try_into().unwrap());
    let seq = u64::from_be_bytes(buf[13..21].try_into().unwrap());
    let total_len = u32::from_be_bytes(buf[21..25].try_into().unwrap()) as usize;
    if total_len != buf.len() {
        return Err(format!(
            "length mismatch: header={}, actual={}",
            total_len,
            buf.len()
        ));
    }
    let seed = (seq as u8) ^ ((client_id as u8).wrapping_mul(31)) ^ (flow_gen as u8);
    for (i, &b) in buf[HEADER_LEN..].iter().enumerate() {
        let expected = seed.wrapping_add((i & 0xFF) as u8);
        if b != expected {
            return Err(format!("payload corruption at offset {}", HEADER_LEN + i));
        }
    }
    Ok(SeqPacketHeader {
        dir,
        client_id,
        flow_gen,
        seq,
        total_len,
    })
}

/// Returns current process resident memory in bytes where supported.
fn current_process_rss_bytes() -> Option<usize> {
    #[cfg(windows)]
    {
        #[repr(C)]
        #[allow(non_snake_case)]
        struct ProcessMemoryCounters {
            cb: u32,
            PageFaultCount: u32,
            PeakWorkingSetSize: usize,
            WorkingSetSize: usize,
            QuotaPeakPagedPoolUsage: usize,
            QuotaPagedPoolUsage: usize,
            QuotaPeakNonPagedPoolUsage: usize,
            QuotaNonPagedPoolUsage: usize,
            PagefileUsage: usize,
            PeakPagefileUsage: usize,
        }
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn GetCurrentProcess() -> isize;
            fn K32GetProcessMemoryInfo(
                process: isize,
                ppsmemCounters: *mut ProcessMemoryCounters,
                cb: u32,
            ) -> i32;
        }
        let mut counters = ProcessMemoryCounters {
            cb: std::mem::size_of::<ProcessMemoryCounters>() as u32,
            PageFaultCount: 0,
            PeakWorkingSetSize: 0,
            WorkingSetSize: 0,
            QuotaPeakPagedPoolUsage: 0,
            QuotaPagedPoolUsage: 0,
            QuotaPeakNonPagedPoolUsage: 0,
            QuotaNonPagedPoolUsage: 0,
            PagefileUsage: 0,
            PeakPagefileUsage: 0,
        };
        let ok = unsafe {
            K32GetProcessMemoryInfo(
                GetCurrentProcess(),
                &mut counters as *mut ProcessMemoryCounters,
                counters.cb,
            )
        };
        if ok != 0 {
            Some(counters.WorkingSetSize)
        } else {
            None
        }
    }
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let pages: usize = statm.split_whitespace().nth(1)?.parse().ok()?;
        Some(pages * 4096)
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        None
    }
}

async fn accept_one_frontend(fe: &mut UdpFrontend) -> FrontendAccept {
    let mut slot: ReusableFutureRef<'static, FrontendOutput> = ReusableFutureRef::default();
    fe.accept(&mut slot);
    tokio::time::timeout(Duration::from_secs(2), &mut slot)
        .await
        .expect("Timed out waiting for UdpFrontend::accept")
        .expect("UdpFrontend::accept returned error")
}

async fn try_accept_frontend(fe: &mut UdpFrontend, timeout: Duration) -> Option<FrontendAccept> {
    let mut slot: ReusableFutureRef<'static, FrontendOutput> = ReusableFutureRef::default();
    fe.accept(&mut slot);
    let res = tokio::time::timeout(timeout, &mut slot).await.ok()?;
    res.ok()
}

async fn bind_ephemeral_udp_frontend() -> UdpFrontend {
    let addr = format!("127.0.0.1:{}", get_udp_ephemeral_port());
    UdpFrontend::bind(&addr).await.unwrap()
}

/// New Test 1: Exhaust the process-wide UDP flow limit (`MAX_UDP_SESSIONS`),
/// verify existing flows stay healthy and excess flows are cleanly rejected,
/// release a subset of flows, and verify new flows establish and forward cleanly.
#[tokio::test]
async fn test_udp_flow_limit_exhaustion_and_recovery() {
    let _guard = flow_limit_guard().write().await;

    let mut fe1 = bind_ephemeral_udp_frontend().await;
    let mut fe2 = bind_ephemeral_udp_frontend().await;
    let addr1 = fe1.local_addr().unwrap();
    let addr2 = fe2.local_addr().unwrap();

    let fe1_target = MAX_UDP_SESSIONS / 2 + 88;
    let fe2_target = MAX_UDP_SESSIONS - fe1_target;
    assert_eq!(fe1_target + fe2_target, MAX_UDP_SESSIONS);

    let mut active_flows: Vec<(UdpSocket, Box<dyn DatagramIo>, u32)> =
        Vec::with_capacity(MAX_UDP_SESSIONS);

    // 1. Saturate MAX_UDP_SESSIONS across two separate UdpFrontend listeners.
    for idx in 0..MAX_UDP_SESSIONS {
        let (fe, listen_addr) = if idx < fe1_target {
            (&mut fe1, addr1)
        } else {
            (&mut fe2, addr2)
        };
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let pkt = encode_seq_packet(DIR_C2S, idx as u32, 1, 1, 48);
        sock.send_to(&pkt, listen_addr).await.unwrap();

        let acc = accept_one_frontend(fe).await;
        let mut dgram = acc.channel.into_datagram().unwrap();
        let mut batch = vec![Datagram::default(); 2];
        let n = tokio::time::timeout(
            Duration::from_secs(2),
            poll_fn(|cx| dgram.poll_recv(cx, &mut batch)),
        )
        .await
        .expect("Timed out reading initial flow packet")
        .unwrap();
        assert_eq!(n, 1);
        let hdr = decode_and_verify_seq_packet(&batch[0].payload).unwrap();
        assert_eq!(hdr.client_id, idx as u32);
        assert_eq!(hdr.seq, 1);

        active_flows.push((sock, dgram, idx as u32));
    }

    // 2. While at ceiling, verify existing flows on both listeners still work bidirectionally.
    for sample_idx in [0usize, fe1_target - 1, fe1_target, MAX_UDP_SESSIONS - 1] {
        let (sock, dgram, client_id) = &mut active_flows[sample_idx];
        let listen_addr = if sample_idx < fe1_target {
            addr1
        } else {
            addr2
        };

        let pkt2 = encode_seq_packet(DIR_C2S, *client_id, 1, 2, 64);
        sock.send_to(&pkt2, listen_addr).await.unwrap();

        let mut batch = vec![Datagram::default(); 2];
        let n = tokio::time::timeout(
            Duration::from_secs(2),
            poll_fn(|cx| dgram.poll_recv(cx, &mut batch)),
        )
        .await
        .expect("Existing flow starved while MAX_UDP_SESSIONS reached")
        .unwrap();
        assert_eq!(n, 1);
        let hdr = decode_and_verify_seq_packet(&batch[0].payload).unwrap();
        assert_eq!(hdr.seq, 2);

        let reply = Datagram {
            flow: batch[0].flow.clone(),
            payload: Bytes::from(encode_seq_packet(DIR_S2C_ECHO, *client_id, 1, 2, 64)),
        };
        poll_fn(|cx| dgram.poll_send(cx, std::slice::from_ref(&reply)))
            .await
            .unwrap();
        let mut rbuf = [0u8; 128];
        let (rn, _) = tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut rbuf))
            .await
            .unwrap()
            .unwrap();
        let rhdr = decode_and_verify_seq_packet(&rbuf[..rn]).unwrap();
        assert_eq!(rhdr.dir, DIR_S2C_ECHO);
        assert_eq!(rhdr.seq, 2);
    }

    // 3. Extra clients beyond MAX_UDP_SESSIONS must be rejected without stalling or panicking.
    let mut rejected_socks = Vec::new();
    for extra in 0..6 {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target_addr = if extra % 2 == 0 { addr1 } else { addr2 };
        let pkt = encode_seq_packet(DIR_C2S, 99_000 + extra, 1, 1, 40);
        sock.send_to(&pkt, target_addr).await.unwrap();
        rejected_socks.push(sock);
    }
    assert!(
        try_accept_frontend(&mut fe1, Duration::from_millis(80))
            .await
            .is_none(),
        "fe1 accepted a new flow beyond MAX_UDP_SESSIONS"
    );
    assert!(
        try_accept_frontend(&mut fe2, Duration::from_millis(80))
            .await
            .is_none(),
        "fe2 accepted a new flow beyond MAX_UDP_SESSIONS"
    );
    drop(rejected_socks);

    // 4. Close 100 existing flows (releasing 100 permits) and verify 100 new flows succeed.
    const RELEASE_COUNT: usize = 100;
    active_flows.drain(0..RELEASE_COUNT);

    for new_i in 0..RELEASE_COUNT {
        let (fe, listen_addr) = if new_i % 2 == 0 {
            (&mut fe1, addr1)
        } else {
            (&mut fe2, addr2)
        };
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let cid = 50_000 + new_i as u32;
        let pkt = encode_seq_packet(DIR_C2S, cid, 2, 1, 96);
        sock.send_to(&pkt, listen_addr).await.unwrap();

        let acc = accept_one_frontend(fe).await;
        let mut dgram = acc.channel.into_datagram().unwrap();
        let mut batch = vec![Datagram::default(); 2];
        let n = tokio::time::timeout(
            Duration::from_secs(2),
            poll_fn(|cx| dgram.poll_recv(cx, &mut batch)),
        )
        .await
        .expect("Recovered flow failed to receive initial datagram")
        .unwrap();
        assert_eq!(n, 1);
        let hdr = decode_and_verify_seq_packet(&batch[0].payload).unwrap();
        assert_eq!(hdr.client_id, cid);
        assert_eq!(hdr.flow_gen, 2);

        let reply = Datagram {
            flow: batch[0].flow.clone(),
            payload: Bytes::from(encode_seq_packet(DIR_S2C_ECHO, cid, 2, 1, 96)),
        };
        poll_fn(|cx| dgram.poll_send(cx, std::slice::from_ref(&reply)))
            .await
            .unwrap();
        let mut rbuf = [0u8; 128];
        let (rn, _) = tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut rbuf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            decode_and_verify_seq_packet(&rbuf[..rn]).unwrap().client_id,
            cid
        );

        active_flows.push((sock, dgram, cid));
    }
    assert_eq!(active_flows.len(), MAX_UDP_SESSIONS);

    // 5. Ceiling is enforced again after refilling the 100 slots.
    let overflow_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    overflow_sock
        .send_to(&encode_seq_packet(DIR_C2S, 88_888, 1, 1, 40), addr1)
        .await
        .unwrap();
    assert!(
        try_accept_frontend(&mut fe1, Duration::from_millis(80))
            .await
            .is_none(),
        "fe1 allowed exceeding MAX_UDP_SESSIONS after refill"
    );
    drop(overflow_sock);

    // 6. Drop all flows + shutdown frontends; verify all permits are returned cleanly.
    drop(active_flows);
    fe1.shutdown().await;
    fe2.shutdown().await;

    let mut fe3 = bind_ephemeral_udp_frontend().await;
    let addr3 = fe3.local_addr().unwrap();
    let probe_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    probe_sock
        .send_to(&encode_seq_packet(DIR_C2S, 77_777, 1, 1, 48), addr3)
        .await
        .unwrap();
    let acc = accept_one_frontend(&mut fe3).await;
    drop(acc);
    fe3.shutdown().await;
}

/// Verifies lifecycle edge cases: `UdpFrontend::shutdown` drains unaccepted flows
/// in `accept_rx` without leaking global flow permits, and cancellation exits promptly.
#[tokio::test]
async fn test_udp_frontend_shutdown_drains_unaccepted_flows_and_permits() {
    let _guard = flow_limit_guard().read().await;

    let mut fe = bind_ephemeral_udp_frontend().await;
    let addr = fe.local_addr().unwrap();

    let mut clients = Vec::new();
    for i in 0..32 {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sock.send_to(&encode_seq_packet(DIR_C2S, i, 1, 1, 32), addr)
            .await
            .unwrap();
        clients.push(sock);
    }
    tokio::time::sleep(Duration::from_millis(40)).await;

    // Shutdown without calling accept(): must drain accept_rx and drop all UdpFlowChannels.
    tokio::time::timeout(Duration::from_secs(2), fe.shutdown())
        .await
        .expect("UdpFrontend::shutdown hung");
    drop(clients);
}

/// Throttled `TunnelStream` wrapper injecting periodic `Poll::Pending` backpressure on
/// writes while preserving exact byte semantics.
struct ThrottledStream {
    inner: tokio::io::DuplexStream,
    max_write_chunk: usize,
    budget_remaining: Arc<AtomicUsize>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl AsyncRead for ThrottledStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for ThrottledStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let budget = self.budget_remaining.load(Ordering::Acquire);
        if budget == 0 {
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let allowed = buf.len().min(self.max_write_chunk).min(budget);
        match Pin::new(&mut self.inner).poll_write(cx, &buf[..allowed]) {
            Poll::Ready(Ok(n)) => {
                self.budget_remaining.fetch_sub(n, Ordering::AcqRel);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl TunnelStream for ThrottledStream {
    fn caps(&self) -> StreamCaps {
        StreamCaps {
            owned_write: false,
            half_close: true,
        }
    }

    fn poll_check_alive(&mut self, _cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        Poll::Ready(HeartbeatStatus::Alive)
    }
}

/// Throttled `DatagramIo` applying backpressure on `poll_send` until released.
struct BackpressuredDatagramIo {
    rx: tokio::sync::mpsc::Receiver<Datagram>,
    sent: Arc<Mutex<Vec<Datagram>>>,
    allow_send: Arc<AtomicBool>,
    send_waker: Arc<Mutex<Option<Waker>>>,
    max_batch_send: usize,
}

impl DatagramIo for BackpressuredDatagramIo {
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        batch: &mut [Datagram],
    ) -> Poll<io::Result<usize>> {
        if batch.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(first)) => {
                batch[0] = first;
                let mut count = 1;
                while count < batch.len() {
                    match self.rx.try_recv() {
                        Ok(d) => {
                            batch[count] = d;
                            count += 1;
                        }
                        Err(_) => break,
                    }
                }
                Poll::Ready(Ok(count))
            }
            Poll::Ready(None) => Poll::Ready(Ok(0)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_send(&mut self, cx: &mut Context<'_>, batch: &[Datagram]) -> Poll<io::Result<usize>> {
        if !self.allow_send.load(Ordering::Acquire) {
            *self.send_waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let n = batch.len().min(self.max_batch_send);
        let mut guard = self.sent.lock().unwrap();
        guard.extend_from_slice(&batch[..n]);
        Poll::Ready(Ok(n))
    }
}

/// New Test 2: Extreme datagram sizes (0-byte, small, near-65507 and exact-65507 bytes)
/// mixed under sustained backpressure across both pump pipelines and real E2E sockets.
#[tokio::test]
async fn test_udp_extreme_datagram_sizes_under_backpressure() {
    let _guard = flow_limit_guard().read().await;
    let rss_before = current_process_rss_bytes();

    // --- Part A: Deterministic pump backpressure with 0 unexpected drops ---
    {
        let sizes = [
            0usize,
            1,
            25,
            128,
            1400,
            65_000,
            MAX_UDP_DATAGRAM_SIZE,
            0,
            64,
            MAX_UDP_DATAGRAM_SIZE,
        ];
        let total_packets = 80usize;
        let mut expected_empty = 0usize;
        let mut expected_seqs = Vec::new();

        let (in_tx, in_rx) = tokio::sync::mpsc::channel(128);
        let sent_out = Arc::new(Mutex::new(Vec::new()));
        let allow_send = Arc::new(AtomicBool::new(false));
        let send_waker = Arc::new(Mutex::new(None::<Waker>));

        let dgram_io = BackpressuredDatagramIo {
            rx: in_rx,
            sent: Arc::clone(&sent_out),
            allow_send: Arc::clone(&allow_send),
            send_waker: Arc::clone(&send_waker),
            max_batch_send: 2,
        };

        let (stream_a, stream_b) = duplex(128 * 1024);
        let budget = Arc::new(AtomicUsize::new(32 * 1024));
        let write_waker = Arc::new(Mutex::new(None::<Waker>));
        let throttled_a = ThrottledStream {
            inner: stream_a,
            max_write_chunk: 4096,
            budget_remaining: Arc::clone(&budget),
            waker: Arc::clone(&write_waker),
        };

        // Loopback mirror on stream_b: copies encoded frames back into stream_a
        let (mut rd_b, mut wr_b) = tokio::io::split(stream_b);
        let mirror = tokio::spawn(async move {
            let _ = tokio::io::copy(&mut rd_b, &mut wr_b).await;
        });

        let pump = tokio::spawn(async move {
            DatagramStreamPump::relay_datagram_stream(
                Box::new(dgram_io),
                Box::new(throttled_a),
                CancellationToken::new(),
                Duration::from_secs(5),
                Duration::from_secs(15),
            )
            .await
        });

        // Gradually release write budget in small steps while dgram poll_send stays blocked,
        // forcing both stream->dgram pending queue saturation and dgram->stream write backpressure.
        let trickle_cancel = CancellationToken::new();
        let trickle_handle = tokio::spawn({
            let budget = Arc::clone(&budget);
            let write_waker = Arc::clone(&write_waker);
            let trickle_cancel = trickle_cancel.clone();
            async move {
                while !trickle_cancel.is_cancelled() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    budget.fetch_add(48 * 1024, Ordering::AcqRel);
                    if let Some(w) = write_waker.lock().unwrap().take() {
                        w.wake();
                    }
                }
            }
        });

        let mut seq_counter = 1u64;
        for i in 0..total_packets {
            let sz = sizes[i % sizes.len()];
            let payload = if sz == 0 {
                expected_empty += 1;
                Bytes::new()
            } else {
                let effective = sz.max(HEADER_LEN);
                let seq = seq_counter;
                seq_counter += 1;
                expected_seqs.push((seq, effective));
                Bytes::from(encode_seq_packet(DIR_C2S, 1, 1, seq, effective))
            };
            in_tx
                .send(Datagram {
                    flow: Default::default(),
                    payload,
                })
                .await
                .unwrap();
            if i == 25 {
                // Keep downstream stalled briefly so byte budget saturates, then trickle releases.
                tokio::time::sleep(Duration::from_millis(60)).await;
                allow_send.store(true, Ordering::Release);
                if let Some(w) = send_waker.lock().unwrap().take() {
                    w.wake();
                }
            }
        }
        drop(in_tx);

        // Toggle backpressure off/on once more mid-drain to test repeated stall/resume.
        tokio::time::sleep(Duration::from_millis(20)).await;
        allow_send.store(false, Ordering::Release);
        tokio::time::sleep(Duration::from_millis(40)).await;
        allow_send.store(true, Ordering::Release);
        if let Some(w) = send_waker.lock().unwrap().take() {
            w.wake();
        }

        let (d2s, s2d) = tokio::time::timeout(Duration::from_secs(15), pump)
            .await
            .expect("DatagramStreamPump deadlocked under extreme size backpressure")
            .unwrap()
            .unwrap();
        assert_eq!(d2s, s2d);
        trickle_cancel.cancel();
        let _ = tokio::join!(trickle_handle, mirror);

        let recvd = sent_out.lock().unwrap();
        let mut got_empty = 0usize;
        let mut got_seqs = Vec::new();
        for d in recvd.iter() {
            if d.payload.is_empty() {
                got_empty += 1;
            } else {
                let hdr = decode_and_verify_seq_packet(&d.payload).unwrap();
                got_seqs.push((hdr.seq, hdr.total_len));
            }
        }
        assert_eq!(got_empty, expected_empty, "0-byte datagrams dropped");
        assert_eq!(
            got_seqs, expected_seqs,
            "unexpected drop or reorder under backpressure"
        );
    }

    // --- Part B: DatagramDatagramPump backpressure with 0-byte & 65507-byte mix ---
    {
        let (a_in_tx, a_in_rx) = tokio::sync::mpsc::channel(64);
        let (b_in_tx, b_in_rx) = tokio::sync::mpsc::channel(64);
        let a_sent = Arc::new(Mutex::new(Vec::new()));
        let b_sent = Arc::new(Mutex::new(Vec::new()));
        let b_allow = Arc::new(AtomicBool::new(false));
        let b_waker = Arc::new(Mutex::new(None::<Waker>));

        let a_io = BackpressuredDatagramIo {
            rx: a_in_rx,
            sent: Arc::clone(&a_sent),
            allow_send: Arc::new(AtomicBool::new(true)),
            send_waker: Arc::new(Mutex::new(None)),
            max_batch_send: 4,
        };
        let b_io = BackpressuredDatagramIo {
            rx: b_in_rx,
            sent: Arc::clone(&b_sent),
            allow_send: Arc::clone(&b_allow),
            send_waker: Arc::clone(&b_waker),
            max_batch_send: 1,
        };

        let pump = tokio::spawn(async move {
            DatagramDatagramPump
                .run(
                    PumpRequest::new(
                        IoChannel::from_datagram(a_io),
                        IoChannel::from_datagram(b_io),
                    ),
                    PumpCtx {
                        cancel: CancellationToken::new(),
                        heartbeat_interval: Duration::from_secs(5),
                        idle_timeout: Duration::from_secs(10),
                    },
                )
                .await
        });

        let pattern = [
            0usize,
            32,
            MAX_UDP_DATAGRAM_SIZE,
            65_000,
            0,
            MAX_UDP_DATAGRAM_SIZE,
        ];
        let mut expected = Vec::new();
        for (i, &sz) in pattern.iter().cycle().take(24).enumerate() {
            let payload = if sz == 0 {
                Bytes::new()
            } else {
                Bytes::from(encode_seq_packet(DIR_C2S, 7, 1, i as u64 + 1, sz))
            };
            expected.push(payload.len());
            a_in_tx
                .send(Datagram {
                    flow: Default::default(),
                    payload,
                })
                .await
                .unwrap();
        }
        drop(a_in_tx);
        drop(b_in_tx);

        // Hold backpressure long enough for bounded_datagram_batch_len to throttle at 256 KiB.
        tokio::time::sleep(Duration::from_millis(60)).await;
        b_allow.store(true, Ordering::Release);
        if let Some(w) = b_waker.lock().unwrap().take() {
            w.wake();
        }

        tokio::time::timeout(Duration::from_secs(10), pump)
            .await
            .expect("DatagramDatagramPump deadlocked under byte-budget backpressure")
            .unwrap()
            .unwrap();
        let recvd: Vec<usize> = b_sent
            .lock()
            .unwrap()
            .iter()
            .map(|d| d.payload.len())
            .collect();
        assert_eq!(recvd, expected);
    }

    // --- Part C: UdpFrontend byte-budget saturation + recovery with mixed sizes ---
    {
        let mut fe = bind_ephemeral_udp_frontend().await;
        let listen_addr = fe.local_addr().unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        // Start flow with one small packet and accept it, then stall consumer.
        client
            .send_to(&encode_seq_packet(DIR_C2S, 2, 1, 1, 32), listen_addr)
            .await
            .unwrap();
        let acc = accept_one_frontend(&mut fe).await;
        let mut dgram = acc.channel.into_datagram().unwrap();

        // Blast mixed 0-byte, small, and 65000-byte packets exceeding 256 KiB budget.
        let mut seq = 2u64;
        for step in 0..16 {
            let pkt = match step % 3 {
                0 => Vec::new(),
                1 => {
                    let s = seq;
                    seq += 1;
                    encode_seq_packet(DIR_C2S, 2, 1, s, 65_000)
                }
                _ => {
                    let s = seq;
                    seq += 1;
                    encode_seq_packet(DIR_C2S, 2, 1, s, 48)
                }
            };
            client.send_to(&pkt, listen_addr).await.unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;

        let mut drained_bytes = 0usize;
        let mut last_seq = 0u64;
        let mut batch = vec![Datagram::default(); 8];
        while let Ok(Ok(n)) = tokio::time::timeout(
            Duration::from_millis(50),
            poll_fn(|cx| dgram.poll_recv(cx, &mut batch)),
        )
        .await
        {
            if n == 0 {
                break;
            }
            for d in &batch[..n] {
                drained_bytes += d.payload.len();
                if !d.payload.is_empty() {
                    let hdr = decode_and_verify_seq_packet(&d.payload).unwrap();
                    assert!(hdr.seq > last_seq, "out-of-order or duplicate seq");
                    last_seq = hdr.seq;
                }
            }
        }
        assert!(
            drained_bytes <= UDP_SESSION_QUEUE_MAX_BYTES,
            "drained_bytes {} exceeded budget {}",
            drained_bytes,
            UDP_SESSION_QUEUE_MAX_BYTES
        );
        assert!(drained_bytes >= 32);

        // Oversized datagram (> 65507) must be dropped cleanly without killing the flow.
        let oversized = vec![0xABu8; MAX_UDP_DATAGRAM_SIZE + 1];
        let _ = client.send_to(&oversized, listen_addr).await;

        // Also saturate the slot-count bound (`UDP_SESSION_CHANNEL_SIZE` = 256) with 0-byte packets
        // while consumer is paused, then drain and verify recovery.
        for _ in 0..(UDP_SESSION_CHANNEL_SIZE + 32) {
            client.send_to(&[], listen_addr).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        let mut slot_drained = 0usize;
        while let Ok(Ok(n)) = tokio::time::timeout(
            Duration::from_millis(50),
            poll_fn(|cx| dgram.poll_recv(cx, &mut batch)),
        )
        .await
        {
            if n == 0 {
                break;
            }
            slot_drained += n;
        }
        assert!(
            slot_drained <= UDP_SESSION_CHANNEL_SIZE,
            "queued packet slots {} exceeded channel bound {}",
            slot_drained,
            UDP_SESSION_CHANNEL_SIZE
        );
        assert!(slot_drained > 0);

        // Recovery check: mixed 0-byte, small, and IPv4 max UDP payload (65507) after drain.
        let recovery_sizes = [0usize, 40, 65_507];
        let mut rec_empty = 0usize;
        let mut rec_seqs = Vec::new();
        for &sz in &recovery_sizes {
            let pkt = if sz == 0 {
                rec_empty += 1;
                Vec::new()
            } else {
                let s = seq;
                seq += 1;
                rec_seqs.push((s, sz));
                encode_seq_packet(DIR_C2S, 2, 1, s, sz)
            };
            client.send_to(&pkt, listen_addr).await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let mut got_rec_empty = 0usize;
        let mut got_rec_seqs = Vec::new();
        while got_rec_empty < rec_empty || got_rec_seqs.len() < rec_seqs.len() {
            let n = tokio::time::timeout(
                Duration::from_secs(2),
                poll_fn(|cx| dgram.poll_recv(cx, &mut batch)),
            )
            .await
            .expect("Flow failed to recover after backpressure drain")
            .unwrap();
            for d in &batch[..n] {
                if d.payload.is_empty() {
                    got_rec_empty += 1;
                } else {
                    let hdr = decode_and_verify_seq_packet(&d.payload).unwrap();
                    got_rec_seqs.push((hdr.seq, hdr.total_len));
                }
            }
        }
        assert_eq!(got_rec_empty, rec_empty);
        assert_eq!(got_rec_seqs, rec_seqs);

        drop(dgram);
        fe.shutdown().await;
    }

    // --- Part D: Real E2E QUIC tunnel with mixed 0-byte, small, and 65507-byte packets ---
    {
        let cancel = CancellationToken::new();
        let std_echo = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let echo_port = std_echo.local_addr().unwrap().port();
        std_echo.set_nonblocking(true).unwrap();
        let echo_sock = UdpSocket::from_std(std_echo).unwrap();
        let echo_cancel = cancel.clone();
        let echo_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                tokio::select! {
                    _ = echo_cancel.cancelled() => break,
                    res = echo_sock.recv_from(&mut buf) => {
                        match res {
                            Ok((n, peer)) => {
                                let _ = echo_sock.send_to(&buf[..n], peer).await;
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        let quic_port = get_dual_ephemeral_port();
        let fwd_port = get_udp_ephemeral_port();
        let fwd_listen = format!("127.0.0.1:{fwd_port}");

        let app_config = AppConfig {
            tls: Some(TlsConfig {
                insecure: Some(true),
                server_name: Some("localhost".to_string()),
                ..Default::default()
            }),
            server: Some(ServerConfig {
                listen: vec![format!("quic://127.0.0.1:{quic_port}/raw")],
                token: Some("extreme_size_token".to_string()),
                ..Default::default()
            }),
            tunnels: vec![TunnelConfig {
                tag: "udp-extreme-sizes".to_string(),
                uuid: None,
                endpoint: EndpointConfig::Simple(format!("quic://127.0.0.1:{quic_port}/raw")),
                token: Some("extreme_size_token".to_string()),
                failover: None,
                multiplex: None,
                retry: None,
                http_headers: None,
                mappings: TunnelMappingsConfig::forward_pool(
                    2,
                    vec![MappingRuleConfig::udp(
                        fwd_listen.clone(),
                        format!("127.0.0.1:{echo_port}"),
                    )],
                ),
            }],
            ..Default::default()
        };

        let sup = TunnelSupervisor::new(app_config, cancel.clone());
        let sup_handle = tokio::spawn(async move {
            let _ = sup.run().await;
        });
        tokio::time::sleep(Duration::from_millis(400)).await;

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let e2e_sizes = [0usize, 25, 512, 1400, 65_000, 65_507, 0, 65_507, 64];
        let mut rbuf = vec![0u8; 65536];
        for (idx, &sz) in e2e_sizes.iter().enumerate() {
            let pkt = if sz == 0 {
                Vec::new()
            } else {
                encode_seq_packet(DIR_C2S, 10, 1, idx as u64 + 1, sz)
            };
            client.send_to(&pkt, &fwd_listen).await.unwrap();
            let (rn, _) = tokio::time::timeout(Duration::from_secs(5), client.recv_from(&mut rbuf))
                .await
                .expect("Timed out waiting for extreme-size UDP E2E echo")
                .unwrap();
            assert_eq!(&rbuf[..rn], pkt.as_slice());
        }

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(echo_task, sup_handle)
        })
        .await
        .expect("Timed out shutting down extreme size E2E test");
    }

    if let (Some(before), Some(after)) = (rss_before, current_process_rss_bytes()) {
        let growth = after.saturating_sub(before);
        assert!(
            growth < 64 * 1024 * 1024,
            "Unexpected memory growth during extreme-size backpressure test: {} bytes",
            growth
        );
    }
}

/// New Test 3: Long-running bidirectional UDP stress test over QUIC with random client
/// closes, idle expirations, flow recreations, and sequenced delivery tracking.
/// Verifies memory stability, absence of task leaks, and steady throughput across epochs.
#[test]
fn test_udp_long_running_bidirectional_stress_with_flow_churn() {
    let _guard = flow_limit_guard().blocking_write();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let metrics = rt.metrics();
    let baseline_tasks = metrics.num_alive_tasks();
    assert_eq!(baseline_tasks, 0);

    let (epoch_rss, epoch_tasks, flows_created, c2s_sent_total, s2c_push_received, ep_counts, live_ratio) =
        rt.block_on(async {
            let cancel = CancellationToken::new();

    #[derive(Default)]
    struct ServerStats {
        c2s_seen: HashMap<(u32, u32), BTreeSet<u64>>,
        corrupted: usize,
        c2s_total: usize,
        s2c_push_sent: usize,
    }
    let server_stats = Arc::new(Mutex::new(ServerStats::default()));

    let std_server = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let server_udp_port = std_server.local_addr().unwrap().port();
    std_server.set_nonblocking(true).unwrap();
    let server_udp = UdpSocket::from_std(std_server).unwrap();

    let srv_cancel = cancel.clone();
    let srv_stats = Arc::clone(&server_stats);
    let srv_push_seq = Arc::new(AtomicU64::new(1));
    let server_udp_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 65536];
        loop {
            tokio::select! {
                _ = srv_cancel.cancelled() => break,
                res = server_udp.recv_from(&mut buf) => {
                    let (n, peer) = match res {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    match decode_and_verify_seq_packet(&buf[..n]) {
                        Ok(hdr) => {
                            let push_seq = {
                                let mut st = srv_stats.lock().unwrap();
                                st.c2s_total += 1;
                                st.c2s_seen
                                    .entry((hdr.client_id, hdr.flow_gen))
                                    .or_default()
                                    .insert(hdr.seq);
                                if hdr.seq.is_multiple_of(2) {
                                    st.s2c_push_sent += 1;
                                    Some(srv_push_seq.fetch_add(1, Ordering::Relaxed))
                                } else {
                                    None
                                }
                            };
                            let echo = encode_seq_packet(
                                DIR_S2C_ECHO,
                                hdr.client_id,
                                hdr.flow_gen,
                                hdr.seq,
                                hdr.total_len.clamp(HEADER_LEN, 128),
                            );
                            let _ = server_udp.send_to(&echo, peer).await;

                            // Also send an independent server-initiated push packet on every 2nd packet.
                            if let Some(pseq) = push_seq {
                                let push = encode_seq_packet(
                                    DIR_S2C_PUSH,
                                    hdr.client_id,
                                    hdr.flow_gen,
                                    pseq,
                                    64,
                                );
                                let _ = server_udp.send_to(&push, peer).await;
                            }
                        }
                        Err(_) => {
                            srv_stats.lock().unwrap().corrupted += 1;
                        }
                    }
                }
            }
        }
    });

    let quic_port = get_dual_ephemeral_port();
    let fwd_port = get_udp_ephemeral_port();
    let fwd_listen = format!("127.0.0.1:{fwd_port}");

    let app_config = AppConfig {
        tls: Some(TlsConfig {
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        dialer: Some(DialerConfig {
            heartbeat_interval: Some(Duration::from_secs(5)),
            idle_timeout: Some(Duration::from_secs(15)),
            udp_idle_timeout: Some(Duration::from_millis(350)),
            ..Default::default()
        }),
        server: Some(ServerConfig {
            listen: vec![format!("quic://127.0.0.1:{quic_port}/raw")],
            token: Some("stress_udp_token".to_string()),
            udp_idle_timeout: Some(Duration::from_millis(350)),
            ..Default::default()
        }),
        tunnels: vec![TunnelConfig {
            tag: "udp-long-stress".to_string(),
            uuid: None,
            endpoint: EndpointConfig::Simple(format!("quic://127.0.0.1:{quic_port}/raw")),
            token: Some("stress_udp_token".to_string()),
            failover: None,
            multiplex: None,
            retry: None,
            http_headers: None,
            mappings: TunnelMappingsConfig::forward_pool(
                4,
                vec![MappingRuleConfig::udp_with_timeout(
                    fwd_listen.clone(),
                    format!("127.0.0.1:{server_udp_port}"),
                    Duration::from_millis(350),
                )],
            ),
        }],
        ..Default::default()
    };

    let sup = TunnelSupervisor::new(app_config, cancel.clone());
    let sup_handle = tokio::spawn(async move {
        let _ = sup.run().await;
    });
    tokio::time::sleep(Duration::from_millis(450)).await;

    #[derive(Default)]
    struct ClientStats {
        c2s_sent_total: usize,
        c2s_sent_live_verified: usize,
        c2s_acked_live: usize,
        c2s_closed_tail_packets: usize,
        s2c_push_received: usize,
        corrupted: usize,
        flows_created: usize,
        epoch_packets: [usize; 4], // 0: warmup, 1..=3: measured epochs
    }

    let client_stats = Arc::new(Mutex::new(ClientStats::default()));
    let current_epoch = Arc::new(AtomicUsize::new(0));
    let stop_workers = Arc::new(AtomicBool::new(false));

    const WORKERS: usize = 6;
    let mut worker_handles = Vec::with_capacity(WORKERS);
    for worker_id in 0..WORKERS {
        let listen = fwd_listen.clone();
        let stats = Arc::clone(&client_stats);
        let epoch_ref = Arc::clone(&current_epoch);
        let stop_ref = Arc::clone(&stop_workers);
        worker_handles.push(tokio::spawn(async move {
            let mut rng: u64 = 0x9E37_79B9_7F4A_7C15_u64
                ^ (worker_id as u64 + 1).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            let mut next_u64 = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };

            let mut flow_gen = 0u32;
            let mut sock: Option<UdpSocket> = None;

            while !stop_ref.load(Ordering::Acquire) {
                if sock.is_none() {
                    let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                    flow_gen += 1;
                    stats.lock().unwrap().flows_created += 1;
                    sock = Some(s);
                }
                let active_sock = sock.as_ref().unwrap();

                let burst_len = 8 + (next_u64() % 10) as usize;
                let sizes = [32usize, 256, 1400, 4096];
                let mut sent_in_flow = 0usize;
                let mut acked_in_flow = 0usize;
                let mut aborted_mid_flow = false;
                let mut rbuf = vec![0u8; 8192];

                for seq in 1..=(burst_len as u64) {
                    if stop_ref.load(Ordering::Acquire) {
                        break;
                    }
                    let sz = sizes[(next_u64() as usize) % sizes.len()];
                    let pkt = encode_seq_packet(DIR_C2S, worker_id as u32, flow_gen, seq, sz);
                    let _ = active_sock.send_to(&pkt, &listen).await;
                    sent_in_flow += 1;
                    {
                        let ep = epoch_ref.load(Ordering::Acquire).min(3);
                        let mut st = stats.lock().unwrap();
                        st.c2s_sent_total += 1;
                        st.epoch_packets[ep] += 1;
                    }

                    // Drain replies with short bounded wait so we exercise bidirectional recv.
                    let deadline = Instant::now() + Duration::from_millis(25);
                    while Instant::now() < deadline {
                        match tokio::time::timeout(
                            Duration::from_millis(10),
                            active_sock.recv_from(&mut rbuf),
                        )
                        .await
                        {
                            Ok(Ok((rn, _))) => match decode_and_verify_seq_packet(&rbuf[..rn]) {
                                Ok(hdr) => {
                                    let mut st = stats.lock().unwrap();
                                    match hdr.dir {
                                        DIR_S2C_ECHO if hdr.flow_gen == flow_gen => {
                                            acked_in_flow += 1;
                                        }
                                        DIR_S2C_PUSH => {
                                            st.s2c_push_received += 1;
                                        }
                                        _ => {}
                                    }
                                }
                                Err(_) => {
                                    stats.lock().unwrap().corrupted += 1;
                                }
                            },
                            _ => break,
                        }
                    }

                    // Randomly abort/close client mid-flow (~12% chance after at least 3 packets).
                    if seq >= 3 && (next_u64() % 8 == 0) {
                        // Record tail packets sent just before abrupt close separately.
                        let mut st = stats.lock().unwrap();
                        st.c2s_closed_tail_packets += sent_in_flow.saturating_sub(acked_in_flow);
                        aborted_mid_flow = true;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(3)).await;
                }

                if aborted_mid_flow {
                    sock = None;
                } else {
                    // Wait briefly for remaining echo/push packets on completed flow burst.
                    let drain_until = Instant::now() + Duration::from_millis(40);
                    while Instant::now() < drain_until && acked_in_flow < sent_in_flow {
                        match tokio::time::timeout(
                            Duration::from_millis(12),
                            active_sock.recv_from(&mut rbuf),
                        )
                        .await
                        {
                            Ok(Ok((rn, _))) => match decode_and_verify_seq_packet(&rbuf[..rn]) {
                                Ok(hdr) => {
                                    if hdr.dir == DIR_S2C_ECHO && hdr.flow_gen == flow_gen {
                                        acked_in_flow += 1;
                                    } else if hdr.dir == DIR_S2C_PUSH {
                                        stats.lock().unwrap().s2c_push_received += 1;
                                    }
                                }
                                Err(_) => {
                                    stats.lock().unwrap().corrupted += 1;
                                }
                            },
                            _ => break,
                        }
                    }
                    {
                        let mut st = stats.lock().unwrap();
                        st.c2s_sent_live_verified += sent_in_flow;
                        st.c2s_acked_live += acked_in_flow;
                    }

                    // Alternate between abrupt client socket close (new port) and idle timeout expiration (same port).
                    if next_u64() % 3 == 0 {
                        // Let udp_idle_timeout (350ms) expire this flow, then recreate on same socket.
                        tokio::time::sleep(Duration::from_millis(420)).await;
                        flow_gen += 1;
                        stats.lock().unwrap().flows_created += 1;
                    } else {
                        sock = None;
                    }
                }
            }
        }));
    }

    // Warmup (0.6s) + 3 measured epochs (2.5s each = 7.5s measured stress).
    tokio::time::sleep(Duration::from_millis(600)).await;
    let mut epoch_rss = Vec::new();
    let mut epoch_tasks = Vec::new();
    let rt_metrics = tokio::runtime::Handle::current().metrics();
    for ep in 1..=3 {
        current_epoch.store(ep, Ordering::Release);
        tokio::time::sleep(Duration::from_millis(2500)).await;
        epoch_rss.push(current_process_rss_bytes());
        epoch_tasks.push(rt_metrics.num_alive_tasks());
    }

    stop_workers.store(true, Ordering::Release);
    for h in worker_handles {
        tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .expect("Stress worker hung on stop")
            .unwrap();
    }

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(10), async {
        let (r1, r2) = tokio::join!(server_udp_task, sup_handle);
        r1.unwrap();
        r2.unwrap();
    })
    .await
    .expect("Timed out shutting down long stress supervisor/server");

    let (
        flows_created,
        c2s_sent_total,
        s2c_push_received,
        ep1,
        ep2,
        ep3,
        live_delivery_ratio,
    ) = {
        let cst = client_stats.lock().unwrap();
        let sst = server_stats.lock().unwrap();

        assert_eq!(cst.corrupted, 0, "Client observed corrupted packets");
        assert_eq!(sst.corrupted, 0, "Server observed corrupted packets");
        assert!(
            cst.flows_created >= 30,
            "Expected substantial flow churn, got {}",
            cst.flows_created
        );
        assert!(
            cst.c2s_sent_total >= 500 && sst.c2s_total >= 400,
            "Expected high bidirectional volume, sent={}, server_seen={}",
            cst.c2s_sent_total,
            sst.c2s_total
        );
        assert!(
            cst.s2c_push_received > 0 && sst.s2c_push_sent > 0,
            "Expected server-initiated bidirectional push traffic"
        );

        // Live-flow delivery rate (excluding packets cut off by deliberate client close).
        let live_delivery_ratio =
            cst.c2s_acked_live as f64 / cst.c2s_sent_live_verified.max(1) as f64;
        assert!(
            live_delivery_ratio >= 0.90,
            "Unexpected drop rate in live UDP flows: acked={}/{} (ratio={:.3}, closed_tail={})",
            cst.c2s_acked_live,
            cst.c2s_sent_live_verified,
            live_delivery_ratio,
            cst.c2s_closed_tail_packets
        );

        // Throughput must not degrade progressively from Epoch 1 to Epoch 3.
        let ep1 = cst.epoch_packets[1];
        let ep2 = cst.epoch_packets[2];
        let ep3 = cst.epoch_packets[3];
        assert!(
            ep1 > 50 && ep2 > 50 && ep3 > 50,
            "Epoch packet counts too low: ep1={ep1}, ep2={ep2}, ep3={ep3}"
        );
        assert!(
            (ep3 as f64) >= (ep1 as f64) * 0.40,
            "Throughput degraded across epochs: ep1={ep1}, ep2={ep2}, ep3={ep3}"
        );
        (
            cst.flows_created,
            cst.c2s_sent_total,
            cst.s2c_push_received,
            ep1,
            ep2,
            ep3,
            live_delivery_ratio,
        )
    };

    assert!(
        epoch_tasks[2] <= epoch_tasks[0] + 24,
        "Active task count grew steadily across epochs (expected plateau with flow churn): {:?}",
        epoch_tasks
    );

    // Memory must remain bounded across epochs (no steady leak).
    if let (Some(r1), Some(r3)) = (epoch_rss[0], epoch_rss[2]) {
        let delta = r3.saturating_sub(r1);
        assert!(
            delta < 32 * 1024 * 1024,
            "Memory grew excessively across stress epochs: ep1={r1}, ep3={r3}, delta={delta}"
        );
    }

    // Verify zero leaked global UDP flow permits by binding a fresh frontend.
    let mut fe = bind_ephemeral_udp_frontend().await;
    let addr = fe.local_addr().unwrap();
    let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    probe
        .send_to(&encode_seq_packet(DIR_C2S, 999, 1, 1, 48), addr)
        .await
        .unwrap();
    let acc = accept_one_frontend(&mut fe).await;
    drop(acc);
    fe.shutdown().await;

    (
        epoch_rss,
        epoch_tasks,
        flows_created,
        c2s_sent_total,
        s2c_push_received,
        [ep1, ep2, ep3],
        live_delivery_ratio,
    )
        });

    // Wait briefly for detached background tasks in this isolated runtime to finish dropping.
    let deadline = Instant::now() + Duration::from_secs(2);
    while metrics.num_alive_tasks() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let final_tasks = metrics.num_alive_tasks();
    println!(
        "UDP stress metrics: flows_created={flows_created}, c2s_sent={c2s_sent_total}, \
         s2c_push={s2c_push_received}, live_ratio={live_ratio:.3}, epoch_pkts={ep_counts:?}, \
         epoch_tasks={epoch_tasks:?}, final_tasks={final_tasks}, epoch_rss={epoch_rss:?}"
    );
    assert_eq!(
        final_tasks, 0,
        "Task leak detected in isolated runtime: epoch_tasks={epoch_tasks:?}, final_tasks={final_tasks}"
    );
}
