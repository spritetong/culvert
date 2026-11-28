// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! UDP datagram frontend implementing peer-based NAT flow demultiplexing.
//! All comments and docstrings must be in English.

use bytes::Bytes;
use parking_lot::Mutex;
use reusable_future::ReusableFutureRef;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::common::CancelFutureExt;
use crate::consts::{
    ACCEPTED_STREAM_QUEUE_SIZE, MAX_UDP_DATAGRAM_SIZE, MAX_UDP_SESSIONS, UDP_SESSION_CHANNEL_SIZE,
    UDP_SESSION_QUEUE_MAX_BYTES,
};
use crate::frontend::{
    AcceptBackoff, Frontend, FrontendAccept, FrontendFactory, FrontendOutput,
    is_unrecoverable_io_error,
};
use crate::io::addr::{L4Protocol, ListenAddr, MappingSpec, PeerAddr, parse_listen_addr};
use crate::io::channel::{Datagram, DatagramIo, FlowKey, IoChannel};
use crate::io::error::TunnelError;
use crate::io::stream::BoxFuture;

/// Session table mapping client `SocketAddr` -> `(flow_id, inbound_sender)`.
type UdpSessionMap = HashMap<SocketAddr, (u64, mpsc::Sender<Datagram>, Arc<AtomicUsize>)>;

fn try_reserve_session_bytes(queued: &AtomicUsize, amount: usize) -> bool {
    queued
        .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current
                .checked_add(amount)
                .filter(|next| *next <= UDP_SESSION_QUEUE_MAX_BYTES)
        })
        .is_ok()
}

fn release_session_bytes(queued: &AtomicUsize, amount: usize) {
    let result = queued.try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        current.checked_sub(amount)
    });
    debug_assert!(result.is_ok(), "UDP session byte accounting underflow");
}

/// Process-wide semaphore bounding active UDP peer flows across all listeners.
fn udp_session_semaphore() -> &'static Arc<tokio::sync::Semaphore> {
    static LIMIT: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    LIMIT.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(MAX_UDP_SESSIONS)))
}

/// Registered frontend factory for UDP port mappings.
pub struct UdpFrontendFactory;

impl FrontendFactory for UdpFrontendFactory {
    fn protocol(&self) -> L4Protocol {
        L4Protocol::Udp
    }

    fn bind(
        &self,
        spec: MappingSpec,
    ) -> BoxFuture<'static, Result<Box<dyn Frontend>, TunnelError>> {
        Box::pin(async move {
            let frontend = UdpFrontend::bind(&spec.listen).await?;
            Ok(Box::new(frontend) as Box<dyn Frontend>)
        })
    }
}

/// UDP frontend listening on one bound socket and yielding per-peer `DatagramIo` flows.
pub struct UdpFrontend {
    socket: Arc<UdpSocket>,
    sessions: Arc<Mutex<UdpSessionMap>>,
    accept_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<FrontendOutput>>>,
    cancel_token: CancellationToken,
    recv_task: Option<tokio::task::JoinHandle<()>>,
}

async fn handle_udp_recv_error(
    err: io::Error,
    backoff: &mut AcceptBackoff,
    accept_tx: &mpsc::Sender<FrontendOutput>,
    cancel: &CancellationToken,
) -> std::ops::ControlFlow<()> {
    if cancel.is_cancelled() {
        return std::ops::ControlFlow::Break(());
    }
    if is_unrecoverable_io_error(&err) {
        tracing::error!(error = %err, "UDP frontend recv_from failed permanently");
        let _ = accept_tx
            .send(Err(TunnelError::Io(err)))
            .cancel_with(cancel)
            .await;
        return std::ops::ControlFlow::Break(());
    }
    let delay = backoff.next_delay();
    tracing::debug!(error = %err, backoff = ?delay, "UDP frontend recv_from error; backing off");
    if backoff.sleep_cancel(delay, cancel).await {
        std::ops::ControlFlow::Continue(())
    } else {
        std::ops::ControlFlow::Break(())
    }
}

impl Drop for UdpFrontend {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.sessions.lock().clear();
    }
}

impl UdpFrontend {
    /// Binds a UDP socket at `listen_addr` and starts the packet demultiplexer task.
    /// Supports dual-stack `:port`, standard `addr:port`, and `hostname:port`.
    pub async fn bind(listen_addr: &str) -> Result<Self, TunnelError> {
        let listen_target = parse_listen_addr(listen_addr)?;
        let socket = match listen_target {
            ListenAddr::DualStack(port) => {
                let std_sock =
                    crate::platform::create_dual_stack_udp_socket(port).map_err(TunnelError::Io)?;
                std_sock.set_nonblocking(true).map_err(TunnelError::Io)?;
                UdpSocket::from_std(std_sock).map_err(TunnelError::Io)?
            }
            ListenAddr::Ip(sa) => UdpSocket::bind(sa).await.map_err(TunnelError::Io)?,
            ListenAddr::Domain(host, port) => UdpSocket::bind((host.as_str(), port))
                .await
                .map_err(TunnelError::Io)?,
            ListenAddr::Path(_) => {
                return Err(TunnelError::Config(format!(
                    "UDP frontend does not support Unix domain socket address '{}'",
                    listen_addr
                )));
            }
        };

        let socket = Arc::new(socket);
        let sessions: Arc<Mutex<UdpSessionMap>> = Arc::new(Mutex::new(HashMap::new()));
        let (accept_tx, accept_rx) = mpsc::channel(ACCEPTED_STREAM_QUEUE_SIZE);
        let cancel_token = CancellationToken::new();

        let task_socket = Arc::clone(&socket);
        let task_sessions = Arc::clone(&sessions);
        let task_cancel = cancel_token.clone();

        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            let mut next_flow_id = 1u64;
            let mut recv_backoff = AcceptBackoff::default();

            loop {
                let recv_res = tokio::select! {
                    biased;
                    _ = task_cancel.cancelled() => break,
                    res = task_socket.recv_from(&mut buf) => res,
                };

                let (n, raw_peer_addr) = match recv_res {
                    Ok(pair) => {
                        recv_backoff.reset();
                        pair
                    }
                    Err(e) => {
                        if handle_udp_recv_error(e, &mut recv_backoff, &accept_tx, &task_cancel)
                            .await
                            .is_break()
                        {
                            break;
                        }
                        continue;
                    }
                };

                if n > MAX_UDP_DATAGRAM_SIZE {
                    tracing::debug!(size = n, "dropping oversized UDP datagram");
                    continue;
                }

                let peer_addr = match raw_peer_addr {
                    SocketAddr::V6(v6) => SocketAddr::new(v6.ip().to_canonical(), v6.port()),
                    v4 => v4,
                };

                let dgram = Datagram {
                    flow: FlowKey {
                        peer: PeerAddr::Socket(peer_addr),
                        // `UdpFlowChannel` is per-peer flow-scoped; avoid cloning TargetAddr per packet.
                        target: None,
                        protocol: L4Protocol::Udp,
                    },
                    payload: Bytes::copy_from_slice(&buf[..n]),
                };

                // Look up existing session sender without holding the mutex across awaits.
                let existing_session = {
                    let guard = task_sessions.lock();
                    guard
                        .get(&peer_addr)
                        .map(|(_, tx, queued)| (tx.clone(), Arc::clone(queued)))
                };

                let pending_first = if let Some((tx, queued_bytes)) = existing_session {
                    let payload_len = dgram.payload.len();
                    if !try_reserve_session_bytes(&queued_bytes, payload_len) {
                        continue;
                    }
                    match tx.try_send(dgram) {
                        Ok(()) => continue,
                        Err(mpsc::error::TrySendError::Full(returned)) => {
                            release_session_bytes(&queued_bytes, returned.payload.len());
                            // Backpressure drop-tail when the session queue is saturated.
                            continue;
                        }
                        Err(mpsc::error::TrySendError::Closed(returned)) => {
                            release_session_bytes(&queued_bytes, returned.payload.len());
                            Some(returned)
                        }
                    }
                } else {
                    Some(dgram)
                };

                if let Some(first_dgram) = pending_first {
                    if first_dgram.payload.len() > UDP_SESSION_QUEUE_MAX_BYTES {
                        continue;
                    }

                    // Reserve an accept-queue slot before acquiring a global permit or mutating
                    // the session table so a full accept queue never churns allocations or locks.
                    let accept_permit = match accept_tx.try_reserve() {
                        Ok(permit) => permit,
                        Err(mpsc::error::TrySendError::Full(())) => continue,
                        Err(mpsc::error::TrySendError::Closed(())) => break,
                    };

                    // One process-wide permit bounds sessions across all UDP listeners.
                    let flow_permit = match udp_session_semaphore().clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => continue,
                    };

                    let flow_id = next_flow_id;
                    next_flow_id = next_flow_id.wrapping_add(1).max(1);

                    let (flow_tx, flow_rx) = mpsc::channel(UDP_SESSION_CHANNEL_SIZE);
                    let queued_bytes = Arc::new(AtomicUsize::new(first_dgram.payload.len()));
                    if flow_tx.try_send(first_dgram).is_err() {
                        continue;
                    }

                    {
                        let mut guard = task_sessions.lock();
                        let stale = guard
                            .get(&peer_addr)
                            .is_some_and(|(_, sender, _)| sender.is_closed());
                        if stale {
                            guard.remove(&peer_addr);
                        }
                        guard.insert(peer_addr, (flow_id, flow_tx, Arc::clone(&queued_bytes)));
                    }

                    let channel = UdpFlowChannel {
                        socket: Arc::clone(&task_socket),
                        peer_addr,
                        flow_id,
                        _flow_permit: flow_permit,
                        rx: flow_rx,
                        queued_bytes,
                        sessions: Arc::clone(&task_sessions),
                    };

                    let accepted = FrontendAccept {
                        channel: IoChannel::from_datagram(channel),
                        peer: PeerAddr::Socket(peer_addr),
                    };

                    accept_permit.send(Ok(accepted));
                }
            }
        });

        Ok(Self {
            socket,
            sessions,
            accept_rx: Arc::new(tokio::sync::Mutex::new(accept_rx)),
            cancel_token,
            recv_task: Some(recv_task),
        })
    }

    /// Returns the actual local socket address bound by this frontend.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

impl Frontend for UdpFrontend {
    fn accept(&mut self, slot: &mut ReusableFutureRef<'static, FrontendOutput>) {
        if !slot.is_empty() {
            return;
        }
        let rx = Arc::clone(&self.accept_rx);
        let cancel = self.cancel_token.clone();
        slot.set(move || async move {
            let Some(mut guard) = rx.lock().cancel_with(&cancel).await else {
                return Err(TunnelError::Shutdown);
            };
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(TunnelError::Shutdown),
                item = guard.recv() => item.unwrap_or(Err(TunnelError::Shutdown)),
            }
        });
    }

    fn shutdown<'a>(&'a mut self) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.cancel_token.cancel();
            self.sessions.lock().clear();
            if let Some(task) = self.recv_task.take() {
                let _ = task.await;
            }
            let mut rx = self.accept_rx.lock().await;
            rx.close();
            while rx.try_recv().is_ok() {}
        })
    }
}

/// Logical per-peer NAT flow yielding inbound packets from `rx` and sending replies to `peer_addr`.
pub struct UdpFlowChannel {
    socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    flow_id: u64,
    _flow_permit: tokio::sync::OwnedSemaphorePermit,
    rx: mpsc::Receiver<Datagram>,
    queued_bytes: Arc<AtomicUsize>,
    sessions: Arc<Mutex<UdpSessionMap>>,
}

impl Drop for UdpFlowChannel {
    fn drop(&mut self) {
        let mut guard = self.sessions.lock();
        if let Some((current_id, _, _)) = guard.get(&self.peer_addr)
            && *current_id == self.flow_id
        {
            guard.remove(&self.peer_addr);
        }
    }
}

impl DatagramIo for UdpFlowChannel {
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
                release_session_bytes(&self.queued_bytes, first.payload.len());
                batch[0] = first;
                let mut count = 1;
                while count < batch.len() {
                    match self.rx.try_recv() {
                        Ok(next) => {
                            release_session_bytes(&self.queued_bytes, next.payload.len());
                            batch[count] = next;
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
        let mut sent = 0;
        for dgram in batch {
            match self.socket.poll_send_to(cx, &dgram.payload, self.peer_addr) {
                Poll::Ready(Ok(_)) => {
                    sent += 1;
                }
                Poll::Ready(Err(e)) => {
                    if sent > 0 {
                        return Poll::Ready(Ok(sent));
                    }
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    if sent > 0 {
                        return Poll::Ready(Ok(sent));
                    }
                    return Poll::Pending;
                }
            }
        }
        Poll::Ready(Ok(sent))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::stage::ActiveStage;
    use crate::relay::ChannelRelay;
    use std::time::Duration;

    #[test]
    fn session_queue_byte_budget_is_strict_and_releasable() {
        let queued = AtomicUsize::new(0);
        assert!(try_reserve_session_bytes(
            &queued,
            UDP_SESSION_QUEUE_MAX_BYTES - 1
        ));
        assert!(!try_reserve_session_bytes(&queued, 2));
        release_session_bytes(&queued, UDP_SESSION_QUEUE_MAX_BYTES - 1);
        assert_eq!(queued.load(Ordering::Acquire), 0);
        assert!(try_reserve_session_bytes(
            &queued,
            UDP_SESSION_QUEUE_MAX_BYTES
        ));
        release_session_bytes(&queued, UDP_SESSION_QUEUE_MAX_BYTES);
        assert_eq!(queued.load(Ordering::Acquire), 0);
    }

    fn get_ephemeral_udp_addr() -> String {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        format!("127.0.0.1:{}", port)
    }

    #[tokio::test]
    async fn test_udp_frontend_demux_multi_peer_and_reply() {
        let mut fe = UdpFrontend::bind(&get_ephemeral_udp_addr()).await.unwrap();
        let listen_addr = fe.local_addr().unwrap();

        let client_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        client_a.send_to(b"hello-a", listen_addr).await.unwrap();
        client_b.send_to(b"hello-b", listen_addr).await.unwrap();

        let mut slot = ReusableFutureRef::default();
        fe.accept(&mut slot);
        let acc1 = (&mut slot).await.unwrap();

        fe.accept(&mut slot);
        let acc2 = (&mut slot).await.unwrap();

        assert_ne!(acc1.peer, acc2.peer);

        // Echo both flows via serve() using DatagramDatagramPump
        for acc in [acc1, acc2] {
            let mut dgram = acc.channel.into_datagram().unwrap();
            tokio::spawn(async move {
                let mut batch = vec![Datagram::default(); 4];
                let n = std::future::poll_fn(|cx| dgram.poll_recv(cx, &mut batch))
                    .await
                    .unwrap();
                assert_eq!(n, 1);
                let reply = Datagram {
                    flow: batch[0].flow.clone(),
                    payload: Bytes::from(format!(
                        "echo:{}",
                        String::from_utf8_lossy(&batch[0].payload)
                    )),
                };
                std::future::poll_fn(|cx| dgram.poll_send(cx, std::slice::from_ref(&reply)))
                    .await
                    .unwrap();
            });
        }

        let mut buf_a = [0u8; 64];
        let (n_a, _) = tokio::time::timeout(Duration::from_secs(2), client_a.recv_from(&mut buf_a))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf_a[..n_a], b"echo:hello-a");

        let mut buf_b = [0u8; 64];
        let (n_b, _) = tokio::time::timeout(Duration::from_secs(2), client_b.recv_from(&mut buf_b))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf_b[..n_b], b"echo:hello-b");

        fe.shutdown().await;
    }

    #[tokio::test]
    async fn test_udp_frontend_with_serve_pump() {
        let mut fe = UdpFrontend::bind(&get_ephemeral_udp_addr()).await.unwrap();
        let listen_addr = fe.local_addr().unwrap();

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(b"ping", listen_addr).await.unwrap();

        let mut slot = ReusableFutureRef::default();
        fe.accept(&mut slot);
        let acc = slot.await.unwrap();

        let (stream_tunnel, mut stream_target) = tokio::io::duplex(4096);
        let cancel = CancellationToken::new();

        let serve_task = tokio::spawn({
            let cancel = cancel.clone();
            async move {
                let mut relay = ChannelRelay::from_channels(
                    acc.channel,
                    IoChannel::from_stream(stream_tunnel),
                    Duration::from_secs(5),
                    Duration::from_secs(10),
                    Duration::from_millis(100),
                    None,
                );
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => Ok(()),
                    res = std::future::poll_fn(|cx| relay.poll_drive(cx)) => res,
                }
            }
        });

        let mut hdr = [0u8; 2];
        tokio::io::AsyncReadExt::read_exact(&mut stream_target, &mut hdr)
            .await
            .unwrap();
        assert_eq!(u16::from_be_bytes(hdr), 4);
        let mut body = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(&mut stream_target, &mut body)
            .await
            .unwrap();
        assert_eq!(&body, b"ping");

        // Write framed response back
        tokio::io::AsyncWriteExt::write_all(&mut stream_target, &4u16.to_be_bytes())
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut stream_target, b"pong")
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::flush(&mut stream_target)
            .await
            .unwrap();

        let mut reply = [0u8; 16];
        let (n, _) = client.recv_from(&mut reply).await.unwrap();
        assert_eq!(&reply[..n], b"pong");

        cancel.cancel();
        let _ = serve_task.await;
        fe.shutdown().await;
    }

    #[tokio::test]
    async fn test_udp_frontend_enforces_session_byte_budget() {
        let mut fe = UdpFrontend::bind(&get_ephemeral_udp_addr()).await.unwrap();
        let listen_addr = fe.local_addr().unwrap();

        let client_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        // First establish client_a's flow and accept it without polling dgram_a yet.
        let big_payload = vec![0x42u8; 60_000];
        client_a.send_to(&big_payload, listen_addr).await.unwrap();

        let mut slot = ReusableFutureRef::default();
        fe.accept(&mut slot);
        let acc_a = tokio::time::timeout(Duration::from_secs(2), &mut slot)
            .await
            .unwrap()
            .unwrap();

        // Send 5 more 60_000-byte packets with brief yields so the OS socket buffer drains into
        // the session's mpsc queue (total 360_000 > UDP_SESSION_QUEUE_MAX_BYTES = 262_144).
        for _ in 0..5 {
            client_a.send_to(&big_payload, listen_addr).await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;

        client_b.send_to(b"peer-b-ok", listen_addr).await.unwrap();
        fe.accept(&mut slot);
        let acc_b = tokio::time::timeout(Duration::from_secs(2), &mut slot)
            .await
            .unwrap()
            .unwrap();

        let mut dgram_a = acc_a.channel.into_datagram().unwrap();
        let mut total_a = 0usize;
        let mut batch = vec![Datagram::default(); 8];
        while let Ok(Ok(n)) = tokio::time::timeout(
            Duration::from_millis(50),
            std::future::poll_fn(|cx| dgram_a.poll_recv(cx, &mut batch)),
        )
        .await
        {
            if n == 0 {
                break;
            }
            for d in &batch[..n] {
                total_a += d.payload.len();
            }
        }
        assert!(
            total_a <= UDP_SESSION_QUEUE_MAX_BYTES,
            "queued bytes {total_a} exceeded budget {UDP_SESSION_QUEUE_MAX_BYTES}"
        );
        assert!(total_a >= 60_000, "expected at least first packet");

        let mut dgram_b = acc_b.channel.into_datagram().unwrap();
        let n_b = tokio::time::timeout(
            Duration::from_secs(1),
            std::future::poll_fn(|cx| dgram_b.poll_recv(cx, &mut batch)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(n_b, 1);
        assert_eq!(batch[0].payload.as_ref(), b"peer-b-ok");

        drop(dgram_a);
        drop(dgram_b);
        fe.shutdown().await;
    }

    #[tokio::test]
    async fn test_udp_frontend_full_accept_queue_does_not_stall_or_leak_permits() {
        let mut fe = UdpFrontend::bind(&get_ephemeral_udp_addr()).await.unwrap();
        let listen_addr = fe.local_addr().unwrap();

        let mut clients = Vec::new();
        for i in 0..(ACCEPTED_STREAM_QUEUE_SIZE + 8) {
            let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            sock.send_to(format!("pkt-{i}").as_bytes(), listen_addr)
                .await
                .unwrap();
            clients.push(sock);
        }

        // Wait briefly so recv_task processes all packets without stalling when accept_tx fills.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut slot = ReusableFutureRef::default();
        let mut accepted = Vec::new();
        for _ in 0..ACCEPTED_STREAM_QUEUE_SIZE {
            fe.accept(&mut slot);
            let acc = tokio::time::timeout(Duration::from_secs(1), &mut slot)
                .await
                .expect("accept stalled")
                .unwrap();
            accepted.push(acc);
        }
        assert_eq!(accepted.len(), ACCEPTED_STREAM_QUEUE_SIZE);

        drop(accepted);
        fe.shutdown().await;
    }

    #[tokio::test]
    async fn handle_udp_recv_error_backs_off_recoverable_and_exits_fatal() {
        let (tx, mut rx) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let mut backoff = AcceptBackoff::new(Duration::from_millis(20), Duration::from_millis(80));

        // 1. Recoverable errors (ConnectionReset / EMFILE) back off exponentially and reset cleanly.
        let t0 = std::time::Instant::now();
        let flow = handle_udp_recv_error(
            io::Error::new(io::ErrorKind::ConnectionReset, "ICMP unreachable"),
            &mut backoff,
            &tx,
            &cancel,
        )
        .await;
        assert!(matches!(flow, std::ops::ControlFlow::Continue(())));
        assert!(t0.elapsed() >= Duration::from_millis(10));
        assert!(rx.try_recv().is_err());

        // 2. Cancellation immediately interrupts backoff sleep.
        let long_cancel = CancellationToken::new();
        let mut long_backoff = AcceptBackoff::new(Duration::from_secs(5), Duration::from_secs(10));
        let killer = long_cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(15)).await;
            killer.cancel();
        });
        let started_cancel = std::time::Instant::now();
        let flow_cancel = handle_udp_recv_error(
            io::Error::other("simulated EMFILE"),
            &mut long_backoff,
            &tx,
            &long_cancel,
        )
        .await;
        assert!(matches!(flow_cancel, std::ops::ControlFlow::Break(())));
        assert!(started_cancel.elapsed() < Duration::from_millis(300));

        // 3. Unrecoverable error breaks immediately and reports Err through accept queue.
        let flow_fatal = handle_udp_recv_error(
            io::Error::new(io::ErrorKind::PermissionDenied, "fatal socket error"),
            &mut backoff,
            &tx,
            &cancel,
        )
        .await;
        assert!(matches!(flow_fatal, std::ops::ControlFlow::Break(())));
        let delivered = rx.recv().await.unwrap().err().unwrap();
        assert!(matches!(
            delivered,
            TunnelError::Io(ref e) if e.kind() == io::ErrorKind::PermissionDenied
        ));
    }

    #[tokio::test]
    async fn udp_frontend_ipv6_large_datagram_boundaries() {
        let probe = std::net::UdpSocket::bind("[::1]:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let mut fe = UdpFrontend::bind(&format!("[::1]:{port}")).await.unwrap();
        let listen_addr = fe.local_addr().unwrap();
        assert!(listen_addr.is_ipv6());

        let client = UdpSocket::bind("[::1]:0").await.unwrap();
        let sizes = [65_507usize, 65_508usize, MAX_UDP_DATAGRAM_SIZE];
        assert_eq!(MAX_UDP_DATAGRAM_SIZE, 65_527);

        let first_payload = vec![0x11u8; sizes[0]];
        client.send_to(&first_payload, listen_addr).await.unwrap();

        let mut slot = ReusableFutureRef::default();
        fe.accept(&mut slot);
        let acc = tokio::time::timeout(Duration::from_secs(2), &mut slot)
            .await
            .unwrap()
            .unwrap();
        let mut dgram = acc.channel.into_datagram().unwrap();
        let mut batch = vec![Datagram::default(); 4];

        let mut received = Vec::new();
        let n = tokio::time::timeout(
            Duration::from_secs(2),
            std::future::poll_fn(|cx| dgram.poll_recv(cx, &mut batch)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(n, 1);
        assert_eq!(batch[0].payload.len(), sizes[0]);
        assert!(batch[0].payload.iter().all(|&b| b == 0x11));
        received.push(batch[0].payload.len());

        for (fill, size) in [(0x22u8, sizes[1]), (0x33u8, sizes[2])] {
            let payload = vec![fill; size];
            client.send_to(&payload, listen_addr).await.unwrap();

            let n = tokio::time::timeout(
                Duration::from_secs(2),
                std::future::poll_fn(|cx| dgram.poll_recv(cx, &mut batch)),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(n, 1);
            assert_eq!(batch[0].payload.len(), size);
            assert!(batch[0].payload.iter().all(|&b| b == fill));
            received.push(batch[0].payload.len());
        }

        assert_eq!(received, vec![65_507, 65_508, 65_527]);

        drop(dgram);
        fe.shutdown().await;
    }
}
