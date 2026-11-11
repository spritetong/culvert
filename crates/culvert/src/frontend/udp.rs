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
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::common::CancelFutureExt;
use crate::consts::{ACCEPTED_STREAM_QUEUE_SIZE, UDP_SESSION_CHANNEL_SIZE};
use crate::frontend::{Frontend, FrontendAccept, FrontendFactory, FrontendOutput};
use crate::io::addr::{L4Protocol, MappingSpec, PeerAddr, TargetAddr, parse_target_addr};
use crate::io::channel::{Datagram, DatagramIo, FlowKey, IoChannel};
use crate::io::error::TunnelError;
use crate::io::stream::BoxFuture;

/// Session table mapping client `SocketAddr` -> `(flow_id, inbound_sender)`.
type UdpSessionMap = HashMap<SocketAddr, (u64, mpsc::Sender<Datagram>)>;

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
            let frontend = UdpFrontend::bind(&spec.listen, Some(&spec.target)).await?;
            Ok(Box::new(frontend) as Box<dyn Frontend>)
        })
    }
}

/// UDP frontend listening on one bound socket and yielding per-peer `DatagramIo` flows.
pub struct UdpFrontend {
    socket: Arc<UdpSocket>,
    sessions: Arc<Mutex<UdpSessionMap>>,
    accept_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<FrontendAccept>>>,
    cancel_token: CancellationToken,
    recv_task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for UdpFrontend {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.sessions.lock().clear();
    }
}

impl UdpFrontend {
    /// Binds a UDP socket at `listen_addr` and starts the packet demultiplexer task.
    pub async fn bind(listen_addr: &str, target_str: Option<&str>) -> Result<Self, TunnelError> {
        let listen_target = parse_target_addr(listen_addr)?;
        let socket = match listen_target {
            TargetAddr::Ip(sa) => UdpSocket::bind(sa).await.map_err(TunnelError::Io)?,
            TargetAddr::Domain(host, port) => UdpSocket::bind((host.as_str(), port))
                .await
                .map_err(TunnelError::Io)?,
            TargetAddr::Path(_) => {
                return Err(TunnelError::Config(format!(
                    "UDP frontend does not support Unix domain socket address '{}'",
                    listen_addr
                )));
            }
        };

        let target_addr = target_str.and_then(|t| parse_target_addr(t).ok());
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

            loop {
                let recv_res = tokio::select! {
                    biased;
                    _ = task_cancel.cancelled() => break,
                    res = task_socket.recv_from(&mut buf) => res,
                };

                let (n, peer_addr) = match recv_res {
                    Ok(pair) => pair,
                    Err(e) => {
                        if !task_cancel.is_cancelled() {
                            tracing::debug!(error = %e, "UDP frontend recv_from error");
                        }
                        continue;
                    }
                };

                let dgram = Datagram {
                    flow: FlowKey {
                        peer: PeerAddr::Socket(peer_addr),
                        target: target_addr.clone(),
                        protocol: L4Protocol::Udp,
                    },
                    payload: Bytes::copy_from_slice(&buf[..n]),
                };

                // Look up existing session sender without holding the mutex across awaits.
                let existing_tx = {
                    let guard = task_sessions.lock();
                    guard.get(&peer_addr).map(|(_, tx)| tx.clone())
                };

                let pending_first = if let Some(tx) = existing_tx {
                    match tx.try_send(dgram) {
                        Ok(()) => continue,
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            // Backpressure drop-tail when session queue is saturated.
                            continue;
                        }
                        Err(mpsc::error::TrySendError::Closed(returned)) => Some(returned),
                    }
                } else {
                    Some(dgram)
                };

                if let Some(first_dgram) = pending_first {
                    let flow_id = next_flow_id;
                    next_flow_id = next_flow_id.wrapping_add(1).max(1);

                    let (flow_tx, flow_rx) = mpsc::channel(UDP_SESSION_CHANNEL_SIZE);
                    if flow_tx.try_send(first_dgram).is_err() {
                        continue;
                    }

                    {
                        let mut guard = task_sessions.lock();
                        guard.insert(peer_addr, (flow_id, flow_tx));
                    }

                    let channel = UdpFlowChannel {
                        socket: Arc::clone(&task_socket),
                        peer_addr,
                        flow_id,
                        rx: flow_rx,
                        sessions: Arc::clone(&task_sessions),
                    };

                    let accepted = FrontendAccept {
                        channel: IoChannel::from_datagram(channel),
                        peer: PeerAddr::Socket(peer_addr),
                    };

                    if accept_tx
                        .send(accepted)
                        .cancel_with(&task_cancel)
                        .await
                        .is_none_or(|r| r.is_err())
                    {
                        let mut guard = task_sessions.lock();
                        if let Some((cur_id, _)) = guard.get(&peer_addr)
                            && *cur_id == flow_id
                        {
                            guard.remove(&peer_addr);
                        }
                        break;
                    }
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
            let mut guard = rx.lock().await;
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(TunnelError::Shutdown),
                item = guard.recv() => item.ok_or(TunnelError::Shutdown),
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
        })
    }
}

/// Logical per-peer NAT flow yielding inbound packets from `rx` and sending replies to `peer_addr`.
pub struct UdpFlowChannel {
    socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    flow_id: u64,
    rx: mpsc::Receiver<Datagram>,
    sessions: Arc<Mutex<UdpSessionMap>>,
}

impl Drop for UdpFlowChannel {
    fn drop(&mut self) {
        let mut guard = self.sessions.lock();
        if let Some((current_id, _)) = guard.get(&self.peer_addr)
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
                batch[0] = first;
                let mut count = 1;
                while count < batch.len() {
                    match self.rx.try_recv() {
                        Ok(next) => {
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
    use crate::core::serve::serve;
    use crate::io::pump::{PumpCtx, PumpRequest};
    use std::time::Duration;

    fn get_ephemeral_udp_addr() -> String {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        format!("127.0.0.1:{}", port)
    }

    #[tokio::test]
    async fn test_udp_frontend_demux_multi_peer_and_reply() {
        let mut fe = UdpFrontend::bind(&get_ephemeral_udp_addr(), Some("127.0.0.1:9999"))
            .await
            .unwrap();
        let listen_addr = fe.local_addr().unwrap();

        let client_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        client_a.send_to(b"hello-a", listen_addr).await.unwrap();
        client_b.send_to(b"hello-b", listen_addr).await.unwrap();

        let mut slot = ReusableFutureRef::default();
        fe.accept(&mut slot);
        let acc1 = (&mut slot).await.unwrap();
        slot.reset();

        fe.accept(&mut slot);
        let acc2 = (&mut slot).await.unwrap();
        slot.reset();

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
        let mut fe = UdpFrontend::bind(&get_ephemeral_udp_addr(), Some("127.0.0.1:9999"))
            .await
            .unwrap();
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
                serve(
                    PumpRequest::new(acc.channel, IoChannel::from_stream(stream_tunnel)),
                    PumpCtx {
                        cancel,
                        heartbeat_interval: Duration::from_secs(5),
                        idle_timeout: Duration::from_secs(10),
                    },
                )
                .await
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
}
