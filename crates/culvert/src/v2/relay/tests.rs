// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Deterministic unit tests for `DuplexRelay` and `RelayClosing`.
//! All comments and docstrings must be in English.

use crate::consts::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT, DEFAULT_RELAY_DRAIN_TIMEOUT,
};
use crate::core::stream::{HeartbeatStatus, TunnelStream, activity_now_ms};
use crate::v2::relay::{DuplexRelay, RelayCounters};
use crate::v2::stage::ConnectingContext;
use crate::v2::{Coordinator, TaskError, TaskOutcome, TaskWorkflow, TunnelPhase};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf, duplex,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct RelayWorkflow;

struct RelayInput {
    a: Box<dyn TunnelStream>,
    b: Box<dyn TunnelStream>,
    idle_timeout: Duration,
    drain_timeout: Duration,
    counters: Arc<RelayCounters>,
    on_active: Option<Arc<Notify>>,
}

impl TaskWorkflow for RelayWorkflow {
    type Input = RelayInput;
    type Active = DuplexRelay;

    async fn connect(
        &self,
        _ctx: &mut ConnectingContext<'_>,
        input: Self::Input,
    ) -> Result<Self::Active, TaskError> {
        if let Some(n) = input.on_active {
            n.notify_one();
        }
        Ok(DuplexRelay::new(
            input.a,
            input.b,
            DEFAULT_HEARTBEAT_INTERVAL,
            input.idle_timeout,
            input.drain_timeout,
        )
        .with_counters(input.counters))
    }
}

struct AgedStream {
    inner: DuplexStream,
    activity_ms: u64,
    dead: Arc<AtomicBool>,
}

impl AsyncRead for AgedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for AgedStream {
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

impl TunnelStream for AgedStream {
    fn last_activity_ms(&self) -> Option<u64> {
        Some(self.activity_ms)
    }

    fn poll_check_alive(&mut self, _cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        if self.dead.load(Ordering::Acquire) {
            Poll::Ready(HeartbeatStatus::Dead)
        } else {
            Poll::Ready(HeartbeatStatus::Alive)
        }
    }
}

#[tokio::test]
async fn duplex_relay_transfers_half_closes_and_recycles() {
    let cancel = CancellationToken::new();
    let (handle, join) = Coordinator::start(RelayWorkflow, DEFAULT_RELAY_DRAIN_TIMEOUT, cancel);

    let (mut app_a, a) = duplex(4096);
    let (mut app_b, b) = duplex(4096);
    let counters = Arc::new(RelayCounters::new());

    let task_id = handle
        .assign(RelayInput {
            a: Box::new(a),
            b: Box::new(b),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            drain_timeout: DEFAULT_RELAY_DRAIN_TIMEOUT,
            counters: Arc::clone(&counters),
            on_active: None,
        })
        .await
        .unwrap();

    // Half-close A -> B while B -> A keeps streaming.
    app_a.write_all(b"hello ").await.unwrap();
    app_a.write_all(b"world").await.unwrap();
    app_a.shutdown().await.unwrap();

    let mut recv_b = vec![0u8; 11];
    app_b.read_exact(&mut recv_b).await.unwrap();
    assert_eq!(&recv_b, b"hello world");

    let mut eof = [0u8; 1];
    assert_eq!(app_b.read(&mut eof).await.unwrap(), 0);

    app_b.write_all(b"reply").await.unwrap();
    app_b.shutdown().await.unwrap();

    let mut recv_a = vec![0u8; 5];
    app_a.read_exact(&mut recv_a).await.unwrap();
    assert_eq!(&recv_a, b"reply");
    assert_eq!(app_a.read(&mut eof).await.unwrap(), 0);

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(counters.snapshot(), (11, 5));

    let phase = handle.graceful_stop().await.unwrap();
    assert!(matches!(
        phase,
        TunnelPhase::Draining | TunnelPhase::Stopped
    ));

    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.completed_tasks_len(), 1);
    assert_eq!(
        summary.completed_jobs,
        vec![(task_id, TaskOutcome::Completed)]
    );
}

#[tokio::test]
async fn duplex_relay_force_stop_exits_immediately_while_idle_forever() {
    let cancel = CancellationToken::new();
    let (handle, join) = Coordinator::start(RelayWorkflow, Duration::from_secs(30), cancel);

    let (_app_a, a) = duplex(1024);
    let (_app_b, b) = duplex(1024);
    let counters = Arc::new(RelayCounters::new());
    let active_notify = Arc::new(Notify::new());

    let task_id = handle
        .assign(RelayInput {
            a: Box::new(a),
            b: Box::new(b),
            idle_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(30),
            counters,
            on_active: Some(Arc::clone(&active_notify)),
        })
        .await
        .unwrap();
    active_notify.notified().await;

    let started = std::time::Instant::now();
    handle.force_stop();

    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .expect("force stop must not wait for 30s drain or idle timeout")
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(250));
    assert_eq!(
        summary.completed_jobs,
        vec![(task_id, TaskOutcome::ForceStopped)]
    );
}

#[tokio::test]
async fn duplex_relay_idle_timeout_reports_failure_and_recycles() {
    let cancel = CancellationToken::new();
    let (handle, join) = Coordinator::start(RelayWorkflow, DEFAULT_RELAY_DRAIN_TIMEOUT, cancel);

    let (_app_a, a) = duplex(1024);
    let (_app_b, b) = duplex(1024);
    let counters = Arc::new(RelayCounters::new());

    let stale_a = AgedStream {
        inner: a,
        activity_ms: activity_now_ms().saturating_sub(500),
        dead: Arc::new(AtomicBool::new(false)),
    };

    let task_id = handle
        .assign(RelayInput {
            a: Box::new(stale_a),
            b: Box::new(b),
            idle_timeout: Duration::from_millis(50),
            drain_timeout: DEFAULT_RELAY_DRAIN_TIMEOUT,
            counters,
            on_active: None,
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(120)).await;
    handle.graceful_stop().await.unwrap();

    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.completed_jobs.len(), 1);
    assert_eq!(summary.completed_jobs[0].0, task_id);
    assert!(matches!(
        summary.completed_jobs[0].1,
        TaskOutcome::Failed(_)
    ));
}

trait SummaryExt {
    fn completed_tasks_len(&self) -> usize;
}

impl SummaryExt for crate::v2::CoordinatorSummary {
    fn completed_tasks_len(&self) -> usize {
        self.completed_jobs.len()
    }
}

struct MockDatagramChannel {
    inbound_rx: tokio::sync::mpsc::UnboundedReceiver<crate::io::channel::Datagram>,
    outbound_tx: tokio::sync::mpsc::UnboundedSender<crate::io::channel::Datagram>,
}

impl crate::io::channel::DatagramIo for MockDatagramChannel {
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        batch: &mut [crate::io::channel::Datagram],
    ) -> Poll<io::Result<usize>> {
        if batch.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match self.inbound_rx.poll_recv(cx) {
            Poll::Ready(Some(pkt)) => {
                batch[0] = pkt;
                let mut count = 1usize;
                while count < batch.len() {
                    match self.inbound_rx.try_recv() {
                        Ok(more) => {
                            batch[count] = more;
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

    fn poll_send(
        &mut self,
        _cx: &mut Context<'_>,
        batch: &[crate::io::channel::Datagram],
    ) -> Poll<io::Result<usize>> {
        for d in batch {
            self.outbound_tx
                .send(d.clone())
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "dgram peer closed"))?;
        }
        Poll::Ready(Ok(batch.len()))
    }
}

struct ChannelRelayWorkflow;

struct ChannelRelayInput {
    a: crate::io::channel::IoChannel,
    b: crate::io::channel::IoChannel,
    counters: Arc<RelayCounters>,
}

impl TaskWorkflow for ChannelRelayWorkflow {
    type Input = ChannelRelayInput;
    type Active = crate::v2::relay::ChannelRelay;

    async fn connect(
        &self,
        _ctx: &mut ConnectingContext<'_>,
        input: Self::Input,
    ) -> Result<Self::Active, TaskError> {
        Ok(crate::v2::relay::ChannelRelay::from_channels(
            input.a,
            input.b,
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
            DEFAULT_RELAY_DRAIN_TIMEOUT,
            Some(input.counters),
        ))
    }
}

#[tokio::test]
async fn channel_relay_datagram_stream_frames_and_half_closes() {
    use crate::io::channel::{Datagram, FlowKey, IoChannel};
    use bytes::Bytes;

    let cancel = CancellationToken::new();
    let (handle, join) =
        Coordinator::start(ChannelRelayWorkflow, DEFAULT_RELAY_DRAIN_TIMEOUT, cancel);

    let (dgram_in_tx, dgram_in_rx) = tokio::sync::mpsc::unbounded_channel();
    let (dgram_out_tx, mut dgram_out_rx) = tokio::sync::mpsc::unbounded_channel();
    let dgram_io = MockDatagramChannel {
        inbound_rx: dgram_in_rx,
        outbound_tx: dgram_out_tx,
    };

    let (mut remote_stream, local_stream) = duplex(4096);
    let counters = Arc::new(RelayCounters::new());

    let task_id = handle
        .assign(ChannelRelayInput {
            a: IoChannel::from_datagram(dgram_io),
            b: IoChannel::from_stream(local_stream),
            counters: Arc::clone(&counters),
        })
        .await
        .unwrap();

    // Send datagram -> encoded frame on stream
    dgram_in_tx
        .send(Datagram {
            flow: FlowKey::default(),
            payload: Bytes::from_static(b"udp-ping"),
        })
        .unwrap();

    let mut hdr = [0u8; 2];
    remote_stream.read_exact(&mut hdr).await.unwrap();
    assert_eq!(u16::from_be_bytes(hdr), 8);
    let mut payload = vec![0u8; 8];
    remote_stream.read_exact(&mut payload).await.unwrap();
    assert_eq!(&payload, b"udp-ping");

    // Half-close stream write side; Datagram -> Stream must remain alive.
    remote_stream.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    dgram_in_tx
        .send(Datagram {
            flow: FlowKey::default(),
            payload: Bytes::from_static(b"still-up"),
        })
        .unwrap();
    remote_stream.read_exact(&mut hdr).await.unwrap();
    assert_eq!(u16::from_be_bytes(hdr), 8);
    remote_stream.read_exact(&mut payload).await.unwrap();
    assert_eq!(&payload, b"still-up");

    // Close datagram source so relay finishes cleanly
    drop(dgram_in_tx);
    let mut eof = [0u8; 1];
    assert_eq!(remote_stream.read(&mut eof).await.unwrap(), 0);
    assert!(dgram_out_rx.try_recv().is_err());

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(counters.snapshot(), (16, 0));

    handle.graceful_stop().await.unwrap();
    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        summary.completed_jobs,
        vec![(task_id, TaskOutcome::Completed)]
    );
}

#[tokio::test]
async fn channel_relay_datagram_datagram_forwards_batches() {
    use crate::io::channel::{Datagram, FlowKey, IoChannel};
    use bytes::Bytes;

    let cancel = CancellationToken::new();
    let (handle, join) =
        Coordinator::start(ChannelRelayWorkflow, DEFAULT_RELAY_DRAIN_TIMEOUT, cancel);

    let (a_in_tx, a_in_rx) = tokio::sync::mpsc::unbounded_channel();
    let (a_out_tx, mut a_out_rx) = tokio::sync::mpsc::unbounded_channel();
    let (b_in_tx, b_in_rx) = tokio::sync::mpsc::unbounded_channel();
    let (b_out_tx, mut b_out_rx) = tokio::sync::mpsc::unbounded_channel();

    let counters = Arc::new(RelayCounters::new());
    let task_id = handle
        .assign(ChannelRelayInput {
            a: IoChannel::from_datagram(MockDatagramChannel {
                inbound_rx: a_in_rx,
                outbound_tx: a_out_tx,
            }),
            b: IoChannel::from_datagram(MockDatagramChannel {
                inbound_rx: b_in_rx,
                outbound_tx: b_out_tx,
            }),
            counters: Arc::clone(&counters),
        })
        .await
        .unwrap();

    a_in_tx
        .send(Datagram {
            flow: FlowKey::default(),
            payload: Bytes::from_static(b"pkt-a"),
        })
        .unwrap();
    b_in_tx
        .send(Datagram {
            flow: FlowKey::default(),
            payload: Bytes::from_static(b"pkt-bb"),
        })
        .unwrap();

    let got_b = tokio::time::timeout(Duration::from_secs(1), b_out_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let got_a = tokio::time::timeout(Duration::from_secs(1), a_out_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got_b.payload.as_ref(), b"pkt-a");
    assert_eq!(got_a.payload.as_ref(), b"pkt-bb");

    drop(a_in_tx);
    drop(b_in_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(counters.snapshot(), (5, 6));

    handle.graceful_stop().await.unwrap();
    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        summary.completed_jobs,
        vec![(task_id, TaskOutcome::Completed)]
    );
}
