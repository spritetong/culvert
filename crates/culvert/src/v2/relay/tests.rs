// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Deterministic unit tests for `DuplexRelay` and `RelayClosing`.
//! All comments and docstrings must be in English.

use crate::consts::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT, DEFAULT_RELAY_DRAIN_TIMEOUT,
};
use crate::core::stream::{HeartbeatStatus, TunnelStream, activity_now_ms};
use crate::v2::relay::{ChannelRelay, DuplexRelay, RelayCounters};
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
    assert_eq!(summary.completed_jobs[0].task_id, task_id);
    assert_eq!(summary.completed_jobs[0].outcome, TaskOutcome::Completed);
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
    assert_eq!(summary.completed_jobs.len(), 1);
    assert_eq!(summary.completed_jobs[0].task_id, task_id);
    assert_eq!(summary.completed_jobs[0].outcome, TaskOutcome::ForceStopped);
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
    assert_eq!(summary.completed_jobs[0].task_id, task_id);
    assert!(matches!(
        summary.completed_jobs[0].outcome,
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
    assert_eq!(summary.completed_jobs.len(), 1);
    assert_eq!(summary.completed_jobs[0].task_id, task_id);
    assert_eq!(summary.completed_jobs[0].outcome, TaskOutcome::Completed);
}

struct BackpressuredDatagram {
    inbound_rx: tokio::sync::mpsc::UnboundedReceiver<crate::io::channel::Datagram>,
    allow_send: Arc<AtomicBool>,
    sent: Arc<parking_lot::Mutex<std::collections::VecDeque<bytes::Bytes>>>,
    waker_slot: Arc<parking_lot::Mutex<Option<std::task::Waker>>>,
}

impl crate::io::channel::DatagramIo for BackpressuredDatagram {
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        batch: &mut [crate::io::channel::Datagram],
    ) -> Poll<io::Result<usize>> {
        match self.inbound_rx.poll_recv(cx) {
            Poll::Ready(Some(d)) if !batch.is_empty() => {
                batch[0] = d;
                Poll::Ready(Ok(1))
            }
            Poll::Ready(None) => Poll::Ready(Ok(0)),
            _ => Poll::Pending,
        }
    }

    fn poll_send(
        &mut self,
        cx: &mut Context<'_>,
        batch: &[crate::io::channel::Datagram],
    ) -> Poll<io::Result<usize>> {
        if batch.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if !self.allow_send.load(Ordering::Acquire) {
            *self.waker_slot.lock() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let mut sent = self.sent.lock();
        for d in batch {
            sent.push_back(d.payload.clone());
        }
        Poll::Ready(Ok(batch.len()))
    }
}

struct SingleShotRelayWorkflow {
    idle_timeout: Duration,
    counters: Arc<RelayCounters>,
}

struct SingleShotInput {
    a: crate::io::channel::IoChannel,
    b: crate::io::channel::IoChannel,
}

impl TaskWorkflow for SingleShotRelayWorkflow {
    type Input = SingleShotInput;
    type Active = ChannelRelay;

    fn connect(
        &self,
        _ctx: &mut ConnectingContext<'_>,
        input: Self::Input,
    ) -> impl std::future::Future<Output = Result<Self::Active, TaskError>> + Send {
        let idle_timeout = self.idle_timeout;
        let counters = Arc::clone(&self.counters);
        async move {
            Ok(ChannelRelay::from_channels(
                input.a,
                input.b,
                Duration::from_millis(10),
                idle_timeout,
                DEFAULT_RELAY_DRAIN_TIMEOUT,
                Some(counters),
            ))
        }
    }
}

#[tokio::test]
async fn datagram_stream_pending_flush_does_not_exit_on_idle_timeout() {
    use crate::io::channel::{Datagram, FlowKey, IoChannel};
    use bytes::Bytes;

    struct PendingFlushStream {
        allow_flush: Arc<AtomicBool>,
        written: Arc<parking_lot::Mutex<Vec<u8>>>,
        flushed: Arc<std::sync::atomic::AtomicUsize>,
        waker: Arc<parking_lot::Mutex<Option<std::task::Waker>>>,
        activity_ms: u64,
    }

    impl AsyncRead for PendingFlushStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for PendingFlushStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.written.lock().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            if !self.allow_flush.load(Ordering::Acquire) {
                *self.waker.lock() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            self.flushed
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl TunnelStream for PendingFlushStream {
        fn last_activity_ms(&self) -> Option<u64> {
            Some(self.activity_ms)
        }

        fn poll_check_alive(&mut self, _cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
            Poll::Ready(HeartbeatStatus::Alive)
        }
    }

    struct FlushTestWorkflow;
    struct FlushTestInput {
        dgram: MockDatagramChannel,
        stream: PendingFlushStream,
    }
    impl TaskWorkflow for FlushTestWorkflow {
        type Input = FlushTestInput;
        type Active = ChannelRelay;
        async fn connect(
            &self,
            _ctx: &mut ConnectingContext<'_>,
            input: Self::Input,
        ) -> Result<Self::Active, TaskError> {
            Ok(ChannelRelay::from_channels(
                IoChannel::from_datagram(input.dgram),
                IoChannel::from_boxed_stream(Box::new(input.stream)),
                Duration::from_millis(10),
                Duration::from_millis(45),
                DEFAULT_RELAY_DRAIN_TIMEOUT,
                None,
            ))
        }
    }

    let cancel = CancellationToken::new();
    let (handle, join) = Coordinator::start(FlushTestWorkflow, DEFAULT_RELAY_DRAIN_TIMEOUT, cancel);

    let (d_in_tx, d_in_rx) = tokio::sync::mpsc::unbounded_channel();
    let (d_out_tx, _d_out_rx) = tokio::sync::mpsc::unbounded_channel();
    let allow_flush = Arc::new(AtomicBool::new(false));
    let written = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let flushed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let waker = Arc::new(parking_lot::Mutex::new(None));

    let task_id = handle
        .assign(FlushTestInput {
            dgram: MockDatagramChannel {
                inbound_rx: d_in_rx,
                outbound_tx: d_out_tx,
            },
            stream: PendingFlushStream {
                allow_flush: Arc::clone(&allow_flush),
                written: Arc::clone(&written),
                flushed: Arc::clone(&flushed),
                waker: Arc::clone(&waker),
                activity_ms: activity_now_ms(),
            },
        })
        .await
        .unwrap();

    // Send a datagram: poll_write succeeds and clears in_flight_chunk, setting need_flush = true,
    // while poll_flush stays Pending well past the 45ms idle_timeout.
    d_in_tx
        .send(Datagram {
            flow: FlowKey::default(),
            payload: Bytes::from_static(b"flush-backpressure"),
        })
        .unwrap();

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(written.lock().len(), 2 + b"flush-backpressure".len());
    assert_eq!(flushed.load(Ordering::Acquire), 0);
    assert_eq!(
        handle.query_task_state(task_id).await.unwrap(),
        Some(crate::v2::ExternalState::Active)
    );

    allow_flush.store(true, Ordering::Release);
    if let Some(w) = waker.lock().take() {
        w.wake();
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(flushed.load(Ordering::Acquire) >= 1);

    drop(d_in_tx);
    handle.graceful_stop().await.unwrap();
    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
}

#[tokio::test]
async fn datagram_stream_backpressure_does_not_drop_buffered_data_on_idle_timeout() {
    use crate::io::channel::IoChannel;

    let cancel = CancellationToken::new();
    let (handle, join) = Coordinator::start(
        SingleShotRelayWorkflow {
            idle_timeout: Duration::from_millis(45),
            counters: Arc::new(RelayCounters::new()),
        },
        DEFAULT_RELAY_DRAIN_TIMEOUT,
        cancel,
    );

    let (mut app_stream, relay_stream) = duplex(1024);
    let (_d_in_tx, d_in_rx) = tokio::sync::mpsc::unbounded_channel();
    let allow_send = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(parking_lot::Mutex::new(std::collections::VecDeque::new()));
    let waker = Arc::new(parking_lot::Mutex::new(None));

    handle
        .assign(SingleShotInput {
            a: IoChannel::from_boxed_stream(Box::new(relay_stream)),
            b: IoChannel::from_datagram(BackpressuredDatagram {
                inbound_rx: d_in_rx,
                allow_send: Arc::clone(&allow_send),
                sent: Arc::clone(&sent),
                waker_slot: Arc::clone(&waker),
            }),
        })
        .await
        .unwrap();

    // Write one length-prefixed datagram frame from stream into backpressured DatagramIo:
    // 2-byte big-endian len (13) + b"dgram-bp-test".
    let mut framed = Vec::from(13u16.to_be_bytes());
    framed.extend_from_slice(b"dgram-bp-test");
    app_stream.write_all(&framed).await.unwrap();

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(sent.lock().is_empty());

    allow_send.store(true, Ordering::Release);
    if let Some(w) = waker.lock().take() {
        w.wake();
    }

    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        sent.lock().pop_front().as_deref(),
        Some(&b"dgram-bp-test"[..])
    );

    drop(app_stream);
    handle.graceful_stop().await.unwrap();
    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
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
    assert_eq!(summary.completed_jobs.len(), 1);
    assert_eq!(summary.completed_jobs[0].task_id, task_id);
    assert_eq!(summary.completed_jobs[0].outcome, TaskOutcome::Completed);
}

#[tokio::test]
async fn udp_relay_backpressure_does_not_drop_queued_datagrams_on_idle_timeout() {
    use crate::io::channel::{Datagram, FlowKey, IoChannel};
    use bytes::Bytes;
    use std::collections::VecDeque;

    let cancel = CancellationToken::new();
    let counters = Arc::new(RelayCounters::new());
    let (handle, join) = Coordinator::start(
        SingleShotRelayWorkflow {
            idle_timeout: Duration::from_millis(45),
            counters: Arc::clone(&counters),
        },
        DEFAULT_RELAY_DRAIN_TIMEOUT,
        cancel,
    );

    let (a_in_tx, a_in_rx) = tokio::sync::mpsc::unbounded_channel();
    let (_b_in_tx, b_in_rx) = tokio::sync::mpsc::unbounded_channel();
    let allow_send_b = Arc::new(AtomicBool::new(false));
    let sent_b = Arc::new(parking_lot::Mutex::new(VecDeque::new()));
    let waker_b = Arc::new(parking_lot::Mutex::new(None));

    let a_dev = MockDatagramChannel {
        inbound_rx: a_in_rx,
        outbound_tx: tokio::sync::mpsc::unbounded_channel().0,
    };
    let b_dev = BackpressuredDatagram {
        inbound_rx: b_in_rx,
        allow_send: Arc::clone(&allow_send_b),
        sent: Arc::clone(&sent_b),
        waker_slot: Arc::clone(&waker_b),
    };

    handle
        .assign(SingleShotInput {
            a: IoChannel::from_datagram(a_dev),
            b: IoChannel::from_datagram(b_dev),
        })
        .await
        .unwrap();

    // Send a datagram while destination poll_send() stays Pending beyond idle_timeout (45ms).
    a_in_tx
        .send(Datagram {
            flow: FlowKey::default(),
            payload: Bytes::from_static(b"must-not-drop-under-backpressure"),
        })
        .unwrap();

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(sent_b.lock().is_empty());

    // Release backpressure -> queued packet MUST still be flushed intact.
    allow_send_b.store(true, Ordering::Release);
    if let Some(w) = waker_b.lock().take() {
        w.wake();
    }

    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        sent_b.lock().pop_front().as_deref(),
        Some(&b"must-not-drop-under-backpressure"[..])
    );

    drop(a_in_tx);
    handle.graceful_stop().await.unwrap();
    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(counters.snapshot(), (32, 0));
}

#[tokio::test]
async fn datagram_datagram_asymmetric_close_continues_reverse_direction() {
    use crate::io::channel::{Datagram, FlowKey, IoChannel};
    use bytes::Bytes;
    use std::collections::VecDeque;

    let cancel = CancellationToken::new();
    let counters = Arc::new(RelayCounters::new());
    let (handle, join) = Coordinator::start(
        SingleShotRelayWorkflow {
            idle_timeout: Duration::from_secs(5),
            counters: Arc::clone(&counters),
        },
        DEFAULT_RELAY_DRAIN_TIMEOUT,
        cancel,
    );

    let (a_in_tx, a_in_rx) = tokio::sync::mpsc::unbounded_channel();
    let (b_in_tx, b_in_rx) = tokio::sync::mpsc::unbounded_channel();
    let (b_out_tx, mut b_out_rx) = tokio::sync::mpsc::unbounded_channel();

    // Side A uses BackpressuredDatagram for its outbound half while its inbound half closes early.
    let allow_send_a = Arc::new(AtomicBool::new(false));
    let sent_a = Arc::new(parking_lot::Mutex::new(VecDeque::new()));
    let waker_a = Arc::new(parking_lot::Mutex::new(None));

    let a_dev = BackpressuredDatagram {
        inbound_rx: a_in_rx,
        allow_send: Arc::clone(&allow_send_a),
        sent: Arc::clone(&sent_a),
        waker_slot: Arc::clone(&waker_a),
    };
    let b_dev = MockDatagramChannel {
        inbound_rx: b_in_rx,
        outbound_tx: b_out_tx,
    };

    handle
        .assign(SingleShotInput {
            a: IoChannel::from_datagram(a_dev),
            b: IoChannel::from_datagram(b_dev),
        })
        .await
        .unwrap();

    // A -> B succeeds first.
    a_in_tx
        .send(Datagram {
            flow: FlowKey::default(),
            payload: Bytes::from_static(b"req-1"),
        })
        .unwrap();
    assert_eq!(b_out_rx.recv().await.unwrap().payload.as_ref(), b"req-1");

    // B queues a reply while A's send direction is backpressured, AND A immediately closes recv!
    b_in_tx
        .send(Datagram {
            flow: FlowKey::default(),
            payload: Bytes::from_static(b"reply-while-a-closed"),
        })
        .unwrap();
    drop(a_in_tx);

    // Relay MUST stay active while B -> A queued reply awaits flush AND further replies arrive.
    tokio::time::sleep(Duration::from_millis(30)).await;
    allow_send_a.store(true, Ordering::Release);
    if let Some(w) = waker_a.lock().take() {
        w.wake();
    }

    // Even after a_in_tx closed, B can still send additional reply packets until B itself closes.
    tokio::time::sleep(Duration::from_millis(20)).await;
    b_in_tx
        .send(Datagram {
            flow: FlowKey::default(),
            payload: Bytes::from_static(b"reply-after-a-closed"),
        })
        .unwrap();
    drop(b_in_tx);

    tokio::time::sleep(Duration::from_millis(30)).await;
    {
        let mut got = sent_a.lock();
        assert_eq!(
            got.pop_front().as_deref(),
            Some(&b"reply-while-a-closed"[..])
        );
        assert_eq!(
            got.pop_front().as_deref(),
            Some(&b"reply-after-a-closed"[..])
        );
        assert!(got.is_empty());
    }
    assert_eq!(counters.snapshot(), (5, 40));

    handle.graceful_stop().await.unwrap();
    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
}

#[tokio::test]
async fn udp_counters_exclude_unsent_queued_packets_on_force_stop() {
    use crate::io::channel::{Datagram, FlowKey, IoChannel};
    use bytes::Bytes;
    use std::collections::VecDeque;

    let cancel = CancellationToken::new();
    let counters = Arc::new(RelayCounters::new());
    let (handle, join) = Coordinator::start(
        SingleShotRelayWorkflow {
            idle_timeout: Duration::from_secs(5),
            counters: Arc::clone(&counters),
        },
        DEFAULT_RELAY_DRAIN_TIMEOUT,
        cancel,
    );

    let (a_in_tx, a_in_rx) = tokio::sync::mpsc::unbounded_channel();
    let (_b_in_tx, b_in_rx) = tokio::sync::mpsc::unbounded_channel();
    let allow_send_b = Arc::new(AtomicBool::new(false));
    let sent_b = Arc::new(parking_lot::Mutex::new(VecDeque::new()));
    let waker_b = Arc::new(parking_lot::Mutex::new(None));

    let a_dev = MockDatagramChannel {
        inbound_rx: a_in_rx,
        outbound_tx: tokio::sync::mpsc::unbounded_channel().0,
    };
    let b_dev = BackpressuredDatagram {
        inbound_rx: b_in_rx,
        allow_send: Arc::clone(&allow_send_b),
        sent: Arc::clone(&sent_b),
        waker_slot: Arc::clone(&waker_b),
    };

    handle
        .assign(SingleShotInput {
            a: IoChannel::from_datagram(a_dev),
            b: IoChannel::from_datagram(b_dev),
        })
        .await
        .unwrap();

    // Enqueue 11 bytes into queue_a_to_b while poll_send() returns Pending, then ForceStop.
    a_in_tx
        .send(Datagram {
            flow: FlowKey::default(),
            payload: Bytes::from_static(b"never-sent!"),
        })
        .unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    handle.force_stop();
    let summary = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.final_phase, TunnelPhase::Stopped);
    assert!(sent_b.lock().is_empty());
    // Unsent queued packet must NOT be counted as delivered bytes.
    assert_eq!(counters.snapshot(), (0, 0));
}

#[tokio::test]
async fn relay_graceful_drain_reports_io_errors_and_honors_timeout() {
    use crate::io::channel::{Datagram, FlowKey, IoChannel};
    use bytes::Bytes;

    struct DrainControllableStream {
        inner: DuplexStream,
        allow_write: Arc<AtomicBool>,
        fail_write: Arc<AtomicBool>,
        waker_slot: Arc<parking_lot::Mutex<Option<std::task::Waker>>>,
    }

    impl tokio::io::AsyncRead for DrainControllableStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl tokio::io::AsyncWrite for DrainControllableStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.fail_write.load(Ordering::Acquire) {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "simulated drain broken pipe",
                )));
            }
            if !self.allow_write.load(Ordering::Acquire) {
                *self.waker_slot.lock() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.fail_write.load(Ordering::Acquire) {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "simulated flush broken pipe",
                )));
            }
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.fail_write.load(Ordering::Acquire) {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "simulated shutdown broken pipe",
                )));
            }
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    impl TunnelStream for DrainControllableStream {
        fn poll_check_alive(&mut self, cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
            Pin::new(&mut self.inner).poll_check_alive(cx)
        }
    }

    struct FaultingDatagram {
        inbound_rx: tokio::sync::mpsc::UnboundedReceiver<Datagram>,
        allow_send: Arc<AtomicBool>,
        fail_send: Arc<AtomicBool>,
        waker_slot: Arc<parking_lot::Mutex<Option<std::task::Waker>>>,
    }

    impl crate::io::channel::DatagramIo for FaultingDatagram {
        fn poll_recv(
            &mut self,
            cx: &mut Context<'_>,
            batch: &mut [Datagram],
        ) -> Poll<io::Result<usize>> {
            match self.inbound_rx.poll_recv(cx) {
                Poll::Ready(Some(d)) if !batch.is_empty() => {
                    batch[0] = d;
                    Poll::Ready(Ok(1))
                }
                Poll::Ready(None) => Poll::Ready(Ok(0)),
                _ => Poll::Pending,
            }
        }

        fn poll_send(
            &mut self,
            cx: &mut Context<'_>,
            batch: &[Datagram],
        ) -> Poll<io::Result<usize>> {
            if self.fail_send.load(Ordering::Acquire) {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "simulated datagram drain broken pipe",
                )));
            }
            if !self.allow_send.load(Ordering::Acquire) {
                *self.waker_slot.lock() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            Poll::Ready(Ok(batch.len()))
        }
    }

    // 1. DuplexRelay: normal drain finishes cleanly -> TaskOutcome::GracefullyDrained.
    {
        let (handle, join) = Coordinator::start(
            SingleShotRelayWorkflow {
                idle_timeout: Duration::from_secs(5),
                counters: Arc::new(RelayCounters::new()),
            },
            Duration::from_millis(200),
            CancellationToken::new(),
        );
        let (mut peer_a, stream_a) = duplex(1024);
        let (mut peer_b, stream_b) = duplex(1024);
        let allow_b = Arc::new(AtomicBool::new(false));
        let fail_b = Arc::new(AtomicBool::new(false));
        let waker_b = Arc::new(parking_lot::Mutex::new(None));

        handle
            .assign(SingleShotInput {
                a: IoChannel::from_stream(stream_a),
                b: IoChannel::from_boxed_stream(Box::new(DrainControllableStream {
                    inner: stream_b,
                    allow_write: Arc::clone(&allow_b),
                    fail_write: Arc::clone(&fail_b),
                    waker_slot: Arc::clone(&waker_b),
                })),
            })
            .await
            .unwrap();

        peer_a.write_all(b"drain-ok").await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        handle.graceful_stop().await.unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        allow_b.store(true, Ordering::Release);
        if let Some(w) = waker_b.lock().take() {
            w.wake();
        }
        let mut got = [0u8; 8];
        peer_b.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"drain-ok");

        let summary = join.await.unwrap();
        assert_eq!(summary.completed_jobs.len(), 1);
        assert_eq!(
            summary.completed_jobs[0].outcome,
            TaskOutcome::GracefullyDrained
        );
    }

    // 2. DuplexRelay: BrokenPipe during graceful drain must report TaskOutcome::Failed.
    {
        let (handle, join) = Coordinator::start(
            SingleShotRelayWorkflow {
                idle_timeout: Duration::from_secs(5),
                counters: Arc::new(RelayCounters::new()),
            },
            Duration::from_millis(300),
            CancellationToken::new(),
        );
        let (mut peer_a, stream_a) = duplex(1024);
        let (_peer_b, stream_b) = duplex(1024);
        let allow_b = Arc::new(AtomicBool::new(false));
        let fail_b = Arc::new(AtomicBool::new(false));
        let waker_b = Arc::new(parking_lot::Mutex::new(None));

        handle
            .assign(SingleShotInput {
                a: IoChannel::from_stream(stream_a),
                b: IoChannel::from_boxed_stream(Box::new(DrainControllableStream {
                    inner: stream_b,
                    allow_write: Arc::clone(&allow_b),
                    fail_write: Arc::clone(&fail_b),
                    waker_slot: Arc::clone(&waker_b),
                })),
            })
            .await
            .unwrap();

        peer_a.write_all(b"will-fail-drain").await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        handle.graceful_stop().await.unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        fail_b.store(true, Ordering::Release);
        if let Some(w) = waker_b.lock().take() {
            w.wake();
        }

        let summary = join.await.unwrap();
        assert_eq!(summary.completed_jobs.len(), 1);
        assert!(matches!(
            summary.completed_jobs[0].outcome,
            TaskOutcome::Failed(_)
        ));
    }

    // 3. DuplexRelay: forever Pending during drain exits bounded by Relay's drain_timeout.
    {
        let (handle, join) = Coordinator::start(
            SingleShotRelayWorkflow {
                idle_timeout: Duration::from_secs(5),
                counters: Arc::new(RelayCounters::new()),
            },
            Duration::from_secs(2),
            CancellationToken::new(),
        );
        let (mut peer_a, stream_a) = duplex(1024);
        let (_peer_b, stream_b) = duplex(1024);
        let allow_b = Arc::new(AtomicBool::new(false));
        let fail_b = Arc::new(AtomicBool::new(false));
        let waker_b = Arc::new(parking_lot::Mutex::new(None));

        handle
            .assign(SingleShotInput {
                a: IoChannel::from_stream(stream_a),
                b: IoChannel::from_boxed_stream(Box::new(DrainControllableStream {
                    inner: stream_b,
                    allow_write: Arc::clone(&allow_b),
                    fail_write: Arc::clone(&fail_b),
                    waker_slot: Arc::clone(&waker_b),
                })),
            })
            .await
            .unwrap();

        peer_a.write_all(b"stuck-drain").await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        handle.graceful_stop().await.unwrap();
        let summary = tokio::time::timeout(Duration::from_millis(1500), join)
            .await
            .expect("forever-pending drain must exit within relay drain_timeout (500ms)")
            .unwrap();
        assert_eq!(summary.completed_jobs.len(), 1);
        assert_eq!(
            summary.completed_jobs[0].outcome,
            TaskOutcome::GracefullyDrained
        );
    }

    // 4. DatagramStreamRelay: BrokenPipe during drain -> Failed; forever Pending -> bounded timeout.
    for fail_during_drain in [true, false] {
        let (handle, join) = Coordinator::start(
            SingleShotRelayWorkflow {
                idle_timeout: Duration::from_secs(5),
                counters: Arc::new(RelayCounters::new()),
            },
            Duration::from_secs(2),
            CancellationToken::new(),
        );
        let (dgram_tx, dgram_rx) = tokio::sync::mpsc::unbounded_channel();
        let allow_stream = Arc::new(AtomicBool::new(false));
        let fail_stream = Arc::new(AtomicBool::new(false));
        let waker_stream = Arc::new(parking_lot::Mutex::new(None));
        let (_peer_s, stream_end) = duplex(1024);

        handle
            .assign(SingleShotInput {
                a: IoChannel::from_datagram(MockDatagramChannel {
                    inbound_rx: dgram_rx,
                    outbound_tx: tokio::sync::mpsc::unbounded_channel().0,
                }),
                b: IoChannel::from_boxed_stream(Box::new(DrainControllableStream {
                    inner: stream_end,
                    allow_write: Arc::clone(&allow_stream),
                    fail_write: Arc::clone(&fail_stream),
                    waker_slot: Arc::clone(&waker_stream),
                })),
            })
            .await
            .unwrap();

        dgram_tx
            .send(Datagram {
                flow: FlowKey::default(),
                payload: Bytes::from_static(b"ds-drain"),
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        if fail_during_drain {
            handle.graceful_stop().await.unwrap();
            tokio::time::sleep(Duration::from_millis(15)).await;
            fail_stream.store(true, Ordering::Release);
            if let Some(w) = waker_stream.lock().take() {
                w.wake();
            }
            let summary = join.await.unwrap();
            assert!(matches!(
                summary.completed_jobs[0].outcome,
                TaskOutcome::Failed(_)
            ));
        } else {
            handle.graceful_stop().await.unwrap();
            let summary = tokio::time::timeout(Duration::from_millis(1500), join)
                .await
                .expect("datagram-stream pending drain must exit cleanly within 500ms")
                .unwrap();
            assert_eq!(
                summary.completed_jobs[0].outcome,
                TaskOutcome::GracefullyDrained
            );
        }
    }

    // 5. DatagramDatagramRelay: BrokenPipe during drain -> Failed; forever Pending -> bounded timeout.
    for fail_during_drain in [true, false] {
        let (handle, join) = Coordinator::start(
            SingleShotRelayWorkflow {
                idle_timeout: Duration::from_secs(5),
                counters: Arc::new(RelayCounters::new()),
            },
            Duration::from_secs(2),
            CancellationToken::new(),
        );
        let (a_tx, a_rx) = tokio::sync::mpsc::unbounded_channel();
        let (_b_tx, b_rx) = tokio::sync::mpsc::unbounded_channel();
        let allow_b = Arc::new(AtomicBool::new(false));
        let fail_b = Arc::new(AtomicBool::new(false));
        let waker_b = Arc::new(parking_lot::Mutex::new(None));

        handle
            .assign(SingleShotInput {
                a: IoChannel::from_datagram(MockDatagramChannel {
                    inbound_rx: a_rx,
                    outbound_tx: tokio::sync::mpsc::unbounded_channel().0,
                }),
                b: IoChannel::from_datagram(FaultingDatagram {
                    inbound_rx: b_rx,
                    allow_send: Arc::clone(&allow_b),
                    fail_send: Arc::clone(&fail_b),
                    waker_slot: Arc::clone(&waker_b),
                }),
            })
            .await
            .unwrap();

        a_tx.send(Datagram {
            flow: FlowKey::default(),
            payload: Bytes::from_static(b"dd-drain"),
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        if fail_during_drain {
            handle.graceful_stop().await.unwrap();
            tokio::time::sleep(Duration::from_millis(15)).await;
            fail_b.store(true, Ordering::Release);
            if let Some(w) = waker_b.lock().take() {
                w.wake();
            }
            let summary = join.await.unwrap();
            assert!(matches!(
                summary.completed_jobs[0].outcome,
                TaskOutcome::Failed(_)
            ));
        } else {
            handle.graceful_stop().await.unwrap();
            let summary = tokio::time::timeout(Duration::from_millis(1500), join)
                .await
                .expect("datagram-datagram pending drain must exit cleanly within 500ms")
                .unwrap();
            assert_eq!(
                summary.completed_jobs[0].outcome,
                TaskOutcome::GracefullyDrained
            );
        }
    }
}
