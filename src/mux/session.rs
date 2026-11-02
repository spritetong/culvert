//! Multiplexing session manager operating on any underlying `TunnelStream`.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use tokio::io::split;
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use crate::core::error::TunnelError;
use crate::core::stream::TunnelStream;
use crate::mux::frame::{FrameType, MuxCodec, MuxFrame};
use crate::mux::stream::MuxStream;

/// Default inbound stream channel buffer size per logical stream.
const STREAM_BUFFER_SIZE: usize = 128;
/// Default outbound frame channel queue capacity.
const SESSION_FRAME_QUEUE_SIZE: usize = 256;

/// Multiplex session driving multiple logical streams over a single underlying transport.
#[derive(Clone)]
pub struct MuxSession {
    /// Outbound frame queue to the transport writer task.
    tx_frames: mpsc::Sender<MuxFrame>,
    /// Inbound newly opened streams channel.
    rx_new_streams: Arc<tokio::sync::Mutex<mpsc::Receiver<MuxStream>>>,
    /// Next logical stream identifier counter.
    next_stream_id: Arc<AtomicU32>,
    /// Map of active stream IDs to their inbound data sender and termination flag.
    active_streams: Arc<DashMap<u32, (mpsc::Sender<Bytes>, Arc<AtomicBool>)>>,
    /// Set of closed / reset stream IDs whose trailing in-flight frames are silently dropped.
    closed_streams: Arc<DashMap<u32, ()>>,
    /// Session cancellation token.
    cancel_token: CancellationToken,
}

impl MuxSession {
    /// Initializes a new `MuxSession` over an arbitrary underlying `TunnelStream`.
    ///
    /// - `is_client`: If `true`, allocates odd stream IDs (1, 3, 5...). If `false`, allocates even IDs (2, 4, 6...).
    pub fn new(
        underlying: Box<dyn TunnelStream>,
        is_client: bool,
        cancel_token: CancellationToken,
    ) -> Self {
        let (tx_frames, rx_frames) = mpsc::channel(SESSION_FRAME_QUEUE_SIZE);
        let (tx_new_streams, rx_new_streams) = mpsc::channel(64);

        let initial_id = if is_client { 1 } else { 2 };
        let next_stream_id = Arc::new(AtomicU32::new(initial_id));
        let active_streams: Arc<DashMap<u32, (mpsc::Sender<Bytes>, Arc<AtomicBool>)>> =
            Arc::new(DashMap::new());
        let closed_streams: Arc<DashMap<u32, ()>> = Arc::new(DashMap::new());

        let session = Self {
            tx_frames: tx_frames.clone(),
            rx_new_streams: Arc::new(tokio::sync::Mutex::new(rx_new_streams)),
            next_stream_id,
            active_streams: Arc::clone(&active_streams),
            closed_streams: Arc::clone(&closed_streams),
            cancel_token: cancel_token.clone(),
        };

        // Spawn session driver tasks
        Self::spawn_driver(
            underlying,
            rx_frames,
            tx_frames,
            tx_new_streams,
            active_streams,
            closed_streams,
            cancel_token,
        );

        session
    }

    /// Spawns the reader and writer tasks for frame multiplexing.
    fn spawn_driver(
        underlying: Box<dyn TunnelStream>,
        mut rx_frames: mpsc::Receiver<MuxFrame>,
        tx_frames: mpsc::Sender<MuxFrame>,
        tx_new_streams: mpsc::Sender<MuxStream>,
        active_streams: Arc<DashMap<u32, (mpsc::Sender<Bytes>, Arc<AtomicBool>)>>,
        closed_streams: Arc<DashMap<u32, ()>>,
        cancel_token: CancellationToken,
    ) {
        let (read_half, write_half) = split(underlying);
        let mut framed_reader = FramedRead::new(read_half, MuxCodec);
        let mut framed_writer = FramedWrite::new(write_half, MuxCodec);

        // Writer task: drains outbound frames from `rx_frames` to `framed_writer`
        let cancel_writer = cancel_token.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_writer.cancelled() => break,
                    frame_opt = rx_frames.recv() => {
                        match frame_opt {
                            Some(frame) => {
                                if let Err(e) = framed_writer.send(frame).await {
                                    debug!("Multiplexer writer send error: {:?}", e);
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
            let _ = framed_writer.flush().await;
        });

        // Reader task: decodes inbound frames from `framed_reader` and demuxes to streams
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    frame_res = framed_reader.next() => {
                        match frame_res {
                            Some(Ok(frame)) => {
                                Self::dispatch_inbound_frame(
                                    frame,
                                    &tx_frames,
                                    &tx_new_streams,
                                    &active_streams,
                                    &closed_streams,
                                ).await;
                            }
                            Some(Err(e)) => {
                                warn!("Multiplexer frame decoding error: {:?}", e);
                                break;
                            }
                            None => {
                                trace!("Multiplexer underlying stream EOF received");
                                break;
                            }
                        }
                    }
                }
            }

            // Cleanup all active streams on disconnect
            active_streams.clear();
            cancel_token.cancel();
        });
    }

    /// Dispatches an incoming frame to the corresponding logical stream or control handler.
    async fn dispatch_inbound_frame(
        frame: MuxFrame,
        tx_frames: &mpsc::Sender<MuxFrame>,
        tx_new_streams: &mpsc::Sender<MuxStream>,
        active_streams: &Arc<DashMap<u32, (mpsc::Sender<Bytes>, Arc<AtomicBool>)>>,
        closed_streams: &Arc<DashMap<u32, ()>>,
    ) {
        match frame.frame_type {
            FrameType::Syn => {
                let stream_id = frame.stream_id;
                // If stream was previously closed or is already active, ignore duplicate SYN
                if closed_streams.contains_key(&stream_id)
                    || active_streams.contains_key(&stream_id)
                {
                    return;
                }

                let (data_tx, data_rx) = mpsc::channel(STREAM_BUFFER_SIZE);
                let closed_flag = Arc::new(AtomicBool::new(false));
                active_streams.insert(stream_id, (data_tx, Arc::clone(&closed_flag)));

                let new_stream = MuxStream::new(stream_id, tx_frames.clone(), data_rx, closed_flag);

                match tx_new_streams.try_send(new_stream) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        warn!(
                            stream_id,
                            "Multiplexer inbound stream backlog full; rejecting SYN with RST"
                        );
                        let _ = tx_frames.try_send(MuxFrame::rst(stream_id));
                        active_streams.remove(&stream_id);
                        closed_streams.insert(stream_id, ());
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        active_streams.remove(&stream_id);
                    }
                }
            }
            FrameType::Data => {
                let stream_id = frame.stream_id;
                // Discard frames for already closed/reset streams to prevent cross-session contamination
                if closed_streams.contains_key(&stream_id) {
                    trace!(
                        stream_id,
                        "Silently discarding in-flight data frame for terminated stream"
                    );
                    return;
                }

                if let Some(entry) = active_streams.get(&stream_id) {
                    let (data_tx, _) = entry.value();
                    if let Err(e) = data_tx.try_send(frame.payload) {
                        match e {
                            mpsc::error::TrySendError::Full(_) => {
                                warn!(
                                    stream_id,
                                    "Stream buffer full; dropping frame to avoid stall"
                                );
                            }
                            mpsc::error::TrySendError::Closed(_) => {
                                active_streams.remove(&stream_id);
                                closed_streams.insert(stream_id, ());
                            }
                        }
                    }
                }
            }
            FrameType::Fin => {
                let stream_id = frame.stream_id;
                // Dropping the data sender triggers EOF (0 bytes) on the reader side of MuxStream
                active_streams.remove(&stream_id);
                closed_streams.insert(stream_id, ());
            }
            FrameType::Rst => {
                let stream_id = frame.stream_id;
                if let Some((_, (_, closed_flag))) = active_streams.remove(&stream_id) {
                    closed_flag.store(true, Ordering::Release);
                }
                closed_streams.insert(stream_id, ());
            }
            FrameType::Ping => {
                // Instantly reply with PONG
                let _ = tx_frames.try_send(MuxFrame::pong(frame.stream_id));
            }
            FrameType::Pong => {
                trace!(
                    seq = frame.stream_id,
                    "Multiplexer heartbeat pong acknowledged"
                );
            }
        }
    }

    /// Opens a new outgoing logical stream.
    pub async fn open_stream(&self) -> Result<MuxStream, TunnelError> {
        if self.cancel_token.is_cancelled() {
            return Err(TunnelError::Multiplex(
                "Multiplexer session is closed".to_string(),
            ));
        }

        let stream_id = self.next_stream_id.fetch_add(2, Ordering::AcqRel);
        let (data_tx, data_rx) = mpsc::channel(STREAM_BUFFER_SIZE);
        let closed_flag = Arc::new(AtomicBool::new(false));

        self.active_streams
            .insert(stream_id, (data_tx, Arc::clone(&closed_flag)));

        // Send SYN to remote peer
        if let Err(e) = self.tx_frames.send(MuxFrame::syn(stream_id)).await {
            self.active_streams.remove(&stream_id);
            return Err(TunnelError::Multiplex(format!(
                "Failed to send SYN frame: {:?}",
                e
            )));
        }

        Ok(MuxStream::new(
            stream_id,
            self.tx_frames.clone(),
            data_rx,
            closed_flag,
        ))
    }

    /// Sends a raw frame directly to the session writer queue (useful for testing or injects).
    pub async fn send_frame(
        &self,
        frame: MuxFrame,
    ) -> Result<(), mpsc::error::SendError<MuxFrame>> {
        self.tx_frames.send(frame).await
    }

    /// Accepts the next incoming logical stream initiated by the remote peer.
    pub async fn accept_stream(&self) -> Option<MuxStream> {
        let mut rx = self.rx_new_streams.lock().await;
        rx.recv().await
    }

    /// Returns the number of currently active streams in this session.
    pub fn active_stream_count(&self) -> usize {
        self.active_streams.len()
    }

    /// Returns the number of closed/reset streams tracked for in-flight discarding.
    pub fn closed_stream_count(&self) -> usize {
        self.closed_streams.len()
    }

    /// Gracefully closes the session.
    pub fn close(&self) {
        self.cancel_token.cancel();
    }
}
