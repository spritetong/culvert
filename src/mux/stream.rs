//! Logical multiplexed stream implementation adhering to `TunnelStream`.
//! All comments and docstrings must be in English.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;

use crate::core::stream::{HeartbeatStatus, TunnelStream};
use crate::mux::frame::{MAX_FRAME_PAYLOAD_SIZE, MuxFrame};

/// A logical bi-directional multiplexed sub-stream.
pub struct MuxStream {
    stream_id: u32,
    /// Channel for sending outgoing frames to the multiplexer writer task with waker registration.
    outbound: PollSender<MuxFrame>,
    /// Channel for receiving inbound data slices from the multiplexer reader task.
    rx_data: mpsc::Receiver<Bytes>,
    /// Partially consumed incoming slice from a previous read poll.
    read_buf: Option<Bytes>,
    /// In-flight pending payload waiting for outbound channel capacity.
    pending_payload: Option<(Bytes, usize)>,
    /// Flag indicating if local write direction has sent FIN.
    fin_sent: bool,
    /// Flag indicating if stream is terminated/reset.
    closed: Arc<AtomicBool>,
}

impl MuxStream {
    /// Constructs a new `MuxStream`.
    pub fn new(
        stream_id: u32,
        tx_frames: mpsc::Sender<MuxFrame>,
        rx_data: mpsc::Receiver<Bytes>,
        closed: Arc<AtomicBool>,
    ) -> Self {
        Self {
            stream_id,
            outbound: PollSender::new(tx_frames),
            rx_data,
            read_buf: None,
            pending_payload: None,
            fin_sent: false,
            closed,
        }
    }

    /// Returns the logical identifier of this multiplexed stream.
    pub fn stream_id(&self) -> u32 {
        self.stream_id
    }
}

impl AsyncRead for MuxStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // First consume any remaining bytes from read_buf
        if let Some(mut current) = self.read_buf.take() {
            let to_copy = std::cmp::min(buf.remaining(), current.len());
            buf.put_slice(&current[..to_copy]);
            current.advance(to_copy);
            if current.has_remaining() {
                self.read_buf = Some(current);
            }
            return Poll::Ready(Ok(()));
        }

        // Poll incoming data channel
        match self.rx_data.poll_recv(cx) {
            Poll::Ready(Some(mut bytes)) => {
                let to_copy = std::cmp::min(buf.remaining(), bytes.len());
                buf.put_slice(&bytes[..to_copy]);
                bytes.advance(to_copy);
                if bytes.has_remaining() {
                    self.read_buf = Some(bytes);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => {
                // Channel closed by remote FIN or reset: EOF
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for MuxStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.fin_sent || self.closed.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Stream write closed or reset",
            )));
        }

        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let this = &mut *self;
        if this.pending_payload.is_none() {
            let chunk_size = std::cmp::min(buf.len(), MAX_FRAME_PAYLOAD_SIZE);
            let payload = Bytes::copy_from_slice(&buf[..chunk_size]);
            this.pending_payload = Some((payload, chunk_size));
        }

        match this.outbound.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let (payload, chunk_size) = this.pending_payload.take().unwrap();
                let frame = MuxFrame::data(this.stream_id, payload);
                if this.outbound.send_item(frame).is_err() {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "Multiplexer session terminated",
                    )));
                }
                Poll::Ready(Ok(chunk_size))
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "Multiplexer session terminated",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.fin_sent {
            return Poll::Ready(Ok(()));
        }

        let this = &mut *self;
        match this.outbound.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let frame = MuxFrame::fin(this.stream_id);
                let _ = this.outbound.send_item(frame);
                this.fin_sent = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(_)) => {
                this.fin_sent = true;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl TunnelStream for MuxStream {
    fn poll_check_alive(&mut self, _cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        if self.closed.load(Ordering::Acquire) {
            Poll::Ready(HeartbeatStatus::Dead)
        } else {
            Poll::Ready(HeartbeatStatus::Alive)
        }
    }
}

impl Drop for MuxStream {
    fn drop(&mut self) {
        let is_already_closed = self.closed.swap(true, Ordering::AcqRel);
        if !is_already_closed && !self.fin_sent {
            // Unclean drop without graceful shutdown: emit RST to immediately inform peer
            // and trigger silent dropping of any remaining in-flight frames.
            if let Some(sender) = self.outbound.get_ref() {
                let _ = sender.try_send(MuxFrame::rst(self.stream_id));
            }
        }
    }
}
