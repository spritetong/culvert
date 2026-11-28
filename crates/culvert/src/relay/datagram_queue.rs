// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Bounded datagram batch queue and length-prefixed framing helpers for `relay`.
//! All comments and docstrings must be in English.

use crate::consts::MAX_UDP_DATAGRAM_SIZE;
use crate::core::error::TaskError;
use crate::io::channel::{Datagram, DatagramIo, FlowKey};
use bytes::{Buf, BufMut, BytesMut};
use std::collections::VecDeque;
use std::io;
use std::task::{Context, Poll};

pub(crate) const DATAGRAM_BATCH_SIZE: usize = 4;
pub(crate) const MAX_PENDING_DATAGRAMS: usize = 64;
pub(crate) const MAX_PENDING_DATAGRAM_BYTES: usize = 256 * 1024;

/// Bounded outbound datagram queue with byte-budget enforcement.
///
/// `total_bytes()` tracks payload bytes actually accepted by `dst.poll_send()`
/// in `poll_flush()`, matching TCP `TransferDirection::total_bytes()` semantics.
pub struct DatagramQueue {
    queue: VecDeque<Datagram>,
    queued_bytes: usize,
    total_bytes: u64,
}

impl DatagramQueue {
    /// Creates an empty `DatagramQueue`.
    pub fn new() -> Self {
        Self {
            queue: VecDeque::with_capacity(DATAGRAM_BATCH_SIZE),
            queued_bytes: 0,
            total_bytes: 0,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Returns total payload bytes successfully written to the outbound `DatagramIo`.
    #[inline]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Returns how many more datagrams can be pulled without violating packet or byte budgets.
    #[inline]
    pub fn available_batch_slots(&self, batch_capacity: usize) -> usize {
        let packet_slots = MAX_PENDING_DATAGRAMS.saturating_sub(self.queue.len());
        let byte_slots =
            MAX_PENDING_DATAGRAM_BYTES.saturating_sub(self.queued_bytes) / MAX_UDP_DATAGRAM_SIZE;
        packet_slots.min(byte_slots).min(batch_capacity)
    }

    /// Returns `true` when at least one more max-sized datagram fits in the pending budget.
    #[inline]
    pub fn can_accept_stream_frame(&self) -> bool {
        self.queue.len() < MAX_PENDING_DATAGRAMS
            && self.queued_bytes + MAX_UDP_DATAGRAM_SIZE <= MAX_PENDING_DATAGRAM_BYTES
    }

    /// Enqueues a datagram after validating payload size and pending queue budget.
    pub fn push(&mut self, datagram: Datagram) -> io::Result<()> {
        let plen = datagram.payload.len();
        if plen > MAX_UDP_DATAGRAM_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UDP datagram exceeds maximum payload size",
            ));
        }
        if self.queued_bytes + plen > MAX_PENDING_DATAGRAM_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UDP datagram batch exceeded pending byte budget",
            ));
        }
        self.queued_bytes += plen;
        self.queue.push_back(datagram);
        Ok(())
    }

    /// Decodes complete `[u16 len | payload]` frames from `rx_buf` into the queue.
    pub fn decode_frames(&mut self, rx_buf: &mut BytesMut) -> io::Result<bool> {
        let before = self.queue.len();
        while self.queue.len() < MAX_PENDING_DATAGRAMS && rx_buf.len() >= 2 {
            let frame_len = u16::from_be_bytes([rx_buf[0], rx_buf[1]]) as usize;
            if frame_len > MAX_UDP_DATAGRAM_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "UDP datagram exceeds maximum payload size",
                ));
            }
            if self.queued_bytes + frame_len > MAX_PENDING_DATAGRAM_BYTES {
                break;
            }
            if rx_buf.len() < 2 + frame_len {
                break;
            }

            rx_buf.advance(2);
            let payload = rx_buf.split_to(frame_len).freeze();
            self.queued_bytes += frame_len;
            self.queue.push_back(Datagram {
                flow: FlowKey::default(),
                payload,
            });
        }
        Ok(self.queue.len() > before)
    }

    /// Encodes a received batch of datagrams into `[u16 len | payload]` frames in `scratch`,
    /// returning total unframed payload bytes contained in the encoded batch.
    pub fn encode_batch(scratch: &mut BytesMut, batch: &[Datagram]) -> io::Result<u64> {
        scratch.clear();
        let mut payload_bytes = 0u64;
        for d in batch {
            let plen = d.payload.len();
            if plen > MAX_UDP_DATAGRAM_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "UDP datagram exceeds maximum payload size",
                ));
            }
            if scratch.len() + 2 + plen > MAX_PENDING_DATAGRAM_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "UDP datagram batch exceeded pending byte budget",
                ));
            }
            scratch.put_u16(plen as u16);
            scratch.extend_from_slice(&d.payload);
            payload_bytes += plen as u64;
        }
        Ok(payload_bytes)
    }

    /// Flushes queued datagrams to `dst`, returning `Ok(true)` if any datagrams were sent.
    pub fn poll_flush(
        &mut self,
        cx: &mut Context<'_>,
        dst: &mut dyn DatagramIo,
    ) -> Result<bool, TaskError> {
        let mut flushed_any = false;
        while !self.queue.is_empty() {
            match dst.poll_send(cx, self.queue.make_contiguous()) {
                Poll::Ready(Ok(0)) => {
                    return Err(TaskError::StageFailed(
                        "datagram sender returned zero sent".into(),
                    ));
                }
                Poll::Ready(Ok(sent)) => {
                    let count = sent.min(self.queue.len());
                    let sent_bytes: usize =
                        self.queue.iter().take(count).map(|d| d.payload.len()).sum();
                    self.queue.drain(..count);
                    self.queued_bytes -= sent_bytes;
                    self.total_bytes += sent_bytes as u64;
                    flushed_any = true;
                }
                Poll::Ready(Err(err)) => {
                    return Err(TaskError::StageFailed(err.to_string()));
                }
                Poll::Pending => break,
            }
        }
        Ok(flushed_any)
    }
}

impl Default for DatagramQueue {
    fn default() -> Self {
        Self::new()
    }
}
