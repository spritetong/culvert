// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Directional transfer state machine for one half of a duplex byte relay.
//! All comments and docstrings must be in English.

use crate::io::stream::{StreamCaps, TunnelStream};
use bytes::Bytes;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::ReadBuf;

/// Buffered unidirectional transfer state between a reader and a writer stream.
pub struct TransferDirection {
    buffer: Vec<u8>,
    read_pos: usize,
    write_pos: usize,
    in_flight_chunk: Option<Bytes>,
    chunk_pos: usize,
    read_done: bool,
    write_shutdown: bool,
    total_bytes: u64,
}

impl TransferDirection {
    /// Allocates a new transfer buffer of `capacity` bytes.
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![0u8; capacity],
            read_pos: 0,
            write_pos: 0,
            in_flight_chunk: None,
            chunk_pos: 0,
            read_done: false,
            write_shutdown: false,
            total_bytes: 0,
        }
    }

    /// Returns `true` if the source side has already reached EOF (or stopped reading).
    #[inline]
    pub fn is_read_done(&self) -> bool {
        self.read_done
    }

    /// Returns `true` when reading is finished, all buffered bytes are flushed, and
    /// write half-close has completed.
    #[inline]
    pub fn is_finished(&self) -> bool {
        self.read_done
            && self.write_pos == self.read_pos
            && self.in_flight_chunk.is_none()
            && self.write_shutdown
    }

    /// Returns the total number of bytes written in this direction.
    #[inline]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Advances one poll step for this direction. Returns `Ok(true)` if any I/O progress occurred.
    pub fn poll_step(
        &mut self,
        cx: &mut Context<'_>,
        reader: &mut dyn TunnelStream,
        writer: &mut dyn TunnelStream,
        drain_only: bool,
        writer_caps: StreamCaps,
    ) -> Poll<io::Result<bool>> {
        let mut progress = false;

        if writer_caps.owned_write {
            while let Some(ref chunk) = self.in_flight_chunk {
                let to_write = if self.chunk_pos == 0 {
                    chunk.clone()
                } else {
                    chunk.slice(self.chunk_pos..)
                };

                match writer.poll_write_owned(cx, to_write) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "writer returned zero bytes",
                        )));
                    }
                    Poll::Ready(Ok(n)) => {
                        self.chunk_pos += n;
                        self.total_bytes += n as u64;
                        progress = true;
                        if self.chunk_pos >= chunk.len() {
                            self.in_flight_chunk = None;
                            self.chunk_pos = 0;
                        }
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => break,
                }
            }

            if !self.read_done && !drain_only && self.in_flight_chunk.is_none() {
                let mut read_buf = ReadBuf::new(&mut self.buffer);
                match Pin::new(&mut *reader).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let n = read_buf.filled().len();
                        if n == 0 {
                            self.read_done = true;
                        } else {
                            self.in_flight_chunk = Some(Bytes::copy_from_slice(&self.buffer[..n]));
                            self.chunk_pos = 0;
                        }
                        progress = true;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => {}
                }
            }
        } else {
            while self.write_pos < self.read_pos {
                match Pin::new(&mut *writer)
                    .poll_write(cx, &self.buffer[self.write_pos..self.read_pos])
                {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "writer returned zero bytes",
                        )));
                    }
                    Poll::Ready(Ok(n)) => {
                        self.write_pos += n;
                        self.total_bytes += n as u64;
                        progress = true;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => break,
                }
            }

            if self.write_pos == self.read_pos {
                self.read_pos = 0;
                self.write_pos = 0;
            }

            if !self.read_done && !drain_only && self.read_pos == 0 {
                let mut read_buf = ReadBuf::new(&mut self.buffer);
                match Pin::new(&mut *reader).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let n = read_buf.filled().len();
                        if n == 0 {
                            self.read_done = true;
                        } else {
                            self.read_pos = n;
                        }
                        progress = true;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => {}
                }
            }
        }

        if drain_only {
            self.read_done = true;
        }

        let all_written = if writer_caps.owned_write {
            self.in_flight_chunk.is_none()
        } else {
            self.write_pos == self.read_pos
        };

        if self.read_done && all_written && !self.write_shutdown {
            match Pin::new(&mut *writer).poll_flush(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(err)) => {
                    tracing::debug!(error = %err, "Relay writer flush error during shutdown");
                }
                Poll::Pending => return Poll::Ready(Ok(progress)),
            }

            if writer_caps.half_close {
                match Pin::new(&mut *writer).poll_shutdown(cx) {
                    Poll::Ready(Ok(())) => {
                        self.write_shutdown = true;
                        progress = true;
                    }
                    Poll::Ready(Err(err)) => {
                        tracing::debug!(error = %err, "Relay writer shutdown error");
                        self.write_shutdown = true;
                        progress = true;
                    }
                    Poll::Pending => {}
                }
            } else {
                self.write_shutdown = true;
                progress = true;
            }
        }

        Poll::Ready(Ok(progress))
    }
}
