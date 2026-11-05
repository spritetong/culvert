// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Compact binary frame definitions and Tokio codec for multiplexing.
//! All comments and docstrings must be in English.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

/// Maximum payload size per frame (64 KB) to ensure fair scheduling between streams.
pub const MAX_FRAME_PAYLOAD_SIZE: usize = 64 * 1024;

/// Frame header length: 1 byte type + 4 bytes stream_id + 4 bytes payload_len = 9 bytes.
pub const FRAME_HEADER_LEN: usize = 9;

/// Multiplexing frame types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    /// Carries payload data for a logical stream.
    Data = 0x01,
    /// Requests opening a new logical stream.
    Syn = 0x02,
    /// Half-closes writing direction of a logical stream (graceful FIN).
    Fin = 0x03,
    /// Abortively resets a stream and discards any in-flight frames.
    Rst = 0x04,
    /// Keepalive ping request.
    Ping = 0x05,
    /// Keepalive pong response.
    Pong = 0x06,
}

impl TryFrom<u8> for FrameType {
    type Error = MuxFrameError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(FrameType::Data),
            0x02 => Ok(FrameType::Syn),
            0x03 => Ok(FrameType::Fin),
            0x04 => Ok(FrameType::Rst),
            0x05 => Ok(FrameType::Ping),
            0x06 => Ok(FrameType::Pong),
            other => Err(MuxFrameError::UnknownFrameType(other)),
        }
    }
}

/// Errors occurring during frame encoding or decoding.
#[derive(Debug, Error)]
pub enum MuxFrameError {
    #[error("Unknown multiplex frame type identifier: {0:#x}")]
    UnknownFrameType(u8),

    #[error("Frame payload size ({0} bytes) exceeds maximum limit ({1} bytes)")]
    PayloadTooLarge(usize, usize),

    #[error("Underlying I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Discrete multiplex protocol frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MuxFrame {
    pub frame_type: FrameType,
    pub stream_id: u32,
    pub payload: Bytes,
}

impl MuxFrame {
    /// Creates a new `SYN` frame opening `stream_id`.
    pub fn syn(stream_id: u32) -> Self {
        Self {
            frame_type: FrameType::Syn,
            stream_id,
            payload: Bytes::new(),
        }
    }

    /// Creates a new `DATA` frame carrying bytes for `stream_id`.
    pub fn data(stream_id: u32, payload: Bytes) -> Self {
        Self {
            frame_type: FrameType::Data,
            stream_id,
            payload,
        }
    }

    /// Creates a new graceful `FIN` frame for `stream_id`.
    pub fn fin(stream_id: u32) -> Self {
        Self {
            frame_type: FrameType::Fin,
            stream_id,
            payload: Bytes::new(),
        }
    }

    /// Creates an abortive `RST` frame for `stream_id`.
    pub fn rst(stream_id: u32) -> Self {
        Self {
            frame_type: FrameType::Rst,
            stream_id,
            payload: Bytes::new(),
        }
    }

    /// Creates a keepalive `PING` frame.
    pub fn ping(seq: u32) -> Self {
        Self {
            frame_type: FrameType::Ping,
            stream_id: seq,
            payload: Bytes::new(),
        }
    }

    /// Creates a keepalive `PONG` frame.
    pub fn pong(seq: u32) -> Self {
        Self {
            frame_type: FrameType::Pong,
            stream_id: seq,
            payload: Bytes::new(),
        }
    }
}

/// Tokio codec for framing and deframing multiplex frames.
#[derive(Debug, Default, Clone)]
pub struct MuxCodec;

impl Decoder for MuxCodec {
    type Item = MuxFrame;
    type Error = MuxFrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < FRAME_HEADER_LEN {
            return Ok(None);
        }

        let frame_type = FrameType::try_from(src[0])?;
        let stream_id = u32::from_be_bytes([src[1], src[2], src[3], src[4]]);
        let payload_len = u32::from_be_bytes([src[5], src[6], src[7], src[8]]) as usize;

        if payload_len > MAX_FRAME_PAYLOAD_SIZE {
            return Err(MuxFrameError::PayloadTooLarge(
                payload_len,
                MAX_FRAME_PAYLOAD_SIZE,
            ));
        }

        if src.len() < FRAME_HEADER_LEN + payload_len {
            src.reserve(FRAME_HEADER_LEN + payload_len - src.len());
            return Ok(None);
        }

        src.advance(FRAME_HEADER_LEN);
        let payload = src.split_to(payload_len).freeze();

        Ok(Some(MuxFrame {
            frame_type,
            stream_id,
            payload,
        }))
    }
}

impl Encoder<MuxFrame> for MuxCodec {
    type Error = MuxFrameError;

    fn encode(&mut self, item: MuxFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload_len = item.payload.len();
        if payload_len > MAX_FRAME_PAYLOAD_SIZE {
            return Err(MuxFrameError::PayloadTooLarge(
                payload_len,
                MAX_FRAME_PAYLOAD_SIZE,
            ));
        }

        dst.reserve(FRAME_HEADER_LEN + payload_len);
        dst.put_u8(item.frame_type as u8);
        dst.put_u32(item.stream_id);
        dst.put_u32(payload_len as u32);
        dst.extend_from_slice(&item.payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mux_frame_codec_roundtrip() {
        let mut codec = MuxCodec;
        let mut buf = BytesMut::new();

        let original = MuxFrame::data(42, Bytes::from_static(b"hello multiplexer"));
        codec.encode(original.clone(), &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().expect("must decode frame");
        assert_eq!(original, decoded);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_mux_frame_partial_decode() {
        let mut codec = MuxCodec;
        let mut buf = BytesMut::new();

        let original = MuxFrame::data(100, Bytes::from_static(b"streaming chunk"));
        codec.encode(original.clone(), &mut buf).unwrap();

        let mut partial = buf.split_to(5);
        assert!(codec.decode(&mut partial).unwrap().is_none());

        partial.unsplit(buf);
        let decoded = codec
            .decode(&mut partial)
            .unwrap()
            .expect("should complete");
        assert_eq!(original, decoded);
    }
}
