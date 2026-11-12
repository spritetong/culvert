// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Unified stream activation encoding and decoding for both forward and reverse pools.
//! All comments and docstrings must be in English.

use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use crate::common::{CancelFutureExt, TimeoutOrCancel};
use crate::consts::{
    ACTIVATE_STREAM_MAGIC, ACTIVATE_STREAM_PROMOTE, ACTIVATE_STREAM_UDP, MAX_ACTIVATION_TARGET_LEN,
};
use crate::core::error::TunnelError;
use crate::io::addr::{L4Protocol, TargetAddr, parse_target_addr};

/// Result of decoding an activation packet from a pooled stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationHeader {
    /// L4 transport protocol for the activated stream.
    pub protocol: L4Protocol,
    /// Optional dynamic target address encoded in the activation payload.
    pub target: Option<TargetAddr>,
    /// Whether the stream was promoted to a signaling channel (magic `0xA6`).
    pub is_promotion: bool,
}

/// Encodes an activation packet with target and protocol: `[magic, len: u16, target_str]`.
pub fn encode_activation_with_proto(target: Option<&TargetAddr>, protocol: L4Protocol) -> Vec<u8> {
    let mut pkt = Vec::new();
    let magic = match protocol {
        L4Protocol::Tcp => ACTIVATE_STREAM_MAGIC,
        L4Protocol::Udp => ACTIVATE_STREAM_UDP,
    };
    pkt.push(magic);
    if let Some(t) = target {
        let s = t.to_string();
        let b = s.as_bytes();
        let len = b.len().min(u16::MAX as usize) as u16;
        pkt.extend_from_slice(&len.to_be_bytes());
        pkt.extend_from_slice(&b[..len as usize]);
    } else {
        pkt.extend_from_slice(&0u16.to_be_bytes());
    }
    pkt
}

/// Decodes an activation packet from an idle stream.
///
/// Returns `Ok(Some(header))` on valid activation, `Ok(None)` if stream is closed (EOF)
/// or cancelled/timed out, or `Err(TunnelError)` on invalid framing.
pub async fn decode_activation<S: AsyncReadExt + Unpin + Send>(
    stream: &mut S,
    cancel_token: &CancellationToken,
    timeout_duration: Duration,
) -> Result<Option<ActivationHeader>, TunnelError> {
    let mut magic = [0u8; 1];
    let read_magic = stream
        .read_exact(&mut magic)
        .cancel_with(cancel_token)
        .await;
    let Some(res) = read_magic else {
        return Ok(None);
    };
    if res.is_err() {
        return Ok(None);
    }

    let byte = magic[0];
    if byte == ACTIVATE_STREAM_PROMOTE {
        return Ok(Some(ActivationHeader {
            protocol: L4Protocol::Tcp,
            target: None,
            is_promotion: true,
        }));
    }

    let protocol = if byte == ACTIVATE_STREAM_MAGIC {
        L4Protocol::Tcp
    } else if byte == ACTIVATE_STREAM_UDP {
        L4Protocol::Udp
    } else {
        return Err(TunnelError::ControlProtocol(format!(
            "Invalid activation magic byte: {:02x}",
            byte
        )));
    };

    // Read 2-byte big-endian target length
    let mut len_bytes = [0u8; 2];
    let read_len = match stream
        .read_exact(&mut len_bytes)
        .timeout_or_cancel(timeout_duration, cancel_token)
        .await
    {
        TimeoutOrCancel::Ok(r) => r,
        TimeoutOrCancel::TimedOut => {
            return Err(TunnelError::ControlProtocol(
                "Timed out reading activation target length".into(),
            ));
        }
        TimeoutOrCancel::Cancelled => return Ok(None),
    };

    if read_len.is_err() {
        return Ok(None);
    }

    let target_len = u16::from_be_bytes(len_bytes) as usize;
    if target_len >= MAX_ACTIVATION_TARGET_LEN {
        return Err(TunnelError::ControlProtocol(format!(
            "Activation target length too large: {}",
            target_len
        )));
    }

    let target = if target_len > 0 {
        let mut target_bytes = vec![0u8; target_len];
        let read_target = match stream
            .read_exact(&mut target_bytes)
            .timeout_or_cancel(timeout_duration, cancel_token)
            .await
        {
            TimeoutOrCancel::Ok(r) => r,
            TimeoutOrCancel::TimedOut => {
                return Err(TunnelError::ControlProtocol(
                    "Timed out reading activation target payload".into(),
                ));
            }
            TimeoutOrCancel::Cancelled => return Ok(None),
        };

        if read_target.is_err() {
            return Ok(None);
        }

        let target_str = std::str::from_utf8(&target_bytes).map_err(|e| {
            TunnelError::ControlProtocol(format!("Invalid UTF-8 in activation target: {}", e))
        })?;
        Some(parse_target_addr(target_str)?)
    } else {
        None
    };

    Ok(Some(ActivationHeader {
        protocol,
        target,
        is_promotion: false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_encode_decode_activation_tcp_with_target() {
        let target = parse_target_addr("127.0.0.1:8080").unwrap();
        let encoded = encode_activation_with_proto(Some(&target), L4Protocol::Tcp);
        let mut cursor = Cursor::new(encoded);
        let cancel = CancellationToken::new();

        let decoded = decode_activation(&mut cursor, &cancel, Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(decoded.protocol, L4Protocol::Tcp);
        assert_eq!(decoded.target, Some(target));
        assert!(!decoded.is_promotion);
    }

    #[tokio::test]
    async fn test_encode_decode_activation_udp_no_target() {
        let encoded = encode_activation_with_proto(None, L4Protocol::Udp);
        let mut cursor = Cursor::new(encoded);
        let cancel = CancellationToken::new();

        let decoded = decode_activation(&mut cursor, &cancel, Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(decoded.protocol, L4Protocol::Udp);
        assert_eq!(decoded.target, None);
        assert!(!decoded.is_promotion);
    }

    #[tokio::test]
    async fn test_decode_activation_promotion() {
        let mut cursor = Cursor::new(vec![ACTIVATE_STREAM_PROMOTE]);
        let cancel = CancellationToken::new();

        let decoded = decode_activation(&mut cursor, &cancel, Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();

        assert!(decoded.is_promotion);
    }
}
