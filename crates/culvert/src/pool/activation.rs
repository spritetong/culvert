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

async fn read_activation_field<S: AsyncReadExt + Unpin + Send>(
    stream: &mut S,
    buf: &mut [u8],
    cancel_token: &CancellationToken,
    timeout: Option<Duration>,
) -> TimeoutOrCancel<std::io::Result<usize>> {
    match timeout {
        Some(duration) => {
            stream
                .read_exact(buf)
                .timeout_or_cancel(duration, cancel_token)
                .await
        }
        None => match stream.read_exact(buf).cancel_with(cancel_token).await {
            Some(result) => TimeoutOrCancel::Ok(result),
            None => TimeoutOrCancel::Cancelled,
        },
    }
}

/// Decodes an activation packet from an idle stream with a bounded read timeout.
///
/// Returns `Ok(Some(header))` on valid activation, `Ok(None)` if stream is closed (EOF)
/// or cancelled, `Err(TunnelError)` on timeout or invalid framing.
pub async fn decode_activation<S: AsyncReadExt + Unpin + Send>(
    stream: &mut S,
    cancel_token: &CancellationToken,
    timeout_duration: Duration,
) -> Result<Option<ActivationHeader>, TunnelError> {
    decode_activation_inner(stream, cancel_token, Some(timeout_duration)).await
}

/// Decodes an activation packet without a local timeout: the read ends on
/// activation, peer EOF, or cancellation. Used where the peer owns the stream's
/// idle lifetime, so a local deadline would close a healthy waiting stream.
pub async fn decode_activation_until_cancel<S: AsyncReadExt + Unpin + Send>(
    stream: &mut S,
    cancel_token: &CancellationToken,
) -> Result<Option<ActivationHeader>, TunnelError> {
    decode_activation_inner(stream, cancel_token, None).await
}

async fn read_activation_bytes<S: AsyncReadExt + Unpin + Send>(
    stream: &mut S,
    buf: &mut [u8],
    cancel_token: &CancellationToken,
    timeout: Option<Duration>,
    field_name: &str,
    eof_is_clean: bool,
) -> Result<bool, TunnelError> {
    let io_res = match read_activation_field(stream, buf, cancel_token, timeout).await {
        TimeoutOrCancel::Ok(r) => r,
        TimeoutOrCancel::TimedOut => {
            return Err(TunnelError::ControlProtocol(format!(
                "Timed out reading activation {field_name}"
            )));
        }
        TimeoutOrCancel::Cancelled => return Ok(false),
    };

    match io_res {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            if eof_is_clean {
                Ok(false)
            } else {
                Err(TunnelError::ControlProtocol(format!(
                    "Truncated activation header while reading {field_name}: {e}"
                )))
            }
        }
        Err(e) => Err(TunnelError::Io(e)),
    }
}

async fn decode_activation_inner<S: AsyncReadExt + Unpin + Send>(
    stream: &mut S,
    cancel_token: &CancellationToken,
    timeout_duration: Option<Duration>,
) -> Result<Option<ActivationHeader>, TunnelError> {
    let mut magic = [0u8; 1];
    if !read_activation_bytes(
        &mut *stream,
        &mut magic,
        cancel_token,
        timeout_duration,
        "magic",
        true,
    )
    .await?
    {
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
    if !read_activation_bytes(
        &mut *stream,
        &mut len_bytes,
        cancel_token,
        timeout_duration,
        "target length",
        false,
    )
    .await?
    {
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
        if !read_activation_bytes(
            &mut *stream,
            &mut target_bytes,
            cancel_token,
            timeout_duration,
            "target payload",
            false,
        )
        .await?
        {
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
    async fn test_decode_activation_until_cancel_has_no_local_deadline() {
        let (_client, mut server) = tokio::io::duplex(64);
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let handle =
            tokio::spawn(
                async move { decode_activation_until_cancel(&mut server, &task_cancel).await },
            );

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !handle.is_finished(),
            "cancellation is the only local bound"
        );
        cancel.cancel();
        let decoded = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(decoded, Ok(None)));
    }

    #[tokio::test]
    async fn test_decode_activation_magic_byte_is_bounded() {
        let (mut reader, _writer) = tokio::io::duplex(64);
        let cancel = CancellationToken::new();
        let started = std::time::Instant::now();
        let decoded = decode_activation(&mut reader, &cancel, Duration::from_millis(20)).await;
        assert!(decoded.is_err());
        assert!(started.elapsed() >= Duration::from_millis(20));
    }

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

    #[tokio::test]
    async fn test_decode_activation_truncation_by_offset_and_io_errors() {
        use std::io;
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::io::{AsyncRead, ReadBuf};

        struct PrefixThenErrorReader {
            prefix: Vec<u8>,
            pos: usize,
            err_kind: io::ErrorKind,
        }

        impl AsyncRead for PrefixThenErrorReader {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                if self.pos < self.prefix.len() {
                    let remaining = self.prefix.len() - self.pos;
                    let n = remaining.min(buf.remaining());
                    buf.put_slice(&self.prefix[self.pos..self.pos + n]);
                    self.pos += n;
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Ready(Err(io::Error::new(
                        self.err_kind,
                        "simulated transport read error",
                    )))
                }
            }
        }

        let target = parse_target_addr("127.0.0.1:8080").unwrap();
        let encoded = encode_activation_with_proto(Some(&target), L4Protocol::Tcp);
        let cancel = CancellationToken::new();

        // 0-byte clean EOF before magic -> Ok(None)
        let mut empty_cursor = Cursor::new(Vec::<u8>::new());
        let zero_res = decode_activation(&mut empty_cursor, &cancel, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(zero_res.is_none());

        // Truncated at any non-zero byte offset (magic only, partial len, full len, partial target)
        // must fail as a protocol framing error rather than returning Ok(None).
        for offset in 1..encoded.len() {
            let mut truncated = Cursor::new(encoded[..offset].to_vec());
            let err = decode_activation(&mut truncated, &cancel, Duration::from_secs(1))
                .await
                .expect_err("truncated activation frame must return error");
            assert!(
                matches!(err, TunnelError::ControlProtocol(_)),
                "offset {offset} should produce ControlProtocol error, got {err:?}"
            );
        }

        // Underlying non-EOF I/O errors must be preserved at magic, length, and target stages.
        for prefix_len in [0usize, 1, 2, 3, 5] {
            let mut reader = PrefixThenErrorReader {
                prefix: encoded[..prefix_len].to_vec(),
                pos: 0,
                err_kind: io::ErrorKind::ConnectionReset,
            };
            let err = decode_activation(&mut reader, &cancel, Duration::from_secs(1))
                .await
                .expect_err("ConnectionReset must propagate");
            match err {
                TunnelError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::ConnectionReset),
                other => panic!("prefix_len {prefix_len} expected TunnelError::Io, got {other:?}"),
            }
        }
    }
}
