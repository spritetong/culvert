// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Core transfer entry point. Contains no byte-moving code: it dispatches the
//! two I/O channels to the pump registered for their channel-kind pair.
//! All comments and docstrings must be in English.

use crate::io::error::TunnelError;
use crate::io::pump::{PumpCtx, PumpRequest, pump_registry};

/// Hands a frontend/backend channel pair to the matching I/O strategy.
///
/// Adding a new frontend capability (for example UDP port mapping) means
/// registering a new `Pump`; this function does not change.
pub async fn serve(req: PumpRequest, ctx: PumpCtx) -> Result<(u64, u64), TunnelError> {
    let pair = (req.frontend.kind(), req.backend.kind());
    let pump = pump_registry().lookup(pair).ok_or_else(|| {
        TunnelError::UnsupportedChannel(format!("no pump registered for {:?}", pair))
    })?;
    pump.run(req, ctx).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::channel::{ChannelKind, Datagram, DatagramIo, IoChannel};
    use crate::io::pump::{Pump, PumpRegistry};
    use crate::io::stream::BoxFuture;
    use std::io;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
    use tokio_util::sync::CancellationToken;

    struct NoDatagrams;

    impl DatagramIo for NoDatagrams {
        fn poll_recv(
            &mut self,
            _cx: &mut Context<'_>,
            _batch: &mut [Datagram],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_send(
            &mut self,
            _cx: &mut Context<'_>,
            _batch: &[Datagram],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(0))
        }
    }

    struct NoopPump;

    impl Pump for NoopPump {
        fn run<'a>(
            &'a self,
            _req: PumpRequest,
            _ctx: PumpCtx,
        ) -> BoxFuture<'a, Result<(u64, u64), TunnelError>> {
            Box::pin(async { Ok((0, 0)) })
        }
    }

    fn ctx() -> PumpCtx {
        PumpCtx {
            cancel: CancellationToken::new(),
            heartbeat_interval: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(30),
        }
    }

    #[tokio::test]
    async fn test_serve_dispatches_stream_stream_pump() {
        let (mut app_a, a) = duplex(4096);
        let (mut app_b, b) = duplex(4096);

        let handle = tokio::spawn(async move {
            serve(
                PumpRequest::new(IoChannel::from_stream(a), IoChannel::from_stream(b)),
                ctx(),
            )
            .await
        });

        app_a.write_all(b"ping").await.unwrap();
        app_a.shutdown().await.unwrap();
        let mut buf = [0u8; 4];
        app_b.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        app_b.write_all(b"pong").await.unwrap();
        app_b.shutdown().await.unwrap();
        app_a.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");

        let (a_to_b, b_to_a) = handle.await.unwrap().unwrap();
        assert_eq!(a_to_b, 4);
        assert_eq!(b_to_a, 4);
    }

    #[tokio::test]
    async fn test_serve_rejects_unregistered_channel_pair() {
        let (_, a) = duplex(64);
        let datagram = IoChannel::Datagram(Box::new(NoDatagrams));
        let err = serve(PumpRequest::new(IoChannel::from_stream(a), datagram), ctx())
            .await
            .unwrap_err();
        assert!(matches!(err, TunnelError::UnsupportedChannel(_)));
    }

    #[tokio::test]
    async fn test_into_stream_rejects_datagram_channel() {
        let channel = IoChannel::Datagram(Box::new(NoDatagrams));
        assert!(matches!(
            channel.into_stream(),
            Err(TunnelError::UnsupportedChannel(_))
        ));
    }

    #[test]
    fn test_registry_extensibility() {
        let registry = PumpRegistry::new();
        assert!(
            registry
                .lookup((ChannelKind::Stream, ChannelKind::Datagram))
                .is_none()
        );
        registry.register(
            (ChannelKind::Stream, ChannelKind::Datagram),
            Arc::new(NoopPump),
        );
        assert!(
            registry
                .lookup((ChannelKind::Stream, ChannelKind::Datagram))
                .is_some()
        );
        assert!(
            pump_registry()
                .lookup((ChannelKind::Stream, ChannelKind::Stream))
                .is_some()
        );
    }
}
