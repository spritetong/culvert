//! Reverse tunnel control channel messages and stream definitions.
//! All comments and docstrings must be in English.

use bytes::Bytes;
use futures_core::Stream;
use futures_sink::Sink;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::core::error::TunnelError;

/// Signaling protocol message exchanged over the reverse tunnel control channel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControlMessage {
    /// Client requests the server to bind a listening port on the remote host.
    RegisterReverse {
        listen_addr: String,
        target_addr: String,
    },
    /// Server acknowledges the port binding request.
    RegisterAck {
        listen_addr: String,
        success: bool,
        error_msg: Option<String>,
    },
    /// Server notifies client that a new connection has arrived on the listening port.
    NewConnection {
        session_id: String,
        client_addr: String,
    },
    /// Heartbeat ping to keep NAT mappings alive.
    Ping,
    /// Heartbeat pong reply.
    Pong,
}

/// Dynamic trait object combining Stream and Sink for control signaling without `async_trait`.
pub trait ControlStream:
    Stream<Item = Result<ControlMessage, TunnelError>>
    + Sink<ControlMessage, Error = TunnelError>
    + Send
    + Unpin
    + 'static
{
}

// Blanket implementation
impl<T> ControlStream for T where
    T: Stream<Item = Result<ControlMessage, TunnelError>>
        + Sink<ControlMessage, Error = TunnelError>
        + Send
        + Unpin
        + 'static
{
}

/// Adapts any `AsyncRead + AsyncWrite` stream into a strongly-typed `ControlStream`.
pub struct FramedControlStream<S> {
    inner: Framed<S, LengthDelimitedCodec>,
}

impl<S> FramedControlStream<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    /// Creates a new `FramedControlStream` wrapping the provided stream.
    pub fn new(stream: S) -> Self {
        Self {
            inner: LengthDelimitedCodec::builder().new_framed(stream),
        }
    }
}

impl<S> Stream for FramedControlStream<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Item = Result<ControlMessage, TunnelError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => match rmp_serde::from_slice::<ControlMessage>(&bytes) {
                Ok(msg) => Poll::Ready(Some(Ok(msg))),
                Err(e) => Poll::Ready(Some(Err(TunnelError::ControlProtocol(e.to_string())))),
            },
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(TunnelError::Io(e)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> Sink<ControlMessage> for FramedControlStream<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Error = TunnelError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::<Bytes>::poll_ready(Pin::new(&mut self.inner), cx).map_err(TunnelError::Io)
    }

    fn start_send(mut self: Pin<&mut Self>, item: ControlMessage) -> Result<(), Self::Error> {
        let msgpack_vec = rmp_serde::to_vec_named(&item)
            .map_err(|e| TunnelError::ControlProtocol(e.to_string()))?;
        Sink::<Bytes>::start_send(Pin::new(&mut self.inner), Bytes::from(msgpack_vec))
            .map_err(TunnelError::Io)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::<Bytes>::poll_flush(Pin::new(&mut self.inner), cx).map_err(TunnelError::Io)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::<Bytes>::poll_close(Pin::new(&mut self.inner), cx).map_err(TunnelError::Io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_framed_control_stream_roundtrip() {
        let (raw_a, raw_b) = duplex(16384);
        let mut ctrl_a = FramedControlStream::new(raw_a);
        let mut ctrl_b = FramedControlStream::new(raw_b);

        let msg = ControlMessage::RegisterReverse {
            listen_addr: "0.0.0.0:2222".to_string(),
            target_addr: "127.0.0.1:22".to_string(),
        };

        ctrl_a.send(msg.clone()).await.unwrap();

        let received = ctrl_b.next().await.unwrap().unwrap();
        assert_eq!(received, msg);

        let ack = ControlMessage::RegisterAck {
            listen_addr: "0.0.0.0:2222".to_string(),
            success: true,
            error_msg: None,
        };

        ctrl_b.send(ack.clone()).await.unwrap();
        let received_ack = ctrl_a.next().await.unwrap().unwrap();
        assert_eq!(received_ack, ack);
    }
}
