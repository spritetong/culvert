//! Reverse tunnel control channel messages and stream definitions.
//! All comments and docstrings must be in English.

use futures_core::Stream;
use futures_sink::Sink;
use serde::{Deserialize, Serialize};

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
    + Sync
    + Unpin
    + 'static
{
}

// Blanket implementation
impl<T> ControlStream for T where
    T: Stream<Item = Result<ControlMessage, TunnelError>>
        + Sink<ControlMessage, Error = TunnelError>
        + Send
        + Sync
        + Unpin
        + 'static
{
}
