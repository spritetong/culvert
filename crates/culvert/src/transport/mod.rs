// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Transport layer traits and driver abstractions.
//! All comments and docstrings must be in English.

use crate::config::model::TunnelMode;
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::dialer::endpoint::EndpointContext;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

pub use crate::consts::ACCEPTED_STREAM_QUEUE_SIZE;

pub mod h3;
pub mod registry;
pub mod uds;
pub mod ws;

pub use registry::TransportRegistry;

/// Target address abstraction for remote forwarding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetAddr {
    Ip(SocketAddr),
    Domain(String, u16),
    Path(std::path::PathBuf),
}

impl std::fmt::Display for TargetAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetAddr::Ip(addr) => write!(f, "{}", addr),
            TargetAddr::Domain(host, port) => write!(f, "{}:{}", host, port),
            TargetAddr::Path(path) => write!(f, "unix://{}", path.display()),
        }
    }
}

/// Metadata extracted during inbound handshake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetadata {
    pub mode: TunnelMode,
    pub session_id: Option<String>,
    pub target: Option<TargetAddr>,
    #[serde(default)]
    pub pool_idle: bool,
}

/// Dynamic trait object for establishing outbound tunnel streams.
pub trait TransportConnector: Send + Sync {
    /// Opens a new data stream through the underlying transport to the peer.
    fn connect<'a>(
        &'a self,
        target: &'a TargetAddr,
        session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>>;

    /// Opens a pre-warmed pooled stream that remains idle on the server until activated.
    fn connect_pooled<'a>(
        &'a self,
        target: &'a TargetAddr,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        self.connect(target, None)
    }

    /// Returns the protocol scheme implemented by this connector.
    fn protocol_scheme(&self) -> &'static str;

    /// Hints to the connector that underlying streams have failed or become dead,
    /// allowing multiplexed transports (such as HTTP/3) to invalidate broken cached sessions.
    fn invalidate_broken_sessions(&self) {}
}

/// Dynamic trait object for accepting inbound tunnel streams from clients.
pub trait TransportListener: Send + Sync {
    /// Asynchronously awaits and accepts the next incoming tunnel stream.
    #[allow(clippy::type_complexity)]
    fn accept<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>>;

    /// Explicit termination boundary for the listener's own background tasks.
    ///
    /// Implementations cancel their accept loop and **await every owned task
    /// handle** before returning. `Drop` is only a safety net for abnormal
    /// paths and must never abort or leak a live handle in normal shutdown.
    /// The owner must call this before dropping the listener.
    fn shutdown<'a>(&'a mut self) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}

/// Plugin driver interface for transport factory registration.
pub trait TransportDriver: Send + Sync + 'static {
    /// Returns the URI scheme identifiers handled by this driver (e.g. ["ws", "wss"]).
    fn schemes(&self) -> &[&'static str];

    /// Builds a connector bound to the given endpoint context.
    fn build_connector(
        &self,
        endpoint: &EndpointContext,
        auth_token: &str,
        mode: TunnelMode,
    ) -> Result<Box<dyn TransportConnector>, TunnelError>;

    /// Builds a listener bound to the given listening address.
    fn build_listener(
        &self,
        listen_addr: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        tls: Option<&crate::config::model::TlsConfig>,
    ) -> Result<Box<dyn TransportListener>, TunnelError>;

    /// Builds a listener wired to the root shutdown token.
    ///
    /// Ingress listeners must not own private cancellation tokens: those are
    /// only cancelled in `Drop`, which happens after the connection tracker has
    /// already drained. Drivers that do not override this method keep their
    /// legacy lifecycle for backwards compatibility.
    fn build_listener_with_cancel(
        &self,
        listen_addr: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        tls: Option<&crate::config::model::TlsConfig>,
        _cancel: CancellationToken,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        self.build_listener(listen_addr, auth_token, path, tls)
    }
}
