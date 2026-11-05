// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Transport layer traits and driver abstractions.
//! All comments and docstrings must be in English.

use crate::config::model::TunnelMode;
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::dialer::endpoint::EndpointContext;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

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
            TargetAddr::Path(path) => write!(f, "{}", path.display()),
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
}
