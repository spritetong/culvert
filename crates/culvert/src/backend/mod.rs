// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Backend: the uniform I/O interface over relay channels.
//! `transport/` holds driver implementations and re-exports these traits.
//! All comments and docstrings must be in English.

use crate::config::model::{TlsConfig, TunnelMode};
use crate::dialer::endpoint::EndpointContext;
use crate::io::addr::{L4Protocol, TargetAddr};
use crate::io::channel::IoChannel;
use crate::io::error::TunnelError;
use crate::io::stream::BoxFuture;
use reusable_future::ReusableFutureRef;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

pub mod registry;

/// Metadata extracted during inbound handshake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetadata {
    pub mode: TunnelMode,
    pub session_id: Option<String>,
    pub target: Option<TargetAddr>,
    #[serde(default)]
    pub pool_idle: bool,
    #[serde(default)]
    pub protocol: L4Protocol,
    #[serde(default)]
    pub tunnel_id: Option<String>,
}

/// Result type produced when accepting a channel from a `TransportListener`.
pub type AcceptOutput = Result<(IoChannel, StreamMetadata), TunnelError>;

/// Dynamic trait object for establishing outbound relay channels.
pub trait TransportConnector: Send + Sync {
    /// Opens a new channel through the underlying transport to the peer.
    fn connect<'a>(
        &'a self,
        target: &'a TargetAddr,
        session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>>;

    /// Opens a pre-warmed pooled stream that remains idle on the server until activated.
    ///
    /// Required: a default that forwards to `connect` would silently omit the
    /// `x-tunnel-pool: idle` marker and produce an ordinary data stream.
    fn connect_pooled<'a>(
        &'a self,
        target: &'a TargetAddr,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>>;

    /// Opens a new channel through the underlying transport carrying the explicit L4 protocol hint.
    fn connect_with_protocol<'a>(
        &'a self,
        target: &'a TargetAddr,
        session_id: Option<&'a str>,
        protocol: L4Protocol,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        let _ = protocol;
        self.connect(target, session_id)
    }

    /// Opens a pre-warmed pooled stream tagged with the explicit L4 protocol hint.
    fn connect_pooled_with_protocol<'a>(
        &'a self,
        target: &'a TargetAddr,
        protocol: L4Protocol,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        let _ = protocol;
        self.connect_pooled(target)
    }

    /// Opens a pre-warmed pooled stream tagged with an optional Tunnel UUID.
    fn connect_pooled_with_tunnel<'a>(
        &'a self,
        target: &'a TargetAddr,
        protocol: L4Protocol,
        tunnel_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        let _ = tunnel_id;
        self.connect_pooled_with_protocol(target, protocol)
    }

    /// Hints to the connector that underlying streams have failed or become dead,
    /// allowing multiplexed transports (such as HTTP/3) to invalidate broken cached sessions.
    fn invalidate_broken_sessions(&self) {}
}

/// Dynamic trait object for accepting inbound relay channels from clients.
pub trait TransportListener: Send + Sync {
    /// Asynchronously awaits and accepts the next incoming channel into `slot`.
    ///
    /// Loop callers keep the slot outside `tokio::select!` and poll `&mut slot`
    /// in a branch, so an in-flight accept survives other branches winning and
    /// no allocation is needed per loop turn. `is_empty()` uses a null-pointer
    /// sentinel, so zero-sized futures are safe.
    fn accept(&mut self, slot: &mut ReusableFutureRef<'static, AcceptOutput>);

    /// Explicit termination boundary for the listener's own background tasks.
    ///
    /// Implementations cancel their accept loop and **await every owned task
    /// handle** before returning. `Drop` is only a safety net for abnormal
    /// paths and must never abort or leak a live handle in normal shutdown.
    /// The owner must call this before dropping the listener.
    fn shutdown<'a>(&'a mut self) -> BoxFuture<'a, ()>;
}

/// Single-shot accept helper. It is a free function, not a trait default:
/// installing a fresh slot inside a `select!` branch would drop the in-flight
/// accept on every losing iteration.
pub async fn accept_one<L>(listener: &mut L) -> AcceptOutput
where
    L: TransportListener + ?Sized,
{
    let mut slot: ReusableFutureRef<'static, AcceptOutput> = ReusableFutureRef::default();
    listener.accept(&mut slot);
    slot.await
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

    /// Builds a listener wired to the root shutdown token.
    ///
    /// Ingress listeners must not own private cancellation tokens: those are
    /// only cancelled in `Drop`, which happens after the connection tracker has
    /// already drained.
    fn build_listener(
        &self,
        listen_addr: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        tls: Option<&TlsConfig>,
        cancel: CancellationToken,
    ) -> Result<Box<dyn TransportListener>, TunnelError>;
}
