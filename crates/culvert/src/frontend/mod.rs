// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Frontend: the port-mapping service exposed to the outside world.
//! Core only sees `Frontend` and the neutral `IoChannel`.
//! All comments and docstrings must be in English.

use crate::io::addr::{L4Protocol, MappingSpec, PeerAddr};
use crate::io::channel::IoChannel;
use crate::io::error::TunnelError;
use crate::io::stream::BoxFuture;
use reusable_future::ReusableFutureRef;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub mod tcp;
pub mod udp;

/// One accepted frontend connection or datagram flow.
pub struct FrontendAccept {
    pub channel: IoChannel,
    pub peer: PeerAddr,
}

pub type FrontendOutput = Result<FrontendAccept, TunnelError>;

/// Core-facing frontend abstraction. Implementations own their listener socket
/// and decide what I/O shape they yield.
pub trait Frontend: Send {
    /// Installs the next accept into `slot`. Callers keep `slot` outside the
    /// `select!` loop so an in-flight accept survives other branches winning.
    fn accept(&mut self, slot: &mut ReusableFutureRef<'static, FrontendOutput>);

    fn shutdown<'a>(&'a mut self) -> BoxFuture<'a, ()>;
}

/// Builds a frontend for one L4 protocol. Registering a new protocol here is
/// the only frontend-side step needed for a new mapping capability.
pub trait FrontendFactory: Send + Sync + 'static {
    fn protocol(&self) -> L4Protocol;
    fn bind(&self, spec: MappingSpec)
    -> BoxFuture<'static, Result<Box<dyn Frontend>, TunnelError>>;
}

pub struct FrontendRegistry {
    factories: RwLock<HashMap<L4Protocol, Arc<dyn FrontendFactory>>>,
}

impl FrontendRegistry {
    pub fn new() -> Self {
        Self {
            factories: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, factory: Arc<dyn FrontendFactory>) {
        self.factories
            .write()
            .expect("frontend registry lock poisoned")
            .insert(factory.protocol(), factory);
    }

    pub fn lookup(&self, protocol: L4Protocol) -> Option<Arc<dyn FrontendFactory>> {
        self.factories
            .read()
            .expect("frontend registry lock poisoned")
            .get(&protocol)
            .cloned()
    }
}

impl Default for FrontendRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns the process-wide frontend registry, seeded with TCP/UDS and UDP frontends.
pub fn frontend_registry() -> &'static FrontendRegistry {
    static REGISTRY: OnceLock<FrontendRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let registry = FrontendRegistry::new();
        registry.register(Arc::new(tcp::TcpFrontendFactory));
        registry.register(Arc::new(udp::UdpFrontendFactory));
        registry
    })
}

/// Binds a mapping spec through the registry.
pub async fn bind(spec: &MappingSpec) -> Result<Box<dyn Frontend>, TunnelError> {
    let factory = frontend_registry().lookup(spec.protocol).ok_or_else(|| {
        TunnelError::UnsupportedChannel(format!(
            "no frontend registered for {:?} port mapping",
            spec.protocol
        ))
    })?;
    factory.bind(spec.clone()).await
}

const FRONTEND_ACCEPT_BACKOFF_BASE: Duration = Duration::from_millis(10);
const FRONTEND_ACCEPT_BACKOFF_MAX: Duration = Duration::from_millis(200);

/// Bounded exponential backoff for transient listener/UDP error recovery.
pub(crate) struct AcceptBackoff {
    current: Duration,
    base: Duration,
    max: Duration,
}

impl Default for AcceptBackoff {
    fn default() -> Self {
        Self::new(FRONTEND_ACCEPT_BACKOFF_BASE, FRONTEND_ACCEPT_BACKOFF_MAX)
    }
}

impl AcceptBackoff {
    pub(crate) fn new(base: Duration, max: Duration) -> Self {
        Self {
            current: base,
            base,
            max,
        }
    }

    #[inline]
    pub(crate) fn reset(&mut self) {
        self.current = self.base;
    }

    pub(crate) fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current.saturating_mul(2)).min(self.max);
        delay
    }

    /// Sleeps for `delay` or returns `false` immediately when `cancel` triggers.
    pub(crate) async fn sleep_cancel(&self, delay: Duration, cancel: &CancellationToken) -> bool {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => false,
            _ = tokio::time::sleep(delay) => true,
        }
    }
}

/// Returns `true` when an OS I/O error indicates a permanent socket/config failure
/// rather than a transient resource exhaustion (`EMFILE`, `ENOBUFS`, etc.) or peer reset.
pub(crate) fn is_unrecoverable_io_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::InvalidInput
            | io::ErrorKind::PermissionDenied
            | io::ErrorKind::AddrInUse
            | io::ErrorKind::AddrNotAvailable
            | io::ErrorKind::NotConnected
            | io::ErrorKind::Unsupported
            | io::ErrorKind::NotFound
    )
}

/// Returns `true` when a frontend accept error is permanent and should abort the accept loop.
pub(crate) fn is_unrecoverable_accept_error(err: &TunnelError) -> bool {
    match err {
        TunnelError::Io(io_err) => is_unrecoverable_io_error(io_err),
        other => other.is_permanent(),
    }
}

/// Awaits the next accepted frontend connection/flow or shutdown, backing off on
/// recoverable accept errors and failing fast on unrecoverable errors.
pub(crate) async fn next_frontend_accept(
    frontend: &mut dyn Frontend,
    accept_slot: &mut ReusableFutureRef<'static, FrontendOutput>,
    backoff: &mut AcceptBackoff,
    shutdown_token: &CancellationToken,
    label: &str,
) -> Result<Option<FrontendAccept>, TunnelError> {
    loop {
        if accept_slot.is_empty() {
            frontend.accept(accept_slot);
        }

        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => return Ok(None),
            accept_res = &mut *accept_slot => {
                match accept_res {
                    Ok(accepted) => {
                        backoff.reset();
                        return Ok(Some(accepted));
                    }
                    Err(TunnelError::Shutdown) => return Ok(None),
                    Err(err) if shutdown_token.is_cancelled() => {
                        tracing::debug!(error = %err, "{label} accept ended during shutdown");
                        return Ok(None);
                    }
                    Err(err) if is_unrecoverable_accept_error(&err) => {
                        tracing::error!(error = %err, "{label} accept hit unrecoverable error");
                        return Err(err);
                    }
                    Err(err) => {
                        let delay = backoff.next_delay();
                        tracing::warn!(
                            error = %err,
                            backoff = ?delay,
                            "{label} accept failed; backing off before retry"
                        );
                        if !backoff.sleep_cancel(delay, shutdown_token).await {
                            return Ok(None);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_udp_mapping_binds_frontend() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let spec = MappingSpec {
            protocol: L4Protocol::Udp,
            listen: format!("127.0.0.1:{}", port),
            target: "127.0.0.1:9".into(),
        };
        let mut fe = bind(&spec).await.unwrap();
        fe.shutdown().await;
    }

    #[test]
    fn test_registry_seeds_tcp_and_udp() {
        assert!(frontend_registry().lookup(L4Protocol::Tcp).is_some());
        assert!(frontend_registry().lookup(L4Protocol::Udp).is_some());
    }
}
