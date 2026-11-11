// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Frontend: the port-mapping service exposed to the outside world.
//! Core only sees `Frontend` and the neutral `IoChannel`.
//! All comments and docstrings must be in English.

use crate::io::addr::{L4Protocol, MappingSpec};
use crate::io::channel::IoChannel;
use crate::io::error::TunnelError;
use crate::io::stream::BoxFuture;
use reusable_future::ReusableFutureRef;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

pub mod tcp;

/// One accepted frontend connection or datagram flow.
pub struct FrontendAccept {
    pub channel: IoChannel,
    pub peer: String,
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

/// Returns the process-wide frontend registry, seeded with TCP/UDS only.
pub fn frontend_registry() -> &'static FrontendRegistry {
    static REGISTRY: OnceLock<FrontendRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let registry = FrontendRegistry::new();
        registry.register(Arc::new(tcp::TcpFrontendFactory));
        registry
    })
}

/// Binds a mapping spec through the registry. An unregistered protocol fails
/// before any socket is created; this is where UDP port mapping will plug in.
pub async fn bind(spec: &MappingSpec) -> Result<Box<dyn Frontend>, TunnelError> {
    let factory = frontend_registry().lookup(spec.protocol).ok_or_else(|| {
        TunnelError::UnsupportedChannel(format!(
            "no frontend registered for {:?} port mapping",
            spec.protocol
        ))
    })?;
    factory.bind(spec.clone()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_udp_mapping_has_no_frontend() {
        let spec = MappingSpec {
            protocol: L4Protocol::Udp,
            listen: "127.0.0.1:0".into(),
            target: "127.0.0.1:9".into(),
        };
        assert!(matches!(
            bind(&spec).await,
            Err(TunnelError::UnsupportedChannel(_))
        ));
    }

    #[test]
    fn test_registry_seeds_only_tcp() {
        assert!(frontend_registry().lookup(L4Protocol::Tcp).is_some());
        assert!(frontend_registry().lookup(L4Protocol::Udp).is_none());
    }
}
