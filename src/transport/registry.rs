// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Transport driver registry for dynamic pluggable protocol dispatch.
//! All comments and docstrings must be in English.

use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::transport::{TransportConnector, TransportDriver, TransportListener};

/// Global transport registry managing driver instances.
#[derive(Clone)]
pub struct TransportRegistry {
    drivers: Arc<HashMap<&'static str, Arc<dyn TransportDriver>>>,
}

impl Default for TransportRegistry {
    /// Creates a `TransportRegistry` pre-populated with standard transport drivers (WS, H3, UDS).
    fn default() -> Self {
        let mut reg = Self::new();
        reg.register(crate::transport::ws::WsTransportDriver);
        reg.register(crate::transport::h3::H3TransportDriver);
        reg.register(crate::transport::uds::UdsTransportDriver);
        reg
    }
}

impl TransportRegistry {
    /// Creates a new empty `TransportRegistry`.
    pub fn new() -> Self {
        Self {
            drivers: Arc::new(HashMap::new()),
        }
    }

    /// Registers a new transport driver into the registry.
    pub fn register(&mut self, driver: impl TransportDriver) {
        let drivers = Arc::make_mut(&mut self.drivers);
        let driver_arc: Arc<dyn TransportDriver> = Arc::new(driver);
        for &scheme in driver_arc.schemes() {
            drivers.insert(scheme, Arc::clone(&driver_arc));
        }
    }

    /// Looks up a driver by scheme name.
    pub fn get_driver(&self, scheme: &str) -> Option<Arc<dyn TransportDriver>> {
        self.drivers.get(scheme).cloned()
    }

    /// Creates a dynamic `TransportConnector` based on the endpoint scheme.
    pub fn create_connector(
        &self,
        endpoint: &EndpointContext,
        auth_token: Option<&str>,
        mode: crate::config::model::TunnelMode,
    ) -> Result<Box<dyn TransportConnector>, TunnelError> {
        let scheme = endpoint.endpoint.scheme.as_str();
        let driver = self
            .get_driver(scheme)
            .ok_or_else(|| TunnelError::UnsupportedScheme(scheme.to_string()))?;

        driver.build_connector(endpoint, auth_token.unwrap_or(""), mode)
    }

    /// Creates a dynamic `TransportListener` for the specified scheme.
    pub fn create_listener(
        &self,
        scheme: &str,
        listen_addr: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        tls: Option<&crate::config::model::TlsConfig>,
        cancel: CancellationToken,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        let driver = self
            .get_driver(scheme)
            .ok_or_else(|| TunnelError::UnsupportedScheme(scheme.to_string()))?;

        // Format a full URL for the driver if scheme is provided
        let full_addr = if listen_addr.contains("://") {
            listen_addr.to_string()
        } else {
            format!("{}://{}", scheme, listen_addr)
        };

        driver.build_listener_with_cancel(&full_addr, auth_token, path, tls, cancel)
    }
}
