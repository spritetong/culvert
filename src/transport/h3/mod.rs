// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! HTTP/3 (QUIC) transport driver implementation utilizing `quinn` and `h3`.
//! All comments and docstrings must be in English.

pub mod connector;
pub mod listener;
pub mod stream;

pub use connector::H3Connector;
pub use listener::H3Listener;
pub use stream::{H3TunnelStream, QuinnTunnelStream};

use crate::config::model::TunnelMode;
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::transport::{TransportConnector, TransportDriver, TransportListener};

/// HTTP/3 transport driver handling `h3`, `https`, and `quic` schemes.
pub struct H3TransportDriver;

impl TransportDriver for H3TransportDriver {
    fn schemes(&self) -> &[&'static str] {
        &["h3", "https", "quic"]
    }

    fn build_connector(
        &self,
        endpoint: &EndpointContext,
        auth_token: &str,
        mode: TunnelMode,
    ) -> Result<Box<dyn TransportConnector>, TunnelError> {
        let connector = H3Connector::new(endpoint, auth_token, mode)?;
        Ok(Box::new(connector))
    }

    fn build_listener(
        &self,
        listen_addr: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        tls: Option<&crate::config::model::TlsConfig>,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        let mut alpn = tls
            .and_then(|t| t.alpn.as_ref())
            .map(|list| {
                list.iter()
                    .map(|s| s.as_bytes().to_vec())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !alpn.iter().any(|p| p == b"h3") {
            alpn.push(b"h3".to_vec());
        }
        if !alpn.iter().any(|p| p == b"micro-tunnel-quic") {
            alpn.push(b"micro-tunnel-quic".to_vec());
        }
        let cert_str = tls
            .and_then(|t| t.certificate.as_ref())
            .and_then(|p| p.to_str());
        let key_str = tls.and_then(|t| t.key.as_ref()).and_then(|p| p.to_str());
        let (min_v, max_v) = match tls {
            Some(t) => (t.min_version.as_deref(), t.max_version.as_deref()),
            None => (None, None),
        };

        let server_rustls = crate::dialer::tls::TlsConfigBuilder::build_server_config(
            cert_str,
            key_str,
            Some(alpn),
            min_v,
            max_v,
        )?;

        let listener = H3Listener::bind(listen_addr, auth_token, path, server_rustls)?;
        Ok(Box::new(listener))
    }
}
