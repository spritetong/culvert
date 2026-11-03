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
        tls_cert: Option<&str>,
        tls_key: Option<&str>,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        let alpn = vec![b"h3".to_vec(), b"micro-tunnel-quic".to_vec()];
        let server_rustls = crate::dialer::tls::TlsConfigBuilder::build_server_config(
            tls_cert,
            tls_key,
            Some(alpn),
        )?;

        let listener = H3Listener::bind(listen_addr, auth_token, path, server_rustls)?;
        Ok(Box::new(listener))
    }
}
