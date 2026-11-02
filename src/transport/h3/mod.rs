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
        auth_token: &str,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        // Generate an ephemeral self-signed certificate for zero-configuration startup
        let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
        let cert_key = rcgen::generate_simple_self_signed(subject_alt_names)
            .map_err(|e| TunnelError::Tls(format!("Failed to generate self-signed cert: {}", e)))?;

        let cert_der = cert_key.cert.der().to_vec();
        let key_der = cert_key.signing_key.serialize_der();

        let cert_chain = vec![rustls::pki_types::CertificateDer::from(cert_der)];
        let private_key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
        );

        let server_rustls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| TunnelError::Tls(format!("Failed to build server TLS config: {}", e)))?;

        let listener = H3Listener::bind(listen_addr, auth_token, server_rustls)?;
        Ok(Box::new(listener))
    }
}
