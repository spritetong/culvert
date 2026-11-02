//! HTTP/3 (QUIC) transport driver implementation utilizing `quinn` and `h3`.
//! All comments and docstrings must be in English.

use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::dialer::endpoint::EndpointContext;
use crate::transport::{StreamMetadata, TargetAddr, TransportConnector, TransportDriver, TransportListener};

/// HTTP/3 transport driver.
pub struct H3TransportDriver;

impl TransportDriver for H3TransportDriver {
    fn scheme(&self) -> &'static str {
        "h3"
    }

    fn build_connector(
        &self,
        endpoint: &EndpointContext,
        auth_token: &str,
    ) -> Result<Box<dyn TransportConnector>, TunnelError> {
        Ok(Box::new(H3Connector {
            endpoint: endpoint.clone(),
            auth_token: auth_token.to_string(),
        }))
    }

    fn build_listener(
        &self,
        listen_addr: &str,
        auth_token: &str,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        Ok(Box::new(H3Listener {
            listen_addr: listen_addr.to_string(),
            auth_token: auth_token.to_string(),
        }))
    }
}

/// HTTP/3 connector.
pub struct H3Connector {
    pub endpoint: EndpointContext,
    pub auth_token: String,
}

impl TransportConnector for H3Connector {
    fn connect<'a>(
        &'a self,
        _target: &'a TargetAddr,
        _session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        Box::pin(async move {
            Err(TunnelError::Internal("H3Connector::connect not yet implemented".to_string()))
        })
    }

    fn protocol_scheme(&self) -> &'static str {
        "h3"
    }
}

/// HTTP/3 listener.
pub struct H3Listener {
    pub listen_addr: String,
    pub auth_token: String,
}

impl TransportListener for H3Listener {
    fn accept<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>> {
        Box::pin(async move {
            Err(TunnelError::Internal("H3Listener::accept not yet implemented".to_string()))
        })
    }
}
