//! Unix Domain Socket transport for Caddy upstream IPC.
//! All comments and docstrings must be in English.

use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::dialer::endpoint::EndpointContext;
use crate::transport::{
    StreamMetadata, TargetAddr, TransportConnector, TransportDriver, TransportListener,
};

/// UDS transport driver.
pub struct UdsTransportDriver;

impl TransportDriver for UdsTransportDriver {
    fn schemes(&self) -> &[&'static str] {
        &["uds", "unix"]
    }

    fn build_connector(
        &self,
        endpoint: &EndpointContext,
        auth_token: &str,
        _mode: crate::config::model::TunnelMode,
    ) -> Result<Box<dyn TransportConnector>, TunnelError> {
        Ok(Box::new(UdsConnector {
            endpoint: endpoint.clone(),
            auth_token: auth_token.to_string(),
        }))
    }

    fn build_listener(
        &self,
        socket_path: &str,
        auth_token: Option<&str>,
        _path: Option<&str>,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        Ok(Box::new(UdsListener {
            socket_path: socket_path.to_string(),
            auth_token: auth_token.unwrap_or("").to_string(),
        }))
    }
}

/// UDS connector.
pub struct UdsConnector {
    pub endpoint: EndpointContext,
    pub auth_token: String,
}

impl TransportConnector for UdsConnector {
    fn connect<'a>(
        &'a self,
        _target: &'a TargetAddr,
        _session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        Box::pin(async move {
            Err(TunnelError::Internal(
                "UdsConnector not supported on this platform".to_string(),
            ))
        })
    }

    fn protocol_scheme(&self) -> &'static str {
        "uds"
    }
}

/// UDS listener.
pub struct UdsListener {
    pub socket_path: String,
    pub auth_token: String,
}

impl TransportListener for UdsListener {
    fn accept<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>> {
        Box::pin(async move {
            Err(TunnelError::Internal(
                "UdsListener not supported on this platform".to_string(),
            ))
        })
    }
}
