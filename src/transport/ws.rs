//! WebSocket transport implementation utilizing SIMD-accelerated `fastwebsockets`.
//! All comments and docstrings must be in English.

use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::dialer::endpoint::EndpointContext;
use crate::transport::{StreamMetadata, TargetAddr, TransportConnector, TransportDriver, TransportListener};

/// WebSocket transport driver.
pub struct WsTransportDriver;

impl TransportDriver for WsTransportDriver {
    fn scheme(&self) -> &'static str {
        "wss"
    }

    fn build_connector(
        &self,
        endpoint: &EndpointContext,
        auth_token: &str,
    ) -> Result<Box<dyn TransportConnector>, TunnelError> {
        Ok(Box::new(WsConnector {
            endpoint: endpoint.clone(),
            auth_token: auth_token.to_string(),
        }))
    }

    fn build_listener(
        &self,
        listen_addr: &str,
        auth_token: &str,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        Ok(Box::new(WsListener {
            listen_addr: listen_addr.to_string(),
            auth_token: auth_token.to_string(),
        }))
    }
}

/// WebSocket connector.
pub struct WsConnector {
    pub endpoint: EndpointContext,
    pub auth_token: String,
}

impl TransportConnector for WsConnector {
    fn connect<'a>(
        &'a self,
        _target: &'a TargetAddr,
        _session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        Box::pin(async move {
            // Dial underlying TCP/TLS stream
            let raw_stream = self
                .endpoint
                .dialer
                .dial(&self.endpoint.endpoint.host, self.endpoint.endpoint.port)
                .await?;

            // Handshake using fastwebsockets upgrade will be performed here
            Ok(raw_stream)
        })
    }

    fn protocol_scheme(&self) -> &'static str {
        "wss"
    }
}

/// WebSocket listener.
pub struct WsListener {
    pub listen_addr: String,
    pub auth_token: String,
}

impl TransportListener for WsListener {
    fn accept<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>> {
        Box::pin(async move {
            Err(TunnelError::Internal("WsListener::accept not yet implemented".to_string()))
        })
    }
}
