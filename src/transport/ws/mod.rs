//! WebSocket transport implementation utilizing SIMD-accelerated `fastwebsockets`.
//! All comments and docstrings must be in English.

pub mod connector;
pub mod listener;
pub mod model;
pub mod stream;

pub use connector::WsConnector;
pub use listener::WsListener;
pub use model::WsHandshakeHeaders;
pub use stream::WsTunnelStream;

use crate::config::model::TunnelMode;
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::transport::{TransportConnector, TransportDriver, TransportListener};

/// WebSocket transport driver implementation.
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
        Ok(Box::new(WsConnector::new(
            endpoint.clone(),
            auth_token,
            TunnelMode::Forward,
        )))
    }

    fn build_listener(
        &self,
        listen_addr: &str,
        auth_token: &str,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        Ok(Box::new(WsListener::new(listen_addr, auth_token)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use crate::config::model::{AppConfig, EndpointConfig};
    use crate::transport::TargetAddr;

    #[tokio::test]
    async fn test_ws_connector_listener_e2e() {
        let auth_token = "test_secret_token_456";
        let mut listener = WsListener::new("127.0.0.1:29876", auth_token);

        let app_cfg = AppConfig::default();
        let ep_cfg = EndpointConfig::Simple("http://127.0.0.1:29876/ws".to_string());
        let ep_ctx = EndpointContext::build(&ep_cfg, &app_cfg).unwrap();

        let connector = WsConnector::new(ep_ctx, auth_token, TunnelMode::Forward);
        let target = TargetAddr::Domain("127.0.0.1".to_string(), 80);

        let listener_handle = tokio::spawn(async move {
            let (server_stream, meta) = listener.accept().await.unwrap();
            assert_eq!(meta.mode, TunnelMode::Forward);
            server_stream
        });

        // Yield to allow the listener server to bind and accept
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut client_stream = connector.connect(&target, None).await.unwrap();
        let mut server_stream = listener_handle.await.unwrap();

        let ping_msg = b"ping from ws client";
        client_stream.write_all(ping_msg).await.unwrap();
        client_stream.flush().await.unwrap();

        let mut buf = vec![0u8; ping_msg.len()];
        server_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, ping_msg);

        let pong_msg = b"pong from ws server";
        server_stream.write_all(pong_msg).await.unwrap();
        server_stream.flush().await.unwrap();

        let mut reply = vec![0u8; pong_msg.len()];
        client_stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, pong_msg);
    }
}
