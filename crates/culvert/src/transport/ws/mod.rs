// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

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
use crate::consts::{SCHEME_WS, SCHEME_WSS};
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::transport::{TransportConnector, TransportDriver, TransportListener};
use tokio_util::sync::CancellationToken;

/// WebSocket transport driver implementation.
pub struct WsTransportDriver;

impl TransportDriver for WsTransportDriver {
    fn schemes(&self) -> &[&'static str] {
        &[SCHEME_WS, SCHEME_WSS]
    }

    fn build_connector(
        &self,
        endpoint: &EndpointContext,
        auth_token: &str,
        mode: TunnelMode,
    ) -> Result<Box<dyn TransportConnector>, TunnelError> {
        Ok(Box::new(WsConnector::new(
            endpoint.clone(),
            auth_token,
            mode,
        )?))
    }

    fn build_listener(
        &self,
        listen_addr: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        tls: Option<&crate::config::model::TlsConfig>,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        self.build_listener_with_cancel(
            listen_addr,
            auth_token,
            path,
            tls,
            CancellationToken::new(),
        )
    }

    fn build_listener_with_cancel(
        &self,
        listen_addr: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        tls: Option<&crate::config::model::TlsConfig>,
        cancel: CancellationToken,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        let is_wss = listen_addr.starts_with("wss://") || listen_addr.starts_with("https://");
        let cert_str = tls
            .and_then(|t| t.certificate.as_ref())
            .and_then(|p| p.to_str());
        let key_str = tls.and_then(|t| t.key.as_ref()).and_then(|p| p.to_str());
        let (min_v, max_v) = match tls {
            Some(t) => (t.min_version.as_deref(), t.max_version.as_deref()),
            None => (None, None),
        };

        let alpn = tls.and_then(|t| t.alpn.as_ref()).and_then(|list| {
            let filtered: Vec<Vec<u8>> = list
                .iter()
                .filter(|s| *s != "h3" && *s != "culvert-quic")
                .map(|s| s.as_bytes().to_vec())
                .collect();
            if filtered.is_empty() {
                None
            } else {
                Some(filtered)
            }
        });

        let tls_acceptor = if is_wss {
            let server_rustls = crate::dialer::tls::TlsConfigBuilder::build_server_config(
                cert_str, key_str, alpn, min_v, max_v,
            )?;
            Some(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(
                server_rustls,
            )))
        } else {
            None
        };

        Ok(Box::new(WsListener::new_with_cancel(
            listen_addr,
            auth_token,
            path,
            tls_acceptor,
            cancel,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{AppConfig, EndpointConfig};
    use crate::transport::TargetAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_ws_connector_listener_e2e() {
        let auth_token = "test_secret_token_456";
        let mut listener = WsListener::new("127.0.0.1:29876", Some(auth_token), Some("/ws"), None);

        let app_cfg = AppConfig::default();
        let ep_cfg = EndpointConfig::Simple("ws://127.0.0.1:29876/ws".to_string());
        let ep_ctx = EndpointContext::build(&ep_cfg, &app_cfg).unwrap();

        let connector = WsConnector::new(ep_ctx, auth_token, TunnelMode::Forward).unwrap();
        let target = TargetAddr::Domain("127.0.0.1".to_string(), 80);

        let listener_handle = tokio::spawn(async move {
            let (server_stream, meta) = listener.accept_one().await.unwrap();
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
