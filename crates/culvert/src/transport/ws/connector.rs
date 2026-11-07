// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! WebSocket client connector implementing `TransportConnector`.
//! All comments and docstrings must be in English.

use fastwebsockets::{Role, handshake};
use http::Request;
use http::header::{CONNECTION, HOST, UPGRADE};
use http_body_util::Empty;
use hyper::body::Bytes;
use rustls::pki_types::ServerName;
use std::future::Future;
use std::sync::Arc;

use crate::config::model::TunnelMode;
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, BufferedTlsStream, TunnelStream};
use crate::dialer::endpoint::EndpointContext;
use crate::dialer::tls::TlsConfigBuilder;
use crate::transport::ws::model::WsHandshakeHeaders;
use crate::transport::ws::stream::WsTunnelStream;
use crate::transport::{TargetAddr, TransportConnector};

/// Hyper executor bridging Hyper futures onto the Tokio runtime.
struct TokioSpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for TokioSpawnExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn(fut);
    }
}

/// WebSocket transport connector for opening outbound tunnels.
pub struct WsConnector {
    pub endpoint: EndpointContext,
    pub auth_token: Option<String>,
    pub mode: TunnelMode,
    pub tls_connector: Option<Arc<tokio_rustls::TlsConnector>>,
}

impl WsConnector {
    /// Creates a new `WsConnector`. Pre-builds the TLS connector if TLS is enabled.
    pub fn new(
        endpoint: EndpointContext,
        auth_token: impl Into<String>,
        mode: TunnelMode,
    ) -> Result<Self, TunnelError> {
        let tok: String = auth_token.into();
        let auth_token = if tok.is_empty() { None } else { Some(tok) };
        let tls_connector = if let Some(ref tls_cfg) = endpoint.tls_config {
            let conn = TlsConfigBuilder::build_connector(tls_cfg)?;
            Some(Arc::new(conn))
        } else {
            None
        };
        Ok(Self {
            endpoint,
            auth_token,
            mode,
            tls_connector,
        })
    }

    async fn connect_internal(
        &self,
        target: &TargetAddr,
        session_id: Option<&str>,
        pool_idle: bool,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let host = &self.endpoint.endpoint.host;
        let port = self.endpoint.endpoint.port;

        // 1. Establish underlying raw TCP stream
        let raw_stream = self.endpoint.dialer.dial(host, port).await?;

        // 2. Perform TLS handshake if TLS is configured for this endpoint (e.g. wss://)
        let tls_stream: Box<dyn TunnelStream> = if let Some(ref connector) = self.tls_connector {
            let sni = self
                .endpoint
                .tls_config
                .as_ref()
                .and_then(|t| t.server_name.as_deref())
                .unwrap_or(host.as_str());

            let server_name = ServerName::try_from(sni.to_string())
                .map_err(|e| TunnelError::Tls(format!("Invalid SNI server name: {}", e)))?;

            let tls_conn = tokio::time::timeout(
                crate::consts::DEFAULT_HANDSHAKE_TIMEOUT,
                connector.connect(server_name, raw_stream),
            )
            .await
            .map_err(|_| TunnelError::ConnectTimeout(crate::consts::DEFAULT_HANDSHAKE_TIMEOUT))?
            .map_err(|e| TunnelError::TlsHandshake(format!("TLS handshake failed: {}", e)))?;

            Box::new(BufferedTlsStream::new(tls_conn))
        } else {
            raw_stream
        };

        // 3. Construct HTTP Upgrade Request with custom tunnel headers
        let key = handshake::generate_key();

        let host_header = self
            .endpoint
            .http_headers
            .as_ref()
            .and_then(|headers| {
                headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("host"))
                    .map(|(_, v)| v.clone())
            })
            .unwrap_or_else(|| format!("{}:{}", host, port));

        let mut req_builder = Request::builder()
            .method("GET")
            .uri(&self.endpoint.endpoint.path)
            .header(HOST, host_header)
            .header(UPGRADE, crate::consts::HEADER_VALUE_WEBSOCKET)
            .header(CONNECTION, crate::consts::HEADER_VALUE_UPGRADE)
            .header(crate::consts::HEADER_SEC_WEBSOCKET_KEY, key)
            .header(
                crate::consts::HEADER_SEC_WEBSOCKET_VERSION,
                crate::consts::SEC_WEBSOCKET_VERSION_13,
            );

        if let Some(ref custom_headers) = self.endpoint.http_headers {
            for (key, val) in custom_headers {
                if !key.eq_ignore_ascii_case("host")
                    && !key.eq_ignore_ascii_case("upgrade")
                    && !key.eq_ignore_ascii_case("connection")
                    && !key.eq_ignore_ascii_case("sec-websocket-key")
                    && !key.eq_ignore_ascii_case("sec-websocket-version")
                {
                    req_builder = req_builder.header(key, val);
                }
            }
        }

        let headers = WsHandshakeHeaders::new(self.auth_token.clone(), self.mode)
            .with_session_id(session_id.map(|s| s.to_string()))
            .with_target(Some(target))
            .with_pool_idle(pool_idle);

        req_builder = headers.apply_to_request_builder(req_builder);

        let req = req_builder.body(Empty::<Bytes>::new()).map_err(|e| {
            TunnelError::Internal(format!("Failed to build upgrade request: {}", e))
        })?;

        // 4. Execute client-side WebSocket upgrade
        let (ws, _response) = tokio::time::timeout(
            crate::consts::DEFAULT_HANDSHAKE_TIMEOUT,
            handshake::client(&TokioSpawnExecutor, req, tls_stream),
        )
        .await
        .map_err(|_| TunnelError::ConnectTimeout(crate::consts::DEFAULT_HANDSHAKE_TIMEOUT))?
        .map_err(|e| {
            TunnelError::ControlProtocol(format!("WebSocket client handshake failed: {}", e))
        })?;

        // 5. Wrap upgraded stream as a dynamic TunnelStream
        let tunnel_stream = WsTunnelStream::new(ws.into_inner(), Role::Client);
        Ok(Box::new(tunnel_stream) as Box<dyn TunnelStream>)
    }
}

impl TransportConnector for WsConnector {
    fn connect<'a>(
        &'a self,
        target: &'a TargetAddr,
        session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        Box::pin(self.connect_internal(target, session_id, false))
    }

    fn connect_pooled<'a>(
        &'a self,
        target: &'a TargetAddr,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        Box::pin(self.connect_internal(target, None, true))
    }

    fn protocol_scheme(&self) -> &'static str {
        crate::consts::SCHEME_WSS
    }
}
