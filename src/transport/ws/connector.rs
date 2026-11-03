//! WebSocket client connector implementing `TransportConnector`.
//! All comments and docstrings must be in English.

use fastwebsockets::{Role, handshake};
use http::Request;
use http::header::{CONNECTION, HOST, UPGRADE};
use http_body_util::Empty;
use hyper::body::Bytes;
use rustls::pki_types::ServerName;
use std::future::Future;

use crate::config::model::TunnelMode;
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
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
    pub auth_token: String,
    pub mode: TunnelMode,
}

impl WsConnector {
    /// Creates a new `WsConnector`.
    pub fn new(endpoint: EndpointContext, auth_token: impl Into<String>, mode: TunnelMode) -> Self {
        Self {
            endpoint,
            auth_token: auth_token.into(),
            mode,
        }
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

        // 2. Perform TLS handshake if TLS is enabled for this endpoint
        let tls_stream: Box<dyn TunnelStream> = if let Some(ref tls_cfg) = self.endpoint.tls_config
        {
            if tls_cfg.enabled {
                let connector = TlsConfigBuilder::build_connector(tls_cfg)?;
                let sni = tls_cfg.server_name.as_deref().unwrap_or(host.as_str());

                let server_name = ServerName::try_from(sni.to_string())
                    .map_err(|e| TunnelError::Tls(format!("Invalid SNI server name: {}", e)))?;

                let tls_conn = connector
                    .connect(server_name, raw_stream)
                    .await
                    .map_err(|e| TunnelError::Tls(format!("TLS handshake failed: {}", e)))?;

                Box::new(tls_conn)
            } else {
                raw_stream
            }
        } else {
            raw_stream
        };

        // 3. Construct HTTP Upgrade Request with custom tunnel headers
        let key = handshake::generate_key();
        let mut req_builder = Request::builder()
            .method("GET")
            .uri(&self.endpoint.endpoint.path)
            .header(HOST, format!("{}:{}", host, port))
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "upgrade")
            .header("Sec-WebSocket-Key", key)
            .header("Sec-WebSocket-Version", "13");

        let headers = WsHandshakeHeaders::new(&self.auth_token, self.mode)
            .with_session_id(session_id.map(|s| s.to_string()))
            .with_target(Some(target))
            .with_pool_idle(pool_idle);

        req_builder = headers.apply_to_request_builder(req_builder);

        let req = req_builder.body(Empty::<Bytes>::new()).map_err(|e| {
            TunnelError::Internal(format!("Failed to build upgrade request: {}", e))
        })?;

        // 4. Execute client-side WebSocket upgrade
        let (ws, _response) = handshake::client(&TokioSpawnExecutor, req, tls_stream)
            .await
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
        "wss"
    }
}
