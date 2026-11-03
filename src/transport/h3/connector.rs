//! HTTP/3 and QUIC transport connector implementation.
//! All comments and docstrings must be in English.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http::{Request, StatusCode};
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::config::model::TunnelMode;
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::dialer::endpoint::EndpointContext;
use crate::dialer::tls::TlsConfigBuilder;
use crate::transport::h3::stream::{H3TunnelStream, QuinnTunnelStream};
use crate::transport::{TargetAddr, TransportConnector};

/// HTTP/3 and QUIC outbound transport connector.
pub struct H3Connector {
    endpoint: EndpointContext,
    auth_token: String,
    mode: TunnelMode,
    quinn_endpoint: quinn::Endpoint,
    cached_h3_sender: Arc<Mutex<Option<h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>>>>,
    cached_quic_conn: Arc<Mutex<Option<quinn::Connection>>>,
}

impl H3Connector {
    /// Creates a new `H3Connector` bound to the given endpoint context.
    pub fn new(
        endpoint: &EndpointContext,
        auth_token: &str,
        mode: TunnelMode,
    ) -> Result<Self, TunnelError> {
        let scheme = endpoint.endpoint.scheme.as_str();
        let is_quic_raw = scheme == "quic";

        let mut tls_config = endpoint.tls_config.clone().unwrap_or_default();
        if is_quic_raw {
            // For raw QUIC endpoints, force ALPN to micro-tunnel-quic regardless of generic global TLS settings
            tls_config.alpn = Some(vec!["micro-tunnel-quic".to_string()]);
        } else if tls_config.alpn.is_none() {
            tls_config.alpn = Some(vec!["h3".to_string()]);
        }

        let rustls_client = TlsConfigBuilder::build_client_config(&tls_config)?;
        let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(rustls_client)
            .map_err(|e| {
                TunnelError::Tls(format!("Failed to build QUIC client crypto: {:?}", e))
            })?;

        let mut client_config = quinn::ClientConfig::new(Arc::new(quic_crypto));
        let mut transport_config = quinn::TransportConfig::default();
        transport_config
            .max_idle_timeout(Some(std::time::Duration::from_secs(30).try_into().unwrap()));
        transport_config.keep_alive_interval(Some(std::time::Duration::from_secs(10)));
        transport_config.max_concurrent_bidi_streams(quinn::VarInt::from_u32(10_000));
        transport_config.max_concurrent_uni_streams(quinn::VarInt::from_u32(10_000));
        transport_config.stream_receive_window(quinn::VarInt::from_u32(16 * 1024 * 1024).into());
        transport_config.receive_window(quinn::VarInt::from_u32(32 * 1024 * 1024).into());
        transport_config.send_window(32 * 1024 * 1024);
        client_config.transport_config(Arc::new(transport_config));

        let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let mut quinn_endpoint =
            quinn::Endpoint::client(bind_addr).map_err(|e| TunnelError::Io(e))?;
        quinn_endpoint.set_default_client_config(client_config);

        Ok(Self {
            endpoint: endpoint.clone(),
            auth_token: auth_token.to_string(),
            mode,
            quinn_endpoint,
            cached_h3_sender: Arc::new(Mutex::new(None)),
            cached_quic_conn: Arc::new(Mutex::new(None)),
        })
    }

    /// Establishes or returns an active QUIC connection to the target server.
    async fn get_or_connect(&self) -> Result<quinn::Connection, TunnelError> {
        let mut guard = self.cached_quic_conn.lock().await;
        if let Some(ref conn) = *guard {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
        }

        let ips = self
            .endpoint
            .dns_resolver
            .resolve(&self.endpoint.endpoint.host)
            .await?;
        let ip = ips.first().copied().ok_or_else(|| {
            TunnelError::DnsNoAddresses(self.endpoint.endpoint.host.clone(), "any".to_string())
        })?;
        let resolved_addr = SocketAddr::new(ip, self.endpoint.endpoint.port);
        let sni = self
            .endpoint
            .tls_config
            .as_ref()
            .and_then(|t| t.server_name.as_deref())
            .unwrap_or_else(|| self.endpoint.endpoint.host.as_str());

        info!(server = %resolved_addr, sni = %sni, "Dialing QUIC transport endpoint");
        let connecting = self
            .quinn_endpoint
            .connect(resolved_addr, sni)
            .map_err(|e| TunnelError::Tls(format!("QUIC connect error: {:?}", e)))?;

        let conn = connecting
            .await
            .map_err(|e| TunnelError::Tls(format!("QUIC handshake error: {:?}", e)))?;

        *guard = Some(conn.clone());
        Ok(conn)
    }

    /// Connects via standard HTTP/3 request stream.
    async fn connect_h3(
        &self,
        target: &TargetAddr,
        session_id: Option<&str>,
        pool_idle: bool,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let mut sender_guard = self.cached_h3_sender.lock().await;
        let mut send_request = None;

        let quic_alive = {
            let qguard = self.cached_quic_conn.lock().await;
            qguard
                .as_ref()
                .map(|c| c.close_reason().is_none())
                .unwrap_or(false)
        };

        if !quic_alive {
            *sender_guard = None;
        } else if let Some(ref sender) = *sender_guard {
            send_request = Some(sender.clone());
        }

        if send_request.is_none() {
            let conn = self.get_or_connect().await?;
            let (mut driver, sender) = h3::client::builder()
                .build::<_, _, Bytes>(h3_quinn::Connection::new(conn))
                .await
                .map_err(|e| {
                    TunnelError::ControlProtocol(format!("H3 handshake failed: {:?}", e))
                })?;

            tokio::spawn(async move {
                let _ = driver.wait_idle().await;
            });

            *sender_guard = Some(sender.clone());
            send_request = Some(sender);
        }
        drop(sender_guard);

        let mut send_request = send_request.unwrap();
        let path = if self.endpoint.endpoint.path.is_empty() {
            "/tunnel"
        } else {
            &self.endpoint.endpoint.path
        };

        let authority = format!(
            "{}:{}",
            self.endpoint.endpoint.host, self.endpoint.endpoint.port
        );
        let mut req_builder = Request::builder()
            .method("CONNECT")
            .uri(path)
            .header("host", &authority);

        if !self.auth_token.is_empty() {
            req_builder = req_builder
                .header("authorization", format!("Bearer {}", self.auth_token))
                .header(crate::consts::HEADER_TUNNEL_AUTH, &self.auth_token);
        }

        let mode_str = match self.mode {
            TunnelMode::Forward => crate::consts::TUNNEL_MODE_FORWARD,
            TunnelMode::Reverse => {
                if session_id.is_some() {
                    crate::consts::TUNNEL_MODE_REVERSE_DATA
                } else {
                    crate::consts::TUNNEL_MODE_REVERSE_CTRL
                }
            }
        };
        req_builder = req_builder.header(crate::consts::HEADER_TUNNEL_MODE, mode_str);

        if pool_idle {
            req_builder = req_builder.header(
                crate::consts::HEADER_TUNNEL_POOL,
                crate::consts::TUNNEL_POOL_IDLE,
            );
        }

        req_builder = req_builder.header("x-tunnel-target", target.to_string());

        if let Some(sid) = session_id {
            req_builder = req_builder.header(crate::consts::HEADER_SESSION_ID, sid);
        }

        let request = req_builder
            .body(())
            .map_err(|e| TunnelError::Config(format!("Invalid request headers: {:?}", e)))?;

        debug!(path = %path, target = %target, pool_idle, "Sending HTTP/3 CONNECT tunnel request");
        let mut stream = match send_request.send_request(request).await {
            Ok(s) => s,
            Err(e) => {
                let mut guard = self.cached_h3_sender.lock().await;
                *guard = None;
                let mut qguard = self.cached_quic_conn.lock().await;
                *qguard = None;
                return Err(TunnelError::ControlProtocol(format!(
                    "Failed to send H3 request: {:?}",
                    e
                )));
            }
        };

        let response = match stream.recv_response().await {
            Ok(r) => r,
            Err(e) => {
                let mut guard = self.cached_h3_sender.lock().await;
                *guard = None;
                let mut qguard = self.cached_quic_conn.lock().await;
                *qguard = None;
                return Err(TunnelError::ControlProtocol(format!(
                    "Failed to receive H3 response: {:?}",
                    e
                )));
            }
        };

        if response.status() == StatusCode::UNAUTHORIZED
            || response.status() == StatusCode::FORBIDDEN
        {
            return Err(TunnelError::AuthFailed);
        }

        if !response.status().is_success() {
            return Err(TunnelError::ControlProtocol(format!(
                "H3 server rejected tunnel with status: {}",
                response.status()
            )));
        }

        Ok(Box::new(H3TunnelStream::from_client(stream)) as Box<dyn TunnelStream>)
    }

    /// Connects via raw QUIC bidirectional stream.
    async fn connect_quic_raw(
        &self,
        target: &TargetAddr,
        session_id: Option<&str>,
        pool_idle: bool,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let conn = self.get_or_connect().await?;
        let (mut send, recv) = match conn.open_bi().await {
            Ok(pair) => pair,
            Err(e) => {
                let mut guard = self.cached_quic_conn.lock().await;
                *guard = None;
                return Err(TunnelError::Tls(format!(
                    "Failed to open QUIC bi-stream: {:?}",
                    e
                )));
            }
        };

        // Send handshake control header over the raw QUIC stream
        let handshake_meta = crate::transport::StreamMetadata {
            mode: self.mode,
            session_id: session_id.map(|s| s.to_string()),
            target: Some(target.clone()),
            pool_idle,
        };

        let handshake_bytes = rmp_serde::to_vec_named(&handshake_meta)
            .map_err(|e| TunnelError::ControlProtocol(format!("Serialization error: {:?}", e)))?;

        // Format: 4 bytes auth_len + auth_token + 4 bytes meta_len + meta
        let auth_bytes = self.auth_token.as_bytes();
        let mut header = Vec::new();
        header.extend_from_slice(&(auth_bytes.len() as u32).to_be_bytes());
        header.extend_from_slice(auth_bytes);
        header.extend_from_slice(&(handshake_bytes.len() as u32).to_be_bytes());
        header.extend_from_slice(&handshake_bytes);

        if let Err(e) = tokio::io::AsyncWriteExt::write_all(&mut send, &header).await {
            let mut guard = self.cached_quic_conn.lock().await;
            *guard = None;
            return Err(TunnelError::Io(e));
        }

        Ok(Box::new(QuinnTunnelStream::new(send, recv)) as Box<dyn TunnelStream>)
    }
}

impl TransportConnector for H3Connector {
    fn connect<'a>(
        &'a self,
        target: &'a TargetAddr,
        session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        Box::pin(async move {
            let scheme = self.endpoint.endpoint.scheme.as_str();
            if scheme == "quic" {
                self.connect_quic_raw(target, session_id, false).await
            } else {
                self.connect_h3(target, session_id, false).await
            }
        })
    }

    fn connect_pooled<'a>(
        &'a self,
        target: &'a TargetAddr,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        Box::pin(async move {
            let scheme = self.endpoint.endpoint.scheme.as_str();
            if scheme == "quic" {
                self.connect_quic_raw(target, None, true).await
            } else {
                self.connect_h3(target, None, true).await
            }
        })
    }

    fn protocol_scheme(&self) -> &'static str {
        if self.endpoint.endpoint.scheme == "quic" {
            "quic"
        } else {
            "h3"
        }
    }
}
