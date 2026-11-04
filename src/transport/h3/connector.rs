//! HTTP/3 and QUIC transport connector implementation.
//! All comments and docstrings must be in English.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http::{Request, StatusCode};
use parking_lot::RwLock;
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
    cached_h3_sender: Arc<RwLock<Option<h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>>>>,
    cached_quic_conn: Arc<RwLock<Option<quinn::Connection>>>,
    h3_establish_gate: Arc<Mutex<()>>,
    quic_establish_gate: Arc<Mutex<()>>,
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
        transport_config.stream_receive_window(quinn::VarInt::from_u32(16 * 1024 * 1024));
        transport_config.receive_window(quinn::VarInt::from_u32(32 * 1024 * 1024));
        transport_config.send_window(32 * 1024 * 1024);
        client_config.transport_config(Arc::new(transport_config));

        let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let mut quinn_endpoint = quinn::Endpoint::client(bind_addr).map_err(TunnelError::Io)?;
        quinn_endpoint.set_default_client_config(client_config);

        Ok(Self {
            endpoint: endpoint.clone(),
            auth_token: auth_token.to_string(),
            mode,
            quinn_endpoint,
            cached_h3_sender: Arc::new(RwLock::new(None)),
            cached_quic_conn: Arc::new(RwLock::new(None)),
            h3_establish_gate: Arc::new(Mutex::new(())),
            quic_establish_gate: Arc::new(Mutex::new(())),
        })
    }

    /// Establishes or returns an active QUIC connection to the target server.
    /// Uses lock-free read path and single-flight gate; never holds cache locks across network RTTs.
    async fn get_or_connect(&self) -> Result<quinn::Connection, TunnelError> {
        // Fast path: inspect under non-blocking read lock
        if let Some(ref conn) = *self.cached_quic_conn.read()
            && conn.close_reason().is_none()
        {
            return Ok(conn.clone());
        }

        // Single-flight gate to serialize connection attempts without blocking readers
        let _gate = self.quic_establish_gate.lock().await;

        // Double-check after acquiring establishment gate
        if let Some(ref conn) = *self.cached_quic_conn.read()
            && conn.close_reason().is_none()
        {
            return Ok(conn.clone());
        }

        let ips = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.endpoint
                .dns_resolver
                .resolve(&self.endpoint.endpoint.host),
        )
        .await
        .map_err(|_| TunnelError::ConnectTimeout(std::time::Duration::from_secs(10)))??;
        let ip = ips.first().copied().ok_or_else(|| {
            TunnelError::DnsNoAddresses(self.endpoint.endpoint.host.clone(), "any".to_string())
        })?;
        let resolved_addr = SocketAddr::new(ip, self.endpoint.endpoint.port);
        let sni = self
            .endpoint
            .tls_config
            .as_ref()
            .and_then(|t| t.server_name.as_deref())
            .unwrap_or(self.endpoint.endpoint.host.as_str());

        info!(server = %resolved_addr, sni = %sni, "Dialing QUIC transport endpoint");
        let connecting = self
            .quinn_endpoint
            .connect(resolved_addr, sni)
            .map_err(|e| TunnelError::Tls(format!("QUIC connect error: {:?}", e)))?;

        let conn = tokio::time::timeout(std::time::Duration::from_secs(10), connecting)
            .await
            .map_err(|_| TunnelError::ConnectTimeout(std::time::Duration::from_secs(10)))?
            .map_err(|e| TunnelError::Tls(format!("QUIC handshake error: {:?}", e)))?;

        *self.cached_quic_conn.write() = Some(conn.clone());
        Ok(conn)
    }

    /// Establishes and caches the H3 client sender with single-flight gating,
    /// keeping subsequent multi-stream creation outside the lock and avoiding RTT holding.
    async fn establish_h3_sender(
        &self,
    ) -> Result<h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>, TunnelError> {
        // Fast path: inspect cache under read lock
        if let Some(ref sender) = *self.cached_h3_sender.read() {
            let quic_alive = self
                .cached_quic_conn
                .read()
                .as_ref()
                .map(|c| c.close_reason().is_none())
                .unwrap_or(false);
            if quic_alive {
                return Ok(sender.clone());
            }
        }

        // Single-flight gate
        let _gate = self.h3_establish_gate.lock().await;

        // Double check
        if let Some(ref sender) = *self.cached_h3_sender.read() {
            let quic_alive = self
                .cached_quic_conn
                .read()
                .as_ref()
                .map(|c| c.close_reason().is_none())
                .unwrap_or(false);
            if quic_alive {
                return Ok(sender.clone());
            }
        }

        let conn = self.get_or_connect().await?;
        let (mut driver, sender) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            h3::client::builder().build::<_, _, Bytes>(h3_quinn::Connection::new(conn)),
        )
        .await
        .map_err(|_| TunnelError::ConnectTimeout(std::time::Duration::from_secs(10)))?
        .map_err(|e| TunnelError::ControlProtocol(format!("H3 handshake failed: {:?}", e)))?;

        tokio::spawn(async move {
            let _ = driver.wait_idle().await;
        });

        *self.cached_h3_sender.write() = Some(sender.clone());
        Ok(sender)
    }

    /// Connects via standard HTTP/3 request stream.
    async fn connect_h3(
        &self,
        target: &TargetAddr,
        session_id: Option<&str>,
        pool_idle: bool,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        // Fast path: clone cached sender under non-blocking read lock
        let existing_sender = self.cached_h3_sender.read().clone();

        let send_request = if let Some(sender) = existing_sender {
            let quic_alive = self
                .cached_quic_conn
                .read()
                .as_ref()
                .map(|c| c.close_reason().is_none())
                .unwrap_or(false);
            if quic_alive {
                sender
            } else {
                self.establish_h3_sender().await?
            }
        } else {
            self.establish_h3_sender().await?
        };

        let mut send_request = send_request;
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
        let mut stream = match tokio::time::timeout(
            std::time::Duration::from_secs(10),
            send_request.send_request(request),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                *self.cached_h3_sender.write() = None;
                *self.cached_quic_conn.write() = None;
                return Err(TunnelError::ControlProtocol(format!(
                    "Failed to send H3 request: {:?}",
                    e
                )));
            }
            Err(_) => {
                *self.cached_h3_sender.write() = None;
                *self.cached_quic_conn.write() = None;
                return Err(TunnelError::ConnectTimeout(std::time::Duration::from_secs(
                    10,
                )));
            }
        };

        let response =
            match tokio::time::timeout(std::time::Duration::from_secs(10), stream.recv_response())
                .await
            {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    *self.cached_h3_sender.write() = None;
                    *self.cached_quic_conn.write() = None;
                    return Err(TunnelError::ControlProtocol(format!(
                        "Failed to receive H3 response: {:?}",
                        e
                    )));
                }
                Err(_) => {
                    *self.cached_h3_sender.write() = None;
                    *self.cached_quic_conn.write() = None;
                    return Err(TunnelError::ConnectTimeout(std::time::Duration::from_secs(
                        10,
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
        let (mut send, recv) =
            match tokio::time::timeout(std::time::Duration::from_secs(10), conn.open_bi()).await {
                Ok(Ok(pair)) => pair,
                Ok(Err(e)) => {
                    *self.cached_quic_conn.write() = None;
                    return Err(TunnelError::Tls(format!(
                        "Failed to open QUIC bi-stream: {:?}",
                        e
                    )));
                }
                Err(_) => {
                    *self.cached_quic_conn.write() = None;
                    return Err(TunnelError::ConnectTimeout(std::time::Duration::from_secs(
                        10,
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

        let write_res = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio::io::AsyncWriteExt::write_all(&mut send, &header),
        )
        .await;

        match write_res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                *self.cached_quic_conn.write() = None;
                return Err(TunnelError::Io(e));
            }
            Err(_) => {
                *self.cached_quic_conn.write() = None;
                return Err(TunnelError::ConnectTimeout(std::time::Duration::from_secs(
                    10,
                )));
            }
        }

        Ok(
            Box::new(QuinnTunnelStream::new(send, recv).with_connection(conn))
                as Box<dyn TunnelStream>,
        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{AppConfig, EndpointConfig};

    #[tokio::test]
    async fn test_h3_connector_fast_path_read_lock_concurrency() {
        use std::sync::Arc;
        use tokio::sync::Barrier;

        let app_cfg = AppConfig::default();
        let ep_cfg = EndpointConfig::Simple("h3://127.0.0.1:4433/tunnel".to_string());
        let ctx = EndpointContext::build(&ep_cfg, &app_cfg).unwrap();
        let connector =
            Arc::new(H3Connector::new(&ctx, "test-token", TunnelMode::Forward).unwrap());

        // Cached sender starts empty
        assert!(connector.cached_h3_sender.read().is_none());
        assert!(connector.cached_quic_conn.read().is_none());

        let barrier = Arc::new(Barrier::new(8));
        let mut tasks = Vec::new();

        for _ in 0..8 {
            let conn = Arc::clone(&connector);
            let b = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                b.wait().await;
                let s = conn.cached_h3_sender.read().clone();
                let q = conn.cached_quic_conn.read().clone();
                assert!(s.is_none());
                assert!(q.is_none());
            }));
        }

        for t in tasks {
            t.await.unwrap();
        }
    }
}
