//! HTTP/3 and QUIC transport connector implementation.
//! All comments and docstrings must be in English.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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

/// Active HTTP/3 shared session holding an anchor SendRequest, underlying QUIC connection, and driver health.
struct H3Session {
    id: usize,
    sender: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    conn: quinn::Connection,
    driver_alive: Arc<AtomicBool>,
}

impl H3Session {
    fn is_alive(&self) -> bool {
        self.driver_alive.load(Ordering::Acquire) && self.conn.close_reason().is_none()
    }
}

/// Active raw QUIC shared session holding the underlying QUIC connection.
struct RawQuicSession {
    id: usize,
    conn: quinn::Connection,
}

impl RawQuicSession {
    fn is_alive(&self) -> bool {
        self.conn.close_reason().is_none()
    }
}

/// HTTP/3 and QUIC outbound transport connector.
pub struct H3Connector {
    endpoint: EndpointContext,
    auth_token: String,
    mode: TunnelMode,
    quinn_endpoint: quinn::Endpoint,
    cached_h3_session: Arc<RwLock<Option<Arc<H3Session>>>>,
    cached_quic_session: Arc<RwLock<Option<Arc<RawQuicSession>>>>,
    last_h3_failure: Arc<RwLock<Option<(std::time::Instant, String)>>>,
    last_quic_failure: Arc<RwLock<Option<(std::time::Instant, String)>>>,
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
            .max_idle_timeout(Some(std::time::Duration::from_secs(8).try_into().unwrap()));
        transport_config.keep_alive_interval(Some(std::time::Duration::from_secs(2)));
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
            cached_h3_session: Arc::new(RwLock::new(None)),
            cached_quic_session: Arc::new(RwLock::new(None)),
            last_h3_failure: Arc::new(RwLock::new(None)),
            last_quic_failure: Arc::new(RwLock::new(None)),
            h3_establish_gate: Arc::new(Mutex::new(())),
            quic_establish_gate: Arc::new(Mutex::new(())),
        })
    }

    /// Dials a fresh underlying QUIC connection to the target endpoint.
    async fn dial_quic_connection(&self) -> Result<quinn::Connection, TunnelError> {
        let ips = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.endpoint
                .dns_resolver
                .resolve(&self.endpoint.endpoint.host),
        )
        .await
        .map_err(|_| TunnelError::ConnectTimeout(std::time::Duration::from_secs(5)))??;
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

        let conn = tokio::time::timeout(std::time::Duration::from_secs(5), connecting)
            .await
            .map_err(|_| TunnelError::ConnectTimeout(std::time::Duration::from_secs(5)))?
            .map_err(|e| TunnelError::Tls(format!("QUIC handshake error: {:?}", e)))?;

        Ok(conn)
    }

    /// Establishes and caches the H3 client session with single-flight gating and a persistent anchor SendRequest.
    /// Never reuses a QUIC connection for a second H3 session, eliminating H3_STREAM_CREATION_ERROR.
    async fn get_or_establish_h3_session(&self) -> Result<Arc<H3Session>, TunnelError> {
        // Fast path: inspect cache under read lock
        if let Some(ref session) = *self.cached_h3_session.read()
            && session.is_alive()
        {
            return Ok(Arc::clone(session));
        }

        // Single-flight gate
        let _gate = self.h3_establish_gate.lock().await;

        // Double check
        if let Some(ref session) = *self.cached_h3_session.read()
            && session.is_alive()
        {
            return Ok(Arc::clone(session));
        }

        // Fail fast if an establishment attempt failed within the last 500ms to avoid serial cascading 5s timeouts
        if let Some((failed_at, ref err_msg)) = *self.last_h3_failure.read()
            && failed_at.elapsed() < std::time::Duration::from_millis(500)
        {
            return Err(TunnelError::ControlProtocol(format!(
                "H3 establishment failed recently: {}",
                err_msg
            )));
        }

        // If there is an existing dead session, close its QUIC connection explicitly
        if let Some(old_session) = self.cached_h3_session.write().take() {
            old_session
                .conn
                .close(quinn::VarInt::from_u32(0), b"replacing dead H3 session");
        }

        // Always dial a fresh QUIC connection dedicated to this H3 session
        let conn = match self.dial_quic_connection().await {
            Ok(c) => c,
            Err(e) => {
                *self.last_h3_failure.write() = Some((std::time::Instant::now(), e.to_string()));
                return Err(e);
            }
        };
        let id = conn.stable_id();

        let build_res = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            h3::client::builder().build::<_, _, Bytes>(h3_quinn::Connection::new(conn.clone())),
        )
        .await;

        let (mut driver, sender) = match build_res {
            Ok(Ok(pair)) => pair,
            Ok(Err(e)) => {
                let err_str = format!("H3 handshake failed: {:?}", e);
                *self.last_h3_failure.write() = Some((std::time::Instant::now(), err_str.clone()));
                conn.close(quinn::VarInt::from_u32(0), b"H3 build failed");
                return Err(TunnelError::ControlProtocol(err_str));
            }
            Err(_) => {
                *self.last_h3_failure.write() =
                    Some((std::time::Instant::now(), "H3 build timeout".to_string()));
                conn.close(quinn::VarInt::from_u32(0), b"H3 build timeout");
                return Err(TunnelError::ConnectTimeout(std::time::Duration::from_secs(
                    5,
                )));
            }
        };

        let driver_alive = Arc::new(AtomicBool::new(true));
        let driver_alive_clone = Arc::clone(&driver_alive);

        tokio::spawn(async move {
            let _ = driver.wait_idle().await;
            driver_alive_clone.store(false, Ordering::Release);
        });

        let new_session = Arc::new(H3Session {
            id,
            sender,
            conn,
            driver_alive,
        });

        *self.last_h3_failure.write() = None;
        *self.cached_h3_session.write() = Some(Arc::clone(&new_session));
        Ok(new_session)
    }

    /// Atomically clears the cached H3 session only if it matches the specified session ID,
    /// preventing lost updates from wiping out newer healthy connections.
    fn invalidate_h3_session_if_same(&self, session_id: usize) {
        let mut guard = self.cached_h3_session.write();
        if let Some(ref current) = *guard
            && current.id == session_id
        {
            debug!(session_id, "Invalidating and closing broken H3 session");
            current
                .conn
                .close(quinn::VarInt::from_u32(0), b"session invalidated");
            *guard = None;
        }
    }

    /// Establishes or returns an active raw QUIC session.
    async fn get_or_connect_quic_raw(&self) -> Result<Arc<RawQuicSession>, TunnelError> {
        if let Some(ref session) = *self.cached_quic_session.read()
            && session.is_alive()
        {
            return Ok(Arc::clone(session));
        }

        let _gate = self.quic_establish_gate.lock().await;

        if let Some(ref session) = *self.cached_quic_session.read()
            && session.is_alive()
        {
            return Ok(Arc::clone(session));
        }

        // Fail fast if raw QUIC dial failed within the last 500ms
        if let Some((failed_at, ref err_msg)) = *self.last_quic_failure.read()
            && failed_at.elapsed() < std::time::Duration::from_millis(500)
        {
            return Err(TunnelError::ControlProtocol(format!(
                "Raw QUIC establishment failed recently: {}",
                err_msg
            )));
        }

        if let Some(old_session) = self.cached_quic_session.write().take() {
            old_session.conn.close(
                quinn::VarInt::from_u32(0),
                b"replacing dead raw QUIC session",
            );
        }

        let conn = match self.dial_quic_connection().await {
            Ok(c) => c,
            Err(e) => {
                *self.last_quic_failure.write() = Some((std::time::Instant::now(), e.to_string()));
                return Err(e);
            }
        };
        let session = Arc::new(RawQuicSession {
            id: conn.stable_id(),
            conn,
        });

        *self.last_quic_failure.write() = None;
        *self.cached_quic_session.write() = Some(Arc::clone(&session));
        Ok(session)
    }

    /// Atomically clears the cached raw QUIC session only if it matches the specified session ID.
    fn invalidate_quic_session_if_same(&self, session_id: usize) {
        let mut guard = self.cached_quic_session.write();
        if let Some(ref current) = *guard
            && current.id == session_id
        {
            debug!(
                session_id,
                "Invalidating and closing broken raw QUIC session"
            );
            current
                .conn
                .close(quinn::VarInt::from_u32(0), b"raw session invalidated");
            *guard = None;
        }
    }

    /// Connects via standard HTTP/3 request stream.
    async fn connect_h3(
        &self,
        target: &TargetAddr,
        session_id: Option<&str>,
        pool_idle: bool,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let session = self.get_or_establish_h3_session().await?;
        let mut send_request = session.sender.clone();
        let session_id_val = session.id;
        let conn_clone = session.conn.clone();

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
                self.invalidate_h3_session_if_same(session_id_val);
                return Err(TunnelError::ControlProtocol(format!(
                    "Failed to send H3 request: {:?}",
                    e
                )));
            }
            Err(_) => {
                self.invalidate_h3_session_if_same(session_id_val);
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
                    self.invalidate_h3_session_if_same(session_id_val);
                    return Err(TunnelError::ControlProtocol(format!(
                        "Failed to receive H3 response: {:?}",
                        e
                    )));
                }
                Err(_) => {
                    self.invalidate_h3_session_if_same(session_id_val);
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

        Ok(
            Box::new(H3TunnelStream::from_client(stream).with_connection(conn_clone))
                as Box<dyn TunnelStream>,
        )
    }

    /// Connects via raw QUIC bidirectional stream.
    async fn connect_quic_raw(
        &self,
        target: &TargetAddr,
        session_id: Option<&str>,
        pool_idle: bool,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let session = self.get_or_connect_quic_raw().await?;
        let conn = session.conn.clone();
        let session_id_val = session.id;

        let (mut send, recv) =
            match tokio::time::timeout(std::time::Duration::from_secs(5), conn.open_bi()).await {
                Ok(Ok(pair)) => pair,
                Ok(Err(e)) => {
                    self.invalidate_quic_session_if_same(session_id_val);
                    return Err(TunnelError::Tls(format!(
                        "Failed to open QUIC bi-stream: {:?}",
                        e
                    )));
                }
                Err(_) => {
                    self.invalidate_quic_session_if_same(session_id_val);
                    return Err(TunnelError::ConnectTimeout(std::time::Duration::from_secs(
                        5,
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
            std::time::Duration::from_secs(5),
            tokio::io::AsyncWriteExt::write_all(&mut send, &header),
        )
        .await;

        match write_res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                self.invalidate_quic_session_if_same(session_id_val);
                return Err(TunnelError::Io(e));
            }
            Err(_) => {
                self.invalidate_quic_session_if_same(session_id_val);
                return Err(TunnelError::ConnectTimeout(std::time::Duration::from_secs(
                    5,
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

    fn invalidate_broken_sessions(&self) {
        if let Some(s) = self.cached_h3_session.write().take() {
            s.conn
                .close(quinn::VarInt::from_u32(0), b"pool evicted dead streams");
        }
        if let Some(s) = self.cached_quic_session.write().take() {
            s.conn
                .close(quinn::VarInt::from_u32(0), b"pool evicted dead streams");
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

        // Cached sessions start empty
        assert!(connector.cached_h3_session.read().is_none());
        assert!(connector.cached_quic_session.read().is_none());

        let barrier = Arc::new(Barrier::new(8));
        let mut tasks = Vec::new();

        for _ in 0..8 {
            let conn = Arc::clone(&connector);
            let b = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                b.wait().await;
                let s = conn.cached_h3_session.read().clone();
                let q = conn.cached_quic_session.read().clone();
                assert!(s.is_none());
                assert!(q.is_none());
            }));
        }

        for t in tasks {
            t.await.unwrap();
        }
    }

    async fn create_test_quic_connection_pair() -> (quinn::Connection, quinn::Connection) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().to_vec();
        let key_der = cert.signing_key.serialize_der();
        let cert_chain = vec![rustls::pki_types::CertificateDer::from(cert_der.clone())];
        let private_key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
        );
        let mut server_crypto = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .unwrap();
        server_crypto.alpn_protocols = vec![b"micro-tunnel-quic".to_vec()];
        let server_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto).unwrap(),
        ));
        let server_ep =
            quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let server_addr = server_ep.local_addr().unwrap();

        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls::pki_types::CertificateDer::from(cert_der))
            .unwrap();
        let mut client_crypto = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client_crypto.alpn_protocols = vec![b"micro-tunnel-quic".to_vec()];
        let client_config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto).unwrap(),
        ));
        let mut client_ep = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client_ep.set_default_client_config(client_config);

        let connecting = client_ep.connect(server_addr, "localhost").unwrap();
        let server_incoming = server_ep.accept().await.unwrap();
        let (server_conn, client_conn) =
            tokio::join!(async { server_incoming.await.unwrap() }, async {
                connecting.await.unwrap()
            });
        (server_conn, client_conn)
    }

    #[tokio::test]
    async fn test_compare_and_clear_prevents_lost_update() {
        let app_cfg = AppConfig::default();
        let ep_cfg = EndpointConfig::Simple("h3://127.0.0.1:4433/tunnel".to_string());
        let ctx = EndpointContext::build(&ep_cfg, &app_cfg).unwrap();
        let connector = H3Connector::new(&ctx, "test-token", TunnelMode::Forward).unwrap();

        let (server_conn, client_conn) = create_test_quic_connection_pair().await;
        let dummy_quic_session = Arc::new(RawQuicSession {
            id: client_conn.stable_id(),
            conn: client_conn,
        });

        *connector.cached_quic_session.write() = Some(Arc::clone(&dummy_quic_session));

        // Attempt to invalidate with an older/different session ID
        let wrong_id = dummy_quic_session.id + 999;
        connector.invalidate_quic_session_if_same(wrong_id);

        // Cached session MUST NOT be cleared (lost update prevented!)
        assert!(connector.cached_quic_session.read().is_some());
        assert_eq!(
            connector.cached_quic_session.read().as_ref().unwrap().id,
            dummy_quic_session.id
        );

        // Invalidate with matching session ID
        let matching_id = dummy_quic_session.id;
        connector.invalidate_quic_session_if_same(matching_id);

        // Cached session is now cleared
        assert!(connector.cached_quic_session.read().is_none());
        drop(server_conn);
    }
}
