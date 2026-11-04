//! HTTP/3 and QUIC ingress transport listener implementation.
//! All comments and docstrings must be in English.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http::{Response, StatusCode};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::model::TunnelMode;
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::transport::h3::stream::{H3TunnelStream, QuinnTunnelStream};
use crate::transport::{StreamMetadata, TargetAddr, TransportListener};

/// HTTP/3 and QUIC transport listener.
pub struct H3Listener {
    rx_streams: mpsc::Receiver<(Box<dyn TunnelStream>, StreamMetadata)>,
    cancel_token: CancellationToken,
}

impl H3Listener {
    /// Binds an `H3Listener` using an existing `rustls::ServerConfig`.
    pub fn bind(
        listen_addr: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        server_rustls_config: rustls::ServerConfig,
    ) -> Result<Self, TunnelError> {
        let clean_addr = if let Some(idx) = listen_addr.find("://") {
            let without_scheme = &listen_addr[idx + 3..];
            without_scheme.split('/').next().unwrap_or(without_scheme)
        } else {
            listen_addr
        };

        let addr: SocketAddr = clean_addr.parse().map_err(|e| {
            TunnelError::Config(format!("Invalid listen address '{}': {}", listen_addr, e))
        })?;

        let is_quic_listener = listen_addr.starts_with("quic://");
        let mut rustls_server = server_rustls_config;
        rustls_server.alpn_protocols = if is_quic_listener {
            vec![b"micro-tunnel-quic".to_vec()]
        } else {
            vec![b"h3".to_vec(), b"micro-tunnel-quic".to_vec()]
        };

        let quic_server_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(rustls_server)
            .map_err(|e| {
            TunnelError::Tls(format!("Failed to build QUIC server crypto: {:?}", e))
        })?;

        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server_crypto));
        let mut transport_config = quinn::TransportConfig::default();
        transport_config
            .max_idle_timeout(Some(std::time::Duration::from_secs(30).try_into().unwrap()));
        transport_config.keep_alive_interval(Some(std::time::Duration::from_secs(10)));
        transport_config.max_concurrent_bidi_streams(quinn::VarInt::from_u32(10_000));
        transport_config.max_concurrent_uni_streams(quinn::VarInt::from_u32(10_000));
        transport_config.stream_receive_window(quinn::VarInt::from_u32(16 * 1024 * 1024).into());
        transport_config.receive_window(quinn::VarInt::from_u32(32 * 1024 * 1024).into());
        transport_config.send_window(32 * 1024 * 1024);
        server_config.transport_config(Arc::new(transport_config));

        let quinn_endpoint =
            quinn::Endpoint::server(server_config, addr).map_err(|e| TunnelError::Io(e))?;

        info!(addr = %addr, is_quic_listener, "Bound HTTP/3 & QUIC listener on UDP socket");

        let (tx_streams, rx_streams) = mpsc::channel(128);
        let cancel_token = CancellationToken::new();

        Self::spawn_listener_loop(
            quinn_endpoint,
            auth_token.map(|s| s.to_string()),
            path.map(|p| p.to_string()),
            is_quic_listener,
            tx_streams,
            cancel_token.clone(),
        );

        Ok(Self {
            rx_streams,
            cancel_token,
        })
    }

    /// Spawns the main listener event loop accepting QUIC connections.
    fn spawn_listener_loop(
        endpoint: quinn::Endpoint,
        auth_token: Option<String>,
        path: Option<String>,
        is_quic_listener: bool,
        tx_streams: mpsc::Sender<(Box<dyn TunnelStream>, StreamMetadata)>,
        cancel_token: CancellationToken,
    ) {
        let auth_token = Arc::new(auth_token);
        let path = Arc::new(path);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    incoming = endpoint.accept() => {
                        match incoming {
                            Some(incoming) => {
                                match incoming.accept() {
                                    Ok(mut connecting) => {
                                        let auth_clone = Arc::clone(&auth_token);
                                        let path_clone = Arc::clone(&path);
                                        let tx_clone = tx_streams.clone();
                                        let cancel_conn = cancel_token.clone();

                                        tokio::spawn(async move {
                                            let alpn = {
                                                let handshake_res = connecting.handshake_data().await;
                                                handshake_res
                                                    .ok()
                                                    .and_then(|h| h.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
                                                    .and_then(|h| h.protocol)
                                            };
                                            match connecting.await {
                                                Ok(conn) => {
                                                    Self::handle_connection(
                                                        conn,
                                                        alpn,
                                                        is_quic_listener,
                                                        auth_clone,
                                                        path_clone,
                                                        tx_clone,
                                                        cancel_conn,
                                                    ).await;
                                                }
                                                Err(e) => {
                                                    debug!("QUIC handshake error: {:?}", e);
                                                }
                                            }
                                        });
                                    }
                                    Err(e) => {
                                        debug!("Failed to accept incoming connection: {:?}", e);
                                    }
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });
    }

    /// Handles an established QUIC connection, demuxing H3 or raw QUIC streams.
    async fn handle_connection(
        conn: quinn::Connection,
        alpn: Option<Vec<u8>>,
        is_quic_listener: bool,
        auth_token: Arc<Option<String>>,
        path: Arc<Option<String>>,
        tx_streams: mpsc::Sender<(Box<dyn TunnelStream>, StreamMetadata)>,
        cancel_token: CancellationToken,
    ) {
        let is_raw_quic = is_quic_listener || alpn.as_deref() == Some(b"micro-tunnel-quic");
        debug!(
            alpn = ?alpn.as_ref().map(|b| String::from_utf8_lossy(b)),
            is_quic_listener,
            is_raw_quic,
            "Accepted incoming QUIC connection"
        );

        if is_raw_quic {
            Self::handle_raw_quic_streams(conn, auth_token, tx_streams, cancel_token).await;
        } else {
            Self::handle_h3_streams(conn, auth_token, path, tx_streams, cancel_token).await;
        }
    }

    /// Processes HTTP/3 request streams on the connection.
    async fn handle_h3_streams(
        conn: quinn::Connection,
        auth_token: Arc<Option<String>>,
        expected_path: Arc<Option<String>>,
        tx_streams: mpsc::Sender<(Box<dyn TunnelStream>, StreamMetadata)>,
        cancel_token: CancellationToken,
    ) {
        let mut h3_conn = match h3::server::builder()
            .build::<_, Bytes>(h3_quinn::Connection::new(conn))
            .await
        {
            Ok(server) => server,
            Err(e) => {
                debug!("Failed to start H3 server session: {:?}", e);
                return;
            }
        };

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                resolver_opt = h3_conn.accept() => {
                    match resolver_opt {
                        Ok(Some(resolver)) => {
                            let auth_clone = Arc::clone(&auth_token);
                            let path_clone = Arc::clone(&expected_path);
                            let tx_clone = tx_streams.clone();

                            tokio::spawn(async move {
                                let (req, mut stream) = match resolver.resolve_request().await {
                                    Ok(pair) => pair,
                                    Err(e) => {
                                        debug!("Failed to resolve H3 request: {:?}", e);
                                        return;
                                    }
                                };

                                // Validate URI path
                                if let Some(ref p) = *path_clone {
                                    if req.uri().path() != p.as_str() {
                                        let resp = Response::builder()
                                            .status(StatusCode::NOT_FOUND)
                                            .body(())
                                            .unwrap();
                                        let _ = stream.send_response(resp).await;
                                        return;
                                    }
                                }

                                // Validate authorization token if configured
                                if let Some(ref token) = *auth_clone {
                                    if !token.is_empty() {
                                        let auth_header = req
                                            .headers()
                                            .get("authorization")
                                            .or_else(|| req.headers().get(crate::consts::HEADER_TUNNEL_AUTH))
                                            .and_then(|v| v.to_str().ok());

                                        let expected_bearer = format!("Bearer {}", token);
                                        let is_authed = auth_header == Some(&expected_bearer)
                                            || auth_header == Some(token.as_str());

                                        if !is_authed {
                                            warn!("H3 request authentication failed");
                                            let resp = Response::builder()
                                                .status(StatusCode::UNAUTHORIZED)
                                                .body(())
                                                .unwrap();
                                            let _ = stream.send_response(resp).await;
                                            return;
                                        }
                                    }
                                }

                                let mode = match req
                                    .headers()
                                    .get(crate::consts::HEADER_TUNNEL_MODE)
                                    .and_then(|v| v.to_str().ok())
                                {
                                    Some(crate::consts::TUNNEL_MODE_REVERSE_CTRL)
                                    | Some(crate::consts::TUNNEL_MODE_REVERSE_DATA)
                                    | Some("reverse") => TunnelMode::Reverse,
                                    _ => TunnelMode::Forward,
                                };

                                let target = req
                                    .headers()
                                    .get("x-tunnel-target")
                                    .and_then(|v| v.to_str().ok())
                                    .and_then(|s| {
                                        if let Ok(addr) = s.parse::<SocketAddr>() {
                                            Some(TargetAddr::Ip(addr))
                                        } else if let Some((h, p)) = s.split_once(':') {
                                            p.parse::<u16>()
                                                .ok()
                                                .map(|port| TargetAddr::Domain(h.to_string(), port))
                                        } else {
                                            None
                                        }
                                    })
                                    .or_else(|| {
                                        let host = req
                                            .headers()
                                            .get(crate::consts::HEADER_TARGET_HOST)
                                            .and_then(|v| v.to_str().ok());
                                        let port = req
                                            .headers()
                                            .get(crate::consts::HEADER_TARGET_PORT)
                                            .and_then(|v| v.to_str().ok())
                                            .and_then(|p| p.parse::<u16>().ok());
                                        match (host, port) {
                                            (Some(h), Some(p)) => {
                                                if let Ok(ip) = h.parse::<std::net::IpAddr>() {
                                                    Some(TargetAddr::Ip(SocketAddr::new(ip, p)))
                                                } else {
                                                    Some(TargetAddr::Domain(h.to_string(), p))
                                                }
                                            }
                                            _ => None,
                                        }
                                    });

                                let session_id = req
                                    .headers()
                                    .get(crate::consts::HEADER_SESSION_ID)
                                    .and_then(|v| v.to_str().ok())
                                    .map(|s| s.to_string());

                                let pool_idle = req
                                    .headers()
                                    .get(crate::consts::HEADER_TUNNEL_POOL)
                                    .and_then(|v| v.to_str().ok())
                                    .map(|v| v == crate::consts::TUNNEL_POOL_IDLE)
                                    .unwrap_or(false);

                                let resp = Response::builder()
                                    .status(StatusCode::OK)
                                    .body(())
                                    .unwrap();

                                if let Err(e) = stream.send_response(resp).await {
                                    debug!("Failed to send H3 200 response: {:?}", e);
                                    return;
                                }

                                let metadata = StreamMetadata {
                                    mode,
                                    session_id,
                                    target,
                                    pool_idle,
                                };
                                let tunnel_stream = Box::new(H3TunnelStream::from_server(stream)) as Box<dyn TunnelStream>;

                                let _ = tx_clone.send((tunnel_stream, metadata)).await;
                            });
                        }
                        Ok(None) => break,
                        Err(e) => {
                            debug!("H3 accept error: {:?}", e);
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Processes raw QUIC bidirectional streams on the connection.
    async fn handle_raw_quic_streams(
        conn: quinn::Connection,
        auth_token: Arc<Option<String>>,
        tx_streams: mpsc::Sender<(Box<dyn TunnelStream>, StreamMetadata)>,
        cancel_token: CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                res = conn.accept_bi() => {
                    match res {
                        Ok((send, mut recv)) => {
                            let auth_clone = Arc::clone(&auth_token);
                            let tx_clone = tx_streams.clone();

                            tokio::spawn(async move {
                                // Read auth length
                                let mut len_buf = [0u8; 4];
                                if recv.read_exact(&mut len_buf).await.is_err() {
                                    return;
                                }
                                let auth_len = u32::from_be_bytes(len_buf) as usize;
                                if auth_len > 1024 {
                                    return;
                                }
                                let mut auth_buf = vec![0u8; auth_len];
                                if recv.read_exact(&mut auth_buf).await.is_err() {
                                    return;
                                }
                                if let Some(ref token) = *auth_clone {
                                    if !token.is_empty() && auth_buf != token.as_bytes() {
                                        warn!("Raw QUIC authentication failed");
                                        return;
                                    }
                                }

                                // Read metadata
                                if recv.read_exact(&mut len_buf).await.is_err() {
                                    return;
                                }
                                let meta_len = u32::from_be_bytes(len_buf) as usize;
                                if meta_len > 65536 {
                                    return;
                                }
                                let mut meta_buf = vec![0u8; meta_len];
                                if recv.read_exact(&mut meta_buf).await.is_err() {
                                    return;
                                }
                                let metadata: StreamMetadata = match rmp_serde::from_slice(&meta_buf) {
                                    Ok(m) => m,
                                    Err(_) => return,
                                };

                                let tunnel_stream = Box::new(QuinnTunnelStream::new(send, recv)) as Box<dyn TunnelStream>;
                                let _ = tx_clone.send((tunnel_stream, metadata)).await;
                            });
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    }
}

impl TransportListener for H3Listener {
    fn accept<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>> {
        Box::pin(async move {
            match self.rx_streams.recv().await {
                Some((stream, meta)) => Ok((stream, meta)),
                None => Err(TunnelError::SessionClosed),
            }
        })
    }
}

impl Drop for H3Listener {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}
