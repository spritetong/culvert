// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! HTTP/3 and QUIC ingress transport listener implementation.
//! All comments and docstrings must be in English.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http::{Response, StatusCode};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::common::{CancelFutureExt, TimeoutOrCancel};
use crate::config::model::TunnelMode;
use crate::consts::{
    ALPN_H3, ALPN_QUIC, AUTH_BEARER_PREFIX, DEFAULT_H3_REQUEST_DRAIN_TIMEOUT,
    DEFAULT_HANDSHAKE_TIMEOUT, DEFAULT_QUIC_KEEP_ALIVE_INTERVAL,
    DEFAULT_QUIC_MAX_CONCURRENT_BIDI_STREAMS, DEFAULT_QUIC_MAX_CONCURRENT_UNI_STREAMS,
    DEFAULT_QUIC_MAX_IDLE_TIMEOUT, DEFAULT_QUIC_RECEIVE_WINDOW, DEFAULT_QUIC_SEND_WINDOW,
    DEFAULT_QUIC_STREAM_RECEIVE_WINDOW, HEADER_SESSION_ID, HEADER_TARGET_HOST, HEADER_TARGET_PATH,
    HEADER_TARGET_PORT, HEADER_TUNNEL_AUTH, HEADER_TUNNEL_MODE, HEADER_TUNNEL_POOL,
    HEADER_TUNNEL_TARGET, RAW_QUIC_MAX_AUTH_LEN, RAW_QUIC_MAX_META_LEN, TUNNEL_MODE_REVERSE,
    TUNNEL_MODE_REVERSE_CTRL, TUNNEL_MODE_REVERSE_DATA, TUNNEL_POOL_IDLE,
};
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::transport::h3::stream::{H3TunnelStream, QuinnTunnelStream};
use crate::transport::{ACCEPTED_STREAM_QUEUE_SIZE, StreamMetadata, TargetAddr, TransportListener};

/// Resolves a hostname listen address without pinning a Tokio worker.
fn resolve_listen_socket_addr(
    clean_addr: &str,
    listen_addr: &str,
) -> Result<SocketAddr, TunnelError> {
    use std::net::ToSocketAddrs;

    let resolve = || {
        clean_addr
            .to_socket_addrs()
            .map_err(|e| {
                TunnelError::Config(format!("Invalid listen address '{}': {}", listen_addr, e))
            })?
            .next()
            .ok_or_else(|| {
                TunnelError::Config(format!(
                    "Listen address '{}' resolved to no socket addresses",
                    listen_addr
                ))
            })
    };

    // `getaddrinfo` is a blocking libc call with no timeout. On a multi-thread
    // runtime hand the worker off to the blocking pool first.
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(resolve)
        }
        _ => resolve(),
    }
}

/// HTTP/3 and QUIC transport listener.
pub struct H3Listener {
    rx_streams: mpsc::Receiver<(Box<dyn TunnelStream>, StreamMetadata)>,
    cancel_token: CancellationToken,
    server_task: Option<tokio::task::JoinHandle<()>>,
}

impl H3Listener {
    /// Binds an `H3Listener` using an existing `rustls::ServerConfig`.
    pub fn bind(
        listen_addr: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        server_rustls_config: rustls::ServerConfig,
    ) -> Result<Self, TunnelError> {
        Self::bind_with_cancel(
            listen_addr,
            auth_token,
            path,
            server_rustls_config,
            CancellationToken::new(),
        )
    }

    /// Binds an `H3Listener` wired to the root shutdown token.
    pub fn bind_with_cancel(
        listen_addr: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        server_rustls_config: rustls::ServerConfig,
        cancel: CancellationToken,
    ) -> Result<Self, TunnelError> {
        let clean_addr = if let Some(idx) = listen_addr.find("://") {
            let without_scheme = &listen_addr[idx + 3..];
            without_scheme.split('/').next().unwrap_or(without_scheme)
        } else {
            listen_addr
        };

        let addr: SocketAddr = match clean_addr.parse() {
            Ok(a) => a,
            Err(_) => resolve_listen_socket_addr(clean_addr, listen_addr)?,
        };

        let is_quic_listener = listen_addr.starts_with("quic://");
        let mut rustls_server = server_rustls_config;
        if is_quic_listener {
            if !rustls_server.alpn_protocols.iter().any(|p| p == ALPN_QUIC) {
                rustls_server.alpn_protocols.push(ALPN_QUIC.to_vec());
            }
        } else {
            // For H3 or generic listen addresses, ensure h3 is prioritized at index 0
            if let Some(pos) = rustls_server
                .alpn_protocols
                .iter()
                .position(|p| p == ALPN_H3)
            {
                if pos != 0 {
                    let h3 = rustls_server.alpn_protocols.remove(pos);
                    rustls_server.alpn_protocols.insert(0, h3);
                }
            } else {
                rustls_server.alpn_protocols.insert(0, ALPN_H3.to_vec());
            }
            // Retain micro-tunnel-quic fallback so raw QUIC clients can still negotiate
            if !rustls_server.alpn_protocols.iter().any(|p| p == ALPN_QUIC) {
                rustls_server.alpn_protocols.push(ALPN_QUIC.to_vec());
            }
        }

        let quic_server_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(rustls_server)
            .map_err(|e| {
            TunnelError::Tls(format!("Failed to build QUIC server crypto: {:?}", e))
        })?;

        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server_crypto));
        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_idle_timeout(Some(DEFAULT_QUIC_MAX_IDLE_TIMEOUT.try_into().unwrap()));
        transport_config.keep_alive_interval(Some(DEFAULT_QUIC_KEEP_ALIVE_INTERVAL));
        transport_config.max_concurrent_bidi_streams(quinn::VarInt::from_u32(
            DEFAULT_QUIC_MAX_CONCURRENT_BIDI_STREAMS,
        ));
        transport_config.max_concurrent_uni_streams(quinn::VarInt::from_u32(
            DEFAULT_QUIC_MAX_CONCURRENT_UNI_STREAMS,
        ));
        // Per-stream window must stay small relative to the connection window:
        // otherwise a couple of unread streams pin the whole connection's
        // receive_window and head-of-line block every other stream (quinn docs
        // explicitly warn about this). 4MiB x 32 concurrent stalled streams
        // still fits inside the 128MiB connection window.
        transport_config
            .stream_receive_window(quinn::VarInt::from_u32(DEFAULT_QUIC_STREAM_RECEIVE_WINDOW));
        transport_config.receive_window(quinn::VarInt::from_u32(DEFAULT_QUIC_RECEIVE_WINDOW));
        transport_config.send_window(DEFAULT_QUIC_SEND_WINDOW);
        server_config.transport_config(Arc::new(transport_config));

        let quinn_endpoint =
            quinn::Endpoint::server(server_config, addr).map_err(TunnelError::Io)?;

        info!(addr = %addr, is_quic_listener, "Bound HTTP/3 & QUIC listener on UDP socket");

        let (tx_streams, rx_streams) = mpsc::channel(ACCEPTED_STREAM_QUEUE_SIZE);
        let cancel_token = cancel.child_token();

        let server_task = Self::spawn_listener_loop(
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
            server_task: Some(server_task),
        })
    }

    /// Spawns the main listener event loop accepting QUIC connections.
    /// Returns the owned handle; the listener awaits it in `shutdown()`.
    fn spawn_listener_loop(
        endpoint: quinn::Endpoint,
        auth_token: Option<String>,
        path: Option<String>,
        is_quic_listener: bool,
        tx_streams: mpsc::Sender<(Box<dyn TunnelStream>, StreamMetadata)>,
        cancel_token: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let auth_token = Arc::new(auth_token);
        let path = Arc::new(path);

        tokio::spawn(async move {
            let mut conn_tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    Some(_) = conn_tasks.join_next(), if !conn_tasks.is_empty() => continue,
                    incoming = endpoint.accept() => {
                        match incoming {
                            Some(incoming) => {
                                match incoming.accept() {
                                    Ok(mut connecting) => {
                                        let auth_clone = Arc::clone(&auth_token);
                                        let path_clone = Arc::clone(&path);
                                        let tx_clone = tx_streams.clone();
                                        let cancel_conn = cancel_token.clone();

                                        conn_tasks.spawn(async move {
                                            let cancel_for_handler = cancel_conn.clone();
                                            tokio::select! {
                                                _ = cancel_conn.cancelled() => {}
                                                _ = async {
                                                    let handshake = tokio::time::timeout(DEFAULT_HANDSHAKE_TIMEOUT, async {
                                                        let alpn = {
                                                            let handshake_res = connecting.handshake_data().await;
                                                            handshake_res
                                                                .ok()
                                                                .and_then(|h| h.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
                                                                .and_then(|h| h.protocol)
                                                        };
                                                        let conn = connecting.await.map_err(|e| format!("{:?}", e))?;
                                                        Ok::<_, String>((conn, alpn))
                                                    }).await;

                                                    match handshake {
                                                        Ok(Ok((conn, alpn))) => {
                                                            Self::handle_connection(
                                                                conn,
                                                                alpn,
                                                                is_quic_listener,
                                                                auth_clone,
                                                                path_clone,
                                                                tx_clone,
                                                                cancel_for_handler,
                                                            ).await;
                                                        }
                                                        Ok(Err(e)) => {
                                                            debug!("QUIC handshake error: {:?}", e);
                                                        }
                                                        Err(_) => {
                                                            warn!("QUIC incoming connection handshake timed out (10s)");
                                                        }
                                                    }
                                                } => {}
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
            // Each connection task observes the cancellation token and bounds
            // every await (handshake, request drain, connection close), so
            // awaiting the JoinSet is a real termination boundary.
            while conn_tasks.join_next().await.is_some() {}
        })
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
        let is_raw_quic = is_quic_listener || alpn.as_deref() == Some(ALPN_QUIC);
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
            .build::<_, Bytes>(h3_quinn::Connection::new(conn.clone()))
            .await
        {
            Ok(server) => server,
            Err(e) => {
                debug!("Failed to start H3 server session: {:?}", e);
                return;
            }
        };

        let mut request_tasks = tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                Some(_) = request_tasks.join_next(), if !request_tasks.is_empty() => continue,
                resolver_opt = h3_conn.accept() => {
                    match resolver_opt {
                        Ok(Some(resolver)) => {
                            let auth_clone = Arc::clone(&auth_token);
                            let path_clone = Arc::clone(&expected_path);
                            let tx_clone = tx_streams.clone();
                            let conn_clone = conn.clone();
                            let cancel_task = cancel_token.clone();

                            request_tasks.spawn(async move {
                                let (req, mut stream) = match resolver.resolve_request().await {
                                    Ok(pair) => pair,
                                    Err(e) => {
                                        debug!("Failed to resolve H3 request: {:?}", e);
                                        return;
                                    }
                                };

                                // Validate URI path
                                if let Some(ref p) = *path_clone
                                    && req.uri().path() != p.as_str() {
                                        let resp = Response::builder()
                                            .status(StatusCode::NOT_FOUND)
                                            .body(())
                                            .unwrap();
                                        let _ = stream.send_response(resp).await;
                                        return;
                                    }

                                // Validate authorization token if configured
                                if let Some(ref token) = *auth_clone
                                    && !token.is_empty() {
                                        let auth_header = req
                                            .headers()
                                            .get("authorization")
                                            .or_else(|| req.headers().get(HEADER_TUNNEL_AUTH))
                                            .and_then(|v| v.to_str().ok());

                                        let expected_bearer = format!("{}{}", AUTH_BEARER_PREFIX, token);
                                        let is_authed = auth_header
                                            .map(|h| {
                                                crate::common::constant_time_token_eq(h.as_bytes(), expected_bearer.as_bytes())
                                                    || crate::common::constant_time_token_eq(h.as_bytes(), token.as_bytes())
                                            })
                                            .unwrap_or(false);

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

                                let mode = match req
                                    .headers()
                                    .get(HEADER_TUNNEL_MODE)
                                    .and_then(|v| v.to_str().ok())
                                {
                                    Some(TUNNEL_MODE_REVERSE_CTRL)
                                    | Some(TUNNEL_MODE_REVERSE_DATA)
                                    | Some(TUNNEL_MODE_REVERSE) => TunnelMode::Reverse,
                                    _ => TunnelMode::Forward,
                                };

                                let target = req
                                    .headers()
                                    .get(HEADER_TUNNEL_TARGET)
                                    .and_then(|v| v.to_str().ok())
                                    .and_then(|s| crate::tunnel::forward::parse_target_addr(s).ok())
                                    .or_else(|| {
                                        req.headers()
                                            .get(HEADER_TARGET_PATH)
                                            .and_then(|v| v.to_str().ok())
                                            .map(|p| TargetAddr::Path(std::path::PathBuf::from(p)))
                                    })
                                    .or_else(|| {
                                        let host = req
                                            .headers()
                                            .get(HEADER_TARGET_HOST)
                                            .and_then(|v| v.to_str().ok());
                                        let port = req
                                            .headers()
                                            .get(HEADER_TARGET_PORT)
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
                                    .get(HEADER_SESSION_ID)
                                    .and_then(|v| v.to_str().ok())
                                    .map(|s| s.to_string());

                                let pool_idle = req
                                    .headers()
                                    .get(HEADER_TUNNEL_POOL)
                                    .and_then(|v| v.to_str().ok())
                                    .map(|v| v == TUNNEL_POOL_IDLE)
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
                                let tunnel_stream = Box::new(
                                    H3TunnelStream::from_server(stream)
                                        .with_connection(conn_clone),
                                ) as Box<dyn TunnelStream>;

                                let _ = tx_clone
                                    .send((tunnel_stream, metadata))
                                    .cancel_with(&cancel_task)
                                    .await;
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

        // Allow in-flight request setup tasks to finish. Abort is only the
        // deadline fallback; the handles are still awaited afterwards.
        let drain_fut = async { while request_tasks.join_next().await.is_some() {} };
        let drain_res = drain_fut
            .timeout_or_cancel(DEFAULT_H3_REQUEST_DRAIN_TIMEOUT, &cancel_token)
            .await;
        if !drain_res.is_ok() {
            request_tasks.abort_all();
        }
        while request_tasks.join_next().await.is_some() {}

        // Keep h3_conn alive until connection is closed by peer or cancelled,
        // preventing premature QUIC teardown while accepted streams are still relaying.
        let _ = conn.closed().cancel_with(&cancel_token).await;
    }

    /// Processes raw QUIC bidirectional streams on the connection.
    async fn handle_raw_quic_streams(
        conn: quinn::Connection,
        auth_token: Arc<Option<String>>,
        tx_streams: mpsc::Sender<(Box<dyn TunnelStream>, StreamMetadata)>,
        cancel_token: CancellationToken,
    ) {
        // Handshake futures must run concurrently with `accept_bi`, but that
        // is not a reason to detach them. They live in this owned JoinSet and
        // are awaited before the connection handler returns.
        let mut handshake_tasks: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                Some(_) = handshake_tasks.join_next(), if !handshake_tasks.is_empty() => continue,
                res = conn.accept_bi() => {
                    match res {
                        Ok((send, mut recv)) => {
                            let auth_clone = Arc::clone(&auth_token);
                            let tx_clone = tx_streams.clone();
                            let conn_clone = conn.clone();
                            let cancel_stream = cancel_token.clone();
                            let cancel_handshake = cancel_token.clone();

                            handshake_tasks.spawn(async move {
                                let handshake_fut = async {
                                    // Read auth length
                                    let mut len_buf = [0u8; 4];
                                    recv.read_exact(&mut len_buf).await.map_err(|_| ())?;
                                    let auth_len = u32::from_be_bytes(len_buf) as usize;
                                    if auth_len > RAW_QUIC_MAX_AUTH_LEN {
                                        return Err(());
                                    }
                                    let mut auth_buf = vec![0u8; auth_len];
                                    recv.read_exact(&mut auth_buf).await.map_err(|_| ())?;
                                    if let Some(ref token) = *auth_clone
                                        && !token.is_empty() && !crate::common::constant_time_token_eq(auth_buf.as_slice(), token.as_bytes()) {
                                            warn!("Raw QUIC authentication failed");
                                            return Err(());
                                        }

                                    // Read metadata
                                    recv.read_exact(&mut len_buf).await.map_err(|_| ())?;
                                    let meta_len = u32::from_be_bytes(len_buf) as usize;
                                    if meta_len > RAW_QUIC_MAX_META_LEN {
                                        return Err(());
                                    }
                                    let mut meta_buf = vec![0u8; meta_len];
                                    recv.read_exact(&mut meta_buf).await.map_err(|_| ())?;
                                    let metadata: StreamMetadata = match rmp_serde::from_slice(&meta_buf) {
                                        Ok(m) => m,
                                        Err(_) => return Err(()),
                                    };

                                    Ok((send, recv, metadata))
                                };

                                let res = handshake_fut
                                    .timeout_or_cancel(DEFAULT_HANDSHAKE_TIMEOUT, &cancel_handshake)
                                    .await;
                                match res {
                                    TimeoutOrCancel::Ok(Ok((send, recv, metadata))) => {
                                        let tunnel_stream = Box::new(QuinnTunnelStream::new(send, recv).with_connection(conn_clone)) as Box<dyn TunnelStream>;
                                        // The accepted-stream queue can be full during
                                        // shutdown; the owned handshake task must
                                        // never sit on an uncancellable send.
                                        let _ = tx_clone
                                            .send((tunnel_stream, metadata))
                                            .cancel_with(&cancel_stream)
                                            .await;
                                    }
                                    TimeoutOrCancel::Ok(Err(())) => {
                                        debug!("Raw QUIC handshake aborted due to protocol error");
                                    }
                                    TimeoutOrCancel::TimedOut => {
                                        warn!("Raw QUIC inbound handshake timed out (10s)");
                                    }
                                    TimeoutOrCancel::Cancelled => {}
                                }
                            });
                        }
                        Err(_) => break,
                    }
                }
            }
        }
        // Each handshake is bounded by a 10s timeout and observes
        // cancellation, so this await is a bounded termination boundary.
        while handshake_tasks.join_next().await.is_some() {}
    }
}

impl TransportListener for H3Listener {
    fn accept<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>> {
        Box::pin(async move {
            tokio::select! {
                _ = self.cancel_token.cancelled() => Err(TunnelError::Shutdown),
                item = self.rx_streams.recv() => match item {
                    Some((stream, meta)) => Ok((stream, meta)),
                    None => Err(TunnelError::SessionClosed),
                },
            }
        })
    }

    fn shutdown<'a>(&'a mut self) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            // Cancel the accept loop and all of its owned connection tasks,
            // then await the listener handle itself.
            self.cancel_token.cancel();
            if let Some(task) = self.server_task.take() {
                let _ = task.await;
            }
        })
    }
}

impl Drop for H3Listener {
    fn drop(&mut self) {
        // `shutdown()` is the awaited termination boundary. Drop is only an
        // abnormal-path safety net: signal cancellation, never abort a live
        // handle and never hide an unawaited task behind detach.
        self.cancel_token.cancel();
        if self.server_task.is_some() {
            tracing::warn!(
                "H3Listener dropped without shutdown(); accept loop cancelled but not awaited"
            );
        }
    }
}
