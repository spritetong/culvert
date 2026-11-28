// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! HTTP/3 and QUIC transport connector implementation.
//! All comments and docstrings must be in English.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, StreamExt};
use http::{Request, StatusCode};
use parking_lot::RwLock;
use tokio::sync::Mutex;
use tracing::debug;

use super::{QuicSessionHandle, apply_quic_config, quic_endpoint_config};
use crate::config::model::TunnelMode;
use crate::consts::{
    ALPN_H3, ALPN_QUIC, AUTH_BEARER_PREFIX, DEFAULT_HANDSHAKE_TIMEOUT,
    DEFAULT_QUIC_KEEP_ALIVE_INTERVAL, DEFAULT_QUIC_MAX_CONCURRENT_BIDI_STREAMS,
    DEFAULT_QUIC_MAX_CONCURRENT_UNI_STREAMS, DEFAULT_QUIC_MAX_IDLE_TIMEOUT,
    DEFAULT_QUIC_RECEIVE_WINDOW, DEFAULT_QUIC_SEND_WINDOW, DEFAULT_QUIC_STREAM_RECEIVE_WINDOW,
    DEFAULT_TUNNEL_PATH, H3_FAILURE_SUPPRESSION_COOLDOWN, HEADER_SESSION_ID, HEADER_TUNNEL_AUTH,
    HEADER_TUNNEL_ID, HEADER_TUNNEL_MODE, HEADER_TUNNEL_POOL, HEADER_TUNNEL_PROTO,
    HEADER_TUNNEL_TARGET, SCHEME_QUIC, TUNNEL_MODE_FORWARD, TUNNEL_MODE_REVERSE_CTRL,
    TUNNEL_MODE_REVERSE_DATA, TUNNEL_POOL_IDLE, TUNNEL_PROTO_UDP,
};
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::dialer::endpoint::EndpointContext;
use crate::dialer::tls::TlsConfigBuilder;
use crate::io::addr::L4Protocol;
use crate::io::channel::IoChannel;
use crate::transport::h3::stream::{H3TunnelStream, QuinnTunnelStream};
use crate::transport::{TargetAddr, TransportConnector};

/// Active HTTP/3 shared session holding an anchor SendRequest, underlying QUIC connection, and driver health.
struct H3Session {
    id: usize,
    sender: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    handle: QuicSessionHandle,
    driver_alive: Arc<AtomicBool>,
}

impl H3Session {
    fn is_alive(&self) -> bool {
        self.driver_alive.load(Ordering::Acquire) && self.handle.is_alive()
    }
}

/// Active raw QUIC shared session holding the underlying QUIC connection.
struct RawQuicSession {
    id: usize,
    handle: QuicSessionHandle,
}

impl RawQuicSession {
    fn is_alive(&self) -> bool {
        self.handle.is_alive()
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
    #[cfg(test)]
    h3_request_timeout: std::time::Duration,
}

impl H3Connector {
    /// Creates a new `H3Connector` bound to the given endpoint context.
    pub fn new(
        endpoint: &EndpointContext,
        auth_token: &str,
        mode: TunnelMode,
    ) -> Result<Self, TunnelError> {
        let scheme = endpoint.endpoint.scheme.as_str();
        let is_quic_raw = scheme == SCHEME_QUIC;
        let quic = &endpoint.quic_config;

        let mut tls_config = endpoint.tls_config.clone().unwrap_or_default();
        if is_quic_raw {
            match tls_config.alpn.take() {
                Some(mut list) => {
                    // Client can override the default ALPN.
                    if list.is_empty() {
                        list.push(ALPN_QUIC.to_string());
                    }
                    tls_config.alpn = Some(list);
                }
                None => {
                    tls_config.alpn = Some(vec![ALPN_QUIC.to_string()]);
                }
            }
        } else {
            // For HTTP/3 endpoints, ensure "h3" is included in ALPN protocols
            match tls_config.alpn.take() {
                Some(mut list) => {
                    if !list.iter().any(|p| p == ALPN_H3) {
                        list.push(ALPN_H3.to_string());
                    }
                    tls_config.alpn = Some(list);
                }
                None => {
                    tls_config.alpn = Some(vec![ALPN_H3.to_string()]);
                }
            }
        }

        let rustls_client = TlsConfigBuilder::build_client_config(&tls_config)?;
        let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(rustls_client)
            .map_err(|e| {
                TunnelError::Tls(format!("Failed to build QUIC client crypto: {:?}", e))
            })?;

        let mut client_config = quinn::ClientConfig::new(Arc::new(quic_crypto));
        let mut transport_config = quinn::TransportConfig::default();
        let max_idle = endpoint
            .dialer_config
            .idle_timeout
            .unwrap_or(DEFAULT_QUIC_MAX_IDLE_TIMEOUT);
        let keep_alive = endpoint
            .dialer_config
            .heartbeat_interval
            .unwrap_or(DEFAULT_QUIC_KEEP_ALIVE_INTERVAL);
        transport_config.max_idle_timeout(Some(max_idle.try_into().unwrap()));
        transport_config.keep_alive_interval(Some(keep_alive));
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
        apply_quic_config(&mut transport_config, quic);
        client_config.transport_config(Arc::new(transport_config));

        let socket = crate::platform::create_dual_stack_udp_socket(0).map_err(TunnelError::Io)?;
        let runtime = quinn::default_runtime()
            .ok_or_else(|| TunnelError::Io(std::io::Error::other("No quinn runtime available")))?;
        let mut quinn_endpoint =
            quinn::Endpoint::new(quic_endpoint_config(quic)?, None, socket, runtime)
                .map_err(TunnelError::Io)?;
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
            #[cfg(test)]
            h3_request_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
        })
    }

    /// Dials one resolved address. Parallel attempts share the connect deadline.
    async fn dial_quic_addr(
        &self,
        resolved_addr: SocketAddr,
        sni: &str,
        connect_timeout: std::time::Duration,
    ) -> Result<quinn::Connection, TunnelError> {
        debug!(server = %resolved_addr, sni = %sni, "Dialing QUIC transport endpoint");
        let connecting = self
            .quinn_endpoint
            .connect(resolved_addr, sni)
            .map_err(|e| TunnelError::TlsHandshake(format!("QUIC connect error: {:?}", e)))?;

        tokio::time::timeout(connect_timeout, connecting)
            .await
            .map_err(|_| TunnelError::ConnectTimeout(connect_timeout))?
            .map_err(|e| TunnelError::TlsHandshake(format!("QUIC handshake error: {:?}", e)))
    }

    /// Dials all resolved addresses concurrently and returns the first connection.
    async fn dial_quic_connection(&self) -> Result<quinn::Connection, TunnelError> {
        let connect_timeout = self.endpoint.connect_timeout();
        let ips = tokio::time::timeout(
            connect_timeout,
            self.endpoint
                .dns_resolver
                .resolve(&self.endpoint.endpoint.host),
        )
        .await
        .map_err(|_| TunnelError::ConnectTimeout(connect_timeout))??;
        if ips.is_empty() {
            return Err(TunnelError::DnsNoAddresses(
                self.endpoint.endpoint.host.clone(),
                "any".to_string(),
            ));
        }
        // `resolve` already applied the configured DnsStrategy (family filter +
        // preference order). Do not reorder it here.

        let sni = self
            .endpoint
            .tls_config
            .as_ref()
            .and_then(|t| t.server_name.as_deref())
            .unwrap_or(self.endpoint.endpoint.host.as_str())
            .to_string();

        let mut attempts = FuturesUnordered::new();
        for ip in ips {
            attempts.push(Box::pin(self.dial_quic_addr(
                SocketAddr::new(ip, self.endpoint.endpoint.port),
                &sni,
                connect_timeout,
            )));
        }

        let mut last_err = None;
        while let Some(res) = attempts.next().await {
            match res {
                Ok(conn) => {
                    attempts.clear();
                    return Ok(conn);
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or(TunnelError::ConnectTimeout(connect_timeout)))
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

        // Fail fast if an establishment attempt failed within cooldown to avoid serial cascading timeouts
        if let Some((failed_at, ref err_msg)) = *self.last_h3_failure.read()
            && failed_at.elapsed() < H3_FAILURE_SUPPRESSION_COOLDOWN
        {
            return Err(TunnelError::ControlProtocol(format!(
                "H3 establishment failed recently: {}",
                err_msg
            )));
        }

        // If there is an existing dead session, close its QUIC connection explicitly
        if let Some(old_session) = self.cached_h3_session.write().take() {
            old_session.handle.mark_dead(b"replacing dead H3 session");
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
        let handle = QuicSessionHandle::new(conn.clone());

        let connect_timeout = self.endpoint.connect_timeout();
        let build_res = tokio::time::timeout(
            connect_timeout,
            h3::client::builder().build::<_, _, Bytes>(h3_quinn::Connection::new(conn.clone())),
        )
        .await;

        let (mut driver, sender) = match build_res {
            Ok(Ok(pair)) => pair,
            Ok(Err(e)) => {
                let err_str = format!("H3 handshake failed: {:?}", e);
                *self.last_h3_failure.write() = Some((std::time::Instant::now(), err_str.clone()));
                handle.mark_dead(b"H3 build failed");
                return Err(TunnelError::ControlProtocol(err_str));
            }
            Err(_) => {
                *self.last_h3_failure.write() =
                    Some((std::time::Instant::now(), "H3 build timeout".to_string()));
                handle.mark_dead(b"H3 build timeout");
                return Err(TunnelError::ConnectTimeout(connect_timeout));
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
            handle,
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
        let matches = guard
            .as_ref()
            .map(|current| current.id == session_id)
            .unwrap_or(false);
        if matches && let Some(session) = guard.take() {
            debug!(session_id, "Invalidating broken H3 session cache");
            session.handle.mark_dead(b"invalidated H3 session");
        }
    }

    #[inline]
    fn effective_h3_request_timeout(&self) -> std::time::Duration {
        #[cfg(test)]
        {
            self.h3_request_timeout
        }
        #[cfg(not(test))]
        {
            DEFAULT_HANDSHAKE_TIMEOUT
        }
    }

    /// Distinguishes connection-scoped HTTP/3 errors from per-stream failures so a
    /// single stalled/reset request never tears down healthy sibling streams.
    fn handle_h3_stream_error(&self, session: &H3Session, err: &h3::error::StreamError) {
        match err {
            h3::error::StreamError::ConnectionError { .. } => {
                self.invalidate_h3_session_if_same(session.id);
            }
            h3::error::StreamError::RemoteClosing { .. } => {
                // Peer sent GOAWAY: stop routing new requests to this slot without
                // force-killing streams that are still actively relaying.
                let mut guard = self.cached_h3_session.write();
                if guard.as_ref().is_some_and(|cur| cur.id == session.id) {
                    guard.take();
                }
            }
            _ if !session.is_alive() => {
                self.invalidate_h3_session_if_same(session.id);
            }
            _ => {}
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

        // Fail fast if raw QUIC dial failed within cooldown
        if let Some((failed_at, ref err_msg)) = *self.last_quic_failure.read()
            && failed_at.elapsed() < H3_FAILURE_SUPPRESSION_COOLDOWN
        {
            return Err(TunnelError::ControlProtocol(format!(
                "Raw QUIC establishment failed recently: {}",
                err_msg
            )));
        }

        if let Some(old_session) = self.cached_quic_session.write().take() {
            old_session
                .handle
                .mark_dead(b"replacing dead raw QUIC session");
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
            handle: QuicSessionHandle::new(conn),
        });

        *self.last_quic_failure.write() = None;
        *self.cached_quic_session.write() = Some(Arc::clone(&session));
        Ok(session)
    }

    /// Atomically clears the cached raw QUIC session only if it matches the specified session ID.
    fn invalidate_quic_session_if_same(&self, session_id: usize) {
        let mut guard = self.cached_quic_session.write();
        let matches = guard
            .as_ref()
            .map(|current| current.id == session_id)
            .unwrap_or(false);
        if matches && let Some(session) = guard.take() {
            debug!(session_id, "Invalidating broken raw QUIC session cache");
            session.handle.mark_dead(b"invalidated raw QUIC session");
        }
    }

    /// Connects via standard HTTP/3 request stream.
    async fn connect_h3(
        &self,
        target: &TargetAddr,
        session_id: Option<&str>,
        pool_idle: bool,
        protocol: L4Protocol,
        tunnel_id: Option<&str>,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let session = self.get_or_establish_h3_session().await?;
        let mut send_request = session.sender.clone();
        let session_id_val = session.id;

        let path = if self.endpoint.endpoint.path.is_empty() {
            DEFAULT_TUNNEL_PATH
        } else {
            &self.endpoint.endpoint.path
        };

        let authority = self
            .endpoint
            .http_headers
            .as_ref()
            .and_then(|headers| {
                headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("host"))
                    .map(|(_, v)| v.clone())
            })
            .unwrap_or_else(|| {
                format!(
                    "{}:{}",
                    self.endpoint.endpoint.host, self.endpoint.endpoint.port
                )
            });

        let mut req_builder = Request::builder()
            .method("CONNECT")
            .uri(path)
            .header("host", &authority);

        if let Some(ref custom_headers) = self.endpoint.http_headers {
            for (key, val) in custom_headers {
                if !key.eq_ignore_ascii_case("host") {
                    req_builder = req_builder.header(key, val);
                }
            }
        }

        if !self.auth_token.is_empty() {
            req_builder = req_builder
                .header(
                    "authorization",
                    format!("{}{}", AUTH_BEARER_PREFIX, self.auth_token),
                )
                .header(HEADER_TUNNEL_AUTH, &self.auth_token);
        }

        let mode_str = match self.mode {
            TunnelMode::Forward => TUNNEL_MODE_FORWARD,
            TunnelMode::Reverse => {
                if session_id.is_some() || pool_idle {
                    TUNNEL_MODE_REVERSE_DATA
                } else {
                    TUNNEL_MODE_REVERSE_CTRL
                }
            }
        };
        req_builder = req_builder.header(HEADER_TUNNEL_MODE, mode_str);

        if pool_idle {
            req_builder = req_builder.header(HEADER_TUNNEL_POOL, TUNNEL_POOL_IDLE);
        }

        if protocol == L4Protocol::Udp {
            req_builder = req_builder.header(HEADER_TUNNEL_PROTO, TUNNEL_PROTO_UDP);
        }

        req_builder = req_builder.header(HEADER_TUNNEL_TARGET, target.to_string());

        if let Some(sid) = session_id {
            req_builder = req_builder.header(HEADER_SESSION_ID, sid);
        }

        if let Some(tid) = tunnel_id {
            req_builder = req_builder.header(HEADER_TUNNEL_ID, tid);
        }

        let request = req_builder
            .body(())
            .map_err(|e| TunnelError::Config(format!("Invalid request headers: {:?}", e)))?;

        debug!(path = %path, target = %target, pool_idle, "Sending HTTP/3 CONNECT tunnel request");
        let h3_timeout = self.effective_h3_request_timeout();
        let mut stream =
            match tokio::time::timeout(h3_timeout, send_request.send_request(request)).await {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    self.handle_h3_stream_error(&session, &e);
                    return Err(TunnelError::ControlProtocol(format!(
                        "Failed to send H3 request: {:?}",
                        e
                    )));
                }
                Err(_) => {
                    if !session.is_alive() {
                        self.invalidate_h3_session_if_same(session_id_val);
                    }
                    return Err(TunnelError::ConnectTimeout(h3_timeout));
                }
            };

        let response = match tokio::time::timeout(h3_timeout, stream.recv_response()).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                self.handle_h3_stream_error(&session, &e);
                return Err(TunnelError::ControlProtocol(format!(
                    "Failed to receive H3 response: {:?}",
                    e
                )));
            }
            Err(_) => {
                if !session.is_alive() {
                    self.invalidate_h3_session_if_same(session_id_val);
                }
                return Err(TunnelError::ConnectTimeout(h3_timeout));
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
            Box::new(H3TunnelStream::from_client(stream).with_session(session.handle.clone()))
                as Box<dyn TunnelStream>,
        )
    }

    /// Connects via raw QUIC bidirectional stream.
    async fn connect_quic_raw(
        &self,
        target: &TargetAddr,
        session_id: Option<&str>,
        pool_idle: bool,
        protocol: L4Protocol,
        tunnel_id: Option<&str>,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let session = self.get_or_connect_quic_raw().await?;
        let session_id_val = session.id;

        let connect_timeout = self.endpoint.connect_timeout();
        let (mut send, recv) =
            match tokio::time::timeout(connect_timeout, session.handle.conn().open_bi()).await {
                Ok(Ok(pair)) => pair,
                Ok(Err(e)) => {
                    self.invalidate_quic_session_if_same(session_id_val);
                    return Err(TunnelError::Tls(format!(
                        "Failed to open QUIC bi-stream: {:?}",
                        e
                    )));
                }
                Err(_) => {
                    // `open_bi()` can stall when the peer's concurrent stream limit is
                    // temporarily exhausted; keep healthy sessions and sibling streams alive.
                    if !session.is_alive() {
                        self.invalidate_quic_session_if_same(session_id_val);
                    }
                    return Err(TunnelError::ConnectTimeout(connect_timeout));
                }
            };

        // Send handshake control header over the raw QUIC stream
        let handshake_meta = crate::transport::StreamMetadata {
            mode: self.mode,
            session_id: session_id.map(|s| s.to_string()),
            target: Some(target.clone()),
            pool_idle,
            protocol,
            tunnel_id: tunnel_id.map(|s| s.to_string()),
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
            connect_timeout,
            tokio::io::AsyncWriteExt::write_all(&mut send, &header),
        )
        .await;

        match write_res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if !session.is_alive() {
                    self.invalidate_quic_session_if_same(session_id_val);
                }
                return Err(TunnelError::Io(e));
            }
            Err(_) => {
                if !session.is_alive() {
                    self.invalidate_quic_session_if_same(session_id_val);
                }
                return Err(TunnelError::ConnectTimeout(connect_timeout));
            }
        }

        Ok(
            Box::new(QuinnTunnelStream::new(send, recv).with_session(session.handle.clone()))
                as Box<dyn TunnelStream>,
        )
    }
}

impl TransportConnector for H3Connector {
    fn connect<'a>(
        &'a self,
        target: &'a TargetAddr,
        session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.connect_with_protocol(target, session_id, L4Protocol::Tcp)
    }

    fn connect_pooled<'a>(
        &'a self,
        target: &'a TargetAddr,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.connect_pooled_with_protocol(target, L4Protocol::Tcp)
    }

    fn connect_with_protocol<'a>(
        &'a self,
        target: &'a TargetAddr,
        session_id: Option<&'a str>,
        protocol: L4Protocol,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        Box::pin(async move {
            let stream = if self.endpoint.endpoint.scheme == "quic" {
                self.connect_quic_raw(target, session_id, false, protocol, None)
                    .await?
            } else {
                self.connect_h3(target, session_id, false, protocol, None)
                    .await?
            };
            Ok(IoChannel::from_boxed_stream(stream))
        })
    }

    fn connect_pooled_with_protocol<'a>(
        &'a self,
        target: &'a TargetAddr,
        protocol: L4Protocol,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        Box::pin(async move {
            let stream = if self.endpoint.endpoint.scheme == "quic" {
                self.connect_quic_raw(target, None, true, protocol, None)
                    .await?
            } else {
                self.connect_h3(target, None, true, protocol, None).await?
            };
            Ok(IoChannel::from_boxed_stream(stream))
        })
    }

    fn connect_pooled_with_tunnel<'a>(
        &'a self,
        target: &'a TargetAddr,
        protocol: L4Protocol,
        tunnel_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        Box::pin(async move {
            let stream = if self.endpoint.endpoint.scheme == "quic" {
                self.connect_quic_raw(target, None, true, protocol, tunnel_id)
                    .await?
            } else {
                self.connect_h3(target, None, true, protocol, tunnel_id)
                    .await?
            };
            Ok(IoChannel::from_boxed_stream(stream))
        })
    }
}

#[cfg(test)]
pub(super) mod tests {
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

    pub(in crate::transport::h3) async fn create_test_quic_connection_pair()
    -> (quinn::Connection, quinn::Connection) {
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
        server_crypto.alpn_protocols = vec![ALPN_QUIC.as_bytes().to_vec()];
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
        client_crypto.alpn_protocols = vec![ALPN_QUIC.as_bytes().to_vec()];
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
            handle: QuicSessionHandle::new(client_conn),
        });

        *connector.cached_quic_session.write() = Some(Arc::clone(&dummy_quic_session));

        // Attempt to invalidate with an older/different session ID
        let wrong_id = dummy_quic_session.id + 999;
        connector.invalidate_quic_session_if_same(wrong_id);

        // Cached session MUST NOT be cleared (lost update prevented!)
        assert!(connector.cached_quic_session.read().is_some());
        assert!(dummy_quic_session.handle.is_alive());
        assert_eq!(
            connector.cached_quic_session.read().as_ref().unwrap().id,
            dummy_quic_session.id
        );

        // Invalidate with matching session ID
        let matching_id = dummy_quic_session.id;
        connector.invalidate_quic_session_if_same(matching_id);

        // Cached session is now cleared and its shared handle is marked dead
        assert!(connector.cached_quic_session.read().is_none());
        assert!(!dummy_quic_session.handle.is_alive());
        drop(server_conn);
    }

    fn build_test_quic_server_endpoint(
        alpn: &[u8],
        max_bidi_streams: Option<u32>,
    ) -> (quinn::Endpoint, SocketAddr) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_chain = vec![rustls::pki_types::CertificateDer::from(
            cert.cert.der().to_vec(),
        )];
        let private_key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()),
        );
        let mut server_crypto = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .unwrap();
        server_crypto.alpn_protocols = vec![alpn.to_vec()];
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto).unwrap(),
        ));
        if let Some(limit) = max_bidi_streams {
            let mut transport = quinn::TransportConfig::default();
            transport.max_concurrent_bidi_streams(quinn::VarInt::from_u32(limit));
            server_config.transport_config(Arc::new(transport));
        }
        let server_ep =
            quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = server_ep.local_addr().unwrap();
        (server_ep, addr)
    }

    #[tokio::test]
    async fn test_quic_raw_open_bi_timeout_keeps_healthy_session_alive() {
        use crate::config::model::{AppConfig, DialerConfig, EndpointConfig, TlsConfig};
        use std::time::Duration;
        use tokio::io::AsyncReadExt;
        use tokio::sync::Notify;

        let (server_ep, server_addr) =
            build_test_quic_server_endpoint(ALPN_QUIC.as_bytes(), Some(1));
        let release_first = Arc::new(Notify::new());
        let release_clone = Arc::clone(&release_first);
        let server_task = tokio::spawn(async move {
            let conn = server_ep.accept().await.unwrap().await.unwrap();
            // Stream 1: hold open until `release_clone` fires so stream credit remains exhausted.
            let (mut s_send1, mut s_recv1) = conn.accept_bi().await.unwrap();
            let mut buf = [0u8; 256];
            let n = s_recv1.read(&mut buf).await.unwrap().unwrap();
            assert!(n > 0);
            s_send1.write_all(b"pong1").await.unwrap();
            release_clone.notified().await;
            let _ = s_send1.finish();
            drop((s_send1, s_recv1));

            // Stream 2 (after stream 1 closes and credit refills): verify same connection still works.
            let (mut s_send2, mut s_recv2) = conn.accept_bi().await.unwrap();
            let n2 = s_recv2.read(&mut buf).await.unwrap().unwrap();
            assert!(n2 > 0);
            s_send2.write_all(b"pong2").await.unwrap();
            let _ = s_send2.finish();
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        let ep_cfg = EndpointConfig::Detailed {
            url: format!("quic://{server_addr}/tunnel"),
            dns: None,
            dialer: Some(DialerConfig {
                connect_timeout: Some(Duration::from_millis(80)),
                ..Default::default()
            }),
            tls: Some(TlsConfig {
                insecure: Some(true),
                server_name: Some("localhost".to_string()),
                ..Default::default()
            }),
            http_headers: None,
        };
        let ctx = EndpointContext::build(&ep_cfg, &AppConfig::default()).unwrap();
        let connector = H3Connector::new(&ctx, "tok", TunnelMode::Forward).unwrap();
        let target = TargetAddr::Domain("echo.local".to_string(), 80);

        // 1. Open stream 1, exhausting peer bidi stream quota (= 1).
        let mut s1 = connector
            .connect_quic_raw(&target, None, false, L4Protocol::Tcp, None)
            .await
            .unwrap();
        let session_id_1 = connector.cached_quic_session.read().as_ref().unwrap().id;
        let mut r1 = [0u8; 5];
        s1.read_exact(&mut r1).await.unwrap();
        assert_eq!(&r1, b"pong1");

        // 2. While quota is exhausted, open_bi() times out; request must fail with ConnectTimeout,
        //    but cached session MUST stay alive.
        let timed_out = connector
            .connect_quic_raw(&target, None, false, L4Protocol::Tcp, None)
            .await;
        assert!(
            matches!(timed_out, Err(TunnelError::ConnectTimeout(_))),
            "expected ConnectTimeout under exhausted stream credit"
        );
        {
            let guard = connector.cached_quic_session.read();
            let cached = guard.as_ref().expect("cached raw QUIC session must remain");
            assert_eq!(cached.id, session_id_1);
            assert!(cached.is_alive());
        }

        // 3. Release stream 1 to replenish quota; next request reuses the EXACT same session.
        release_first.notify_one();
        drop(s1);

        let mut s2 = connector
            .connect_quic_raw(&target, None, false, L4Protocol::Tcp, None)
            .await
            .unwrap();
        assert_eq!(
            connector.cached_quic_session.read().as_ref().unwrap().id,
            session_id_1
        );
        let mut r2 = [0u8; 5];
        s2.read_exact(&mut r2).await.unwrap();
        assert_eq!(&r2, b"pong2");

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_h3_connect_timeout_keeps_healthy_session_and_sibling_alive() {
        use crate::config::model::{AppConfig, EndpointConfig, TlsConfig};
        use bytes::Buf;
        use http::Response;
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::sync::Notify;

        let (server_ep, server_addr) = build_test_quic_server_endpoint(ALPN_H3.as_bytes(), None);
        let stop_slow = Arc::new(Notify::new());
        let stop_slow_srv = Arc::clone(&stop_slow);
        let server_task = tokio::spawn(async move {
            let conn = server_ep.accept().await.unwrap().await.unwrap();
            let mut h3_conn =
                h3::server::Connection::<_, Bytes>::new(h3_quinn::Connection::new(conn))
                    .await
                    .unwrap();
            let mut held = Vec::new();
            while let Ok(Some(resolver)) = h3_conn.accept().await {
                let stop_clone = Arc::clone(&stop_slow_srv);
                let h = tokio::spawn(async move {
                    let (req, mut stream) = resolver.resolve_request().await.unwrap();
                    let target_hdr = req
                        .headers()
                        .get(HEADER_TUNNEL_TARGET)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    if target_hdr.starts_with("slow.") {
                        // Withhold response so the client's recv_response() times out.
                        stop_clone.notified().await;
                        return;
                    }
                    stream.send_response(Response::new(())).await.unwrap();
                    while let Ok(Some(mut chunk)) = stream.recv_data().await {
                        let data = chunk.copy_to_bytes(chunk.remaining());
                        stream.send_data(data).await.unwrap();
                    }
                    let _ = stream.finish().await;
                });
                held.push(h);
            }
            for h in held {
                let _ = h.await;
            }
        });

        let ep_cfg = EndpointConfig::Detailed {
            url: format!("h3://{server_addr}/tunnel"),
            dns: None,
            dialer: None,
            tls: Some(TlsConfig {
                insecure: Some(true),
                server_name: Some("localhost".to_string()),
                ..Default::default()
            }),
            http_headers: None,
        };
        let ctx = EndpointContext::build(&ep_cfg, &AppConfig::default()).unwrap();
        let mut connector = H3Connector::new(&ctx, "tok", TunnelMode::Forward).unwrap();
        connector.h3_request_timeout = Duration::from_millis(80);

        let fast_target = TargetAddr::Domain("fast.local".to_string(), 80);
        let slow_target = TargetAddr::Domain("slow.local".to_string(), 80);

        // 1. Establish healthy H3 session with fast sibling #1.
        let mut fast_1 = connector
            .connect_h3(&fast_target, None, false, L4Protocol::Tcp, None)
            .await
            .unwrap();
        let session_id_1 = connector.cached_h3_session.read().as_ref().unwrap().id;

        // 2. Send a slow CONNECT request whose recv_response() times out.
        let slow_res = connector
            .connect_h3(&slow_target, None, false, L4Protocol::Tcp, None)
            .await;
        assert!(
            matches!(slow_res, Err(TunnelError::ConnectTimeout(_))),
            "expected slow CONNECT to time out"
        );

        // Session MUST stay cached & alive, and fast sibling #1 MUST continue relaying.
        {
            let guard = connector.cached_h3_session.read();
            let cached = guard.as_ref().expect("cached H3 session must remain");
            assert_eq!(cached.id, session_id_1);
            assert!(cached.is_alive());
        }

        fast_1.write_all(b"ping1").await.unwrap();
        fast_1.flush().await.unwrap();
        let mut echo1 = [0u8; 5];
        fast_1.read_exact(&mut echo1).await.unwrap();
        assert_eq!(&echo1, b"ping1");

        // 3. Another CONNECT request MUST reuse the same H3 session and work normally.
        let mut fast_2 = connector
            .connect_h3(&fast_target, None, false, L4Protocol::Tcp, None)
            .await
            .unwrap();
        assert_eq!(
            connector.cached_h3_session.read().as_ref().unwrap().id,
            session_id_1
        );
        fast_2.write_all(b"ping2").await.unwrap();
        fast_2.flush().await.unwrap();
        let mut echo2 = [0u8; 5];
        fast_2.read_exact(&mut echo2).await.unwrap();
        assert_eq!(&echo2, b"ping2");

        stop_slow.notify_waiters();
        drop((fast_1, fast_2));
        server_task.abort();
        let _ = server_task.await;
    }
}
