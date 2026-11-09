// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Unix Domain Socket transport for reverse proxy (Caddy/Nginx) IPC and local tunnel endpoints.
//! All comments and docstrings must be in English.

use fastwebsockets::{Role, handshake, upgrade::upgrade};
use http_body_util::Empty;
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::sync::CancellationToken;

use crate::common::{CancelFutureExt, TimeoutOrCancel};
use crate::config::model::TunnelMode;
use crate::consts::{DEFAULT_HANDSHAKE_TIMEOUT, SCHEME_WS_UNIX, SCHEME_WSS_UNIX};
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, BufferedTlsStream, TunnelStream};
use crate::dialer::endpoint::EndpointContext;
use crate::dialer::tls::TlsConfigBuilder;
use crate::platform::{PlatformUdsListener, connect_uds, normalize_uds_path};
use crate::transport::ws::model::WsHandshakeHeaders;
use crate::transport::ws::stream::WsTunnelStream;
use crate::transport::{
    ACCEPTED_STREAM_QUEUE_SIZE, AcceptOutput, StreamMetadata, TargetAddr, TransportConnector,
    TransportDriver, TransportListener,
};
use reusable_future::ReusableFutureRef;

type AcceptedResult = Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>;

/// An upgrade future handed back by the HTTP service to its owning connection
/// task. It is never spawned without an owner.
type UpgradeWork = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>;

/// Hyper executor bridging Hyper futures onto Tokio runtime.
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

/// UDS transport driver.
pub struct UdsTransportDriver;

impl TransportDriver for UdsTransportDriver {
    fn schemes(&self) -> &[&'static str] {
        &[SCHEME_WS_UNIX, SCHEME_WSS_UNIX]
    }

    fn build_connector(
        &self,
        endpoint: &EndpointContext,
        auth_token: &str,
        mode: TunnelMode,
    ) -> Result<Box<dyn TransportConnector>, TunnelError> {
        Ok(Box::new(UdsConnector::new(
            endpoint.clone(),
            auth_token,
            mode,
        )?))
    }

    fn build_listener(
        &self,
        socket_path: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        tls: Option<&crate::config::model::TlsConfig>,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        self.build_listener_with_cancel(
            socket_path,
            auth_token,
            path,
            tls,
            CancellationToken::new(),
        )
    }

    fn build_listener_with_cancel(
        &self,
        socket_path: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        tls: Option<&crate::config::model::TlsConfig>,
        cancel: CancellationToken,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        let is_wss = socket_path.starts_with("wss+unix://");
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
            let server_rustls =
                TlsConfigBuilder::build_server_config(cert_str, key_str, alpn, min_v, max_v)?;
            Some(TlsAcceptor::from(Arc::new(server_rustls)))
        } else {
            None
        };

        let clean_path = normalize_uds_path(socket_path);
        let listener = UdsListener::bind_with_cancel(
            clean_path.to_string_lossy(),
            auth_token,
            path,
            tls_acceptor,
            cancel,
        )?;

        // Apply `?chmod=` for callers that build a listener directly from a URL.
        // The production server path parses chmod in `ServerListenSpec::parse()`
        // and applies it in `ServerRunner`, so use the same octal parser here.
        if let Some(q_idx) = socket_path.find('?') {
            for param in socket_path[q_idx + 1..].split('&') {
                if let Some(val) = param.strip_prefix("chmod=")
                    && let Some(mode) = crate::config::model::parse_socket_mode(val)
                {
                    let _ = crate::platform::set_socket_permissions(&clean_path, mode);
                }
            }
        }

        Ok(Box::new(listener))
    }
}

pub struct UdsListener {
    cancel_token: CancellationToken,
    incoming_rx: Option<Arc<tokio::sync::Mutex<mpsc::Receiver<AcceptedResult>>>>,
    server_task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for UdsListener {
    fn drop(&mut self) {
        // `shutdown()` is the awaited termination boundary. Drop is only an
        // abnormal-path safety net: signal cancellation, never abort a live
        // handle and never hide an unawaited task behind detach.
        self.cancel_token.cancel();
        if self.server_task.is_some() {
            tracing::warn!(
                "UdsListener dropped without shutdown(); accept loop cancelled but not awaited"
            );
        }
    }
}

impl UdsListener {
    /// Binds a listener with a private lifecycle token.
    pub fn bind(
        socket_path: impl Into<String>,
        auth_token: Option<impl Into<String>>,
        http_path: Option<impl Into<String>>,
        tls_acceptor: Option<TlsAcceptor>,
    ) -> Result<Self, TunnelError> {
        Self::bind_with_cancel(
            socket_path,
            auth_token,
            http_path,
            tls_acceptor,
            CancellationToken::new(),
        )
    }

    /// Binds a listener whose accept loop is a child of the root shutdown token.
    pub fn bind_with_cancel(
        socket_path: impl Into<String>,
        auth_token: Option<impl Into<String>>,
        http_path: Option<impl Into<String>>,
        tls_acceptor: Option<TlsAcceptor>,
        cancel: CancellationToken,
    ) -> Result<Self, TunnelError> {
        let socket_path_str = socket_path.into();
        let path = normalize_uds_path(&socket_path_str);
        let mut listener = PlatformUdsListener::bind(&path).map_err(TunnelError::Io)?;

        let (tx, rx) = mpsc::channel(ACCEPTED_STREAM_QUEUE_SIZE);
        let auth_token = Arc::new(auth_token.map(|t| t.into()));
        let expected_path = Arc::new(http_path.map(|p| p.into()));
        let cancel_token = cancel.child_token();
        let cancel_loop = cancel_token.clone();

        let task = tokio::spawn(async move {
            let mut conn_tasks = tokio::task::JoinSet::new();
            loop {
                let stream = tokio::select! {
                    _ = cancel_loop.cancelled() => break,
                    Some(_) = conn_tasks.join_next(), if !conn_tasks.is_empty() => continue,
                    res = listener.accept() => match res {
                        Ok((conn, _addr)) => conn,
                        Err(_) => break,
                    }
                };

                let tx = tx.clone();
                let token = Arc::clone(&auth_token);
                let path = Arc::clone(&expected_path);
                let acceptor = tls_acceptor.clone();
                let cancel_conn = cancel_loop.child_token();

                conn_tasks.spawn(async move {
                    // Termination contract for one accepted connection: the
                    // HTTP service returns immediately after an upgrade, so the
                    // upgrade future must be driven concurrently. It is handed
                    // back over this channel and owned by a JoinSet here; this
                    // connection task awaits every one before returning.
                    let (upgrade_tx, mut upgrade_rx) = mpsc::unbounded_channel::<UpgradeWork>();
                    let mut upgrade_tasks: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
                    let cancel_serve = cancel_conn.clone();

                    let mut serve = Box::pin(async move {
                        let io_stream: Box<dyn TunnelStream> = if let Some(tls) = acceptor {
                            match tokio::time::timeout(
                                DEFAULT_HANDSHAKE_TIMEOUT,
                                tls.accept(stream),
                            )
                            .await
                            {
                                Ok(Ok(tls_stream)) => Box::new(BufferedTlsStream::new(tls_stream)),
                                Ok(Err(e)) => {
                                    tracing::debug!("UDS WSS server TLS handshake failed: {:?}", e);
                                    return;
                                }
                                Err(_) => {
                                    tracing::debug!("UDS WSS server TLS handshake timed out");
                                    return;
                                }
                            }
                        } else {
                            stream
                        };

                        let io = TokioIo::new(io_stream);
                        let service_cancel = cancel_serve.clone();
                        let service = service_fn(move |mut req: Request<Incoming>| {
                            let tx = tx.clone();
                            let token = Arc::clone(&token);
                            let path = Arc::clone(&path);
                            let cancel_upgrade = service_cancel.clone();
                            let upgrade_tx = upgrade_tx.clone();

                            async move {
                                if let Some(ref p) = *path
                                    && req.uri().path() != p.as_str()
                                {
                                    return Ok::<_, hyper::Error>(
                                        Response::builder()
                                            .status(StatusCode::NOT_FOUND)
                                            .body(Empty::<Bytes>::new())
                                            .unwrap(),
                                    );
                                }

                                let headers =
                                    match WsHandshakeHeaders::from_http_headers(req.headers()) {
                                        Ok(h) => h,
                                        Err(_) => {
                                            return Ok(Response::builder()
                                                .status(StatusCode::UNAUTHORIZED)
                                                .body(Empty::<Bytes>::new())
                                                .unwrap());
                                        }
                                    };

                                if let Some(ref expected) = *token
                                    && !expected.is_empty()
                                {
                                    let matches = headers
                                        .auth_token
                                        .as_deref()
                                        .map(|t| {
                                            crate::common::constant_time_token_eq(
                                                t.as_bytes(),
                                                expected.as_bytes(),
                                            )
                                        })
                                        .unwrap_or(false);
                                    if !matches {
                                        return Ok(Response::builder()
                                            .status(StatusCode::FORBIDDEN)
                                            .body(Empty::<Bytes>::new())
                                            .unwrap());
                                    }
                                }

                                let metadata = headers.to_stream_metadata();

                                let (response, fut) = match upgrade(&mut req) {
                                    Ok(res) => res,
                                    Err(_) => {
                                        return Ok(Response::builder()
                                            .status(StatusCode::BAD_REQUEST)
                                            .body(Empty::<Bytes>::new())
                                            .unwrap());
                                    }
                                };

                                let work: UpgradeWork = Box::pin(async move {
                                    let res = fut
                                        .timeout_or_cancel(
                                            DEFAULT_HANDSHAKE_TIMEOUT,
                                            &cancel_upgrade,
                                        )
                                        .await;
                                    let item: AcceptedResult = match res {
                                        TimeoutOrCancel::Ok(Ok(ws)) => {
                                            let ws_stream =
                                                WsTunnelStream::new(ws.into_inner(), Role::Server);
                                            Ok((
                                                Box::new(ws_stream) as Box<dyn TunnelStream>,
                                                metadata,
                                            ))
                                        }
                                        TimeoutOrCancel::Ok(Err(e)) => {
                                            Err(TunnelError::Internal(e.to_string()))
                                        }
                                        TimeoutOrCancel::TimedOut => Err(TunnelError::Internal(
                                            "UDS WebSocket upgrade timed out".to_string(),
                                        )),
                                        TimeoutOrCancel::Cancelled => return,
                                    };
                                    // A full accepted-stream queue must never pin an
                                    // owned upgrade task during shutdown.
                                    let _ = tx.send(item).cancel_with(&cancel_upgrade).await;
                                });
                                let _ = upgrade_tx.send(work);

                                Ok(response)
                            }
                        });

                        let _ = tokio::time::timeout(
                            DEFAULT_HANDSHAKE_TIMEOUT,
                            hyper::server::conn::http1::Builder::new()
                                .serve_connection(io, service)
                                .with_upgrades(),
                        )
                        .await;
                    });

                    loop {
                        tokio::select! {
                            _ = cancel_conn.cancelled() => break,
                            _ = &mut serve => break,
                            Some(work) = upgrade_rx.recv() => { upgrade_tasks.spawn(work); },
                            Some(_) = upgrade_tasks.join_next(), if !upgrade_tasks.is_empty() => {},
                        }
                    }

                    // Dropping `serve` releases the last sender, so any future
                    // queued in the same poll is still visible before the drain.
                    drop(serve);
                    while let Ok(work) = upgrade_rx.try_recv() {
                        upgrade_tasks.spawn(work);
                    }
                    while upgrade_tasks.join_next().await.is_some() {}
                });
            }

            // Every connection task owns its upgrade futures, observes
            // cancellation and bounds each await, so awaiting the JoinSet is
            // a real termination boundary rather than a guessed timeout.
            while conn_tasks.join_next().await.is_some() {}
        });

        Ok(Self {
            cancel_token,
            incoming_rx: Some(Arc::new(tokio::sync::Mutex::new(rx))),
            server_task: Some(task),
        })
    }

    pub fn new(
        socket_path: impl Into<String>,
        auth_token: Option<impl Into<String>>,
        http_path: Option<impl Into<String>>,
        tls_acceptor: Option<TlsAcceptor>,
    ) -> Result<Self, TunnelError> {
        Self::bind(socket_path, auth_token, http_path, tls_acceptor)
    }
}

impl TransportListener for UdsListener {
    fn accept(&mut self, slot: &mut ReusableFutureRef<'static, AcceptOutput>) {
        if !slot.is_empty() {
            return;
        }

        let Some(rx_arc) = self.incoming_rx.as_ref().map(Arc::clone) else {
            slot.set(|| async move { Err(TunnelError::Shutdown) });
            return;
        };
        let cancel = self.cancel_token.clone();

        slot.set(move || async move {
            let mut guard = rx_arc.lock().await;
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(TunnelError::Shutdown),
                item = guard.recv() => item.unwrap_or(Err(TunnelError::Shutdown)),
            }
        });
    }

    fn shutdown<'a>(&'a mut self) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            // Cancel first, then close the accepted-stream queue so no sender
            // can stay parked, then await the accept loop and all of its owned
            // connection/upgrade tasks.
            self.cancel_token.cancel();
            self.incoming_rx.take();
            if let Some(task) = self.server_task.take() {
                let _ = task.await;
            }
        })
    }
}

pub struct UdsConnector {
    endpoint: EndpointContext,
    auth_token: Option<String>,
    mode: TunnelMode,
    tls_connector: Option<Arc<TlsConnector>>,
}

impl UdsConnector {
    pub fn new(
        endpoint: EndpointContext,
        auth_token: impl Into<String>,
        mode: TunnelMode,
    ) -> Result<Self, TunnelError> {
        let tok: String = auth_token.into();
        let auth_token = if tok.is_empty() { None } else { Some(tok) };
        let is_wss = endpoint.endpoint.scheme == "wss+unix";
        let tls_connector = if is_wss {
            let tls_cfg = endpoint.tls_config.clone().unwrap_or_default();
            let conn = TlsConfigBuilder::build_connector(&tls_cfg)?;
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
        let socket_path = normalize_uds_path(&self.endpoint.endpoint.path);
        let raw_stream = connect_uds(&socket_path).await.map_err(TunnelError::Io)?;

        let stream: Box<dyn TunnelStream> = if let Some(ref connector) = self.tls_connector {
            let sni = self
                .endpoint
                .tls_config
                .as_ref()
                .and_then(|t| t.server_name.as_deref())
                .unwrap_or("localhost");
            let server_name = ServerName::try_from(sni.to_string())
                .map_err(|e| TunnelError::Tls(e.to_string()))?;
            let tls_stream = connector
                .connect(server_name, raw_stream)
                .await
                .map_err(|e| TunnelError::Tls(e.to_string()))?;
            Box::new(BufferedTlsStream::new(tls_stream))
        } else {
            raw_stream
        };

        // Perform client HTTP upgrade over Unix socket
        let uri = if let Some(ref q) = self.endpoint.endpoint.query {
            let mut custom_path = None;
            for param in q.split('&') {
                if let Some(val) = param.strip_prefix("path=") {
                    custom_path = Some(if !val.starts_with('/') {
                        format!("/{}", val)
                    } else {
                        val.to_string()
                    });
                    break;
                }
            }
            custom_path.unwrap_or_else(|| "/".to_string())
        } else {
            "/".to_string()
        };

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
            .unwrap_or_else(|| "localhost".to_string());

        let mut req_builder = Request::builder()
            .method("GET")
            .uri(uri)
            .header("Host", host_header)
            .header("Upgrade", "websocket")
            .header("Connection", "Upgrade")
            .header(
                "Sec-WebSocket-Key",
                fastwebsockets::handshake::generate_key(),
            )
            .header("Sec-WebSocket-Version", "13");

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

        let req_builder = headers.apply_to_request_builder(req_builder);

        let req = req_builder
            .body(Empty::<Bytes>::new())
            .map_err(|e| TunnelError::Internal(e.to_string()))?;

        let (ws, _response) = handshake::client(&TokioSpawnExecutor, req, stream)
            .await
            .map_err(|e| {
                TunnelError::ControlProtocol(format!(
                    "UDS WebSocket client handshake failed: {}",
                    e
                ))
            })?;

        let tunnel_stream = WsTunnelStream::new(ws.into_inner(), Role::Client);
        Ok(Box::new(tunnel_stream) as Box<dyn TunnelStream>)
    }
}

impl TransportConnector for UdsConnector {
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
        if self.endpoint.endpoint.scheme == "wss+unix" {
            "wss+unix"
        } else {
            "ws+unix"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_uds_listener_connector_handshake() {
        let tmp = tempdir().unwrap();
        let sock_path = tmp.path().join("uds_transport.sock");
        let sock_str = sock_path.to_string_lossy().to_string();

        let mut listener =
            UdsListener::new(&sock_str, Some("test_token"), None::<String>, None).unwrap();

        let srv_task = tokio::spawn(async move {
            let (stream, meta) = listener.accept_one().await.unwrap();
            assert_eq!(meta.mode, TunnelMode::Forward);
            stream
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let app_cfg = crate::config::model::AppConfig::default();
        let ep_cfg =
            crate::config::model::EndpointConfig::Simple(format!("ws+unix://{}", sock_str));
        let ep_ctx = EndpointContext::build(&ep_cfg, &app_cfg).unwrap();

        let connector = UdsConnector::new(ep_ctx, "test_token", TunnelMode::Forward).unwrap();
        let target = TargetAddr::Domain("127.0.0.1".to_string(), 80);
        let _client_stream = connector.connect(&target, None).await.unwrap();

        let _srv_stream = srv_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_uds_listener_chmod_applied() {
        let tmp = tempdir().unwrap();
        let sock_path = tmp.path().join("uds_chmod.sock");
        let sock_str = sock_path.to_string_lossy().to_string();

        let driver = UdsTransportDriver;
        let url = format!("ws+unix://{}?chmod=0o660", sock_str);
        let listener = driver.build_listener(&url, None, None, None);
        assert!(listener.is_ok());

        assert!(sock_path.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let meta = std::fs::metadata(&sock_path).unwrap();
            assert_eq!(meta.permissions().mode() & 0o777, 0o660);
        }
    }
}
