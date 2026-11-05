// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::sync::CancellationToken;

use crate::config::model::TunnelMode;
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, BufferedTlsStream, TunnelStream};
use crate::dialer::endpoint::EndpointContext;
use crate::dialer::tls::TlsConfigBuilder;
use crate::platform::{PlatformUdsListener, connect_uds, normalize_uds_path};
use crate::transport::ws::model::WsHandshakeHeaders;
use crate::transport::ws::stream::WsTunnelStream;
use crate::transport::{
    StreamMetadata, TargetAddr, TransportConnector, TransportDriver, TransportListener,
};

type AcceptedResult = Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>;

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
        &["ws+unix", "wss+unix"]
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
                .filter(|s| *s != "h3" && *s != "micro-tunnel-quic")
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
        let listener =
            UdsListener::bind(clean_path.to_string_lossy(), auth_token, path, tls_acceptor)?;

        // If chmod query parameter was supplied in socket_path URL, apply it immediately
        if let Some(q_idx) = socket_path.find('?') {
            for param in socket_path[q_idx + 1..].split('&') {
                if let Some(val) = param.strip_prefix("chmod=") {
                    let mode = if val.starts_with("0o") || val.starts_with("0O") {
                        u32::from_str_radix(&val[2..], 8).ok()
                    } else if val.starts_with('0') && val.len() > 1 {
                        u32::from_str_radix(val, 8).ok()
                    } else {
                        val.parse::<u32>().ok()
                    };
                    if let Some(m) = mode {
                        let _ = crate::platform::set_socket_permissions(&clean_path, m);
                    }
                }
            }
        }

        Ok(Box::new(listener))
    }
}

pub struct UdsListener {
    cancel_token: CancellationToken,
    incoming_rx: mpsc::Receiver<AcceptedResult>,
    server_task: tokio::task::JoinHandle<()>,
}

impl Drop for UdsListener {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.server_task.abort();
    }
}

impl UdsListener {
    pub fn bind(
        socket_path: impl Into<String>,
        auth_token: Option<impl Into<String>>,
        http_path: Option<impl Into<String>>,
        tls_acceptor: Option<TlsAcceptor>,
    ) -> Result<Self, TunnelError> {
        let socket_path_str = socket_path.into();
        let path = normalize_uds_path(&socket_path_str);
        let mut listener = PlatformUdsListener::bind(&path).map_err(TunnelError::Io)?;

        let (tx, rx) = mpsc::channel(128);
        let auth_token = Arc::new(auth_token.map(|t| t.into()));
        let expected_path = Arc::new(http_path.map(|p| p.into()));
        let cancel_token = CancellationToken::new();
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
                    tokio::select! {
                        _ = cancel_conn.cancelled() => {}
                        _ = async {
                            let io_stream: Box<dyn TunnelStream> = if let Some(tls) = acceptor {
                                match tokio::time::timeout(Duration::from_secs(10), tls.accept(stream)).await {
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
                            let service_cancel = cancel_conn.clone();
                            let service = service_fn(move |mut req: Request<Incoming>| {
                                let tx = tx.clone();
                                let token = Arc::clone(&token);
                                let path = Arc::clone(&path);
                                let cancel_upgrade = service_cancel.clone();

                                async move {
                                    if let Some(ref p) = *path
                                        && req.uri().path() != p.as_str() {
                                            return Ok::<_, hyper::Error>(
                                                Response::builder()
                                                    .status(StatusCode::NOT_FOUND)
                                                    .body(Empty::<Bytes>::new())
                                                    .unwrap(),
                                            );
                                    }

                                    let headers = match WsHandshakeHeaders::from_http_headers(req.headers()) {
                                        Ok(h) => h,
                                        Err(_) => {
                                            return Ok(Response::builder()
                                                .status(StatusCode::UNAUTHORIZED)
                                                .body(Empty::<Bytes>::new())
                                                .unwrap());
                                        }
                                    };

                                    if let Some(ref expected) = *token
                                        && !expected.is_empty() {
                                            let matches = headers
                                                .auth_token
                                                .as_deref()
                                                .map(|t| crate::common::constant_time_token_eq(t.as_bytes(), expected.as_bytes()))
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

                                    tokio::spawn(async move {
                                        tokio::select! {
                                            _ = cancel_upgrade.cancelled() => {}
                                            res = tokio::time::timeout(Duration::from_secs(10), fut) => match res {
                                                Ok(Ok(ws)) => {
                                                    let ws_stream = WsTunnelStream::new(ws.into_inner(), Role::Server);
                                                    let _ = tx.send(Ok((Box::new(ws_stream) as Box<dyn TunnelStream>, metadata))).await;
                                                }
                                                Ok(Err(e)) => {
                                                    let _ = tx.send(Err(TunnelError::Internal(e.to_string()))).await;
                                                }
                                                Err(_) => {
                                                    let _ = tx.send(Err(TunnelError::Internal("UDS WebSocket upgrade timed out".to_string()))).await;
                                                }
                                            }
                                        }
                                    });

                                    Ok(response)
                                }
                            });

                            let _ = tokio::time::timeout(
                                Duration::from_secs(10),
                                hyper::server::conn::http1::Builder::new()
                                    .serve_connection(io, service)
                                    .with_upgrades(),
                            )
                            .await;
                        } => {}
                    }
                });
            }
            conn_tasks.abort_all();
            while conn_tasks.join_next().await.is_some() {}
        });

        Ok(Self {
            cancel_token,
            incoming_rx: rx,
            server_task: task,
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
    fn accept<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>> {
        Box::pin(async move {
            tokio::select! {
                _ = self.cancel_token.cancelled() => Err(TunnelError::Shutdown),
                item = self.incoming_rx.recv() => item.unwrap_or(Err(TunnelError::Shutdown)),
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

        let req_builder = Request::builder()
            .method("GET")
            .uri(uri)
            .header("Host", "localhost")
            .header("Upgrade", "websocket")
            .header("Connection", "Upgrade")
            .header(
                "Sec-WebSocket-Key",
                fastwebsockets::handshake::generate_key(),
            )
            .header("Sec-WebSocket-Version", "13");

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
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_uds_listener_connector_handshake() {
        let tmp = tempdir().unwrap();
        let sock_path = tmp.path().join("uds_transport.sock");
        let sock_str = sock_path.to_string_lossy().to_string();

        let mut listener =
            UdsListener::new(&sock_str, Some("test_token"), None::<String>, None).unwrap();

        let srv_task = tokio::spawn(async move {
            let (stream, meta) = listener.accept().await.unwrap();
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
