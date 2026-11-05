// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Unix Domain Socket transport for reverse proxy (Caddy/Nginx) IPC.
//! All comments and docstrings must be in English.

use crate::config::model::TunnelMode;
use crate::core::error::TunnelError;
#[cfg(unix)]
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::dialer::endpoint::EndpointContext;
#[cfg(unix)]
use crate::transport::{StreamMetadata, TargetAddr};
use crate::transport::{TransportConnector, TransportDriver, TransportListener};

/// UDS transport driver.
pub struct UdsTransportDriver;

impl TransportDriver for UdsTransportDriver {
    fn schemes(&self) -> &[&'static str] {
        &["uds", "unix"]
    }

    fn build_connector(
        &self,
        endpoint: &EndpointContext,
        auth_token: &str,
        mode: TunnelMode,
    ) -> Result<Box<dyn TransportConnector>, TunnelError> {
        #[cfg(unix)]
        {
            Ok(Box::new(unix_impl::UdsConnector::new(
                endpoint.clone(),
                auth_token,
                mode,
            )))
        }
        #[cfg(not(unix))]
        {
            let _ = (endpoint, auth_token, mode);
            Err(TunnelError::UnsupportedScheme(
                "Unix Domain Sockets (uds:// or unix://) are only supported on Unix platforms (Linux/macOS)"
                    .to_string(),
            ))
        }
    }

    fn build_listener(
        &self,
        socket_path: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        _tls: Option<&crate::config::model::TlsConfig>,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        #[cfg(unix)]
        {
            let clean_path = if let Some(idx) = socket_path.find("://") {
                &socket_path[idx + 3..]
            } else {
                socket_path
            };
            Ok(Box::new(unix_impl::UdsListener::new(
                clean_path,
                auth_token,
                path,
            )))
        }
        #[cfg(not(unix))]
        {
            let _ = (socket_path, auth_token, path);
            Err(TunnelError::UnsupportedScheme(
                "Unix Domain Sockets (uds:// or unix://) are only supported on Unix platforms (Linux/macOS)"
                    .to_string(),
            ))
        }
    }
}

#[cfg(unix)]
mod unix_impl {
    use super::*;
    use fastwebsockets::{upgrade::upgrade, Role, handshake};
    use http_body_util::Empty;
    use hyper::body::{Bytes, Incoming};
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::future::Future;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::{UnixListener as TokioUnixListener, UnixStream as TokioUnixStream};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::transport::ws::model::WsHandshakeHeaders;
    use crate::transport::ws::stream::WsTunnelStream;

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

    pub struct UdsListener {
        socket_path: String,
        auth_token: Option<String>,
        path: Option<String>,
        incoming_rx: Option<mpsc::Receiver<AcceptedResult>>,
        cancel_token: CancellationToken,
        server_task: Option<tokio::task::JoinHandle<()>>,
    }

    impl Drop for UdsListener {
        fn drop(&mut self) {
            self.cancel_token.cancel();
            if let Some(task) = self.server_task.take() {
                task.abort();
            }
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }

    impl UdsListener {
        pub fn new(
            socket_path: impl Into<String>,
            auth_token: Option<impl Into<String>>,
            path: Option<impl Into<String>>,
        ) -> Self {
            Self {
                socket_path: socket_path.into(),
                auth_token: auth_token.map(|t| t.into()),
                path: path.map(|p| p.into()),
                incoming_rx: None,
                cancel_token: CancellationToken::new(),
                server_task: None,
            }
        }

        async fn ensure_started(&mut self) -> Result<(), TunnelError> {
            if self.incoming_rx.is_some() {
                return Ok(());
            }

            // Remove existing socket file if present
            if Path::new(&self.socket_path).exists() {
                let _ = std::fs::remove_file(&self.socket_path);
            }

            let listener = TokioUnixListener::bind(&self.socket_path)
                .map_err(TunnelError::Io)?;

            let (tx, rx) = mpsc::channel(128);
            self.incoming_rx = Some(rx);

            let auth_token = Arc::new(self.auth_token.clone());
            let expected_path = Arc::new(self.path.clone());
            let cancel_token = self.cancel_token.clone();

            let task = tokio::spawn(async move {
                let mut conn_tasks = tokio::task::JoinSet::new();
                loop {
                    let stream = tokio::select! {
                        _ = cancel_token.cancelled() => break,
                        Some(_) = conn_tasks.join_next(), if !conn_tasks.is_empty() => continue,
                        res = listener.accept() => match res {
                            Ok((conn, _addr)) => conn,
                            Err(_) => break,
                        }
                    };

                    let tx = tx.clone();
                    let token = Arc::clone(&auth_token);
                    let path = Arc::clone(&expected_path);
                    let cancel_conn = cancel_token.child_token();

                    conn_tasks.spawn(async move {
                        tokio::select! {
                            _ = cancel_conn.cancelled() => {}
                            _ = async {
                                let io = TokioIo::new(stream);
                                let service_cancel = cancel_conn.clone();
                                let service = service_fn(move |mut req: Request<Incoming>| {
                                    let tx = tx.clone();
                                    let token = Arc::clone(&token);
                                    let path = Arc::clone(&path);
                                    let cancel_upgrade = service_cancel.clone();

                                    async move {
                                        if let Some(ref p) = *path {
                                            if req.uri().path() != p.as_str() {
                                                return Ok::<_, hyper::Error>(
                                                    Response::builder()
                                                        .status(StatusCode::NOT_FOUND)
                                                        .body(Empty::<Bytes>::new())
                                                        .unwrap(),
                                                );
                                            }
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

                                        if let Some(ref expected) = *token {
                                            if !expected.is_empty() {
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
                while conn_tasks.join_next().await.is_some() {}
            });

            self.server_task = Some(task);
            Ok(())
        }
    }

    impl TransportListener for UdsListener {
        fn accept<'a>(
            &'a mut self,
        ) -> BoxFuture<'a, Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>> {
            Box::pin(async move {
                self.ensure_started().await?;
                if let Some(ref mut rx) = self.incoming_rx {
                    tokio::select! {
                        _ = self.cancel_token.cancelled() => Err(TunnelError::Shutdown),
                        item = rx.recv() => item.unwrap_or(Err(TunnelError::Shutdown)),
                    }
                } else {
                    Err(TunnelError::Shutdown)
                }
            })
        }
    }

    pub struct UdsConnector {
        endpoint: EndpointContext,
        auth_token: Option<String>,
        mode: TunnelMode,
    }

    impl UdsConnector {
        pub fn new(
            endpoint: EndpointContext,
            auth_token: impl Into<String>,
            mode: TunnelMode,
        ) -> Self {
            let tok: String = auth_token.into();
            let auth_token = if tok.is_empty() { None } else { Some(tok) };
            Self {
                endpoint,
                auth_token,
                mode,
            }
        }
    }

    impl TransportConnector for UdsConnector {
        fn connect<'a>(
            &'a self,
            target: &'a TargetAddr,
            session_id: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
            Box::pin(async move {
                let socket_path = self.endpoint.endpoint.path.as_str();
                let stream = TokioUnixStream::connect(socket_path)
                    .await
                    .map_err(TunnelError::Io)?;

                // Perform client HTTP upgrade over Unix socket
                let req_builder = Request::builder()
                    .method("GET")
                    .uri("/")
                    .header("Host", "localhost")
                    .header("Upgrade", "websocket")
                    .header("Connection", "Upgrade")
                    .header("Sec-WebSocket-Key", fastwebsockets::handshake::generate_key())
                    .header("Sec-WebSocket-Version", "13");

                let pool_idle = self.mode == TunnelMode::Reverse && session_id.is_none();
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
                        TunnelError::ControlProtocol(format!("UDS WebSocket client handshake failed: {}", e))
                    })?;

                let tunnel_stream = WsTunnelStream::new(ws.into_inner(), Role::Client);
                Ok(Box::new(tunnel_stream) as Box<dyn TunnelStream>)
            })
        }

        fn protocol_scheme(&self) -> &'static str {
            match self.endpoint.endpoint.scheme.as_str() {
                "unix" => "unix",
                _ => "uds",
            }
        }
    }
}
