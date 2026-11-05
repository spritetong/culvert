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
                "Unix Domain Sockets (uds://) are only supported on Unix platforms (Linux/macOS)"
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
            Ok(Box::new(unix_impl::UdsListener::new(
                socket_path,
                auth_token,
                path,
            )))
        }
        #[cfg(not(unix))]
        {
            let _ = (socket_path, auth_token, path);
            Err(TunnelError::UnsupportedScheme(
                "Unix Domain Sockets (uds://) are only supported on Unix platforms (Linux/macOS)"
                    .to_string(),
            ))
        }
    }
}

#[cfg(unix)]
mod unix_impl {
    use super::*;
    use fastwebsockets::{upgrade::upgrade, Role};
    use http_body_util::Empty;
    use hyper::body::{Bytes, Incoming};
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::{UnixListener as TokioUnixListener, UnixStream as TokioUnixStream};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::transport::ws::model::WsHandshakeHeaders;
    use crate::transport::ws::stream::WsTunnelStream;

    type AcceptedResult = Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>;

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
                                let io = TokioIo::new(Box::new(stream) as Box<dyn TunnelStream>);
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

                                        let headers = WsHandshakeHeaders::from_header_map(req.headers());
                                        if let Some(ref expected_tok) = *token {
                                            let client_tok = headers.auth_token.as_deref().unwrap_or("");
                                            if !crate::common::auth::constant_time_token_eq(client_tok, expected_tok) {
                                                return Ok(Response::builder()
                                                    .status(StatusCode::UNAUTHORIZED)
                                                    .body(Empty::<Bytes>::new())
                                                    .unwrap());
                                            }
                                        }

                                        let mode = headers.mode;
                                        let target = headers.target.clone();
                                        let session_id = headers.session_id.clone();
                                        let pool_idle = headers.pool_idle;

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
                                                res = fut => match res {
                                                    Ok(ws) => {
                                                        let ws_stream = WsTunnelStream::new(ws, Role::Server);
                                                        let meta = StreamMetadata {
                                                            mode,
                                                            session_id,
                                                            target,
                                                            pool_idle,
                                                        };
                                                        let _ = tx.send(Ok((Box::new(ws_stream), meta))).await;
                                                    }
                                                    Err(e) => {
                                                        let _ = tx.send(Err(TunnelError::Internal(e.to_string()))).await;
                                                    }
                                                }
                                            }
                                        });

                                        Ok(response)
                                    }
                                });

                                let _ = hyper::server::conn::http1::Builder::new()
                                    .serve_connection(io, service)
                                    .with_upgrades()
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
                let socket_path = self.endpoint.endpoint.host.as_str();
                let stream = TokioUnixStream::connect(socket_path)
                    .await
                    .map_err(TunnelError::Io)?;

                // Perform client HTTP upgrade over Unix socket
                let req_builder = Request::builder()
                    .method("GET")
                    .uri(self.endpoint.endpoint.path.as_deref().unwrap_or("/"))
                    .header("Host", "localhost")
                    .header("Upgrade", "websocket")
                    .header("Connection", "Upgrade")
                    .header("Sec-WebSocket-Key", fastwebsockets::handshake::generate_key())
                    .header("Sec-WebSocket-Version", "13");

                let mut req_builder = match self.mode {
                    TunnelMode::Forward => req_builder
                        .header("X-Tunnel-Mode", "forward")
                        .header("X-Tunnel-Target", target.to_string()),
                    TunnelMode::Reverse => {
                        let b = req_builder.header("X-Tunnel-Mode", "reverse");
                        if let Some(sid) = session_id {
                            b.header("X-Tunnel-Session-Id", sid)
                        } else {
                            b
                        }
                    }
                };

                if let Some(ref tok) = self.auth_token {
                    req_builder = req_builder.header("X-Tunnel-Auth", tok);
                }

                let req = req_builder
                    .body(Empty::<Bytes>::new())
                    .map_err(|e| TunnelError::Internal(e.to_string()))?;

                let io = TokioIo::new(Box::new(stream) as Box<dyn TunnelStream>);
                let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
                    .await
                    .map_err(|e| TunnelError::Internal(e.to_string()))?;

                tokio::spawn(async move {
                    let _ = conn.with_upgrades().await;
                });

                let res = sender
                    .send_request(req)
                    .await
                    .map_err(|e| TunnelError::Internal(e.to_string()))?;

                if res.status() != StatusCode::SWITCHING_PROTOCOLS {
                    return Err(TunnelError::Internal(format!(
                        "UDS WebSocket handshake failed with status {}",
                        res.status()
                    )));
                }

                let upgraded = hyper::upgrade::on(res)
                    .await
                    .map_err(|e| TunnelError::Internal(e.to_string()))?;

                let ws = fastwebsockets::WebSocket::after_handshake(
                    TokioIo::new(upgraded),
                    Role::Client,
                );

                Ok(Box::new(WsTunnelStream::new(ws, Role::Client)) as Box<dyn TunnelStream>)
            })
        }

        fn protocol_scheme(&self) -> &'static str {
            "uds"
        }
    }
}
