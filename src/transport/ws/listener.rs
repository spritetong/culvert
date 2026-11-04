//! WebSocket server listener implementing `TransportListener`.
//! All comments and docstrings must be in English.

use fastwebsockets::{Role, upgrade::upgrade};
use http_body_util::Empty;
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

use crate::common::net::SocketTuner;
use crate::core::error::TunnelError;
use crate::core::stream::{BufferedTlsStream, BoxFuture, TunnelStream};
use crate::transport::ws::model::WsHandshakeHeaders;
use crate::transport::ws::stream::WsTunnelStream;
use crate::transport::{StreamMetadata, TransportListener};

/// Result of an accepted WebSocket connection on the listener.
type AcceptedResult = Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>;

/// WebSocket server listener.
pub struct WsListener {
    listen_addr: String,
    auth_token: Option<String>,
    path: Option<String>,
    tls_acceptor: Option<TlsAcceptor>,
    incoming_rx: Option<mpsc::Receiver<AcceptedResult>>,
    cancel_token: CancellationToken,
    server_task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for WsListener {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        if let Some(task) = self.server_task.take() {
            task.abort();
        }
    }
}

impl WsListener {
    /// Creates a new `WsListener`.
    pub fn new(
        listen_addr: impl Into<String>,
        auth_token: Option<impl Into<String>>,
        path: Option<impl Into<String>>,
        tls_acceptor: Option<TlsAcceptor>,
    ) -> Self {
        Self {
            listen_addr: listen_addr.into(),
            auth_token: auth_token.map(|t| t.into()),
            path: path.map(|p| p.into()),
            tls_acceptor,
            incoming_rx: None,
            cancel_token: CancellationToken::new(),
            server_task: None,
        }
    }

    /// Spawns the background HTTP server accepting WebSocket upgrades.
    async fn start_server(&mut self) -> Result<(), TunnelError> {
        let clean_addr = if let Some(idx) = self.listen_addr.find("://") {
            let without_scheme = &self.listen_addr[idx + 3..];
            without_scheme.split('/').next().unwrap_or(without_scheme)
        } else {
            &self.listen_addr
        };

        let listener = TcpListener::bind(clean_addr).await?;
        let (tx, rx) = mpsc::channel(32);
        self.incoming_rx = Some(rx);

        let auth_token = Arc::new(self.auth_token.clone());
        let expected_path = Arc::new(self.path.clone());
        let tls_acceptor = self.tls_acceptor.clone();
        let cancel_token = self.cancel_token.clone();

        let task = tokio::spawn(async move {
            let mut conn_tasks = tokio::task::JoinSet::new();
            loop {
                let stream = tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    Some(_) = conn_tasks.join_next(), if !conn_tasks.is_empty() => continue,
                    res = listener.accept() => match res {
                        Ok((conn, _addr)) => {
                            let _ = SocketTuner::tune_tcp_stream(&conn, true, None);
                            conn
                        }
                        Err(_) => break,
                    }
                };

                let tx = tx.clone();
                let token = Arc::clone(&auth_token);
                let path = Arc::clone(&expected_path);
                let acceptor = tls_acceptor.clone();
                let cancel_conn = cancel_token.clone();

                conn_tasks.spawn(async move {
                    tokio::select! {
                        _ = cancel_conn.cancelled() => {}
                        _ = async {
                            // If TLS acceptor is configured (wss://), perform TLS handshake with 10s timeout
                            let io: TokioIo<Box<dyn TunnelStream>> = if let Some(tls) = acceptor {
                                match tokio::time::timeout(Duration::from_secs(10), tls.accept(stream)).await {
                                    Ok(Ok(tls_stream)) => {
                                        TokioIo::new(Box::new(BufferedTlsStream::new(tls_stream)) as Box<dyn TunnelStream>)
                                    }
                                    Ok(Err(e)) => {
                                        tracing::debug!("WSS server TLS handshake failed: {:?}", e);
                                        return;
                                    }
                                    Err(_) => {
                                        tracing::warn!("WSS server TLS handshake timed out (10s)");
                                        return;
                                    }
                                }
                            } else {
                                TokioIo::new(Box::new(stream) as Box<dyn TunnelStream>)
                            };

                            let service_cancel = cancel_conn.clone();
                            let service = service_fn(move |mut req: Request<Incoming>| {
                                let tx = tx.clone();
                                let token = Arc::clone(&token);
                                let path = Arc::clone(&path);
                                let cancel_upgrade = service_cancel.clone();

                                async move {
                                    // Validate URI path if configured on listener
                                    if let Some(ref p) = *path
                                        && req.uri().path() != p.as_str() {
                                            let resp = Response::builder()
                                                .status(StatusCode::NOT_FOUND)
                                                .body(Empty::<Bytes>::new())
                                                .unwrap();
                                            return Ok::<_, hyper::Error>(resp);
                                        }

                                    // Validate custom handshake headers
                                    let headers = match WsHandshakeHeaders::from_http_headers(req.headers())
                                    {
                                        Ok(h) => h,
                                        Err(_) => {
                                            let resp = Response::builder()
                                                .status(StatusCode::UNAUTHORIZED)
                                                .body(Empty::<Bytes>::new())
                                                .unwrap();
                                            return Ok::<_, hyper::Error>(resp);
                                        }
                                    };

                                    // Validate token if configured on listener using constant-time equality
                                    if let Some(ref expected) = *token
                                        && !expected.is_empty() {
                                            let matches = headers
                                                .auth_token
                                                .as_deref()
                                                .map(|t| crate::common::constant_time_token_eq(t.as_bytes(), expected.as_bytes()))
                                                .unwrap_or(false);
                                            if !matches {
                                                let resp = Response::builder()
                                                    .status(StatusCode::FORBIDDEN)
                                                    .body(Empty::<Bytes>::new())
                                                    .unwrap();
                                                return Ok::<_, hyper::Error>(resp);
                                            }
                                        }

                                    let metadata = headers.to_stream_metadata();

                                    // Upgrade HTTP connection to WebSocket
                                    match upgrade(&mut req) {
                                        Ok((response, fut)) => {
                                            tokio::spawn(async move {
                                                tokio::select! {
                                                    _ = cancel_upgrade.cancelled() => {}
                                                    res = tokio::time::timeout(Duration::from_secs(10), fut) => {
                                                        if let Ok(Ok(ws)) = res {
                                                            let stream =
                                                                WsTunnelStream::new(ws.into_inner(), Role::Server);
                                                            let _ = tx
                                                                .send(Ok((
                                                                    Box::new(stream) as Box<dyn TunnelStream>,
                                                                    metadata,
                                                                )))
                                                                .await;
                                                        }
                                                    }
                                                }
                                            });
                                            Ok::<_, hyper::Error>(response)
                                        }
                                        Err(_) => {
                                            let resp = Response::builder()
                                                .status(StatusCode::BAD_REQUEST)
                                                .body(Empty::<Bytes>::new())
                                                .unwrap();
                                            Ok::<_, hyper::Error>(resp)
                                        }
                                    }
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

        self.server_task = Some(task);
        Ok(())
    }
}

impl TransportListener for WsListener {
    fn accept<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>> {
        Box::pin(async move {
            if self.incoming_rx.is_none() {
                self.start_server().await?;
            }

            if let Some(ref mut rx) = self.incoming_rx {
                rx.recv()
                    .await
                    .ok_or_else(|| TunnelError::Internal("WsListener channel closed".to_string()))?
            } else {
                Err(TunnelError::Internal(
                    "WsListener not initialized".to_string(),
                ))
            }
        })
    }
}
