//! WebSocket server listener implementing `TransportListener`.
//! All comments and docstrings must be in English.

use fastwebsockets::{Role, upgrade::upgrade};
use http_body_util::Empty;
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;

use crate::common::net::SocketTuner;
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
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

        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok((conn, addr)) => {
                        let _ = SocketTuner::tune_tcp_stream(&conn, true, None);
                        (conn, addr)
                    }
                    Err(_) => break,
                };

                let tx = tx.clone();
                let token = Arc::clone(&auth_token);
                let path = Arc::clone(&expected_path);
                let acceptor = tls_acceptor.clone();

                tokio::spawn(async move {
                    // If TLS acceptor is configured (wss://), perform TLS handshake first
                    let io: TokioIo<Box<dyn TunnelStream>> = if let Some(tls) = acceptor {
                        match tls.accept(stream).await {
                            Ok(tls_stream) => {
                                TokioIo::new(Box::new(tls_stream) as Box<dyn TunnelStream>)
                            }
                            Err(e) => {
                                tracing::debug!("WSS server TLS handshake failed: {:?}", e);
                                return;
                            }
                        }
                    } else {
                        TokioIo::new(Box::new(stream) as Box<dyn TunnelStream>)
                    };

                    let service = service_fn(move |mut req: Request<Incoming>| {
                        let tx = tx.clone();
                        let token = Arc::clone(&token);
                        let path = Arc::clone(&path);

                        async move {
                            // Validate URI path if configured on listener
                            if let Some(ref p) = *path {
                                if req.uri().path() != p.as_str() {
                                    let resp = Response::builder()
                                        .status(StatusCode::NOT_FOUND)
                                        .body(Empty::<Bytes>::new())
                                        .unwrap();
                                    return Ok::<_, hyper::Error>(resp);
                                }
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

                            // Validate token if configured on listener
                            if let Some(ref expected) = *token {
                                if !expected.is_empty() {
                                    let matches = headers
                                        .auth_token
                                        .as_ref()
                                        .map(|t| t == expected)
                                        .unwrap_or(false);
                                    if !matches {
                                        let resp = Response::builder()
                                            .status(StatusCode::FORBIDDEN)
                                            .body(Empty::<Bytes>::new())
                                            .unwrap();
                                        return Ok::<_, hyper::Error>(resp);
                                    }
                                }
                            }

                            let metadata = headers.to_stream_metadata();

                            // Upgrade HTTP connection to WebSocket
                            match upgrade(&mut req) {
                                Ok((response, fut)) => {
                                    tokio::spawn(async move {
                                        if let Ok(ws) = fut.await {
                                            let stream =
                                                WsTunnelStream::new(ws.into_inner(), Role::Server);
                                            let _ = tx
                                                .send(Ok((
                                                    Box::new(stream) as Box<dyn TunnelStream>,
                                                    metadata,
                                                )))
                                                .await;
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

                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .with_upgrades()
                        .await;
                });
            }
        });

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
