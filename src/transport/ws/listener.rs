//! WebSocket server listener implementing `TransportListener`.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use fastwebsockets::{upgrade::upgrade, Role};
use http_body_util::Empty;
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

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
    auth_token: String,
    incoming_rx: Option<mpsc::Receiver<AcceptedResult>>,
}

impl WsListener {
    /// Creates a new `WsListener`.
    pub fn new(listen_addr: impl Into<String>, auth_token: impl Into<String>) -> Self {
        Self {
            listen_addr: listen_addr.into(),
            auth_token: auth_token.into(),
            incoming_rx: None,
        }
    }

    /// Spawns the background HTTP server accepting WebSocket upgrades.
    async fn start_server(&mut self) -> Result<(), TunnelError> {
        let listener = TcpListener::bind(&self.listen_addr).await?;
        let (tx, rx) = mpsc::channel(32);
        self.incoming_rx = Some(rx);

        let auth_token = Arc::new(self.auth_token.clone());

        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(_) => break,
                };

                let tx = tx.clone();
                let token = Arc::clone(&auth_token);

                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let service = service_fn(move |mut req: Request<Incoming>| {
                        let tx = tx.clone();
                        let token = Arc::clone(&token);

                        async move {
                            // Validate custom handshake headers
                            let headers = match WsHandshakeHeaders::from_http_headers(req.headers()) {
                                Ok(h) => h,
                                Err(_) => {
                                    let resp = Response::builder()
                                        .status(StatusCode::UNAUTHORIZED)
                                        .body(Empty::<Bytes>::new())
                                        .unwrap();
                                    return Ok::<_, hyper::Error>(resp);
                                }
                            };

                            if headers.auth_token != *token {
                                let resp = Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Empty::<Bytes>::new())
                                    .unwrap();
                                return Ok::<_, hyper::Error>(resp);
                            }

                            let metadata = headers.to_stream_metadata();

                            // Upgrade HTTP connection to WebSocket
                            match upgrade(&mut req) {
                                Ok((response, fut)) => {
                                    tokio::spawn(async move {
                                        if let Ok(ws) = fut.await {
                                            let stream = WsTunnelStream::new(ws.into_inner(), Role::Server);
                                            let _ = tx.send(Ok((Box::new(stream) as Box<dyn TunnelStream>, metadata))).await;
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
                Err(TunnelError::Internal("WsListener not initialized".to_string()))
            }
        })
    }
}
