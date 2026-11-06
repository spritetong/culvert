// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
use tokio_util::sync::CancellationToken;

use crate::common::net::SocketTuner;
use crate::common::{CancelFutureExt, TimeoutOrCancel};
use crate::consts::DEFAULT_HANDSHAKE_TIMEOUT;
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, BufferedTlsStream, TunnelStream};
use crate::transport::ws::model::WsHandshakeHeaders;
use crate::transport::ws::stream::WsTunnelStream;
use crate::transport::{ACCEPTED_STREAM_QUEUE_SIZE, StreamMetadata, TransportListener};

/// Result of an accepted WebSocket connection on the listener.
type AcceptedResult = Result<(Box<dyn TunnelStream>, StreamMetadata), TunnelError>;

/// An upgrade future handed back by the HTTP service to its owning connection
/// task. It is never spawned without an owner.
type UpgradeWork = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>;

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
        // `shutdown()` is the awaited termination boundary. Drop is only an
        // abnormal-path safety net: signal cancellation, never abort a live
        // handle and never hide an unawaited task behind detach.
        self.cancel_token.cancel();
        if self.server_task.is_some() {
            tracing::warn!(
                "WsListener dropped without shutdown(); accept loop cancelled but not awaited"
            );
        }
    }
}

impl WsListener {
    /// Creates a new `WsListener` with a private lifecycle token.
    pub fn new(
        listen_addr: impl Into<String>,
        auth_token: Option<impl Into<String>>,
        path: Option<impl Into<String>>,
        tls_acceptor: Option<TlsAcceptor>,
    ) -> Self {
        Self::new_with_cancel(
            listen_addr,
            auth_token,
            path,
            tls_acceptor,
            CancellationToken::new(),
        )
    }

    /// Creates a listener whose accept loop is a child of the root shutdown token.
    pub fn new_with_cancel(
        listen_addr: impl Into<String>,
        auth_token: Option<impl Into<String>>,
        path: Option<impl Into<String>>,
        tls_acceptor: Option<TlsAcceptor>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            listen_addr: listen_addr.into(),
            auth_token: auth_token.map(|t| t.into()),
            path: path.map(|p| p.into()),
            tls_acceptor,
            incoming_rx: None,
            cancel_token: cancel.child_token(),
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
        let (tx, rx) = mpsc::channel(ACCEPTED_STREAM_QUEUE_SIZE);
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
                let cancel_conn = cancel_token.child_token();

                conn_tasks.spawn(async move {
                    // Termination contract for one accepted connection: hyper
                    // requires the upgrade future to be driven concurrently
                    // with `serve_connection`, but that does not mean the
                    // upgrade may be detached. The service hands the future
                    // back over this channel; this connection task owns it in
                    // a JoinSet and awaits every one before returning.
                    let (upgrade_tx, mut upgrade_rx) = mpsc::unbounded_channel::<UpgradeWork>();
                    let mut upgrade_tasks: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
                    let cancel_serve = cancel_conn.clone();

                    let mut serve = Box::pin(async move {
                        // If TLS acceptor is configured (wss://), perform TLS handshake with timeout
                        let io: TokioIo<Box<dyn TunnelStream>> = if let Some(tls) = acceptor {
                            match tokio::time::timeout(
                                DEFAULT_HANDSHAKE_TIMEOUT,
                                tls.accept(stream),
                            )
                            .await
                            {
                                Ok(Ok(tls_stream)) => {
                                    TokioIo::new(Box::new(BufferedTlsStream::new(tls_stream))
                                        as Box<dyn TunnelStream>)
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

                        let service_cancel = cancel_serve.clone();
                        let service = service_fn(move |mut req: Request<Incoming>| {
                            let tx = tx.clone();
                            let token = Arc::clone(&token);
                            let path = Arc::clone(&path);
                            let cancel_upgrade = service_cancel.clone();
                            let upgrade_tx = upgrade_tx.clone();

                            async move {
                                // Validate URI path if configured on listener
                                if let Some(ref p) = *path
                                    && req.uri().path() != p.as_str()
                                {
                                    let resp = Response::builder()
                                        .status(StatusCode::NOT_FOUND)
                                        .body(Empty::<Bytes>::new())
                                        .unwrap();
                                    return Ok::<_, hyper::Error>(resp);
                                }

                                // Validate custom handshake headers
                                let headers =
                                    match WsHandshakeHeaders::from_http_headers(req.headers()) {
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
                                        let work: UpgradeWork = Box::pin(async move {
                                            let res = fut
                                                .timeout_or_cancel(
                                                    DEFAULT_HANDSHAKE_TIMEOUT,
                                                    &cancel_upgrade,
                                                )
                                                .await;
                                            if let TimeoutOrCancel::Ok(Ok(ws)) = res {
                                                let stream = WsTunnelStream::new(
                                                    ws.into_inner(),
                                                    Role::Server,
                                                );
                                                let item = Ok((
                                                    Box::new(stream) as Box<dyn TunnelStream>,
                                                    metadata,
                                                ));
                                                // A full accepted-stream queue must never
                                                // pin an owned upgrade task during shutdown.
                                                let _ = tx
                                                    .send(item)
                                                    .cancel_with(&cancel_upgrade)
                                                    .await;
                                            }
                                        });
                                        let _ = upgrade_tx.send(work);
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
                tokio::select! {
                    _ = self.cancel_token.cancelled() => Err(TunnelError::Shutdown),
                    item = rx.recv() => item.unwrap_or(Err(TunnelError::Shutdown)),
                }
            } else {
                Err(TunnelError::Internal(
                    "WsListener not initialized".to_string(),
                ))
            }
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// A listener must not "solve" a slow connection by detaching it: with a
    /// client parked inside hyper's 10s handshake timeout, shutdown still has
    /// to terminate promptly and must leave no unawaited accept-loop handle.
    #[tokio::test]
    async fn shutdown_awaits_owned_tasks_with_a_live_connection() {
        let addr = format!("127.0.0.1:{}", free_port());
        let cancel = CancellationToken::new();
        let mut listener = WsListener::new_with_cancel(
            addr.as_str(),
            Some("token"),
            Some("/ws"),
            None,
            cancel.clone(),
        );
        listener.start_server().await.unwrap();

        let _client = tokio::net::TcpStream::connect(addr.as_str()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        cancel.cancel();
        let finished = tokio::time::timeout(Duration::from_secs(2), listener.shutdown()).await;
        assert!(
            finished.is_ok(),
            "shutdown must await owned tasks without hanging on the parked connection"
        );
        assert!(
            listener.server_task.is_none(),
            "the accept-loop handle must be awaited, never detached"
        );
    }
}
