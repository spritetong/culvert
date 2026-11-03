//! Ingress server daemon handling forward proxying and reverse tunnel pairing.
//! All comments and docstrings must be in English.

use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::common::net::SocketTuner;
use crate::common::task::TaskTracker;
use crate::config::model::{ServerConfig, TunnelMode};
use crate::consts::{ACTIVATE_STREAM_MAGIC, DEFAULT_SESSION_PAIR_TIMEOUT};
use crate::control::stream::{ControlMessage, FramedControlStream};
use crate::core::error::TunnelError;
use crate::core::relay::RelayEngine;
use crate::session::SessionManager;
use crate::transport::{TargetAddr, TransportRegistry};

static REVERSE_SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Ingress server runner that listens on an ingress endpoint (e.g. WSS/UDS) and dispatches tunnels.
pub struct ServerRunner {
    config: ServerConfig,
    transport_registry: Arc<TransportRegistry>,
    session_manager: Arc<SessionManager>,
}

impl ServerRunner {
    /// Creates a new `ServerRunner`.
    pub fn new(
        config: ServerConfig,
        transport_registry: Arc<TransportRegistry>,
        session_manager: Arc<SessionManager>,
    ) -> Self {
        Self {
            config,
            transport_registry,
            session_manager,
        }
    }

    /// Runs the server listener and stream dispatch loop until cancelled.
    pub async fn run(&self, cancel_token: CancellationToken) -> Result<(), TunnelError> {
        info!(listen = %self.config.listen, "Starting micro-tunnel server ingress listener");

        // Determine scheme (e.g. "uds" or default "ws")
        let scheme =
            if self.config.listen.starts_with('/') || self.config.listen.starts_with("uds://") {
                "uds"
            } else if self.config.listen.starts_with("h3://")
                || self.config.listen.starts_with("https://")
            {
                "h3"
            } else if self.config.listen.starts_with("quic://") {
                "quic"
            } else {
                "ws"
            };

        let mut listener = self.transport_registry.create_listener(
            scheme,
            &self.config.listen,
            &self.config.token,
        )?;

        let mut conn_tracker = TaskTracker::<()>::new(cancel_token.clone());

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    info!("Server ingress listener received cancellation signal");
                    break;
                }
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, metadata)) => {
                            let session_mgr = Arc::clone(&self.session_manager);
                            let default_target = self.config.default_target.clone();

                            conn_tracker.spawn_with_cancel(move |token| async move {
                                match metadata.mode {
                                    // 1. Forward mode: Dial remote target and relay
                                    TunnelMode::Forward => {
                                        let target_opt = metadata.target.or_else(|| {
                                            default_target.and_then(|dt| {
                                                if let Ok(sa) = dt.parse() {
                                                    Some(TargetAddr::Ip(sa))
                                                } else {
                                                    let parts: Vec<&str> = dt.rsplitn(2, ':').collect();
                                                    if parts.len() == 2 {
                                                        parts[0].parse().ok().map(|p| TargetAddr::Domain(parts[1].to_string(), p))
                                                    } else {
                                                        None
                                                    }
                                                }
                                            })
                                        });

                                        let target = match target_opt {
                                            Some(t) => t,
                                            None => {
                                                error!("Forward stream arrived without target address");
                                                return;
                                            }
                                        };

                                        let mut stream = stream;

                                        // If this stream was pre-warmed as an idle connection in client pool,
                                        // wait for the client activation signal before touching the target service.
                                        if metadata.pool_idle {
                                            let mut magic = [0u8; 1];
                                            tokio::select! {
                                                _ = token.cancelled() => return,
                                                res = stream.read_exact(&mut magic) => {
                                                    match res {
                                                        Ok(_) if magic[0] == ACTIVATE_STREAM_MAGIC => {
                                                            // Activated! Proceed to dial target.
                                                        }
                                                        Ok(_) => {
                                                            warn!("Received invalid activation magic from pool stream: {:02x}", magic[0]);
                                                            return;
                                                        }
                                                        Err(_) => {
                                                            // Stale or evicted stream closed by client pool without being used
                                                            return;
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        info!(target = %target, "Dialing forward target in local/internal network");
                                        let target_conn = match TcpStream::connect(target.to_string()).await {
                                            Ok(s) => {
                                                let _ = SocketTuner::tune_tcp_stream(&s, true, None);
                                                s
                                            }
                                            Err(e) => {
                                                error!(target = %target, "Failed to connect to forward target: {:?}", e);
                                                return;
                                            }
                                        };

                                        let _ = RelayEngine::relay_with_cancel(
                                            stream,
                                            Box::new(target_conn),
                                            token,
                                        ).await;
                                    }

                                    // 2. Reverse mode: Either Control channel or Data channel pairing
                                    TunnelMode::Reverse => {
                                        if let Some(session_id) = metadata.session_id {
                                            // Data stream arrival: fulfill waiting session
                                            if let Err(e) = session_mgr.fulfill(&session_id, stream) {
                                                warn!(session = %session_id, "Failed to fulfill reverse session: {:?}", e);
                                            }
                                        } else {
                                            // Control stream arrival: handle registration and listen on requested port
                                            Self::handle_reverse_control(stream, session_mgr, token).await;
                                        }
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!("Error accepting stream on server ingress: {:?}", e);
                        }
                    }
                }
            }
        }

        conn_tracker.shutdown_and_wait().await;
        Ok(())
    }

    /// Handles a reverse control channel: binds requested port on server and notifies client.
    async fn handle_reverse_control(
        stream: Box<dyn crate::core::stream::TunnelStream>,
        session_mgr: Arc<SessionManager>,
        cancel_token: CancellationToken,
    ) {
        let mut ctrl_stream = FramedControlStream::new(stream);

        // Await RegisterReverse
        let (listen_addr, target_addr) = match ctrl_stream.next().await {
            Some(Ok(ControlMessage::RegisterReverse {
                listen_addr,
                target_addr,
            })) => (listen_addr, target_addr),
            other => {
                warn!(
                    "Expected RegisterReverse on reverse control channel, got: {:?}",
                    other
                );
                return;
            }
        };

        info!(addr = %listen_addr, target = %target_addr, "Attempting to bind reverse listening port on server");

        let listener = match TcpListener::bind(&listen_addr).await {
            Ok(l) => {
                let _ = ctrl_stream
                    .send(ControlMessage::RegisterAck {
                        listen_addr: listen_addr.clone(),
                        success: true,
                        error_msg: None,
                    })
                    .await;
                l
            }
            Err(e) => {
                warn!(addr = %listen_addr, "Failed to bind reverse listener: {:?}", e);
                let _ = ctrl_stream
                    .send(ControlMessage::RegisterAck {
                        listen_addr: listen_addr.clone(),
                        success: false,
                        error_msg: Some(e.to_string()),
                    })
                    .await;
                return;
            }
        };

        info!(addr = %listen_addr, "Server bound reverse port successfully. Waiting for external traffic...");
        let mut local_tracker = TaskTracker::<()>::new(cancel_token.clone());

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((visitor_stream, visitor_addr)) => {
                            let _ = SocketTuner::tune_tcp_stream(&visitor_stream, true, None);
                            let session_id = format!(
                                "sess_{}_{}",
                                std::process::id(),
                                REVERSE_SESSION_COUNTER.fetch_add(1, Ordering::Relaxed)
                            );

                            info!(
                                session = %session_id,
                                visitor = %visitor_addr,
                                "External visitor connected to reverse port. Notifying client..."
                            );

                            // Register in session manager before notifying client
                            let pair_rx = session_mgr.register(session_id.clone());

                            // Notify client
                            let notify_msg = ControlMessage::NewConnection {
                                session_id: session_id.clone(),
                                client_addr: visitor_addr.to_string(),
                            };

                            if let Err(e) = ctrl_stream.send(notify_msg).await {
                                warn!(session = %session_id, "Failed to notify client over control channel: {:?}", e);
                                session_mgr.remove(&session_id);
                                continue;
                            }

                            // Spawn pairing relay task
                            let session_mgr_clone = Arc::clone(&session_mgr);
                            let session_id_clone = session_id.clone();

                            local_tracker.spawn_with_cancel(move |token| async move {
                                match timeout(DEFAULT_SESSION_PAIR_TIMEOUT, pair_rx).await {
                                    Ok(Ok(data_stream)) => {
                                        info!(session = %session_id_clone, "Reverse data stream paired successfully. Starting relay.");
                                        let _ = RelayEngine::relay_with_cancel(
                                            Box::new(visitor_stream),
                                            data_stream,
                                            token,
                                        ).await;
                                    }
                                    _ => {
                                        warn!(session = %session_id_clone, "Timed out waiting for reverse data stream pairing");
                                        session_mgr_clone.remove(&session_id_clone);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            warn!(addr = %listen_addr, "Error accepting visitor on reverse port: {:?}", e);
                            break;
                        }
                    }
                }
                msg_opt = ctrl_stream.next() => {
                    match msg_opt {
                        Some(Ok(ControlMessage::Ping)) => {
                            let _ = ctrl_stream.send(ControlMessage::Pong).await;
                        }
                        Some(Ok(ControlMessage::Pong)) => {}
                        _ => {
                            info!(addr = %listen_addr, "Reverse control channel disconnected by client");
                            break;
                        }
                    }
                }
            }
        }

        local_tracker.shutdown_and_wait().await;
        info!(addr = %listen_addr, "Reverse listener closed.");
    }
}
