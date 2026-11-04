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
use crate::config::model::{ServerConfig, TlsConfig, TunnelMode};
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
    global_tls: Option<TlsConfig>,
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
        Self::with_global_tls(config, None, transport_registry, session_manager)
    }

    /// Creates a new `ServerRunner` with inherited global TLS configuration.
    pub fn with_global_tls(
        config: ServerConfig,
        global_tls: Option<TlsConfig>,
        transport_registry: Arc<TransportRegistry>,
        session_manager: Arc<SessionManager>,
    ) -> Self {
        Self {
            config,
            global_tls,
            transport_registry,
            session_manager,
        }
    }

    /// Runs the server listener and stream dispatch loop until cancelled.
    pub async fn run(&self, cancel_token: CancellationToken) -> Result<(), TunnelError> {
        let specs = self.config.parse_specs()?;
        if specs.is_empty() {
            info!("No server listen endpoints configured, skipping server runner");
            return Ok(());
        }

        info!(
            listeners = ?self.config.listen,
            "Starting micro-tunnel server ingress listeners"
        );

        let mut tracker = TaskTracker::<Result<(), TunnelError>>::new(cancel_token.clone());

        for spec in specs {
            let spec_clone = spec.clone();
            let registry = Arc::clone(&self.transport_registry);
            let session_mgr = Arc::clone(&self.session_manager);
            let default_target = self.config.default_target.clone();
            let token_opt = self.config.token.clone();
            let server_tls = self.config.effective_tls(self.global_tls.as_ref());

            tracker.spawn_with_cancel(move |token| async move {
                let mut listener = registry.create_listener(
                    &spec_clone.scheme,
                    &spec_clone.bind_addr,
                    token_opt.as_deref(),
                    spec_clone.path.as_deref(),
                    server_tls.as_ref(),
                )?;

                info!(
                    scheme = %spec_clone.scheme,
                    bind = %spec_clone.bind_addr,
                    path = ?spec_clone.path,
                    "Server ingress listener running"
                );

                let mut conn_tracker = TaskTracker::<()>::new(token.clone());

                loop {
                    tokio::select! {
                        _ = token.cancelled() => break,
                        Some(_) = conn_tracker.join_next(), if !conn_tracker.is_empty() => {}
                        accept_res = listener.accept() => {
                            match accept_res {
                                Ok((stream, metadata)) => {
                                    let session_mgr = Arc::clone(&session_mgr);
                                    let default_target = default_target.clone();

                                    conn_tracker.spawn_with_cancel(move |conn_token| async move {
                                        match metadata.mode {
                                            // 1. Forward mode: Dial remote target and relay
                                            TunnelMode::Forward => {
                                                let mut stream = stream;
                                                let mut dynamic_target: Option<TargetAddr> = None;

                                                // If this stream was pre-warmed as an idle connection in client pool,
                                                // wait for the client activation signal before touching the target service.
                                                if metadata.pool_idle {
                                                    let mut magic = [0u8; 1];
                                                    tokio::select! {
                                                        _ = conn_token.cancelled() => return,
                                                        res = stream.read_exact(&mut magic) => {
                                                            match res {
                                                                Ok(_) if magic[0] == ACTIVATE_STREAM_MAGIC => {
                                                                    // Activated! Check if dynamic target address is appended: [len: u16, target_str] with 10s timeout
                                                                    let mut len_bytes = [0u8; 2];
                                                                    let read_len_res = tokio::select! {
                                                                        _ = conn_token.cancelled() => return,
                                                                        res = tokio::time::timeout(std::time::Duration::from_secs(10), stream.read_exact(&mut len_bytes)) => match res {
                                                                            Ok(r) => r,
                                                                            Err(_) => {
                                                                                warn!("Timed out reading activation target length (10s)");
                                                                                return;
                                                                            }
                                                                        },
                                                                    };
                                                                    if let Err(e) = read_len_res {
                                                                        warn!("Failed to read activation target length: {:?}", e);
                                                                        return;
                                                                    }
                                                                    let target_len = u16::from_be_bytes(len_bytes) as usize;
                                                                    if target_len >= 512 {
                                                                        warn!("Activation target length too large: {}", target_len);
                                                                        return;
                                                                    }
                                                                    if target_len > 0 {
                                                                        let mut target_bytes = vec![0u8; target_len];
                                                                        let read_target_res = tokio::select! {
                                                                            _ = conn_token.cancelled() => return,
                                                                            res = tokio::time::timeout(std::time::Duration::from_secs(10), stream.read_exact(&mut target_bytes)) => match res {
                                                                                Ok(r) => r,
                                                                                Err(_) => {
                                                                                    warn!("Timed out reading activation target payload (10s)");
                                                                                    return;
                                                                                }
                                                                            },
                                                                        };
                                                                        if let Err(e) = read_target_res {
                                                                            warn!("Failed to read activation target payload: {:?}", e);
                                                                            return;
                                                                        }
                                                                        if let Ok(t_str) = std::str::from_utf8(&target_bytes) {
                                                                            if let Ok(sa) = t_str.parse() {
                                                                                dynamic_target = Some(TargetAddr::Ip(sa));
                                                                            } else if let Some((h, p)) = t_str.rsplit_once(':')
                                                                                && let Ok(port) = p.parse() {
                                                                                    dynamic_target = Some(TargetAddr::Domain(h.to_string(), port));
                                                                                }
                                                                        }
                                                                    }
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

                                                let is_dummy_target = match &metadata.target {
                                                    Some(TargetAddr::Domain(host, _)) => host == "pool.idle",
                                                    _ => false,
                                                };
                                                let valid_meta_target = if is_dummy_target { None } else { metadata.target };

                                                let target_opt = dynamic_target.or(valid_meta_target).or_else(|| {
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

                                                info!(target = %target, "Dialing forward target in local/internal network");
                                                let dial_target_fut = TcpStream::connect(target.to_string());
                                                let target_conn = match tokio::select! {
                                                    _ = conn_token.cancelled() => return,
                                                    res = dial_target_fut => res,
                                                } {
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
                                                    Box::new(target_conn),
                                                    stream,
                                                    conn_token,
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
                                                    Self::handle_reverse_control(stream, session_mgr, conn_token).await;
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
            });
        }

        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!("Server ingress runner received cancellation signal");
            }
            res = tracker.join_next() => {
                if let Some(Ok(Err(e))) = res {
                    error!("A server ingress listener terminated with error: {:?}", e);
                }
            }
        }

        tracker.shutdown_and_wait().await;
        Ok(())
    }

    /// Handles a reverse control channel: binds requested port on server and notifies client.
    pub(crate) async fn handle_reverse_control(
        stream: Box<dyn crate::core::stream::TunnelStream>,
        session_mgr: Arc<SessionManager>,
        cancel_token: CancellationToken,
    ) {
        let mut ctrl_stream = FramedControlStream::new(stream);

        // Await RegisterReverse
        let (listen_addr, target_addr) = match tokio::select! {
            _ = cancel_token.cancelled() => return,
            res = ctrl_stream.next() => res,
        } {
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
                let _ = tokio::select! {
                    _ = cancel_token.cancelled() => return,
                    res = ctrl_stream.send(ControlMessage::RegisterAck {
                        listen_addr: listen_addr.clone(),
                        success: true,
                        error_msg: None,
                    }) => res,
                };
                l
            }
            Err(e) => {
                warn!(addr = %listen_addr, "Failed to bind reverse listener: {:?}", e);
                let _ = tokio::select! {
                    _ = cancel_token.cancelled() => return,
                    res = ctrl_stream.send(ControlMessage::RegisterAck {
                        listen_addr: listen_addr.clone(),
                        success: false,
                        error_msg: Some(e.to_string()),
                    }) => res,
                };
                return;
            }
        };

        info!(addr = %listen_addr, "Server bound reverse port successfully. Waiting for external traffic...");
        let mut local_tracker = TaskTracker::<()>::new(cancel_token.clone());

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                Some(_) = local_tracker.join_next(), if !local_tracker.is_empty() => {}
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

                            let send_res = tokio::select! {
                                _ = cancel_token.cancelled() => {
                                    session_mgr.remove(&session_id);
                                    break;
                                }
                                res = ctrl_stream.send(notify_msg) => res,
                            };

                            if let Err(e) = send_res {
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
                            let _ = tokio::select! {
                                _ = cancel_token.cancelled() => break,
                                res = ctrl_stream.send(ControlMessage::Pong) => res,
                            };
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_server_reverse_control_cancellation_under_silent_client() {
        let (client, server) = tokio::io::duplex(4096);
        let _server_holder = tokio::spawn(async move {
            let mut s = server;
            let mut buf = [0u8; 1024];
            while let Ok(n) = tokio::io::AsyncReadExt::read(&mut s, &mut buf).await {
                if n == 0 {
                    break;
                }
            }
        });

        let session_mgr = Arc::new(SessionManager::new());
        let cancel_token = CancellationToken::new();
        let cancel_clone = cancel_token.clone();

        let handle = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(Box::new(client), session_mgr, cancel_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel_token.cancel();

        let res = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            res.is_ok(),
            "handle_reverse_control hung on silent client despite cancellation token!"
        );
    }
}
