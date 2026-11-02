//! Reverse tunnel worker (`-R` mode).
//! All comments and docstrings must be in English.

use std::sync::Arc;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::common::net::SocketTuner;
use crate::common::task::TaskTracker;
use crate::config::model::TunnelConfig;
use crate::constants::DEFAULT_HEARTBEAT_INTERVAL;
use crate::control::stream::{ControlMessage, FramedControlStream};
use crate::core::error::TunnelError;
use crate::core::relay::RelayEngine;
use crate::dialer::endpoint::EndpointContext;
use crate::transport::{TargetAddr, TransportRegistry};

/// Runs a reverse tunnel worker that requests a remote listener on the server and streams traffic back locally.
pub async fn run_reverse_tunnel(
    config: TunnelConfig,
    endpoint_ctx: EndpointContext,
    transport_registry: Arc<TransportRegistry>,
    cancel_token: CancellationToken,
) -> Result<(), TunnelError> {
    info!(
        tag = %config.tag,
        listen = %config.listen,
        target = %config.target,
        "Starting reverse tunnel control loop"
    );

    let dummy_target = TargetAddr::Domain("control".to_string(), 0);
    let mut conn_tracker = TaskTracker::<()>::new(cancel_token.clone());

    while !cancel_token.is_cancelled() {
        let connector = transport_registry.create_connector(&endpoint_ctx, &config.token, config.mode)?;

        info!(tag = %config.tag, "Connecting reverse control channel to remote server...");
        let raw_stream = match connector.connect(&dummy_target, None).await {
            Ok(s) => s,
            Err(e) => {
                warn!(tag = %config.tag, "Failed to connect control channel: {:?}. Retrying in 3s...", e);
                tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(3)) => continue,
                }
            }
        };

        let mut ctrl_stream = FramedControlStream::new(raw_stream);

        // Send RegisterReverse request to remote server
        let reg_msg = ControlMessage::RegisterReverse {
            listen_addr: config.listen.clone(),
            target_addr: config.target.clone(),
        };

        if let Err(e) = ctrl_stream.send(reg_msg).await {
            warn!(tag = %config.tag, "Failed to send RegisterReverse: {:?}", e);
            continue;
        }

        // Await RegisterAck from remote server
        match ctrl_stream.next().await {
            Some(Ok(ControlMessage::RegisterAck { listen_addr, success, error_msg })) => {
                if !success {
                    error!(
                        tag = %config.tag,
                        addr = %listen_addr,
                        err = ?error_msg,
                        "Server rejected reverse listener registration"
                    );
                    tokio::select! {
                        _ = cancel_token.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_secs(5)) => continue,
                    }
                }
                info!(tag = %config.tag, addr = %listen_addr, "Remote server bound reverse port successfully");
            }
            other => {
                warn!(tag = %config.tag, "Unexpected response to RegisterReverse: {:?}", other);
                continue;
            }
        }

        let mut heartbeat = interval(DEFAULT_HEARTBEAT_INTERVAL);

        // Main event loop handling incoming connection alerts from remote server
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    info!(tag = %config.tag, "Reverse tunnel received cancellation signal");
                    break;
                }
                _ = heartbeat.tick() => {
                    if let Err(e) = ctrl_stream.send(ControlMessage::Ping).await {
                        warn!(tag = %config.tag, "Failed to send heartbeat ping: {:?}", e);
                        break;
                    }
                }
                msg_opt = ctrl_stream.next() => {
                    match msg_opt {
                        Some(Ok(msg)) => match msg {
                            ControlMessage::NewConnection { session_id, client_addr } => {
                                info!(
                                    tag = %config.tag,
                                    session = %session_id,
                                    client = %client_addr,
                                    "New reverse connection arrival notification from server"
                                );

                                let connector = match transport_registry.create_connector(&endpoint_ctx, &config.token, config.mode) {
                                    Ok(c) => c,
                                    Err(e) => {
                                        error!("Failed to create data connector: {:?}", e);
                                        continue;
                                    }
                                };
                                let target_addr = config.target.clone();
                                let session_id_clone = session_id.clone();
                                let dummy_target = dummy_target.clone();

                                conn_tracker.spawn_with_cancel(move |token| async move {
                                    // 1. Dial local target service
                                    let local_stream = match TcpStream::connect(&target_addr).await {
                                        Ok(s) => {
                                            let _ = SocketTuner::tune_tcp_stream(&s, true, None);
                                            s
                                        }
                                        Err(e) => {
                                            error!("Failed to connect to local target {}: {:?}", target_addr, e);
                                            return;
                                        }
                                    };

                                    // 2. Open reverse data stream to server matching session_id
                                    let data_stream = match connector.connect(&dummy_target, Some(&session_id_clone)).await {
                                        Ok(s) => s,
                                        Err(e) => {
                                            error!("Failed to connect reverse data stream for session {}: {:?}", session_id_clone, e);
                                            return;
                                        }
                                    };

                                    // 3. Relay bidirectionally
                                    let _ = RelayEngine::relay_with_cancel(
                                        Box::new(local_stream),
                                        data_stream,
                                        token,
                                    ).await;
                                });
                            }
                            ControlMessage::Pong => {
                                tracing::trace!(tag = %config.tag, "Heartbeat pong received");
                            }
                            ControlMessage::Ping => {
                                let _ = ctrl_stream.send(ControlMessage::Pong).await;
                            }
                            _ => {}
                        },
                        Some(Err(e)) => {
                            warn!(tag = %config.tag, "Control stream error: {:?}. Reconnecting...", e);
                            break;
                        }
                        None => {
                            warn!(tag = %config.tag, "Control stream EOF from server. Reconnecting...");
                            break;
                        }
                    }
                }
            }
        }
    }

    conn_tracker.shutdown_and_wait().await;
    Ok(())
}
