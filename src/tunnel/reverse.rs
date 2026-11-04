//! Reverse tunnel worker (`-R` mode).
//! All comments and docstrings must be in English.

use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::{Duration, interval};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::common::net::SocketTuner;
use crate::common::task::TaskTracker;
use crate::config::model::{TunnelConfig, TunnelMode};
use crate::consts::DEFAULT_HEARTBEAT_INTERVAL;
use crate::control::stream::{ControlMessage, FramedControlStream};
use crate::core::error::TunnelError;
use crate::core::relay::RelayEngine;
use crate::dialer::endpoint::EndpointContext;
use crate::transport::{TargetAddr, TransportConnector, TransportRegistry};

/// Runs a single reverse mapping control loop.
pub async fn run_reverse_mapping(
    tag: String,
    listen: String,
    target_addr: String,
    token: Option<String>,
    endpoint_ctx: EndpointContext,
    transport_registry: Arc<TransportRegistry>,
    cancel_token: CancellationToken,
) -> Result<(), TunnelError> {
    info!(
        tag = %tag,
        listen = %listen,
        target = %target_addr,
        "Starting reverse tunnel control loop"
    );

    let dummy_target = TargetAddr::Domain("control".to_string(), 0);
    let mut conn_tracker = TaskTracker::<()>::new(cancel_token.clone());

    // Share data connector across all incoming visitor sessions to maximize connection/0-RTT reuse
    let data_connector: Arc<dyn TransportConnector> = Arc::from(
        transport_registry.create_connector(&endpoint_ctx, token.as_deref(), TunnelMode::Reverse)?
    );

    while !cancel_token.is_cancelled() {
        let connector = transport_registry.create_connector(
            &endpoint_ctx,
            token.as_deref(),
            TunnelMode::Reverse,
        )?;

        info!(tag = %tag, "Connecting reverse control channel to remote server...");
        let raw_stream = match connector.connect(&dummy_target, None).await {
            Ok(s) => s,
            Err(e) => {
                warn!(tag = %tag, "Failed to connect control channel: {:?}. Retrying in 3s...", e);
                tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(3)) => continue,
                }
            }
        };

        let mut ctrl_stream = FramedControlStream::new(raw_stream);

        // Send RegisterReverse request to remote server
        let reg_msg = ControlMessage::RegisterReverse {
            listen_addr: listen.clone(),
            target_addr: target_addr.clone(),
        };

        if let Err(e) = ctrl_stream.send(reg_msg).await {
            warn!(tag = %tag, "Failed to send RegisterReverse: {:?}", e);
            continue;
        }

        // Await RegisterAck from remote server
        match ctrl_stream.next().await {
            Some(Ok(ControlMessage::RegisterAck {
                listen_addr: ack_addr,
                success,
                error_msg,
            })) => {
                if !success {
                    error!(
                        tag = %tag,
                        addr = %ack_addr,
                        err = ?error_msg,
                        "Server rejected reverse listener registration"
                    );
                    tokio::select! {
                        _ = cancel_token.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_secs(5)) => continue,
                    }
                }
                info!(tag = %tag, addr = %ack_addr, "Remote server bound reverse port successfully");
            }
            other => {
                warn!(tag = %tag, "Unexpected response to RegisterReverse: {:?}", other);
                continue;
            }
        }

        let mut heartbeat = interval(DEFAULT_HEARTBEAT_INTERVAL);

        // Main event loop handling incoming connection alerts from remote server
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    info!(tag = %tag, "Reverse tunnel received cancellation signal");
                    break;
                }
                Some(_) = conn_tracker.join_next(), if !conn_tracker.is_empty() => {}
                _ = heartbeat.tick() => {
                    if let Err(e) = ctrl_stream.send(ControlMessage::Ping).await {
                        warn!(tag = %tag, "Failed to send heartbeat ping: {:?}", e);
                        break;
                    }
                }
                msg_opt = ctrl_stream.next() => {
                    match msg_opt {
                        Some(Ok(msg)) => match msg {
                            ControlMessage::NewConnection { session_id, client_addr } => {
                                info!(
                                    tag = %tag,
                                    session = %session_id,
                                    client = %client_addr,
                                    "New reverse connection arrival notification from server"
                                );

                                let connector = Arc::clone(&data_connector);
                                let target_clone = target_addr.clone();
                                let session_id_clone = session_id.clone();
                                let dummy_target = dummy_target.clone();

                                conn_tracker.spawn_with_cancel(move |token| async move {
                                    // 1. Dial local target service
                                    let dial_local_fut = TcpStream::connect(&target_clone);
                                    let local_stream = match tokio::select! {
                                        _ = token.cancelled() => return,
                                        res = dial_local_fut => res,
                                    } {
                                        Ok(s) => {
                                            let _ = SocketTuner::tune_tcp_stream(&s, true, None);
                                            s
                                        }
                                        Err(e) => {
                                            error!("Failed to connect to local target {}: {:?}", target_clone, e);
                                            return;
                                        }
                                    };

                                    // 2. Open reverse data stream to server matching session_id
                                    let dial_remote_fut = connector.connect(&dummy_target, Some(&session_id_clone));
                                    let data_stream = match tokio::select! {
                                        _ = token.cancelled() => return,
                                        res = dial_remote_fut => res,
                                    } {
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
                                tracing::trace!(tag = %tag, "Heartbeat pong received");
                            }
                            ControlMessage::Ping => {
                                let _ = ctrl_stream.send(ControlMessage::Pong).await;
                            }
                            _ => {}
                        },
                        Some(Err(e)) => {
                            warn!(tag = %tag, "Control stream error: {:?}. Reconnecting...", e);
                            break;
                        }
                        None => {
                            warn!(tag = %tag, "Control stream EOF from server. Reconnecting...");
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

/// Runs a reverse tunnel worker that requests a remote listener on the server and streams traffic back locally.
pub async fn run_reverse_tunnel(
    config: TunnelConfig,
    endpoint_ctx: EndpointContext,
    transport_registry: Arc<TransportRegistry>,
    cancel_token: CancellationToken,
) -> Result<(), TunnelError> {
    let listen = config.listen.clone().unwrap_or_default();
    let target = config.target.clone().unwrap_or_default();
    run_reverse_mapping(
        config.tag,
        listen,
        target,
        config.token,
        endpoint_ctx,
        transport_registry,
        cancel_token,
    )
    .await
}
