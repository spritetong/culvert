// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Reverse tunnel worker (`-R` mode).
//! All comments and docstrings must be in English.

use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::common::task::TaskTracker;
use crate::config::model::{TunnelConfig, TunnelMode};
use crate::consts::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_REVERSE_BACKOFF_INITIAL, DEFAULT_REVERSE_BACKOFF_MAX,
    REVERSE_BACKOFF_RESET_THRESHOLD, REVERSE_CONTROL_TARGET_HOST,
};
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

    let dummy_target = TargetAddr::Domain(REVERSE_CONTROL_TARGET_HOST.to_string(), 0);
    let mut conn_tracker = TaskTracker::<()>::new(cancel_token.clone());

    let cancel_fut = cancel_token.cancelled();
    tokio::pin!(cancel_fut);

    // Share data connector across all incoming visitor sessions to maximize connection/0-RTT reuse
    let data_connector: Arc<dyn TransportConnector> =
        Arc::from(transport_registry.create_connector(
            &endpoint_ctx,
            token.as_deref(),
            TunnelMode::Reverse,
        )?);

    let mut backoff = DEFAULT_REVERSE_BACKOFF_INITIAL;

    while !cancel_token.is_cancelled() {
        let connector = transport_registry.create_connector(
            &endpoint_ctx,
            token.as_deref(),
            TunnelMode::Reverse,
        )?;

        info!(tag = %tag, "Connecting reverse control channel to remote server...");
        let raw_stream = match tokio::select! {
            biased;
            _ = &mut cancel_fut => break,
            res = connector.connect(&dummy_target, None) => res,
        } {
            Ok(s) => s,
            Err(e) => {
                warn!(tag = %tag, "Failed to connect control channel: {:?}. Retrying in {:?}...", e, backoff);
                tokio::select! {
                    biased;
                    _ = &mut cancel_fut => break,
                    _ = tokio::time::sleep(backoff) => {
                        backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                        continue;
                    }
                }
            }
        };

        let mut ctrl_stream = FramedControlStream::new(raw_stream);

        // Send RegisterReverse request to remote server
        let reg_msg = ControlMessage::RegisterReverse {
            listen_addr: listen.clone(),
            target_addr: target_addr.clone(),
        };

        let send_res = tokio::select! {
            biased;
            _ = &mut cancel_fut => break,
            res = ctrl_stream.send(reg_msg) => res,
        };
        if let Err(e) = send_res {
            warn!(tag = %tag, "Failed to send RegisterReverse: {:?}. Retrying in {:?}...", e, backoff);
            tokio::select! {
                biased;
                _ = &mut cancel_fut => break,
                _ = tokio::time::sleep(backoff) => {
                    backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                    continue;
                }
            }
        }

        // Await RegisterAck from remote server
        let ack_res = tokio::select! {
            biased;
            _ = &mut cancel_fut => break,
            res = ctrl_stream.next() => res,
        };
        match ack_res {
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
                        "Server rejected reverse listener registration. Retrying in {:?}...", backoff
                    );
                    tokio::select! {
                        biased;
                        _ = &mut cancel_fut => break,
                        _ = tokio::time::sleep(backoff) => {
                            backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                            continue;
                        }
                    }
                }
                info!(tag = %tag, addr = %ack_addr, "Remote server bound reverse port successfully");
            }
            other => {
                warn!(tag = %tag, "Unexpected response to RegisterReverse: {:?}. Retrying in {:?}...", other, backoff);
                tokio::select! {
                    biased;
                    _ = &mut cancel_fut => break,
                    _ = tokio::time::sleep(backoff) => {
                        backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                        continue;
                    }
                }
            }
        }

        let loop_start = tokio::time::Instant::now();
        let mut heartbeat = interval(DEFAULT_HEARTBEAT_INTERVAL);

        // Main event loop handling incoming connection alerts from remote server
        loop {
            tokio::select! {
                biased;
                _ = &mut cancel_fut => {
                    info!(tag = %tag, "Reverse tunnel received cancellation signal");
                    break;
                }
                Some(_) = conn_tracker.join_next(), if !conn_tracker.is_empty() => {}
                _ = heartbeat.tick() => {
                    let send_ping = tokio::select! {
                        biased;
                        _ = &mut cancel_fut => break,
                        res = ctrl_stream.send(ControlMessage::Ping) => res,
                    };
                    if let Err(e) = send_ping {
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
                                    let dial_local_fut = crate::platform::connect_target_str(&target_clone);
                                    let local_stream = match tokio::select! {
                                        _ = token.cancelled() => return,
                                        res = dial_local_fut => res,
                                    } {
                                        Ok(s) => s,
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
                                        local_stream,
                                        data_stream,
                                        token,
                                    ).await;
                                });
                            }
                            ControlMessage::Pong => {
                                tracing::trace!(tag = %tag, "Heartbeat pong received");
                            }
                            ControlMessage::Ping => {
                                let _ = tokio::select! {
                                    biased;
                                    _ = &mut cancel_fut => break,
                                    res = ctrl_stream.send(ControlMessage::Pong) => res,
                                };
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

        if loop_start.elapsed() > REVERSE_BACKOFF_RESET_THRESHOLD {
            backoff = DEFAULT_REVERSE_BACKOFF_INITIAL;
        } else {
            backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
        }

        tokio::select! {
            biased;
            _ = &mut cancel_fut => break,
            _ = tokio::time::sleep(backoff) => {}
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
    let mappings = config.canonical_mappings()?;
    let mapping = mappings.first().ok_or_else(|| {
        TunnelError::Config(format!("Tunnel '{}' has no port mappings", config.tag))
    })?;
    let listen = mapping.listen.clone();
    let target = mapping.target.clone();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{AppConfig, EndpointConfig};
    use crate::core::stream::{BoxFuture, TunnelStream};
    use crate::transport::{TargetAddr, TransportConnector, TransportDriver, TransportListener};
    use std::time::Duration;

    struct MockSilentConnector;

    impl TransportConnector for MockSilentConnector {
        fn connect<'a>(
            &'a self,
            _target: &'a TargetAddr,
            _session_id: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
            Box::pin(async move {
                let (client, server) = tokio::io::duplex(4096);
                // Keep server stream alive without dropping or sending anything back
                tokio::spawn(async move {
                    let mut s = server;
                    let mut buf = [0u8; 1024];
                    // Read incoming registration bytes so client send buffer does not block
                    while let Ok(n) = tokio::io::AsyncReadExt::read(&mut s, &mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                });
                Ok(Box::new(client) as Box<dyn TunnelStream>)
            })
        }

        fn protocol_scheme(&self) -> &'static str {
            "ws"
        }
    }

    struct MockSilentDriver;

    impl TransportDriver for MockSilentDriver {
        fn schemes(&self) -> &[&'static str] {
            &["ws"]
        }

        fn build_connector(
            &self,
            _endpoint: &EndpointContext,
            _auth_token: &str,
            _mode: TunnelMode,
        ) -> Result<Box<dyn TransportConnector>, TunnelError> {
            Ok(Box::new(MockSilentConnector))
        }

        fn build_listener(
            &self,
            _listen_addr: &str,
            _auth_token: Option<&str>,
            _path: Option<&str>,
            _tls: Option<&crate::config::model::TlsConfig>,
        ) -> Result<Box<dyn TransportListener>, TunnelError> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_reverse_mapping_cancellation_under_silent_peer() {
        let mut reg = TransportRegistry::new();
        reg.register(MockSilentDriver);
        let reg = Arc::new(reg);

        let cancel_token = CancellationToken::new();
        let cancel_clone = cancel_token.clone();

        let ep_cfg = EndpointConfig::Simple("ws://127.0.0.1:8080".to_string());
        let app_cfg = AppConfig::default();
        let ep_ctx = EndpointContext::build(&ep_cfg, &app_cfg).unwrap();

        let handle = tokio::spawn(async move {
            run_reverse_mapping(
                "test_silent".to_string(),
                "127.0.0.1:9090".to_string(),
                "127.0.0.1:9091".to_string(),
                None,
                ep_ctx,
                reg,
                cancel_clone,
            )
            .await
        });

        // Allow reverse mapping to connect, send RegisterReverse, and enter ctrl_stream.next() awaiting RegisterAck
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Cancel token while waiting on silent peer's RegisterAck
        cancel_token.cancel();

        // Verify task exits cleanly within 2 seconds instead of hanging on the silent peer
        let res = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            res.is_ok(),
            "run_reverse_mapping hung on silent peer despite cancellation token!"
        );
        let inner_res = res.unwrap().unwrap();
        assert!(inner_res.is_ok());
    }
}
