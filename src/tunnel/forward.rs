//! Forward tunnel worker (`-L` mode).
//! All comments and docstrings must be in English.

use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::common::net::SocketTuner;
use crate::common::task::TaskTracker;
use crate::config::model::{TunnelConfig, TunnelMode};
use crate::core::error::TunnelError;
use crate::core::relay::RelayEngine;
use crate::dialer::endpoint::EndpointContext;
use crate::pool::ReplenishingStreamPool;
use crate::transport::{TargetAddr, TransportConnector, TransportRegistry};

/// Helper to parse target address string.
pub fn parse_target_addr(target_str: &str) -> Result<TargetAddr, TunnelError> {
    if let Ok(sa) = target_str.parse() {
        Ok(TargetAddr::Ip(sa))
    } else {
        let parts: Vec<&str> = target_str.rsplitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(TunnelError::Config(format!(
                "Invalid target: {}",
                target_str
            )));
        }
        let port: u16 = parts[0]
            .parse()
            .map_err(|_| TunnelError::Config("Invalid target port".to_string()))?;
        Ok(TargetAddr::Domain(parts[1].to_string(), port))
    }
}

/// Runs a single forward mapping listener and forwards inbound connections to remote peer.
pub async fn run_forward_mapping(
    tag: String,
    listen: String,
    target_str: String,
    token: Option<String>,
    endpoint_ctx: EndpointContext,
    transport_registry: Arc<TransportRegistry>,
    pool: Option<Arc<ReplenishingStreamPool>>,
    cancel_token: CancellationToken,
) -> Result<(), TunnelError> {
    info!(
        tag = %tag,
        listen = %listen,
        target = %target_str,
        "Starting forward tunnel listener"
    );

    let listener = TcpListener::bind(&listen).await?;
    let mut conn_tracker = TaskTracker::<()>::new(cancel_token.clone());
    let target = parse_target_addr(&target_str)?;

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!(tag = %tag, "Forward tunnel received cancellation signal");
                break;
            }
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((inbound_stream, peer_addr)) => {
                        let _ = SocketTuner::tune_tcp_stream(&inbound_stream, true, None);
                        info!(tag = %tag, peer = %peer_addr, "Inbound connection accepted");
                        let target = target.clone();
                        let pool_clone = pool.clone();
                        let registry_clone = Arc::clone(&transport_registry);
                        let endpoint_clone = endpoint_ctx.clone();
                        let token_clone = token.clone();
                        let pool_for_close = pool.clone();

                        conn_tracker.spawn_with_cancel(move |conn_token| async move {
                            let acquire_fut = async {
                                if let Some(ref p) = pool_clone {
                                    p.acquire(Some(&target)).await
                                } else {
                                    match registry_clone.create_connector(&endpoint_clone, token_clone.as_deref(), TunnelMode::Forward) {
                                        Ok(connector) => connector.connect(&target, None).await,
                                        Err(e) => Err(e),
                                    }
                                }
                            };

                            let remote_res = tokio::select! {
                                _ = conn_token.cancelled() => return,
                                res = acquire_fut => res,
                            };

                            match remote_res {
                                Ok(mut remote_stream) => {
                                    if pool_clone.is_some() {
                                        let activation_pkt = ReplenishingStreamPool::encode_activation(Some(&target));
                                        let write_act_res = tokio::select! {
                                            _ = conn_token.cancelled() => {
                                                if let Some(ref p) = pool_for_close {
                                                    p.on_stream_closed();
                                                }
                                                return;
                                            }
                                            res = async {
                                                remote_stream.write_all(&activation_pkt).await?;
                                                remote_stream.flush().await?;
                                                Ok::<(), std::io::Error>(())
                                            } => res,
                                        };

                                        if let Err(e) = write_act_res {
                                            error!("Failed to write stream activation packet: {:?}", e);
                                            if let Some(ref p) = pool_for_close {
                                                p.on_stream_closed();
                                            }
                                            return;
                                        }
                                    }

                                    let _ = RelayEngine::relay_with_cancel(
                                        Box::new(inbound_stream),
                                        remote_stream,
                                        conn_token,
                                    ).await;
                                    if let Some(ref p) = pool_for_close {
                                        p.on_stream_closed();
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to open remote tunnel stream: {:?}", e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!(tag = %tag, "Listener accept failed: {:?}", e);
                    }
                }
            }
        }
    }

    conn_tracker.shutdown_and_wait().await;
    Ok(())
}

/// Runs a forward tunnel worker that listens locally and relays to the remote peer.
pub async fn run_forward_tunnel(
    config: TunnelConfig,
    endpoint_ctx: EndpointContext,
    transport_registry: Arc<TransportRegistry>,
    cancel_token: CancellationToken,
) -> Result<(), TunnelError> {
    let listen = config.listen.clone().unwrap_or_default();
    let target = config.target.clone().unwrap_or_default();

    let pool = if let Some(size) = config.pool_size.filter(|&s| s > 0) {
        let connector: Arc<dyn TransportConnector> =
            Arc::from(transport_registry.create_connector(
                &endpoint_ctx,
                config.token.as_deref(),
                TunnelMode::Forward,
            )?);
        Some(Arc::new(ReplenishingStreamPool::new_shared(
            connector,
            size,
            cancel_token.clone(),
        )))
    } else {
        None
    };

    run_forward_mapping(
        config.tag,
        listen,
        target,
        config.token,
        endpoint_ctx,
        transport_registry,
        pool,
        cancel_token,
    )
    .await
}
