//! Forward tunnel worker (`-L` mode).
//! All comments and docstrings must be in English.

use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::common::net::SocketTuner;
use crate::common::task::TaskTracker;
use crate::config::model::TunnelConfig;
use crate::core::error::TunnelError;
use crate::core::relay::RelayEngine;
use crate::dialer::endpoint::EndpointContext;
use crate::pool::ReplenishingStreamPool;
use crate::transport::{TargetAddr, TransportConnector, TransportRegistry};

/// Runs a forward tunnel worker that listens locally and relays to the remote peer.
pub async fn run_forward_tunnel(
    config: TunnelConfig,
    endpoint_ctx: EndpointContext,
    transport_registry: Arc<TransportRegistry>,
    cancel_token: CancellationToken,
) -> Result<(), TunnelError> {
    info!(
        tag = %config.tag,
        listen = %config.listen,
        target = %config.target,
        "Starting forward tunnel listener"
    );

    let listener = TcpListener::bind(&config.listen).await?;
    let mut conn_tracker = TaskTracker::<()>::new(cancel_token.clone());

    // Parse target address
    let target = if let Ok(sa) = config.target.parse() {
        TargetAddr::Ip(sa)
    } else {
        let parts: Vec<&str> = config.target.rsplitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(TunnelError::Config(format!("Invalid target: {}", config.target)));
        }
        let port: u16 = parts[0]
            .parse()
            .map_err(|_| TunnelError::Config("Invalid target port".to_string()))?;
        TargetAddr::Domain(parts[1].to_string(), port)
    };

    let pool = if let Some(size) = config.pool_size.filter(|&s| s > 0) {
        let connector: Arc<dyn TransportConnector> = Arc::from(
            transport_registry.create_connector(&endpoint_ctx, &config.token, config.mode)?,
        );
        Some(Arc::new(ReplenishingStreamPool::new(
            connector,
            target.clone(),
            size,
            cancel_token.clone(),
        )))
    } else {
        None
    };

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!(tag = %config.tag, "Forward tunnel received cancellation signal");
                break;
            }
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((inbound_stream, peer_addr)) => {
                        let _ = SocketTuner::tune_tcp_stream(&inbound_stream, true, None);
                        info!(tag = %config.tag, peer = %peer_addr, "Inbound connection accepted");
                        let target = target.clone();
                        let pool_clone = pool.clone();
                        let registry_clone = Arc::clone(&transport_registry);
                        let endpoint_clone = endpoint_ctx.clone();
                        let token_clone = config.token.clone();
                        let mode = config.mode;

                        conn_tracker.spawn_with_cancel(move |token| async move {
                            let remote_res = if let Some(ref p) = pool_clone {
                                p.acquire().await
                            } else {
                                match registry_clone.create_connector(&endpoint_clone, &token_clone, mode) {
                                    Ok(connector) => connector.connect(&target, None).await,
                                    Err(e) => Err(e),
                                }
                            };

                            match remote_res {
                                Ok(remote_stream) => {
                                    let _ = RelayEngine::relay_with_cancel(
                                        Box::new(inbound_stream),
                                        remote_stream,
                                        token,
                                    ).await;
                                }
                                Err(e) => {
                                    error!("Failed to open remote tunnel stream: {:?}", e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!(tag = %config.tag, "Listener accept failed: {:?}", e);
                    }
                }
            }
        }
    }

    conn_tracker.shutdown_and_wait().await;
    Ok(())
}
