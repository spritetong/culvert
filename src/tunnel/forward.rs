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
use crate::transport::{TargetAddr, TransportRegistry};

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
                        let connector = match transport_registry.create_connector(&endpoint_ctx, &config.token, config.mode) {
                            Ok(c) => c,
                            Err(e) => {
                                error!(tag = %config.tag, "Failed to create connector: {:?}", e);
                                continue;
                            }
                        };
                        let target = target.clone();

                        conn_tracker.spawn_with_cancel(move |token| async move {
                            match connector.connect(&target, None).await {
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
