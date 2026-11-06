// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Forward tunnel worker (`-L` mode).
//! All comments and docstrings must be in English.

use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::common::{CancelFutureExt, TaskTracker};
use crate::config::model::{TunnelConfig, TunnelMode};
use crate::core::error::TunnelError;
use crate::core::relay::RelayEngine;
use crate::dialer::endpoint::EndpointContext;
use crate::pool::ReplenishingStreamPool;
use crate::transport::{TargetAddr, TransportConnector, TransportRegistry};

struct ActiveStreamGuard(Option<Arc<ReplenishingStreamPool>>);

impl Drop for ActiveStreamGuard {
    fn drop(&mut self) {
        if let Some(ref p) = self.0 {
            p.on_stream_closed();
        }
    }
}

/// Helper to validate if a string conforms to a valid hostname / domain name.
fn is_valid_hostname(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 {
        return false;
    }
    for label in host.split('.') {
        if label.is_empty() || label.len() > 63 {
            return false;
        }
        if label.starts_with('-') || label.ends_with('-') {
            return false;
        }
        if !label
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return false;
        }
    }
    true
}

/// Helper to parse target address string supporting three formats:
/// 1. ip:port (e.g. 127.0.0.1:8080, [::1]:8080)
/// 2. hostname:port (e.g. localhost:8080, example.com:443)
/// 3. unix://path (e.g. unix:///run/tunnel.sock, unix://C:/temp/tunnel.sock)
///
/// Port number cannot be omitted for IP and hostname addresses.
pub fn parse_target_addr(target_str: &str) -> Result<TargetAddr, TunnelError> {
    let target_str = target_str.trim();
    if target_str.starts_with("unix://") {
        let path = crate::platform::normalize_uds_path(target_str);
        if path.as_os_str().is_empty() {
            return Err(TunnelError::Config(format!(
                "Invalid target UDS path: {}",
                target_str
            )));
        }
        return Ok(TargetAddr::Path(path));
    }

    // Must be ip:port or hostname:port. Port number cannot be omitted.
    if let Some((host, port_str)) = target_str.rsplit_once(':') {
        let is_bracketed = host.starts_with('[') && host.ends_with(']');
        if host.contains(':') && !is_bracketed {
            return Err(TunnelError::Config(format!(
                "Invalid mapping address '{}'. IPv6 addresses with port must be enclosed in brackets (e.g. '[::1]:8080')",
                target_str
            )));
        }

        let clean_host = if is_bracketed {
            &host[1..host.len() - 1]
        } else {
            host
        };

        if clean_host.is_empty() {
            return Err(TunnelError::Config(format!(
                "Invalid mapping address '{}': missing host",
                target_str
            )));
        }

        let port = port_str.parse::<u16>().map_err(|_| {
            TunnelError::Config(format!(
                "Invalid mapping address '{}': invalid port '{}'",
                target_str, port_str
            ))
        })?;

        if port == 0 {
            return Err(TunnelError::Config(format!(
                "Invalid mapping address '{}': port must not be 0",
                target_str
            )));
        }

        if let Ok(ip) = clean_host.parse::<std::net::IpAddr>() {
            return Ok(TargetAddr::Ip(std::net::SocketAddr::new(ip, port)));
        } else if is_valid_hostname(clean_host) {
            return Ok(TargetAddr::Domain(clean_host.to_string(), port));
        }
    }

    Err(TunnelError::Config(format!(
        "Invalid mapping address '{}'. Expected 'ip:port', 'hostname:port', or 'unix://path'",
        target_str
    )))
}

/// Runs a single forward mapping listener and forwards inbound connections to remote peer.
#[allow(clippy::too_many_arguments)]
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

    let mut listener = crate::platform::GenericListener::bind(&listen).await?;
    let mut conn_tracker = TaskTracker::<()>::new(cancel_token.clone());
    let target = parse_target_addr(&target_str)?;

    let cancel_fut = cancel_token.cancelled();
    tokio::pin!(cancel_fut);

    // Create shared transport connector at the mapping level so internal QUIC/H3 connection
    // caches and zero-RTT handshakes are actually shared across multiple inbound visitor connections.
    let shared_connector: Option<Arc<dyn TransportConnector>> = if pool.is_none() {
        Some(Arc::from(transport_registry.create_connector(
            &endpoint_ctx,
            token.as_deref(),
            TunnelMode::Forward,
        )?))
    } else {
        None
    };

    let hb_interval = endpoint_ctx.heartbeat_interval();

    loop {
        tokio::select! {
            biased;
            _ = &mut cancel_fut => {
                info!(tag = %tag, "Forward tunnel received cancellation signal");
                break;
            }
            Some(_) = conn_tracker.join_next(), if !conn_tracker.is_empty() => {}
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((inbound_stream, peer_addr)) => {
                        info!(tag = %tag, peer = %peer_addr, "Inbound connection accepted");
                        let target = target.clone();
                        let pool_clone = pool.clone();
                        let connector_clone = shared_connector.clone();
                        let pool_for_close = pool.clone();

                        conn_tracker.spawn_with_cancel(move |conn_token| async move {
                            let acquire_fut = async {
                                if let Some(ref p) = pool_clone {
                                    p.acquire(Some(&target)).await
                                } else if let Some(ref c) = connector_clone {
                                    c.connect(&target, None).await
                                } else {
                                    Err(TunnelError::Internal("Connector unavailable".into()))
                                }
                            };

                            let Some(remote_res) = acquire_fut.cancel_with(&conn_token).await else {
                                return;
                            };

                            match remote_res {
                                Ok(mut remote_stream) => {
                                    let _stream_guard = ActiveStreamGuard(pool_for_close.clone());

                                    if pool_clone.is_some() {
                                        let activation_pkt = ReplenishingStreamPool::encode_activation(Some(&target));
                                        let Some(write_act_res) = async {
                                            remote_stream.write_all(&activation_pkt).await?;
                                            remote_stream.flush().await?;
                                            Ok::<(), std::io::Error>(())
                                        }
                                        .cancel_with(&conn_token)
                                        .await
                                        else {
                                            return;
                                        };

                                        if let Err(e) = write_act_res {
                                            error!("Failed to write stream activation packet: {:?}", e);
                                            return;
                                        }
                                    }

                                    let _ = RelayEngine::relay_with_heartbeat(
                                        inbound_stream,
                                        remote_stream,
                                        conn_token,
                                        hb_interval,
                                    ).await;
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
    let mappings = config.canonical_mappings()?;
    let mapping = mappings.first().ok_or_else(|| {
        TunnelError::Config(format!("Tunnel '{}' has no port mappings", config.tag))
    })?;
    let listen = mapping.listen.clone();
    let target = mapping.target.clone();

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
