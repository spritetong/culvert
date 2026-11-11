// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Forward tunnel worker (`-L` mode).
//! All comments and docstrings must be in English.

use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::common::{CancelFutureExt, TaskTracker};
use crate::config::model::{TunnelConfig, TunnelMode};
use crate::core::error::TunnelError;
use crate::core::serve::serve;
use crate::dialer::endpoint::EndpointContext;
use crate::frontend::{self, FrontendOutput};
use crate::io::addr::{L4Protocol, MappingSpec, parse_target_addr};
use crate::io::channel::IoChannel;
use crate::io::pump::{PumpCtx, PumpRequest};
use crate::pool::ReplenishingStreamPool;
use crate::transport::{TransportConnector, TransportRegistry};
use reusable_future::ReusableFutureRef;

struct ActiveStreamGuard(Option<Arc<ReplenishingStreamPool>>);

impl Drop for ActiveStreamGuard {
    fn drop(&mut self) {
        if let Some(ref p) = self.0 {
            p.on_stream_closed();
        }
    }
}

/// Runs a single forward mapping listener and forwards inbound connections to remote peer.
#[allow(clippy::too_many_arguments)]
pub async fn run_forward_mapping(
    tag: String,
    protocol: L4Protocol,
    listen: String,
    target_str: String,
    udp_idle_timeout: Option<Duration>,
    token: Option<String>,
    endpoint_ctx: EndpointContext,
    transport_registry: Arc<TransportRegistry>,
    pool: Option<Arc<ReplenishingStreamPool>>,
    cancel_token: CancellationToken,
) -> Result<(), TunnelError> {
    info!(
        tag = %tag,
        protocol = %protocol,
        listen = %listen,
        target = %target_str,
        "Forward tunnel listener started"
    );

    let spec = MappingSpec::new(protocol, &listen, &target_str);
    let mut frontend = frontend::bind(&spec).await?;
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
    let idle_timeout = match protocol {
        L4Protocol::Tcp => endpoint_ctx.idle_timeout(),
        L4Protocol::Udp => udp_idle_timeout.unwrap_or_else(|| endpoint_ctx.udp_idle_timeout()),
    };
    let mut accept_slot: ReusableFutureRef<'static, FrontendOutput> = ReusableFutureRef::default();

    loop {
        if accept_slot.is_empty() {
            frontend.accept(&mut accept_slot);
        }

        tokio::select! {
            biased;
            _ = &mut cancel_fut => {
                debug!(tag = %tag, "Forward tunnel stopping on cancellation signal");
                break;
            }
            Some(_) = conn_tracker.join_next(), if !conn_tracker.is_empty() => {}
            accept_res = &mut accept_slot => {
                match accept_res {
                    Ok(accepted) => {
                        let peer_addr = accepted.peer;
                        debug!(tag = %tag, peer = %peer_addr, "Accepted inbound connection");
                        let target = target.clone();
                        let pool_clone = pool.clone();
                        let connector_clone = shared_connector.clone();
                        let pool_for_close = pool.clone();
                        let tag_for_task = tag.clone();

                        conn_tracker.spawn_with_cancel(move |conn_token| async move {
                            let acquire_fut = async {
                                if let Some(ref p) = pool_clone {
                                    p.acquire(Some(&target))
                                        .await
                                        .map(IoChannel::from_boxed_stream)
                                } else if let Some(ref c) = connector_clone {
                                    c.connect_with_protocol(&target, None, protocol).await
                                } else {
                                    Err(TunnelError::Internal("Connector unavailable".into()))
                                }
                            };

                            let Some(remote_res) = acquire_fut.cancel_with(&conn_token).await else {
                                return;
                            };

                            match remote_res {
                                Ok(mut remote_channel) => {
                                    let _stream_guard = ActiveStreamGuard(pool_for_close.clone());

                                    if pool_clone.is_some() {
                                        let activation_pkt = ReplenishingStreamPool::encode_activation_with_proto(Some(&target), protocol);
                                        let Some(write_act_res) = async {
                                            remote_channel.write_all(&activation_pkt).await?;
                                            remote_channel.flush().await?;
                                            Ok::<(), std::io::Error>(())
                                        }
                                        .cancel_with(&conn_token)
                                        .await
                                        else {
                                            return;
                                        };

                                        if let Err(e) = write_act_res {
                                            debug!("Failed to write stream activation packet: {}", e);
                                            return;
                                        }
                                    }

                                    let _ = serve(
                                        PumpRequest {
                                            frontend: accepted.channel,
                                            backend: remote_channel,
                                        },
                                        PumpCtx {
                                            cancel: conn_token,
                                            heartbeat_interval: hb_interval,
                                            idle_timeout,
                                        },
                                    ).await;
                                }
                                Err(e) => {
                                    if !conn_token.is_cancelled() {
                                        warn!(tag = %tag_for_task, "Failed to open remote tunnel stream: {}", e);
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        if !cancel_token.is_cancelled() && !matches!(e, TunnelError::Shutdown) {
                            error!(tag = %tag, "Listener accept failed: {}", e);
                        }
                    }
                }
            }
        }
    }

    accept_slot.reset();
    frontend.shutdown().await;
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
    let protocol = mapping.protocol;
    let udp_idle_timeout = mapping.udp_idle_timeout;

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
            endpoint_ctx.heartbeat_interval(),
            endpoint_ctx.idle_timeout(),
        )))
    } else {
        None
    };

    run_forward_mapping(
        config.tag,
        protocol,
        listen,
        target,
        udp_idle_timeout,
        config.token,
        endpoint_ctx,
        transport_registry,
        pool,
        cancel_token,
    )
    .await
}
