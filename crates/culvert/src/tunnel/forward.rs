// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Forward tunnel worker (`-L` mode).
//! All comments and docstrings must be in English.

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::common::{CancelFutureExt, TaskTracker};
use crate::config::model::{TunnelConfig, TunnelMode};
use crate::consts::{ACTIVATE_STREAM_ACK, reverse_ack_timeout};
use crate::core::error::TunnelError;
use crate::core::serve::serve;
use crate::dialer::endpoint::EndpointContext;
use crate::frontend::{self, FrontendOutput};
use crate::io::addr::{L4Protocol, MappingSpec, TargetAddr, parse_target_addr};
use crate::io::channel::IoChannel;
use crate::io::pump::{PumpCtx, PumpRequest};
use crate::pool::ReplenishingStreamPool;
use crate::transport::{TransportConnector, TransportRegistry};
use reusable_future::ReusableFutureRef;

/// RAII lease releasing a pooled stream's `active_count` slot exactly once when dropped.
struct PoolStreamLease(Option<Arc<ReplenishingStreamPool>>);

impl Drop for PoolStreamLease {
    fn drop(&mut self) {
        if let Some(pool) = self.0.take() {
            pool.on_stream_closed();
        }
    }
}

async fn acquire_and_activate_channel(
    tag: &str,
    target: &TargetAddr,
    protocol: L4Protocol,
    idle_timeout: Duration,
    pool: Option<&Arc<ReplenishingStreamPool>>,
    connector: Option<&Arc<dyn TransportConnector>>,
    conn_token: &CancellationToken,
) -> Option<(IoChannel, PoolStreamLease)> {
    let max_attempts = if pool.is_some() { 2 } else { 1 };
    let mut attempts = 0usize;

    while attempts < max_attempts {
        attempts += 1;
        let acquire_fut = async {
            if let Some(p) = pool {
                p.acquire(Some(target))
                    .await
                    .map(IoChannel::from_boxed_stream)
            } else if let Some(c) = connector {
                c.connect_with_protocol(target, None, protocol).await
            } else {
                Err(TunnelError::Internal("Connector unavailable".into()))
            }
        };

        let remote_res = acquire_fut.cancel_with(conn_token).await?;

        match remote_res {
            Ok(mut channel) => {
                // Register the close hook right after `acquire()` succeeds so handshake
                // cancellation, activation failure, or relay exit always releases `active_count`.
                let lease = PoolStreamLease(pool.cloned());
                if pool.is_some() {
                    let activation_pkt = ReplenishingStreamPool::encode_activation_with_proto(
                        Some(target),
                        protocol,
                    );
                    let ack_timeout = reverse_ack_timeout(idle_timeout);
                    let write_act_res = async {
                        channel.write_all(&activation_pkt).await?;
                        channel.flush().await?;
                        let mut ack = [0u8; 1];
                        tokio::time::timeout(ack_timeout, channel.read_exact(&mut ack))
                            .await
                            .map_err(|_| {
                                std::io::Error::new(
                                    std::io::ErrorKind::TimedOut,
                                    "Timed out waiting for activation ACK",
                                )
                            })??;
                        if ack[0] == ACTIVATE_STREAM_ACK {
                            Ok::<(), std::io::Error>(())
                        } else {
                            Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "Invalid activation ACK byte",
                            ))
                        }
                    }
                    .cancel_with(conn_token)
                    .await?;

                    if let Err(e) = write_act_res {
                        if attempts < max_attempts {
                            debug!(
                                "Pooled stream activation failed (stale stream); retrying once with fresh stream: {}",
                                e
                            );
                            continue;
                        }
                        debug!("Failed to write stream activation packet: {}", e);
                        return None;
                    }
                }
                return Some((channel, lease));
            }
            Err(e) => {
                if attempts < max_attempts {
                    debug!("Acquire failed on attempt {}, retrying: {}", attempts, e);
                    continue;
                }
                if !conn_token.is_cancelled() {
                    warn!(tag = %tag, "Failed to open remote tunnel stream: {}", e);
                }
                return None;
            }
        }
    }

    None
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
                        let tag_for_task = tag.clone();

                        conn_tracker.spawn_with_cancel(move |conn_token| async move {
                            let Some((backend, _lease)) = acquire_and_activate_channel(
                                &tag_for_task,
                                &target,
                                protocol,
                                idle_timeout,
                                pool_clone.as_ref(),
                                connector_clone.as_ref(),
                                &conn_token,
                            )
                            .await
                            else {
                                return;
                            };

                            let res = serve(
                                PumpRequest {
                                    frontend: accepted.channel,
                                    backend,
                                }
                                ,
                                PumpCtx {
                                    cancel: conn_token,
                                    heartbeat_interval: hb_interval,
                                    idle_timeout,
                                },
                            ).await;
                            if let Err(ref e) = res {
                                debug!(tag = %tag_for_task, "Client forward relay pump finished with error: {:?}", e);
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

    let pool = if let Some(size) = config.forward_pool_size().filter(|&s| s > 0) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::{DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT};
    use crate::core::stream::BoxFuture;

    struct PendingConnector;

    impl TransportConnector for PendingConnector {
        fn connect<'a>(
            &'a self,
            _target: &'a TargetAddr,
            _session_id: Option<&'a str>,
        ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
            Box::pin(std::future::pending())
        }

        fn connect_pooled<'a>(
            &'a self,
            target: &'a TargetAddr,
        ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
            self.connect(target, None)
        }
    }

    #[tokio::test]
    async fn forward_activation_cancellation_and_retry_do_not_leak_active_count() {
        let cancel = CancellationToken::new();
        let pool = Arc::new(ReplenishingStreamPool::new_shared(
            Arc::new(PendingConnector),
            0,
            cancel.clone(),
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
        ));
        let target = parse_target_addr("127.0.0.1:8080").unwrap();
        let mut magic = [0u8; 1];

        // 1. Cancel while awaiting activation ACK -> must release active_count back to 0.
        let (c_cancel, mut s_cancel) = tokio::io::duplex(256);
        pool.push_idle(Box::new(c_cancel));
        let req_token = CancellationToken::new();
        let req_token_task = req_token.clone();
        let pool_cancel = Arc::clone(&pool);
        let target_cancel = target.clone();
        let cancel_handle = tokio::spawn(async move {
            acquire_and_activate_channel(
                "t-cancel",
                &target_cancel,
                L4Protocol::Tcp,
                Duration::from_secs(5),
                Some(&pool_cancel),
                None,
                &req_token_task,
            )
            .await
        });
        s_cancel.read_exact(&mut magic).await.unwrap();
        assert_eq!(pool.stats().active_count, 1);
        req_token.cancel();
        assert!(cancel_handle.await.unwrap().is_none());
        assert_eq!(
            pool.stats().active_count,
            0,
            "cancelling during activation must release active_count"
        );

        // 2. First stream fails activation (closed without ACK), retry succeeds on second stream:
        //    active_count must stay 1 (not 2) while leased, and return to 0 on lease drop.
        let (healthy_c, mut healthy_s) = tokio::io::duplex(256);
        let (dead_ack_c, dead_ack_s) = tokio::io::duplex(256);
        pool.push_idle(Box::new(healthy_c)); // popped second (LIFO)
        pool.push_idle(Box::new(dead_ack_c)); // popped first (LIFO)
        drop(dead_ack_s);

        let ack_task = tokio::spawn(async move {
            let mut hdr_buf = [0u8; 17];
            healthy_s.read_exact(&mut hdr_buf).await.unwrap();
            healthy_s.write_all(&[ACTIVATE_STREAM_ACK]).await.unwrap();
            healthy_s
        });

        let retry_token = CancellationToken::new();
        let res = acquire_and_activate_channel(
            "t-retry",
            &target,
            L4Protocol::Tcp,
            Duration::from_secs(5),
            Some(&pool),
            None,
            &retry_token,
        )
        .await;
        assert!(res.is_some());
        assert_eq!(pool.stats().active_count, 1);
        drop(res);
        assert_eq!(pool.stats().active_count, 0);
        let _ = ack_task.await.unwrap();

        // 3. Cancelling while pool.acquire() itself is pending must not underflow active_count.
        let empty_token = CancellationToken::new();
        let empty_token_task = empty_token.clone();
        let pool_empty = Arc::clone(&pool);
        let empty_handle = tokio::spawn(async move {
            acquire_and_activate_channel(
                "t-empty",
                &target,
                L4Protocol::Tcp,
                Duration::from_secs(5),
                Some(&pool_empty),
                None,
                &empty_token_task,
            )
            .await
        });
        tokio::task::yield_now().await;
        empty_token.cancel();
        assert!(empty_handle.await.unwrap().is_none());
        assert_eq!(pool.stats().active_count, 0);

        cancel.cancel();
    }
}
