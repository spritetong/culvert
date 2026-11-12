// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Reverse tunnel worker (`-R` mode).
//! All comments and docstrings must be in English.

use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::common::{CancelFutureExt, CancellationTokenExt, TaskTracker};
use crate::config::model::{TunnelConfig, TunnelMode};
use crate::consts::{
    ACTIVATE_STREAM_ACK,
    DEFAULT_REVERSE_BACKOFF_INITIAL, DEFAULT_REVERSE_BACKOFF_MAX, DEFAULT_REVERSE_PROMOTE_WAIT,
    POOL_BACKOFF_ACTIVE_WAITERS_MAX, POOL_BACKOFF_BASE, POOL_MAX_CONCURRENT_CONNECTS,
    REVERSE_BACKOFF_RESET_THRESHOLD, REVERSE_CONTROL_TARGET_HOST, clamp_reverse_pool_size,
};
use crate::control::{ControlMessage, SignalingChannel, SignalingConfig, SignalingRouter};
use crate::core::error::TunnelError;
use crate::core::serve::serve;
use crate::dialer::endpoint::EndpointContext;
use crate::io::addr::L4Protocol;
use crate::io::pump::{PumpCtx, PumpRequest};
use crate::pool::activation::decode_activation;
use crate::transport::{TargetAddr, TransportConnector, TransportRegistry};

/// Coordinates readiness between reverse mapping control channel registration and the shared reverse pool.
#[derive(Default)]
pub struct ReverseReadyNotifier {
    is_ready: std::sync::atomic::AtomicBool,
    notify: tokio::sync::Notify,
}

impl ReverseReadyNotifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mark_ready(&self) {
        self.is_ready.store(true, std::sync::atomic::Ordering::Release);
        self.notify.notify_waiters();
    }

    pub async fn wait_ready(&self, cancel_token: &CancellationToken) -> bool {
        if self.is_ready.load(std::sync::atomic::Ordering::Acquire) {
            return true;
        }
        tokio::select! {
            _ = cancel_token.cancelled() => false,
            _ = self.notify.notified() => true,
        }
    }
}

/// Runs a single reverse mapping control loop.
#[allow(clippy::too_many_arguments)]
pub async fn run_reverse_mapping(
    tag: String,
    protocol: L4Protocol,
    listen: String,
    target_addr: String,
    udp_idle_timeout: Option<Duration>,
    token: Option<String>,
    endpoint_ctx: EndpointContext,
    transport_registry: Arc<TransportRegistry>,
    pool_size: Option<usize>,
    tunnel_id: Option<String>,
    reverse_ready: Option<Arc<ReverseReadyNotifier>>,
    cancel_token: CancellationToken,
) -> Result<(), TunnelError> {
    info!(
        tag = %tag,
        protocol = %protocol,
        listen = %listen,
        target = %target_addr,
        "Reverse tunnel worker started"
    );

    let dummy_target = TargetAddr::Domain(REVERSE_CONTROL_TARGET_HOST.to_string(), 0);
    let hb_interval = endpoint_ctx.heartbeat_interval();
    let idle_timeout = endpoint_ctx.idle_timeout();
    let relay_idle_timeout = match protocol {
        L4Protocol::Tcp => idle_timeout,
        L4Protocol::Udp => udp_idle_timeout.unwrap_or_else(|| endpoint_ctx.udp_idle_timeout()),
    };
    // Clamp once and advertise the effective worker count: the server derives
    // its soft cap from exactly this value instead of an independent constant.
    let pool_workers = clamp_reverse_pool_size(pool_size);
    if let Some(configured) = pool_size.filter(|&s| s > 0)
        && pool_workers != Some(configured)
    {
        warn!(
            tag = %tag,
            configured,
            size = pool_workers.unwrap_or(0),
            "Clamping reverse inbound pool size to the worker cap"
        );
    }
    let mut conn_tracker = TaskTracker::<()>::new(cancel_token.clone());

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

        debug!(tag = %tag, "Connecting reverse control channel to remote server");
        let Some(conn_res) = connector
            .connect(&dummy_target, None)
            .cancel_with(&cancel_token)
            .await
        else {
            break;
        };

        let raw_stream = match conn_res {
            Ok(s) => s,
            Err(e) => {
                warn!(tag = %tag, "Failed to connect control channel: {}. Retrying in {:?}...", e, backoff);
                if !cancel_token.sleep_or_cancel(backoff).await {
                    break;
                }
                backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                continue;
            }
        };

        let sig_config = SignalingConfig {
            heartbeat_interval: hb_interval,
            idle_timeout,
        };
        let (signaling, mut sig_rx) =
            SignalingChannel::spawn(raw_stream, sig_config, cancel_token.child_token());
        let router = SignalingRouter::new(signaling);

        // Send RegisterReverse request to remote server
        let reg_msg = ControlMessage::RegisterReverse {
            listen_addr: listen.clone(),
            target_addr: target_addr.clone(),
            pool_size: pool_workers,
            protocol,
            tunnel_id: tunnel_id.clone(),
        };

        let send_result = router.send(reg_msg).await;
        if let Err(e) = send_result {
            warn!(tag = %tag, "Failed to send RegisterReverse: {}. Retrying in {:?}...", e, backoff);
            if !cancel_token.sleep_or_cancel(backoff).await {
                break;
            }
            backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
            continue;
        }

        // Await RegisterAck from remote server, bounded by the same idle window.
        match timeout(idle_timeout, sig_rx.recv())
            .cancel_with(&cancel_token)
            .await
        {
            None => break,
            Some(Ok(Some(Ok(ControlMessage::RegisterAck {
                listen_addr: ack_addr,
                success,
                error_msg,
            })))) => {
                if !success {
                    error!(
                        tag = %tag,
                        addr = %ack_addr,
                        err = ?error_msg,
                        "Server rejected reverse listener registration. Retrying in {:?}...", backoff
                    );
                    if !cancel_token.sleep_or_cancel(backoff).await {
                        break;
                    }
                    backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                    continue;
                }
                info!(tag = %tag, addr = %ack_addr, "Remote server bound reverse port");
                if let Some(ref ready) = reverse_ready {
                    ready.mark_ready();
                }
            }
            Some(Ok(Some(other))) => {
                warn!(tag = %tag, "Unexpected response to RegisterReverse: {:?}. Retrying in {:?}...", other, backoff);
                if !cancel_token.sleep_or_cancel(backoff).await {
                    break;
                }
                backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                continue;
            }
            _ => {
                warn!(tag = %tag, "Timed out waiting for RegisterAck; reconnecting");
                if !cancel_token.sleep_or_cancel(backoff).await {
                    break;
                }
                backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                continue;
            }
        };

        let loop_start = tokio::time::Instant::now();
        let pool_cancel = cancel_token.child_token();
        let mut pool_tracker = TaskTracker::<()>::new(pool_cancel.clone());

        if tunnel_id.is_none()
            && let Some(size) = pool_workers
        {
            // One worker holds at most one idle stream, so an unbounded config
            // value would spawn an unbounded number of dialing tasks.
            let listen_target = crate::io::addr::parse_target_addr(&listen)
                .unwrap_or_else(|_| dummy_target.clone());
            debug!(
                tag = %tag,
                size,
                "Pre-warming reverse inbound pool connections"
            );

            // Throttle concurrent outbound dial handshakes across all workers in this
            // reverse mapping to protect low-bandwidth links from connection storms.
            // Symmetrically matches Forward mode's POOL_MAX_CONCURRENT_CONNECTS.
                let dial_semaphore =
                    Arc::new(tokio::sync::Semaphore::new(POOL_MAX_CONCURRENT_CONNECTS));

                let event_sink = sig_rx.event_sink();
                let router_clone = router.clone();

                for worker_idx in 0..size {
                    let connector = Arc::clone(&data_connector);
                    let listen_target = listen_target.clone();
                    let target_clone = target_addr.clone();
                    let dial_semaphore = Arc::clone(&dial_semaphore);
                    let router_worker = router_clone.clone();
                    let event_sink_worker = event_sink.clone();
                    let sig_cfg = sig_config;
                    let hb = hb_interval;
                    let idle = relay_idle_timeout;

                    pool_tracker.spawn_with_cancel(move |token| async move {
                        // Every relay this worker starts is owned and awaited; no task
                        // may outlive the control-channel generation that created it.
                        let mut relays = TaskTracker::<()>::new(token.child_token());
                        // Stagger initial backoff slightly by worker index to avoid synchronous lockstep retries
                        let initial_retry =
                            POOL_BACKOFF_BASE + Duration::from_millis((worker_idx as u64) * 25);
                        let mut retry = initial_retry;

                        while !token.is_cancelled() {
                            let _permit = match dial_semaphore.acquire().cancel_with(&token).await {
                                Some(Ok(p)) => p,
                                _ => break,
                            };

                            let conn_res_opt = connector
                                .connect_pooled_with_protocol(&listen_target, protocol)
                                .cancel_with(&token)
                                .await;
                            drop(_permit);

                            let Some(conn_res) = conn_res_opt else {
                                break;
                            };

                            let mut stream = match conn_res {
                                Ok(s) => {
                                    retry = initial_retry;
                                    s
                                }
                                Err(e) => {
                                    tracing::debug!("Reverse pool pre-warm dial failed: {:?}", e);
                                    if !token.sleep_or_cancel(retry).await {
                                        break;
                                    }
                                    retry = (retry * 2).min(POOL_BACKOFF_ACTIVE_WAITERS_MAX);
                                    continue;
                                }
                            };

                            let activation_hdr = {
                                let activation = decode_activation(&mut stream, &token, idle);
                                tokio::pin!(activation);

                                loop {
                                    tokio::select! {
                                        biased;
                                        _ = token.cancelled() => break None,
                                        res = &mut activation => {
                                            break res.ok().flatten();
                                        }
                                        Some(_) = relays.join_next(), if !relays.is_empty() => {}
                                    }
                                }
                            };

                            let Some(hdr) = activation_hdr else {
                                if token.is_cancelled() {
                                    break;
                                }
                                continue;
                            };

                            if hdr.is_promotion {
                                if let Some(ref sink) = event_sink_worker {
                                    info!("Pre-warmed reverse stream promoted to signaling channel by remote server (0-RTT)");
                                    let new_handle = SignalingChannel::spawn_with_event_sink(
                                        stream,
                                        sig_cfg,
                                        sink.clone(),
                                        token.child_token(),
                                    );
                                    router_worker.attach(new_handle);
                                }
                                continue;
                            } else if hdr.protocol != protocol {
                                if !token.sleep_or_cancel(Duration::from_millis(100)).await {
                                    break;
                                }
                                continue;
                            }

                            // Echo ACTIVATE_STREAM_ACK (0xAC) back to server so server verifies stream liveness
                            let ack_sent = async {
                                tokio::io::AsyncWriteExt::write_all(
                                    &mut stream,
                                    &[ACTIVATE_STREAM_ACK],
                                )
                                .await?;
                                tokio::io::AsyncWriteExt::flush(&mut stream).await
                            };
                            let ack_res = tokio::time::timeout(Duration::from_secs(2), ack_sent)
                                .cancel_with(&token)
                                .await;
                            if !matches!(ack_res, Some(Ok(Ok(())))) {
                                debug!("Failed to echo ACTIVATE_STREAM_ACK; recycling reverse worker stream");
                                continue;
                            }

                            let target_to_connect = hdr.target.unwrap_or_else(|| {
                                crate::io::addr::parse_target_addr(&target_clone)
                                    .unwrap_or_else(|_| TargetAddr::Ip(std::net::SocketAddr::from(([127, 0, 0, 1], 0))))
                            });

                            let Some(local_res) =
                                crate::platform::connect_target_channel(&target_to_connect, protocol)
                                    .cancel_with(&token)
                                    .await
                            else {
                                break;
                            };
                            let local_channel = match local_res {
                                Ok(s) => s,
                                Err(e) => {
                                    error!(
                                        "Failed to connect to local target {}: {:?}",
                                        target_to_connect, e
                                    );
                                    if !token.sleep_or_cancel(Duration::from_millis(100)).await {
                                        break;
                                    }
                                    continue;
                                }
                            };

                            relays.spawn_with_cancel(move |relay_token| async move {
                                let _ = serve(
                                    PumpRequest::new(local_channel, stream),
                                    PumpCtx {
                                        cancel: relay_token,
                                        heartbeat_interval: hb,
                                        idle_timeout: idle,
                                    },
                                )
                                .await;
                            });
                            // Loop immediately: the consumed slot is replenished in
                            // one dial RTT, with no extra control-channel signaling.
                        }

                        relays.shutdown_and_wait().await;
                    });
                }
        }

        // Main event loop handling incoming connection alerts from remote server
        loop {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    debug!(tag = %tag, "Reverse tunnel stopping on cancellation signal");
                    break;
                }
                Some(_) = conn_tracker.join_next(), if !conn_tracker.is_empty() => {}
                Some(_) = pool_tracker.join_next(), if !pool_tracker.is_empty() => {}
                msg_opt = sig_rx.recv() => {
                    match msg_opt {
                        Some(Ok(ControlMessage::NewConnection { session_id, client_addr })) => {
                            debug!(
                                tag = %tag,
                                session = %session_id,
                                client = %client_addr,
                                "Received new connection notification"
                            );

                            let connector = Arc::clone(&data_connector);
                            let target_clone = target_addr.clone();
                            let session_id_clone = session_id.clone();
                            let dummy_target = dummy_target.clone();
                            let tag_clone = tag.clone();
                            let relay_idle = relay_idle_timeout;

                            conn_tracker.spawn_with_cancel(move |token| async move {
                                // 1. Dial local target service
                                let Some(local_res) =
                                    crate::platform::connect_target_channel_str(&target_clone, protocol)
                                        .cancel_with(&token)
                                        .await
                                else {
                                    return;
                                };
                                let local_channel = match local_res {
                                    Ok(s) => s,
                                    Err(e) => {
                                        if !token.is_cancelled() {
                                            warn!(tag = %tag_clone, "Failed to connect to local target {}: {}", target_clone, e);
                                        }
                                        return;
                                    }
                                };

                                // 2. Open reverse data stream to server matching session_id
                                let Some(remote_res) = connector
                                    .connect_with_protocol(&dummy_target, Some(&session_id_clone), protocol)
                                    .cancel_with(&token)
                                    .await
                                else {
                                    return;
                                };
                                let data_stream = match remote_res {
                                    Ok(s) => s,
                                    Err(e) => {
                                        if !token.is_cancelled() {
                                            warn!(tag = %tag_clone, "Failed to connect reverse data stream for session {}: {}", session_id_clone, e);
                                        }
                                        return;
                                    }
                                };

                                // 3. Relay bidirectionally
                                let _ = serve(
                                    PumpRequest::new(local_channel, data_stream),
                                    PumpCtx {
                                        cancel: token,
                                        heartbeat_interval: hb_interval,
                                        idle_timeout: relay_idle,
                                    },
                                ).await;
                            });
                        }
                        Some(Ok(_)) => {}
                        _ => {
                            if router.promote_standby_if_closed() || router.has_active_channel() {
                                debug!(tag = %tag, "Primary signaling channel ended while standby remains active");
                                continue;
                            }

                            if pool_workers.is_some() && !cancel_token.is_cancelled() {
                                let wait_window = DEFAULT_REVERSE_PROMOTE_WAIT.min(idle_timeout);
                                let promote_deadline = tokio::time::sleep(wait_window);
                                tokio::pin!(promote_deadline);
                                let mut promoted = false;
                                loop {
                                    let attached = router.notified();
                                    tokio::pin!(attached);
                                    if router.promote_standby_if_closed()
                                        || router.has_active_channel()
                                    {
                                        promoted = true;
                                        break;
                                    }
                                    tokio::select! {
                                        biased;
                                        _ = cancel_token.cancelled() => break,
                                        _ = &mut promote_deadline => break,
                                        _ = &mut attached => {
                                            if router.promote_standby_if_closed()
                                                || router.has_active_channel()
                                            {
                                                promoted = true;
                                                break;
                                            }
                                        }
                                        Some(_) = conn_tracker.join_next(), if !conn_tracker.is_empty() => {}
                                        Some(_) = pool_tracker.join_next(), if !pool_tracker.is_empty() => {}
                                    }
                                }
                                if promoted {
                                    debug!(tag = %tag, "Recovered reverse signaling via server 0xA6 promotion");
                                    continue;
                                }
                            }

                            if !cancel_token.is_cancelled() {
                                warn!(tag = %tag, "Control channel disconnected or timed out. Reconnecting...");
                            } else {
                                debug!(tag = %tag, "Control channel closed on shutdown");
                            }
                            break;
                        }
                    }
                }
            }
        }

        pool_tracker.shutdown_and_wait().await;

        if loop_start.elapsed() > REVERSE_BACKOFF_RESET_THRESHOLD {
            backoff = DEFAULT_REVERSE_BACKOFF_INITIAL;
        } else {
            backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
        }

        if !cancel_token.sleep_or_cancel(backoff).await {
            break;
        }
    }

    conn_tracker.shutdown_and_wait().await;
    Ok(())
}

/// Runs the shared pre-warming worker pool for all reverse mappings under a single tunnel.
pub async fn run_shared_reverse_pool(
    tunnel_id: String,
    pool_size: usize,
    token: Option<String>,
    endpoint_ctx: EndpointContext,
    transport_registry: Arc<TransportRegistry>,
    reverse_ready: Option<Arc<ReverseReadyNotifier>>,
    cancel_token: CancellationToken,
) -> Result<(), TunnelError> {
    let size = clamp_reverse_pool_size(Some(pool_size)).unwrap_or(0);
    if size == 0 {
        return Ok(());
    }

    // Await control channel registration before dialing pre-warm pool connections
    if let Some(ref ready) = reverse_ready
        && !ready.wait_ready(&cancel_token).await
    {
        return Ok(());
    }

    info!(
        tunnel_id = %tunnel_id,
        pool_size = size,
        "Starting shared reverse connection pool"
    );

    let dummy_target = TargetAddr::Domain(REVERSE_CONTROL_TARGET_HOST.to_string(), 0);
    let hb_interval = endpoint_ctx.heartbeat_interval();
    let idle_timeout = endpoint_ctx.idle_timeout();
    let udp_idle_timeout = endpoint_ctx.udp_idle_timeout();

    let data_connector: Arc<dyn TransportConnector> =
        Arc::from(transport_registry.create_connector(
            &endpoint_ctx,
            token.as_deref(),
            TunnelMode::Reverse,
        )?);

    let dial_semaphore = Arc::new(tokio::sync::Semaphore::new(POOL_MAX_CONCURRENT_CONNECTS));
    let mut pool_tracker = TaskTracker::<()>::new(cancel_token.clone());

    for worker_idx in 0..size {
        let connector = Arc::clone(&data_connector);
        let dummy_target = dummy_target.clone();
        let dial_semaphore = Arc::clone(&dial_semaphore);
        let tunnel_id = tunnel_id.clone();

        pool_tracker.spawn_with_cancel(move |worker_token| async move {
            let mut relays = TaskTracker::<()>::new(worker_token.child_token());
            let initial_retry =
                POOL_BACKOFF_BASE + Duration::from_millis((worker_idx as u64) * 25);
            let mut retry = initial_retry;

            while !worker_token.is_cancelled() {
                let _permit = match dial_semaphore.acquire().cancel_with(&worker_token).await {
                    Some(Ok(p)) => p,
                    _ => break,
                };

                let conn_res_opt = connector
                    .connect_pooled_with_tunnel(&dummy_target, L4Protocol::Tcp, Some(&tunnel_id))
                    .cancel_with(&worker_token)
                    .await;
                drop(_permit);

                let Some(conn_res) = conn_res_opt else {
                    break;
                };

                let mut stream = match conn_res {
                    Ok(s) => {
                        retry = initial_retry;
                        s
                    }
                    Err(e) => {
                        tracing::debug!("Shared reverse pool pre-warm dial failed: {:?}", e);
                        if !worker_token.sleep_or_cancel(retry).await {
                            break;
                        }
                        retry = (retry * 2).min(POOL_BACKOFF_ACTIVE_WAITERS_MAX);
                        continue;
                    }
                };

                // Server is sole owner of idle stream lifetime; await activation from server.
                let activation_hdr = {
                    let activation = decode_activation(&mut stream, &worker_token, idle_timeout);
                    tokio::pin!(activation);

                    loop {
                        tokio::select! {
                            biased;
                            _ = worker_token.cancelled() => break None,
                            res = &mut activation => {
                                break res.ok().flatten();
                            }
                            Some(_) = relays.join_next(), if !relays.is_empty() => {}
                        }
                    }
                };

                let Some(hdr) = activation_hdr else {
                    if worker_token.is_cancelled() {
                        break;
                    }
                    continue;
                };

                if hdr.is_promotion {
                    continue;
                }

                let target = match hdr.target {
                    Some(t) => t,
                    None => {
                        tracing::debug!("Activated reverse stream had no target specified; skipping");
                        continue;
                    }
                };

                // Echo ACTIVATE_STREAM_ACK (0xAC) back to server so server verifies stream liveness
                let ack_sent = async {
                    tokio::io::AsyncWriteExt::write_all(
                        &mut stream,
                        &[ACTIVATE_STREAM_ACK],
                    )
                    .await?;
                    tokio::io::AsyncWriteExt::flush(&mut stream).await
                };
                let ack_res = tokio::time::timeout(Duration::from_secs(2), ack_sent)
                    .cancel_with(&worker_token)
                    .await;
                if !matches!(ack_res, Some(Ok(Ok(())))) {
                    tracing::debug!("Failed to echo ACTIVATE_STREAM_ACK; recycling reverse worker stream");
                    continue;
                }

                let proto = hdr.protocol;
                let relay_idle = match proto {
                    L4Protocol::Tcp => idle_timeout,
                    L4Protocol::Udp => udp_idle_timeout,
                };

                let Some(local_res) =
                    crate::platform::connect_target_channel(&target, proto)
                        .cancel_with(&worker_token)
                        .await
                else {
                    break;
                };

                let local_channel = match local_res {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(
                            "Failed to connect to local target {}: {:?}",
                            target, e
                        );
                        if !worker_token.sleep_or_cancel(Duration::from_millis(100)).await {
                            break;
                        }
                        continue;
                    }
                };

                relays.spawn_with_cancel(move |relay_token| async move {
                    let _ = serve(
                        PumpRequest::new(local_channel, stream),
                        PumpCtx {
                            cancel: relay_token,
                            heartbeat_interval: hb_interval,
                            idle_timeout: relay_idle,
                        },
                    )
                    .await;
                });
            }

            relays.shutdown_and_wait().await;
        });
    }

    cancel_token.cancelled().await;
    pool_tracker.shutdown_and_wait().await;
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
    let rev_pool = config.reverse_pool_size();
    run_reverse_mapping(
        config.tag,
        mapping.protocol,
        listen,
        target,
        mapping.udp_idle_timeout,
        config.token,
        endpoint_ctx,
        transport_registry,
        rev_pool,
        None,
        None,
        cancel_token,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{AppConfig, DialerConfig, EndpointConfig};
    use crate::control::FramedControlStream;
    use crate::core::stream::BoxFuture;
    use crate::io::channel::IoChannel;
    use crate::transport::{TargetAddr, TransportConnector, TransportDriver, TransportListener};
    use std::time::Duration;

    struct MockSilentConnector;

    impl TransportConnector for MockSilentConnector {
        fn connect<'a>(
            &'a self,
            _target: &'a TargetAddr,
            _session_id: Option<&'a str>,
        ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
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
                Ok(IoChannel::from_boxed_stream(Box::new(client)))
            })
        }

        fn connect_pooled<'a>(
            &'a self,
            target: &'a TargetAddr,
        ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
            self.connect(target, None)
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
            _cancel: CancellationToken,
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
                L4Protocol::Tcp,
                "127.0.0.1:9090".to_string(),
                "127.0.0.1:9091".to_string(),
                None,
                None,
                ep_ctx,
                reg,
                None,
                None,
                None,
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

    struct MockThrottledConnector {
        concurrent: Arc<std::sync::atomic::AtomicUsize>,
        max_concurrent: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl TransportConnector for MockThrottledConnector {
        fn connect<'a>(
            &'a self,
            _target: &'a TargetAddr,
            _session_id: Option<&'a str>,
        ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
            let concurrent = Arc::clone(&self.concurrent);
            let max_concurrent = Arc::clone(&self.max_concurrent);
            Box::pin(async move {
                let (client, server) = tokio::io::duplex(4096);
                let cur = concurrent.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                max_concurrent.fetch_max(cur, std::sync::atomic::Ordering::SeqCst);

                tokio::spawn(async move {
                    let mut framed = FramedControlStream::new(server);
                    // Handle RegisterReverse by replying RegisterAck if this is a control channel
                    if let Some(Ok(ControlMessage::RegisterReverse { listen_addr, .. })) =
                        futures_util::StreamExt::next(&mut framed).await
                    {
                        let _ = futures_util::SinkExt::send(
                            &mut framed,
                            ControlMessage::RegisterAck {
                                listen_addr,
                                success: true,
                                error_msg: None,
                            },
                        )
                        .await;
                    }
                    // Keep stream open
                    while futures_util::StreamExt::next(&mut framed).await.is_some() {}
                });

                tokio::time::sleep(Duration::from_millis(60)).await;
                concurrent.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                Ok(IoChannel::from_boxed_stream(Box::new(client)))
            })
        }

        fn connect_pooled<'a>(
            &'a self,
            target: &'a TargetAddr,
        ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
            self.connect(target, None)
        }
    }

    struct MockThrottledDriver {
        concurrent: Arc<std::sync::atomic::AtomicUsize>,
        max_concurrent: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl TransportDriver for MockThrottledDriver {
        fn schemes(&self) -> &[&'static str] {
            &["ws"]
        }

        fn build_connector(
            &self,
            _endpoint: &EndpointContext,
            _auth_token: &str,
            _mode: TunnelMode,
        ) -> Result<Box<dyn TransportConnector>, TunnelError> {
            Ok(Box::new(MockThrottledConnector {
                concurrent: Arc::clone(&self.concurrent),
                max_concurrent: Arc::clone(&self.max_concurrent),
            }))
        }

        fn build_listener(
            &self,
            _listen_addr: &str,
            _auth_token: Option<&str>,
            _path: Option<&str>,
            _tls: Option<&crate::config::model::TlsConfig>,
            _cancel: CancellationToken,
        ) -> Result<Box<dyn TransportListener>, TunnelError> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_reverse_pool_prewarm_dial_concurrency_throttled() {
        let concurrent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_concurrent = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let driver = MockThrottledDriver {
            concurrent: Arc::clone(&concurrent),
            max_concurrent: Arc::clone(&max_concurrent),
        };

        let mut reg = TransportRegistry::new();
        reg.register(driver);
        let reg = Arc::new(reg);

        let cancel_token = CancellationToken::new();
        let cancel_clone = cancel_token.clone();

        let ep_cfg = EndpointConfig::Simple("ws://127.0.0.1:8080".to_string());
        let app_cfg = AppConfig::default();
        let ep_ctx = EndpointContext::build(&ep_cfg, &app_cfg).unwrap();

        // Spawn with pool_size = 24 (> POOL_MAX_CONCURRENT_CONNECTS = 16)
        let handle = tokio::spawn(async move {
            run_reverse_mapping(
                "test_throttle".to_string(),
                L4Protocol::Tcp,
                "127.0.0.1:9090".to_string(),
                "127.0.0.1:9091".to_string(),
                None,
                None,
                ep_ctx,
                reg,
                Some(24),
                None,
                None,
                cancel_clone,
            )
            .await
        });

        // Allow workers to start pre-warming
        tokio::time::sleep(Duration::from_millis(200)).await;

        cancel_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

        let peak = max_concurrent.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            peak <= POOL_MAX_CONCURRENT_CONNECTS + 1, // +1 for the control channel dial
            "Peak concurrent dials ({peak}) must be bounded by POOL_MAX_CONCURRENT_CONNECTS ({})",
            POOL_MAX_CONCURRENT_CONNECTS
        );
        assert!(peak > 0, "At least one pre-warm dial should have completed");
    }

    /// A peer that accepts the connection but never reads a single byte. The
    /// server halves are kept alive in `held` (never dropped, never polled), so
    /// a control-frame flush against the 1-byte duplex buffer can never finish.
    struct StalledPeerConnector {
        held: Arc<std::sync::Mutex<Vec<tokio::io::DuplexStream>>>,
    }

    impl TransportConnector for StalledPeerConnector {
        fn connect<'a>(
            &'a self,
            _target: &'a TargetAddr,
            _session_id: Option<&'a str>,
        ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
            Box::pin(async move {
                let (client, server) = tokio::io::duplex(1);
                {
                    let mut guard = self.held.lock().unwrap();
                    guard.push(server);
                }
                Ok(IoChannel::from_boxed_stream(Box::new(client)))
            })
        }

        fn connect_pooled<'a>(
            &'a self,
            target: &'a TargetAddr,
        ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
            self.connect(target, None)
        }
    }

    struct StalledPeerDriver {
        held: Arc<std::sync::Mutex<Vec<tokio::io::DuplexStream>>>,
    }

    impl TransportDriver for StalledPeerDriver {
        fn schemes(&self) -> &[&'static str] {
            &["ws"]
        }

        fn build_connector(
            &self,
            _endpoint: &EndpointContext,
            _auth_token: &str,
            _mode: TunnelMode,
        ) -> Result<Box<dyn TransportConnector>, TunnelError> {
            Ok(Box::new(StalledPeerConnector {
                held: Arc::clone(&self.held),
            }))
        }

        fn build_listener(
            &self,
            _listen_addr: &str,
            _auth_token: Option<&str>,
            _path: Option<&str>,
            _tls: Option<&crate::config::model::TlsConfig>,
            _cancel: CancellationToken,
        ) -> Result<Box<dyn TransportListener>, TunnelError> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_reverse_control_register_send_is_bounded_by_stalled_peer() {
        let held = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut reg = TransportRegistry::new();
        reg.register(StalledPeerDriver {
            held: Arc::clone(&held),
        });
        let reg = Arc::new(reg);

        // A short idle window keeps reverse_frame_timeout() short as well, so
        // this regression does not have to wait for the WAN-scale default.
        let app_cfg = AppConfig {
            dialer: Some(DialerConfig {
                idle_timeout: Some(Duration::from_millis(300)),
                ..Default::default()
            }),
            ..Default::default()
        };
        let ep_cfg = EndpointConfig::Simple("ws://127.0.0.1:8080".to_string());
        let ep_ctx = EndpointContext::build(&ep_cfg, &app_cfg).unwrap();

        let cancel_token = CancellationToken::new();
        let cancel_clone = cancel_token.clone();
        let handle = tokio::spawn(async move {
            run_reverse_mapping(
                "test_stalled_register".to_string(),
                L4Protocol::Tcp,
                "127.0.0.1:9090".to_string(),
                "127.0.0.1:9091".to_string(),
                None,
                None,
                ep_ctx,
                reg,
                Some(1),
                None,
                None,
                cancel_clone,
            )
            .await
        });

        // Let the control loop reach the wedged RegisterReverse flush.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !held.lock().unwrap().is_empty(),
            "stalled peer should have been dialed"
        );

        cancel_token.cancel();
        let res = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            res.is_ok(),
            "run_reverse_mapping wedged on an unbounded RegisterReverse send"
        );
        assert!(res.unwrap().unwrap().is_ok());
    }
}
