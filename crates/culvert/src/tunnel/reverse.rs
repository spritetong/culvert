// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Reverse tunnel worker (`-R` mode).
//! All comments and docstrings must be in English.

use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::common::{CancelFutureExt, CancellationTokenExt, TaskTracker};
use crate::config::model::{TunnelConfig, TunnelMode};
use crate::consts::{
    ACTIVATE_STREAM_MAGIC, DEFAULT_REVERSE_BACKOFF_INITIAL, DEFAULT_REVERSE_BACKOFF_MAX,
    POOL_BACKOFF_BASE, POOL_BACKOFF_MAX, POOL_MAX_CONCURRENT_CONNECTS,
    REVERSE_BACKOFF_RESET_THRESHOLD, REVERSE_CONTROL_TARGET_HOST, clamp_reverse_pool_size,
};
use crate::control::stream::{ControlMessage, FramedControlStream};
use crate::core::error::TunnelError;
use crate::core::relay::RelayEngine;
use crate::core::stream::{activity_now_ms, heartbeat_timing};
use crate::dialer::endpoint::EndpointContext;
use crate::transport::{TargetAddr, TransportConnector, TransportRegistry};

/// Runs a single reverse mapping control loop.
#[allow(clippy::too_many_arguments)]
pub async fn run_reverse_mapping(
    tag: String,
    listen: String,
    target_addr: String,
    token: Option<String>,
    endpoint_ctx: EndpointContext,
    transport_registry: Arc<TransportRegistry>,
    pool_size: Option<usize>,
    cancel_token: CancellationToken,
) -> Result<(), TunnelError> {
    info!(
        tag = %tag,
        listen = %listen,
        target = %target_addr,
        "Starting reverse tunnel control loop"
    );

    let dummy_target = TargetAddr::Domain(REVERSE_CONTROL_TARGET_HOST.to_string(), 0);
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

        info!(tag = %tag, "Connecting reverse control channel to remote server...");
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
                warn!(tag = %tag, "Failed to connect control channel: {:?}. Retrying in {:?}...", e, backoff);
                if !cancel_token.sleep_or_cancel(backoff).await {
                    break;
                }
                backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                continue;
            }
        };

        let mut ctrl_stream = FramedControlStream::new(raw_stream);

        // Send RegisterReverse request to remote server
        let reg_msg = ControlMessage::RegisterReverse {
            listen_addr: listen.clone(),
            target_addr: target_addr.clone(),
            pool_size: pool_workers,
        };

        let Some(send_res) = ctrl_stream.send(reg_msg).cancel_with(&cancel_token).await else {
            break;
        };
        if let Err(e) = send_res {
            warn!(tag = %tag, "Failed to send RegisterReverse: {:?}. Retrying in {:?}...", e, backoff);
            if !cancel_token.sleep_or_cancel(backoff).await {
                break;
            }
            backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
            continue;
        }

        // Await RegisterAck from remote server
        let Some(ack_res) = ctrl_stream.next().cancel_with(&cancel_token).await else {
            break;
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
                    if !cancel_token.sleep_or_cancel(backoff).await {
                        break;
                    }
                    backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                    continue;
                }
                info!(tag = %tag, addr = %ack_addr, "Remote server bound reverse port successfully");
            }
            other => {
                warn!(tag = %tag, "Unexpected response to RegisterReverse: {:?}. Retrying in {:?}...", other, backoff);
                if !cancel_token.sleep_or_cancel(backoff).await {
                    break;
                }
                backoff = (backoff * 2).min(DEFAULT_REVERSE_BACKOFF_MAX);
                continue;
            }
        }

        let loop_start = tokio::time::Instant::now();
        let hb_interval = endpoint_ctx.heartbeat_interval();
        let idle_timeout = endpoint_ctx.idle_timeout();
        let mut heartbeat = interval(hb_interval);
        // Liveness is pure activity: any RX or real TX refreshes the deadline.
        // Probe PINGs are deliberately excluded, otherwise our own keepalives
        // would keep a silently dead peer marked alive forever.
        let mut last_activity_ms = activity_now_ms();
        let idle_deadline = tokio::time::sleep(idle_timeout);
        tokio::pin!(idle_deadline);

        let pool_cancel = cancel_token.child_token();
        let mut pool_tracker = TaskTracker::<()>::new(pool_cancel.clone());

        if let Some(size) = pool_workers {
            // One worker holds at most one idle stream, so an unbounded config
            // value would spawn an unbounded number of dialing tasks.
            let listen_target = crate::tunnel::forward::parse_target_addr(&listen)
                .unwrap_or_else(|_| dummy_target.clone());
            info!(
                tag = %tag,
                size,
                "Pre-warming reverse inbound pool connections to remote server"
            );

            // Throttle concurrent outbound dial handshakes across all workers in this
            // reverse mapping to protect low-bandwidth links from connection storms.
            // Symmetrically matches Forward mode's POOL_MAX_CONCURRENT_CONNECTS.
            let dial_semaphore =
                Arc::new(tokio::sync::Semaphore::new(POOL_MAX_CONCURRENT_CONNECTS));

            for worker_idx in 0..size {
                let connector = Arc::clone(&data_connector);
                let listen_target = listen_target.clone();
                let target_clone = target_addr.clone();
                let dial_semaphore = Arc::clone(&dial_semaphore);
                let hb = hb_interval;
                let idle = idle_timeout;

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
                            .connect_pooled(&listen_target)
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
                                retry = (retry * 2).min(POOL_BACKOFF_MAX);
                                continue;
                            }
                        };

                        // The server is the sole owner of an idle stream's
                        // lifetime. This worker must NEVER close the stream on
                        // its own timer: the server may already have popped it
                        // and be writing the activation byte, and a local
                        // recycle in that window would blackhole the visitor.
                        // Eviction, re-registration and listener teardown all
                        // close the stream from the server side; this worker
                        // then sees EOF (or cancellation) and re-dials.
                        // The inner scope releases the stream borrow before the
                        // stream is moved into the relay task.
                        let activated = {
                            let activation = async {
                                let mut magic = [0u8; 1];
                                let res = stream.read_exact(&mut magic).await;
                                (res, magic)
                            };
                            tokio::pin!(activation);

                            loop {
                                tokio::select! {
                                    biased;
                                    _ = token.cancelled() => break false,
                                    (res, magic) = &mut activation => {
                                        break res.is_ok() && magic[0] == ACTIVATE_STREAM_MAGIC;
                                    }
                                    Some(_) = relays.join_next(), if !relays.is_empty() => {}
                                }
                            }
                        };

                        if token.is_cancelled() {
                            break;
                        }
                        if !activated {
                            if !token.sleep_or_cancel(Duration::from_millis(100)).await {
                                break;
                            }
                            continue;
                        }

                        let Some(local_res) = crate::platform::connect_target_str(&target_clone)
                            .cancel_with(&token)
                            .await
                        else {
                            break;
                        };
                        let local_stream = match local_res {
                            Ok(s) => s,
                            Err(e) => {
                                error!(
                                    "Failed to connect to local target {}: {:?}",
                                    target_clone, e
                                );
                                if !token.sleep_or_cancel(Duration::from_millis(100)).await {
                                    break;
                                }
                                continue;
                            }
                        };

                        relays.spawn_with_cancel(move |relay_token| async move {
                            let _ = RelayEngine::relay_with_heartbeat(
                                local_stream,
                                stream,
                                relay_token,
                                hb,
                                idle,
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
                    info!(tag = %tag, "Reverse tunnel received cancellation signal");
                    break;
                }
                Some(_) = conn_tracker.join_next(), if !conn_tracker.is_empty() => {}
                Some(_) = pool_tracker.join_next(), if !pool_tracker.is_empty() => {}
                _ = &mut idle_deadline => {
                    warn!(
                        tag = %tag,
                        idle_timeout = ?idle_timeout,
                        "Reverse control channel idle timeout: no inbound or outbound activity. Reconnecting..."
                    );
                    break;
                }
                _ = heartbeat.tick() => {
                    // Recent TX/RX already proves liveness; a probe would be pure
                    // overhead. PING only exists to provoke RX and never proves
                    // anything by itself, so it must not refresh the clock.
                    let (send_probe, _) = heartbeat_timing(
                        Some(last_activity_ms),
                        activity_now_ms(),
                        hb_interval,
                        idle_timeout,
                    );
                    if send_probe {
                        let Some(send_ping) = ctrl_stream.send(ControlMessage::Ping).cancel_with(&cancel_token).await else {
                            break;
                        };
                        if let Err(e) = send_ping {
                            warn!(tag = %tag, "Failed to send heartbeat ping: {:?}", e);
                            break;
                        }
                    }
                }
                msg_opt = ctrl_stream.next() => {
                    // ANY inbound frame counts, including Ping/Pong. PONG is not
                    // load-bearing for liveness: it is merely one possible RX.
                    last_activity_ms = activity_now_ms();
                    idle_deadline.as_mut().reset(tokio::time::Instant::now() + idle_timeout);
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
                                    let Some(local_res) = crate::platform::connect_target_str(&target_clone)
                                        .cancel_with(&token)
                                        .await
                                    else {
                                        return;
                                    };
                                    let local_stream = match local_res {
                                        Ok(s) => s,
                                        Err(e) => {
                                            error!("Failed to connect to local target {}: {:?}", target_clone, e);
                                            return;
                                        }
                                    };

                                    // 2. Open reverse data stream to server matching session_id
                                    let Some(remote_res) = connector
                                        .connect(&dummy_target, Some(&session_id_clone))
                                        .cancel_with(&token)
                                        .await
                                    else {
                                        return;
                                    };
                                    let data_stream = match remote_res {
                                        Ok(s) => s,
                                        Err(e) => {
                                            error!("Failed to connect reverse data stream for session {}: {:?}", session_id_clone, e);
                                            return;
                                        }
                                    };

                                    // 3. Relay bidirectionally
                                    let _ = RelayEngine::relay_with_heartbeat(
                                        local_stream,
                                        data_stream,
                                        token,
                                        hb_interval,
                                        idle_timeout,
                                    ).await;
                                });
                            }
                            ControlMessage::Pong => {
                                tracing::trace!(tag = %tag, "Heartbeat pong received");
                            }
                            ControlMessage::Ping => {
                                // Optional echo: it only exists to give the peer
                                // some RX. Correctness never depends on a PONG.
                                let Some(res) = ctrl_stream.send(ControlMessage::Pong).cancel_with(&cancel_token).await else {
                                    break;
                                };
                                let _ = res;
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
        config.pool_size,
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
        ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
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
                Ok(Box::new(client) as Box<dyn TunnelStream>)
            })
        }

        fn protocol_scheme(&self) -> &'static str {
            "ws"
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

        // Spawn with pool_size = 12 (> POOL_MAX_CONCURRENT_CONNECTS = 4)
        let handle = tokio::spawn(async move {
            run_reverse_mapping(
                "test_throttle".to_string(),
                "127.0.0.1:9090".to_string(),
                "127.0.0.1:9091".to_string(),
                None,
                ep_ctx,
                reg,
                Some(12),
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
}
