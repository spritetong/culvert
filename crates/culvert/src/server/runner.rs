// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Ingress server daemon handling forward proxying and reverse tunnel pairing.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::common::{CancelFutureExt, TaskTracker, TimeoutOrCancel};
use crate::config::model::{ServerConfig, TlsConfig, TunnelMode};
use crate::consts::{
    ACTIVATE_STREAM_MAGIC, DEFAULT_ACTIVATION_READ_TIMEOUT, DEFAULT_SESSION_PAIR_TIMEOUT,
    MAX_ACTIVATION_TARGET_LEN, POOL_DUMMY_HOST, reverse_frame_timeout, reverse_pool_soft_cap,
};
use crate::control::{ControlMessage, SignalingChannel, SignalingConfig, SignalingRouter};
use crate::core::error::TunnelError;
use crate::core::serve::serve;
use crate::frontend::{self, FrontendOutput};
use crate::io::addr::MappingSpec;
use crate::io::pump::{PumpCtx, PumpRequest};
use crate::session::SessionManager;
use crate::transport::{TargetAddr, TransportRegistry};
use reusable_future::ReusableFutureRef;

static REVERSE_SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Ingress server runner that listens on an ingress endpoint (e.g. WSS/UDS) and dispatches tunnels.
pub struct ServerRunner {
    config: ServerConfig,
    global_tls: Option<TlsConfig>,
    transport_registry: Arc<TransportRegistry>,
    session_manager: Arc<SessionManager>,
}

impl ServerRunner {
    /// Creates a new `ServerRunner` with default transport registry and session manager.
    pub fn from_config(config: ServerConfig) -> Self {
        Self::with_global_tls(
            config,
            None,
            Arc::new(TransportRegistry::default()),
            Arc::new(SessionManager::new()),
        )
    }

    /// Creates a new `ServerRunner` with global TLS and default registries.
    pub fn from_config_with_tls(config: ServerConfig, global_tls: Option<TlsConfig>) -> Self {
        Self::with_global_tls(
            config,
            global_tls,
            Arc::new(TransportRegistry::default()),
            Arc::new(SessionManager::new()),
        )
    }

    /// Creates a new `ServerRunner`.
    pub fn new(
        config: ServerConfig,
        transport_registry: Arc<TransportRegistry>,
        session_manager: Arc<SessionManager>,
    ) -> Self {
        Self::with_global_tls(config, None, transport_registry, session_manager)
    }

    /// Creates a new `ServerRunner` with inherited global TLS configuration.
    pub fn with_global_tls(
        config: ServerConfig,
        global_tls: Option<TlsConfig>,
        transport_registry: Arc<TransportRegistry>,
        session_manager: Arc<SessionManager>,
    ) -> Self {
        Self {
            config,
            global_tls,
            transport_registry,
            session_manager,
        }
    }

    /// Runs the server listener and stream dispatch loop until cancelled.
    pub async fn run(&self, cancel_token: CancellationToken) -> Result<(), TunnelError> {
        let specs = self.config.parse_specs()?;
        if specs.is_empty() {
            info!("No server listen endpoints configured, waiting for cancellation signal");
            cancel_token.cancelled().await;
            return Ok(());
        }

        info!(
            listeners = ?self.config.listen,
            "Starting culvert server ingress listeners"
        );

        let mut tracker = TaskTracker::<Result<(), TunnelError>>::new(cancel_token.clone());

        for spec in specs {
            let spec_clone = spec.clone();
            let registry = Arc::clone(&self.transport_registry);
            let session_mgr = Arc::clone(&self.session_manager);
            let default_target = self.config.default_target.clone();
            let token_opt = self.config.token.clone();
            let server_tls = self.config.effective_tls(self.global_tls.as_ref());
            let idle_timeout = self
                .config
                .idle_timeout
                .unwrap_or(crate::consts::DEFAULT_IDLE_TIMEOUT);
            let hb_interval = self
                .config
                .heartbeat_interval
                .unwrap_or(crate::consts::DEFAULT_HEARTBEAT_INTERVAL);

            tracker.spawn_with_cancel(move |token| async move {
                let mut listener = registry.create_listener(
                    &spec_clone.scheme,
                    &spec_clone.bind_addr,
                    token_opt.as_deref(),
                    spec_clone.path.as_deref(),
                    server_tls.as_ref(),
                    token.clone(),
                )?;

                if let Some(mode) = spec_clone.chmod
                    && let Err(e) = crate::platform::set_socket_permissions(
                        &crate::platform::normalize_uds_path(&spec_clone.bind_addr),
                        mode,
                    )
                {
                    tracing::warn!(
                        bind = %spec_clone.bind_addr,
                        mode = format_args!("{:#o}", mode),
                        error = %e,
                        "Failed to set socket permissions"
                    );
                }

                info!(
                    scheme = %spec_clone.scheme,
                    bind = %spec_clone.bind_addr,
                    path = ?spec_clone.path,
                    "Server ingress listener running"
                );

                let mut conn_tracker = TaskTracker::<()>::new(token.clone());
                let mut accept_err: Option<TunnelError> = None;
                let mut accept_slot = ReusableFutureRef::default();

                loop {
                    if accept_slot.is_empty() {
                        listener.accept(&mut accept_slot);
                    }

                    tokio::select! {
                        biased;
                        _ = token.cancelled() => break,
                        Some(_) = conn_tracker.join_next(), if !conn_tracker.is_empty() => {}
                        accept_res = &mut accept_slot, if !accept_slot.is_empty() => {
                            accept_slot.reset();
                            match accept_res {
                                Ok((stream, metadata)) => {
                                    let session_mgr = Arc::clone(&session_mgr);
                                    let default_target = default_target.clone();
                                    let idle_timeout = idle_timeout;
                                    let hb_interval = hb_interval;

                                    conn_tracker.spawn_with_cancel(move |conn_token| async move {
                                        match metadata.mode {
                                            // 1. Forward mode: Dial remote target and relay
                                            TunnelMode::Forward => {
                                                let mut stream = stream;
                                                let mut dynamic_target: Option<TargetAddr> = None;

                                                // If this stream was pre-warmed as an idle connection in client pool,
                                                // wait for the client activation signal before touching the target service.
                                                if metadata.pool_idle {
                                                    let mut magic = [0u8; 1];
                                                    let Some(res) = stream.read_exact(&mut magic).cancel_with(&conn_token).await else {
                                                        return;
                                                    };
                                                    match res {
                                                        Ok(_) if magic[0] == ACTIVATE_STREAM_MAGIC => {
                                                            // Activated! Check if dynamic target address is appended: [len: u16, target_str] with timeout
                                                            let mut len_bytes = [0u8; 2];
                                                            let read_len_res = match stream
                                                                .read_exact(&mut len_bytes)
                                                                .timeout_or_cancel(DEFAULT_ACTIVATION_READ_TIMEOUT, &conn_token)
                                                                .await
                                                            {
                                                                TimeoutOrCancel::Ok(r) => r,
                                                                TimeoutOrCancel::TimedOut => {
                                                                    debug!("Timed out reading activation target length (10s)");
                                                                    return;
                                                                }
                                                                TimeoutOrCancel::Cancelled => return,
                                                            };
                                                            if let Err(e) = read_len_res {
                                                                debug!("Failed to read activation target length: {}", e);
                                                                return;
                                                            }
                                                            let target_len = u16::from_be_bytes(len_bytes) as usize;
                                                            if target_len >= MAX_ACTIVATION_TARGET_LEN {
                                                                warn!("Activation target length too large: {}", target_len);
                                                                return;
                                                            }
                                                            if target_len > 0 {
                                                                let mut target_bytes = vec![0u8; target_len];
                                                                let read_target_res = match stream
                                                                    .read_exact(&mut target_bytes)
                                                                    .timeout_or_cancel(DEFAULT_ACTIVATION_READ_TIMEOUT, &conn_token)
                                                                    .await
                                                                {
                                                                    TimeoutOrCancel::Ok(r) => r,
                                                                    TimeoutOrCancel::TimedOut => {
                                                                        debug!("Timed out reading activation target payload (10s)");
                                                                        return;
                                                                    }
                                                                    TimeoutOrCancel::Cancelled => return,
                                                                };
                                                                if let Err(e) = read_target_res {
                                                                    debug!("Failed to read activation target payload: {}", e);
                                                                    return;
                                                                }
                                                                        if let Ok(t_str) = std::str::from_utf8(&target_bytes)
                                                                            && let Ok(sa) = crate::io::addr::parse_target_addr(t_str) {
                                                                                dynamic_target = Some(sa);
                                                                        }
                                                                    }
                                                                }
                                                                Ok(_) => {
                                                                    debug!("Received invalid activation magic from pool stream: {:02x}", magic[0]);
                                                                    return;
                                                                }
                                                                Err(_) => {
                                                                    // Stale or evicted stream closed by client pool without being used
                                                                    return;
                                                                }
                                                            }
                                                        }

                                                let is_dummy_target = match &metadata.target {
                                                    Some(TargetAddr::Domain(host, _)) => host == POOL_DUMMY_HOST,
                                                    _ => false,
                                                };
                                                let valid_meta_target = if is_dummy_target { None } else { metadata.target };

                                                let target_opt = dynamic_target.or(valid_meta_target).or_else(|| {
                                                    default_target.and_then(|dt| crate::io::addr::parse_target_addr(&dt).ok())
                                                });

                                                let target = match target_opt {
                                                    Some(t) => t,
                                                    None => {
                                                        error!("Forward stream arrived without target address");
                                                        return;
                                                    }
                                                };

                                                debug!(target = %target, "Dialing forward target in internal network");
                                                let Some(target_res) = crate::platform::connect_target(&target)
                                                    .cancel_with(&conn_token)
                                                    .await
                                                else {
                                                    return;
                                                };
                                                let target_conn = match target_res {
                                                    Ok(s) => s,
                                                    Err(e) => {
                                                        error!(target = %target, "Failed to connect to forward target: {:?}", e);
                                                        return;
                                                    }
                                                };

                                                let _ = serve(
                                                    PumpRequest::new(target_conn, stream),
                                                    PumpCtx {
                                                        cancel: conn_token,
                                                        heartbeat_interval: hb_interval,
                                                        idle_timeout,
                                                    },
                                                ).await;
                                            }

                                            // 2. Reverse mode: Either Control channel or Data channel pairing
                                            TunnelMode::Reverse => {
                                                // The reverse control plane is stream-scoped; it takes
                                                // its byte-stream view at this boundary.
                                                let Ok(stream) = stream.into_stream() else {
                                                    warn!("Reverse control plane requires a byte-stream channel");
                                                    return;
                                                };
                                                if let Some(session_id) = metadata.session_id {
                                                    // Data stream arrival: fulfill waiting session
                                                    if let Err(e) = session_mgr.fulfill(&session_id, stream) {
                                                        warn!(session = %session_id, "Failed to fulfill reverse session: {:?}", e);
                                                    }
                                                } else if metadata.pool_idle {
                                                    // Pre-warmed idle reverse stream arrival: push into inbound pool
                                                    if !session_mgr.push_idle_stream(metadata.target.as_ref(), stream) {
                                                        // Expected briefly while a control channel reconnects:
                                                        // the orphaned worker will recycle after a short cooldown.
                                                        tracing::debug!("Dropped reverse idle pool stream: no matching listener");
                                                    }
                                                } else {
                                                    // Control stream arrival: handle registration and listen on requested port
                                                    Self::handle_reverse_control(
                                                        stream,
                                                        session_mgr,
                                                        conn_token,
                                                        idle_timeout,
                                                        hb_interval,
                                                    )
                                                    .await;
                                                }
                                            }
                                        }
                                    });
                                }
                                Err(TunnelError::Shutdown) => {
                                    // Graceful shutdown requested; terminate the accept loop cleanly.
                                    break;
                                }
                                Err(_) if token.is_cancelled() => {
                                    // Shutdown in progress; ignore accept termination errors.
                                    break;
                                }
                                Err(e) => {
                                    error!(
                                        scheme = %spec_clone.scheme,
                                        bind = %spec_clone.bind_addr,
                                        "Server ingress listener accept failure: {:?}",
                                        e
                                    );
                                    // Do not return here: the listener owns
                                    // background tasks that must be awaited by
                                    // `shutdown()` on every exit path.
                                    accept_err = Some(e);
                                    break;
                                }
                            }
                        }
                    }
                }

                accept_slot.reset();
                conn_tracker.shutdown_and_wait().await;
                listener.shutdown().await;
                match accept_err {
                    Some(TunnelError::Shutdown) => Ok(()),
                    Some(_) if token.is_cancelled() => Ok(()),
                    Some(e) => Err(e),
                    None => Ok(()),
                }
            });
        }

        let mut exit_err: Option<TunnelError> = None;
        loop {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    debug!("Server ingress runner stopping on cancellation signal");
                    break;
                }
                res = tracker.join_next() => {
                    match res {
                        Some(Ok(Err(TunnelError::Shutdown))) => {
                            if tracker.is_empty() {
                                break;
                            }
                        }
                        Some(Ok(Err(e))) => {
                            if cancel_token.is_cancelled() {
                                if tracker.is_empty() {
                                    break;
                                }
                                continue;
                            }
                            error!("A server ingress listener terminated with error: {:?}", e);
                            if exit_err.is_none() {
                                exit_err = Some(e);
                            }
                            cancel_token.cancel();
                            break;
                        }
                        Some(Err(join_err)) => {
                            if cancel_token.is_cancelled() {
                                if tracker.is_empty() {
                                    break;
                                }
                                continue;
                            }
                            error!("A server ingress listener panicked: {:?}", join_err);
                            if exit_err.is_none() {
                                exit_err = Some(TunnelError::Internal(format!("Listener panic: {:?}", join_err)));
                            }
                            cancel_token.cancel();
                            break;
                        }
                        Some(Ok(Ok(()))) => {
                            if tracker.is_empty() {
                                break;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
            }
        }

        tracker.shutdown_and_wait().await;
        if let Some(e) = exit_err
            && !matches!(e, TunnelError::Shutdown)
        {
            return Err(e);
        }
        Ok(())
    }

    /// Handles a reverse control channel: binds requested port on server and notifies client.
    pub(crate) async fn handle_reverse_control(
        stream: Box<dyn crate::core::stream::TunnelStream>,
        session_mgr: Arc<SessionManager>,
        cancel_token: CancellationToken,
        idle_timeout: Duration,
        heartbeat_interval: Duration,
    ) {
        let sig_config = SignalingConfig {
            heartbeat_interval,
            idle_timeout,
        };
        let (signaling, mut sig_rx) =
            SignalingChannel::spawn(stream, sig_config, cancel_token.child_token());
        let router = SignalingRouter::new(signaling);

        // Await RegisterReverse bounded by idle_timeout
        let (listen_addr, target_addr, pool_size) = match timeout(idle_timeout, sig_rx.recv())
            .cancel_with(&cancel_token)
            .await
        {
            Some(Ok(Some(Ok(ControlMessage::RegisterReverse {
                listen_addr,
                target_addr,
                pool_size,
            })))) => (listen_addr, target_addr, pool_size),
            Some(Err(_)) => {
                warn!(
                    idle_timeout = ?idle_timeout,
                    "Reverse control channel registration timed out waiting for client request"
                );
                return;
            }
            Some(Ok(Some(other))) => {
                warn!(
                    "Expected RegisterReverse on reverse control channel, got: {:?}",
                    other
                );
                return;
            }
            _ => return,
        };

        debug!(addr = %listen_addr, target = %target_addr, "Binding reverse listening port on server");

        let mut frontend = match frontend::bind(&MappingSpec::tcp(&listen_addr, &target_addr)).await
        {
            Ok(l) => l,
            Err(e) => {
                warn!(addr = %listen_addr, "Failed to bind reverse listener: {}", e);
                let _ = router
                    .send(ControlMessage::RegisterAck {
                        listen_addr: listen_addr.clone(),
                        success: false,
                        error_msg: Some(e.to_string()),
                    })
                    .await;
                return;
            }
        };

        // Register the inbound pool BEFORE acknowledging. The client starts its
        // pre-warm workers as soon as RegisterAck arrives; a late registration
        // would discard the whole first generation and delay the first 0-RTT pair.
        // The soft cap is derived from the worker count the client negotiated,
        // so both ends size the same pool.
        let idle_pool = session_mgr.register_pool(&listen_addr, reverse_pool_soft_cap(pool_size));

        let ack_sent = router
            .send(ControlMessage::RegisterAck {
                listen_addr: listen_addr.clone(),
                success: true,
                error_msg: None,
            })
            .await;
        if ack_sent.is_err() {
            warn!(addr = %listen_addr, "Failed to send RegisterAck; dropping reverse listener");
            session_mgr.unregister_pool(&listen_addr, &idle_pool);
            return;
        }

        info!(addr = %listen_addr, "Bound reverse listener port");
        let mut local_tracker = TaskTracker::<()>::new(cancel_token.clone());
        let frame_timeout = reverse_frame_timeout(idle_timeout);
        let mut accept_slot: ReusableFutureRef<'static, FrontendOutput> =
            ReusableFutureRef::default();

        loop {
            if accept_slot.is_empty() {
                frontend.accept(&mut accept_slot);
            }

            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                Some(_) = local_tracker.join_next(), if !local_tracker.is_empty() => {}
                msg_opt = sig_rx.recv() => {
                    match msg_opt {
                        Some(Ok(ControlMessage::Ping)) | Some(Ok(ControlMessage::Pong)) => {}
                        Some(Ok(_)) => {}
                        _ => {
                            debug!(addr = %listen_addr, "Reverse control channel disconnected; checking for emergency promotion");
                            if let Some(promoted_stream) = idle_pool.promote_idle(idle_timeout).await
                                && let Some(sink) = sig_rx.event_sink()
                            {
                                info!(addr = %listen_addr, "Promoted idle pool stream to replacement signaling channel (0-RTT)");
                                let new_handle = SignalingChannel::spawn_with_event_sink(
                                    promoted_stream,
                                    sig_config,
                                    sink,
                                    cancel_token.child_token(),
                                );
                                router.attach(new_handle);
                                continue;
                            }
                            debug!(addr = %listen_addr, "No idle stream available for promotion; closing listener");
                            break;
                        }
                    }
                }
                accept_res = &mut accept_slot, if !accept_slot.is_empty() => {
                    accept_slot.reset();
                    match accept_res {
                        Ok(accepted) => {
                            let visitor_stream = accepted.channel;
                            let visitor_addr = accepted.peer;
                            // 1. FAST PATH (0-RTT Pool): only pair streams that are
                            // fresh and still alive. A stale generation must fall
                            // through to signaling rather than blackhole the visitor.
                            // The server owns the idle-stream lifetime; entries are
                            // removed under the pool lock, and activation is driven
                            // in the spawned task so WAN RTT never blocks the accept loop.
                            if let Some(mut candidate) = idle_pool.pop_live(idle_timeout).await {
                                debug!(
                                    visitor = %visitor_addr,
                                    "Paired reverse visitor with idle pooled stream (0-RTT)"
                                );
                                local_tracker.spawn_with_cancel(move |token| async move {
                                    let activate = async {
                                        tokio::io::AsyncWriteExt::write_all(
                                            &mut candidate,
                                            &[ACTIVATE_STREAM_MAGIC],
                                        )
                                        .await?;
                                        tokio::io::AsyncWriteExt::flush(&mut candidate).await
                                    };
                                    match tokio::time::timeout(frame_timeout, activate)
                                        .cancel_with(&token)
                                        .await
                                    {
                                                        Some(Ok(Ok(()))) => {
                                                            let _ = serve(
                                                                PumpRequest::new(visitor_stream, candidate),
                                                                PumpCtx {
                                                                    cancel: token,
                                                                    heartbeat_interval,
                                                                    idle_timeout,
                                                                },
                                                            ).await;
                                                        }
                                        Some(Ok(Err(e))) => {
                                            debug!("Discarding dead pooled reverse stream: {}", e);
                                        }
                                        Some(Err(_)) => {
                                            debug!("Discarding stalled pooled reverse stream");
                                        }
                                        None => {}
                                    }
                                });
                                continue;
                            }

                            // 2. SLOW PATH (Fallback): Allocate session_id and notify client via control channel
                            let session_id = format!(
                                "sess_{}_{}",
                                std::process::id(),
                                REVERSE_SESSION_COUNTER.fetch_add(1, Ordering::Relaxed)
                            );

                            debug!(
                                session = %session_id,
                                visitor = %visitor_addr,
                                "External visitor connected to reverse port, notifying client"
                            );

                            // Register in session manager before notifying client
                            let pair_rx = session_mgr.register(session_id.clone());

                            // Notify client
                            let notify_msg = ControlMessage::NewConnection {
                                session_id: session_id.clone(),
                                client_addr: visitor_addr.to_string(),
                            };

                            // Bounded signaling: a stalled or dead control channel
                            // must never block the accept loop and starve later
                            // visitors. Transport errors close the listener, but
                            // an individual timeout drops the session and continues
                            // without taking down the entire listening port.
                            let notify_res = router.send(notify_msg).await;
                            match notify_res {
                                Ok(()) => {}
                                Err(TunnelError::ControlProtocol(ref e)) if e.contains("timed out") => {
                                    warn!(session = %session_id, "Timed out signaling NewConnection; dropping session and continuing");
                                    session_mgr.remove(&session_id);
                                    continue;
                                }
                                Err(e) => {
                                    warn!(session = %session_id, "Failed to notify client over control channel: {:?}", e);
                                    session_mgr.remove(&session_id);
                                    break;
                                }
                            }

                            // Spawn pairing relay task
                            let session_mgr_clone = Arc::clone(&session_mgr);
                            let session_id_clone = session_id.clone();

                            local_tracker.spawn_with_cancel(move |token| async move {
                                match timeout(DEFAULT_SESSION_PAIR_TIMEOUT, pair_rx).await {
                                    Ok(Ok(data_stream)) => {
                                        debug!(session = %session_id_clone, "Reverse data stream paired, starting relay");
                                                        let _ = serve(
                                                            PumpRequest::new(visitor_stream, data_stream),
                                                            PumpCtx {
                                                                cancel: token,
                                                                heartbeat_interval,
                                                                idle_timeout,
                                                            },
                                                        ).await;
                                    }
                                    _ => {
                                        warn!(session = %session_id_clone, "Timed out waiting for reverse data stream pairing");
                                        session_mgr_clone.remove(&session_id_clone);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            if !cancel_token.is_cancelled() && !matches!(e, TunnelError::Shutdown) {
                                warn!(addr = %listen_addr, "Error accepting visitor on reverse port: {}", e);
                            }
                            break;
                        }
                    }
                }
            }
        }

        accept_slot.reset();

        // Immediately drop the listening socket and unregister the inbound pool.
        // This frees the port and pool key instantly so a reconnecting client
        // can re-bind the reverse listener without failing on "Address already in use",
        // even while existing visitor relay tasks are winding down gracefully.
        frontend.shutdown().await;
        drop(frontend);
        session_mgr.unregister_pool(&listen_addr, &idle_pool);

        local_tracker.shutdown_and_wait().await;
        debug!(addr = %listen_addr, "Reverse listener closed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::FramedControlStream;
    use std::time::Duration;

    #[tokio::test]
    async fn test_server_reverse_control_cancellation_under_silent_client() {
        let (client, server) = tokio::io::duplex(4096);
        let _server_holder = tokio::spawn(async move {
            let mut s = server;
            let mut buf = [0u8; 1024];
            while let Ok(n) = tokio::io::AsyncReadExt::read(&mut s, &mut buf).await {
                if n == 0 {
                    break;
                }
            }
        });

        let session_mgr = Arc::new(SessionManager::new());
        let cancel_token = CancellationToken::new();
        let cancel_clone = cancel_token.clone();

        let handle = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(
                Box::new(client),
                session_mgr,
                cancel_clone,
                Duration::from_secs(5),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel_token.cancel();

        let res = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            res.is_ok(),
            "handle_reverse_control hung on silent client despite cancellation token!"
        );
    }

    #[tokio::test]
    async fn test_server_reverse_control_idle_timeout_terminates_silent_channel() {
        let (client, _server) = tokio::io::duplex(4096);
        let session_mgr = Arc::new(SessionManager::new());
        let cancel_token = CancellationToken::new();

        // Silent client should time out on registration within idle_timeout
        let res = tokio::time::timeout(
            Duration::from_secs(2),
            ServerRunner::handle_reverse_control(
                Box::new(client),
                session_mgr,
                cancel_token,
                Duration::from_millis(50),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
            ),
        )
        .await;

        assert!(
            res.is_ok(),
            "handle_reverse_control did not time out on silent client within idle_timeout!"
        );
    }

    /// A peer that stops reading must not wedge the control channel forever.
    /// `SinkExt::send` flushes, so with a tiny duplex buffer the RegisterAck
    /// write blocks; before the fix this happened outside any timeout and the
    /// idle deadline (which lives in the enclosing select!) could never fire.
    #[tokio::test]
    async fn test_reverse_control_ack_send_is_bounded_by_stalled_peer() {
        use futures_util::SinkExt;

        let (client, server) = tokio::io::duplex(16);
        let mut ctrl_client = FramedControlStream::new(client);
        let session_mgr = Arc::new(SessionManager::new());
        let cancel_token = CancellationToken::new();

        let free_port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listen_addr = format!("127.0.0.1:{free_port}");

        let handle = tokio::spawn({
            let addr = listen_addr.clone();
            async move {
                ServerRunner::handle_reverse_control(
                    Box::new(server),
                    session_mgr,
                    cancel_token,
                    Duration::from_millis(300),
                    crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
                )
                .await;
                let _ = addr;
            }
        });

        ctrl_client
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: None,
            })
            .await
            .unwrap();

        // Deliberately never read the ack. The 16-byte duplex fills and the
        // server's flush stalls until the derived frame deadline fires.
        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("unbounded RegisterAck send wedged the reverse control channel")
            .unwrap();
    }

    /// Same class of stall inside the select! branch body: a peer floods PINGs
    /// but never reads the PONGs, filling the server->client buffer. The loop
    /// must break on the bounded Pong send instead of stopping visitor accepts.
    #[tokio::test]
    async fn test_reverse_control_pong_send_is_bounded_by_stalled_peer() {
        use futures_util::{SinkExt, StreamExt};

        let (client, server) = tokio::io::duplex(64);
        let mut ctrl_client = FramedControlStream::new(client);
        let session_mgr = Arc::new(SessionManager::new());
        let cancel_token = CancellationToken::new();

        let free_port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listen_addr = format!("127.0.0.1:{free_port}");

        let handle = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(
                Box::new(server),
                session_mgr,
                cancel_token,
                Duration::from_millis(300),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
            )
            .await;
        });

        ctrl_client
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: None,
            })
            .await
            .unwrap();
        let ack = ctrl_client.next().await.unwrap().unwrap();
        assert!(matches!(
            ack,
            ControlMessage::RegisterAck { success: true, .. }
        ));

        // Keep the client's read half idle from here on so PONGs accumulate.
        let pinger = tokio::spawn(async move {
            for _ in 0..200 {
                if ctrl_client.send(ControlMessage::Ping).await.is_err() {
                    break;
                }
            }
        });

        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("unbounded Pong send wedged the reverse control loop")
            .unwrap();
        let _ = pinger.await;
    }

    #[tokio::test]
    async fn test_server_runner_graceful_shutdown_returns_ok() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let cancel_token = CancellationToken::new();
        let server_config = ServerConfig {
            listen: vec![format!("ws://127.0.0.1:{}/test", port)],
            token: None,
            default_target: None,
            ..Default::default()
        };
        let runner = ServerRunner::from_config(server_config);
        let cancel_clone = cancel_token.clone();
        let handle = tokio::spawn(async move { runner.run(cancel_clone).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel_token.cancel();

        let res = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(res.is_ok(), "ServerRunner hung on shutdown");
        let run_res = res.unwrap().expect("ServerRunner task panicked");
        assert!(
            run_res.is_ok(),
            "ServerRunner should exit with Ok(()) on shutdown, got: {:?}",
            run_res
        );
    }

    #[tokio::test]
    async fn test_server_reverse_control_immediate_port_release_on_disconnect() {
        use futures_util::{SinkExt, StreamExt};

        let (client, server) = tokio::io::duplex(4096);
        let mut ctrl_client = FramedControlStream::new(client);
        let session_mgr = Arc::new(SessionManager::new());
        let cancel_token = CancellationToken::new();

        let listen_port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listen_addr = format!("127.0.0.1:{}", listen_port);

        let s_mgr = Arc::clone(&session_mgr);
        let c_token = cancel_token.clone();
        let l_addr = listen_addr.clone();

        let handle = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(
                Box::new(server),
                s_mgr,
                c_token,
                Duration::from_secs(30),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
            )
            .await;
        });

        // Send RegisterReverse
        ctrl_client
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: Some(2),
            })
            .await
            .unwrap();

        // Await RegisterAck
        let ack = ctrl_client.next().await.unwrap().unwrap();
        assert!(matches!(
            ack,
            ControlMessage::RegisterAck { success: true, .. }
        ));

        // Drop client control stream to trigger disconnect
        drop(ctrl_client);

        // Await handle termination
        let res = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            res.is_ok(),
            "handle_reverse_control should exit promptly on client disconnect"
        );

        // The bound port must be released immediately so a reconnecting client can rebind
        let rebind = crate::platform::GenericListener::bind(&l_addr).await;
        assert!(
            rebind.is_ok(),
            "Port must be freed immediately for rebinding"
        );
    }

    /// Under a burst of visitor connections, control frames (like Ping) must not
    /// be starved by `listener.accept()`. Prioritizing `ctrl_stream.next()` over
    /// `listener.accept()` ensures keepalive Pongs are returned promptly.
    #[tokio::test]
    async fn test_server_reverse_control_prioritizes_control_messages_over_visitor_burst() {
        use futures_util::{SinkExt, StreamExt};

        let (client, server) = tokio::io::duplex(4096);
        let mut ctrl_client = FramedControlStream::new(client);
        let session_mgr = Arc::new(SessionManager::new());
        let cancel_token = CancellationToken::new();

        let listen_port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listen_addr = format!("127.0.0.1:{}", listen_port);

        let s_mgr = Arc::clone(&session_mgr);
        let c_token = cancel_token.clone();
        let l_addr = listen_addr.clone();

        let handle = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(
                Box::new(server),
                s_mgr,
                c_token,
                Duration::from_secs(30),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
            )
            .await;
        });

        // Register reverse port without pre-warmed pool
        ctrl_client
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: None,
            })
            .await
            .unwrap();

        let ack = ctrl_client.next().await.unwrap().unwrap();
        assert!(matches!(
            ack,
            ControlMessage::RegisterAck { success: true, .. }
        ));

        // Connect multiple visitors to flood accept backlog
        let mut visitors = Vec::new();
        for _ in 0..5 {
            if let Ok(conn) = tokio::net::TcpStream::connect(&l_addr).await {
                visitors.push(conn);
            }
        }

        // Send a heartbeat Ping while accept loop is active
        ctrl_client.send(ControlMessage::Ping).await.unwrap();

        // Must receive Pong promptly without being starved by visitor accepts
        let mut received_pong = false;
        let timeout_pong = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(Ok(msg)) = ctrl_client.next().await {
                if matches!(msg, ControlMessage::Pong) {
                    received_pong = true;
                    break;
                }
            }
        })
        .await;

        assert!(
            timeout_pong.is_ok() && received_pong,
            "Control channel Ping was starved by visitor accept queue; no Pong received"
        );

        drop(visitors);
        cancel_token.cancel();
        let _ = handle.await;
    }

    /// When a single NewConnection notification times out under channel congestion,
    /// the server must NOT drop the reverse listener. It should remove the failed
    /// session and continue serving other incoming connections.
    #[tokio::test]
    async fn test_server_reverse_control_new_connection_timeout_does_not_close_listener() {
        use futures_util::{SinkExt, StreamExt};

        // Small duplex buffer so a single unread frame easily fills it
        let (client, server) = tokio::io::duplex(32);
        let mut ctrl_client = FramedControlStream::new(client);
        let session_mgr = Arc::new(SessionManager::new());
        let cancel_token = CancellationToken::new();

        let listen_port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listen_addr = format!("127.0.0.1:{}", listen_port);

        let s_mgr = Arc::clone(&session_mgr);
        let c_token = cancel_token.clone();
        let l_addr = listen_addr.clone();

        // 300 ms idle_timeout -> 100 ms frame_timeout
        let handle = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(
                Box::new(server),
                s_mgr,
                c_token,
                Duration::from_millis(300),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
            )
            .await;
        });

        // Register reverse port
        ctrl_client
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: None,
            })
            .await
            .unwrap();

        let ack = ctrl_client.next().await.unwrap().unwrap();
        assert!(matches!(
            ack,
            ControlMessage::RegisterAck { success: true, .. }
        ));

        // Deliberately stop reading on ctrl_client.
        // A visitor connects, triggering NewConnection send which stalls on the 32-byte duplex.
        let visitor = tokio::net::TcpStream::connect(&l_addr).await.unwrap();

        // Wait past the frame_timeout (100 ms) to let NewConnection send time out
        tokio::time::sleep(Duration::from_millis(250)).await;

        // The listener must still be alive (handle not finished!)
        assert!(
            !handle.is_finished(),
            "Reverse listener unexpectedly broke out and terminated on NewConnection timeout!"
        );

        drop(visitor);
        cancel_token.cancel();
        let _ = handle.await;
    }
}
