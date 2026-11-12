// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Ingress server daemon handling forward proxying and reverse tunnel pairing.
//! All comments and docstrings must be in English.

use futures_util::{SinkExt, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::futures::OwnedNotified;
use tokio::sync::mpsc;
use tokio::time::{Instant, Sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::common::{CancelFutureExt, TaskTracker};
use crate::config::model::{ServerConfig, TlsConfig, TunnelMode};
use crate::consts::{
    ACTIVATE_STREAM_ACK, DEFAULT_ACTIVATION_READ_TIMEOUT, DEFAULT_SESSION_PAIR_TIMEOUT,
    POOL_DUMMY_HOST, reverse_frame_timeout, reverse_grace_period, reverse_pool_soft_cap,
};
use crate::control::{
    ControlMessage, FramedControlStream, SignalingChannel, SignalingConfig, SignalingReceiver,
    SignalingRouter,
};
use crate::core::error::TunnelError;
use crate::core::serve::serve;
use crate::core::stream::TunnelStream;
use crate::frontend::{self, FrontendAccept, FrontendOutput};
use crate::io::addr::{L4Protocol, MappingSpec, PeerAddr};
use crate::io::pump::{PumpCtx, PumpRequest};
use crate::session::{ReverseGraceHandle, ReverseGraceReattach, ReversePoolGuard, SessionManager};
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
            let udp_idle_timeout = self
                .config
                .udp_idle_timeout
                .unwrap_or(crate::consts::DEFAULT_UDP_IDLE_TIMEOUT);

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
                        accept_res = &mut accept_slot => {
                            match accept_res {
                                Ok((stream, metadata)) => {
                                    let session_mgr = Arc::clone(&session_mgr);
                                    let default_target = default_target.clone();
                                    let idle_timeout = idle_timeout;
                                    let hb_interval = hb_interval;
                                    let udp_idle_timeout = udp_idle_timeout;

                                    conn_tracker.spawn_with_cancel(move |conn_token| async move {
                                        match metadata.mode {
                                            // 1. Forward mode: Dial remote target and relay
                                            TunnelMode::Forward => {
                                                let mut stream = stream;
                                                let mut dynamic_target: Option<TargetAddr> = None;
                                                let mut activated_proto: Option<L4Protocol> = None;

                                                // If this stream was pre-warmed as an idle connection in client pool,
                                                // wait for the client activation signal before touching the target service.
                                                if metadata.pool_idle {
                                                    let decoded = crate::pool::activation::decode_activation(
                                                        &mut stream,
                                                        &conn_token,
                                                        DEFAULT_ACTIVATION_READ_TIMEOUT,
                                                    )
                                                    .await;

                                                    match decoded {
                                                        Ok(Some(header)) => {
                                                            activated_proto = Some(header.protocol);
                                                            if let Some(t) = header.target {
                                                                dynamic_target = Some(t);
                                                            }
                                                        }
                                                        _ => return,
                                                    }
                                                }

                                                let effective_proto = activated_proto.unwrap_or(metadata.protocol);
                                                let relay_idle_timeout = match effective_proto {
                                                    L4Protocol::Tcp => idle_timeout,
                                                    L4Protocol::Udp => udp_idle_timeout,
                                                };

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

                                                debug!(target = %target, protocol = %effective_proto, "Dialing forward target in internal network");
                                                let Some(target_res) = crate::platform::connect_target_channel(&target, effective_proto)
                                                    .cancel_with(&conn_token)
                                                    .await
                                                else {
                                                    return;
                                                };
                                                let target_conn = match target_res {
                                                    Ok(s) => s,
                                                    Err(e) => {
                                                        error!(target = %target, protocol = %effective_proto, "Failed to connect to forward target: {:?}", e);
                                                        return;
                                                    }
                                                };

                                                let _ = serve(
                                                    PumpRequest::new(target_conn, stream),
                                                    PumpCtx {
                                                        cancel: conn_token,
                                                        heartbeat_interval: hb_interval,
                                                        idle_timeout: relay_idle_timeout,
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
                                                    if !session_mgr.push_idle_stream_with_tunnel(
                                                        metadata.tunnel_id.as_deref(),
                                                        metadata.protocol,
                                                        metadata.target.as_ref(),
                                                        stream,
                                                    ) {
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
                                                        udp_idle_timeout,
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

    /// Reads the initial `RegisterReverse` frame bounded by an absolute `idle_timeout` deadline.
    async fn read_initial_register_reverse(
        ctrl_stream: &mut FramedControlStream<Box<dyn TunnelStream>>,
        idle_timeout: Duration,
        cancel_token: &CancellationToken,
    ) -> Option<(String, String, Option<usize>, L4Protocol, Option<String>)> {
        let deadline = Instant::now() + idle_timeout;
        let frame_timeout = reverse_frame_timeout(idle_timeout);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                warn!(
                    idle_timeout = ?idle_timeout,
                    "Reverse control channel registration timed out waiting for client request"
                );
                return None;
            }
            match timeout(remaining, ctrl_stream.next())
                .cancel_with(cancel_token)
                .await
            {
                Some(Ok(Some(Ok(ControlMessage::RegisterReverse {
                    listen_addr,
                    target_addr,
                    pool_size,
                    protocol,
                    tunnel_id,
                })))) => return Some((listen_addr, target_addr, pool_size, protocol, tunnel_id)),
                Some(Ok(Some(Ok(ControlMessage::Ping)))) => {
                    let pong = timeout(frame_timeout, ctrl_stream.send(ControlMessage::Pong))
                        .cancel_with(cancel_token)
                        .await;
                    if !matches!(pong, Some(Ok(Ok(())))) {
                        return None;
                    }
                }
                Some(Err(_)) => {
                    warn!(
                        idle_timeout = ?idle_timeout,
                        "Reverse control channel registration timed out waiting for client request"
                    );
                    return None;
                }
                Some(Ok(Some(other))) => {
                    warn!(
                        "Expected RegisterReverse on reverse control channel, got: {:?}",
                        other
                    );
                    return None;
                }
                _ => return None,
            }
        }
    }

    /// Pops and promotes a live idle stream into a signaling channel (`0xA6`),
    /// resolving any concurrent `RegisterReverse` reattachment that arrived while
    /// probing the promoted stream. Returns `true` if signaling recovered.
    #[allow(clippy::too_many_arguments)]
    async fn try_promote_idle_signaling(
        listen_addr: &str,
        protocol: L4Protocol,
        idle_timeout: Duration,
        sig_config: SignalingConfig,
        cancel_token: &CancellationToken,
        session_mgr: &SessionManager,
        pool_guard: &mut ReversePoolGuard,
        router: &SignalingRouter,
        sig_rx: &SignalingReceiver,
        grace_handle: &mut Option<ReverseGraceHandle>,
        grace_deadline: &mut Option<Pin<Box<Sleep>>>,
        pool_notify: &mut Option<Pin<Box<OwnedNotified>>>,
        context_msg: &'static str,
    ) -> bool {
        // Arm the pool notifier BEFORE calling `promote_idle` so an inbound idle
        // stream pushed concurrently is never missed between the check and select!
        let next_notify = Box::pin(pool_guard.notified_owned());
        let frame_timeout = reverse_frame_timeout(idle_timeout);
        let promote_fut = async {
            timeout(frame_timeout, pool_guard.promote_idle(idle_timeout))
                .await
                .ok()
                .flatten()
        };
        let Some(promoted_stream) = promote_fut.cancel_with(cancel_token).await.flatten() else {
            if cancel_token.is_cancelled() {
                return false;
            }
            *pool_notify = Some(next_notify);
            return false;
        };

        if let Some(sink) = sig_rx.event_sink() {
            info!(addr = %listen_addr, "{}", context_msg);
            let new_handle = SignalingChannel::spawn_with_event_sink(
                promoted_stream,
                sig_config,
                sink,
                cancel_token.child_token(),
            );
            router.attach(new_handle);
            if let Some(reattach) = grace_handle.as_mut().and_then(|g| g.close_and_try_recv()) {
                Self::attach_reconnected_control(
                    reattach,
                    listen_addr,
                    protocol,
                    sig_config,
                    cancel_token,
                    session_mgr,
                    pool_guard,
                    router,
                    sig_rx,
                    grace_handle,
                    grace_deadline,
                    pool_notify,
                )
                .await;
                return true;
            }
            if let Some(g) = grace_handle.take() {
                g.finish();
            }
            *grace_deadline = None;
            *pool_notify = None;
            return true;
        }
        *pool_notify = Some(next_notify);
        false
    }

    /// Attempts immediate standby promotion or `0xA6` idle-stream promotion;
    /// if unavailable, retains the listener in the bounded grace period.
    #[allow(clippy::too_many_arguments)]
    async fn handle_signaling_disconnect(
        listen_addr: &str,
        protocol: L4Protocol,
        idle_timeout: Duration,
        grace_period: Duration,
        sig_config: SignalingConfig,
        cancel_token: &CancellationToken,
        session_mgr: &SessionManager,
        pool_guard: &mut ReversePoolGuard,
        router: &SignalingRouter,
        sig_rx: &SignalingReceiver,
        grace_handle: &mut Option<ReverseGraceHandle>,
        grace_deadline: &mut Option<Pin<Box<Sleep>>>,
        pool_notify: &mut Option<Pin<Box<OwnedNotified>>>,
    ) {
        if cancel_token.is_cancelled() {
            return;
        }
        if router.promote_standby_if_closed() {
            debug!(addr = %listen_addr, "Primary reverse signaling channel closed; failed over to standby");
            return;
        }

        // Register the grace-period handoff slot synchronously BEFORE awaiting
        // `promote_idle`, so an aggressive reconnect during the async probe cannot
        // miss the slot and hit transient EADDRINUSE.
        if grace_handle.is_none() {
            *grace_handle = Some(session_mgr.register_grace_listener(protocol, listen_addr));
        }

        debug!(addr = %listen_addr, "Reverse control channel disconnected; checking for emergency promotion");
        if pool_guard.tunnel_id.is_none()
            && Self::try_promote_idle_signaling(
                listen_addr,
                protocol,
                idle_timeout,
                sig_config,
                cancel_token,
                session_mgr,
                pool_guard,
                router,
                sig_rx,
                grace_handle,
                grace_deadline,
                pool_notify,
                "Promoted idle pool stream to replacement signaling channel (0-RTT)",
            )
            .await
        {
            return;
        }

        if cancel_token.is_cancelled() {
            return;
        }

        if grace_deadline.is_none() {
            info!(
                addr = %listen_addr,
                protocol = %protocol,
                grace = ?grace_period,
                "No idle stream available for promotion; retaining reverse listener for grace period"
            );
            *grace_deadline = Some(Box::pin(tokio::time::sleep(grace_period)));
        }
    }

    /// Reattaches a newly connected `RegisterReverse` control stream into an existing
    /// listener in its grace period without re-binding the frontend socket.
    #[allow(clippy::too_many_arguments)]
    async fn attach_reconnected_control(
        reattach: ReverseGraceReattach,
        listen_addr: &str,
        protocol: L4Protocol,
        sig_config: SignalingConfig,
        cancel_token: &CancellationToken,
        session_mgr: &SessionManager,
        pool_guard: &mut ReversePoolGuard,
        router: &SignalingRouter,
        sig_rx: &SignalingReceiver,
        grace_handle: &mut Option<ReverseGraceHandle>,
        grace_deadline: &mut Option<Pin<Box<Sleep>>>,
        pool_notify: &mut Option<Pin<Box<OwnedNotified>>>,
    ) {
        if let Some(g) = grace_handle.take() {
            g.finish();
        }
        *grace_deadline = None;
        *pool_notify = None;

        // Register the new pool generation BEFORE sending RegisterAck so the
        // client's freshly spawned pre-warm workers never race ahead of registration.
        let tid_opt = pool_guard.tunnel_id.clone();
        if let Some(ref tid) = tid_opt {
            pool_guard.replace_pool(session_mgr.register_tunnel_pool(
                tid,
                reverse_pool_soft_cap(reattach.pool_size),
            ));
        } else {
            pool_guard.replace_pool(session_mgr.register_pool(
                protocol,
                listen_addr,
                reverse_pool_soft_cap(reattach.pool_size),
            ));
        }

        if let Some(sink) = sig_rx.event_sink() {
            let new_handle = SignalingChannel::spawn_framed_with_event_sink(
                reattach.ctrl_stream,
                sig_config,
                sink,
                cancel_token.child_token(),
            );
            router.attach(new_handle);
            let ack_res = router
                .send(ControlMessage::RegisterAck {
                    listen_addr: listen_addr.to_string(),
                    success: true,
                    error_msg: None,
                })
                .cancel_with(cancel_token)
                .await;
            if matches!(ack_res, Some(Ok(()))) {
                info!(addr = %listen_addr, protocol = %protocol, "Reattached reverse control channel within grace period");
            } else if !cancel_token.is_cancelled() {
                warn!(addr = %listen_addr, "Failed to send RegisterAck on reattached reverse control channel");
            }
        }
    }

    /// Signals a new visitor connection to the client over the control channel (slow path).
    /// Used directly when the inbound pool is empty or as failover when 0-RTT pool stream activation fails.
    #[allow(clippy::too_many_arguments)]
    async fn signal_new_connection(
        visitor_stream: crate::io::channel::IoChannel,
        visitor_addr: PeerAddr,
        listen_addr: &str,
        protocol: L4Protocol,
        idle_timeout: Duration,
        relay_idle_timeout: Duration,
        heartbeat_interval: Duration,
        sig_config: SignalingConfig,
        grace_period: Duration,
        cancel_token: &CancellationToken,
        session_mgr: &Arc<SessionManager>,
        pool_guard: &mut ReversePoolGuard,
        router: &SignalingRouter,
        sig_rx: &SignalingReceiver,
        grace_handle: &mut Option<ReverseGraceHandle>,
        grace_deadline: &mut Option<Pin<Box<Sleep>>>,
        pool_notify: &mut Option<Pin<Box<OwnedNotified>>>,
        local_tracker: &mut TaskTracker<()>,
    ) {
        // SLOW PATH (Fallback): if signaling is temporarily in its grace window with an empty pool,
        // drop only this fallback visitor without tearing down the bound port.
        if !router.has_active_channel() {
            debug!(
                visitor = %visitor_addr,
                protocol = %protocol,
                "Dropping fallback reverse visitor while control channel is in grace period"
            );
            return;
        }

        // Allocate session_id and notify client via control channel
        let session_id = format!(
            "sess_{}_{}",
            std::process::id(),
            REVERSE_SESSION_COUNTER.fetch_add(1, Ordering::Relaxed)
        );

        debug!(
            session = %session_id,
            visitor = %visitor_addr,
            protocol = %protocol,
            "External visitor connected to reverse port, notifying client"
        );

        // Register in session manager with Drop guard before notifying client
        let (pair_rx, session_guard) = session_mgr.register_guarded(session_id.clone());

        // Notify client
        let notify_msg = ControlMessage::NewConnection {
            session_id: session_id.clone(),
            client_addr: visitor_addr.to_string(),
        };

        // Bounded signaling: must be constrained by cancel_token so shutdown never blocks the accept loop!
        let notify_res = router.send(notify_msg).cancel_with(cancel_token).await;
        let Some(notify_res) = notify_res else {
            // Cancelled: guard drops and removes session automatically.
            return;
        };

        match notify_res {
            Ok(()) => {}
            Err(TunnelError::ControlProtocol(ref e)) if e.contains("timed out") => {
                warn!(session = %session_id, "Timed out signaling NewConnection; dropping session and continuing");
                return;
            }
            Err(e) => {
                warn!(session = %session_id, "Failed to notify client over control channel: {:?}", e);
                drop(session_guard);
                Self::handle_signaling_disconnect(
                    listen_addr,
                    protocol,
                    idle_timeout,
                    grace_period,
                    sig_config,
                    cancel_token,
                    session_mgr,
                    pool_guard,
                    router,
                    sig_rx,
                    grace_handle,
                    grace_deadline,
                    pool_notify,
                )
                .await;
                return;
            }
        }

        // Spawn pairing relay task
        let session_id_clone = session_id.clone();
        local_tracker.spawn_with_cancel(move |token| async move {
            match timeout(DEFAULT_SESSION_PAIR_TIMEOUT, pair_rx).await {
                Ok(Ok(data_stream)) => {
                    scopeguard::ScopeGuard::into_inner(session_guard);
                    debug!(session = %session_id_clone, "Reverse data stream paired, starting relay");
                    let _ = serve(
                        PumpRequest::new(visitor_stream, data_stream),
                        PumpCtx {
                            cancel: token,
                            heartbeat_interval,
                            idle_timeout: relay_idle_timeout,
                        },
                    )
                    .await;
                }
                _ => {
                    warn!(session = %session_id_clone, "Timed out waiting for reverse data stream pairing");
                }
            }
        });
    }

    /// Dispatches an accepted visitor connection on a reverse listening port:
    /// tries 0-RTT pooled stream first (falling back to control signaling via failover_tx if activation fails),
    /// or signals the client over the control channel to dial a dedicated reverse data stream.
    #[allow(clippy::too_many_arguments)]
    async fn handle_accepted_reverse_visitor(
        accepted: FrontendAccept,
        listen_addr: &str,
        target_addr: &str,
        protocol: L4Protocol,
        idle_timeout: Duration,
        relay_idle_timeout: Duration,
        heartbeat_interval: Duration,
        frame_timeout: Duration,
        sig_config: SignalingConfig,
        grace_period: Duration,
        cancel_token: &CancellationToken,
        session_mgr: &Arc<SessionManager>,
        pool_guard: &mut ReversePoolGuard,
        router: &SignalingRouter,
        sig_rx: &SignalingReceiver,
        grace_handle: &mut Option<ReverseGraceHandle>,
        grace_deadline: &mut Option<Pin<Box<Sleep>>>,
        pool_notify: &mut Option<Pin<Box<OwnedNotified>>>,
        local_tracker: &mut TaskTracker<()>,
        failover_tx: &mpsc::Sender<(crate::io::channel::IoChannel, PeerAddr)>,
    ) {
        let visitor_stream = accepted.channel;
        let visitor_addr = accepted.peer;
        let target_parsed = crate::io::addr::parse_target_addr(target_addr).ok();
        let activation_pkt = crate::pool::activation::encode_activation_with_proto(
            target_parsed.as_ref(),
            protocol,
        );

        // 1. FAST PATH (0-RTT Pool): only pair streams that are fresh and still alive.
        // A stale generation must fall through to signaling rather than blackhole the visitor.
        // The server owns the idle-stream lifetime; entries are removed under the pool lock,
        // and activation is driven in the spawned task so WAN RTT never blocks the accept loop.
        if let Some(mut candidate) = pool_guard.pop_live(idle_timeout).await {
            debug!(
                visitor = %visitor_addr,
                protocol = %protocol,
                "Paired reverse visitor with idle pooled stream (0-RTT)"
            );
            let failover_tx = failover_tx.clone();
            local_tracker.spawn_with_cancel(move |token| async move {
                let activate = async {
                    tokio::io::AsyncWriteExt::write_all(
                        &mut candidate,
                        &activation_pkt,
                    )
                    .await?;
                    tokio::io::AsyncWriteExt::flush(&mut candidate).await?;
                    let mut ack = [0u8; 1];
                    tokio::io::AsyncReadExt::read_exact(&mut candidate, &mut ack).await?;
                    if ack[0] == ACTIVATE_STREAM_ACK {
                        Ok(())
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Invalid activation ACK byte",
                        ))
                    }
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
                                idle_timeout: relay_idle_timeout,
                            },
                        )
                        .await;
                    }
                    Some(activation_failure) => {
                        debug!(
                            "0-RTT pool stream activation failed ({:?}); falling back to control signaling",
                            activation_failure
                        );
                        drop(candidate);
                        let _ = failover_tx.send((visitor_stream, visitor_addr)).await;
                    }
                    None => {}
                }
            });
            return;
        }

        // 2. SLOW PATH (Fallback):
        Self::signal_new_connection(
            visitor_stream,
            visitor_addr,
            listen_addr,
            protocol,
            idle_timeout,
            relay_idle_timeout,
            heartbeat_interval,
            sig_config,
            grace_period,
            cancel_token,
            session_mgr,
            pool_guard,
            router,
            sig_rx,
            grace_handle,
            grace_deadline,
            pool_notify,
            local_tracker,
        )
        .await;
    }

    /// Handles a reverse control channel: binds requested port on server and notifies client.
    pub(crate) async fn handle_reverse_control(
        stream: Box<dyn crate::core::stream::TunnelStream>,
        session_mgr: Arc<SessionManager>,
        cancel_token: CancellationToken,
        idle_timeout: Duration,
        heartbeat_interval: Duration,
        udp_idle_timeout: Duration,
    ) {
        let mut ctrl_stream = FramedControlStream::new(stream);
        let Some((listen_addr, target_addr, pool_size, protocol, tunnel_id)) =
            Self::read_initial_register_reverse(&mut ctrl_stream, idle_timeout, &cancel_token)
                .await
        else {
            return;
        };

        // If the previous control channel for this listener dropped recently and
        // is still inside its 10s grace window, hand this stream directly to it so
        // the bound frontend socket (and active UDP NAT flows) never tear down.
        let reattach = ReverseGraceReattach {
            ctrl_stream,
            pool_size,
        };
        let mut ctrl_stream = match session_mgr
            .try_reattach_grace_listener(protocol, &listen_addr, reattach, &cancel_token)
            .await
        {
            Ok(()) => {
                info!(addr = %listen_addr, protocol = %protocol, "Reattached incoming reverse control stream to grace-period listener");
                return;
            }
            Err(returned) => returned.ctrl_stream,
        };

        if cancel_token.is_cancelled() {
            return;
        }

        debug!(addr = %listen_addr, target = %target_addr, protocol = %protocol, "Binding reverse listening port on server");

        let frame_timeout = reverse_frame_timeout(idle_timeout);
        let mut frontend = match frontend::bind(&MappingSpec::new(
            protocol,
            &listen_addr,
            &target_addr,
        ))
        .await
        {
            Ok(l) => l,
            Err(e) => {
                warn!(addr = %listen_addr, protocol = %protocol, "Failed to bind reverse listener: {}", e);
                let _ = timeout(
                    frame_timeout,
                    ctrl_stream.send(ControlMessage::RegisterAck {
                        listen_addr: listen_addr.clone(),
                        success: false,
                        error_msg: Some(e.to_string()),
                    }),
                )
                .cancel_with(&cancel_token)
                .await;
                return;
            }
        };

        // Register the inbound pool BEFORE acknowledging, wrapped in an RAII Drop guard
        // so pool registration is cleaned up even if this future is cancelled or aborted.
        let mut pool_guard = session_mgr.register_pool_guarded_with_tunnel(
            tunnel_id,
            protocol,
            &listen_addr,
            reverse_pool_soft_cap(pool_size),
        );

        let sig_config = SignalingConfig {
            heartbeat_interval,
            idle_timeout,
        };
        let (signaling, mut sig_rx) =
            SignalingChannel::spawn_framed(ctrl_stream, sig_config, cancel_token.child_token());
        let router = SignalingRouter::new(signaling);

        let ack_sent = router
            .send(ControlMessage::RegisterAck {
                listen_addr: listen_addr.clone(),
                success: true,
                error_msg: None,
            })
            .cancel_with(&cancel_token)
            .await;
        if !matches!(ack_sent, Some(Ok(()))) {
            warn!(addr = %listen_addr, "Failed to send RegisterAck; dropping reverse listener");
            frontend.shutdown().await;
            drop(frontend);
            return;
        }

        info!(addr = %listen_addr, protocol = %protocol, "Bound reverse listener port");
        let relay_idle_timeout = match protocol {
            L4Protocol::Tcp => idle_timeout,
            L4Protocol::Udp => udp_idle_timeout,
        };
        let grace_period = reverse_grace_period(idle_timeout);
        let mut grace_handle: Option<ReverseGraceHandle> = None;
        let mut grace_deadline: Option<Pin<Box<Sleep>>> = None;
        let mut pool_notify: Option<Pin<Box<OwnedNotified>>> = None;
        let mut local_tracker = TaskTracker::<()>::new(cancel_token.clone());
        let mut accept_slot: ReusableFutureRef<'static, FrontendOutput> =
            ReusableFutureRef::default();
        let (failover_tx, mut failover_rx) = mpsc::channel::<(
            crate::io::channel::IoChannel,
            PeerAddr,
        )>(reverse_pool_soft_cap(pool_size) + 16);

        loop {
            if accept_slot.is_empty() {
                frontend.accept(&mut accept_slot);
            }

            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                Some(_) = local_tracker.join_next(), if !local_tracker.is_empty() => {}
                Some((visitor_stream, visitor_addr)) = failover_rx.recv() => {
                    Self::signal_new_connection(
                        visitor_stream,
                        visitor_addr,
                        &listen_addr,
                        protocol,
                        idle_timeout,
                        relay_idle_timeout,
                        heartbeat_interval,
                        sig_config,
                        grace_period,
                        &cancel_token,
                        &session_mgr,
                        &mut pool_guard,
                        &router,
                        &sig_rx,
                        &mut grace_handle,
                        &mut grace_deadline,
                        &mut pool_notify,
                        &mut local_tracker,
                    )
                    .await;
                }
                Some(reattach) = async {
                    match grace_handle.as_mut() {
                        Some(g) => g.recv().await,
                        None => None,
                    }
                }, if grace_handle.is_some() => {
                    Self::attach_reconnected_control(
                        reattach,
                        &listen_addr,
                        protocol,
                        sig_config,
                        &cancel_token,
                        &session_mgr,
                        &mut pool_guard,
                        &router,
                        &sig_rx,
                        &mut grace_handle,
                        &mut grace_deadline,
                        &mut pool_notify,
                    )
                    .await;
                }
                _ = async {
                    if let Some(notify) = pool_notify.as_mut() {
                        notify.as_mut().await;
                    }
                }, if grace_handle.is_some() && pool_notify.is_some() => {
                    let _ = Self::try_promote_idle_signaling(
                        &listen_addr,
                        protocol,
                        idle_timeout,
                        sig_config,
                        &cancel_token,
                        &session_mgr,
                        &mut pool_guard,
                        &router,
                        &sig_rx,
                        &mut grace_handle,
                        &mut grace_deadline,
                        &mut pool_notify,
                        "Promoted newly arrived idle stream to signaling channel during grace period (0-RTT)",
                    )
                    .await;
                }
                _ = async {
                    if let Some(deadline) = grace_deadline.as_mut() {
                        deadline.as_mut().await;
                    }
                }, if grace_deadline.is_some() => {
                    if let Some(reattach) = grace_handle.as_mut().and_then(|g| g.close_and_try_recv()) {
                        Self::attach_reconnected_control(
                            reattach,
                            &listen_addr,
                            protocol,
                            sig_config,
                            &cancel_token,
                            &session_mgr,
                            &mut pool_guard,
                            &router,
                            &sig_rx,
                            &mut grace_handle,
                            &mut grace_deadline,
                            &mut pool_notify,
                        )
                        .await;
                        continue;
                    }
                    debug!(addr = %listen_addr, protocol = %protocol, "Reverse listener grace period expired; closing listener");
                    break;
                }
                msg_opt = sig_rx.recv(), if grace_handle.is_none() => {
                    match msg_opt {
                        Some(Ok(ControlMessage::Ping)) | Some(Ok(ControlMessage::Pong)) => {}
                        Some(Ok(_)) => {}
                        _ => {
                            Self::handle_signaling_disconnect(
                                &listen_addr,
                                protocol,
                                idle_timeout,
                                grace_period,
                                sig_config,
                                &cancel_token,
                                &session_mgr,
                                &mut pool_guard,
                                &router,
                                &sig_rx,
                                &mut grace_handle,
                                &mut grace_deadline,
                                &mut pool_notify,
                            )
                            .await;
                        }
                    }
                }
                accept_res = &mut accept_slot => {
                    match accept_res {
                        Ok(accepted) => {
                            Self::handle_accepted_reverse_visitor(
                                accepted,
                                &listen_addr,
                                &target_addr,
                                protocol,
                                idle_timeout,
                                relay_idle_timeout,
                                heartbeat_interval,
                                frame_timeout,
                                sig_config,
                                grace_period,
                                &cancel_token,
                                &session_mgr,
                                &mut pool_guard,
                                &router,
                                &sig_rx,
                                &mut grace_handle,
                                &mut grace_deadline,
                                &mut pool_notify,
                                &mut local_tracker,
                                &failover_tx,
                            )
                            .await;
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

        // Drop the listening socket and unregister the inbound pool BEFORE
        // finishing any grace registration, so any waiting reconnector only
        // binds after the OS socket has been released.
        frontend.shutdown().await;
        drop(frontend);
        drop(pool_guard);
        if let Some(g) = grace_handle.take() {
            g.finish();
        }

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
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
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
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
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
                    crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
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
                protocol: L4Protocol::Tcp,
                tunnel_id: None,
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
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
            )
            .await;
        });

        ctrl_client
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: None,
                protocol: L4Protocol::Tcp,
                tunnel_id: None,
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
    async fn test_server_reverse_control_grace_period_retains_port_and_reattaches() {
        use futures_util::{SinkExt, StreamExt};

        let (client1, server1) = tokio::io::duplex(4096);
        let mut ctrl_client1 = FramedControlStream::new(client1);
        let session_mgr = Arc::new(SessionManager::new());
        let cancel_token = CancellationToken::new();

        let listen_port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listen_addr = format!("127.0.0.1:{}", listen_port);

        let s_mgr1 = Arc::clone(&session_mgr);
        let c_token1 = cancel_token.clone();
        let handle1 = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(
                Box::new(server1),
                s_mgr1,
                c_token1,
                Duration::from_secs(30),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
            )
            .await;
        });

        ctrl_client1
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: Some(2),
                protocol: L4Protocol::Tcp,
                tunnel_id: None,
            })
            .await
            .unwrap();

        let ack1 = ctrl_client1.next().await.unwrap().unwrap();
        assert!(matches!(
            ack1,
            ControlMessage::RegisterAck { success: true, .. }
        ));

        // Drop primary control stream; listener must remain bound during the 10s grace period.
        drop(ctrl_client1);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !handle1.is_finished(),
            "handle_reverse_control should remain alive inside the grace window"
        );
        assert_eq!(session_mgr.grace_listener_count(), 1);
        assert!(
            crate::platform::GenericListener::bind(&listen_addr)
                .await
                .is_err(),
            "Port must remain retained during grace period"
        );

        // Reconnect a new control stream for the same listener and verify seamless reattach.
        let (client2, server2) = tokio::io::duplex(4096);
        let mut ctrl_client2 = FramedControlStream::new(client2);
        let s_mgr2 = Arc::clone(&session_mgr);
        let c_token2 = cancel_token.clone();
        let handle2 = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(
                Box::new(server2),
                s_mgr2,
                c_token2,
                Duration::from_secs(30),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
            )
            .await;
        });

        ctrl_client2
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: Some(2),
                protocol: L4Protocol::Tcp,
                tunnel_id: None,
            })
            .await
            .unwrap();

        let ack2 = ctrl_client2.next().await.unwrap().unwrap();
        assert!(matches!(
            ack2,
            ControlMessage::RegisterAck { success: true, .. }
        ));

        // Incoming control handler hands ownership to the active grace listener and exits cleanly.
        let reattach_res = tokio::time::timeout(Duration::from_secs(2), handle2).await;
        assert!(
            reattach_res.is_ok(),
            "Incoming reattach handler must exit after handing off stream"
        );
        assert!(
            !handle1.is_finished(),
            "Retained grace listener handler must continue serving with the new control stream"
        );
        assert_eq!(session_mgr.grace_listener_count(), 0);

        // Verify the retained TCP listener dispatches new visitors through ctrl_client2.
        let _visitor = tokio::net::TcpStream::connect(&listen_addr).await.unwrap();
        let next_msg = tokio::time::timeout(Duration::from_secs(2), ctrl_client2.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(next_msg, ControlMessage::NewConnection { .. }));

        cancel_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle1).await;
    }

    #[tokio::test]
    async fn test_server_reverse_control_grace_period_udp_retains_socket_and_reattaches() {
        use futures_util::{SinkExt, StreamExt};

        let (client1, server1) = tokio::io::duplex(4096);
        let mut ctrl_client1 = FramedControlStream::new(client1);
        let session_mgr = Arc::new(SessionManager::new());
        let cancel_token = CancellationToken::new();

        let listen_port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listen_addr = format!("127.0.0.1:{}", listen_port);

        let s_mgr1 = Arc::clone(&session_mgr);
        let c_token1 = cancel_token.clone();
        let handle1 = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(
                Box::new(server1),
                s_mgr1,
                c_token1,
                Duration::from_secs(30),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
            )
            .await;
        });

        ctrl_client1
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: Some(2),
                protocol: L4Protocol::Udp,
                tunnel_id: None,
            })
            .await
            .unwrap();

        let ack1 = ctrl_client1.next().await.unwrap().unwrap();
        assert!(matches!(
            ack1,
            ControlMessage::RegisterAck { success: true, .. }
        ));

        drop(ctrl_client1);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!handle1.is_finished());
        assert_eq!(session_mgr.grace_listener_count(), 1);

        let (client2, server2) = tokio::io::duplex(4096);
        let mut ctrl_client2 = FramedControlStream::new(client2);
        let s_mgr2 = Arc::clone(&session_mgr);
        let c_token2 = cancel_token.clone();
        let handle2 = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(
                Box::new(server2),
                s_mgr2,
                c_token2,
                Duration::from_secs(30),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
            )
            .await;
        });

        ctrl_client2
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: Some(2),
                protocol: L4Protocol::Udp,
                tunnel_id: None,
            })
            .await
            .unwrap();

        let ack2 = ctrl_client2.next().await.unwrap().unwrap();
        assert!(matches!(
            ack2,
            ControlMessage::RegisterAck { success: true, .. }
        ));

        let reattach_res = tokio::time::timeout(Duration::from_secs(2), handle2).await;
        assert!(
            reattach_res.is_ok(),
            "Incoming UDP reattach handler must exit after handing off stream"
        );
        assert!(
            !handle1.is_finished(),
            "Retained UDP grace listener handler must continue serving with the new control stream"
        );
        assert_eq!(session_mgr.grace_listener_count(), 0);

        // Verify the retained UDP socket dispatches new datagrams through ctrl_client2.
        let visitor = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        visitor
            .send_to(b"grace-udp-ping", &listen_addr)
            .await
            .unwrap();
        let next_msg = tokio::time::timeout(Duration::from_secs(2), ctrl_client2.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(next_msg, ControlMessage::NewConnection { .. }));

        cancel_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle1).await;
    }

    #[tokio::test]
    async fn test_server_reverse_control_grace_period_expires_and_releases_port() {
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

        // Bound grace period to 120ms via idle_timeout so expiry happens deterministically.
        let handle = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(
                Box::new(server),
                s_mgr,
                c_token,
                Duration::from_millis(120),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
            )
            .await;
        });

        ctrl_client
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: Some(2),
                protocol: L4Protocol::Tcp,
                tunnel_id: None,
            })
            .await
            .unwrap();

        let ack = ctrl_client.next().await.unwrap().unwrap();
        assert!(matches!(
            ack,
            ControlMessage::RegisterAck { success: true, .. }
        ));

        drop(ctrl_client);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!handle.is_finished());
        assert_eq!(session_mgr.grace_listener_count(), 1);

        let res = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            res.is_ok(),
            "handle_reverse_control should exit promptly after grace period expires"
        );
        assert_eq!(session_mgr.grace_listener_count(), 0);

        let rebind = crate::platform::GenericListener::bind(&l_addr).await;
        assert!(
            rebind.is_ok(),
            "Port must be released cleanly once the grace period expires"
        );
    }

    #[tokio::test]
    async fn test_server_reverse_control_grace_period_promotes_newly_pushed_idle_stream() {
        use futures_util::{SinkExt, StreamExt};
        use tokio::io::AsyncReadExt;

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
        let handle = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(
                Box::new(server),
                s_mgr,
                c_token,
                Duration::from_secs(30),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
            )
            .await;
        });

        ctrl_client
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: Some(2),
                protocol: L4Protocol::Tcp,
                tunnel_id: None,
            })
            .await
            .unwrap();

        let ack = ctrl_client.next().await.unwrap().unwrap();
        assert!(matches!(
            ack,
            ControlMessage::RegisterAck { success: true, .. }
        ));

        drop(ctrl_client);
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!handle.is_finished());
        assert_eq!(session_mgr.grace_listener_count(), 1);

        // Push a fresh idle stream during the grace window; server must promote it via 0xA6.
        let (mut client_idle, server_idle) = tokio::io::duplex(4096);
        assert!(session_mgr.push_idle_stream(L4Protocol::Tcp, None, Box::new(server_idle)));

        let mut magic = [0u8; 1];
        tokio::time::timeout(Duration::from_secs(2), client_idle.read_exact(&mut magic))
            .await
            .expect("Must receive 0xA6 promotion magic during grace window")
            .unwrap();
        assert_eq!(magic[0], crate::consts::ACTIVATE_STREAM_PROMOTE);

        let wait_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while session_mgr.grace_listener_count() > 0 && tokio::time::Instant::now() < wait_deadline
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(session_mgr.grace_listener_count(), 0);
        assert!(!handle.is_finished());

        // Promoted idle stream now serves FramedControlStream frames for incoming visitors.
        let mut promoted_ctrl = FramedControlStream::new(client_idle);
        let _visitor = tokio::net::TcpStream::connect(&listen_addr).await.unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(2), promoted_ctrl.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(msg, ControlMessage::NewConnection { .. }));

        cancel_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
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
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
            )
            .await;
        });

        // Register reverse port without pre-warmed pool
        ctrl_client
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: None,
                protocol: L4Protocol::Tcp,
                tunnel_id: None,
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
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
            )
            .await;
        });

        // Register reverse port
        ctrl_client
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: None,
                protocol: L4Protocol::Tcp,
                tunnel_id: None,
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

    /// Verifies that when the event loop is inside `router.send(notify_msg)` awaiting a stalled peer,
    /// triggering `cancel_token` cancels the loop immediately rather than waiting for the timeout,
    /// and that `PendingSessionGuard` and `ReversePoolGuard` cleanly release all registrations.
    #[tokio::test]
    async fn test_reverse_control_cancels_promptly_during_stalled_visitor_signaling() {
        use futures_util::{SinkExt, StreamExt};

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

        // 30-second idle timeout so any unconstrained await would hang for 10s
        let handle = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(
                Box::new(server),
                s_mgr,
                c_token,
                Duration::from_secs(30),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
            )
            .await;
        });

        // Register reverse port
        ctrl_client
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: None,
                protocol: L4Protocol::Tcp,
                tunnel_id: None,
            })
            .await
            .unwrap();

        let ack = ctrl_client.next().await.unwrap().unwrap();
        assert!(matches!(
            ack,
            ControlMessage::RegisterAck { success: true, .. }
        ));
        assert_eq!(session_mgr.pool_count(), 1);

        // Connect visitor. Duplex buffer is 32 bytes and client does not read,
        // so router.send(NewConnection) blocks immediately.
        let visitor = tokio::net::TcpStream::connect(&l_addr).await.unwrap();

        // Give event loop a few milliseconds to hit router.send
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Cancel the token! Must respond immediately (< 500ms), NOT 10s timeout!
        cancel_token.cancel();

        let exit_res = tokio::time::timeout(Duration::from_millis(500), handle).await;
        assert!(
            exit_res.is_ok(),
            "Event loop did not respond immediately to cancel_token while awaiting router.send!"
        );

        drop(visitor);
        // Both the pool and any pending session must be cleaned up cleanly on exit:
        assert_eq!(session_mgr.pool_count(), 0);
        assert_eq!(session_mgr.len(), 0);
    }

    /// Verifies that if `handle_reverse_control` task is aborted abruptly (simulating TaskTracker timeout fallback),
    /// RAII drop guards (`ReversePoolGuard`) guarantee the pool is unregistered from SessionManager.
    #[tokio::test]
    async fn test_reverse_control_future_abort_cleans_up_resources() {
        use futures_util::{SinkExt, StreamExt};

        let (client, server) = tokio::io::duplex(1024);
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

        let handle = tokio::spawn(async move {
            ServerRunner::handle_reverse_control(
                Box::new(server),
                s_mgr,
                c_token,
                Duration::from_secs(30),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
            )
            .await;
        });

        // Register reverse port
        ctrl_client
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: None,
                protocol: L4Protocol::Tcp,
                tunnel_id: None,
            })
            .await
            .unwrap();

        let ack = ctrl_client.next().await.unwrap().unwrap();
        assert!(matches!(
            ack,
            ControlMessage::RegisterAck { success: true, .. }
        ));
        assert_eq!(session_mgr.pool_count(), 1);

        // Abruptly abort the task (simulating tokio abort / TaskTracker forced abort)
        handle.abort();
        let _ = handle.await;

        // Pool MUST be unregistered from session_mgr via ReversePoolGuard Drop:
        assert_eq!(session_mgr.pool_count(), 0);
    }

    /// Verifies that when an idle stream in ReverseInboundPool is silently dead (half-open:
    /// server write succeeds into buffer, but reading ACTIVATE_STREAM_ACK times out),
    /// the fast-path activation task drops the dead stream and fails over via failover_tx
    /// to signal NewConnection over the control channel, so the visitor is NEVER dropped.
    #[tokio::test]
    async fn test_reverse_pool_half_open_stream_fails_over_to_signaling() {
        use futures_util::{SinkExt, StreamExt};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (client, server) = tokio::io::duplex(1024);
        let ctrl_client = FramedControlStream::new(client);
        let (mut ctrl_tx, mut ctrl_rx) = ctrl_client.split();
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
                Duration::from_millis(1500),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
            )
            .await;
        });

        // Register reverse port with pool_size = 4
        ctrl_tx
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: Some(4),
                protocol: L4Protocol::Tcp,
                tunnel_id: None,
            })
            .await
            .unwrap();

        let ack = ctrl_rx.next().await.unwrap().unwrap();
        assert!(matches!(
            ack,
            ControlMessage::RegisterAck { success: true, .. }
        ));

        // Spawn a background task sending periodic Pings on ctrl_tx to prevent signaling idle timeout
        let ping_token = cancel_token.child_token();
        let ping_task = tokio::spawn({
            let ping_token = ping_token.clone();
            async move {
                let mut interval = tokio::time::interval(Duration::from_millis(200));
                while !ping_token.is_cancelled() {
                    tokio::select! {
                        _ = ping_token.cancelled() => break,
                        _ = interval.tick() => {
                            if ctrl_tx.send(ControlMessage::Ping).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Create a half-open mock stream: server can write into it, but client never reads or writes ACK.
        let (_half_open_client, half_open_server) = tokio::io::duplex(1024);
        assert!(session_mgr.push_idle_stream(L4Protocol::Tcp, None, Box::new(half_open_server)));

        // A visitor connects to the reverse listening port
        let mut visitor = tokio::net::TcpStream::connect(&l_addr).await.unwrap();

        // The server fast-path task pops the half-open stream, writes 0xA5, flushes (succeeds into buffer),
        // and waits for 0xAC ACK. Since half_open_client never sends 0xAC, read_exact times out (1.5s).
        // Then, the visitor is routed through failover_tx into the select loop, which calls signal_new_connection.
        // Therefore, ctrl_rx MUST receive ControlMessage::NewConnection (skipping Pong messages)!
        let session_id = loop {
            let msg = tokio::time::timeout(Duration::from_secs(5), ctrl_rx.next())
                .await
                .expect("Timed out waiting for NewConnection failover")
                .expect("Control channel closed")
                .expect("Control decode error");
            match msg {
                ControlMessage::NewConnection { session_id, .. } => break session_id,
                ControlMessage::Pong => continue,
                other => panic!("Expected NewConnection, got {:?}", other),
            }
        };

        // The client connects a dedicated reverse-data stream to complete the pair:
        let (data_client, data_server) = tokio::io::duplex(1024);
        session_mgr
            .fulfill(&session_id, Box::new(data_server))
            .unwrap();

        // The visitor sends data and data_client echoes back:
        visitor.write_all(b"hello via failover").await.unwrap();
        visitor.flush().await.unwrap();

        let mut data_client = data_client;
        let mut recv_buf = vec![0u8; 18];
        data_client.read_exact(&mut recv_buf).await.unwrap();
        assert_eq!(&recv_buf, b"hello via failover");

        data_client.write_all(b"pong from backend").await.unwrap();
        data_client.flush().await.unwrap();

        let mut reply_buf = vec![0u8; 17];
        visitor.read_exact(&mut reply_buf).await.unwrap();
        assert_eq!(&reply_buf, b"pong from backend");

        drop(visitor);
        drop(data_client);
        cancel_token.cancel();
        let _ = ping_task.await;
        let _ = handle.await;
    }

    /// Verifies that when an idle stream in ReverseInboundPool actively responds with 0xAC ACK,
    /// 0-RTT fast path succeeds immediately without control signaling.
    #[tokio::test]
    async fn test_reverse_pool_active_ack_fast_path() {
        use futures_util::{SinkExt, StreamExt};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (client, server) = tokio::io::duplex(1024);
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
                Duration::from_millis(300),
                crate::consts::DEFAULT_HEARTBEAT_INTERVAL,
                crate::consts::DEFAULT_UDP_IDLE_TIMEOUT,
            )
            .await;
        });

        ctrl_client
            .send(ControlMessage::RegisterReverse {
                listen_addr: listen_addr.clone(),
                target_addr: "127.0.0.1:9099".to_string(),
                pool_size: Some(4),
                protocol: L4Protocol::Tcp,
                tunnel_id: None,
            })
            .await
            .unwrap();

        let ack = ctrl_client.next().await.unwrap().unwrap();
        assert!(matches!(
            ack,
            ControlMessage::RegisterAck { success: true, .. }
        ));

        // Create an active worker stream that reads 0xA5 and echoes ACTIVATE_STREAM_ACK (0xAC)
        let (mut active_client, active_server) = tokio::io::duplex(1024);
        assert!(session_mgr.push_idle_stream(L4Protocol::Tcp, None, Box::new(active_server)));

        // Worker background task: decodes activation and echoes 0xAC
        tokio::spawn(async move {
            let header = crate::pool::activation::decode_activation(
                &mut active_client,
                &CancellationToken::new(),
                Duration::from_secs(5),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(header.protocol, L4Protocol::Tcp);
            assert_eq!(
                header.target.map(|t| t.to_string()),
                Some("127.0.0.1:9099".to_string())
            );
            active_client
                .write_all(&[ACTIVATE_STREAM_ACK])
                .await
                .unwrap();
            active_client.flush().await.unwrap();

            // Then echo data
            let mut buf = [0u8; 5];
            active_client.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping!");
            active_client.write_all(b"pong!").await.unwrap();
            active_client.flush().await.unwrap();
        });

        // Visitor connects
        let mut visitor = tokio::net::TcpStream::connect(&l_addr).await.unwrap();
        visitor.write_all(b"ping!").await.unwrap();
        visitor.flush().await.unwrap();

        let mut reply = [0u8; 5];
        visitor.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"pong!");

        drop(visitor);
        cancel_token.cancel();
        let _ = handle.await;
    }
}
