// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Unit and full pure-v2 E2E tests for `v2::server` (`RouterReverseSignaler`,
//! `ReverseSessionHub`, `ServerForwardWorkflow`, and `v2::ServerRunner`).
//! All comments and docstrings must be in English.

use crate::consts::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT, DEFAULT_RELAY_DRAIN_TIMEOUT,
};
use crate::control::{ControlMessage, SignalingChannel, SignalingConfig, SignalingRouter};
use crate::io::addr::{L4Protocol, PeerAddr, TargetAddr};
use crate::v2::TunnelPhase;
use crate::v2::reverse::ReverseSignaler;
use crate::v2::server::{ReverseSessionHub, RouterReverseSignaler, ServerRunner};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn router_reverse_signaler_pairs_and_cleans_up_on_cancellation() {
    let hub = Arc::new(ReverseSessionHub::new());
    let (ctrl_server, ctrl_client) = duplex(4096);
    let cancel = CancellationToken::new();
    let (sig_handle, _sig_server_rx) = SignalingChannel::spawn(
        Box::new(ctrl_server),
        SignalingConfig::default(),
        cancel.child_token(),
    );
    let (_client_handle, mut client_rx) = SignalingChannel::spawn(
        Box::new(ctrl_client),
        SignalingConfig::default(),
        cancel.child_token(),
    );

    let router = SignalingRouter::new(sig_handle);
    let disconnect_notify = Arc::new(Notify::new());
    let signaler = RouterReverseSignaler::new(
        router,
        Arc::clone(&hub),
        Duration::from_secs(2),
        disconnect_notify,
    );

    // 1. Successful pairing delivers data stream through ReverseSessionHub::fulfill.
    let pair_fut = signaler.request_pair(PeerAddr::Socket("127.0.0.1:42000".parse().unwrap()));
    let pair_task = tokio::spawn(pair_fut);

    let msg = tokio::time::timeout(Duration::from_secs(1), client_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let session_id = match msg {
        ControlMessage::NewConnection { session_id, .. } => session_id,
        other => panic!("unexpected control message: {other:?}"),
    };

    let (client_data, mut server_data) = duplex(1024);
    hub.fulfill(&session_id, Box::new(client_data)).unwrap();
    let mut paired = pair_task.await.unwrap().unwrap();
    paired.write_all(b"paired-ok").await.unwrap();
    let mut got = [0u8; 9];
    server_data.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, b"paired-ok");

    // 2. Cancelling request_pair future removes session_id via RAII guard.
    let drop_task =
        tokio::spawn(signaler.request_pair(PeerAddr::Socket("127.0.0.1:42001".parse().unwrap())));
    let cancelled_sid = match client_rx.recv().await.unwrap().unwrap() {
        ControlMessage::NewConnection { session_id, .. } => session_id,
        other => panic!("unexpected control message: {other:?}"),
    };
    drop_task.abort();
    let _ = drop_task.await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (dummy_a, _dummy_b) = duplex(64);
    assert!(hub.fulfill(&cancelled_sid, Box::new(dummy_a)).is_err());

    cancel.cancel();
}

#[tokio::test]
async fn pure_v2_server_and_clients_ws_forward_and_reverse_e2e() {
    use crate::config::model::{AppConfig, EndpointConfig, ServerConfig, TunnelMode};
    use crate::dialer::endpoint::EndpointContext;
    use crate::frontend;
    use crate::io::addr::MappingSpec;
    use crate::transport::ws::WsTransportDriver;
    use crate::transport::{TransportConnector, TransportRegistry};
    use crate::v2::forward::{ForwardRunner, ForwardWorkflow};
    use crate::v2::reverse::{ReverseClientConfig, ReverseClientRunner};
    use crate::v2::{CoordinatorConfig, TunnelPhase};

    let probe_echo = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let echo_port = probe_echo.local_addr().unwrap().port();
    probe_echo.set_nonblocking(true).unwrap();
    let echo_listener = tokio::net::TcpListener::from_std(probe_echo).unwrap();

    let probe_ws = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let ws_port = probe_ws.local_addr().unwrap().port();
    drop(probe_ws);

    let probe_fwd = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let fwd_port = probe_fwd.local_addr().unwrap().port();
    drop(probe_fwd);

    let probe_rev = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rev_port = probe_rev.local_addr().unwrap().port();
    drop(probe_rev);

    let server_cancel = CancellationToken::new();
    let echo_cancel = server_cancel.clone();
    let echo_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = echo_cancel.cancelled() => break,
                res = echo_listener.accept() => {
                    let Ok((mut sock, _)) = res else { break; };
                    tokio::spawn(async move {
                        let (mut rd, mut wr) = sock.split();
                        let _ = tokio::io::copy(&mut rd, &mut wr).await;
                    });
                }
            }
        }
    });

    let mut registry = TransportRegistry::new();
    registry.register(WsTransportDriver);
    let registry = Arc::new(registry);

    let auth_token = "pure-v2-server-e2e";
    let ws_listen = format!("127.0.0.1:{ws_port}");
    let v2_server = ServerRunner::new(
        ServerConfig {
            listen: vec![format!("ws://{ws_listen}/tunnel")],
            token: Some(auth_token.to_string()),
            default_target: None,
            ..Default::default()
        },
        Arc::clone(&registry),
        Arc::new(ReverseSessionHub::new()),
    );
    let server_tok = server_cancel.clone();
    let server_handle = tokio::spawn(async move { v2_server.run(server_tok).await });
    tokio::time::sleep(Duration::from_millis(80)).await;

    let endpoint_cfg = EndpointConfig::Simple(format!("ws://{ws_listen}/tunnel"));
    let endpoint_ctx = EndpointContext::build(&endpoint_cfg, &AppConfig::default()).unwrap();
    let echo_target = format!("127.0.0.1:{echo_port}");

    // 1. v2 Forward client (with unified Coordinator Idle + DialDriver 0-RTT activation) -> v2::ServerRunner
    let fwd_connector: Arc<dyn TransportConnector> = Arc::from(
        registry
            .create_connector(&endpoint_ctx, Some(auth_token), TunnelMode::Forward)
            .unwrap(),
    );
    let fwd_coord_cfg = CoordinatorConfig::forward_pooled(
        Arc::clone(&fwd_connector),
        2,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_RELAY_DRAIN_TIMEOUT,
    );
    let fwd_listen = format!("127.0.0.1:{fwd_port}");
    let fwd_fe = frontend::bind(&MappingSpec::new(
        L4Protocol::Tcp,
        &fwd_listen,
        &echo_target,
    ))
    .await
    .unwrap();
    let fwd_runner = ForwardRunner::new(
        fwd_fe,
        TargetAddr::Ip(echo_target.parse().unwrap()),
        L4Protocol::Tcp,
        ForwardWorkflow::new(
            Some(fwd_connector),
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
            DEFAULT_RELAY_DRAIN_TIMEOUT,
        )
        .with_pooled(true),
        fwd_coord_cfg,
    );
    let fwd_cancel = CancellationToken::new();
    let fwd_handle = tokio::spawn(fwd_runner.run(fwd_cancel.clone()));

    // 2. v2 Reverse client -> v2::ServerRunner
    let rev_connector: Arc<dyn TransportConnector> = Arc::from(
        registry
            .create_connector(&endpoint_ctx, Some(auth_token), TunnelMode::Reverse)
            .unwrap(),
    );
    let rev_listen = format!("127.0.0.1:{rev_port}");
    let rev_runner = ReverseClientRunner::new(
        ReverseClientConfig {
            listen_addr: rev_listen.clone(),
            target_addr: echo_target,
            protocol: L4Protocol::Tcp,
            pool_size: Some(2),
            tunnel_id: None,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            relay_idle_timeout: DEFAULT_IDLE_TIMEOUT,
            drain_timeout: DEFAULT_RELAY_DRAIN_TIMEOUT,
        },
        rev_connector,
    );
    let rev_cancel = CancellationToken::new();
    let rev_handle = tokio::spawn(rev_runner.run(rev_cancel.clone()));

    // Verify Forward through v2 server
    let mut fwd_sock = tokio::net::TcpStream::connect(&fwd_listen).await.unwrap();
    fwd_sock.write_all(b"v2-fwd-ok").await.unwrap();
    fwd_sock.shutdown().await.unwrap();
    let mut fwd_buf = [0u8; 9];
    tokio::time::timeout(Duration::from_secs(5), fwd_sock.read_exact(&mut fwd_buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&fwd_buf, b"v2-fwd-ok");

    // Verify Reverse (both 0-RTT fast path and second request) through v2 server
    let mut visitor1 = None;
    for _ in 0..40 {
        if let Ok(s) = tokio::net::TcpStream::connect(&rev_listen).await {
            visitor1 = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let mut visitor1 = visitor1.expect("v2 server reverse port must bind");
    tokio::time::sleep(Duration::from_millis(80)).await;
    visitor1.write_all(b"v2-rev-one").await.unwrap();
    visitor1.shutdown().await.unwrap();
    let mut rev_buf = [0u8; 10];
    tokio::time::timeout(Duration::from_secs(5), visitor1.read_exact(&mut rev_buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&rev_buf, b"v2-rev-one");

    let mut visitor2 = tokio::net::TcpStream::connect(&rev_listen).await.unwrap();
    visitor2.write_all(b"v2-rev-two").await.unwrap();
    visitor2.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), visitor2.read_exact(&mut rev_buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&rev_buf, b"v2-rev-two");

    fwd_cancel.cancel();
    rev_cancel.cancel();
    let fwd_summary = tokio::time::timeout(Duration::from_secs(3), fwd_handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let rev_summary = tokio::time::timeout(Duration::from_secs(3), rev_handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(fwd_summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(rev_summary.final_phase, TunnelPhase::Stopped);
    assert_eq!(fwd_summary.completed_jobs.len(), 1);
    assert!(rev_summary.completed_jobs.len() >= 2);

    server_cancel.cancel();
    tokio::time::timeout(Duration::from_secs(3), server_handle)
        .await
        .expect("v2 ServerRunner must exit cleanly")
        .unwrap()
        .unwrap();
    let _ = echo_handle.await;
}

struct NoopSignaler;

impl ReverseSignaler for NoopSignaler {
    fn request_pair(
        &self,
        _visitor_peer: PeerAddr,
    ) -> crate::core::stream::BoxFuture<
        'static,
        Result<Box<dyn crate::core::stream::TunnelStream>, crate::v2::TaskError>,
    > {
        Box::pin(async {
            Err(crate::v2::TaskError::StageFailed(
                "noop signaler".to_string(),
            ))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_register_and_last_unregister_preserve_live_coordinator() {
    let hub = Arc::new(ReverseSessionHub::new());
    let signaler: Arc<dyn ReverseSignaler> = Arc::new(NoopSignaler);

    for iter in 0..40usize {
        let tid = format!("race-tid-{}", iter % 4);
        let (target_a, guard_a) = hub.register_or_join_coordinator(
            L4Protocol::Tcp,
            "127.0.0.1:18080",
            Some(tid.clone()),
            Arc::clone(&signaler),
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_IDLE_TIMEOUT,
            DEFAULT_IDLE_TIMEOUT,
            4,
        );
        assert_eq!(
            target_a.coord.query_phase().await.unwrap(),
            TunnelPhase::Running
        );

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let bar_a = Arc::clone(&barrier);
        let bar_b = Arc::clone(&barrier);
        let hub_b = Arc::clone(&hub);
        let sig_b = Arc::clone(&signaler);
        let tid_b = tid.clone();

        let unregister_a = tokio::spawn(async move {
            bar_a.wait().await;
            guard_a.shutdown().await;
        });

        let register_b = tokio::spawn(async move {
            bar_b.wait().await;
            let (target_b, guard_b) = hub_b.register_or_join_coordinator(
                L4Protocol::Tcp,
                "127.0.0.1:18081",
                Some(tid_b),
                sig_b,
                DEFAULT_HEARTBEAT_INTERVAL,
                DEFAULT_IDLE_TIMEOUT,
                DEFAULT_IDLE_TIMEOUT,
                4,
            );
            // Whoever won the atomic shard critical section, B must ALWAYS hold a live Coordinator.
            let phase = target_b.coord.query_phase().await;
            assert_eq!(phase, Ok(TunnelPhase::Running));
            guard_b.shutdown().await;
        });

        unregister_a.await.unwrap();
        register_b.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grace_reattach_does_not_lose_slot_cleanup_notification() {
    use crate::control::FramedControlStream;
    use crate::v2::server::reverse::ReattachPayload;

    let hub = Arc::new(ReverseSessionHub::new());
    let addr = "127.0.0.1:19090";

    // Case 1: Old handle closes rx first, reattach starts, then old finish() runs -> must not hang.
    let mut g1 = hub.register_grace_listener(L4Protocol::Tcp, addr);
    g1.close_rx_only();

    let hub_c = Arc::clone(&hub);
    let cancel = CancellationToken::new();
    let cancel_c = cancel.clone();
    let reattach_task = tokio::spawn(async move {
        let (s, _c) = duplex(256);
        let payload = ReattachPayload {
            ctrl_stream: Box::new(FramedControlStream::new(Box::new(s))),
        };
        hub_c
            .try_reattach_grace_listener(L4Protocol::Tcp, addr, payload, &cancel_c)
            .await
    });

    tokio::task::yield_now().await;
    g1.finish();

    let res = tokio::time::timeout(Duration::from_millis(500), reattach_task)
        .await
        .expect("reattach must never stall on lost notify_waiters() wakeup")
        .unwrap();
    assert!(res.is_err());

    // Case 2: Race `close_rx_only()` -> `finish()` against `try_reattach_grace_listener()` across
    // 50 iterations to ensure the `tx.send() -> Err` to `notified` path never loses a wakeup.
    for _ in 0..50 {
        let mut g_old = hub.register_grace_listener(L4Protocol::Tcp, addr);
        g_old.close_rx_only();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let bar_1 = Arc::clone(&barrier);
        let bar_2 = Arc::clone(&barrier);
        let hub_r = Arc::clone(&hub);
        let cancel_r = cancel.clone();

        let finisher = tokio::spawn(async move {
            bar_1.wait().await;
            g_old.finish();
        });
        let waiter = tokio::spawn(async move {
            bar_2.wait().await;
            let (s, _c) = duplex(256);
            let payload = ReattachPayload {
                ctrl_stream: Box::new(FramedControlStream::new(Box::new(s))),
            };
            hub_r
                .try_reattach_grace_listener(L4Protocol::Tcp, addr, payload, &cancel_r)
                .await
        });

        finisher.await.unwrap();
        let outcome = tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .expect("concurrent finish/reattach race must converge promptly")
            .unwrap();
        assert!(outcome.is_err());
    }
}

#[tokio::test]
async fn grace_slot_replacement_does_not_remove_new_registration() {
    use crate::control::FramedControlStream;
    use crate::v2::server::reverse::ReattachPayload;

    let hub = Arc::new(ReverseSessionHub::new());
    let addr = "127.0.0.1:19091";

    let g1 = hub.register_grace_listener(L4Protocol::Tcp, addr);
    let mut g2 = hub.register_grace_listener(L4Protocol::Tcp, addr);

    // Cleaning up the superseded g1 must NOT remove g2's active registration.
    g1.finish();

    let (s, _peer) = duplex(256);
    let payload = ReattachPayload {
        ctrl_stream: Box::new(FramedControlStream::new(Box::new(s))),
    };
    hub.try_reattach_grace_listener(L4Protocol::Tcp, addr, payload, &CancellationToken::new())
        .await
        .map_err(|_| ())
        .expect("replacement grace slot g2 must still receive reattach payload");

    assert!(g2.recv().await.is_some());
    g2.finish();
}

#[tokio::test]
async fn shared_coordinator_survives_one_mapping_reconnect() {
    use crate::config::model::{AppConfig, EndpointConfig, ServerConfig, TunnelMode};
    use crate::control::{ControlMessage, FramedControlStream};
    use crate::dialer::endpoint::EndpointContext;
    use crate::transport::ws::WsTransportDriver;
    use crate::transport::{TransportConnector, TransportRegistry};
    use crate::v2::reverse::{ReverseClientConfig, ReverseClientRunner};
    use futures_util::{SinkExt, StreamExt};

    let probe_echo_a = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let echo_a_port = probe_echo_a.local_addr().unwrap().port();
    probe_echo_a.set_nonblocking(true).unwrap();
    let echo_a_listener = tokio::net::TcpListener::from_std(probe_echo_a).unwrap();

    let probe_echo_b = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let echo_b_port = probe_echo_b.local_addr().unwrap().port();
    probe_echo_b.set_nonblocking(true).unwrap();
    let echo_b_listener = tokio::net::TcpListener::from_std(probe_echo_b).unwrap();

    let probe_ws = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let ws_port = probe_ws.local_addr().unwrap().port();
    drop(probe_ws);
    let probe_rev_a = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rev_a_port = probe_rev_a.local_addr().unwrap().port();
    drop(probe_rev_a);
    let probe_rev_b = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rev_b_port = probe_rev_b.local_addr().unwrap().port();
    drop(probe_rev_b);

    let test_cancel = CancellationToken::new();
    for (listener, tag) in [(echo_a_listener, "A"), (echo_b_listener, "B")] {
        let c = test_cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = c.cancelled() => break,
                    res = listener.accept() => {
                        let Ok((mut sock, _)) = res else { break; };
                        tokio::spawn(async move {
                            let mut buf = Vec::new();
                            let _ = sock.read_to_end(&mut buf).await;
                            let reply = format!("[{tag}]{}", String::from_utf8_lossy(&buf));
                            let _ = sock.write_all(reply.as_bytes()).await;
                            let _ = sock.shutdown().await;
                        });
                    }
                }
            }
        });
    }

    let mut reg = TransportRegistry::new();
    reg.register(WsTransportDriver);
    let reg = Arc::new(reg);
    let hub = Arc::new(ReverseSessionHub::new());

    let ws_listen = format!("127.0.0.1:{ws_port}");
    let auth_tok = "rev-reconnect-shared";
    let server = ServerRunner::new(
        ServerConfig {
            listen: vec![format!("ws://{ws_listen}/tunnel")],
            token: Some(auth_tok.to_string()),
            idle_timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        },
        Arc::clone(&reg),
        Arc::clone(&hub),
    );
    let srv_tok = test_cancel.clone();
    let srv_handle = tokio::spawn(async move { server.run(srv_tok).await });
    tokio::time::sleep(Duration::from_millis(60)).await;

    let ep_cfg = EndpointConfig::Simple(format!("ws://{ws_listen}/tunnel"));
    let ep_ctx = EndpointContext::build(&ep_cfg, &AppConfig::default()).unwrap();
    let connector: Arc<dyn TransportConnector> = Arc::from(
        reg.create_connector(&ep_ctx, Some(auth_tok), TunnelMode::Reverse)
            .unwrap(),
    );

    let shared_tid = "shared-reconnect-tid".to_string();
    let rev_a_addr = format!("127.0.0.1:{rev_a_port}");
    let rev_b_addr = format!("127.0.0.1:{rev_b_port}");
    let target_a = format!("127.0.0.1:{echo_a_port}");
    let target_b = format!("127.0.0.1:{echo_b_port}");

    // Start Mapping B via ReverseClientRunner (stays online throughout Mapping A reconnect).
    let runner_b = ReverseClientRunner::new(
        ReverseClientConfig {
            listen_addr: rev_b_addr.clone(),
            target_addr: target_b.clone(),
            protocol: L4Protocol::Tcp,
            pool_size: Some(2),
            tunnel_id: Some(shared_tid.clone()),
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            idle_timeout: Duration::from_secs(2),
            relay_idle_timeout: Duration::from_secs(2),
            drain_timeout: DEFAULT_RELAY_DRAIN_TIMEOUT,
        },
        Arc::clone(&connector),
    );
    let cancel_b = CancellationToken::new();
    let handle_b = tokio::spawn(runner_b.run(cancel_b.clone()));

    // Register Mapping A explicitly over a manual control stream, then drop A and reconnect it.
    let ctrl_a1 = connector
        .connect(&TargetAddr::Domain("control.local".into(), 0), None)
        .await
        .unwrap()
        .into_stream()
        .unwrap();
    let mut framed_a1 = FramedControlStream::new(ctrl_a1);
    framed_a1
        .send(ControlMessage::RegisterReverse {
            listen_addr: rev_a_addr.clone(),
            target_addr: target_a.clone(),
            pool_size: Some(2),
            protocol: L4Protocol::Tcp,
            tunnel_id: Some(shared_tid.clone()),
        })
        .await
        .unwrap();
    assert!(matches!(
        framed_a1.next().await,
        Some(Ok(ControlMessage::RegisterAck { success: true, .. }))
    ));

    // Confirm shared Coordinator is present on the server hub.
    let (coord_before, _) = hub
        .lookup_idle_coordinator(Some(&shared_tid), L4Protocol::Tcp, None)
        .expect("shared Coordinator must exist");

    // Drop Mapping A's first control connection (enters grace) while Mapping B keeps running.
    drop(framed_a1);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Mapping B remains completely functional while Mapping A is in grace/reconnecting.
    let mut vis_b = tokio::net::TcpStream::connect(&rev_b_addr).await.unwrap();
    vis_b.write_all(b"during-A-drop").await.unwrap();
    vis_b.shutdown().await.unwrap();
    let mut out_b = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), vis_b.read_to_end(&mut out_b))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(String::from_utf8(out_b).unwrap(), "[B]during-A-drop");

    // Mapping A reconnects over grace reattach; shared Coordinator instance must remain the SAME.
    let ctrl_a2 = connector
        .connect(&TargetAddr::Domain("control.local".into(), 0), None)
        .await
        .unwrap()
        .into_stream()
        .unwrap();
    let mut framed_a2 = FramedControlStream::new(ctrl_a2);
    framed_a2
        .send(ControlMessage::RegisterReverse {
            listen_addr: rev_a_addr,
            target_addr: target_a,
            pool_size: Some(2),
            protocol: L4Protocol::Tcp,
            tunnel_id: Some(shared_tid.clone()),
        })
        .await
        .unwrap();
    assert!(matches!(
        framed_a2.next().await,
        Some(Ok(ControlMessage::RegisterAck { success: true, .. }))
    ));

    let (coord_after, _) = hub
        .lookup_idle_coordinator(Some(&shared_tid), L4Protocol::Tcp, None)
        .expect("shared Coordinator must still be registered");
    assert!(coord_before.same_coordinator(&coord_after));
    assert_eq!(
        coord_after.query_phase().await.unwrap(),
        TunnelPhase::Running
    );

    cancel_b.cancel();
    drop(framed_a2);
    let _ = tokio::time::timeout(Duration::from_secs(3), handle_b).await;
    test_cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), srv_handle).await;
}

#[tokio::test]
async fn last_mapping_shutdown_stops_shared_coordinator() {
    let hub = Arc::new(ReverseSessionHub::new());
    let signaler: Arc<dyn ReverseSignaler> = Arc::new(NoopSignaler);
    let tid = "shared-stop-tid".to_string();

    let (t1, g1) = hub.register_or_join_coordinator(
        L4Protocol::Tcp,
        "127.0.0.1:20001",
        Some(tid.clone()),
        Arc::clone(&signaler),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_IDLE_TIMEOUT,
        4,
    );
    let (t2, g2) = hub.register_or_join_coordinator(
        L4Protocol::Tcp,
        "127.0.0.1:20002",
        Some(tid.clone()),
        signaler,
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_IDLE_TIMEOUT,
        4,
    );
    assert!(t1.coord.same_coordinator(&t2.coord));

    // Shutting down g1 keeps shared Coordinator running for g2.
    g1.shutdown().await;
    assert_eq!(t2.coord.query_phase().await, Ok(TunnelPhase::Running));
    assert!(
        hub.lookup_idle_coordinator(Some(&tid), L4Protocol::Tcp, None)
            .is_some()
    );

    // Shutting down last listener g2 removes the entry and stops the shared Coordinator.
    g2.shutdown().await;
    assert!(
        hub.lookup_idle_coordinator(Some(&tid), L4Protocol::Tcp, None)
            .is_none()
    );
    assert!(t2.coord.query_phase().await.is_err());
}
