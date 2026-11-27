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
