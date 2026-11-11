// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Rigorous tests verifying dual-channel emergency promotion and command deduplication.
//! All comments and docstrings must be in English.

use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::io::duplex;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use culvert::consts::ACTIVATE_STREAM_PROMOTE;
use culvert::control::{
    ControlMessage, FramedControlStream, SignalingChannel, SignalingConfig, SignalingRouter,
};
use culvert::session::manager::ReverseInboundPool;

#[tokio::test]
async fn test_dual_channel_strictly_no_duplicate_dispatch() {
    let cancel = CancellationToken::new();
    let (ch1_tx, ch1_rx) = duplex(4096);
    let (ch2_tx, ch2_rx) = duplex(4096);

    let cfg = SignalingConfig {
        heartbeat_interval: Duration::from_secs(15),
        idle_timeout: Duration::from_secs(30),
    };

    let (h1, _rx1) = SignalingChannel::spawn(ch1_tx, cfg, cancel.child_token());
    let (h2, _rx2) = SignalingChannel::spawn(ch2_tx, cfg, cancel.child_token());

    let router = SignalingRouter::new(h1);
    router.attach(h2);

    let mut peer1 = FramedControlStream::new(ch1_rx);
    let mut peer2 = FramedControlStream::new(ch2_rx);

    let msg = ControlMessage::NewConnection {
        session_id: "strict_sess_1".into(),
        client_addr: "10.0.0.1:80".into(),
    };

    // Dispatch message through router
    router.send(msg.clone()).await.expect("send must succeed");

    // Primary channel must receive the message
    let received = timeout(Duration::from_millis(500), peer1.next())
        .await
        .expect("peer1 timeout")
        .expect("stream ended")
        .expect("control decode error");
    assert_eq!(received, msg);

    // Standby channel must NOT receive duplicate command dispatch
    let standby_res = timeout(Duration::from_millis(150), peer2.next()).await;
    assert!(
        standby_res.is_err(),
        "Standby channel must not receive duplicate command"
    );

    cancel.cancel();
}

#[tokio::test]
async fn test_dual_channel_burst_deduplication() {
    let cancel = CancellationToken::new();
    let (ch1_tx, ch1_rx) = duplex(16384);
    let (ch2_tx, ch2_rx) = duplex(16384);

    let cfg = SignalingConfig {
        heartbeat_interval: Duration::from_secs(15),
        idle_timeout: Duration::from_secs(30),
    };

    let (_h1, mut unified_rx) = SignalingChannel::spawn(ch1_rx, cfg, cancel.child_token());
    let _h2 = SignalingChannel::spawn_with_event_sink(
        ch2_rx,
        cfg,
        unified_rx.event_sink().expect("sink"),
        cancel.child_token(),
    );

    let mut client1 = FramedControlStream::new(ch1_tx);
    let mut client2 = FramedControlStream::new(ch2_tx);

    let session_id = "burst_shared_session_42".to_string();

    // Keep client streams alive so channels do not close prematurely
    for _ in 0..50 {
        let msg = ControlMessage::NewConnection {
            session_id: session_id.clone(),
            client_addr: "1.1.1.1:100".into(),
        };
        client1.send(msg).await.unwrap();
    }
    for _ in 0..50 {
        let msg = ControlMessage::NewConnection {
            session_id: session_id.clone(),
            client_addr: "2.2.2.2:200".into(),
        };
        client2.send(msg).await.unwrap();
    }

    // Application reader must receive EXACTLY ONE NewConnection for this session_id
    let first = timeout(Duration::from_millis(500), unified_rx.recv_msg())
        .await
        .expect("timeout waiting for first")
        .expect("recv error")
        .expect("no message");

    match first {
        ControlMessage::NewConnection {
            session_id: ref s, ..
        } => {
            assert_eq!(s, &session_id);
        }
        other => panic!("Unexpected message: {:?}", other),
    }

    // All 99 other duplicate frames must be completely suppressed by the deduplication filter
    let second = timeout(Duration::from_millis(150), unified_rx.recv_msg()).await;
    match second {
        Err(_) => {}       // Timed out as expected: zero duplicate frames were delivered!
        Ok(Ok(None)) => {} // Clean EOF
        Ok(Ok(Some(dup))) => {
            panic!(
                "Erroneous duplicate command delivered across channels: {:?}",
                dup
            );
        }
        Ok(Err(e)) => panic!("Unexpected channel error: {:?}", e),
    }

    drop(client1);
    drop(client2);
    cancel.cancel();
}

#[tokio::test]
async fn test_inbound_pool_stream_emergency_promotion_e2e() {
    let pool = ReverseInboundPool::new(8);
    let (client, mut server) = duplex(1024);

    assert!(pool.push(Box::new(client)));
    assert_eq!(pool.len(), 1);

    // Promote idle connection to signaling stream
    let promoted = pool
        .promote_idle(Duration::from_secs(5))
        .await
        .expect("promotion should succeed");
    assert_eq!(pool.len(), 0);

    // Peer reading the stream must detect the 0xA6 promotion byte
    let mut magic = [0u8; 1];
    tokio::io::AsyncReadExt::read_exact(&mut server, &mut magic)
        .await
        .expect("read magic");
    assert_eq!(magic[0], ACTIVATE_STREAM_PROMOTE);

    // Both ends can now safely establish a new SignalingChannel over the promoted stream
    let cancel = CancellationToken::new();
    let cfg = SignalingConfig {
        heartbeat_interval: Duration::from_millis(50),
        idle_timeout: Duration::from_millis(500),
    };

    let (srv_handle, _srv_rx) = SignalingChannel::spawn(promoted, cfg, cancel.child_token());
    let mut client_framed = FramedControlStream::new(server);

    // Verify control communication over promoted stream
    let test_msg = ControlMessage::NewConnection {
        session_id: "promoted_stream_test".into(),
        client_addr: "127.0.0.1:9999".into(),
    };
    srv_handle
        .send(test_msg.clone())
        .await
        .expect("send over promoted");

    let received = client_framed.next().await.unwrap().unwrap();
    assert_eq!(received, test_msg);

    cancel.cancel();
}

#[tokio::test]
async fn test_dual_channel_failover_maintains_deduplication_integrity() {
    let cancel = CancellationToken::new();
    let (ch1_tx, ch1_rx) = duplex(4096);
    let (ch2_tx, ch2_rx) = duplex(4096);

    let cfg = SignalingConfig {
        heartbeat_interval: Duration::from_secs(15),
        idle_timeout: Duration::from_secs(30),
    };

    let (h1, mut rx) = SignalingChannel::spawn(ch1_rx, cfg, cancel.child_token());
    let h2 = SignalingChannel::spawn_with_event_sink(
        ch2_rx,
        cfg,
        rx.event_sink().expect("sink"),
        cancel.child_token(),
    );

    let router = SignalingRouter::new(h1);
    router.attach(h2);

    let mut peer1 = FramedControlStream::new(ch1_tx);
    let mut peer2 = FramedControlStream::new(ch2_tx);

    // 1. Send command via Primary
    let msg1 = ControlMessage::NewConnection {
        session_id: "sess_pre_failover".into(),
        client_addr: "10.0.0.1:1".into(),
    };
    router.send(msg1.clone()).await.expect("send msg1");
    let r1 = peer1.next().await.unwrap().unwrap();
    assert_eq!(r1, msg1);

    // 2. Simulate Primary connection break
    drop(peer1);

    // 3. Send command via router after failover
    let msg2 = ControlMessage::NewConnection {
        session_id: "sess_post_failover".into(),
        client_addr: "10.0.0.2:2".into(),
    };
    router.send(msg2.clone()).await.expect("send msg2");
    let r2 = peer2.next().await.unwrap().unwrap();
    assert_eq!(r2, msg2);

    // 4. If a stale/retransmitted msg1 arrives on Channel 2, receiver must reject it
    peer2
        .send(msg1.clone())
        .await
        .expect("send stale msg1 on ch2");
    peer2.send(msg2.clone()).await.expect("send msg2 on ch2");

    // Receiver should deliver msg1 once, msg2 once, and no duplicates!
    let delivered_1 = rx.recv_msg().await.unwrap().unwrap();
    assert_eq!(delivered_1, msg1);

    let delivered_2 = rx.recv_msg().await.unwrap().unwrap();
    assert_eq!(delivered_2, msg2);

    // Nothing further should be delivered
    let trailing = timeout(Duration::from_millis(150), rx.recv_msg()).await;
    assert!(
        trailing.is_err(),
        "Duplicate commands must not be delivered"
    );

    cancel.cancel();
}
