// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Targeted probe tests reproducing long-idle first-connection disconnect scenarios.
//! All comments and docstrings must be in English.

use culvert::backend::TransportConnector;
use culvert::consts::{DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT};
use culvert::core::error::TunnelError;
use culvert::core::stream::{BoxFuture, HeartbeatStatus, StreamCaps, TunnelStream};
use culvert::io::addr::L4Protocol;
use culvert::io::channel::IoChannel;
use culvert::pool::connection_pool::ReplenishingStreamPool;
use culvert::transport::TargetAddr;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio_util::sync::CancellationToken;

/// A mock stream belonging to a shared session.
/// When the shared session is invalidated/closed, writing to this stream fails with BrokenPipe.
struct SessionBoundStream {
    session_closed: Arc<AtomicBool>,
    is_dead: Arc<AtomicBool>,
}

impl AsyncRead for SessionBoundStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.session_closed.load(Ordering::SeqCst) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Shared session closed",
            )))
        } else {
            Poll::Pending
        }
    }
}

impl AsyncWrite for SessionBoundStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.session_closed.load(Ordering::SeqCst) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Shared session closed",
            )))
        } else {
            Poll::Ready(Ok(buf.len()))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.session_closed.load(Ordering::SeqCst) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Shared session closed",
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl TunnelStream for SessionBoundStream {
    fn caps(&self) -> StreamCaps {
        StreamCaps::default()
    }

    fn poll_check_alive(&mut self, _cx: &mut Context<'_>) -> Poll<HeartbeatStatus> {
        if self.session_closed.load(Ordering::SeqCst) || self.is_dead.load(Ordering::SeqCst) {
            Poll::Ready(HeartbeatStatus::Dead)
        } else {
            Poll::Ready(HeartbeatStatus::Alive)
        }
    }
}

struct MockSession {
    closed: Arc<AtomicBool>,
}

/// A connector simulating a multiplexed transport (such as QUIC or HTTP/3)
/// where multiple streams share an underlying cached session.
struct MultiplexedMockConnector {
    current_session: parking_lot::RwLock<Arc<MockSession>>,
    invalidations: Arc<AtomicUsize>,
}

impl TransportConnector for MultiplexedMockConnector {
    fn connect<'a>(
        &'a self,
        _target: &'a TargetAddr,
        _session_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        Box::pin(async {
            let session = Arc::clone(&*self.current_session.read());
            let stream = SessionBoundStream {
                session_closed: Arc::clone(&session.closed),
                is_dead: Arc::new(AtomicBool::new(false)),
            };
            Ok(IoChannel::from_boxed_stream(
                Box::new(stream) as Box<dyn TunnelStream>
            ))
        })
    }

    fn connect_pooled<'a>(
        &'a self,
        _target: &'a TargetAddr,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.connect_pooled_with_protocol(_target, L4Protocol::Tcp)
    }

    fn connect_with_protocol<'a>(
        &'a self,
        target: &'a TargetAddr,
        session_id: Option<&'a str>,
        _protocol: L4Protocol,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.connect(target, session_id)
    }

    fn connect_pooled_with_protocol<'a>(
        &'a self,
        target: &'a TargetAddr,
        _protocol: L4Protocol,
    ) -> BoxFuture<'a, Result<IoChannel, TunnelError>> {
        self.connect(target, None)
    }

    fn invalidate_broken_sessions(&self) {
        self.invalidations.fetch_add(1, Ordering::SeqCst);
        let mut guard = self.current_session.write();
        guard.closed.store(true, Ordering::SeqCst);
        *guard = Arc::new(MockSession {
            closed: Arc::new(AtomicBool::new(false)),
        });
    }
}

#[tokio::test]
async fn probe_pool_acquire_session_invalidation_kill_race() {
    let session = Arc::new(MockSession {
        closed: Arc::new(AtomicBool::new(false)),
    });
    let invalidations = Arc::new(AtomicUsize::new(0));

    let connector = Arc::new(MultiplexedMockConnector {
        current_session: parking_lot::RwLock::new(Arc::clone(&session)),
        invalidations: Arc::clone(&invalidations),
    });

    let cancel_token = CancellationToken::new();
    let pool = ReplenishingStreamPool::new_shared(
        connector,
        0, // target_size = 0 keeps background replenish from masking the test
        cancel_token.clone(),
        DEFAULT_HEARTBEAT_INTERVAL,
        DEFAULT_IDLE_TIMEOUT,
    );

    // Seed Stream 0: healthy stream sharing the session
    let stream0 = SessionBoundStream {
        session_closed: Arc::clone(&session.closed),
        is_dead: Arc::new(AtomicBool::new(false)),
    };
    pool.push_idle(Box::new(stream0));

    // Seed Stream 1: dead stream sharing the session (pushed second, so it is at the back of idle_streams)
    let stream1 = SessionBoundStream {
        session_closed: Arc::clone(&session.closed),
        is_dead: Arc::new(AtomicBool::new(true)),
    };
    pool.push_idle(Box::new(stream1));

    // Immediately call acquire so acquire inspects idle_streams directly!
    let mut acquired_stream = tokio::time::timeout(Duration::from_secs(2), pool.acquire(None))
        .await
        .expect("Acquire timed out")
        .expect("Acquire failed");

    // The caller attempts to write the forward activation packet:
    let write_res = acquired_stream.write_all(b"\xA5\x00\x00").await;

    println!("write_res: {:?}", write_res);
    println!("invalidations: {}", invalidations.load(Ordering::SeqCst));
    println!("session_closed: {}", session.closed.load(Ordering::SeqCst));

    // Now test: caller's write to acquired_stream must SUCCEED because
    // the pool avoided handing out the killed sibling and dialed a fresh healthy session!
    assert!(
        write_res.is_ok(),
        "Expected write_res to SUCCEED with freshly dialed session: {:?}",
        write_res
    );
    assert_eq!(
        invalidations.load(Ordering::SeqCst),
        1,
        "invalidate_broken_sessions was called exactly once"
    );

    cancel_token.cancel();
}

fn generate_test_quic_pki() -> (
    rustls::ServerConfig,
    rustls::pki_types::CertificateDer<'static>,
) {
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let cert_key = rcgen::generate_simple_self_signed(subject_alt_names).unwrap();

    let cert_der = cert_key.cert.der().to_vec();
    let key_der = cert_key.signing_key.serialize_der();

    let cert_chain = vec![rustls::pki_types::CertificateDer::from(cert_der.clone())];
    let private_key = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
    );

    let server_rustls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .unwrap();

    (
        server_rustls,
        rustls::pki_types::CertificateDer::from(cert_der),
    )
}

#[tokio::test]
async fn probe_quinn_tunnel_stream_poll_check_alive_blind_to_closed_stream() {
    use culvert::backend::accept_one;
    use culvert::config::model::{AppConfig, EndpointConfig, TlsConfig, TunnelMode};
    use culvert::dialer::endpoint::EndpointContext;
    use culvert::transport::h3::H3Connector;
    use culvert::transport::h3::listener::H3Listener;

    let port = {
        let t = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        t.local_addr().unwrap().port()
    };
    let listen_addr = format!("127.0.0.1:{}", port);
    let auth_token = "probe_quic_token";

    let (server_rustls, _) = generate_test_quic_pki();
    let mut listener =
        H3Listener::bind(&listen_addr, Some(auth_token), None, server_rustls).unwrap();

    let server_task = tokio::spawn(async move {
        // Accept the raw QUIC stream
        let (stream, metadata) = accept_one(&mut listener).await.unwrap();
        assert!(metadata.pool_idle);
        // Server immediately closes/drops the stream!
        drop(stream);
        // Keep listener alive so connection doesn't terminate
        tokio::time::sleep(Duration::from_millis(500)).await;
    });

    let endpoint_cfg = EndpointConfig::Detailed {
        url: format!("quic://{}/raw", listen_addr),
        dns: None,
        dialer: None,
        tls: Some(TlsConfig {
            insecure: Some(true),
            server_name: Some("localhost".to_string()),
            ..Default::default()
        }),
        http_headers: None,
    };

    let app_cfg = AppConfig::default();
    let endpoint_ctx = EndpointContext::build(&endpoint_cfg, &app_cfg).unwrap();
    let connector = H3Connector::new(&endpoint_ctx, auth_token, TunnelMode::Forward).unwrap();

    let dummy_target = TargetAddr::Domain("pool.idle".to_string(), 0);
    let mut client_stream = connector
        .connect_pooled(&dummy_target)
        .await
        .unwrap()
        .into_stream()
        .unwrap();

    // Wait for the server drop and QUIC RESET_STREAM / FIN packets to arrive at client:
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Now test: what does poll_check_alive report on client_stream?
    let alive_status = std::future::poll_fn(|cx| client_stream.poll_check_alive(cx)).await;
    println!("client_stream.poll_check_alive: {:?}", alive_status);

    // QuinnTunnelStream::poll_check_alive must detect the closed/reset stream and report Dead!
    assert_eq!(
        alive_status,
        HeartbeatStatus::Dead,
        "poll_check_alive must detect closed stream as Dead, not Alive!"
    );

    let _ = server_task.await;
}
