//! Unit tests for multiplexing framing, session driver, and stream isolation.
//! All comments and docstrings must be in English.

use bytes::Bytes;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio_util::sync::CancellationToken;

use crate::mux::frame::MuxFrame;
use crate::mux::session::MuxSession;

#[tokio::test]
async fn test_mux_bidirectional_single_stream() {
    let (pipe_client, pipe_server) = duplex(65536);
    let cancel = CancellationToken::new();

    let client_session = MuxSession::new(Box::new(pipe_client), true, cancel.clone());
    let server_session = MuxSession::new(Box::new(pipe_server), false, cancel.clone());

    // Client opens stream 1
    let mut client_stream = client_session.open_stream().await.unwrap();
    assert_eq!(client_stream.stream_id(), 1);

    // Server accepts stream 1
    let mut server_stream = server_session.accept_stream().await.unwrap();
    assert_eq!(server_stream.stream_id(), 1);

    // Client writes to server
    client_stream.write_all(b"ping from client").await.unwrap();

    let mut buf = vec![0u8; 16];
    server_stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping from client");

    // Server writes to client
    server_stream.write_all(b"pong from server").await.unwrap();

    let mut buf2 = vec![0u8; 16];
    client_stream.read_exact(&mut buf2).await.unwrap();
    assert_eq!(&buf2, b"pong from server");

    // Graceful client FIN
    client_stream.shutdown().await.unwrap();

    // Server reads EOF
    let mut eof_buf = [0u8; 10];
    let n = server_stream.read(&mut eof_buf).await.unwrap();
    assert_eq!(n, 0);

    cancel.cancel();
}

#[tokio::test]
async fn test_mux_multiple_concurrent_streams() {
    let (pipe_client, pipe_server) = duplex(131072);
    let cancel = CancellationToken::new();

    let client_session = MuxSession::new(Box::new(pipe_client), true, cancel.clone());
    let server_session = MuxSession::new(Box::new(pipe_server), false, cancel.clone());

    const STREAM_COUNT: usize = 5;

    // Spawn server accept task
    let server_session_clone = server_session.clone();
    let server_acceptor = tokio::spawn(async move {
        let mut streams = Vec::new();
        for _ in 0..STREAM_COUNT {
            if let Some(mut stream) = server_session_clone.accept_stream().await {
                let h = tokio::spawn(async move {
                    let mut data = Vec::new();
                    stream.read_to_end(&mut data).await.unwrap();
                    // Echo back in reverse
                    data.reverse();
                    stream.write_all(&data).await.unwrap();
                    stream.shutdown().await.unwrap();
                });
                streams.push(h);
            }
        }
        for h in streams {
            h.await.unwrap();
        }
    });

    // Client opens STREAM_COUNT streams concurrently
    let mut client_handles = Vec::new();
    for i in 0..STREAM_COUNT {
        let mut client_stream = client_session.open_stream().await.unwrap();
        let payload = format!("concurrent payload for stream {}", i).into_bytes();
        let h = tokio::spawn(async move {
            client_stream.write_all(&payload).await.unwrap();
            client_stream.shutdown().await.unwrap();

            let mut resp = Vec::new();
            client_stream.read_to_end(&mut resp).await.unwrap();

            let mut expected = payload.clone();
            expected.reverse();
            assert_eq!(resp, expected);
        });
        client_handles.push(h);
    }

    for h in client_handles {
        h.await.unwrap();
    }
    server_acceptor.await.unwrap();

    cancel.cancel();
}

#[tokio::test]
async fn test_mux_rst_drops_trailing_in_flight_frames() {
    let (pipe_client, pipe_server) = duplex(65536);
    let cancel = CancellationToken::new();

    let client_session = MuxSession::new(Box::new(pipe_client), true, cancel.clone());
    let server_session = MuxSession::new(Box::new(pipe_server), false, cancel.clone());

    // Client opens stream 1
    let client_stream = client_session.open_stream().await.unwrap();
    let stream_id = client_stream.stream_id();

    // Server accepts stream 1
    let mut server_stream = server_session.accept_stream().await.unwrap();

    // Client drops stream abruptly without FIN -> triggers RST frame
    drop(client_stream);

    // Wait a brief moment for RST frame to arrive at server
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Server attempts to read or write -> should detect closure or EOF
    let mut read_buf = [0u8; 10];
    let n = server_stream.read(&mut read_buf).await.unwrap();
    assert_eq!(n, 0, "Server must observe EOF after RST");

    // Manually inject a late in-flight DATA frame for the terminated stream ID into client session's tx_frames
    // to simulate packets arriving after reset.
    let late_frame = MuxFrame::data(stream_id, Bytes::from_static(b"ghost data"));
    let _ = client_session.send_frame(late_frame).await;

    // Now open a brand new stream 3
    let mut stream_3 = client_session.open_stream().await.unwrap();
    assert_eq!(stream_3.stream_id(), 3);

    let mut server_stream_3 = server_session.accept_stream().await.unwrap();
    assert_eq!(server_stream_3.stream_id(), 3);

    // Stream 3 sends clean data
    stream_3.write_all(b"clean stream 3 data").await.unwrap();
    stream_3.shutdown().await.unwrap();

    let mut stream_3_buf = vec![0u8; 19];
    server_stream_3.read_exact(&mut stream_3_buf).await.unwrap();
    // Verify stream 3 received ONLY its own data, NOT the ghost data from stream 1!
    assert_eq!(&stream_3_buf, b"clean stream 3 data");

    cancel.cancel();
}

#[tokio::test]
async fn test_mux_slow_consumer_does_not_block_session_hol() {
    let (pipe_client, pipe_server) = duplex(131072);
    let cancel = CancellationToken::new();

    let client_session = MuxSession::new(Box::new(pipe_client), true, cancel.clone());
    let server_session = MuxSession::new(Box::new(pipe_server), false, cancel.clone());

    // Open stream 1: a slow consumer that never reads
    let mut client_slow = client_session.open_stream().await.unwrap();
    let _server_slow = server_session.accept_stream().await.unwrap();

    // Open stream 3: normal active stream
    let mut client_fast = client_session.open_stream().await.unwrap();
    let mut server_fast = server_session.accept_stream().await.unwrap();

    // Flood slow stream beyond STREAM_BUFFER_SIZE (128)
    for _ in 0..150 {
        let _ = client_slow.write_all(b"data chunk").await;
        let _ = client_slow.flush().await;
    }

    // Now send data on fast stream. If HOL blocking occurred, this would hang indefinitely!
    client_fast.write_all(b"fast stream payload").await.unwrap();
    client_fast.flush().await.unwrap();

    let mut fast_buf = vec![0u8; 19];
    server_fast.read_exact(&mut fast_buf).await.unwrap();
    assert_eq!(&fast_buf, b"fast stream payload");

    cancel.cancel();
}

#[tokio::test]
async fn test_mux_stream_drop_cleans_active_streams() {
    let (pipe_client, pipe_server) = duplex(65536);
    let cancel = CancellationToken::new();

    let client_session = MuxSession::new(Box::new(pipe_client), true, cancel.clone());
    let _server_session = MuxSession::new(Box::new(pipe_server), false, cancel.clone());

    let stream = client_session.open_stream().await.unwrap();
    assert_eq!(client_session.active_stream_count(), 1);

    drop(stream);
    // Unregistered synchronously on drop via registry handle
    assert_eq!(client_session.active_stream_count(), 0);

    cancel.cancel();
}

#[tokio::test]
async fn test_mux_poll_write_bytes_and_shutdown_flushes_outbound() {
    use crate::core::stream::TunnelStream;
    use std::future::poll_fn;

    let (pipe_client, pipe_server) = duplex(65536);
    let cancel = CancellationToken::new();

    let client_session = MuxSession::new(Box::new(pipe_client), true, cancel.clone());
    let server_session = MuxSession::new(Box::new(pipe_server), false, cancel.clone());

    let mut client_stream = client_session.open_stream().await.unwrap();
    let mut server_stream = server_session.accept_stream().await.unwrap();

    // Use zero-copy poll_write_bytes on client
    let payload = Bytes::from_static(b"zero copy bytes over mux stream");
    let n = poll_fn(|cx| client_stream.poll_write_bytes(cx, payload.clone()))
        .await
        .unwrap();
    assert_eq!(n, payload.len());

    // Call shutdown which must drive poll_flush before emitting FIN
    client_stream.shutdown().await.unwrap();

    let mut recv_buf = Vec::new();
    server_stream.read_to_end(&mut recv_buf).await.unwrap();
    assert_eq!(recv_buf, payload.as_ref());

    cancel.cancel();
}

#[tokio::test]
async fn test_mux_poll_write_and_poll_write_bytes_interleaving_no_data_loss() {
    use crate::core::stream::TunnelStream;
    use std::future::poll_fn;
    use tokio::io::AsyncWrite;

    let (pipe_client, pipe_server) = duplex(65536);
    let cancel = CancellationToken::new();

    let client_session = MuxSession::new(Box::new(pipe_client), true, cancel.clone());
    let server_session = MuxSession::new(Box::new(pipe_server), false, cancel.clone());

    let mut client_stream = client_session.open_stream().await.unwrap();
    let mut server_stream = server_session.accept_stream().await.unwrap();

    // 1. First write via poll_write_bytes
    let chunk_a = Bytes::from_static(b"chunk_aaa_");
    let n_a = poll_fn(|cx| client_stream.poll_write_bytes(cx, chunk_a.clone()))
        .await
        .unwrap();
    assert_eq!(n_a, chunk_a.len());

    // 2. Immediate interleaved write via standard AsyncWrite poll_write
    let chunk_b = b"chunk_bbb_end";
    let n_b = poll_fn(|cx| std::pin::Pin::new(&mut client_stream).poll_write(cx, chunk_b))
        .await
        .unwrap();
    assert_eq!(n_b, chunk_b.len());

    client_stream.shutdown().await.unwrap();

    let mut recv_buf = Vec::new();
    server_stream.read_to_end(&mut recv_buf).await.unwrap();

    let mut expected = Vec::new();
    expected.extend_from_slice(b"chunk_aaa_");
    expected.extend_from_slice(b"chunk_bbb_end");
    assert_eq!(
        recv_buf, expected,
        "Interleaving poll_write_bytes and poll_write must preserve both chunks in exact sequence without data corruption"
    );

    cancel.cancel();
}

#[tokio::test]
async fn test_mux_backpressure_retry_no_duplicate_frames() {
    use crate::mux::frame::MuxFrame;
    use crate::mux::stream::MuxStream;
    use std::future::poll_fn;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::task::Poll;
    use tokio::io::AsyncWrite;
    use tokio::sync::mpsc;

    let (tx_frames, mut rx_frames) = mpsc::channel::<MuxFrame>(1);
    let (_tx_data, rx_data) = mpsc::channel(1);
    let closed = Arc::new(AtomicBool::new(false));

    let mut stream = MuxStream::new(1, tx_frames, rx_data, closed);

    // 1. Fill outbound capacity (capacity = 1)
    let first_payload = b"frame_1";
    let n1 = poll_fn(|cx| Pin::new(&mut stream).poll_write(cx, first_payload))
        .await
        .unwrap();
    assert_eq!(n1, 7);

    // 2. Next write must return Poll::Pending without buffering or duplicate sending
    let second_payload = b"frame_2_no_dup";
    let pending_observed =
        poll_fn(
            |cx| match Pin::new(&mut stream).poll_write(cx, second_payload) {
                Poll::Pending => Poll::Ready(true),
                Poll::Ready(_) => panic!("Expected Poll::Pending when outbound is full"),
            },
        )
        .await;
    assert!(pending_observed);

    // 3. Drain first frame to open up capacity
    let frame1 = rx_frames.recv().await.unwrap();
    assert_eq!(frame1.payload.as_ref(), first_payload);

    // 4. Retry write of second frame
    let n2 = poll_fn(|cx| Pin::new(&mut stream).poll_write(cx, second_payload))
        .await
        .unwrap();
    assert_eq!(n2, second_payload.len());

    // 5. Read second frame and verify NO DUPLICATE frames exist
    let frame2 = rx_frames.recv().await.unwrap();
    assert_eq!(frame2.payload.as_ref(), second_payload);
    assert!(
        rx_frames.try_recv().is_err(),
        "Must not send duplicate frames upon retry"
    );
}

#[tokio::test]
async fn test_mux_poll_write_bytes_backpressure_retry_no_duplicate_frames() {
    use crate::core::stream::TunnelStream;
    use crate::mux::frame::MuxFrame;
    use crate::mux::stream::MuxStream;
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::task::Poll;
    use tokio::sync::mpsc;

    let (tx_frames, mut rx_frames) = mpsc::channel::<MuxFrame>(1);
    let (_tx_data, rx_data) = mpsc::channel(1);
    let closed = Arc::new(AtomicBool::new(false));

    let mut stream = MuxStream::new(1, tx_frames, rx_data, closed);

    let first_payload = Bytes::from_static(b"bytes_1");
    let n1 = poll_fn(|cx| stream.poll_write_bytes(cx, first_payload.clone()))
        .await
        .unwrap();
    assert_eq!(n1, 7);

    let second_payload = Bytes::from_static(b"bytes_2_no_dup");
    let pending_observed =
        poll_fn(
            |cx| match stream.poll_write_bytes(cx, second_payload.clone()) {
                Poll::Pending => Poll::Ready(true),
                Poll::Ready(_) => panic!("Expected Poll::Pending when outbound is full"),
            },
        )
        .await;
    assert!(pending_observed);

    let frame1 = rx_frames.recv().await.unwrap();
    assert_eq!(frame1.payload.as_ref(), first_payload.as_ref());

    let n2 = poll_fn(|cx| stream.poll_write_bytes(cx, second_payload.clone()))
        .await
        .unwrap();
    assert_eq!(n2, second_payload.len());

    let frame2 = rx_frames.recv().await.unwrap();
    assert_eq!(frame2.payload.as_ref(), second_payload.as_ref());
    assert!(
        rx_frames.try_recv().is_err(),
        "poll_write_bytes must not send duplicate frames upon retry"
    );
}
