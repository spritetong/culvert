//! Unit tests for multiplexing framing, session driver, and stream isolation.
//! All comments and docstrings must be in English.

use std::time::Duration;
use bytes::Bytes;
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
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
