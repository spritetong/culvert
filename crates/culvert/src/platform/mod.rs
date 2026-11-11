// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Platform-specific abstractions and operating system integrations.
//! All comments and docstrings must be in English.

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::*;

#[cfg(not(windows))]
mod unix;
#[cfg(not(windows))]
pub use unix::*;

use reusable_future::ReusableFutureRef;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

use crate::common::net::SocketTuner;
use crate::core::error::TunnelError;
use crate::core::stream::TunnelStream;
use crate::io::addr::TargetAddr;

pub use crate::io::addr::{PeerAddr, is_uds_path, normalize_uds_path, parse_target_addr};

/// Result type produced when accepting a stream from a `GenericListener`.
pub type GenericAcceptOutput = Result<(Box<dyn TunnelStream>, PeerAddr), TunnelError>;

/// Generic listener capable of accepting incoming connections over TCP or Unix Domain Sockets.
#[derive(Clone)]
pub enum GenericListener {
    Tcp(Arc<TcpListener>),
    Uds(Arc<tokio::sync::Mutex<PlatformUdsListener>>),
}

impl GenericListener {
    /// Binds to either a TCP address or a Unix Domain Socket path.
    /// Supports: ip:port, hostname:port, and unix://path. Port must not be omitted.
    pub async fn bind(addr: &str) -> Result<Self, TunnelError> {
        let target = parse_target_addr(addr)?;
        match target {
            TargetAddr::Path(path) => {
                let listener = PlatformUdsListener::bind(&path).map_err(TunnelError::Io)?;
                Ok(GenericListener::Uds(Arc::new(tokio::sync::Mutex::new(
                    listener,
                ))))
            }
            TargetAddr::Ip(sa) => {
                let listener = TcpListener::bind(sa).await.map_err(TunnelError::Io)?;
                Ok(GenericListener::Tcp(Arc::new(listener)))
            }
            TargetAddr::Domain(host, port) => {
                let listener = TcpListener::bind((host.as_str(), port))
                    .await
                    .map_err(TunnelError::Io)?;
                Ok(GenericListener::Tcp(Arc::new(listener)))
            }
        }
    }

    /// Returns an owned `'static` future for the next incoming connection.
    /// Cloning the listener is two `Arc` clones; callers can embed this in a
    /// reused slot without allocating a throwaway future container per accept.
    pub fn accept_owned(&self) -> impl std::future::Future<Output = GenericAcceptOutput> + 'static {
        let listener = self.clone();
        async move {
            match &listener {
                GenericListener::Tcp(l) => {
                    let (stream, addr) = l.accept().await.map_err(TunnelError::Io)?;
                    let _ = SocketTuner::tune_tcp_stream(&stream, true);
                    Ok((
                        Box::new(stream) as Box<dyn TunnelStream>,
                        PeerAddr::Socket(addr),
                    ))
                }
                GenericListener::Uds(l) => {
                    let mut guard = l.lock().await;
                    let (stream, peer_addr) = guard.accept().await.map_err(TunnelError::Io)?;
                    let peer = if peer_addr.is_empty() {
                        PeerAddr::Path(std::path::PathBuf::from("unnamed"))
                    } else {
                        PeerAddr::Path(std::path::PathBuf::from(peer_addr))
                    };
                    Ok((stream, peer))
                }
            }
        }
    }

    /// Asynchronously awaits and accepts the next incoming connection stream into `slot`.
    ///
    /// Loop callers keep `slot` outside `tokio::select!` and poll `&mut slot` in a
    /// branch, so an in-flight accept survives other branches winning.
    pub fn accept(&mut self, slot: &mut ReusableFutureRef<'static, GenericAcceptOutput>) {
        if !slot.is_empty() {
            return;
        }
        let pending = self.accept_owned();
        slot.set(move || pending);
    }

    /// Convenience helper for single-shot / non-loop awaiting.
    pub async fn accept_one(&mut self) -> GenericAcceptOutput {
        self.accept_owned().await
    }
}

/// Connects to a destination TargetAddr (IP, Domain, or UDS Path).
pub async fn connect_target(target: &TargetAddr) -> Result<Box<dyn TunnelStream>, TunnelError> {
    match target {
        TargetAddr::Ip(sa) => {
            let stream = TcpStream::connect(*sa).await.map_err(TunnelError::Io)?;
            let _ = SocketTuner::tune_tcp_stream(&stream, true);
            Ok(Box::new(stream))
        }
        TargetAddr::Domain(host, port) => {
            let stream = TcpStream::connect((host.as_str(), *port))
                .await
                .map_err(TunnelError::Io)?;
            let _ = SocketTuner::tune_tcp_stream(&stream, true);
            Ok(Box::new(stream))
        }
        TargetAddr::Path(path) => {
            let stream = connect_uds(path).await.map_err(TunnelError::Io)?;
            Ok(stream)
        }
    }
}

/// Connects to a target address or UDS path specified as a string.
pub async fn connect_target_str(target_str: &str) -> Result<Box<dyn TunnelStream>, TunnelError> {
    let target = parse_target_addr(target_str)?;
    connect_target(&target).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn test_is_uds_path() {
        assert!(is_uds_path("unix:///tmp/test.sock"));
        assert!(is_uds_path("ws+unix:///tmp/test.sock"));
        assert!(is_uds_path("wss+unix:///tmp/test.sock"));
        assert!(is_uds_path("unix://C:/temp/my.sock"));
        assert!(is_uds_path("unix://./relative.sock"));

        assert!(!is_uds_path("/var/run/my.sock"));
        assert!(!is_uds_path("test.sock"));
        assert!(!is_uds_path("./local.sock"));
        assert!(!is_uds_path("127.0.0.1:8080"));
        assert!(!is_uds_path("0.0.0.0:443"));
        assert!(!is_uds_path("[::1]:8080"));
        assert!(!is_uds_path("localhost:8080"));
        assert!(!is_uds_path("example.com:443"));
    }

    #[test]
    fn test_normalize_uds_path() {
        let p1 = normalize_uds_path("unix:///tmp/test.sock");
        assert_eq!(p1, PathBuf::from("/tmp/test.sock"));

        let p2 = normalize_uds_path("ws+unix:///tmp/test.sock");
        assert_eq!(p2, PathBuf::from("/tmp/test.sock"));

        let p3 = normalize_uds_path("wss+unix:///tmp/test.sock");
        assert_eq!(p3, PathBuf::from("/tmp/test.sock"));

        let p4 = normalize_uds_path("unix://C:/temp/test.sock");
        assert_eq!(p4, PathBuf::from("C:/temp/test.sock"));

        let p5 = normalize_uds_path("unix:///C:/temp/test.sock");
        assert_eq!(p5, PathBuf::from("C:/temp/test.sock"));
    }

    #[tokio::test]
    async fn test_platform_uds_bidirectional_transfer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let sock_path = temp_dir.path().join("test_pipe.sock");

        let mut listener = PlatformUdsListener::bind(&sock_path).unwrap();

        let server_task = tokio::spawn(async move {
            let (mut server_stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            server_stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping!");
            server_stream.write_all(b"pong!").await.unwrap();
            server_stream.flush().await.unwrap();
        });

        let mut client_stream = connect_uds(&sock_path).await.unwrap();
        client_stream.write_all(b"ping!").await.unwrap();
        client_stream.flush().await.unwrap();

        let mut buf = [0u8; 5];
        client_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong!");

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_generic_listener_and_target_connect() {
        let temp_dir = tempfile::tempdir().unwrap();
        let sock_path = temp_dir.path().join("generic_test.sock");
        let sock_str = format!("unix://{}", sock_path.display());

        let mut listener = GenericListener::bind(&sock_str).await.unwrap();

        let sock_str_clone = sock_str.clone();
        let srv_task = tokio::spawn(async move {
            let (mut stream, _peer) = listener.accept_one().await.unwrap();
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"echo");
            stream.write_all(b"echo").await.unwrap();
            stream.flush().await.unwrap();
        });

        let mut client = connect_target_str(&sock_str_clone).await.unwrap();
        client.write_all(b"echo").await.unwrap();
        client.flush().await.unwrap();

        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"echo");

        srv_task.await.unwrap();
    }
}
