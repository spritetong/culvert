// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
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

use std::path::PathBuf;
use tokio::net::{TcpListener, TcpStream};

use crate::common::net::SocketTuner;
use crate::core::error::TunnelError;
use crate::core::stream::TunnelStream;
use crate::transport::TargetAddr;

/// Checks if a given address string represents a Unix Domain Socket path or URL.
/// Supports "unix://path", "ws+unix://path", and "wss+unix://path".
pub fn is_uds_path(s: &str) -> bool {
    let trimmed = s.trim();
    trimmed.starts_with("unix://")
        || trimmed.starts_with("ws+unix://")
        || trimmed.starts_with("wss+unix://")
}

/// Normalizes a UDS path or URL string into a standard OS filesystem PathBuf.
pub fn normalize_uds_path(s: &str) -> PathBuf {
    let trimmed = s.trim();
    let rest = if let Some(stripped) = trimmed.strip_prefix("unix://") {
        stripped
    } else if let Some(stripped) = trimmed.strip_prefix("ws+unix://") {
        stripped
    } else if let Some(stripped) = trimmed.strip_prefix("wss+unix://") {
        stripped
    } else {
        trimmed
    };

    let path_only = if let Some(q_idx) = rest.find('?') {
        &rest[..q_idx]
    } else {
        rest
    };

    // If path begins with "/C:..." on Windows or if drive letter follows leading slash,
    // strip the leading slash so PathBuf recognizes the drive letter.
    let normalized_str = if path_only.starts_with('/') && path_only.len() >= 3 {
        let b = path_only.as_bytes();
        if b[1].is_ascii_alphabetic() && b[2] == b':' {
            &path_only[1..]
        } else {
            path_only
        }
    } else {
        path_only
    };

    PathBuf::from(normalized_str)
}

/// Generic listener capable of accepting incoming connections over TCP or Unix Domain Sockets.
pub enum GenericListener {
    Tcp(TcpListener),
    Uds(PlatformUdsListener),
}

impl GenericListener {
    /// Binds to either a TCP address or a Unix Domain Socket path.
    /// Supports: ip:port, hostname:port, and unix://path. Port must not be omitted.
    pub async fn bind(addr: &str) -> Result<Self, TunnelError> {
        let target = crate::tunnel::forward::parse_target_addr(addr)?;
        match target {
            TargetAddr::Path(path) => {
                let listener = PlatformUdsListener::bind(&path).map_err(TunnelError::Io)?;
                Ok(GenericListener::Uds(listener))
            }
            TargetAddr::Ip(sa) => {
                let listener = TcpListener::bind(sa).await.map_err(TunnelError::Io)?;
                Ok(GenericListener::Tcp(listener))
            }
            TargetAddr::Domain(host, port) => {
                let listener = TcpListener::bind((host.as_str(), port))
                    .await
                    .map_err(TunnelError::Io)?;
                Ok(GenericListener::Tcp(listener))
            }
        }
    }

    /// Asynchronously accepts an incoming connection stream and reports the peer identifier.
    pub async fn accept(&mut self) -> Result<(Box<dyn TunnelStream>, String), TunnelError> {
        match self {
            GenericListener::Tcp(l) => {
                let (stream, addr) = l.accept().await.map_err(TunnelError::Io)?;
                let _ = SocketTuner::tune_tcp_stream(&stream, true, None);
                Ok((Box::new(stream), addr.to_string()))
            }
            GenericListener::Uds(l) => {
                let (stream, peer_addr) = l.accept().await.map_err(TunnelError::Io)?;
                Ok((stream, peer_addr))
            }
        }
    }
}

/// Connects to a destination TargetAddr (IP, Domain, or UDS Path).
pub async fn connect_target(target: &TargetAddr) -> Result<Box<dyn TunnelStream>, TunnelError> {
    match target {
        TargetAddr::Ip(sa) => {
            let stream = TcpStream::connect(*sa).await.map_err(TunnelError::Io)?;
            let _ = SocketTuner::tune_tcp_stream(&stream, true, None);
            Ok(Box::new(stream))
        }
        TargetAddr::Domain(host, port) => {
            let stream = TcpStream::connect((host.as_str(), *port))
                .await
                .map_err(TunnelError::Io)?;
            let _ = SocketTuner::tune_tcp_stream(&stream, true, None);
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
    let target = crate::tunnel::forward::parse_target_addr(target_str)?;
    connect_target(&target).await
}

#[cfg(test)]
mod tests {
    use super::*;
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
            let (mut stream, _peer) = listener.accept().await.unwrap();
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
