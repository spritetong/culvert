// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Neutral address vocabulary: L4 protocol, mapping spec, and target addresses.
//! All comments and docstrings must be in English.

use crate::io::error::TunnelError;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Transport-layer protocol of a port mapping.
///
/// `Udp` is vocabulary only in this release: no configuration field, frontend,
/// or datagram driver constructs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum L4Protocol {
    #[default]
    Tcp,
    Udp,
}

/// Neutral description of one port mapping handed to `frontend::bind`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingSpec {
    pub protocol: L4Protocol,
    pub listen: String,
    pub target: String,
}

impl MappingSpec {
    /// Builds the only mapping kind supported today.
    pub fn tcp(listen: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            protocol: L4Protocol::Tcp,
            listen: listen.into(),
            target: target.into(),
        }
    }
}

/// Neutral address representing an inbound or outbound peer.
///
/// Standard network traffic uses `Socket(SocketAddr)` which requires zero heap
/// allocations and is trivial to serialize via MessagePack or serde.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PeerAddr {
    /// Standard IP socket address (TCP/UDP).
    Socket(SocketAddr),
    /// Unix domain socket peer or filesystem path.
    Path(PathBuf),
    /// Future P2P / WebRTC or opaque peer identifier.
    P2p(String),
}

impl Default for PeerAddr {
    fn default() -> Self {
        Self::Socket(SocketAddr::from(([0, 0, 0, 0], 0)))
    }
}

impl std::fmt::Display for PeerAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Socket(addr) => write!(f, "{}", addr),
            Self::Path(path) => write!(f, "unix://{}", path.display()),
            Self::P2p(id) => write!(f, "p2p://{}", id),
        }
    }
}

impl From<SocketAddr> for PeerAddr {
    #[inline]
    fn from(addr: SocketAddr) -> Self {
        Self::Socket(addr)
    }
}

impl From<PathBuf> for PeerAddr {
    #[inline]
    fn from(path: PathBuf) -> Self {
        Self::Path(path)
    }
}

impl std::str::FromStr for PeerAddr {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(sa) = s.parse::<SocketAddr>() {
            Ok(Self::Socket(sa))
        } else if is_uds_path(s) {
            Ok(Self::Path(normalize_uds_path(s)))
        } else {
            Ok(Self::P2p(s.to_string()))
        }
    }
}

impl From<&str> for PeerAddr {
    fn from(s: &str) -> Self {
        if let Ok(sa) = s.parse::<SocketAddr>() {
            Self::Socket(sa)
        } else if is_uds_path(s) {
            Self::Path(normalize_uds_path(s))
        } else {
            Self::P2p(s.to_string())
        }
    }
}

impl From<String> for PeerAddr {
    fn from(s: String) -> Self {
        if let Ok(sa) = s.parse::<SocketAddr>() {
            Self::Socket(sa)
        } else if is_uds_path(&s) {
            Self::Path(normalize_uds_path(&s))
        } else {
            Self::P2p(s)
        }
    }
}

/// Target address abstraction for remote forwarding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TargetAddr {
    Ip(SocketAddr),
    Domain(String, u16),
    Path(PathBuf),
}

impl std::fmt::Display for TargetAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetAddr::Ip(addr) => write!(f, "{}", addr),
            TargetAddr::Domain(host, port) => write!(f, "{}:{}", host, port),
            TargetAddr::Path(path) => write!(f, "unix://{}", path.display()),
        }
    }
}

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

/// Helper to validate if a string conforms to a valid hostname / domain name.
fn is_valid_hostname(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 {
        return false;
    }
    for label in host.split('.') {
        if label.is_empty() || label.len() > 63 {
            return false;
        }
        if label.starts_with('-') || label.ends_with('-') {
            return false;
        }
        if !label
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return false;
        }
    }
    true
}

/// Parses a target address string supporting three formats:
/// 1. ip:port (e.g. 127.0.0.1:8080, [::1]:8080)
/// 2. hostname:port (e.g. localhost:8080, example.com:443)
/// 3. unix://path (e.g. unix:///run/tunnel.sock, unix://C:/temp/tunnel.sock)
///
/// Port number cannot be omitted for IP and hostname addresses.
pub fn parse_target_addr(target_str: &str) -> Result<TargetAddr, TunnelError> {
    let target_str = target_str.trim();
    if target_str.starts_with("unix://") {
        let path = normalize_uds_path(target_str);
        if path.as_os_str().is_empty() {
            return Err(TunnelError::Config(format!(
                "Invalid target UDS path: {}",
                target_str
            )));
        }
        return Ok(TargetAddr::Path(path));
    }

    // Must be ip:port or hostname:port. Port number cannot be omitted.
    if let Some((host, port_str)) = target_str.rsplit_once(':') {
        let is_bracketed = host.starts_with('[') && host.ends_with(']');
        if host.contains(':') && !is_bracketed {
            return Err(TunnelError::Config(format!(
                "Invalid mapping address '{}'. IPv6 addresses with port must be enclosed in brackets (e.g. '[::1]:8080')",
                target_str
            )));
        }

        let clean_host = if is_bracketed {
            &host[1..host.len() - 1]
        } else {
            host
        };

        if clean_host.is_empty() {
            return Err(TunnelError::Config(format!(
                "Invalid mapping address '{}': missing host",
                target_str
            )));
        }

        let port = port_str.parse::<u16>().map_err(|_| {
            TunnelError::Config(format!(
                "Invalid mapping address '{}': invalid port '{}'",
                target_str, port_str
            ))
        })?;

        if port == 0 {
            return Err(TunnelError::Config(format!(
                "Invalid mapping address '{}': port must not be 0",
                target_str
            )));
        }

        if let Ok(ip) = clean_host.parse::<std::net::IpAddr>() {
            return Ok(TargetAddr::Ip(std::net::SocketAddr::new(ip, port)));
        } else if is_valid_hostname(clean_host) {
            return Ok(TargetAddr::Domain(clean_host.to_string(), port));
        }
    }

    Err(TunnelError::Config(format!(
        "Invalid mapping address '{}'. Expected 'ip:port', 'hostname:port', or 'unix://path'",
        target_str
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_addr_msgpack_roundtrip() {
        let sa: SocketAddr = "192.168.1.100:8080".parse().unwrap();
        let peer_socket = PeerAddr::Socket(sa);
        let bytes = rmp_serde::to_vec_named(&peer_socket).unwrap();
        let decoded: PeerAddr = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(peer_socket, decoded);

        let peer_path = PeerAddr::Path(PathBuf::from("/var/run/test.sock"));
        let bytes = rmp_serde::to_vec_named(&peer_path).unwrap();
        let decoded: PeerAddr = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(peer_path, decoded);

        let peer_p2p = PeerAddr::P2p("QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco".into());
        let bytes = rmp_serde::to_vec_named(&peer_p2p).unwrap();
        let decoded: PeerAddr = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(peer_p2p, decoded);
    }

    #[test]
    fn test_peer_addr_conversions_and_display() {
        let sa: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        let peer: PeerAddr = sa.into();
        assert_eq!(peer.to_string(), "127.0.0.1:9090");

        let from_str: PeerAddr = "127.0.0.1:9090".into();
        assert_eq!(peer, from_str);

        let uds_peer: PeerAddr = "unix:///tmp/test.sock".into();
        assert!(matches!(uds_peer, PeerAddr::Path(_)));

        let p2p_peer: PeerAddr = "node_abc123".into();
        assert_eq!(p2p_peer, PeerAddr::P2p("node_abc123".into()));
        assert_eq!(p2p_peer.to_string(), "p2p://node_abc123");
    }
}
