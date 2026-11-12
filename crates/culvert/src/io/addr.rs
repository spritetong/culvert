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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum L4Protocol {
    #[default]
    Tcp,
    Udp,
}

impl L4Protocol {
    /// Returns the canonical display string (`"tcp"` or `"udp"`).
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tcp => crate::consts::TUNNEL_PROTO_TCP,
            Self::Udp => crate::consts::TUNNEL_PROTO_UDP,
        }
    }
}

impl std::fmt::Display for L4Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for L4Protocol {
    type Err = TunnelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "tcp" => Ok(Self::Tcp),
            "udp" => Ok(Self::Udp),
            other => Err(TunnelError::Config(format!(
                "Unsupported L4 protocol '{}'. Expected 'TCP' or 'UDP'.",
                other
            ))),
        }
    }
}

impl Serialize for L4Protocol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Tcp => serializer.serialize_str("TCP"),
            Self::Udp => serializer.serialize_str("UDP"),
        }
    }
}

impl<'de> Deserialize<'de> for L4Protocol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct L4ProtocolVisitor;

        impl<'de> serde::de::Visitor<'de> for L4ProtocolVisitor {
            type Value = L4Protocol;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a protocol string: 'TCP' or 'UDP'")
            }

            fn visit_str<E>(self, value: &str) -> Result<L4Protocol, E>
            where
                E: serde::de::Error,
            {
                value.parse().map_err(serde::de::Error::custom)
            }
        }

        deserializer.deserialize_str(L4ProtocolVisitor)
    }
}

/// Neutral description of one port mapping handed to `frontend::bind`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingSpec {
    pub protocol: L4Protocol,
    pub listen: String,
    pub target: String,
}

impl MappingSpec {
    /// Builds a mapping spec for any L4 protocol.
    pub fn new(protocol: L4Protocol, listen: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            protocol,
            listen: listen.into(),
            target: target.into(),
        }
    }

    /// Builds a TCP mapping spec.
    pub fn tcp(listen: impl Into<String>, target: impl Into<String>) -> Self {
        Self::new(L4Protocol::Tcp, listen, target)
    }

    /// Builds a UDP mapping spec.
    pub fn udp(listen: impl Into<String>, target: impl Into<String>) -> Self {
        Self::new(L4Protocol::Udp, listen, target)
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
///
/// Supports:
/// 1. `ip:port` (e.g. 127.0.0.1:8080, [::1]:8080)
/// 2. `hostname:port` (e.g. localhost:8080, example.com:443)
/// 3. `unix://path` (e.g. unix:///run/tunnel.sock, unix://C:/temp/tunnel.sock)
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

/// Listen address abstraction for local frontends and servers.
///
/// Supports:
/// 1. `:port` (Dual-stack IPv6/IPv4 binding on `[::]:port` via socket2 `set_only_v6(false)`)
/// 2. `addr:port` (OS default configuration for IP, e.g. `127.0.0.1:8080`, `0.0.0.0:8080`, `[::1]:8080`)
/// 3. `hostname:port` (OS default configuration, e.g. `localhost:8080`)
/// 4. `unix://path` (Unix domain socket path)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ListenAddr {
    DualStack(u16),
    Ip(SocketAddr),
    Domain(String, u16),
    Path(PathBuf),
}

impl std::fmt::Display for ListenAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ListenAddr::DualStack(port) => write!(f, ":{}", port),
            ListenAddr::Ip(addr) => write!(f, "{}", addr),
            ListenAddr::Domain(host, port) => write!(f, "{}:{}", host, port),
            ListenAddr::Path(path) => write!(f, "unix://{}", path.display()),
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
/// 1. `ip:port` (e.g. 127.0.0.1:8080, [::1]:8080)
/// 2. `hostname:port` (e.g. localhost:8080, example.com:443)
/// 3. `unix://path` (e.g. unix:///run/tunnel.sock, unix://C:/temp/tunnel.sock)
///
/// Port number cannot be omitted for IP and hostname addresses. Target addresses must specify a host.
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
                "Invalid target address '{}'. IPv6 addresses with port must be enclosed in brackets (e.g. '[::1]:8080')",
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
                "Invalid target address '{}': missing host. Target must be 'ip:port', 'hostname:port', or 'unix://path'",
                target_str
            )));
        }

        let port = port_str.parse::<u16>().map_err(|_| {
            TunnelError::Config(format!(
                "Invalid target address '{}': invalid port '{}'",
                target_str, port_str
            ))
        })?;

        if port == 0 {
            return Err(TunnelError::Config(format!(
                "Invalid target address '{}': port must not be 0",
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
        "Invalid target address '{}'. Expected 'ip:port', 'hostname:port', or 'unix://path'",
        target_str
    )))
}

/// Parses a listen address string supporting four formats:
/// 1. `:port` (Dual-stack IPv6/IPv4, e.g. `:8080`)
/// 2. `addr:port` (OS-default configuration for IP, e.g. `127.0.0.1:8080`, `[::1]:8080`, `0.0.0.0:8080`)
/// 3. `hostname:port` (OS-default configuration, e.g. `localhost:8080`)
/// 4. `unix://path` (Unix domain socket path)
///
/// Port number cannot be omitted for non-UDS addresses.
pub fn parse_listen_addr(listen_str: &str) -> Result<ListenAddr, TunnelError> {
    let listen_str = listen_str.trim();
    if listen_str.starts_with("unix://") {
        let path = normalize_uds_path(listen_str);
        if path.as_os_str().is_empty() {
            return Err(TunnelError::Config(format!(
                "Invalid listen UDS path: {}",
                listen_str
            )));
        }
        return Ok(ListenAddr::Path(path));
    }

    // Dual-stack ":port" format
    if let Some(port_str) = listen_str.strip_prefix(':') {
        let port = port_str.parse::<u16>().map_err(|_| {
            TunnelError::Config(format!(
                "Invalid listen address '{}': invalid port '{}'",
                listen_str, port_str
            ))
        })?;

        if port == 0 {
            return Err(TunnelError::Config(format!(
                "Invalid listen address '{}': port must not be 0",
                listen_str
            )));
        }

        return Ok(ListenAddr::DualStack(port));
    }

    // Must be ip:port or hostname:port. Port number cannot be omitted.
    if let Some((host, port_str)) = listen_str.rsplit_once(':') {
        let is_bracketed = host.starts_with('[') && host.ends_with(']');
        if host.contains(':') && !is_bracketed {
            return Err(TunnelError::Config(format!(
                "Invalid listen address '{}'. IPv6 addresses with port must be enclosed in brackets (e.g. '[::1]:8080')",
                listen_str
            )));
        }

        let clean_host = if is_bracketed {
            &host[1..host.len() - 1]
        } else {
            host
        };

        if clean_host.is_empty() {
            return Err(TunnelError::Config(format!(
                "Invalid listen address '{}': missing host",
                listen_str
            )));
        }

        let port = port_str.parse::<u16>().map_err(|_| {
            TunnelError::Config(format!(
                "Invalid listen address '{}': invalid port '{}'",
                listen_str, port_str
            ))
        })?;

        if port == 0 {
            return Err(TunnelError::Config(format!(
                "Invalid listen address '{}': port must not be 0",
                listen_str
            )));
        }

        if let Ok(ip) = clean_host.parse::<std::net::IpAddr>() {
            return Ok(ListenAddr::Ip(std::net::SocketAddr::new(ip, port)));
        } else if is_valid_hostname(clean_host) {
            return Ok(ListenAddr::Domain(clean_host.to_string(), port));
        }
    }

    Err(TunnelError::Config(format!(
        "Invalid listen address '{}'. Expected ':port', 'ip:port', 'hostname:port', or 'unix://path'",
        listen_str
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

    #[test]
    fn test_l4_protocol_case_insensitive_deserialization() {
        let tcp_upper: L4Protocol = serde_yaml::from_str("TCP").unwrap();
        assert_eq!(tcp_upper, L4Protocol::Tcp);
        let tcp_lower: L4Protocol = serde_yaml::from_str("tcp").unwrap();
        assert_eq!(tcp_lower, L4Protocol::Tcp);

        let udp_upper: L4Protocol = serde_yaml::from_str("UDP").unwrap();
        assert_eq!(udp_upper, L4Protocol::Udp);
        let udp_lower: L4Protocol = serde_yaml::from_str("udp").unwrap();
        assert_eq!(udp_lower, L4Protocol::Udp);

        assert!(serde_yaml::from_str::<L4Protocol>("http").is_err());
        assert!(serde_yaml::from_str::<L4Protocol>("\"\"").is_err());

        // Serializes as uppercase TCP/UDP
        assert_eq!(
            serde_yaml::to_string(&L4Protocol::Tcp).unwrap().trim(),
            "TCP"
        );
        assert_eq!(
            serde_yaml::to_string(&L4Protocol::Udp).unwrap().trim(),
            "UDP"
        );
    }

    #[test]
    fn test_parse_listen_and_target_addr_dual_stack_and_formats() {
        let l1 = parse_listen_addr(":8080").unwrap();
        assert_eq!(l1, ListenAddr::DualStack(8080));
        assert_eq!(l1.to_string(), ":8080");

        // Target parser strictly rejects :port format
        assert!(parse_target_addr(":8080").is_err());

        let t1 = parse_target_addr("127.0.0.1:8080").unwrap();
        assert_eq!(t1, TargetAddr::Ip("127.0.0.1:8080".parse().unwrap()));
        assert_eq!(t1.to_string(), "127.0.0.1:8080");

        let l2 = parse_listen_addr("127.0.0.1:8080").unwrap();
        assert_eq!(l2, ListenAddr::Ip("127.0.0.1:8080".parse().unwrap()));

        let l3 = parse_listen_addr("[::1]:8080").unwrap();
        assert_eq!(l3, ListenAddr::Ip("[::1]:8080".parse().unwrap()));

        let l4 = parse_listen_addr("localhost:8080").unwrap();
        assert_eq!(l4, ListenAddr::Domain("localhost".to_string(), 8080));

        let l5 = parse_listen_addr("unix:///tmp/test.sock").unwrap();
        assert!(matches!(l5, ListenAddr::Path(_)));

        // Zero and invalid ports are rejected
        assert!(parse_listen_addr(":0").is_err());
        assert!(parse_listen_addr(":99999").is_err());
        assert!(parse_listen_addr(":abc").is_err());
        assert!(parse_target_addr(":0").is_err());
    }
}
