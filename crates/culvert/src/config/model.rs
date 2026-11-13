// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Declarative YAML configuration data models.
//! All comments and docstrings must be in English.

use crate::io::addr::L4Protocol;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Top-level application configuration structure.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default)]
    pub log: LogConfig,

    pub dns: Option<DnsConfig>,
    pub dialer: Option<DialerConfig>,
    pub tls: Option<TlsConfig>,

    pub server: Option<ServerConfig>,

    /// Number of Tokio worker threads (None = auto-detect based on CPU cores).
    pub worker_threads: Option<usize>,

    #[serde(default)]
    pub tunnels: Vec<TunnelConfig>,
}

/// Parses a UDS `chmod=` value. Socket modes are always octal: a plain `660`
/// means `0o660`, never decimal 660. Accepts `0o660`, `0660` and `660`.
pub fn parse_socket_mode(value: &str) -> Option<u32> {
    let digits = value
        .strip_prefix("0o")
        .or_else(|| value.strip_prefix("0O"))
        .unwrap_or(value);
    u32::from_str_radix(digits, 8).ok().filter(|m| *m <= 0o7777)
}

/// Parsed server listening specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerListenSpec {
    pub scheme: String,
    pub bind_addr: String,
    pub path: Option<String>,
    pub chmod: Option<u32>,
}

impl ServerListenSpec {
    /// Returns true if this listener uses Unix Domain Sockets.
    pub fn is_uds(&self) -> bool {
        self.scheme == "ws+unix" || self.scheme == "wss+unix"
    }

    pub fn parse(raw: &str) -> Result<Self, crate::core::error::TunnelError> {
        let raw = raw.trim();
        let (scheme, rest) = if let Some(idx) = raw.find("://") {
            let s = &raw[..idx];
            let r = &raw[idx + 3..];
            (s.to_lowercase(), r)
        } else {
            return Err(crate::core::error::TunnelError::Config(format!(
                "Invalid server listen URL '{}'. Protocol scheme must be explicitly specified (e.g. ws://0.0.0.0:8080/tunnel, h3://0.0.0.0:8443/tunnel, ws+unix:///tmp/tunnel.sock). Implicit protocol is not supported.",
                raw
            )));
        };

        match scheme.as_str() {
            "ws" | "wss" | "h3" | "quic" | "https" => {
                let (host_port, path_part) = if let Some(slash_idx) = rest.find('/') {
                    let hp = &rest[..slash_idx];
                    let p = &rest[slash_idx..];
                    (hp, p)
                } else {
                    (rest, "/")
                };

                let (_, port_str) = host_port.rsplit_once(':').ok_or_else(|| {
                    crate::core::error::TunnelError::Config(format!(
                        "Invalid server listen URL '{}': missing port in '{}'. Expected format '<host>:<port>'",
                        raw, host_port
                    ))
                })?;

                let port: u16 = port_str.parse().map_err(|_| {
                    crate::core::error::TunnelError::Config(format!(
                        "Invalid server listen URL '{}': invalid port '{}' (must be a valid integer 1-65535)",
                        raw, port_str
                    ))
                })?;

                if port == 0 {
                    return Err(crate::core::error::TunnelError::Config(format!(
                        "Invalid server listen URL '{}': port must not be 0",
                        raw
                    )));
                }

                let path = if path_part.is_empty() || path_part == "/" {
                    None
                } else {
                    Some(path_part.to_string())
                };

                Ok(Self {
                    scheme,
                    bind_addr: host_port.to_string(),
                    path,
                    chmod: None,
                })
            }
            "ws+unix" | "wss+unix" => {
                let (path_part, query_part) = if let Some(q_idx) = rest.find('?') {
                    (&rest[..q_idx], Some(&rest[q_idx + 1..]))
                } else {
                    (rest, None)
                };

                let socket_path = crate::platform::normalize_uds_path(path_part);
                let socket_path_str = socket_path.to_string_lossy().to_string();
                if socket_path_str.is_empty() || socket_path_str == "/" {
                    return Err(crate::core::error::TunnelError::Config(format!(
                        "Invalid server listen URL '{}': socket file path must not be empty",
                        raw
                    )));
                }

                let mut http_path = None;
                let mut chmod = None;

                if let Some(q) = query_part {
                    for param in q.split('&') {
                        if let Some(val) = param.strip_prefix("path=") {
                            let p = if !val.starts_with('/') {
                                format!("/{}", val)
                            } else {
                                val.to_string()
                            };
                            http_path = Some(p);
                        } else if let Some(val) = param.strip_prefix("chmod=") {
                            chmod = parse_socket_mode(val);
                        }
                    }
                }

                Ok(Self {
                    scheme,
                    bind_addr: socket_path_str,
                    path: http_path,
                    chmod,
                })
            }
            other => Err(crate::core::error::TunnelError::Config(format!(
                "Unsupported server protocol scheme '{}' in URL '{}'. Supported schemes: ws, wss, https, h3, quic, ws+unix, wss+unix",
                other, raw
            ))),
        }
    }
}

fn deserialize_one_or_many<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany<T> {
        One(T),
        Many(Vec<T>),
    }

    match OneOrMany::deserialize(deserializer)? {
        OneOrMany::One(s) => Ok(vec![s]),
        OneOrMany::Many(vec) => Ok(vec),
    }
}

fn serialize_one_or_many<S, T>(vec: &[T], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: serde::Serialize,
{
    vec.serialize(serializer)
}

/// Server configuration for running as an ingress listener on VPS or cloud host.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Ingress listening URLs (e.g. "ws://0.0.0.0:8080/tunnel", ["ws://...", "ws+unix:///run/a.sock"]).
    #[serde(
        default,
        deserialize_with = "deserialize_one_or_many",
        serialize_with = "serialize_one_or_many"
    )]
    pub listen: Vec<String>,

    /// Authentication token for validating incoming tunnels. Optional.
    pub token: Option<String>,

    /// Default target to forward to when client doesn't specify one.
    pub default_target: Option<String>,

    /// TLS certificate file path for direct TLS/WSS/H3 termination (optional, fallback to self-signed).
    #[serde(default)]
    pub certificate: Option<String>,

    /// TLS private key file path for direct TLS/WSS/H3 termination (optional, fallback to self-signed).
    #[serde(default)]
    pub key: Option<String>,

    /// Advanced TLS options (version bounds, cert paths, ALPN) for server termination.
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// Periodic heartbeat probe interval for ingress transport keepalive.
    #[serde(with = "humantime_serde_opt", default)]
    pub heartbeat_interval: Option<Duration>,

    /// Maximum silent duration before terminating an idle client connection.
    #[serde(with = "humantime_serde_opt", default)]
    pub idle_timeout: Option<Duration>,

    /// Idle session expiration timeout for relayed UDP NAT flows.
    #[serde(with = "humantime_serde_opt", default)]
    pub udp_idle_timeout: Option<Duration>,
}

impl ServerConfig {
    pub fn new(
        listen: impl IntoIterator<Item = impl Into<String>>,
        token: Option<String>,
        default_target: Option<String>,
    ) -> Self {
        Self {
            listen: listen.into_iter().map(Into::into).collect(),
            token,
            default_target,
            certificate: None,
            key: None,
            tls: None,
            heartbeat_interval: None,
            idle_timeout: None,
            udp_idle_timeout: None,
        }
    }

    /// Returns the effective `TlsConfig` for server termination, merging explicit fields.
    pub fn effective_tls(&self, global_tls: Option<&TlsConfig>) -> Option<TlsConfig> {
        let merged = match (global_tls, self.tls.as_ref()) {
            (Some(g), Some(s)) => Some(g.merge(Some(s))),
            (Some(g), None) => Some(g.clone()),
            (None, Some(s)) => Some(s.clone()),
            (None, None) => None,
        };

        if self.certificate.is_some() || self.key.is_some() {
            let mut cfg = merged.unwrap_or_default();
            if let Some(ref c) = self.certificate {
                cfg.certificate = Some(PathBuf::from(c));
            }
            if let Some(ref k) = self.key {
                cfg.key = Some(PathBuf::from(k));
            }
            Some(cfg)
        } else {
            merged
        }
    }

    pub fn parse_specs(&self) -> Result<Vec<ServerListenSpec>, crate::core::error::TunnelError> {
        let mut specs = Vec::new();
        for url in &self.listen {
            specs.push(ServerListenSpec::parse(url)?);
        }
        Ok(specs)
    }
}

/// Logging configuration options.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,

    #[serde(default = "default_log_output")]
    pub output: String,

    #[serde(default)]
    pub no_color: bool,
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_output() -> String {
    "stdout".to_string()
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            output: default_log_output(),
            no_color: false,
        }
    }
}

/// IP address family selection strategy for DNS resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DnsStrategy {
    #[default]
    PreferIpv4,
    PreferIpv6,
    Ipv4Only,
    Ipv6Only,
}

/// Configuration for an individual DNS resolver backend.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum DnsResolverConfig {
    Detailed(DnsServerConfig),
    Simple(String),
}

impl From<&str> for DnsResolverConfig {
    fn from(s: &str) -> Self {
        DnsResolverConfig::Simple(s.to_string())
    }
}

impl From<String> for DnsResolverConfig {
    fn from(s: String) -> Self {
        DnsResolverConfig::Simple(s)
    }
}

impl From<DnsServerConfig> for DnsResolverConfig {
    fn from(cfg: DnsServerConfig) -> Self {
        DnsResolverConfig::Detailed(cfg)
    }
}

impl<'de> Deserialize<'de> for DnsResolverConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DnsResolverConfigVisitor;

        impl<'de> serde::de::Visitor<'de> for DnsResolverConfigVisitor {
            type Value = DnsResolverConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a DNS resolver URL string or a structured DNS server map")
            }

            fn visit_str<E>(self, value: &str) -> Result<DnsResolverConfig, E>
            where
                E: serde::de::Error,
            {
                Ok(DnsResolverConfig::Simple(value.to_string()))
            }

            fn visit_string<E>(self, value: String) -> Result<DnsResolverConfig, E>
            where
                E: serde::de::Error,
            {
                Ok(DnsResolverConfig::Simple(value))
            }

            fn visit_map<M>(self, map: M) -> Result<DnsResolverConfig, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                let cfg = DnsServerConfig::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )?;
                Ok(DnsResolverConfig::Detailed(cfg))
            }
        }

        deserializer.deserialize_any(DnsResolverConfigVisitor)
    }
}

/// Structured DNS server configuration.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct DnsServerConfig {
    /// DNS transport protocol type: "https", "tls", "udp", "dns", "tcp", "system".
    #[serde(rename = "type")]
    pub r#type: String,

    /// Upstream DNS server address (IP address or hostname, optionally with port).
    #[serde(default)]
    pub server: Option<String>,

    /// Upstream DNS server port (optional; defaults to standard port for protocol).
    #[serde(default)]
    pub port: Option<u16>,

    /// URL path for DNS-over-HTTPS (e.g. "/dns-query").
    #[serde(default)]
    pub path: Option<String>,

    /// TLS options for encrypted DNS (DoH and DoT).
    #[serde(default)]
    pub tls: Option<DnsTlsConfig>,
}

/// TLS settings for DNS-over-HTTPS or DNS-over-TLS.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct DnsTlsConfig {
    /// Expected SNI hostname for TLS verification (e.g. "cloudflare-dns.com").
    #[serde(default)]
    pub server_name: Option<String>,
}

/// DNS resolver configuration.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DnsConfig {
    pub strategy: Option<DnsStrategy>,

    pub resolvers: Option<Vec<DnsResolverConfig>>,

    pub cache_capacity: Option<usize>,
}

/// Socket dialing and TCP tuning options.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DialerConfig {
    #[serde(with = "humantime_serde_opt", default)]
    pub connect_timeout: Option<Duration>,

    pub tcp_nodelay: Option<bool>,
    pub bind_interface: Option<String>,
    pub proxy: Option<String>,

    #[serde(with = "humantime_serde_opt", default)]
    pub heartbeat_interval: Option<Duration>,

    #[serde(with = "humantime_serde_opt", default)]
    pub idle_timeout: Option<Duration>,

    #[serde(with = "humantime_serde_opt", default)]
    pub udp_idle_timeout: Option<Duration>,
}

pub(crate) mod humantime_serde_opt {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum DurHelper {
        Str(String),
        Secs(u64),
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<DurHelper>::deserialize(deserializer)?;
        match opt {
            Some(DurHelper::Secs(secs)) => Ok(Some(Duration::from_secs(secs))),
            Some(DurHelper::Str(s)) => {
                let s_trimmed = s.trim();
                if let Ok(secs) = s_trimmed.parse::<u64>() {
                    Ok(Some(Duration::from_secs(secs)))
                } else if let Some(stripped) = s_trimmed.strip_suffix("ms")
                    && let Ok(ms) = stripped.parse::<u64>()
                {
                    Ok(Some(Duration::from_millis(ms)))
                } else if let Some(stripped) = s_trimmed.strip_suffix('s')
                    && let Ok(secs) = stripped.parse::<u64>()
                {
                    Ok(Some(Duration::from_secs(secs)))
                } else {
                    Err(serde::de::Error::custom(format!("invalid duration: {}", s)))
                }
            }
            None => Ok(None),
        }
    }

    pub fn serialize<S>(opt: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match opt {
            Some(d) => serializer.serialize_str(&format!("{}s", d.as_secs())),
            None => serializer.serialize_none(),
        }
    }
}

/// Advanced TLS connection and verification options.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Override the SNI server name in the ClientHello.
    pub server_name: Option<String>,

    /// Skip certificate chain validation (for self-signed or test certificates).
    #[serde(default)]
    pub insecure: Option<bool>,

    /// List of ALPN protocol negotiation identifiers (e.g. ["h3", "http/1.1"]).
    pub alpn: Option<Vec<String>>,

    /// Custom CA certificate path for validating the server.
    pub ca_certificate: Option<PathBuf>,

    /// Client certificate path for mutual TLS authentication (mTLS).
    pub client_certificate: Option<PathBuf>,

    /// Client private key path for mTLS.
    pub client_key: Option<PathBuf>,

    /// Server certificate path for TLS termination.
    #[serde(default)]
    pub certificate: Option<PathBuf>,

    /// Server private key path for TLS termination.
    #[serde(default)]
    pub key: Option<PathBuf>,

    /// Minimum TLS version constraint (e.g. "1.2", "1.3").
    pub min_version: Option<String>,

    /// Maximum TLS version constraint.
    pub max_version: Option<String>,
}

impl TlsConfig {
    /// Determines whether certificate chain verification is skipped.
    pub fn is_insecure(&self) -> bool {
        self.insecure.unwrap_or(false)
    }

    /// Merges an override TLS configuration over this base configuration field-by-field.
    pub fn merge(&self, r#override: Option<&TlsConfig>) -> Self {
        let Some(over) = r#override else {
            return self.clone();
        };
        Self {
            server_name: over
                .server_name
                .clone()
                .or_else(|| self.server_name.clone()),
            insecure: over.insecure.or(self.insecure),
            alpn: over.alpn.clone().or_else(|| self.alpn.clone()),
            ca_certificate: over
                .ca_certificate
                .clone()
                .or_else(|| self.ca_certificate.clone()),
            client_certificate: over
                .client_certificate
                .clone()
                .or_else(|| self.client_certificate.clone()),
            client_key: over.client_key.clone().or_else(|| self.client_key.clone()),
            certificate: over
                .certificate
                .clone()
                .or_else(|| self.certificate.clone()),
            key: over.key.clone().or_else(|| self.key.clone()),
            min_version: over
                .min_version
                .clone()
                .or_else(|| self.min_version.clone()),
            max_version: over
                .max_version
                .clone()
                .or_else(|| self.max_version.clone()),
        }
    }
}

/// Operational mode of the tunnel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TunnelMode {
    #[default]
    Forward,
    Reverse,
}

/// An individual port mapping rule within a forward or reverse group.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MappingRuleConfig {
    /// Transport-layer protocol (TCP or UDP, case-insensitive). Required; cannot be omitted.
    pub protocol: L4Protocol,
    pub listen: String,
    pub target: String,
    #[serde(with = "humantime_serde_opt", default)]
    pub udp_idle_timeout: Option<Duration>,
}

impl MappingRuleConfig {
    pub fn tcp(listen: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            protocol: L4Protocol::Tcp,
            listen: listen.into(),
            target: target.into(),
            udp_idle_timeout: None,
        }
    }

    pub fn udp(listen: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            protocol: L4Protocol::Udp,
            listen: listen.into(),
            target: target.into(),
            udp_idle_timeout: None,
        }
    }

    pub fn udp_with_timeout(
        listen: impl Into<String>,
        target: impl Into<String>,
        timeout: Duration,
    ) -> Self {
        Self {
            protocol: L4Protocol::Udp,
            listen: listen.into(),
            target: target.into(),
            udp_idle_timeout: Some(timeout),
        }
    }

    pub fn new(protocol: L4Protocol, listen: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            protocol,
            listen: listen.into(),
            target: target.into(),
            udp_idle_timeout: None,
        }
    }
}

/// A group of mapping rules for a specific direction (forward or reverse), with an optional connection pool size.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MappingGroupConfig {
    /// 0-RTT pre-warmed connection pool capacity for this direction.
    #[serde(default, alias = "pool")]
    pub pool_size: Option<usize>,

    /// Port mapping rules in this group.
    #[serde(default, alias = "ports", alias = "items")]
    pub rules: Vec<MappingRuleConfig>,
}

impl MappingGroupConfig {
    pub fn new(rules: Vec<MappingRuleConfig>) -> Self {
        Self {
            pool_size: None,
            rules,
        }
    }

    pub fn with_pool(pool_size: usize, rules: Vec<MappingRuleConfig>) -> Self {
        Self {
            pool_size: Some(pool_size),
            rules,
        }
    }
}

/// Categorized port mappings partitioned into `forward` and `reverse` sections.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TunnelMappingsConfig {
    #[serde(default)]
    pub forward: Option<MappingGroupConfig>,

    #[serde(default)]
    pub reverse: Option<MappingGroupConfig>,
}

impl TunnelMappingsConfig {
    pub fn forward(rules: Vec<MappingRuleConfig>) -> Self {
        Self {
            forward: Some(MappingGroupConfig::new(rules)),
            reverse: None,
        }
    }

    pub fn forward_pool(pool_size: usize, rules: Vec<MappingRuleConfig>) -> Self {
        Self {
            forward: Some(MappingGroupConfig::with_pool(pool_size, rules)),
            reverse: None,
        }
    }

    pub fn reverse(rules: Vec<MappingRuleConfig>) -> Self {
        Self {
            forward: None,
            reverse: Some(MappingGroupConfig::new(rules)),
        }
    }

    pub fn reverse_pool(pool_size: usize, rules: Vec<MappingRuleConfig>) -> Self {
        Self {
            forward: None,
            reverse: Some(MappingGroupConfig::with_pool(pool_size, rules)),
        }
    }

    pub fn forward_and_reverse(forward: MappingGroupConfig, reverse: MappingGroupConfig) -> Self {
        Self {
            forward: Some(forward),
            reverse: Some(reverse),
        }
    }

    pub fn from_port_mappings(
        mappings: Vec<PortMappingConfig>,
        forward_pool: Option<usize>,
        reverse_pool: Option<usize>,
    ) -> Self {
        let mut fwd_rules = Vec::new();
        let mut rev_rules = Vec::new();
        for m in mappings {
            let rule = MappingRuleConfig {
                protocol: m.protocol,
                listen: m.listen,
                target: m.target,
                udp_idle_timeout: m.udp_idle_timeout,
            };
            match m.mode {
                TunnelMode::Forward => fwd_rules.push(rule),
                TunnelMode::Reverse => rev_rules.push(rule),
            }
        }
        Self {
            forward: if !fwd_rules.is_empty() || forward_pool.is_some() {
                Some(MappingGroupConfig {
                    pool_size: forward_pool,
                    rules: fwd_rules,
                })
            } else {
                None
            },
            reverse: if !rev_rules.is_empty() || reverse_pool.is_some() {
                Some(MappingGroupConfig {
                    pool_size: reverse_pool,
                    rules: rev_rules,
                })
            } else {
                None
            },
        }
    }

    pub fn is_empty(&self) -> bool {
        let f_empty = self.forward.as_ref().is_none_or(|g| g.rules.is_empty());
        let r_empty = self.reverse.as_ref().is_none_or(|g| g.rules.is_empty());
        f_empty && r_empty
    }

    pub fn rule_count(&self) -> usize {
        let f = self.forward.as_ref().map_or(0, |g| g.rules.len());
        let r = self.reverse.as_ref().map_or(0, |g| g.rules.len());
        f + r
    }
}

impl From<Vec<PortMappingConfig>> for TunnelMappingsConfig {
    fn from(mappings: Vec<PortMappingConfig>) -> Self {
        Self::from_port_mappings(mappings, None, None)
    }
}

/// Canonical internal representation of a port mapping bound to a specific tunnel mode.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct PortMappingConfig {
    pub mode: TunnelMode,
    pub protocol: L4Protocol,
    pub listen: String,
    pub target: String,
    #[serde(with = "humantime_serde_opt", default)]
    pub udp_idle_timeout: Option<Duration>,
}

/// Tunnel retry configuration.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TunnelRetryConfig {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration"
    )]
    pub delay: Option<Duration>,
    pub max_retries: Option<usize>,
}

fn deserialize_optional_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum DurHelper {
        Str(String),
        Secs(u64),
    }

    match Option::<DurHelper>::deserialize(deserializer)? {
        Some(DurHelper::Str(s)) => {
            let s = s.trim();
            if let Some(stripped) = s.strip_suffix("ms") {
                stripped
                    .parse::<u64>()
                    .map(|ms| Some(Duration::from_millis(ms)))
                    .map_err(serde::de::Error::custom)
            } else if let Some(stripped) = s.strip_suffix('s') {
                stripped
                    .parse::<u64>()
                    .map(|sec| Some(Duration::from_secs(sec)))
                    .map_err(serde::de::Error::custom)
            } else {
                s.parse::<u64>()
                    .map(|sec| Some(Duration::from_secs(sec)))
                    .map_err(serde::de::Error::custom)
            }
        }
        Some(DurHelper::Secs(secs)) => Ok(Some(Duration::from_secs(secs))),
        None => Ok(None),
    }
}

fn serialize_optional_duration<S>(opt: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match opt {
        Some(d) => serializer.serialize_str(&format!("{}s", d.as_secs())),
        None => serializer.serialize_none(),
    }
}

/// Configuration for an individual tunnel instance.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TunnelConfig {
    pub tag: String,
    /// Unique identifier for this tunnel instance (Tunnel UUID).
    /// If omitted, a deterministic UUID v5 derived from `tag` is used.
    #[serde(default)]
    pub uuid: Option<String>,
    pub endpoint: EndpointConfig,
    pub token: Option<String>,
    pub failover: Option<FailoverConfig>,
    pub multiplex: Option<MultiplexConfig>,
    pub retry: Option<TunnelRetryConfig>,

    /// Custom HTTP request headers sent when connecting to the server (e.g. Host override, User-Agent).
    #[serde(default)]
    pub http_headers: Option<std::collections::HashMap<String, String>>,

    /// Directional port mappings categorized into forward and reverse groups.
    pub mappings: TunnelMappingsConfig,
}

impl TunnelConfig {
    /// Returns the canonical list of port mappings configured for this tunnel.
    pub fn canonical_mappings(
        &self,
    ) -> Result<Vec<PortMappingConfig>, crate::core::error::TunnelError> {
        let mut list = Vec::new();

        if let Some(ref fwd) = self.mappings.forward {
            for r in &fwd.rules {
                list.push(PortMappingConfig {
                    mode: TunnelMode::Forward,
                    protocol: r.protocol,
                    listen: r.listen.clone(),
                    target: r.target.clone(),
                    udp_idle_timeout: r.udp_idle_timeout,
                });
            }
        }

        if let Some(ref rev) = self.mappings.reverse {
            for r in &rev.rules {
                list.push(PortMappingConfig {
                    mode: TunnelMode::Reverse,
                    protocol: r.protocol,
                    listen: r.listen.clone(),
                    target: r.target.clone(),
                    udp_idle_timeout: r.udp_idle_timeout,
                });
            }
        }

        if list.is_empty() {
            Err(crate::core::error::TunnelError::Config(format!(
                "Tunnel '{}' has no port mappings configured. Please provide 'mappings.forward' or 'mappings.reverse' with at least one rule.",
                self.tag
            )))
        } else {
            Ok(list)
        }
    }

    /// Effective forward pool size from `mappings.forward.pool_size`.
    pub fn forward_pool_size(&self) -> Option<usize> {
        self.mappings.forward.as_ref().and_then(|g| g.pool_size)
    }

    /// Effective reverse pool size from `mappings.reverse.pool_size`.
    pub fn reverse_pool_size(&self) -> Option<usize> {
        self.mappings.reverse.as_ref().and_then(|g| g.pool_size)
    }

    /// Effective tunnel UUID: prefers configured `uuid`, falls back to deterministic UUID v5 from `tag`.
    pub fn effective_tunnel_id(&self) -> String {
        if let Some(ref id) = self.uuid {
            id.clone()
        } else {
            uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, self.tag.as_bytes()).to_string()
        }
    }
}

/// Stream multiplexing configuration (sing-box style generic layer over any transport).
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MultiplexConfig {
    #[serde(default)]
    pub enabled: bool,

    /// Maximum concurrent logical streams over a single underlying transport (default: 128).
    #[serde(default = "default_mux_max_streams")]
    pub max_streams: usize,

    /// Enable frame padding to obfuscate packet length distributions.
    #[serde(default)]
    pub padding: bool,
}

fn default_mux_max_streams() -> usize {
    128
}

/// Endpoint definition supporting either a simple URL or detailed context.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum EndpointConfig {
    Simple(String),
    Detailed {
        url: String,
        dns: Option<DnsConfig>,
        dialer: Option<DialerConfig>,
        tls: Option<TlsConfig>,
        http_headers: Option<std::collections::HashMap<String, String>>,
    },
}

impl EndpointConfig {
    /// Extracts the URL string regardless of the variant.
    pub fn url(&self) -> &str {
        match self {
            EndpointConfig::Simple(s) => s,
            EndpointConfig::Detailed { url, .. } => url,
        }
    }
}

impl Serialize for EndpointConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            EndpointConfig::Simple(s) => serializer.serialize_str(s),
            EndpointConfig::Detailed {
                url,
                dns,
                dialer,
                tls,
                http_headers,
            } => {
                use serde::ser::SerializeStruct;
                let mut state = serializer.serialize_struct("EndpointConfig", 5)?;
                state.serialize_field("url", url)?;
                if let Some(d) = dns {
                    state.serialize_field("dns", d)?;
                }
                if let Some(dl) = dialer {
                    state.serialize_field("dialer", dl)?;
                }
                if let Some(t) = tls {
                    state.serialize_field("tls", t)?;
                }
                if let Some(h) = http_headers {
                    state.serialize_field("http_headers", h)?;
                }
                state.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for EndpointConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct EndpointConfigVisitor;

        impl<'de> serde::de::Visitor<'de> for EndpointConfigVisitor {
            type Value = EndpointConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a URL string or an endpoint map containing 'url'")
            }

            fn visit_str<E>(self, value: &str) -> Result<EndpointConfig, E>
            where
                E: serde::de::Error,
            {
                Ok(EndpointConfig::Simple(value.to_string()))
            }

            fn visit_string<E>(self, value: String) -> Result<EndpointConfig, E>
            where
                E: serde::de::Error,
            {
                Ok(EndpointConfig::Simple(value))
            }

            fn visit_map<M>(self, map: M) -> Result<EndpointConfig, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct DetailedHelper {
                    url: String,
                    #[serde(default)]
                    dns: Option<DnsConfig>,
                    #[serde(default)]
                    dialer: Option<DialerConfig>,
                    #[serde(default)]
                    tls: Option<TlsConfig>,
                    #[serde(default)]
                    http_headers: Option<std::collections::HashMap<String, String>>,
                }

                let helper =
                    DetailedHelper::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
                Ok(EndpointConfig::Detailed {
                    url: helper.url,
                    dns: helper.dns,
                    dialer: helper.dialer,
                    tls: helper.tls,
                    http_headers: helper.http_headers,
                })
            }
        }

        deserializer.deserialize_any(EndpointConfigVisitor)
    }
}

/// Failover configuration for dynamic seamless transport migration.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FailoverConfig {
    #[serde(default)]
    pub enabled: bool,
    pub standby_endpoint: EndpointConfig,
    pub ring_buffer_capacity: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::parse_socket_mode;

    #[test]
    fn socket_modes_are_always_octal() {
        assert_eq!(parse_socket_mode("660"), Some(0o660));
        assert_eq!(parse_socket_mode("0660"), Some(0o660));
        assert_eq!(parse_socket_mode("0o660"), Some(0o660));
        assert_eq!(parse_socket_mode("0O660"), Some(0o660));
        assert_eq!(parse_socket_mode("7777"), Some(0o7777));
        assert_eq!(parse_socket_mode("10000"), None, "mode above 0o7777");
        assert_eq!(parse_socket_mode("888"), None, "non-octal digit");
        assert_eq!(parse_socket_mode(""), None);
    }
}
