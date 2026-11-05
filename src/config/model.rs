// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Declarative YAML configuration data models.
//! All comments and docstrings must be in English.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Top-level application configuration structure.
#[derive(Debug, Clone, Deserialize, Default)]
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

/// Parsed server listening specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerListenSpec {
    pub scheme: String,
    pub bind_addr: String,
    pub path: Option<String>,
}

impl ServerListenSpec {
    pub fn parse(raw: &str) -> Result<Self, crate::core::error::TunnelError> {
        let raw = raw.trim();
        let (scheme, rest) = if let Some(idx) = raw.find("://") {
            let s = &raw[..idx];
            let r = &raw[idx + 3..];
            (s.to_lowercase(), r)
        } else {
            return Err(crate::core::error::TunnelError::Config(format!(
                "Invalid server listen URL '{}'. Protocol scheme must be explicitly specified (e.g. ws://0.0.0.0:8080/tunnel, h3://0.0.0.0:8443/tunnel, uds:///tmp/tunnel.sock). Implicit protocol is not supported.",
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

                if host_port.is_empty() {
                    return Err(crate::core::error::TunnelError::Config(format!(
                        "Invalid server listen URL '{}': missing host and port",
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
                })
            }
            "uds" => {
                let socket_path = if rest.starts_with('/') {
                    rest.to_string()
                } else {
                    format!("/{}", rest)
                };
                Ok(Self {
                    scheme: "uds".to_string(),
                    bind_addr: socket_path,
                    path: None,
                })
            }
            other => Err(crate::core::error::TunnelError::Config(format!(
                "Unsupported server protocol scheme '{}' in URL '{}'. Supported schemes: ws, wss, h3, quic, uds",
                other, raw
            ))),
        }
    }
}

fn deserialize_one_or_many<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }

    match OneOrMany::deserialize(deserializer)? {
        OneOrMany::One(s) => Ok(vec![s]),
        OneOrMany::Many(vec) => Ok(vec),
    }
}

/// Server configuration for running as an ingress listener on VPS or cloud host.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ServerConfig {
    /// Ingress listening URLs (e.g. "ws://0.0.0.0:8080/tunnel", ["ws://...", "h3://..."]).
    #[serde(
        default,
        deserialize_with = "deserialize_one_or_many"
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
}

impl ServerConfig {
    pub fn new(listen: Vec<String>, token: Option<String>, default_target: Option<String>) -> Self {
        Self {
            listen,
            token,
            default_target,
            certificate: None,
            key: None,
            tls: None,
        }
    }

    /// Returns the effective `TlsConfig` for server termination, merging explicit fields.
    pub fn effective_tls(&self, global_tls: Option<&TlsConfig>) -> Option<TlsConfig> {
        let base = self.tls.clone().or_else(|| global_tls.cloned());

        if self.certificate.is_some() || self.key.is_some() {
            let mut cfg = base.unwrap_or_default();
            if cfg.certificate.is_none() {
                cfg.certificate = self.certificate.as_ref().map(PathBuf::from);
            }
            if cfg.key.is_none() {
                cfg.key = self.key.as_ref().map(PathBuf::from);
            }
            cfg.enabled = true;
            Some(cfg)
        } else {
            base
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
#[derive(Debug, Clone, Deserialize)]
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
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
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

/// Structured DNS server configuration.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
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
pub struct DnsTlsConfig {
    /// Expected SNI hostname for TLS verification (e.g. "cloudflare-dns.com").
    #[serde(default)]
    pub server_name: Option<String>,

    /// Skip TLS certificate verification (default: false).
    #[serde(default)]
    pub insecure: bool,
}

/// DNS resolver configuration.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DnsConfig {
    pub strategy: Option<DnsStrategy>,

    pub resolvers: Option<Vec<DnsResolverConfig>>,

    pub cache_capacity: Option<usize>,
}

/// Socket dialing and TCP tuning options.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct DialerConfig {
    #[serde(with = "humantime_serde_opt", default)]
    pub connect_timeout: Option<Duration>,

    #[serde(with = "humantime_serde_opt", default)]
    pub tcp_keepalive: Option<Duration>,

    pub tcp_nodelay: Option<bool>,
    pub bind_interface: Option<String>,
}

mod humantime_serde_opt {
    use serde::{Deserialize, Deserializer};
    use std::time::Duration;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<String>::deserialize(deserializer)?;
        match opt {
            Some(s) => {
                // If it's a raw number of seconds or a duration string like "5s"
                if let Ok(secs) = s.parse::<u64>() {
                    Ok(Some(Duration::from_secs(secs)))
                } else {
                    let s_trimmed = s.trim();
                    if let Some(stripped) = s_trimmed.strip_suffix('s')
                        && let Ok(secs) = stripped.parse::<u64>()
                    {
                        return Ok(Some(Duration::from_secs(secs)));
                    }
                    if let Some(stripped) = s_trimmed.strip_suffix("ms")
                        && let Ok(ms) = stripped.parse::<u64>()
                    {
                        return Ok(Some(Duration::from_millis(ms)));
                    }
                    Err(serde::de::Error::custom(format!("invalid duration: {}", s)))
                }
            }
            None => Ok(None),
        }
    }
}

/// Advanced TLS connection and verification options.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,

    /// Override the SNI server name in the ClientHello.
    pub server_name: Option<String>,

    /// Skip certificate chain validation (for self-signed or test certificates).
    #[serde(default)]
    pub insecure: bool,

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

/// Operational mode of the tunnel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TunnelMode {
    Forward,
    Reverse,
}

/// Individual bidirectional port mapping definition.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PortMappingConfig {
    #[serde(default = "default_mapping_mode")]
    pub mode: TunnelMode,
    pub listen: String,
    pub target: String,
}

fn default_mapping_mode() -> TunnelMode {
    TunnelMode::Forward
}

/// Tunnel retry configuration.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
pub struct TunnelRetryConfig {
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
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

/// Configuration for an individual tunnel instance.
#[derive(Debug, Clone, Deserialize)]
pub struct TunnelConfig {
    pub tag: String,
    pub endpoint: EndpointConfig,
    pub token: Option<String>,
    #[serde(default)]
    pub pool_size: Option<usize>,
    pub failover: Option<FailoverConfig>,
    pub multiplex: Option<MultiplexConfig>,
    pub retry: Option<TunnelRetryConfig>,

    /// Multiple bidirectional port mappings sharing this endpoint and connection pool.
    #[serde(default)]
    pub mappings: Vec<PortMappingConfig>,
}

impl TunnelConfig {
    /// Returns the canonical list of port mappings configured for this tunnel.
    pub fn canonical_mappings(
        &self,
    ) -> Result<Vec<PortMappingConfig>, crate::core::error::TunnelError> {
        if !self.mappings.is_empty() {
            Ok(self.mappings.clone())
        } else {
            Err(crate::core::error::TunnelError::Config(format!(
                "Tunnel '{}' has no port mappings configured. Please provide 'mappings: [...]'.",
                self.tag
            )))
        }
    }
}

/// Stream multiplexing configuration (sing-box style generic layer over any transport).
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
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
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum EndpointConfig {
    Simple(String),
    Detailed {
        url: String,
        dns: Option<DnsConfig>,
        dialer: Option<DialerConfig>,
        tls: Option<TlsConfig>,
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

/// Failover configuration for dynamic seamless transport migration.
#[derive(Debug, Clone, Deserialize)]
pub struct FailoverConfig {
    #[serde(default)]
    pub enabled: bool,
    pub standby_endpoint: EndpointConfig,
    pub ring_buffer_capacity: Option<usize>,
}
