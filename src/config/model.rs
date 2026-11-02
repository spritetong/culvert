//! Declarative YAML configuration data models.
//! All comments and docstrings must be in English.

use std::path::PathBuf;
use std::time::Duration;
use serde::Deserialize;

/// Top-level application configuration structure.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AppConfig {
    #[serde(default)]
    pub log: LogConfig,

    pub dns: Option<DnsConfig>,
    pub dialer: Option<DialerConfig>,
    pub tls: Option<TlsConfig>,

    #[serde(default)]
    pub tunnels: Vec<TunnelConfig>,
}

/// Logging configuration options.
#[derive(Debug, Clone, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,

    #[serde(default = "default_log_output")]
    pub output: String,
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
        }
    }
}

/// IP address family selection strategy for DNS resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DnsStrategy {
    #[default]
    PreferIpv4,
    PreferIpv6,
    Ipv4Only,
    Ipv6Only,
}

/// DNS resolver configuration.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct DnsConfig {
    pub strategy: Option<DnsStrategy>,
    pub servers: Option<Vec<String>>,
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
                    if let Some(stripped) = s_trimmed.strip_suffix('s') {
                        if let Ok(secs) = stripped.parse::<u64>() {
                            return Ok(Some(Duration::from_secs(secs)));
                        }
                    }
                    if let Some(stripped) = s_trimmed.strip_suffix("ms") {
                        if let Ok(ms) = stripped.parse::<u64>() {
                            return Ok(Some(Duration::from_millis(ms)));
                        }
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

    /// Minimum TLS version constraint (e.g. "1.2", "1.3").
    pub min_version: Option<String>,

    /// Maximum TLS version constraint.
    pub max_version: Option<String>,
}

/// Operational mode of the tunnel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TunnelMode {
    Forward,
    Reverse,
}

/// Configuration for an individual tunnel instance.
#[derive(Debug, Clone, Deserialize)]
pub struct TunnelConfig {
    pub tag: String,
    pub mode: TunnelMode,
    pub listen: String,
    pub target: String,
    pub token: String,
    pub endpoint: EndpointConfig,
    pub failover: Option<FailoverConfig>,
}

/// Endpoint definition supporting either a simple URL or detailed context.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
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
