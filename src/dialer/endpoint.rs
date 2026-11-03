//! URL parsing and EndpointContext resolution.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use url::Url;

use crate::config::model::{AppConfig, DialerConfig, DnsConfig, EndpointConfig, TlsConfig};
use crate::core::error::TunnelError;
use crate::dialer::dns::{CachedDnsResolver, DnsResolver};
use crate::dialer::unified::{DefaultDialer, Dialer};

/// Structured representation of a parsed network endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub query: Option<String>,
}

impl Endpoint {
    /// Parses an endpoint URL string into a structured `Endpoint`.
    pub fn parse(url_str: &str) -> Result<Self, TunnelError> {
        let parsed = Url::parse(url_str)?;
        let scheme = parsed.scheme().to_lowercase();
        let host = parsed
            .host_str()
            .ok_or_else(|| {
                TunnelError::Config(format!("Missing host in endpoint URL: {}", url_str))
            })?
            .to_string();

        let port = parsed.port().unwrap_or_else(|| match scheme.as_str() {
            "https" | "wss" | "h3" | "quic" => 443,
            "http" | "ws" => 80,
            _ => 0,
        });

        let path = if parsed.path().is_empty() {
            "/".to_string()
        } else {
            parsed.path().to_string()
        };

        let query = parsed.query().map(|q| q.to_string());

        Ok(Self {
            scheme,
            host,
            port,
            path,
            query,
        })
    }
}

/// Runtime context bound to an endpoint with resolved DNS, Dialer, and TLS policies.
#[derive(Clone)]
pub struct EndpointContext {
    pub endpoint: Endpoint,
    pub dns_resolver: Arc<dyn DnsResolver>,
    pub dialer: Arc<dyn Dialer>,
    pub tls_config: Option<TlsConfig>,
}

impl EndpointContext {
    /// Builds an `EndpointContext` by merging per-endpoint configuration with global defaults.
    pub fn build(
        endpoint_cfg: &EndpointConfig,
        global_app: &AppConfig,
    ) -> Result<Self, TunnelError> {
        let raw_url = endpoint_cfg.url();
        let endpoint = Endpoint::parse(raw_url)?;

        // Extract per-endpoint overrides if available
        let (endpoint_dns, endpoint_dialer, endpoint_tls) = match endpoint_cfg {
            EndpointConfig::Simple(_) => (None, None, None),
            EndpointConfig::Detailed {
                dns, dialer, tls, ..
            } => (dns.as_ref(), dialer.as_ref(), tls.as_ref()),
        };

        // Resolve effective DNS configuration
        let effective_dns = DnsConfig {
            strategy: endpoint_dns
                .and_then(|d| d.strategy)
                .or_else(|| global_app.dns.as_ref().and_then(|d| d.strategy)),
            servers: endpoint_dns
                .and_then(|d| d.servers.clone())
                .or_else(|| global_app.dns.as_ref().and_then(|d| d.servers.clone())),
            cache_capacity: endpoint_dns
                .and_then(|d| d.cache_capacity)
                .or_else(|| global_app.dns.as_ref().and_then(|d| d.cache_capacity)),
        };

        let dns_resolver: Arc<dyn DnsResolver> = Arc::new(CachedDnsResolver::new(effective_dns));

        // Resolve effective Dialer options
        let effective_dialer = DialerConfig {
            connect_timeout: endpoint_dialer
                .and_then(|d| d.connect_timeout)
                .or_else(|| global_app.dialer.as_ref().and_then(|d| d.connect_timeout)),
            tcp_keepalive: endpoint_dialer
                .and_then(|d| d.tcp_keepalive)
                .or_else(|| global_app.dialer.as_ref().and_then(|d| d.tcp_keepalive)),
            tcp_nodelay: endpoint_dialer
                .and_then(|d| d.tcp_nodelay)
                .or_else(|| global_app.dialer.as_ref().and_then(|d| d.tcp_nodelay)),
            bind_interface: endpoint_dialer
                .and_then(|d| d.bind_interface.clone())
                .or_else(|| {
                    global_app
                        .dialer
                        .as_ref()
                        .and_then(|d| d.bind_interface.clone())
                }),
        };

        let dialer: Arc<dyn Dialer> = Arc::new(DefaultDialer::new(
            effective_dialer,
            Arc::clone(&dns_resolver),
        ));

        // Resolve effective TLS options
        let mut effective_tls = endpoint_tls.cloned().or_else(|| global_app.tls.clone());

        // Automatically infer TLS if scheme is https/wss/h3/quic and not explicitly disabled
        if matches!(endpoint.scheme.as_str(), "https" | "wss" | "h3" | "quic") {
            let tls = effective_tls.get_or_insert_with(TlsConfig::default);
            tls.enabled = true;
            if tls.server_name.is_none() {
                tls.server_name = Some(endpoint.host.clone());
            }
        }

        Ok(Self {
            endpoint,
            dns_resolver,
            dialer,
            tls_config: effective_tls,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_parse_wss() {
        let ep = Endpoint::parse("wss://tunnel.example.com/ws?token=123").unwrap();
        assert_eq!(ep.scheme, "wss");
        assert_eq!(ep.host, "tunnel.example.com");
        assert_eq!(ep.port, 443);
        assert_eq!(ep.path, "/ws");
        assert_eq!(ep.query, Some("token=123".to_string()));
    }

    #[test]
    fn test_endpoint_parse_custom_port() {
        let ep = Endpoint::parse("https://127.0.0.1:8443/tunnel").unwrap();
        assert_eq!(ep.scheme, "https");
        assert_eq!(ep.host, "127.0.0.1");
        assert_eq!(ep.port, 8443);
        assert_eq!(ep.path, "/tunnel");
    }
}
