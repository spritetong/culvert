// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

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
        let scheme = if let Some(idx) = url_str.find("://") {
            url_str[..idx].to_lowercase()
        } else {
            return Err(TunnelError::Config(format!(
                "Missing scheme in endpoint URL: {}",
                url_str
            )));
        };

        if scheme == "ws+unix" || scheme == "wss+unix" {
            let path_raw = &url_str[scheme.len() + 3..];
            let (path_part, query_part) = if let Some(q_idx) = path_raw.find('?') {
                (&path_raw[..q_idx], Some(path_raw[q_idx + 1..].to_string()))
            } else {
                (path_raw, None)
            };
            let norm_path = crate::platform::normalize_uds_path(path_part);
            let mut path_str = norm_path.to_string_lossy().to_string();
            if path_str.is_empty() {
                path_str = "/".to_string();
            }
            return Ok(Self {
                scheme,
                host: "localhost".to_string(),
                port: 0,
                path: path_str,
                query: query_part,
            });
        }

        if !matches!(scheme.as_str(), "ws" | "wss" | "https" | "h3" | "quic") {
            return Err(TunnelError::Config(format!(
                "Unsupported endpoint scheme '{}' in URL '{}'. Supported schemes: ws, wss, https, h3, quic, ws+unix, wss+unix",
                scheme, url_str
            )));
        }

        let parsed = Url::parse(url_str)?;
        let host = parsed
            .host_str()
            .ok_or_else(|| {
                TunnelError::Config(format!("Missing host in endpoint URL: {}", url_str))
            })?
            .to_string();

        let port = parsed.port().unwrap_or(match scheme.as_str() {
            "https" | "wss" | "h3" | "quic" => crate::consts::DEFAULT_HTTPS_PORT,
            "ws" => crate::consts::DEFAULT_HTTP_PORT,
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
    pub dialer_config: DialerConfig,
    pub tls_config: Option<TlsConfig>,
    pub http_headers: Option<std::collections::HashMap<String, String>>,
}

impl EndpointContext {
    /// Returns the effective heartbeat interval (configured or protocol default).
    pub fn heartbeat_interval(&self) -> std::time::Duration {
        self.dialer_config
            .heartbeat_interval
            .unwrap_or(crate::consts::DEFAULT_HEARTBEAT_INTERVAL)
    }

    /// Returns the effective idle timeout (configured or protocol default).
    pub fn idle_timeout(&self) -> std::time::Duration {
        self.dialer_config
            .idle_timeout
            .unwrap_or(crate::consts::DEFAULT_IDLE_TIMEOUT)
    }

    /// Returns the effective connect timeout (configured or protocol default).
    pub fn connect_timeout(&self) -> std::time::Duration {
        self.dialer_config
            .connect_timeout
            .unwrap_or(crate::consts::DEFAULT_CONNECT_TIMEOUT)
    }

    /// Returns the effective UDP NAT session idle timeout (configured or protocol default).
    pub fn udp_idle_timeout(&self) -> std::time::Duration {
        self.dialer_config
            .udp_idle_timeout
            .unwrap_or(crate::consts::DEFAULT_UDP_IDLE_TIMEOUT)
    }

    /// Builds an `EndpointContext` by merging per-endpoint configuration with global defaults.
    pub fn build(
        endpoint_cfg: &EndpointConfig,
        global_app: &AppConfig,
    ) -> Result<Self, TunnelError> {
        let raw_url = endpoint_cfg.url();
        let endpoint = Endpoint::parse(raw_url)?;

        // Extract per-endpoint overrides if available
        let (endpoint_dns, endpoint_dialer, endpoint_tls, endpoint_headers) = match endpoint_cfg {
            EndpointConfig::Simple(_) => (None, None, None, None),
            EndpointConfig::Detailed {
                dns,
                dialer,
                tls,
                http_headers,
                ..
            } => (
                dns.as_ref(),
                dialer.as_ref(),
                tls.as_ref(),
                http_headers.clone(),
            ),
        };

        // Resolve effective DNS configuration
        let effective_dns = DnsConfig {
            strategy: endpoint_dns
                .and_then(|d| d.strategy)
                .or_else(|| global_app.dns.as_ref().and_then(|d| d.strategy)),
            resolvers: endpoint_dns
                .and_then(|d| d.resolvers.clone())
                .or_else(|| global_app.dns.as_ref().and_then(|d| d.resolvers.clone())),
            cache_capacity: endpoint_dns
                .and_then(|d| d.cache_capacity)
                .or_else(|| global_app.dns.as_ref().and_then(|d| d.cache_capacity)),
        };

        let dns_resolver: Arc<dyn DnsResolver> =
            Arc::new(CachedDnsResolver::try_new(effective_dns)?);

        // Resolve effective Dialer options
        let effective_dialer = DialerConfig {
            connect_timeout: endpoint_dialer
                .and_then(|d| d.connect_timeout)
                .or_else(|| global_app.dialer.as_ref().and_then(|d| d.connect_timeout)),
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
            heartbeat_interval: endpoint_dialer
                .and_then(|d| d.heartbeat_interval)
                .or_else(|| {
                    global_app
                        .dialer
                        .as_ref()
                        .and_then(|d| d.heartbeat_interval)
                }),
            idle_timeout: endpoint_dialer
                .and_then(|d| d.idle_timeout)
                .or_else(|| global_app.dialer.as_ref().and_then(|d| d.idle_timeout)),
            udp_idle_timeout: endpoint_dialer
                .and_then(|d| d.udp_idle_timeout)
                .or_else(|| global_app.dialer.as_ref().and_then(|d| d.udp_idle_timeout)),
        };

        let dialer: Arc<dyn Dialer> = Arc::new(DefaultDialer::new(
            effective_dialer.clone(),
            Arc::clone(&dns_resolver),
        ));

        // Resolve effective TLS options based purely on whether endpoint scheme is TLS-based
        let is_tls_scheme = matches!(
            endpoint.scheme.as_str(),
            "https" | "wss" | "h3" | "quic" | "wss+unix"
        );
        if !is_tls_scheme && endpoint_tls.is_some() {
            return Err(TunnelError::Config(format!(
                "Endpoint '{}' uses non-TLS scheme '{}', but 'tls' options are configured. Use a TLS scheme (e.g. wss://, h3://) or remove the 'tls' block.",
                raw_url, endpoint.scheme
            )));
        }

        let effective_tls = if is_tls_scheme {
            let mut tls = match (global_app.tls.as_ref(), endpoint_tls) {
                (Some(g), Some(e)) => g.merge(Some(e)),
                (Some(g), None) => g.clone(),
                (None, Some(e)) => e.clone(),
                (None, None) => crate::config::model::TlsConfig::default(),
            };
            if tls.server_name.is_none() {
                tls.server_name = Some(endpoint.host.clone());
            }
            Some(tls)
        } else {
            None
        };

        Ok(Self {
            endpoint,
            dns_resolver,
            dialer,
            dialer_config: effective_dialer,
            tls_config: effective_tls,
            http_headers: endpoint_headers,
        })
    }

    /// Attaches or replaces custom HTTP headers sent during handshake.
    pub fn with_http_headers(
        mut self,
        headers: Option<std::collections::HashMap<String, String>>,
    ) -> Self {
        self.http_headers = headers;
        self
    }

    /// Merges custom HTTP headers into this endpoint context, overriding duplicates case-insensitively.
    pub fn merge_http_headers(&mut self, headers: &std::collections::HashMap<String, String>) {
        if let Some(ref mut existing) = self.http_headers {
            for (k, v) in headers {
                existing.retain(|ek, _| !ek.eq_ignore_ascii_case(k));
                existing.insert(k.clone(), v.clone());
            }
        } else {
            self.http_headers = Some(headers.clone());
        }
    }

    /// Builds an `EndpointContext` for a specific `TunnelConfig`, incorporating tunnel-level overrides.
    pub fn build_for_tunnel(
        tunnel_cfg: &crate::config::model::TunnelConfig,
        global_app: &AppConfig,
    ) -> Result<Self, TunnelError> {
        let mut ctx = Self::build(&tunnel_cfg.endpoint, global_app)?;
        if let Some(ref headers) = tunnel_cfg.http_headers {
            ctx.merge_http_headers(headers);
        }
        Ok(ctx)
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
