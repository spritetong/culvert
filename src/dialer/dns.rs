// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! DNS resolution engine supporting IPv4/IPv6 strategy selection, caching,
//! and modern DNS transports (UDP, DoH, DoT, and system/libc resolver).
//! All comments and docstrings must be in English.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::{IpAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Instant;

use hickory_resolver::config::{NameServerConfig, ResolverConfig};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::{Resolver, TokioResolver};
use tracing::{debug, warn};

use crate::config::model::{DnsConfig, DnsResolverConfig, DnsServerConfig, DnsStrategy};
use crate::consts::{DEFAULT_DNS_CACHE_CAPACITY, DEFAULT_DNS_CACHE_TTL};
use crate::core::error::TunnelError;
use crate::core::stream::BoxFuture;

/// Abstract DNS resolver interface.
pub trait DnsResolver: Send + Sync {
    /// Resolves a hostname to a list of IP addresses according to the configured strategy.
    fn resolve<'a>(&'a self, host: &'a str) -> BoxFuture<'a, Result<Vec<IpAddr>, TunnelError>>;

    /// Returns the currently active DNS resolution strategy.
    fn strategy(&self) -> DnsStrategy;
}

/// Parsed DNS resolver target backend.
#[derive(Debug, Clone)]
pub enum ParsedDnsServer {
    /// Native operating system libc resolver (system://0.0.0.0).
    System,
    /// Upstream DNS nameserver (UDP, DNS-over-HTTPS, or DNS-over-TLS).
    NameServer(NameServerConfig),
}

/// In-memory cached DNS resolver supporting UDP, DoH, DoT, and system libc.
pub struct CachedDnsResolver {
    config: DnsConfig,
    cache: RwLock<HashMap<String, (Instant, Vec<IpAddr>)>>,
    capacity: usize,
    hickory_resolver: Option<Arc<TokioResolver>>,
    has_system_resolver: bool,
}

impl CachedDnsResolver {
    /// Creates a new `CachedDnsResolver` with the provided configuration, returning an error on invalid configuration.
    pub fn try_new(config: DnsConfig) -> Result<Self, TunnelError> {
        let capacity = config.cache_capacity.unwrap_or(DEFAULT_DNS_CACHE_CAPACITY);
        let mut name_servers = Vec::new();
        let mut has_system_resolver = false;

        if let Some(ref resolvers) = config.resolvers {
            for resolver in resolvers {
                match Self::parse_resolver(resolver)? {
                    ParsedDnsServer::System => {
                        has_system_resolver = true;
                    }
                    ParsedDnsServer::NameServer(ns) => {
                        name_servers.push(ns);
                    }
                }
            }
        }

        let hickory_resolver = if !name_servers.is_empty() {
            let resolver_config = ResolverConfig::from_name_servers(name_servers);
            match Resolver::builder_with_config(resolver_config, TokioRuntimeProvider::default())
                .build()
            {
                Ok(res) => Some(Arc::new(res)),
                Err(e) => {
                    warn!(
                        error = ?e,
                        "Failed to build custom Hickory DNS resolver, falling back to system"
                    );
                    has_system_resolver = true;
                    None
                }
            }
        } else if !has_system_resolver {
            // When no custom servers are configured, attempt system-config TokioResolver
            match TokioResolver::builder_tokio() {
                Ok(builder) => match builder.build() {
                    Ok(res) => Some(Arc::new(res)),
                    Err(_) => {
                        has_system_resolver = true;
                        None
                    }
                },
                Err(_) => {
                    has_system_resolver = true;
                    None
                }
            }
        } else {
            None
        };

        Ok(Self {
            config,
            cache: RwLock::new(HashMap::with_capacity(capacity)),
            capacity,
            hickory_resolver,
            has_system_resolver,
        })
    }

    /// Creates a new `CachedDnsResolver` with the provided configuration.
    ///
    /// # Panics
    /// Panics if the resolver configuration is invalid. Prefer [`CachedDnsResolver::try_new`].
    #[deprecated(note = "Prefer CachedDnsResolver::try_new to handle configuration errors gracefully")]
    pub fn new(config: DnsConfig) -> Self {
        Self::try_new(config).expect("invalid DNS configuration")
    }

    /// Parses a DNS resolver configuration into a `ParsedDnsServer`.
    pub fn parse_resolver(resolver: &DnsResolverConfig) -> Result<ParsedDnsServer, TunnelError> {
        match resolver {
            DnsResolverConfig::Detailed(cfg) => Self::parse_detailed_server(cfg),
            DnsResolverConfig::Simple(raw) => Self::parse_server(raw),
        }
    }

    /// Parses a structured `DnsServerConfig` into a `ParsedDnsServer`.
    pub fn parse_detailed_server(cfg: &DnsServerConfig) -> Result<ParsedDnsServer, TunnelError> {
        let proto = cfg.r#type.trim().to_lowercase();
        if proto == "system" {
            return Ok(ParsedDnsServer::System);
        }

        let raw_server = cfg.server.as_deref().unwrap_or("").trim();
        if raw_server.is_empty() {
            return Err(TunnelError::Config(format!(
                "Missing 'server' field for DNS resolver of type '{}'",
                proto
            )));
        }

        let (host_str, extracted_port) = Self::extract_host_and_port(raw_server);
        let ip = Self::resolve_or_parse_ip(&host_str)?;
        let effective_port = cfg.port.or(extracted_port);

        let sni: Arc<str> = cfg
            .tls
            .as_ref()
            .and_then(|t| t.server_name.as_deref())
            .map(Arc::from)
            .unwrap_or_else(|| Arc::from(host_str.as_str()));

        match proto.as_str() {
            "udp" | "dns" => {
                let port = effective_port.unwrap_or(53);
                let mut ns = NameServerConfig::udp(ip);
                if let Some(conn) = ns.connections.first_mut() {
                    conn.port = port;
                }
                Ok(ParsedDnsServer::NameServer(ns))
            }
            "tcp" => {
                let port = effective_port.unwrap_or(53);
                let mut ns = NameServerConfig::tcp(ip);
                if let Some(conn) = ns.connections.first_mut() {
                    conn.port = port;
                }
                Ok(ParsedDnsServer::NameServer(ns))
            }
            "tls" => {
                let port = effective_port.unwrap_or(853);
                let mut ns = NameServerConfig::tls(ip, sni);
                if let Some(conn) = ns.connections.first_mut() {
                    conn.port = port;
                }
                Ok(ParsedDnsServer::NameServer(ns))
            }
            "https" => {
                let port = effective_port.unwrap_or(443);
                let path = cfg.path.as_deref().map(Arc::from);
                let mut ns = NameServerConfig::https(ip, sni, path);
                if let Some(conn) = ns.connections.first_mut() {
                    conn.port = port;
                }
                Ok(ParsedDnsServer::NameServer(ns))
            }
            unknown => Err(TunnelError::Config(format!(
                "Unsupported DNS resolver type '{}'. Supported types: 'https', 'tls', 'udp', 'tcp', 'system'",
                unknown
            ))),
        }
    }

    /// Extracts host and optional port from a string like "1.1.1.1:53" or "[::1]:53".
    fn extract_host_and_port(raw: &str) -> (String, Option<u16>) {
        let clean = raw.trim();
        if clean.starts_with('[')
            && let Some(closing) = clean.find(']')
        {
            let host = clean[1..closing].to_string();
            let port = if let Some(colon) = clean[closing..].find(':') {
                clean[closing + colon + 1..].parse::<u16>().ok()
            } else {
                None
            };
            return (host, port);
        }
        if let Some(colon) = clean.rfind(':')
            && clean.matches(':').count() == 1
        {
            let host = clean[..colon].to_string();
            let port = clean[colon + 1..].parse::<u16>().ok();
            return (host, port);
        }
        (clean.to_string(), None)
    }

    /// Resolves or parses an IP address literal or hostname.
    fn resolve_or_parse_ip(host_str: &str) -> Result<IpAddr, TunnelError> {
        let clean_host = host_str.trim_start_matches('[').trim_end_matches(']');
        if let Ok(ip) = clean_host.parse::<IpAddr>() {
            return Ok(ip);
        }
        let addr_str = format!("{}:53", clean_host);
        match addr_str.to_socket_addrs() {
            Ok(mut addrs) => addrs.next().map(|sa| sa.ip()).ok_or_else(|| {
                TunnelError::Config(format!("No IP found for DNS host '{}'", clean_host))
            }),
            Err(e) => Err(TunnelError::Config(format!(
                "Invalid or unresolvable DNS server address '{}': {}",
                clean_host, e
            ))),
        }
    }

    /// Parses a DNS resolver server URI into a `ParsedDnsServer`.
    ///
    /// Supported formats:
    /// - `system://0.0.0.0` or `system`: Use native libc resolver.
    /// - `udp://1.1.1.1[:port]` or `dns://1.1.1.1[:port]`: Standard unencrypted UDP DNS.
    /// - `https://1.1.1.1[:port][/path]?sni=cloudflare-dns.com`: DNS over HTTPS (DoH).
    /// - `tls://8.8.8.8[:port]?sni=dns.google`: DNS over TLS (DoT).
    /// - `tcp://1.1.1.1[:port]`: DNS over TCP.
    pub fn parse_server(raw: &str) -> Result<ParsedDnsServer, TunnelError> {
        let clean = raw.trim();
        if clean.eq_ignore_ascii_case("system") || clean.starts_with("system://") {
            return Ok(ParsedDnsServer::System);
        }

        // Explicitly reject non-standard "dns+" schemes
        if clean.starts_with("dns+https://") || clean.starts_with("dns+tls://") {
            return Err(TunnelError::Config(format!(
                "Non-standard 'dns+' schemes are no longer supported in '{}'. Use standard 'https://', 'tls://', 'udp://', or structured resolver configuration [{{ type: \"https\", server: \"...\", tls: {{ server_name: \"...\" }} }}].",
                raw
            )));
        }

        let url_str = if clean.contains("://") {
            clean.to_string()
        } else {
            format!("udp://{}", clean)
        };

        let parsed = url::Url::parse(&url_str).map_err(|e| {
            TunnelError::Config(format!("Invalid DNS resolver URL '{}': {}", raw, e))
        })?;

        let host_str = parsed.host_str().ok_or_else(|| {
            TunnelError::Config(format!("Missing host in DNS resolver URL '{}'", raw))
        })?;

        let clean_host = host_str.trim_start_matches('[').trim_end_matches(']');
        let ip = Self::resolve_or_parse_ip(clean_host)?;

        let scheme = parsed.scheme();
        let query_sni = parsed
            .query_pairs()
            .find(|(k, _)| k == "sni")
            .map(|(_, v)| v.to_string());

        let sni: Arc<str> = query_sni
            .as_deref()
            .map(Arc::from)
            .unwrap_or_else(|| Arc::from(clean_host));

        match scheme {
            "dns" | "udp" => {
                let port = parsed.port().unwrap_or(53);
                let mut ns = NameServerConfig::udp(ip);
                if let Some(conn) = ns.connections.first_mut() {
                    conn.port = port;
                }
                Ok(ParsedDnsServer::NameServer(ns))
            }
            "tcp" => {
                let port = parsed.port().unwrap_or(53);
                let mut ns = NameServerConfig::tcp(ip);
                if let Some(conn) = ns.connections.first_mut() {
                    conn.port = port;
                }
                Ok(ParsedDnsServer::NameServer(ns))
            }
            "tls" => {
                let port = parsed.port().unwrap_or(853);
                let mut ns = NameServerConfig::tls(ip, sni);
                if let Some(conn) = ns.connections.first_mut() {
                    conn.port = port;
                }
                Ok(ParsedDnsServer::NameServer(ns))
            }
            "https" => {
                let port = parsed.port().unwrap_or(443);
                let path = if !parsed.path().is_empty() && parsed.path() != "/" {
                    Some(Arc::from(parsed.path()))
                } else {
                    None
                };
                let mut ns = NameServerConfig::https(ip, sni, path);
                if let Some(conn) = ns.connections.first_mut() {
                    conn.port = port;
                }
                Ok(ParsedDnsServer::NameServer(ns))
            }
            unknown => Err(TunnelError::Config(format!(
                "Unsupported DNS resolver scheme '{}' in '{}'. Supported schemes: https, tls, udp, tcp, dns, system",
                unknown, raw
            ))),
        }
    }

    /// Sorts and filters the given IP addresses according to the specified strategy.
    pub fn apply_strategy(
        ips: Vec<IpAddr>,
        strategy: DnsStrategy,
    ) -> Result<Vec<IpAddr>, TunnelError> {
        let (mut v4s, mut v6s): (Vec<IpAddr>, Vec<IpAddr>) =
            ips.into_iter().partition(|ip| ip.is_ipv4());

        let result = match strategy {
            DnsStrategy::PreferIpv4 => {
                v4s.extend(v6s);
                v4s
            }
            DnsStrategy::PreferIpv6 => {
                v6s.extend(v4s);
                v6s
            }
            DnsStrategy::Ipv4Only => v4s,
            DnsStrategy::Ipv6Only => v6s,
        };

        if result.is_empty() {
            Err(TunnelError::DnsNoAddresses(
                "target".to_string(),
                format!("{:?}", strategy),
            ))
        } else {
            Ok(result)
        }
    }
}

impl DnsResolver for CachedDnsResolver {
    fn resolve<'a>(&'a self, host: &'a str) -> BoxFuture<'a, Result<Vec<IpAddr>, TunnelError>> {
        Box::pin(async move {
            // 1. Direct IP literal fast path (0-RTT bypass)
            if let Ok(ip) = host.parse::<IpAddr>() {
                return Self::apply_strategy(vec![ip], self.strategy());
            }

            // 2. Check in-memory cache
            {
                let read_guard = self.cache.read();
                if let Some((inserted_at, ips)) = read_guard.get(host)
                    && inserted_at.elapsed() < DEFAULT_DNS_CACHE_TTL
                {
                    return Ok(ips.clone());
                }
            }

            // 3. Resolve using Hickory (UDP / DoH / DoT) if configured
            let mut resolved_ips = Vec::new();
            let mut hickory_failed = false;

            if let Some(ref resolver) = self.hickory_resolver {
                match resolver.lookup_ip(host).await {
                    Ok(lookup) => {
                        for ip in lookup.iter() {
                            if !resolved_ips.contains(&ip) {
                                resolved_ips.push(ip);
                            }
                        }
                    }
                    Err(e) => {
                        debug!(host = %host, error = ?e, "Hickory DNS lookup failed");
                        hickory_failed = true;
                    }
                }
            }

            // 4. Fallback to system libc resolver if hickory failed or system:// was configured
            let should_try_system = self.has_system_resolver
                || self.hickory_resolver.is_none()
                || (resolved_ips.is_empty() && hickory_failed);

            if should_try_system
                && let Ok(addrs) = tokio::net::lookup_host(format!("{}:0", host)).await
            {
                for sa in addrs {
                    if !resolved_ips.contains(&sa.ip()) {
                        resolved_ips.push(sa.ip());
                    }
                }
            }

            if resolved_ips.is_empty() {
                return Err(TunnelError::DnsResolutionFailed(
                    host.to_string(),
                    "All configured DNS resolvers returned no addresses".to_string(),
                ));
            }

            let filtered = Self::apply_strategy(resolved_ips, self.strategy())?;

            // 5. Update cache
            {
                let mut write_guard = self.cache.write();
                if write_guard.len() >= self.capacity {
                    write_guard.clear();
                }
                write_guard.insert(host.to_string(), (Instant::now(), filtered.clone()));
            }

            Ok(filtered)
        })
    }

    fn strategy(&self) -> DnsStrategy {
        self.config.strategy.unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn test_parse_dns_resolver_schemes() {
        // 1. UDP DNS
        let udp = CachedDnsResolver::parse_server("dns://1.1.1.1").unwrap();
        match udp {
            ParsedDnsServer::NameServer(ns) => {
                assert_eq!(ns.ip, IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)));
                assert_eq!(ns.connections[0].port, 53);
            }
            _ => panic!("Expected NameServer"),
        }

        // Custom port UDP
        let udp_custom = CachedDnsResolver::parse_server("dns://1.1.1.1:5353").unwrap();
        match udp_custom {
            ParsedDnsServer::NameServer(ns) => {
                assert_eq!(ns.connections[0].port, 5353);
            }
            _ => panic!("Expected NameServer"),
        }

        // 2. DNS over HTTPS (DoH) using standard URL scheme
        let doh =
            CachedDnsResolver::parse_server("https://1.1.1.1?sni=cloudflare-dns.com").unwrap();
        match doh {
            ParsedDnsServer::NameServer(ns) => {
                assert_eq!(ns.ip, IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)));
                assert_eq!(ns.connections[0].port, 443);
            }
            _ => panic!("Expected NameServer"),
        }

        // 3. DNS over TLS (DoT) using standard URL scheme
        let dot = CachedDnsResolver::parse_server("tls://8.8.8.8?sni=dns.google").unwrap();
        match dot {
            ParsedDnsServer::NameServer(ns) => {
                assert_eq!(ns.ip, IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)));
                assert_eq!(ns.connections[0].port, 853);
            }
            _ => panic!("Expected NameServer"),
        }

        // 4. System / libc resolver
        let sys = CachedDnsResolver::parse_server("system://0.0.0.0").unwrap();
        match sys {
            ParsedDnsServer::System => {}
            _ => panic!("Expected System resolver"),
        }
        let sys_bare = CachedDnsResolver::parse_server("system").unwrap();
        match sys_bare {
            ParsedDnsServer::System => {}
            _ => panic!("Expected System resolver"),
        }

        // 5. Standard structured resolver configuration: [{ type: "https", server: "1.1.1.1", tls: { server_name: "cloudflare-dns.com" } }]
        let struct_doh_yaml = r#"
type: "https"
server: "1.1.1.1"
tls:
  server_name: "cloudflare-dns.com"
"#;
        let struct_doh: DnsResolverConfig = serde_yaml::from_str(struct_doh_yaml).unwrap();
        let parsed_doh = CachedDnsResolver::parse_resolver(&struct_doh).unwrap();
        match parsed_doh {
            ParsedDnsServer::NameServer(ns) => {
                assert_eq!(ns.ip, IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)));
                assert_eq!(ns.connections[0].port, 443);
            }
            _ => panic!("Expected NameServer for structured DoH"),
        }

        // 6. Structured DoT configuration
        let struct_dot = DnsResolverConfig::Detailed(DnsServerConfig {
            r#type: "tls".to_string(),
            server: Some("8.8.8.8".to_string()),
            port: Some(853),
            path: None,
            tls: Some(crate::config::model::DnsTlsConfig {
                server_name: Some("dns.google".to_string()),
            }),
        });
        let parsed_dot = CachedDnsResolver::parse_resolver(&struct_dot).unwrap();
        match parsed_dot {
            ParsedDnsServer::NameServer(ns) => {
                assert_eq!(ns.ip, IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)));
                assert_eq!(ns.connections[0].port, 853);
            }
            _ => panic!("Expected NameServer for structured DoT"),
        }

        // 7. Verify non-standard "dns+" schemes are rejected
        assert!(
            CachedDnsResolver::parse_server("dns+https://1.1.1.1?sni=cloudflare-dns.com").is_err()
        );
        assert!(CachedDnsResolver::parse_server("dns+tls://8.8.8.8?sni=dns.google").is_err());
    }

    #[test]
    fn test_dns_strategy_prefer_ipv4() {
        let v4 = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));
        let v6 = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let ips = vec![v6, v4];

        let result = CachedDnsResolver::apply_strategy(ips, DnsStrategy::PreferIpv4).unwrap();
        assert_eq!(result, vec![v4, v6]);
    }

    #[test]
    fn test_dns_strategy_ipv4_only() {
        let v4 = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));
        let v6 = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let ips = vec![v6, v4];

        let result = CachedDnsResolver::apply_strategy(ips, DnsStrategy::Ipv4Only).unwrap();
        assert_eq!(result, vec![v4]);
    }

    #[test]
    fn test_dns_strategy_ipv6_only() {
        let v4 = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));
        let v6 = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let ips = vec![v6, v4];

        let result = CachedDnsResolver::apply_strategy(ips, DnsStrategy::Ipv6Only).unwrap();
        assert_eq!(result, vec![v6]);
    }
}
