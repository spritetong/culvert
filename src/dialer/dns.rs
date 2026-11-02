//! DNS resolution engine supporting IPv4/IPv6 strategy selection and caching.
//! All comments and docstrings must be in English.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Instant;
use parking_lot::RwLock;

use crate::config::model::{DnsConfig, DnsStrategy};
use crate::constants::{DEFAULT_DNS_CACHE_CAPACITY, DEFAULT_DNS_CACHE_TTL};
use crate::core::error::TunnelError;
use crate::core::stream::BoxFuture;

/// Abstract DNS resolver interface.
pub trait DnsResolver: Send + Sync {
    /// Resolves a hostname to a list of IP addresses according to the configured strategy.
    fn resolve<'a>(&'a self, host: &'a str) -> BoxFuture<'a, Result<Vec<IpAddr>, TunnelError>>;

    /// Returns the currently active DNS resolution strategy.
    fn strategy(&self) -> DnsStrategy;
}

/// In-memory cached DNS resolver using `parking_lot::RwLock`.
pub struct CachedDnsResolver {
    config: DnsConfig,
    cache: RwLock<HashMap<String, (Instant, Vec<IpAddr>)>>,
    capacity: usize,
}

impl CachedDnsResolver {
    /// Creates a new `CachedDnsResolver` with the provided configuration.
    pub fn new(config: DnsConfig) -> Self {
        let capacity = config.cache_capacity.unwrap_or(DEFAULT_DNS_CACHE_CAPACITY);
        Self {
            config,
            cache: RwLock::new(HashMap::with_capacity(capacity)),
            capacity,
        }
    }

    /// Sorts and filters the given IP addresses according to the specified strategy.
    pub fn apply_strategy(ips: Vec<IpAddr>, strategy: DnsStrategy) -> Result<Vec<IpAddr>, TunnelError> {
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
            // Direct IP literal parse check
            if let Ok(ip) = host.parse::<IpAddr>() {
                return Self::apply_strategy(vec![ip], self.strategy());
            }

            // Check cache
            {
                let read_guard = self.cache.read();
                if let Some((inserted_at, ips)) = read_guard.get(host) {
                    if inserted_at.elapsed() < DEFAULT_DNS_CACHE_TTL {
                        return Ok(ips.clone());
                    }
                }
            }

            // Perform asynchronous system lookup
            let lookup_res = tokio::net::lookup_host(format!("{}:0", host)).await;
            match lookup_res {
                Ok(addrs) => {
                    let ips: Vec<IpAddr> = addrs.map(|sa| sa.ip()).collect();
                    let filtered = Self::apply_strategy(ips, self.strategy())?;

                    // Update cache
                    {
                        let mut write_guard = self.cache.write();
                        if write_guard.len() >= self.capacity {
                            write_guard.clear();
                        }
                        write_guard.insert(host.to_string(), (Instant::now(), filtered.clone()));
                    }

                    Ok(filtered)
                }
                Err(e) => Err(TunnelError::DnsResolutionFailed(host.to_string(), e.to_string())),
            }
        })
    }

    fn strategy(&self) -> DnsStrategy {
        self.config.strategy.unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;

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
