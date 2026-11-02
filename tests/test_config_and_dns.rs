//! Comprehensive test suite for YAML configuration parsing and DNS Resolver strategies.
//! All comments and docstrings must be in English.

use std::net::Ipv4Addr;
use std::time::Duration;

use micro_tunnel::config::loader::ConfigLoader;
use micro_tunnel::config::model::{DnsConfig, DnsStrategy, TunnelMode};
use micro_tunnel::dialer::dns::{CachedDnsResolver, DnsResolver};
use micro_tunnel::dialer::endpoint::EndpointContext;

#[test]
fn test_complex_yaml_config_parsing() {
    let yaml = r#"
log:
  level: "debug"
  output: "stdout"

dns:
  strategy: "prefer_ipv4"
  servers:
    - "udp://8.8.8.8:53"
    - "udp://1.1.1.1:53"
  cache_capacity: 1024

dialer:
  connect_timeout: "4s"
  tcp_keepalive: "20s"
  tcp_nodelay: true
  bind_interface: "eth0"

tls:
  enabled: true
  insecure: false
  alpn:
    - "h3"
    - "http/1.1"
  min_version: "1.3"

server:
  listen: "127.0.0.1:8443"
  token: "server_secret_token"
  default_target: "127.0.0.1:8080"

tunnels:
  - tag: "ssh-reverse"
    mode: "reverse"
    listen: "0.0.0.0:2222"
    target: "127.0.0.1:22"
    token: "tunnel_secret_token_1"
    pool_size: 4
    endpoint:
      url: "wss://vps.example.com:443/ws"
      dns:
        strategy: "ipv4_only"
      tls:
        enabled: true
        server_name: "custom-camouflage.example.com"
        insecure: true
      dialer:
        connect_timeout: "2s"

  - tag: "web-forward"
    mode: "forward"
    listen: "127.0.0.1:8000"
    target: "internal.corp:80"
    token: "tunnel_secret_token_2"
    endpoint: "ws://10.0.0.1:8080/tunnel"
"#;

    let app_config = ConfigLoader::from_yaml_str(yaml).expect("Failed to parse complex YAML");

    // Verify Log
    assert_eq!(app_config.log.level, "debug");
    assert_eq!(app_config.log.output, "stdout");

    // Verify DNS
    let dns = app_config.dns.as_ref().unwrap();
    assert_eq!(dns.strategy, Some(DnsStrategy::PreferIpv4));
    assert_eq!(dns.cache_capacity, Some(1024));
    assert_eq!(dns.servers.as_ref().unwrap().len(), 2);

    // Verify Dialer
    let dialer = app_config.dialer.as_ref().unwrap();
    assert_eq!(dialer.connect_timeout, Some(Duration::from_secs(4)));
    assert_eq!(dialer.tcp_keepalive, Some(Duration::from_secs(20)));
    assert_eq!(dialer.tcp_nodelay, Some(true));
    assert_eq!(dialer.bind_interface.as_deref(), Some("eth0"));

    // Verify TLS
    let tls = app_config.tls.as_ref().unwrap();
    assert!(tls.enabled);
    assert!(!tls.insecure);
    assert_eq!(tls.alpn.as_ref().unwrap(), &vec!["h3".to_string(), "http/1.1".to_string()]);
    assert_eq!(tls.min_version.as_deref(), Some("1.3"));

    // Verify Server
    let server = app_config.server.as_ref().unwrap();
    assert_eq!(server.listen, "127.0.0.1:8443");
    assert_eq!(server.token, "server_secret_token");
    assert_eq!(server.default_target.as_deref(), Some("127.0.0.1:8080"));

    // Verify Tunnels
    assert_eq!(app_config.tunnels.len(), 2);

    // Tunnel 1: detailed endpoint overriding global dns/tls/dialer
    let t1 = &app_config.tunnels[0];
    assert_eq!(t1.tag, "ssh-reverse");
    assert_eq!(t1.mode, TunnelMode::Reverse);
    assert_eq!(t1.pool_size, Some(4));
    assert_eq!(t1.endpoint.url(), "wss://vps.example.com:443/ws");

    let ep_ctx1 = EndpointContext::build(&t1.endpoint, &app_config).expect("Failed to build endpoint context");
    assert_eq!(ep_ctx1.endpoint.host, "vps.example.com");
    assert_eq!(ep_ctx1.endpoint.port, 443);
    assert_eq!(ep_ctx1.endpoint.scheme, "wss");

    // Tunnel 1 TLS SNI override check
    let t1_tls = ep_ctx1.tls_config.as_ref().unwrap();
    assert_eq!(t1_tls.server_name.as_deref(), Some("custom-camouflage.example.com"));
    assert!(t1_tls.insecure);

    // Tunnel 2: simple endpoint inheriting global defaults
    let t2 = &app_config.tunnels[1];
    assert_eq!(t2.tag, "web-forward");
    assert_eq!(t2.mode, TunnelMode::Forward);
    assert_eq!(t2.endpoint.url(), "ws://10.0.0.1:8080/tunnel");

    let ep_ctx2 = EndpointContext::build(&t2.endpoint, &app_config).expect("Failed to build endpoint context");
    assert_eq!(ep_ctx2.endpoint.host, "10.0.0.1");
    assert_eq!(ep_ctx2.endpoint.port, 8080);
    assert_eq!(ep_ctx2.endpoint.scheme, "ws");
}

#[tokio::test]
async fn test_dns_resolver_strategies_and_caching() {
    // 1. IPv4 only strategy on localhost
    let ipv4_cfg = DnsConfig {
        strategy: Some(DnsStrategy::Ipv4Only),
        servers: None,
        cache_capacity: Some(100),
    };
    let resolver = CachedDnsResolver::new(ipv4_cfg);
    let addrs = resolver.resolve("localhost").await.expect("Failed to resolve localhost");
    assert!(!addrs.is_empty());
    assert!(addrs.iter().all(|ip| ip.is_ipv4()), "Expected only IPv4 addresses, got: {:?}", addrs);

    // 2. IPv6 only strategy on localhost
    let ipv6_cfg = DnsConfig {
        strategy: Some(DnsStrategy::Ipv6Only),
        servers: None,
        cache_capacity: Some(100),
    };
    let resolver = CachedDnsResolver::new(ipv6_cfg);
    if let Ok(addrs) = resolver.resolve("localhost").await {
        assert!(addrs.iter().all(|ip| ip.is_ipv6()), "Expected only IPv6 addresses, got: {:?}", addrs);
    }

    // 3. Prefer IPv4 strategy
    let prefer_v4_cfg = DnsConfig {
        strategy: Some(DnsStrategy::PreferIpv4),
        servers: None,
        cache_capacity: Some(100),
    };
    let resolver = CachedDnsResolver::new(prefer_v4_cfg);
    let addrs = resolver.resolve("localhost").await.expect("Failed to resolve");
    assert!(!addrs.is_empty());
    // If both v4 and v6 are present, the first one must be IPv4
    assert!(addrs[0].is_ipv4(), "Prefer IPv4 should place IPv4 first");

    // 4. DNS Cache hit test
    let res1 = resolver.resolve("localhost").await.expect("First lookup");
    let res2 = resolver.resolve("localhost").await.expect("Cached lookup");
    assert_eq!(res1, res2);

    // 5. IP literal fast-path (bypass DNS)
    let ip_str = "127.0.0.1";
    let literal_res = resolver.resolve(ip_str).await.expect("Resolve literal IP");
    assert_eq!(literal_res, vec![std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))]);
}
