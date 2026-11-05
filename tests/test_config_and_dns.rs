// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
    #[cfg(target_os = "linux")]
    let iface_line = "  bind_interface: \"eth0\"\n";
    #[cfg(not(target_os = "linux"))]
    let iface_line = "";

    let yaml = format!(r#"
log:
  level: "debug"
  output: "stdout"
  no_color: true

worker_threads: 8

dns:
  strategy: "prefer_ipv4"
  resolvers:
    - "udp://8.8.8.8:53"
    - "udp://1.1.1.1:53"
  cache_capacity: 1024

dialer:
  connect_timeout: "4s"
  tcp_keepalive: "20s"
  tcp_nodelay: true
{}
tls:
  insecure: false
  alpn:
    - "h3"
    - "http/1.1"
  min_version: "1.3"

server:
  listen: "ws://127.0.0.1:8443/tunnel"
  token: "server_secret_token"
  default_target: "127.0.0.1:8080"

tunnels:
  - tag: "ssh-reverse"
    mappings:
      - mode: "reverse"
        listen: "0.0.0.0:2222"
        target: "127.0.0.1:22"
    token: "tunnel_secret_token_1"
    pool_size: 4
    endpoint:
      url: "wss://vps.example.com:443/ws"
      dns:
        strategy: "ipv4_only"
      tls:
        server_name: "custom-camouflage.example.com"
        insecure: true
      dialer:
        connect_timeout: "2s"

  - tag: "web-forward"
    mappings:
      - mode: "forward"
        listen: "127.0.0.1:8000"
        target: "internal.corp:80"
    token: "tunnel_secret_token_2"
    endpoint: "ws://10.0.0.1:8080/tunnel"
"#, iface_line);

    let app_config = ConfigLoader::from_yaml_str(&yaml).expect("Failed to parse complex YAML");

    // Verify Log
    assert_eq!(app_config.log.level, "debug");
    assert_eq!(app_config.log.output, "stdout");
    assert!(app_config.log.no_color);

    // Verify Worker Threads
    assert_eq!(app_config.worker_threads, Some(8));

    // Verify DNS
    let dns = app_config.dns.as_ref().unwrap();
    assert_eq!(dns.strategy, Some(DnsStrategy::PreferIpv4));
    assert_eq!(dns.cache_capacity, Some(1024));
    assert_eq!(dns.resolvers.as_ref().unwrap().len(), 2);

    // Verify Dialer
    let dialer = app_config.dialer.as_ref().unwrap();
    assert_eq!(dialer.connect_timeout, Some(Duration::from_secs(4)));
    assert_eq!(dialer.tcp_keepalive, Some(Duration::from_secs(20)));
    assert_eq!(dialer.tcp_nodelay, Some(true));
    #[cfg(target_os = "linux")]
    assert_eq!(dialer.bind_interface.as_deref(), Some("eth0"));

    // Verify TLS
    let tls = app_config.tls.as_ref().unwrap();
    assert!(!tls.is_insecure());
    assert_eq!(
        tls.alpn.as_ref().unwrap(),
        &vec!["h3".to_string(), "http/1.1".to_string()]
    );
    assert_eq!(tls.min_version.as_deref(), Some("1.3"));

    // Verify Server
    let server = app_config.server.as_ref().unwrap();
    assert_eq!(server.listen, vec!["ws://127.0.0.1:8443/tunnel"]);
    assert_eq!(server.token.as_deref(), Some("server_secret_token"));
    assert_eq!(server.default_target.as_deref(), Some("127.0.0.1:8080"));

    let specs = server.parse_specs().expect("Should parse specs");
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].scheme, "ws");
    assert_eq!(specs[0].bind_addr, "127.0.0.1:8443");
    assert_eq!(specs[0].path.as_deref(), Some("/tunnel"));

    // Verify Tunnels
    assert_eq!(app_config.tunnels.len(), 2);

    // Tunnel 1: detailed endpoint overriding global dns/tls/dialer
    let t1 = &app_config.tunnels[0];
    assert_eq!(t1.tag, "ssh-reverse");
    assert_eq!(t1.mappings[0].mode, TunnelMode::Reverse);
    assert_eq!(t1.mappings[0].listen, "0.0.0.0:2222");
    assert_eq!(t1.mappings[0].target, "127.0.0.1:22");
    assert_eq!(t1.pool_size, Some(4));
    assert_eq!(t1.endpoint.url(), "wss://vps.example.com:443/ws");

    let ep_ctx1 = EndpointContext::build(&t1.endpoint, &app_config)
        .expect("Failed to build endpoint context");
    assert_eq!(ep_ctx1.endpoint.host, "vps.example.com");
    assert_eq!(ep_ctx1.endpoint.port, 443);
    assert_eq!(ep_ctx1.endpoint.scheme, "wss");

    // Tunnel 1 TLS SNI override check
    let t1_tls = ep_ctx1.tls_config.as_ref().unwrap();
    assert_eq!(
        t1_tls.server_name.as_deref(),
        Some("custom-camouflage.example.com")
    );
    assert!(t1_tls.is_insecure());

    // Tunnel 2: simple endpoint inheriting global defaults
    let t2 = &app_config.tunnels[1];
    assert_eq!(t2.tag, "web-forward");
    assert_eq!(t2.mappings[0].mode, TunnelMode::Forward);
    assert_eq!(t2.mappings[0].listen, "127.0.0.1:8000");
    assert_eq!(t2.mappings[0].target, "internal.corp:80");
    assert_eq!(t2.endpoint.url(), "ws://10.0.0.1:8080/tunnel");

    let ep_ctx2 = EndpointContext::build(&t2.endpoint, &app_config)
        .expect("Failed to build endpoint context");
    assert_eq!(ep_ctx2.endpoint.host, "10.0.0.1");
    assert_eq!(ep_ctx2.endpoint.port, 8080);
    assert_eq!(ep_ctx2.endpoint.scheme, "ws");
}

#[tokio::test]
async fn test_dns_resolver_strategies_and_caching() {
    // 1. IPv4 only strategy on localhost
    let ipv4_cfg = DnsConfig {
        strategy: Some(DnsStrategy::Ipv4Only),
        resolvers: None,
        cache_capacity: Some(100),
    };
    let resolver = CachedDnsResolver::try_new(ipv4_cfg).unwrap();
    let addrs = resolver
        .resolve("localhost")
        .await
        .expect("Failed to resolve localhost");
    assert!(!addrs.is_empty());
    assert!(
        addrs.iter().all(|ip| ip.is_ipv4()),
        "Expected only IPv4 addresses, got: {:?}",
        addrs
    );

    // 2. IPv6 only strategy on localhost
    let ipv6_cfg = DnsConfig {
        strategy: Some(DnsStrategy::Ipv6Only),
        resolvers: None,
        cache_capacity: Some(100),
    };
    let resolver = CachedDnsResolver::try_new(ipv6_cfg).unwrap();
    if let Ok(addrs) = resolver.resolve("localhost").await {
        assert!(
            addrs.iter().all(|ip| ip.is_ipv6()),
            "Expected only IPv6 addresses, got: {:?}",
            addrs
        );
    }

    // 3. Prefer IPv4 strategy
    let prefer_v4_cfg = DnsConfig {
        strategy: Some(DnsStrategy::PreferIpv4),
        resolvers: None,
        cache_capacity: Some(100),
    };
    let resolver = CachedDnsResolver::try_new(prefer_v4_cfg).unwrap();
    let addrs = resolver
        .resolve("localhost")
        .await
        .expect("Failed to resolve");
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
    assert_eq!(
        literal_res,
        vec![std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))]
    );
}

#[tokio::test]
async fn test_standard_dns_resolvers() {
    // 1. System libc resolver
    let sys_cfg = DnsConfig {
        strategy: Some(DnsStrategy::PreferIpv4),
        resolvers: Some(vec!["system://0.0.0.0".into()]),
        cache_capacity: Some(10),
    };
    let sys_resolver = CachedDnsResolver::try_new(sys_cfg).unwrap();
    let sys_addrs = sys_resolver.resolve("localhost").await.unwrap();
    assert!(!sys_addrs.is_empty());

    // 2. Custom standard multi-protocol DNS configurations (https, tls, udp, system)
    let multi_cfg = DnsConfig {
        strategy: Some(DnsStrategy::PreferIpv4),
        resolvers: Some(vec![
            "dns://1.1.1.1".into(),
            "https://1.1.1.1?sni=cloudflare-dns.com".into(),
            "tls://8.8.8.8?sni=dns.google".into(),
            "system://0.0.0.0".into(),
        ]),
        cache_capacity: Some(100),
    };
    let multi_resolver = CachedDnsResolver::try_new(multi_cfg).unwrap();
    // Direct literal fast path
    let fast_path = multi_resolver.resolve("1.1.1.1").await.unwrap();
    assert_eq!(
        fast_path,
        vec![std::net::IpAddr::V4(std::net::Ipv4Addr::new(1, 1, 1, 1))]
    );

    // 3. YAML parsing with standard structured resolvers: [{ type: "https", server: "1.1.1.1", tls: { server_name: "cloudflare-dns.com" } }]
    let yaml = r#"
server:
  listen: ["ws://127.0.0.1:8080"]
dns:
  strategy: "ipv4_only"
  resolvers:
    - type: "https"
      server: "1.1.1.1"
      tls:
        server_name: "cloudflare-dns.com"
    - type: "tls"
      server: "8.8.8.8"
      tls:
        server_name: "dns.google"
    - type: "udp"
      server: "1.1.1.1:53"
    - type: "system"
"#;
    let config = ConfigLoader::from_yaml_str(yaml).unwrap();
    let dns = config.dns.unwrap();
    assert_eq!(dns.strategy, Some(DnsStrategy::Ipv4Only));
    let resolvers = dns.resolvers.unwrap();
    assert_eq!(resolvers.len(), 4);

    let structured_resolver = CachedDnsResolver::try_new(DnsConfig {
        strategy: Some(DnsStrategy::Ipv4Only),
        resolvers: Some(resolvers),
        cache_capacity: Some(100),
    }).unwrap();
    let fast_path2 = structured_resolver.resolve("1.1.1.1").await.unwrap();
    assert_eq!(
        fast_path2,
        vec![std::net::IpAddr::V4(std::net::Ipv4Addr::new(1, 1, 1, 1))]
    );
}

#[cfg(not(target_os = "linux"))]
#[test]
fn test_reject_bind_interface_on_non_linux() {
    let yaml = r#"
dialer:
  bind_interface: "eth0"
server:
  listen: ["ws://127.0.0.1:8080"]
"#;
    let res = ConfigLoader::from_yaml_str(yaml);
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("SO_BINDTODEVICE"));
}

#[test]
fn test_reject_dns_cache_capacity_zero() {
    let yaml = r#"
dns:
  cache_capacity: 0
server:
  listen: ["ws://127.0.0.1:8080"]
"#;
    let res = ConfigLoader::from_yaml_str(yaml);
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("dns.cache_capacity must be at least 1"));
}

#[test]
fn test_reject_global_dialer_connect_timeout_zero() {
    let yaml = r#"
dialer:
  connect_timeout: "0s"
server:
  listen: ["ws://127.0.0.1:8080"]
"#;
    let res = ConfigLoader::from_yaml_str(yaml);
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("dialer.connect_timeout must be greater than 0"));
}

#[tokio::test]
async fn test_empty_server_listen_with_tunnels_does_not_exit_prematurely() {
    let port_listen = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let yaml = format!(r#"
server:
  listen: []
tunnels:
  - tag: "client-only-tunnel"
    endpoint: "ws://127.0.0.1:{}/tunnel"
    retry:
      delay: "1s"
      max_retries: 5
    mappings:
      - mode: "forward"
        listen: "127.0.0.1:{}"
        target: "127.0.0.1:5201"
"#, port_listen, port_listen);
    let config = ConfigLoader::from_yaml_str(&yaml).expect("Failed to parse config with empty server listen");
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let shutdown_token = cancel_token.clone();

    let supervisor = micro_tunnel::supervisor::TunnelSupervisor::new(config, cancel_token);

    // Cancel after 300ms
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        shutdown_token.cancel();
    });

    let start = std::time::Instant::now();
    let res = supervisor.run().await;
    let elapsed = start.elapsed();

    // Verify it did not exit immediately in 16ms, but stayed alive until cancel_token was triggered
    assert!(elapsed >= std::time::Duration::from_millis(250), "Supervisor exited too quickly: {:?}", elapsed);
    assert!(res.is_ok(), "Supervisor should exit cleanly on cancellation: {:?}", res);
}

#[test]
fn test_cached_dns_resolver_try_new_rejects_missing_server() {
    use micro_tunnel::dialer::dns::CachedDnsResolver;
    use micro_tunnel::config::model::{DnsConfig, DnsResolverConfig, DnsServerConfig};

    let dns_cfg = DnsConfig {
        strategy: None,
        resolvers: Some(vec![DnsResolverConfig::Detailed(DnsServerConfig {
            r#type: "https".to_string(),
            server: None,
            port: None,
            path: None,
            tls: None,
        })]),
        cache_capacity: Some(100),
    };

    let res = CachedDnsResolver::try_new(dns_cfg);
    match res {
        Err(e) => assert!(e.to_string().contains("Missing 'server' field")),
        Ok(_) => panic!("Expected error for missing server field"),
    }
}

#[test]
fn test_reject_duplicate_server_listener_ports() {
    let yaml = r#"
server:
  listen:
    - "ws://127.0.0.1:19220/a"
    - "ws://127.0.0.1:19220/b"
"#;
    let res = ConfigLoader::from_yaml_str(yaml);
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("Duplicate server listener address '127.0.0.1:19220'"));
}


