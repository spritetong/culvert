// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Configuration loader for parsing and validating YAML config files.
//! All comments and docstrings must be in English.

use crate::config::model::AppConfig;
use crate::core::error::TunnelError;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::time::Duration;

/// Configuration loader utility.
pub struct ConfigLoader;

impl ConfigLoader {
    /// Loads and parses an `AppConfig` from a YAML string.
    pub fn from_yaml_str(yaml_str: &str) -> Result<AppConfig, TunnelError> {
        let config: AppConfig =
            serde_yaml::from_str(yaml_str).map_err(|e| TunnelError::Config(e.to_string()))?;
        Self::validate(&config)?;
        Ok(config)
    }

    /// Reads and parses an `AppConfig` from a YAML file path.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<AppConfig, TunnelError> {
        let content = fs::read_to_string(path.as_ref()).map_err(TunnelError::Io)?;
        Self::from_yaml_str(&content)
    }

    /// Validates configuration constraints, semantic integrity, and fail-fast constraints.
    pub fn validate(config: &AppConfig) -> Result<(), TunnelError> {
        // 1. Top-level: config must define at least one server listener or one tunnel
        let has_server = config.server.as_ref().is_some_and(|s| !s.listen.is_empty());
        let has_tunnels = !config.tunnels.is_empty();
        if !has_server && !has_tunnels {
            return Err(TunnelError::Config(
                "Configuration must define at least one 'server' listener or one tunnel in 'tunnels'"
                    .to_string(),
            ));
        }

        // 2. worker_threads boundary check
        if let Some(w) = config.worker_threads
            && w == 0
        {
            return Err(TunnelError::Config(
                "worker_threads must be at least 1".to_string(),
            ));
        }

        // 3. Log level & output validation
        let level = config.log.level.to_lowercase();
        if !matches!(
            level.as_str(),
            "trace" | "debug" | "info" | "warn" | "error"
        ) {
            return Err(TunnelError::Config(format!(
                "Invalid log.level '{}'. Supported levels: trace, debug, info, warn, error",
                config.log.level
            )));
        }
        let output = config.log.output.to_lowercase();
        if !matches!(output.as_str(), "stdout" | "stderr") {
            return Err(TunnelError::Config(format!(
                "Invalid log.output '{}'. Supported outputs: stdout, stderr",
                config.log.output
            )));
        }

        // 4. Global TLS validation
        if let Some(ref tls) = config.tls {
            Self::validate_tls_config(tls, "global 'tls'")?;
        }

        // 5. Global DNS validation
        if let Some(ref dns) = config.dns {
            Self::validate_dns_config(dns, "global 'dns'")?;
        }

        // Global dialer validation
        if let Some(ref d) = config.dialer {
            if let Some(ct) = d.connect_timeout
                && ct.is_zero()
            {
                return Err(TunnelError::Config(
                    "dialer.connect_timeout must be greater than 0".to_string(),
                ));
            }
            if let Some(hb) = d.heartbeat_interval
                && hb.is_zero()
            {
                return Err(TunnelError::Config(
                    "dialer.heartbeat_interval must be greater than 0".to_string(),
                ));
            }
            if let Some(it) = d.idle_timeout
                && it.is_zero()
            {
                return Err(TunnelError::Config(
                    "dialer.idle_timeout must be greater than 0".to_string(),
                ));
            }
            if let Some(ut) = d.udp_idle_timeout
                && ut.is_zero()
            {
                return Err(TunnelError::Config(
                    "dialer.udp_idle_timeout must be greater than 0".to_string(),
                ));
            }
            if let Some(ref iface) = d.bind_interface {
                crate::platform::validate_bind_interface(iface)?;
            }
        }

        // 6. Server configuration validation
        if let Some(ref server) = config.server {
            if server.listen.is_empty() && !has_tunnels {
                return Err(TunnelError::Config(
                    "server.listen must contain at least one listening URL when no tunnels are configured".to_string(),
                ));
            }

            if let Some(ref token) = server.token {
                if token.is_empty() {
                    return Err(TunnelError::Config(
                        "server.token must not be empty. Omit the 'token' field if authentication is not required.".to_string(),
                    ));
                }
                if token.len() > crate::common::auth::TOKEN_MAX_LEN {
                    return Err(TunnelError::Config(format!(
                        "server.token length ({} bytes) exceeds maximum supported token length ({} bytes)",
                        token.len(),
                        crate::common::auth::TOKEN_MAX_LEN
                    )));
                }
            }

            if let Some(hb) = server.heartbeat_interval
                && hb.is_zero()
            {
                return Err(TunnelError::Config(
                    "server.heartbeat_interval must be greater than 0".to_string(),
                ));
            }
            if let Some(it) = server.idle_timeout
                && it.is_zero()
            {
                return Err(TunnelError::Config(
                    "server.idle_timeout must be greater than 0".to_string(),
                ));
            }
            if let Some(ut) = server.udp_idle_timeout
                && ut.is_zero()
            {
                return Err(TunnelError::Config(
                    "server.udp_idle_timeout must be greater than 0".to_string(),
                ));
            }
            if let Some(ref dt) = server.default_target {
                crate::io::addr::parse_target_addr(dt).map_err(|_| {
                    TunnelError::Config(format!(
                        "server.default_target '{}' is invalid. Expected 'host:port' or 'ip:port'",
                        dt
                    ))
                })?;
            }

            // Server cert/key pairing validation
            match (&server.certificate, &server.key) {
                (Some(c), Some(k)) => {
                    Self::validate_pem_certs(Path::new(c), "server certificate", "server")?;
                    Self::validate_pem_private_key(Path::new(k), "server private key", "server")?;
                }
                (Some(_), None) => {
                    return Err(TunnelError::Config(
                        "server.certificate is provided but server.key is missing. Both must be provided together.".to_string(),
                    ));
                }
                (None, Some(_)) => {
                    return Err(TunnelError::Config(
                        "server.key is provided but server.certificate is missing. Both must be provided together.".to_string(),
                    ));
                }
                (None, None) => {}
            }

            if let Some(ref tls) = server.tls {
                Self::validate_tls_config(tls, "server.tls")?;
            }

            let specs = server.parse_specs()?;

            // Reject TLS cert configuration if all server listeners are plaintext (ws:// or ws+unix://)
            let any_tls_listener = specs.iter().any(|s| {
                matches!(
                    s.scheme.as_str(),
                    "wss" | "h3" | "quic" | "https" | "wss+unix"
                )
            });
            let has_server_certs = server.certificate.is_some()
                || server.key.is_some()
                || server
                    .tls
                    .as_ref()
                    .is_some_and(|t| t.certificate.is_some() || t.key.is_some());
            if !any_tls_listener && has_server_certs {
                return Err(TunnelError::Config(
                    "Server is configured with TLS certificates, but all server.listen URLs use plaintext schemes ('ws://' or 'ws+unix://'). Use a TLS scheme ('wss://', 'h3://', 'wss+unix://') or remove certificate settings.".to_string(),
                ));
            }
        }

        // 7. Tunnel configurations validation
        let mut seen_tags = HashSet::new();
        let mut seen_forward_binds = HashSet::new();

        for tunnel in &config.tunnels {
            if tunnel.tag.trim().is_empty() {
                return Err(TunnelError::Config(
                    "Tunnel 'tag' must not be empty".to_string(),
                ));
            }
            if !seen_tags.insert(&tunnel.tag) {
                return Err(TunnelError::Config(format!(
                    "Duplicate tunnel tag '{}'. Each tunnel must have a unique tag.",
                    tunnel.tag
                )));
            }

            if let Some(ref token) = tunnel.token {
                if token.is_empty() {
                    return Err(TunnelError::Config(format!(
                        "tunnel '{}': token must not be empty. Omit the 'token' field if authentication is not required.",
                        tunnel.tag
                    )));
                }
                if token.len() > crate::common::auth::TOKEN_MAX_LEN {
                    return Err(TunnelError::Config(format!(
                        "tunnel '{}': token length ({} bytes) exceeds maximum supported token length ({} bytes)",
                        tunnel.tag,
                        token.len(),
                        crate::common::auth::TOKEN_MAX_LEN
                    )));
                }
            }

            if let Some(ref headers) = tunnel.http_headers {
                Self::validate_http_headers(headers, &format!("tunnel '{}'", tunnel.tag))?;
            }

            if let Some(ref fwd) = tunnel.mappings.forward {
                if let Some(p) = fwd.pool_size
                    && p == 0
                {
                    return Err(TunnelError::Config(format!(
                        "tunnel '{}': forward pool_size must be at least 1",
                        tunnel.tag
                    )));
                }
                if fwd.pool_size.is_some() && fwd.rules.is_empty() {
                    return Err(TunnelError::Config(format!(
                        "tunnel '{}': forward pool_size is configured but no forward rules are defined",
                        tunnel.tag
                    )));
                }
            }

            if let Some(ref rev) = tunnel.mappings.reverse {
                if let Some(p) = rev.pool_size
                    && p == 0
                {
                    return Err(TunnelError::Config(format!(
                        "tunnel '{}': reverse pool_size must be at least 1",
                        tunnel.tag
                    )));
                }
                if rev.pool_size.is_some() && rev.rules.is_empty() {
                    return Err(TunnelError::Config(format!(
                        "tunnel '{}': reverse pool_size is configured but no reverse rules are defined",
                        tunnel.tag
                    )));
                }
            }

            if let Some(ref r) = tunnel.retry {
                if let Some(d) = r.delay
                    && d.is_zero()
                {
                    return Err(TunnelError::Config(format!(
                        "tunnel '{}': retry.delay must be greater than 0",
                        tunnel.tag
                    )));
                }
                if let Some(m) = r.max_retries
                    && m == 0
                {
                    return Err(TunnelError::Config(format!(
                        "tunnel '{}': retry.max_retries must be greater than 0",
                        tunnel.tag
                    )));
                }
            }

            if tunnel.multiplex.is_some() {
                return Err(TunnelError::Config(format!(
                    "tunnel '{}': multiplex is not wired into the data path yet; remove the 'multiplex' section",
                    tunnel.tag
                )));
            }

            if tunnel.failover.is_some() {
                return Err(TunnelError::Config(format!(
                    "tunnel '{}': failover is not wired into the data path yet; remove the 'failover' section",
                    tunnel.tag
                )));
            }

            // Port mappings validation
            let canonical_mappings = tunnel.canonical_mappings()?;
            for mapping in &canonical_mappings {
                let _ = crate::io::addr::parse_target_addr(&mapping.listen).map_err(|_| {
                    TunnelError::Config(format!(
                        "tunnel '{}': invalid listen address '{}' in mapping. Expected 'ip:port', 'hostname:port', or 'unix://path'",
                        tunnel.tag, mapping.listen
                    ))
                })?;

                let _ = crate::io::addr::parse_target_addr(&mapping.target).map_err(|_| {
                    TunnelError::Config(format!(
                        "tunnel '{}': invalid target address '{}' in mapping. Expected 'ip:port', 'hostname:port', or 'unix://path'",
                        tunnel.tag, mapping.target
                    ))
                })?;

                if mapping.protocol == crate::io::addr::L4Protocol::Udp
                    && (crate::io::addr::is_uds_path(&mapping.listen)
                        || crate::io::addr::is_uds_path(&mapping.target))
                {
                    return Err(TunnelError::Config(format!(
                        "tunnel '{}': UDP mapping ('{}' -> '{}') cannot use 'unix://' domain socket addresses",
                        tunnel.tag, mapping.listen, mapping.target
                    )));
                }

                if let Some(ut) = mapping.udp_idle_timeout
                    && ut.is_zero()
                {
                    return Err(TunnelError::Config(format!(
                        "tunnel '{}': mapping.udp_idle_timeout must be greater than 0",
                        tunnel.tag
                    )));
                }

                if mapping.mode == crate::config::model::TunnelMode::Forward
                    && !seen_forward_binds.insert((mapping.protocol, mapping.listen.clone()))
                {
                    return Err(TunnelError::Config(format!(
                        "Duplicate forward {} mapping listen address '{}' in tunnel '{}'. Multiple forward tunnels cannot bind the same local {} address.",
                        mapping.protocol, mapping.listen, tunnel.tag, mapping.protocol
                    )));
                }
            }

            // Endpoint validation
            let ep = crate::dialer::endpoint::Endpoint::parse(tunnel.endpoint.url())?;
            let is_tls = matches!(
                ep.scheme.as_str(),
                "https" | "wss" | "h3" | "quic" | "wss+unix"
            );
            let is_supported = matches!(
                ep.scheme.as_str(),
                "ws" | "wss" | "h3" | "quic" | "https" | "ws+unix" | "wss+unix"
            );
            if !is_supported {
                return Err(TunnelError::Config(format!(
                    "tunnel '{}': unsupported endpoint scheme '{}'. Supported schemes: ws, wss, https, h3, quic, ws+unix, wss+unix",
                    tunnel.tag, ep.scheme
                )));
            }

            if (ep.scheme == "ws+unix" || ep.scheme == "wss+unix")
                && (ep.path.is_empty() || ep.path == "/")
            {
                return Err(TunnelError::Config(format!(
                    "tunnel '{}': Unix Domain Socket endpoint URL must include a socket file path (e.g. ws+unix:///run/tunnel.sock)",
                    tunnel.tag
                )));
            }

            if let crate::config::model::EndpointConfig::Detailed {
                ref tls,
                ref dns,
                ref dialer,
                ref http_headers,
                ..
            } = tunnel.endpoint
            {
                if let Some(headers) = http_headers {
                    Self::validate_http_headers(
                        headers,
                        &format!("tunnel '{}' endpoint", tunnel.tag),
                    )?;
                }

                if !is_tls && tls.is_some() {
                    return Err(TunnelError::Config(format!(
                        "tunnel '{}': endpoint uses non-TLS scheme '{}', but 'tls' options are configured. Use a TLS scheme (e.g. wss://, h3://) or remove the 'tls' block.",
                        tunnel.tag, ep.scheme
                    )));
                }

                if let Some(t) = tls {
                    Self::validate_tls_config(t, &format!("tunnel '{}' endpoint.tls", tunnel.tag))?;
                }
                if let Some(d) = dns {
                    Self::validate_dns_config(d, &format!("tunnel '{}' endpoint.dns", tunnel.tag))?;
                }
                if let Some(dl) = dialer {
                    if let Some(ct) = dl.connect_timeout
                        && ct.is_zero()
                    {
                        return Err(TunnelError::Config(format!(
                            "tunnel '{}': dialer.connect_timeout must be greater than 0",
                            tunnel.tag
                        )));
                    }
                    if let Some(hb) = dl.heartbeat_interval
                        && hb.is_zero()
                    {
                        return Err(TunnelError::Config(format!(
                            "tunnel '{}': dialer.heartbeat_interval must be greater than 0",
                            tunnel.tag
                        )));
                    }
                    if let Some(it) = dl.idle_timeout
                        && it.is_zero()
                    {
                        return Err(TunnelError::Config(format!(
                            "tunnel '{}': dialer.idle_timeout must be greater than 0",
                            tunnel.tag
                        )));
                    }
                    if let Some(ut) = dl.udp_idle_timeout
                        && ut.is_zero()
                    {
                        return Err(TunnelError::Config(format!(
                            "tunnel '{}': dialer.udp_idle_timeout must be greater than 0",
                            tunnel.tag
                        )));
                    }
                    if let Some(ref iface) = dl.bind_interface {
                        crate::platform::validate_bind_interface(iface)?;
                    }
                }
            }

            // Pre-flight check: ensure EndpointContext builds cleanly
            crate::dialer::endpoint::EndpointContext::build_for_tunnel(tunnel, config)?;
        }

        // 8. Effective heartbeat/idle timing validation (post-merge).
        //
        // Runtime timers are built from effective values, never from the raw
        // optional fields. Checking only the explicitly configured pair lets
        // `idle_timeout <= heartbeat_interval` through (e.g. a lone
        // `server.idle_timeout: 10s` with the default 15s heartbeat, or a
        // tunnel-level idle_timeout merged with a global heartbeat_interval).
        // Then the first probe coincides with the idle deadline and healthy
        // connections disconnect themselves.
        let default_hb = crate::consts::DEFAULT_HEARTBEAT_INTERVAL;
        let default_it = crate::consts::DEFAULT_IDLE_TIMEOUT;

        if let Some(ref d) = config.dialer {
            Self::validate_effective_timing(
                "",
                "dialer.idle_timeout",
                "dialer.heartbeat_interval",
                d.heartbeat_interval.unwrap_or(default_hb),
                d.idle_timeout.unwrap_or(default_it),
            )?;
        }

        if let Some(ref s) = config.server {
            Self::validate_effective_timing(
                "",
                "server.idle_timeout",
                "server.heartbeat_interval",
                s.heartbeat_interval.unwrap_or(default_hb),
                s.idle_timeout.unwrap_or(default_it),
            )?;
        }

        for tunnel in &config.tunnels {
            let ep_dialer = match &tunnel.endpoint {
                crate::config::model::EndpointConfig::Detailed { dialer, .. } => dialer.as_ref(),
                crate::config::model::EndpointConfig::Simple(_) => None,
            };
            let hb = ep_dialer
                .and_then(|d| d.heartbeat_interval)
                .or_else(|| config.dialer.as_ref().and_then(|d| d.heartbeat_interval))
                .unwrap_or(default_hb);
            let it = ep_dialer
                .and_then(|d| d.idle_timeout)
                .or_else(|| config.dialer.as_ref().and_then(|d| d.idle_timeout))
                .unwrap_or(default_it);
            Self::validate_effective_timing(
                &format!("tunnel '{}'", tunnel.tag),
                "dialer.idle_timeout",
                "dialer.heartbeat_interval",
                hb,
                it,
            )?;
        }

        Ok(())
    }

    /// Validates one effective (defaulted + globally merged) heartbeat pair.
    fn validate_effective_timing(
        scope: &str,
        idle_field: &str,
        heartbeat_field: &str,
        heartbeat_interval: Duration,
        idle_timeout: Duration,
    ) -> Result<(), TunnelError> {
        if idle_timeout <= heartbeat_interval {
            let prefix = if scope.is_empty() {
                String::new()
            } else {
                format!("{}: ", scope)
            };
            return Err(TunnelError::Config(format!(
                "{}{} must be greater than {} (effective idle_timeout={:?}, heartbeat_interval={:?})",
                prefix, idle_field, heartbeat_field, idle_timeout, heartbeat_interval
            )));
        }
        Ok(())
    }

    fn validate_pem_certs(path: &Path, file_role: &str, context: &str) -> Result<(), TunnelError> {
        if !path.exists() {
            return Err(TunnelError::Config(format!(
                "{}: {} file not found: {:?}",
                context, file_role, path
            )));
        }
        let file = std::fs::File::open(path).map_err(|e| {
            TunnelError::Config(format!(
                "{}: failed to open {} file {:?}: {}",
                context, file_role, path, e
            ))
        })?;
        let mut reader = std::io::BufReader::new(file);
        let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                TunnelError::Config(format!(
                    "{}: {} file {:?} contains invalid PEM: {}",
                    context, file_role, path, e
                ))
            })?;
        if certs.is_empty() {
            return Err(TunnelError::Config(format!(
                "{}: {} file {:?} contains no certificates",
                context, file_role, path
            )));
        }
        Ok(())
    }

    fn validate_pem_private_key(
        path: &Path,
        file_role: &str,
        context: &str,
    ) -> Result<(), TunnelError> {
        if !path.exists() {
            return Err(TunnelError::Config(format!(
                "{}: {} file not found: {:?}",
                context, file_role, path
            )));
        }
        let file = std::fs::File::open(path).map_err(|e| {
            TunnelError::Config(format!(
                "{}: failed to open {} file {:?}: {}",
                context, file_role, path, e
            ))
        })?;
        let mut reader = std::io::BufReader::new(file);
        let key = rustls_pemfile::private_key(&mut reader).map_err(|e| {
            TunnelError::Config(format!(
                "{}: {} file {:?} contains invalid private key: {}",
                context, file_role, path, e
            ))
        })?;
        if key.is_none() {
            return Err(TunnelError::Config(format!(
                "{}: {} file {:?} contains no private key",
                context, file_role, path
            )));
        }
        Ok(())
    }

    fn validate_tls_config(
        tls: &crate::config::model::TlsConfig,
        context: &str,
    ) -> Result<(), TunnelError> {
        crate::dialer::tls::TlsConfigBuilder::parse_protocol_versions(
            tls.min_version.as_deref(),
            tls.max_version.as_deref(),
        )?;

        // Server cert/key pairing validation
        match (&tls.certificate, &tls.key) {
            (Some(c), Some(k)) => {
                Self::validate_pem_certs(c, "server certificate", context)?;
                Self::validate_pem_private_key(k, "server private key", context)?;
            }
            (Some(_), None) => {
                return Err(TunnelError::Config(format!(
                    "{}: 'certificate' is provided but 'key' is missing. Both must be provided together.",
                    context
                )));
            }
            (None, Some(_)) => {
                return Err(TunnelError::Config(format!(
                    "{}: 'key' is provided but 'certificate' is missing. Both must be provided together.",
                    context
                )));
            }
            (None, None) => {}
        }

        // Client mTLS cert/key pairing validation
        match (&tls.client_certificate, &tls.client_key) {
            (Some(c), Some(k)) => {
                Self::validate_pem_certs(c, "client certificate", context)?;
                Self::validate_pem_private_key(k, "client private key", context)?;
            }
            (Some(_), None) => {
                return Err(TunnelError::Config(format!(
                    "{}: 'client_certificate' is provided but 'client_key' is missing for mTLS. Both must be provided together.",
                    context
                )));
            }
            (None, Some(_)) => {
                return Err(TunnelError::Config(format!(
                    "{}: 'client_key' is provided but 'client_certificate' is missing for mTLS. Both must be provided together.",
                    context
                )));
            }
            (None, None) => {}
        }

        // CA cert existence and validity
        if let Some(ref ca) = tls.ca_certificate {
            Self::validate_pem_certs(ca, "CA certificate", context)?;
        }

        Ok(())
    }

    fn validate_dns_config(
        dns: &crate::config::model::DnsConfig,
        context: &str,
    ) -> Result<(), TunnelError> {
        if let Some(c) = dns.cache_capacity
            && c == 0
        {
            return Err(TunnelError::Config(format!(
                "{}: dns.cache_capacity must be at least 1",
                context
            )));
        }

        if let Some(ref resolvers) = dns.resolvers {
            for resolver in resolvers {
                crate::dialer::dns::CachedDnsResolver::parse_resolver(resolver).map_err(|e| {
                    TunnelError::Config(format!(
                        "{}: invalid DNS resolver '{:?}': {}",
                        context, resolver, e
                    ))
                })?;

                if let crate::config::model::DnsResolverConfig::Detailed(d) = resolver {
                    let t = d.r#type.to_lowercase();
                    if !matches!(
                        t.as_str(),
                        "https" | "tls" | "udp" | "dns" | "tcp" | "system"
                    ) {
                        return Err(TunnelError::Config(format!(
                            "{}: unsupported DNS resolver type '{}'. Supported types: https, tls, udp, tcp, system",
                            context, d.r#type
                        )));
                    }
                    if t != "system" && d.server.is_none() {
                        return Err(TunnelError::Config(format!(
                            "{}: DNS resolver of type '{}' requires a 'server' address",
                            context, d.r#type
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Validates HTTP request headers for proper syntax and constraints.
    pub fn validate_http_headers(
        headers: &std::collections::HashMap<String, String>,
        context: &str,
    ) -> Result<(), TunnelError> {
        for (name, value) in headers {
            if name.trim().is_empty() {
                return Err(TunnelError::Config(format!(
                    "{}: HTTP header name must not be empty",
                    context
                )));
            }
            http::header::HeaderName::try_from(name.as_str()).map_err(|e| {
                TunnelError::Config(format!(
                    "{}: invalid HTTP header name '{}': {}",
                    context, name, e
                ))
            })?;
            http::header::HeaderValue::try_from(value.as_str()).map_err(|e| {
                TunnelError::Config(format!(
                    "{}: invalid HTTP header value for '{}': {}",
                    context, name, e
                ))
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_yaml_config() {
        let yaml = r#"
log:
  level: "debug"
dns:
  strategy: "ipv4_only"
dialer:
  connect_timeout: "3s"
tls:
  alpn: ["h3"]
tunnels:
  - tag: "test-tunnel"
    token: "token123"
    endpoint:
      url: "wss://tunnel.example.com/ws"
      tls:
        server_name: "custom.example.com"
        insecure: true
    mappings:
      reverse:
        rules:
          - protocol: "TCP"
            listen: "0.0.0.0:2222"
            target: "127.0.0.1:22"
"#;
        let config = ConfigLoader::from_yaml_str(yaml).unwrap();
        assert_eq!(config.log.level, "debug");
        assert_eq!(config.tunnels.len(), 1);
        assert_eq!(config.tunnels[0].tag, "test-tunnel");
        assert_eq!(
            config.tunnels[0].endpoint.url(),
            "wss://tunnel.example.com/ws"
        );
    }

    #[test]
    fn test_reject_unwired_multiplex_enabled() {
        let yaml = r#"
tunnels:
  - tag: "mux-tunnel"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10001"
            target: "127.0.0.1:10002"
    multiplex:
      enabled: true
"#;
        let res = ConfigLoader::from_yaml_str(yaml);
        assert!(res.is_err());
        let err_msg = res.unwrap_err().to_string();
        assert!(err_msg.contains("multiplex is not wired into the data path yet"));
    }

    #[test]
    fn test_reject_invalid_tls_versions() {
        // Test invalid version string
        let yaml_bad_ver = r#"
tls:
  min_version: "1.4"
tunnels:
  - tag: "tunnel1"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10003"
            target: "127.0.0.1:10004"
"#;
        let res = ConfigLoader::from_yaml_str(yaml_bad_ver);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("unsupported TLS version")
        );

        // Test contradictory range (min 1.3 > max 1.2)
        let yaml_contradictory = r#"
tls:
  min_version: "1.3"
  max_version: "1.2"
tunnels:
  - tag: "tunnel2"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10005"
            target: "127.0.0.1:10006"
"#;
        let res2 = ConfigLoader::from_yaml_str(yaml_contradictory);
        assert!(res2.is_err());
        assert!(
            res2.unwrap_err()
                .to_string()
                .contains("invalid TLS version range")
        );

        // Test server.tls validation
        let yaml_server_bad_tls = r#"
server:
  listen: ["wss://0.0.0.0:8443"]
  tls:
    min_version: "1.1"
"#;
        let res3 = ConfigLoader::from_yaml_str(yaml_server_bad_tls);
        assert!(res3.is_err());
        assert!(
            res3.unwrap_err()
                .to_string()
                .contains("unsupported TLS version")
        );

        // Test valid combinations
        let yaml_valid_tls = r#"
server:
  listen: ["wss://0.0.0.0:8443"]
  tls:
    min_version: "1.2"
    max_version: "1.2"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_valid_tls).is_ok());

        let yaml_valid_max13 = r#"
tls:
  max_version: "1.3"
tunnels:
  - tag: "valid13"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10007"
            target: "127.0.0.1:10008"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_valid_max13).is_ok());
    }

    #[test]
    fn test_reject_token_exceeding_max_len() {
        let long_token = "a".repeat(257);
        let yaml_server_long_token = format!(
            r#"
server:
  listen: ["ws://127.0.0.1:8080"]
  token: "{}"
"#,
            long_token
        );
        let res = ConfigLoader::from_yaml_str(&yaml_server_long_token);
        assert!(res.is_err(), "Must reject server.token > 256 bytes");

        let yaml_tunnel_long_token = format!(
            r#"
tunnels:
  - tag: "long_tok_tunnel"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    token: "{}"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10009"
            target: "127.0.0.1:10010"
"#,
            long_token
        );
        let res2 = ConfigLoader::from_yaml_str(&yaml_tunnel_long_token);
        assert!(res2.is_err(), "Must reject tunnel.token > 256 bytes");
    }

    #[test]
    fn test_deny_unknown_fields_top_and_nested() {
        // Unknown field at top level
        let yaml_top = r#"
unknown_field: 123
tunnels:
  - tag: "t1"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10011"
            target: "127.0.0.1:10012"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_top).is_err());

        // Unknown field in tls (e.g. deprecated 'enabled')
        let yaml_tls = r#"
tls:
  enabled: true
tunnels:
  - tag: "t1"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10013"
            target: "127.0.0.1:10014"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_tls).is_err());

        // Unknown field at tunnel level (e.g. legacy 'mode' or mislocated 'tls')
        let yaml_tunnel = r#"
tunnels:
  - tag: "t1"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    tls:
      insecure: true
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10015"
            target: "127.0.0.1:10016"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_tunnel).is_err());

        // Typo in endpoint (e.g. 'uri' instead of 'url')
        let yaml_ep = r#"
tunnels:
  - tag: "t1"
    endpoint:
      uri: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10017"
            target: "127.0.0.1:10018"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_ep).is_err());

        // Typo in DNS resolver (e.g. 'servr' instead of 'server')
        let yaml_dns = r#"
dns:
  resolvers:
    - type: "https"
      servr: "1.1.1.1"
tunnels:
  - tag: "t1"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10019"
            target: "127.0.0.1:10020"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_dns).is_err());
    }

    #[test]
    fn test_reject_scheme_tls_contradiction() {
        let yaml = r#"
tunnels:
  - tag: "ws-with-tls"
    endpoint:
      url: "ws://127.0.0.1:8080/tunnel"
      tls:
        insecure: true
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10021"
            target: "127.0.0.1:10022"
"#;
        let res = ConfigLoader::from_yaml_str(yaml);
        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(err.contains("uses non-TLS scheme 'ws', but 'tls' options are configured"));
    }

    #[test]
    fn test_reject_duplicate_tags_and_ports() {
        // Duplicate tag
        let yaml_tags = r#"
tunnels:
  - tag: "dup-tag"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10023"
            target: "127.0.0.1:10024"
  - tag: "dup-tag"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10025"
            target: "127.0.0.1:10026"
"#;
        let res = ConfigLoader::from_yaml_str(yaml_tags);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("Duplicate tunnel tag")
        );

        // Duplicate forward mapping listen port
        let yaml_ports = r#"
tunnels:
  - tag: "t1"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10027"
            target: "127.0.0.1:10028"
  - tag: "t2"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10027"
            target: "127.0.0.1:10029"
"#;
        let res2 = ConfigLoader::from_yaml_str(yaml_ports);
        assert!(res2.is_err());
        assert!(
            res2.unwrap_err()
                .to_string()
                .contains("Duplicate forward tcp mapping listen address")
        );
    }

    #[test]
    fn test_reject_empty_config_and_invalid_boundaries() {
        // Empty config
        assert!(ConfigLoader::from_yaml_str("{}").is_err());

        // worker_threads = 0
        let yaml_w0 = r#"
worker_threads: 0
server:
  listen: ["ws://127.0.0.1:8080"]
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_w0).is_err());

        // Invalid log level
        let yaml_bad_log = r#"
log:
  level: "banana"
server:
  listen: ["ws://127.0.0.1:8080"]
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_bad_log).is_err());

        // pool_size = 0
        let yaml_pool0 = r#"
tunnels:
  - tag: "pool0"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        pool_size: 0
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10030"
            target: "127.0.0.1:10031"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_pool0).is_err());

        // retry delay = 0s
        let yaml_delay0 = r#"
tunnels:
  - tag: "delay0"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    retry:
      delay: "0s"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10032"
            target: "127.0.0.1:10033"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_delay0).is_err());

        // Missing mappings
        let yaml_no_map = r#"
tunnels:
  - tag: "no-map"
    endpoint: "ws://127.0.0.1:8080/tunnel"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_no_map).is_err());

        // Empty mappings
        let yaml_empty_map = r#"
tunnels:
  - tag: "empty-map"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings: {}
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_empty_map).is_err());

        // Missing protocol in rule
        let yaml_no_proto = r#"
tunnels:
  - tag: "no-proto"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - listen: "127.0.0.1:10030"
            target: "127.0.0.1:10031"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_no_proto).is_err());
    }

    #[test]
    fn test_load_config_example_file() {
        let path = std::path::Path::new("config-example.yaml");
        let alt_path = std::path::Path::new("../../config-example.yaml");
        let p = if path.exists() {
            path
        } else if alt_path.exists() {
            alt_path
        } else {
            return;
        };
        let config = ConfigLoader::from_file(p).expect("config-example.yaml must parse cleanly");
        assert_eq!(config.log.level, "info");
        assert_eq!(config.worker_threads, Some(4));
        assert_eq!(config.tunnels.len(), 2);
    }

    #[test]
    fn test_reject_empty_tokens() {
        let yaml_server_empty_token = r#"
server:
  listen: ["ws://127.0.0.1:8080"]
  token: ""
"#;
        let res = ConfigLoader::from_yaml_str(yaml_server_empty_token);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("server.token must not be empty")
        );

        let yaml_tunnel_empty_token = r#"
tunnels:
  - tag: "empty-tok"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    token: ""
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10040"
            target: "127.0.0.1:10041"
"#;
        let res2 = ConfigLoader::from_yaml_str(yaml_tunnel_empty_token);
        assert!(res2.is_err());
        assert!(
            res2.unwrap_err()
                .to_string()
                .contains("token must not be empty")
        );
    }

    #[test]
    fn test_reject_invalid_server_default_target() {
        let yaml = r#"
server:
  listen: ["ws://127.0.0.1:8080"]
  default_target: "bad@@target@@without_port"
"#;
        let res = ConfigLoader::from_yaml_str(yaml);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("server.default_target")
        );
    }

    #[test]
    fn test_reject_uds_without_socket_path() {
        let yaml = r#"
tunnels:
  - tag: "uds-no-path"
    endpoint: "ws+unix://"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10050"
            target: "127.0.0.1:10051"
"#;
        let res = ConfigLoader::from_yaml_str(yaml);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("Unix Domain Socket endpoint URL must include a socket file path")
        );
    }

    #[test]
    fn test_reject_unwired_failover_even_if_disabled() {
        let yaml = r#"
tunnels:
  - tag: "failover-tunnel"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    failover:
      enabled: false
      standby_endpoint: "ws://127.0.0.1:8081/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:10060"
            target: "127.0.0.1:10061"
"#;
        let res = ConfigLoader::from_yaml_str(yaml);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("failover is not wired into the data path yet")
        );
    }

    #[test]
    fn test_accept_server_uds_url() {
        let yaml = r#"
server:
  listen: "ws+unix:///tmp/tunnel.sock?path=/ws&chmod=0o660"
"#;
        let res = ConfigLoader::from_yaml_str(yaml);
        assert!(res.is_ok());
        let cfg = res.unwrap();
        let specs = cfg.server.unwrap().parse_specs().unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].scheme, "ws+unix");
        assert_eq!(specs[0].path.as_deref(), Some("/ws"));
        assert_eq!(specs[0].chmod, Some(0o660));
        assert!(specs[0].is_uds());
    }

    #[test]
    fn test_reject_removed_uds_and_unix_schemes() {
        let yaml1 = r#"
server:
  listen: "uds:///tmp/tunnel.sock"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml1).is_err());

        let yaml2 = r#"
server:
  listen: "unix:///tmp/tunnel.sock"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml2).is_err());

        let yaml3 = r#"
server:
  listen: "ws+uds:///tmp/tunnel.sock"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml3).is_err());
    }

    #[test]
    fn test_accept_mapping_with_uds_listen_or_target() {
        let yaml = r#"
tunnels:
  - tag: "uds-mapping-tunnel"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "unix:///tmp/forward_listen.sock"
            target: "127.0.0.1:8080"
          - protocol: "TCP"
            listen: "127.0.0.1:18080"
            target: "unix:///tmp/forward_target.sock"
      reverse:
        rules:
          - protocol: "TCP"
            listen: "unix:///tmp/reverse_listen.sock"
            target: "unix:///tmp/reverse_target.sock"
"#;
        let res = ConfigLoader::from_yaml_str(yaml);
        assert!(res.is_ok());
        let cfg = res.unwrap();
        let tunnel = &cfg.tunnels[0];
        assert_eq!(tunnel.canonical_mappings().unwrap().len(), 3);
        assert_eq!(tunnel.mappings.rule_count(), 3);
    }

    #[test]
    fn test_accept_udp_mapping_and_same_port_tcp_udp() {
        let yaml = r#"
dialer:
  heartbeat_interval: "15s"
  udp_idle_timeout: "5s"
server:
  listen: ["ws://127.0.0.1:8080"]
  udp_idle_timeout: "20s"
tunnels:
  - tag: "dns-dual-stack"
    endpoint:
      url: "ws://127.0.0.1:8080/tunnel"
      dialer:
        udp_idle_timeout: "3s"
    mappings:
      forward:
        rules:
          - protocol: "TCP"
            listen: "127.0.0.1:5353"
            target: "8.8.8.8:53"
          - protocol: "UDP"
            listen: "127.0.0.1:5353"
            target: "8.8.8.8:53"
            udp_idle_timeout: "2s"
      reverse:
        rules:
          - protocol: "UDP"
            listen: "0.0.0.0:15353"
            target: "127.0.0.1:53"
"#;
        let cfg = ConfigLoader::from_yaml_str(yaml).unwrap();
        assert_eq!(
            cfg.dialer.as_ref().unwrap().udp_idle_timeout,
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            cfg.server.as_ref().unwrap().udp_idle_timeout,
            Some(Duration::from_secs(20))
        );
        let m = cfg.tunnels[0].canonical_mappings().unwrap();
        assert_eq!(m[0].protocol, crate::io::addr::L4Protocol::Tcp);
        assert_eq!(m[1].protocol, crate::io::addr::L4Protocol::Udp);
        assert_eq!(m[1].udp_idle_timeout, Some(Duration::from_secs(2)));
        assert_eq!(m[2].protocol, crate::io::addr::L4Protocol::Udp);
        assert_eq!(m[2].udp_idle_timeout, None);
    }

    #[test]
    fn test_reject_udp_uds_duplicate_and_zero_timeouts() {
        // Reject UDP + UDS listen
        let yaml_udp_uds = r#"
tunnels:
  - tag: "bad-udp-uds"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "UDP"
            listen: "unix:///tmp/not_allowed.sock"
            target: "127.0.0.1:5353"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_udp_uds).is_err());

        // Reject UDP + UDS target
        let yaml_udp_uds_tgt = r#"
tunnels:
  - tag: "bad-udp-uds-target"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "UDP"
            listen: "127.0.0.1:5353"
            target: "unix:///tmp/not_allowed.sock"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_udp_uds_tgt).is_err());

        // Reject duplicate UDP forward listen address
        let yaml_dup_udp = r#"
tunnels:
  - tag: "dup-udp"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "UDP"
            listen: "127.0.0.1:5354"
            target: "127.0.0.1:53"
          - protocol: "UDP"
            listen: "127.0.0.1:5354"
            target: "127.0.0.1:54"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_dup_udp).is_err());

        // Reject 0s udp_idle_timeout across all scopes
        let yaml_zero_dialer = r#"
dialer:
  udp_idle_timeout: "0s"
server:
  listen: ["ws://127.0.0.1:8080"]
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_zero_dialer).is_err());

        let yaml_zero_server = r#"
server:
  listen: ["ws://127.0.0.1:8080"]
  udp_idle_timeout: "0s"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_zero_server).is_err());

        let yaml_zero_mapping = r#"
tunnels:
  - tag: "zero-map"
    endpoint: "ws://127.0.0.1:8080/tunnel"
    mappings:
      forward:
        rules:
          - protocol: "UDP"
            listen: "127.0.0.1:5355"
            target: "127.0.0.1:53"
            udp_idle_timeout: "0s"
"#;
        assert!(ConfigLoader::from_yaml_str(yaml_zero_mapping).is_err());
    }
}
