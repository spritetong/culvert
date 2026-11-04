//! Configuration loader for parsing and validating YAML config files.
//! All comments and docstrings must be in English.

use crate::config::model::AppConfig;
use crate::core::error::TunnelError;
use std::fs;
use std::path::Path;

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

    /// Validates configuration constraints and unsupported/preview feature flags.
    pub fn validate(config: &AppConfig) -> Result<(), TunnelError> {
        for tunnel in &config.tunnels {
            if let Some(ref m) = tunnel.multiplex
                && m.enabled
            {
                return Err(TunnelError::Config(format!(
                    "tunnel '{}': multiplex is not wired into the data path yet; remove it or set enabled: false",
                    tunnel.tag
                )));
            }
            if let Some(ref f) = tunnel.failover
                && f.enabled
            {
                return Err(TunnelError::Config(format!(
                    "tunnel '{}': failover is not wired into the data path yet; remove it or set enabled: false",
                    tunnel.tag
                )));
            }
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
  enabled: true
  alpn: ["h3"]
tunnels:
  - tag: "test-tunnel"
    mode: "reverse"
    listen: "0.0.0.0:2222"
    target: "127.0.0.1:22"
    token: "token123"
    endpoint:
      url: "wss://tunnel.example.com/ws"
      tls:
        server_name: "custom.example.com"
        insecure: true
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
    multiplex:
      enabled: true
"#;
        let res = ConfigLoader::from_yaml_str(yaml);
        assert!(res.is_err());
        let err_msg = res.unwrap_err().to_string();
        assert!(err_msg.contains("multiplex is not wired into the data path yet"));
    }

    #[test]
    fn test_load_config_example_file() {
        let path = std::path::Path::new("config-example.yaml");
        if path.exists() {
            let config =
                ConfigLoader::from_file(path).expect("config-example.yaml must parse cleanly");
            assert_eq!(config.log.level, "info");
            assert_eq!(config.worker_threads, Some(4));
            assert_eq!(config.tunnels.len(), 2);
        }
    }
}
