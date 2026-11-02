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
        serde_yaml::from_str(yaml_str).map_err(|e| TunnelError::Config(e.to_string()))
    }

    /// Reads and parses an `AppConfig` from a YAML file path.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<AppConfig, TunnelError> {
        let content = fs::read_to_string(path.as_ref()).map_err(TunnelError::Io)?;
        Self::from_yaml_str(&content)
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
}
