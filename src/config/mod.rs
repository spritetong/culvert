//! Configuration module providing declarative YAML models and file loaders.
//! All comments and docstrings must be in English.

pub mod loader;
pub mod model;

pub use loader::ConfigLoader;
pub use model::{
    AppConfig, DialerConfig, DnsConfig, DnsStrategy, EndpointConfig, FailoverConfig, LogConfig,
    TlsConfig, TunnelConfig, TunnelMode,
};
