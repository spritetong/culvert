//! Unified error definitions for the micro-tunnel system.
//! All comments and docstrings must be in English.

use std::io;
use thiserror::Error;

/// Enumeration of all recoverable and fatal tunnel errors.
#[derive(Debug, Error)]
pub enum TunnelError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("URL parsing error: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Unsupported protocol scheme: {0}")]
    UnsupportedScheme(String),

    #[error("DNS resolution failed for host '{0}': {1}")]
    DnsResolutionFailed(String, String),

    #[error("No resolved IP addresses for host '{0}' under strategy {1}")]
    DnsNoAddresses(String, String),

    #[error("TLS configuration error: {0}")]
    Tls(String),

    #[error("Connection timeout after {0:?}")]
    ConnectTimeout(std::time::Duration),

    #[error("Authentication failed: invalid or missing token")]
    AuthFailed,

    #[error("Session not found: {0}")]
    SessionNotFound(String),

    #[error("Session channel closed prematurely")]
    SessionClosed,

    #[error("Session pair timeout for session '{0}'")]
    SessionPairTimeout(String),

    #[error("Control protocol error: {0}")]
    ControlProtocol(String),

    #[error("Connection pool exhausted or shut down")]
    PoolExhausted,

    #[error("Channel shutdown requested")]
    Shutdown,

    #[error("Internal error: {0}")]
    Internal(String),
}
