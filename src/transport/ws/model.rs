//! Handshake data models and header codecs for the WebSocket transport.
//! All comments and docstrings must be in English.

use http::header::HeaderMap;
use http::request::Builder as RequestBuilder;

use crate::config::model::TunnelMode;
use crate::consts::{
    HEADER_SESSION_ID, HEADER_TARGET_HOST, HEADER_TARGET_PORT, HEADER_TUNNEL_AUTH,
    HEADER_TUNNEL_MODE, TUNNEL_MODE_FORWARD, TUNNEL_MODE_REVERSE_CTRL, TUNNEL_MODE_REVERSE_DATA,
};
use crate::core::error::TunnelError;
use crate::transport::{StreamMetadata, TargetAddr};

/// Structured representation of handshake metadata carried in HTTP upgrade headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsHandshakeHeaders {
    pub auth_token: String,
    pub tunnel_mode: TunnelMode,
    pub session_id: Option<String>,
    pub target_host: Option<String>,
    pub target_port: Option<u16>,
}

impl WsHandshakeHeaders {
    /// Creates a new `WsHandshakeHeaders` with the required authentication token and mode.
    pub fn new(auth_token: impl Into<String>, tunnel_mode: TunnelMode) -> Self {
        Self {
            auth_token: auth_token.into(),
            tunnel_mode,
            session_id: None,
            target_host: None,
            target_port: None,
        }
    }

    /// Attaches the session identifier.
    pub fn with_session_id(mut self, session_id: Option<String>) -> Self {
        self.session_id = session_id;
        self
    }

    /// Attaches the dynamic forwarding target.
    pub fn with_target(mut self, target: Option<&TargetAddr>) -> Self {
        if let Some(t) = target {
            match t {
                TargetAddr::Ip(sa) => {
                    self.target_host = Some(sa.ip().to_string());
                    self.target_port = Some(sa.port());
                }
                TargetAddr::Domain(host, port) => {
                    self.target_host = Some(host.clone());
                    self.target_port = Some(*port);
                }
            }
        }
        self
    }

    /// Parses and validates headers from an incoming HTTP request HeaderMap.
    pub fn from_http_headers(headers: &HeaderMap) -> Result<Self, TunnelError> {
        // Extract authentication token
        let auth_token = headers
            .get(HEADER_TUNNEL_AUTH)
            .and_then(|v| v.to_str().ok())
            .ok_or(TunnelError::AuthFailed)?
            .to_string();

        // Extract and map tunnel mode
        let mode_str = headers
            .get(HEADER_TUNNEL_MODE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(TUNNEL_MODE_FORWARD);

        let tunnel_mode = match mode_str {
            TUNNEL_MODE_FORWARD => TunnelMode::Forward,
            TUNNEL_MODE_REVERSE_CTRL => TunnelMode::Reverse,
            TUNNEL_MODE_REVERSE_DATA => TunnelMode::Reverse,
            other => {
                return Err(TunnelError::ControlProtocol(format!(
                    "Unrecognized tunnel mode: {}",
                    other
                )));
            }
        };

        // Extract optional session id
        let session_id = headers
            .get(HEADER_SESSION_ID)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Extract optional target host and port
        let target_host = headers
            .get(HEADER_TARGET_HOST)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let target_port = headers
            .get(HEADER_TARGET_PORT)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u16>().ok());

        Ok(Self {
            auth_token,
            tunnel_mode,
            session_id,
            target_host,
            target_port,
        })
    }

    /// Injects these handshake headers into an outgoing HTTP request builder.
    pub fn apply_to_request_builder(&self, mut builder: RequestBuilder) -> RequestBuilder {
        builder = builder.header(HEADER_TUNNEL_AUTH, &self.auth_token);

        let mode_str = match self.tunnel_mode {
            TunnelMode::Forward => TUNNEL_MODE_FORWARD,
            TunnelMode::Reverse => {
                if self.session_id.is_some() {
                    TUNNEL_MODE_REVERSE_DATA
                } else {
                    TUNNEL_MODE_REVERSE_CTRL
                }
            }
        };
        builder = builder.header(HEADER_TUNNEL_MODE, mode_str);

        if let Some(ref sid) = self.session_id {
            builder = builder.header(HEADER_SESSION_ID, sid);
        }

        if let Some(ref host) = self.target_host {
            builder = builder.header(HEADER_TARGET_HOST, host);
        }

        if let Some(port) = self.target_port {
            builder = builder.header(HEADER_TARGET_PORT, port.to_string());
        }

        builder
    }

    /// Converts the handshake headers into unified `StreamMetadata`.
    pub fn to_stream_metadata(&self) -> StreamMetadata {
        let target = match (&self.target_host, self.target_port) {
            (Some(host), Some(port)) => {
                if let Ok(ip) = host.parse() {
                    Some(TargetAddr::Ip(std::net::SocketAddr::new(ip, port)))
                } else {
                    Some(TargetAddr::Domain(host.clone(), port))
                }
            }
            _ => None,
        };

        StreamMetadata {
            mode: self.tunnel_mode,
            session_id: self.session_id.clone(),
            target,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Request;

    #[test]
    fn test_ws_handshake_headers_roundtrip() {
        let headers = WsHandshakeHeaders::new("secret_psk_123", TunnelMode::Forward)
            .with_session_id(Some("sess_abc".to_string()))
            .with_target(Some(&TargetAddr::Domain("127.0.0.1".to_string(), 22)));

        let req = headers
            .apply_to_request_builder(Request::builder())
            .body(())
            .unwrap();

        let parsed = WsHandshakeHeaders::from_http_headers(req.headers()).unwrap();
        assert_eq!(parsed.auth_token, "secret_psk_123");
        assert_eq!(parsed.tunnel_mode, TunnelMode::Forward);
        assert_eq!(parsed.session_id, Some("sess_abc".to_string()));
        assert_eq!(parsed.target_host, Some("127.0.0.1".to_string()));
        assert_eq!(parsed.target_port, Some(22));

        let metadata = parsed.to_stream_metadata();
        assert_eq!(metadata.mode, TunnelMode::Forward);
        assert_eq!(metadata.session_id, Some("sess_abc".to_string()));
        assert_eq!(
            metadata.target,
            Some(TargetAddr::Ip("127.0.0.1:22".parse().unwrap()))
        );
    }

    #[test]
    fn test_ws_handshake_missing_auth() {
        let req = Request::builder().body(()).unwrap();
        let res = WsHandshakeHeaders::from_http_headers(req.headers());
        assert!(res.is_err());
    }
}
