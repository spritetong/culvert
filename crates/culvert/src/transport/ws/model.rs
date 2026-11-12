// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Handshake data models and header codecs for the WebSocket transport.
//! All comments and docstrings must be in English.

use http::header::HeaderMap;
use http::request::Builder as RequestBuilder;

use crate::config::model::TunnelMode;
use crate::consts::{
    HEADER_SESSION_ID, HEADER_TARGET_HOST, HEADER_TARGET_PATH, HEADER_TARGET_PORT,
    HEADER_TUNNEL_AUTH, HEADER_TUNNEL_ID, HEADER_TUNNEL_MODE, HEADER_TUNNEL_POOL,
    HEADER_TUNNEL_PROTO, TUNNEL_MODE_FORWARD, TUNNEL_MODE_REVERSE_CTRL, TUNNEL_MODE_REVERSE_DATA,
    TUNNEL_POOL_IDLE, TUNNEL_PROTO_UDP,
};
use crate::core::error::TunnelError;
use crate::io::addr::L4Protocol;
use crate::transport::{StreamMetadata, TargetAddr};

/// Structured representation of handshake metadata carried in HTTP upgrade headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsHandshakeHeaders {
    pub auth_token: Option<String>,
    pub tunnel_mode: TunnelMode,
    pub session_id: Option<String>,
    pub target_host: Option<String>,
    pub target_port: Option<u16>,
    pub target_path: Option<String>,
    pub pool_idle: bool,
    pub protocol: L4Protocol,
    pub tunnel_id: Option<String>,
}

impl WsHandshakeHeaders {
    /// Creates a new `WsHandshakeHeaders` with the optional authentication token and mode.
    pub fn new(auth_token: Option<String>, tunnel_mode: TunnelMode) -> Self {
        Self {
            auth_token,
            tunnel_mode,
            session_id: None,
            target_host: None,
            target_port: None,
            target_path: None,
            pool_idle: false,
            protocol: L4Protocol::Tcp,
            tunnel_id: None,
        }
    }

    /// Sets the optional tunnel instance identifier (Tunnel UUID).
    pub fn with_tunnel_id(mut self, tunnel_id: Option<String>) -> Self {
        self.tunnel_id = tunnel_id;
        self
    }

    /// Sets the L4 mapping protocol (`tcp` or `udp`).
    pub fn with_protocol(mut self, protocol: L4Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Attaches whether this stream is a pre-warmed idle pool connection.
    pub fn with_pool_idle(mut self, pool_idle: bool) -> Self {
        self.pool_idle = pool_idle;
        self
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
                    self.target_path = None;
                }
                TargetAddr::Domain(host, port) => {
                    self.target_host = Some(host.clone());
                    self.target_port = Some(*port);
                    self.target_path = None;
                }
                TargetAddr::Path(path) => {
                    self.target_host = None;
                    self.target_port = None;
                    self.target_path = Some(path.to_string_lossy().to_string());
                }
            }
        }
        self
    }

    /// Parses and validates headers from an incoming HTTP request HeaderMap.
    pub fn from_http_headers(headers: &HeaderMap) -> Result<Self, TunnelError> {
        // Extract optional authentication token
        let auth_token = headers
            .get(HEADER_TUNNEL_AUTH)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

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

        let target_path = headers
            .get(HEADER_TARGET_PATH)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let tunnel_id = headers
            .get(HEADER_TUNNEL_ID)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let pool_idle = headers
            .get(HEADER_TUNNEL_POOL)
            .and_then(|v| v.to_str().ok())
            .map(|v| v == TUNNEL_POOL_IDLE)
            .unwrap_or(false);

        let protocol = match headers
            .get(HEADER_TUNNEL_PROTO)
            .and_then(|v| v.to_str().ok())
        {
            Some(s) => s
                .parse::<L4Protocol>()
                .map_err(|e| TunnelError::ControlProtocol(e.to_string()))?,
            None => L4Protocol::Tcp,
        };

        Ok(Self {
            auth_token,
            tunnel_mode,
            session_id,
            target_host,
            target_port,
            target_path,
            pool_idle,
            protocol,
            tunnel_id,
        })
    }

    /// Injects these handshake headers into an outgoing HTTP request builder.
    pub fn apply_to_request_builder(&self, mut builder: RequestBuilder) -> RequestBuilder {
        if let Some(ref token) = self.auth_token
            && !token.is_empty()
        {
            builder = builder.header(HEADER_TUNNEL_AUTH, token);
        }

        let mode_str = match self.tunnel_mode {
            TunnelMode::Forward => TUNNEL_MODE_FORWARD,
            TunnelMode::Reverse => {
                if self.session_id.is_some() || self.pool_idle {
                    TUNNEL_MODE_REVERSE_DATA
                } else {
                    TUNNEL_MODE_REVERSE_CTRL
                }
            }
        };
        builder = builder.header(HEADER_TUNNEL_MODE, mode_str);

        if self.pool_idle {
            builder = builder.header(HEADER_TUNNEL_POOL, TUNNEL_POOL_IDLE);
        }

        if self.protocol == L4Protocol::Udp {
            builder = builder.header(HEADER_TUNNEL_PROTO, TUNNEL_PROTO_UDP);
        }

        if let Some(ref tid) = self.tunnel_id {
            builder = builder.header(HEADER_TUNNEL_ID, tid);
        }

        if let Some(ref sid) = self.session_id {
            builder = builder.header(HEADER_SESSION_ID, sid);
        }

        if let Some(ref host) = self.target_host {
            builder = builder.header(HEADER_TARGET_HOST, host);
        }

        if let Some(port) = self.target_port {
            builder = builder.header(HEADER_TARGET_PORT, port.to_string());
        }

        if let Some(ref path) = self.target_path {
            builder = builder.header(HEADER_TARGET_PATH, path);
        }

        builder
    }

    /// Converts the handshake headers into unified `StreamMetadata`.
    pub fn to_stream_metadata(&self) -> StreamMetadata {
        let target = if let Some(ref path) = self.target_path {
            Some(TargetAddr::Path(std::path::PathBuf::from(path)))
        } else {
            match (&self.target_host, self.target_port) {
                (Some(host), Some(port)) => {
                    if let Ok(ip) = host.parse() {
                        Some(TargetAddr::Ip(std::net::SocketAddr::new(ip, port)))
                    } else {
                        Some(TargetAddr::Domain(host.clone(), port))
                    }
                }
                _ => None,
            }
        };

        StreamMetadata {
            mode: self.tunnel_mode,
            session_id: self.session_id.clone(),
            target,
            pool_idle: self.pool_idle,
            protocol: self.protocol,
            tunnel_id: self.tunnel_id.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Request;

    #[test]
    fn test_ws_handshake_headers_roundtrip() {
        let headers =
            WsHandshakeHeaders::new(Some("secret_psk_123".to_string()), TunnelMode::Forward)
                .with_session_id(Some("sess_abc".to_string()))
                .with_target(Some(&TargetAddr::Domain("127.0.0.1".to_string(), 22)));

        let req = headers
            .apply_to_request_builder(Request::builder())
            .body(())
            .unwrap();

        let parsed = WsHandshakeHeaders::from_http_headers(req.headers()).unwrap();
        assert_eq!(parsed.auth_token, Some("secret_psk_123".to_string()));
        assert_eq!(parsed.tunnel_mode, TunnelMode::Forward);
        assert_eq!(parsed.protocol, L4Protocol::Tcp);
        assert_eq!(parsed.session_id, Some("sess_abc".to_string()));
        assert_eq!(parsed.target_host, Some("127.0.0.1".to_string()));
        assert_eq!(parsed.target_port, Some(22));

        let metadata = parsed.to_stream_metadata();
        assert_eq!(metadata.mode, TunnelMode::Forward);
        assert_eq!(metadata.protocol, L4Protocol::Tcp);
        assert_eq!(metadata.session_id, Some("sess_abc".to_string()));
        assert_eq!(
            metadata.target,
            Some(TargetAddr::Ip("127.0.0.1:22".parse().unwrap()))
        );
    }

    #[test]
    fn test_ws_handshake_headers_udp_protocol() {
        let headers = WsHandshakeHeaders::new(None, TunnelMode::Forward)
            .with_protocol(L4Protocol::Udp)
            .with_target(Some(&TargetAddr::Domain("127.0.0.1".to_string(), 5353)));

        let req = headers
            .apply_to_request_builder(Request::builder())
            .body(())
            .unwrap();

        let parsed = WsHandshakeHeaders::from_http_headers(req.headers()).unwrap();
        assert_eq!(parsed.protocol, L4Protocol::Udp);
        assert_eq!(parsed.to_stream_metadata().protocol, L4Protocol::Udp);
    }

    #[test]
    fn test_ws_handshake_optional_auth() {
        let req = Request::builder().body(()).unwrap();
        let parsed = WsHandshakeHeaders::from_http_headers(req.headers()).unwrap();
        assert_eq!(parsed.auth_token, None);
        assert_eq!(parsed.tunnel_mode, TunnelMode::Forward);
    }
}
