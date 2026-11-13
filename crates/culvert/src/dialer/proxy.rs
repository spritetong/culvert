// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Outbound proxy dialer implementing transparent HTTP/HTTPS CONNECT and SOCKS5 tunnels.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::dialer::unified::Dialer;

/// Supported outbound proxy protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyScheme {
    /// Plain HTTP forward proxy using CONNECT method (RFC 7231).
    Http,
    /// HTTPS forward proxy using TLS followed by CONNECT method.
    Https,
    /// SOCKS5 proxy using domain-based ATYP=0x03 (RFC 1928, RFC 1929).
    Socks5,
}

/// Parsed configuration for an outbound proxy server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyConfig {
    pub scheme: ProxyScheme,
    pub host: String,
    pub port: u16,
    pub auth: Option<(String, String)>,
}

impl ProxyConfig {
    /// Parses a proxy URL string (e.g. `http://proxy:8080`, `socks5://user:pass@127.0.0.1:1080`).
    pub fn parse(proxy_url: &str) -> Result<Self, TunnelError> {
        let parsed = Url::parse(proxy_url).map_err(|e| {
            TunnelError::Config(format!("Invalid proxy URL '{}': {}", proxy_url, e))
        })?;

        let scheme = match parsed.scheme() {
            "http" => ProxyScheme::Http,
            "https" => ProxyScheme::Https,
            "socks5" | "socks5h" => ProxyScheme::Socks5,
            other => {
                return Err(TunnelError::Config(format!(
                    "Unsupported proxy scheme '{}' in proxy URL '{}'. Supported schemes: http, https, socks5, socks5h",
                    other, proxy_url
                )));
            }
        };

        let host = parsed
            .host_str()
            .ok_or_else(|| {
                TunnelError::Config(format!("Missing host in proxy URL '{}'", proxy_url))
            })?
            .to_string();

        let port = parsed.port().unwrap_or(match scheme {
            ProxyScheme::Http => 8080,
            ProxyScheme::Https => 443,
            ProxyScheme::Socks5 => 1080,
        });

        let auth = if !parsed.username().is_empty() || parsed.password().is_some() {
            let user = percent_decode(parsed.username());
            let pass = parsed.password().map(percent_decode).unwrap_or_default();
            Some((user, pass))
        } else {
            None
        };

        Ok(Self {
            scheme,
            host,
            port,
            auth,
        })
    }
}

/// Safely percent-decodes standard URL characters without panics or external dependencies.
fn percent_decode(input: &str) -> String {
    let mut bytes = Vec::with_capacity(input.len());
    let mut chars = input.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let h1 = chars.next().unwrap_or(0);
            let h2 = chars.next().unwrap_or(0);
            let hex = [h1, h2];
            if let Ok(s) = std::str::from_utf8(&hex)
                && let Ok(val) = u8::from_str_radix(s, 16)
            {
                bytes.push(val);
                continue;
            }
            bytes.push(b'%');
            if h1 != 0 {
                bytes.push(h1);
            }
            if h2 != 0 {
                bytes.push(h2);
            }
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8_lossy(&bytes).to_string()
}

/// Encodes raw bytes to a standard Base64 string for HTTP Basic Authentication.
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = if chunk.len() > 1 { chunk[1] } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] } else { 0 };
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// Performs an HTTP CONNECT handshake over the raw stream (RFC 7231).
async fn handshake_http_connect(
    mut stream: Box<dyn TunnelStream>,
    target_host: &str,
    target_port: u16,
    auth: Option<&(String, String)>,
) -> Result<Box<dyn TunnelStream>, TunnelError> {
    let mut req = format!(
        "CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\nProxy-Connection: Keep-Alive\r\nUser-Agent: culvert\r\n",
        target_host, target_port, target_host, target_port
    );
    if let Some((user, pass)) = auth {
        let creds = format!("{}:{}", user, pass);
        let b64 = base64_encode(creds.as_bytes());
        req.push_str(&format!("Proxy-Authorization: Basic {}\r\n", b64));
    }
    req.push_str("\r\n");

    stream
        .write_all(req.as_bytes())
        .await
        .map_err(TunnelError::Io)?;
    stream.flush().await.map_err(TunnelError::Io)?;

    // Read response line-by-line / byte-by-byte until \r\n\r\n to prevent consuming subsequent payload
    let mut header_buf = Vec::with_capacity(256);
    let mut single_byte = [0u8; 1];

    while header_buf.len() < 8192 {
        let n = stream
            .read(&mut single_byte)
            .await
            .map_err(TunnelError::Io)?;
        if n == 0 {
            return Err(TunnelError::Proxy(
                "HTTP proxy closed connection unexpectedly during CONNECT handshake".to_string(),
            ));
        }
        header_buf.push(single_byte[0]);
        if header_buf.ends_with(b"\r\n\r\n") {
            break;
        }
    }

    if !header_buf.ends_with(b"\r\n\r\n") {
        return Err(TunnelError::Proxy(
            "HTTP proxy response header exceeded 8KB or was incomplete".to_string(),
        ));
    }

    let header_str = String::from_utf8_lossy(&header_buf);
    let status_line = header_str.lines().next().unwrap_or("").trim();

    let mut parts = status_line.split_whitespace();
    let _version = parts.next();
    let status_code = parts.next().and_then(|s| s.parse::<u16>().ok());

    match status_code {
        Some(code) if (200..=299).contains(&code) => Ok(stream),
        Some(code) => Err(TunnelError::Proxy(format!(
            "HTTP proxy rejected CONNECT request with status {}: {}",
            code, status_line
        ))),
        None => Err(TunnelError::Proxy(format!(
            "Malformed HTTP proxy response status line: '{}'",
            status_line
        ))),
    }
}

/// Performs a SOCKS5 handshake over the raw stream (RFC 1928, RFC 1929).
/// Always uses domain name (ATYP=0x03) when target_host is a domain to delegate DNS resolution to proxy.
async fn handshake_socks5(
    mut stream: Box<dyn TunnelStream>,
    target_host: &str,
    target_port: u16,
    auth: Option<&(String, String)>,
) -> Result<Box<dyn TunnelStream>, TunnelError> {
    // 1. Method Selection
    if auth.is_some() {
        // Offer NO AUTH (0x00) and USERNAME/PASSWORD (0x02)
        stream
            .write_all(&[0x05, 0x02, 0x00, 0x02])
            .await
            .map_err(TunnelError::Io)?;
    } else {
        // Offer only NO AUTH (0x00)
        stream
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .map_err(TunnelError::Io)?;
    }
    stream.flush().await.map_err(TunnelError::Io)?;

    // 2. Read Method Selection response (2 bytes)
    let mut method_resp = [0u8; 2];
    stream
        .read_exact(&mut method_resp)
        .await
        .map_err(TunnelError::Io)?;
    if method_resp[0] != 0x05 {
        return Err(TunnelError::Proxy(format!(
            "Invalid SOCKS5 version in method negotiation response: {:#04x}",
            method_resp[0]
        )));
    }

    match method_resp[1] {
        0x00 => {
            // No authentication required
        }
        0x02 => {
            // Username/Password authentication (RFC 1929)
            let (user, pass) = auth.ok_or_else(|| {
                TunnelError::Proxy(
                    "SOCKS5 proxy requested authentication, but no credentials configured"
                        .to_string(),
                )
            })?;
            let ulen = u8::try_from(user.len())
                .map_err(|_| TunnelError::Proxy("SOCKS5 username exceeds 255 bytes".to_string()))?;
            let plen = u8::try_from(pass.len())
                .map_err(|_| TunnelError::Proxy("SOCKS5 password exceeds 255 bytes".to_string()))?;

            let mut auth_req = Vec::with_capacity(3 + user.len() + pass.len());
            auth_req.push(0x01); // Subnegotiation version 1
            auth_req.push(ulen);
            auth_req.extend_from_slice(user.as_bytes());
            auth_req.push(plen);
            auth_req.extend_from_slice(pass.as_bytes());

            stream.write_all(&auth_req).await.map_err(TunnelError::Io)?;
            stream.flush().await.map_err(TunnelError::Io)?;

            let mut auth_resp = [0u8; 2];
            stream
                .read_exact(&mut auth_resp)
                .await
                .map_err(TunnelError::Io)?;
            if auth_resp[0] != 0x01 || auth_resp[1] != 0x00 {
                return Err(TunnelError::Proxy(format!(
                    "SOCKS5 authentication failed with status code {:#04x}",
                    auth_resp[1]
                )));
            }
        }
        0xFF => {
            return Err(TunnelError::Proxy(
                "SOCKS5 proxy rejected authentication methods (no acceptable methods)".to_string(),
            ));
        }
        other => {
            return Err(TunnelError::Proxy(format!(
                "SOCKS5 proxy requested unsupported authentication method {:#04x}",
                other
            )));
        }
    }

    // 3. Send CONNECT Request
    // [0x05, 0x01 (CMD=CONNECT), 0x00 (RSV), ATYP, ADDR, PORT(2 bytes big-endian)]
    let mut connect_req = Vec::with_capacity(4 + target_host.len() + 3);
    connect_req.extend_from_slice(&[0x05, 0x01, 0x00]);

    if let Ok(ipv4) = target_host.parse::<std::net::Ipv4Addr>() {
        connect_req.push(0x01); // ATYP: IPv4
        connect_req.extend_from_slice(&ipv4.octets());
    } else if let Ok(ipv6) = target_host.parse::<std::net::Ipv6Addr>() {
        connect_req.push(0x04); // ATYP: IPv6
        connect_req.extend_from_slice(&ipv6.octets());
    } else {
        // Domain name (ATYP=0x03) -> DNS resolved remotely by the proxy!
        let domain_len = u8::try_from(target_host.len())
            .map_err(|_| TunnelError::Proxy("Target hostname exceeds 255 bytes".to_string()))?;
        connect_req.push(0x03);
        connect_req.push(domain_len);
        connect_req.extend_from_slice(target_host.as_bytes());
    }
    connect_req.extend_from_slice(&target_port.to_be_bytes());

    stream
        .write_all(&connect_req)
        .await
        .map_err(TunnelError::Io)?;
    stream.flush().await.map_err(TunnelError::Io)?;

    // 4. Read CONNECT Response
    // Format: [VER, REP, RSV, ATYP, BND.ADDR, BND.PORT]
    let mut resp_header = [0u8; 4];
    stream
        .read_exact(&mut resp_header)
        .await
        .map_err(TunnelError::Io)?;
    if resp_header[0] != 0x05 {
        return Err(TunnelError::Proxy(format!(
            "Invalid SOCKS5 version in CONNECT response: {:#04x}",
            resp_header[0]
        )));
    }

    let rep = resp_header[1];
    if rep != 0x00 {
        let msg = match rep {
            0x01 => "general SOCKS server failure",
            0x02 => "connection not allowed by ruleset",
            0x03 => "network unreachable",
            0x04 => "host unreachable",
            0x05 => "connection refused",
            0x06 => "TTL expired",
            0x07 => "command not supported",
            0x08 => "address type not supported",
            _ => "unknown error",
        };
        return Err(TunnelError::Proxy(format!(
            "SOCKS5 proxy CONNECT failed with status {:#04x}: {}",
            rep, msg
        )));
    }

    // Drain bound address and port based on ATYP
    match resp_header[3] {
        0x01 => {
            // IPv4: 4 bytes addr + 2 bytes port
            let mut buf = [0u8; 6];
            stream.read_exact(&mut buf).await.map_err(TunnelError::Io)?;
        }
        0x04 => {
            // IPv6: 16 bytes addr + 2 bytes port
            let mut buf = [0u8; 18];
            stream.read_exact(&mut buf).await.map_err(TunnelError::Io)?;
        }
        0x03 => {
            // Domain: 1 byte len + len bytes addr + 2 bytes port
            let mut len_buf = [0u8; 1];
            stream
                .read_exact(&mut len_buf)
                .await
                .map_err(TunnelError::Io)?;
            let len = len_buf[0] as usize;
            let mut rest = vec![0u8; len + 2];
            stream
                .read_exact(&mut rest)
                .await
                .map_err(TunnelError::Io)?;
        }
        other => {
            return Err(TunnelError::Proxy(format!(
                "Invalid ATYP in SOCKS5 CONNECT response: {:#04x}",
                other
            )));
        }
    }

    Ok(stream)
}

/// Dialer that routes outbound connection requests through an intermediate proxy server.
pub struct ProxyDialer {
    inner_dialer: Arc<dyn Dialer>,
    proxy: ProxyConfig,
    tls_connector: Option<Arc<tokio_rustls::TlsConnector>>,
    handshake_timeout: Duration,
}

impl ProxyDialer {
    /// Creates a new `ProxyDialer` wrapping an inner dialer.
    pub fn try_new(
        inner_dialer: Arc<dyn Dialer>,
        proxy: ProxyConfig,
        handshake_timeout: Duration,
    ) -> Result<Self, TunnelError> {
        let tls_connector = if proxy.scheme == ProxyScheme::Https {
            let tls_builder = crate::dialer::tls::TlsConfigBuilder::build_connector(
                &crate::config::model::TlsConfig::default(),
            )?;
            Some(Arc::new(tls_builder))
        } else {
            None
        };

        Ok(Self {
            inner_dialer,
            proxy,
            tls_connector,
            handshake_timeout,
        })
    }
}

impl Dialer for ProxyDialer {
    fn dial<'a>(
        &'a self,
        target_host: &'a str,
        target_port: u16,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        Box::pin(async move {
            // 1. Dial proxy server (inner_dialer resolves self.proxy.host using local DNS)
            let raw_stream = self
                .inner_dialer
                .dial(&self.proxy.host, self.proxy.port)
                .await?;

            // 2. Perform TLS handshake if proxy itself is an HTTPS proxy
            let proxy_stream: Box<dyn TunnelStream> = if let Some(ref tls_conn) = self.tls_connector
            {
                let server_name = rustls::pki_types::ServerName::try_from(self.proxy.host.clone())
                    .map_err(|e| TunnelError::Tls(format!("Invalid proxy SNI: {}", e)))?;
                let tls_stream = tokio::time::timeout(
                    self.handshake_timeout,
                    tls_conn.connect(server_name, raw_stream),
                )
                .await
                .map_err(|_| TunnelError::ConnectTimeout(self.handshake_timeout))?
                .map_err(|e| {
                    TunnelError::TlsHandshake(format!("HTTPS proxy TLS handshake failed: {}", e))
                })?;
                Box::new(crate::io::stream::BufferedTlsStream::new(tls_stream))
                    as Box<dyn TunnelStream>
            } else {
                raw_stream
            };

            // 3. Negotiate tunnel through the proxy (target_host is passed directly without local DNS lookup)
            let handshake_fut = async {
                match self.proxy.scheme {
                    ProxyScheme::Http | ProxyScheme::Https => {
                        handshake_http_connect(
                            proxy_stream,
                            target_host,
                            target_port,
                            self.proxy.auth.as_ref(),
                        )
                        .await
                    }
                    ProxyScheme::Socks5 => {
                        handshake_socks5(
                            proxy_stream,
                            target_host,
                            target_port,
                            self.proxy.auth.as_ref(),
                        )
                        .await
                    }
                }
            };

            tokio::time::timeout(self.handshake_timeout, handshake_fut)
                .await
                .map_err(|_| TunnelError::ConnectTimeout(self.handshake_timeout))?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_config_parse_http() {
        let p = ProxyConfig::parse("http://proxy.corp.com:8080").unwrap();
        assert_eq!(p.scheme, ProxyScheme::Http);
        assert_eq!(p.host, "proxy.corp.com");
        assert_eq!(p.port, 8080);
        assert_eq!(p.auth, None);
    }

    #[test]
    fn test_proxy_config_parse_auth() {
        let p = ProxyConfig::parse("socks5://alice:secret%20123@127.0.0.1:1080").unwrap();
        assert_eq!(p.scheme, ProxyScheme::Socks5);
        assert_eq!(p.host, "127.0.0.1");
        assert_eq!(p.port, 1080);
        assert_eq!(
            p.auth,
            Some(("alice".to_string(), "secret 123".to_string()))
        );
    }

    #[test]
    fn test_proxy_config_parse_defaults() {
        let p = ProxyConfig::parse("socks5h://10.0.0.1").unwrap();
        assert_eq!(p.scheme, ProxyScheme::Socks5);
        assert_eq!(p.host, "10.0.0.1");
        assert_eq!(p.port, 1080);

        let p_https = ProxyConfig::parse("https://proxy.example.com").unwrap();
        assert_eq!(p_https.scheme, ProxyScheme::Https);
        assert_eq!(p_https.port, 443);
    }

    #[test]
    fn test_proxy_config_unsupported_scheme() {
        assert!(ProxyConfig::parse("ftp://127.0.0.1").is_err());
    }

    #[test]
    fn test_base64_encode() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"alice:secret"), "YWxpY2U6c2VjcmV0");
    }

    #[tokio::test]
    async fn test_http_connect_tunnel_roundtrip() {
        let target_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = target_listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            socket.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
            socket.write_all(b"pong").await.unwrap();
        });

        let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut client_sock, _) = proxy_listener.accept().await.unwrap();
            let mut req = Vec::new();
            let mut byte = [0u8; 1];
            while req.len() < 1024 {
                client_sock.read_exact(&mut byte).await.unwrap();
                req.push(byte[0]);
                if req.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let req_str = String::from_utf8_lossy(&req);
            assert!(req_str.starts_with(&format!(
                "CONNECT 127.0.0.1:{} HTTP/1.1",
                target_addr.port()
            )));
            client_sock
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();

            let mut target_sock = tokio::net::TcpStream::connect(target_addr).await.unwrap();
            let _ = tokio::io::copy_bidirectional(&mut client_sock, &mut target_sock).await;
        });

        let proxy_config = ProxyConfig::parse(&format!("http://{}", proxy_addr)).unwrap();
        let default_dialer = Arc::new(crate::dialer::unified::DefaultDialer::new(
            crate::config::model::DialerConfig::default(),
            Arc::new(
                crate::dialer::dns::CachedDnsResolver::try_new(
                    crate::config::model::DnsConfig::default(),
                )
                .unwrap(),
            ),
        ));
        let proxy_dialer =
            ProxyDialer::try_new(default_dialer, proxy_config, Duration::from_secs(5)).unwrap();

        let mut stream = proxy_dialer
            .dial("127.0.0.1", target_addr.port())
            .await
            .unwrap();
        stream.write_all(b"ping").await.unwrap();
        stream.flush().await.unwrap();

        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");
    }

    #[tokio::test]
    async fn test_http_connect_rejected() {
        let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut client_sock, _) = proxy_listener.accept().await.unwrap();
            let mut req = Vec::new();
            let mut byte = [0u8; 1];
            while req.len() < 1024 {
                client_sock.read_exact(&mut byte).await.unwrap();
                req.push(byte[0]);
                if req.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            client_sock
                .write_all(b"HTTP/1.1 403 Forbidden\r\n\r\n")
                .await
                .unwrap();
        });

        let proxy_config = ProxyConfig::parse(&format!("http://{}", proxy_addr)).unwrap();
        let default_dialer = Arc::new(crate::dialer::unified::DefaultDialer::new(
            crate::config::model::DialerConfig::default(),
            Arc::new(
                crate::dialer::dns::CachedDnsResolver::try_new(
                    crate::config::model::DnsConfig::default(),
                )
                .unwrap(),
            ),
        ));
        let proxy_dialer =
            ProxyDialer::try_new(default_dialer, proxy_config, Duration::from_secs(5)).unwrap();

        match proxy_dialer.dial("example.com", 80).await {
            Err(e) => assert!(e.to_string().contains("403")),
            Ok(_) => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_socks5_tunnel_roundtrip() {
        let target_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = target_listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            socket.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
            socket.write_all(b"pong").await.unwrap();
        });

        let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut client_sock, _) = proxy_listener.accept().await.unwrap();
            // Method selection
            let mut greeting = [0u8; 3];
            client_sock.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting[0], 0x05);
            client_sock.write_all(&[0x05, 0x00]).await.unwrap();

            // Connect request
            let mut req_header = [0u8; 4];
            client_sock.read_exact(&mut req_header).await.unwrap();
            assert_eq!(req_header[0], 0x05);
            assert_eq!(req_header[1], 0x01); // CONNECT
            assert_eq!(req_header[3], 0x03); // ATYP=Domain

            let mut len_buf = [0u8; 1];
            client_sock.read_exact(&mut len_buf).await.unwrap();
            let mut domain = vec![0u8; len_buf[0] as usize];
            client_sock.read_exact(&mut domain).await.unwrap();
            assert_eq!(String::from_utf8_lossy(&domain), "example.internal");

            let mut port_buf = [0u8; 2];
            client_sock.read_exact(&mut port_buf).await.unwrap();
            assert_eq!(u16::from_be_bytes(port_buf), target_addr.port());

            // Success response
            client_sock
                .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
                .await
                .unwrap();

            let mut target_sock = tokio::net::TcpStream::connect(target_addr).await.unwrap();
            let _ = tokio::io::copy_bidirectional(&mut client_sock, &mut target_sock).await;
        });

        let proxy_config = ProxyConfig::parse(&format!("socks5://{}", proxy_addr)).unwrap();
        let default_dialer = Arc::new(crate::dialer::unified::DefaultDialer::new(
            crate::config::model::DialerConfig::default(),
            Arc::new(
                crate::dialer::dns::CachedDnsResolver::try_new(
                    crate::config::model::DnsConfig::default(),
                )
                .unwrap(),
            ),
        ));
        let proxy_dialer =
            ProxyDialer::try_new(default_dialer, proxy_config, Duration::from_secs(5)).unwrap();

        let mut stream = proxy_dialer
            .dial("example.internal", target_addr.port())
            .await
            .unwrap();
        stream.write_all(b"ping").await.unwrap();
        stream.flush().await.unwrap();

        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");
    }

    #[tokio::test]
    async fn test_socks5_with_auth() {
        let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut client_sock, _) = proxy_listener.accept().await.unwrap();
            let mut greeting = [0u8; 4];
            client_sock.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting[0], 0x05);
            // Require USER/PASS
            client_sock.write_all(&[0x05, 0x02]).await.unwrap();

            // Read auth request: [0x01, ulen, uname, plen, pass]
            let mut ver = [0u8; 1];
            client_sock.read_exact(&mut ver).await.unwrap();
            assert_eq!(ver[0], 0x01);
            let mut ulen = [0u8; 1];
            client_sock.read_exact(&mut ulen).await.unwrap();
            let mut uname = vec![0u8; ulen[0] as usize];
            client_sock.read_exact(&mut uname).await.unwrap();
            let mut plen = [0u8; 1];
            client_sock.read_exact(&mut plen).await.unwrap();
            let mut pass = vec![0u8; plen[0] as usize];
            client_sock.read_exact(&mut pass).await.unwrap();

            assert_eq!(&uname, b"admin");
            assert_eq!(&pass, b"pass123");

            // Success auth
            client_sock.write_all(&[0x01, 0x00]).await.unwrap();

            // Connect req
            let mut req = [0u8; 10];
            client_sock.read_exact(&mut req).await.unwrap();
            client_sock
                .write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
        });

        let proxy_config =
            ProxyConfig::parse(&format!("socks5://admin:pass123@{}", proxy_addr)).unwrap();
        let default_dialer = Arc::new(crate::dialer::unified::DefaultDialer::new(
            crate::config::model::DialerConfig::default(),
            Arc::new(
                crate::dialer::dns::CachedDnsResolver::try_new(
                    crate::config::model::DnsConfig::default(),
                )
                .unwrap(),
            ),
        ));
        let proxy_dialer =
            ProxyDialer::try_new(default_dialer, proxy_config, Duration::from_secs(5)).unwrap();

        let res = proxy_dialer.dial("127.0.0.1", 8080).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_socks5_auth_rejected() {
        let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut client_sock, _) = proxy_listener.accept().await.unwrap();
            let mut greeting = [0u8; 4];
            client_sock.read_exact(&mut greeting).await.unwrap();
            // Tell client 0xFF: no acceptable method
            client_sock.write_all(&[0x05, 0xFF]).await.unwrap();
        });

        let proxy_config =
            ProxyConfig::parse(&format!("socks5://user:wrong@{}", proxy_addr)).unwrap();
        let default_dialer = Arc::new(crate::dialer::unified::DefaultDialer::new(
            crate::config::model::DialerConfig::default(),
            Arc::new(
                crate::dialer::dns::CachedDnsResolver::try_new(
                    crate::config::model::DnsConfig::default(),
                )
                .unwrap(),
            ),
        ));
        let proxy_dialer =
            ProxyDialer::try_new(default_dialer, proxy_config, Duration::from_secs(5)).unwrap();

        match proxy_dialer.dial("127.0.0.1", 8080).await {
            Err(e) => assert!(e.to_string().contains("no acceptable methods")),
            Ok(_) => panic!("expected error"),
        }
    }
}
