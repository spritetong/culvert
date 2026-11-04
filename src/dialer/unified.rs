//! Unified dialer integrating DNS strategy, socket tuning, and connection timeouts.
//! All comments and docstrings must be in English.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::common::net::SocketTuner;
use crate::config::model::DialerConfig;
use crate::consts::DEFAULT_CONNECT_TIMEOUT;
use crate::core::error::TunnelError;
use crate::core::stream::{BoxFuture, TunnelStream};
use crate::dialer::dns::DnsResolver;

/// Interface for establishing outbound raw TCP connections.
pub trait Dialer: Send + Sync {
    /// Connects to the target host and port using the configured DNS and socket parameters.
    fn dial<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>>;
}

/// Default dialer implementation.
pub struct DefaultDialer {
    config: DialerConfig,
    dns_resolver: Arc<dyn DnsResolver>,
}

impl DefaultDialer {
    /// Creates a new `DefaultDialer` instance.
    pub fn new(config: DialerConfig, dns_resolver: Arc<dyn DnsResolver>) -> Self {
        Self {
            config,
            dns_resolver,
        }
    }
}

impl Dialer for DefaultDialer {
    fn dial<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> BoxFuture<'a, Result<Box<dyn TunnelStream>, TunnelError>> {
        Box::pin(async move {
            if let Some(ref iface) = self.config.bind_interface {
                #[cfg(not(target_os = "linux"))]
                {
                    return Err(TunnelError::Config(format!(
                        "bind_interface '{}' is only supported on Linux (SO_BINDTODEVICE)",
                        iface
                    )));
                }
                #[cfg(target_os = "linux")]
                let _ = iface;
            }

            let ips = self.dns_resolver.resolve(host).await?;
            if ips.is_empty() {
                return Err(TunnelError::Internal(format!(
                    "No IP addresses resolved for {}:{}",
                    host, port
                )));
            }

            let connect_timeout = self
                .config
                .connect_timeout
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT);

            let nodelay = self.config.tcp_nodelay.unwrap_or(true);
            let tcp_keepalive = self.config.tcp_keepalive;

            if ips.len() == 1 {
                let addr = SocketAddr::new(ips[0], port);
                let stream = match timeout(connect_timeout, TcpStream::connect(addr)).await {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => return Err(TunnelError::Io(e)),
                    Err(_) => return Err(TunnelError::ConnectTimeout(connect_timeout)),
                };
                SocketTuner::tune_tcp_stream(&stream, nodelay, tcp_keepalive)
                    .map_err(TunnelError::Io)?;
                return Ok(Box::new(stream) as Box<dyn TunnelStream>);
            }

            // Multiple IPs: race connection attempts concurrently (Happy Eyeballs / RFC 8305)
            let futs = ips.into_iter().map(|ip| {
                let addr = SocketAddr::new(ip, port);
                Box::pin(async move {
                    match timeout(connect_timeout, TcpStream::connect(addr)).await {
                        Ok(Ok(stream)) => Ok(stream),
                        Ok(Err(e)) => Err(TunnelError::Io(e)),
                        Err(_) => Err(TunnelError::ConnectTimeout(connect_timeout)),
                    }
                })
            });

            match futures_util::future::select_ok(futs).await {
                Ok((stream, _remaining)) => {
                    SocketTuner::tune_tcp_stream(&stream, nodelay, tcp_keepalive)
                        .map_err(TunnelError::Io)?;
                    Ok(Box::new(stream) as Box<dyn TunnelStream>)
                }
                Err(err) => Err(err),
            }
        })
    }
}
