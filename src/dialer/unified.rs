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
            let ips = self.dns_resolver.resolve(host).await?;
            let connect_timeout = self
                .config
                .connect_timeout
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT);

            let mut last_error = None;
            for ip in ips {
                let addr = SocketAddr::new(ip, port);
                let connect_fut = TcpStream::connect(addr);

                match timeout(connect_timeout, connect_fut).await {
                    Ok(Ok(stream)) => {
                        let nodelay = self.config.tcp_nodelay.unwrap_or(true);
                        SocketTuner::tune_tcp_stream(&stream, nodelay, self.config.tcp_keepalive)
                            .map_err(TunnelError::Io)?;
                        return Ok(Box::new(stream) as Box<dyn TunnelStream>);
                    }
                    Ok(Err(e)) => {
                        last_error = Some(TunnelError::Io(e));
                    }
                    Err(_) => {
                        last_error = Some(TunnelError::ConnectTimeout(connect_timeout));
                    }
                }
            }

            Err(last_error.unwrap_or_else(|| {
                TunnelError::Internal(format!(
                    "Failed to connect to any resolved IP for {}:{}",
                    host, port
                ))
            }))
        })
    }
}
