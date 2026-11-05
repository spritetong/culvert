// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Unified dialer integrating DNS strategy, socket tuning, and connection timeouts.
//! All comments and docstrings must be in English.

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
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

            let iface_opt = self.config.bind_interface.as_deref();

            let connect_timeout = self
                .config
                .connect_timeout
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT);

            let nodelay = self.config.tcp_nodelay.unwrap_or(true);
            let tcp_keepalive = self.config.tcp_keepalive;

            async fn try_connect(
                addr: SocketAddr,
                timeout_dur: std::time::Duration,
                bind_iface: Option<&str>,
            ) -> Result<TcpStream, TunnelError> {
                let connect_fut = async {
                    #[cfg(target_os = "linux")]
                    if let Some(iface) = bind_iface {
                        let socket = if addr.is_ipv6() {
                            tokio::net::TcpSocket::new_v6().map_err(TunnelError::Io)?
                        } else {
                            tokio::net::TcpSocket::new_v4().map_err(TunnelError::Io)?
                        };
                        let sock_ref = socket2::SockRef::from(&socket);
                        sock_ref
                            .bind_device(Some(iface.as_bytes()))
                            .map_err(TunnelError::Io)?;
                        return socket.connect(addr).await.map_err(TunnelError::Io);
                    }
                    #[cfg(not(target_os = "linux"))]
                    if let Some(iface) = bind_iface {
                        return Err(TunnelError::Config(format!(
                            "bind_interface '{}' is only supported on Linux (SO_BINDTODEVICE)",
                            iface
                        )));
                    }
                    TcpStream::connect(addr).await.map_err(TunnelError::Io)
                };

                match timeout(timeout_dur, connect_fut).await {
                    Ok(res) => res,
                    Err(_) => Err(TunnelError::ConnectTimeout(timeout_dur)),
                }
            }

            if ips.len() == 1 {
                let addr = SocketAddr::new(ips[0], port);
                let stream = try_connect(addr, connect_timeout, iface_opt).await?;
                SocketTuner::tune_tcp_stream(&stream, nodelay, tcp_keepalive)
                    .map_err(TunnelError::Io)?;
                return Ok(Box::new(stream) as Box<dyn TunnelStream>);
            }

            // Multiple IPs: race connection attempts with RFC 8305 250ms staggering and IPv6 priority
            let mut ordered_ips = ips;
            ordered_ips.sort_by_key(|ip| if ip.is_ipv6() { 0 } else { 1 });

            let mut futs = FuturesUnordered::new();
            let first_addr = SocketAddr::new(ordered_ips[0], port);
            futs.push(Box::pin(try_connect(first_addr, connect_timeout, iface_opt)));

            let mut next_idx = 1;
            let delay = tokio::time::sleep(std::time::Duration::from_millis(250));
            tokio::pin!(delay);

            let mut last_err = None;
            while !futs.is_empty() || next_idx < ordered_ips.len() {
                tokio::select! {
                    Some(res) = futs.next(), if !futs.is_empty() => {
                        match res {
                            Ok(stream) => {
                                SocketTuner::tune_tcp_stream(&stream, nodelay, tcp_keepalive)
                                    .map_err(TunnelError::Io)?;
                                return Ok(Box::new(stream) as Box<dyn TunnelStream>);
                            }
                            Err(e) => {
                                last_err = Some(e);
                            }
                        }
                    }
                    _ = &mut delay, if next_idx < ordered_ips.len() => {
                        let next_addr = SocketAddr::new(ordered_ips[next_idx], port);
                        next_idx += 1;
                        futs.push(Box::pin(try_connect(next_addr, connect_timeout, iface_opt)));
                        delay.as_mut().reset(tokio::time::Instant::now() + std::time::Duration::from_millis(250));
                    }
                }
            }

            Err(last_err.unwrap_or(TunnelError::ConnectTimeout(connect_timeout)))
        })
    }
}
