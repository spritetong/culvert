// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! `ClientRunner` and `TunnelSupervisor` managing full client/server topologies
//! with retry supervision and coordinated cancellation.
//! All comments and docstrings must be in English.

use crate::common::CancellationTokenExt;
use crate::config::model::{AppConfig, TunnelConfig};
use crate::consts::DEFAULT_RETRY_DELAY;
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::server::{ReverseSessionHub, ServerRunner};
use crate::transport::TransportRegistry;
use crate::tunnel::cluster::TunnelCluster;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Supervised client runner executing one or more `TunnelConfig` clusters with retry backoff.
pub struct ClientRunner {
    tunnels: Vec<(TunnelConfig, EndpointContext)>,
    transport_registry: Arc<TransportRegistry>,
    client_uuid: String,
}

impl ClientRunner {
    /// Creates a new `ClientRunner` for a single tunnel configuration.
    pub fn new(tunnel: TunnelConfig) -> Result<Self, TunnelError> {
        Self::from_tunnels(vec![tunnel])
    }

    /// Creates a new `ClientRunner` for multiple tunnel configurations.
    pub fn from_tunnels(tunnels: Vec<TunnelConfig>) -> Result<Self, TunnelError> {
        let app_config = AppConfig {
            tunnels,
            ..Default::default()
        };
        Self::from_app_config(&app_config)
    }

    /// Creates a new `ClientRunner` from `AppConfig`.
    pub fn from_app_config(app_config: &AppConfig) -> Result<Self, TunnelError> {
        Self::with_registry(app_config, Arc::new(TransportRegistry::default()))
    }

    /// Creates a new `ClientRunner` with an explicit `TransportRegistry`.
    pub fn with_registry(
        app_config: &AppConfig,
        transport_registry: Arc<TransportRegistry>,
    ) -> Result<Self, TunnelError> {
        if app_config.tunnels.is_empty() {
            return Err(TunnelError::Config(
                "No client tunnels configured.".to_string(),
            ));
        }

        let mut tunnels = Vec::with_capacity(app_config.tunnels.len());
        for tunnel_cfg in &app_config.tunnels {
            let endpoint_ctx = EndpointContext::build_for_tunnel(tunnel_cfg, app_config)?;
            tunnels.push((tunnel_cfg.clone(), endpoint_ctx));
        }

        Ok(Self {
            tunnels,
            transport_registry,
            client_uuid: Uuid::new_v4().to_string(),
        })
    }

    /// Runs all configured client tunnels with retry supervision until `cancel_token` cancels.
    pub async fn run(&self, cancel_token: CancellationToken) -> Result<(), TunnelError> {
        let mut workers: JoinSet<Result<(), TunnelError>> = JoinSet::new();
        let child_cancel = cancel_token.child_token();

        for (cfg, endpoint_ctx) in &self.tunnels {
            let config = cfg.clone();
            let ctx = endpoint_ctx.clone();
            let registry = Arc::clone(&self.transport_registry);
            let client_uuid = self.client_uuid.clone();
            let tok = child_cancel.child_token();

            workers.spawn(async move {
                Self::run_tunnel_with_retry(config, ctx, registry, tok, &client_uuid).await
            });
        }

        Self::supervise_workers(workers, cancel_token, child_cancel).await
    }

    /// Supervised retry loop for one `TunnelConfig` cluster.
    pub async fn run_tunnel_with_retry(
        config: TunnelConfig,
        endpoint_ctx: EndpointContext,
        registry: Arc<TransportRegistry>,
        token: CancellationToken,
        client_uuid: &str,
    ) -> Result<(), TunnelError> {
        let retry_cfg = config.retry.clone().unwrap_or_default();
        let retry_delay = retry_cfg.delay.unwrap_or(DEFAULT_RETRY_DELAY);
        let max_retries = retry_cfg.max_retries;
        let mut attempts = 0usize;

        loop {
            if token.is_cancelled() {
                break;
            }

            match TunnelCluster::run(
                config.clone(),
                endpoint_ctx.clone(),
                Arc::clone(&registry),
                token.clone(),
                client_uuid,
            )
            .await
            {
                Ok(()) | Err(TunnelError::Shutdown) => break,
                Err(_) if token.is_cancelled() => break,
                Err(err) => {
                    if err.is_permanent() {
                        return Err(err);
                    }
                    attempts += 1;
                    if let Some(max) = max_retries
                        && attempts >= max
                    {
                        return Err(err);
                    }
                    if !token.sleep_or_cancel(retry_delay).await {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn supervise_workers(
        mut workers: JoinSet<Result<(), TunnelError>>,
        cancel_token: CancellationToken,
        child_cancel: CancellationToken,
    ) -> Result<(), TunnelError> {
        let mut exit_err: Option<TunnelError> = None;
        loop {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                res = workers.join_next(), if !workers.is_empty() => {
                    match res {
                        Some(Ok(Err(TunnelError::Shutdown))) => {
                            if workers.is_empty() {
                                break;
                            }
                        }
                        Some(Ok(Err(err))) => {
                            if cancel_token.is_cancelled() {
                                if workers.is_empty() {
                                    break;
                                }
                                continue;
                            }
                            if exit_err.is_none() {
                                exit_err = Some(err);
                            }
                            cancel_token.cancel();
                            break;
                        }
                        Some(Err(join_err)) => {
                            if !cancel_token.is_cancelled() && exit_err.is_none() {
                                exit_err = Some(TunnelError::Internal(format!(
                                    "worker task terminated: {join_err}"
                                )));
                                cancel_token.cancel();
                            }
                            break;
                        }
                        Some(Ok(Ok(()))) => {
                            if workers.is_empty() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        child_cancel.cancel();
        while let Some(res) = workers.join_next().await {
            if let Ok(Err(err)) = res
                && !matches!(err, TunnelError::Shutdown)
                && !cancel_token.is_cancelled()
                && exit_err.is_none()
            {
                exit_err = Some(err);
            }
        }

        if let Some(err) = exit_err
            && !matches!(err, TunnelError::Shutdown)
        {
            return Err(err);
        }
        Ok(())
    }
}

/// Top-level supervisor coordinating server ingress and all client tunnel clusters.
pub struct TunnelSupervisor {
    config: AppConfig,
    cancel_token: CancellationToken,
    transport_registry: Arc<TransportRegistry>,
    hub: Arc<ReverseSessionHub>,
    client_uuid: String,
}

impl TunnelSupervisor {
    /// Creates a new `TunnelSupervisor` with default registries.
    pub fn new(config: AppConfig, cancel_token: CancellationToken) -> Self {
        Self::with_registry(config, cancel_token, Arc::new(TransportRegistry::default()))
    }

    /// Creates a new `TunnelSupervisor` using an explicit `TransportRegistry`.
    pub fn with_registry(
        config: AppConfig,
        cancel_token: CancellationToken,
        transport_registry: Arc<TransportRegistry>,
    ) -> Self {
        Self {
            config,
            cancel_token,
            transport_registry,
            hub: Arc::new(ReverseSessionHub::new()),
            client_uuid: Uuid::new_v4().to_string(),
        }
    }

    /// Starts server and client workers and runs until `cancel_token` cancels.
    pub async fn run(&self) -> Result<(), TunnelError> {
        let has_server = self
            .config
            .server
            .as_ref()
            .is_some_and(|s| !s.listen.is_empty());
        let has_tunnels = !self.config.tunnels.is_empty();
        if !has_server && !has_tunnels {
            return Err(TunnelError::Config(
                "No server listeners and no tunnels are configured. Configuration must not be empty."
                    .to_string(),
            ));
        }

        let mut workers: JoinSet<Result<(), TunnelError>> = JoinSet::new();
        let child_cancel = self.cancel_token.child_token();

        if let Some(ref server_cfg) = self.config.server {
            let server_runner = ServerRunner::with_quic_config(
                server_cfg.clone(),
                self.config.tls.clone(),
                self.config.quic.clone(),
                Arc::clone(&self.transport_registry),
                Arc::clone(&self.hub),
            );
            let tok = child_cancel.child_token();
            workers.spawn(async move { server_runner.run(tok).await });
        }

        for tunnel_cfg in &self.config.tunnels {
            let cfg = tunnel_cfg.clone();
            let registry = Arc::clone(&self.transport_registry);
            let endpoint_ctx = EndpointContext::build_for_tunnel(&cfg, &self.config)?;
            let client_uuid = self.client_uuid.clone();
            let tok = child_cancel.child_token();

            workers.spawn(async move {
                ClientRunner::run_tunnel_with_retry(cfg, endpoint_ctx, registry, tok, &client_uuid)
                    .await
            });
        }

        ClientRunner::supervise_workers(workers, self.cancel_token.clone(), child_cancel).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{
        EndpointConfig, MappingGroupConfig, MappingRuleConfig, ServerConfig, TunnelMappingsConfig,
    };
    use crate::io::addr::L4Protocol;
    use crate::transport::ws::WsTransportDriver;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn tunnel_supervisor_multi_mapping_forward_and_reverse_e2e() {
        let probe_echo = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let echo_port = probe_echo.local_addr().unwrap().port();
        probe_echo.set_nonblocking(true).unwrap();
        let echo_listener = tokio::net::TcpListener::from_std(probe_echo).unwrap();

        let probe_ws = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let ws_port = probe_ws.local_addr().unwrap().port();
        drop(probe_ws);

        let probe_fwd1 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let fwd1_port = probe_fwd1.local_addr().unwrap().port();
        drop(probe_fwd1);

        let probe_fwd2 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let fwd2_port = probe_fwd2.local_addr().unwrap().port();
        drop(probe_fwd2);

        let probe_rev = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let rev_port = probe_rev.local_addr().unwrap().port();
        drop(probe_rev);

        let sup_cancel = CancellationToken::new();
        let echo_cancel = sup_cancel.clone();
        let echo_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = echo_cancel.cancelled() => break,
                    res = echo_listener.accept() => {
                        let Ok((mut sock, _)) = res else { break; };
                        tokio::spawn(async move {
                            let (mut rd, mut wr) = sock.split();
                            let _ = tokio::io::copy(&mut rd, &mut wr).await;
                        });
                    }
                }
            }
        });

        let ws_addr = format!("127.0.0.1:{ws_port}");
        let echo_addr = format!("127.0.0.1:{echo_port}");
        let fwd1_addr = format!("127.0.0.1:{fwd1_port}");
        let fwd2_addr = format!("127.0.0.1:{fwd2_port}");
        let rev_addr = format!("127.0.0.1:{rev_port}");

        let app_config = AppConfig {
            server: Some(ServerConfig {
                listen: vec![format!("ws://{ws_addr}/tunnel")],
                token: Some("sup-token".to_string()),
                ..Default::default()
            }),
            tunnels: vec![TunnelConfig {
                tag: "multi-map".to_string(),
                endpoint: EndpointConfig::Simple(format!("ws://{ws_addr}/tunnel")),
                token: Some("sup-token".to_string()),
                uuid: None,
                retry: None,
                failover: None,
                http_headers: None,
                multiplex: None,
                mappings: TunnelMappingsConfig {
                    forward: Some(MappingGroupConfig {
                        pool_size: Some(2),
                        rules: vec![
                            MappingRuleConfig {
                                protocol: L4Protocol::Tcp,
                                listen: fwd1_addr.clone(),
                                target: echo_addr.clone(),
                                udp_idle_timeout: None,
                            },
                            MappingRuleConfig {
                                protocol: L4Protocol::Tcp,
                                listen: fwd2_addr.clone(),
                                target: echo_addr.clone(),
                                udp_idle_timeout: None,
                            },
                        ],
                    }),
                    reverse: Some(MappingGroupConfig {
                        pool_size: Some(2),
                        rules: vec![MappingRuleConfig {
                            protocol: L4Protocol::Tcp,
                            listen: rev_addr.clone(),
                            target: echo_addr,
                            udp_idle_timeout: None,
                        }],
                    }),
                },
            }],
            ..Default::default()
        };

        let mut registry = TransportRegistry::new();
        registry.register(WsTransportDriver);
        let sup =
            TunnelSupervisor::with_registry(app_config, sup_cancel.clone(), Arc::new(registry));
        let sup_handle = tokio::spawn(async move { sup.run().await });

        // Wait until forward and reverse ports are bound cleanly.
        let mut s1 = None;
        for _ in 0..40 {
            if let Ok(s) = tokio::net::TcpStream::connect(&fwd1_addr).await {
                s1 = Some(s);
                break;
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        let mut s1 = s1.expect("fwd1 must bind");
        s1.write_all(b"cluster-fwd1").await.unwrap();
        s1.shutdown().await.unwrap();
        let mut buf = [0u8; 12];
        tokio::time::timeout(Duration::from_secs(5), s1.read_exact(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf, b"cluster-fwd1");

        let mut s2 = tokio::net::TcpStream::connect(&fwd2_addr).await.unwrap();
        s2.write_all(b"cluster-fwd2").await.unwrap();
        s2.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), s2.read_exact(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf, b"cluster-fwd2");

        let mut rev = None;
        for _ in 0..40 {
            if let Ok(s) = tokio::net::TcpStream::connect(&rev_addr).await {
                rev = Some(s);
                break;
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        let mut rev = rev.expect("reverse cluster port must bind");
        tokio::time::sleep(Duration::from_millis(80)).await;
        rev.write_all(b"cluster-rev1").await.unwrap();
        rev.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), rev.read_exact(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf, b"cluster-rev1");

        sup_cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), sup_handle)
            .await
            .expect("TunnelSupervisor must exit cleanly")
            .unwrap()
            .unwrap();
        let _ = echo_handle.await;
    }
}
