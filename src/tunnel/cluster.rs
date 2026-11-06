// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Multi-mapping tunnel cluster runner sharing a common connection pool.
//! All comments and docstrings must be in English.

use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::common::task::TaskTracker;
use crate::config::model::{PortMappingConfig, TunnelConfig, TunnelMode};
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::pool::ReplenishingStreamPool;
use crate::transport::{TransportConnector, TransportRegistry};
use crate::tunnel::forward::run_forward_mapping;
use crate::tunnel::reverse::run_reverse_mapping;

/// Runs all configured port mappings for a single tunnel cluster, sharing the same endpoint and pool.
pub async fn run_tunnel_cluster(
    config: TunnelConfig,
    endpoint_ctx: EndpointContext,
    transport_registry: Arc<TransportRegistry>,
    cancel_token: CancellationToken,
) -> Result<(), TunnelError> {
    let mappings = config.canonical_mappings()?;
    info!(
        tag = %config.tag,
        mapping_count = mappings.len(),
        "Starting tunnel cluster mappings"
    );

    // Shared connection pool for all forward mappings under this tunnel
    let pool = if let Some(size) = config.pool_size.filter(|&s| s > 0) {
        let connector: Arc<dyn TransportConnector> =
            Arc::from(transport_registry.create_connector(
                &endpoint_ctx,
                config.token.as_deref(),
                TunnelMode::Forward,
            )?);
        Some(Arc::new(ReplenishingStreamPool::new_shared(
            connector,
            size,
            cancel_token.clone(),
        )))
    } else {
        None
    };

    let mut tracker = TaskTracker::<Result<(), TunnelError>>::new(cancel_token.clone());

    for (idx, mapping) in mappings.into_iter().enumerate() {
        let mapping_tag = format!("{}-map{}", config.tag, idx + 1);
        let PortMappingConfig {
            mode,
            listen,
            target,
        } = mapping;

        let registry = Arc::clone(&transport_registry);
        let endpoint = endpoint_ctx.clone();
        let token = config.token.clone();
        let pool_clone = pool.clone();

        tracker.spawn_with_cancel(move |token_cancel| async move {
            match mode {
                TunnelMode::Forward => {
                    run_forward_mapping(
                        mapping_tag,
                        listen,
                        target,
                        token,
                        endpoint,
                        registry,
                        pool_clone,
                        token_cancel,
                    )
                    .await
                }
                TunnelMode::Reverse => {
                    run_reverse_mapping(
                        mapping_tag,
                        listen,
                        target,
                        token,
                        endpoint,
                        registry,
                        token_cancel,
                    )
                    .await
                }
            }
        });
    }

    let mut exit_err = None;
    tokio::select! {
        biased;
        _ = cancel_token.cancelled() => {
            info!(tag = %config.tag, "Tunnel cluster received cancellation signal");
        }
        res = tracker.join_next() => {
            if let Some(Ok(Err(e))) = res {
                error!(tag = %config.tag, "A port mapping in cluster failed: {:?}", e);
                exit_err = Some(e);
            }
        }
    }

    tracker.shutdown_and_wait().await;
    if let Some(e) = exit_err {
        return Err(e);
    }
    Ok(())
}
