//! Reverse tunnel worker (`-R` mode).
//! All comments and docstrings must be in English.

use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::model::TunnelConfig;
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::transport::TransportRegistry;

/// Runs a reverse tunnel worker that requests a remote listener and streams traffic back locally.
pub async fn run_reverse_tunnel(
    config: TunnelConfig,
    _endpoint_ctx: EndpointContext,
    _transport_registry: Arc<TransportRegistry>,
    cancel_token: CancellationToken,
) -> Result<(), TunnelError> {
    info!(
        tag = %config.tag,
        listen = %config.listen,
        target = %config.target,
        "Starting reverse tunnel control loop"
    );

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!(tag = %config.tag, "Reverse tunnel received cancellation signal");
                break;
            }
            // Control loop will await incoming connection alerts from the remote server
        }
    }

    Ok(())
}
