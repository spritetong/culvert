// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Decoupled signaling channel actor and handles for reverse tunnel control.
//! All comments and docstrings must be in English.

use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, interval, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::common::CancelFutureExt;
use crate::consts::{DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_IDLE_TIMEOUT, reverse_frame_timeout};
use crate::control::stream::{ControlMessage, FramedControlStream};
use crate::core::error::TunnelError;
use crate::io::stream::{activity_now_ms, heartbeat_timing};

/// Configuration parameters for a signaling session.
#[derive(Debug, Clone, Copy)]
pub struct SignalingConfig {
    pub heartbeat_interval: Duration,
    pub idle_timeout: Duration,
}

impl Default for SignalingConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
        }
    }
}

struct OutboundCommand {
    msg: ControlMessage,
    tx: oneshot::Sender<Result<(), TunnelError>>,
    timeout: Duration,
}

/// Cloned handle used by business logic to transmit control commands asynchronously.
#[derive(Clone)]
pub struct SignalingHandle {
    cmd_tx: mpsc::Sender<OutboundCommand>,
    default_frame_timeout: Duration,
}

impl SignalingHandle {
    /// Transmits a control message using the session's derived frame timeout.
    pub async fn send(&self, msg: ControlMessage) -> Result<(), TunnelError> {
        self.send_timeout(msg, self.default_frame_timeout).await
    }

    /// Transmits a control message with an explicit bounded timeout.
    pub async fn send_timeout(
        &self,
        msg: ControlMessage,
        send_timeout: Duration,
    ) -> Result<(), TunnelError> {
        let (tx, rx) = oneshot::channel();
        let cmd = OutboundCommand {
            msg,
            tx,
            timeout: send_timeout,
        };
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| TunnelError::ControlProtocol("Signaling channel closed".into()))?;

        rx.await
            .map_err(|_| TunnelError::ControlProtocol("Signaling response dropped".into()))?
    }
}

/// Receiver yielding incoming application-level control messages.
pub struct SignalingReceiver {
    rx: mpsc::Receiver<Result<ControlMessage, TunnelError>>,
}

impl SignalingReceiver {
    /// Awaits the next application-level control message.
    ///
    /// Heartbeat frames (`Ping` and `Pong`) are answered and accounted for
    /// automatically by the signaling actor and are never delivered here.
    pub async fn recv(&mut self) -> Option<Result<ControlMessage, TunnelError>> {
        self.rx.recv().await
    }

    /// Convenience helper yielding `Ok(Some(msg))`, `Ok(None)` on EOF/clean close, or `Err(e)`.
    pub async fn recv_msg(&mut self) -> Result<Option<ControlMessage>, TunnelError> {
        match self.rx.recv().await {
            Some(Ok(msg)) => Ok(Some(msg)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

/// Spawns an autonomous signaling actor managing transport framing, keepalive, and timeouts.
pub struct SignalingChannel;

impl SignalingChannel {
    /// Spawns the signaling actor over `stream` and returns its command handle and event receiver.
    pub fn spawn<S>(
        stream: S,
        config: SignalingConfig,
        cancel_token: CancellationToken,
    ) -> (SignalingHandle, SignalingReceiver)
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let frame_timeout = reverse_frame_timeout(config.idle_timeout);
        let (cmd_tx, cmd_rx) = mpsc::channel::<OutboundCommand>(32);
        let (event_tx, event_rx) = mpsc::channel::<Result<ControlMessage, TunnelError>>(32);

        let handle = SignalingHandle {
            cmd_tx,
            default_frame_timeout: frame_timeout,
        };
        let receiver = SignalingReceiver { rx: event_rx };

        tokio::spawn(Self::run_actor(
            stream,
            config,
            frame_timeout,
            cmd_rx,
            event_tx,
            cancel_token,
        ));

        (handle, receiver)
    }

    async fn run_actor<S>(
        stream: S,
        config: SignalingConfig,
        frame_timeout: Duration,
        mut cmd_rx: mpsc::Receiver<OutboundCommand>,
        event_tx: mpsc::Sender<Result<ControlMessage, TunnelError>>,
        cancel_token: CancellationToken,
    ) where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let mut ctrl_stream = FramedControlStream::new(stream);
        let mut heartbeat = interval(config.heartbeat_interval);
        let mut last_activity_ms = activity_now_ms();
        let idle_deadline = sleep(config.idle_timeout);
        tokio::pin!(idle_deadline);

        loop {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    debug!("Signaling actor stopping on cancellation signal");
                    break;
                }
                _ = &mut idle_deadline => {
                    warn!(
                        idle_timeout = ?config.idle_timeout,
                        "Signaling channel idle timeout: no activity. Closing channel."
                    );
                    break;
                }
                cmd_opt = cmd_rx.recv() => {
                    match cmd_opt {
                        Some(cmd) => {
                            let send_res = timeout(cmd.timeout, ctrl_stream.send(cmd.msg))
                                .cancel_with(&cancel_token)
                                .await;
                            match send_res {
                                Some(Ok(Ok(()))) => {
                                    last_activity_ms = activity_now_ms();
                                    idle_deadline.as_mut().reset(Instant::now() + config.idle_timeout);
                                    let _ = cmd.tx.send(Ok(()));
                                }
                                Some(Ok(Err(e))) => {
                                    let _ = cmd.tx.send(Err(e));
                                    break;
                                }
                                Some(Err(_)) => {
                                    let err_msg = format!("Send timed out after {:?}", cmd.timeout);
                                    let _ = cmd.tx.send(Err(TunnelError::ControlProtocol(err_msg)));
                                    break;
                                }
                                None => break,
                            }
                        }
                        None => {
                            debug!("Signaling handle dropped; closing signaling actor");
                            break;
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    let (send_probe, _) = heartbeat_timing(
                        Some(last_activity_ms),
                        activity_now_ms(),
                        config.heartbeat_interval,
                        config.idle_timeout,
                    );
                    if send_probe {
                        let ping = timeout(frame_timeout, ctrl_stream.send(ControlMessage::Ping))
                            .cancel_with(&cancel_token)
                            .await;
                        match ping {
                            Some(Ok(Ok(()))) => {}
                            Some(Ok(Err(e))) => {
                                debug!("Signaling actor failed to send heartbeat ping: {}", e);
                                break;
                            }
                            Some(Err(_)) => {
                                warn!("Signaling actor heartbeat ping stalled; closing channel");
                                break;
                            }
                            None => break,
                        }
                    }
                }
                msg_opt = ctrl_stream.next() => {
                    match msg_opt {
                        Some(Ok(msg)) => {
                            last_activity_ms = activity_now_ms();
                            idle_deadline.as_mut().reset(Instant::now() + config.idle_timeout);
                            match msg {
                                ControlMessage::Ping => {
                                    let pong = timeout(frame_timeout, ctrl_stream.send(ControlMessage::Pong))
                                        .cancel_with(&cancel_token)
                                        .await;
                                    if !matches!(pong, Some(Ok(Ok(())))) {
                                        debug!("Signaling actor Pong stalled; closing channel");
                                        break;
                                    }
                                }
                                ControlMessage::Pong => {
                                    // Activity clock already refreshed above
                                }
                                app_msg => {
                                    if event_tx.send(Ok(app_msg)).await.is_err() {
                                        debug!("Signaling event receiver dropped; closing actor");
                                        break;
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            let _ = event_tx.send(Err(e)).await;
                            break;
                        }
                        None => {
                            debug!("Signaling transport disconnected by peer");
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_signaling_channel_autonomous_ping_pong() {
        let (client_raw, server_raw) = duplex(4096);
        let cancel = CancellationToken::new();

        let cfg = SignalingConfig {
            heartbeat_interval: Duration::from_millis(50),
            idle_timeout: Duration::from_millis(500),
        };

        // Spawn server as autonomous signaling channel
        let (_srv_handle, mut srv_rx) =
            SignalingChannel::spawn(server_raw, cfg, cancel.child_token());

        // Client sends Ping manually
        let mut client_framed = FramedControlStream::new(client_raw);
        client_framed.send(ControlMessage::Ping).await.unwrap();

        // Client should receive automatic Pong from server actor without server rx waking up
        let reply = client_framed.next().await.unwrap().unwrap();
        assert_eq!(reply, ControlMessage::Pong);

        // Application message should pass through to srv_rx
        client_framed
            .send(ControlMessage::NewConnection {
                session_id: "test_sess".into(),
                client_addr: "1.2.3.4:5678".into(),
            })
            .await
            .unwrap();

        let event = srv_rx.recv_msg().await.unwrap().unwrap();
        assert!(matches!(event, ControlMessage::NewConnection { .. }));

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_signaling_channel_idle_timeout() {
        let (_client_raw, server_raw) = duplex(4096);
        let cancel = CancellationToken::new();

        let cfg = SignalingConfig {
            heartbeat_interval: Duration::from_millis(100),
            idle_timeout: Duration::from_millis(100),
        };

        let (_srv_handle, mut srv_rx) =
            SignalingChannel::spawn(server_raw, cfg, cancel.child_token());

        // No activity for 100ms should trigger idle timeout and close receiver
        let res = timeout(Duration::from_millis(500), srv_rx.recv_msg()).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap().unwrap(), None);
    }
}
