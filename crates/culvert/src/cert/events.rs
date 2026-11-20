// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Certificate lifecycle and reload event types and event bus.
//! All comments and docstrings must be in English.

use std::path::PathBuf;
use tokio::sync::broadcast;

/// Event emitted during certificate lifecycle and dynamic reload operations.
#[derive(Debug, Clone)]
pub enum CertEvent {
    /// Certificate and key pair reloaded successfully.
    ServerCertReloaded {
        cert_path: PathBuf,
        key_path: PathBuf,
    },
    /// Server certificate reload failed (previous certificate retained).
    ServerCertReloadFailed {
        cert_path: PathBuf,
        key_path: PathBuf,
        error: String,
    },
    /// Client CA root store reloaded successfully.
    ClientCaReloaded {
        ca_path: PathBuf,
    },
    /// Client CA root store reload failed (previous root store retained).
    ClientCaReloadFailed {
        ca_path: PathBuf,
        error: String,
    },
}

/// Decoupled event bus for certificate lifecycle events.
#[derive(Clone)]
pub struct CertEventBus {
    sender: broadcast::Sender<CertEvent>,
}

impl CertEventBus {
    /// Creates a new event bus with the given channel capacity.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Emits a certificate event to all active subscribers.
    pub fn emit(&self, event: CertEvent) {
        // If there are no active receivers, send error is ignored.
        let _ = self.sender.send(event);
    }

    /// Subscribes to certificate events.
    pub fn subscribe(&self) -> broadcast::Receiver<CertEvent> {
        self.sender.subscribe()
    }
}

impl Default for CertEventBus {
    fn default() -> Self {
        Self::new(64)
    }
}
