// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! File system watcher with asynchronous debouncing for certificate hot-reloading.
//! All comments and docstrings must be in English.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::core::error::TunnelError;

/// Default quiet window for coalescing rapid multi-file writes during certificate renewal.
pub const DEFAULT_DEBOUNCE_INTERVAL: Duration = Duration::from_millis(1000);

/// Filesystem watcher that monitors certificate and key files, aggregating events
/// through a debounce quiet window to avoid partial-write race conditions.
pub struct CertFileWatcher {
    _watcher: RecommendedWatcher,
    watched_paths: Arc<HashSet<PathBuf>>,
}

impl CertFileWatcher {
    /// Creates and spawns a debounced file watcher for the provided target paths.
    /// When changes are detected and settle after `debounce_interval`, `on_reload` is invoked.
    pub fn spawn<F, Fut>(
        paths: Vec<PathBuf>,
        debounce_interval: Duration,
        mut on_reload: F,
        cancel: CancellationToken,
    ) -> Result<Self, TunnelError>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded_channel::<()>();

        // Canonicalize paths if possible, or keep absolute representations
        let mut canonical_paths = HashSet::new();
        let mut watch_dirs = HashSet::new();

        for p in &paths {
            let abs_path = if p.is_absolute() {
                p.clone()
            } else {
                std::env::current_dir()
                    .map(|cd| cd.join(p))
                    .unwrap_or_else(|_| p.clone())
            };
            let can = abs_path.canonicalize().unwrap_or_else(|_| abs_path.clone());
            canonical_paths.insert(can.clone());

            // Watch the parent directory to capture atomic renames and file replacements cleanly
            if let Some(parent) = can.parent() {
                watch_dirs.insert(parent.to_path_buf());
            } else {
                watch_dirs.insert(can);
            }
        }

        let watched_set = Arc::new(canonical_paths);
        let watched_set_clone = Arc::clone(&watched_set);

        let mut watcher = RecommendedWatcher::new(
            move |res: Result<Event, notify::Error>| match res {
                Ok(event) => {
                    // Check if any path in the event matches our watched target paths
                    let is_relevant = event.paths.iter().any(|ep| {
                        let can_ep = ep.canonicalize().unwrap_or_else(|_| ep.clone());
                        let match_direct = watched_set_clone.contains(ep);
                        let match_can = watched_set_clone.contains(&can_ep);
                        let match_name = watched_set_clone.iter().any(|wp| {
                            wp.file_name().is_some() && wp.file_name() == ep.file_name()
                        });
                        match_direct || match_can || match_name
                    });

                    if is_relevant {
                        debug!("Observed certificate filesystem event: {:?}", event.kind);
                        let _ = tx.send(());
                    }
                }
                Err(err) => {
                    warn!("Filesystem watcher encountered an error: {}", err);
                }
            },
            notify::Config::default(),
        )
        .map_err(|e| TunnelError::Tls(format!("Failed to initialize file watcher: {}", e)))?;

        // Register all parent directories
        for dir in &watch_dirs {
            if dir.exists() {
                watcher
                    .watch(dir, RecursiveMode::NonRecursive)
                    .map_err(|e| {
                        TunnelError::Tls(format!("Failed to watch directory {:?}: {}", dir, e))
                    })?;
                debug!("Watching directory for certificate changes: {:?}", dir);
            }
        }

        // Spawn Tokio debouncing task if a Tokio runtime is available
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let cancel_task = cancel.clone();
            handle.spawn(async move {
                let mut deadline: Option<tokio::time::Instant> = None;

                loop {
                    tokio::select! {
                        biased;

                        _ = cancel_task.cancelled() => {
                            debug!("Certificate watcher task received cancellation; exiting");
                            break;
                        }

                        Some(()) = rx.recv() => {
                            // Reset or set debounce deadline to (now + debounce_interval)
                            let new_deadline = tokio::time::Instant::now() + debounce_interval;
                            deadline = Some(new_deadline);
                            debug!("Certificate change event received; debounce timer reset to {:?}", debounce_interval);
                        }

                        _ = async {
                            match deadline {
                                Some(d) => tokio::time::sleep_until(d).await,
                                None => std::future::pending().await,
                            }
                        } => {
                            deadline = None;
                            info!("Debounce window elapsed; triggering certificate reload");
                            on_reload().await;
                        }
                    }
                }
            });
        }

        Ok(Self {
            _watcher: watcher,
            watched_paths: watched_set,
        })
    }

    /// Returns the set of watched file paths.
    pub fn watched_paths(&self) -> &HashSet<PathBuf> {
        &self.watched_paths
    }
}
